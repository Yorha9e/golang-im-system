package transport

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/pion/webrtc/v3"
	"golang-im-system/internal/proto"
	"golang-im-system/internal/signaling"
	googleproto "google.golang.org/protobuf/proto"
)

const fallbackTimeout = 15 * time.Second

// WANP2PTransport implements WAN P2P with STUN hole-punching.
// Falls back to ServerTransport if direct connection fails.
type WANP2PTransport struct {
	nodeID   string
	nodeName string
	sigAddr  string           // signaling server addr (host:port)
	fallback string           // ChatServer addr for fallback
	token    string           // JWT for fallback

	sigClient *signaling.SignalingClient
	peerConns map[string]*webrtc.PeerConnection
	dataChans map[string]*webrtc.DataChannel
	mu        sync.RWMutex
	recv      chan *Message
	errCh     chan error
	ctx       context.Context
	cancel    context.CancelFunc

	fallbackTransport *ServerTransport
	fallbackUsed      bool
	OnFallback        func()
}

// NewWANP2PTransport creates a WAN P2P transport.
func NewWANP2PTransport(name, signalingAddr, fallbackServerAddr, token string) *WANP2PTransport {
	return &WANP2PTransport{
		nodeID:   uuid.New().String()[:8],
		nodeName: name,
		sigAddr:  signalingAddr,
		fallback: fallbackServerAddr,
		token:    token,
		peerConns: make(map[string]*webrtc.PeerConnection),
		dataChans: make(map[string]*webrtc.DataChannel),
		recv:      make(chan *Message, 256),
		errCh:     make(chan error, 16),
	}
}

// Start connects to signaling, begins ICE negotiation, arms fallback timer.
func (t *WANP2PTransport) Start(ctx context.Context) error {
	t.ctx, t.cancel = context.WithCancel(ctx)

	t.sigClient = signaling.NewSignalingClient(t.sigAddr, t.nodeName)
	t.sigClient.OnPeerJoin = t.onPeerJoin
	t.sigClient.OnPeerLeave = t.onPeerLeave
	t.sigClient.OnMessage = t.onSigMessage

	if err := t.sigClient.Connect(); err != nil {
		return fmt.Errorf("signaling: %w", err)
	}

	// Arm fallback timer.
	go func() {
		timer := time.NewTimer(fallbackTimeout)
		defer timer.Stop()
		select {
		case <-timer.C:
			t.tryFallback()
		case <-t.ctx.Done():
		}
	}()

	return nil
}

// Stop tears down all PeerConnections and the signaling connection.
func (t *WANP2PTransport) Stop() error {
	if t.cancel != nil {
		t.cancel()
	}
	if t.sigClient != nil {
		t.sigClient.Close()
	}
	t.mu.Lock()
	for _, pc := range t.peerConns {
		pc.Close()
	}
	t.mu.Unlock()
	if t.fallbackTransport != nil {
		t.fallbackTransport.Stop()
	}
	return nil
}

// --- Transport interface ---

func (t *WANP2PTransport) Broadcast(msg *Message) error {
	if t.fallbackUsed {
		return t.fallbackTransport.Broadcast(msg)
	}
	data, _ := googleproto.Marshal(msg.Msg)
	t.mu.RLock()
	defer t.mu.RUnlock()
	var lastErr error
	for peerID, dc := range t.dataChans {
		if err := dc.Send(data); err != nil {
			lastErr = fmt.Errorf("send to %s: %w", peerID, err)
		}
	}
	return lastErr
}

func (t *WANP2PTransport) PrivateSend(target string, msg *Message) error {
	if t.fallbackUsed {
		return t.fallbackTransport.PrivateSend(target, msg)
	}
	data, _ := googleproto.Marshal(msg.Msg)
	t.mu.RLock()
	dc, ok := t.dataChans[target]
	t.mu.RUnlock()
	if !ok {
		return fmt.Errorf("peer %s not found", target)
	}
	return dc.Send(data)
}

func (t *WANP2PTransport) Who() ([]Peer, error) {
	if t.fallbackUsed {
		return t.fallbackTransport.Who()
	}
	t.mu.RLock()
	defer t.mu.RUnlock()
	peers := make([]Peer, 0, len(t.dataChans))
	for id := range t.dataChans {
		peers = append(peers, Peer{ID: id})
	}
	return peers, nil
}

func (t *WANP2PTransport) Rename(name string) error {
	t.nodeName = name
	return nil
}

func (t *WANP2PTransport) Recv() <-chan *Message { return t.recv }
func (t *WANP2PTransport) SendErr() <-chan error  { return t.errCh }

// --- WebRTC negotiation ---

func (t *WANP2PTransport) onPeerJoin(peerID, peerName string) {
	// Glare avoidance: lower nodeID sends the offer.
	if t.nodeID < peerID {
		go t.createOffer(peerID, false)
	}
	// Higher nodeID waits for incoming offer.
}

func (t *WANP2PTransport) onPeerLeave(peerID string) {
	t.mu.Lock()
	if pc, ok := t.peerConns[peerID]; ok {
		pc.Close()
		delete(t.peerConns, peerID)
		delete(t.dataChans, peerID)
	}
	t.mu.Unlock()
}

func (t *WANP2PTransport) onSigMessage(msg signaling.SigMessage) {
	switch msg.Type {
	case "offer":
		go t.handleOffer(msg)
	case "answer":
		go t.handleAnswer(msg)
	case "ice_candidate":
		go t.handleICE(msg)
	}
}

func (t *WANP2PTransport) createOffer(peerID string, polite bool) {
	pc, _, err := t.newPeerConnection(peerID, polite)
	if err != nil {
		t.tryFallback()
		return
	}

	offer, err := pc.CreateOffer(nil)
	if err != nil {
		return
	}
	pc.SetLocalDescription(offer)

	data, _ := json.Marshal(offer)
	t.sigClient.Send(signaling.SigMessage{
		Type:    "offer",
		To:      peerID,
		Payload: data,
	})
}

func (t *WANP2PTransport) handleOffer(msg signaling.SigMessage) {
	peerID := msg.From

	t.mu.RLock()
	_, exists := t.peerConns[peerID]
	t.mu.RUnlock()
	if exists {
		return // already negotiating
	}

	// Only respond if we're the polite side (higher ID).
	if t.nodeID < peerID {
		// We already sent our own offer (impolite), ignore incoming.
		// Actually, create a new PC but handle collision.
	}

	pc, _, err := t.newPeerConnection(peerID, true)
	if err != nil {
		return
	}

	var offer webrtc.SessionDescription
	json.Unmarshal(msg.Payload, &offer)
	pc.SetRemoteDescription(offer)

	answer, err := pc.CreateAnswer(nil)
	if err != nil {
		return
	}
	pc.SetLocalDescription(answer)

	data, _ := json.Marshal(answer)
	t.sigClient.Send(signaling.SigMessage{
		Type:    "answer",
		To:      peerID,
		Payload: data,
	})
}

func (t *WANP2PTransport) handleAnswer(msg signaling.SigMessage) {
	t.mu.RLock()
	pc, ok := t.peerConns[msg.From]
	t.mu.RUnlock()
	if !ok {
		return
	}

	var answer webrtc.SessionDescription
	json.Unmarshal(msg.Payload, &answer)
	pc.SetRemoteDescription(answer)
}

func (t *WANP2PTransport) handleICE(msg signaling.SigMessage) {
	t.mu.RLock()
	pc, ok := t.peerConns[msg.From]
	t.mu.RUnlock()
	if !ok {
		return
	}

	var candidate webrtc.ICECandidateInit
	json.Unmarshal(msg.Payload, &candidate)
	pc.AddICECandidate(candidate)
}

func (t *WANP2PTransport) newPeerConnection(peerID string, polite bool) (*webrtc.PeerConnection, *webrtc.DataChannel, error) {
	config := webrtc.Configuration{
		ICEServers: []webrtc.ICEServer{
			{URLs: []string{"stun:stun.l.google.com:19302"}},
		},
	}

	se := webrtc.SettingEngine{}
	se.SetICETimeouts(10*time.Second, 5*time.Second, 2*time.Second)

	api := webrtc.NewAPI(webrtc.WithSettingEngine(se))
	pc, err := api.NewPeerConnection(config)
	if err != nil {
		return nil, nil, err
	}

	dc, err := pc.CreateDataChannel("chat", nil)
	if err != nil {
		pc.Close()
		return nil, nil, err
	}

	setupDataChannel := func(ch *webrtc.DataChannel) {
		ch.OnOpen(func() {
			t.mu.Lock()
			t.dataChans[peerID] = ch
			t.mu.Unlock()
		})
		ch.OnMessage(func(msg webrtc.DataChannelMessage) {
			var wsm proto.WsMessage
			googleproto.Unmarshal(msg.Data, &wsm)
			select {
			case t.recv <- &Message{Msg: &wsm, From: peerID}:
			case <-t.ctx.Done():
			}
		})
	}
	setupDataChannel(dc)

	// Handle data channel created by the remote peer (answering side).
	pc.OnDataChannel(func(ch *webrtc.DataChannel) {
		setupDataChannel(ch)
	})

	pc.OnICECandidate(func(c *webrtc.ICECandidate) {
		if c == nil {
			return
		}
		cJSON, _ := json.Marshal(c.ToJSON())
		t.sigClient.Send(signaling.SigMessage{
			Type:    "ice_candidate",
			To:      peerID,
			Payload: cJSON,
		})
	})

	pc.OnConnectionStateChange(func(state webrtc.PeerConnectionState) {
		if state == webrtc.PeerConnectionStateConnected {
			t.mu.Lock()
			t.peerConns[peerID] = pc
			t.mu.Unlock()
		}
		if state == webrtc.PeerConnectionStateFailed || state == webrtc.PeerConnectionStateDisconnected {
			t.onPeerLeave(peerID)
			if len(t.dataChans) == 0 {
				t.tryFallback()
			}
		}
	})

	t.mu.Lock()
	t.peerConns[peerID] = pc
	t.mu.Unlock()

	return pc, dc, nil
}

// --- Fallback ---

func (t *WANP2PTransport) tryFallback() {
	t.mu.Lock()
	if t.fallbackUsed {
		t.mu.Unlock()
		return
	}
	t.fallbackUsed = true
	t.mu.Unlock()

	if t.OnFallback != nil {
		t.OnFallback()
	}

	t.fallbackTransport = NewServerTransport(t.fallback, t.token)
	go func() {
		if err := t.fallbackTransport.Start(t.ctx); err != nil {
			t.errCh <- fmt.Errorf("fallback start: %w", err)
			return
		}
		// Forward messages from fallback transport to the main recv channel.
		for msg := range t.fallbackTransport.Recv() {
			select {
			case t.recv <- msg:
			case <-t.ctx.Done():
				return
			}
		}
	}()
}

// IsFallback returns true if the transport has fallen back to server mode.
func (t *WANP2PTransport) IsFallback() bool {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.fallbackUsed
}
