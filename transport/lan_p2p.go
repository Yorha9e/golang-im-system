package transport

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"sync"

	"github.com/google/uuid"
	"github.com/gorilla/websocket"
	"github.com/Yorha9e/golang-im-system/discovery"
	"github.com/Yorha9e/golang-im-system/proto"
	googleproto "google.golang.org/protobuf/proto"
)

// LANP2PTransport implements full-mesh P2P over LAN using UDP multicast discovery.
type LANP2PTransport struct {
	nodeID   string
	nodeName string
	wsPort   int

	mu     sync.RWMutex
	peers  map[string]*p2pPeer // nodeID -> peer
	recv   chan *Message
	errCh  chan error
	ctx    context.Context
	cancel context.CancelFunc

	disc     *discovery.MulticastDiscovery
	listener net.Listener // the raw listener so we can close it
}

type p2pPeer struct {
	NodeID   string
	NodeName string
	conn     *websocket.Conn
	send     chan []byte
}

var upgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool { return true },
}

// NewLANP2PTransport creates a LAN P2P transport.
func NewLANP2PTransport(name string, wsPort int) *LANP2PTransport {
	return &LANP2PTransport{
		nodeID:   uuid.New().String()[:8],
		nodeName: name,
		wsPort:   wsPort,
		peers:    make(map[string]*p2pPeer),
		recv:     make(chan *Message, 256),
		errCh:    make(chan error, 16),
	}
}

// Start begins the WebSocket server and multicast discovery.
func (t *LANP2PTransport) Start(ctx context.Context) error {
	t.ctx, t.cancel = context.WithCancel(ctx)

	mux := http.NewServeMux()
	mux.HandleFunc("/ws", t.handleWS)
	addr := fmt.Sprintf(":%d", t.wsPort)
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("listen %s: %w", addr, err)
	}
	t.listener = ln
	go http.Serve(ln, mux)

	t.disc = discovery.NewMulticastDiscovery(t.nodeID, t.nodeName, t.wsPort)
	t.disc.SetCallbacks(t.onPeerJoin, t.onPeerLeave)
	if err := t.disc.Start(ctx); err != nil {
		return fmt.Errorf("discovery: %w", err)
	}
	return nil
}

// Stop shuts down all peer connections and discovery.
func (t *LANP2PTransport) Stop() error {
	if t.cancel != nil {
		t.cancel()
	}
	t.mu.Lock()
	for _, p := range t.peers {
		close(p.send)
		p.conn.Close()
	}
	t.peers = make(map[string]*p2pPeer)
	t.mu.Unlock()

	if t.disc != nil {
		t.disc.Stop()
	}
	if t.listener != nil {
		t.listener.Close()
	}
	return nil
}

// ---- Transport interface ----

func (t *LANP2PTransport) Broadcast(msg *Message) error {
	data, _ := googleproto.Marshal(msg.Msg)
	t.mu.RLock()
	defer t.mu.RUnlock()
	for _, p := range t.peers {
		select {
		case p.send <- data:
		default:
		}
	}
	return nil
}

func (t *LANP2PTransport) PrivateSend(target string, msg *Message) error {
	data, _ := googleproto.Marshal(msg.Msg)
	t.mu.RLock()
	p, ok := t.peers[target]
	t.mu.RUnlock()
	if !ok {
		return fmt.Errorf("peer %s not found", target)
	}
	select {
	case p.send <- data:
	default:
	}
	return nil
}

func (t *LANP2PTransport) Who() ([]Peer, error) {
	t.mu.RLock()
	defer t.mu.RUnlock()
	peers := make([]Peer, 0, len(t.peers)+1)
	for _, p := range t.peers {
		peers = append(peers, Peer{ID: p.NodeID, Name: p.NodeName})
	}
	return peers, nil
}

func (t *LANP2PTransport) Rename(name string) error {
	t.nodeName = name
	t.disc.SetLocalName(name)
	return nil
}

func (t *LANP2PTransport) Recv() <-chan *Message { return t.recv }
func (t *LANP2PTransport) SendErr() <-chan error  { return t.errCh }

// ---- Internal ----

// handleWS accepts incoming peer connections.
func (t *LANP2PTransport) handleWS(w http.ResponseWriter, r *http.Request) {
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		return
	}

	var hs discovery.Announce
	if err := conn.ReadJSON(&hs); err != nil {
		conn.Close()
		return
	}

	// Dedup: if we already have a connection to this peer, drop the duplicate.
	t.mu.Lock()
	if _, exists := t.peers[hs.NodeID]; exists {
		t.mu.Unlock()
		conn.Close()
		return
	}
	peer := &p2pPeer{NodeID: hs.NodeID, NodeName: hs.NodeName, conn: conn, send: make(chan []byte, 256)}
	t.peers[hs.NodeID] = peer
	t.mu.Unlock()

	go t.readPump(peer)
	t.writePump(peer)
}

// onPeerJoin is called when discovery finds a new peer.
func (t *LANP2PTransport) onPeerJoin(p discovery.DiscoveredPeer) {
	t.mu.RLock()
	_, exists := t.peers[p.NodeID]
	t.mu.RUnlock()
	if exists {
		return
	}

	addr := fmt.Sprintf("%s:%d", p.Addr.String(), p.WSPort)
	u := url.URL{Scheme: "ws", Host: addr, Path: "/ws"}
	conn, _, err := websocket.DefaultDialer.Dial(u.String(), nil)
	if err != nil {
		return
	}

	// Send handshake with our node info.
	hs := discovery.Announce{NodeID: t.nodeID, NodeName: t.nodeName, WSPort: t.wsPort}
	if err := conn.WriteJSON(hs); err != nil {
		conn.Close()
		return
	}

	peer := &p2pPeer{NodeID: p.NodeID, NodeName: p.NodeName, conn: conn, send: make(chan []byte, 256)}

	// Dedup check after connection established.
	t.mu.Lock()
	if _, exists := t.peers[p.NodeID]; exists {
		t.mu.Unlock()
		conn.Close()
		return
	}
	t.peers[p.NodeID] = peer
	t.mu.Unlock()

	go t.readPump(peer)
	t.writePump(peer)
}

// onPeerLeave is called when discovery prunes a stale peer.
func (t *LANP2PTransport) onPeerLeave(nodeID string) {
	t.mu.Lock()
	if p, ok := t.peers[nodeID]; ok {
		close(p.send)
		p.conn.Close()
		delete(t.peers, nodeID)
	}
	t.mu.Unlock()
}

func (t *LANP2PTransport) readPump(p *p2pPeer) {
	defer func() {
		t.mu.Lock()
		if peer, ok := t.peers[p.NodeID]; ok && peer == p {
			delete(t.peers, p.NodeID)
		}
		t.mu.Unlock()
		close(p.send)
		p.conn.Close()
	}()

	for {
		_, raw, err := p.conn.ReadMessage()
		if err != nil {
			return
		}
		msg := &proto.WsMessage{}
		if err := googleproto.Unmarshal(raw, msg); err != nil {
			continue
		}
		select {
		case t.recv <- &Message{Msg: msg, From: p.NodeName}:
		case <-t.ctx.Done():
			return
		}
	}
}

func (t *LANP2PTransport) writePump(p *p2pPeer) {
	for data := range p.send {
		p.conn.WriteMessage(websocket.BinaryMessage, data)
	}
}
