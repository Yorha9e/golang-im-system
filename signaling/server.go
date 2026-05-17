package signaling

import (
	"encoding/json"
	"net/http"
	"sync"

	"github.com/google/uuid"
	"github.com/gorilla/websocket"
)

var upgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool { return true },
}

// SigMessage is the signaling protocol message.
type SigMessage struct {
	Type    string          `json:"type"`
	From    string          `json:"from,omitempty"`
	To      string          `json:"to,omitempty"`
	Payload json.RawMessage `json:"payload,omitempty"`
}

// SigPeer represents a connected signaling client.
type SigPeer struct {
	ID   string
	Name string
	conn *websocket.Conn
	send chan []byte
}

// SignalingServer brokers SDP/ICE exchange between peers for NAT traversal.
// It does NOT relay chat messages — only SDP offers, answers, and ICE candidates.
type SignalingServer struct {
	addr  string
	peers map[string]*SigPeer
	mu    sync.RWMutex
}

// NewSignalingServer creates a signaling server.
func NewSignalingServer(addr string) *SignalingServer {
	return &SignalingServer{
		addr:  addr,
		peers: make(map[string]*SigPeer),
	}
}

// Start begins listening for WebSocket connections.
func (s *SignalingServer) Start() error {
	http.HandleFunc("/signal", s.handleWS)
	return http.ListenAndServe(s.addr, nil)
}

func (s *SignalingServer) handleWS(w http.ResponseWriter, r *http.Request) {
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		return
	}

	peer := &SigPeer{
		ID:   uuid.New().String()[:8],
		conn: conn,
		send: make(chan []byte, 64),
		Name: r.URL.Query().Get("name"),
	}

	s.mu.Lock()
	s.peers[peer.ID] = peer
	s.mu.Unlock()

	s.notifyJoin(peer)

	go s.readPump(peer)
	s.writePump(peer)
}

func (s *SignalingServer) readPump(p *SigPeer) {
	defer func() {
		s.mu.Lock()
		delete(s.peers, p.ID)
		s.mu.Unlock()
		close(p.send)
		p.conn.Close()
		s.notifyLeave(p)
	}()

	for {
		_, raw, err := p.conn.ReadMessage()
		if err != nil {
			return
		}
		var msg SigMessage
		if err := json.Unmarshal(raw, &msg); err != nil {
			continue
		}
		msg.From = p.ID

		data, _ := json.Marshal(msg)

		if msg.To != "" {
			// Route to specific peer.
			s.mu.RLock()
			target, ok := s.peers[msg.To]
			s.mu.RUnlock()
			if ok {
				select {
				case target.send <- data:
				default:
				}
			}
		}
	}
}

func (s *SignalingServer) writePump(p *SigPeer) {
	for data := range p.send {
		p.conn.WriteMessage(websocket.TextMessage, data)
	}
}

func (s *SignalingServer) notifyJoin(p *SigPeer) {
	info := PeerInfo{ID: p.ID, Name: p.Name}
	s.broadcast(SigMessage{
		Type:    "peer_join",
		From:    p.ID,
		Payload: mustMarshalJSON(info),
	}, p.ID)
}

func (s *SignalingServer) notifyLeave(p *SigPeer) {
	info := PeerInfo{ID: p.ID, Name: p.Name}
	s.broadcast(SigMessage{
		Type:    "peer_leave",
		From:    p.ID,
		Payload: mustMarshalJSON(info),
	}, p.ID)
}

func (s *SignalingServer) broadcast(msg SigMessage, excludeID string) {
	data, _ := json.Marshal(msg)
	s.mu.RLock()
	defer s.mu.RUnlock()
	for id, peer := range s.peers {
		if id != excludeID {
			select {
			case peer.send <- data:
			default:
			}
		}
	}
}

// PeerInfo is sent in join/leave notifications.
type PeerInfo struct {
	ID   string `json:"id"`
	Name string `json:"name"`
}

func mustMarshalJSON(v interface{}) json.RawMessage {
	data, _ := json.Marshal(v)
	return data
}
