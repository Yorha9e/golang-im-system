package signaling

import (
	"encoding/json"
	"net/url"
	"sync"

	"github.com/gorilla/websocket"
)

// SignalingClient connects to a SignalingServer for SDP/ICE exchange.
type SignalingClient struct {
	serverURL   string
	name        string
	conn        *websocket.Conn
	writeMu     sync.Mutex
	OnMessage   func(SigMessage)
	OnPeerJoin  func(id, name string)
	OnPeerLeave func(id string)
}

// NewSignalingClient creates a client that connects to the given signaling server.
func NewSignalingClient(serverAddr, name string) *SignalingClient {
	return &SignalingClient{
		serverURL: "ws://" + serverAddr + "/signal?name=" + url.QueryEscape(name),
		name:      name,
	}
}

// Connect dials the signaling server and starts reading messages.
func (c *SignalingClient) Connect() error {
	conn, _, err := websocket.DefaultDialer.Dial(c.serverURL, nil)
	if err != nil {
		return err
	}
	c.conn = conn
	go c.readLoop()
	return nil
}

func (c *SignalingClient) readLoop() {
	for {
		_, raw, err := c.conn.ReadMessage()
		if err != nil {
			return
		}
		var msg SigMessage
		if err := json.Unmarshal(raw, &msg); err != nil {
			continue
		}

		switch msg.Type {
		case "peer_join":
			var info PeerInfo
			json.Unmarshal(msg.Payload, &info)
			if c.OnPeerJoin != nil {
				c.OnPeerJoin(info.ID, info.Name)
			}
		case "peer_leave":
			var info PeerInfo
			json.Unmarshal(msg.Payload, &info)
			if c.OnPeerLeave != nil {
				c.OnPeerLeave(info.ID)
			}
		default:
			if c.OnMessage != nil {
				c.OnMessage(msg)
			}
		}
	}
}

// Send marshals and sends a signaling message. Thread-safe.
func (c *SignalingClient) Send(msg SigMessage) error {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	data, _ := json.Marshal(msg)
	return c.conn.WriteMessage(websocket.TextMessage, data)
}

// Close shuts down the connection. Safe to call even if not connected.
func (c *SignalingClient) Close() error {
	if c.conn != nil {
		return c.conn.Close()
	}
	return nil
}
