package engine

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/gorilla/websocket"
	"go.uber.org/zap"
	"golang-im-system/internal/burn"
	"golang-im-system/internal/proto"
	googleproto "google.golang.org/protobuf/proto"
)

var upgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool { return true },
}

// ClientConn represents a connected user.
type ClientConn struct {
	Name   string
	ID     string
	conn   *websocket.Conn
	send   chan *proto.WsMessage
	closed bool
}

// ChatServer is the centralized messaging server.
type ChatServer struct {
	addr   string
	online map[string]*ClientConn
	mu     sync.RWMutex
	server *http.Server
	logger *zap.Logger
	burnMgr *burn.Manager
}

// New creates a ChatServer.
func New(addr string) *ChatServer {
	logger, _ := zap.NewDevelopment()
	s := &ChatServer{
		addr:   addr,
		online: make(map[string]*ClientConn),
		logger: logger,
		burnMgr: burn.New(),
	}
	s.burnMgr.OnBurn = s.onBurn
	return s
}

// onBurn is called when a burn timer expires.
func (s *ChatServer) onBurn(messageID string) {
	s.broadcast(&proto.WsMessage{
		Type: proto.MsgType_BURN_RECEIPT,
		Payload: &proto.WsMessage_Receipt{
			Receipt: &proto.BurnReceipt{
				MessageId: messageID,
				BurnedAt:  time.Now().Unix(),
			},
		},
	})
}

// Start begins listening.
func (s *ChatServer) Start() error {
	mux := http.NewServeMux()
	mux.HandleFunc("/ws", s.handleWS)
	s.server = &http.Server{Addr: s.addr, Handler: mux}
	s.logger.Info("ChatServer starting", zap.String("addr", s.addr))
	return s.server.ListenAndServe()
}

// Stop gracefully shuts down.
func (s *ChatServer) Stop(ctx context.Context) error {
	s.mu.Lock()
	conns := make([]*ClientConn, 0, len(s.online))
	for _, c := range s.online {
		c.closed = true
		conns = append(conns, c)
	}
	s.mu.Unlock()

	for _, c := range conns {
		c.conn.Close() // triggers readPump defer → natural cleanup
	}
	return s.server.Shutdown(ctx)
}

func (s *ChatServer) handleWS(w http.ResponseWriter, r *http.Request) {
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		s.logger.Error("upgrade failed", zap.Error(err))
		return
	}

	client := &ClientConn{
		ID:   uuid.New().String()[:8],
		conn: conn,
		send: make(chan *proto.WsMessage, 256),
	}
	client.Name = "user-" + client.ID

	s.mu.Lock()
	s.online[client.ID] = client
	s.mu.Unlock()

	s.logger.Info("user online", zap.String("name", client.Name), zap.String("id", client.ID))
	s.broadcastSystem(proto.SystemType_SYS_JOIN, client.Name)

	go s.readPump(client)
	go s.writePump(client)
}

func (s *ChatServer) readPump(c *ClientConn) {
	defer func() {
		s.mu.Lock()
		if !c.closed {
			c.closed = true
			close(c.send)
			c.conn.Close()
		}
		delete(s.online, c.ID)
		s.mu.Unlock()
		s.logger.Info("user offline", zap.String("name", c.Name))
		s.broadcastSystem(proto.SystemType_SYS_LEAVE, c.Name)
	}()

	for {
		_, raw, err := c.conn.ReadMessage()
		if err != nil {
			return
		}
		msg := &proto.WsMessage{}
		if err := googleproto.Unmarshal(raw, msg); err != nil {
			continue
		}
		msg.MessageId = uuid.New().String()
		msg.Timestamp = time.Now().Unix()
		s.dispatch(c, msg)
	}
}

func (s *ChatServer) writePump(c *ClientConn) {
	for msg := range c.send {
		data, _ := googleproto.Marshal(msg)
		c.conn.WriteMessage(websocket.BinaryMessage, data)
	}
}

func (s *ChatServer) dispatch(from *ClientConn, msg *proto.WsMessage) {
	switch msg.Type {
	case proto.MsgType_CHAT:
		s.handleChat(from, msg.GetChat())
	case proto.MsgType_PRIVATE_CHAT:
		s.handlePrivateChat(from, msg.GetChat())
	case proto.MsgType_WHO:
		s.handleWho(from)
	case proto.MsgType_RENAME:
		s.handleRename(from, msg.GetSystem().GetContent())
	case proto.MsgType_HEARTBEAT:
		from.send <- &proto.WsMessage{
			MessageId: msg.MessageId,
			Type:      proto.MsgType_HEARTBEAT,
			Payload: &proto.WsMessage_Hb{
				Hb: &proto.Heartbeat{PingTs: time.Now().Unix()},
			},
		}
	}
}

func (s *ChatServer) handleChat(from *ClientConn, chat *proto.ChatMessage) {
	msgID := uuid.New().String()
	if chat.BurnSeconds > 0 {
		s.burnMgr.Add(msgID, chat.BurnSeconds)
	}
	out := &proto.WsMessage{
		MessageId: msgID,
		Type:      proto.MsgType_CHAT,
		Payload: &proto.WsMessage_Chat{
			Chat: &proto.ChatMessage{
				From:        from.Name,
				Content:     chat.Content,
				BurnSeconds: chat.BurnSeconds,
			},
		},
	}
	s.broadcast(out)
}

func (s *ChatServer) handlePrivateChat(from *ClientConn, chat *proto.ChatMessage) {
	s.mu.RLock()
	target, ok := s.findByName(chat.To)
	s.mu.RUnlock()
	if !ok {
		from.send <- systemMsg("user not found: " + chat.To)
		return
	}
	msgID := uuid.New().String()
	if chat.BurnSeconds > 0 {
		s.burnMgr.Add(msgID, chat.BurnSeconds)
	}
	out := &proto.WsMessage{
		MessageId: msgID,
		Type:      proto.MsgType_PRIVATE_CHAT,
		Payload: &proto.WsMessage_Chat{
			Chat: &proto.ChatMessage{
				From:        from.Name,
				Content:     chat.Content,
				BurnSeconds: chat.BurnSeconds,
			},
		},
	}
	target.send <- out
	from.send <- out // echo back to sender
}

func (s *ChatServer) handleWho(from *ClientConn) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	peers := make([]map[string]string, 0)
	for _, c := range s.online {
		peers = append(peers, map[string]string{
			"id": c.ID, "name": c.Name,
		})
	}
	data, _ := json.Marshal(peers)
	from.send <- systemMsg(string(data))
}

func (s *ChatServer) handleRename(from *ClientConn, newName string) {
	s.mu.Lock()
	if _, exists := s.findByNameLocked(newName); exists {
		s.mu.Unlock()
		from.send <- systemMsg("name already taken")
		return
	}
	oldName := from.Name
	from.Name = newName
	s.mu.Unlock()

	s.broadcastSystem(proto.SystemType_SYS_RENAME, fmt.Sprintf("%s->%s", oldName, newName))
}

func (s *ChatServer) findByName(name string) (*ClientConn, bool) {
	for _, c := range s.online {
		if c.Name == name {
			return c, true
		}
	}
	return nil, false
}

func (s *ChatServer) findByNameLocked(name string) (*ClientConn, bool) {
	return s.findByName(name)
}

func (s *ChatServer) broadcast(msg *proto.WsMessage) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	for _, c := range s.online {
		select {
		case c.send <- msg:
		default:
		}
	}
}

func (s *ChatServer) broadcastSystem(st proto.SystemType, content string) {
	s.broadcast(&proto.WsMessage{
		Type: proto.MsgType_SYSTEM,
		Payload: &proto.WsMessage_System{
			System: &proto.SystemNotice{Type: st, Content: content},
		},
	})
}

func systemMsg(content string) *proto.WsMessage {
	return &proto.WsMessage{
		Type: proto.MsgType_SYSTEM,
		Payload: &proto.WsMessage_System{
			System: &proto.SystemNotice{Type: proto.SystemType_SYS_ERROR, Content: content},
		},
	}
}
