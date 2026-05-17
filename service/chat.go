package service

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"golang-im-system/burn"
	"golang-im-system/proto"
	"golang-im-system/transport"
	googleproto "google.golang.org/protobuf/proto"
)

// DisplayMsg is a UI-agnostic message representation.
type DisplayMsg struct {
	Type        string `json:"type"` // chat, system, private, burn, who
	From        string `json:"from,omitempty"`
	Content     string `json:"content"`
	BurnSeconds int32  `json:"burn_seconds,omitempty"`
	MessageID   string `json:"message_id,omitempty"`
}

// Session is the UI-agnostic chat session. It wraps a Transport and
// BurnManager. Replace the Transport to switch modes (server → P2P).
// Replace the UI adapter (CLI, TUI, Flutter) without touching Session.
type Session struct {
	transport   transport.Transport
	burnMgr     *burn.Manager
	onReceive   func(DisplayMsg) // called on each incoming message
	onError     func(error)
	onMu        sync.RWMutex

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

// NewSession creates a Session with the given transport.
func NewSession(t transport.Transport) *Session {
	return &Session{
		transport: t,
		burnMgr:   burn.New(),
	}
}

// SetOnReceive sets the incoming message callback (must be set before Start).
func (s *Session) SetOnReceive(fn func(DisplayMsg)) {
	s.onMu.Lock()
	s.onReceive = fn
	s.onMu.Unlock()
}

// SetOnError sets the error callback.
func (s *Session) SetOnError(fn func(error)) {
	s.onMu.Lock()
	s.onError = fn
	s.onMu.Unlock()
}

// Start connects the transport and begins receiving messages.
func (s *Session) Start(ctx context.Context) error {
	s.ctx, s.cancel = context.WithCancel(ctx)
	if err := s.transport.Start(s.ctx); err != nil {
		return fmt.Errorf("transport start: %w", err)
	}
	s.wg.Add(2)
	go s.recvLoop()
	go s.errLoop()
	return nil
}

// Stop disconnects and cleans up.
func (s *Session) Stop() error {
	if s.cancel != nil {
		s.cancel()
	}
	s.wg.Wait()
	return s.transport.Stop()
}

// SwitchTransport hot-swaps the transport (e.g., server → P2P).
// The old transport is stopped first.
func (s *Session) SwitchTransport(t transport.Transport) error {
	s.transport.Stop()
	s.transport = t
	return s.transport.Start(s.ctx)
}

// Send broadcasts a chat message.
func (s *Session) Send(content string, burnSeconds int32) {
	msgID := uuid.New().String()
	if burnSeconds > 0 {
		s.burnMgr.Add(msgID, burnSeconds)
	}
	wsm := &proto.WsMessage{
		MessageId: msgID,
		Timestamp: time.Now().Unix(),
		Type:      proto.MsgType_CHAT,
		Payload: &proto.WsMessage_Chat{
			Chat: &proto.ChatMessage{Content: content, BurnSeconds: burnSeconds},
		},
	}
	s.transport.Broadcast(&transport.Message{Msg: wsm})
}

// PrivateSend sends a private message.
func (s *Session) PrivateSend(target, content string, burnSeconds int32) {
	msgID := uuid.New().String()
	if burnSeconds > 0 {
		s.burnMgr.Add(msgID, burnSeconds)
	}
	wsm := &proto.WsMessage{
		MessageId: msgID,
		Type:      proto.MsgType_PRIVATE_CHAT,
		Payload: &proto.WsMessage_Chat{
			Chat: &proto.ChatMessage{To: target, Content: content, BurnSeconds: burnSeconds},
		},
	}
	s.transport.PrivateSend(target, &transport.Message{Msg: wsm})
}

// Rename changes the local display name.
func (s *Session) Rename(name string) error {
	return s.transport.Rename(name)
}

// Who requests the online user list.
func (s *Session) Who() error {
	_, err := s.transport.Who()
	return err
}

// --- Internal ---

func (s *Session) recvLoop() {
	defer s.wg.Done()
	for {
		select {
		case msg, ok := <-s.transport.Recv():
			if !ok {
				return
			}
			s.dispatch(msg.Msg)
		case <-s.ctx.Done():
			return
		}
	}
}

func (s *Session) errLoop() {
	defer s.wg.Done()
	for {
		select {
		case err, ok := <-s.transport.SendErr():
			if !ok {
				return
			}
			s.onMu.RLock()
			fn := s.onError
			s.onMu.RUnlock()
			if fn != nil {
				fn(err)
			}
		case <-s.ctx.Done():
			return
		}
	}
}

func (s *Session) dispatch(msg *proto.WsMessage) {
	s.onMu.RLock()
	fn := s.onReceive
	s.onMu.RUnlock()
	if fn == nil {
		return
	}
	switch msg.Type {
	case proto.MsgType_CHAT:
		chat := msg.GetChat()
		burnHint := ""
		if chat.BurnSeconds > 0 {
			burnHint = fmt.Sprintf(" [burns in %ds]", chat.BurnSeconds)
		}
		s.emit(DisplayMsg{
			Type: "chat", From: chat.From, Content: chat.Content + burnHint,
			BurnSeconds: chat.BurnSeconds, MessageID: msg.MessageId,
		})
	case proto.MsgType_PRIVATE_CHAT:
		chat := msg.GetChat()
		burnHint := ""
		if chat.BurnSeconds > 0 {
			burnHint = fmt.Sprintf(" [burns in %ds]", chat.BurnSeconds)
		}
		s.emit(DisplayMsg{
			Type: "private", From: chat.From, Content: chat.Content + burnHint,
			BurnSeconds: chat.BurnSeconds, MessageID: msg.MessageId,
		})
	case proto.MsgType_SYSTEM:
		sys := msg.GetSystem()
		if strings.HasPrefix(sys.Content, "[") {
			s.emit(DisplayMsg{Type: "who", Content: sys.Content})
		} else {
			s.emit(DisplayMsg{Type: "system", Content: s.formatSystem(sys)})
		}
	case proto.MsgType_BURN_RECEIPT:
		s.emit(DisplayMsg{Type: "burn", Content: "message burned", MessageID: msg.GetReceipt().MessageId})
	case proto.MsgType_WHO:
		_ = msg.GetSystem().Content // raw JSON, passed through
	}
}

func (s *Session) formatSystem(sys *proto.SystemNotice) string {
	switch sys.Type {
	case proto.SystemType_SYS_JOIN:
		return sys.Content + " joined"
	case proto.SystemType_SYS_LEAVE:
		return sys.Content + " left"
	case proto.SystemType_SYS_RENAME:
		return sys.Content
	case proto.SystemType_SYS_ERROR:
		return "error: " + sys.Content
	default:
		return sys.Content
	}
}

func (s *Session) emit(d DisplayMsg) {
	s.onMu.RLock()
	fn := s.onReceive
	s.onMu.RUnlock()
	if fn != nil {
		go fn(d)
	}
}

// Ensure json import isn't optimized away (used by DisplayMsg json tags).
var _ = json.Marshal

// Ensure proto / googleproto are importable from this package for consumers.
var _ = googleproto.Marshal
