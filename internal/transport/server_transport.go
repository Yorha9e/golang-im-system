package transport

import (
	"context"
	"fmt"
	"net/url"

	"github.com/gorilla/websocket"
	"golang-im-system/internal/proto"
	googleproto "google.golang.org/protobuf/proto"
)

// ServerTransport connects to a ChatServer via WebSocket.
type ServerTransport struct {
	serverAddr string
	conn       *websocket.Conn
	recv       chan *Message
	errCh      chan error
	ctx        context.Context
	cancel     context.CancelFunc
}

// NewServerTransport creates a client transport targeting a ChatServer.
func NewServerTransport(serverAddr string) *ServerTransport {
	return &ServerTransport{
		serverAddr: serverAddr,
		recv:       make(chan *Message, 256),
		errCh:      make(chan error, 16),
	}
}

func (t *ServerTransport) Start(ctx context.Context) error {
	t.ctx, t.cancel = context.WithCancel(ctx)
	u := url.URL{Scheme: "ws", Host: t.serverAddr, Path: "/ws"}
	conn, _, err := websocket.DefaultDialer.Dial(u.String(), nil)
	if err != nil {
		return fmt.Errorf("dial server: %w", err)
	}
	t.conn = conn
	go t.readPump()
	return nil
}

func (t *ServerTransport) readPump() {
	defer close(t.recv)
	for {
		_, raw, err := t.conn.ReadMessage()
		if err != nil {
			select {
			case t.errCh <- err:
			case <-t.ctx.Done():
			}
			return
		}
		msg := &proto.WsMessage{}
		if err := googleproto.Unmarshal(raw, msg); err != nil {
			continue
		}
		select {
		case t.recv <- &Message{Msg: msg}:
		case <-t.ctx.Done():
			return
		}
	}
}

func (t *ServerTransport) send(msg *proto.WsMessage) error {
	data, _ := googleproto.Marshal(msg)
	return t.conn.WriteMessage(websocket.BinaryMessage, data)
}

// Broadcast sends a message to all connected peers (via server).
func (t *ServerTransport) Broadcast(msg *Message) error {
	return t.send(msg.Msg)
}

// PrivateSend sends a private message via server relay.
func (t *ServerTransport) PrivateSend(target string, msg *Message) error {
	msg.Msg.Type = proto.MsgType_PRIVATE_CHAT
	if chat := msg.Msg.GetChat(); chat != nil {
		chat.To = target
	}
	return t.send(msg.Msg)
}

// Who requests the online user list from the server.
func (t *ServerTransport) Who() ([]Peer, error) {
	msg := &proto.WsMessage{Type: proto.MsgType_WHO}
	return nil, t.send(msg)
}

// Rename changes the local display name via server.
func (t *ServerTransport) Rename(name string) error {
	msg := &proto.WsMessage{
		Type: proto.MsgType_RENAME,
		Payload: &proto.WsMessage_System{
			System: &proto.SystemNotice{Content: name},
		},
	}
	return t.send(msg)
}

// Recv returns the incoming message channel.
func (t *ServerTransport) Recv() <-chan *Message { return t.recv }

// SendErr returns the transport error channel.
func (t *ServerTransport) SendErr() <-chan error { return t.errCh }

// Stop closes the connection.
func (t *ServerTransport) Stop() error {
	if t.cancel != nil {
		t.cancel()
	}
	if t.conn != nil {
		return t.conn.Close()
	}
	return nil
}
