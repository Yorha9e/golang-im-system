package engine

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/url"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"golang-im-system/proto"
	googleproto "google.golang.org/protobuf/proto"
)

const testAddr = ":19888"

func getToken(t *testing.T, username string) string {
	t.Helper()
	resp, err := http.Get("http://127.0.0.1" + testAddr + "/login?user=" + username)
	if err != nil {
		t.Fatalf("login: %v", err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	var result struct{ Token string }
	json.Unmarshal(body, &result)
	return result.Token
}

func dial(t *testing.T) *websocket.Conn {
	t.Helper()
	token := getToken(t, "testuser")
	u := url.URL{Scheme: "ws", Host: testAddr, Path: "/ws", RawQuery: "token=" + token}
	conn, _, err := websocket.DefaultDialer.Dial(u.String(), nil)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	return conn
}

func recv(t *testing.T, conn *websocket.Conn) *proto.WsMessage {
	t.Helper()
	_, raw, err := conn.ReadMessage()
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	msg := &proto.WsMessage{}
	if err := googleproto.Unmarshal(raw, msg); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	return msg
}

func send(t *testing.T, conn *websocket.Conn, msg *proto.WsMessage) {
	t.Helper()
	data, err := googleproto.Marshal(msg)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if err := conn.WriteMessage(websocket.BinaryMessage, data); err != nil {
		t.Fatalf("write: %v", err)
	}
}

func startServer(t *testing.T) *ChatServer {
	t.Helper()
	s, err := New(Config{Addr: testAddr, DBPath: t.TempDir() + "/im.db"})
	if err != nil {
		t.Fatalf("create server: %v", err)
	}
	go s.Start()
	time.Sleep(100 * time.Millisecond)
	t.Cleanup(func() { s.Stop(context.Background()) })
	return s
}

func TestChatBroadcast(t *testing.T) {
	startServer(t)

	alice := dial(t)
	bob := dial(t)
	time.Sleep(50 * time.Millisecond)

	send(t, alice, &proto.WsMessage{
		Type: proto.MsgType_CHAT,
		Payload: &proto.WsMessage_Chat{
			Chat: &proto.ChatMessage{Content: "hello everyone"},
		},
	})

	// Alice and Bob should both get chat + join notifications
	// Skip the JOIN system messages
	for {
		m := recv(t, bob)
		if m.Type == proto.MsgType_CHAT {
			if got := m.GetChat().Content; got != "hello everyone" {
				t.Fatalf("expected 'hello everyone', got %q", got)
			}
			break
		}
	}

	for {
		m := recv(t, alice)
		if m.Type == proto.MsgType_CHAT {
			if got := m.GetChat().Content; got != "hello everyone" {
				t.Fatalf("expected 'hello everyone', got %q", got)
			}
			break
		}
	}
}

func TestWho(t *testing.T) {
	startServer(t)

	conn := dial(t)
	time.Sleep(50 * time.Millisecond)

	// Consume the JOIN message
	recv(t, conn)

	send(t, conn, &proto.WsMessage{Type: proto.MsgType_WHO})

	msg := recv(t, conn)
	if msg.Type != proto.MsgType_SYSTEM {
		t.Fatalf("expected SYSTEM, got %v", msg.Type)
	}
	content := msg.GetSystem().Content
	if content == "" {
		t.Fatal("expected non-empty who response")
	}
}

func TestRename(t *testing.T) {
	startServer(t)

	conn := dial(t)
	time.Sleep(50 * time.Millisecond)

	// Consume JOIN
	recv(t, conn)

	send(t, conn, &proto.WsMessage{
		Type: proto.MsgType_RENAME,
		Payload: &proto.WsMessage_System{
			System: &proto.SystemNotice{Content: "Neo"},
		},
	})

	// Should get a rename broadcast
	for {
		msg := recv(t, conn)
		if msg.Type == proto.MsgType_SYSTEM && msg.GetSystem().Type == proto.SystemType_SYS_RENAME {
			content := msg.GetSystem().Content
			if content == "" {
				t.Fatal("expected non-empty rename content")
			}
			t.Logf("rename: %s", content)
			break
		}
	}
}

func TestRenameDuplicate(t *testing.T) {
	startServer(t)

	alice := dial(t)
	bob := dial(t)
	time.Sleep(50 * time.Millisecond)

	// Skip join messages
	recv(t, alice)
	recv(t, bob)

	// Alice renames to "Neo"
	send(t, alice, &proto.WsMessage{
		Type: proto.MsgType_RENAME,
		Payload: &proto.WsMessage_System{
			System: &proto.SystemNotice{Content: "Neo"},
		},
	})

	// Wait for rename broadcast on both connections
	for {
		m := recv(t, alice)
		if m.Type == proto.MsgType_SYSTEM && m.GetSystem().Type == proto.SystemType_SYS_RENAME {
			break
		}
	}
	for {
		m := recv(t, bob)
		if m.Type == proto.MsgType_SYSTEM && m.GetSystem().Type == proto.SystemType_SYS_RENAME {
			break
		}
	}

	// Bob tries to rename to "Neo" too (should fail)
	send(t, bob, &proto.WsMessage{
		Type: proto.MsgType_RENAME,
		Payload: &proto.WsMessage_System{
			System: &proto.SystemNotice{Content: "Neo"},
		},
	})

	msg := recv(t, bob)
	if msg.Type != proto.MsgType_SYSTEM {
		t.Fatalf("expected SYSTEM error, got %v", msg.Type)
	}
	if got := msg.GetSystem().Content; got != "name already taken" {
		t.Fatalf("expected 'name already taken', got %q", got)
	}
}

func TestPrivateChat(t *testing.T) {
	startServer(t)

	alice := dial(t)
	bob := dial(t)
	time.Sleep(50 * time.Millisecond)

	// Skip join messages
	recv(t, alice)
	recv(t, bob)

	// Alice renames to "Alice" so we have a known name
	send(t, alice, &proto.WsMessage{
		Type: proto.MsgType_RENAME,
		Payload: &proto.WsMessage_System{
			System: &proto.SystemNotice{Content: "Alice"},
		},
	})
	// Consume rename broadcast
	for {
		m := recv(t, alice)
		if m.Type == proto.MsgType_SYSTEM && m.GetSystem().Type == proto.SystemType_SYS_RENAME {
			break
		}
	}
	// Bob receives rename too
	for {
		m := recv(t, bob)
		if m.Type == proto.MsgType_SYSTEM && m.GetSystem().Type == proto.SystemType_SYS_RENAME {
			break
		}
	}

	// Bob sends private message to Alice
	send(t, bob, &proto.WsMessage{
		Type: proto.MsgType_PRIVATE_CHAT,
		Payload: &proto.WsMessage_Chat{
			Chat: &proto.ChatMessage{To: "Alice", Content: "secret"},
		},
	})

	msg := recv(t, alice)
	if msg.Type != proto.MsgType_PRIVATE_CHAT {
		t.Fatalf("expected PRIVATE_CHAT, got %v", msg.Type)
	}
	if got := msg.GetChat().Content; got != "secret" {
		t.Fatalf("expected 'secret', got %q", got)
	}
	from := msg.GetChat().From
	if from == "" {
		t.Fatal("expected non-empty sender name")
	}
	t.Logf("private chat from %s: %s", from, msg.GetChat().Content)
}

func TestHeartbeat(t *testing.T) {
	startServer(t)

	conn := dial(t)
	time.Sleep(50 * time.Millisecond)

	// Consume JOIN
	recv(t, conn)

	send(t, conn, &proto.WsMessage{
		Type: proto.MsgType_HEARTBEAT,
		Payload: &proto.WsMessage_Hb{
			Hb: &proto.Heartbeat{PingTs: time.Now().Unix()},
		},
	})

	msg := recv(t, conn)
	if msg.Type != proto.MsgType_HEARTBEAT {
		t.Fatalf("expected HEARTBEAT, got %v", msg.Type)
	}
	t.Logf("heartbeat pong: %d", msg.GetHb().PingTs)
}

func TestBurnAfterReading(t *testing.T) {
	startServer(t)

	alice := dial(t)
	bob := dial(t)
	time.Sleep(50 * time.Millisecond)

	// Skip join messages
	recv(t, alice)
	recv(t, bob)

	// Alice sends a burn-after-reading message (2 seconds)
	send(t, alice, &proto.WsMessage{
		Type: proto.MsgType_CHAT,
		Payload: &proto.WsMessage_Chat{
			Chat: &proto.ChatMessage{Content: "this will self-destruct", BurnSeconds: 2},
		},
	})

	// Both receive the message
	var msgID string
	for _, conn := range []*websocket.Conn{alice, bob} {
		for {
			m := recv(t, conn)
			if m.Type == proto.MsgType_CHAT {
				chat := m.GetChat()
				if chat.BurnSeconds == 2 {
					msgID = m.MessageId
					t.Logf("received burn message: %s (id=%s, burn=%ds)", chat.Content, msgID[:8], chat.BurnSeconds)
					break
				}
			}
		}
	}

	// Wait for the burn receipt
	for _, conn := range []*websocket.Conn{alice, bob} {
		for {
			m := recv(t, conn)
			if m.Type == proto.MsgType_BURN_RECEIPT {
				receipt := m.GetReceipt()
				if receipt.MessageId == msgID {
					t.Logf("burn receipt received: %s at %d", receipt.MessageId[:8], receipt.BurnedAt)
					break
				}
			}
		}
	}
}
