package transport

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"testing"
	"time"

	"github.com/Yorha9e/golang-im-system/internal/engine"
	"github.com/Yorha9e/golang-im-system/proto"
	"github.com/Yorha9e/golang-im-system/signaling"
)

func loginForTest(t *testing.T, port, username string) string {
	t.Helper()
	resp, err := http.Get("http://127.0.0.1:" + port + "/login?user=" + username)
	if err != nil {
		t.Fatalf("login: %v", err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	var result struct{ Token string }
	json.Unmarshal(body, &result)
	if result.Token == "" {
		t.Fatalf("no token")
	}
	return result.Token
}

func TestWANP2PSignalingAndFallback(t *testing.T) {
	// Start SignalingServer.
	sig := signaling.NewSignalingServer(":17010")
	go sig.Start()
	time.Sleep(100 * time.Millisecond)

	// Start ChatServer for fallback.
	srv, err := engine.New(engine.Config{
		Addr:   ":17011",
		DBPath: ":memory:",
	})
	if err != nil {
		t.Fatalf("server: %v", err)
	}
	go srv.Start()
	time.Sleep(100 * time.Millisecond)
	defer srv.Stop(context.Background())

	// Login to get tokens for fallback.
	tokenA := loginForTest(t, "17011", "alice")
	tokenB := loginForTest(t, "17011", "bob")

	alice := NewWANP2PTransport("alice", "127.0.0.1:17010", "127.0.0.1:17011", tokenA)
	bob := NewWANP2PTransport("bob", "127.0.0.1:17010", "127.0.0.1:17011", tokenB)

	aliceFellBack := false
	bobFellBack := false
	alice.OnFallback = func() { aliceFellBack = true }
	bob.OnFallback = func() { bobFellBack = true }

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := alice.Start(ctx); err != nil {
		t.Fatalf("alice start: %v", err)
	}
	defer alice.Stop()

	if err := bob.Start(ctx); err != nil {
		t.Fatalf("bob start: %v", err)
	}
	defer bob.Stop()

	// Wait for ICE or fallback (fallback timeout is 15s, give it 20s).
	time.Sleep(20 * time.Second)

	t.Logf("Alice fallback: %v, Bob fallback: %v", aliceFellBack, bobFellBack)

	// Regardless of ICE result, message delivery should work.
	// If ICE works: direct P2P. If not: fallback through ChatServer.
	msg := &proto.WsMessage{
		Type: proto.MsgType_CHAT,
		Payload: &proto.WsMessage_Chat{
			Chat: &proto.ChatMessage{Content: "hello via WAN P2P (or fallback)"},
		},
	}
	if err := alice.Broadcast(&Message{Msg: msg}); err != nil {
		t.Fatalf("broadcast: %v", err)
	}

	// Bob should receive (skip system/history messages).
	var received *Message
loop:
	for {
		select {
		case r := <-bob.Recv():
			if r.Msg.Type == proto.MsgType_CHAT {
				received = r
				break loop
			}
			t.Logf("Bob: skipping type=%v", r.Msg.Type)
		case <-time.After(10 * time.Second):
			t.Fatal("timeout waiting for message (ICE or fallback)")
		}
	}
	chat := received.Msg.GetChat()
	t.Logf("Bob received: [%s] %s", received.From, chat.Content)
	if chat.Content == "" {
		t.Fatal("empty content")
	}

	// Verify transport state.
	if aliceFellBack != alice.IsFallback() {
		t.Error("alice fallback state mismatch")
	}
}

func TestWANP2PWhoAndStop(t *testing.T) {
	sig := signaling.NewSignalingServer(":17012")
	go sig.Start()
	time.Sleep(100 * time.Millisecond)

	srv, err := engine.New(engine.Config{
		Addr:   ":17013",
		DBPath: ":memory:",
	})
	if err != nil {
		t.Fatalf("server: %v", err)
	}
	go srv.Start()
	time.Sleep(100 * time.Millisecond)
	defer srv.Stop(context.Background())

	token := loginForTest(t, "17013", "testuser")

	wan := NewWANP2PTransport("testuser", "127.0.0.1:17012", "127.0.0.1:17013", token)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := wan.Start(ctx); err != nil {
		t.Fatalf("start: %v", err)
	}

	// Stop should be clean - no hang or panic.
	time.Sleep(2 * time.Second)
	wan.Stop()
	t.Log("WAN P2P stop completed without panic")
}

func TestWANP2PNilFallback(t *testing.T) {
	// WAN P2P with no signaling server: should not crash on Start.
	srv, err := engine.New(engine.Config{
		Addr:   ":17014",
		DBPath: ":memory:",
	})
	if err != nil {
		t.Fatalf("server: %v", err)
	}
	go srv.Start()
	time.Sleep(100 * time.Millisecond)
	defer srv.Stop(context.Background())

	token := loginForTest(t, "17014", "loner")
	wan := NewWANP2PTransport("loner", "127.0.0.1:17099", "127.0.0.1:17014", token)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Start should fail (bad signaling addr) or connect but have no peers.
	_ = wan.Start(ctx)
	// Either way, Stop must not panic.
	time.Sleep(3 * time.Second)
	wan.Stop()
	t.Log("WAN P2P with dead signaling handled gracefully")
}
