package service

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"sync"
	"testing"
	"time"

	"golang-im-system/internal/engine"
	"golang-im-system/transport"
)

func startTestServer(t *testing.T) *engine.ChatServer {
	t.Helper()
	s, err := engine.New(engine.Config{Addr: ":19889", DBPath: ":memory:"})
	if err != nil {
		t.Fatalf("create server: %v", err)
	}
	go s.Start()
	time.Sleep(100 * time.Millisecond)
	t.Cleanup(func() { s.Stop(context.Background()) })
	return s
}

func login(t *testing.T, username string) string {
	t.Helper()
	resp, err := http.Get("http://127.0.0.1:19889/login?user=" + username)
	if err != nil {
		t.Fatalf("login: %v", err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	var result struct{ Token string }
	json.Unmarshal(body, &result)
	return result.Token
}

func newSession(t *testing.T, username string) *Session {
	t.Helper()
	token := login(t, username)
	s := NewSession(transport.NewServerTransport("127.0.0.1:19889", token))
	if err := s.Start(context.Background()); err != nil {
		t.Fatalf("start session: %v", err)
	}
	return s
}

func collect(t *testing.T, s *Session) []DisplayMsg {
	t.Helper()
	var mu sync.Mutex
	var msgs []DisplayMsg
	done := make(chan struct{})

	s.SetOnReceive(func(d DisplayMsg) {
		mu.Lock()
		msgs = append(msgs, d)
		mu.Unlock()

		if len(msgs) >= 2 {
			select {
			case done <- struct{}{}:
			default:
			}
		}
	})

	select {
	case <-done:
	case <-time.After(3 * time.Second):
	}

	mu.Lock()
	defer mu.Unlock()
	return msgs
}

func TestSessionChatRoundtrip(t *testing.T) {
	startTestServer(t)

	alice := newSession(t, "alice")
	bob := newSession(t, "bob")
	defer alice.Stop()
	defer bob.Stop()

	time.Sleep(200 * time.Millisecond)

	var mu sync.Mutex
	var bobMsgs []DisplayMsg
	bob.SetOnReceive(func(d DisplayMsg) {
		mu.Lock()
		bobMsgs = append(bobMsgs, d)
		mu.Unlock()
	})

	alice.Send("hello from alice", 0)
	time.Sleep(500 * time.Millisecond)

	mu.Lock()
	found := false
	for _, m := range bobMsgs {
		if m.Type == "chat" && m.From != "" && m.Content != "" {
			t.Logf("BOB received: [%s] %s", m.From, m.Content)
			found = true
		}
	}
	mu.Unlock()

	if !found {
		t.Fatal("bob did not receive alice's message")
	}
}

func TestSessionPrivateChat(t *testing.T) {
	startTestServer(t)

	alice := newSession(t, "alice")
	bob := newSession(t, "bob")
	defer alice.Stop()
	defer bob.Stop()

	time.Sleep(200 * time.Millisecond)

	alice.Rename("Alice")
	time.Sleep(200 * time.Millisecond)

	bob.Rename("Bob")
	time.Sleep(200 * time.Millisecond)

	var mu sync.Mutex
	var aliceMsgs []DisplayMsg
	alice.SetOnReceive(func(d DisplayMsg) {
		mu.Lock()
		aliceMsgs = append(aliceMsgs, d)
		mu.Unlock()
	})

	bob.PrivateSend("Alice", "secret message for alice", 0)
	time.Sleep(500 * time.Millisecond)

	mu.Lock()
	found := false
	for _, m := range aliceMsgs {
		if m.Type == "private" {
			t.Logf("ALICE received PM: [%s] %s", m.From, m.Content)
			found = true
		}
	}
	mu.Unlock()

	if !found {
		t.Fatal("alice did not receive bob's private message")
	}
}

func TestSessionBurnAfterReading(t *testing.T) {
	startTestServer(t)

	alice := newSession(t, "alice")
	bob := newSession(t, "bob")
	defer alice.Stop()
	defer bob.Stop()

	time.Sleep(200 * time.Millisecond)

	var mu sync.Mutex
	var bobMsgs []DisplayMsg
	bob.SetOnReceive(func(d DisplayMsg) {
		mu.Lock()
		bobMsgs = append(bobMsgs, d)
		mu.Unlock()
	})

	alice.Send("this will burn", 2)

	time.Sleep(3500 * time.Millisecond)

	mu.Lock()
	chatReceived := false
	burnReceived := false
	for _, m := range bobMsgs {
		t.Logf("BOB: type=%s content=%s msgID=%s", m.Type, m.Content, m.MessageID)
		if m.Type == "chat" && m.Content != "" {
			chatReceived = true
		}
		if m.Type == "burn" {
			burnReceived = true
		}
	}
	mu.Unlock()

	if !chatReceived {
		t.Fatal("bob did not receive burn message")
	}
	if !burnReceived {
		t.Fatal("bob did not receive burn receipt")
	}
}

func TestSessionWho(t *testing.T) {
	startTestServer(t)

	alice := newSession(t, "alice")
	bob := newSession(t, "bob")
	defer alice.Stop()
	defer bob.Stop()

	time.Sleep(200 * time.Millisecond)

	var mu sync.Mutex
	var aliceMsgs []DisplayMsg
	alice.SetOnReceive(func(d DisplayMsg) {
		mu.Lock()
		aliceMsgs = append(aliceMsgs, d)
		mu.Unlock()
	})

	alice.Who()
	time.Sleep(300 * time.Millisecond)

	mu.Lock()
	t.Logf("ALICE received %d messages", len(aliceMsgs))
	foundSystem := false
	for _, m := range aliceMsgs {
		t.Logf("  type=%s content=%s", m.Type, m.Content)
		if m.Type == "system" || m.Type == "who" {
			foundSystem = true
		}
	}
	mu.Unlock()

	if !foundSystem {
		t.Fatal("alice did not receive who response")
	}
}
