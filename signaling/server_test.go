package signaling

import (
	"encoding/json"
	"net/url"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

func TestSignalingServerPeerJoin(t *testing.T) {
	s := NewSignalingServer(":17001")
	go s.Start()
	time.Sleep(100 * time.Millisecond)

	// Alice connects.
	alice, err := dialSignal(":17001", "alice")
	if err != nil {
		t.Fatalf("alice connect: %v", err)
	}

	// Bob connects.
	bob, err := dialSignal(":17001", "bob")
	if err != nil {
		t.Fatalf("bob connect: %v", err)
	}

	// Alice should receive peer_join for Bob.
	msg := readSignal(t, alice)
	if msg.Type != "peer_join" {
		t.Fatalf("expected peer_join, got %q", msg.Type)
	}
	var info PeerInfo
	json.Unmarshal(msg.Payload, &info)
	if info.Name != "bob" {
		t.Fatalf("expected bob, got %q", info.Name)
	}
	t.Logf("Alice sees Bob join: id=%s name=%s", info.ID, info.Name)

	// Bob should also receive peer_join for Alice (from initial notifyJoin).
	msg = readSignal(t, bob)
	if msg.Type != "peer_join" {
		t.Fatalf("expected peer_join, got %q", msg.Type)
	}
	var info2 PeerInfo
	json.Unmarshal(msg.Payload, &info2)
	t.Logf("Bob sees Alice join: id=%s name=%s", info2.ID, info2.Name)

	alice.Close()
	bob.Close()
}

func TestSignalingServerMessageRouting(t *testing.T) {
	s := NewSignalingServer(":17002")
	go s.Start()
	time.Sleep(100 * time.Millisecond)

	alice, err := dialSignal(":17002", "alice")
	if err != nil {
		t.Fatalf("alice connect: %v", err)
	}
	bob, err := dialSignal(":17002", "bob")
	if err != nil {
		t.Fatalf("bob connect: %v", err)
	}

	// Consume join notifications: alice sees bob join.
	bobJoinMsg := readSignal(t, alice)
	var bobInfo PeerInfo
	json.Unmarshal(bobJoinMsg.Payload, &bobInfo)
	t.Logf("Alice sees Bob: id=%s", bobInfo.ID)

	// Bob sees alice join.
	readSignal(t, bob)

	// Alice sends an offer to Bob.
	payload := json.RawMessage(`{"sdp":"test-offer"}`)
	alice.WriteJSON(SigMessage{
		Type:    "offer",
		To:      bobInfo.ID,
		Payload: payload,
	})

	// Bob should receive it.
	msg := readSignal(t, bob)
	if msg.Type != "offer" {
		t.Fatalf("expected offer, got %q", msg.Type)
	}
	t.Logf("Bob received offer from Alice: %s", string(msg.Payload))

	alice.Close()
	bob.Close()
}

func TestSignalingServerPeerLeave(t *testing.T) {
	s := NewSignalingServer(":17003")
	go s.Start()
	time.Sleep(100 * time.Millisecond)

	alice, err := dialSignal(":17003", "alice")
	if err != nil {
		t.Fatalf("alice connect: %v", err)
	}
	bob, err := dialSignal(":17003", "bob")
	if err != nil {
		t.Fatalf("bob connect: %v", err)
	}

	// Consume join notifications.
	readSignal(t, alice) // alice sees bob join
	readSignal(t, bob)  // bob sees alice join

	// Bob disconnects.
	bob.Close()

	// Alice should receive peer_leave for Bob.
	msg := readSignal(t, alice)
	if msg.Type != "peer_leave" {
		t.Fatalf("expected peer_leave, got %q", msg.Type)
	}
	var info PeerInfo
	json.Unmarshal(msg.Payload, &info)
	t.Logf("Alice sees Bob leave: id=%s name=%s", info.ID, info.Name)

	alice.Close()
}

// --- helpers ---

func dialSignal(addr, name string) (*websocket.Conn, error) {
	u := url.URL{Scheme: "ws", Host: addr, Path: "/signal", RawQuery: "name=" + name}
	conn, _, err := websocket.DefaultDialer.Dial(u.String(), nil)
	return conn, err
}

func readSignal(t *testing.T, conn *websocket.Conn) SigMessage {
	t.Helper()
	conn.SetReadDeadline(time.Now().Add(3 * time.Second))
	_, raw, err := conn.ReadMessage()
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	var msg SigMessage
	json.Unmarshal(raw, &msg)
	return msg
}

func getPeerID(t *testing.T, conn *websocket.Conn) string {
	t.Helper()
	return "" // We don't know the ID, but To is optional in our test
}
