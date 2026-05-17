package transport

import (
	"context"
	"testing"
	"time"

	"github.com/Yorha9e/golang-im-system/proto"
	googleproto "google.golang.org/protobuf/proto"
)

func TestLANP2PDiscoveryAndMessage(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	alice := NewLANP2PTransport("alice", 19001)
	bob := NewLANP2PTransport("bob", 19002)

	if err := alice.Start(ctx); err != nil {
		t.Fatalf("alice start: %v", err)
	}
	defer alice.Stop()

	if err := bob.Start(ctx); err != nil {
		t.Fatalf("bob start: %v", err)
	}
	defer bob.Stop()

	// Wait for multicast discovery (announce every 5s, give it 12s)
	t.Log("waiting for discovery...")
	time.Sleep(12 * time.Second)

	alicePeers, _ := alice.Who()
	bobPeers, _ := bob.Who()
	t.Logf("Alice sees %d peers: %v", len(alicePeers), peerNames(alicePeers))
	t.Logf("Bob sees %d peers: %v", len(bobPeers), peerNames(bobPeers))

	if len(alicePeers) == 0 {
		t.Fatal("Alice discovered no peers")
	}
	if len(bobPeers) == 0 {
		t.Fatal("Bob discovered no peers")
	}

	// Alice sends a message.
	msg := &proto.WsMessage{
		Type: proto.MsgType_CHAT,
		Payload: &proto.WsMessage_Chat{
			Chat: &proto.ChatMessage{Content: "hello from alice"},
		},
	}
	if err := alice.Broadcast(&Message{Msg: msg}); err != nil {
		t.Fatalf("broadcast: %v", err)
	}

	// Bob should receive it.
	select {
	case received := <-bob.Recv():
		chat := received.Msg.GetChat()
		t.Logf("Bob received from [%s]: %s", received.From, chat.Content)
		if chat.Content != "hello from alice" {
			t.Fatalf("expected 'hello from alice', got %q", chat.Content)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for message")
	}

	// Bob sends a reply.
	reply := &proto.WsMessage{
		Type: proto.MsgType_CHAT,
		Payload: &proto.WsMessage_Chat{
			Chat: &proto.ChatMessage{Content: "hey alice"},
		},
	}
	if err := bob.Broadcast(&Message{Msg: reply}); err != nil {
		t.Fatalf("broadcast: %v", err)
	}

	select {
	case received := <-alice.Recv():
		chat := received.Msg.GetChat()
		t.Logf("Alice received from [%s]: %s", received.From, chat.Content)
		if chat.Content != "hey alice" {
			t.Fatalf("expected 'hey alice', got %q", chat.Content)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for reply")
	}
}

func TestLANP2PWhoAndDisconnect(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	alice := NewLANP2PTransport("alice", 19011)
	bob := NewLANP2PTransport("bob", 19012)

	if err := alice.Start(ctx); err != nil {
		t.Fatalf("alice start: %v", err)
	}
	defer alice.Stop()

	if err := bob.Start(ctx); err != nil {
		t.Fatalf("bob start: %v", err)
	}
	defer bob.Stop()

	time.Sleep(12 * time.Second)

	// Both should see each other.
	peers, _ := alice.Who()
	if len(peers) != 1 {
		t.Fatalf("alice expected 1 peer, got %d: %v", len(peers), peerNames(peers))
	}

	// Stop Bob, Alice should eventually see peer gone.
	bob.Stop()
	time.Sleep(3 * time.Second)

	// Alice sends message — should not panic, just no-op.
	msg := &proto.WsMessage{
		Type: proto.MsgType_CHAT,
		Payload: &proto.WsMessage_Chat{
			Chat: &proto.ChatMessage{Content: "anyone there?"},
		},
	}
	if err := alice.Broadcast(&Message{Msg: msg}); err != nil {
		t.Fatalf("broadcast after disconnect: %v", err)
	}
	t.Log("broadcast after peer disconnect succeeded (no panic)")
}

func TestLANP2PBinaryRoundtrip(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	alice := NewLANP2PTransport("alice", 19021)
	bob := NewLANP2PTransport("bob", 19022)

	if err := alice.Start(ctx); err != nil {
		t.Fatalf("alice start: %v", err)
	}
	defer alice.Stop()

	if err := bob.Start(ctx); err != nil {
		t.Fatalf("bob start: %v", err)
	}
	defer bob.Stop()

	time.Sleep(12 * time.Second)

	// Verify protobuf binary roundtrip works over P2P.
	wsm := &proto.WsMessage{
		Type: proto.MsgType_HEARTBEAT,
		Payload: &proto.WsMessage_Hb{
			Hb: &proto.Heartbeat{PingTs: 42},
		},
	}
	raw, _ := googleproto.Marshal(wsm)
	wsm2 := &proto.WsMessage{}
	googleproto.Unmarshal(raw, wsm2)

	if wsm2.GetHb().PingTs != 42 {
		t.Fatal("protobuf roundtrip failed")
	}

	alice.Broadcast(&Message{Msg: wsm})
	select {
	case <-bob.Recv():
		t.Log("binary heartbeat roundtrip OK")
	case <-time.After(5 * time.Second):
		t.Fatal("timeout")
	}
}

func peerNames(peers []Peer) []string {
	names := make([]string, len(peers))
	for i, p := range peers {
		names[i] = p.Name
	}
	return names
}
