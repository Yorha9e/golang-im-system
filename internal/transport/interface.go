package transport

import (
	"context"

	"golang-im-system/internal/proto"
)

// Peer represents a connected peer/node in the network.
type Peer struct {
	ID   string
	Name string
	Addr string
}

// Message wraps a protobuf WsMessage with sender metadata.
type Message struct {
	Msg  *proto.WsMessage
	From string
}

// Transport is the unified interface for all communication modes.
// Implementations: ServerTransport, LANP2PTransport, WANP2PTransport.
type Transport interface {
	// Start initializes the transport (connect, listen, discover).
	Start(ctx context.Context) error

	// Stop gracefully shuts down the transport.
	Stop() error

	// Broadcast sends a message to all connected peers.
	Broadcast(msg *Message) error

	// PrivateSend sends a message to a specific peer by their ID.
	PrivateSend(targetPeerID string, msg *Message) error

	// Who returns the list of currently online peers.
	Who() ([]Peer, error)

	// Rename changes the local display name.
	Rename(name string) error

	// Recv returns the channel for incoming messages.
	Recv() <-chan *Message

	// SendErr returns the channel for transport-level errors.
	SendErr() <-chan error
}
