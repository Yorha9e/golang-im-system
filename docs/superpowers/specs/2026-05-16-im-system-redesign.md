# IM System Redesign: Dual-Mode Architecture + Burn-After-Reading

## Overview

Transform the current TCP-based IM system into a three-mode messaging platform:
- **Server mode**: centralized chat via ChatServer
- **LAN P2P mode**: UDP multicast discovery + WebSocket full mesh (same subnet)
- **WAN P2P mode**: Signaling Server + STUN hole-punching, with automatic fallback to server mode

All modes share a common `Transport` interface and Protobuf-based message protocol.

## Architecture

```
                      +----------------------+
                      |    ChatService       |
                      |    (Transport 接口)   |
                      +----------+-----------+
              +------------------+------------------+
              |                  |                  |
              v                  v                  v
     +--------------+  +--------------+  +------------------+
     | Server       |  | LAN P2P      |  | WAN P2P          |
     | Transport    |  | Transport    |  | Transport        |
     | (中心化)     |  | (多播发现)    |  | (信令+STUN)      |
     |              |  |              |  |                  |
     | 连ChatServer |  | 直连同子网    |  | 试直连 -> 失败    |
     |              |  |              |  | -> 降级Server     |
     +--------------+  +--------------+  +------------------+
```

### Components

| Component | Role |
|-----------|------|
| **ChatServer** | Centralized message relay, online user list, P2P fallback target |
| **SignalingServer** | Peer discovery, SDP/ICE candidate forwarding, NO message relay |
| **ChatClient** | CLI client, supports all 3 transport modes, mode switch at runtime |
| **BurnManager** | Message auto-deletion timer, shared across all modes |
| **Discovery** | UDP multicast announce/listen (LAN), STUN + ICE (WAN) |

## Message Protocol (Protobuf)

```protobuf
message WsMessage {
  string message_id = 1;  // UUID for dedup and trace
  int64  timestamp  = 2;
  MsgType type      = 3;
  oneof payload {
    ChatMessage    chat    = 10;
    SystemNotice   system  = 11;
    Heartbeat      hb      = 12;
    BurnReceipt    receipt = 13;
  }
}

message ChatMessage {
  string content      = 1;
  int32  burn_seconds = 2;  // 0 = normal, >0 = burn-after-reading
  int64  burned_at    = 3;  // filled by receiver
}
```

## Transport Interface

```go
type Transport interface {
    Start(ctx context.Context) error
    Stop() error
    Broadcast(msg *Message) error
    PrivateSend(target string, msg *Message) error
    Who() ([]Peer, error)
    Rename(name string) error
    Recv() <-chan *Message
}
```

## Burn-After-Reading Design

- Sender sets `burn_seconds` in ChatMessage
- Receiver's BurnManager starts countdown on message display
- After countdown: local deletion + UI refresh (show "burned")
- Optional: burn receipt sent back to sender
- Server mode: server can enforce burn timestamp consistency
- P2P mode: each node manages independently

## Fallback Strategy

```
P2P Mode Start
    |
    +-- 1. Connect to Signaling Server (WS)
    +-- 2. STUN to get ICE candidates
    +-- 3. Exchange SDP Offer/Answer
    +-- 4. ICE connectivity check
         |
         +-- Success --> P2P direct (no server relay)
         |
         +-- Failure --> Circuit-break degrade
                            |
                            +-- Notify user
                            +-- Connect to ChatServer
                            +-- Switch to ServerTransport
```

No TURN server — ChatServer IS the fallback relay. Two services, not three.

## Project Structure

```
golang-im-system/
  cmd/
    server/main.go          # ChatServer entry
    signaling/main.go       # SignalingServer entry
    client/main.go           # Client entry (all modes)
  internal/
    proto/
      message.proto
    transport/
      interface.go
      server.go
      lan_p2p.go
      wan_p2p.go
    discovery/
      multicast.go          # UDP multicast (LAN)
      stun.go               # STUN client (WAN)
    signaling/
      server.go             # Signaling server (forward SDP/ICE)
      client.go             # Signaling client (connect to signaling)
    burn/
      manager.go
    peer/
      manager.go
    service/
      chat.go               # ChatService unified logic
  go.mod
```

## Implementation Phases

| Phase | Content |
|-------|---------|
| **1** | Fix 4 bugs in current code |
| **2a** | Protobuf protocol + Transport interface |
| **2b** | ServerTransport + ChatServer refactor |
| **2c** | P2PTransport - LAN multicast discovery |
| **2d** | SignalingServer + STUN hole-punching |
| **2e** | Circuit-break fallback (P2P -> Server) |
| **2f** | BurnManager (burn-after-reading) |
| **2g** | Unified CLI + runtime mode switch |

## Key Libraries

| Layer | Choice |
|-------|--------|
| Transport | gorilla/websocket |
| Serialization | google.golang.org/protobuf |
| LAN Discovery | net (UDP multicast, stdlib) |
| NAT Traversal | github.com/pion/webrtc/v3 |
| Signaling | gorilla/websocket |
| Logging | go.uber.org/zap |
