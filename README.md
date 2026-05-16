# Golang IM System

Multi-mode instant messaging system written in Go, designed as a production-grade architecture demo.

## Features

- **Centralized ChatServer** — WebSocket + Protobuf message relay, supports broadcast, private chat, rename, heartbeat
- **LAN P2P** — UDP multicast peer discovery + WebSocket full mesh (same subnet)
- **WAN P2P** — STUN hole-punching via Pion WebRTC + SignalingServer for SDP/ICE exchange, auto fallback to ChatServer
- **Burn-After-Reading** — server-enforced message self-destruction with BurnReceipt broadcast
- **3-Mode Architecture** — unified `Transport` interface: ServerTransport, LANP2PTransport, WANP2PTransport

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
     +--------------+  +--------------+  +------------------+
```

## Project Structure

```
golang-im-system/
├── cmd/
│   ├── server/main.go          # ChatServer entry (:8888)
│   └── signaling/main.go       # SignalingServer entry (:7000)
├── internal/
│   ├── proto/                  # Protobuf message definitions
│   ├── transport/              # Transport layer
│   │   ├── interface.go        # Transport interface
│   │   ├── server_transport.go # Server mode (client->server)
│   │   ├── lan_p2p.go          # LAN P2P (multicast + full mesh)
│   │   └── wan_p2p.go          # WAN P2P (STUN + signaling + fallback)
│   ├── engine/                 # ChatServer engine
│   ├── signaling/              # Signaling server/client (SDP/ICE relay)
│   ├── discovery/              # UDP multicast LAN peer discovery
│   └── burn/                   # Burn-after-reading manager
├── docs/                       # Design spec & implementation plan
├── go.mod
└── go.sum
```

## Quick Start

### Prerequisites

- Go 1.21+
- protoc (for regenerating `.proto` files)

### Run ChatServer

```bash
cd golang-im-system
go run ./cmd/server/ --addr :8888
```

### Run SignalingServer

```bash
go run ./cmd/signaling/ --addr :7000
```

### Run Tests

```bash
go test -v ./internal/engine/
```

## Protocol

Uses Protobuf over WebSocket binary frames. Message types:

| Type | Description |
|------|-------------|
| `CHAT` | Public broadcast message |
| `PRIVATE_CHAT` | Private message |
| `SYSTEM` | Join/leave/rename/error notifications |
| `HEARTBEAT` | Ping-pong keepalive |
| `WHO` | Online user list query |
| `BURN_RECEIPT` | Burn-after-reading confirmation |

## Tech Stack

| Layer | Library |
|-------|---------|
| Transport | `gorilla/websocket` |
| Serialization | `google.golang.org/protobuf` |
| NAT Traversal | `pion/webrtc/v3` |
| Logging | `go.uber.org/zap` |
