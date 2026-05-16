# IM System Redesign Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Fix 4 bugs in the current TCP-based IM system, then rebuild with Protobuf protocol, WebSocket transport, 3-mode architecture (Server / LAN P2P / WAN P2P), and burn-after-reading.

**Architecture:** Transport interface abstracts 3 modes — ServerTransport (centralized relay), LAN P2P (UDP multicast + WebSocket full mesh), WAN P2P (Signaling + STUN, fallback to server). BurnManager handles message self-destruction timers. Protobuf for message framing over WebSocket.

**Tech Stack:** Go 1.21+, gorilla/websocket, google.golang.org/protobuf, pion/webrtc/v3, go.uber.org/zap, google/uuid

---

## Phase 1: Bug Fixes

### Task 1.1: Fix private chat protocol

**Files:**
- Modify: `client/client.go:68-78`
- Modify: `server/user.go:71-87`

- [ ] **Step 1: Fix client PrivateChat to send correct format**

In `client/client.go`, replace the `PrivateChat` function:

```go
func (client *Client) PrivateChat() {
	var remoteName string
	var chatmsg string
	client.Searchmap()
	fmt.Print("please choose chatmate, enter exit to quit\n")
	fmt.Scanln(&remoteName)
	for remoteName != "exit" {
		fmt.Println("begin chat......")
		fmt.Scanln(&chatmsg)
		for chatmsg != "exit" {
			sendMsg := "to|" + remoteName + "|" + chatmsg + "\n"
			_, err := client.conn.Write([]byte(sendMsg + "r"))
			if err != nil {
				fmt.Println("write error")
				break
			}
			chatmsg = ""
			fmt.Println("send exit to quit")
			fmt.Scanln(&chatmsg)
		}
	}
}
```

- [ ] **Step 2: Fix server DoMessage private chat handling for edge case**

In `server/user.go`, the `DoMessage` private chat section — add length validation:

```go
} else if len(msg) > 4 && msg[:3] == "to|" {
	parts := strings.SplitN(msg, "|", 3)
	if len(parts) < 3 {
		this.SendMsg("syntax error: use to|username|message\n")
		return
	}
	remotename := parts[1]
	if remotename == "" {
		this.SendMsg("syntax error: empty username\n")
		return
	}
	remoteUser, ok := this.server.OnlinemAP[remotename]
	if !ok {
		this.SendMsg("can't find the user\n")
		return
	}
	msg1 := parts[2]
	remoteUser.SendMsg(this.Name + ":" + msg1 + "\n")
}
```

- [ ] **Step 3: Commit**

```bash
git add client/client.go server/user.go
git commit -m "fix: private chat protocol format - use 3-part to|name|msg"
```

---

### Task 1.2: Fix who/who1 command mismatch

**Files:**
- Modify: `client/client.go:50-57`
- Modify: `server/user.go:49`

- [ ] **Step 1: Fix client Searchmap to send correct command**

In `client/client.go`, change `Searchmap`:

```go
func (client *Client) Searchmap() {
	sendMsg := "who\n"
	_, err := client.conn.Write([]byte(sendMsg))
	if err != nil {
		fmt.Println("search failed")
		return
	}
}
```

- [ ] **Step 2: Add who1 alias support on server (for compatibility)**

In `server/user.go`, change the `who` condition to also accept `who1`:

```go
if msg == "who" || msg == "who1" {
```

- [ ] **Step 3: Commit**

```bash
git add client/client.go server/user.go
git commit -m "fix: unify who command - client sends who, server accepts both"
```

---

### Task 1.3: Fix recursive main() call on reconnect

**Files:**
- Modify: `client/client.go:124-126`

- [ ] **Step 1: Fix reconnect to re-create Client instead of calling main()**

In `client/client.go`, change case 4 in `Run()`:

```go
case 4:
	client.conn.Close()
	newClient := NewClient(client.ServerIp, client.ServerPort)
	if newClient == nil {
		fmt.Println("reconnect failed")
		return
	}
	*client = *newClient
	go client.DealResponse()
}
```

- [ ] **Step 2: Commit**

```bash
git add client/client.go
git commit -m "fix: reconnect creates new Client instead of recursive main()"
```

---

### Task 1.4: Fix line-ending protocol inconsistency

**Files:**
- Modify: `server/server.go:37-58`

- [ ] **Step 1: Replace raw conn.Read with bufio.Reader**

In `server/server.go`, add `"bufio"` import, then replace the reader goroutine in `Handler`:

```go
func (this *Server) Handler(conn net.Conn) {
	user := NewUser(conn, this)
	user.Online()
	islive := make(chan bool)
	go func() {
		reader := bufio.NewReader(conn)
		for {
			msg, err := reader.ReadString('\n')
			if err != nil {
				this.Broadcast(user, "offline")
				return
			}
			msg = strings.TrimRight(msg, "\r\n")
			user.DoMessage(msg)
			islive <- true
		}
	}()
	for {
		select {
		case <-islive:
		case <-time.After(time.Minute + 10):
			user.SendMsg("out of line")
			delete(this.OnlinemAP, user.Name)
			close(user.c)
			conn.Close()
			return
		}
	}
}
```

Also add `"strings"` and `"bufio"` to imports in `server.go`.

- [ ] **Step 2: Fix client message sending format**

In `client/client.go`, change all `sendMsg := ... + "\n"` to append `"r"` after `\n` is wrong. Remove the extra `"r"`:

PublicChat:
```go
sendMsg := chatMsg + "\n"
_, err := client.conn.Write([]byte(sendMsg))
```

PrivateChat:
```go
sendMsg := "to|" + remoteName + "|" + chatmsg + "\n"
_, err := client.conn.Write([]byte(sendMsg))
```

Searchmap:
```go
sendMsg := "who\n"
_, err := client.conn.Write([]byte(sendMsg))
```

UpdateName:
```go
sendMsg := "rename|" + client.Name + "\n"
_, err := client.conn.Write([]byte(sendMsg))
```

- [ ] **Step 3: Commit**

```bash
git add server/server.go client/client.go
git commit -m "fix: use bufio.Reader ReadString for consistent line protocol"
```

---

## Phase 2a: Protobuf Protocol + Transport Interface

### Task 2a.1: Initialize go module and install dependencies

**Files:**
- Create: `go.mod`
- Create: `go.sum`

- [ ] **Step 1: Initialize go module**

```bash
cd golang-im-system && go mod init golang-im-system
```

- [ ] **Step 2: Install protobuf compiler and Go plugins**

```bash
go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
```

(Done once per machine; verify with `which protoc-gen-go`)

- [ ] **Step 3: Install runtime dependencies**

```bash
go get github.com/gorilla/websocket
go get github.com/google/uuid
go get go.uber.org/zap
```

- [ ] **Step 4: Commit**

```bash
git add go.mod go.sum
git commit -m "chore: init go module, add websocket/uuid/zap deps"
```

---

### Task 2a.2: Define Protobuf message protocol

**Files:**
- Create: `internal/proto/message.proto`

- [ ] **Step 1: Write the proto file**

```protobuf
syntax = "proto3";
package proto;
option go_package = "golang-im-system/internal/proto";

message WsMessage {
  string message_id = 1;
  int64  timestamp  = 2;
  MsgType type      = 3;
  oneof payload {
    ChatMessage   chat    = 10;
    SystemNotice  system  = 11;
    Heartbeat     hb      = 12;
    BurnReceipt   receipt = 13;
    PeerInfo      peer    = 14;
  }
}

enum MsgType {
  CHAT          = 0;
  SYSTEM        = 1;
  HEARTBEAT     = 2;
  BURN_RECEIPT  = 3;
  PEER_DISCOVER = 4;
  PRIVATE_CHAT  = 5;
  RENAME        = 6;
  WHO           = 7;
}

message ChatMessage {
  string from        = 1;
  string to          = 2;  // empty = broadcast
  string content     = 3;
  int32  burn_seconds = 4; // 0 = normal, >0 = burn-after-reading seconds
  int64  burned_at   = 5;  // filled by receiver
}

message SystemNotice {
  SystemType type = 1;
  string content  = 2;
  string user     = 3;
}

enum SystemType {
  JOIN   = 0;
  LEAVE  = 1;
  RENAME = 2;
  ERROR  = 3;
}

message Heartbeat {
  int64 ping_ts = 1;
}

message BurnReceipt {
  string message_id = 1;
  int64  burned_at  = 2;
}

message PeerInfo {
  string node_id   = 1;
  string node_name = 2;
  string addr      = 3;
  int32  ws_port   = 4;
}
```

- [ ] **Step 2: Generate Go code from proto**

```bash
mkdir -p internal/proto
protoc --go_out=. --go_opt=paths=source_relative internal/proto/message.proto
```

- [ ] **Step 3: Commit**

```bash
git add internal/proto/
git commit -m "feat: add Protobuf message protocol definition"
```

---

### Task 2a.3: Define Transport interface and Message type

**Files:**
- Create: `internal/transport/interface.go`

- [ ] **Step 1: Write Transport interface**

```go
package transport

import (
	"context"
	"golang-im-system/internal/proto"
)

type Peer struct {
	ID   string
	Name string
	Addr string
}

type Message struct {
	Msg  *proto.WsMessage
	From string
}

type Transport interface {
	Start(ctx context.Context) error
	Stop() error
	Broadcast(msg *Message) error
	PrivateSend(targetPeerID string, msg *Message) error
	Who() ([]Peer, error)
	Rename(name string) error
	Recv() <-chan *Message
	SendErr() <-chan error
}
```

- [ ] **Step 2: Commit**

```bash
git add internal/transport/interface.go
git commit -m "feat: define Transport interface and Message/Peer types"
```

---

## Phase 2b: ServerTransport + ChatServer Refactor

### Task 2b.1: Refactor server.go to use WebSocket and Protobuf

**Files:**
- Create: `internal/transport/server.go`
- Modify: `server/server.go` (refactor)
- Modify: `server/user.go` (refactor or delete)
- Modify: `server/main.go`

- [ ] **Step 1: Rewrite server.go with WebSocket + Protobuf**

```go
package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"sync"

	"github.com/gorilla/websocket"
	"go.uber.org/zap"
)

var upgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool { return true },
}

type ChatServer struct {
	addr     string
	port     int
	online   map[string]*ClientConn
	mu       sync.RWMutex
	server   *http.Server
	logger   *zap.Logger
}

type ClientConn struct {
	Name string
	ID   string
	conn *websocket.Conn
	send chan *proto.WsMessage
}

func NewChatServer(addr string, port int) *ChatServer {
	logger, _ := zap.NewDevelopment()
	return &ChatServer{
		addr:   addr,
		port:   port,
		online: make(map[string]*ClientConn),
		logger: logger,
	}
}

func (s *ChatServer) handleWS(w http.ResponseWriter, r *http.Request) {
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		s.logger.Error("upgrade failed", zap.Error(err))
		return
	}

	client := &ClientConn{
		ID:   r.RemoteAddr + "-" + fmt.Sprintf("%d", time.Now().UnixNano()),
		conn: conn,
		send: make(chan *proto.WsMessage, 256),
	}
	client.Name = client.ID[:8]

	s.mu.Lock()
	s.online[client.Name] = client
	s.mu.Unlock()

	s.logger.Info("user online", zap.String("name", client.Name))
	s.broadcastSystem(SystemType_JOIN, client.Name)

	go s.readPump(client)
	go s.writePump(client)
}

func (s *ChatServer) readPump(c *ClientConn) {
	defer func() {
		s.mu.Lock()
		delete(s.online, c.Name)
		s.mu.Unlock()
		close(c.send)
		c.conn.Close()
		s.logger.Info("user offline", zap.String("name", c.Name))
		s.broadcastSystem(SystemType_LEAVE, c.Name)
	}()

	for {
		_, raw, err := c.conn.ReadMessage()
		if err != nil {
			return
		}
		msg := &proto.WsMessage{}
		if err := proto.Unmarshal(raw, msg); err != nil {
			continue
		}
		s.handleMessage(c, msg)
	}
}

func (s *ChatServer) writePump(c *ClientConn) {
	for msg := range c.send {
		data, _ := proto.Marshal(msg)
		c.conn.WriteMessage(websocket.BinaryMessage, data)
	}
}

func (s *ChatServer) handleMessage(from *ClientConn, msg *proto.WsMessage) {
	switch msg.Type {
	case proto.MsgType_CHAT:
		s.handleChat(from, msg.GetChat())
	case proto.MsgType_PRIVATE_CHAT:
		s.handlePrivateChat(from, msg.GetChat())
	case proto.MsgType_WHO:
		s.handleWho(from)
	case proto.MsgType_RENAME:
		s.handleRename(from, msg)
	case proto.MsgType_HEARTBEAT:
		hb := msg.GetHb()
		go func() {
			resp := &proto.WsMessage{
				MessageId: msg.MessageId,
				Type:      proto.MsgType_HEARTBEAT,
				Payload:   &proto.WsMessage_Hb{Hb: &proto.Heartbeat{PingTs: hb.PingTs}},
			}
			from.send <- resp
		}()
	}
}

func (s *ChatServer) handleChat(from *ClientConn, chat *proto.ChatMessage) {
	msg := &proto.WsMessage{
		MessageId: uuid.New().String(),
		Timestamp: time.Now().Unix(),
		Type:      proto.MsgType_CHAT,
		Payload: &proto.WsMessage_Chat{
			Chat: &proto.ChatMessage{
				From:        from.Name,
				Content:     chat.Content,
				BurnSeconds: chat.BurnSeconds,
			},
		},
	}
	s.broadcast(msg)
}

func (s *ChatServer) handlePrivateChat(from *ClientConn, chat *proto.ChatMessage) {
	s.mu.RLock()
	target, ok := s.online[chat.To]
	s.mu.RUnlock()
	if !ok {
		from.send <- systemMsg("user not found: " + chat.To)
		return
	}
	msg := &proto.WsMessage{
		MessageId: uuid.New().String(),
		Timestamp: time.Now().Unix(),
		Type:      proto.MsgType_PRIVATE_CHAT,
		Payload: &proto.WsMessage_Chat{
			Chat: &proto.ChatMessage{
				From:        from.Name,
				Content:     chat.Content,
				BurnSeconds: chat.BurnSeconds,
			},
		},
	}
	target.send <- msg
}

func (s *ChatServer) handleWho(from *ClientConn) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var peers []string
	for name := range s.online {
		peers = append(peers, name)
	}
	data, _ := json.Marshal(peers)
	from.send <- systemMsg(string(data))
}

func (s *ChatServer) handleRename(from *ClientConn, msg *proto.WsMessage) {
	newName := msg.GetSystem().Content
	s.mu.Lock()
	if _, exists := s.online[newName]; exists {
		s.mu.Unlock()
		from.send <- systemMsg("name already taken")
		return
	}
	oldName := from.Name
	delete(s.online, oldName)
	from.Name = newName
	s.online[newName] = from
	s.mu.Unlock()

	broadcastMsg := &proto.WsMessage{
		MessageId: uuid.New().String(),
		Timestamp: time.Now().Unix(),
		Type:      proto.MsgType_SYSTEM,
		Payload: &proto.WsMessage_System{
			System: &proto.SystemNotice{
				Type:    proto.SystemType_RENAME,
				Content: newName,
				User:    oldName,
			},
		},
	}
	s.broadcast(broadcastMsg)
}

func (s *ChatServer) broadcast(msg *proto.WsMessage) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	for _, c := range s.online {
		select {
		case c.send <- msg:
		default:
		}
	}
}

func (s *ChatServer) broadcastSystem(sysType proto.SystemType, name string) {
	msg := &proto.WsMessage{
		MessageId: uuid.New().String(),
		Timestamp: time.Now().Unix(),
		Type:      proto.MsgType_SYSTEM,
		Payload: &proto.WsMessage_System{
			System: &proto.SystemNotice{
				Type:    sysType,
				Content: name,
			},
		},
	}
	s.broadcast(msg)
}

func systemMsg(content string) *proto.WsMessage {
	return &proto.WsMessage{
		MessageId: uuid.New().String(),
		Timestamp: time.Now().Unix(),
		Type:      proto.MsgType_SYSTEM,
		Payload: &proto.WsMessage_System{
			System: &proto.SystemNotice{Type: proto.SystemType_ERROR, Content: content},
		},
	}
}

func (s *ChatServer) Start() error {
	mux := http.NewServeMux()
	mux.HandleFunc("/ws", s.handleWS)
	s.server = &http.Server{Addr: fmt.Sprintf("%s:%d", s.addr, s.port), Handler: mux}
	s.logger.Info("ChatServer starting", zap.String("addr", s.server.Addr))
	return s.server.ListenAndServe()
}

func (s *ChatServer) Stop(ctx context.Context) error {
	return s.server.Shutdown(ctx)
}
```

- [ ] **Step 2: Rewrite main.go for ChatServer**

```go
package main

import (
	"flag"
	"os"
	"os/signal"
	"syscall"
)

func main() {
	ip := flag.String("ip", "0.0.0.0", "listen address")
	port := flag.Int("port", 8888, "listen port")
	flag.Parse()

	server := NewChatServer(*ip, *port)
	go func() {
		if err := server.Start(); err != nil {
			panic(err)
		}
	}()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit
	server.Stop(context.Background())
}
```

- [ ] **Step 3: Commit**

```bash
rm server/user.go server/server.go 2>/dev/null
mkdir -p cmd/server internal/transport
mv server/main.go cmd/server/main.go
# Write the new server.go as shown
git add cmd/server/main.go internal/transport/server.go
git rm server/user.go server/server.go server/server.exe 2>/dev/null
git commit -m "feat: refactor ChatServer to WebSocket + Protobuf"
```

---

### Task 2b.2: Implement ServerTransport (client side)

**Files:**
- Create: `internal/transport/server_transport.go`

- [ ] **Step 1: Write ServerTransport**

```go
package transport

import (
	"context"
	"fmt"
	"net/url"

	"github.com/gorilla/websocket"
	"golang-im-system/internal/proto"
)

type ServerTransport struct {
	serverAddr string
	name       string
	conn       *websocket.Conn
	recv       chan *Message
	errCh      chan error
	ctx        context.Context
	cancel     context.CancelFunc
}

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
		if err := proto.Unmarshal(raw, msg); err != nil {
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
	data, _ := proto.Marshal(msg)
	return t.conn.WriteMessage(websocket.BinaryMessage, data)
}

func (t *ServerTransport) Broadcast(msg *Message) error {
	return t.send(msg.Msg)
}

func (t *ServerTransport) PrivateSend(target string, msg *Message) error {
	msg.Msg.Type = proto.MsgType_PRIVATE_CHAT
	if chat := msg.Msg.GetChat(); chat != nil {
		chat.To = target
	}
	return t.send(msg.Msg)
}

func (t *ServerTransport) Who() ([]Peer, error) {
	msg := &proto.WsMessage{
		Type: proto.MsgType_WHO,
	}
	return nil, t.send(msg)
}

func (t *ServerTransport) Rename(name string) error {
	msg := &proto.WsMessage{
		Type: proto.MsgType_RENAME,
		Payload: &proto.WsMessage_System{
			System: &proto.SystemNotice{Content: name},
		},
	}
	return t.send(msg)
}

func (t *ServerTransport) Recv() <-chan *Message {
	return t.recv
}

func (t *ServerTransport) SendErr() <-chan error {
	return t.errCh
}

func (t *ServerTransport) Stop() error {
	if t.cancel != nil {
		t.cancel()
	}
	if t.conn != nil {
		return t.conn.Close()
	}
	return nil
}
```

- [ ] **Step 2: Commit**

```bash
git add internal/transport/server_transport.go
git commit -m "feat: add ServerTransport - client-side WS connection to ChatServer"
```

---

## Phase 2c: LAN P2P Transport

### Task 2c.1: Implement UDP multicast discovery

**Files:**
- Create: `internal/discovery/multicast.go`

- [ ] **Step 1: Write multicast discovery**

```go
package discovery

import (
	"context"
	"encoding/json"
	"net"
	"sync"
	"time"
)

const (
	DefaultMulticastAddr = "239.255.0.3:9999"
	AnnounceInterval     = 5 * time.Second
	PeerTimeout          = 15 * time.Second
)

type Announce struct {
	NodeID   string `json:"node_id"`
	NodeName string `json:"node_name"`
	WSPort   int    `json:"ws_port"`
}

type DiscoveredPeer struct {
	Announce
	LastSeen time.Time
	Addr     *net.UDPAddr
}

type MulticastDiscovery struct {
	multicastAddr string
	localNode     Announce
	conn          *net.UDPConn
	peers         map[string]*DiscoveredPeer
	mu            sync.RWMutex
	onJoin        func(DiscoveredPeer)
	onLeave       func(string) // nodeID
}

func NewMulticastDiscovery(nodeID, nodeName string, wsPort int) *MulticastDiscovery {
	return &MulticastDiscovery{
		multicastAddr: DefaultMulticastAddr,
		localNode: Announce{
			NodeID:   nodeID,
			NodeName: nodeName,
			WSPort:   wsPort,
		},
		peers: make(map[string]*DiscoveredPeer),
	}
}

func (d *MulticastDiscovery) SetCallbacks(onJoin func(DiscoveredPeer), onLeave func(string)) {
	d.onJoin = onJoin
	d.onLeave = onLeave
}

func (d *MulticastDiscovery) Start(ctx context.Context) error {
	addr, err := net.ResolveUDPAddr("udp", d.multicastAddr)
	if err != nil {
		return err
	}
	conn, err := net.ListenMulticastUDP("udp", nil, addr)
	if err != nil {
		return err
	}
	d.conn = conn

	go d.listen(ctx)
	go d.announce(ctx)
	go d.prune(ctx)
	return nil
}

func (d *MulticastDiscovery) listen(ctx context.Context) {
	buf := make([]byte, 1024)
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}
		d.conn.SetReadDeadline(time.Now().Add(1 * time.Second))
		n, remote, err := d.conn.ReadFromUDP(buf)
		if err != nil {
			if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
				continue
			}
			return
		}
		var a Announce
		if err := json.Unmarshal(buf[:n], &a); err != nil {
			continue
		}
		if a.NodeID == d.localNode.NodeID {
			continue
		}
		d.mu.Lock()
		if _, exists := d.peers[a.NodeID]; !exists {
			peer := &DiscoveredPeer{Announce: a, LastSeen: time.Now(), Addr: remote}
			d.peers[a.NodeID] = peer
			d.mu.Unlock()
			if d.onJoin != nil {
				d.onJoin(*peer)
			}
		} else {
			d.peers[a.NodeID].LastSeen = time.Now()
			d.mu.Unlock()
		}
	}
}

func (d *MulticastDiscovery) announce(ctx context.Context) {
	ticker := time.NewTicker(AnnounceInterval)
	defer ticker.Stop()
	data, _ := json.Marshal(d.localNode)
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			addr, _ := net.ResolveUDPAddr("udp", d.multicastAddr)
			d.conn.WriteToUDP(data, addr)
		}
	}
}

func (d *MulticastDiscovery) prune(ctx context.Context) {
	ticker := time.NewTicker(PeerTimeout)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			d.mu.Lock()
			for id, p := range d.peers {
				if time.Since(p.LastSeen) > PeerTimeout {
					delete(d.peers, id)
					if d.onLeave != nil {
						d.onLeave(id)
					}
				}
			}
			d.mu.Unlock()
		}
	}
}

func (d *MulticastDiscovery) Peers() []DiscoveredPeer {
	d.mu.RLock()
	defer d.mu.RUnlock()
	var result []DiscoveredPeer
	for _, p := range d.peers {
		result = append(result, *p)
	}
	return result
}

func (d *MulticastDiscovery) Stop() error {
	if d.conn != nil {
		return d.conn.Close()
	}
	return nil
}
```

- [ ] **Step 2: Commit**

```bash
git add internal/discovery/multicast.go
git commit -m "feat: add UDP multicast LAN discovery"
```

---

### Task 2c.2: Implement LAN P2P Transport

**Files:**
- Create: `internal/transport/lan_p2p.go`

- [ ] **Step 1: Write LAN P2P Transport**

```go
package transport

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"sync"

	"github.com/google/uuid"
	"github.com/gorilla/websocket"
	"golang-im-system/internal/discovery"
	"golang-im-system/internal/proto"
)

type LANP2PTransport struct {
	nodeID   string
	nodeName string
	wsPort   int
	upgrader websocket.Upgrader

	mu     sync.RWMutex
	peers  map[string]*wsPeer // nodeID -> peer
	server *http.Server
	recv   chan *Message
	errCh  chan error
	ctx    context.Context
	cancel context.CancelFunc

	discovery *discovery.MulticastDiscovery
}

type wsPeer struct {
	NodeID   string
	NodeName string
	conn     *websocket.Conn
	send     chan *proto.WsMessage
}

func NewLANP2PTransport(name string, wsPort int) *LANP2PTransport {
	return &LANP2PTransport{
		nodeID:   uuid.New().String()[:8],
		nodeName: name,
		wsPort:   wsPort,
		peers:    make(map[string]*wsPeer),
		recv:     make(chan *Message, 256),
		errCh:    make(chan error, 16),
		upgrader: websocket.Upgrader{CheckOrigin: func(r *http.Request) bool { return true }},
	}
}

func (t *LANP2PTransport) Start(ctx context.Context) error {
	t.ctx, t.cancel = context.WithCancel(ctx)

	// Start WS server for incoming peer connections
	mux := http.NewServeMux()
	mux.HandleFunc("/ws", t.handleIncomingWS)
	t.server = &http.Server{Addr: fmt.Sprintf(":%d", t.wsPort), Handler: mux}
	ln, err := net.Listen("tcp", t.server.Addr)
	if err != nil {
		return fmt.Errorf("listen: %w", err)
	}
	go t.server.Serve(ln)

	// Start multicast discovery
	t.discovery = discovery.NewMulticastDiscovery(t.nodeID, t.nodeName, t.wsPort)
	t.discovery.SetCallbacks(t.onPeerJoin, t.onPeerLeave)
	if err := t.discovery.Start(ctx); err != nil {
		return fmt.Errorf("discovery: %w", err)
	}
	return nil
}

func (t *LANP2PTransport) handleIncomingWS(w http.ResponseWriter, r *http.Request) {
	conn, err := t.upgrader.Upgrade(w, r, nil)
	if err != nil {
		return
	}
	// Read initial handshake to get peer info
	var hs proto.PeerInfo
	if err := conn.ReadJSON(&hs); err != nil {
		conn.Close()
		return
	}
	peer := &wsPeer{
		NodeID:   hs.NodeId,
		NodeName: hs.NodeName,
		conn:     conn,
		send:     make(chan *proto.WsMessage, 256),
	}
	t.mu.Lock()
	t.peers[hs.NodeId] = peer
	t.mu.Unlock()
	go t.readPump(peer)
	go t.writePump(peer)
}

func (t *LANP2PTransport) onPeerJoin(p discovery.DiscoveredPeer) {
	addr := fmt.Sprintf("%s:%d", p.Addr.IP.String(), p.WSPort)
	u := url.URL{Scheme: "ws", Host: addr, Path: "/ws"}
	conn, _, err := websocket.DefaultDialer.Dial(u.String(), nil)
	if err != nil {
		return
	}
	// Send handshake
	hs := &proto.PeerInfo{NodeId: t.nodeID, NodeName: t.nodeName}
	conn.WriteJSON(hs)

	peer := &wsPeer{
		NodeID:   p.NodeID,
		NodeName: p.NodeName,
		conn:     conn,
		send:     make(chan *proto.WsMessage, 256),
	}
	t.mu.Lock()
	t.peers[p.NodeID] = peer
	t.mu.Unlock()

	go t.readPump(peer)
	go t.writePump(peer)
}

func (t *LANP2PTransport) onPeerLeave(nodeID string) {
	t.mu.Lock()
	if peer, ok := t.peers[nodeID]; ok {
		close(peer.send)
		peer.conn.Close()
		delete(t.peers, nodeID)
	}
	t.mu.Unlock()
}

func (t *LANP2PTransport) readPump(peer *wsPeer) {
	for {
		_, raw, err := peer.conn.ReadMessage()
		if err != nil {
			t.onPeerLeave(peer.NodeID)
			return
		}
		msg := &proto.WsMessage{}
		proto.Unmarshal(raw, msg)
		select {
		case t.recv <- &Message{Msg: msg, From: peer.NodeID}:
		case <-t.ctx.Done():
			return
		}
	}
}

func (t *LANP2PTransport) writePump(peer *wsPeer) {
	for msg := range peer.send {
		data, _ := proto.Marshal(msg)
		peer.conn.WriteMessage(websocket.BinaryMessage, data)
	}
}

func (t *LANP2PTransport) Broadcast(msg *Message) error {
	data, _ := proto.Marshal(msg.Msg)
	t.mu.RLock()
	defer t.mu.RUnlock()
	for _, peer := range t.peers {
		peer.conn.WriteMessage(websocket.BinaryMessage, data)
	}
	return nil
}

func (t *LANP2PTransport) PrivateSend(target string, msg *Message) error {
	t.mu.RLock()
	peer, ok := t.peers[target]
	t.mu.RUnlock()
	if !ok {
		return fmt.Errorf("peer %s not found", target)
	}
	data, _ := proto.Marshal(msg.Msg)
	return peer.conn.WriteMessage(websocket.BinaryMessage, data)
}

func (t *LANP2PTransport) Who() ([]Peer, error) {
	t.mu.RLock()
	defer t.mu.RUnlock()
	var peers []Peer
	for _, p := range t.peers {
		peers = append(peers, Peer{ID: p.NodeID, Name: p.NodeName})
	}
	return peers, nil
}

func (t *LANP2PTransport) Rename(name string) error {
	t.nodeName = name
	t.discovery.SetLocalName(name)
	return nil
}

func (t *LANP2PTransport) Recv() <-chan *Message  { return t.recv }
func (t *LANP2PTransport) SendErr() <-chan error   { return t.errCh }

func (t *LANP2PTransport) Stop() error {
	t.cancel()
	if t.server != nil {
		t.server.Shutdown(context.Background())
	}
	return t.discovery.Stop()
}
```

- [ ] **Step 2: Commit**

```bash
git add internal/transport/lan_p2p.go
git commit -m "feat: add LAN P2P transport with multicast discovery + WS full mesh"
```

---

## Phase 2d: Signaling Server + STUN (WAN P2P)

### Task 2d.1: Install Pion WebRTC dependency

**Files:**
- Modify: `go.mod`

- [ ] **Step 1: Install Pion WebRTC**

```bash
cd golang-im-system && go get github.com/pion/webrtc/v3
```

- [ ] **Step 2: Commit**

```bash
git add go.mod go.sum
git commit -m "chore: add pion/webrtc for STUN/ICE NAT traversal"
```

---

### Task 2d.2: Implement Signaling Server

**Files:**
- Create: `cmd/signaling/main.go`
- Create: `internal/signaling/server.go`

- [ ] **Step 1: Write SignalingServer**

In `internal/signaling/server.go`:

```go
package signaling

import (
	"encoding/json"
	"net/http"
	"sync"

	"github.com/gorilla/websocket"
	"github.com/google/uuid"
)

var upgrader = websocket.Upgrader{CheckOrigin: func(r *http.Request) bool { return true }}

type SigMessage struct {
	Type    string          `json:"type"`
	From    string          `json:"from,omitempty"`
	To      string          `json:"to,omitempty"`
	Payload json.RawMessage `json:"payload,omitempty"`
}

type SigPeer struct {
	ID   string
	Name string
	conn *websocket.Conn
	send chan []byte
}

type SignalingServer struct {
	addr  string
	peers map[string]*SigPeer
	mu    sync.RWMutex
}

func NewSignalingServer(addr string) *SignalingServer {
	return &SignalingServer{
		addr:  addr,
		peers: make(map[string]*SigPeer),
	}
}

func (s *SignalingServer) Start() error {
	http.HandleFunc("/signal", s.handleWS)
	return http.ListenAndServe(s.addr, nil)
}

func (s *SignalingServer) handleWS(w http.ResponseWriter, r *http.Request) {
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		return
	}
	peer := &SigPeer{
		ID:   uuid.New().String()[:8],
		conn: conn,
		send: make(chan []byte, 64),
		Name: r.URL.Query().Get("name"),
	}

	s.mu.Lock()
	s.peers[peer.ID] = peer
	s.notifyPeers(peer.ID, "peer_join", peer.ID, peer.Name)
	s.mu.Unlock()

	go s.readPump(peer)
	go s.writePump(peer)
}

func (s *SignalingServer) readPump(p *SigPeer) {
	defer func() {
		s.mu.Lock()
		delete(s.peers, p.ID)
		s.notifyPeers(p.ID, "peer_leave", p.ID, p.Name)
		s.mu.Unlock()
		close(p.send)
		p.conn.Close()
	}()

	for {
		_, raw, err := p.conn.ReadMessage()
		if err != nil {
			return
		}
		var msg SigMessage
		json.Unmarshal(raw, &msg)
		msg.From = p.ID

		if msg.To != "" {
			// Route to specific peer
			s.mu.RLock()
			target, ok := s.peers[msg.To]
			s.mu.RUnlock()
			if ok {
				data, _ := json.Marshal(msg)
				target.send <- data
			}
		} else {
			// Broadcast to all except sender
			data, _ := json.Marshal(msg)
			s.mu.RLock()
			for id, peer := range s.peers {
				if id != p.ID {
					peer.send <- data
				}
			}
			s.mu.RUnlock()
		}
	}
}

func (s *SignalingServer) writePump(p *SigPeer) {
	for data := range p.send {
		p.conn.WriteMessage(websocket.TextMessage, data)
	}
}

func (s *SignalingServer) notifyPeers(excludeID, typ, id, name string) {
	msg := SigMessage{
		Type: typ,
		From: id,
	}
	payload, _ := json.Marshal(map[string]string{"id": id, "name": name})
	msg.Payload = payload
	data, _ := json.Marshal(msg)
	for pid, peer := range s.peers {
		if pid != excludeID {
			peer.send <- data
		}
	}
}

func (s *SignalingServer) PeerList() []string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	ids := make([]string, 0, len(s.peers))
	for id := range s.peers {
		ids = append(ids, id)
	}
	return ids
}
```

In `cmd/signaling/main.go`:

```go
package main

import (
	"flag"
	"log"

	"golang-im-system/internal/signaling"
)

func main() {
	addr := flag.String("addr", ":7000", "signaling server listen address")
	flag.Parse()

	server := signaling.NewSignalingServer(*addr)
	log.Printf("SignalingServer starting on %s", *addr)
	log.Fatal(server.Start())
}
```

- [ ] **Step 2: Commit**

```bash
git add cmd/signaling/main.go internal/signaling/server.go
git commit -m "feat: add SignalingServer for SDP/ICE forwarding"
```

---

### Task 2d.3: Implement WAN P2P Transport with STUN + Fallback

**Files:**
- Create: `internal/transport/wan_p2p.go`
- Create: `internal/signaling/client.go`

- [ ] **Step 1: Write SignalingClient**

In `internal/signaling/client.go`:

```go
package signaling

import (
	"encoding/json"
	"net/url"

	"github.com/gorilla/websocket"
)

type SignalingClient struct {
	serverURL string
	name      string
	conn      *websocket.Conn
	OnMessage func(SigMessage)
	OnPeerJoin func(id, name string)
	OnPeerLeave func(id string)
}

func NewSignalingClient(serverAddr, name string) *SignalingClient {
	return &SignalingClient{
		serverURL: "ws://" + serverAddr + "/signal?name=" + url.QueryEscape(name),
		name:      name,
	}
}

func (c *SignalingClient) Connect() error {
	conn, _, err := websocket.DefaultDialer.Dial(c.serverURL, nil)
	if err != nil {
		return err
	}
	c.conn = conn
	go c.readLoop()
	return nil
}

func (c *SignalingClient) readLoop() {
	for {
		_, raw, err := c.conn.ReadMessage()
		if err != nil {
			return
		}
		var msg SigMessage
		json.Unmarshal(raw, &msg)

		switch msg.Type {
		case "peer_join":
			var info map[string]string
			json.Unmarshal(msg.Payload, &info)
			if c.OnPeerJoin != nil {
				c.OnPeerJoin(info["id"], info["name"])
			}
		case "peer_leave":
			var info map[string]string
			json.Unmarshal(msg.Payload, &info)
			if c.OnPeerLeave != nil {
				c.OnPeerLeave(info["id"])
			}
		default:
			if c.OnMessage != nil {
				c.OnMessage(msg)
			}
		}
	}
}

func (c *SignalingClient) Send(msg SigMessage) error {
	data, _ := json.Marshal(msg)
	return c.conn.WriteMessage(websocket.TextMessage, data)
}

func (c *SignalingClient) Close() error {
	return c.conn.Close()
}
```

- [ ] **Step 2: Write WAN P2P Transport**

In `internal/transport/wan_p2p.go`:

```go
package transport

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/pion/webrtc/v3"
	"golang-im-system/internal/signaling"
	"golang-im-system/internal/proto"
)

type WANP2PTransport struct {
	nodeID   string
	nodeName string
	sigURL   string
	fallback string // ChatServer address for fallback

	sigClient  *signaling.SignalingClient
	peers      map[string]*webrtc.DataChannel
	peerConns  map[string]*webrtc.PeerConnection
	mu         sync.RWMutex
	recv       chan *Message
	errCh      chan error
	ctx        context.Context
	cancel     context.CancelFunc

	fallbackTransport *ServerTransport
	fallbackUsed      bool
	onFallback        func()
}

func NewWANP2PTransport(name, signalingAddr, fallbackAddr string) *WANP2PTransport {
	return &WANP2PTransport{
		nodeID:   uuid.New().String()[:8],
		nodeName: name,
		sigURL:   signalingAddr,
		fallback: fallbackAddr,
		peers:    make(map[string]*webrtc.DataChannel),
		peerConns: make(map[string]*webrtc.PeerConnection),
		recv:     make(chan *Message, 256),
		errCh:    make(chan error, 16),
	}
}

func (t *WANP2PTransport) Start(ctx context.Context) error {
	t.ctx, t.cancel = context.WithCancel(ctx)

	t.sigClient = signaling.NewSignalingClient(t.sigURL, t.nodeName)
	t.sigClient.OnPeerJoin = t.onPeerJoin
	t.sigClient.OnMessage = t.onSigMessage

	if err := t.sigClient.Connect(); err != nil {
		return fmt.Errorf("connect to signaling: %w", err)
	}

	// Give ICE a few seconds, then check if we connected to anyone
	go func() {
		select {
		case <-time.After(10 * time.Second):
			t.mu.RLock()
			connected := len(t.peerConns) > 0
			t.mu.RUnlock()
			if !connected {
				t.activateFallback()
			}
		case <-t.ctx.Done():
		}
	}()

	return nil
}

func (t *WANP2PTransport) onPeerJoin(peerID, peerName string) {
	// Create WebRTC PeerConnection for this peer
	config := webrtc.Configuration{
		ICEServers: []webrtc.ICEServer{
			{URLs: []string{"stun:stun.l.google.com:19302"}},
		},
	}
	pc, err := webrtc.NewPeerConnection(config)
	if err != nil {
		t.activateFallback()
		return
	}

	dc, err := pc.CreateDataChannel("chat", nil)
	if err != nil {
		t.activateFallback()
		return
	}

	dc.OnMessage(func(msg webrtc.DataChannelMessage) {
		var wsm proto.WsMessage
		proto.Unmarshal(msg.Data, &wsm)
		t.recv <- &Message{Msg: &wsm, From: peerID}
	})

	pc.OnICECandidate(func(c *webrtc.ICECandidate) {
		if c == nil {
			return
		}
		candidateJSON, _ := json.Marshal(c.ToJSON())
		t.sigClient.Send(signaling.SigMessage{
			Type: "ice_candidate",
			To:   peerID,
			Payload: candidateJSON,
		})
	})

	// Create and send offer
	offer, err := pc.CreateOffer(nil)
	if err != nil {
		return
	}
	pc.SetLocalDescription(offer)
	offerJSON, _ := json.Marshal(offer)
	t.sigClient.Send(signaling.SigMessage{
		Type:    "offer",
		To:      peerID,
		Payload: offerJSON,
	})

	t.mu.Lock()
	t.peers[peerID] = dc
	t.peerConns[peerID] = pc
	t.mu.Unlock()
}

func (t *WANP2PTransport) onSigMessage(msg signaling.SigMessage) {
	t.mu.RLock()
	pc, exists := t.peerConns[msg.From]
	t.mu.RUnlock()

	if !exists {
		// New incoming peer, create PC
		config := webrtc.Configuration{
			ICEServers: []webrtc.ICEServer{
				{URLs: []string{"stun:stun.l.google.com:19302"}},
			},
		}
		var err error
		pc, err = webrtc.NewPeerConnection(config)
		if err != nil {
			return
		}

		dc, err := pc.CreateDataChannel("chat", nil)
		if err != nil {
			return
		}
		dc.OnMessage(func(msg webrtc.DataChannelMessage) {
			var wsm proto.WsMessage
			proto.Unmarshal(msg.Data, &wsm)
			t.recv <- &Message{Msg: &wsm, From: msg.From}
		})

		pc.OnICECandidate(func(c *webrtc.ICECandidate) {
			if c == nil {
				return
			}
			cJSON, _ := json.Marshal(c.ToJSON())
			t.sigClient.Send(signaling.SigMessage{
				Type: "ice_candidate",
				To:   msg.From,
				Payload: cJSON,
			})
		})

		pc.OnDataChannel(func(dc *webrtc.DataChannel) {
			dc.OnMessage(func(msg webrtc.DataChannelMessage) {
				var wsm proto.WsMessage
				proto.Unmarshal(msg.Data, &wsm)
				t.recv <- &Message{Msg: &wsm, From: msg.From}
			})
		})

		t.mu.Lock()
		t.peerConns[msg.From] = pc
		t.mu.Unlock()
	}

	switch msg.Type {
	case "offer":
		var offer webrtc.SessionDescription
		json.Unmarshal(msg.Payload, &offer)
		pc.SetRemoteDescription(offer)
		answer, _ := pc.CreateAnswer(nil)
		pc.SetLocalDescription(answer)
		answerJSON, _ := json.Marshal(answer)
		t.sigClient.Send(signaling.SigMessage{
			Type:    "answer",
			To:      msg.From,
			Payload: answerJSON,
		})
	case "answer":
		var answer webrtc.SessionDescription
		json.Unmarshal(msg.Payload, &answer)
		pc.SetRemoteDescription(answer)
	case "ice_candidate":
		var candidate webrtc.ICECandidateInit
		json.Unmarshal(msg.Payload, &candidate)
		pc.AddICECandidate(candidate)
	}
}

func (t *WANP2PTransport) activateFallback() {
	if t.fallbackUsed {
		return
	}
	t.fallbackUsed = true
	if t.onFallback != nil {
		t.onFallback()
	}
	t.fallbackTransport = NewServerTransport(t.fallback)
	t.fallbackTransport.Start(t.ctx)
	// Forward messages from fallback transport
	go func() {
		for msg := range t.fallbackTransport.Recv() {
			select {
			case t.recv <- msg:
			case <-t.ctx.Done():
			}
		}
	}()
}

func (t *WANP2PTransport) selfTransport() Transport {
	if t.fallbackUsed && t.fallbackTransport != nil {
		return t.fallbackTransport
	}
	return t
}

func (t *WANP2PTransport) Broadcast(msg *Message) error {
	if t.fallbackUsed {
		return t.fallbackTransport.Broadcast(msg)
	}
	data, _ := proto.Marshal(msg.Msg)
	t.mu.RLock()
	defer t.mu.RUnlock()
	for _, dc := range t.peers {
		dc.Send(data)
	}
	return nil
}

func (t *WANP2PTransport) PrivateSend(target string, msg *Message) error {
	if t.fallbackUsed {
		return t.fallbackTransport.PrivateSend(target, msg)
	}
	data, _ := proto.Marshal(msg.Msg)
	t.mu.RLock()
	dc, ok := t.peers[target]
	t.mu.RUnlock()
	if !ok {
		return fmt.Errorf("peer not found")
	}
	return dc.Send(data)
}

func (t *WANP2PTransport) Who() ([]Peer, error) {
	if t.fallbackUsed {
		return t.fallbackTransport.Who()
	}
	return nil, nil
}

func (t *WANP2PTransport) Rename(name string) error {
	t.nodeName = name
	return nil
}

func (t *WANP2PTransport) Recv() <-chan *Message    { return t.recv }
func (t *WANP2PTransport) SendErr() <-chan error     { return t.errCh }

func (t *WANP2PTransport) Stop() error {
	t.cancel()
	if t.sigClient != nil {
		t.sigClient.Close()
	}
	t.mu.RLock()
	defer t.mu.RUnlock()
	for _, pc := range t.peerConns {
		pc.Close()
	}
	if t.fallbackTransport != nil {
		t.fallbackTransport.Stop()
	}
	return nil
}

func (t *WANP2PTransport) IsFallback() bool { return t.fallbackUsed }

func (t *WANP2PTransport) SetOnFallback(fn func()) { t.onFallback = fn }
```

- [ ] **Step 3: Commit**

```bash
git add internal/signaling/client.go internal/transport/wan_p2p.go
git commit -m "feat: add WAN P2P transport with STUN + SignalingServer + fallback"
```

---

## Phase 2e: Circuit-Break Fallback

(Self-contained in WAN P2P Transport above — the `activateFallback()` method)

### Task 2e.1: Wire fallback notification UI

**Files:**
- Create: `internal/service/chat.go` (or modify client main)

- [ ] **Step 1: Placeholder for Phase 2g (wired in CLI)**

This task is implemented within Task 2d.3's `activateFallback()`. The user-visible notification will be wired in Phase 2g's CLI.

- [ ] **Step 2: Commit (no-op, fallback is in 2d.3)**

---

## Phase 2f: Burn Manager (Burn-After-Reading)

### Task 2f.1: Implement BurnManager

**Files:**
- Create: `internal/burn/manager.go`

- [ ] **Step 1: Write BurnManager**

```go
package burn

import (
	"sync"
	"time"
)

type BurnEntry struct {
	MessageID string
	BurnedAt  time.Time
	Duration  time.Duration
	timer     *time.Timer
}

type BurnManager struct {
	entries  map[string]*BurnEntry
	mu       sync.Mutex
	OnBurn   func(messageID string)
}

func New() *BurnManager {
	return &BurnManager{
		entries: make(map[string]*BurnEntry),
	}
}

func (m *BurnManager) Add(messageID string, seconds int32) {
	if seconds <= 0 {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()

	duration := time.Duration(seconds) * time.Second
	entry := &BurnEntry{
		MessageID: messageID,
		BurnedAt:  time.Now().Add(duration),
		Duration:  duration,
	}
	entry.timer = time.AfterFunc(duration, func() {
		m.mu.Lock()
		delete(m.entries, messageID)
		m.mu.Unlock()
		if m.OnBurn != nil {
			m.OnBurn(messageID)
		}
	})
	m.entries[messageID] = entry
}

func (m *BurnManager) Cancel(messageID string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if entry, ok := m.entries[messageID]; ok {
		entry.timer.Stop()
		delete(m.entries, messageID)
	}
}

func (m *BurnManager) IsBurned(messageID string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	_, exists := m.entries[messageID]
	return !exists
}

func (m *BurnManager) Remaining(messageID string) time.Duration {
	m.mu.Lock()
	defer m.mu.Unlock()
	if entry, ok := m.entries[messageID]; ok {
		return time.Until(entry.BurnedAt)
	}
	return 0
}
```

- [ ] **Step 2: Commit**

```bash
git add internal/burn/manager.go
git commit -m "feat: add BurnManager for burn-after-reading messages"
```

---

## Phase 2g: Unified CLI + Mode Switch

### Task 2g.1: Implement ChatService (unified logic)

**Files:**
- Create: `internal/service/chat.go`

- [ ] **Step 1: Write ChatService**

```go
package service

import (
	"context"
	"fmt"
	"os"
	"strings"

	"github.com/google/uuid"
	"go.uber.org/zap"
	"golang-im-system/internal/burn"
	"golang-im-system/internal/proto"
	"golang-im-system/internal/transport"
)

type Mode string

const (
	ModeServer Mode = "server"
	ModeLANP2P Mode = "lan_p2p"
	ModeWANP2P Mode = "wan_p2p"
)

type ChatService struct {
	transport   transport.Transport
	burnManager *burn.BurnManager
	logger      *zap.Logger
	messages    map[string]*proto.WsMessage // messageID -> message (for burn lookup)
	ctx         context.Context
	cancel      context.CancelFunc
}

func New(transport transport.Transport) *ChatService {
	logger, _ := zap.NewDevelopment()
	svc := &ChatService{
		transport:   transport,
		burnManager: burn.New(),
		logger:      logger,
		messages:    make(map[string]*proto.WsMessage),
	}
	svc.burnManager.OnBurn = svc.onBurn
	return svc
}

func (s *ChatService) Start(ctx context.Context) error {
	s.ctx, s.cancel = context.WithCancel(ctx)
	if err := s.transport.Start(ctx); err != nil {
		return err
	}
	go s.recvLoop()
	return nil
}

func (s *ChatService) recvLoop() {
	for {
		select {
		case msg, ok := <-s.transport.Recv():
			if !ok {
				return
			}
			s.handleIncoming(msg)
		case err, ok := <-s.transport.SendErr():
			if ok {
				s.logger.Error("transport error", zap.Error(err))
			}
		case <-s.ctx.Done():
			return
		}
	}
}

func (s *ChatService) handleIncoming(msg *transport.Message) {
	wsm := msg.Msg
	if chat := wsm.GetChat(); chat != nil && chat.BurnSeconds > 0 {
		s.burnManager.Add(wsm.MessageId, chat.BurnSeconds)
		s.messages[wsm.MessageId] = wsm
	}

	switch wsm.Type {
	case proto.MsgType_CHAT, proto.MsgType_PRIVATE_CHAT:
		chat := wsm.GetChat()
		burnHint := ""
		if chat.BurnSeconds > 0 {
			burnHint = fmt.Sprintf(" [burns in %ds]", chat.BurnSeconds)
		}
		if wsm.Type == proto.MsgType_PRIVATE_CHAT {
			fmt.Printf("\r[PM from %s] %s%s\n", chat.From, chat.Content, burnHint)
		} else {
			fmt.Printf("\r[%s] %s%s\n", chat.From, chat.Content, burnHint)
		}
	case proto.MsgType_SYSTEM:
		sys := wsm.GetSystem()
		switch sys.Type {
		case proto.SystemType_JOIN:
			fmt.Printf("\r>>> %s joined <<<\n", sys.Content)
		case proto.SystemType_LEAVE:
			fmt.Printf("\r>>> %s left <<<\n", sys.Content)
		case proto.SystemType_RENAME:
			fmt.Printf("\r>>> %s renamed to %s <<<\n", sys.User, sys.Content)
		default:
			fmt.Printf("\r[system] %s\n", sys.Content)
		}
	case proto.MsgType_WHO:
		fmt.Printf("\r[system] %s\n", wsm.GetSystem().Content)
	}
	fmt.Print("> ")
}

func (s *ChatService) onBurn(messageID string) {
	fmt.Printf("\r[system] message %s... has been burned\n", messageID[:8])
	fmt.Print("> ")
}

func (s *ChatService) Send(msg string) error {
	wsm := &proto.WsMessage{
		MessageId: uuid.New().String(),
		Timestamp: 0,
		Type:      proto.MsgType_CHAT,
		Payload: &proto.WsMessage_Chat{
			Chat: &proto.ChatMessage{Content: msg},
		},
	}
	return s.transport.Broadcast(&transport.Message{Msg: wsm})
}

func (s *ChatService) PrivateSend(target, msg string) error {
	wsm := &proto.WsMessage{
		MessageId: uuid.New().String(),
		Type:      proto.MsgType_PRIVATE_CHAT,
		Payload: &proto.WsMessage_Chat{
			Chat: &proto.ChatMessage{Content: msg, To: target},
		},
	}
	return s.transport.PrivateSend(target, &transport.Message{Msg: wsm})
}

func (s *ChatService) SendBurn(msg string, seconds int32) error {
	wsm := &proto.WsMessage{
		MessageId: uuid.New().String(),
		Type:      proto.MsgType_CHAT,
		Payload: &proto.WsMessage_Chat{
			Chat: &proto.ChatMessage{Content: msg, BurnSeconds: seconds},
		},
	}
	s.burnManager.Add(wsm.MessageId, seconds)
	s.messages[wsm.MessageId] = wsm
	return s.transport.Broadcast(&transport.Message{Msg: wsm})
}

func (s *ChatService) Rename(name string) error {
	return s.transport.Rename(name)
}

func (s *ChatService) Stop() error {
	s.cancel()
	return s.transport.Stop()
}
```

- [ ] **Step 2: Commit**

```bash
git add internal/service/chat.go
git commit -m "feat: add ChatService with unified message handling and burn integration"
```

---

### Task 2g.2: Implement unified CLI client

**Files:**
- Create: `cmd/client/main.go`

- [ ] **Step 1: Write CLI client**

```go
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"strings"

	"golang-im-system/internal/service"
	"golang-im-system/internal/transport"
)

var (
	mode      = flag.String("mode", "server", "server|lan_p2p|wan_p2p")
	server    = flag.String("server", "127.0.0.1:8888", "ChatServer address")
	signaling = flag.String("signaling", "127.0.0.1:7000", "SignalingServer address")
	name      = flag.String("name", "", "display name")
	port      = flag.Int("port", 0, "local WS port for P2P modes")
)

func main() {
	flag.Parse()

	name := *name
	if name == "" {
		fmt.Print("Enter your name: ")
		fmt.Scanln(&name)
	}

	var t transport.Transport
	var modeLabel string

	switch *mode {
	case "server", "client":
		t = transport.NewServerTransport(*server)
		modeLabel = "Server (centralized)"
	case "lan_p2p":
		p := *port
		if p == 0 {
			p = 9000
		}
		t = transport.NewLANP2PTransport(name, p)
		modeLabel = fmt.Sprintf("LAN P2P (port %d)", p)
	case "wan_p2p":
		wan := transport.NewWANP2PTransport(name, *signaling, *server)
		wan.SetOnFallback(func() {
			fmt.Println("\n>>> P2P failed, falling back to server mode <<<")
			fmt.Print("> ")
		})
		t = wan
		modeLabel = "WAN P2P (with fallback)"
	default:
		fmt.Fprintf(os.Stderr, "unknown mode: %s\n", *mode)
		os.Exit(1)
	}

	svc := service.New(t)

	ctx := context.Background()
	if err := svc.Start(ctx); err != nil {
		fmt.Fprintf(os.Stderr, "failed to start: %v\n", err)
		os.Exit(1)
	}
	defer svc.Stop()

	fmt.Printf("=== IM Chat (%s) ===\n", modeLabel)
	fmt.Println("Commands:")
	fmt.Println("  /who          — list online peers")
	fmt.Println("  /rename <n>   — change name")
	fmt.Println("  /pm <name> <msg> — private message")
	fmt.Println("  /burn <s> <msg>   — send burn-after-reading (s seconds)")
	fmt.Println("  /quit         — exit")
	fmt.Println()

	go func() {
		for {
			var input string
			fmt.Print("> ")
			fmt.Scanln(&input)

			if input == "" {
				continue
			}

			switch {
			case input == "/quit":
				svc.Stop()
				os.Exit(0)
			case input == "/who":
				peers, _ := t.Who()
				fmt.Println("Online:")
				for _, p := range peers {
					fmt.Printf("  - %s (%s)\n", p.Name, p.ID)
				}
			case strings.HasPrefix(input, "/rename "):
				newName := strings.TrimPrefix(input, "/rename ")
				svc.Rename(newName)
				fmt.Printf("Renamed to %s\n", newName)
			case strings.HasPrefix(input, "/pm "):
				parts := strings.SplitN(strings.TrimPrefix(input, "/pm "), " ", 2)
				if len(parts) >= 2 {
					svc.PrivateSend(parts[0], parts[1])
				} else {
					fmt.Println("Usage: /pm <name> <message>")
				}
			case strings.HasPrefix(input, "/burn "):
				parts := strings.SplitN(strings.TrimPrefix(input, "/burn "), " ", 2)
				if len(parts) >= 2 {
					var seconds int32
					fmt.Sscanf(parts[0], "%d", &seconds)
					svc.SendBurn(parts[1], seconds)
				} else {
					fmt.Println("Usage: /burn <seconds> <message>")
				}
			default:
				svc.Send(input)
			}
		}
	}()

	<-ctx.Done()
}
```

- [ ] **Step 2: Commit**

```bash
git add cmd/client/main.go
git commit -m "feat: add unified CLI client with 3-mode support"
```

---

### Task 2g.3: Final integration and cleanup

**Files:**
- Remove old files: `server/main.go`, `server/server.go`, `server/user.go`, `client/client.go`
- Remove old executables

- [ ] **Step 1: Clean up old files**

```bash
git rm server/main.go server/server.go server/user.go client/client.go
git rm server/server.exe client/client.exe 2>/dev/null
go mod tidy
```

- [ ] **Step 2: Verify build**

```bash
cd golang-im-system && go build ./cmd/server && go build ./cmd/signaling && go build ./cmd/client
```

- [ ] **Step 3: Commit**

```bash
git add -A
git commit -m "chore: remove old TCP code, finalize new project structure"
```

---

## Plan Summary

| Phase | Tasks | Files Created | Key Changes |
|-------|-------|---------------|-------------|
| 1 | 4 | 0 | Fix bugs in existing TCP code |
| 2a | 3 | 3 | Proto definition, Transport interface, go.mod |
| 2b | 2 | 2 | ChatServer WebSocket refactor, ServerTransport |
| 2c | 2 | 2 | Multicast discovery, LAN P2P transport |
| 2d | 3 | 4 | Signaling server, WAN P2P with STUN + fallback |
| 2e | 1 | 0 | Fallback wired into WAN P2P (no separate code) |
| 2f | 1 | 1 | BurnManager |
| 2g | 3 | 2 | ChatService + CLI client + cleanup |
| **Total** | **16** | **14** | **~800 LOC new code** |
