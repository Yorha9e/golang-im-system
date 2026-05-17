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
	PeerTimeout          = 20 * time.Second
)

// Announce is the JSON payload broadcast over multicast.
type Announce struct {
	NodeID   string `json:"node_id"`
	NodeName string `json:"node_name"`
	WSPort   int    `json:"ws_port"`
}

// DiscoveredPeer represents a peer found via multicast.
type DiscoveredPeer struct {
	Announce
	LastSeen time.Time
	Addr     net.IP
}

// MulticastDiscovery handles LAN peer discovery via UDP multicast.
type MulticastDiscovery struct {
	multicastAddr string
	conn          *net.UDPConn
	localNode     Announce

	mu      sync.RWMutex
	peers   map[string]*DiscoveredPeer
	onJoin  func(DiscoveredPeer)
	onLeave func(string) // nodeID
}

// NewMulticastDiscovery creates a discovery instance.
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

// SetCallbacks registers join/leave callbacks for peer discovery events.
func (d *MulticastDiscovery) SetCallbacks(onJoin func(DiscoveredPeer), onLeave func(string)) {
	d.onJoin = onJoin
	d.onLeave = onLeave
}

// SetLocalName updates the displayed name in announce packets.
func (d *MulticastDiscovery) SetLocalName(name string) {
	d.localNode.NodeName = name
}

// Start begins listening and announcing on the multicast group.
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
	buf := make([]byte, 2048)
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
		if existing, ok := d.peers[a.NodeID]; ok {
			existing.LastSeen = time.Now()
			existing.NodeName = a.NodeName
		} else {
			peer := &DiscoveredPeer{
				Announce: a,
				LastSeen: time.Now(),
				Addr:     remote.IP,
			}
			d.peers[a.NodeID] = peer
			d.mu.Unlock()
			if d.onJoin != nil {
				d.onJoin(*peer)
			}
			continue
		}
		d.mu.Unlock()
	}
}

func (d *MulticastDiscovery) announce(ctx context.Context) {
	ticker := time.NewTicker(AnnounceInterval)
	defer ticker.Stop()
	data, _ := json.Marshal(d.localNode)
	addr, _ := net.ResolveUDPAddr("udp", d.multicastAddr)

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			d.conn.WriteToUDP(data, addr)
		}
	}
}

func (d *MulticastDiscovery) prune(ctx context.Context) {
	ticker := time.NewTicker(10 * time.Second)
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

// Peers returns all currently discovered peers.
func (d *MulticastDiscovery) Peers() []DiscoveredPeer {
	d.mu.RLock()
	defer d.mu.RUnlock()
	result := make([]DiscoveredPeer, 0, len(d.peers))
	for _, p := range d.peers {
		result = append(result, *p)
	}
	return result
}

// Stop shuts down discovery.
func (d *MulticastDiscovery) Stop() error {
	if d.conn != nil {
		return d.conn.Close()
	}
	return nil
}
