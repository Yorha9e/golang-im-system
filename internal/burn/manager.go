package burn

import (
	"sync"
	"time"
)

// Entry tracks a single burn-after-reading message.
type Entry struct {
	MessageID string
	BurnedAt  time.Time
	Duration  time.Duration
	timer     *time.Timer
}

// Manager handles burn-after-reading message lifecycle.
type Manager struct {
	entries map[string]*Entry
	mu      sync.Mutex
	OnBurn  func(messageID string)
}

// New creates a BurnManager.
func New() *Manager {
	return &Manager{
		entries: make(map[string]*Entry),
	}
}

// Add schedules a message for automatic deletion after `seconds`.
func (m *Manager) Add(messageID string, seconds int32) {
	if seconds <= 0 {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()

	duration := time.Duration(seconds) * time.Second
	entry := &Entry{
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

// Cancel prevents a scheduled burn from firing.
func (m *Manager) Cancel(messageID string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if entry, ok := m.entries[messageID]; ok {
		entry.timer.Stop()
		delete(m.entries, messageID)
	}
}

// IsBurned returns true if the message has already been burned.
func (m *Manager) IsBurned(messageID string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	_, exists := m.entries[messageID]
	return !exists // Not in the map = already burned
}

// HasBurned returns true if the message was ever tracked.
// A message with burn_seconds=0 was never added, so this returns false for normal messages.
func (m *Manager) HasBurned(messageID string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	_, exists := m.entries[messageID]
	return !exists
}

// Remaining returns the time until burn. Returns 0 if not found or already burned.
func (m *Manager) Remaining(messageID string) time.Duration {
	m.mu.Lock()
	defer m.mu.Unlock()
	if entry, ok := m.entries[messageID]; ok {
		return time.Until(entry.BurnedAt)
	}
	return 0
}

// Count returns the number of unburned messages being tracked.
func (m *Manager) Count() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.entries)
}
