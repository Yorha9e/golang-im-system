package ratelimit

import (
	"sync"
	"time"
)

// TokenBucket implements a token-bucket rate limiter.
type TokenBucket struct {
	rate       float64 // tokens per second
	burst      int     // max tokens
	tokens     float64
	lastRefill time.Time
	mu         sync.Mutex
}

// New creates a TokenBucket with the given rate and burst size.
func New(ratePerSec float64, burst int) *TokenBucket {
	return &TokenBucket{
		rate:       ratePerSec,
		burst:      burst,
		tokens:     float64(burst),
		lastRefill: time.Now(),
	}
}

// Allow returns true if a token is available and consumes it.
func (b *TokenBucket) Allow() bool {
	b.mu.Lock()
	defer b.mu.Unlock()

	now := time.Now()
	elapsed := now.Sub(b.lastRefill).Seconds()
	b.tokens += elapsed * b.rate
	if b.tokens > float64(b.burst) {
		b.tokens = float64(b.burst)
	}
	b.lastRefill = now

	if b.tokens >= 1.0 {
		b.tokens--
		return true
	}
	return false
}

// Limiter manages per-key rate limiters.
type Limiter struct {
	buckets map[string]*TokenBucket
	mu      sync.Mutex
	rate    float64
	burst   int
}

// NewLimiter creates a rate limiter manager.
func NewLimiter(ratePerSec float64, burst int) *Limiter {
	return &Limiter{
		buckets: make(map[string]*TokenBucket),
		rate:    ratePerSec,
		burst:   burst,
	}
}

// Allow checks if the given key is allowed. Creates a new bucket if needed.
func (l *Limiter) Allow(key string) bool {
	l.mu.Lock()
	b, ok := l.buckets[key]
	if !ok {
		b = New(l.rate, l.burst)
		l.buckets[key] = b
	}
	l.mu.Unlock()
	return b.Allow()
}

// Remove cleans up a key (call on disconnect).
func (l *Limiter) Remove(key string) {
	l.mu.Lock()
	delete(l.buckets, key)
	l.mu.Unlock()
}
