package auth

import (
	"fmt"
	"time"

	"github.com/golang-jwt/jwt/v5"
)

const defaultSecret = "im-system-secret-key-change-in-production"

// Claims carries the authenticated username.
type Claims struct {
	Username string `json:"username"`
	jwt.RegisteredClaims
}

// Manager handles JWT token generation and validation.
type Manager struct {
	secret []byte
	ttl    time.Duration
}

// New creates a JWT Manager.
func New(secret string, ttl time.Duration) *Manager {
	if secret == "" {
		secret = defaultSecret
	}
	if ttl == 0 {
		ttl = 24 * time.Hour
	}
	return &Manager{secret: []byte(secret), ttl: ttl}
}

// Generate creates a signed JWT for the given username.
func (m *Manager) Generate(username string) (string, error) {
	claims := Claims{
		Username: username,
		RegisteredClaims: jwt.RegisteredClaims{
			ExpiresAt: jwt.NewNumericDate(time.Now().Add(m.ttl)),
			IssuedAt:  jwt.NewNumericDate(time.Now()),
		},
	}
	token := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
	return token.SignedString(m.secret)
}

// Validate parses and validates a token string, returning the username.
func (m *Manager) Validate(tokenStr string) (string, error) {
	token, err := jwt.ParseWithClaims(tokenStr, &Claims{},
		func(t *jwt.Token) (interface{}, error) {
			if _, ok := t.Method.(*jwt.SigningMethodHMAC); !ok {
				return nil, fmt.Errorf("unexpected signing method: %v", t.Header["alg"])
			}
			return m.secret, nil
		})
	if err != nil {
		return "", fmt.Errorf("invalid token: %w", err)
	}
	claims, ok := token.Claims.(*Claims)
	if !ok || !token.Valid {
		return "", fmt.Errorf("invalid token claims")
	}
	return claims.Username, nil
}
