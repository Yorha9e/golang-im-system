package store

import (
	"time"

	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

// User represents a registered user.
type User struct {
	ID        uint   `gorm:"primaryKey"`
	Username  string `gorm:"uniqueIndex;size:64"`
	CreatedAt time.Time
}

// Message represents a stored chat message.
type Message struct {
	ID          uint   `gorm:"primaryKey"`
	FromUser    string `gorm:"index;size:64"`
	ToUser      string `gorm:"index;size:64"` // empty = broadcast
	Content     string `gorm:"size:4096"`
	BurnSeconds int32
	BurnedAt    *time.Time
	CreatedAt   time.Time `gorm:"index"`
}

// Store wraps the database connection.
type Store struct {
	db *gorm.DB
}

// New creates a Store with SQLite backend.
// Use ":memory:" for volatile in-memory DB, or a file path for persistence.
func New(dbPath string) (*Store, error) {
	dsn := dbPath
	if dbPath == ":memory:" {
		dsn = "file::memory:?cache=shared"
	}
	db, err := gorm.Open(sqlite.Open(dsn), &gorm.Config{})
	if err != nil {
		return nil, err
	}
	// SQLite only supports a single writer; use a single connection to avoid table-not-found.
	sqlDB, _ := db.DB()
	if sqlDB != nil {
		sqlDB.SetMaxOpenConns(1)
	}
	if err := db.AutoMigrate(&User{}, &Message{}); err != nil {
		return nil, err
	}
	return &Store{db: db}, nil
}

// EnsureUser creates the user if not exists. Returns the username.
func (s *Store) EnsureUser(username string) error {
	return s.db.Where(User{Username: username}).
		FirstOrCreate(&User{Username: username}).Error
}

// SaveMessage persists a chat message.
func (s *Store) SaveMessage(from, to, content string, burnSeconds int32) (*Message, error) {
	msg := &Message{
		FromUser:    from,
		ToUser:      to,
		Content:     content,
		BurnSeconds: burnSeconds,
	}
	return msg, s.db.Create(msg).Error
}

// RecentMessages returns the last `limit` broadcast and PM messages.
func (s *Store) RecentMessages(limit int) ([]Message, error) {
	var msgs []Message
	err := s.db.Order("created_at DESC").Limit(limit).
		Find(&msgs).Error
	// Reverse to chronological order.
	for i, j := 0, len(msgs)-1; i < j; i, j = i+1, j-1 {
		msgs[i], msgs[j] = msgs[j], msgs[i]
	}
	return msgs, err
}

// MarkBurned sets the burned timestamp on a message.
func (s *Store) MarkBurned(msgID uint) error {
	now := time.Now()
	return s.db.Model(&Message{}).Where("id = ?", msgID).
		Update("burned_at", now).Error
}

// DB returns the underlying GORM DB for advanced queries.
func (s *Store) DB() *gorm.DB {
	return s.db
}

// Close shuts down the database connection.
func (s *Store) Close() error {
	sqlDB, _ := s.db.DB()
	if sqlDB != nil {
		return sqlDB.Close()
	}
	return nil
}
