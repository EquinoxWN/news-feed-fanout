// Package post stores each post exactly once; timelines hold only post IDs.
package post

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"
	"unicode/utf8"
)

// MaxBodyBytes bounds the size of a post body.
const MaxBodyBytes = 1024

var (
	// ErrNotFound is returned for an unknown post ID.
	ErrNotFound = errors.New("post: not found")
	// ErrExists is returned when a post ID is stored a second time.
	ErrExists = errors.New("post: already stored")
	// ErrInvalid is returned for a body that fails validation.
	ErrInvalid = errors.New("post: invalid body")
)

// Post is one stored post.
type Post struct {
	ID        uint64
	Author    uint32
	Body      string
	CreatedAt time.Time
}

// Store keeps posts by ID.
type Store interface {
	Put(ctx context.Context, p Post) error
	Get(ctx context.Context, id uint64) (Post, error)
	GetMany(ctx context.Context, ids []uint64) ([]Post, error)
}

// Validate checks that a body is non-empty, valid UTF-8 and within MaxBodyBytes.
func Validate(body string) error {
	switch {
	case body == "":
		return fmt.Errorf("%w: empty", ErrInvalid)
	case len(body) > MaxBodyBytes:
		return fmt.Errorf("%w: %d bytes, limit %d", ErrInvalid, len(body), MaxBodyBytes)
	case !utf8.ValidString(body):
		return fmt.Errorf("%w: not valid UTF-8", ErrInvalid)
	}
	return nil
}

// MemStore is an in-process Store guarded by a read-write lock.
type MemStore struct {
	mu    sync.RWMutex
	posts map[uint64]Post
	bytes int64
}

// NewMemStore returns an empty in-memory store.
func NewMemStore() *MemStore {
	return &MemStore{posts: make(map[uint64]Post)}
}

// Put stores p once; a second Put with the same ID fails with ErrExists.
func (s *MemStore) Put(_ context.Context, p Post) error {
	if err := Validate(p.Body); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.posts[p.ID]; ok {
		return fmt.Errorf("%w: %d", ErrExists, p.ID)
	}
	s.posts[p.ID] = p
	s.bytes += int64(len(p.Body))
	return nil
}

// Get returns the post with the given ID.
func (s *MemStore) Get(_ context.Context, id uint64) (Post, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	p, ok := s.posts[id]
	if !ok {
		return Post{}, fmt.Errorf("%w: %d", ErrNotFound, id)
	}
	return p, nil
}

// GetMany returns the posts that exist, in the order of ids, skipping unknown IDs.
func (s *MemStore) GetMany(_ context.Context, ids []uint64) ([]Post, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]Post, 0, len(ids))
	for _, id := range ids {
		if p, ok := s.posts[id]; ok {
			out = append(out, p)
		}
	}
	return out, nil
}

// Len is the number of stored posts.
func (s *MemStore) Len() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.posts)
}

// BodyBytes is the total size of all stored bodies.
func (s *MemStore) BodyBytes() int64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.bytes
}
