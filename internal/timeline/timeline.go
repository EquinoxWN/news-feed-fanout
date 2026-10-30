// Package timeline keeps each user's home timeline as a capped set of post
// IDs ordered by ID (IDs are time-ordered). Valkey stores it as a sorted set
// per user; Memory implements the same contract in-process.
//
// Adding the same post twice is a no-op, so at-least-once delivery from the
// bus never duplicates a timeline entry.
package timeline

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sync"
)

// DefaultMaxLen is the number of entries kept per timeline (older ones are trimmed).
const DefaultMaxLen = 800

// ErrArgs is returned for out-of-range users or limits.
var ErrArgs = errors.New("timeline: invalid arguments")

// Store holds home timelines.
type Store interface {
	// AddBatch inserts postID into the timeline of every user in users.
	AddBatch(ctx context.Context, users []uint32, postID uint64) error
	// Latest returns up to n post IDs of user, newest first.
	Latest(ctx context.Context, user uint32, n int) ([]uint64, error)
	// CountMissing returns how many of users do not have postID in their timeline.
	CountMissing(ctx context.Context, users []uint32, postID uint64) (int, error)
}

const shards = 256

// Memory is an in-process Store: one ascending slice per user, lock-striped by user.
type Memory struct {
	maxLen int
	locks  [shards]sync.Mutex
	lines  [][]uint64
}

// NewMemory returns a store for users 0..users-1 keeping maxLen entries each.
func NewMemory(users, maxLen int) (*Memory, error) {
	if users < 1 || maxLen < 1 {
		return nil, fmt.Errorf("%w: users %d maxLen %d", ErrArgs, users, maxLen)
	}
	return &Memory{maxLen: maxLen, lines: make([][]uint64, users)}, nil
}

// check validates a user ID.
func (m *Memory) check(u uint32) error {
	if int(u) >= len(m.lines) {
		return fmt.Errorf("%w: user %d", ErrArgs, u)
	}
	return nil
}

// AddBatch inserts postID into each user's timeline, keeping the newest maxLen entries.
func (m *Memory) AddBatch(ctx context.Context, users []uint32, postID uint64) error {
	for _, u := range users {
		if err := m.check(u); err != nil {
			return err
		}
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	for _, u := range users {
		mu := &m.locks[u%shards]
		mu.Lock()
		m.lines[u] = insert(m.lines[u], postID, m.maxLen)
		mu.Unlock()
	}
	return nil
}

// insert adds id to an ascending slice unless present, dropping the oldest beyond maxLen.
func insert(line []uint64, id uint64, maxLen int) []uint64 {
	i, found := slices.BinarySearch(line, id)
	if found {
		return line
	}
	if len(line) == maxLen {
		if i == 0 {
			return line // older than everything kept
		}
		copy(line, line[1:i])
		line[i-1] = id
		return line
	}
	return slices.Insert(line, i, id)
}

// Latest returns up to n newest post IDs of user.
func (m *Memory) Latest(_ context.Context, user uint32, n int) ([]uint64, error) {
	if err := m.check(user); err != nil {
		return nil, err
	}
	if n < 1 {
		return nil, fmt.Errorf("%w: n %d", ErrArgs, n)
	}
	mu := &m.locks[user%shards]
	mu.Lock()
	defer mu.Unlock()
	line := m.lines[user]
	out := make([]uint64, 0, min(n, len(line)))
	for i := len(line) - 1; i >= 0 && len(out) < n; i-- {
		out = append(out, line[i])
	}
	return out, nil
}

// CountMissing counts users whose timeline lacks postID.
func (m *Memory) CountMissing(_ context.Context, users []uint32, postID uint64) (int, error) {
	missing := 0
	for _, u := range users {
		if err := m.check(u); err != nil {
			return 0, err
		}
		mu := &m.locks[u%shards]
		mu.Lock()
		if _, ok := slices.BinarySearch(m.lines[u], postID); !ok {
			missing++
		}
		mu.Unlock()
	}
	return missing, nil
}

// Entries is the total number of timeline entries held.
func (m *Memory) Entries() int64 {
	var n int64
	for u := range m.lines {
		mu := &m.locks[u%shards]
		mu.Lock()
		n += int64(len(m.lines[u]))
		mu.Unlock()
	}
	return n
}
