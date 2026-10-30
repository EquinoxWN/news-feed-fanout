// Package ids issues time-ordered post IDs that fit in 53 bits, so a
// float64 sorted-set score (Valkey, Redis) holds every ID exactly.
package ids

import (
	"errors"
	"sync"
	"time"
)

const (
	seqBits  = 12
	timeBits = 41
	maxSeq   = 1<<seqBits - 1
	maxMS    = 1<<timeBits - 1

	// MaxID is the largest ID the generator can issue (2^53 - 1).
	MaxID uint64 = 1<<(seqBits+timeBits) - 1
)

// Epoch is the zero point of the millisecond part of every ID.
var Epoch = time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

// ErrClock is returned when the clock is before Epoch or past the 41-bit range.
var ErrClock = errors.New("ids: clock outside the supported range")

// Generator hands out strictly increasing IDs: 41 bits of milliseconds since
// Epoch followed by a 12-bit sequence within the millisecond.
type Generator struct {
	mu     sync.Mutex
	now    func() time.Time
	lastMS int64
	seq    int64
}

// New returns a generator that reads time from now (time.Now when nil).
func New(now func() time.Time) *Generator {
	if now == nil {
		now = time.Now
	}
	return &Generator{now: now, lastMS: -1}
}

// Next returns the next ID; IDs never repeat or go backwards, even if the clock does.
func (g *Generator) Next() (uint64, error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	ms := g.now().Sub(Epoch).Milliseconds()
	if ms < 0 {
		return 0, ErrClock
	}
	if ms < g.lastMS {
		ms = g.lastMS
	}
	if ms == g.lastMS {
		g.seq++
		if g.seq > maxSeq {
			ms++
			g.seq = 0
		}
	} else {
		g.seq = 0
	}
	if ms > maxMS {
		return 0, ErrClock
	}
	g.lastMS = ms
	return uint64(ms)<<seqBits | uint64(g.seq), nil
}

// Time returns the millisecond timestamp encoded in id.
func Time(id uint64) time.Time {
	return Epoch.Add(time.Duration(id>>seqBits) * time.Millisecond)
}
