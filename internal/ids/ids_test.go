package ids

import (
	"errors"
	"sync"
	"testing"
	"time"
)

// fixedClock returns a clock the test can move.
func fixedClock(t time.Time) (func() time.Time, *time.Time) {
	cur := t
	return func() time.Time { return cur }, &cur
}

func TestNextIsStrictlyIncreasingWithinOneMillisecond(t *testing.T) {
	now, _ := fixedClock(Epoch.Add(time.Hour))
	g := New(now)
	prev, _ := g.Next()
	for range 10_000 { // more than the 4096 sequence numbers in one millisecond
		id, err := g.Next()
		if err != nil {
			t.Fatal(err)
		}
		if id <= prev {
			t.Fatalf("id %d not above %d", id, prev)
		}
		prev = id
	}
}

func TestClockGoingBackwardsKeepsOrder(t *testing.T) {
	now, cur := fixedClock(Epoch.Add(time.Hour))
	g := New(now)
	a, _ := g.Next()
	*cur = cur.Add(-time.Minute)
	b, err := g.Next()
	if err != nil || b <= a {
		t.Fatalf("after clock step back: %d then %d (%v)", a, b, err)
	}
}

func TestIDsFitInFloat64Exactly(t *testing.T) {
	now, _ := fixedClock(Epoch.Add(60 * 365 * 24 * time.Hour)) // 60 years after the epoch
	id, err := New(now).Next()
	if err != nil {
		t.Fatal(err)
	}
	if id > MaxID || uint64(float64(id)) != id {
		t.Fatalf("id %d does not round-trip through float64", id)
	}
	if got := Time(id); got.Sub(now()).Abs() > time.Millisecond {
		t.Fatalf("Time(id) = %v, want %v", got, now())
	}
}

func TestClockOutOfRangeFails(t *testing.T) {
	before, _ := fixedClock(Epoch.Add(-time.Second))
	if _, err := New(before).Next(); !errors.Is(err, ErrClock) {
		t.Fatalf("before epoch: %v", err)
	}
	after, _ := fixedClock(Epoch.Add(80 * 365 * 24 * time.Hour))
	if _, err := New(after).Next(); !errors.Is(err, ErrClock) {
		t.Fatalf("past 41 bits: %v", err)
	}
}

func TestConcurrentCallersGetUniqueIDs(t *testing.T) {
	g := New(nil)
	var mu sync.Mutex
	seen := make(map[uint64]bool)
	var wg sync.WaitGroup
	for range 8 {
		wg.Go(func() {
			for range 2000 {
				id, err := g.Next()
				if err != nil {
					t.Error(err)
					return
				}
				mu.Lock()
				if seen[id] {
					t.Errorf("duplicate id %d", id)
				}
				seen[id] = true
				mu.Unlock()
			}
		})
	}
	wg.Wait()
}
