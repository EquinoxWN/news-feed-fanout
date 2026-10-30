package timeline

import (
	"context"
	"errors"
	"math/rand/v2"
	"slices"
	"sync"
	"testing"
)

// newMem returns a memory store or fails the test.
func newMem(t *testing.T, users, maxLen int) *Memory {
	t.Helper()
	m, err := NewMemory(users, maxLen)
	if err != nil {
		t.Fatal(err)
	}
	return m
}

func TestLatestNewestFirstAndIdempotent(t *testing.T) {
	ctx := context.Background()
	m := newMem(t, 3, 10)
	for _, id := range []uint64{5, 1, 9, 5, 3, 9} {
		if err := m.AddBatch(ctx, []uint32{0, 2}, id); err != nil {
			t.Fatal(err)
		}
	}
	got, _ := m.Latest(ctx, 0, 10)
	if !slices.Equal(got, []uint64{9, 5, 3, 1}) {
		t.Fatalf("Latest = %v", got)
	}
	if got, _ := m.Latest(ctx, 2, 2); !slices.Equal(got, []uint64{9, 5}) {
		t.Fatalf("Latest(2) = %v", got)
	}
	if got, _ := m.Latest(ctx, 1, 5); len(got) != 0 {
		t.Fatalf("untouched user has %v", got)
	}
	if m.Entries() != 8 {
		t.Fatalf("Entries = %d, want 8 (duplicates ignored)", m.Entries())
	}
}

func TestTrimKeepsNewestMaxLenRandomized(t *testing.T) {
	ctx := context.Background()
	r := rand.New(rand.NewPCG(1, 2))
	for trial := range 50 {
		maxLen := 1 + r.IntN(20)
		m := newMem(t, 1, maxLen)
		seen := map[uint64]bool{}
		for range r.IntN(100) {
			id := uint64(r.IntN(200))
			seen[id] = true
			_ = m.AddBatch(ctx, []uint32{0}, id)
		}
		var want []uint64
		for id := range seen {
			want = append(want, id)
		}
		slices.Sort(want)
		slices.Reverse(want)
		want = want[:min(maxLen, len(want))]
		got, _ := m.Latest(ctx, 0, maxLen+5)
		if !slices.Equal(got, want) {
			t.Fatalf("trial %d maxLen %d: got %v want %v", trial, maxLen, got, want)
		}
	}
}

func TestCountMissing(t *testing.T) {
	ctx := context.Background()
	m := newMem(t, 5, 4)
	_ = m.AddBatch(ctx, []uint32{0, 1, 2}, 7)
	if n, _ := m.CountMissing(ctx, []uint32{0, 1, 2, 3, 4}, 7); n != 2 {
		t.Fatalf("missing %d, want 2", n)
	}
}

func TestBadArguments(t *testing.T) {
	ctx := context.Background()
	if _, err := NewMemory(0, 1); !errors.Is(err, ErrArgs) {
		t.Errorf("NewMemory(0,1): %v", err)
	}
	m := newMem(t, 2, 2)
	if err := m.AddBatch(ctx, []uint32{0, 2}, 1); !errors.Is(err, ErrArgs) {
		t.Errorf("AddBatch out of range: %v", err)
	}
	if got, _ := m.Latest(ctx, 0, 5); len(got) != 0 {
		t.Errorf("partial write happened before validation failed: %v", got)
	}
	if _, err := m.Latest(ctx, 5, 1); !errors.Is(err, ErrArgs) {
		t.Errorf("Latest out of range: %v", err)
	}
	if _, err := m.Latest(ctx, 0, 0); !errors.Is(err, ErrArgs) {
		t.Errorf("Latest n=0: %v", err)
	}
	if _, err := m.CountMissing(ctx, []uint32{9}, 1); !errors.Is(err, ErrArgs) {
		t.Errorf("CountMissing out of range: %v", err)
	}
	if _, err := NewValkey("", 1); !errors.Is(err, ErrArgs) {
		t.Errorf("NewValkey empty addr: %v", err)
	}
}

func TestConcurrentWriters(t *testing.T) {
	ctx := context.Background()
	m := newMem(t, 100, 1000)
	users := make([]uint32, 100)
	for i := range users {
		users[i] = uint32(i)
	}
	var wg sync.WaitGroup
	for w := range 8 {
		wg.Go(func() {
			for i := range 50 {
				_ = m.AddBatch(ctx, users, uint64(w*1000+i))
			}
		})
	}
	wg.Wait()
	if m.Entries() != 100*8*50 {
		t.Fatalf("Entries = %d, want %d", m.Entries(), 100*8*50)
	}
}
