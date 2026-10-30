package bus

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// newBus returns a memory bus or fails the test.
func newBus(t *testing.T, parts int) *Memory {
	t.Helper()
	b, err := NewMemory(parts)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

// consumeUntilIdle runs Consume until the group has no lag, then stops it.
func consumeUntilIdle(t *testing.T, b *Memory, topic, group string, workers int, h Handler) error {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	errc := make(chan error, 1)
	go func() { errc <- b.Consume(ctx, topic, group, workers, h) }()
	idle := make(chan error, 1)
	go func() { idle <- b.WaitIdle(ctx, topic, group) }()
	select {
	case err := <-errc:
		return err
	case err := <-idle:
		if err != nil {
			t.Fatal(err)
		}
		cancel()
		return <-errc
	}
}

func TestPerKeyOrderAcrossWorkers(t *testing.T) {
	b := newBus(t, 8)
	ctx := context.Background()
	for i := range 1000 {
		key := fmt.Sprintf("k%d", i%10)
		if err := b.Publish(ctx, "t", []byte(key), []byte(fmt.Sprint(i))); err != nil {
			t.Fatal(err)
		}
	}
	var mu sync.Mutex
	last := map[string]int{}
	count := 0
	err := consumeUntilIdle(t, b, "t", "g", 3, func(_ context.Context, m Message) error {
		var v int
		fmt.Sscan(string(m.Value), &v)
		mu.Lock()
		defer mu.Unlock()
		if prev, ok := last[string(m.Key)]; ok && v <= prev {
			return fmt.Errorf("key %s: %d after %d", m.Key, v, prev)
		}
		last[string(m.Key)] = v
		count++
		return nil
	})
	if err != nil || count != 1000 {
		t.Fatalf("err=%v count=%d", err, count)
	}
}

func TestHandlerErrorRedeliversUncommitted(t *testing.T) {
	b := newBus(t, 1)
	ctx := context.Background()
	for i := range 5 {
		_ = b.Publish(ctx, "t", nil, []byte{byte(i)})
	}
	boom := errors.New("boom")
	var seen []byte
	err := consumeUntilIdle(t, b, "t", "g", 1, func(_ context.Context, m Message) error {
		if m.Value[0] == 3 {
			return boom
		}
		seen = append(seen, m.Value[0])
		return nil
	})
	if !errors.Is(err, boom) {
		t.Fatalf("Consume returned %v, want boom", err)
	}
	if b.Lag("t", "g") != 2 {
		t.Fatalf("lag %d after failure at offset 3, want 2", b.Lag("t", "g"))
	}
	if err := consumeUntilIdle(t, b, "t", "g", 1, func(_ context.Context, m Message) error {
		seen = append(seen, m.Value[0])
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if string(seen) != string([]byte{0, 1, 2, 3, 4}) {
		t.Fatalf("delivered %v, want 0..4 with 3 redelivered after restart", seen)
	}
}

func TestGroupsAreIndependent(t *testing.T) {
	b := newBus(t, 4)
	for i := range 50 {
		_ = b.Publish(context.Background(), "t", []byte{byte(i)}, nil)
	}
	var a, c atomic.Int64
	_ = consumeUntilIdle(t, b, "t", "a", 2, func(context.Context, Message) error { a.Add(1); return nil })
	_ = consumeUntilIdle(t, b, "t", "c", 5, func(context.Context, Message) error { c.Add(1); return nil })
	if a.Load() != 50 || c.Load() != 50 {
		t.Fatalf("group a got %d, group c got %d, want 50 each", a.Load(), c.Load())
	}
}

func TestConsumeWakesOnLatePublishAndStopsOnCancel(t *testing.T) {
	b := newBus(t, 2)
	ctx, cancel := context.WithCancel(context.Background())
	got := make(chan struct{}, 1)
	done := make(chan error, 1)
	go func() {
		done <- b.Consume(ctx, "t", "g", 2, func(context.Context, Message) error { got <- struct{}{}; return nil })
	}()
	time.Sleep(20 * time.Millisecond)
	_ = b.Publish(context.Background(), "t", []byte("x"), []byte("late"))
	select {
	case <-got:
	case <-time.After(5 * time.Second):
		t.Fatal("late publish never delivered")
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatalf("Consume after cancel: %v", err)
	}
}

func TestSecondConsumerOfGroupRejected(t *testing.T) {
	b := newBus(t, 1)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- b.Consume(ctx, "t", "g", 1, func(context.Context, Message) error { return nil }) }()
	g := b.topic("t").group("g")
	for deadline := time.Now().Add(5 * time.Second); !g.active.Load(); time.Sleep(time.Millisecond) {
		if time.Now().After(deadline) {
			t.Fatal("first consumer never started")
		}
	}
	if err := b.Consume(ctx, "t", "g", 1, func(context.Context, Message) error { return nil }); !errors.Is(err, ErrGroupBusy) {
		t.Fatalf("second consumer not rejected: %v", err)
	}
	cancel()
	<-done
}

func TestInvalidArguments(t *testing.T) {
	if _, err := NewMemory(0); !errors.Is(err, ErrArgs) {
		t.Errorf("NewMemory(0): %v", err)
	}
	b := newBus(t, 1)
	h := func(context.Context, Message) error { return nil }
	ctx := context.Background()
	for name, err := range map[string]error{
		"empty topic": b.Consume(ctx, "", "g", 1, h),
		"empty group": b.Consume(ctx, "t", "", 1, h),
		"no workers":  b.Consume(ctx, "t", "g", 0, h),
		"nil handler": b.Consume(ctx, "t", "g", 1, nil),
		"publish":     b.Publish(ctx, "", nil, nil),
	} {
		if !errors.Is(err, ErrArgs) {
			t.Errorf("%s: %v", name, err)
		}
	}
	if _, err := NewKafka(nil); !errors.Is(err, ErrArgs) {
		t.Errorf("NewKafka(nil): %v", err)
	}
}

func TestPublishCopiesBuffers(t *testing.T) {
	b := newBus(t, 1)
	buf := []byte("abc")
	_ = b.Publish(context.Background(), "t", buf, buf)
	buf[0] = 'z'
	_ = consumeUntilIdle(t, b, "t", "g", 1, func(_ context.Context, m Message) error {
		if string(m.Value) != "abc" || string(m.Key) != "abc" {
			t.Errorf("stored record changed with caller buffer: %q", m.Value)
		}
		return nil
	})
}
