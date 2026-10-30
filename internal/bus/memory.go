package bus

import (
	"context"
	"fmt"
	"hash/fnv"
	"sync"
	"sync/atomic"
	"time"
)

// Memory is an in-process Bus with a fixed number of partitions per topic.
// Partitions are assigned to group members statically (partition p goes to
// member p mod workers), and each member processes its partitions in order.
type Memory struct {
	partitions int
	mu         sync.Mutex
	topics     map[string]*topic
}

type topic struct {
	mu     sync.Mutex
	parts  [][]Message
	notify chan struct{} // closed and replaced on every publish
	groups map[string]*group
}

type group struct {
	active    atomic.Bool
	committed []atomic.Int64
}

// NewMemory returns a bus whose topics have the given number of partitions (1..1024).
func NewMemory(partitions int) (*Memory, error) {
	if partitions < 1 || partitions > 1024 {
		return nil, fmt.Errorf("%w: partitions %d", ErrArgs, partitions)
	}
	return &Memory{partitions: partitions, topics: make(map[string]*topic)}, nil
}

// topic returns the named topic, creating it on first use.
func (b *Memory) topic(name string) *topic {
	b.mu.Lock()
	defer b.mu.Unlock()
	t, ok := b.topics[name]
	if !ok {
		t = &topic{parts: make([][]Message, b.partitions), notify: make(chan struct{}), groups: make(map[string]*group)}
		b.topics[name] = t
	}
	return t
}

// group returns the named consumer group of t, creating it at offset 0.
func (t *topic) group(name string) *group {
	t.mu.Lock()
	defer t.mu.Unlock()
	g, ok := t.groups[name]
	if !ok {
		g = &group{committed: make([]atomic.Int64, len(t.parts))}
		t.groups[name] = g
	}
	return g
}

// Partition maps a key to a partition with FNV-1a (Kafka's default uses murmur2).
func (b *Memory) Partition(key []byte) int32 {
	h := fnv.New32a()
	_, _ = h.Write(key)
	return int32(h.Sum32() % uint32(b.partitions))
}

// Publish appends a copy of the record to its key's partition.
func (b *Memory) Publish(ctx context.Context, topicName string, key, value []byte) error {
	if topicName == "" {
		return ErrArgs
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	t := b.topic(topicName)
	p := b.Partition(key)
	t.mu.Lock()
	m := Message{Topic: topicName, Partition: p, Offset: int64(len(t.parts[p])), Key: clone(key), Value: clone(value)}
	t.parts[p] = append(t.parts[p], m)
	close(t.notify)
	t.notify = make(chan struct{})
	t.mu.Unlock()
	return nil
}

// clone copies b so callers may reuse their buffers.
func clone(b []byte) []byte { return append([]byte(nil), b...) }

// next returns the message at offset in partition p, if published.
func (t *topic) next(p int, offset int64) (Message, bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if offset < int64(len(t.parts[p])) {
		return t.parts[p][offset], true
	}
	return Message{}, false
}

// wake returns the channel closed by the next publish.
func (t *topic) wake() <-chan struct{} {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.notify
}

// Consume runs the group until ctx ends or a handler returns an error.
func (b *Memory) Consume(ctx context.Context, topicName, groupName string, workers int, h Handler) error {
	if err := checkArgs(topicName, groupName, workers, h); err != nil {
		return err
	}
	t := b.topic(topicName)
	g := t.group(groupName)
	if !g.active.CompareAndSwap(false, true) {
		return fmt.Errorf("%w: %s/%s", ErrGroupBusy, topicName, groupName)
	}
	defer g.active.Store(false)
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	errc := make(chan error, workers)
	for w := range workers {
		var mine []int
		for p := w; p < b.partitions; p += workers {
			mine = append(mine, p)
		}
		go func() { errc <- member(ctx, t, g, mine, h) }()
	}
	var first error
	for range workers {
		if err := <-errc; err != nil && first == nil {
			first = err
			cancel()
		}
	}
	return first
}

// member processes its partitions round-robin, committing each offset after the handler succeeds.
func member(ctx context.Context, t *topic, g *group, parts []int, h Handler) error {
	if len(parts) == 0 {
		<-ctx.Done()
		return nil
	}
	for {
		progressed := false
		wake := t.wake() // taken before the scan so a publish during it is not missed
		for _, p := range parts {
			off := g.committed[p].Load()
			m, ok := t.next(p, off)
			if !ok {
				continue
			}
			if ctx.Err() != nil {
				return nil
			}
			if err := h(ctx, m); err != nil {
				return fmt.Errorf("bus: %s partition %d offset %d: %w", m.Topic, p, off, err)
			}
			g.committed[p].Store(off + 1)
			progressed = true
		}
		if !progressed {
			select {
			case <-ctx.Done():
				return nil
			case <-wake:
			}
		}
	}
}

// Lag is the number of published records the group has not committed yet.
func (b *Memory) Lag(topicName, groupName string) int64 {
	t := b.topic(topicName)
	g := t.group(groupName)
	t.mu.Lock()
	defer t.mu.Unlock()
	var lag int64
	for p := range t.parts {
		lag += int64(len(t.parts[p])) - g.committed[p].Load()
	}
	return lag
}

// WaitIdle blocks until the group's lag on topic is zero or ctx ends.
func (b *Memory) WaitIdle(ctx context.Context, topicName, groupName string) error {
	tick := time.NewTicker(2 * time.Millisecond) // commits do not notify, so poll
	defer tick.Stop()
	for b.Lag(topicName, groupName) != 0 {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-tick.C:
		}
	}
	return nil
}
