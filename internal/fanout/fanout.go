// Package fanout implements fan-out on write in two Kafka-style stages:
//
//  1. Publish stores the post once and emits PostCreated on the posts topic.
//  2. Splitters (consumer group "fanout-split") turn each PostCreated into one
//     FanoutTask per chunk of the author's followers on the tasks topic.
//  3. Writers (consumer group "fanout-write") push the post ID into the
//     timeline of every follower in their chunk.
//
// Splitting lets one post from an account with millions of followers spread
// across every tasks partition instead of pinning one worker.
package fanout

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/EquinoxWN/news-feed-fanout/internal/bus"
	"github.com/EquinoxWN/news-feed-fanout/internal/event"
	"github.com/EquinoxWN/news-feed-fanout/internal/ids"
	"github.com/EquinoxWN/news-feed-fanout/internal/post"
	"github.com/EquinoxWN/news-feed-fanout/internal/timeline"
)

// Consumer group names.
const (
	SplitGroup = "fanout-split"
	WriteGroup = "fanout-write"
)

// ErrAuthor is returned when a post's author is not in the graph.
var ErrAuthor = errors.New("fanout: unknown author")

// Followers is the read side of the follow graph.
type Followers interface {
	Users() int
	FollowerCount(u uint32) (int, error)
	Followers(u uint32, offset, limit int) ([]uint32, error)
}

// Config tunes topics, chunking and parallelism.
type Config struct {
	PostsTopic   string
	TasksTopic   string
	ChunkSize    int // followers per FanoutTask
	SplitWorkers int
	WriteWorkers int
}

// DefaultConfig returns the settings used by the demo.
func DefaultConfig() Config {
	return Config{PostsTopic: "posts", TasksTopic: "fanout-tasks", ChunkSize: 1000, SplitWorkers: 2, WriteWorkers: 8}
}

// Validate checks c.
func (c Config) Validate() error {
	if c.PostsTopic == "" || c.TasksTopic == "" || c.PostsTopic == c.TasksTopic {
		return fmt.Errorf("fanout: topics %q and %q must be distinct and non-empty", c.PostsTopic, c.TasksTopic)
	}
	if c.ChunkSize < 1 || c.ChunkSize > event.MaxChunk {
		return fmt.Errorf("fanout: chunk size %d not in [1, %d]", c.ChunkSize, event.MaxChunk)
	}
	if c.SplitWorkers < 1 || c.WriteWorkers < 1 {
		return fmt.Errorf("fanout: workers must be positive")
	}
	return nil
}

// Service is the write API: it stores a post once and announces it.
type Service struct {
	cfg   Config
	posts post.Store
	ids   *ids.Generator
	bus   bus.Bus
	graph Followers
}

// NewService wires the post store, ID generator, bus and graph.
func NewService(cfg Config, posts post.Store, gen *ids.Generator, b bus.Bus, g Followers) (*Service, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	return &Service{cfg: cfg, posts: posts, ids: gen, bus: b, graph: g}, nil
}

// Publish validates and stores the post, then emits PostCreated keyed by author.
func (s *Service) Publish(ctx context.Context, author uint32, body string) (post.Post, error) {
	if int(author) >= s.graph.Users() {
		return post.Post{}, fmt.Errorf("%w: %d", ErrAuthor, author)
	}
	if err := post.Validate(body); err != nil {
		return post.Post{}, err
	}
	id, err := s.ids.Next()
	if err != nil {
		return post.Post{}, err
	}
	p := post.Post{ID: id, Author: author, Body: body, CreatedAt: ids.Time(id)}
	if err := s.posts.Put(ctx, p); err != nil {
		return post.Post{}, err
	}
	var k [4]byte
	binary.BigEndian.PutUint32(k[:], author)
	if err := s.bus.Publish(ctx, s.cfg.PostsTopic, k[:], event.PostCreated{PostID: id, Author: author}.Encode()); err != nil {
		return post.Post{}, fmt.Errorf("fanout: post %d stored but not announced: %w", id, err)
	}
	return p, nil
}

// Stats counts work done by the workers since start.
type Stats struct {
	PostsSplit     int64 // PostCreated messages handled (including redeliveries)
	TasksEmitted   int64 // FanoutTask messages published
	TasksWritten   int64 // FanoutTask messages applied to timelines
	EntriesWritten int64 // timeline insertions attempted (write amplification numerator)
}

// Workers runs the splitter and writer consumer groups.
type Workers struct {
	cfg   Config
	bus   bus.Bus
	graph Followers
	tl    timeline.Store

	postsSplit, tasksEmitted, tasksWritten, entries atomic.Int64
}

// NewWorkers wires the bus, graph and timeline store.
func NewWorkers(cfg Config, b bus.Bus, g Followers, tl timeline.Store) (*Workers, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	return &Workers{cfg: cfg, bus: b, graph: g, tl: tl}, nil
}

// Run consumes both stages until ctx ends; it returns the first handler or bus error.
func (w *Workers) Run(ctx context.Context) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	var wg sync.WaitGroup
	errs := make([]error, 2)
	wg.Go(func() {
		if errs[0] = w.bus.Consume(ctx, w.cfg.PostsTopic, SplitGroup, w.cfg.SplitWorkers, w.split); errs[0] != nil {
			cancel()
		}
	})
	wg.Go(func() {
		if errs[1] = w.bus.Consume(ctx, w.cfg.TasksTopic, WriteGroup, w.cfg.WriteWorkers, w.write); errs[1] != nil {
			cancel()
		}
	})
	wg.Wait()
	return errors.Join(errs...)
}

// split turns one PostCreated into FanoutTasks covering all of the author's followers.
func (w *Workers) split(ctx context.Context, m bus.Message) error {
	e, err := event.DecodePostCreated(m.Value)
	if err != nil {
		return err
	}
	n, err := w.graph.FollowerCount(e.Author)
	if err != nil {
		return err
	}
	var k [12]byte
	binary.BigEndian.PutUint64(k[:], e.PostID)
	for off := 0; off < n; off += w.cfg.ChunkSize {
		t := event.FanoutTask{PostID: e.PostID, Author: e.Author, Offset: uint32(off), Count: uint32(min(w.cfg.ChunkSize, n-off))}
		binary.BigEndian.PutUint32(k[8:], t.Offset)
		if err := w.bus.Publish(ctx, w.cfg.TasksTopic, k[:], t.Encode()); err != nil {
			return err
		}
		w.tasksEmitted.Add(1)
	}
	w.postsSplit.Add(1)
	return nil
}

// write applies one FanoutTask to the followers' timelines.
func (w *Workers) write(ctx context.Context, m bus.Message) error {
	t, err := event.DecodeFanoutTask(m.Value)
	if err != nil {
		return err
	}
	users, err := w.graph.Followers(t.Author, int(t.Offset), int(t.Count))
	if err != nil {
		return err
	}
	if err := w.tl.AddBatch(ctx, users, t.PostID); err != nil {
		return err
	}
	w.entries.Add(int64(len(users)))
	w.tasksWritten.Add(1)
	return nil
}

// Stats returns a snapshot of the counters.
func (w *Workers) Stats() Stats {
	return Stats{PostsSplit: w.postsSplit.Load(), TasksEmitted: w.tasksEmitted.Load(), TasksWritten: w.tasksWritten.Load(), EntriesWritten: w.entries.Load()}
}

// Home reads a user's timeline and loads the posts it points to (newest first).
func Home(ctx context.Context, tl timeline.Store, posts post.Store, user uint32, n int) ([]post.Post, error) {
	idsList, err := tl.Latest(ctx, user, n)
	if err != nil {
		return nil, err
	}
	return posts.GetMany(ctx, idsList)
}

// Missing counts followers of author whose timeline lacks postID, checking chunk by chunk.
func Missing(ctx context.Context, g Followers, tl timeline.Store, author uint32, postID uint64, chunk int) (int, error) {
	if chunk < 1 {
		return 0, fmt.Errorf("fanout: chunk %d must be positive", chunk)
	}
	n, err := g.FollowerCount(author)
	if err != nil {
		return 0, err
	}
	total := 0
	for off := 0; off < n; off += chunk {
		users, err := g.Followers(author, off, chunk)
		if err != nil {
			return 0, err
		}
		miss, err := tl.CountMissing(ctx, users, postID)
		if err != nil {
			return 0, err
		}
		total += miss
	}
	return total, nil
}
