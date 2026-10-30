package fanout

import (
	"context"
	"errors"
	"math/rand/v2"
	"slices"
	"sync/atomic"
	"testing"
	"time"

	"github.com/EquinoxWN/news-feed-fanout/internal/bus"
	"github.com/EquinoxWN/news-feed-fanout/internal/graph"
	"github.com/EquinoxWN/news-feed-fanout/internal/ids"
	"github.com/EquinoxWN/news-feed-fanout/internal/post"
	"github.com/EquinoxWN/news-feed-fanout/internal/timeline"
)

type rig struct {
	g     *graph.Graph
	bus   *bus.Memory
	posts *post.MemStore
	tl    timeline.Store
	svc   *Service
	w     *Workers
	cfg   Config
}

// newRig wires an in-process pipeline over a generated graph.
func newRig(t *testing.T, users int, seed uint64, cfg Config, tl func(users int) timeline.Store) *rig {
	t.Helper()
	gc := graph.DefaultConfig(users)
	gc.MaxFollows, gc.Seed = min(300, users-1), seed
	g, err := graph.Generate(gc)
	if err != nil {
		t.Fatal(err)
	}
	b, _ := bus.NewMemory(8)
	r := &rig{g: g, bus: b, posts: post.NewMemStore(), cfg: cfg}
	if tl == nil {
		m, _ := timeline.NewMemory(users, 10_000)
		r.tl = m
	} else {
		r.tl = tl(users)
	}
	if r.svc, err = NewService(cfg, r.posts, ids.New(nil), b, g); err != nil {
		t.Fatal(err)
	}
	if r.w, err = NewWorkers(cfg, b, g, r.tl); err != nil {
		t.Fatal(err)
	}
	return r
}

// run starts the workers, waits until both groups are idle, and stops them.
func (r *rig) run(t *testing.T) error {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- r.w.Run(ctx) }()
	idle := make(chan error, 1)
	go func() {
		if err := r.bus.WaitIdle(ctx, r.cfg.PostsTopic, SplitGroup); err != nil {
			idle <- err
			return
		}
		idle <- r.bus.WaitIdle(ctx, r.cfg.TasksTopic, WriteGroup)
	}()
	select {
	case err := <-done:
		return err
	case err := <-idle:
		if err != nil {
			t.Fatal(err)
		}
		cancel()
		return <-done
	}
}

// verify checks every follower of every post has it in their timeline.
func (r *rig) verify(t *testing.T, ps []post.Post) {
	t.Helper()
	for _, p := range ps {
		miss, err := Missing(context.Background(), r.g, r.tl, p.Author, p.ID, 512)
		if err != nil || miss != 0 {
			t.Fatalf("post %d by %d: %d followers missing it (%v)", p.ID, p.Author, miss, err)
		}
	}
}

func TestCelebrityPostReachesEveryFollower(t *testing.T) {
	cfg := DefaultConfig()
	cfg.ChunkSize = 250
	r := newRig(t, 20_000, 1, cfg, nil)
	p, err := r.svc.Publish(context.Background(), 0, "hello from the top account")
	if err != nil {
		t.Fatal(err)
	}
	if err := r.run(t); err != nil {
		t.Fatal(err)
	}
	n, _ := r.g.FollowerCount(0)
	st := r.w.Stats()
	if st.EntriesWritten != int64(n) || st.TasksWritten != int64((n+249)/250) {
		t.Fatalf("stats %+v for %d followers", st, n)
	}
	r.verify(t, []post.Post{p})
	if r.posts.Len() != 1 {
		t.Fatalf("post stored %d times, want once", r.posts.Len())
	}
	f, _ := r.g.Followers(0, n-1, 1)
	home, _ := Home(context.Background(), r.tl, r.posts, f[0], 5)
	if len(home) != 1 || home[0].Body != "hello from the top account" {
		t.Fatalf("home of follower %d = %+v", f[0], home)
	}
}

func TestMatchesBruteForceOracle(t *testing.T) {
	// Randomized: random graphs, chunk sizes and authors; every timeline must equal
	// the set of posts by the accounts that user follows.
	for trial := range 5 {
		r := rand.New(rand.NewPCG(uint64(trial), 7))
		cfg := DefaultConfig()
		cfg.ChunkSize = 1 + r.IntN(400)
		cfg.WriteWorkers = 1 + r.IntN(6)
		users := 500 + r.IntN(1500)
		rg := newRig(t, users, uint64(trial+10), cfg, nil)
		var ps []post.Post
		for range 50 + r.IntN(100) {
			author := uint32(r.IntN(users))
			if r.IntN(4) == 0 {
				author = uint32(r.IntN(5)) // favour the popular accounts
			}
			p, err := rg.svc.Publish(context.Background(), author, "x")
			if err != nil {
				t.Fatal(err)
			}
			ps = append(ps, p)
		}
		if err := rg.run(t); err != nil {
			t.Fatal(err)
		}
		want := make([][]uint64, users)
		var entries int64
		for _, p := range ps {
			n, _ := rg.g.FollowerCount(p.Author)
			fs, _ := rg.g.Followers(p.Author, 0, n)
			for _, f := range fs {
				want[f] = append(want[f], p.ID)
			}
			entries += int64(n)
		}
		for u := range users {
			got, _ := rg.tl.Latest(context.Background(), uint32(u), 10_000)
			slices.Sort(got)
			slices.Sort(want[u])
			if !slices.Equal(got, want[u]) {
				t.Fatalf("trial %d user %d: got %v want %v", trial, u, got, want[u])
			}
		}
		if st := rg.w.Stats(); st.EntriesWritten != entries {
			t.Fatalf("trial %d: wrote %d entries, oracle %d", trial, st.EntriesWritten, entries)
		}
	}
}

// flaky fails every k-th AddBatch call, as a crashing timeline backend would.
type flaky struct {
	timeline.Store
	calls, failures atomic.Int64
	every           int64
}

func (f *flaky) AddBatch(ctx context.Context, users []uint32, id uint64) error {
	if f.calls.Add(1)%f.every == 0 {
		f.failures.Add(1)
		return errors.New("injected timeline failure")
	}
	return f.Store.AddBatch(ctx, users, id)
}

func TestRedeliveryAfterFailuresIsIdempotent(t *testing.T) {
	var fl *flaky
	cfg := DefaultConfig()
	cfg.ChunkSize = 100
	r := newRig(t, 5000, 3, cfg, func(users int) timeline.Store {
		m, _ := timeline.NewMemory(users, 10_000)
		fl = &flaky{Store: m, every: 7}
		return fl
	})
	var ps []post.Post
	for a := range uint32(20) {
		p, _ := r.svc.Publish(context.Background(), a, "y")
		ps = append(ps, p)
	}
	restarts := 0
	for {
		err := r.run(t)
		if err == nil {
			break
		}
		restarts++
		if restarts > 10_000 {
			t.Fatalf("never converged: %v", err)
		}
	}
	if restarts == 0 || fl.failures.Load() == 0 {
		t.Fatal("no failure was injected")
	}
	r.verify(t, ps)
	mem := fl.Store.(*timeline.Memory)
	var want int64
	for _, p := range ps {
		n, _ := r.g.FollowerCount(p.Author)
		want += int64(n)
	}
	if mem.Entries() != want {
		t.Fatalf("%d entries stored, want exactly %d (no duplicates)", mem.Entries(), want)
	}
	t.Logf("%d restarts, %d injected failures, %d entries", restarts, fl.failures.Load(), want)
}

func TestPublishValidation(t *testing.T) {
	r := newRig(t, 1000, 1, DefaultConfig(), nil)
	ctx := context.Background()
	if _, err := r.svc.Publish(ctx, 1000, "x"); !errors.Is(err, ErrAuthor) {
		t.Errorf("unknown author: %v", err)
	}
	if _, err := r.svc.Publish(ctx, 1, ""); !errors.Is(err, post.ErrInvalid) {
		t.Errorf("empty body: %v", err)
	}
	if r.posts.Len() != 0 || r.bus.Lag(r.cfg.PostsTopic, SplitGroup) != 0 {
		t.Error("rejected post was stored or announced")
	}
}

func TestMalformedMessageStopsWorkers(t *testing.T) {
	r := newRig(t, 1000, 1, DefaultConfig(), nil)
	_ = r.bus.Publish(context.Background(), r.cfg.TasksTopic, nil, []byte("junk"))
	if err := r.run(t); err == nil {
		t.Fatal("malformed task accepted")
	}
}

func TestConfigValidation(t *testing.T) {
	bad := []func(*Config){
		func(c *Config) { c.PostsTopic = "" },
		func(c *Config) { c.TasksTopic = c.PostsTopic },
		func(c *Config) { c.ChunkSize = 0 },
		func(c *Config) { c.ChunkSize = 1 << 20 },
		func(c *Config) { c.WriteWorkers = 0 },
	}
	for i, mut := range bad {
		c := DefaultConfig()
		mut(&c)
		if c.Validate() == nil {
			t.Errorf("case %d accepted", i)
		}
		if _, err := NewWorkers(c, nil, nil, nil); err == nil {
			t.Errorf("case %d: NewWorkers accepted", i)
		}
	}
	if _, err := Missing(context.Background(), nil, nil, 0, 1, 0); err == nil {
		t.Error("Missing with chunk 0 accepted")
	}
}
