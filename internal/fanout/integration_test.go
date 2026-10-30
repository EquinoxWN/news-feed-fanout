//go:build integration

package fanout

import (
	"context"
	"fmt"
	"os"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/EquinoxWN/news-feed-fanout/internal/bus"
	"github.com/EquinoxWN/news-feed-fanout/internal/graph"
	"github.com/EquinoxWN/news-feed-fanout/internal/ids"
	"github.com/EquinoxWN/news-feed-fanout/internal/post"
	"github.com/EquinoxWN/news-feed-fanout/internal/timeline"
)

// Runs only with -tags integration and NFF_KAFKA / NFF_VALKEY set (CI service containers).
func TestKafkaAndValkeyEndToEnd(t *testing.T) {
	brokers, addr := os.Getenv("NFF_KAFKA"), os.Getenv("NFF_VALKEY")
	if brokers == "" || addr == "" {
		t.Skip("NFF_KAFKA and NFF_VALKEY not set")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	gc := graph.DefaultConfig(5000)
	gc.MaxFollows = 300
	g, err := graph.Generate(gc)
	if err != nil {
		t.Fatal(err)
	}
	k, err := bus.NewKafka(strings.Split(brokers, ","))
	if err != nil {
		t.Fatal(err)
	}
	defer k.Close()
	suffix := fmt.Sprint(time.Now().UnixNano())
	cfg := DefaultConfig()
	cfg.PostsTopic, cfg.TasksTopic, cfg.ChunkSize = "it-posts-"+suffix, "it-tasks-"+suffix, 200
	for _, topic := range []string{cfg.PostsTopic, cfg.TasksTopic} {
		if err := k.EnsureTopic(ctx, topic, 8); err != nil {
			t.Fatal(err)
		}
	}
	tl, err := timeline.NewValkey(addr, timeline.DefaultMaxLen)
	if err != nil {
		t.Fatal(err)
	}
	defer tl.Close()
	posts := post.NewMemStore()
	svc, _ := NewService(cfg, posts, ids.New(nil), k, g)
	w, _ := NewWorkers(cfg, k, g, tl)
	wctx, stop := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() { done <- w.Run(wctx) }()

	var ps []post.Post
	var tasks int64
	for _, a := range []uint32{0, 1, 2, 77, 4000} {
		p, err := svc.Publish(ctx, a, fmt.Sprintf("hello from %d", a))
		if err != nil {
			t.Fatal(err)
		}
		n, _ := g.FollowerCount(a)
		tasks += int64((n + cfg.ChunkSize - 1) / cfg.ChunkSize)
		ps = append(ps, p)
	}
	for w.Stats().TasksWritten < tasks {
		select {
		case err := <-done:
			t.Fatalf("workers stopped: %v", err)
		case <-ctx.Done():
			t.Fatalf("timed out at %+v, want %d tasks", w.Stats(), tasks)
		case <-time.After(50 * time.Millisecond):
		}
	}
	stop()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	var checked int
	for _, p := range ps {
		miss, err := Missing(ctx, g, tl, p.Author, p.ID, 500)
		if err != nil || miss != 0 {
			t.Fatalf("post %d: %d followers missing (%v)", p.ID, miss, err)
		}
		n, _ := g.FollowerCount(p.Author)
		checked += n
	}
	f, _ := g.Followers(0, 0, 1)
	latest, err := tl.Latest(ctx, f[0], 10)
	if err != nil || !slices.Contains(latest, ps[0].ID) {
		t.Fatalf("Valkey timeline of %d = %v (%v)", f[0], latest, err)
	}
	t.Logf("kafka+valkey: %d posts, %d tasks, %d timeline entries verified", len(ps), tasks, checked)
}
