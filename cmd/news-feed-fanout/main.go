// Command news-feed-fanout generates a power-law follow graph and runs
// fan-out on write over it, in-process or against Kafka and Valkey.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"math/rand/v2"
	"os"
	"os/signal"
	"runtime"
	"strings"
	"time"

	"github.com/EquinoxWN/news-feed-fanout/internal/bus"
	"github.com/EquinoxWN/news-feed-fanout/internal/fanout"
	"github.com/EquinoxWN/news-feed-fanout/internal/graph"
	"github.com/EquinoxWN/news-feed-fanout/internal/ids"
	"github.com/EquinoxWN/news-feed-fanout/internal/post"
	"github.com/EquinoxWN/news-feed-fanout/internal/timeline"
)

const usage = `usage:
  news-feed-fanout graph [-users N] [-seed S]       follower-distribution summary
  news-feed-fanout demo  [-users N] [-posts P] ...  fan-out on write, then verify every timeline
run "news-feed-fanout <command> -h" for flags`

// main runs the command and exits non-zero on error.
func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	err := run(ctx, os.Args[1:], os.Stdout)
	stop()
	if err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}

// run dispatches to a subcommand.
func run(ctx context.Context, args []string, out io.Writer) error {
	if len(args) == 0 {
		return errors.New(usage)
	}
	switch args[0] {
	case "graph":
		return runGraph(args[1:], out)
	case "demo":
		return runDemo(ctx, args[1:], out)
	default:
		return fmt.Errorf("unknown command %q\n%s", args[0], usage)
	}
}

// graphFlags registers the graph-shape flags shared by both commands.
func graphFlags(fs *flag.FlagSet) *graph.Config {
	cfg := graph.DefaultConfig(0)
	fs.IntVar(&cfg.Users, "users", 200_000, "number of accounts")
	fs.Uint64Var(&cfg.Seed, "seed", cfg.Seed, "graph seed")
	fs.IntVar(&cfg.MinFollows, "min-follows", cfg.MinFollows, "fewest accounts a user follows")
	fs.IntVar(&cfg.MaxFollows, "max-follows", cfg.MaxFollows, "most accounts a user follows")
	fs.Float64Var(&cfg.FollowsExponent, "follows-exp", cfg.FollowsExponent, "Pareto exponent of the following count")
	fs.Float64Var(&cfg.PopularityExponent, "popularity-exp", cfg.PopularityExponent, "Zipf exponent of popularity")
	return &cfg
}

// buildGraph generates the graph and prints its summary.
func buildGraph(cfg graph.Config, out io.Writer) (*graph.Graph, error) {
	start := time.Now()
	g, err := graph.Generate(cfg)
	if err != nil {
		return nil, err
	}
	took := time.Since(start)
	st := g.Stats()
	fmt.Fprintf(out, "graph: %d users, %d follow edges (mean %.1f followed per user), generated in %s, CSR %.1f MiB\n",
		st.Users, st.Edges, st.MeanFollowing, took.Round(time.Millisecond), float64(g.MemoryBytes())/(1<<20))
	fmt.Fprintf(out, "followers: p50=%d p90=%d p99=%d p99.9=%d max=%d; accounts with >=100k: %d, >=1M: %d; nobody follows: %d\n",
		st.P50, st.P90, st.P99, st.P999, st.Top[0].Followers, st.Over100K, st.Over1M, st.ZeroFollowers)
	fmt.Fprintf(out, "top 1%% of accounts receive %.1f%% of all follows; tail exponent (Hill, top 1%%) alpha=%.2f\n",
		100*st.TopOnePercentShare, st.HillAlpha)
	for i, a := range st.Top {
		fmt.Fprintf(out, "  #%d user %d: %d followers (%.1f%% of users)\n", i+1, a.User, a.Followers, 100*float64(a.Followers)/float64(st.Users))
	}
	return g, nil
}

// runGraph prints the follower distribution of a generated graph.
func runGraph(args []string, out io.Writer) error {
	fs := flag.NewFlagSet("graph", flag.ContinueOnError)
	cfg := graphFlags(fs)
	if err := fs.Parse(args); err != nil {
		return err
	}
	_, err := buildGraph(*cfg, out)
	return err
}

type demoOpts struct {
	posts, celebrities, partitions, maxLen int
	kafka, valkey                          string
	timeout                                time.Duration
	fan                                    fanout.Config
}

// runDemo publishes posts, waits for fan-out and checks every follower's timeline.
func runDemo(ctx context.Context, args []string, out io.Writer) error {
	fs := flag.NewFlagSet("demo", flag.ContinueOnError)
	gcfg := graphFlags(fs)
	o := demoOpts{fan: fanout.DefaultConfig()}
	fs.IntVar(&o.posts, "posts", 1000, "posts from random authors")
	fs.IntVar(&o.celebrities, "celebrities", 3, "posts from the most-followed accounts (one each)")
	fs.IntVar(&o.partitions, "partitions", 16, "partitions per topic")
	fs.IntVar(&o.maxLen, "maxlen", timeline.DefaultMaxLen, "entries kept per timeline")
	fs.IntVar(&o.fan.ChunkSize, "chunk", o.fan.ChunkSize, "followers per fan-out task")
	fs.IntVar(&o.fan.SplitWorkers, "split-workers", o.fan.SplitWorkers, "splitter group members")
	fs.IntVar(&o.fan.WriteWorkers, "write-workers", o.fan.WriteWorkers, "writer group members")
	fs.StringVar(&o.kafka, "kafka", "", "comma-separated Kafka brokers (empty: in-process bus)")
	fs.StringVar(&o.valkey, "valkey", "", "Valkey address host:port (empty: in-process timelines)")
	fs.DurationVar(&o.timeout, "timeout", 10*time.Minute, "give up after this long")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if o.posts < 0 || o.posts > 1_000_000 || o.celebrities < 0 || o.celebrities > 100 || o.posts+o.celebrities == 0 {
		return errors.New("demo: need 1..1,000,000 posts and 0..100 celebrity posts")
	}
	if o.celebrities > gcfg.Users {
		return errors.New("demo: more celebrity posts than users")
	}
	g, err := buildGraph(*gcfg, out)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(ctx, o.timeout)
	defer cancel()
	return demo(ctx, g, o, out)
}

// backends returns the bus, timeline store, a wait function and a cleanup.
func backends(ctx context.Context, g *graph.Graph, o demoOpts) (bus.Bus, timeline.Store, func(context.Context, *fanout.Workers, int64) error, func(), error) {
	var b bus.Bus
	var wait func(context.Context, *fanout.Workers, int64) error
	cleanup := func() {}
	if o.kafka == "" {
		mem, err := bus.NewMemory(o.partitions)
		if err != nil {
			return nil, nil, nil, nil, err
		}
		b = mem
		wait = func(ctx context.Context, _ *fanout.Workers, _ int64) error {
			if err := mem.WaitIdle(ctx, o.fan.PostsTopic, fanout.SplitGroup); err != nil {
				return err
			}
			return mem.WaitIdle(ctx, o.fan.TasksTopic, fanout.WriteGroup)
		}
	} else {
		k, err := bus.NewKafka(strings.Split(o.kafka, ","))
		if err != nil {
			return nil, nil, nil, nil, err
		}
		for _, t := range []string{o.fan.PostsTopic, o.fan.TasksTopic} {
			if err := k.EnsureTopic(ctx, t, int32(o.partitions)); err != nil {
				k.Close()
				return nil, nil, nil, nil, err
			}
		}
		b, cleanup = k, k.Close
		wait = func(ctx context.Context, w *fanout.Workers, tasks int64) error {
			for w.Stats().TasksWritten < tasks {
				select {
				case <-ctx.Done():
					return ctx.Err()
				case <-time.After(20 * time.Millisecond):
				}
			}
			return nil
		}
	}
	var tl timeline.Store
	if o.valkey == "" {
		mem, err := timeline.NewMemory(g.Users(), o.maxLen)
		if err != nil {
			cleanup()
			return nil, nil, nil, nil, err
		}
		tl = mem
	} else {
		v, err := timeline.NewValkey(o.valkey, o.maxLen)
		if err != nil {
			cleanup()
			return nil, nil, nil, nil, err
		}
		tl = v
		prev := cleanup
		cleanup = func() { v.Close(); prev() }
	}
	return b, tl, wait, cleanup, nil
}

// demo runs the pipeline on g and reports write amplification and verification.
func demo(ctx context.Context, g *graph.Graph, o demoOpts, out io.Writer) error {
	b, tl, wait, cleanup, err := backends(ctx, g, o)
	if err != nil {
		return err
	}
	defer cleanup()
	posts := post.NewMemStore()
	svc, err := fanout.NewService(o.fan, posts, ids.New(nil), b, g)
	if err != nil {
		return err
	}
	w, err := fanout.NewWorkers(o.fan, b, g, tl)
	if err != nil {
		return err
	}
	wctx, stopWorkers := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() { done <- w.Run(wctx) }()
	defer func() { stopWorkers(); <-done }()

	rng := rand.New(rand.NewPCG(g.Config().Seed, 99))
	var published []post.Post
	var expectTasks, expectEntries int64
	start := time.Now()
	for i := range o.celebrities + o.posts {
		author := uint32(i)
		if i >= o.celebrities {
			author = uint32(rng.IntN(g.Users()))
		}
		p, err := svc.Publish(ctx, author, fmt.Sprintf("post %d from user %d", i, author))
		if err != nil {
			return err
		}
		n, _ := g.FollowerCount(author)
		expectEntries += int64(n)
		expectTasks += int64((n + o.fan.ChunkSize - 1) / o.fan.ChunkSize)
		published = append(published, p)
	}
	pubTook := time.Since(start)
	waitErr := make(chan error, 1)
	go func() { waitErr <- wait(ctx, w, expectTasks) }()
	select {
	case err := <-waitErr:
		if err != nil {
			return fmt.Errorf("waiting for fan-out: %w", err)
		}
	case err := <-done:
		done <- err // let the deferred stop read it
		return fmt.Errorf("workers stopped: %w", err)
	}
	took := time.Since(start)
	st := w.Stats()
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)

	fmt.Fprintf(out, "backend: bus=%s timelines=%s, %d partitions, chunk %d, %d split + %d write workers\n",
		name(o.kafka, "kafka", "in-process"), name(o.valkey, "valkey", "in-process"), o.partitions, o.fan.ChunkSize, o.fan.SplitWorkers, o.fan.WriteWorkers)
	fmt.Fprintf(out, "posts: %d published (%d from the top accounts), stored once: %d rows, %d body bytes; publish took %s\n",
		len(published), o.celebrities, posts.Len(), posts.BodyBytes(), pubTook.Round(time.Millisecond))
	for _, p := range published[:o.celebrities] {
		n, _ := g.FollowerCount(p.Author)
		fmt.Fprintf(out, "  celebrity post %d by user %d -> %d timelines\n", p.ID, p.Author, n)
	}
	fmt.Fprintf(out, "fan-out: %d tasks, %d timeline entries written (expected %d) in %s = %.0f entries/s\n",
		st.TasksWritten, st.EntriesWritten, expectEntries, took.Round(time.Millisecond), float64(st.EntriesWritten)/took.Seconds())
	fmt.Fprintf(out, "write amplification: %.1f timeline writes per post (max %d for one post)\n",
		float64(st.EntriesWritten)/float64(len(published)), maxFollowers(g, published))
	fmt.Fprintf(out, "go heap in use: %.0f MiB\n", float64(ms.HeapInuse)/(1<<20))

	var missing, checked int
	for _, p := range published {
		m, err := fanout.Missing(ctx, g, tl, p.Author, p.ID, 4096)
		if err != nil {
			return err
		}
		n, _ := g.FollowerCount(p.Author)
		missing += m
		checked += n
	}
	if len(published) > 0 && o.celebrities > 0 {
		f, _ := g.Followers(published[0].Author, 0, 1)
		if len(f) == 1 {
			home, err := fanout.Home(ctx, tl, posts, f[0], 3)
			if err != nil {
				return err
			}
			fmt.Fprintf(out, "home timeline of user %d (newest first):", f[0])
			for _, p := range home {
				fmt.Fprintf(out, " [%d by %d: %q]", p.ID, p.Author, p.Body)
			}
			fmt.Fprintln(out)
		}
	}
	fmt.Fprintf(out, "verify: %d of %d follower timelines hold their post, %d missing\n", checked-missing, checked, missing)
	if missing > 0 {
		return fmt.Errorf("verify: %d timeline entries missing", missing)
	}
	fmt.Fprintln(out, "PASS")
	return nil
}

// name picks a label depending on whether an address was given.
func name(addr, set, unset string) string {
	if addr == "" {
		return unset
	}
	return set + "(" + addr + ")"
}

// maxFollowers is the largest audience among the published posts.
func maxFollowers(g *graph.Graph, ps []post.Post) int {
	m := 0
	for _, p := range ps {
		if n, _ := g.FollowerCount(p.Author); n > m {
			m = n
		}
	}
	return m
}
