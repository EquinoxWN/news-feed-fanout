// Package graph generates a synthetic follow graph with a power-law shape and
// stores each account's followers in a compact CSR layout (two uint32 arrays).
//
// Model: each user follows k accounts, with k drawn from a truncated Pareto
// distribution (most users follow few accounts, some follow many). Targets are
// drawn from a Zipf-like popularity law over ranks, so account 0 is the most
// followed, account 1 the second, and a handful of accounts collect a large
// share of all followers. Generation is deterministic for a given Config.
package graph

import (
	"errors"
	"fmt"
	"math"
	"math/rand/v2"
	"runtime"
	"slices"
	"sync"
	"sync/atomic"
)

const (
	// MaxUsers bounds the graph size accepted by Generate.
	MaxUsers = 1 << 26
	// MaxEdges bounds the number of follow edges (the follower array is uint32-indexed).
	MaxEdges   = 1 << 31
	chunkUsers = 8192
)

// ErrConfig is returned for an invalid Config.
var ErrConfig = errors.New("graph: invalid config")

// ErrUser is returned for a user ID outside the graph.
var ErrUser = errors.New("graph: unknown user")

// Config describes the synthetic graph.
type Config struct {
	Users              int     // number of accounts, IDs 0..Users-1
	MinFollows         int     // smallest number of accounts a user follows
	MaxFollows         int     // largest number of accounts a user follows
	FollowsExponent    float64 // Pareto exponent of the following count (> 1)
	PopularityExponent float64 // Zipf exponent of who gets followed (> 0)
	Seed               uint64  // RNG seed; same seed, same graph
}

// DefaultConfig returns the parameters used by the demo and the README numbers.
func DefaultConfig(users int) Config {
	return Config{Users: users, MinFollows: 8, MaxFollows: 2000, FollowsExponent: 2.5, PopularityExponent: 1.0, Seed: 1}
}

// Validate checks every field of c.
func (c Config) Validate() error {
	switch {
	case c.Users < 2 || c.Users > MaxUsers:
		return fmt.Errorf("%w: users %d not in [2, %d]", ErrConfig, c.Users, MaxUsers)
	case c.MinFollows < 1 || c.MaxFollows < c.MinFollows:
		return fmt.Errorf("%w: follows range [%d, %d]", ErrConfig, c.MinFollows, c.MaxFollows)
	case c.MaxFollows >= c.Users:
		return fmt.Errorf("%w: max follows %d must be below users %d", ErrConfig, c.MaxFollows, c.Users)
	case !(c.FollowsExponent > 1) || math.IsInf(c.FollowsExponent, 0):
		return fmt.Errorf("%w: follows exponent %v must be > 1", ErrConfig, c.FollowsExponent)
	case !(c.PopularityExponent > 0) || c.PopularityExponent > 4:
		return fmt.Errorf("%w: popularity exponent %v not in (0, 4]", ErrConfig, c.PopularityExponent)
	}
	return nil
}

// Graph holds followers per account: followers[offsets[u]:offsets[u+1]], sorted ascending.
type Graph struct {
	cfg       Config
	offsets   []uint32
	followers []uint32
}

// sampler draws one user's follow targets; one sampler per goroutine.
type sampler struct {
	cfg  Config
	r    *rand.Rand
	n    float64
	pow  float64 // n^(1-s), precomputed
	seen map[uint32]struct{}
	buf  []uint32
}

// newSampler prepares a sampler for cfg.
func newSampler(cfg Config) *sampler {
	n := float64(cfg.Users)
	return &sampler{cfg: cfg, n: n, pow: math.Pow(n, 1-cfg.PopularityExponent), seen: make(map[uint32]struct{}, cfg.MaxFollows)}
}

// reseed makes the sampler's output depend only on the seed and the chunk index.
func (s *sampler) reseed(chunk int) {
	s.r = rand.New(rand.NewPCG(s.cfg.Seed, uint64(chunk)))
}

// outDegree draws how many accounts one user follows.
func (s *sampler) outDegree() int {
	k := float64(s.cfg.MinFollows) * math.Pow(1-s.r.Float64(), -1/(s.cfg.FollowsExponent-1))
	if k > float64(s.cfg.MaxFollows) || math.IsNaN(k) {
		return s.cfg.MaxFollows
	}
	return int(k)
}

// target draws one account by popularity rank (inverse CDF of a continuous Zipf law).
func (s *sampler) target() uint32 {
	u := s.r.Float64()
	var x float64
	if sx := s.cfg.PopularityExponent; math.Abs(1-sx) < 1e-9 {
		x = math.Pow(s.n, u)
	} else {
		x = math.Pow(1+u*(s.pow-1), 1/(1-sx))
	}
	r := int(x)
	if r < 1 {
		r = 1
	} else if r > s.cfg.Users {
		r = s.cfg.Users
	}
	return uint32(r - 1)
}

// follows returns the distinct accounts user u follows (valid until the next call).
func (s *sampler) follows(u uint32) []uint32 {
	k := s.outDegree()
	clear(s.seen)
	s.buf = s.buf[:0]
	for attempts := 0; len(s.buf) < k && attempts < 8*k; attempts++ {
		t := s.target()
		if t == u {
			continue
		}
		if _, dup := s.seen[t]; dup {
			continue
		}
		s.seen[t] = struct{}{}
		s.buf = append(s.buf, t)
	}
	return s.buf
}

// parallel runs fn over jobs 0..n-1 on GOMAXPROCS goroutines, each with its own sampler.
func parallel(cfg Config, n int, fn func(job int, s *sampler)) {
	var next atomic.Int64
	var wg sync.WaitGroup
	for range min(runtime.GOMAXPROCS(0), n) {
		wg.Go(func() {
			s := newSampler(cfg)
			for j := int(next.Add(1) - 1); j < n; j = int(next.Add(1) - 1) {
				fn(j, s)
			}
		})
	}
	wg.Wait()
}

// eachUser calls fn for every user of a chunk with that user's follow targets.
func eachUser(cfg Config, chunk int, s *sampler, fn func(u uint32, targets []uint32)) {
	s.reseed(chunk)
	end := min((chunk+1)*chunkUsers, cfg.Users)
	for u := chunk * chunkUsers; u < end; u++ {
		fn(uint32(u), s.follows(uint32(u)))
	}
}

// Generate builds the graph in two deterministic passes: count in-degrees, then fill.
func Generate(cfg Config) (*Graph, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	n := cfg.Users
	chunks := (n + chunkUsers - 1) / chunkUsers
	counts := make([]uint32, n)
	var edges atomic.Int64
	parallel(cfg, chunks, func(c int, s *sampler) {
		eachUser(cfg, c, s, func(_ uint32, targets []uint32) {
			for _, t := range targets {
				atomic.AddUint32(&counts[t], 1)
			}
			edges.Add(int64(len(targets)))
		})
	})
	if edges.Load() >= MaxEdges {
		return nil, fmt.Errorf("%w: %d edges exceed %d", ErrConfig, edges.Load(), MaxEdges)
	}
	offsets := make([]uint32, n+1)
	for t := range n {
		offsets[t+1] = offsets[t] + counts[t]
		counts[t] = offsets[t] // reuse as the fill cursor
	}
	followers := make([]uint32, edges.Load())
	parallel(cfg, chunks, func(c int, s *sampler) {
		eachUser(cfg, c, s, func(u uint32, targets []uint32) {
			for _, t := range targets {
				followers[atomic.AddUint32(&counts[t], 1)-1] = u
			}
		})
	})
	g := &Graph{cfg: cfg, offsets: offsets, followers: followers}
	sortChunks := (n + chunkUsers - 1) / chunkUsers
	parallel(cfg, sortChunks, func(c int, _ *sampler) {
		for t := c * chunkUsers; t < min((c+1)*chunkUsers, n); t++ {
			slices.Sort(followers[offsets[t]:offsets[t+1]])
		}
	})
	return g, nil
}

// Config returns the parameters the graph was generated with.
func (g *Graph) Config() Config { return g.cfg }

// Users is the number of accounts.
func (g *Graph) Users() int { return g.cfg.Users }

// Edges is the number of follow relations.
func (g *Graph) Edges() int { return len(g.followers) }

// MemoryBytes is the size of the CSR arrays.
func (g *Graph) MemoryBytes() int64 { return int64(len(g.offsets)+len(g.followers)) * 4 }

// FollowerCount returns how many accounts follow u.
func (g *Graph) FollowerCount(u uint32) (int, error) {
	if int(u) >= g.cfg.Users {
		return 0, fmt.Errorf("%w: %d", ErrUser, u)
	}
	return int(g.offsets[u+1] - g.offsets[u]), nil
}

// Followers returns up to limit followers of u starting at offset; the slice is read-only.
func (g *Graph) Followers(u uint32, offset, limit int) ([]uint32, error) {
	if int(u) >= g.cfg.Users {
		return nil, fmt.Errorf("%w: %d", ErrUser, u)
	}
	if offset < 0 || limit <= 0 {
		return nil, fmt.Errorf("graph: bad range offset=%d limit=%d", offset, limit)
	}
	lo, hi := int(g.offsets[u]), int(g.offsets[u+1])
	if offset >= hi-lo {
		return g.followers[hi:hi:hi], nil
	}
	a := lo + offset
	b := a + min(limit, hi-a)
	return g.followers[a:b:b], nil
}
