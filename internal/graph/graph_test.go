package graph

import (
	"errors"
	"math"
	"slices"
	"testing"
)

// small generates a 20,000-user graph shared by the tests.
func small(t *testing.T) *Graph {
	t.Helper()
	cfg := DefaultConfig(20_000)
	cfg.MaxFollows = 500
	g, err := Generate(cfg)
	if err != nil {
		t.Fatal(err)
	}
	return g
}

func TestDeterministicForSeed(t *testing.T) {
	cfg := DefaultConfig(5000)
	cfg.MaxFollows = 200
	a, _ := Generate(cfg)
	b, _ := Generate(cfg)
	if !slices.Equal(a.offsets, b.offsets) || !slices.Equal(a.followers, b.followers) {
		t.Fatal("same seed produced different graphs")
	}
	cfg.Seed = 2
	c, _ := Generate(cfg)
	if slices.Equal(a.followers, c.followers) {
		t.Fatal("different seed produced the same graph")
	}
}

func TestNoSelfFollowsNoDuplicatesSorted(t *testing.T) {
	g := small(t)
	for u := range g.Users() {
		n, _ := g.FollowerCount(uint32(u))
		f, _ := g.Followers(uint32(u), 0, n)
		for i, v := range f {
			if v == uint32(u) {
				t.Fatalf("user %d follows itself", u)
			}
			if i > 0 && f[i-1] >= v {
				t.Fatalf("followers of %d not strictly ascending at %d", u, i)
			}
		}
	}
}

func TestPowerLawShape(t *testing.T) {
	st := small(t).Stats()
	if st.Top[0].User != 0 {
		t.Errorf("most followed is user %d, want rank 0", st.Top[0].User)
	}
	if share := float64(st.Top[0].Followers) / float64(st.Users); share < 0.3 {
		t.Errorf("top account reaches %.2f of users, want a celebrity above 0.3", share)
	}
	if st.P50 > 20 || st.Top[0].Followers < 1000*max(1, st.P50) {
		t.Errorf("not heavy-tailed: p50=%d max=%d", st.P50, st.Top[0].Followers)
	}
	if st.TopOnePercentShare < 0.4 {
		t.Errorf("top 1%% hold %.2f of edges, want > 0.4", st.TopOnePercentShare)
	}
	if math.IsNaN(st.HillAlpha) || st.HillAlpha < 0.6 || st.HillAlpha > 2.0 {
		t.Errorf("tail exponent %.2f outside [0.6, 2.0]", st.HillAlpha)
	}
	if st.MeanFollowing < 8 || st.MeanFollowing > 40 {
		t.Errorf("mean following %.1f outside [8, 40]", st.MeanFollowing)
	}
}

func TestFollowersPaging(t *testing.T) {
	g := small(t)
	n, _ := g.FollowerCount(0)
	var all []uint32
	for off := 0; off < n; off += 777 {
		page, err := g.Followers(0, off, 777)
		if err != nil {
			t.Fatal(err)
		}
		all = append(all, page...)
	}
	full, _ := g.Followers(0, 0, n)
	if !slices.Equal(all, full) || len(all) != n {
		t.Fatalf("paged %d followers, want %d", len(all), n)
	}
	if past, err := g.Followers(0, n+10, 5); err != nil || len(past) != 0 {
		t.Fatalf("past the end: %v %v", past, err)
	}
	if huge, err := g.Followers(0, math.MaxInt, 5); err != nil || len(huge) != 0 {
		t.Fatalf("offset MaxInt: %v %v", huge, err)
	}
	page, _ := g.Followers(0, 0, 3)
	if cap(page) != len(page) {
		t.Fatal("returned slice can grow into the next account's followers")
	}
}

func TestBadInputs(t *testing.T) {
	g := small(t)
	if _, err := g.FollowerCount(uint32(g.Users())); !errors.Is(err, ErrUser) {
		t.Errorf("FollowerCount out of range: %v", err)
	}
	if _, err := g.Followers(uint32(g.Users()), 0, 1); !errors.Is(err, ErrUser) {
		t.Errorf("Followers out of range: %v", err)
	}
	if _, err := g.Followers(0, -1, 1); err == nil {
		t.Error("negative offset accepted")
	}
	if _, err := g.Followers(0, 0, 0); err == nil {
		t.Error("zero limit accepted")
	}
	bad := []Config{
		{Users: 1, MinFollows: 1, MaxFollows: 1, FollowsExponent: 2, PopularityExponent: 1},
		{Users: 100, MinFollows: 0, MaxFollows: 5, FollowsExponent: 2, PopularityExponent: 1},
		{Users: 100, MinFollows: 5, MaxFollows: 100, FollowsExponent: 2, PopularityExponent: 1},
		{Users: 100, MinFollows: 1, MaxFollows: 5, FollowsExponent: 1, PopularityExponent: 1},
		{Users: 100, MinFollows: 1, MaxFollows: 5, FollowsExponent: math.NaN(), PopularityExponent: 1},
		{Users: 100, MinFollows: 1, MaxFollows: 5, FollowsExponent: 2, PopularityExponent: 0},
		{Users: MaxUsers + 1, MinFollows: 1, MaxFollows: 5, FollowsExponent: 2, PopularityExponent: 1},
	}
	for i, c := range bad {
		if _, err := Generate(c); !errors.Is(err, ErrConfig) {
			t.Errorf("config %d accepted: %v", i, err)
		}
	}
}

func TestOtherPopularityExponent(t *testing.T) {
	cfg := DefaultConfig(5000)
	cfg.MaxFollows, cfg.PopularityExponent = 100, 1.3
	g, err := Generate(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if st := g.Stats(); st.Top[0].User != 0 || st.Edges == 0 {
		t.Fatalf("unexpected stats %+v", st)
	}
}
