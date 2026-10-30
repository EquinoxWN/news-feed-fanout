package graph

import (
	"math"
	"slices"
)

// Account is one account and its follower count.
type Account struct {
	User      uint32
	Followers int
}

// Stats summarises the follower-count distribution of a graph.
type Stats struct {
	Users, Edges       int
	MeanFollowing      float64   // average accounts followed per user
	Top                []Account // the five most followed accounts
	P50, P90, P99      int       // follower-count percentiles
	P999               int
	Over100K, Over1M   int     // accounts with at least 100,000 / 1,000,000 followers
	ZeroFollowers      int     // accounts nobody follows
	TopOnePercentShare float64 // share of all edges that point at the top 1% of accounts
	HillAlpha          float64 // tail exponent of P(followers > x) ~ x^-alpha, Hill estimator on the top 1%
}

// Stats computes the summary in O(n log n).
func (g *Graph) Stats() Stats {
	n := g.cfg.Users
	counts := make([]int, n)
	for u := range n {
		counts[u] = int(g.offsets[u+1] - g.offsets[u])
	}
	st := Stats{Users: n, Edges: len(g.followers), MeanFollowing: float64(len(g.followers)) / float64(n)}
	order := make([]uint32, n)
	for i := range order {
		order[i] = uint32(i)
	}
	slices.SortStableFunc(order, func(a, b uint32) int { return counts[b] - counts[a] })
	for _, u := range order[:min(5, n)] {
		st.Top = append(st.Top, Account{User: u, Followers: counts[u]})
	}
	desc := make([]int, n)
	for i, u := range order {
		desc[i] = counts[u]
	}
	pct := func(p float64) int { return desc[min(n-1, int(float64(n)*(1-p)))] }
	st.P50, st.P90, st.P99, st.P999 = pct(0.50), pct(0.90), pct(0.99), pct(0.999)
	top := max(1, n/100)
	var topEdges int
	for i, c := range desc {
		if c >= 100_000 {
			st.Over100K++
		}
		if c >= 1_000_000 {
			st.Over1M++
		}
		if c == 0 {
			st.ZeroFollowers++
		}
		if i < top {
			topEdges += c
		}
	}
	if st.Edges > 0 {
		st.TopOnePercentShare = float64(topEdges) / float64(st.Edges)
	}
	st.HillAlpha = hill(desc, top)
	return st
}

// hill estimates the tail exponent from the k largest values of a descending slice.
func hill(desc []int, k int) float64 {
	if k < 2 || k >= len(desc) || desc[k] <= 0 {
		return math.NaN()
	}
	xk := float64(desc[k])
	var sum float64
	for _, x := range desc[:k] {
		sum += math.Log(float64(x) / xk)
	}
	if sum == 0 {
		return math.NaN()
	}
	return float64(k) / sum
}
