package timeline

import (
	"context"
	"fmt"
	"strconv"
)

// Valkey stores each timeline as a sorted set "tl:<user>" with score = member = post ID.
// Post IDs fit in 53 bits, so the float64 score is exact. It speaks RESP2 over
// plain TCP through a small connection pool (see ADR 0003).
type Valkey struct {
	pool   *pool
	maxLen int
}

// NewValkey connects to a Valkey server at addr (host:port) and checks it with PING.
func NewValkey(addr string, maxLen int) (*Valkey, error) {
	if addr == "" || maxLen < 1 {
		return nil, fmt.Errorf("%w: addr %q maxLen %d", ErrArgs, addr, maxLen)
	}
	v := &Valkey{pool: newPool(addr), maxLen: maxLen}
	rs, err := v.pool.pipeline(context.Background(), [][]string{{"PING"}})
	if err != nil {
		return nil, err
	}
	if err := rs[0].err(); err != nil {
		return nil, err
	}
	return v, nil
}

// key is the sorted-set key of a user's timeline.
func key(u uint32) string { return "tl:" + strconv.FormatUint(uint64(u), 10) }

// AddBatch pipelines ZADD and a rank trim for every user in one round trip.
func (v *Valkey) AddBatch(ctx context.Context, users []uint32, postID uint64) error {
	if len(users) == 0 {
		return nil
	}
	member := strconv.FormatUint(postID, 10)
	stop := strconv.Itoa(-(v.maxLen + 1))
	cmds := make([][]string, 0, 2*len(users))
	for _, u := range users {
		k := key(u)
		cmds = append(cmds, []string{"ZADD", k, member, member}, []string{"ZREMRANGEBYRANK", k, "0", stop})
	}
	rs, err := v.pool.pipeline(ctx, cmds)
	if err != nil {
		return err
	}
	for _, r := range rs {
		if err := r.err(); err != nil {
			return err
		}
	}
	return nil
}

// Latest reads the newest n members with ZRANGE ... BYSCORE REV LIMIT.
func (v *Valkey) Latest(ctx context.Context, user uint32, n int) ([]uint64, error) {
	if n < 1 {
		return nil, fmt.Errorf("%w: n %d", ErrArgs, n)
	}
	rs, err := v.pool.pipeline(ctx, [][]string{{"ZRANGE", key(user), "+inf", "-inf", "BYSCORE", "REV", "LIMIT", "0", strconv.Itoa(n)}})
	if err != nil {
		return nil, err
	}
	r := rs[0]
	if err := r.err(); err != nil {
		return nil, err
	}
	if r.kind != '*' {
		return nil, fmt.Errorf("%w: ZRANGE returned type %q", ErrProtocol, r.kind)
	}
	out := make([]uint64, 0, len(r.arr))
	for _, m := range r.arr {
		id, err := strconv.ParseUint(m.str, 10, 64)
		if err != nil || m.kind != '$' {
			return nil, fmt.Errorf("%w: member %q", ErrProtocol, m.str)
		}
		out = append(out, id)
	}
	return out, nil
}

// CountMissing pipelines ZSCORE for every user; a nil reply means missing.
func (v *Valkey) CountMissing(ctx context.Context, users []uint32, postID uint64) (int, error) {
	if len(users) == 0 {
		return 0, nil
	}
	member := strconv.FormatUint(postID, 10)
	cmds := make([][]string, 0, len(users))
	for _, u := range users {
		cmds = append(cmds, []string{"ZSCORE", key(u), member})
	}
	rs, err := v.pool.pipeline(ctx, cmds)
	if err != nil {
		return 0, err
	}
	missing := 0
	for _, r := range rs {
		if err := r.err(); err != nil {
			return 0, err
		}
		if r.null {
			missing++
		}
	}
	return missing, nil
}

// Close releases idle connections.
func (v *Valkey) Close() { v.pool.close() }
