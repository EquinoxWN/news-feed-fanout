package timeline

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"net"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
)

// fakeValkey is a loopback RESP2 server with just the sorted-set commands the adapter uses.
type fakeValkey struct {
	ln   net.Listener
	mu   sync.Mutex
	sets map[string]map[string]float64
	wg   sync.WaitGroup
}

// startFake listens on 127.0.0.1 and serves until the test ends.
func startFake(t *testing.T) *fakeValkey {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	f := &fakeValkey{ln: ln, sets: map[string]map[string]float64{}}
	f.wg.Go(func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			f.wg.Go(func() { f.serve(c) })
		}
	})
	t.Cleanup(func() { _ = ln.Close(); f.wg.Wait() })
	return f
}

// serve answers commands on one connection.
func (f *fakeValkey) serve(c net.Conn) {
	defer c.Close()
	r, w := newByteReader(c), bufio.NewWriter(c)
	for {
		cmd, err := readReply(r, 0)
		if err != nil {
			return
		}
		args := make([]string, len(cmd.arr))
		for i, a := range cmd.arr {
			args[i] = a.str
		}
		f.mu.Lock()
		f.exec(w, args)
		f.mu.Unlock()
		if r.r == r.w {
			_ = w.Flush()
		}
	}
}

// sorted returns members of key ascending by score.
func (f *fakeValkey) sorted(key string) []string {
	var ms []string
	for m := range f.sets[key] {
		ms = append(ms, m)
	}
	slices.SortFunc(ms, func(a, b string) int {
		sa, sb := f.sets[key][a], f.sets[key][b]
		switch {
		case sa < sb:
			return -1
		case sa > sb:
			return 1
		}
		return strings.Compare(a, b)
	})
	return ms
}

// exec runs one command and writes its reply.
func (f *fakeValkey) exec(w *bufio.Writer, a []string) {
	switch strings.ToUpper(a[0]) {
	case "PING":
		w.WriteString("+PONG\r\n")
	case "ZADD":
		s, _ := strconv.ParseFloat(a[2], 64)
		if f.sets[a[1]] == nil {
			f.sets[a[1]] = map[string]float64{}
		}
		_, had := f.sets[a[1]][a[3]]
		f.sets[a[1]][a[3]] = s
		w.WriteString(":" + map[bool]string{true: "0", false: "1"}[had] + "\r\n")
	case "ZREMRANGEBYRANK":
		stop, _ := strconv.Atoi(a[3])
		ms := f.sorted(a[1])
		end := len(ms) + stop // negative stop counts from the end
		n := 0
		for i := 0; i <= end && i < len(ms); i++ {
			delete(f.sets[a[1]], ms[i])
			n++
		}
		w.WriteString(":" + strconv.Itoa(n) + "\r\n")
	case "ZRANGE": // ZRANGE key +inf -inf BYSCORE REV LIMIT 0 n
		n, _ := strconv.Atoi(a[8])
		ms := f.sorted(a[1])
		slices.Reverse(ms)
		ms = ms[:min(n, len(ms))]
		w.WriteString("*" + strconv.Itoa(len(ms)) + "\r\n")
		for _, m := range ms {
			w.WriteString("$" + strconv.Itoa(len(m)) + "\r\n" + m + "\r\n")
		}
	case "ZSCORE":
		if s, ok := f.sets[a[1]][a[2]]; ok {
			v := strconv.FormatFloat(s, 'f', -1, 64)
			w.WriteString("$" + strconv.Itoa(len(v)) + "\r\n" + v + "\r\n")
		} else {
			w.WriteString("$-1\r\n")
		}
	default:
		w.WriteString("-ERR unknown command\r\n")
	}
}

func TestValkeyAdapterAgainstFakeServer(t *testing.T) {
	f := startFake(t)
	ctx := context.Background()
	v, err := NewValkey(f.ln.Addr().String(), 3)
	if err != nil {
		t.Fatal(err)
	}
	defer v.Close()
	for _, id := range []uint64{10, 30, 20, 30, 40} {
		if err := v.AddBatch(ctx, []uint32{1, 2}, id); err != nil {
			t.Fatal(err)
		}
	}
	got, err := v.Latest(ctx, 1, 10)
	if err != nil || !slices.Equal(got, []uint64{40, 30, 20}) {
		t.Fatalf("Latest = %v, %v (want newest 3, trimmed)", got, err)
	}
	if miss, err := v.CountMissing(ctx, []uint32{1, 2, 3}, 40); err != nil || miss != 1 {
		t.Fatalf("CountMissing = %d, %v", miss, err)
	}
	if miss, _ := v.CountMissing(ctx, []uint32{1}, 10); miss != 1 {
		t.Fatal("trimmed entry still present")
	}
	if got, _ := v.Latest(ctx, 9, 5); len(got) != 0 {
		t.Fatalf("empty timeline = %v", got)
	}
}

func TestValkeyConcurrentPipelines(t *testing.T) {
	f := startFake(t)
	v, err := NewValkey(f.ln.Addr().String(), 1000)
	if err != nil {
		t.Fatal(err)
	}
	defer v.Close()
	users := []uint32{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}
	var wg sync.WaitGroup
	for w := range 12 {
		wg.Go(func() {
			for i := range 20 {
				if err := v.AddBatch(context.Background(), users, uint64(w*100+i)); err != nil {
					t.Error(err)
				}
			}
		})
	}
	wg.Wait()
	got, _ := v.Latest(context.Background(), 5, 1000)
	if len(got) != 240 {
		t.Fatalf("user 5 has %d entries, want 240", len(got))
	}
}

func TestValkeyServerErrorAndDialFailure(t *testing.T) {
	f := startFake(t)
	v, _ := NewValkey(f.ln.Addr().String(), 3)
	defer v.Close()
	rs, err := v.pool.pipeline(context.Background(), [][]string{{"NOPE"}})
	if err != nil {
		t.Fatal(err)
	}
	var se ServerError
	if !errors.As(rs[0].err(), &se) {
		t.Fatalf("error reply not surfaced: %+v", rs[0])
	}
	ln, _ := net.Listen("tcp", "127.0.0.1:0")
	addr := ln.Addr().String()
	_ = ln.Close()
	if _, err := NewValkey(addr, 3); err == nil {
		t.Fatal("dial to a closed port succeeded")
	}
}

func TestReadReplyRejectsMalformed(t *testing.T) {
	for name, in := range map[string]string{
		"bare LF":       "+OK\n",
		"bad type":      "!x\r\n",
		"empty":         "\r\n",
		"bad integer":   ":abc\r\n",
		"huge bulk":     "$99999999\r\n",
		"short bulk":    "$5\r\nab\r\n",
		"bulk no CRLF":  "$2\r\nabXY",
		"huge array":    "*99999999\r\n",
		"deep nesting":  "*1\r\n*1\r\n*1\r\n*1\r\n*1\r\n*1\r\n:1\r\n",
		"long line":     "+" + strings.Repeat("a", maxLine+10) + "\r\n",
		"truncated":     "*2\r\n:1\r\n",
		"negative bulk": "$-5\r\n",
	} {
		if _, err := readReply(newByteReader(strings.NewReader(in)), 0); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
	r, err := readReply(newByteReader(strings.NewReader("*3\r\n$1\r\na\r\n$-1\r\n:7\r\n")), 0)
	if err != nil || len(r.arr) != 3 || r.arr[0].str != "a" || !r.arr[1].null || r.arr[2].num != 7 {
		t.Fatalf("valid array decoded as %+v, %v", r, err)
	}
}

func FuzzReadReply(f *testing.F) {
	f.Add([]byte("*2\r\n$1\r\na\r\n:5\r\n"))
	f.Add([]byte("$-1\r\n"))
	f.Add([]byte("-ERR x\r\n"))
	f.Fuzz(func(t *testing.T, b []byte) {
		_, _ = readReply(newByteReader(bytes.NewReader(b)), 0)
	})
}
