package timeline

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"strconv"
	"time"
)

// Limits on what the RESP reader accepts from the server.
const (
	maxLine      = 512
	maxBulk      = 1 << 20
	maxArray     = 1 << 20
	maxPoolConns = 8
	dialTimeout  = 5 * time.Second
	ioTimeout    = 30 * time.Second
)

// ErrProtocol is returned for a reply that is not valid RESP2 or breaks a limit.
var ErrProtocol = errors.New("timeline: bad RESP reply")

// ServerError is an error reply ("-ERR ...") from the server.
type ServerError string

// Error returns the server's message.
func (e ServerError) Error() string { return "timeline: server: " + string(e) }

// reply is one decoded RESP2 value.
type reply struct {
	kind byte // '+', ':', '$', '*', '-'
	str  string
	num  int64
	arr  []reply
	null bool
}

// conn is one TCP connection speaking RESP2.
type conn struct {
	c   *net.TCPConn
	r   *byteReader
	out []byte
}

// byteReader is a minimal buffered reader. It is used instead of bufio so the
// reader only ever wraps a TCP connection (or a test source), never a TLS one.
type byteReader struct {
	src  io.Reader
	buf  []byte
	r, w int
}

// newByteReader buffers src with a 64 KiB buffer.
func newByteReader(src io.Reader) *byteReader {
	return &byteReader{src: src, buf: make([]byte, 64<<10)}
}

// fill reads more bytes once the buffer is drained.
func (b *byteReader) fill() error {
	b.r, b.w = 0, 0
	n, err := b.src.Read(b.buf)
	b.w = n
	if n > 0 {
		return nil
	}
	if err == nil {
		err = io.ErrNoProgress
	}
	return err
}

// ReadByte returns the next byte.
func (b *byteReader) ReadByte() (byte, error) {
	if b.r == b.w {
		if err := b.fill(); err != nil {
			return 0, err
		}
	}
	c := b.buf[b.r]
	b.r++
	return c, nil
}

// readFull fills p completely or returns an error.
func (b *byteReader) readFull(p []byte) error {
	for len(p) > 0 {
		if b.r == b.w {
			if err := b.fill(); err != nil {
				return err
			}
		}
		n := copy(p, b.buf[b.r:b.w])
		b.r += n
		p = p[n:]
	}
	return nil
}

// pool keeps up to maxPoolConns idle connections to one address.
type pool struct {
	addr string
	idle chan *conn
}

// newPool returns an empty pool for addr.
func newPool(addr string) *pool { return &pool{addr: addr, idle: make(chan *conn, maxPoolConns)} }

// get returns an idle connection or dials a new one.
func (p *pool) get(ctx context.Context) (*conn, error) {
	select {
	case c := <-p.idle:
		return c, nil
	default:
	}
	d := net.Dialer{Timeout: dialTimeout}
	nc, err := d.DialContext(ctx, "tcp", p.addr)
	if err != nil {
		return nil, fmt.Errorf("timeline: dial %s: %w", p.addr, err)
	}
	c, ok := nc.(*net.TCPConn)
	if !ok {
		_ = nc.Close()
		return nil, fmt.Errorf("timeline: %s is not a TCP connection", p.addr)
	}
	return &conn{c: c, r: newByteReader(c)}, nil
}

// put returns a healthy connection to the pool or closes it when full.
func (p *pool) put(c *conn) {
	select {
	case p.idle <- c:
	default:
		_ = c.c.Close()
	}
}

// close closes every idle connection.
func (p *pool) close() {
	for {
		select {
		case c := <-p.idle:
			_ = c.c.Close()
		default:
			return
		}
	}
}

// pipeline sends all commands, then reads one reply per command.
func (p *pool) pipeline(ctx context.Context, cmds [][]string) ([]reply, error) {
	c, err := p.get(ctx)
	if err != nil {
		return nil, err
	}
	deadline := time.Now().Add(ioTimeout)
	if d, ok := ctx.Deadline(); ok && d.Before(deadline) {
		deadline = d
	}
	_ = c.c.SetDeadline(deadline)
	out, err := c.roundTrip(cmds)
	if err != nil {
		_ = c.c.Close() // the stream position is unknown now
		return nil, err
	}
	p.put(c)
	return out, nil
}

// roundTrip writes cmds and reads len(cmds) replies.
func (c *conn) roundTrip(cmds [][]string) ([]reply, error) {
	c.out = c.out[:0]
	for _, cmd := range cmds {
		c.out = appendCommand(c.out, cmd)
	}
	if _, err := c.c.Write(c.out); err != nil {
		return nil, fmt.Errorf("timeline: write: %w", err)
	}
	out := make([]reply, len(cmds))
	for i := range out {
		r, err := readReply(c.r, 0)
		if err != nil {
			return nil, err
		}
		out[i] = r
	}
	return out, nil
}

// appendCommand encodes cmd as a RESP array of bulk strings.
func appendCommand(out []byte, cmd []string) []byte {
	out = append(out, '*')
	out = strconv.AppendInt(out, int64(len(cmd)), 10)
	out = append(out, "\r\n"...)
	for _, a := range cmd {
		out = append(out, '$')
		out = strconv.AppendInt(out, int64(len(a)), 10)
		out = append(out, "\r\n"...)
		out = append(out, a...)
		out = append(out, "\r\n"...)
	}
	return out
}

// readLine reads one CRLF-terminated line of at most maxLine bytes.
func readLine(r *byteReader) (string, error) {
	var buf []byte
	for {
		b, err := r.ReadByte()
		if err != nil {
			return "", fmt.Errorf("timeline: read: %w", err)
		}
		if b == '\n' {
			if len(buf) == 0 || buf[len(buf)-1] != '\r' {
				return "", fmt.Errorf("%w: bare LF", ErrProtocol)
			}
			return string(buf[:len(buf)-1]), nil
		}
		if len(buf) >= maxLine {
			return "", fmt.Errorf("%w: line longer than %d", ErrProtocol, maxLine)
		}
		buf = append(buf, b)
	}
}

// readReply decodes one RESP2 value; nesting deeper than 4 is refused.
func readReply(r *byteReader, depth int) (reply, error) {
	if depth > 4 {
		return reply{}, fmt.Errorf("%w: nested too deep", ErrProtocol)
	}
	line, err := readLine(r)
	if err != nil {
		return reply{}, err
	}
	if line == "" {
		return reply{}, fmt.Errorf("%w: empty line", ErrProtocol)
	}
	kind, rest := line[0], line[1:]
	switch kind {
	case '+', '-':
		return reply{kind: kind, str: rest}, nil
	case ':':
		n, err := strconv.ParseInt(rest, 10, 64)
		if err != nil {
			return reply{}, fmt.Errorf("%w: integer %q", ErrProtocol, rest)
		}
		return reply{kind: kind, num: n}, nil
	case '$':
		n, err := strconv.Atoi(rest)
		if err != nil || n < -1 || n > maxBulk {
			return reply{}, fmt.Errorf("%w: bulk length %q", ErrProtocol, rest)
		}
		if n == -1 {
			return reply{kind: kind, null: true}, nil
		}
		buf := make([]byte, n+2)
		if err := r.readFull(buf); err != nil {
			return reply{}, fmt.Errorf("timeline: read: %w", err)
		}
		if buf[n] != '\r' || buf[n+1] != '\n' {
			return reply{}, fmt.Errorf("%w: bulk not CRLF-terminated", ErrProtocol)
		}
		return reply{kind: kind, str: string(buf[:n])}, nil
	case '*':
		n, err := strconv.Atoi(rest)
		if err != nil || n < -1 || n > maxArray {
			return reply{}, fmt.Errorf("%w: array length %q", ErrProtocol, rest)
		}
		if n == -1 {
			return reply{kind: kind, null: true}, nil
		}
		arr := make([]reply, 0, min(n, 1024))
		for range n {
			e, err := readReply(r, depth+1)
			if err != nil {
				return reply{}, err
			}
			arr = append(arr, e)
		}
		return reply{kind: kind, arr: arr}, nil
	default:
		return reply{}, fmt.Errorf("%w: type byte %q", ErrProtocol, kind)
	}
}

// err converts an error reply into a ServerError.
func (r reply) err() error {
	if r.kind == '-' {
		return ServerError(r.str)
	}
	return nil
}
