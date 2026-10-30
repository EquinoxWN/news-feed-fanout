// Package event encodes the messages that travel through the fan-out topics.
// The format is fixed-size binary: version, type, then big-endian fields.
package event

import (
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/EquinoxWN/news-feed-fanout/internal/ids"
)

const (
	version byte = 1

	typePostCreated byte = 1
	typeFanoutTask  byte = 2

	postCreatedLen = 2 + 8 + 4
	fanoutTaskLen  = 2 + 8 + 4 + 4 + 4

	// MaxChunk bounds the followers covered by one fan-out task.
	MaxChunk = 1 << 16
)

// ErrMalformed is returned for bytes that are not a valid event.
var ErrMalformed = errors.New("event: malformed")

// PostCreated says a post was stored and must be fanned out.
type PostCreated struct {
	PostID uint64
	Author uint32
}

// FanoutTask asks a writer to push PostID into followers [Offset, Offset+Count) of Author.
type FanoutTask struct {
	PostID uint64
	Author uint32
	Offset uint32
	Count  uint32
}

// Encode returns the wire form of e.
func (e PostCreated) Encode() []byte {
	b := make([]byte, postCreatedLen)
	b[0], b[1] = version, typePostCreated
	binary.BigEndian.PutUint64(b[2:], e.PostID)
	binary.BigEndian.PutUint32(b[10:], e.Author)
	return b
}

// Encode returns the wire form of t.
func (t FanoutTask) Encode() []byte {
	b := make([]byte, fanoutTaskLen)
	b[0], b[1] = version, typeFanoutTask
	binary.BigEndian.PutUint64(b[2:], t.PostID)
	binary.BigEndian.PutUint32(b[10:], t.Author)
	binary.BigEndian.PutUint32(b[14:], t.Offset)
	binary.BigEndian.PutUint32(b[18:], t.Count)
	return b
}

// header checks length, version and type.
func header(b []byte, typ byte, n int) error {
	if len(b) != n {
		return fmt.Errorf("%w: length %d, want %d", ErrMalformed, len(b), n)
	}
	if b[0] != version || b[1] != typ {
		return fmt.Errorf("%w: version %d type %d", ErrMalformed, b[0], b[1])
	}
	return nil
}

// DecodePostCreated parses and validates a PostCreated message.
func DecodePostCreated(b []byte) (PostCreated, error) {
	if err := header(b, typePostCreated, postCreatedLen); err != nil {
		return PostCreated{}, err
	}
	e := PostCreated{PostID: binary.BigEndian.Uint64(b[2:]), Author: binary.BigEndian.Uint32(b[10:])}
	if e.PostID > ids.MaxID {
		return PostCreated{}, fmt.Errorf("%w: post id %d out of range", ErrMalformed, e.PostID)
	}
	return e, nil
}

// DecodeFanoutTask parses and validates a FanoutTask message.
func DecodeFanoutTask(b []byte) (FanoutTask, error) {
	if err := header(b, typeFanoutTask, fanoutTaskLen); err != nil {
		return FanoutTask{}, err
	}
	t := FanoutTask{
		PostID: binary.BigEndian.Uint64(b[2:]),
		Author: binary.BigEndian.Uint32(b[10:]),
		Offset: binary.BigEndian.Uint32(b[14:]),
		Count:  binary.BigEndian.Uint32(b[18:]),
	}
	switch {
	case t.PostID > ids.MaxID:
		return FanoutTask{}, fmt.Errorf("%w: post id %d out of range", ErrMalformed, t.PostID)
	case t.Count == 0 || t.Count > MaxChunk:
		return FanoutTask{}, fmt.Errorf("%w: count %d", ErrMalformed, t.Count)
	case uint64(t.Offset)+uint64(t.Count) > 1<<32-1:
		return FanoutTask{}, fmt.Errorf("%w: offset overflow", ErrMalformed)
	}
	return t, nil
}
