package event

import (
	"errors"
	"testing"

	"github.com/EquinoxWN/news-feed-fanout/internal/ids"
)

func TestRoundTrip(t *testing.T) {
	pc := PostCreated{PostID: ids.MaxID, Author: 1<<32 - 1}
	if got, err := DecodePostCreated(pc.Encode()); err != nil || got != pc {
		t.Fatalf("PostCreated: %+v, %v", got, err)
	}
	ft := FanoutTask{PostID: 42, Author: 3, Offset: 5000, Count: MaxChunk}
	if got, err := DecodeFanoutTask(ft.Encode()); err != nil || got != ft {
		t.Fatalf("FanoutTask: %+v, %v", got, err)
	}
}

func TestRejectsMalformed(t *testing.T) {
	good := FanoutTask{PostID: 1, Author: 1, Count: 1}.Encode()
	cases := map[string][]byte{
		"empty":       nil,
		"short":       good[:5],
		"long":        append(append([]byte{}, good...), 0),
		"bad version": append([]byte{9}, good[1:]...),
		"wrong type":  PostCreated{PostID: 1}.Encode(),
		"zero count":  FanoutTask{PostID: 1, Count: 0}.Encode(),
		"huge count":  FanoutTask{PostID: 1, Count: MaxChunk + 1}.Encode(),
		"overflow":    FanoutTask{PostID: 1, Offset: 1<<32 - 1, Count: 1}.Encode(),
		"id too big":  FanoutTask{PostID: ids.MaxID + 1, Count: 1}.Encode(),
	}
	for name, b := range cases {
		if _, err := DecodeFanoutTask(b); !errors.Is(err, ErrMalformed) {
			t.Errorf("%s: %v, want ErrMalformed", name, err)
		}
	}
	if _, err := DecodePostCreated(PostCreated{PostID: ids.MaxID + 1}.Encode()); !errors.Is(err, ErrMalformed) {
		t.Errorf("PostCreated id too big: %v", err)
	}
}

func FuzzDecode(f *testing.F) {
	f.Add(PostCreated{PostID: 1, Author: 2}.Encode())
	f.Add(FanoutTask{PostID: 1, Author: 2, Offset: 3, Count: 4}.Encode())
	f.Fuzz(func(t *testing.T, b []byte) {
		if e, err := DecodePostCreated(b); err == nil {
			if again, err := DecodePostCreated(e.Encode()); err != nil || again != e {
				t.Fatalf("PostCreated not stable: %+v", e)
			}
		}
		if e, err := DecodeFanoutTask(b); err == nil {
			if e.Count == 0 || e.Count > MaxChunk {
				t.Fatalf("accepted count %d", e.Count)
			}
		}
	})
}
