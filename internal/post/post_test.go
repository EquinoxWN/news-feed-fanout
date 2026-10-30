package post

import (
	"context"
	"errors"
	"strings"
	"testing"
)

func TestPutGetAndStoredOnce(t *testing.T) {
	ctx := context.Background()
	s := NewMemStore()
	p := Post{ID: 7, Author: 1, Body: "hello"}
	if err := s.Put(ctx, p); err != nil {
		t.Fatal(err)
	}
	if err := s.Put(ctx, p); !errors.Is(err, ErrExists) {
		t.Fatalf("second Put: %v, want ErrExists", err)
	}
	got, err := s.Get(ctx, 7)
	if err != nil || got.Body != "hello" {
		t.Fatalf("Get = %+v, %v", got, err)
	}
	if s.Len() != 1 || s.BodyBytes() != 5 {
		t.Fatalf("Len=%d BodyBytes=%d, want 1 and 5", s.Len(), s.BodyBytes())
	}
}

func TestGetUnknown(t *testing.T) {
	if _, err := NewMemStore().Get(context.Background(), 1); !errors.Is(err, ErrNotFound) {
		t.Fatalf("got %v", err)
	}
}

func TestGetManyKeepsOrderAndSkipsUnknown(t *testing.T) {
	ctx := context.Background()
	s := NewMemStore()
	for _, id := range []uint64{1, 2, 3} {
		if err := s.Put(ctx, Post{ID: id, Body: "b"}); err != nil {
			t.Fatal(err)
		}
	}
	got, _ := s.GetMany(ctx, []uint64{3, 9, 1})
	if len(got) != 2 || got[0].ID != 3 || got[1].ID != 1 {
		t.Fatalf("GetMany = %+v", got)
	}
}

func TestValidateRejectsBadBodies(t *testing.T) {
	for name, body := range map[string]string{
		"empty":    "",
		"too long": strings.Repeat("x", MaxBodyBytes+1),
		"bad utf8": "\xff\xfe",
	} {
		if err := Validate(body); !errors.Is(err, ErrInvalid) {
			t.Errorf("%s: %v, want ErrInvalid", name, err)
		}
		if err := NewMemStore().Put(context.Background(), Post{ID: 1, Body: body}); !errors.Is(err, ErrInvalid) {
			t.Errorf("%s: Put accepted it: %v", name, err)
		}
	}
	if err := Validate(strings.Repeat("é", MaxBodyBytes/2)); err != nil {
		t.Fatalf("limit-sized body rejected: %v", err)
	}
}
