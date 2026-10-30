package main

import (
	"bytes"
	"context"
	"strings"
	"testing"
)

func TestDemoEndToEndInProcess(t *testing.T) {
	var out bytes.Buffer
	err := run(context.Background(), []string{"demo", "-users", "30000", "-posts", "300", "-celebrities", "2", "-chunk", "500"}, &out)
	if err != nil {
		t.Fatalf("%v\n%s", err, out.String())
	}
	s := out.String()
	for _, want := range []string{"stored once: 302 rows", "0 missing", "PASS", "write amplification"} {
		if !strings.Contains(s, want) {
			t.Errorf("output lacks %q:\n%s", want, s)
		}
	}
}

func TestGraphCommand(t *testing.T) {
	var out bytes.Buffer
	if err := run(context.Background(), []string{"graph", "-users", "10000", "-max-follows", "500"}, &out); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out.String(), "#1 user 0:") {
		t.Fatalf("unexpected output:\n%s", out.String())
	}
}

func TestBadArguments(t *testing.T) {
	for _, args := range [][]string{
		nil,
		{"nope"},
		{"graph", "-users", "1"},
		{"demo", "-posts", "0", "-celebrities", "0"},
		{"demo", "-users", "1000", "-chunk", "0"},
		{"graph", "-bogus"},
	} {
		if err := run(context.Background(), args, &bytes.Buffer{}); err == nil {
			t.Errorf("%v accepted", args)
		}
	}
}
