package client

import (
	"strings"
	"testing"
	"time"

	"github.com/djism/shardis/internal/resp"
)

func TestParseRedirect(t *testing.T) {
	for _, tc := range []struct {
		in   string
		slot uint16
		addr string
		ok   bool
	}{
		{"MOVED 3999 127.0.0.1:6381", 3999, "127.0.0.1:6381", true},
		{"ASK 0 10.0.0.5:7000", 0, "10.0.0.5:7000", true},
		{"MOVED 16384 h:1", 0, "", false},
		{"MOVED x h:1", 0, "", false},
		{"MOVED 1", 0, "", false},
	} {
		s, a, ok := parseRedirect([]byte(tc.in))
		if s != tc.slot || a != tc.addr || ok != tc.ok {
			t.Errorf("%q: got %d %q %v", tc.in, s, a, ok)
		}
	}
}

func TestBackoff(t *testing.T) {
	top := func(n int64) int64 { return n - 1 } // the largest wait jitter can pick
	base, maxWait := 5*time.Millisecond, 500*time.Millisecond
	want := []time.Duration{5, 10, 20, 40, 80, 160, 320, 500, 500}
	for i, w := range want {
		if got := backoff(i, base, maxWait, top); got != w*time.Millisecond {
			t.Errorf("attempt %d: max wait %v, want %v", i, got, w*time.Millisecond)
		}
	}
	if got := backoff(1000, base, maxWait, top); got != maxWait {
		t.Errorf("huge attempt number: %v (shift overflow?)", got)
	}
	low := func(int64) int64 { return 0 }
	if got := backoff(5, base, maxWait, low); got <= 0 || got > time.Microsecond {
		t.Errorf("full jitter's smallest wait = %v", got)
	}
}

func bs(s string) [][]byte {
	var out [][]byte
	for _, f := range strings.Fields(s) {
		out = append(out, []byte(f))
	}
	return out
}

func TestKeyPositions(t *testing.T) {
	cmd := func(name string, flag string, first, last, step int) resp.Value {
		return resp.Value{Kind: resp.Array, Elems: []resp.Value{
			{Kind: resp.BulkString, Str: []byte(name)}, {Kind: resp.Integer, Int: -2},
			{Kind: resp.Array, Elems: []resp.Value{{Kind: resp.SimpleString, Str: []byte(flag)}}},
			{Kind: resp.Integer, Int: int64(first)}, {Kind: resp.Integer, Int: int64(last)}, {Kind: resp.Integer, Int: int64(step)},
		}}
	}
	table := parseCommandTable(resp.Value{Kind: resp.Array, Elems: []resp.Value{
		cmd("get", "readonly", 1, 1, 1), cmd("mset", "write", 1, -1, 2), cmd("ping", "fast", 0, 0, 0),
	}})
	if !table["get"].readonly || table["mset"].readonly {
		t.Fatal("flags not parsed")
	}
	got := table["mset"].keys(bs("MSET a 1 b 2 c 3"))
	if len(got) != 3 || string(got[0]) != "a" || string(got[2]) != "c" {
		t.Fatalf("MSET keys = %q", got)
	}
	if k := table["ping"].keys(bs("PING")); k != nil {
		t.Fatalf("PING has keys %q", k)
	}
}

func TestSafeToResend(t *testing.T) {
	ro, rw := cmdInfo{first: 1, last: 1, step: 1, readonly: true}, cmdInfo{first: 1, last: 1, step: 1}
	for _, tc := range []struct {
		cmd  string
		ci   cmdInfo
		safe bool
	}{
		{"GET k", ro, true},
		{"SET k v", rw, true},
		{"SET k v EX 10", rw, true},
		{"SET k v NX", rw, false}, // the 2nd run would answer "not set"
		{"SET k v GET", rw, false},
		{"DEL k", rw, true},
		{"INCR k", rw, false}, // applied twice = wrong count
		{"EXPIRE k 10", rw, false},
		{"FOO k", rw, false}, // unknown: assume unsafe
	} {
		args := bs(tc.cmd)
		if got := safeToResend(lowerName(args[0]), tc.ci, args); got != tc.safe {
			t.Errorf("%s: safe = %v, want %v", tc.cmd, got, tc.safe)
		}
	}
}
