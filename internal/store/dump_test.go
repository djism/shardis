package store

import (
	"bytes"
	"errors"
	"testing"
)

func TestDumpRoundTrip(t *testing.T) {
	for _, v := range [][]byte{{}, []byte("x"), bytes.Repeat([]byte("ab\x00\r\n"), 10000)} {
		got, err := ParseDump(Dump(v))
		if err != nil || !bytes.Equal(got, v) {
			t.Fatalf("round trip of %d bytes: got %d bytes, err %v", len(v), len(got), err)
		}
	}
}

func TestDumpRejectsDamage(t *testing.T) {
	p := Dump([]byte("hello world"))
	for i := range p {
		bad := bytes.Clone(p)
		bad[i] ^= 0x40
		if _, err := ParseDump(bad); err == nil {
			t.Fatalf("flipping a bit at byte %d was not detected", i)
		}
	}
	for n := range len(p) {
		if _, err := ParseDump(p[:n]); err == nil {
			t.Fatalf("truncated to %d bytes was accepted", n)
		}
	}
	if _, err := ParseDump(append(bytes.Clone(p), 0)); !errors.Is(err, ErrBadDump) {
		t.Fatalf("trailing byte: err %v", err)
	}
}

func FuzzParseDump(f *testing.F) {
	f.Add(Dump([]byte("seed")))
	f.Add([]byte{})
	f.Fuzz(func(t *testing.T, p []byte) {
		v, err := ParseDump(p)
		if err == nil && !bytes.Equal(Dump(v), p) {
			t.Fatalf("accepted a payload that doesn't re-encode to itself")
		}
	})
}
