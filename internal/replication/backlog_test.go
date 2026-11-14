package replication

import (
	"bytes"
	"sync/atomic"
	"testing"
	"time"
)

func readAll(t *testing.T, b *backlog, off int64, n int) []byte {
	t.Helper()
	out := make([]byte, 0, n)
	buf := make([]byte, 7) // small reads to exercise wrap-around
	for len(out) < n {
		got, err := b.read(off, buf, func() bool { return false })
		if err != nil {
			t.Fatal(err)
		}
		out = append(out, buf[:got]...)
		off += int64(got)
	}
	return out
}

func TestBacklogWrapsAndTracksOffsets(t *testing.T) {
	b := newBacklog(1024)
	b.append([]byte("before")) // no buffer yet: only the offset moves
	if s, e := b.offsets(); s != 6 || e != 6 || b.active() {
		t.Fatalf("offsets %d,%d before enable", s, e)
	}
	b.enable()
	var all []byte
	for i := range 300 { // 300 * ~10 bytes: wraps the 1 KB ring a few times
		p := []byte("rec-" + string(rune('a'+i%26)) + "-xyz|")
		all = append(all, p...)
		b.append(p)
	}
	start, end := b.offsets()
	if end != 6+int64(len(all)) || end-start != 1024 {
		t.Fatalf("offsets %d,%d", start, end)
	}
	got := readAll(t, b, start, int(end-start))
	if !bytes.Equal(got, all[len(all)-1024:]) {
		t.Fatal("ring contents don't match the last 1024 bytes")
	}
	if _, err := b.read(start-1, make([]byte, 8), func() bool { return false }); err != errTooOld {
		t.Fatalf("read before start: %v", err)
	}
	if !b.covers(start) || b.covers(start-1) || !b.covers(end) {
		t.Fatal("covers wrong")
	}
}

func TestBacklogAppendBiggerThanRing(t *testing.T) {
	b := newBacklog(1024)
	b.enable()
	big := bytes.Repeat([]byte("0123456789"), 300)
	b.append(big)
	start, end := b.offsets()
	if !bytes.Equal(readAll(t, b, start, int(end-start)), big[len(big)-1024:]) {
		t.Fatal("oversized append kept the wrong bytes")
	}
}

func TestBacklogReadWaitsAndStops(t *testing.T) {
	b := newBacklog(1024)
	b.enable()
	var stop atomic.Bool
	done := make(chan error, 1)
	go func() {
		_, err := b.read(0, make([]byte, 8), stop.Load)
		done <- err
	}()
	b.append([]byte("hi")) // a waiting reader wakes up for new data
	if err := <-done; err != nil {
		t.Fatalf("read after append: %v", err)
	}
	go func() {
		_, err := b.read(2, make([]byte, 8), stop.Load) // nothing new: blocks
		done <- err
	}()
	select {
	case err := <-done:
		t.Fatalf("read returned %v without new data", err)
	case <-time.After(30 * time.Millisecond):
	}
	stop.Store(true)
	b.wake()
	select {
	case err := <-done:
		if err != errStop {
			t.Fatalf("got %v, want errStop", err)
		}
	case <-time.After(time.Second):
		t.Fatal("stop didn't wake the reader")
	}
}
