package replication

import (
	"errors"
	"sync"
)

var (
	errTooOld = errors.New("offset no longer in the backlog")
	errStop   = errors.New("stopped")
)

// backlog is a ring buffer of the most recent replication stream bytes,
// addressed by stream offset (ADR 017). It serves two purposes:
//
//   - Live feed: every replica's sender goroutine reads from it at its own
//     offset. One buffer for all replicas, so memory stays bounded however
//     many there are; a replica that falls so far behind that its next byte
//     has been overwritten gets dropped and must do a full resync.
//   - Partial resync: a replica that reconnects asking for offset X gets
//     just the missing bytes, if X is still in the buffer.
//
// end is the stream offset: total bytes ever appended (the primary's
// master_repl_offset). Until the first replica connects, no buffer is
// allocated and only end advances.
type backlog struct {
	mu    sync.Mutex
	cond  *sync.Cond
	size  int64
	buf   []byte
	start int64 // oldest offset still in buf
	end   int64 // next offset to be written
}

func newBacklog(size int64) *backlog {
	b := &backlog{size: max(size, 1024)}
	b.cond = sync.NewCond(&b.mu)
	return b
}

// enable allocates the buffer (first replica). History before now isn't kept.
func (b *backlog) enable() {
	b.mu.Lock()
	if b.buf == nil {
		b.buf = make([]byte, b.size)
		b.start = b.end
	}
	b.mu.Unlock()
}

func (b *backlog) active() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf != nil
}

// reset empties the buffer and restarts it at offset off (a replica after a
// full sync: its history now begins where the primary's snapshot did).
func (b *backlog) reset(off int64) {
	b.mu.Lock()
	if b.buf == nil {
		b.buf = make([]byte, b.size)
	}
	b.start, b.end = off, off
	b.cond.Broadcast()
	b.mu.Unlock()
}

func (b *backlog) append(p []byte) {
	b.mu.Lock()
	n := int64(len(p))
	if b.buf == nil {
		b.end += n
		b.start = b.end
		b.mu.Unlock()
		return
	}
	off := b.end
	if n > b.size { // only the last size bytes can survive anyway
		off += n - b.size
		p = p[n-b.size:]
	}
	for len(p) > 0 {
		c := copy(b.buf[off%b.size:], p)
		p = p[c:]
		off += int64(c)
	}
	b.end += n
	if b.end-b.start > b.size {
		b.start = b.end - b.size
	}
	b.cond.Broadcast()
	b.mu.Unlock()
}

// offsets returns [start, end).
func (b *backlog) offsets() (start, end int64) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.start, b.end
}

// covers reports whether a reader could continue from off.
func (b *backlog) covers(off int64) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf != nil && off >= b.start && off <= b.end
}

// read copies stream bytes starting at off into dst, waiting until there are
// some. It returns errTooOld if off has been overwritten, and errStop as
// soon as stopped() is true (callers wake it with wake()).
func (b *backlog) read(off int64, dst []byte, stopped func() bool) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	for off >= b.end {
		if stopped() {
			return 0, errStop
		}
		b.cond.Wait()
	}
	if stopped() {
		return 0, errStop
	}
	if b.buf == nil || off < b.start {
		return 0, errTooOld
	}
	n := min(int64(len(dst)), b.end-off)
	got := int64(0)
	for got < n {
		i := (off + got) % b.size
		c := copy(dst[got:n], b.buf[i:])
		got += int64(c)
	}
	return int(n), nil
}

// wake makes every blocked read re-check its stop condition.
func (b *backlog) wake() {
	b.mu.Lock()
	b.cond.Broadcast()
	b.mu.Unlock()
}
