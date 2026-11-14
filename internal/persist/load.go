package persist

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"github.com/djism/shardis/internal/resp"
	"github.com/djism/shardis/internal/store"
)

// LoadStats describes what Load replayed.
type LoadStats struct {
	Files     int
	Records   int64
	Bytes     int64
	Truncated int64 // bytes cut off a torn last record (crash mid-write)
	Took      time.Duration
}

// countingReader counts bytes read from the file, so we can tell where the
// last complete record ended.
type countingReader struct {
	r io.Reader
	n int64
}

func (c *countingReader) Read(p []byte) (int, error) {
	n, err := c.r.Read(p)
	c.n += int64(n)
	return n, err
}

// Load replays the manifest's files into e. Call it before e.SetLog, so the
// replayed effects aren't appended to the log again.
//
// A crash can cut the last record of the last file in half. That record was
// never acknowledged (replies wait for write()), so dropping it is safe: we
// truncate the file back to the last complete record and log a warning.
// Garbage anywhere else means real corruption, and we refuse to start rather
// than silently load wrong data.
func (a *AOF) Load(e store.Engine) (LoadStats, error) {
	start := time.Now()
	var st LoadStats
	files := a.man.files()
	for i, f := range files {
		last := i == len(files)-1
		n, err := a.loadFile(e, f.name, last, &st)
		if err != nil {
			return st, err
		}
		st.Files++
		st.Bytes += n
	}
	st.Took = time.Since(start)
	a.loaded = st
	return st, nil
}

const loadBatch = 4096 // records applied per engine call

func (a *AOF) loadFile(e store.Engine, name string, last bool, st *LoadStats) (int64, error) {
	path := filepath.Join(a.dir, name)
	f, err := os.Open(path)
	if errors.Is(err, os.ErrNotExist) && last {
		return 0, nil // the current incr file is created lazily by Start
	}
	if err != nil {
		return 0, err
	}
	defer f.Close()
	cr := &countingReader{r: f}
	r := resp.NewReader(cr)
	var batch [][][]byte
	var good int64 // offset just past the last complete record
	flush := func() error {
		var applyErr error
		e.DoAll(func(tx *store.Tx) {
			for _, rec := range batch {
				if err := applyEffect(tx, rec); err != nil && applyErr == nil {
					applyErr = err
				}
			}
		})
		st.Records += int64(len(batch))
		batch = batch[:0]
		return applyErr
	}
	for {
		rec, err := r.ReadCommand()
		if err == io.EOF {
			break
		}
		if errors.Is(err, io.ErrUnexpectedEOF) && last {
			if err := flush(); err != nil {
				return 0, fmt.Errorf("aof %s: %w", name, err)
			}
			size := cr.n
			if err := os.Truncate(path, good); err != nil {
				return 0, fmt.Errorf("aof %s: truncating torn record: %w", name, err)
			}
			st.Truncated = size - good
			a.log.Warn("AOF ended with an incomplete record (crash mid-write?); truncated it",
				"file", name, "bytes", st.Truncated, "at", good)
			return good, nil
		}
		if err != nil {
			return 0, fmt.Errorf("aof %s is corrupt near byte %d: %w (refusing to load; move the file aside or repair it)", name, good, err)
		}
		good = cr.n - int64(r.Buffered())
		batch = append(batch, rec)
		if len(batch) == loadBatch {
			if err := flush(); err != nil {
				return 0, fmt.Errorf("aof %s near byte %d: %w", name, good, err)
			}
		}
	}
	if err := flush(); err != nil {
		return 0, fmt.Errorf("aof %s: %w", name, err)
	}
	return good, nil
}

// applyEffect replays one effect record. These are exactly the records the
// store writes (effects.go). Deadlines are absolute, so a key whose deadline
// passed while the server was down is simply not loaded.
func applyEffect(tx *store.Tx, rec [][]byte) error {
	cmd := string(bytes.ToUpper(rec[0]))
	switch {
	case cmd == "SET" && (len(rec) == 3 || len(rec) == 5):
		var at int64
		if len(rec) == 5 {
			n, err := strconv.ParseInt(string(rec[4]), 10, 64)
			if err != nil || !bytes.EqualFold(rec[3], []byte("PXAT")) {
				return fmt.Errorf("bad SET record")
			}
			at = n
		}
		if at != 0 && at <= tx.Now() {
			tx.Delete(rec[1]) // expired while we were down
			return nil
		}
		tx.SetWithExpire(rec[1], rec[2], at)
	case cmd == "DEL" && len(rec) >= 2:
		for _, k := range rec[1:] {
			tx.Delete(k)
		}
	case cmd == "PEXPIREAT" && len(rec) == 3:
		at, err := strconv.ParseInt(string(rec[2]), 10, 64)
		if err != nil {
			return fmt.Errorf("bad PEXPIREAT record")
		}
		tx.SetExpire(rec[1], max(at, 1)) // a past deadline deletes the key
	case cmd == "PERSIST" && len(rec) == 2:
		tx.SetExpire(rec[1], 0)
	case cmd == "FLUSHALL" && len(rec) == 1:
		tx.Flush()
	default:
		return fmt.Errorf("unknown AOF record %q with %d args", rec[0], len(rec))
	}
	return nil
}
