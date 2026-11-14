package persist

import (
	"bufio"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

// The AOF is split into files, like Redis 7's multi-part AOF:
//
//	shardis.aof.3.base.aof    a snapshot: one SET per key (written by a rewrite)
//	shardis.aof.3.incr.aof    effects since that snapshot
//	shardis.aof.4.incr.aof    (during a rewrite, writes already go here)
//	shardis.aof.manifest      which of the above make up the current state, in order
//
// Replaying the manifest's files in order rebuilds the keyspace. The manifest
// is only ever replaced atomically (write a temp file, fsync, rename, fsync
// the directory), so after a crash it always names a complete, valid set.
const (
	subdir       = "appendonlydir"
	prefix       = "shardis.aof"
	manifestName = prefix + ".manifest"
)

type aofFile struct {
	name string
	seq  int
	base bool
}

type manifest struct {
	base  *aofFile  // nil until the first rewrite
	incrs []aofFile // oldest first; the last one is the one being appended to
}

func baseName(seq int) string { return fmt.Sprintf("%s.%d.base.aof", prefix, seq) }
func incrName(seq int) string { return fmt.Sprintf("%s.%d.incr.aof", prefix, seq) }

// files returns every file to replay, in order.
func (m *manifest) files() []aofFile {
	var out []aofFile
	if m.base != nil {
		out = append(out, *m.base)
	}
	return append(out, m.incrs...)
}

func (m *manifest) maxSeq() int {
	n := 0
	for _, f := range m.files() {
		n = max(n, f.seq)
	}
	return n
}

func (m *manifest) current() aofFile { return m.incrs[len(m.incrs)-1] }

// encode writes one line per file: "file <name> seq <n> type <b|i>",
// the same shape as Redis's manifest.
func (m *manifest) encode() []byte {
	var b strings.Builder
	for _, f := range m.files() {
		t := "i"
		if f.base {
			t = "b"
		}
		fmt.Fprintf(&b, "file %s seq %d type %s\n", f.name, f.seq, t)
	}
	return []byte(b.String())
}

func parseManifest(data []byte) (*manifest, error) {
	m := &manifest{}
	sc := bufio.NewScanner(strings.NewReader(string(data)))
	for line := 1; sc.Scan(); line++ {
		text := strings.TrimSpace(sc.Text())
		if text == "" {
			continue
		}
		f := strings.Fields(text)
		if len(f) != 6 || f[0] != "file" || f[2] != "seq" || f[4] != "type" {
			return nil, fmt.Errorf("manifest line %d: malformed %q", line, text)
		}
		seq, err := strconv.Atoi(f[3])
		if err != nil || strings.ContainsAny(f[1], `/\`) {
			return nil, fmt.Errorf("manifest line %d: bad entry %q", line, text)
		}
		switch f[5] {
		case "b":
			if m.base != nil || len(m.incrs) > 0 {
				return nil, fmt.Errorf("manifest line %d: base file must come first and only once", line)
			}
			m.base = &aofFile{name: f[1], seq: seq, base: true}
		case "i":
			m.incrs = append(m.incrs, aofFile{name: f[1], seq: seq})
		default:
			return nil, fmt.Errorf("manifest line %d: unknown type %q", line, f[5])
		}
	}
	if len(m.incrs) == 0 {
		return nil, errors.New("manifest has no incremental file")
	}
	return m, nil
}

// loadManifest reads the manifest in dir. ok is false if there isn't one
// (a fresh data directory).
func loadManifest(dir string) (m *manifest, ok bool, err error) {
	data, err := os.ReadFile(filepath.Join(dir, manifestName))
	if errors.Is(err, fs.ErrNotExist) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	m, err = parseManifest(data)
	return m, err == nil, err
}

// writeManifest atomically replaces the manifest.
func writeManifest(dir string, m *manifest) error {
	return writeFileAtomic(dir, manifestName, m.encode())
}

// writeFileAtomic writes data so that after a crash the file holds either
// the old contents or the new ones, never a mix: write a temp file, fsync it,
// rename it over the target (atomic on POSIX), then fsync the directory so
// the rename itself is durable.
func writeFileAtomic(dir, name string, data []byte) error {
	tmp := filepath.Join(dir, name+".tmp")
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	if _, err := f.Write(data); err != nil {
		f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmp, filepath.Join(dir, name)); err != nil {
		return err
	}
	return syncDir(dir)
}

// syncDir fsyncs a directory, which makes file creations, renames and
// deletions inside it durable. Without it, a crash can "un-rename" a file.
func syncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}
