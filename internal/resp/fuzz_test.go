package resp

import (
	"bytes"
	"errors"
	"io"
	"testing"
)

// Fuzz tests feed the parser random bytes for as long as you let them run:
//
//	go test ./internal/resp -run '^$' -fuzz FuzzReadCommand -fuzztime 60s
//
// The seeds below also run as ordinary tests in `make test`. Anything the
// fuzzer finds gets saved under testdata/fuzz/ and becomes a permanent test.

func seedCorpus(f *testing.F) {
	for _, s := range []string{
		"*1\r\n$4\r\nPING\r\n",
		"*2\r\n$4\r\nECHO\r\n$3\r\na\r\n\r\n",
		"PING\r\nECHO hi\n",
		"*-1\r\n*0\r\n",
		"+OK\r\n-ERR x\r\n:5\r\n$-1\r\n*2\r\n:1\r\n*1\r\n+x\r\n",
		"*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$99999\r\nshort",
		"*1\r\n*1\r\n*1\r\n",
	} {
		f.Add([]byte(s))
	}
}

// smallReader uses tiny limits so the fuzzer can hit the limit paths quickly.
func smallReader(data []byte) *Reader {
	r := NewReader(bytes.NewReader(data))
	r.MaxBulkLen, r.MaxArrayLen, r.MaxInlineLen = 1<<12, 64, 256
	return r
}

// Property: on any input, ReadCommand either returns a command, a clean EOF,
// or an error. It never panics, never loops forever, never returns an empty command.
func FuzzReadCommand(f *testing.F) {
	seedCorpus(f)
	f.Fuzz(func(t *testing.T, data []byte) {
		r := smallReader(data)
		// Each successful read consumes at least one byte, so this terminates.
		for range len(data) + 1 {
			args, err := r.ReadCommand()
			if err != nil {
				checkKnownError(t, err)
				return
			}
			if len(args) == 0 {
				t.Fatal("returned an empty command")
			}
		}
		t.Fatal("more commands than input bytes")
	})
}

func FuzzReadValue(f *testing.F) {
	seedCorpus(f)
	f.Fuzz(func(t *testing.T, data []byte) {
		r := smallReader(data)
		for range len(data) + 1 {
			if _, err := r.ReadValue(); err != nil {
				checkKnownError(t, err)
				return
			}
		}
		t.Fatal("more values than input bytes")
	})
}

// Property: encoding any list of arguments and decoding it gives back the same list.
func FuzzCommandRoundTrip(f *testing.F) {
	f.Add([]byte("SET key value"), byte(' '))
	f.Add([]byte("a\r\nb\x00c"), byte('\n'))
	f.Fuzz(func(t *testing.T, data []byte, sep byte) {
		args := bytes.Split(data, []byte{sep}) // always at least one element
		var buf bytes.Buffer
		w := NewWriter(&buf)
		w.WriteCommand(args...)
		if err := w.Flush(); err != nil {
			t.Fatal(err)
		}
		got, err := NewReader(&buf).ReadCommand()
		if err != nil {
			t.Fatalf("decode: %v", err)
		}
		if len(got) != len(args) {
			t.Fatalf("got %d args, want %d", len(got), len(args))
		}
		for i := range args {
			if !bytes.Equal(got[i], args[i]) {
				t.Fatalf("arg %d: got %q, want %q", i, got[i], args[i])
			}
		}
	})
}

func checkKnownError(t *testing.T, err error) {
	t.Helper()
	var pe *ProtocolError
	if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) || errors.As(err, &pe) {
		return
	}
	t.Fatalf("unexpected error type %T: %v", err, err)
}
