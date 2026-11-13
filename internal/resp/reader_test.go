package resp

import (
	"bytes"
	"errors"
	"io"
	"reflect"
	"strings"
	"testing"
	"testing/iotest"
)

func bs(ss ...string) [][]byte {
	out := make([][]byte, len(ss))
	for i, s := range ss {
		out[i] = []byte(s)
	}
	return out
}

func TestReadCommand(t *testing.T) {
	tests := []struct {
		name string
		in   string
		want [][]byte
	}{
		{"multibulk", "*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$5\r\nhello\r\n", bs("SET", "k", "hello")},
		{"binary-safe bulk with CRLF inside", "*2\r\n$4\r\nECHO\r\n$4\r\na\r\nb\r\n", bs("ECHO", "a\r\nb")},
		{"empty bulk", "*2\r\n$4\r\nECHO\r\n$0\r\n\r\n", bs("ECHO", "")},
		{"inline", "PING\r\n", bs("PING")},
		{"inline LF only", "ECHO hi\n", bs("ECHO", "hi")},
		{"inline extra spaces", "  ECHO   a \t b  \r\n", bs("ECHO", "a", "b")},
		{"skips empty lines", "\r\n\n\r\nPING\r\n", bs("PING")},
		{"skips empty arrays", "*0\r\n*-1\r\n*1\r\n$4\r\nPING\r\n", bs("PING")},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := NewReader(strings.NewReader(tc.in)).ReadCommand()
			if err != nil {
				t.Fatalf("ReadCommand: %v", err)
			}
			if !reflect.DeepEqual(got, tc.want) {
				t.Errorf("got %q, want %q", got, tc.want)
			}
		})
	}
}

// Real networks deliver bytes in arbitrary chunks. Feeding one byte per Read
// proves the parser never assumes a whole command arrives at once.
func TestReadCommandOneByteAtATime(t *testing.T) {
	in := "*2\r\n$4\r\nECHO\r\n$5\r\nhello\r\nPING\r\n*1\r\n$4\r\nPING\r\n"
	r := NewReader(iotest.OneByteReader(strings.NewReader(in)))
	for _, want := range [][][]byte{bs("ECHO", "hello"), bs("PING"), bs("PING")} {
		got, err := r.ReadCommand()
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("got %q, want %q", got, want)
		}
	}
	if _, err := r.ReadCommand(); err != io.EOF {
		t.Fatalf("want io.EOF at end, got %v", err)
	}
}

func TestReadCommandErrors(t *testing.T) {
	tests := []struct {
		name    string
		in      string
		wantMsg string // "" means a non-protocol error is expected (see wantErr)
		wantErr error
	}{
		{"bad array length", "*abc\r\n", "invalid multibulk length", nil},
		{"array too long", "*2000000\r\n", "invalid multibulk length", nil},
		{"plus sign rejected", "*+1\r\n", "invalid multibulk length", nil},
		{"bad bulk length", "*1\r\n$x\r\n", "invalid bulk length", nil},
		{"negative bulk in command", "*1\r\n$-1\r\n", "invalid bulk length", nil},
		{"bulk too long", "*1\r\n$999999999999\r\n", "invalid bulk length", nil},
		{"not a bulk", "*1\r\n:1\r\n", "expected '$', got ':'", nil},
		{"bulk missing CRLF", "*1\r\n$4\r\nPINGxx", "bulk string not terminated by CRLF", nil},
		{"header missing CR", "*1\n", "line not terminated by CRLF", nil},
		{"inline too long", strings.Repeat("a", DefaultMaxInlineLen+10) + "\r\n", "too big inline request", nil},
		{"empty stream", "", "", io.EOF},
		{"truncated header", "*1\r\n$4\r", "", io.ErrUnexpectedEOF},
		{"truncated body", "*1\r\n$4\r\nPI", "", io.ErrUnexpectedEOF},
		{"truncated inline", "PING", "", io.ErrUnexpectedEOF},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := NewReader(strings.NewReader(tc.in)).ReadCommand()
			checkErr(t, err, tc.wantMsg, tc.wantErr)
		})
	}
}

func checkErr(t *testing.T, err error, wantMsg string, wantErr error) {
	t.Helper()
	if wantErr != nil {
		if !errors.Is(err, wantErr) {
			t.Fatalf("got %v, want %v", err, wantErr)
		}
		return
	}
	var pe *ProtocolError
	if !errors.As(err, &pe) {
		t.Fatalf("got %v, want ProtocolError %q", err, wantMsg)
	}
	if pe.Msg != wantMsg {
		t.Fatalf("got message %q, want %q", pe.Msg, wantMsg)
	}
}

func TestLimitsAreConfigurable(t *testing.T) {
	r := NewReader(strings.NewReader("*3\r\n"))
	r.MaxArrayLen = 2
	_, err := r.ReadCommand()
	checkErr(t, err, "invalid multibulk length", nil)

	r = NewReader(strings.NewReader("*1\r\n$11\r\n"))
	r.MaxBulkLen = 10
	_, err = r.ReadCommand()
	checkErr(t, err, "invalid bulk length", nil)
}

// A client claiming a 100 MB argument but sending nothing must not cost us
// 100 MB. We can't measure allocation directly here, but we can check the
// large-body path handles truncation correctly.
func TestLargeBulkTruncated(t *testing.T) {
	_, err := NewReader(strings.NewReader("*1\r\n$100000000\r\nabc")).ReadCommand()
	checkErr(t, err, "", io.ErrUnexpectedEOF)
}

func TestLargeBulkOK(t *testing.T) {
	payload := bytes.Repeat([]byte("x"), eagerAllocLimit+5)
	var buf bytes.Buffer
	w := NewWriter(&buf)
	w.WriteCommand([]byte("SET"), payload)
	w.Flush()
	got, err := NewReader(&buf).ReadCommand()
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got[1], payload) {
		t.Fatal("payload mismatch")
	}
}

func TestReadValue(t *testing.T) {
	tests := []struct {
		in   string
		want Value
	}{
		{"+OK\r\n", Value{Kind: SimpleString, Str: []byte("OK")}},
		{"-ERR nope\r\n", Value{Kind: Error, Str: []byte("ERR nope")}},
		{":-42\r\n", Value{Kind: Integer, Int: -42}},
		{"$3\r\nfoo\r\n", Value{Kind: BulkString, Str: []byte("foo")}},
		{"$-1\r\n", Value{Kind: BulkString, Null: true}},
		{"*-1\r\n", Value{Kind: Array, Null: true}},
		{"*0\r\n", Value{Kind: Array, Elems: []Value{}}},
		{"*2\r\n:1\r\n*1\r\n+x\r\n", Value{Kind: Array, Elems: []Value{
			{Kind: Integer, Int: 1},
			{Kind: Array, Elems: []Value{{Kind: SimpleString, Str: []byte("x")}}},
		}}},
	}
	for _, tc := range tests {
		t.Run(strings.ReplaceAll(tc.in, "\r\n", `\r\n`), func(t *testing.T) {
			got, err := NewReader(strings.NewReader(tc.in)).ReadValue()
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(got, tc.want) {
				t.Errorf("got %v, want %v", got, tc.want)
			}
		})
	}
}

func TestReadValueErrors(t *testing.T) {
	tests := []struct{ in, wantMsg string }{
		{"?x\r\n", "unknown type byte '?'"},
		{":12a\r\n", "invalid integer"},
		{":99999999999999999999\r\n", "invalid integer"},
		{"$-2\r\n", "invalid bulk length"},
		{strings.Repeat("*1\r\n", maxNesting+1) + ":1\r\n", "arrays nested too deeply"},
	}
	for _, tc := range tests {
		_, err := NewReader(strings.NewReader(tc.in)).ReadValue()
		checkErr(t, err, tc.wantMsg, nil)
	}
}

func TestParseInt(t *testing.T) {
	good := map[string]int64{"0": 0, "7": 7, "-1": -1, "9223372036854775807": 1<<63 - 1}
	for s, want := range good {
		if got, ok := parseInt([]byte(s)); !ok || got != want {
			t.Errorf("parseInt(%q) = %d, %v", s, got, ok)
		}
	}
	for _, s := range []string{"", "-", "+1", " 1", "1 ", "0x1", "9223372036854775808", "1.0"} {
		if _, ok := parseInt([]byte(s)); ok {
			t.Errorf("parseInt(%q) should fail", s)
		}
	}
}

func BenchmarkReadCommand(b *testing.B) {
	cmd := []byte("*3\r\n$3\r\nSET\r\n$8\r\nkey:1234\r\n$16\r\nvalue-0123456789\r\n")
	// One op = parsing one command. The reader is recreated every 1000 commands.
	const batch = 1000
	in := bytes.Repeat(cmd, batch)
	b.SetBytes(int64(len(cmd)))
	b.ReportAllocs()
	r := NewReader(bytes.NewReader(in))
	i := 0
	for b.Loop() {
		if i == batch {
			r, i = NewReader(bytes.NewReader(in)), 0
		}
		if _, err := r.ReadCommand(); err != nil {
			b.Fatal(err)
		}
		i++
	}
}
