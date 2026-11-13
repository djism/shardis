package resp

import (
	"bytes"
	"reflect"
	"testing"
)

func TestWriter(t *testing.T) {
	tests := []struct {
		name  string
		write func(w *Writer)
		want  string
	}{
		{"simple", func(w *Writer) { w.WriteSimple("OK") }, "+OK\r\n"},
		{"simple strips CRLF", func(w *Writer) { w.WriteSimple("a\r\nb") }, "+a  b\r\n"},
		{"error", func(w *Writer) { w.WriteError("ERR bad") }, "-ERR bad\r\n"},
		{"int", func(w *Writer) { w.WriteInt(-12) }, ":-12\r\n"},
		{"bulk", func(w *Writer) { w.WriteBulk([]byte("a\r\nb")) }, "$4\r\na\r\nb\r\n"},
		{"bulk string", func(w *Writer) { w.WriteBulkString("") }, "$0\r\n\r\n"},
		{"null", func(w *Writer) { w.WriteNull() }, "$-1\r\n"},
		{"null array", func(w *Writer) { w.WriteNullArray() }, "*-1\r\n"},
		{"array", func(w *Writer) { w.WriteArrayHeader(2); w.WriteInt(1); w.WriteSimple("x") }, "*2\r\n:1\r\n+x\r\n"},
		{"command", func(w *Writer) { w.WriteCommand([]byte("GET"), []byte("k")) }, "*2\r\n$3\r\nGET\r\n$1\r\nk\r\n"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var buf bytes.Buffer
			w := NewWriter(&buf)
			tc.write(w)
			if buf.Len() != 0 {
				t.Fatal("wrote to the network before Flush")
			}
			if err := w.Flush(); err != nil {
				t.Fatal(err)
			}
			if got := buf.String(); got != tc.want {
				t.Errorf("got %q, want %q", got, tc.want)
			}
		})
	}
}

// Whatever WriteValue produces, ReadValue must decode back to the same thing.
func TestValueRoundTrip(t *testing.T) {
	values := []Value{
		{Kind: SimpleString, Str: []byte("PONG")},
		{Kind: Error, Str: []byte("MOVED 3999 127.0.0.1:7001")},
		{Kind: Integer, Int: 1 << 40},
		{Kind: BulkString, Str: []byte("\x00\xff\r\n")},
		{Kind: BulkString, Null: true},
		{Kind: Array, Null: true},
		{Kind: Array, Elems: []Value{{Kind: Integer, Int: 0}, {Kind: BulkString, Null: true}}},
	}
	var buf bytes.Buffer
	w := NewWriter(&buf)
	for _, v := range values {
		w.WriteValue(v)
	}
	w.Flush()
	r := NewReader(&buf)
	for _, want := range values {
		got, err := r.ReadValue()
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(got, want) {
			t.Errorf("got %v, want %v", got, want)
		}
	}
}
