package resp

import (
	"bufio"
	"io"
	"strconv"
	"strings"
)

const writeBufSize = 16 << 10

// Writer encodes RESP2 replies into a buffer. Nothing reaches the network
// until Flush (or until the buffer fills). That's deliberate: the server
// decides *when* to flush, which is what makes pipelining cheap (ADR 003).
//
// Write methods don't return errors. bufio.Writer remembers the first error
// and returns it from every later call, so checking once at Flush is enough.
type Writer struct {
	bw  *bufio.Writer
	num []byte // scratch for formatting integers without allocating
}

func NewWriter(w io.Writer) *Writer {
	return &Writer{bw: bufio.NewWriterSize(w, writeBufSize), num: make([]byte, 0, 24)}
}

// Flush sends everything buffered so far.
func (w *Writer) Flush() error { return w.bw.Flush() }

// Buffered reports how many reply bytes are waiting to be flushed.
func (w *Writer) Buffered() int { return w.bw.Buffered() }

// WriteSimple writes +s\r\n. CR/LF in s are replaced with spaces, because a
// newline inside a simple string would end it early and corrupt the stream.
func (w *Writer) WriteSimple(s string) { w.writeLine('+', s) }

// WriteError writes -msg\r\n. By convention msg starts with an uppercase
// code like "ERR" or "WRONGTYPE" (later: "MOVED", "ASK"), which clients parse.
func (w *Writer) WriteError(msg string) { w.writeLine('-', msg) }

func (w *Writer) WriteInt(n int64) { w.writePrefixed(':', n) }

// WriteBulk writes $len\r\n<bytes>\r\n. Any bytes are allowed.
func (w *Writer) WriteBulk(b []byte) {
	w.writePrefixed('$', int64(len(b)))
	w.bw.Write(b)
	w.bw.WriteString("\r\n")
}

func (w *Writer) WriteBulkString(s string) {
	w.writePrefixed('$', int64(len(s)))
	w.bw.WriteString(s)
	w.bw.WriteString("\r\n")
}

// WriteNull writes the null bulk string, which is RESP2's "no value" (e.g. GET on a missing key).
func (w *Writer) WriteNull() { w.bw.WriteString("$-1\r\n") }

// WriteNullArray writes *-1\r\n.
func (w *Writer) WriteNullArray() { w.bw.WriteString("*-1\r\n") }

// WriteArrayHeader starts an array; the caller then writes exactly n values.
func (w *Writer) WriteArrayHeader(n int) { w.writePrefixed('*', int64(n)) }

// WriteCommand encodes args the way clients send commands: an array of bulk strings.
func (w *Writer) WriteCommand(args ...[]byte) {
	w.WriteArrayHeader(len(args))
	for _, a := range args {
		w.WriteBulk(a)
	}
}

// WriteValue encodes any Value, recursing into arrays.
func (w *Writer) WriteValue(v Value) {
	switch v.Kind {
	case SimpleString:
		w.WriteSimple(string(v.Str))
	case Error:
		w.WriteError(string(v.Str))
	case Integer:
		w.WriteInt(v.Int)
	case BulkString:
		if v.Null {
			w.WriteNull()
		} else {
			w.WriteBulk(v.Str)
		}
	case Array:
		if v.Null {
			w.WriteNullArray()
			return
		}
		w.WriteArrayHeader(len(v.Elems))
		for _, e := range v.Elems {
			w.WriteValue(e)
		}
	}
}

func (w *Writer) writeLine(prefix byte, s string) {
	if strings.ContainsAny(s, "\r\n") {
		s = strings.NewReplacer("\r", " ", "\n", " ").Replace(s)
	}
	w.bw.WriteByte(prefix)
	w.bw.WriteString(s)
	w.bw.WriteString("\r\n")
}

func (w *Writer) writePrefixed(prefix byte, n int64) {
	w.num = append(w.num[:0], prefix)
	w.num = strconv.AppendInt(w.num, n, 10)
	w.num = append(w.num, '\r', '\n')
	w.bw.Write(w.num)
}
