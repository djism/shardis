package resp

import (
	"bufio"
	"bytes"
	"errors"
	"io"
	"math"
)

// Default limits match Redis's defaults (proto-max-bulk-len, the multibulk
// cap and the inline cap). They exist so a client can't make us allocate
// gigabytes just by *claiming* a huge length in a header.
const (
	DefaultMaxBulkLen   = 512 << 20 // 512 MB per bulk string
	DefaultMaxArrayLen  = 1 << 20   // 1M arguments per command
	DefaultMaxInlineLen = 64 << 10  // 64 KB per inline command / header line

	// maxNesting stops "*1\r\n*1\r\n*1\r\n..." from recursing until the stack
	// blows. Commands are never nested; replies rarely go past a few levels.
	maxNesting = 32

	// Bulk strings up to this size are allocated in one go. Bigger ones grow
	// as bytes actually arrive, so a lying header costs nothing.
	eagerAllocLimit = 1 << 20

	readBufSize = 16 << 10
)

// Reader decodes RESP2 from a byte stream. It is not safe for concurrent use;
// each connection has its own.
type Reader struct {
	br *bufio.Reader

	MaxBulkLen   int64
	MaxArrayLen  int64
	MaxInlineLen int

	long []byte // scratch space for lines longer than the bufio buffer
}

// NewReader wraps r with buffering and the default limits.
func NewReader(r io.Reader) *Reader {
	return &Reader{
		br:           bufio.NewReaderSize(r, readBufSize),
		MaxBulkLen:   DefaultMaxBulkLen,
		MaxArrayLen:  DefaultMaxArrayLen,
		MaxInlineLen: DefaultMaxInlineLen,
	}
}

// Buffered reports how many bytes are already read from the network but not
// yet parsed. Non-zero means "the client has sent more; don't block yet".
func (r *Reader) Buffered() int { return r.br.Buffered() }

// ReadCommand reads one client command and returns its arguments, e.g.
// [][]byte{"SET", "k", "v"}. It accepts both forms clients use:
//   - multibulk: an array of bulk strings (what redis-cli and every library send)
//   - inline: a plain text line like "PING\r\n" (what you type into telnet/nc)
//
// Empty lines and empty arrays are skipped, like Redis does. The returned
// slices are owned by the caller.
//
// Errors: io.EOF if the stream ended cleanly between commands,
// io.ErrUnexpectedEOF if it ended mid-command, *ProtocolError for bad input.
func (r *Reader) ReadCommand() ([][]byte, error) {
	for {
		first, err := r.br.Peek(1)
		if err != nil {
			return nil, err // io.EOF here is a clean close
		}
		var args [][]byte
		if first[0] == '*' {
			args, err = r.readMultiBulk()
		} else {
			args, err = r.readInline()
		}
		if err != nil {
			return nil, err
		}
		if len(args) > 0 {
			return args, nil
		}
	}
}

func (r *Reader) readMultiBulk() ([][]byte, error) {
	line, err := r.readHeader("too big mbulk count string")
	if err != nil {
		return nil, err
	}
	n, ok := parseInt(line[1:])
	if !ok || n > r.MaxArrayLen {
		return nil, protoErr("invalid multibulk length")
	}
	if n <= 0 {
		return nil, nil // "*0" and "*-1" are no-ops
	}

	// Cap the up-front allocation: the count is just a claim until the
	// arguments actually arrive.
	args := make([][]byte, 0, min(n, 1024))
	for range n {
		line, err := r.readHeader("too big bulk count string")
		if err != nil {
			return nil, err
		}
		if line[0] != '$' {
			return nil, protoErr("expected '$', got '%c'", line[0])
		}
		size, ok := parseInt(line[1:])
		if !ok || size < 0 || size > r.MaxBulkLen {
			return nil, protoErr("invalid bulk length")
		}
		arg, err := r.readBulkBody(size)
		if err != nil {
			return nil, err
		}
		args = append(args, arg)
	}
	return args, nil
}

func (r *Reader) readInline() ([][]byte, error) {
	line, err := r.readLine(r.MaxInlineLen, "too big inline request")
	if err != nil {
		return nil, err
	}
	line = bytes.TrimSuffix(line, []byte("\n"))
	line = bytes.TrimSuffix(line, []byte("\r"))
	// One copy for the whole line; Fields returns sub-slices of the copy, so
	// the arguments don't alias the reader's buffer.
	// Note: unlike Redis, we don't support "quoted strings" in inline commands.
	return bytes.Fields(bytes.Clone(line)), nil
}

// ReadValue reads one RESP2 value of any kind (replies, nested arrays, nulls).
func (r *Reader) ReadValue() (Value, error) {
	if _, err := r.br.Peek(1); err != nil {
		return Value{}, err
	}
	return r.readValue(0)
}

func (r *Reader) readValue(depth int) (Value, error) {
	line, err := r.readHeader("too big line")
	if err != nil {
		return Value{}, err
	}
	kind, body := Kind(line[0]), line[1:]
	switch kind {
	case SimpleString, Error:
		return Value{Kind: kind, Str: bytes.Clone(body)}, nil

	case Integer:
		n, ok := parseInt(body)
		if !ok {
			return Value{}, protoErr("invalid integer")
		}
		return Value{Kind: Integer, Int: n}, nil

	case BulkString:
		size, ok := parseInt(body)
		if !ok || size < -1 || size > r.MaxBulkLen {
			return Value{}, protoErr("invalid bulk length")
		}
		if size == -1 {
			return Value{Kind: BulkString, Null: true}, nil
		}
		data, err := r.readBulkBody(size)
		return Value{Kind: BulkString, Str: data}, err

	case Array:
		n, ok := parseInt(body)
		if !ok || n < -1 || n > r.MaxArrayLen {
			return Value{}, protoErr("invalid multibulk length")
		}
		if n == -1 {
			return Value{Kind: Array, Null: true}, nil
		}
		if depth >= maxNesting {
			return Value{}, protoErr("arrays nested too deeply")
		}
		elems := make([]Value, 0, min(n, 1024))
		for range n {
			v, err := r.readValue(depth + 1)
			if err != nil {
				return Value{}, err
			}
			elems = append(elems, v)
		}
		return Value{Kind: Array, Elems: elems}, nil

	default:
		return Value{}, protoErr("unknown type byte '%c'", line[0])
	}
}

// readBulkBody reads exactly size bytes plus the trailing \r\n. The
// returned slice is allocated at exactly size bytes (the CRLF is read
// separately), because the store keeps these slices as values and counts
// their memory (store/memory.go).
func (r *Reader) readBulkBody(size int64) ([]byte, error) {
	var buf []byte
	if size <= eagerAllocLimit {
		buf = make([]byte, size)
		if _, err := io.ReadFull(r.br, buf); err != nil {
			return nil, unexpected(err)
		}
	} else {
		// Grow as data arrives instead of trusting the header, then copy
		// into an exact-size slice (ReadAll leaves spare capacity).
		got, err := io.ReadAll(io.LimitReader(r.br, size))
		if err != nil {
			return nil, unexpected(err)
		}
		if int64(len(got)) < size {
			return nil, io.ErrUnexpectedEOF
		}
		buf = append(make([]byte, 0, size), got...)
	}
	// ReadByte, not ReadFull into a small array: passing the array to an
	// io.Reader makes it escape to the heap (+1 allocation per argument).
	cr, err := r.br.ReadByte()
	if err != nil {
		return nil, unexpected(err)
	}
	lf, err := r.br.ReadByte()
	if err != nil {
		return nil, unexpected(err)
	}
	if cr != '\r' || lf != '\n' {
		return nil, protoErr("bulk string not terminated by CRLF")
	}
	return buf, nil
}

// readHeader reads a "<type byte><text>\r\n" line and returns it without the
// CRLF. The result aliases the read buffer: use it before the next read.
func (r *Reader) readHeader(tooBigMsg string) ([]byte, error) {
	line, err := r.readLine(r.MaxInlineLen, tooBigMsg)
	if err != nil {
		return nil, err
	}
	if len(line) < 3 || line[len(line)-2] != '\r' {
		return nil, protoErr("line not terminated by CRLF")
	}
	return line[:len(line)-2], nil
}

// readLine returns bytes up to and including '\n', allowing lines longer than
// the bufio buffer up to limit. The result aliases internal buffers.
func (r *Reader) readLine(limit int, tooBigMsg string) ([]byte, error) {
	line, err := r.br.ReadSlice('\n')
	if err == nil {
		if len(line) > limit {
			return nil, protoErr("%s", tooBigMsg)
		}
		return line, nil
	}
	if !errors.Is(err, bufio.ErrBufferFull) {
		return nil, unexpected(err)
	}
	// Line is longer than the buffer: stitch pieces together, up to the limit.
	r.long = append(r.long[:0], line...)
	for {
		if len(r.long) > limit {
			return nil, protoErr("%s", tooBigMsg)
		}
		line, err = r.br.ReadSlice('\n')
		r.long = append(r.long, line...)
		if err == nil {
			if len(r.long) > limit {
				return nil, protoErr("%s", tooBigMsg)
			}
			return r.long, nil
		}
		if !errors.Is(err, bufio.ErrBufferFull) {
			return nil, unexpected(err)
		}
	}
}

// unexpected turns io.EOF into io.ErrUnexpectedEOF: once we've started a
// value, the stream ending is a truncation, not a clean close.
func unexpected(err error) error {
	if errors.Is(err, io.EOF) {
		return io.ErrUnexpectedEOF
	}
	return err
}

// parseInt is a strict decimal parser: optional '-', then digits only. It's
// stricter than strconv (rejects "+5", " 5", "0x5") and doesn't allocate.
func parseInt(b []byte) (int64, bool) {
	neg := len(b) > 0 && b[0] == '-'
	if neg {
		b = b[1:]
	}
	if len(b) == 0 || len(b) > 19 {
		return 0, false
	}
	var n int64
	for _, c := range b {
		if c < '0' || c > '9' {
			return 0, false
		}
		d := int64(c - '0')
		if n > (math.MaxInt64-d)/10 {
			return 0, false
		}
		n = n*10 + d
	}
	if neg {
		n = -n
	}
	return n, true
}
