// Package resp reads and writes RESP2, the Redis wire protocol.
//
// Every RESP2 value starts with one type byte and ends with \r\n:
//
//	+OK\r\n                   simple string (no \r or \n inside)
//	-ERR bad thing\r\n        error
//	:42\r\n                   integer
//	$5\r\nhello\r\n           bulk string: length first, then exactly that many bytes
//	$-1\r\n                   null bulk string
//	*2\r\n$3\r\nGET\r\n$1\r\nk\r\n   array: count first, then that many values
//
// Clients send commands as arrays of bulk strings. Because bulk strings are
// length-prefixed, the payload can contain any bytes (even \r\n), and the
// parser never has to scan the payload looking for a terminator.
//
// Implemented from the public spec at https://redis.io/docs/latest/develop/reference/protocol-spec/
package resp

import "fmt"

// Kind is the RESP2 type byte.
type Kind byte

const (
	SimpleString Kind = '+'
	Error        Kind = '-'
	Integer      Kind = ':'
	BulkString   Kind = '$'
	Array        Kind = '*'
)

// Value is one decoded RESP2 value of any kind. Only the fields that match
// Kind are meaningful. The server's hot path uses ReadCommand instead, which
// skips this struct; Value is for replies, tests and (later) our own client.
type Value struct {
	Kind  Kind
	Str   []byte  // SimpleString, Error, BulkString
	Int   int64   // Integer
	Elems []Value // Array
	Null  bool    // null BulkString ($-1) or null Array (*-1)
}

func (v Value) String() string {
	switch {
	case v.Null:
		return fmt.Sprintf("%c(nil)", v.Kind)
	case v.Kind == Integer:
		return fmt.Sprintf(":%d", v.Int)
	case v.Kind == Array:
		return fmt.Sprintf("*%v", v.Elems)
	default:
		return fmt.Sprintf("%c%q", v.Kind, v.Str)
	}
}

// ProtocolError means the peer sent bytes that aren't valid RESP. The
// connection can't be trusted after that (we no longer know where the next
// message starts), so the server replies with an error and hangs up.
type ProtocolError struct{ Msg string }

func (e *ProtocolError) Error() string { return "resp: protocol error: " + e.Msg }

func protoErr(format string, args ...any) error {
	return &ProtocolError{Msg: fmt.Sprintf(format, args...)}
}
