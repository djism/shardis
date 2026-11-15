package cluster

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"

	"github.com/djism/shardis/internal/keyslot"
)

// Cluster bus wire format (ADR 024). Every message:
//
//	offset  size  field
//	0       4     magic "SBUS"
//	4       1     version (1)
//	5       1     type (PING, PONG, MEET, FAIL, UPDATE)
//	6       4     total length, including these 10 bytes
//	10      40    sender node ID (hex)
//	50      8     sender's current epoch
//	58      8     sender's config epoch
//	66      2     sender flags (primary / replica)
//	68      40    sender's primary ID (zeros if it is a primary)
//	108     2     sender's bus port
//	110     ...   sender client address, then internal address (1-byte length + bytes)
//	...     2048  slots the sender serves: one bit per slot (16384 bits)
//	...     2     number of gossip entries, then each entry:
//	              40 ID | 2 flags | 2 bus port | 4 ms since ping sent | 4 ms since pong
//	              received | client address | internal address
//
// Every PING and PONG carries the slot bitmap, so slot ownership spreads with
// ordinary heartbeats. 2 KB per message is the price (Redis pays the same).
// Integers are big-endian.
const (
	wireMagic   = "SBUS"
	wireVersion = 1
	prefixLen   = 10
	idLen       = 40
	slotBytes   = keyslot.Count / 8
	maxMessage  = 1 << 20 // a sanity cap: real messages are a few KB
)

// Message types.
const (
	msgPing uint8 = iota
	msgPong
	msgMeet
	msgFail   // Iteration 9
	msgUpdate // Iteration 9-10
)

var msgNames = [...]string{"ping", "pong", "meet", "fail", "update"}

// Wire flags (a subset of the node flags; see cluster.go).
const (
	wirePrimary uint16 = 1 << iota
	wireReplica
	wirePFail
	wireFail
)

type message struct {
	typ          uint8
	sender       string
	currentEpoch uint64
	configEpoch  uint64
	flags        uint16
	primaryID    string // "" if the sender is a primary
	busPort      uint16
	clientAddr   string
	internalAddr string
	slots        [slotBytes]byte
	gossip       []gossipEntry
}

// gossipEntry is what the sender knows about one other node.
type gossipEntry struct {
	id           string
	flags        uint16
	busPort      uint16
	pingSentAgo  uint32 // ms; 0 = no ping pending
	pongRecvAgo  uint32 // ms since the sender last heard from it
	clientAddr   string
	internalAddr string
}

func (m *message) hasSlot(s int) bool { return m.slots[s/8]&(1<<(s%8)) != 0 }
func (m *message) setSlot(s int)      { m.slots[s/8] |= 1 << (s % 8) }

func putID(dst []byte, id string) []byte {
	var b [idLen]byte
	copy(b[:], id)
	return append(dst, b[:]...)
}

func putStr(dst []byte, s string) []byte {
	if len(s) > 255 {
		s = s[:255]
	}
	return append(append(dst, byte(len(s))), s...)
}

func (m *message) encode() []byte {
	b := make([]byte, 0, 2400+len(m.gossip)*120)
	b = append(b, wireMagic...)
	b = append(b, wireVersion, m.typ, 0, 0, 0, 0) // length patched below
	b = putID(b, m.sender)
	b = binary.BigEndian.AppendUint64(b, m.currentEpoch)
	b = binary.BigEndian.AppendUint64(b, m.configEpoch)
	b = binary.BigEndian.AppendUint16(b, m.flags)
	b = putID(b, m.primaryID)
	b = binary.BigEndian.AppendUint16(b, m.busPort)
	b = putStr(b, m.clientAddr)
	b = putStr(b, m.internalAddr)
	b = append(b, m.slots[:]...)
	b = binary.BigEndian.AppendUint16(b, uint16(len(m.gossip)))
	for _, g := range m.gossip {
		b = putID(b, g.id)
		b = binary.BigEndian.AppendUint16(b, g.flags)
		b = binary.BigEndian.AppendUint16(b, g.busPort)
		b = binary.BigEndian.AppendUint32(b, g.pingSentAgo)
		b = binary.BigEndian.AppendUint32(b, g.pongRecvAgo)
		b = putStr(b, g.clientAddr)
		b = putStr(b, g.internalAddr)
	}
	binary.BigEndian.PutUint32(b[6:10], uint32(len(b)))
	return b
}

var errBadMessage = errors.New("malformed bus message")

// readMessage reads one framed message. The decoder trusts nothing: every
// length is checked against what's left, so a garbage or hostile peer gets
// an error, never a panic or a huge allocation (fuzzed in wire_test.go).
func readMessage(r io.Reader) (*message, int, error) {
	var prefix [prefixLen]byte
	if _, err := io.ReadFull(r, prefix[:]); err != nil {
		return nil, 0, err
	}
	if string(prefix[:4]) != wireMagic || prefix[4] != wireVersion {
		return nil, 0, fmt.Errorf("%w: bad magic/version", errBadMessage)
	}
	total := binary.BigEndian.Uint32(prefix[6:10])
	if total < prefixLen || total > maxMessage {
		return nil, 0, fmt.Errorf("%w: length %d", errBadMessage, total)
	}
	body := make([]byte, total-prefixLen)
	if _, err := io.ReadFull(r, body); err != nil {
		return nil, 0, err
	}
	m, err := decodeBody(prefix[5], body)
	return m, int(total), err
}

func decodeBody(typ uint8, b []byte) (*message, error) {
	d := decoder{b: b}
	m := &message{typ: typ}
	m.sender = d.id()
	m.currentEpoch = d.u64()
	m.configEpoch = d.u64()
	m.flags = d.u16()
	m.primaryID = d.id()
	m.busPort = d.u16()
	m.clientAddr = d.str()
	m.internalAddr = d.str()
	copy(m.slots[:], d.take(slotBytes))
	n := int(d.u16())
	if d.err == nil && n > len(d.b)/idLen { // each entry is at least idLen bytes
		return nil, fmt.Errorf("%w: %d gossip entries can't fit", errBadMessage, n)
	}
	for range n {
		g := gossipEntry{id: d.id()}
		g.flags = d.u16()
		g.busPort = d.u16()
		g.pingSentAgo = d.u32()
		g.pongRecvAgo = d.u32()
		g.clientAddr = d.str()
		g.internalAddr = d.str()
		m.gossip = append(m.gossip, g)
	}
	if d.err != nil {
		return nil, d.err
	}
	if typ > msgUpdate || m.sender == "" {
		return nil, fmt.Errorf("%w: type %d / empty sender", errBadMessage, typ)
	}
	return m, nil
}

// decoder reads fields in order and remembers the first error, so the
// decode code above stays a flat list instead of an if-err after each field.
type decoder struct {
	b   []byte
	err error
}

func (d *decoder) take(n int) []byte {
	if d.err != nil {
		return nil
	}
	if n > len(d.b) {
		d.err = fmt.Errorf("%w: truncated", errBadMessage)
		return nil
	}
	out := d.b[:n]
	d.b = d.b[n:]
	return out
}

func (d *decoder) u16() uint16 {
	if b := d.take(2); b != nil {
		return binary.BigEndian.Uint16(b)
	}
	return 0
}

func (d *decoder) u32() uint32 {
	if b := d.take(4); b != nil {
		return binary.BigEndian.Uint32(b)
	}
	return 0
}

func (d *decoder) u64() uint64 {
	if b := d.take(8); b != nil {
		return binary.BigEndian.Uint64(b)
	}
	return 0
}

// id reads a 40-byte ID field; all zeros means "none".
func (d *decoder) id() string {
	b := d.take(idLen)
	if b == nil || b[0] == 0 {
		return ""
	}
	return string(b)
}

func (d *decoder) str() string {
	b := d.take(1)
	if b == nil {
		return ""
	}
	return string(d.take(int(b[0])))
}
