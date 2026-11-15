package store

import (
	"encoding/binary"
	"errors"
	"hash/crc64"
)

// DUMP / RESTORE payload (ADR 034): an opaque, self-checking encoding of one
// value, used by MIGRATE to move keys between nodes.
//
//	[0]      format version (dumpVersion)
//	[1]      value type (0 = string; the only type so far)
//	uvarint  value length, then the value bytes
//	[8]      CRC-64 (ECMA) of everything before it, little-endian
//
// The TTL isn't in the payload: RESTORE takes it as an argument, like Redis.
// This is our own format, not Redis's RDB encoding. Both ends of a MIGRATE are
// Shardis, and the version byte lets a newer node refuse a payload it
// doesn't understand instead of misreading it.

const (
	dumpVersion    = 1
	dumpTypeString = 0
)

var crcTable = crc64.MakeTable(crc64.ECMA)

// ErrBadDump means the payload is corrupt, truncated or from an unknown version.
var ErrBadDump = errors.New("DUMP payload version or checksum are wrong")

// Dump encodes a string value.
func Dump(val []byte) []byte {
	p := make([]byte, 0, 2+binary.MaxVarintLen64+len(val)+8)
	p = append(p, dumpVersion, dumpTypeString)
	p = binary.AppendUvarint(p, uint64(len(val)))
	p = append(p, val...)
	return binary.LittleEndian.AppendUint64(p, crc64.Checksum(p, crcTable))
}

// ParseDump checks and decodes a payload made by Dump. The returned value is
// a fresh copy (the store keeps it).
func ParseDump(p []byte) ([]byte, error) {
	if len(p) < 2+1+8 {
		return nil, ErrBadDump
	}
	body, sum := p[:len(p)-8], binary.LittleEndian.Uint64(p[len(p)-8:])
	if crc64.Checksum(body, crcTable) != sum || body[0] != dumpVersion {
		return nil, ErrBadDump
	}
	if body[1] != dumpTypeString {
		return nil, errors.New("Bad data format")
	}
	n, used := binary.Uvarint(body[2:])
	if used <= 0 || uint64(len(body)-2-used) != n {
		return nil, ErrBadDump
	}
	return append([]byte(nil), body[2+used:]...), nil
}
