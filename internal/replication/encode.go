package replication

import "strconv"

// encode writes args as a RESP array of bulk strings, the canonical form the
// store uses for effects. A replica re-encodes each record it receives into
// its own backlog; because the encoding is canonical, the bytes (and so the
// offsets) are identical to the primary's.
func encode(dst []byte, args ...[]byte) []byte {
	dst = append(dst, '*')
	dst = strconv.AppendInt(dst, int64(len(args)), 10)
	dst = append(dst, '\r', '\n')
	for _, a := range args {
		dst = append(dst, '$')
		dst = strconv.AppendInt(dst, int64(len(a)), 10)
		dst = append(dst, '\r', '\n')
		dst = append(dst, a...)
		dst = append(dst, '\r', '\n')
	}
	return dst
}

func encodeStrings(args ...string) []byte {
	b := make([][]byte, len(args))
	for i, a := range args {
		b[i] = []byte(a)
	}
	return encode(nil, b...)
}

var (
	pingRecord        = encodeStrings("PING")
	getackRecord      = encodeStrings("REPLCONF", "GETACK", "*")
	snapshotEndRecord = encodeStrings("SNAPSHOT-END")
)
