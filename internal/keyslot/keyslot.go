// Package keyslot maps keys to one of Redis Cluster's 16384 hash slots.
//
// slot = CRC16(key) mod 16384, using the CRC16-XMODEM variant named in the
// Redis Cluster spec. Using the exact same function as Redis is what lets
// real cluster clients (redis-cli -c, go-redis) compute the right node for a
// key themselves.
//
// Hash tags: if a key contains "{...}" with something inside, only that part
// is hashed. So "{user:1}:cart" and "{user:1}:profile" land in the same slot,
// which is what makes multi-key commands possible in a cluster.
package keyslot

// Count is the number of hash slots. Fixed by the Redis Cluster spec.
const Count = 16384

// crcTable is computed at startup from the polynomial, not hard-coded, so the
// whole algorithm is visible here. Each entry is the CRC of one byte value.
var crcTable = makeTable(0x1021)

// makeTable builds a lookup table for a CRC16 with the given polynomial,
// MSB-first (no bit reflection), which is the XMODEM variant.
func makeTable(poly uint16) [256]uint16 {
	var t [256]uint16
	for b := range 256 {
		crc := uint16(b) << 8
		for range 8 {
			if crc&0x8000 != 0 {
				crc = crc<<1 ^ poly
			} else {
				crc <<= 1
			}
		}
		t[b] = crc
	}
	return t
}

// CRC16 is CRC16-XMODEM: polynomial 0x1021, initial value 0, no reflection,
// no final XOR. Standard check value: CRC16("123456789") = 0x31C3.
func CRC16(data []byte) uint16 {
	var crc uint16
	for _, b := range data {
		crc = crc<<8 ^ crcTable[byte(crc>>8)^b]
	}
	return crc
}

// Of returns the hash slot for key, honoring hash tags.
func Of(key []byte) uint16 {
	return CRC16(HashPart(key)) & (Count - 1) // Count is a power of two, so & == mod
}

// HashPart returns the part of key that gets hashed. Rules from the spec:
// find the first '{'; find the first '}' after it; if there's at least one
// byte between them, hash only those bytes. Otherwise hash the whole key.
//
//	"{user1000}.following" → "user1000"
//	"foo{}{bar}"           → whole key (first {} is empty)
//	"foo{{bar}}zap"        → "{bar"
//	"foo{bar}{zap}"        → "bar"
func HashPart(key []byte) []byte {
	for i, c := range key {
		if c != '{' {
			continue
		}
		for j := i + 1; j < len(key); j++ {
			if key[j] == '}' {
				if j == i+1 {
					return key // empty tag "{}": hash everything
				}
				return key[i+1 : j]
			}
		}
		return key // '{' with no closing '}'
	}
	return key
}
