package keyslot

import (
	"encoding/json"
	"fmt"
	"os"
	"testing"
)

// The website computes slots in the browser (website/src/lib/keyslot.ts).
// Both sides check the same vectors, so they can't silently disagree.
// SHARDIS_WRITE_VECTORS=1 regenerates the file from this implementation.
const vectorFile = "../../website/src/lib/keyslot.vectors.json"

type vector struct {
	Key  string `json:"key"`
	CRC  uint16 `json:"crc16"` // of the hashed part (the tag, if any)
	Slot uint16 `json:"slot"`
}

func vectorKeys() []string {
	keys := []string{"", "a", "foo", "bar", "hello", "123456789", "user:1000", "user:1001",
		"{user1000}.following", "{user1000}.followers", "foo{}{bar}", "foo{{bar}}zap", "foo{bar}{zap}",
		"{}", "{", "}", "{a", "a}", "{user:42}:cart", "{user:42}:profile", "émoji-✓", "key\x00with\x00nul"}
	for i := range 40 {
		keys = append(keys, fmt.Sprint("key:", i*7919))
	}
	return keys
}

func TestSharedVectors(t *testing.T) {
	if os.Getenv("SHARDIS_WRITE_VECTORS") == "1" {
		var vs []vector
		for _, k := range vectorKeys() {
			vs = append(vs, vector{k, CRC16(HashPart([]byte(k))), Of([]byte(k))})
		}
		b, _ := json.MarshalIndent(vs, "", " ")
		if err := os.WriteFile(vectorFile, b, 0o644); err != nil {
			t.Fatal(err)
		}
	}
	b, err := os.ReadFile(vectorFile)
	if err != nil {
		t.Fatalf("%v (generate with SHARDIS_WRITE_VECTORS=1)", err)
	}
	var vs []vector
	if err := json.Unmarshal(b, &vs); err != nil {
		t.Fatal(err)
	}
	for _, v := range vs {
		if got := Of([]byte(v.Key)); got != v.Slot {
			t.Errorf("%q: slot %d, vector says %d", v.Key, got, v.Slot)
		}
	}
}
