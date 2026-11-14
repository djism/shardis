package config

import (
	"fmt"
	"strconv"
	"strings"
)

// ParseMemory parses a memory size the way redis.conf does: a number with an
// optional unit. Note the two families: k/m/g are powers of 1000, kb/mb/gb
// are powers of 1024 (so "100mb" = 104,857,600 bytes).
func ParseMemory(s string) (int64, error) {
	t := strings.ToLower(strings.TrimSpace(s))
	units := []struct {
		suffix string
		mult   int64
	}{ // longest suffixes first, so "kb" isn't read as "k" + "b"
		{"gb", 1 << 30}, {"mb", 1 << 20}, {"kb", 1 << 10},
		{"g", 1e9}, {"m", 1e6}, {"k", 1e3}, {"b", 1},
	}
	mult := int64(1)
	for _, u := range units {
		if strings.HasSuffix(t, u.suffix) {
			t, mult = strings.TrimSuffix(t, u.suffix), u.mult
			break
		}
	}
	n, err := strconv.ParseInt(t, 10, 64)
	if err != nil || n < 0 || n > (1<<62)/mult {
		return 0, fmt.Errorf("invalid memory size %q", s)
	}
	return n * mult, nil
}
