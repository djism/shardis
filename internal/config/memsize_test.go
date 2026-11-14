package config

import "testing"

func TestParseMemory(t *testing.T) {
	good := map[string]int64{
		"0": 0, "1024": 1024, "1k": 1000, "1kb": 1024, "100mb": 100 << 20,
		"2GB": 2 << 30, "1g": 1e9, " 5m ": 5e6, "10b": 10,
	}
	for in, want := range good {
		if got, err := ParseMemory(in); err != nil || got != want {
			t.Errorf("ParseMemory(%q) = %d, %v; want %d", in, got, err, want)
		}
	}
	for _, in := range []string{"", "mb", "-1", "1.5gb", "1tb", "abc", "99999999999gb"} {
		if _, err := ParseMemory(in); err == nil {
			t.Errorf("ParseMemory(%q) should fail", in)
		}
	}
}
