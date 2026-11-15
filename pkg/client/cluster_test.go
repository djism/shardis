package client

import "testing"

func TestParseRedirect(t *testing.T) {
	for _, tc := range []struct {
		in   string
		slot uint16
		addr string
		ok   bool
	}{
		{"MOVED 3999 127.0.0.1:6381", 3999, "127.0.0.1:6381", true},
		{"ASK 0 10.0.0.5:7000", 0, "10.0.0.5:7000", true},
		{"MOVED 16384 h:1", 0, "", false},
		{"MOVED x h:1", 0, "", false},
		{"MOVED 1", 0, "", false},
	} {
		s, a, ok := parseRedirect([]byte(tc.in))
		if s != tc.slot || a != tc.addr || ok != tc.ok {
			t.Errorf("%q: got %d %q %v", tc.in, s, a, ok)
		}
	}
}
