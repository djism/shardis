package keyslot

import (
	"bufio"
	"fmt"
	"math/rand/v2"
	"net"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"
)

func TestCRC16CheckValue(t *testing.T) {
	// The standard check value for CRC16-XMODEM, also quoted in the Redis Cluster spec.
	if got := CRC16([]byte("123456789")); got != 0x31C3 {
		t.Fatalf("CRC16(123456789) = %#04x, want 0x31c3", got)
	}
}

func TestHashPart(t *testing.T) {
	tests := map[string]string{
		"{user1000}.following": "user1000",
		"{user1000}.followers": "user1000",
		"foo{}{bar}":           "foo{}{bar}",
		"foo{{bar}}zap":        "{bar",
		"foo{bar}{zap}":        "bar",
		"plain":                "plain",
		"open{only":            "open{only",
		"}{x}":                 "x",
		"":                     "",
	}
	for key, want := range tests {
		if got := string(HashPart([]byte(key))); got != want {
			t.Errorf("HashPart(%q) = %q, want %q", key, got, want)
		}
	}
}

func TestSameTagSameSlot(t *testing.T) {
	a, b := Of([]byte("{user:1}:cart")), Of([]byte("{user:1}:profile"))
	if a != b {
		t.Fatalf("hash-tagged keys in different slots: %d vs %d", a, b)
	}
}

// Compatibility check against the real thing: start redis-server in cluster
// mode and compare CLUSTER KEYSLOT for many random keys. Skipped if
// redis-server isn't installed (e.g. in CI).
func TestMatchesRealRedis(t *testing.T) {
	bin, err := exec.LookPath("redis-server")
	if err != nil {
		t.Skip("redis-server not installed")
	}
	port := 20000 + rand.IntN(20000)
	dir := t.TempDir()
	cmd := exec.Command(bin, "--port", strconv.Itoa(port), "--cluster-enabled", "yes",
		"--cluster-config-file", filepath.Join(dir, "nodes.conf"), "--dir", dir,
		"--save", "", "--appendonly", "no")
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { cmd.Process.Kill(); cmd.Wait() })

	var conn net.Conn
	for range 100 {
		if conn, err = net.Dial("tcp", "127.0.0.1:"+strconv.Itoa(port)); err == nil {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	br := bufio.NewReader(conn)

	keys := []string{"foo", "bar", "{user1000}.following", "foo{}{bar}", "foo{{bar}}zap", "foo{bar}{zap}", ""}
	for range 500 {
		b := make([]byte, rand.IntN(20))
		for i := range b {
			b[i] = "abc{}xyz:01"[rand.IntN(11)]
		}
		keys = append(keys, string(b))
	}
	for _, k := range keys {
		fmt.Fprintf(conn, "*3\r\n$7\r\nCLUSTER\r\n$7\r\nKEYSLOT\r\n$%d\r\n%s\r\n", len(k), k)
		line, err := br.ReadString('\n')
		if err != nil {
			t.Fatal(err)
		}
		want, err := strconv.Atoi(strings.TrimSpace(strings.TrimPrefix(line, ":")))
		if err != nil {
			t.Fatalf("redis replied %q for key %q", line, k)
		}
		if got := Of([]byte(k)); int(got) != want {
			t.Errorf("key %q: ours %d, redis %d", k, got, want)
		}
	}
}

func BenchmarkOf(b *testing.B) {
	key := []byte("user:1234567:session")
	for b.Loop() {
		Of(key)
	}
}
