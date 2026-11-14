package cluster

import (
	"strings"
	"testing"

	"github.com/djism/shardis/internal/keyslot"
)

const sample = `
# three primaries, one replica each
node a 127.0.0.1:7000 shardis-a:7000 primary 0-5460
node b 127.0.0.1:7001 shardis-b:7000 primary 5461-10922
node c 127.0.0.1:7002 shardis-c:7000 primary 10923-16383
node d 127.0.0.1:7003 shardis-d:7000 replica a
node e 127.0.0.1:7004 shardis-e:7000 replica b
node f 127.0.0.1:7005 shardis-f:7000 replica c
`

func TestParseSample(t *testing.T) {
	top, err := Parse(strings.NewReader(sample), "d")
	if err != nil {
		t.Fatal(err)
	}
	if top.Me.Name != "d" || top.Me.Primary.Name != "a" || top.Me.IsPrimary() {
		t.Fatalf("me = %+v", top.Me)
	}
	if o := top.Owner(0); o.Name != "a" {
		t.Errorf("slot 0 → %s", o.Name)
	}
	if o := top.Owner(keyslot.Of([]byte("foo"))); o.Name != "c" { // foo = 12182
		t.Errorf("foo → %s", o.Name)
	}
	if got := len(top.SlotRanges()); got != 3 {
		t.Errorf("%d ranges", got)
	}
	if len(top.Primaries()) != 3 || len(top.Nodes[0].Replicas) != 1 || top.MaxEpoch() != 3 {
		t.Error("primaries / replicas / epochs wrong")
	}
	if len(top.Me.ID) != 40 || top.Me.ID == top.Nodes[0].ID {
		t.Errorf("ID %q", top.Me.ID)
	}
	if h, p := top.Nodes[2].HostPort(); h != "127.0.0.1" || p != 7002 {
		t.Errorf("HostPort = %s %d", h, p)
	}
}

func TestParseErrors(t *testing.T) {
	base := "node a 127.0.0.1:1 127.0.0.1:1 primary 0-16383\n"
	for name, text := range map[string]string{
		"gap":                "node a 127.0.0.1:1 127.0.0.1:1 primary 0-16382\n",
		"overlap":            base + "node b 127.0.0.1:2 127.0.0.1:2 primary 100\n",
		"duplicate":          base + "node a 127.0.0.1:2 127.0.0.1:2 replica a\n",
		"bad primary":        base + "node b 127.0.0.1:2 127.0.0.1:2 replica zz\n",
		"replica of replica": base + "node b 127.0.0.1:2 127.0.0.1:2 replica a\nnode c 127.0.0.1:3 127.0.0.1:3 replica b\n",
		"bad range":          "node a 127.0.0.1:1 127.0.0.1:1 primary 0-16384\n",
		"bad role":           "node a 127.0.0.1:1 127.0.0.1:1 boss 0-16383\n",
		"bad addr":           "node a nowhere 127.0.0.1:1 primary 0-16383\n",
		"me missing":         "node b 127.0.0.1:1 127.0.0.1:1 primary 0-16383\n",
	} {
		if _, err := Parse(strings.NewReader(text), "a"); err == nil {
			t.Errorf("%s: expected an error", name)
		}
	}
}
