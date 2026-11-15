package cluster

import (
	"bytes"
	"reflect"
	"testing"
)

func sampleMessage() *message {
	m := &message{
		typ: msgPing, sender: NodeID("a"), currentEpoch: 7, configEpoch: 3, flags: wirePrimary,
		busPort: 17000, clientAddr: "127.0.0.1:7000", internalAddr: "shardis-a:7000",
		gossip: []gossipEntry{
			{id: NodeID("b"), flags: wirePrimary | wirePFail, busPort: 17001, pingSentAgo: 1500, pongRecvAgo: 20, clientAddr: "127.0.0.1:7001", internalAddr: "shardis-b:7000"},
			{id: NodeID("d"), flags: wireReplica, busPort: 17003, clientAddr: "127.0.0.1:7003", internalAddr: "shardis-d:7000"},
		},
	}
	for s := 0; s <= 5460; s++ {
		m.setSlot(s)
	}
	return m
}

func TestWireRoundTrip(t *testing.T) {
	want := sampleMessage()
	buf := want.encode()
	got, n, err := readMessage(bytes.NewReader(buf))
	if err != nil {
		t.Fatal(err)
	}
	if n != len(buf) || !reflect.DeepEqual(got, want) {
		t.Fatalf("round trip:\n got %+v\nwant %+v", got, want)
	}
	if !got.hasSlot(0) || !got.hasSlot(5460) || got.hasSlot(5461) {
		t.Fatal("slot bitmap wrong")
	}
	t.Logf("a PING with 2 gossip entries is %d bytes (the slot bitmap is %d)", len(buf), slotBytes)
}

func TestWireRejectsGarbage(t *testing.T) {
	good := sampleMessage().encode()
	cases := map[string][]byte{
		"bad magic":   append([]byte("XBUS"), good[4:]...),
		"huge length": append(append([]byte{}, good[:6]...), 0xff, 0xff, 0xff, 0xff),
		"truncated":   good[:len(good)-10],
		"too many entries": func() []byte {
			noGossip := sampleMessage()
			noGossip.gossip = nil
			at := len(noGossip.encode()) - 2 // the 2-byte entry count is the last field before the entries
			b := append([]byte{}, good...)
			b[at], b[at+1] = 0xff, 0xff
			return b
		}(),
	}
	for name, b := range cases {
		if _, _, err := readMessage(bytes.NewReader(b)); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
}

// Whatever bytes arrive on the bus, decoding must fail cleanly, never panic.
func FuzzReadMessage(f *testing.F) {
	f.Add(sampleMessage().encode())
	f.Add([]byte("SBUS\x01\x00\x00\x00\x00\x0a"))
	f.Fuzz(func(t *testing.T, data []byte) {
		m, _, err := readMessage(bytes.NewReader(data))
		if err == nil {
			// Anything we accept must re-encode to something we accept again.
			if _, _, err := readMessage(bytes.NewReader(m.encode())); err != nil {
				t.Fatalf("re-encode of an accepted message fails: %v", err)
			}
		}
	})
}
