package faults

import (
	"testing"
	"time"
)

func TestNilAndEmptyRegistryInjectNothing(t *testing.T) {
	var r *Registry
	if r.Check(ReplStream, "x", nil) {
		t.Fatal("nil registry severed")
	}
	if New().Check(ReplStream, "x", nil) {
		t.Fatal("empty registry severed")
	}
}

func TestPauseBlocksUntilCleared(t *testing.T) {
	r := New()
	r.Set(Rule{Point: ReplStream, Kind: Pause})
	done := make(chan struct{})
	go func() { r.Check(ReplStream, "a", nil); close(done) }()
	select {
	case <-done:
		t.Fatal("pause didn't block")
	case <-time.After(50 * time.Millisecond):
	}
	r.Clear("")
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("clear didn't release the pause")
	}
}

func TestPauseEndsWhenStopped(t *testing.T) {
	r := New()
	r.Set(Rule{Point: ReplStream, Kind: Pause})
	stop, done := make(chan struct{}), make(chan struct{})
	go func() { r.Check(ReplStream, "a", stop); close(done) }()
	time.Sleep(20 * time.Millisecond)
	close(stop)
	r.Wake()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("stop didn't end the pause")
	}
}

func TestTargetsAndKinds(t *testing.T) {
	r := New()
	r.Set(Rule{Point: ReplStream, Kind: Sever, Target: "b"})
	if r.Check(ReplStream, "a", nil) || !r.Check(ReplStream, "b", nil) {
		t.Fatal("target matching wrong")
	}
	r.Set(Rule{Point: ReplStream, Kind: Delay, Delay: 30 * time.Millisecond})
	start := time.Now()
	r.Check(ReplStream, "a", nil)
	if time.Since(start) < 30*time.Millisecond {
		t.Fatal("delay not applied")
	}
}

func TestParse(t *testing.T) {
	good := map[string]string{
		"repl.stream pause":           "repl.stream pause",
		"REPL.STREAM delay 50 r1":     "repl.stream delay 50 r1",
		"repl.stream sever 1.2.3.4:5": "repl.stream sever 1.2.3.4:5",
	}
	for in, want := range good {
		r, err := Parse(splitWords(in))
		if err != nil || r.String() != want {
			t.Errorf("Parse(%q) = %q, %v", in, r.String(), err)
		}
	}
	for _, in := range []string{"", "nope pause", "repl.stream delay", "repl.stream delay x", "repl.stream wobble"} {
		if _, err := Parse(splitWords(in)); err == nil {
			t.Errorf("Parse(%q) should fail", in)
		}
	}
}

func splitWords(s string) []string {
	var out []string
	w := ""
	for _, c := range s + " " {
		if c == ' ' {
			if w != "" {
				out = append(out, w)
			}
			w = ""
		} else {
			w += string(c)
		}
	}
	return out
}
