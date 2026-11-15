// Package faults is deterministic fault injection: named points in the code
// where a test (or the website's live demo) can make things go wrong on
// purpose: pause a replication link, delay it, or cut it (ADR 020).
//
// It is off unless the server starts with --enable-fault-injection. When
// off, every check is a single atomic load, so it costs nothing in normal
// operation. Rules are set with the FAULT admin command:
//
//	FAULT SET repl.stream pause            pause the stream to every replica
//	FAULT SET repl.stream delay 50 r1      add 50 ms before each chunk to replica r1
//	FAULT SET repl.stream sever            drop replication links (they reconnect)
//	FAULT SET bus.message drop b           drop cluster bus messages to node b
//	FAULT CLEAR [point]
//
// "Deterministic" means a fault happens exactly when and where the rule
// says, not with some probability, so a failure scenario replays identically
// every time. That's what turns a once-a-month production bug into a test.
package faults

import (
	"fmt"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// Point names a place in the code that consults the registry.
type Point string

const (
	// ReplStream is checked by the primary before sending each chunk of the
	// replication stream to a replica. Target = the replica's ID (host:port).
	ReplStream Point = "repl.stream"
	// BusMessage is checked before sending each cluster bus message.
	// Target = the receiving node's name or ID. Dropping messages in both
	// directions between two nodes is how tests fake a network partition.
	BusMessage Point = "bus.message"
)

var knownPoints = []Point{ReplStream, BusMessage}

// allowed lists the kinds that make sense at each point. Dropping bytes from
// the middle of a replication stream would corrupt it (use sever instead);
// bus messages are independent, so they can be dropped one by one.
var allowed = map[Point][]Kind{
	ReplStream: {Pause, Delay, Sever},
	BusMessage: {Drop, Delay},
}

// Kind is what a rule does.
type Kind int

const (
	Pause Kind = iota // block until the rule is cleared
	Delay             // sleep before proceeding
	Sever             // tell the caller to drop the connection
	Drop              // tell the caller to discard this one message
)

var kindNames = [...]string{"pause", "delay", "sever", "drop"}

// Rule is one active fault.
type Rule struct {
	Point  Point
	Kind   Kind
	Delay  time.Duration
	Target string // "" = all targets
}

func (r Rule) String() string {
	s := fmt.Sprintf("%s %s", r.Point, kindNames[r.Kind])
	if r.Kind == Delay {
		s += fmt.Sprintf(" %d", r.Delay.Milliseconds())
	}
	if r.Target != "" {
		s += " " + r.Target
	}
	return s
}

// Registry holds the active rules. A nil *Registry is valid and never
// injects anything, so code can call Check unconditionally.
type Registry struct {
	active atomic.Bool // any rules at all? (the fast path)
	mu     sync.Mutex
	cond   *sync.Cond
	rules  []Rule
}

func New() *Registry {
	r := &Registry{}
	r.cond = sync.NewCond(&r.mu)
	return r
}

// Check applies any rule matching (p, target): it sleeps for Delay, blocks
// while a Pause rule is active, and returns true for Sever or Drop (the
// caller cuts the connection or discards the message). stop, if non-nil,
// ends a pause early (e.g. the connection closed).
func (r *Registry) Check(p Point, target string, stop <-chan struct{}) (cut bool) {
	if r == nil || !r.active.Load() {
		return false
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	for {
		rule, ok := r.match(p, target)
		if !ok {
			return false
		}
		switch rule.Kind {
		case Sever, Drop:
			return true
		case Delay:
			r.mu.Unlock()
			time.Sleep(rule.Delay)
			r.mu.Lock()
			return false
		case Pause:
			if stop != nil {
				select {
				case <-stop:
					return false
				default:
				}
			}
			r.cond.Wait() // woken by Set/Clear (and by Wake, for stop)
		}
	}
}

func (r *Registry) match(p Point, target string) (Rule, bool) {
	for _, rule := range r.rules {
		if rule.Point == p && (rule.Target == "" || rule.Target == target) {
			return rule, true
		}
	}
	return Rule{}, false
}

// Wake re-checks every paused caller (use after closing a stop channel).
func (r *Registry) Wake() {
	if r == nil {
		return
	}
	r.mu.Lock()
	r.cond.Broadcast()
	r.mu.Unlock()
}

// Set adds a rule, replacing any rule for the same point and target.
func (r *Registry) Set(rule Rule) {
	r.mu.Lock()
	r.rules = slices.DeleteFunc(r.rules, func(o Rule) bool { return o.Point == rule.Point && o.Target == rule.Target })
	r.rules = append(r.rules, rule)
	r.active.Store(true)
	r.cond.Broadcast()
	r.mu.Unlock()
}

// Clear removes rules for point p, or all rules if p is "".
func (r *Registry) Clear(p Point) {
	r.mu.Lock()
	r.rules = slices.DeleteFunc(r.rules, func(o Rule) bool { return p == "" || o.Point == p })
	r.active.Store(len(r.rules) > 0)
	r.cond.Broadcast()
	r.mu.Unlock()
}

func (r *Registry) List() []Rule {
	r.mu.Lock()
	defer r.mu.Unlock()
	return slices.Clone(r.rules)
}

// Parse reads "point kind [ms] [target]" as given to FAULT SET.
func Parse(args []string) (Rule, error) {
	if len(args) < 2 {
		return Rule{}, fmt.Errorf("usage: FAULT SET <point> pause|delay <ms>|sever [target]")
	}
	rule := Rule{Point: Point(strings.ToLower(args[0]))}
	if !slices.Contains(knownPoints, rule.Point) {
		return Rule{}, fmt.Errorf("unknown fault point %q (known: %v)", args[0], knownPoints)
	}
	rest := args[2:]
	switch strings.ToLower(args[1]) {
	case "pause":
		rule.Kind = Pause
	case "sever":
		rule.Kind = Sever
	case "drop":
		rule.Kind = Drop
	case "delay":
		if len(rest) == 0 {
			return Rule{}, fmt.Errorf("delay needs milliseconds")
		}
		ms, err := strconv.Atoi(rest[0])
		if err != nil || ms < 0 {
			return Rule{}, fmt.Errorf("bad delay %q", rest[0])
		}
		rule.Kind, rule.Delay, rest = Delay, time.Duration(ms)*time.Millisecond, rest[1:]
	default:
		return Rule{}, fmt.Errorf("unknown fault kind %q", args[1])
	}
	switch len(rest) {
	case 0:
	case 1:
		rule.Target = rest[0]
	default:
		return Rule{}, fmt.Errorf("too many arguments")
	}
	if !slices.Contains(allowed[rule.Point], rule.Kind) {
		return Rule{}, fmt.Errorf("%s can't be applied to %s", kindNames[rule.Kind], rule.Point)
	}
	return rule, nil
}
