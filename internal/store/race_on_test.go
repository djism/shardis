//go:build race

package store

// raceEnabled lets tests that measure memory skip themselves under -race:
// they don't test concurrency, and the race runtime makes them slow.
const raceEnabled = true
