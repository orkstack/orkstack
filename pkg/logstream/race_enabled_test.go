//go:build race

package logstream_test

// raceBuild reports whether the test binary carries the race detector,
// whose instrumentation slows CPU-bound code several times over (the Go
// documentation names 2–20×). A CPU bound the test sets for the production
// build is scaled by raceSlowdown under it, never dropped.
const raceBuild = true

// raceSlowdown is how many times the race detector may slow the masking the
// CPU bound measures.
const raceSlowdown = 10
