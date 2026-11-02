//go:build race

package hostpolicy

// raceBuild reports whether the test binary carries the race detector,
// whose instrumentation slows CPU-bound code several times over (the Go
// documentation names 2–20×). A CPU bound the test sets for the production
// build is scaled by raceSlowdown under it, never dropped.
const raceBuild = true

// raceSlowdown is how many times the race detector may slow the judgements
// the CPU bound measures; they ran up to five times slower where it was set.
const raceSlowdown = 10
