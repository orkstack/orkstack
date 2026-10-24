//go:build race

package engine

// raceBuild reports whether the test binary carries the race detector,
// whose instrumentation slows CPU-bound code several times over (the Go
// documentation names 2–20×). A wall-clock bound the spec sets for the
// production build is scaled by raceSlowdown under it, never dropped.
const raceBuild = true
