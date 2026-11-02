//go:build race

package orker

// raceSlowdown is how many times the race detector may slow the CPU-bound
// judgements a test bounds (the Go documentation names 2–20×). A bound set
// for the production build is scaled by it under the detector, never dropped.
const raceSlowdown = 10
