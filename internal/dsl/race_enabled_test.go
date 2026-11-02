//go:build race

package dsl_test

// raceSlowdown is how many times the race detector may slow the code a CPU
// bound measures (the Go documentation names 2–20×). A bound set for the
// production build is multiplied by it under the detector, never dropped.
const raceSlowdown = 10
