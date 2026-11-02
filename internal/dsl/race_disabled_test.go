//go:build !race

package dsl_test

// raceSlowdown: see race_enabled_test.go. Without the detector a bound is
// used as it is.
const raceSlowdown = 1
