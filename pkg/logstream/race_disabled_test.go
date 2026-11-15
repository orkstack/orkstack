//go:build !race

package logstream_test

// raceBuild: see race_enabled_test.go.
const raceBuild = false

// raceSlowdown is unused without the race detector.
const raceSlowdown = 1
