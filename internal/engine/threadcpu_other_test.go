//go:build !linux

package engine

import "time"

// threadCPU has no portable implementation here: the cost tests fall back to
// the wall clock, which a loaded host can move (threadcpu_linux_test.go).
func threadCPU() (time.Duration, bool) { return 0, false }
