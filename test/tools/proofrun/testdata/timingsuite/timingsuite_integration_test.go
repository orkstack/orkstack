//go:build integration

// Package timingsuite is a test suite for proofrun's own tests: a test that
// reports a wall-clock check through the proof helper and misses it. It lives
// under testdata, so `./...` never runs it.
package timingsuite

import (
	"testing"

	"github.com/orkstack/orkstack/internal/testsupport/proof"
)

// TestTimingMiss fails with nothing but a timing miss.
func TestTimingMiss(t *testing.T) {
	timing := proof.TimingBounds(t)
	t.Log("a line of ordinary output")
	timing.Check("getrun-live-p95", 2.5, 2, "the fixture")
}
