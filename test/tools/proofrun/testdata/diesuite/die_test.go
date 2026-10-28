package diesuite

import (
	"os"
	"testing"
)

var runs int

// TestDiesOnTheSecondRun passes, then ends the whole test binary while it
// runs, when `go test -count=3` executes it three times in one process.
func TestDiesOnTheSecondRun(t *testing.T) {
	runs++
	if runs == 2 {
		os.Exit(3)
	}
}
