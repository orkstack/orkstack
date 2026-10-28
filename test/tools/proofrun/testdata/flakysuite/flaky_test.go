package flakysuite

import "testing"

var runs int

// TestFailsOnTheSecondRun passes, fails and passes again when `go test -count=3`
// runs it three times in one process.
func TestFailsOnTheSecondRun(t *testing.T) {
	runs++
	if runs == 2 {
		t.Fatal("this run fails")
	}
}

func TestAlwaysPasses(t *testing.T) {}
