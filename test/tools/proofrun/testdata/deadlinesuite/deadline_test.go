package deadlinesuite

import (
	"math"
	"testing"
	"time"
)

// TestReportsItsTimeout logs the minutes left of the binary's -timeout. It
// runs at the start of the binary, so the figure is the timeout, rounded.
func TestReportsItsTimeout(t *testing.T) {
	d, ok := t.Deadline()
	if !ok {
		t.Fatal("go test gave the binary no timeout")
	}
	t.Logf("timeout-minutes=%d", int(math.Round(time.Until(d).Minutes())))
}
