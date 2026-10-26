package s3bucket

import (
	"testing"
	"time"
)

// A bucket create has been measured stalling with the rest of an object store's
// per-bucket work at a higher PROOF_JOBS (an engine boot's first read: 50 s).
// The create must outlast that stall twice over, because a TestMain that cannot
// create its bucket fails every test of its package under ORK_REQUIRE_INFRA.
func TestCreateTimeout_OutlastsTheMeasuredStallTwiceOver(t *testing.T) {
	const measuredStall = 50 * time.Second
	if createTimeout != 120*time.Second {
		t.Fatalf("createTimeout = %s, want 120s", createTimeout)
	}
	if createTimeout < 2*measuredStall {
		t.Fatalf("createTimeout %s does not outlast twice the measured %s stall", createTimeout, measuredStall)
	}
}
