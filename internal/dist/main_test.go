package dist_test

import (
	"os"
	"testing"
)

// undumpableVar asks the test binary, run again by a stand-in of bin/ork, to
// be an undumpable process that waits to be stopped instead of running its
// tests: what ork and orker are since they harden themselves (docs/DEPLOY.md,
// "The processes themselves"). Its value is the file the child writes once it
// is undumpable.
const undumpableVar = "ORKSTACK_TEST_UNDUMPABLE"

func TestMain(m *testing.M) {
	if ready := os.Getenv(undumpableVar); ready != "" {
		os.Exit(beUndumpableUntilStopped(ready))
	}
	os.Exit(m.Run())
}
