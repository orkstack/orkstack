//go:build integration

package failfast

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func started(t *testing.T) string {
	t.Helper()
	dir := os.Getenv("FAILFAST_DIR")
	if dir == "" {
		t.Fatal("FAILFAST_DIR is not set")
	}
	return filepath.Join(dir, "blocker-started")
}

// TestFailsWhileTheBlockerRuns fails once TestBlocksUntilStopped is running.
func TestFailsWhileTheBlockerRuns(t *testing.T) {
	marker := started(t)
	deadline := time.Now().Add(time.Minute)
	for {
		if _, err := os.Stat(marker); err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("the blocker never started")
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("this test fails")
}

// TestBlocksUntilStopped runs until the run is stopped, unless
// FAILFAST_NO_BLOCK=1.
func TestBlocksUntilStopped(t *testing.T) {
	if err := os.WriteFile(started(t), nil, 0o644); err != nil {
		t.Fatal(err)
	}
	if os.Getenv("FAILFAST_NO_BLOCK") == "1" {
		return
	}
	time.Sleep(2 * time.Minute)
	t.Fatal("nothing stopped this test")
}

// TestNeverStarts is queued behind the other two.
func TestNeverStarts(t *testing.T) {}
