//go:build integration

package engine_test

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"go.temporal.io/api/enums/v1"

	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/internal/task"
	"github.com/orkstack/orkstack/internal/testsupport/proof"
)

// TestIntegration_RecordPinCreationTimeoutHistories records the pin creation
// timeout fixtures from REAL runs on a real cluster, by a versioned worker: an
// Orkflow that opens two pins with an unpinned task between them
// (twoPinsDocument, so the run also has a moment with nothing in flight, where
// the continuation tests that walk every fixture switch it) started under a
// plan that carries a 30 second pin creation timeout, once by today's code (the gated path: the
// pin-creation-timeout marker, the creation activity scheduled with the
// plan's 30 seconds) and once with the gate off, which is the command
// sequence of the builds before it (no marker, the five minutes they
// hardcoded, whatever the plan says). It never skips: by default the
// recordings land under ORK_PROOF_DIR and the committed fixtures are left
// alone; only ORK_RECORD_GOLDEN_HISTORY=1 also writes them. It FAILS unless
// every recording completes, records the creation timeout its path promises,
// and replays against current code.
func TestIntegration_RecordPinCreationTimeoutHistories(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	s := newChaosStack(t, ctx, "pin-creation-timeout-fixture")
	deployment := task.Deployment{Name: fmt.Sprintf("ork-pin-creation-%d-%d", os.Getpid(), time.Now().UnixNano()), BuildID: "v1"}
	s.startReplica("replica-a", replicaOptions{deployment: deployment})
	s.startProjection(nil)
	s.startOrkerVersioned("orker-1", s.orkerQueue, s.control, deployment, instant)
	setCurrentVersion(t, ctx, s.control, deployment, s.runQueue, s.orkerQueue)

	for _, legacy := range []bool{true, false} {
		path, want := pinCreationTimeoutGated, pinCreationTimeoutPlanned
		if legacy {
			path, want = pinCreationTimeoutLegacy, 5*time.Minute
		}
		t.Run(strings.TrimSuffix(filepath.Base(path), ".json"), func(t *testing.T) {
			plan := rawPlan(s.orkerQueue)
			plan.CommitRoot = "mem://commits"
			plan.PinCreationTimeoutSeconds = int32(pinCreationTimeoutPlanned / time.Second)
			runID := fmt.Sprintf("ork-pin-creation-timeout-%d", time.Now().UnixNano())
			s.transcript.run(runID)

			// The gate is off only while the run executes: replaying what it
			// recorded below is exactly the check that today's code, with
			// the gate on, still takes the history's own branch.
			func() {
				if legacy {
					defer engine.DisablePinCreationTimeoutGateForTest()()
				}
				s.start(runID, compile(t, twoPinsDocument), nil, plan)
				if status, _ := s.wait(runID, 3*time.Minute); status != enums.WORKFLOW_EXECUTION_STATUS_COMPLETED {
					t.Fatalf("the recorded run closed %v, want Completed", status)
				}
			}()
			recorded := s.history(runID, "")

			markers := versionMarkers(t, recorded)
			if legacy != (markers[engine.PinCreationTimeoutChange] == 0) {
				t.Fatalf("legacy=%v but markers = %v", legacy, markers)
			}
			if got := recordedCreationTimeouts(recorded); len(got) != 2 || got[0] != want || got[1] != want {
				t.Fatalf("the recorded creation timeouts are %v, want two of %s", got, want)
			}
			requireContiguous(t, publishedOf(t, recorded))

			capture, err := replayCapturingPinCreation(t, recorded, runID)
			if err != nil {
				t.Fatalf("the history just recorded does not replay against current code: %v", err)
			}
			if got := capture.creationTimeouts(); len(got) != 2 || got[0] != want || got[1] != want {
				t.Fatalf("replaying the history just recorded sent creation timeouts %v, want two of %s", got, want)
			}

			encoded := encodeHistory(t, recorded)
			proof.Write(t, "pin-creation-timeout", filepath.Base(path), encoded)
			written := "the proof directory only (ORK_RECORD_GOLDEN_HISTORY unset)"
			if os.Getenv("ORK_RECORD_GOLDEN_HISTORY") != "" {
				if err := os.WriteFile(path, encoded, 0o644); err != nil {
					t.Fatalf("write %s: %v", path, err)
				}
				written = path
			}
			t.Logf("recorded %d events of run %s into %s, replayed clean; creation timeout %s, markers %v",
				len(recorded.GetEvents()), runID, written, want, markers)
		})
	}
}
