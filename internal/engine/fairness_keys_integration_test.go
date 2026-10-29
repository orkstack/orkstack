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

// TestIntegration_RecordFairnessKeysHistories records the fairness-keys
// fixtures from REAL runs on a real cluster, by a versioned worker: a linear
// Orkflow started under a plan that carries a fairness selection, once by
// today's code (the gated path: the fairness-keys marker, every task activity
// scheduled with the plan's key and weight) and once with the gate off, which
// is the command sequence of the builds before it (no marker, no priority,
// whatever the plan says). It never skips: by default the recordings land
// under ORK_PROOF_DIR and the committed fixtures are left alone; only
// ORK_RECORD_GOLDEN_HISTORY=1 also writes them. It FAILS unless every
// recording completes, records the priority its path promises, and replays
// against current code.
func TestIntegration_RecordFairnessKeysHistories(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	s := newChaosStack(t, ctx, "fairness-keys-fixture")
	deployment := task.Deployment{Name: fmt.Sprintf("ork-fairness-%d-%d", os.Getpid(), time.Now().UnixNano()), BuildID: "v1"}
	s.startReplica("replica-a", replicaOptions{deployment: deployment})
	s.startProjection(nil)
	s.startOrkerVersioned("orker-1", s.orkerQueue, s.control, deployment, instant)
	setCurrentVersion(t, ctx, s.control, deployment, s.runQueue, s.orkerQueue)

	for _, legacy := range []bool{true, false} {
		path := fairnessKeysGated
		if legacy {
			path = fairnessKeysLegacy
		}
		t.Run(strings.TrimSuffix(filepath.Base(path), ".json"), func(t *testing.T) {
			plan := rawPlan(s.orkerQueue)
			plan.CommitRoot = "mem://commits"
			plan.Fairness = &engine.FairnessSelection{Key: fairnessKeysPlannedKey, Weight: fairnessKeysPlannedWeight}
			runID := fmt.Sprintf("ork-fairness-keys-%d", time.Now().UnixNano())
			s.transcript.run(runID)

			func() {
				if legacy {
					defer engine.DisableFairnessKeysGateForTest()()
				}
				s.start(runID, compile(t, linearDocument), nil, plan)
				if status, _ := s.wait(runID, 3*time.Minute); status != enums.WORKFLOW_EXECUTION_STATUS_COMPLETED {
					t.Fatalf("the recorded run closed %v, want Completed", status)
				}
			}()
			recorded := s.history(runID, "")

			markers := versionMarkers(t, recorded)
			if legacy != (markers[engine.FairnessKeysChange] == 0) {
				t.Fatalf("legacy=%v but markers = %v", legacy, markers)
			}
			requireRecordedFairness(t, path, recorded, legacy)
			requireContiguous(t, publishedOf(t, recorded))
			capture, err := replayCapturingFairness(t, recorded, runID)
			if err != nil {
				t.Fatalf("the history just recorded does not replay against current code: %v", err)
			}
			requireCapturedFairness(t, path, capture, legacy)

			encoded := encodeHistory(t, recorded)
			proof.Write(t, "fairness-keys", filepath.Base(path), encoded)
			written := "the proof directory only (ORK_RECORD_GOLDEN_HISTORY unset)"
			if os.Getenv("ORK_RECORD_GOLDEN_HISTORY") != "" {
				if err := os.WriteFile(path, encoded, 0o644); err != nil {
					t.Fatalf("write %s: %v", path, err)
				}
				written = path
			}
			t.Logf("recorded %d events of run %s into %s, replayed clean; markers %v",
				len(recorded.GetEvents()), runID, written, markers)
		})
	}
}
