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
	historypb "go.temporal.io/api/history/v1"

	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/internal/task"
	"github.com/orkstack/orkstack/internal/testsupport/proof"
)

// recordE1Pair records one gate's gated and legacy fixtures from REAL runs on
// a real cluster, by a versioned worker, from one plan: once by today's code
// and once with the gate off, which is the command sequence of the builds
// before it. It never skips: by default the recordings land under
// ORK_PROOF_DIR and the committed fixtures are left alone; only
// ORK_RECORD_GOLDEN_HISTORY=1 also writes them. It FAILS unless every
// recording completes, holds the marker its path promises, records what check
// asks, and replays against current code with the same values.
func recordE1Pair(t *testing.T, label, change, document string, plan func(orkerQueue string) engine.RunPlan,
	gated, legacy string, disable func() func(), check func(t *testing.T, path string, recorded *historypb.History, legacy bool),
) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	s := newChaosStack(t, ctx, label+"-fixture")
	deployment := task.Deployment{Name: fmt.Sprintf("ork-%s-%d-%d", label, os.Getpid(), time.Now().UnixNano()), BuildID: "v1"}
	s.startReplica("replica-a", replicaOptions{deployment: deployment})
	s.startProjection(nil)
	s.startOrkerVersioned("orker-1", s.orkerQueue, s.control, deployment, instant)
	setCurrentVersion(t, ctx, s.control, deployment, s.runQueue, s.orkerQueue)

	for _, isLegacy := range []bool{true, false} {
		path := gated
		if isLegacy {
			path = legacy
		}
		t.Run(strings.TrimSuffix(filepath.Base(path), ".json"), func(t *testing.T) {
			runID := fmt.Sprintf("ork-%s-%d", label, time.Now().UnixNano())
			s.transcript.run(runID)
			// The gate is off only while the run executes: replaying what it
			// recorded below is exactly the check that today's code, with
			// the gate on, still takes the history's own branch.
			func() {
				if isLegacy {
					defer disable()()
				}
				s.start(runID, compile(t, document), nil, plan(s.orkerQueue))
				if status, _ := s.wait(runID, 3*time.Minute); status != enums.WORKFLOW_EXECUTION_STATUS_COMPLETED {
					t.Fatalf("the recorded run closed %v, want Completed", status)
				}
			}()
			recorded := s.history(runID, "")
			markers := versionMarkers(t, recorded)
			if isLegacy != (markers[change] == 0) {
				t.Fatalf("legacy=%v but markers = %v", isLegacy, markers)
			}
			check(t, path, recorded, isLegacy)
			requireContiguous(t, publishedOf(t, recorded))

			encoded := encodeHistory(t, recorded)
			proof.Write(t, label, filepath.Base(path), encoded)
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

// TestIntegration_RecordFairnessPinCreationHistories records the
// fairness-pin-creation fixtures: twoPinsDocument under a plan that carries a
// fairness selection, so the gated run keys both pins' creation requests and
// the legacy one keys neither; every task is keyed in both.
func TestIntegration_RecordFairnessPinCreationHistories(t *testing.T) {
	recordE1Pair(t, "fairness-pin-creation", engine.FairnessPinCreationChange, twoPinsDocument,
		func(orkerQueue string) engine.RunPlan {
			plan := rawPlan(orkerQueue)
			plan.CommitRoot = "mem://commits"
			plan.Fairness = &engine.FairnessSelection{Key: fairnessKeysPlannedKey, Weight: fairnessKeysPlannedWeight, PinRequests: true}
			return plan
		},
		fairnessPinCreationGated, fairnessPinCreationLegacy, engine.DisableFairnessPinCreationGateForTest,
		func(t *testing.T, path string, recorded *historypb.History, legacy bool) {
			requireRecordedCreationPriority(t, path, recorded, legacy)
			runID, _, _, _, _ := startArgs(t, recorded)
			capture, err := replayCapturingDispatch(t, recorded, runID)
			if err != nil {
				t.Fatalf("the history just recorded does not replay against current code: %v", err)
			}
			requireKeyed(t, "pin creation request on replay", capture.of(sessionCreationActivityName), 2, wantCreationPriority(legacy))
		})
}

// retryFixtureDocument is a chain whose middle task retries with no maximum
// interval of its own, so the plan's number decides its spacing.
const retryFixtureDocument = `
apiVersion: orkstack/v1
kind: Orkflow
name: retry-spacing
tasks:
  - id: prepare
    uses: shell
    with: {command: "echo prepare"}
  - id: fetch
    uses: shell
    needs: [prepare]
    retry: {maximum_attempts: 3}
    with: {command: "echo fetch"}
  - id: report
    uses: shell
    needs: [fetch]
    with: {command: "echo report"}
`

// TestIntegration_RecordRetryMaxIntervalHistories records the
// retry-max-interval fixtures: retryFixtureDocument under a plan that
// carries 60 seconds, so the gated run schedules its retrying task with a
// maximum interval of 60 seconds and the legacy one with 100.
func TestIntegration_RecordRetryMaxIntervalHistories(t *testing.T) {
	recordE1Pair(t, "retry-max-interval", engine.RetryMaxIntervalChange, retryFixtureDocument,
		func(orkerQueue string) engine.RunPlan {
			plan := rawPlan(orkerQueue)
			plan.CommitRoot = "mem://commits"
			plan.RetryMaxIntervalSeconds = int32(retryMaxIntervalPlanned / time.Second)
			return plan
		},
		retryMaxIntervalGated, retryMaxIntervalLegacy, engine.DisableRetryMaxIntervalGateForTest,
		func(t *testing.T, path string, recorded *historypb.History, legacy bool) {
			if got := retryingIntervals(recorded); len(got) != 1 || got[0] != wantRetryMaxInterval(legacy) {
				t.Fatalf("%s recorded retrying tasks with maximum intervals %v, want one of %s", path, got, wantRetryMaxInterval(legacy))
			}
			runID, _, _, _, _ := startArgs(t, recorded)
			capture, err := replayCapturingDispatch(t, recorded, runID)
			if err != nil {
				t.Fatalf("the history just recorded does not replay against current code: %v", err)
			}
			requireIntervals(t, maximumIntervals(capture), 1, wantRetryMaxInterval(legacy))
		})
}
