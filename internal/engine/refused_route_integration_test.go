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

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/internal/task"
	"github.com/orkstack/orkstack/internal/testsupport/proof"
)

// TestIntegration_RecordRefusedRouteHistories records the refused-route
// fixtures from REAL runs on a real cluster, by a versioned worker: for each
// shape (a plain dispatch, a pin's opening, a definition with a failure
// handler) a run whose plan has no route for a task's pool, once by today's
// code (the gated, TASK_FAILED path) and once with the gate off, which is the
// command sequence of the builds before it. It never skips: by default the
// recordings land under ORK_PROOF_DIR and the committed fixtures are left
// alone; only ORK_RECORD_GOLDEN_HISTORY=1 also writes them. It FAILS unless
// every recording ends FAILED, holds the stream the path promises, and
// replays against current code.
func TestIntegration_RecordRefusedRouteHistories(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	s := newChaosStack(t, ctx, "refused-route-fixture")
	deployment := task.Deployment{Name: fmt.Sprintf("ork-refused-route-%d-%d", os.Getpid(), time.Now().UnixNano()), BuildID: "v1"}
	s.startReplica("replica-a", replicaOptions{deployment: deployment})
	s.startProjection(nil)
	s.startOrkerVersioned("orker-1", s.orkerQueue, s.control, deployment, instant)
	setCurrentVersion(t, ctx, s.control, deployment, s.runQueue, s.orkerQueue)

	for _, f := range refusedRouteFixtures {
		for _, legacy := range []bool{true, false} {
			path := refusedRouteFixture(f.shape, legacy)
			t.Run(strings.TrimSuffix(filepath.Base(path), ".json"), func(t *testing.T) {
				if legacy {
					defer engine.DisableRefusedRouteGateForTest()()
				}
				plan := rawPlan(s.orkerQueue)
				plan.CommitRoot = "mem://commits"
				runID := fmt.Sprintf("ork-refused-route-%s-%d", f.shape, time.Now().UnixNano())
				s.transcript.run(runID)
				s.start(runID, compile(t, f.document), nil, plan)
				if status, _ := s.wait(runID, 3*time.Minute); status != enums.WORKFLOW_EXECUTION_STATUS_FAILED {
					t.Fatalf("the recorded run closed %v, want Failed", status)
				}
				recorded := s.history(runID, "")
				if got := failureType(t, recorded); got != engine.ReasonInvalidPool {
					t.Fatalf("the recorded run failed with type %q, want %s", got, engine.ReasonInvalidPool)
				}
				markers := versionMarkers(t, recorded)
				if legacy != (markers[engine.RefusedRouteChange] == 0) {
					t.Fatalf("legacy=%v but markers = %v", legacy, markers)
				}
				published := publishedOf(t, recorded)
				requireContiguous(t, published)
				var failed, logs int
				for _, e := range published {
					if e.GetTaskId() != "crunch" {
						continue
					}
					switch e.GetType() {
					case orkstackv1.EventType_EVENT_TYPE_TASK_FAILED:
						failed++
					case orkstackv1.EventType_EVENT_TYPE_TASK_LOG:
						logs++
					}
				}
				if !legacy && (failed != 1 || logs != 0) {
					t.Fatalf("crunch's stream has %d TASK_FAILED and %d TASK_LOG, want 1 and 0", failed, logs)
				}
				if err := replayRecorded(recorded, runID); err != nil {
					t.Fatalf("the history just recorded does not replay against current code: %v", err)
				}
				encoded := encodeHistory(t, recorded)
				proof.Write(t, "refused-route", filepath.Base(path), encoded)
				written := "the proof directory only (ORK_RECORD_GOLDEN_HISTORY unset)"
				if os.Getenv("ORK_RECORD_GOLDEN_HISTORY") != "" {
					if err := os.WriteFile(path, encoded, 0o644); err != nil {
						t.Fatalf("write %s: %v", path, err)
					}
					written = path
				}
				t.Logf("recorded %d events of run %s into %s, replayed clean; crunch: %d TASK_FAILED, %d TASK_LOG",
					len(recorded.GetEvents()), runID, written, failed, logs)
			})
		}
	}
}
