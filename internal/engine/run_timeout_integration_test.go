//go:build integration

package engine_test

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"go.temporal.io/api/enums/v1"
	"go.temporal.io/api/history/v1"
	"go.temporal.io/api/temporalproto"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/worker"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/internal/task"

	"github.com/orkstack/orkstack/internal/testsupport/must"
	"github.com/orkstack/orkstack/internal/testsupport/proof"
)

// definition is the fixture's Orkflow, as the recorder starts it.
func (f runTimeoutFixture) definition() *orkstackv1.Orkflow {
	return &orkstackv1.Orkflow{Name: "run-timeout-" + f.name, Version: "1.0.0", TimeoutSeconds: f.timeout,
		Tasks: f.tasks}
}

// runTimeoutOrker is the recorder's stand-in Orker: a task whose id starts
// with "slow" heartbeats until it is cancelled, "boom" fails non-retryably,
// and every other task succeeds at once.
func runTimeoutOrker(ctx context.Context, in *orkstackv1.ExecuteTaskInput) (*orkstackv1.ExecuteTaskResult, error) {
	switch {
	case strings.HasPrefix(in.TaskId, "slow"):
		for {
			activity.RecordHeartbeat(ctx)
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(250 * time.Millisecond):
			}
		}
	case in.TaskId == "boom":
		return nil, temporal.NewNonRetryableApplicationError("boom failed", "TASK_FAILED", nil)
	default:
		return &orkstackv1.ExecuteTaskResult{TaskId: in.TaskId,
			ValuesJson: valuesJSON(map[string]any{"echoed_task": in.TaskId})}, nil
	}
}

// TestIntegration_RecordRunTimeoutHistories records every run-timeout fixture
// (runTimeoutFixtures, replay_run_timeout_test.go) against a real cluster, by
// a versioned worker, each from its own definition and plan.
func TestIntegration_RecordRunTimeoutHistories(t *testing.T) {
	// It never skips: by default the recordings land under ORK_PROOF_DIR and
	// the committed fixtures are left alone, so every proof shows today's
	// code still records these paths on a cluster and replays them. Only
	// ORK_RECORD_GOLDEN_HISTORY=1 also writes the fixtures.
	write := os.Getenv("ORK_RECORD_GOLDEN_HISTORY") != ""
	address := temporalAddress()
	if conn, err := net.DialTimeout("tcp", address, 2*time.Second); err != nil {
		infraAbsent(t, "temporal is not reachable at %s: %v", address, err)
	} else {
		_ = conn.Close()
	}
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Second)
	defer cancel()

	namespace := versioningNamespace()
	temporalClient := dialVersioningCluster(t, ctx, namespace)
	suffix := fmt.Sprintf("%d-%d", os.Getpid(), time.Now().UnixNano())
	runQueue := "ork-run-timeout-run-" + suffix
	orkerQueue := "ork-run-timeout-tasks-" + suffix
	deployment := task.Deployment{Name: "ork-run-timeout-" + suffix, BuildID: "v1"}

	runWorker := worker.New(temporalClient, runQueue, deployment.Apply(worker.Options{
		DisableRegistrationAliasing: true,
		LocalActivityWorkerOnly:     true,
	}))
	engine.RegisterWorkflows(runWorker)
	snapshots := newTestSnapshotStore()
	runWorker.RegisterActivityWithOptions(engine.NewSnapshotActivities(snapshots).WriteSnapshot,
		activity.RegisterOptions{Name: engine.ActivityWriteSnapshot})
	orkerWorker := worker.New(temporalClient, orkerQueue, deployment.Apply(worker.Options{
		DisableRegistrationAliasing: true,
		DisableWorkflowWorker:       true,
	}))
	orkerWorker.RegisterActivityWithOptions(runTimeoutOrker, activity.RegisterOptions{Name: task.ActivityExecuteTask})
	eventWorker := worker.New(temporalClient, engine.EventQueue, worker.Options{
		DisableRegistrationAliasing: true,
		DisableWorkflowWorker:       true,
	})
	eventWorker.RegisterActivityWithOptions(
		engine.NewEventActivities(engine.NewEventBus(), engine.NewRunStore()).WithSnapshotStore(snapshots).PublishEvent,
		activity.RegisterOptions{Name: engine.ActivityPublishEvent})
	for name, w := range map[string]worker.Worker{"run": runWorker, "orker": orkerWorker, "event": eventWorker} {
		must.OKf(t, w.Start(), "start %s worker", name)
		defer w.Stop()
	}
	setCurrentVersion(t, ctx, temporalClient, deployment, runQueue, orkerQueue)
	starter := engine.NewTemporalStarterWithOrkerQueue(temporalClient, runQueue, orkerQueue)

	for _, f := range runTimeoutFixtures {
		t.Run(f.name, func(t *testing.T) {
			plan := rawPlan(orkerQueue)
			if f.grace >= 0 {
				plan.RunTimeout = &engine.RunTimeoutPlan{HandlerGraceSeconds: f.grace,
					ResolutionsPerPass: engine.DefaultResolutionsPerPass}
			}
			runID := fmt.Sprintf("ork-run-timeout-%s-%d", f.name, time.Now().UnixNano())
			must.OKf(t, starter.StartRun(ctx, runID, f.definition(), nil, plan), "StartRun")
			if f.cancelAfter > 0 {
				// The run must be mid-task when the operator cancels; this is a
				// recorder, so a real wait on a real cluster is what it records.
				time.Sleep(f.cancelAfter)
				must.OKf(t, starter.CancelRun(ctx, runID, engine.CancelRequest{}), "CancelRun")
			}
			err := temporalClient.GetWorkflow(ctx, runID, "").Get(ctx, nil)
			requireRunTimeoutEnding(t, f, err)
			writeRecordedHistory(t, ctx, temporalClient, runID, f.path(), write)
		})
	}
}

// requireRunTimeoutEnding checks the recorded run ended as its fixture says.
func requireRunTimeoutEnding(t *testing.T, f runTimeoutFixture, err error) {
	t.Helper()
	switch f.wantCode {
	case "":
		must.OKf(t, err, "run %s did not succeed", f.name)
	case "CANCELED":
		var canceled *temporal.CanceledError
		if !errors.As(err, &canceled) {
			t.Fatalf("run %s ended %v, want canceled", f.name, err)
		}
	default:
		var applicationErr *temporal.ApplicationError
		if !errors.As(err, &applicationErr) || applicationErr.Type() != f.wantCode {
			t.Fatalf("run %s ended %v, want %s", f.name, err, f.wantCode)
		}
	}
}

// writeRecordedHistory reads runID's history, requires it to replay against
// current code, and writes it into the proof directory and, with write, to
// path.
func writeRecordedHistory(t *testing.T, ctx context.Context, c client.Client, runID, path string, write bool) {
	t.Helper()
	recorded := &history.History{}
	iter := c.GetWorkflowHistory(ctx, runID, "", false, enums.HISTORY_EVENT_FILTER_TYPE_ALL_EVENT)
	for iter.HasNext() {
		event, err := iter.Next()
		must.OKf(t, err, "read history")
		recorded.Events = append(recorded.Events, event)
	}
	must.OKf(t, replayRecorded(recorded, runID), "%s just recorded does not replay", path)
	encoded, err := temporalproto.CustomJSONMarshalOptions{Indent: "  "}.Marshal(recorded)
	must.OKf(t, err, "encode history")
	data := append(encoded, '\n')
	proof.Write(t, "run-timeout", filepath.Base(path), data)
	if write {
		must.OKf(t, os.WriteFile(path, data, 0o644), "write %s", path)
	}
	t.Logf("recorded %d history events for run %s (fixture written: %v)", len(recorded.Events), runID, write)
}
