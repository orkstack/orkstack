//go:build integration

package engine_test

import (
	"context"
	"fmt"
	"net"
	"os"
	"testing"
	"time"

	"go.temporal.io/api/enums/v1"
	"go.temporal.io/api/history/v1"
	"go.temporal.io/api/temporalproto"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/worker"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/internal/task"

	"github.com/orkstack/orkstack/internal/testsupport/must"
)

// Records the keyed-run replay fixture from a REAL run on a versioned
// worker: a run started under an idempotency key, whose plan carries the
// submission's fingerprint. Opt-in, exactly as the other recorders: the
// fixture is recorded once and then ages.
func TestIntegration_RecordKeyedRunHistory(t *testing.T) {
	if os.Getenv("ORK_RECORD_GOLDEN_HISTORY") == "" {
		t.Skip("set ORK_RECORD_GOLDEN_HISTORY=1 to record " + keyedHistory +
			" — the committed fixture is meant to age, not to be refreshed")
	}
	address := temporalAddress()
	if conn, err := net.DialTimeout("tcp", address, 2*time.Second); err != nil {
		infraAbsent(t, "temporal is not reachable at %s: %v", address, err)
	} else {
		_ = conn.Close()
	}
	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancel()

	namespace := versioningNamespace()
	temporalClient := dialVersioningCluster(t, ctx, namespace)
	suffix := fmt.Sprintf("%d-%d", os.Getpid(), time.Now().UnixNano())
	runQueue := "ork-keyed-run-" + suffix
	orkerQueue := "ork-keyed-tasks-" + suffix
	deployment := task.Deployment{Name: "ork-keyed-" + suffix, BuildID: "v1"}

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
	orkerWorker.RegisterActivityWithOptions(
		func(_ context.Context, in *orkstackv1.ExecuteTaskInput) (*orkstackv1.ExecuteTaskResult, error) {
			return &orkstackv1.ExecuteTaskResult{
				TaskId:     in.TaskId,
				ValuesJson: valuesJSON(map[string]any{"echoed_task": in.TaskId, "upstream_count": float64(len(in.Upstream))}),
			}, nil
		},
		activity.RegisterOptions{Name: task.ActivityExecuteTask})
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

	// a → b, started under a key: the plan carries the fingerprint.
	wf := &orkstackv1.Orkflow{
		Name:    "keyed-replay",
		Version: "1.0.0",
		Tasks: []*orkstackv1.Task{
			{Id: "a", Uses: "shell"},
			{Id: "b", Uses: "shell", Needs: []string{"a"}},
		},
	}
	plan := rawPlan(orkerQueue)
	plan.IdempotencyFingerprint = keyedFixtureFingerprint
	runID := fmt.Sprintf("ork-keyed-%d", time.Now().UnixNano())
	must.OKf(t, starter.StartRun(ctx, runID, wf, nil, plan), "StartRun")
	must.OKf(t, temporalClient.GetWorkflow(ctx, runID, "").Get(ctx, nil), "keyed run %s did not succeed", runID)

	recorded := &history.History{}
	iter := temporalClient.GetWorkflowHistory(ctx, runID, "", false, enums.HISTORY_EVENT_FILTER_TYPE_ALL_EVENT)
	for iter.HasNext() {
		event, err := iter.Next()
		must.OKf(t, err, "read history")
		recorded.Events = append(recorded.Events, event)
	}
	encoded, err := temporalproto.CustomJSONMarshalOptions{Indent: "  "}.Marshal(recorded)
	must.OKf(t, err, "encode history")
	must.OKf(t, os.WriteFile(keyedHistory, append(encoded, '\n'), 0o644), "write %s", keyedHistory)
	t.Logf("recorded %d history events for run %s into %s (deployment version %s)",
		len(recorded.Events), runID, keyedHistory, deployment)
}
