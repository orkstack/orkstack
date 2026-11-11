//go:build integration

package engine_test

import (
	"context"
	"fmt"
	"net"
	"os"
	"strings"
	"testing"
	"time"

	"go.temporal.io/api/enums/v1"
	"go.temporal.io/api/history/v1"
	"go.temporal.io/api/temporalproto"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/worker"
	"google.golang.org/protobuf/types/known/structpb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/internal/task"
)

// Opt-in recorder of testdata/history/run-cross-namespace-v1.json: one run of
// the cached pipeline whose planned definition another namespace shared —
// it runs in "search" and names "platform" as its owner — by a versioned
// worker, with stand-in Orker results (every cached task a miss).
func TestIntegration_RecordCrossNamespaceRunHistory(t *testing.T) {
	if os.Getenv("ORK_RECORD_GOLDEN_HISTORY") == "" {
		t.Skip("set ORK_RECORD_GOLDEN_HISTORY=1 to record " + crossNamespaceHistory +
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
	runQueue := "ork-shared-rec-run-" + suffix
	orkerQueue := "ork-shared-rec-tasks-" + suffix
	deployment := task.Deployment{Name: "ork-shared-rec-" + suffix, BuildID: "v1"}

	runWorker := worker.New(temporalClient, runQueue, deployment.Apply(worker.Options{
		DisableRegistrationAliasing: true, LocalActivityWorkerOnly: true,
	}))
	engine.RegisterWorkflows(runWorker)
	snapshots := newTestSnapshotStore()
	runWorker.RegisterActivityWithOptions(engine.NewSnapshotActivities(snapshots).WriteSnapshot,
		activity.RegisterOptions{Name: engine.ActivityWriteSnapshot})
	runWorker.RegisterActivityWithOptions(engine.NewEachActivities(&memValues{objects: map[string][]byte{}}).CountEachItems,
		activity.RegisterOptions{Name: engine.ActivityCountEachItems})
	orkerWorker := worker.New(temporalClient, orkerQueue, deployment.Apply(worker.Options{
		DisableRegistrationAliasing: true, DisableWorkflowWorker: true,
	}))
	orkerWorker.RegisterActivityWithOptions(func(_ context.Context, in *orkstackv1.ExecuteTaskInput) (*orkstackv1.ExecuteTaskResult, error) {
		result := &orkstackv1.ExecuteTaskResult{
			TaskId: in.GetTaskId(), Attempt: 1, OrkerId: "fixture",
			ValuesJson: []byte(`{}`), ValuesInlined: true,
			ValuesRef: &orkstackv1.ArtifactRef{Name: "values.json", Uri: "mem://values/" + in.GetRunId() + "/" + in.GetTaskId(),
				Sha256: strings.Repeat("a", 64), SizeBytes: 2},
			Commit: &orkstackv1.CommitRef{Uri: in.GetCommitUri(), Sha256: strings.Repeat("b", 64)},
		}
		if in.GetCache() != nil {
			key := strings.Repeat("c", 64)
			result.Cache = &orkstackv1.CacheResult{Outcome: orkstackv1.CacheOutcome_CACHE_OUTCOME_MISS, Key: key,
				MissReason: "MEMO_ABSENT", MissDetail: "no memo for key " + key[:12] + "…", MemoWritten: true}
		}
		return result, nil
	}, activity.RegisterOptions{Name: task.ActivityExecuteTask})
	eventWorker := worker.New(temporalClient, engine.EventQueue, worker.Options{
		DisableRegistrationAliasing: true, DisableWorkflowWorker: true,
	})
	eventWorker.RegisterActivityWithOptions(
		engine.NewEventActivities(engine.NewEventBus(), engine.NewRunStore()).WithSnapshotStore(snapshots).PublishEvent,
		activity.RegisterOptions{Name: engine.ActivityPublishEvent})
	for name, w := range map[string]worker.Worker{"run": runWorker, "orker": orkerWorker, "event": eventWorker} {
		if err := w.Start(); err != nil {
			t.Fatalf("start %s worker: %v", name, err)
		}
		defer w.Stop()
	}
	setCurrentVersion(t, ctx, temporalClient, deployment, runQueue, orkerQueue)
	starter := engine.NewTemporalStarterWithOrkerQueue(temporalClient, runQueue, orkerQueue)
	wf := compileFixture(t, "cached-pipeline.ork.yaml")
	wf.Namespace, wf.DefinitionNamespace = "search", "platform"
	plan := rawPlan(orkerQueue)
	plan.CommitRoot = "mem://commits"
	plan.Cache = &engine.CachePlan{MaxTTLSeconds: int64(engine.DefaultCacheMaxTTL / time.Second)}
	data, _ := structpb.NewStruct(map[string]any{"rows": 1000})
	runID := fmt.Sprintf("ork-shared-fixture-%d", time.Now().UnixNano())
	if err := starter.StartRun(ctx, runID, wf, &orkstackv1.Payload{Data: data}, plan); err != nil {
		t.Fatalf("StartRun: %v", err)
	}
	if err := temporalClient.GetWorkflow(ctx, runID, "").Get(ctx, nil); err != nil {
		t.Fatalf("fixture run %s did not succeed: %v", runID, err)
	}
	recorded := &history.History{}
	iter := temporalClient.GetWorkflowHistory(ctx, runID, "", false, enums.HISTORY_EVENT_FILTER_TYPE_ALL_EVENT)
	for iter.HasNext() {
		event, err := iter.Next()
		if err != nil {
			t.Fatalf("read history: %v", err)
		}
		recorded.Events = append(recorded.Events, event)
	}
	encoded, err := temporalproto.CustomJSONMarshalOptions{Indent: "  "}.Marshal(recorded)
	if err != nil {
		t.Fatalf("encode history: %v", err)
	}
	if err := os.WriteFile(crossNamespaceHistory, append(encoded, '\n'), 0o644); err != nil {
		t.Fatalf("write %s: %v", crossNamespaceHistory, err)
	}
	t.Logf("recorded %d history events for run %s into %s (deployment version %s)",
		len(recorded.Events), runID, crossNamespaceHistory, deployment)
}
