//go:build integration

package engine_test

import (
	"context"
	"fmt"
	"net"
	"os"
	"path/filepath"
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
	"github.com/orkstack/orkstack/pkg/plugin"
)

// goldenHistoryPath is the committed replay fixture. replay_test.go replays it
// on every `make test`; this file is the only thing that writes it.
const goldenHistoryPath = "testdata/history/run-succeeded-v1.json"

// TestIntegration_RecordGoldenRunHistory captures the committed cross-version
// replay fixture from a REAL run on a real cluster.
//
// It is opt-in (ORK_RECORD_GOLDEN_HISTORY=1) and deliberately not part of any
// routine suite. The point of the fixture is that it was recorded once, by an
// older build, and never re-recorded: a guard that re-records before it
// replays only ever proves that today's code agrees with itself, which is the
// limitation this milestone's acceptance calls out. Re-record only when the
// engine's contract legitimately changes, and say so in the commit.
func TestIntegration_RecordGoldenRunHistory(t *testing.T) {
	if os.Getenv("ORK_RECORD_GOLDEN_HISTORY") == "" {
		t.Skip("set ORK_RECORD_GOLDEN_HISTORY=1 to re-record " + goldenHistoryPath +
			" — the committed fixture is meant to age, not to be refreshed")
	}

	address := temporalAddress()
	conn, err := net.DialTimeout("tcp", address, 2*time.Second)
	if err != nil {
		infraAbsent(t, "temporal is not reachable at %s: %v", address, err)
	}
	_ = conn.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	namespace := versioningNamespace()
	temporalClient := dialVersioningCluster(t, ctx, namespace)

	suffix := fmt.Sprintf("%d-%d", os.Getpid(), time.Now().UnixNano())
	runQueue := "ork-golden-run-" + suffix
	orkerQueue := "ork-golden-tasks-" + suffix

	// Record on a versioned worker, so the fixture carries the deployment
	// version and Pinned behaviour a released build stamps into history. That
	// makes the guard a replay of a *pinned v1* history against current code,
	// not merely of an unversioned one.
	deployment := task.Deployment{Name: "ork-golden-" + suffix, BuildID: "v1"}

	runWorker := worker.New(temporalClient, runQueue, deployment.Apply(worker.Options{
		DisableRegistrationAliasing: true,
		LocalActivityWorkerOnly:     true,
	}))
	// Through the shipped list, so the fixture carries whatever behaviour
	// `bin/ork` declares. A copy of it here would let the recorded history
	// claim Pinned while the binary registered something else.
	engine.RegisterWorkflows(runWorker)

	orkerWorker := worker.New(temporalClient, orkerQueue, deployment.Apply(worker.Options{
		DisableRegistrationAliasing: true,
		DisableWorkflowWorker:       true,
	}))
	orkerWorker.RegisterActivityWithOptions(
		func(_ context.Context, in task.ExecuteTaskInput) (task.ExecuteTaskResult, error) {
			return task.ExecuteTaskResult{
				TaskID: in.TaskID,
				Output: plugin.Payload{
					"echoed_task":    in.TaskID,
					"echoed_plugin":  in.Plugin,
					"upstream_count": float64(len(in.Upstream)),
				},
				Artifacts: []plugin.ArtifactRef{{
					Name: "report",
					URI:  fmt.Sprintf("file:///tmp/%s/%s.txt", in.RunID, in.TaskID),
				}},
			}, nil
		},
		activity.RegisterOptions{Name: task.ActivityExecuteTask},
	)

	// The projection worker is unversioned, exactly as `ork` runs it.
	eventWorker := worker.New(temporalClient, engine.EventQueue, worker.Options{
		DisableRegistrationAliasing: true,
		DisableWorkflowWorker:       true,
	})
	eventWorker.RegisterActivityWithOptions(
		engine.NewEventActivities(engine.NewEventBus(), engine.NewRunStore()).PublishEvent,
		activity.RegisterOptions{Name: engine.ActivityPublishEvent},
	)

	for name, w := range map[string]worker.Worker{
		"run": runWorker, "orker": orkerWorker, "event": eventWorker,
	} {
		if err := w.Start(); err != nil {
			t.Fatalf("start %s worker: %v", name, err)
		}
		defer w.Stop()
	}
	setCurrentVersion(t, ctx, temporalClient, deployment, runQueue, orkerQueue)

	runID := fmt.Sprintf("ork-golden-%d", time.Now().UnixNano())
	starter := engine.NewTemporalStarterWithOrkerQueue(temporalClient, runQueue, orkerQueue)
	if err := starter.StartRun(ctx, runID, goldenOrkflow(), nil); err != nil {
		t.Fatalf("StartRun: %v", err)
	}
	if err := temporalClient.GetWorkflow(ctx, runID, "").Get(ctx, nil); err != nil {
		t.Fatalf("run %s did not succeed: %v", runID, err)
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
	if len(recorded.Events) == 0 {
		t.Fatal("recorded an empty history")
	}

	// temporalproto's marshaller is what the SDK's
	// ReplayWorkflowHistoryFromJSONFile reads back, and it renders enums in
	// the same shorthand `temporal workflow show -o json` uses, so the fixture
	// stays readable in a diff.
	encoded, err := temporalproto.CustomJSONMarshalOptions{Indent: "  "}.Marshal(recorded)
	if err != nil {
		t.Fatalf("encode history: %v", err)
	}
	if err := os.MkdirAll(filepath.Dir(goldenHistoryPath), 0o755); err != nil {
		t.Fatalf("create fixture directory: %v", err)
	}
	if err := os.WriteFile(goldenHistoryPath, append(encoded, '\n'), 0o644); err != nil {
		t.Fatalf("write %s: %v", goldenHistoryPath, err)
	}
	t.Logf("recorded %d history events for run %s into %s (deployment version %s)",
		len(recorded.Events), runID, goldenHistoryPath, deployment)
}

// goldenOrkflow is the shape the fixture must exercise, so that a later change
// to the workflow body actually shows up as a replay failure: a fan-out, a
// transitive `needs` closure, a task skipped by a condition, a task skipped
// because its upstream was, and artifacts threaded downstream.
func goldenOrkflow() *orkstackv1.Orkflow {
	with := func(pairs map[string]any) *structpb.Struct {
		s, err := structpb.NewStruct(pairs)
		if err != nil {
			panic(err)
		}
		return s
	}
	return &orkstackv1.Orkflow{
		Name:    "golden-replay",
		Version: "1.0.0",
		Tasks: []*orkstackv1.Task{
			{Id: "fetch", Plugin: "http", With: with(map[string]any{"url": "https://example.invalid/a"})},
			{Id: "transform", Plugin: "shell", Needs: []string{"fetch"}, With: with(map[string]any{"run": "echo transform"})},
			{Id: "sidecar", Plugin: "shell", Needs: []string{"fetch"}, With: with(map[string]any{"run": "echo sidecar"})},
			{Id: "gated", Plugin: "shell", Needs: []string{"transform"}, Condition: "false"},
			{Id: "after-gate", Plugin: "shell", Needs: []string{"gated"}},
			{Id: "publish", Plugin: "file", Needs: []string{"transform", "sidecar"},
				With: with(map[string]any{"path": "/tmp/out.txt"})},
		},
	}
}
