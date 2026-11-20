//go:build integration

package engine_test

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"connectrpc.com/connect"
	"go.temporal.io/api/enums/v1"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/converter"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/api"
	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/pkg/artifacts"

	"github.com/orkstack/orkstack/internal/testsupport/must"
)

// startedPlan reads the plan a run's execution started with from the
// cluster's own record of the start.
func startedPlan(t *testing.T, ctx context.Context, c client.Client, runID string) engine.RunPlan {
	t.Helper()
	iter := c.GetWorkflowHistory(ctx, runID, "", false, enums.HISTORY_EVENT_FILTER_TYPE_ALL_EVENT)
	if !iter.HasNext() {
		t.Fatalf("%s has no history", runID)
	}
	first, err := iter.Next()
	must.OKf(t, err, "read %s's first event", runID)
	payloads := first.GetWorkflowExecutionStartedEventAttributes().GetInput().GetPayloads()
	if len(payloads) < 4 {
		t.Fatalf("%s started with %d arguments", runID, len(payloads))
	}
	var plan engine.RunPlan
	must.OKf(t, converter.GetDefaultDataConverter().FromPayload(payloads[3], &plan), "decode %s's plan", runID)
	return plan
}

// runContextObjects lists the run-context objects under a run's prefix.
func runContextObjects(t *testing.T, root, namespace, runID string) []string {
	t.Helper()
	prefix, err := artifacts.RunSnapshotPrefix(namespace, runID)
	must.OK(t, err)
	found, err := filepath.Glob(filepath.Join(root, filepath.FromSlash(prefix), "run-context-*.json"))
	must.OK(t, err)
	return found
}

// TestIntegration_ReissuedRunIDGetsItsOwnObject: a run whose first id the
// cluster refuses is started under a reissued one, and the run-context object
// its plan names is under the id that started, readable and verified; the
// object names neither id, so both copies are the same bytes.
func TestIntegration_ReissuedRunIDGetsItsOwnObject(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	c := dialRunIDCluster(t, ctx)
	starter := engine.NewTemporalStarter(c, runIDQueue())
	wf := twoTaskOrkflow()

	taken, reissued := engine.NewRunID(), engine.NewRunID()
	terminateAll(t, c, taken, reissued)
	must.OKf(t, starter.StartRun(ctx, taken, wf, nil, defaultPlan()), "occupy %s", taken)

	orkflows := engine.NewMemoryOrkflowStore()
	if _, _, err := orkflows.Put(ctx, wf, artifacts.NoHeadExpectation()); err != nil {
		t.Fatalf("store orkflow: %v", err)
	}
	root := t.TempDir()
	store, err := artifacts.NewLocalRunSnapshotStore(root)
	must.OK(t, err)
	candidates := []string{taken, reissued}
	svc := api.NewService(api.Config{
		Orkflows:    orkflows,
		Runs:        engine.NewRunStore(),
		Bus:         engine.NewEventBus(),
		Starter:     starter,
		RunContexts: store,
		NewRunID: func() string {
			id := candidates[0]
			candidates = candidates[1:]
			return id
		},
	})
	resp, err := svc.SubmitRun(ctx, connect.NewRequest(&orkstackv1.SubmitRunRequest{OrkflowName: wf.GetName()}))
	must.OKf(t, err, "SubmitRun against a taken id")
	if got := resp.Msg.GetRun().GetId(); got != reissued {
		t.Fatalf("SubmitRun returned %q, want the reissued id %q", got, reissued)
	}

	plan := startedPlan(t, ctx, c, reissued)
	if plan.RunContextSHA256 == "" || plan.RunContextMaxBytes != engine.DefaultRunContextMaxBytes {
		t.Fatalf("the started plan's run context = %q, %d", plan.RunContextSHA256, plan.RunContextMaxBytes)
	}
	obj, err := store.ReadRunContext(ctx, "default", reissued, plan.RunContextSHA256)
	must.OKf(t, err, "read %s's run context", reissued)
	if obj.Root.Orkflow.Name != wf.GetName() || obj.Root.Origin.Kind != orkstackv1.RunOriginKind_RUN_ORIGIN_KIND_SUBMIT.String() {
		t.Fatalf("object root = %+v", obj.Root)
	}
	// The refused attempt wrote under the id it tried; the object is the
	// same bytes there, named by the same SHA-256.
	if _, err := store.ReadRunContext(ctx, "default", taken, plan.RunContextSHA256); err != nil {
		t.Fatalf("the refused id's copy: %v", err)
	}
	if n := len(runContextObjects(t, root, "default", reissued)); n != 1 {
		t.Fatalf("%d run-context objects under %s, want 1", n, reissued)
	}
}

// TestIntegration_ComposedTreeWritesOneObjectUnderItsRoot: a run tree has one
// run-context object, written at submit under the root; nothing is written
// under a child's id, so a child that never starts leaves nothing behind.
func TestIntegration_ComposedTreeWritesOneObjectUnderItsRoot(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	c := dialRunIDCluster(t, ctx)
	starter := engine.NewTemporalStarter(c, runIDQueue())

	child := &orkstackv1.Orkflow{Name: "child-ork", Version: "1", Tasks: []*orkstackv1.Task{{Id: "leaf", Uses: "shell"}}}
	parent := &orkstackv1.Orkflow{Name: "parent-ork", Version: "1", Tasks: []*orkstackv1.Task{
		{Id: "compose", Uses: "orkflow:child-ork@1"},
	}}
	orkflows := engine.NewMemoryOrkflowStore()
	for _, wf := range []*orkstackv1.Orkflow{child, parent} {
		if _, _, err := orkflows.Put(ctx, wf, artifacts.NoHeadExpectation()); err != nil {
			t.Fatalf("store %s: %v", wf.GetName(), err)
		}
	}
	root := t.TempDir()
	store, err := artifacts.NewLocalRunSnapshotStore(root)
	must.OK(t, err)
	svc := api.NewService(api.Config{
		Orkflows: orkflows, Runs: engine.NewRunStore(), Bus: engine.NewEventBus(),
		Starter: starter, RunContexts: store,
	})
	resp, err := svc.SubmitRun(ctx, connect.NewRequest(&orkstackv1.SubmitRunRequest{OrkflowName: parent.GetName()}))
	must.OKf(t, err, "submit the parent")
	runID := resp.Msg.GetRun().GetId()
	terminateAll(t, c, runID)

	plan := startedPlan(t, ctx, c, runID)
	planned, ok := plan.Children["compose"]
	if !ok {
		t.Fatalf("the plan has no child for compose: %v", plan.Children)
	}
	if planned.Plan.RunContextIndex != 1 {
		t.Fatalf("the child's tree index = %d, want 1", planned.Plan.RunContextIndex)
	}
	obj, err := store.ReadRunContext(ctx, "default", runID, plan.RunContextSHA256)
	must.OKf(t, err, "read the tree's run context")
	if len(obj.Entries) != 2 || obj.Entries[1].ParentTaskID != "compose" || obj.Entries[1].Orkflow.Name != "child-ork" {
		t.Fatalf("entries = %+v", obj.Entries)
	}
	if n := len(runContextObjects(t, root, "default", planned.RunID)); n != 0 {
		t.Fatalf("%d run-context objects under the unstarted child %s", n, planned.RunID)
	}
}
