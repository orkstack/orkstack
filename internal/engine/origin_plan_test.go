package engine_test

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/workflow"
	"google.golang.org/protobuf/proto"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/internal/task"
)

func TestRunOrigin_ProtoIsNilSafeAndTolerant(t *testing.T) {
	var none *engine.RunOrigin
	if got := none.Proto(); got != nil {
		t.Fatalf("nil origin Proto() = %v, want nil", got)
	}
	if got := engine.OriginFromProto(nil); got != nil {
		t.Fatalf("OriginFromProto(nil) = %+v, want nil", got)
	}
	unknown := &engine.RunOrigin{Kind: "RUN_ORIGIN_KIND_TELEPATHY", ID: "x"}
	if got := unknown.Proto().GetKind(); got != orkstackv1.RunOriginKind_RUN_ORIGIN_KIND_UNSPECIFIED {
		t.Fatalf("an unknown kind name rendered %v, want UNSPECIFIED", got)
	}
	want := &orkstackv1.RunOrigin{
		Kind:        orkstackv1.RunOriginKind_RUN_ORIGIN_KIND_TRIGGER,
		Id:          "trg-4a1c0e2b7d3f8156",
		Reference:   "https://github.com/acme/svc/actions/runs/123",
		Description: "GitHub deploy hook",
		Principal:   "ci-github",
	}
	plan := engine.OriginFromProto(want)
	if plan.Kind != "RUN_ORIGIN_KIND_TRIGGER" {
		t.Fatalf("plan kind = %q, want the enum's proto name", plan.Kind)
	}
	if got := plan.Proto(); !proto.Equal(got, want) {
		t.Fatalf("round trip = %v, want %v", got, want)
	}
}

// TestRunPlan_EncodesIdenticallyWithoutOrigin pins the bytes: a plan with no
// origin — every plan recorded before this field existed — must encode
// exactly as it did, or the golden history fixture stops matching.
func TestRunPlan_EncodesIdenticallyWithoutOrigin(t *testing.T) {
	plan := engine.RunPlan{
		Namespace:             "payments",
		Routes:                map[string]string{"main": "ork-tasks"},
		CommitRoot:            "s3://orkstack-artifacts",
		Limits:                engine.WorkspaceLimits{ValuesMaxBytes: 1, ArtifactMaxCount: 2, ArtifactMaxBytes: 3, LogMaxBytes: 4, CommitMaxBytes: 5},
		TransferBudgetSeconds: 60,
	}
	const golden = `{"namespace":"payments","routes":{"main":"ork-tasks"},"commit_root":"s3://orkstack-artifacts",` +
		`"limits":{"values_max_bytes":1,"artifact_max_count":2,"artifact_max_bytes":3,"log_max_bytes":4,"commit_max_bytes":5},` +
		`"transfer_budget_seconds":60}`
	out, err := json.Marshal(plan)
	if err != nil {
		t.Fatal(err)
	}
	if string(out) != golden {
		t.Fatalf("a plan with no origin encodes\n%s\nwant the pre-origin bytes\n%s", out, golden)
	}

	plan.Origin = &engine.RunOrigin{Kind: "RUN_ORIGIN_KIND_SUBMIT"}
	out, err = json.Marshal(plan)
	if err != nil {
		t.Fatal(err)
	}
	if want := golden[:len(golden)-1] + `,"origin":{"kind":"RUN_ORIGIN_KIND_SUBMIT"}}`; string(out) != want {
		t.Fatalf("a SUBMIT origin encodes\n%s\nwant\n%s", out, want)
	}
}

// snapshotsFor runs the linear orkflow with plan and returns every snapshot
// the projection activity received.
func snapshotsFor(t *testing.T, runID string, plan engine.RunPlan) []*orkstackv1.Run {
	t.Helper()
	var suite testsuite.WorkflowTestSuite
	env := generousDeadlock(suite.NewTestWorkflowEnvironment())
	env.SetTestTimeout(60 * time.Second)
	env.RegisterWorkflowWithOptions(engine.RunWorkflow, workflow.RegisterOptions{Name: engine.RunWorkflowType})
	var snapshots []*orkstackv1.Run
	var executed []*orkstackv1.ExecuteTaskInput
	env.RegisterActivityWithOptions(
		func(_ context.Context, run *orkstackv1.Run, sequence int64) (string, error) {
			snapshots = append(snapshots, run)
			return fmt.Sprintf("memory://snapshots/%s/%d.pb", run.GetId(), sequence), nil
		},
		activity.RegisterOptions{Name: engine.ActivityWriteSnapshot},
	)
	env.RegisterActivityWithOptions(
		func(context.Context, *orkstackv1.Event, string, string) error { return nil },
		activity.RegisterOptions{Name: engine.ActivityPublishEvent},
	)
	env.RegisterActivityWithOptions(
		func(_ context.Context, in *orkstackv1.ExecuteTaskInput) (*orkstackv1.ExecuteTaskResult, error) {
			executed = append(executed, in)
			return &orkstackv1.ExecuteTaskResult{TaskId: in.TaskId, ValuesJson: valuesJSON(map[string]any{"stdout": in.TaskId})}, nil
		},
		activity.RegisterOptions{Name: task.ActivityExecuteTask},
	)
	env.ExecuteWorkflow(engine.RunWorkflow, runID, compile(t, linearDocument), payload(t, nil), plan, (*orkstackv1.RunContinuation)(nil))
	if err := env.GetWorkflowError(); err != nil {
		t.Fatalf("workflow error: %v", err)
	}
	if len(snapshots) == 0 || len(executed) == 0 {
		t.Fatalf("the workflow wrote %d snapshots and executed %d tasks", len(snapshots), len(executed))
	}
	return snapshots
}

func TestSnapshot_CarriesPlanOrigin(t *testing.T) {
	plan := defaultPlan()
	plan.Origin = &engine.RunOrigin{
		Kind: "RUN_ORIGIN_KIND_TRIGGER", ID: "trg-1", Reference: "INC-4412",
		Description: "deploy hook", Principal: "ci-github",
	}
	want := plan.Origin.Proto()
	for i, run := range snapshotsFor(t, "ork-origin-1", plan) {
		if !proto.Equal(run.GetOrigin(), want) {
			t.Fatalf("snapshot %d origin = %v, want %v on every snapshot", i, run.GetOrigin(), want)
		}
	}
	for i, run := range snapshotsFor(t, "ork-origin-2", defaultPlan()) {
		if run.GetOrigin() != nil {
			t.Fatalf("snapshot %d of a plan with no origin carries %v, want none", i, run.GetOrigin())
		}
	}
}

func TestStartRun_StampsOriginMemoAndAttributesFromPlan(t *testing.T) {
	start := func(t *testing.T, starter *engine.TemporalStarter, c *recordingClient, origin *engine.RunOrigin) {
		t.Helper()
		plan := defaultPlan()
		plan.Origin = origin
		stored := twoTaskOrkflow()
		stored.Namespace = "payments"
		if err := starter.StartRun(context.Background(), "ork-1-1", stored, nil, plan); err != nil {
			t.Fatalf("StartRun: %v", err)
		}
		if got := c.startedPlan(t).Origin; got != origin {
			t.Fatalf("the plan handed to Temporal carries origin %+v, want %+v", got, origin)
		}
	}
	trigger := &engine.RunOrigin{Kind: "RUN_ORIGIN_KIND_TRIGGER", ID: "trg-1", Reference: "ref"}

	t.Run("memo carries slug and id; attributes only with the index", func(t *testing.T) {
		c := &recordingClient{}
		start(t, engine.NewTemporalStarter(c, task.DefaultQueue), c, trigger)
		if c.options.Memo[engine.MemoOriginKind] != "trigger" || c.options.Memo[engine.MemoOriginID] != "trg-1" {
			t.Fatalf("memo = %v, want trigger/trg-1", c.options.Memo)
		}
		if c.options.TypedSearchAttributes.Size() != 0 {
			t.Fatalf("attributes stamped without either index: %d", c.options.TypedSearchAttributes.Size())
		}
	})
	t.Run("with the origin index the pair is stamped", func(t *testing.T) {
		c := &recordingClient{}
		start(t, engine.NewTemporalStarter(c, task.DefaultQueue).WithOriginIndex(true), c, trigger)
		kind, ok := c.options.TypedSearchAttributes.GetKeyword(task.SearchKeyOrkflowOriginKind)
		if !ok || kind != "trigger" {
			t.Fatalf("OrkflowOriginKind = %q, %v; want trigger", kind, ok)
		}
		id, ok := c.options.TypedSearchAttributes.GetKeyword(task.SearchKeyOrkflowOriginID)
		if !ok || id != "trg-1" {
			t.Fatalf("OrkflowOriginId = %q, %v; want trg-1", id, ok)
		}
		if _, ok := c.options.TypedSearchAttributes.GetKeyword(task.SearchKeyOrkflowNamespace); ok {
			t.Fatal("the namespace attribute was stamped without its own index")
		}
	})
	t.Run("both indexes stamp all three at once", func(t *testing.T) {
		c := &recordingClient{}
		start(t, engine.NewTemporalStarter(c, task.DefaultQueue).WithNamespaceIndex(true).WithOriginIndex(true), c, trigger)
		if c.options.TypedSearchAttributes.Size() != 3 {
			t.Fatalf("stamped %d attributes, want namespace, kind and id", c.options.TypedSearchAttributes.Size())
		}
		ns, _ := c.options.TypedSearchAttributes.GetKeyword(task.SearchKeyOrkflowNamespace)
		if ns != "payments" {
			t.Fatalf("OrkflowNamespace = %q, want payments", ns)
		}
	})
	t.Run("a nil origin stamps neither memo nor attribute", func(t *testing.T) {
		c := &recordingClient{}
		start(t, engine.NewTemporalStarter(c, task.DefaultQueue).WithOriginIndex(true), c, nil)
		if _, ok := c.options.Memo[engine.MemoOriginKind]; ok {
			t.Fatalf("memo = %v, want no origin keys", c.options.Memo)
		}
		if _, ok := c.options.Memo[engine.MemoOriginID]; ok {
			t.Fatalf("memo = %v, want no origin keys", c.options.Memo)
		}
		if c.options.TypedSearchAttributes.Size() != 0 {
			t.Fatalf("a nil origin stamped %d attributes", c.options.TypedSearchAttributes.Size())
		}
	})
	t.Run("submit stamps kind submit and an empty id", func(t *testing.T) {
		c := &recordingClient{}
		start(t, engine.NewTemporalStarter(c, task.DefaultQueue).WithOriginIndex(true), c, &engine.RunOrigin{Kind: "RUN_ORIGIN_KIND_SUBMIT"})
		if c.options.Memo[engine.MemoOriginKind] != "submit" || c.options.Memo[engine.MemoOriginID] != "" {
			t.Fatalf("memo = %v, want submit and an empty id", c.options.Memo)
		}
		id, ok := c.options.TypedSearchAttributes.GetKeyword(task.SearchKeyOrkflowOriginID)
		if !ok || id != "" {
			t.Fatalf("OrkflowOriginId = %q, %v; want an empty keyword", id, ok)
		}
	})
}
