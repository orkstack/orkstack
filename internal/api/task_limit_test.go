package api_test

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"connectrpc.com/connect"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/api"
	"github.com/orkstack/orkstack/internal/engine"
)

// manyTasksDoc is an Orkflow of n plain tasks, none of them a fan-out.
func manyTasksDoc(name string, n int) string {
	var b strings.Builder
	fmt.Fprintf(&b, "apiVersion: orkstack/v1\nkind: Orkflow\nname: %s\ntasks:\n", name)
	for i := range n {
		fmt.Fprintf(&b, "  - id: t%d\n    uses: busybox:latest\n", i)
	}
	return b.String()
}

// The task ceiling's refusal names the count, the limit and the setting that
// moves it, in every place it is asked.
func requireTaskCeilingRefusal(t *testing.T, message string, count, limit int) {
	t.Helper()
	for _, want := range []string{
		fmt.Sprintf("declares %d tasks", count),
		fmt.Sprintf("this deployment allows at most %d", limit),
		"engine.orkflow_max_tasks", "ORK_ORKFLOW_MAX_TASKS",
	} {
		if !strings.Contains(message, want) {
			t.Fatalf("refusal %q does not say %q", message, want)
		}
	}
}

func TestValidateOrkflow_MoreTasksThanTheCeilingIsAnIssue(t *testing.T) {
	n := int(engine.DefaultOrkflowMaxTasks)
	svc := api.NewService(api.Config{Orkflows: engine.NewMemoryOrkflowStore()})
	resp, err := svc.ValidateOrkflow(context.Background(), connect.NewRequest(&orkstackv1.ValidateOrkflowRequest{
		Yaml: manyTasksDoc("wide", n+1)}))
	if err != nil {
		t.Fatal(err)
	}
	issues := resp.Msg.GetIssues()
	if len(issues) != 1 || issues[0].GetSeverity() != orkstackv1.ValidationSeverity_VALIDATION_SEVERITY_ERROR {
		t.Fatalf("issues = %v; want one error", issues)
	}
	// Anchored at the first task past the ceiling, so an editor can point at it.
	if want := fmt.Sprintf("tasks[%d]", n); issues[0].GetField() != want {
		t.Fatalf("field = %q; want %s", issues[0].GetField(), want)
	}
	requireTaskCeilingRefusal(t, issues[0].GetMessage(), n+1, n)

	// Exactly at the default ceiling is accepted.
	ok, err := svc.ValidateOrkflow(context.Background(), connect.NewRequest(&orkstackv1.ValidateOrkflowRequest{
		Yaml: manyTasksDoc("wide", n)}))
	if err != nil || len(ok.Msg.GetIssues()) != 0 {
		t.Fatalf("%d tasks = %v, %v; want accepted", n, ok.Msg.GetIssues(), err)
	}
}

func TestApplyOrkflow_MoreTasksThanTheCeilingIsInvalidArgumentAndWritesNothing(t *testing.T) {
	n := int(engine.DefaultOrkflowMaxTasks)
	store := engine.NewMemoryOrkflowStore()
	svc := api.NewService(api.Config{Orkflows: store})
	_, err := svc.ApplyOrkflow(context.Background(), connect.NewRequest(&orkstackv1.ApplyOrkflowRequest{
		Yaml: manyTasksDoc("wide", n+1)}))
	if connect.CodeOf(err) != connect.CodeInvalidArgument {
		t.Fatalf("apply = %v; want invalid_argument", err)
	}
	requireTaskCeilingRefusal(t, err.Error(), n+1, n)
	if _, getErr := store.Get(context.Background(), "default", "wide", ""); getErr == nil {
		t.Fatal("a refused apply stored a definition")
	}
}

func TestPlanRun_DocumentWithMoreTasksThanTheCeilingIsInvalidArgument(t *testing.T) {
	svc := api.NewService(api.Config{Orkflows: engine.NewMemoryOrkflowStore(), OrkflowMaxTasks: 3})
	_, err := svc.PlanRun(context.Background(), connect.NewRequest(&orkstackv1.PlanRunRequest{
		Yaml: manyTasksDoc("wide", 4)}))
	if connect.CodeOf(err) != connect.CodeInvalidArgument {
		t.Fatalf("plan = %v; want invalid_argument", err)
	}
	requireTaskCeilingRefusal(t, err.Error(), 4, 3)
}

// The ceiling is a deployment-configurable default: a deployment may lower it
// and may raise it.
func TestValidateOrkflow_TheTaskCeilingIsConfigurable(t *testing.T) {
	lowered := api.NewService(api.Config{Orkflows: engine.NewMemoryOrkflowStore(), OrkflowMaxTasks: 3})
	resp, err := lowered.ValidateOrkflow(context.Background(), connect.NewRequest(&orkstackv1.ValidateOrkflowRequest{
		Yaml: manyTasksDoc("wide", 4)}))
	if err != nil {
		t.Fatal(err)
	}
	if issues := resp.Msg.GetIssues(); len(issues) != 1 || issues[0].GetField() != "tasks[3]" {
		t.Fatalf("lowered to 3, 4 tasks: issues = %v", issues)
	} else {
		requireTaskCeilingRefusal(t, issues[0].GetMessage(), 4, 3)
	}

	n := int(engine.DefaultOrkflowMaxTasks)
	raised := api.NewService(api.Config{Orkflows: engine.NewMemoryOrkflowStore(), OrkflowMaxTasks: int32(n + 500)})
	ok, err := raised.ValidateOrkflow(context.Background(), connect.NewRequest(&orkstackv1.ValidateOrkflowRequest{
		Yaml: manyTasksDoc("wide", n+1)}))
	if err != nil || len(ok.Msg.GetIssues()) != 0 {
		t.Fatalf("raised to %d, %d tasks = %v, %v; want accepted", n+500, n+1, ok.Msg.GetIssues(), err)
	}
}

// A fan-out is one declared task however many items it declares: its items
// are bounded by max_items and ORK_EACH_MAX_ITEMS already.
func TestValidateOrkflow_FanOutItemsAreNotCountedAsTasks(t *testing.T) {
	svc := api.NewService(api.Config{Orkflows: engine.NewMemoryOrkflowStore(), OrkflowMaxTasks: 2})
	resp, err := svc.ValidateOrkflow(context.Background(), connect.NewRequest(&orkstackv1.ValidateOrkflowRequest{
		Yaml: `
apiVersion: orkstack/v1
kind: Orkflow
name: fan
inputs:
  parts: {type: array}
tasks:
  - id: convert
    uses: busybox:latest
    each: {over: "{{ .inputs.parts }}", max_items: 500}
  - id: after
    uses: busybox:latest
    needs: [convert]
`}))
	if err != nil || len(resp.Msg.GetIssues()) != 0 {
		t.Fatalf("2 tasks, one a 500-item fan-out, under a ceiling of 2 = %v, %v; want accepted", resp.Msg.GetIssues(), err)
	}
}

// A composed task is one task of its parent: the child is a separate run with
// its own history, held to the ceiling when it was itself applied.
func TestValidateOrkflow_ComposedChildTasksAreNotCounted(t *testing.T) {
	f := newComposeFixture(t, func(cfg *api.Config) { cfg.OrkflowMaxTasks = 2 })
	f.putStored(t, "release-block", "2.1.0", "sha256:rb",
		shellTask("a"), shellTask("b"), shellTask("c"), shellTask("d"), shellTask("e"))
	issues := f.validate(t, composeParentYAMLWithout(t))
	for _, issue := range issues {
		if issue.GetSeverity() == errorIssue {
			t.Fatalf("a 2-task parent composing a 5-task child, under a ceiling of 2, was refused: %v", issue)
		}
	}
}

// Lowering the ceiling refuses new runs of a version applied under the old
// one — before any run id is minted — as lowering ORK_EACH_MAX_ITEMS does.
func TestSubmitRun_LoweredTaskCeilingIsFailedPreconditionAndMintsNothing(t *testing.T) {
	orkflows := engine.NewMemoryOrkflowStore()
	starter := &fakeStarter{}
	applier := api.NewService(api.Config{Orkflows: orkflows, Starter: starter})
	if _, err := applier.ApplyOrkflow(context.Background(), connect.NewRequest(&orkstackv1.ApplyOrkflowRequest{
		Yaml: manyTasksDoc("wide", 4)})); err != nil {
		t.Fatalf("apply under the default ceiling: %v", err)
	}
	lowered := api.NewService(api.Config{Orkflows: orkflows, Starter: starter, OrkflowMaxTasks: 3})
	_, err := lowered.SubmitRun(context.Background(), connect.NewRequest(&orkstackv1.SubmitRunRequest{OrkflowName: "wide"}))
	if connect.CodeOf(err) != connect.CodeFailedPrecondition {
		t.Fatalf("submit = %v; want failed_precondition", err)
	}
	requireTaskCeilingRefusal(t, err.Error(), 4, 3)
	if !strings.Contains(err.Error(), "was lowered after version") {
		t.Fatalf("submit = %v; want it to say the ceiling was lowered after the apply", err)
	}
	if len(starter.startedRuns()) != 0 {
		t.Fatalf("a refused submission started %v", starter.startedRuns())
	}
}
