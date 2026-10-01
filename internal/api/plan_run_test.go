package api_test

import (
	"context"
	"errors"
	"strings"
	"testing"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/types/known/structpb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/api"
	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

const planDocument = `name: plan-me
version: "1.0.0"
inputs:
  enabled:
    type: boolean
    default: false
tasks:
  - id: first
    uses: busybox:latest
  - id: gated
    uses: busybox:latest
    needs: [first]
    condition: "{{ .inputs.enabled }}"
`

func applyPlanDoc(t *testing.T, svc *api.Service, yaml, namespace string) *orkstackv1.Orkflow {
	t.Helper()
	resp, err := svc.ApplyOrkflow(context.Background(), connect.NewRequest(&orkstackv1.ApplyOrkflowRequest{
		Yaml: yaml, Namespace: namespace,
	}))
	if err != nil {
		t.Fatalf("ApplyOrkflow: %v", err)
	}
	return resp.Msg.GetOrkflow()
}

func planRun(svc *api.Service, req *orkstackv1.PlanRunRequest) (*orkstackv1.PlanRunResponse, error) {
	resp, err := svc.PlanRun(context.Background(), connect.NewRequest(req))
	if err != nil {
		return nil, err
	}
	return resp.Msg, nil
}

func planOutcome(plan *orkstackv1.PlanRunResponse, id string) orkstackv1.PlanOutcome {
	for _, task := range plan.GetTasks() {
		if task.GetTaskId() == id {
			return task.GetOutcome()
		}
	}
	return orkstackv1.PlanOutcome_PLAN_OUTCOME_UNSPECIFIED
}

func requirePlanCode(t *testing.T, err error, code connect.Code, contains string) {
	t.Helper()
	if err == nil {
		t.Fatalf("err = nil, want %v containing %q", code, contains)
	}
	if connect.CodeOf(err) != code {
		t.Fatalf("code = %v (%v), want %v", connect.CodeOf(err), err, code)
	}
	if !strings.Contains(err.Error(), contains) {
		t.Fatalf("error %q does not contain %q", err, contains)
	}
}

// A1: a plan writes nothing and needs none of what a submit needs.
func TestPlanRun_WritesNothingAndNeedsNoStarter(t *testing.T) {
	ctx := context.Background()
	store := engine.NewMemoryOrkflowStore()
	svc := api.NewService(api.Config{Orkflows: store})
	applyPlanDoc(t, svc, planDocument, "")
	before, err := store.Versions(ctx, "default", "plan-me")
	if err != nil {
		t.Fatalf("ListVersions: %v", err)
	}

	plan, err := planRun(svc, &orkstackv1.PlanRunRequest{OrkflowName: "plan-me"})
	if err != nil {
		t.Fatalf("PlanRun on a starter-less service: %v", err)
	}
	if planOutcome(plan, "first") != orkstackv1.PlanOutcome_PLAN_OUTCOME_RUNS ||
		planOutcome(plan, "gated") != orkstackv1.PlanOutcome_PLAN_OUTCOME_SKIPPED {
		t.Fatalf("plan = %v", plan)
	}
	after, err := store.Versions(ctx, "default", "plan-me")
	if err != nil {
		t.Fatalf("ListVersions: %v", err)
	}
	if len(before.All) != len(after.All) || before.Current != after.Current || before.Head != after.Head {
		t.Fatalf("versions before %+v, after %+v: a plan wrote a definition or moved a pointer", before, after)
	}

	_, submitErr := svc.SubmitRun(ctx, connect.NewRequest(&orkstackv1.SubmitRunRequest{OrkflowName: "plan-me"}))
	if connect.CodeOf(submitErr) != connect.CodeFailedPrecondition {
		t.Fatalf("SubmitRun on the same service = %v, want FailedPrecondition", submitErr)
	}
	t.Logf("A1 PlanRun answered (runs=%d skipped=%d) where SubmitRun = %v", plan.GetRuns(), plan.GetSkipped(), submitErr)
}

// A2: exactly one source.
func TestPlanRun_RefusesZeroOrTwoSources(t *testing.T) {
	svc := api.NewService(api.Config{Orkflows: engine.NewMemoryOrkflowStore()})
	_, err := planRun(svc, &orkstackv1.PlanRunRequest{})
	requirePlanCode(t, err, connect.CodeInvalidArgument, "set exactly one of orkflow_name and yaml")
	_, err = planRun(svc, &orkstackv1.PlanRunRequest{OrkflowName: "a", Yaml: planDocument})
	requirePlanCode(t, err, connect.CodeInvalidArgument, "set exactly one of orkflow_name and yaml")
	_, err = planRun(svc, &orkstackv1.PlanRunRequest{Yaml: planDocument, OrkflowVersion: "1.0.0"})
	requirePlanCode(t, err, connect.CodeInvalidArgument, "orkflow_version")
}

// A3: resolution is GetOrkflow's.
func TestPlanRun_ResolvesLikeGetOrkflow(t *testing.T) {
	ctx := context.Background()
	reg := newFakeRegistry(
		&orkstackv1.Namespace{Name: "team-a"},
		&orkstackv1.Namespace{Name: "team-b"},
	)
	svc := api.NewService(api.Config{Orkflows: engine.NewMemoryOrkflowStore(), Namespaces: reg})

	_, err := planRun(svc, &orkstackv1.PlanRunRequest{OrkflowName: "missing"})
	requirePlanCode(t, err, connect.CodeNotFound, "not found")

	v1 := applyPlanDoc(t, svc, planDocument, "team-a")
	_, err = planRun(svc, &orkstackv1.PlanRunRequest{OrkflowName: "plan-me", Namespace: "team-a", OrkflowVersion: "9.9.9"})
	requirePlanCode(t, err, connect.CodeNotFound, "not found")
	_, err = planRun(svc, &orkstackv1.PlanRunRequest{OrkflowName: "plan-me", Namespace: "team-b"})
	requirePlanCode(t, err, connect.CodeNotFound, "not found")

	v2doc := strings.Replace(planDocument, `version: "1.0.0"`, `version: "2.0.0"`, 1)
	v2doc = strings.Replace(v2doc, "default: false", "default: true", 1)
	v2 := applyPlanDoc(t, svc, v2doc, "team-a")
	if _, err := svc.SetCurrentOrkflowVersion(ctx, connect.NewRequest(&orkstackv1.SetCurrentOrkflowVersionRequest{
		Name: "plan-me", Version: "2.0.0", Namespace: "team-a",
	})); err != nil {
		t.Fatalf("SetCurrentOrkflowVersion: %v", err)
	}

	current, err := planRun(svc, &orkstackv1.PlanRunRequest{OrkflowName: "plan-me", Namespace: "team-a"})
	if err != nil {
		t.Fatalf("plan current: %v", err)
	}
	if current.GetOrkflowVersion() != "2.0.0" || current.GetDefinitionDigest() != v2.GetDefinitionDigest() {
		t.Errorf("current plan = %s %s, want 2.0.0 %s", current.GetOrkflowVersion(), current.GetDefinitionDigest(), v2.GetDefinitionDigest())
	}
	pinned, err := planRun(svc, &orkstackv1.PlanRunRequest{OrkflowName: "plan-me", Namespace: "team-a", OrkflowVersion: "1.0.0"})
	if err != nil {
		t.Fatalf("plan pinned: %v", err)
	}
	if pinned.GetOrkflowVersion() != "1.0.0" || pinned.GetDefinitionDigest() != v1.GetDefinitionDigest() {
		t.Errorf("pinned plan = %s %s, want 1.0.0 %s", pinned.GetOrkflowVersion(), pinned.GetDefinitionDigest(), v1.GetDefinitionDigest())
	}
	if planOutcome(pinned, "gated") != orkstackv1.PlanOutcome_PLAN_OUTCOME_SKIPPED ||
		planOutcome(current, "gated") != orkstackv1.PlanOutcome_PLAN_OUTCOME_RUNS {
		t.Errorf("pinned gated %v, current gated %v: want v1's default false and v2's true",
			planOutcome(pinned, "gated"), planOutcome(current, "gated"))
	}
}

// A4: the yaml form is prepared, never stored.
func TestPlanRun_YamlFormIsPreparedNotStored(t *testing.T) {
	ctx := context.Background()
	store := engine.NewMemoryOrkflowStore()
	reg := newFakeRegistry(&orkstackv1.Namespace{Name: "team-a"})
	svc := api.NewService(api.Config{Orkflows: store, Namespaces: reg})

	plan, err := planRun(svc, &orkstackv1.PlanRunRequest{Yaml: planDocument})
	if err != nil {
		t.Fatalf("PlanRun yaml: %v", err)
	}
	if !plan.GetFromYaml() || plan.GetDefinitionDigest() == "" || plan.GetOrkflowName() != "plan-me" || plan.GetOrkflowVersion() != "1.0.0" {
		t.Fatalf("yaml plan = %v", plan)
	}
	if _, err := store.Get(ctx, "default", "plan-me", ""); !errors.Is(err, artifacts.ErrNotFound) {
		t.Fatalf("a yaml plan stored the definition: %v", err)
	}

	_, err = planRun(svc, &orkstackv1.PlanRunRequest{Yaml: "name: bad\ntasks:\n - id: a\n   uses: busybox\n   needs: [missing]\n"})
	requirePlanCode(t, err, connect.CodeInvalidArgument, "tasks[0].needs[0]")

	other := "namespace: team-a\n" + planDocument
	_, err = planRun(svc, &orkstackv1.PlanRunRequest{Yaml: other, Namespace: "default"})
	requirePlanCode(t, err, connect.CodeInvalidArgument, `the document names namespace "team-a" but it was applied into "default"`)
}

// A5: an assumed output for a non-task is refused before anything is evaluated.
func TestPlanRun_UnknownAssumedTaskIsInvalidArgument(t *testing.T) {
	svc := api.NewService(api.Config{Orkflows: engine.NewMemoryOrkflowStore()})
	applyPlanDoc(t, svc, planDocument, "")
	_, err := planRun(svc, &orkstackv1.PlanRunRequest{
		OrkflowName:    "plan-me",
		AssumedOutputs: map[string]*orkstackv1.Payload{"nope": {}},
	})
	requirePlanCode(t, err, connect.CodeInvalidArgument, `assumed_outputs["nope"]: not a task of plan-me@1.0.0`)
}

// A6: the response echoes the effective input and the resolved coordinates.
func TestPlanRun_EchoesEffectiveInputAndCoordinates(t *testing.T) {
	svc := api.NewService(api.Config{Orkflows: engine.NewMemoryOrkflowStore()})
	wf := applyPlanDoc(t, svc, planDocument, "")
	extra, err := structpb.NewStruct(map[string]any{"note": "x"})
	if err != nil {
		t.Fatal(err)
	}
	plan, err := planRun(svc, &orkstackv1.PlanRunRequest{OrkflowName: "plan-me", Input: &orkstackv1.Payload{Data: extra}})
	if err != nil {
		t.Fatalf("PlanRun: %v", err)
	}
	fields := plan.GetInput().GetData().GetFields()
	if v, ok := fields["enabled"]; !ok || v.GetBoolValue() {
		t.Errorf("effective input %v, want the declared default enabled=false", plan.GetInput())
	}
	if fields["note"].GetStringValue() != "x" {
		t.Errorf("effective input %v lost the caller's key", plan.GetInput())
	}
	if plan.GetOrkflowVersion() != "1.0.0" || plan.GetNamespace() != "default" || plan.GetOrkflowName() != "plan-me" ||
		plan.GetDefinitionDigest() != wf.GetDefinitionDigest() || plan.GetFromYaml() {
		t.Errorf("coordinates = %s@%s ns %q digest %s from_yaml %v", plan.GetOrkflowName(), plan.GetOrkflowVersion(),
			plan.GetNamespace(), plan.GetDefinitionDigest(), plan.GetFromYaml())
	}
}
