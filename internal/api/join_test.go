package api_test

import (
	"context"
	"fmt"
	"reflect"
	"strings"
	"testing"

	"connectrpc.com/connect"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/api"
	"github.com/orkstack/orkstack/internal/engine"
)

// joinYAML is a deploy with a re-join before it and two failure handlers
// after it (docs/DSL.md "Joins and failure handlers").
const joinYAML = `apiVersion: orkstack/v1
kind: Orkflow
name: joined
version: "1.0.0"
inputs:
  route: {type: string, default: canary}
tasks:
  - id: canary
    uses: busybox:latest
    condition: '{{ eq .inputs.route "canary" }}'
  - id: full
    uses: busybox:latest
    condition: '{{ eq .inputs.route "full" }}'
  - id: verify
    uses: busybox:latest
    needs: [canary, full]
    join: none_failed
  - id: deploy
    uses: busybox:latest
    needs: [verify]
  - id: announce
    uses: busybox:latest
    needs: [deploy]
  - id: rollback
    uses: busybox:latest
    needs: [deploy]
    join: any_failed
  - id: notify
    uses: busybox:latest
    needs: [deploy]
    join: all_done
`

func TestPlanRun_AssumeFailedRefusesUnknownAndRepeatedIDs(t *testing.T) {
	svc := api.NewService(api.Config{Orkflows: engine.NewMemoryOrkflowStore()})
	applyPlanDoc(t, svc, joinYAML, "")
	_, err := planRun(svc, &orkstackv1.PlanRunRequest{OrkflowName: "joined", AssumeFailed: []string{"deploy", "ghost"}})
	requirePlanCode(t, err, connect.CodeInvalidArgument, `assume_failed[1]: "ghost" is not a task of joined@1.0.0`)
	_, err = planRun(svc, &orkstackv1.PlanRunRequest{OrkflowName: "joined", AssumeFailed: []string{"deploy", "deploy"}})
	requirePlanCode(t, err, connect.CodeInvalidArgument, `assume_failed[1]: "deploy" is listed twice`)
}

func TestPlanRun_AssumeFailedReturnsFailsAtCountsAndUnused(t *testing.T) {
	svc := api.NewService(api.Config{Orkflows: engine.NewMemoryOrkflowStore()})
	applyPlanDoc(t, svc, joinYAML, "")
	plan, err := planRun(svc, &orkstackv1.PlanRunRequest{OrkflowName: "joined", AssumeFailed: []string{"full", "deploy"}})
	if err != nil {
		t.Fatalf("PlanRun: %v", err)
	}
	if plan.GetFailsAt() != "deploy" || plan.GetAssumedFailed() != 1 ||
		!reflect.DeepEqual(plan.GetUnusedAssumeFailed(), []string{"full"}) {
		t.Fatalf("fails_at %q, assumed_failed %d, unused %v", plan.GetFailsAt(), plan.GetAssumedFailed(), plan.GetUnusedAssumeFailed())
	}
	for id, want := range map[string]orkstackv1.PlanOutcome{
		"deploy":   orkstackv1.PlanOutcome_PLAN_OUTCOME_ASSUMED_FAILED,
		"announce": orkstackv1.PlanOutcome_PLAN_OUTCOME_UNREACHED,
		"rollback": orkstackv1.PlanOutcome_PLAN_OUTCOME_RUNS,
		"notify":   orkstackv1.PlanOutcome_PLAN_OUTCOME_RUNS,
		"verify":   orkstackv1.PlanOutcome_PLAN_OUTCOME_RUNS,
	} {
		if got := planOutcome(plan, id); got != want {
			t.Errorf("%s = %v, want %v", id, got, want)
		}
	}
	// Without an assumption: rollback skipped, nothing fails.
	plain, err := planRun(svc, &orkstackv1.PlanRunRequest{OrkflowName: "joined"})
	if err != nil {
		t.Fatal(err)
	}
	if plain.GetFailsAt() != "" || planOutcome(plain, "rollback") != orkstackv1.PlanOutcome_PLAN_OUTCOME_SKIPPED {
		t.Fatalf("plain plan = %v", plain)
	}
}

const joinWarningYAML = `apiVersion: orkstack/v1
kind: Orkflow
name: warned
tasks:
  - id: canary
    uses: busybox:latest
  - id: full
    uses: busybox:latest
  - id: verify
    uses: busybox:latest
    needs: [canary, full]
    join: none_failed
    command: ["echo", "{{ .tasks.canary.output.x }}"]
`

func TestValidateOrkflow_ReturnsJoinWarnings(t *testing.T) {
	svc := api.NewService(api.Config{Starter: &fakeStarter{}})
	issue := issueAt(warningsFor(t, svc, strings.Replace(joinWarningYAML, `    command: ["echo", "{{ .tasks.canary.output.x }}"]`,
		`    condition: '{{ .tasks.canary.output.x }}'`, 1)), "tasks[2].condition")
	if issue == nil || issue.GetSeverity() != orkstackv1.ValidationSeverity_VALIDATION_SEVERITY_WARNING ||
		!strings.HasPrefix(issue.GetMessage(), `task "verify" reads .tasks.canary, which is not certain to have succeeded`) {
		t.Fatalf("issue = %v", issue)
	}
}

func TestApplyOrkflow_StoresWithJoinWarningsAndRefusesRules37To42(t *testing.T) {
	svc := api.NewService(api.Config{Starter: &fakeStarter{}})
	resp := applyYAML(t, svc, strings.Replace(joinWarningYAML, `    command: ["echo", "{{ .tasks.canary.output.x }}"]`,
		`    condition: '{{ .tasks.canary.output.x }}'`, 1))
	found := false
	for _, w := range resp.GetWarnings() {
		if strings.HasPrefix(w, "tasks[2].condition: ") && strings.Contains(w, "not certain to have succeeded") {
			found = true
		}
	}
	if !found {
		t.Fatalf("apply warnings = %v", resp.GetWarnings())
	}
	refused := map[string]string{
		"37": "  - id: x\n    uses: busybox:latest\n    needs: [a]\n    join: on_failure\n",
		"38": "  - id: x\n    uses: busybox:latest\n    join: all_done\n",
		"39": "  - id: x\n    uses: busybox:latest\n    needs: [a]\n    join: all_done\n    cache: {enabled: true}\n",
		"40": "  - id: h\n    uses: busybox:latest\n    needs: [a]\n    join: any_failed\n  - id: x\n    uses: busybox:latest\n    needs: [h]\n",
		"42": func() string {
			var b strings.Builder
			needs := []string{}
			for i := range 33 {
				fmt.Fprintf(&b, "  - id: n%d\n    uses: busybox:latest\n", i)
				needs = append(needs, fmt.Sprintf("n%d", i))
			}
			fmt.Fprintf(&b, "  - id: x\n    uses: busybox:latest\n    needs: [%s]\n    join: all_done\n", strings.Join(needs, ", "))
			return b.String()
		}(),
	}
	for rule, tasks := range refused {
		_, err := svc.ApplyOrkflow(context.Background(), connect.NewRequest(&orkstackv1.ApplyOrkflowRequest{
			Yaml: "apiVersion: orkstack/v1\nkind: Orkflow\nname: refused-" + rule + "\ntasks:\n  - id: a\n    uses: busybox:latest\n" + tasks,
		}))
		if connect.CodeOf(err) != connect.CodeInvalidArgument {
			t.Errorf("rule %s: apply = %v, want InvalidArgument", rule, err)
		}
	}
	_, err := svc.ApplyOrkflow(context.Background(), connect.NewRequest(&orkstackv1.ApplyOrkflowRequest{
		Yaml: "apiVersion: orkstack/v1\nkind: Orkflow\nname: refused-41\noutputs:\n  r: '{{ .tasks.h.output.x }}'\ntasks:\n  - id: a\n    uses: busybox:latest\n" +
			"  - id: h\n    uses: busybox:latest\n    needs: [a]\n    join: any_failed\n",
	}))
	if connect.CodeOf(err) != connect.CodeInvalidArgument || !strings.Contains(err.Error(), "whose join is any_failed") {
		t.Errorf("rule 41: apply = %v", err)
	}
}

// joinSupportOrkers answers DescribeOrker with join_supported as given.
type joinSupportOrkers bool

func (s joinSupportOrkers) OrkerOnPool(_ context.Context, _ string) (*orkstackv1.DescribeOrkerResult, error) {
	return &orkstackv1.DescribeOrkerResult{OrkerId: "gpu-3", BuildId: "2026.09.01", EachSupported: true,
		CacheSupported: true, JoinSupported: bool(s)}, nil
}

func TestSubmitRun_PoolWithoutJoinSupportIsFailedPreconditionAndMintsNothing(t *testing.T) {
	starter := &fakeStarter{}
	gpuQueue := poolQueue(t, "gpu")
	minted := 0
	svc := api.NewService(api.Config{
		Starter: starter, Pools: livePools{gpuQueue: 1}, PoolOrkers: joinSupportOrkers(false),
		NewRunID: func() string { minted++; return fmt.Sprintf("run-%d", minted) },
	})
	applyYAML(t, svc, strings.ReplaceAll(joinYAML, "uses: busybox:latest", "uses: busybox:latest\n    pool: gpu"))
	_, err := svc.SubmitRun(context.Background(), connect.NewRequest(&orkstackv1.SubmitRunRequest{
		OrkflowName: "joined", AllowEmptyPool: true}))
	if connect.CodeOf(err) != connect.CodeFailedPrecondition ||
		!strings.Contains(err.Error(), `tasks: rollback, notify`) || !strings.Contains(err.Error(), "predates join policies (join:)") {
		t.Fatalf("submit = %v", err)
	}
	if len(starter.startedRuns()) != 0 || minted != 0 {
		t.Fatalf("a refused submission started %v / minted %d ids", starter.startedRuns(), minted)
	}
}
