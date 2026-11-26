package api

import (
	"testing"

	"connectrpc.com/connect"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/dsl"
	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/internal/testsupport/must"
	"github.com/orkstack/orkstack/pkg/artifacts"
	"github.com/orkstack/orkstack/pkg/eventsources"
)

// TestPlanTaskLimits_AKeyConflictIsClassedByWhatMadeIt: a task key that is
// the run's own is a refusal every message of a source would meet when both
// keys are fixed (pause), and this message's alone when either is rendered
// from its input (reject).
func TestPlanTaskLimits_AKeyConflictIsClassedByWhatMadeIt(t *testing.T) {
	ns := &orkstackv1.Namespace{Name: artifacts.DefaultNamespace}
	for name, c := range map[string]struct {
		runLabel, taskKey string
		want              verdict
	}{
		"both fixed":          {"db:prod", "\"db:prod\"", verdict{class: refusalPause, code: engine.ConcurrencyKeyConflictCode}},
		"the task's rendered": {"db:prod", "\"db:{{ .inputs.env }}\"", verdict{class: refusalReject, code: eventsources.ReasonMessageConcurrencyKeyInvalid}},
		"the run's rendered":  {"db:{{ .inputs.env }}", "\"db:prod\"", verdict{class: refusalReject, code: eventsources.ReasonMessageConcurrencyKeyInvalid}},
	} {
		wf, err := dsl.Compile([]byte("apiVersion: orkstack/v1\nkind: Orkflow\nname: migrate\nversion: \"1\"\ninputs:\n  env:\n    type: string\n    default: prod\n" +
			"tasks:\n  - id: t0\n    uses: shell\n    with: { op: eval, command: [\"true\"] }\n    concurrency: {limit: 1, key: " + c.taskKey + "}\n"))
		must.OKf(t, err, "%s: compile", name)
		run := 0
		plan := &engine.RunPlan{Concurrency: &engine.ConcurrencyPlan{Run: &run, Slots: []engine.ConcurrencySlotPlan{{
			GateID: engine.GateID(ns.GetName(), "", "db:prod"), Key: "db:prod", Label: c.runLabel, Limit: 1}}}}
		err = (&Service{}).planTaskLimits(ns, wf, plan, &orkstackv1.SubmitRunRequest{}, &orkstackv1.RunOrigin{}, "")
		if connect.CodeOf(err) != connect.CodeInvalidArgument {
			t.Fatalf("%s: %v; want the conflict", name, err)
		}
		if got := classifyRefusal(err); got != c.want {
			t.Errorf("%s: %+v; want %+v", name, got, c.want)
		}
	}
}
