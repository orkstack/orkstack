package engine_test

import (
	"context"
	"slices"
	"strings"
	"sync"
	"testing"

	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/testsuite"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/internal/task"
)

const attemptIDDocument = `
apiVersion: orkstack/v1
kind: Orkflow
name: attempt-ids
inputs:
  parts: {type: array}
tasks:
  - id: build
    uses: shell
    with: {op: exec}
  - id: spread
    uses: shell
    needs: [build]
    each: {over: "{{ .inputs.parts }}", max_items: 10}
    with: {op: exec, part: "{{ .each.item }}"}
`

// TestRunWorkflow_AnAdmittedRunNamesEachAttempt: in a run whose plan names
// its admission, a task's attempts run under ork/<task> and an item's
// under ork/<task>/<item>, the ids the mint looks an attempt up by; every
// other run's activities keep the SDK's own.
func TestRunWorkflow_AnAdmittedRunNamesEachAttempt(t *testing.T) {
	for _, tc := range []struct {
		name     string
		admitted bool
		want     []string
	}{
		{"admitted", true, []string{"ork/build", "ork/spread/0", "ork/spread/1"}},
		{"not admitted", false, nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var suite testsuite.WorkflowTestSuite
			h := newHarness(t, &suite, func(in *orkstackv1.ExecuteTaskInput) (*orkstackv1.ExecuteTaskResult, error) {
				return succeeded(in.TaskId, map[string]any{"ok": true}), nil
			})
			var mu sync.Mutex
			var ids []string
			h.env.SetOnActivityStartedListener(func(info *activity.Info, _ context.Context, _ converter.EncodedValues) {
				if info.ActivityType.Name == task.ActivityExecuteTask {
					mu.Lock()
					ids = append(ids, info.ActivityID)
					mu.Unlock()
				}
			})
			plan := defaultPlan()
			if tc.admitted {
				plan.Identity = &engine.IdentityPlan{AdmissionSHA256: strings.Repeat("a", 64), SpecVersion: 1}
			}
			h.env.ExecuteWorkflow(engine.RunWorkflow, "ork-ids", compile(t, attemptIDDocument),
				payload(t, map[string]any{"parts": []any{"x", "y"}}), plan, (*orkstackv1.RunContinuation)(nil))
			if !h.env.IsWorkflowCompleted() || h.env.GetWorkflowError() != nil {
				t.Fatalf("run did not complete: %v", h.env.GetWorkflowError())
			}
			slices.Sort(ids)
			if len(ids) != 3 {
				t.Fatalf("attempt ids %v; want three attempts", ids)
			}
			for _, id := range ids {
				if tc.admitted != strings.HasPrefix(id, "ork/") {
					t.Fatalf("attempt ids %v", ids)
				}
			}
			if tc.admitted && !slices.Equal(ids, tc.want) {
				t.Fatalf("attempt ids %v; want %v", ids, tc.want)
			}
		})
	}
}
