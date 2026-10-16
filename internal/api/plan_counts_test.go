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
	"github.com/orkstack/orkstack/internal/testsupport/proof"
)

// planCountService is a deployment whose task ceiling is out of the way, so
// only the run plan's own 256 KiB bound can refuse.
func planCountService(orkflows *engine.MemoryOrkflowStore) *api.Service {
	return api.NewService(api.Config{Orkflows: orkflows, Starter: &fakeStarter{}, OrkflowMaxTasks: 20000})
}

// largestSubmitted is the largest n in [1, high) for which build(n) applies
// and submits, found by bisection, with the refusal one past it.
func largestSubmitted(t *testing.T, high int, build func(n int) string) (int, error) {
	t.Helper()
	try := func(n int) error {
		orkflows := engine.NewMemoryOrkflowStore()
		svc := planCountService(orkflows)
		if _, err := svc.ApplyOrkflow(context.Background(), connect.NewRequest(&orkstackv1.ApplyOrkflowRequest{
			Yaml: leafYAML})); err != nil {
			t.Fatalf("apply the leaf: %v", err)
		}
		if _, err := svc.ApplyOrkflow(context.Background(), connect.NewRequest(&orkstackv1.ApplyOrkflowRequest{
			Yaml: build(n)})); err != nil {
			return err // a composition tree is bounded at apply too (rule 31)
		}
		_, err := svc.SubmitRun(context.Background(), connect.NewRequest(&orkstackv1.SubmitRunRequest{OrkflowName: "counted"}))
		return err
	}
	if err := try(high); err == nil {
		t.Fatalf("%d were accepted: raise the search's ceiling", high)
	}
	low := 1
	if err := try(low); err != nil {
		t.Fatalf("one was refused: %v", err)
	}
	for high-low > 1 {
		mid := (low + high) / 2
		if try(mid) == nil {
			low = mid
		} else {
			high = mid
		}
	}
	return low, try(high)
}

const leafYAML = `name: leaf
version: "1.0.0"
tasks:
  - id: work
    uses: busybox:latest
`

// TestPlanSize_LargestAwaitAndComposeCounts records how many awaits with a
// callback, and how many minimal composed tasks, one run plan holds before
// checkRunPlanSize or mintChildren refuses the submit, failed_precondition,
// naming the plan (spec 7.4; docs/DSL.md "How many tasks an Orkflow may
// declare" states both). A definition passed by reference no longer bounds
// the task count, so these are what the 256 KiB plan bound still limits.
func TestPlanSize_LargestAwaitAndComposeCounts(t *testing.T) {
	awaits, awaitErr := largestSubmitted(t, 8000, func(n int) string {
		var b strings.Builder
		b.WriteString("name: counted\nversion: \"1.0.0\"\ntasks:\n")
		for i := range n {
			fmt.Fprintf(&b, "  - id: a%d\n    await: {timeout_seconds: 60, callback: true}\n", i)
		}
		return b.String()
	})
	composed, composeErr := largestSubmitted(t, 8000, func(n int) string {
		var b strings.Builder
		b.WriteString("name: counted\nversion: \"1.0.0\"\ntasks:\n")
		for i := range n {
			fmt.Fprintf(&b, "  - id: c%d\n    uses: orkflow:leaf@1.0.0\n", i)
		}
		return b.String()
	})
	// One await more: refused at submit, the plan named. One composed task
	// more: the composition tree's own bound refuses it at apply (rule 31,
	// invalid_argument, naming the total), or the plan at submit.
	if connect.CodeOf(awaitErr) != connect.CodeFailedPrecondition || !strings.Contains(awaitErr.Error(), "the run's plan") {
		t.Fatalf("one await more = %v, want failed_precondition naming the run's plan", awaitErr)
	}
	planRefused := connect.CodeOf(composeErr) == connect.CodeFailedPrecondition && strings.Contains(composeErr.Error(), "the run's plan")
	treeRefused := connect.CodeOf(composeErr) == connect.CodeInvalidArgument && strings.Contains(composeErr.Error(), "the Orkflows this document composes total")
	if !planRefused && !treeRefused {
		t.Fatalf("one composed task more = %v, want the composition tree's or the run plan's refusal", composeErr)
	}
	short := func(err error) string {
		if message := err.Error(); len(message) > 160 {
			return message[:160] + "…"
		}
		return err.Error()
	}
	t.Logf("E7 one run plan holds %d awaits with a callback (one more: %s)", awaits, short(awaitErr))
	t.Logf("E7 one run tree holds %d minimal composed tasks (one more: %s)", composed, short(composeErr))
	proof.WriteJSON(t, "stage7", "plan-counts.json", map[string]any{
		"awaits_with_callback": awaits, "minimal_composed_tasks": composed,
		"await_refusal": short(awaitErr), "compose_refusal": short(composeErr),
	})
}
