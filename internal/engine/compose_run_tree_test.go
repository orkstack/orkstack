package engine_test

import (
	"context"
	"testing"
	"time"

	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/interceptor"
	"go.temporal.io/sdk/worker"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/internal/task"
)

// composedChildOnAPinDocument is composedChildDocument with a pin and a
// retrying task, so the child run schedules everything a run-tree value
// reaches: a pin's creation request, a pinned member, and a task whose
// retry: sets no maximum interval.
const composedChildOnAPinDocument = `
apiVersion: orkstack/v1
kind: Orkflow
name: release-block
version: "2.1.0"
inputs:
  image: { type: string, required: true }
outputs:
  digest: '{{ .tasks.sign.output.value }}'
  url: 'https://records.example/{{ .tasks.sign.output.value }}'
pins:
  work: {}
tasks:
  - id: stage
    uses: shell
    pin: work
    run_as_orker_user: true
    with: { op: eval, command: ["true"] }
  - id: sign
    uses: shell
    needs: [stage]
    retry: { maximum_attempts: 3 }
    with: { op: eval, command: ["true"] }
`

// TestRunWorkflow_AComposedChildRunsOnItsParentsRetrySpacingAndFairness: a
// child run is a run of its parent's namespace and release, so what the
// submit decided once for the whole tree reaches the activities the CHILD
// schedules: the retry spacing its retrying task waits at most, and the
// fairness key and weight of its task, its pinned member and — because the
// parent's selection asks for it — its pin's creation request. The child's
// own nested plan carries none of them (a plan is built per tree), so a child
// that kept only what its nested plan holds would wait 100 seconds and queue
// with no key.
func TestRunWorkflow_AComposedChildRunsOnItsParentsRetrySpacingAndFairness(t *testing.T) {
	const retrySeconds = 45
	capture := &dispatchCapture{}
	h := newComposeHarness(t, defaultBehavior)
	h.env.SetWorkerOptions(testWorkerOptions(worker.Options{
		EnableSessionWorker: true,
		Interceptors:        []interceptor.WorkerInterceptor{capture},
	}))
	h.env.RegisterActivityWithOptions(func(context.Context, *orkstackv1.ReleasePinInput) error { return nil },
		activity.RegisterOptions{Name: task.ActivityReleasePin})
	h.realChild()
	parent, plan, _ := composedFixture(t, composedChildOnAPinDocument)
	if nested := plan.Children["release"].Plan; nested.RetryMaxIntervalSeconds != 0 || nested.Fairness != nil {
		t.Fatalf("the premise is a nested child plan with no run-tree values, got %+v", nested)
	}
	plan.RetryMaxIntervalSeconds = retrySeconds
	plan.Fairness = &engine.FairnessSelection{Key: "payments", Weight: 3, PinRequests: true}

	h.run("ork-parent", parent, payload(t, nil), plan)
	if err := h.env.GetWorkflowError(); err != nil {
		t.Fatalf("workflow error: %v", err)
	}
	child, ok := h.store.Get(plan.Children["release"].RunID)
	if !ok || child.GetStatus() != orkstackv1.Status_STATUS_SUCCEEDED {
		t.Fatalf("the child did not run to success: %v", child)
	}

	// The parent schedules no retrying task, so the one interval is the
	// child's: the parent's number, not the 100 seconds of a plan without one.
	requireIntervals(t, maximumIntervals(capture), 1, retrySeconds*time.Second)
	if asked := capture.versionsAsked(engine.RetryMaxIntervalChange); len(asked) != 1 || asked[0] != 1 {
		t.Fatalf("%s was answered %v, want one question answered 1", engine.RetryMaxIntervalChange, asked)
	}
	// Two tasks of the parent, the child's pinned member and its task: every
	// one keyed, so the child's are keyed too.
	requireKeyed(t, "ExecuteTask", capture.of(task.ActivityExecuteTask), 4, paymentsPriority)
	// The parent opens no pin: the one creation request is the child's.
	requireKeyed(t, "pin creation request", capture.of(sessionCreationActivityName), 1, paymentsPriority)
	if got := len(capture.of(sessionCreationActivityName)); got != 1 {
		t.Fatalf("%d pin creation requests, want the child's one", got)
	}
}
