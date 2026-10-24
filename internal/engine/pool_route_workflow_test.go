package engine_test

import (
	"strings"
	"testing"

	"go.temporal.io/sdk/testsuite"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/engine"
)

// The three shapes of a task whose pool the plan cannot route, each reaching
// refuseRoute by a different path: a plain dispatch (schedule), a pin's
// opening (openPin), and a plain dispatch in a definition with a failure
// handler (failWithHandlers must not record the row a second time).
const (
	refusedRoutePlainDocument = `
apiVersion: orkstack/v1
kind: Orkflow
name: refused-route
version: "1.0.0"
tasks:
  - id: crunch
    uses: shell
    pool: gpu
    with: {command: "true"}
  - id: after
    uses: shell
    needs: [crunch]
    with: {command: "true"}
`
	refusedRoutePinnedDocument = `
apiVersion: orkstack/v1
kind: Orkflow
name: refused-route-pin
version: "1.0.0"
pins:
  build: {}
tasks:
  - id: crunch
    uses: shell
    pool: gpu
    pin: build
    run_as_orker_user: true
    with: {command: "true"}
  - id: member
    uses: shell
    pool: gpu
    pin: build
    needs: [crunch]
    run_as_orker_user: true
    with: {command: "true"}
`
	refusedRouteHandlerDocument = `
apiVersion: orkstack/v1
kind: Orkflow
name: refused-route-handler
version: "1.0.0"
tasks:
  - id: crunch
    uses: shell
    pool: gpu
    with: {command: "true"}
  - id: rollback
    uses: shell
    needs: [crunch]
    join: any_failed
    with: {command: "true"}
`
)

// TestRunWorkflow_ARefusedRouteIsRecordedOnceInTheRowAndTheStream: a task whose
// pool the plan does not route ends FAILED with INVALID_POOL, a contract
// violation and a finished_at, the stream carries TASK_FAILED for it exactly
// once and no TASK_LOG (the cancel the run's failure path writes for a task it
// finds PENDING), nothing is dispatched to it, and a failure handler runs once
// and does not record the refused task again.
func TestRunWorkflow_ARefusedRouteIsRecordedOnceInTheRowAndTheStream(t *testing.T) {
	for _, tc := range []struct {
		name     string
		document string
		handler  string // a failure handler that must run
		pending  string // a task that never ran and is cancelled
	}{
		{name: "a plain dispatch", document: refusedRoutePlainDocument, pending: "after"},
		{name: "a pin's opening", document: refusedRoutePinnedDocument, pending: "member"},
		{name: "a failure handler in the definition", document: refusedRouteHandlerDocument, handler: "rollback"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var suite testsuite.WorkflowTestSuite
			h := newHarness(t, &suite, okTask)
			const runID = "run-refused-route"
			h.env.ExecuteWorkflow(engine.RunWorkflow, runID, compile(t, tc.document),
				payload(t, nil), defaultPlan(), (*orkstackv1.RunContinuation)(nil))
			if err := h.env.GetWorkflowError(); err == nil || !strings.Contains(err.Error(), engine.ReasonInvalidPool) {
				t.Fatalf("workflow error %v, want the run to fail with %s", err, engine.ReasonInvalidPool)
			}
			run := runOf(t, h, runID)
			if run.GetStatus() != orkstackv1.Status_STATUS_FAILED || run.GetError().GetCode() != "RUN_FAILED" ||
				run.GetError().GetDetails() != `task "crunch"` {
				t.Fatalf("run %v, error %v; want FAILED, RUN_FAILED naming crunch", run.GetStatus(), run.GetError())
			}
			crunch := taskRunOf(t, run, "crunch")
			if crunch.GetStatus() != orkstackv1.Status_STATUS_FAILED ||
				crunch.GetError().GetCode() != engine.ReasonInvalidPool ||
				crunch.GetError().GetCategory() != orkstackv1.FailureCategory_FAILURE_CATEGORY_CONTRACT_VIOLATION ||
				crunch.GetFinishedAt() == nil || crunch.GetScheduledAt() != nil || crunch.GetStartedAt() != nil {
				t.Fatalf("crunch = %v; want FAILED, INVALID_POOL, a contract violation, finished, never scheduled", crunch)
			}
			if n := dispatched(h, "crunch"); n != 0 {
				t.Errorf("crunch dispatched %d times, want never", n)
			}

			var failed, logs int
			for _, e := range eventsFor(h, runID, "crunch") {
				switch e.GetType() {
				case orkstackv1.EventType_EVENT_TYPE_TASK_FAILED:
					failed++
					if e.GetStatus() != orkstackv1.Status_STATUS_FAILED || !strings.Contains(e.GetMessage(), "gpu") {
						t.Errorf("TASK_FAILED = %v, want FAILED naming the pool", e)
					}
				case orkstackv1.EventType_EVENT_TYPE_TASK_LOG:
					logs++
				}
			}
			if failed != 1 || logs != 0 {
				t.Errorf("crunch's stream has %d TASK_FAILED and %d TASK_LOG; want 1 and 0: %v",
					failed, logs, eventTypes(eventsFor(h, runID, "crunch")))
			}
			requireContiguous(t, eventsOf(h, runID))

			if tc.pending != "" && statusOf(run, tc.pending) != orkstackv1.Status_STATUS_CANCELED {
				t.Errorf("%s = %v, want CANCELED: it never started", tc.pending, statusOf(run, tc.pending))
			}
			if tc.handler != "" {
				if statusOf(run, tc.handler) != orkstackv1.Status_STATUS_SUCCEEDED || dispatched(h, tc.handler) != 1 {
					t.Errorf("handler %s = %v, dispatched %d; want SUCCEEDED once",
						tc.handler, statusOf(run, tc.handler), dispatched(h, tc.handler))
				}
			}
		})
	}
}
