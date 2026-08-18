package engine_test

import (
	"strings"
	"testing"

	"go.temporal.io/sdk/testsuite"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/internal/task"
)

// The naming gate (proof/M10.1/naming-sweep.sh) exempts internal/engine,
// because below the seam `queue` means a Temporal task queue and must survive.
// That exemption is also a blind spot: the engine composes the strings it SHIPS
// TO CLIENTS here, and the gate cannot see them. `orkctl watch` prints an
// event's message in its MESSAGE column (cmd/orkctl/watch.go) and the run page
// renders it (ui/src/routes/runs/[id]/+page.svelte), so a message naming a
// queue tells an author who wrote `pool: gpu` about a Temporal concept and an
// internal name they never typed.
//
// So the rule the gate cannot enforce is enforced here instead: any
// user-visible string the sweep cannot see is covered by a test.

const poolDocument = `
name: pool-routing
tasks:
  - id: crunch
    plugin: shell
    pool: gpu
    with: {command: "echo crunch"}
  - id: tidy
    plugin: shell
    needs: [crunch]
    with: {command: "echo tidy"}
`

// TestRunWorkflow_ScheduledEventNamesThePoolNotTheQueue reads the emitted
// event stream — the same stream a client receives — and holds the scheduled
// message to the word the author typed.
func TestRunWorkflow_ScheduledEventNamesThePoolNotTheQueue(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	h := newHarness(t, &suite, func(in task.ExecuteTaskInput) (task.ExecuteTaskResult, error) {
		return task.ExecuteTaskResult{TaskID: in.TaskID}, nil
	})

	// A raw operator queue, as ORK_ORKER_QUEUE sets for an isolated agent
	// (AGENTS.md §6). It is nobody's pool, so a task that names no pool is
	// still reported as running on the built-in default.
	const rawQueue = "ork-tasks-m10-1-agent"

	h.env.ExecuteWorkflow(engine.RunWorkflow, "run-pool-message",
		compile(t, poolDocument), payload(t, nil), rawQueue)
	if err := h.env.GetWorkflowError(); err != nil {
		t.Fatalf("workflow error: %v", err)
	}

	scheduled := map[string]string{}
	for _, event := range h.bus.Events("run-pool-message", 0) {
		if event.GetType() == orkstackv1.EventType_EVENT_TYPE_TASK_SCHEDULED {
			scheduled[event.GetTaskId()] = event.GetMessage()
		}
	}
	if len(scheduled) != 2 {
		t.Fatalf("the stream carries %d TASK_SCHEDULED events (%v), want one per task", len(scheduled), scheduled)
	}

	want := map[string]string{
		"crunch": "pool gpu",  // what the document says
		"tidy":   "pool main", // the built-in default, not the raw queue
	}
	for taskID, wantFragment := range want {
		message := scheduled[taskID]
		if !strings.Contains(message, wantFragment) {
			t.Errorf("TASK_SCHEDULED for %q says %q, want it to name %q — the pool is the word the "+
				"author typed and the only one a client should read", taskID, message, wantFragment)
		}
		if strings.Contains(strings.ToLower(message), "queue") {
			t.Errorf("TASK_SCHEDULED for %q says %q: `queue` is Temporal's word and must not reach a "+
				"client (orkctl prints this message, the UI renders it)", taskID, message)
		}
		// The Temporal task queue a pool maps to is an engine detail with a
		// prefix the author never typed; leaking the name without the word
		// would be the same defect one rename later.
		for _, leaked := range []string{"ork-pool-", rawQueue} {
			if strings.Contains(message, leaked) {
				t.Errorf("TASK_SCHEDULED for %q says %q, which carries the internal name %q",
					taskID, message, leaked)
			}
		}
	}
}

// TestRunWorkflow_TaskRunRecordsThePoolItRanOn covers the same value on the
// run projection: a reader that lists task runs answers "where did this go"
// without reading Temporal, and answers it in pools.
func TestRunWorkflow_TaskRunRecordsThePoolItRanOn(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	h := newHarness(t, &suite, func(in task.ExecuteTaskInput) (task.ExecuteTaskResult, error) {
		return task.ExecuteTaskResult{TaskID: in.TaskID}, nil
	})

	h.env.ExecuteWorkflow(engine.RunWorkflow, "run-pool-projection",
		compile(t, poolDocument), payload(t, nil), task.DefaultQueue)
	if err := h.env.GetWorkflowError(); err != nil {
		t.Fatalf("workflow error: %v", err)
	}

	run := mustWorkflowResult(t, h.env)
	pools := map[string]string{}
	for _, tr := range run.GetTaskRuns() {
		pools[tr.GetTaskId()] = tr.GetPool()
	}
	if pools["crunch"] != "gpu" {
		t.Errorf("TaskRun(crunch).pool = %q, want %q", pools["crunch"], "gpu")
	}
	if pools["tidy"] != task.DefaultPool {
		t.Errorf("TaskRun(tidy).pool = %q, want the built-in default %q", pools["tidy"], task.DefaultPool)
	}
}

// TestRunWorkflow_ExecuteTaskInputCarriesNamespaceAndPool holds the
// engine->Orker contract to the two coordinates M10.1 introduces. The Orker
// keys a run's logs by them (M10.5), and this is the slice that re-records the
// replay fixture, so they are added here rather than forcing a second reset.
func TestRunWorkflow_ExecuteTaskInputCarriesNamespaceAndPool(t *testing.T) {
	const document = `
namespace: payments
name: pool-input
tasks:
  - id: crunch
    plugin: shell
    pool: gpu
  - id: tidy
    plugin: shell
`
	var suite testsuite.WorkflowTestSuite
	h := newHarness(t, &suite, func(in task.ExecuteTaskInput) (task.ExecuteTaskResult, error) {
		return task.ExecuteTaskResult{TaskID: in.TaskID}, nil
	})

	h.env.ExecuteWorkflow(engine.RunWorkflow, "run-pool-input",
		compile(t, document), payload(t, nil), task.DefaultQueue)
	if err := h.env.GetWorkflowError(); err != nil {
		t.Fatalf("workflow error: %v", err)
	}

	for taskID, wantPool := range map[string]string{"crunch": "gpu", "tidy": task.DefaultPool} {
		in, ok := h.inputFor(taskID)
		if !ok {
			t.Fatalf("task %q was not executed", taskID)
		}
		if in.Namespace != "payments" {
			t.Errorf("ExecuteTaskInput(%s).Namespace = %q, want %q", taskID, in.Namespace, "payments")
		}
		if in.Pool != wantPool {
			t.Errorf("ExecuteTaskInput(%s).Pool = %q, want %q", taskID, in.Pool, wantPool)
		}
	}
}
