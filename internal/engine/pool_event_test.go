package engine_test

import (
	"strings"
	"testing"

	"go.temporal.io/sdk/testsuite"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/internal/task"
)

// The rule that no user-facing surface spells `queue` does not reach
// internal/engine, because below the seam `queue` means a Temporal task queue
// and must survive. That exemption is also a blind spot: the engine composes the strings it SHIPS
// TO CLIENTS here, and the gate cannot see them. `orkctl watch` prints an
// event's message in its MESSAGE column (cmd/orkctl/watch.go) and the run page
// renders it (ui/src/routes/runs/[id]/+page.svelte), so a message naming a
// queue tells an author who wrote `pool: gpu` about a Temporal concept and an
// internal name they never typed.
//
// So the rule the gate cannot enforce is enforced here instead: any
// user-visible string the sweep cannot see is covered by a test.

const poolDocument = `
apiVersion: orkstack/v1
kind: Orkflow
name: pool-routing
tasks:
  - id: crunch
    uses: shell
    pool: gpu
    with: {command: "echo crunch"}
  - id: tidy
    uses: shell
    needs: [crunch]
    with: {command: "echo tidy"}
`

// TestRunWorkflow_ScheduledEventNamesThePoolNotTheQueue reads the emitted
// event stream — the same stream a client receives — and holds the scheduled
// message to the word the author typed.
func TestRunWorkflow_ScheduledEventNamesThePoolNotTheQueue(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	h := newHarness(t, &suite, func(in *orkstackv1.ExecuteTaskInput) (*orkstackv1.ExecuteTaskResult, error) {
		return &orkstackv1.ExecuteTaskResult{TaskId: in.TaskId}, nil
	})

	// A raw operator queue, as ORK_ORKER_QUEUE sets for an isolated agent
	// (AGENTS.md §11.1). It is nobody's pool, so a task that names no pool is
	// still reported as running on the built-in default.
	const rawQueue = "ork-tasks-private-agent"

	h.env.ExecuteWorkflow(engine.RunWorkflow, "run-pool-message",
		compile(t, poolDocument), payload(t, nil), rawPlan(rawQueue, "gpu"), (*orkstackv1.RunContinuation)(nil))
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
	h := newHarness(t, &suite, func(in *orkstackv1.ExecuteTaskInput) (*orkstackv1.ExecuteTaskResult, error) {
		return &orkstackv1.ExecuteTaskResult{TaskId: in.TaskId}, nil
	})

	h.env.ExecuteWorkflow(engine.RunWorkflow, "run-pool-projection",
		compile(t, poolDocument), payload(t, nil), defaultPlan("gpu"), (*orkstackv1.RunContinuation)(nil))
	if err := h.env.GetWorkflowError(); err != nil {
		t.Fatalf("workflow error: %v", err)
	}

	run := mustFinalRun(t, h.store, "run-pool-projection")
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
// engine->Orker contract to the two coordinates the Orker keys a run's logs
// by: namespace and pool.
func TestRunWorkflow_ExecuteTaskInputCarriesNamespaceAndPool(t *testing.T) {
	const document = `
apiVersion: orkstack/v1
kind: Orkflow
namespace: payments
name: pool-input
tasks:
  - id: crunch
    uses: shell
    pool: gpu
  - id: tidy
    uses: shell
`
	var suite testsuite.WorkflowTestSuite
	h := newHarness(t, &suite, func(in *orkstackv1.ExecuteTaskInput) (*orkstackv1.ExecuteTaskResult, error) {
		return &orkstackv1.ExecuteTaskResult{TaskId: in.TaskId}, nil
	})

	h.env.ExecuteWorkflow(engine.RunWorkflow, "run-pool-input",
		compile(t, document), payload(t, nil), defaultPlan("gpu"), (*orkstackv1.RunContinuation)(nil))
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

// TestRunWorkflow_TheReportedPoolNamesTheOrkersTheTaskReached closes the gap
// between the two halves of the pool contract. The tests above prove the
// engine SAYS "pool"; this one proves the word is true — the pool a client
// reads names the Orkers the activity was really dispatched to, measured from
// Temporal's own activity info.
//
// It is also the isolation guarantee, stated where a run can see it. Under the
// ORK_ORKER_QUEUE override AGENTS.md §11.1 gives every parallel agent, `pool:
// main` and no pool at all are one route: a document cannot leave the
// operator's channel by naming the pool it was already told it was on
// (docs/DSL.md).
//
// Both shapes of override are run, because they are not the same case. A raw
// isolation queue is nobody's pool; a channel an operator pointed at a pool's
// own queue IS that pool, and `main` — the name of the deployment's default
// pool — must then mean it. Only the second shape catches a `main` that maps
// forward to task.DefaultQueue, and the repository's own harness uses that
// shape (cmd/orkctl/integration_test.go binds task.QueueForPool of a private
// pool).
func TestRunWorkflow_TheReportedPoolNamesTheOrkersTheTaskReached(t *testing.T) {
	// Same pool, spelled both ways, plus a pool of its own.
	const document = `
apiVersion: orkstack/v1
kind: Orkflow
name: pool-agreement
tasks:
  - id: silent
    uses: shell
    with: {command: "echo silent"}
  - id: named
    uses: shell
    pool: main
    with: {command: "echo named"}
  - id: crunch
    uses: shell
    pool: gpu
    with: {command: "echo crunch"}
`
	gpuQueue, err := task.QueueForPool("gpu", "")
	if err != nil {
		t.Fatalf("QueueForPool(gpu): %v", err)
	}

	for _, channel := range []struct {
		name     string
		bound    string
		wantPool string // the pool the bound channel answers to
	}{
		{name: "a plain operator override", bound: "ork-tasks-private-agent", wantPool: task.DefaultPool},
		{name: "an override that is itself pool-shaped", bound: gpuQueue, wantPool: "gpu"},
	} {
		t.Run(channel.name, func(t *testing.T) {
			var suite testsuite.WorkflowTestSuite
			h := newHarness(t, &suite, func(in *orkstackv1.ExecuteTaskInput) (*orkstackv1.ExecuteTaskResult, error) {
				return &orkstackv1.ExecuteTaskResult{TaskId: in.TaskId}, nil
			})
			// Through the real submit decision, because that is where the two
			// halves are now resolved: PoolResolver.Plan stamps each task with
			// the pool it resolved to and binds the route table beside it.
			planned, plan := planFor(t, compile(t, document), channel.bound)
			h.env.ExecuteWorkflow(engine.RunWorkflow, "run-pool-agreement",
				planned, payload(t, nil), plan, (*orkstackv1.RunContinuation)(nil))
			if err := h.env.GetWorkflowError(); err != nil {
				t.Fatalf("workflow error: %v", err)
			}

			// The pool each task was reported on, from the projection a
			// client reads.
			reported := map[string]string{}
			for _, tr := range mustFinalRun(t, h.store, "run-pool-agreement").GetTaskRuns() {
				reported[tr.GetTaskId()] = tr.GetPool()
			}
			// The queue each task's activity arrived on, from Temporal.
			reached := map[string]string{}
			for _, id := range []string{"silent", "named", "crunch"} {
				queue, ok := h.queueFor(id)
				if !ok {
					t.Fatalf("task %q never reached an orker", id)
				}
				reached[id] = queue
			}

			// `pool: main` and no pool are one route, in both projections.
			if reported["silent"] != reported["named"] {
				t.Errorf("no pool reported %q and `pool: main` reported %q; the DSL says they are "+
					"the same pool", reported["silent"], reported["named"])
			}
			if reached["silent"] != reached["named"] {
				t.Errorf("no pool ran on %q and `pool: main` ran on %q; one reported pool, two "+
					"sets of Orkers", reached["silent"], reached["named"])
			}
			// And that route is the operator's channel, never the shared one.
			if reached["named"] != channel.bound {
				t.Errorf("`pool: main` ran on %q, want the run's own channel %q — naming the "+
					"default pool must not escape ORK_ORKER_QUEUE onto the shared queue %q",
					reached["named"], channel.bound, task.DefaultQueue)
			}
			if reported["named"] != channel.wantPool {
				t.Errorf("`pool: main` was reported on pool %q, want %q — the name the bound "+
					"channel %q answers to", reported["named"], channel.wantPool, channel.bound)
			}

			// A named pool goes where the one translation says it goes.
			if reported["crunch"] != "gpu" {
				t.Errorf("`pool: gpu` was reported on pool %q, want %q", reported["crunch"], "gpu")
			}
			if reached["crunch"] != gpuQueue {
				t.Errorf("`pool: gpu` ran on %q, want %q", reached["crunch"], gpuQueue)
			}

			// One pool name, one set of Orkers, in both directions — which is
			// what makes the reported word usable: a client resubmitting
			// against it reaches the Orkers this task reached.
			queueOf, poolOf := map[string]string{}, map[string]string{}
			for _, id := range []string{"silent", "named", "crunch"} {
				if seen, ok := queueOf[reported[id]]; ok && seen != reached[id] {
					t.Errorf("tasks reported on pool %q ran on %q and %q: one pool name, two "+
						"sets of Orkers", reported[id], seen, reached[id])
				}
				if seen, ok := poolOf[reached[id]]; ok && seen != reported[id] {
					t.Errorf("tasks that ran on %q were reported on pools %q and %q: one set of "+
						"Orkers, two names", reached[id], seen, reported[id])
				}
				queueOf[reported[id]] = reached[id]
				poolOf[reached[id]] = reported[id]
			}
		})
	}
}
