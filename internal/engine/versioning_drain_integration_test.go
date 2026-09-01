//go:build integration

package engine_test

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"go.temporal.io/api/enums/v1"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/internal/task"
)

// strandedWindow is how long the pinned run is watched making no progress. It
// is evidence, so it has to be long enough that "it had not got there yet"
// stops being an explanation: a task of this orkflow executes in milliseconds
// once a worker of its version exists, and the run below is given 45 seconds
// to do it in without one.
const strandedWindow = 45 * time.Second

// TestIntegration_APinnedRunStrandsWhenItsVersionsOrkerStopsBeforeDrained is
// docs/DEPLOY.md §3 steps 5+6 — the step the rollout test never performs. Step
// 5 says to wait until the old version reports `Drained`; this is what step 6
// does to a still-pinned run when it is taken early.
//
// TestIntegration_PinnedRunFinishesOnItsOwnVersionWhileNewRunsMoveOn keeps
// v1's Orker polling for its whole length (`defer first.stop()`), so the state
// an operator actually reaches — v2 Current AND v1's workers stopped — was
// asserted nowhere but in prose ("Stopping them there leaves those runs stuck
// with no worker to advance them; they resume if you bring v1 back").
//
// Two things are being held to that sentence, and the second is the one that
// matters:
//
//  1. a run pinned to v1 makes NO progress while v1's Orker is down, and
//     resumes when it comes back — the wait-for-Drained instruction is
//     load-bearing, not caution;
//  2. its tasks are NOT handed to v2. Temporal's own contract has an exception
//     — "when the activity Task Queue workers are not present in the
//     workflow's Deployment Version, … the activity will be sent to the
//     Current Deployment Version of its own task queue"
//     (api/enums/v1/workflow.pb.go, quoted in docs/DEPLOY.md §2) — and if that
//     exception ever fired here, an operator following step 6 would silently
//     run an in-flight run's remaining tasks on NEW plugin code. That is the
//     failure this milestone exists to prevent, and nothing in the repo would
//     have noticed it.
//
// The run is pinned by a start-time versioning override rather than by
// outliving a rollout. That is deterministic — no race between releasing a
// gate task and stopping the worker that is executing it — and it reaches the
// same state: assertRunVersion checks the server's own verdict, PINNED on v1,
// which is exactly what it reports for a run that started while v1 was
// Current.
func TestIntegration_APinnedRunStrandsWhenItsVersionsOrkerStopsBeforeDrained(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Second)
	defer cancel()

	namespace := versioningNamespace()
	c := dialVersioningCluster(t, ctx, namespace)

	suffix := fmt.Sprintf("%d-%d", os.Getpid(), time.Now().UnixNano())
	deploymentName := "ork-drain-" + suffix
	runQueue := "ork-drain-run-" + suffix
	orkerQueue := "ork-drain-tasks-" + suffix
	v1 := task.Deployment{Name: deploymentName, BuildID: "v1"}
	v2 := task.Deployment{Name: deploymentName, BuildID: "v2"}

	transcript := newRolloutTranscript(t, namespace, deploymentName, runQueue, orkerQueue)

	// Nothing in this test's orkflow is the gate task, so release is never
	// read; it exists because a replica takes one.
	release := make(chan struct{})
	snapshots := newTestSnapshotStore()

	first := newReplica(t, c, v1, runQueue, orkerQueue, release, snapshots)
	defer first.stop()
	setCurrentVersion(t, ctx, c, v1, runQueue, orkerQueue)
	transcript.step("v1 up; current version = v1")

	second := newReplica(t, c, v2, runQueue, orkerQueue, release, snapshots)
	defer second.stop()
	setCurrentVersion(t, ctx, c, v2, runQueue, orkerQueue)
	transcript.step("v2 up; current version = v2, and v1 still has runs to finish")

	// --- §3 step 6, performed before v1 reports Drained -------------------
	first.stopOrker()
	transcript.step("stopped v1's Orker (its run worker stays up, as `stop v1` is one step later)")

	runID := "ork-drain-pinned-" + suffix
	startPinnedToVersion(t, ctx, c, v1, runQueue, orkerQueue, runID, drainOrkflow(orkerQueue))
	transcript.step("started " + runID + ", pinned to v1, with no v1 Orker to execute it")

	awaitVersioningVerdict(t, ctx, c, runID, v1)
	assertRunVersion(t, ctx, c, transcript, runID, v1)

	// --- 1 and 2: no progress, and no progress ON V2 ----------------------
	deadline := time.Now().Add(strandedWindow)
	for time.Now().Before(deadline) {
		assertNoTaskOfRunRan(t, second, runID, "v2",
			"a rollout that stopped v1's Orker moved a run pinned to v1 onto v2's plugin code")
		assertNoTaskOfRunRan(t, first, runID, "v1",
			"v1's Orker executed a task after it was stopped")
		if status := executionStatus(t, ctx, c, runID); status != enums.WORKFLOW_EXECUTION_STATUS_RUNNING {
			t.Fatalf("run %s reached status %v while its version had no Orker; it must simply wait",
				runID, status)
		}
		time.Sleep(2 * time.Second)
	}
	waiting := assertTaskScheduledAndNeverStarted(t, ctx, c, runID)
	transcript.result(fmt.Sprintf("after %s with v1's Orker down: %d ExecuteTask activities scheduled, "+
		"0 started, run still RUNNING, v2 ran none of them", strandedWindow, waiting))

	// --- and it resumes when v1 comes back --------------------------------
	first.startOrker(t)
	transcript.step("restarted v1's Orker")
	if err := c.GetWorkflow(ctx, runID, "").Get(ctx, nil); err != nil {
		t.Fatalf("run %s did not finish once v1's Orker was back: %v\n\n"+
			"docs/DEPLOY.md §3 step 5 says such a run resumes when v1 returns; it did not", runID, err)
	}
	transcript.step("run " + runID + " completed")

	ran := assertTaskBuilds(t, ctx, c, transcript, runID, "v1")
	if len(ran) < 2 {
		t.Fatalf("run %s executed %d tasks, want at least 2: the task scheduled while v1 was down "+
			"is the one this test is about", runID, len(ran))
	}
	transcript.logOnly(t)
}

// startPinnedToVersion starts RunWorkflow with a start-time versioning
// override, so the execution is pinned to a version that is not Current.
//
// It is the engine's own start call minus the run-id policies (this test does
// not exercise identity) plus the override, which TemporalStarter has no
// reason to expose: production pins a run by executing it on the Current
// Version, and only a test needs to reach that state on demand.
func startPinnedToVersion(
	t *testing.T,
	ctx context.Context,
	c client.Client,
	deployment task.Deployment,
	runQueue, orkerQueue, runID string,
	wf *orkstackv1.Orkflow,
) {
	t.Helper()
	if _, err := c.ExecuteWorkflow(ctx, client.StartWorkflowOptions{
		ID:                                       runID,
		TaskQueue:                                runQueue,
		WorkflowIDConflictPolicy:                 enums.WORKFLOW_ID_CONFLICT_POLICY_FAIL,
		WorkflowExecutionErrorWhenAlreadyStarted: true,
		VersioningOverride: &client.PinnedVersioningOverride{
			Version: worker.WorkerDeploymentVersion{
				DeploymentName: deployment.Name,
				BuildID:        deployment.BuildID,
			},
		},
	}, engine.RunWorkflowType, runID, wf, (*orkstackv1.Payload)(nil), rawPlan(orkerQueue)); err != nil {
		t.Fatalf("start %s pinned to %s: %v", runID, deployment, err)
	}
}

// awaitVersioningVerdict waits until the server reports the run's deployment
// version. It is not immediate: the execution carries the override from the
// start, but the version it reports is settled by its first completed workflow
// task, which v1's run worker has to pick up first.
func awaitVersioningVerdict(
	t *testing.T,
	ctx context.Context,
	c client.Client,
	runID string,
	want task.Deployment,
) {
	t.Helper()
	deadline := time.Now().Add(60 * time.Second)
	for {
		described, err := c.DescribeWorkflowExecution(ctx, runID, "")
		if err == nil {
			version := described.GetWorkflowExecutionInfo().GetVersioningInfo().GetDeploymentVersion()
			if version.GetBuildId() == want.BuildID && version.GetDeploymentName() == want.Name {
				return
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("run %s never reported deployment version %s (last error: %v); "+
				"without that the rest of this test would prove nothing about pinning", runID, want, err)
		}
		time.Sleep(time.Second)
	}
}

// assertNoTaskOfRunRan fails if the replica executed any task of the run.
func assertNoTaskOfRunRan(t *testing.T, r *replica, runID, build, why string) {
	t.Helper()
	for key := range r.ranTasks() {
		if strings.HasPrefix(key, runID+"/") {
			t.Fatalf("%s's Orker executed %s: %s", build, key, why)
		}
	}
}

func executionStatus(
	t *testing.T,
	ctx context.Context,
	c client.Client,
	runID string,
) enums.WorkflowExecutionStatus {
	t.Helper()
	described, err := c.DescribeWorkflowExecution(ctx, runID, "")
	if err != nil {
		t.Fatalf("describe %s: %v", runID, err)
	}
	return described.GetWorkflowExecutionInfo().GetStatus()
}

// assertTaskScheduledAndNeverStarted reads the stranded run's own history: the
// activity is on the queue, and no worker of any version has taken it. It
// returns how many are waiting.
func assertTaskScheduledAndNeverStarted(
	t *testing.T,
	ctx context.Context,
	c client.Client,
	runID string,
) int {
	t.Helper()
	scheduled := map[int64]string{}
	started := map[string]bool{}
	for _, event := range runHistory(t, ctx, c, runID) {
		if attrs := event.GetActivityTaskScheduledEventAttributes(); attrs != nil {
			if attrs.GetActivityType().GetName() == task.ActivityExecuteTask {
				scheduled[event.GetEventId()] = executeTaskIDFrom(t, attrs.GetInput())
			}
			continue
		}
		if attrs := event.GetActivityTaskStartedEventAttributes(); attrs != nil {
			if taskID, ok := scheduled[attrs.GetScheduledEventId()]; ok {
				started[taskID] = true
			}
		}
	}

	if len(scheduled) == 0 {
		t.Fatalf("run %s scheduled no task activity at all; it never got as far as needing an Orker, "+
			"so nothing here is evidence about one being absent", runID)
	}
	for _, taskID := range scheduled {
		if started[taskID] {
			t.Errorf("task %q of run %s was STARTED while its version's Orker was stopped: some worker "+
				"took it — which, with v2 Current, means the activity fell through to the Current "+
				"Version and an in-flight run reached new plugin code mid-rollout", taskID, runID)
		}
	}
	return len(scheduled)
}

// drainOrkflow is a plain two-task chain. Its task activities use the run's
// bound default pool queue, just as a production submission with no explicit
// pool does. The queue argument remains so callers can name the queue bound
// into the workflow input while this fixture keeps the definition user-facing.
func drainOrkflow(_ string) *orkstackv1.Orkflow {
	return &orkstackv1.Orkflow{
		Name:    "drain-pinned",
		Version: "1.0.0",
		Tasks: []*orkstackv1.Task{
			{Id: "prepare", Uses: "shell", TimeoutSeconds: 240,
				With: mustStruct(map[string]any{"run": "echo prepare"})},
			{Id: "report", Uses: "shell", Needs: []string{"prepare"}, TimeoutSeconds: 240,
				With: mustStruct(map[string]any{"run": "echo report"})},
		},
	}
}
