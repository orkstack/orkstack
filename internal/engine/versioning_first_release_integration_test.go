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

	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/internal/task"
)

// idleWindow is how long the first versioned release is watched receiving
// nothing. Routing is a server decision made when the workflow task is
// dispatched, not a race, so this window only has to be long enough to be
// unambiguous — and short enough that the suite stays usable.
const idleWindow = 15 * time.Second

// TestIntegration_AVersionedReleaseIsIdleUntilItsVersionIsCurrent is the test
// for the first step of a versioned deployment, and for the outage that step
// prevents.
//
// Setting ORK_BUILD_ID makes both binaries register as a Deployment Version.
// It does NOT route anything to them: until an operator makes that version the
// deployment's Current Version, the task queue's current version is nil, and
//
//	"A nil Current Version represents all the unversioned workers"
//	(api@v1.63.5/workflowservice/v1/request_response.pb.go,
//	 SetWorkerDeploymentCurrentVersionRequest.BuildId)
//
// so every workflow task waits for an unversioned poller that a versioned
// release does not have. The workers are healthy, the cluster is healthy, and
// no run moves — which is exactly what a runbook that says "set one variable"
// and stops there produces on the first release.
//
// The test therefore asserts both halves:
//
//  1. with the version registered and polling BOTH queues, a submitted run
//     receives no workflow task at all — not a slow one, none;
//  2. one SetCurrentVersion call, with the SAME workers still running and no
//     restart, releases that same run, which then completes pinned to v1.
//
// It also pins the queue shape the runbook tells an operator to check: the
// engine's own queue appears as the version's WORKFLOW poller and the Orker
// queue as its ACTIVITY poller. They are different queues whenever
// ORK_ORKER_QUEUE is set, and a check that expects one name for both aborts
// healthy rollouts.
func TestIntegration_AVersionedReleaseIsIdleUntilItsVersionIsCurrent(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancel()

	namespace := versioningNamespace()
	c := dialVersioningCluster(t, ctx, namespace)

	// Private names: a deployment and its queues are namespace-wide state,
	// and this test's whole subject is a deployment that has never had a
	// Current Version.
	suffix := fmt.Sprintf("%d-%d", os.Getpid(), time.Now().UnixNano())
	deploymentName := "ork-firstrelease-" + suffix
	runQueue := "ork-firstrelease-run-" + suffix
	orkerQueue := "ork-firstrelease-tasks-" + suffix
	v1 := task.Deployment{Name: deploymentName, BuildID: "v1"}

	// Nothing gates this run: the point is that it does not start, not that
	// the test holds it.
	released := make(chan struct{})
	close(released)
	release := newReplica(t, c, v1, runQueue, orkerQueue, released)
	defer release.stop()

	// The workers are up and the server knows it. This is the load-bearing
	// distinction: an operator staring at a stalled engine can see healthy
	// pollers and conclude the deployment is fine.
	described := awaitVersionPollers(t, ctx, c, v1, runQueue, orkerQueue)
	assertQueueRoles(t, described, runQueue, orkerQueue)

	starter := engine.NewTemporalStarterWithOrkerQueue(c, runQueue, orkerQueue)
	runID := "ork-firstrelease-run-" + suffix
	if err := starter.StartRun(ctx, runID, plainOrkflow(), nil, rawPlan(orkerQueue)); err != nil {
		t.Fatalf("submit the run: %v", err)
	}

	// --- 1. nothing is dispatched --------------------------------------
	deadline := time.Now().Add(idleWindow)
	for time.Now().Before(deadline) {
		assertNoWorkflowTaskStarted(t, ctx, c, runID)
		time.Sleep(time.Second)
	}
	// The version is still not Current — said by the server, not by the test's
	// own bookkeeping, so "nothing was dispatched" cannot be explained by a
	// routing change nobody noticed.
	described = awaitVersionPollers(t, ctx, c, v1, runQueue, orkerQueue)
	if !described.Info.CurrentSinceTime.IsZero() {
		t.Fatalf("version %s became Current at %s during the idle window; the observation above proves nothing",
			v1, described.Info.CurrentSinceTime)
	}
	t.Logf("run %s received no workflow task in %s, with %s polling %s and %s",
		runID, idleWindow, v1, runQueue, orkerQueue)

	// --- 2. one routing change, no restart, and the run moves ------------
	started := time.Now()
	setCurrentVersion(t, ctx, c, v1, runQueue, orkerQueue)
	if err := c.GetWorkflow(ctx, runID, "").Get(ctx, nil); err != nil {
		t.Fatalf("run %s did not complete after %s became Current: %v", runID, v1, err)
	}
	t.Logf("the same run completed %.1fs after set-current-version, with no worker restart",
		time.Since(started).Seconds())

	transcript := newRolloutTranscript(t, namespace, deploymentName, runQueue, orkerQueue)
	assertRunVersion(t, ctx, c, transcript, runID, v1)
	assertTaskBuilds(t, ctx, c, transcript, runID, "v1")
	t.Log("\n" + strings.Join(transcript.lines, "\n"))
}

// awaitVersionPollers waits until the server reports this version polling
// every queue named, and returns what it reported.
func awaitVersionPollers(
	t *testing.T,
	ctx context.Context,
	c client.Client,
	deployment task.Deployment,
	queues ...string,
) client.WorkerDeploymentVersionDescription {
	t.Helper()
	handle := c.WorkerDeploymentClient().GetHandle(deployment.Name)
	deadline := time.Now().Add(60 * time.Second)
	for {
		described, err := handle.DescribeVersion(ctx, client.WorkerDeploymentDescribeVersionOptions{
			BuildID: deployment.BuildID,
		})
		if err == nil && pollsEveryQueue(described, queues) {
			return described
		}
		if time.Now().After(deadline) {
			t.Fatalf("deployment version %s never reported pollers on %v (last error: %v)",
				deployment, queues, err)
		}
		time.Sleep(500 * time.Millisecond)
	}
}

// assertQueueRoles holds the runbook's rollout check to the queues an OrkStack
// release really polls: the engine's own queue carries the workflows, the
// Orker's carries the activities.
func assertQueueRoles(
	t *testing.T,
	described client.WorkerDeploymentVersionDescription,
	runQueue, orkerQueue string,
) {
	t.Helper()
	if runQueue == orkerQueue {
		t.Fatalf("this test needs distinct queues to tell the two roles apart; both are %q", runQueue)
	}
	roles := map[string][]client.TaskQueueType{}
	for _, info := range described.Info.TaskQueuesInfos {
		roles[info.Name] = append(roles[info.Name], info.Type)
	}
	if !hasRole(roles[runQueue], client.TaskQueueTypeWorkflow) {
		t.Errorf("the engine's queue %q is not listed as a WORKFLOW poller of this version (listed: %v); "+
			"a rollout check that looks for the workflow poller under ORK_ORKER_QUEUE finds nothing",
			runQueue, described.Info.TaskQueuesInfos)
	}
	if !hasRole(roles[orkerQueue], client.TaskQueueTypeActivity) {
		t.Errorf("the Orker queue %q is not listed as an ACTIVITY poller of this version (listed: %v); "+
			"a pinned run's tasks would fall through to the queue's own current version",
			orkerQueue, described.Info.TaskQueuesInfos)
	}
	if hasRole(roles[orkerQueue], client.TaskQueueTypeWorkflow) {
		t.Errorf("the Orker queue %q also polls WORKFLOW tasks; the engine's run worker has moved onto it",
			orkerQueue)
	}
}

func hasRole(types []client.TaskQueueType, want client.TaskQueueType) bool {
	for _, got := range types {
		if got == want {
			return true
		}
	}
	return false
}

// assertNoWorkflowTaskStarted fails the moment any worker picks the run up.
// WorkflowTaskStarted is the event that says a worker received the task, so
// its absence is the claim: nothing was dispatched, rather than something was
// dispatched and failed.
func assertNoWorkflowTaskStarted(t *testing.T, ctx context.Context, c client.Client, runID string) {
	t.Helper()
	for _, event := range runHistory(t, ctx, c, runID) {
		if event.GetWorkflowTaskStartedEventAttributes() != nil {
			t.Fatalf("run %s was dispatched to a worker (%v at event %d) before its deployment version "+
				"was made Current: either a versioned release is being routed to without a Current "+
				"Version, or an unversioned worker is polling this queue",
				runID, event.GetEventType(), event.GetEventId())
		}
		if event.GetEventType() == enums.EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED {
			t.Fatalf("run %s completed before its deployment version was made Current", runID)
		}
	}
}
