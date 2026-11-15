//go:build integration

package engine_test

import (
	"context"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"go.temporal.io/api/enums/v1"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/worker"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/task"

	"github.com/orkstack/orkstack/internal/testsupport/must"
)

// handBackOrkflow is one task with a retry, so the attempt its Orker hands back
// is followed by another.
func handBackOrkflow() *orkstackv1.Orkflow {
	return &orkstackv1.Orkflow{
		Name:    "handback-pinned",
		Version: "1.0.0",
		Tasks: []*orkstackv1.Task{
			{Id: "work", Uses: "shell", TimeoutSeconds: 240,
				Retry: &orkstackv1.RetryPolicy{MaximumAttempts: 3},
				With:  mustStruct(map[string]any{"run": "echo work"})},
		},
	}
}

// handBackOrker is a release's Orker that, on a run's first attempt, does what
// a stopping Orker does at the end of its grace (internal/orker stop.go): it
// takes the attempt back, stops its worker at once, and only then reports the
// attempt ORKER_STOPPED itself, by task token. Later attempts succeed.
type handBackOrker struct {
	mu       sync.Mutex
	executed map[string][]string // runID/taskID -> builds, one per attempt
	w        worker.Worker
	taken    chan []byte // the first attempt's task token, once taken
}

func startHandBackOrker(t *testing.T, r *replica, deployment task.Deployment, taken chan []byte) *handBackOrker {
	t.Helper()
	o := &handBackOrker{executed: map[string][]string{}, taken: taken}
	o.w = worker.New(r.client, r.orkerQueue, deployment.Apply(worker.Options{
		DisableRegistrationAliasing: true,
		DisableWorkflowWorker:       true,
	}))
	o.w.RegisterActivityWithOptions(
		func(ctx context.Context, in *orkstackv1.ExecuteTaskInput) (*orkstackv1.ExecuteTaskResult, error) {
			info := activity.GetInfo(ctx)
			o.mu.Lock()
			key := in.GetRunId() + "/" + in.GetTaskId()
			o.executed[key] = append(o.executed[key], deployment.BuildID)
			o.mu.Unlock()
			if info.Attempt == 1 && o.taken != nil {
				o.taken <- info.TaskToken
				return nil, activity.ErrResultPending
			}
			return &orkstackv1.ExecuteTaskResult{TaskId: in.GetTaskId(),
				ValuesJson: valuesJSON(map[string]any{"executed_by_build": deployment.BuildID})}, nil
		},
		activity.RegisterOptions{Name: task.ActivityExecuteTask},
	)
	must.OKf(t, o.w.Start(), "start the hand-back orker of %s", deployment)
	return o
}

func (o *handBackOrker) ran(runID string) []string {
	o.mu.Lock()
	defer o.mu.Unlock()
	return append([]string(nil), o.executed[runID+"/work"]...)
}

// TestIntegration_HandBackDuringARolloutWaitsForItsOwnRelease: a run pinned to
// v1 while v2 is current; v1's only Orker hands back the task it runs and
// stops. The task's next attempt waits, PENDING, for an Orker of v1: v2's
// Orker never starts it, though in the same window it runs a run of its own.
// When a v1 Orker returns, the task runs on it (docs/DEPLOY.md, "Rolling a
// release forward": the old release's Orkers stay until it drains).
func TestIntegration_HandBackDuringARolloutWaitsForItsOwnRelease(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Second)
	defer cancel()

	namespace := versioningNamespace()
	c := dialVersioningCluster(t, ctx, namespace)
	suffix := fmt.Sprintf("%d-%d", os.Getpid(), time.Now().UnixNano())
	deploymentName := "ork-handback-" + suffix
	runQueue := "ork-handback-run-" + suffix
	orkerQueue := "ork-handback-tasks-" + suffix
	v1 := task.Deployment{Name: deploymentName, BuildID: "v1"}
	v2 := task.Deployment{Name: deploymentName, BuildID: "v2"}
	transcript := newRolloutTranscript(t, namespace, deploymentName, runQueue, orkerQueue)
	release := make(chan struct{})
	snapshots := newTestSnapshotStore()

	first := newReplica(t, c, v1, runQueue, orkerQueue, release, snapshots)
	defer first.stop()
	setCurrentVersion(t, ctx, c, v1, runQueue, orkerQueue)
	second := newReplica(t, c, v2, runQueue, orkerQueue, release, snapshots)
	defer second.stop()
	setCurrentVersion(t, ctx, c, v2, runQueue, orkerQueue)
	transcript.step("v1 and v2 up; current version = v2")

	// v1's Orker is replaced by one that hands its first attempt back.
	first.stopOrker()
	taken := make(chan []byte, 1)
	v1Orker := startHandBackOrker(t, first, v1, taken)

	runID := "ork-handback-pinned-" + suffix
	startPinnedToVersion(t, ctx, c, v1, runQueue, orkerQueue, runID, handBackOrkflow())
	awaitVersioningVerdict(t, ctx, c, runID, v1)
	var token []byte
	select {
	case token = <-taken:
	case <-ctx.Done():
		t.Fatal("v1's Orker never received the pinned run's task")
	}
	// The end of the grace: the worker stops, then the attempt reports itself.
	v1Orker.w.Stop()
	handedBack := temporal.NewApplicationError(
		"ORKER_STOPPED: the orker was stopped while this attempt ran, so it handed the attempt back",
		"InfrastructureError", &orkstackv1.ActivityFailureDetail{ReasonCode: "ORKER_STOPPED"})
	must.OKf(t, c.CompleteActivity(ctx, token, nil, handedBack), "report the handed-back attempt")
	transcript.step("v1's only Orker handed the task back and stopped")

	// Positive control: in the same window v2 runs a run of its own.
	controlID := "ork-handback-control-" + suffix
	startPinnedToVersion(t, ctx, c, v2, runQueue, orkerQueue, controlID, drainOrkflow(orkerQueue))
	if err := c.GetWorkflow(ctx, controlID, "").Get(ctx, nil); err != nil {
		t.Fatalf("the control run on v2 did not finish: %v", err)
	}
	deadline := time.Now().Add(strandedWindow)
	for time.Now().Before(deadline) {
		assertNoTaskOfRunRan(t, second, runID, "v2",
			"a task handed back by v1's Orker moved to v2's Orker build")
		if status := executionStatus(t, ctx, c, runID); status != enums.WORKFLOW_EXECUTION_STATUS_RUNNING {
			t.Fatalf("run %s reached %v while no Orker of v1 took work; it must wait", runID, status)
		}
		time.Sleep(2 * time.Second)
	}
	transcript.result(fmt.Sprintf("after %s with v1's Orker stopped: the handed-back task waited, "+
		"v2 ran none of it and ran its own control run", strandedWindow))

	// A v1 Orker returns, and the task runs on it.
	again := startHandBackOrker(t, first, v1, nil)
	defer again.w.Stop()
	if err := c.GetWorkflow(ctx, runID, "").Get(ctx, nil); err != nil {
		t.Fatalf("run %s did not finish once a v1 Orker returned: %v", runID, err)
	}
	if got := append(v1Orker.ran(runID), again.ran(runID)...); len(got) != 2 || got[0] != "v1" || got[1] != "v1" {
		t.Fatalf("the task's attempts ran on %v; want both on v1", got)
	}
	assertNoTaskOfRunRan(t, second, runID, "v2", "v2's Orker ran a task of a run pinned to v1")
	transcript.step("run " + runID + " completed on v1")
	transcript.logOnly(t)
}
