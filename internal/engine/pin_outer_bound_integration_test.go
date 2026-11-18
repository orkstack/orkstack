//go:build integration

package engine_test

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"

	"github.com/orkstack/orkstack/internal/task"
)

// pinOuterBound is the longest session a pausable pin asks for at the
// deployment's defaults: the 31-day pause maximum, a pin timeout of an hour
// and the 600-second margin (design §21.4).
const pinOuterBound = 31*24*time.Hour + time.Hour + 600*time.Second

// outerBoundSession opens one session with the pausable path's outer bound
// as its ExecutionTimeout, runs one activity on it, and completes it.
func outerBoundSession(ctx workflow.Context, queue string) (string, error) {
	base := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{TaskQueue: queue, StartToCloseTimeout: time.Minute})
	sessionCtx, err := workflow.CreateSession(base, &workflow.SessionOptions{
		ExecutionTimeout: pinOuterBound,
		CreationTimeout:  time.Minute,
		HeartbeatTimeout: 10 * time.Second,
	})
	if err != nil {
		return "", err
	}
	defer workflow.CompleteSession(sessionCtx)
	var host string
	if err := workflow.ExecuteActivity(sessionCtx, "OuterBoundMember").Get(sessionCtx, &host); err != nil {
		return "", err
	}
	return workflow.GetSessionInfo(sessionCtx).SessionID + "@" + host, nil
}

// TestIntegration_PinSessionOuterBoundAccepted is the pausable pin's first
// check (design §21.4, §22 L-3): the proof's cluster accepts a session whose
// ExecutionTimeout, the creation activity's StartToClose, is the outer bound
// of more than 31 days, opens it, runs a member on it and completes it. Were
// the cluster to refuse so long a bound, pausable pins could not open.
func TestIntegration_PinSessionOuterBoundAccepted(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	base := os.Getenv("ORK_TEMPORAL_NAMESPACE")
	if base == "" {
		base = task.DefaultNamespace
	}
	namespace := fmt.Sprintf("%s-pin-outer-%d-%d", base, os.Getpid(), time.Now().UnixNano()%1_000_000_000)
	c := dialRoutingCluster(t, ctx, temporalAddress(), namespace)

	queue := fmt.Sprintf("ork-pin-outer-%d-%d", os.Getpid(), time.Now().UnixNano())
	w := worker.New(c, queue, worker.Options{EnableSessionWorker: true})
	w.RegisterWorkflowWithOptions(outerBoundSession, workflow.RegisterOptions{Name: "OuterBoundSession"})
	w.RegisterActivityWithOptions(func(context.Context) (string, error) { return "member", nil },
		activity.RegisterOptions{Name: "OuterBoundMember"})
	if err := w.Start(); err != nil {
		t.Fatalf("start worker: %v", err)
	}
	defer w.Stop()

	// No workflow timeout: the server clamps an activity's StartToClose to
	// it, which would hide a refused bound. The test's context bounds it.
	id := "pin-outer-" + queue
	run, err := c.ExecuteWorkflow(ctx, client.StartWorkflowOptions{ID: id, TaskQueue: queue}, "OuterBoundSession", queue)
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	t.Cleanup(func() { _ = c.TerminateWorkflow(context.Background(), id, "", "pin outer bound test cleanup") })
	var opened string
	if err := run.Get(ctx, &opened); err != nil {
		t.Fatalf("a session with ExecutionTimeout %s was not accepted and opened: %v", pinOuterBound, err)
	}
	if opened == "" {
		t.Fatal("the session opened no member")
	}
	t.Logf("session with ExecutionTimeout %s opened and completed: %s", pinOuterBound, opened)
}
