package engine_test

import (
	"context"
	"errors"
	"testing"

	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/sdk/client"

	"google.golang.org/protobuf/proto"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/internal/task"
)

// recordingClient captures the arguments StartRun hands to Temporal. The
// workflow input is the only place a run's task queue can be decided
// deterministically: workflow code may not read the environment, so whatever
// is recorded here is what every replay of the run will dispatch to.
//
// err, when set, stands in for the server's answer, which is how the
// collision path is exercised without a cluster.
type recordingClient struct {
	client.Client

	options client.StartWorkflowOptions
	args    []any
	err     error
}

func (c *recordingClient) ExecuteWorkflow(
	_ context.Context,
	options client.StartWorkflowOptions,
	_ any,
	args ...any,
) (client.WorkflowRun, error) {
	c.options = options
	c.args = args
	return nil, c.err
}

// startedOrkflow returns the Orkflow StartRun passed to Temporal.
func (c *recordingClient) startedOrkflow(t *testing.T) *orkstackv1.Orkflow {
	t.Helper()
	if len(c.args) < 4 {
		t.Fatalf("ExecuteWorkflow got %d args, want runID, orkflow, input, orker queue", len(c.args))
	}
	wf, ok := c.args[1].(*orkstackv1.Orkflow)
	if !ok {
		t.Fatalf("second workflow arg is %T, want *orkstackv1.Orkflow", c.args[1])
	}
	return wf
}

// startedOrkerQueue returns the queue StartRun bound into the workflow input.
func (c *recordingClient) startedOrkerQueue(t *testing.T) string {
	t.Helper()
	if len(c.args) < 4 {
		t.Fatalf("ExecuteWorkflow got %d args, want runID, orkflow, input, orker queue", len(c.args))
	}
	queue, ok := c.args[3].(string)
	if !ok {
		t.Fatalf("fourth workflow arg is %T, want the orker queue as a string", c.args[3])
	}
	return queue
}

// twoTaskOrkflow has one task that names no pool and one that names its own.
func twoTaskOrkflow() *orkstackv1.Orkflow {
	return &orkstackv1.Orkflow{
		Name:    "hello-ork",
		Version: "0.1.0",
		Tasks: []*orkstackv1.Task{
			{Id: "fetch", Plugin: "http"},
			{Id: "render", Plugin: "shell", Pool: "gpu"},
		},
	}
}

func TestStartRun_BindsTheQueueItsOrkersPollIntoTheWorkflowInput(t *testing.T) {
	c := &recordingClient{}
	starter := engine.NewTemporalStarterWithOrkerQueue(c, task.DefaultQueue, "ork-tasks-m7-9")

	if err := starter.StartRun(context.Background(), "ork-1-1", twoTaskOrkflow(), nil); err != nil {
		t.Fatalf("StartRun: %v", err)
	}

	if got := c.startedOrkerQueue(t); got != "ork-tasks-m7-9" {
		t.Errorf("bound orker queue = %q, want the configured one %q", got, "ork-tasks-m7-9")
	}
}

// TestStartRun_SendsTheStoredDefinitionVerbatim is the M10.1 half of the same
// property. The engine used to clone the orkflow to write a queue name onto
// every task; a definition carries no queue at all now, so the document that
// reaches Temporal is the one the bucket holds — including a task's own pool,
// which is the user's word and not the engine's to rewrite.
func TestStartRun_SendsTheStoredDefinitionVerbatim(t *testing.T) {
	c := &recordingClient{}
	starter := engine.NewTemporalStarterWithOrkerQueue(c, task.DefaultQueue, "ork-tasks-m7-9")

	stored := twoTaskOrkflow()
	if err := starter.StartRun(context.Background(), "ork-1-1", stored, nil); err != nil {
		t.Fatalf("StartRun: %v", err)
	}

	started := c.startedOrkflow(t)
	if started != stored {
		t.Error("StartRun sent a copy of the orkflow; the stored definition travels as it is")
	}
	if !proto.Equal(started, twoTaskOrkflow()) {
		t.Errorf("the orkflow reached Temporal modified: %v", started)
	}
}

func TestStartRun_FallsBackToTheSharedQueueWhenNoOrkerQueueIsConfigured(t *testing.T) {
	c := &recordingClient{}
	starter := engine.NewTemporalStarter(c, task.DefaultQueue)

	if err := starter.StartRun(context.Background(), "ork-1-1", twoTaskOrkflow(), nil); err != nil {
		t.Fatalf("StartRun: %v", err)
	}

	if got := c.startedOrkerQueue(t); got != task.DefaultQueue {
		t.Errorf("bound orker queue = %q, want %q", got, task.DefaultQueue)
	}
}

func TestStartRun_StartsTheWorkflowOnTheEnginesOwnQueue(t *testing.T) {
	c := &recordingClient{}
	starter := engine.NewTemporalStarterWithOrkerQueue(c, task.DefaultQueue, "ork-tasks-m7-9")

	if err := starter.StartRun(context.Background(), "ork-1-1", twoTaskOrkflow(), nil); err != nil {
		t.Fatalf("StartRun: %v", err)
	}

	// The orker queue moves task activities, not the run workflow itself: the
	// workflow stays on the queue this engine's run worker polls.
	if c.options.TaskQueue != task.DefaultQueue {
		t.Errorf("RunWorkflow started on queue %q, want %q", c.options.TaskQueue, task.DefaultQueue)
	}
	if c.options.ID != "ork-1-1" {
		t.Errorf("workflow id = %q, want the run id", c.options.ID)
	}
}

// TestStartRun_MakesARunIDSingleUse pins the server-side half of M8.1.
//
// Without these three options a colliding id is silent: ExecuteWorkflow
// returns a handle to whatever execution already holds the id and the caller
// believes its own run started (SDK v1.47.0
// internal/internal_workflow_client.go:2201 swallows the already-started error
// when WorkflowExecutionErrorWhenAlreadyStarted is false). Uniqueness is then
// only as good as the entropy in the id; with them it is decided by the
// server.
func TestStartRun_MakesARunIDSingleUse(t *testing.T) {
	c := &recordingClient{}
	starter := engine.NewTemporalStarter(c, task.DefaultQueue)

	if err := starter.StartRun(context.Background(), "ork-1-1", twoTaskOrkflow(), nil); err != nil {
		t.Fatalf("StartRun: %v", err)
	}

	if got := c.options.WorkflowIDReusePolicy; got != enumspb.WORKFLOW_ID_REUSE_POLICY_REJECT_DUPLICATE {
		t.Errorf("WorkflowIDReusePolicy = %v, want REJECT_DUPLICATE so a finished run's id is never reused", got)
	}
	if got := c.options.WorkflowIDConflictPolicy; got != enumspb.WORKFLOW_ID_CONFLICT_POLICY_FAIL {
		t.Errorf("WorkflowIDConflictPolicy = %v, want FAIL so a running run's id is never joined", got)
	}
	if !c.options.WorkflowExecutionErrorWhenAlreadyStarted {
		t.Error("WorkflowExecutionErrorWhenAlreadyStarted = false: a collision would return a handle " +
			"to another replica's run instead of an error")
	}
}

// TestStartRun_ReportsATakenRunIDAsErrRunIDTaken is what lets the API tell a
// collision apart from every other start failure, so it can reissue the id
// instead of failing the submission.
func TestStartRun_ReportsATakenRunIDAsErrRunIDTaken(t *testing.T) {
	c := &recordingClient{err: serviceerror.NewWorkflowExecutionAlreadyStarted(
		"Workflow execution is already running", "start-request-id", "temporal-run-id")}
	starter := engine.NewTemporalStarter(c, task.DefaultQueue)

	err := starter.StartRun(context.Background(), "ork-1-1", twoTaskOrkflow(), nil)
	if !errors.Is(err, engine.ErrRunIDTaken) {
		t.Fatalf("StartRun with a taken id = %v, want an error matching engine.ErrRunIDTaken", err)
	}
	var alreadyStarted *serviceerror.WorkflowExecutionAlreadyStarted
	if !errors.As(err, &alreadyStarted) {
		t.Error("the server's own error was dropped; it is the only record of which execution holds the id")
	}
}

// TestStartRun_DoesNotMistakeAnUnrelatedFailureForATakenID keeps the retry
// narrow: a run that failed to start because the cluster was unreachable must
// not be silently reissued under a new id.
func TestStartRun_DoesNotMistakeAnUnrelatedFailureForATakenID(t *testing.T) {
	c := &recordingClient{err: serviceerror.NewUnavailable("temporal is down")}
	starter := engine.NewTemporalStarter(c, task.DefaultQueue)

	err := starter.StartRun(context.Background(), "ork-1-1", twoTaskOrkflow(), nil)
	if err == nil {
		t.Fatal("StartRun succeeded, want the cluster failure reported")
	}
	if errors.Is(err, engine.ErrRunIDTaken) {
		t.Errorf("StartRun reported %v as a taken run id", err)
	}
}
