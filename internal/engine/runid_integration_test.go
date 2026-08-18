//go:build integration

package engine_test

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"testing"
	"time"

	"connectrpc.com/connect"
	"go.temporal.io/sdk/client"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/api"
	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/internal/task"
)

// runIDNamespace is the Temporal namespace these tests use. AGENTS.md §6: a
// namespace is a hard partition, and these tests deliberately collide workflow
// ids, so they must not share one with another agent's runs.
func runIDNamespace() string {
	if ns := os.Getenv("ORK_TEMPORAL_NAMESPACE"); ns != "" {
		return ns
	}
	return "ork-m8-1-run-identity"
}

// runIDQueue names a queue no worker polls. Nothing here needs a run to
// execute — only the server's answer to a workflow id — and an unpolled queue
// keeps every started workflow parked in RUNNING, which is the state a
// conflicting start has to be refused against.
func runIDQueue() string {
	return fmt.Sprintf("ork-runid-%d-%d", os.Getpid(), time.Now().UnixNano())
}

// dialRunIDCluster connects to a real Temporal, provisioning the isolated
// namespace, and skips cleanly when no cluster is there.
func dialRunIDCluster(t *testing.T, ctx context.Context) client.Client {
	t.Helper()
	address := temporalAddress()
	conn, err := net.DialTimeout("tcp", address, 2*time.Second)
	if err != nil {
		infraAbsent(t, "temporal is not reachable at %s: %v", address, err)
	}
	_ = conn.Close()

	namespace := runIDNamespace()
	bootstrap, err := client.DialContext(ctx, client.Options{
		HostPort:  address,
		Namespace: client.DefaultNamespace,
	})
	if err != nil {
		infraAbsent(t, "cannot dial temporal at %s: %v", address, err)
	}
	if err := task.EnsureNamespace(ctx, bootstrap, namespace); err != nil {
		bootstrap.Close()
		t.Fatalf("provision namespace %q: %v", namespace, err)
	}
	bootstrap.Close()

	// A freshly registered namespace is not immediately visible to the
	// frontend's namespace cache, so the first dial is retried rather than
	// failed.
	deadline := time.Now().Add(30 * time.Second)
	for {
		c, err := client.DialContext(ctx, client.Options{HostPort: address, Namespace: namespace})
		if err == nil {
			if _, probe := c.DescribeTaskQueue(ctx, runIDQueue(), 0); probe == nil {
				t.Cleanup(c.Close)
				return c
			}
			c.Close()
		}
		if time.Now().After(deadline) {
			t.Fatalf("namespace %q did not become usable: %v", namespace, err)
		}
		time.Sleep(time.Second)
	}
}

// terminateAll cleans up the parked workflows a test started, so a rerun in
// the same namespace does not inherit them.
func terminateAll(t *testing.T, c client.Client, ids ...string) {
	t.Helper()
	t.Cleanup(func() {
		for _, id := range ids {
			_ = c.TerminateWorkflow(context.Background(), id, "", "run id integration test cleanup")
		}
	})
}

// TestIntegration_WithoutTheStartPoliciesACollisionAttachesToAnotherRun is the
// hazard M8.1 removes, recorded against the real server rather than argued
// from the SDK source: with Temporal's own defaults, starting a workflow id
// that already exists returns no error at all, and the handle names the *other*
// execution. A replica whose id collided would report someone else's run as
// its own.
func TestIntegration_WithoutTheStartPoliciesACollisionAttachesToAnotherRun(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	c := dialRunIDCluster(t, ctx)

	runID, queue := engine.NewRunID(), runIDQueue()
	terminateAll(t, c, runID)

	first, err := c.ExecuteWorkflow(ctx,
		client.StartWorkflowOptions{ID: runID, TaskQueue: queue},
		engine.RunWorkflowType, runID, twoTaskOrkflow(), (*orkstackv1.Payload)(nil), rawPlan(queue))
	if err != nil {
		t.Fatalf("first start: %v", err)
	}
	second, err := c.ExecuteWorkflow(ctx,
		client.StartWorkflowOptions{ID: runID, TaskQueue: queue},
		engine.RunWorkflowType, runID, twoTaskOrkflow(), (*orkstackv1.Payload)(nil), rawPlan(queue))
	if err != nil {
		t.Fatalf("second start with the same id returned %v; "+
			"this test records the default behaviour and must be revisited if the server changed it", err)
	}
	if second.GetRunID() != first.GetRunID() {
		t.Fatalf("second start got execution %q, want the first one %q",
			second.GetRunID(), first.GetRunID())
	}
	t.Logf("defaults: the second start silently attached to execution %s — this is the hijack "+
		"TemporalStarter.StartRun's three policies prevent", first.GetRunID())
}

// TestIntegration_StartRunRefusesARunIDTheClusterAlreadyHolds is acceptance 1
// against the real server: with the policies set, a reused id is an error, so
// two replicas can never both believe they own a run.
func TestIntegration_StartRunRefusesARunIDTheClusterAlreadyHolds(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	c := dialRunIDCluster(t, ctx)

	queue := runIDQueue()
	// Two starters stand for two engine replicas pointed at one cluster.
	replicaA := engine.NewTemporalStarter(c, queue)
	replicaB := engine.NewTemporalStarter(c, queue)

	runID := engine.NewRunID()
	terminateAll(t, c, runID)

	if err := replicaA.StartRun(ctx, runID, twoTaskOrkflow(), nil, defaultPlan()); err != nil {
		t.Fatalf("replica A start: %v", err)
	}
	err := replicaB.StartRun(ctx, runID, twoTaskOrkflow(), nil, defaultPlan())
	if !errors.Is(err, engine.ErrRunIDTaken) {
		t.Fatalf("replica B start with A's id = %v, want engine.ErrRunIDTaken", err)
	}

	// Single use means for all time, not only while the first run is live: a
	// closed execution's id must stay refused, or a late retry could resurrect
	// an id whose history someone already read.
	if err := c.TerminateWorkflow(ctx, runID, "", "closing the execution for the reuse check"); err != nil {
		t.Fatalf("terminate %s: %v", runID, err)
	}
	err = replicaB.StartRun(ctx, runID, twoTaskOrkflow(), nil, defaultPlan())
	if !errors.Is(err, engine.ErrRunIDTaken) {
		t.Fatalf("start with a closed run's id = %v, want engine.ErrRunIDTaken", err)
	}
}

// TestIntegration_ConcurrentReplicasNeverBothOwnOneRunID races the two
// replicas, which is the shape of the real failure: whichever loses must be
// told, not handed the winner's run.
func TestIntegration_ConcurrentReplicasNeverBothOwnOneRunID(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	c := dialRunIDCluster(t, ctx)

	queue := runIDQueue()
	replicas := []*engine.TemporalStarter{
		engine.NewTemporalStarter(c, queue),
		engine.NewTemporalStarter(c, queue),
		engine.NewTemporalStarter(c, queue),
		engine.NewTemporalStarter(c, queue),
	}
	runID := engine.NewRunID()
	terminateAll(t, c, runID)

	start := make(chan struct{})
	results := make(chan error, len(replicas))
	for _, replica := range replicas {
		go func(r *engine.TemporalStarter) {
			<-start
			results <- r.StartRun(ctx, runID, twoTaskOrkflow(), nil, defaultPlan())
		}(replica)
	}
	close(start)

	owners, refused := 0, 0
	for range replicas {
		switch err := <-results; {
		case err == nil:
			owners++
		case errors.Is(err, engine.ErrRunIDTaken):
			refused++
		default:
			t.Errorf("unexpected start failure: %v", err)
		}
	}
	if owners != 1 || refused != len(replicas)-1 {
		t.Errorf("%d replicas owned the run id and %d were refused, want 1 and %d",
			owners, refused, len(replicas)-1)
	}
}

// TestIntegration_SubmitRunReissuesARunIDTheClusterRefuses is the whole slice
// end to end: the API meets a genuinely taken id on a real cluster and the
// submission still succeeds, under a different id, with a workflow to show for
// it.
func TestIntegration_SubmitRunReissuesARunIDTheClusterRefuses(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	c := dialRunIDCluster(t, ctx)

	queue := runIDQueue()
	starter := engine.NewTemporalStarter(c, queue)
	wf := twoTaskOrkflow()

	// Occupy an id, then hand the Service that id first: the server refuses
	// it, exactly as it would for a real cross-replica collision.
	taken, reissued := engine.NewRunID(), engine.NewRunID()
	terminateAll(t, c, taken, reissued)
	if err := starter.StartRun(ctx, taken, wf, nil, defaultPlan()); err != nil {
		t.Fatalf("occupy %s: %v", taken, err)
	}

	orkflows := engine.NewMemoryOrkflowStore()
	if err := orkflows.Put(ctx, wf); err != nil {
		t.Fatalf("store orkflow: %v", err)
	}
	candidates := []string{taken, reissued}
	svc := api.NewService(api.Config{
		Orkflows: orkflows,
		Runs:     engine.NewRunStore(),
		Bus:      engine.NewEventBus(),
		Starter:  starter,
		NewRunID: func() string {
			id := candidates[0]
			candidates = candidates[1:]
			return id
		},
	})

	resp, err := svc.SubmitRun(ctx, connect.NewRequest(&orkstackv1.SubmitRunRequest{
		OrkflowName: wf.GetName(),
	}))
	if err != nil {
		t.Fatalf("SubmitRun against a taken id: %v", err)
	}
	if got := resp.Msg.GetRun().GetId(); got != reissued {
		t.Fatalf("SubmitRun returned %q, want the reissued id %q", got, reissued)
	}

	// The reissued id names a real execution, and it is not the occupied one.
	described, err := c.DescribeWorkflowExecution(ctx, reissued, "")
	if err != nil {
		t.Fatalf("describe %s: %v", reissued, err)
	}
	occupied, err := c.DescribeWorkflowExecution(ctx, taken, "")
	if err != nil {
		t.Fatalf("describe %s: %v", taken, err)
	}
	reissuedExecution := described.GetWorkflowExecutionInfo().GetExecution().GetRunId()
	occupiedExecution := occupied.GetWorkflowExecutionInfo().GetExecution().GetRunId()
	if reissuedExecution == occupiedExecution {
		t.Errorf("the reissued run points at the occupied id's execution %q", occupiedExecution)
	}
}
