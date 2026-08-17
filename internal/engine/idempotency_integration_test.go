//go:build integration

package engine_test

import (
	"context"
	"fmt"
	"net"
	"os"
	"sync"
	"testing"
	"time"

	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"

	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/internal/task"
)

// claimNamespace is the Temporal namespace these tests use. AGENTS.md §6: a
// namespace is a hard partition, and these tests count executions inside it,
// so they must not share one with another agent's runs.
func claimNamespace() string {
	if ns := os.Getenv("ORK_TEMPORAL_NAMESPACE"); ns != "" {
		return ns
	}
	return "ork-m8-2-idempotency"
}

// claimQueue names a queue only this test's worker polls, so no other engine
// can execute its claims.
func claimQueue() string {
	return fmt.Sprintf("ork-claim-%d-%d", os.Getpid(), time.Now().UnixNano())
}

// dialClaimCluster connects to a real Temporal, provisioning the isolated
// namespace, and skips cleanly when no cluster is there.
func dialClaimCluster(t *testing.T, ctx context.Context) client.Client {
	t.Helper()
	address := temporalAddress()
	conn, err := net.DialTimeout("tcp", address, 2*time.Second)
	if err != nil {
		t.Skipf("temporal is not reachable at %s: %v", address, err)
	}
	_ = conn.Close()

	namespace := claimNamespace()
	bootstrap, err := client.DialContext(ctx, client.Options{
		HostPort:  address,
		Namespace: client.DefaultNamespace,
	})
	if err != nil {
		t.Skipf("cannot dial temporal at %s: %v", address, err)
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
			if _, probe := c.DescribeTaskQueue(ctx, claimQueue(), 0); probe == nil {
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

// startClaimWorker hosts IdempotencyClaimWorkflow on a private queue, which is
// what `ork` does on its own run worker.
func startClaimWorker(t *testing.T, c client.Client, queue string) {
	t.Helper()
	w := worker.New(c, queue, worker.Options{
		DisableRegistrationAliasing: true,
		LocalActivityWorkerOnly:     true,
	})
	w.RegisterWorkflowWithOptions(engine.IdempotencyClaimWorkflow,
		workflow.RegisterOptions{Name: engine.ClaimWorkflowName})
	if err := w.Start(); err != nil {
		t.Fatalf("start claim worker: %v", err)
	}
	t.Cleanup(w.Stop)
}

// claimKey names a key unique to this test run, so a rerun starts unbound.
func claimKey(t *testing.T) string {
	t.Helper()
	return fmt.Sprintf("m8-2-%s-%d-%d", t.Name(), os.Getpid(), time.Now().UnixNano())
}

// TestIntegration_ConcurrentClaimsConvergeOnOneRun is acceptance 2 against the
// real server: many callers offering different candidate run ids for one key
// at the same instant are all told the same answer, and the cluster holds one
// claim.
//
// Each caller has its own TemporalIdempotency, which is what a replica is at
// this layer: they share nothing but the cluster.
func TestIntegration_ConcurrentClaimsConvergeOnOneRun(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	c := dialClaimCluster(t, ctx)

	queue := claimQueue()
	startClaimWorker(t, c, queue)

	key := claimKey(t)
	claimID := engine.ClaimWorkflowID(key)
	t.Cleanup(func() {
		_ = c.TerminateWorkflow(context.Background(), claimID, "", "m8.2 claim test cleanup")
	})

	const callers = 16
	winners := make([]engine.Claim, callers)
	candidates := make([]string, callers)
	failures := make([]error, callers)
	start := make(chan struct{})
	var wg sync.WaitGroup
	for i := range callers {
		candidates[i] = engine.NewRunID()
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			idempotency := engine.NewTemporalIdempotency(c, queue, time.Hour)
			<-start
			winner, err := idempotency.Claim(ctx, key, func() (engine.Claim, error) {
				return engine.Claim{
					RunID:    candidates[i],
					Snapshot: []byte("snapshot of " + candidates[i]),
				}, nil
			})
			winners[i], failures[i] = winner, err
		}(i)
	}
	close(start)
	wg.Wait()

	for i, err := range failures {
		if err != nil {
			t.Fatalf("caller %d: %v", i, err)
		}
	}
	winner := winners[0]
	for i, got := range winners {
		if got.RunID != winner.RunID {
			t.Fatalf("caller %d was told run %q, caller 0 was told %q: the key named two runs",
				i, got.RunID, winner.RunID)
		}
		if string(got.Snapshot) != "snapshot of "+winner.RunID {
			t.Errorf("caller %d got snapshot %q, want the winner's", i, string(got.Snapshot))
		}
	}
	if !contains(candidates, winner.RunID) {
		t.Fatalf("the winning run %q was offered by nobody", winner.RunID)
	}
	t.Logf("%d concurrent callers converged on run %s at claim %s", callers, winner.RunID, claimID)

	// One claim, not one per caller.
	executions := countExecutions(ctx, t, c, fmt.Sprintf("WorkflowId = %q", claimID))
	if executions != 1 {
		t.Fatalf("claim id %s names %d executions, want exactly one", claimID, executions)
	}

	// A repeat after the race still gets the same answer: the claim is held,
	// never released, which is what keeps a failed start deduplicated too.
	repeat, err := engine.NewTemporalIdempotency(c, queue, time.Hour).
		Claim(ctx, key, func() (engine.Claim, error) {
			return engine.Claim{RunID: engine.NewRunID()}, nil
		})
	if err != nil {
		t.Fatalf("repeat claim: %v", err)
	}
	if repeat.RunID != winner.RunID {
		t.Errorf("a later claim got %q, want the holder %q", repeat.RunID, winner.RunID)
	}
}

// TestIntegration_ClaimFollowsAReissuedRunID is the reissue path on the real
// server: the holder moves its own binding, and a caller that is not the
// holder cannot.
func TestIntegration_ClaimFollowsAReissuedRunID(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	c := dialClaimCluster(t, ctx)

	queue := claimQueue()
	startClaimWorker(t, c, queue)

	key := claimKey(t)
	t.Cleanup(func() {
		_ = c.TerminateWorkflow(context.Background(), engine.ClaimWorkflowID(key), "",
			"m8.2 claim test cleanup")
	})
	idempotency := engine.NewTemporalIdempotency(c, queue, time.Hour)

	held := engine.NewRunID()
	if _, err := idempotency.Claim(ctx, key, func() (engine.Claim, error) {
		return engine.Claim{RunID: held}, nil
	}); err != nil {
		t.Fatalf("first claim: %v", err)
	}

	// A stranger cannot take the binding over.
	if err := idempotency.Rebind(ctx, key, engine.Claim{RunID: engine.NewRunID()},
		"ork-not-the-holder"); err == nil {
		t.Fatal("a non-holder moved the claim")
	}

	reissued := engine.NewRunID()
	if err := idempotency.Rebind(ctx, key, engine.Claim{RunID: reissued}, held); err != nil {
		t.Fatalf("rebind: %v", err)
	}
	after, err := idempotency.Claim(ctx, key, func() (engine.Claim, error) {
		return engine.Claim{RunID: engine.NewRunID()}, nil
	})
	if err != nil {
		t.Fatalf("claim after rebind: %v", err)
	}
	if after.RunID != reissued {
		t.Errorf("the key names %q, want the reissued run %q", after.RunID, reissued)
	}
}

// countExecutions returns how many executions a visibility query matches,
// polling because standard visibility is updated asynchronously.
func countExecutions(ctx context.Context, t *testing.T, c client.Client, query string) int {
	t.Helper()
	namespace := claimNamespace()
	deadline := time.Now().Add(30 * time.Second)
	for {
		var total int
		var token []byte
		for {
			resp, err := c.ListWorkflow(ctx, &workflowservice.ListWorkflowExecutionsRequest{
				Namespace:     namespace,
				Query:         query,
				NextPageToken: token,
			})
			if err != nil {
				t.Fatalf("ListWorkflow(%s): %v", query, err)
			}
			total += len(resp.GetExecutions())
			token = resp.GetNextPageToken()
			if len(token) == 0 {
				break
			}
		}
		if total > 0 || time.Now().After(deadline) {
			return total
		}
		time.Sleep(500 * time.Millisecond)
	}
}

func contains(values []string, want string) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
}
