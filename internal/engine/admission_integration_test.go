//go:build integration

package engine_test

import (
	"context"
	"fmt"
	"net"
	"os"
	"testing"
	"time"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/internal/task"
)

// TestIntegration_RunningCountMatchesTheExecutionIndexOnBothPaths is the
// admission count against a real cluster: the number max_running is judged by
// is the same population GetStats.running lists, per namespace, on the
// indexed reader (one CountWorkflow with the namespace as a predicate) and on
// the unindexed one (a memo-filtered walk), and a run that finishes drops out
// of both.
//
// It runs in a Temporal namespace of its own and starts workflows with no
// worker anywhere: a run's record is Running from the moment it starts, which
// is exactly the population under test.
func TestIntegration_RunningCountMatchesTheExecutionIndexOnBothPaths(t *testing.T) {
	address := temporalAddress()
	conn, err := net.DialTimeout("tcp", address, 2*time.Second)
	if err != nil {
		infraAbsent(t, "temporal is not reachable at %s: %v", address, err)
	}
	_ = conn.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	temporalNamespace := fmt.Sprintf("%s-admission-%d-%d", baseNamespace(), os.Getpid(),
		time.Now().UnixNano()%1_000_000_000)
	temporalClient := dialRoutingCluster(t, ctx, address, temporalNamespace)
	indexed, err := task.EnsureRunSearchAttributes(ctx, temporalClient, temporalNamespace)
	if err != nil {
		t.Fatalf("provision the run index in %q: %v", temporalNamespace, err)
	}
	if !indexed {
		t.Fatalf("EnsureRunSearchAttributes reported no error and no index")
	}

	starter := engine.NewTemporalStarter(temporalClient, task.DefaultQueue).WithNamespaceIndex(true)
	start := func(namespace, runID string) {
		t.Helper()
		wf := &orkstackv1.Orkflow{
			Name:      "reconcile",
			Version:   "0.1.0",
			Namespace: namespace,
			Tasks:     []*orkstackv1.Task{{Id: "settle", Uses: "shell", Pool: task.DefaultPool}},
		}
		plan := engine.RunPlan{
			Namespace: namespace,
			Routes:    map[string]string{task.DefaultPool: task.DefaultQueue},
		}
		if err := starter.StartRun(ctx, runID, wf, nil, plan); err != nil {
			t.Fatalf("StartRun(%s in %s): %v", runID, namespace, err)
		}
	}

	stamp := time.Now().UnixNano()
	var payments []string
	for i := 0; i < 3; i++ {
		id := fmt.Sprintf("ork-payments-%d-%d", stamp, i)
		payments = append(payments, id)
		start("payments", id)
	}
	for i := 0; i < 2; i++ {
		start("billing", fmt.Sprintf("ork-billing-%d-%d", stamp, i))
	}

	readers := map[string]*engine.TemporalRunReader{
		"indexed":   engine.NewTemporalRunReader(temporalClient).WithNamespaceIndex(true),
		"unindexed": engine.NewTemporalRunReader(temporalClient),
	}
	listed := func(namespace string) int64 {
		t.Helper()
		now := time.Now()
		executions, err := readers["indexed"].Executions(ctx, engine.StatsWindow{
			Namespace: namespace,
			Since:     now.Add(-time.Hour),
			Until:     now.Add(time.Hour),
		})
		if err != nil {
			t.Fatalf("Executions(%s): %v", namespace, err)
		}
		return int64(len(executions.Running))
	}

	// agree waits — by deadline, the index being eventually consistent — for
	// the listing to reach want, then requires every reader's count to equal
	// it on the same index.
	agree := func(namespace string, want int64) {
		t.Helper()
		deadline := time.Now().Add(90 * time.Second)
		for {
			got := listed(namespace)
			counts := map[string]int64{}
			settled := got == want
			for name, reader := range readers {
				count, err := reader.RunningCount(ctx, namespace, 0)
				if err != nil {
					t.Fatalf("%s RunningCount(%s): %v", name, namespace, err)
				}
				counts[name] = count
				if count != got {
					settled = false
				}
			}
			if settled {
				t.Logf("namespace %s: Executions.Running=%d, RunningCount indexed=%d unindexed=%d",
					namespace, got, counts["indexed"], counts["unindexed"])
				return
			}
			if time.Now().After(deadline) {
				t.Fatalf("namespace %s: Executions.Running=%d, RunningCount=%v, want all %d",
					namespace, got, counts, want)
			}
			time.Sleep(500 * time.Millisecond)
		}
	}

	agree("payments", 3)
	agree("billing", 2)

	// A run that finishes is no longer in flight, by either path.
	if err := temporalClient.TerminateWorkflow(ctx, payments[0], "", "admission test: finish one"); err != nil {
		t.Fatalf("TerminateWorkflow(%s): %v", payments[0], err)
	}
	agree("payments", 2)
	agree("billing", 2)
}
