//go:build integration

package engine_test

import (
	"context"
	"fmt"
	"net"
	"os"
	"sort"
	"testing"
	"time"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/internal/task"

	"github.com/orkstack/orkstack/internal/testsupport/must"
)

// TestIntegration_ScopedReadsKeepUnstampedRuns is the mixed fleet, against a
// real cluster: one engine stamps a run with OrkflowNamespace, another booted
// unindexed and started one without it, both in namespace x, and an unstamped
// run of namespace y sits beside them. An indexed reader's scoped list, stats
// durations and live-run count for x return both of x's runs and none of y's.
//
// It runs in a Temporal namespace of its own (AGENTS.md §11.1) and writes no
// object, so it owns no bucket. Its runs start with no worker anywhere: what
// is under test is the visibility index. Half of them are terminated, so the
// durations have closed runs to sample; the other half stay live for the count.
func TestIntegration_ScopedReadsKeepUnstampedRuns(t *testing.T) {
	address := temporalAddress()
	conn, err := net.DialTimeout("tcp", address, 2*time.Second)
	if err != nil {
		infraAbsent(t, "temporal is not reachable at %s: %v", address, err)
	}
	_ = conn.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	temporalNamespace := fmt.Sprintf("%s-nsnull-%d-%d", baseNamespace(), os.Getpid(),
		time.Now().UnixNano()%1_000_000_000)
	temporalClient := dialRoutingCluster(t, ctx, address, temporalNamespace)

	// The engine's own boot steps, as `ork` runs them.
	must.OKf(t, task.WaitNamespaceVisible(ctx, temporalClient, temporalNamespace), "wait for %q", temporalNamespace)
	indexed, err := task.EnsureRunSearchAttributes(ctx, temporalClient, temporalNamespace)
	must.OKf(t, err, "provision the run index in %q", temporalNamespace)
	if !indexed {
		t.Fatal("EnsureRunSearchAttributes reported no error and no index")
	}

	windowStart := time.Now().Add(-time.Minute)
	stamping := engine.NewTemporalStarter(temporalClient, task.DefaultQueue).WithNamespaceIndex(true)
	unindexed := engine.NewTemporalStarter(temporalClient, task.DefaultQueue).WithNamespaceIndex(false)
	start := func(starter *engine.TemporalStarter, namespace, runID string) {
		t.Helper()
		wf := &orkstackv1.Orkflow{
			Name:      "deploy",
			Version:   "0.1.0",
			Namespace: namespace,
			Tasks:     []*orkstackv1.Task{{Id: "settle", Uses: "shell", Pool: task.DefaultPool}},
		}
		plan := engine.RunPlan{
			Namespace: namespace,
			Routes:    map[string]string{task.DefaultPool: task.DefaultQueue},
		}
		must.OKf(t, starter.StartRun(ctx, runID, wf, nil, plan), "StartRun(%s in %s)", runID, namespace)
	}
	stamp := time.Now().UnixNano()
	id := func(name string) string { return fmt.Sprintf("ork-%s-%d", name, stamp) }

	// Closed runs, for the durations.
	start(stamping, "x", id("x-stamped-done"))
	start(unindexed, "x", id("x-unstamped-done"))
	start(unindexed, "y", id("y-unstamped-done"))
	for _, done := range []string{id("x-stamped-done"), id("x-unstamped-done"), id("y-unstamped-done")} {
		must.OKf(t, temporalClient.TerminateWorkflow(ctx, done, "", "closed for the stats window"), "terminate %s", done)
	}
	// Live runs, for the count.
	start(stamping, "x", id("x-stamped-live"))
	start(unindexed, "x", id("x-unstamped-live"))
	start(unindexed, "y", id("y-unstamped-live"))

	reader := engine.NewTemporalRunReader(temporalClient).WithNamespaceIndex(true)
	wantX := []string{id("x-stamped-done"), id("x-stamped-live"), id("x-unstamped-done"), id("x-unstamped-live")}
	sort.Strings(wantX)

	// Visibility is eventually consistent. The positive control is y's own
	// scoped list: once it shows both of y's runs, every record has landed,
	// and the reads of x below are of a settled index.
	deadline := time.Now().Add(90 * time.Second)
	tick := time.NewTicker(250 * time.Millisecond)
	defer tick.Stop()
	for {
		ys, err := reader.ListRuns(ctx, engine.RunQuery{Namespace: "y", Limit: 10})
		must.OKf(t, err, "ListRuns(y)")
		xs, err := reader.ListRuns(ctx, engine.RunQuery{Namespace: "x", Limit: 10})
		must.OKf(t, err, "ListRuns(x)")
		closed := 0
		for _, run := range append(ys, xs...) {
			if run.GetStatus() != orkstackv1.Status_STATUS_RUNNING {
				closed++
			}
		}
		if len(ys) == 2 && len(xs) == len(wantX) && closed == 3 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("visibility settled to x=%v y=%v (%d closed); want x=%v and both of y's",
				runIDs(xs), runIDs(ys), closed, wantX)
		}
		select {
		case <-ctx.Done():
			t.Fatalf("context ended waiting for visibility: %v", ctx.Err())
		case <-tick.C:
		}
	}

	t.Run("ListRuns", func(t *testing.T) {
		runs, err := reader.ListRuns(ctx, engine.RunQuery{Namespace: "x", Limit: 10})
		must.OKf(t, err, "ListRuns(x)")
		got := runIDs(runs)
		sort.Strings(got)
		if fmt.Sprint(got) != fmt.Sprint(wantX) {
			t.Fatalf("ListRuns(x) = %v, want %v", got, wantX)
		}
		for _, run := range runs {
			if run.GetNamespace() != "x" {
				t.Errorf("ListRuns(x) returned %s of namespace %q", run.GetId(), run.GetNamespace())
			}
		}
	})

	t.Run("Executions", func(t *testing.T) {
		executions, err := reader.Executions(ctx, engine.StatsWindow{
			Namespace: "x", Since: windowStart, Until: time.Now().Add(time.Minute),
		})
		must.OKf(t, err, "Executions(x)")
		if len(executions.Durations) != 2 {
			t.Fatalf("x's sampled durations = %v, want two: the stamped and the unstamped closed run",
				executions.Durations)
		}
		running := runIDs(executions.Running)
		sort.Strings(running)
		if want := []string{id("x-stamped-live"), id("x-unstamped-live")}; fmt.Sprint(running) != fmt.Sprint(want) {
			t.Fatalf("x's running = %v, want %v", running, want)
		}
	})

	t.Run("RunningCount", func(t *testing.T) {
		got, err := reader.RunningCount(ctx, "x", 10)
		if err != nil || got != 2 {
			t.Fatalf("RunningCount(x) = %d, %v; want 2", got, err)
		}
		if got, err := reader.RunningCount(ctx, "y", 10); err != nil || got != 1 {
			t.Fatalf("RunningCount(y) = %d, %v; want 1", got, err)
		}
	})
}
