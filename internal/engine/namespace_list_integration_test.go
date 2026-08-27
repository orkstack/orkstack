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

// TestIntegration_AQuietNamespaceIsListedBesideABusyOne is the tenancy half of
// ListRuns, against a real cluster.
//
// Two OrkStack namespaces on one deployment: `alpha` with a single live run,
// `bravo` with more live runs than a scoped list is allowed to page through.
// Filtered after the fact, the walk spends its whole budget on bravo's records
// and reports that alpha has nothing — one tenant's traffic deciding another
// tenant's visibility. Pushed into the query, the cluster returns alpha's run
// and nothing else, whatever bravo is doing.
//
// It runs in a Temporal namespace of its own (AGENTS.md §6), and it starts
// workflows with no worker anywhere: what is under test is the visibility
// index, and a run's record exists from the moment it starts.
func TestIntegration_AQuietNamespaceIsListedBesideABusyOne(t *testing.T) {
	address := temporalAddress()
	conn, err := net.DialTimeout("tcp", address, 2*time.Second)
	if err != nil {
		infraAbsent(t, "temporal is not reachable at %s: %v", address, err)
	}
	_ = conn.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	temporalNamespace := fmt.Sprintf("%s-nslist-%d-%d", baseNamespace(), os.Getpid(),
		time.Now().UnixNano()%1_000_000_000)
	temporalClient := dialRoutingCluster(t, ctx, address, temporalNamespace)

	// The engine's own provisioning step, run exactly as `ork` runs it at boot.
	indexed, err := task.EnsureRunSearchAttributes(ctx, temporalClient, temporalNamespace)
	if err != nil {
		t.Fatalf("provision the run index in %q: %v", temporalNamespace, err)
	}
	if !indexed {
		t.Fatalf("EnsureRunSearchAttributes reported no error and no index")
	}

	starter := engine.NewTemporalStarter(temporalClient, task.DefaultQueue).
		WithNamespaceIndex(true)
	start := func(namespace, runID string) {
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
		if err := starter.StartRun(ctx, runID, wf, nil, plan); err != nil {
			t.Fatalf("StartRun(%s in %s): %v", runID, namespace, err)
		}
	}

	// The quiet tenant goes first, so every one of the neighbour's runs is
	// newer than it: visibility answers newest first, so alpha's run sits
	// behind all of them.
	const limit = 5
	quiet := fmt.Sprintf("ork-alpha-%d", time.Now().UnixNano())
	start("alpha", quiet)

	// One more than the whole bounded walk — the exact shape that reported
	// zero. The pages are the reader's (maxRunListPages), the page size the
	// caller's limit.
	busy := 20*limit + 5
	for i := 0; i < busy; i++ {
		start("bravo", fmt.Sprintf("ork-bravo-%d-%d", time.Now().UnixNano(), i))
	}

	reader := engine.NewTemporalRunReader(temporalClient).WithNamespaceIndex(true)

	// Visibility is eventually consistent, so wait for the neighbour's records
	// to land: until they do, the defect this test is about cannot be present.
	deadline := time.Now().Add(90 * time.Second)
	for {
		listed, err := reader.ListRuns(ctx, engine.RunQuery{Namespace: "bravo", Limit: busy})
		if err != nil {
			t.Fatalf("ListRuns(bravo): %v", err)
		}
		if len(listed) >= busy {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("visibility indexed %d of bravo's %d runs; the crowd this test needs "+
				"never arrived", len(listed), busy)
		}
		time.Sleep(time.Second)
	}

	runs, err := reader.ListRuns(ctx, engine.RunQuery{Namespace: "alpha", Limit: limit})
	if err != nil {
		t.Fatalf("ListRuns(alpha): %v", err)
	}
	if len(runs) != 1 || runs[0].GetId() != quiet {
		t.Fatalf("ListRuns(alpha) = %v, want the one run %s: %d live runs in namespace "+
			"bravo must not decide whether alpha can see its own",
			runIDs(runs), quiet, busy)
	}
	if got := runs[0].GetNamespace(); got != "alpha" {
		t.Errorf("the listed run reports namespace %q, want alpha", got)
	}

	// And the boundary still holds the other way: nothing of bravo's is shown.
	for _, run := range runs {
		if run.GetNamespace() == "bravo" {
			t.Errorf("listing alpha returned %s, a run of namespace bravo", run.GetId())
		}
	}
}

// baseNamespace is the operator's ORK_TEMPORAL_NAMESPACE, or the built-in
// default, used as the prefix of a per-run namespace so the choice is visible
// in `temporal workflow list`.
func baseNamespace() string {
	if base := os.Getenv("ORK_TEMPORAL_NAMESPACE"); base != "" {
		return base
	}
	return task.DefaultNamespace
}

func runIDs(runs []*orkstackv1.Run) []string {
	ids := make([]string, 0, len(runs))
	for _, run := range runs {
		ids = append(ids, run.GetId())
	}
	return ids
}
