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

	"go.temporal.io/api/workflowservice/v1"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/internal/task"
)

// TestIntegration_LiveOriginFilterUsesTheIndex lists live runs by origin
// against a real cluster.
//
// Three runs start with the origin attributes stamped — two from trigger
// trg-a, one from a submit — and one more starts through a starter told the
// cluster has no origin index, so it carries the memo and no attribute. A
// kind+id list through an indexed reader returns exactly the stamped run of
// trg-a that matches, and never the memo-only run: the cluster applied the
// predicate. The same list through a reader without the index walks the
// memo instead and still answers, now including the memo-only run.
func TestIntegration_LiveOriginFilterUsesTheIndex(t *testing.T) {
	address := temporalAddress()
	conn, err := net.DialTimeout("tcp", address, 2*time.Second)
	if err != nil {
		infraAbsent(t, "temporal is not reachable at %s: %v", address, err)
	}
	_ = conn.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	temporalNamespace := fmt.Sprintf("%s-origin-%d-%d", baseNamespace(), os.Getpid(),
		time.Now().UnixNano()%1_000_000_000)
	temporalClient := dialRoutingCluster(t, ctx, address, temporalNamespace)

	namespaceIndexed, err := task.EnsureRunSearchAttributes(ctx, temporalClient, temporalNamespace)
	if err != nil || !namespaceIndexed {
		t.Fatalf("provision the namespace index: %v (indexed=%v)", err, namespaceIndexed)
	}
	originIndexed, err := task.EnsureRunOriginSearchAttributes(ctx, temporalClient, temporalNamespace)
	if err != nil || !originIndexed {
		t.Fatalf("provision the origin index: %v (indexed=%v)", err, originIndexed)
	}
	t.Logf("EVIDENCE run_namespace_index=%v run_origin_index=%v", namespaceIndexed, originIndexed)

	stamped := engine.NewTemporalStarter(temporalClient, task.DefaultQueue).WithNamespaceIndex(true).WithOriginIndex(true)
	memoOnly := engine.NewTemporalStarter(temporalClient, task.DefaultQueue).WithNamespaceIndex(true)
	start := func(starter *engine.TemporalStarter, runID string, origin *engine.RunOrigin) {
		t.Helper()
		wf := &orkstackv1.Orkflow{
			Name: "deploy-service", Version: "0.1.0", Namespace: "alpha",
			Tasks: []*orkstackv1.Task{{Id: "settle", Uses: "shell", Pool: task.DefaultPool}},
		}
		plan := engine.RunPlan{
			Namespace: "alpha",
			Routes:    map[string]string{task.DefaultPool: task.DefaultQueue},
			Origin:    origin,
		}
		if err := starter.StartRun(ctx, runID, wf, nil, plan); err != nil {
			t.Fatalf("StartRun(%s): %v", runID, err)
		}
	}
	stamp := time.Now().UnixNano()
	triggerA := &engine.RunOrigin{Kind: "RUN_ORIGIN_KIND_TRIGGER", ID: "trg-a"}
	wantA := fmt.Sprintf("ork-origin-a-%d", stamp)
	start(stamped, wantA, triggerA)
	start(stamped, fmt.Sprintf("ork-origin-b-%d", stamp), &engine.RunOrigin{Kind: "RUN_ORIGIN_KIND_TRIGGER", ID: "trg-b"})
	start(stamped, fmt.Sprintf("ork-origin-s-%d", stamp), &engine.RunOrigin{Kind: "RUN_ORIGIN_KIND_SUBMIT"})
	unindexed := fmt.Sprintf("ork-origin-memo-%d", stamp)
	start(memoOnly, unindexed, triggerA)

	// The predicate the reader pushes down, asked of the cluster directly: it
	// is valid against the registered attributes and selects the stamped run.
	query := fmt.Sprintf(`WorkflowType = %q AND %s = "alpha" AND %s = "trigger" AND %s = "trg-a"`,
		engine.RunWorkflowType, task.SearchAttrOrkflowNamespace,
		task.SearchAttrOrkflowOriginKind, task.SearchAttrOrkflowOriginId)
	deadline := time.Now().Add(90 * time.Second)
	for {
		response, err := temporalClient.ListWorkflow(ctx, &workflowservice.ListWorkflowExecutionsRequest{Query: query})
		if err != nil {
			t.Fatalf("ListWorkflow(%s): %v", query, err)
		}
		all, err := engine.NewTemporalRunReader(temporalClient).WithNamespaceIndex(true).
			ListRuns(ctx, engine.RunQuery{Namespace: "alpha", Limit: 10})
		if err != nil {
			t.Fatalf("ListRuns(alpha): %v", err)
		}
		if len(response.GetExecutions()) == 1 && len(all) == 4 {
			t.Logf("EVIDENCE query=%s executions=%d", query, len(response.GetExecutions()))
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("visibility indexed %d executions for %s and %d runs in alpha; want 1 and 4",
				len(response.GetExecutions()), query, len(all))
		}
		time.Sleep(time.Second)
	}

	indexedReader := engine.NewTemporalRunReader(temporalClient).WithNamespaceIndex(true).WithOriginIndex(true)
	runs, err := indexedReader.ListRuns(ctx, engine.RunQuery{
		Namespace: "alpha", OriginKind: orkstackv1.RunOriginKind_RUN_ORIGIN_KIND_TRIGGER, OriginID: "trg-a", Limit: 10,
	})
	if err != nil {
		t.Fatalf("indexed ListRuns(trigger:trg-a): %v", err)
	}
	if got := runIDs(runs); len(got) != 1 || got[0] != wantA {
		t.Fatalf("indexed trigger:trg-a = %v, want only %s: the cluster applied the predicate, "+
			"so the memo-only run %s is not selected", got, wantA, unindexed)
	}
	if origin := runs[0].GetOrigin(); origin.GetKind() != orkstackv1.RunOriginKind_RUN_ORIGIN_KIND_TRIGGER || origin.GetId() != "trg-a" {
		t.Fatalf("the listed row carries origin %v, want trigger trg-a from the memo", origin)
	}

	walkingReader := engine.NewTemporalRunReader(temporalClient).WithNamespaceIndex(true)
	runs, err = walkingReader.ListRuns(ctx, engine.RunQuery{
		Namespace: "alpha", OriginKind: orkstackv1.RunOriginKind_RUN_ORIGIN_KIND_TRIGGER, OriginID: "trg-a", Limit: 10,
	})
	if err != nil {
		t.Fatalf("walking ListRuns(trigger:trg-a): %v", err)
	}
	got := runIDs(runs)
	sort.Strings(got)
	want := []string{wantA, unindexed}
	sort.Strings(want)
	if len(got) != 2 || got[0] != want[0] || got[1] != want[1] {
		t.Fatalf("walking trigger:trg-a = %v, want %v: without the index the memo decides", got, want)
	}
	t.Logf("EVIDENCE indexed=[%s] walked=%v", wantA, got)
}
