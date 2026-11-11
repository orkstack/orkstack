package engine_test

import (
	"context"
	"testing"

	"google.golang.org/protobuf/proto"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/engine"
)

// A shared child's planned definition runs in the parent's namespace and
// names its owner (docs/DSL.md "Sharing across namespaces"): the run says
// whose Orkflow it ran, and a cached task's memo key carries the owner.
func TestRunWorkflow_ASharedDefinitionCarriesItsOwner(t *testing.T) {
	h := newEachHarness(t, func(_ context.Context, in *orkstackv1.ExecuteTaskInput) (*orkstackv1.ExecuteTaskResult, error) {
		return plainResult(in), nil
	})
	wf := compile(t, cachedDocument)
	wf.Namespace, wf.DefinitionNamespace = "search", "platform"
	plan := cachePlan(false)
	plan.CommitRoot = "mem://commits"
	h.env.ExecuteWorkflow(engine.RunWorkflow, "run-shared", wf, payload(t, nil), plan, (*orkstackv1.RunContinuation)(nil))
	if !h.env.IsWorkflowCompleted() {
		t.Fatal("workflow did not complete")
	}
	run, ok := h.store.Get("run-shared")
	if !ok {
		t.Fatal("run read model was not populated")
	}
	if run.GetNamespace() != "search" || run.GetOrkflowNamespace() != "platform" {
		t.Fatalf("run namespace %q, orkflow namespace %q; want search and platform", run.GetNamespace(), run.GetOrkflowNamespace())
	}
	for _, id := range []string{"featurize", "train"} {
		if got := h.itemCalls(id)[0].GetCache().GetOrkflowNamespace(); got != "platform" {
			t.Fatalf("%s cache context names owner %q, want platform", id, got)
		}
		if got := h.itemCalls(id)[0].GetNamespace(); got != "search" {
			t.Fatalf("%s runs in %q, want the parent's namespace", id, got)
		}
	}
	if h.itemCalls("stamp")[0].GetCache() != nil {
		t.Fatal("an uncached task of a shared definition got a cache context")
	}
}

func TestRunWorkflow_AnOwnDefinitionNamesNoOwner(t *testing.T) {
	h := newEachHarness(t, func(_ context.Context, in *orkstackv1.ExecuteTaskInput) (*orkstackv1.ExecuteTaskResult, error) {
		return plainResult(in), nil
	})
	run := runCached(t, h, "run-own", cachedDocument, cachePlan(false))
	if run.GetOrkflowNamespace() != "" || h.itemCalls("featurize")[0].GetCache().GetOrkflowNamespace() != "" {
		t.Fatalf("an own definition names an owner: run %q, cache %v", run.GetOrkflowNamespace(), h.itemCalls("featurize")[0].GetCache())
	}
}

func TestRunStartMemo_NamesASharedDefinitionsOwnerOnly(t *testing.T) {
	own := engine.RunStartMemo(&orkstackv1.Orkflow{Name: "deploy", Namespace: "search"}, engine.RunPlan{})
	if _, ok := own[engine.MemoOrkflowOwner]; ok {
		t.Fatalf("an own definition's memo names an owner: %v", own)
	}
	shared := engine.RunStartMemo(&orkstackv1.Orkflow{Name: "deploy", Namespace: "search", DefinitionNamespace: "platform"}, engine.RunPlan{})
	if shared[engine.MemoOrkflowOwner] != "platform" || shared[engine.MemoNamespace] != "search" {
		t.Fatalf("a shared definition's memo = %v", shared)
	}
}

// TestRunWorkflow_ACompositionRowNamesItsChildsOwner: a parent's row for a
// child of another namespace names that namespace, both for a qualified ref
// and for an unqualified one inside a shared definition.
func TestRunWorkflow_ACompositionRowNamesItsChildsOwner(t *testing.T) {
	parent := &orkstackv1.Orkflow{Name: "release", Version: "1", Namespace: "search", Tasks: []*orkstackv1.Task{
		{Id: "qualified", Uses: "orkflow:platform/deploy@1", Orkflow: &orkstackv1.OrkflowRef{Namespace: "platform", Name: "deploy", Version: "1"}},
		{Id: "own", Uses: "orkflow:helper@1", Orkflow: &orkstackv1.OrkflowRef{Name: "helper", Version: "1"}},
	}}
	rows := engine.CompositionRowsForTest(t, parent)
	if rows["qualified"].GetOrkflowNamespace() != "platform" || rows["own"].GetOrkflowNamespace() != "" {
		t.Fatalf("rows of an own definition = %v", rows)
	}
	shared := proto.Clone(parent).(*orkstackv1.Orkflow)
	shared.DefinitionNamespace = "platform"
	rows = engine.CompositionRowsForTest(t, shared)
	if rows["own"].GetOrkflowNamespace() != "platform" {
		t.Fatalf("an unqualified ref inside a shared definition names owner %q, want platform", rows["own"].GetOrkflowNamespace())
	}
}
