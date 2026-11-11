package engine

import (
	"testing"

	"google.golang.org/protobuf/proto"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// TestDecideSharedOwner_TheGateIsWhatNamesTheOwner: with the gate's
// DefaultVersion branch taken, a shared definition's run names no owner, and
// a run of an own definition never asks the gate (a nil context would panic).
func TestDecideSharedOwner_TheGateIsWhatNamesTheOwner(t *testing.T) {
	own := &runState{wf: &orkstackv1.Orkflow{Namespace: "search"}}
	own.decideSharedOwner(nil)
	if own.sharedOwner != "" {
		t.Fatalf("an own definition named owner %q", own.sharedOwner)
	}
	crossNamespaceGateDisabledForTest.Store(true)
	defer crossNamespaceGateDisabledForTest.Store(false)
	shared := &runState{wf: &orkstackv1.Orkflow{Namespace: "search", DefinitionNamespace: "platform"}}
	shared.decideSharedOwner(nil)
	if shared.sharedOwner != "" {
		t.Fatalf("the DefaultVersion branch named owner %q", shared.sharedOwner)
	}
}

// TestCarriedSharedOwner_ReadsTheCarriedDecisionOnly: a generation that
// cannot restore names the owner only when the switching generation decided
// it, so its terminal event says whose Orkflow the run ran.
func TestCarriedSharedOwner_ReadsTheCarriedDecisionOnly(t *testing.T) {
	shared := &orkstackv1.Orkflow{Namespace: "search", DefinitionNamespace: "platform"}
	decided := &orkstackv1.RunContinuation{VersionDecisions: map[string]int32{CrossNamespaceChange: 1}}
	if got := carriedSharedOwner(shared, decided); got != "platform" {
		t.Fatalf("owner = %q, want platform", got)
	}
	if got := carriedSharedOwner(shared, &orkstackv1.RunContinuation{}); got != "" {
		t.Fatalf("an undecided carry named owner %q", got)
	}
	if got := carriedSharedOwner(&orkstackv1.Orkflow{Namespace: "search"}, decided); got != "" {
		t.Fatalf("an own definition named owner %q", got)
	}
}

// TestOrkflowKey_KeysASharedChildByOwnerAndName: wherever runs are indexed
// or grouped, a shared child's runs stand apart from the running
// namespace's own Orkflow of the same name.
func TestOrkflowKey_KeysASharedChildByOwnerAndName(t *testing.T) {
	own := &orkstackv1.Run{Namespace: "search", OrkflowName: "deploy"}
	shared := &orkstackv1.Run{Namespace: "search", OrkflowName: "deploy", OrkflowNamespace: "platform"}
	if OrkflowKey(own) != "deploy" || OrkflowKey(shared) != "platform/deploy" {
		t.Fatalf("keys = %q, %q", OrkflowKey(own), OrkflowKey(shared))
	}
	for key, want := range map[string][2]string{"deploy": {"", "deploy"}, "platform/deploy": {"platform", "deploy"}} {
		if ns, name := SplitOrkflowKey(key); ns != want[0] || name != want[1] {
			t.Fatalf("SplitOrkflowKey(%q) = %q, %q", key, ns, name)
		}
	}
	projected := indexProjection(artifacts.RunIndexEntry{RunID: "r", Namespace: "search", Orkflow: "platform/deploy", Status: "failed"})
	if projected.GetOrkflowName() != "deploy" || projected.GetOrkflowNamespace() != "platform" {
		t.Fatalf("index projection = %v", projected)
	}
}

func TestGroupers_KeepASharedChildApartFromTheOwnOrkflowOfItsName(t *testing.T) {
	own := &orkstackv1.Run{Id: "r1", Namespace: "search", OrkflowName: "deploy", Status: orkstackv1.Status_STATUS_FAILED,
		Error:    &orkstackv1.Error{Code: "X"},
		TaskRuns: []*orkstackv1.TaskRun{{TaskId: "t", Cache: &orkstackv1.CacheResult{Outcome: orkstackv1.CacheOutcome_CACHE_OUTCOME_MISS}}}}
	shared := proto.Clone(own).(*orkstackv1.Run)
	shared.Id, shared.OrkflowNamespace = "r2", "platform"

	byOrkflow := map[orkstackv1.FailureGroupBy]bool{orkstackv1.FailureGroupBy_FAILURE_GROUP_BY_ORKFLOW: true}
	ownKey := keyOf(FingerprintOf(artifacts.RunIndexEntry{RunID: "r1"}, own, nil, false), byOrkflow)
	sharedKey := keyOf(FingerprintOf(artifacts.RunIndexEntry{RunID: "r2"}, shared, nil, false), byOrkflow)
	if ownKey == sharedKey {
		t.Fatal("failure grouping keys a shared child with the own Orkflow of its name")
	}
	groups, _ := GroupFailures([]FailureRecord{
		FingerprintOf(artifacts.RunIndexEntry{RunID: "r1"}, own, nil, false),
		FingerprintOf(artifacts.RunIndexEntry{RunID: "r2"}, shared, nil, false),
	}, []orkstackv1.FailureGroupBy{orkstackv1.FailureGroupBy_FAILURE_GROUP_BY_ORKFLOW}, 0, 0)
	owners := map[string]bool{}
	for _, g := range groups {
		if g.GetOrkflowName() != "deploy" {
			t.Fatalf("group names %q", g.GetOrkflowName())
		}
		owners[g.GetOrkflowNamespace()] = true
	}
	if len(groups) != 2 || !owners[""] || !owners["platform"] {
		t.Fatalf("failure groups = %v", groups)
	}

	if s := SampleTask(artifacts.RunIndexEntry{RunID: "r2"}, shared, shared.GetTaskRuns()[0], nil); s.Orkflow != "platform/deploy" {
		t.Fatalf("flake sample keys %q", s.Orkflow)
	}

	rows := AggregateCacheUsage([]*orkstackv1.Run{own, shared})
	if len(rows) != 2 || rows[0].GetOrkflowNamespace() == rows[1].GetOrkflowNamespace() {
		t.Fatalf("cache usage rows = %v", rows)
	}
}
