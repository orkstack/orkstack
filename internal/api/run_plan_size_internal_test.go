package api

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"connectrpc.com/connect"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/engine"
)

// TestCheckRunPlanSize_RefusesAPlanOverTheBoundWithoutChildren covers the run
// that composes nothing: mintChildren never measures its plan, so this check
// is the only one, and it must refuse past engine.MaxPlanBytes.
func TestCheckRunPlanSize_RefusesAPlanOverTheBoundWithoutChildren(t *testing.T) {
	small := engine.RunPlan{Namespace: "default"}
	if err := checkRunPlanSize(small); err != nil {
		t.Fatalf("a small plan was refused: %v", err)
	}
	big := engine.RunPlan{CommitRoot: strings.Repeat("r", engine.MaxPlanBytes+1)}
	err := checkRunPlanSize(big)
	if connect.CodeOf(err) != connect.CodeFailedPrecondition || !strings.Contains(err.Error(), "the run's plan encodes to") {
		t.Fatalf("checkRunPlanSize(big) = %v, want FailedPrecondition naming the plan", err)
	}
}

// TestPlanSize_RunTreeLimitsCostOncePerTree: the run-tree limits are carried
// once per tree, on the top-level plan, so a composed parent whose plan
// already measured 90% of the 256 KiB plan bound — the largest number of
// composing tasks that fits — still passes mintChildren and checkRunPlanSize,
// and the encoded plan grows by the same few hundred bytes whatever the
// number of composed tasks.
func TestPlanSize_RunTreeLimitsCostOncePerTree(t *testing.T) {
	leaf := &orkstackv1.Orkflow{
		Namespace: "default", Name: "leaf", Version: "1.0.0",
		DefinitionDigest: "sha256:" + strings.Repeat("1e", 32),
		Tasks: []*orkstackv1.Task{{Id: "work", Uses: "shell", Image: &orkstackv1.ImageRef{
			Reference: "ghcr.io/acme/work@sha256:" + strings.Repeat("ab", 32)}}},
	}
	svc := NewService(Config{})
	childPlan := engine.RunPlan{Namespace: "default", Routes: map[string]string{"main": "ork-tasks"}}
	if err := svc.pinDurabilityPlan(&childPlan); err != nil {
		t.Fatal(err)
	}
	child, err := engine.NewChildRunPlan(leaf, childPlan, true, true)
	if err != nil {
		t.Fatal(err)
	}
	child.RunID = strings.Repeat("0", engine.MaxRunIDLength)
	perChild, err := json.Marshal(child)
	if err != nil {
		t.Fatal(err)
	}
	// tree is a parent composing n leaves, planned as prepare plans it.
	tree := func(n int) (*orkstackv1.Orkflow, engine.RunPlan) {
		parent := &orkstackv1.Orkflow{Namespace: "default", Name: "parent", Version: "1.0.0"}
		plan := engine.RunPlan{Namespace: "default", Routes: map[string]string{"main": "ork-tasks"},
			Children: map[string]*engine.ChildRunPlan{}}
		if err := svc.pinDurabilityPlan(&plan); err != nil {
			t.Fatal(err)
		}
		for i := 0; i < n; i++ {
			id := fmt.Sprintf("c%04d", i)
			parent.Tasks = append(parent.Tasks, &orkstackv1.Task{Id: id, Orkflow: &orkstackv1.OrkflowRef{
				Name: "leaf", Version: "1.0.0", DefinitionDigest: leaf.GetDefinitionDigest()}})
			copied := *child
			plan.Children[id] = &copied
		}
		return parent, plan
	}
	size := func(plan engine.RunPlan) int {
		encoded, err := json.Marshal(plan)
		if err != nil {
			t.Fatal(err)
		}
		return len(encoded)
	}
	// The largest tree that measures at most 90% of the bound before.
	largest := (engine.MaxPlanBytes * 9 / 10) / len(perChild)
	for {
		_, plan := tree(largest + 1)
		if size(plan) > engine.MaxPlanBytes*9/10 {
			break
		}
		largest++
	}
	for {
		_, plan := tree(largest)
		if size(plan) <= engine.MaxPlanBytes*9/10 {
			break
		}
		largest--
	}
	_, largestPlan := tree(largest)
	largestBefore := size(largestPlan)
	growth := map[int]int{}
	for _, n := range []int{1, largest / 2, largest} {
		parent, plan := tree(n)
		before := size(plan)
		if n == largest && (before > engine.MaxPlanBytes*9/10 || before < engine.MaxPlanBytes*85/100) {
			t.Fatalf("the largest tree measures %d bytes before, want about 90%% of %d", before, engine.MaxPlanBytes)
		}
		svc.pinRunTreeLimits(&plan)
		after := size(plan)
		growth[n] = after - before
		if err := svc.mintChildren(parent, &plan); err != nil {
			t.Fatalf("%d composed tasks: mintChildren refused the plan: %v", n, err)
		}
		if err := checkRunPlanSize(plan); err != nil {
			t.Fatalf("%d composed tasks: checkRunPlanSize refused the plan: %v", n, err)
		}
		for id, cp := range plan.Children {
			if cp.Plan.History != nil {
				t.Fatalf("child %s carries the run-tree limits", id)
			}
		}
	}
	for n, g := range growth {
		if g >= 1024 || g != growth[1] {
			t.Fatalf("the run-tree limits grew a %d-task plan by %d bytes, want the same < 1 KiB for every tree: %v", n, g, growth)
		}
	}
	t.Logf("E0 plan growth: the run-tree limits add %d bytes to the top-level plan, at 1, %d and %d composed tasks "+
		"(the largest measured %d bytes before, of %d)", growth[1], largest/2, largest, largestBefore, engine.MaxPlanBytes)
}
