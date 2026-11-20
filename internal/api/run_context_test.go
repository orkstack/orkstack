package api

import (
	"bytes"
	"strings"
	"testing"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/authz"
	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

func childPlan(t *testing.T, name, runID string, plan engine.RunPlan) *engine.ChildRunPlan {
	t.Helper()
	cp, err := engine.NewChildRunPlan(&orkstackv1.Orkflow{Name: name, Version: "1", DefinitionDigest: "sha256:" + strings.Repeat("c", 64)},
		plan, false, false)
	if err != nil {
		t.Fatal(err)
	}
	cp.RunID = runID
	return cp
}

// runContextTree is a root with children "b" and "a" (declared out of order),
// "a" composing "x", and "b" an Orkflow another namespace shared.
func runContextTree(t *testing.T) (*orkstackv1.Orkflow, engine.RunPlan) {
	t.Helper()
	grandchild := childPlan(t, "leaf", "ork-child-x", engine.RunPlan{})
	a := childPlan(t, "mid", "ork-child-a", engine.RunPlan{Children: map[string]*engine.ChildRunPlan{"x": grandchild}})
	b := childPlan(t, "shared", "ork-child-b", engine.RunPlan{Shared: &authz.Decision{}})
	root := &orkstackv1.Orkflow{Name: "load", Version: "7", Namespace: "team", DefinitionDigest: "sha256:" + strings.Repeat("a", 64)}
	plan := engine.RunPlan{
		Origin: &engine.RunOrigin{Kind: orkstackv1.RunOriginKind_RUN_ORIGIN_KIND_TRIGGER.String(), ID: "trg-1",
			Reference: "https://ci/4411", Description: "nightly", Principal: "ci-bot"},
		Children: map[string]*engine.ChildRunPlan{"b": b, "a": a},
	}
	return root, plan
}

func TestRunContextObject_TreeIndexIsDeterministic(t *testing.T) {
	root, plan := runContextTree(t)
	raw, err := planRunContext(root, &plan)
	if err != nil {
		t.Fatal(err)
	}
	a, b := plan.Children["a"], plan.Children["b"]
	x := a.Plan.Children["x"]
	// Pre-order, task ids in order: root 0, a 1, a/x 2, b 3.
	if plan.RunContextIndex != 0 || a.Plan.RunContextIndex != 1 || x.Plan.RunContextIndex != 2 || b.Plan.RunContextIndex != 3 {
		t.Fatalf("indexes root %d a %d x %d b %d", plan.RunContextIndex, a.Plan.RunContextIndex, x.Plan.RunContextIndex, b.Plan.RunContextIndex)
	}
	obj, err := artifacts.DecodeRunContextObject(raw)
	if err != nil {
		t.Fatal(err)
	}
	if len(obj.Entries) != 4 {
		t.Fatalf("%d entries", len(obj.Entries))
	}
	if e := obj.Entries[2]; *e.Parent != 1 || e.ParentTaskID != "x" || e.Orkflow.Name != "leaf" {
		t.Fatalf("entry 2 = %+v", e)
	}
	if e := obj.Entries[3]; *e.Parent != 0 || e.ParentTaskID != "b" || !e.Shared {
		t.Fatalf("entry 3 = %+v", e)
	}
	if plan.RunContextSHA256 != artifacts.ContinuationSHA256(raw) || plan.RunContextMaxBytes != engine.DefaultRunContextMaxBytes {
		t.Fatalf("plan's run context = %q, %d", plan.RunContextSHA256, plan.RunContextMaxBytes)
	}
	// The same tree builds the same bytes, however often.
	root2, plan2 := runContextTree(t)
	again, err := planRunContext(root2, &plan2)
	if err != nil || !bytes.Equal(again, raw) {
		t.Fatalf("a second build differs: %v", err)
	}
}

func TestRunContextObject_IdFreeAndStableAcrossReissue(t *testing.T) {
	root, plan := runContextTree(t)
	raw, err := planRunContext(root, &plan)
	if err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{"ork-child-a", "ork-child-b", "ork-child-x"} {
		if bytes.Contains(raw, []byte(id)) {
			t.Fatalf("the object names the tree's run %s", id)
		}
	}
	// A reissue mints new ids; the object is the same.
	root2, plan2 := runContextTree(t)
	plan2.Children["a"].RunID = "ork-other"
	again, err := planRunContext(root2, &plan2)
	if err != nil || !bytes.Equal(again, raw) {
		t.Fatalf("other run ids changed the object: %v", err)
	}
}

func TestRunContextObject_SharedChildOmitsRootPrincipalAndReference(t *testing.T) {
	root, plan := runContextTree(t)
	raw, err := planRunContext(root, &plan)
	if err != nil {
		t.Fatal(err)
	}
	obj, err := artifacts.DecodeRunContextObject(raw)
	if err != nil {
		t.Fatal(err)
	}
	shared := obj.Root.OriginShared
	if shared == nil || shared.Principal != "" || shared.Reference != "" || shared.ID != "trg-1" || shared.Description != "nightly" {
		t.Fatalf("shared origin = %+v", shared)
	}
	if obj.Root.Origin.Principal != "ci-bot" || obj.Root.Origin.Reference != "https://ci/4411" {
		t.Fatalf("root origin = %+v", obj.Root.Origin)
	}
	// A tree with no shared child carries no shared origin.
	root2, plan2 := runContextTree(t)
	delete(plan2.Children, "b")
	raw2, err := planRunContext(root2, &plan2)
	if err != nil {
		t.Fatal(err)
	}
	if obj2, err := artifacts.DecodeRunContextObject(raw2); err != nil || obj2.Root.OriginShared != nil {
		t.Fatalf("unshared tree: %+v, %v", obj2.Root.OriginShared, err)
	}
}
