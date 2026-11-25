package orker

import (
	"reflect"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
)

// lineageRun is a run as its record says it, of the tree
// testRunContextTree describes: the root (no parent), the shared child
// "publish" started, or a run below that one.
func lineageRun(origin *orkstackv1.RunOrigin, orkflow *orkstackv1.OrkflowRef) *orkstackv1.Run {
	return &orkstackv1.Run{Id: "ork-recorded", Origin: origin, OrkflowName: orkflow.GetName(),
		OrkflowVersion: orkflow.GetVersion(), OrkflowDefinitionDigest: orkflow.GetDefinitionDigest(),
		OrkflowNamespace: orkflow.GetNamespace()}
}

func recordedRootOrigin() *orkstackv1.RunOrigin {
	return &orkstackv1.RunOrigin{Kind: orkstackv1.RunOriginKind_RUN_ORIGIN_KIND_TRIGGER, Id: "trg-1",
		Reference: "https://ci/4411", Description: "nightly", Principal: "ci-bot", PrincipalVerified: true}
}

func recordedRootOrkflow() *orkstackv1.OrkflowRef {
	tree := testRunContextTree()
	return &orkstackv1.OrkflowRef{Name: tree.Root.Orkflow.Name, Version: tree.Root.Orkflow.Version,
		DefinitionDigest: tree.Root.Orkflow.DefinitionDigest}
}

// A local run of a recorded run's task reads what that run's own task read:
// the recorded run's lineage is the lineage its run-context object gives, for
// a top-level run and for children alike, and the local run's own start and
// attempt count stand where an attempt's do.
func TestLineageOfRun_AgreesWithTheObject(t *testing.T) {
	start := time.Date(2026, 11, 11, 5, 0, 3, 0, time.UTC)
	childOf := func(parentOrkflow *orkstackv1.OrkflowRef, parent, task, root string) *orkstackv1.RunOrigin {
		return &orkstackv1.RunOrigin{Kind: orkstackv1.RunOriginKind_RUN_ORIGIN_KIND_PARENT_RUN, Id: parent,
			RootRunId: root, ParentTaskId: task, ParentOrkflow: parentOrkflow,
			RootOrkflow: recordedRootOrkflow(), RootOrigin: recordedRootOrigin()}
	}
	unshared := testRunContextTree()
	unshared.Root.Origin.PrincipalVerified = true
	unshared.Root.OriginShared = nil
	unshared.Entries = unshared.Entries[:2]
	unshared.Entries[1].Shared = false
	shared := testRunContextTree()
	shared.Root.Origin.PrincipalVerified = true
	pub := &orkstackv1.OrkflowRef{Name: "pub", Version: "2"}
	sharedPub := &orkstackv1.OrkflowRef{Name: "pub", Version: "2", Namespace: "platform"}

	for name, tc := range map[string]struct {
		run   *orkstackv1.Run
		index int32
		tree  int // 0 unshared, 1 shared
	}{
		"a top-level run":                    {lineageRun(recordedRootOrigin(), recordedRootOrkflow()), 0, 0},
		"a child":                            {lineageRun(childOf(recordedRootOrkflow(), "ork-root", "publish", "ork-root"), pub), 1, 0},
		"a child of a shared Orkflow":        {lineageRun(childOf(recordedRootOrkflow(), "ork-root", "publish", "ork-root"), sharedPub), 1, 1},
		"a run below a shared Orkflow":       {lineageRun(childOf(sharedPub, "ork-mid", "leaf", "ork-root"), &orkstackv1.OrkflowRef{Name: "leafy", Version: "1"}), 2, 1},
		"a run whose parent is not the root": {lineageRun(childOf(pub, "ork-mid", "leaf", "ork-root"), &orkstackv1.OrkflowRef{Name: "leafy", Version: "1"}), 2, 1},
	} {
		t.Run(name, func(t *testing.T) {
			lineage, ok := LineageOfRun(tc.run)
			if !ok {
				t.Fatal("the recorded run says no lineage")
			}
			parent, root := tc.run.GetOrigin().GetId(), tc.run.GetOrigin().GetRootRunId()
			if tc.index == 0 {
				parent, root = "", ""
			}
			in := testRunContextInput(tc.index, parent, root)
			in.RunId = tc.run.GetId()
			tree := unshared
			if tc.tree == 1 {
				tree = shared
			}
			wantRun, wantTask, f := runContextTaskJSON(in, objectLineage(in, &tree), false, 1, start)
			if f != nil {
				t.Fatal(f)
			}
			// The local run has its own id; the lineage is the recorded run's.
			local := proto.Clone(in).(*orkstackv1.ExecuteTaskInput)
			local.RunId = "local-0123456789abcdef"
			gotRun, gotTask, f := runContextTaskJSON(local, fixedLineage(lineage), false, 1, start)
			if f != nil {
				t.Fatal(f)
			}
			if !reflect.DeepEqual(gotRun, wantRun) || gotTask != wantTask {
				t.Fatalf("run\n got %+v\nwant %+v\ntask\n got %+v\nwant %+v", gotRun, wantRun, gotTask, wantTask)
			}
			if tc.tree == 1 && (gotRun.Root.Origin.Principal != "" || gotRun.Root.Origin.Reference != "" || gotRun.Root.Origin.PrincipalVerified) {
				t.Fatalf("a run inside a shared Orkflow reads the root's %+v", gotRun.Root.Origin)
			}
		})
	}
}

// A record that does not say the whole lineage says none: no key is guessed.
func TestLineageOfRun_AnIncompleteRecordSaysNone(t *testing.T) {
	whole := func() *orkstackv1.Run {
		return lineageRun(&orkstackv1.RunOrigin{Kind: orkstackv1.RunOriginKind_RUN_ORIGIN_KIND_PARENT_RUN, Id: "ork-root",
			RootRunId: "ork-root", ParentTaskId: "publish", ParentOrkflow: recordedRootOrkflow(),
			RootOrkflow: recordedRootOrkflow(), RootOrigin: recordedRootOrigin()}, &orkstackv1.OrkflowRef{Name: "pub"})
	}
	if _, ok := LineageOfRun(whole()); !ok {
		t.Fatal("the positive control says no lineage")
	}
	for name, drop := range map[string]func(*orkstackv1.Run){
		"no run id":         func(r *orkstackv1.Run) { r.Id = "" },
		"no parent id":      func(r *orkstackv1.Run) { r.Origin.Id = "" },
		"no root id":        func(r *orkstackv1.Run) { r.Origin.RootRunId = "" },
		"no parent task":    func(r *orkstackv1.Run) { r.Origin.ParentTaskId = "" },
		"no parent orkflow": func(r *orkstackv1.Run) { r.Origin.ParentOrkflow = nil },
		"no root orkflow":   func(r *orkstackv1.Run) { r.Origin.RootOrkflow = nil },
	} {
		t.Run(name, func(t *testing.T) {
			run := whole()
			drop(run)
			if l, ok := LineageOfRun(run); ok {
				t.Fatalf("lineage = %+v, want none", l)
			}
		})
	}
	if _, ok := LineageOfRun(nil); ok {
		t.Fatal("no run says a lineage")
	}
}
