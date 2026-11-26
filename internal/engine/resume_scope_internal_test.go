package engine

import (
	"strings"
	"testing"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// scopedState is an admitted run with a container task, an await and a
// composed task, whose rows are at epoch.
func scopedState(epoch int64, rootScope uint64, origin *RunOrigin) *runState {
	tasks := []*orkstackv1.Task{
		{Id: "apply"},
		{Id: "approve", Await: &orkstackv1.AwaitSpec{}},
		{Id: "ship", Uses: "orkflow:child@1", Orkflow: &orkstackv1.OrkflowRef{Name: "child", Version: "1"}},
		{Id: "report", Needs: []string{"apply", "approve", "ship"}},
	}
	s := &runState{runID: "ork-1", wf: &orkstackv1.Orkflow{Name: "w", Version: "1", Tasks: tasks}, tasks: map[string]*orkstackv1.Task{},
		taskRuns: map[string]*orkstackv1.TaskRun{}, awaits: map[string]*awaitState{"approve": {}}, children: map[string]*childState{"ship": {}},
		plan: RunPlan{Namespace: "default", CommitRoot: "s3://bucket", Origin: origin,
			Identity: &IdentityPlan{AdmissionSHA256: strings.Repeat("c", 64), SpecVersion: 1, ValueRecords: 1, RootScope: rootScope}}}
	for _, t := range tasks {
		s.tasks[t.GetId()] = t
		s.taskRuns[t.GetId()] = &orkstackv1.TaskRun{TaskId: t.GetId(), ResultEpoch: epoch}
	}
	return s
}

var childOrigin = &RunOrigin{Kind: RunOriginKindParentRun, ID: "ork-parent", RootRunID: "ork-root"}

// TestResumeScope_WhatARunNames: the resume scopes a recorded run names to
// the engine's checks, at each place it names them: a root its own rows', a
// run below one the scope its root had when the run was started; neither
// anything before a resume, and a run that is not admitted nothing at all.
func TestResumeScope_WhatARunNames(t *testing.T) {
	for name, tc := range map[string]struct {
		s                       *runState
		resume, root, ofItsTask uint64
	}{
		"never resumed":           {scopedState(0, 0, nil), 0, 0, 0},
		"a resumed root":          {scopedState(42, 0, nil), 42, 0, 42},
		"a child of one":          {scopedState(0, 42, childOrigin), 0, 42, 42},
		"a child of an unresumed": {scopedState(0, 0, childOrigin), 0, 0, 0},
	} {
		t.Run(name, func(t *testing.T) {
			s := tc.s
			in := s.executeTaskInput(s.tasks["apply"], "apply", -1, "", nil)
			s.nameScope(in, s.taskRuns["apply"].GetResultEpoch())
			if id := in.GetIdentity(); id.GetResumeScope() != tc.resume || id.GetRootScope() != tc.root {
				t.Errorf("a container task names %d, %d; want %d, %d", id.GetResumeScope(), id.GetRootScope(), tc.resume, tc.root)
			}
			if got := s.resolutionRequest("approve"); got.ResumeScope != tc.resume {
				t.Errorf("an await is read at %d, want %d", got.ResumeScope, tc.resume)
			}
			if resume, root := s.resumeScope("ship"), s.rootScope(); resume != tc.resume || root != tc.root {
				t.Errorf("a composed task is recorded at %d, %d; want %d, %d", resume, root, tc.resume, tc.root)
			}
			child := s.childStartPlan("ship", &ChildRunPlan{RunID: "ork-child", Plan: RunPlan{Identity: &IdentityPlan{AdmissionSHA256: "x"}}})
			s.nameRootScope(&child, "ship")
			if child.Identity.RootScope != tc.ofItsTask {
				t.Errorf("its child's root scope is %d, want %d", child.Identity.RootScope, tc.ofItsTask)
			}
			for id, kind := range map[string]artifacts.RecordKind{"approve": artifacts.RecordResolution, "ship": artifacts.RecordComposed} {
				key, err := artifacts.RecordKey(kind, "default", "ork-1", id, tc.resume)
				if err != nil {
					t.Fatal(err)
				}
				if got := s.recordURI(id); got != "s3://bucket/"+key {
					t.Errorf("%s's record is %s, want %s", id, got, key)
				}
			}
		})
	}
}

// TestNameRootScope_LeavesThePlanAlone: the planned child is one value the
// replayed plan holds, so naming its scope changes a copy of its identity,
// and a child with none is started with none.
func TestNameRootScope_LeavesThePlanAlone(t *testing.T) {
	s := scopedState(42, 0, nil)
	planned := &ChildRunPlan{RunID: "ork-child", Plan: RunPlan{Identity: &IdentityPlan{AdmissionSHA256: "x"}}}
	child := s.childStartPlan("ship", planned)
	s.nameRootScope(&child, "ship")
	if planned.Plan.Identity.RootScope != 0 || child.Identity.RootScope != 42 {
		t.Fatalf("planned %d, started %d; want 0, 42", planned.Plan.Identity.RootScope, child.Identity.RootScope)
	}
	plain := s.childStartPlan("ship", &ChildRunPlan{RunID: "ork-child"})
	s.nameRootScope(&plain, "ship")
	if plain.Identity != nil {
		t.Fatalf("a child with no identity was given %+v", plain.Identity)
	}
}

// TestNameScope_ARunWithNoIdentityNamesNothing.
func TestNameScope_ARunWithNoIdentityNamesNothing(t *testing.T) {
	s := scopedState(42, 0, nil)
	s.plan.Identity = nil
	in := s.executeTaskInput(s.tasks["apply"], "apply", -1, "", nil)
	s.nameScope(in, 42)
	if in.GetIdentity() != nil {
		t.Fatalf("identity = %v", in.GetIdentity())
	}
}
