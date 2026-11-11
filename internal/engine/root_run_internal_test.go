package engine

import (
	"bytes"
	"encoding/json/v2"
	"testing"

	"go.temporal.io/sdk/temporal"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/stablejson"
	"github.com/orkstack/orkstack/internal/task"
	"github.com/orkstack/orkstack/pkg/artifacts"

	"github.com/orkstack/orkstack/internal/testsupport/must"
)

// TestChildStartPlan_RecordsTheRoot is the rule of a chain's root: a child of
// a top-level run names that run, a deeper child copies what its parent
// holds, and a run accepted before the field records nothing rather than
// guessing, however its parent was started.
func TestChildStartPlan_RecordsTheRoot(t *testing.T) {
	childOf := func(root string) *RunOrigin {
		return &RunOrigin{Kind: RunOriginKindParentRun, ID: "ork-grandparent", RootRunID: root}
	}
	for _, tc := range []struct {
		name     string
		origin   *RunOrigin
		record   bool
		wantRoot string
	}{
		{name: "a child of a submitted run names the submitted run", record: true,
			origin:   &RunOrigin{Kind: "RUN_ORIGIN_KIND_SUBMIT"},
			wantRoot: "ork-this-run"},
		{name: "a child of a run with no origin names that run", record: true,
			wantRoot: "ork-this-run"},
		{name: "a child of a triggered run names the run, not the trigger", record: true,
			origin:   &RunOrigin{Kind: "RUN_ORIGIN_KIND_TRIGGER", ID: "trg-1"},
			wantRoot: "ork-this-run"},
		{name: "a child of a scheduled run names the run, not the schedule", record: true,
			origin:   &RunOrigin{Kind: "RUN_ORIGIN_KIND_SCHEDULE", ID: "sch-1"},
			wantRoot: "ork-this-run"},
		{name: "a child of a run-again names that run, not its source", record: true,
			origin:   &RunOrigin{Kind: "RUN_ORIGIN_KIND_RUN_AGAIN", ID: "ork-source"},
			wantRoot: "ork-this-run"},
		{name: "a grandchild copies the root its parent holds", record: true,
			origin:   childOf("ork-root"),
			wantRoot: "ork-root"},
		{name: "a top-level run accepted before the field records none",
			origin: &RunOrigin{Kind: "RUN_ORIGIN_KIND_SUBMIT"}},
		{name: "a child accepted before the field does not call itself the root",
			origin: childOf("")},
	} {
		t.Run(tc.name, func(t *testing.T) {
			state := &runState{runID: "ork-this-run", plan: RunPlan{Origin: tc.origin, RecordRootRun: tc.record}}
			got := state.childStartPlan(&ChildRunPlan{RunID: "ork-child"})
			if got.Origin == nil || got.Origin.Kind != RunOriginKindParentRun || got.Origin.ID != "ork-this-run" {
				t.Fatalf("child origin = %+v, want the parent named", got.Origin)
			}
			if got.Origin.RootRunID != tc.wantRoot {
				t.Errorf("root = %q, want %q", got.Origin.RootRunID, tc.wantRoot)
			}
			if got.RecordRootRun != tc.record {
				t.Errorf("the child's RecordRootRun = %v, want the parent's %v", got.RecordRootRun, tc.record)
			}
		})
	}
}

// TestChildStartPlan_UnrecordedParentEncodesAsBefore is the payload half's
// premise in one assertion: a parent whose plan does not ask for the root
// sends a child plan with neither new key, so a recorded history's child start
// is byte for byte what its build sent.
func TestChildStartPlan_UnrecordedParentEncodesAsBefore(t *testing.T) {
	state := &runState{runID: "ork-this-run", plan: RunPlan{Origin: &RunOrigin{Kind: "RUN_ORIGIN_KIND_SUBMIT"}}}
	encoded, err := stablejson.Marshal(state.childStartPlan(&ChildRunPlan{RunID: "ork-child"}))
	if err != nil {
		t.Fatal(err)
	}
	var decoded map[string]any
	if err := json.Unmarshal(encoded, &decoded); err != nil {
		t.Fatal(err)
	}
	if _, present := decoded["record_root_run"]; present {
		t.Errorf("the child plan carries record_root_run: %s", encoded)
	}
	origin, _ := decoded["origin"].(map[string]any)
	if _, present := origin["root_run_id"]; present {
		t.Errorf("the child origin carries root_run_id: %s", encoded)
	}
}

// TestRunOrigin_RootRunRoundTrips keeps both directions of the plan-side copy
// honest, and that a continued run's plan, which crosses as JSON, keeps it.
func TestRunOrigin_RootRunRoundTrips(t *testing.T) {
	wire := &orkstackv1.RunOrigin{
		Kind:      orkstackv1.RunOriginKind_RUN_ORIGIN_KIND_PARENT_RUN,
		Id:        "ork-parent",
		RootRunId: "ork-root",
	}
	plan := RunPlan{Origin: OriginFromProto(wire), RecordRootRun: true}
	encoded, err := stablejson.Marshal(plan)
	if err != nil {
		t.Fatal(err)
	}
	var crossed RunPlan
	if err := json.Unmarshal(encoded, &crossed); err != nil {
		t.Fatal(err)
	}
	if !crossed.RecordRootRun {
		t.Error("the plan lost RecordRootRun crossing as JSON")
	}
	if got := crossed.Origin.Proto().GetRootRunId(); got != "ork-root" {
		t.Errorf("the origin's root after crossing = %q, want ork-root", got)
	}
}

// TestRunStartMemo_CarriesTheRootOnlyWhenRecorded: a listing row reads the
// root from the memo, and a run with none writes no key, so a child start with
// no root has the memo its build recorded.
func TestRunStartMemo_CarriesTheRootOnlyWhenRecorded(t *testing.T) {
	wf := &orkstackv1.Orkflow{Name: "leaf", Version: "1"}
	with := RunStartMemo(wf, RunPlan{Origin: &RunOrigin{Kind: RunOriginKindParentRun, ID: "ork-parent", RootRunID: "ork-root"}})
	if with[MemoOriginRootRunID] != "ork-root" {
		t.Errorf("memo root = %v, want ork-root", with[MemoOriginRootRunID])
	}
	for name, plan := range map[string]RunPlan{
		"a child with none": {Origin: &RunOrigin{Kind: RunOriginKindParentRun, ID: "ork-parent"}},
		"a top-level run":   {Origin: &RunOrigin{Kind: "RUN_ORIGIN_KIND_SUBMIT"}},
		"no origin":         {},
	} {
		if value, present := RunStartMemo(wf, plan)[MemoOriginRootRunID]; present {
			t.Errorf("%s: memo root = %v, want no key", name, value)
		}
	}
}

// TestRunStartSearchAttributes_StampsTheRootOnlyWhenTheClusterHasIt: a start
// stamping an attribute the cluster does not have is refused, so the plan's
// pinned answer decides, independently of the origin's.
func TestRunStartSearchAttributes_StampsTheRootOnlyWhenTheClusterHasIt(t *testing.T) {
	wf := &orkstackv1.Orkflow{Name: "leaf", Version: "1"}
	child := &RunOrigin{Kind: RunOriginKindParentRun, ID: "ork-parent", RootRunID: "ork-root"}
	stamped := func(attrs []temporal.SearchAttributeUpdate) (string, bool) {
		return temporal.NewSearchAttributes(attrs...).GetKeyword(task.SearchKeyOrkflowOriginRootRunID)
	}
	if got, ok := stamped(RunStartSearchAttributes(wf, RunPlan{Origin: child, RootRunIndexed: true}, false, false)); !ok || got != "ork-root" {
		t.Errorf("indexed = %q, %v; want ork-root", got, ok)
	}
	if got, ok := stamped(RunStartSearchAttributes(wf, RunPlan{Origin: child}, true, true)); ok {
		t.Errorf("unindexed stamped %q", got)
	}
	noRoot := &RunOrigin{Kind: RunOriginKindParentRun, ID: "ork-parent"}
	if got, ok := stamped(RunStartSearchAttributes(wf, RunPlan{Origin: noRoot, RootRunIndexed: true}, true, true)); ok {
		t.Errorf("a child with no root stamped %q", got)
	}
	if got, ok := stamped(RunStartSearchAttributes(wf, RunPlan{RootRunIndexed: true}, true, true)); ok {
		t.Errorf("a run with no origin stamped %q", got)
	}
}

// TestChildStartPlan_CopiesTheRootIndexAnswer: a grandchild needs the top plan's
// answer, which it can only have through its parent's start plan.
func TestChildStartPlan_CopiesTheRootIndexAnswer(t *testing.T) {
	for _, indexed := range []bool{true, false} {
		state := &runState{runID: "ork-this-run", plan: RunPlan{RecordRootRun: true, RootRunIndexed: indexed}}
		if got := state.childStartPlan(&ChildRunPlan{RunID: "ork-child"}).RootRunIndexed; got != indexed {
			t.Errorf("parent's answer %v became the child's %v", indexed, got)
		}
	}
}

// TestIndexProjection_RootRow: the row is what a residual filter reads, so a
// root row must project every origin field a filter can name.
func TestIndexProjection_RootRow(t *testing.T) {
	got := indexProjection(artifacts.RunIndexEntry{
		RunID: "ork-leaf", Namespace: "ns", Orkflow: "leaf", Status: "succeeded",
		OriginKind: orkstackv1.RunOriginKind_RUN_ORIGIN_KIND_PARENT_RUN, OriginID: "ork-mid", RootRunID: "ork-root",
	}).GetOrigin()
	if got.GetKind() != orkstackv1.RunOriginKind_RUN_ORIGIN_KIND_PARENT_RUN || got.GetId() != "ork-mid" || got.GetRootRunId() != "ork-root" {
		t.Fatalf("projection = %v", got)
	}
}

// TestArchivedBody_RootRun: an archived run keeps its root through the archive
// body's own encoding, and one archived before the field reads back with none
// rather than failing.
func TestArchivedBody_RootRun(t *testing.T) {
	run := &orkstackv1.Run{Id: "ork-leaf", OrkflowName: "leaf", Origin: &orkstackv1.RunOrigin{
		Kind: orkstackv1.RunOriginKind_RUN_ORIGIN_KIND_PARENT_RUN, Id: "ork-mid", RootRunId: "ork-root",
	}}
	body, err := MarshalRunArchive(run, nil)
	must.OK(t, err)
	back, _, err := UnmarshalRunArchive(body)
	must.OK(t, err)
	if got := back.GetOrigin().GetRootRunId(); got != "ork-root" {
		t.Errorf("root after the archive body = %q, want ork-root", got)
	}

	run.Origin.RootRunId = ""
	if body, err = MarshalRunArchive(run, nil); err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(body, []byte("rootRunId")) || bytes.Contains(body, []byte("root_run_id")) {
		t.Errorf("a body with no root names the field: %s", body)
	}
	older, _, err := UnmarshalRunArchive(body)
	must.OK(t, err)
	if older.GetOrigin().GetRootRunId() != "" || older.GetOrigin().GetId() != "ork-mid" {
		t.Errorf("an older body reads as %v", older.GetOrigin())
	}
}
