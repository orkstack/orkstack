package engine

import (
	"reflect"
	"strings"
	"testing"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/runspan"
	"github.com/orkstack/orkstack/internal/stablejson"

	"github.com/orkstack/orkstack/internal/testsupport/must"
)

func originState(runID, orkflow string, origin *RunOrigin) *runState {
	return &runState{runID: runID, plan: RunPlan{Origin: origin, RecordRootRun: true},
		wf: &orkstackv1.Orkflow{Name: orkflow, Version: "1.0.0", DefinitionDigest: "sha256:" + orkflow}}
}

func refPlan(orkflow string) *OrkflowRefPlan {
	return &OrkflowRefPlan{Name: orkflow, Version: "1.0.0", Digest: "sha256:" + orkflow}
}

// A child's origin names the run and task that composed it, that run's
// Orkflow, and its root's Orkflow and whole origin; the child's own
// principal and reference stay empty.
func TestNewChildOrigin_NamesItsParentAndItsRoot(t *testing.T) {
	root := &RunOrigin{Kind: "RUN_ORIGIN_KIND_TRIGGER", ID: "trg-1", Reference: "PR-7", Description: "deploys",
		Principal: "dana", PrincipalVerified: true, AuthMethod: "oidc"}
	got := originState("ork-root", "release", root).newChildOrigin("train")
	want := &RunOrigin{Kind: RunOriginKindParentRun, ID: "ork-root", RootRunID: "ork-root", ParentTaskID: "train",
		ParentOrkflow: refPlan("release"), RootOrkflow: refPlan("release"), RootOrigin: root}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("origin = %+v\nwant     %+v", got, want)
	}
	if got.RootOrigin == root {
		t.Fatal("the child's root origin aliases its parent's plan")
	}
	if back := OriginFromProto(got.Proto()); !reflect.DeepEqual(back, want) {
		t.Fatalf("round trip = %+v\nwant         %+v", back, want)
	}
}

// A grandchild names its own parent and copies the root its parent holds;
// nothing nests below one level.
func TestNewChildOrigin_GrandchildHoldsNoNestedLineage(t *testing.T) {
	root := &RunOrigin{Kind: "RUN_ORIGIN_KIND_SUBMIT", Principal: "dana"}
	child := originState("ork-root", "release", root).newChildOrigin("train")
	got := originState("ork-child", "train", child).newChildOrigin("eval")
	if got.ID != "ork-child" || got.ParentTaskID != "eval" || !reflect.DeepEqual(got.ParentOrkflow, refPlan("train")) ||
		got.RootRunID != "ork-root" || !reflect.DeepEqual(got.RootOrkflow, refPlan("release")) {
		t.Fatalf("origin = %+v", got)
	}
	if !reflect.DeepEqual(got.RootOrigin, root) {
		t.Fatalf("root origin = %+v, want the root's own, with no lineage of its own", got.RootOrigin)
	}
}

// A run that holds no root (its parent was accepted without one) never calls
// itself its child's root; a run with no origin at all has none to copy.
func TestNewChildOrigin_NeverGuessesARoot(t *testing.T) {
	orphan := originState("ork-mid", "mid", &RunOrigin{Kind: RunOriginKindParentRun, ID: "ork-up"})
	orphan.plan.RecordRootRun = false
	if got := orphan.newChildOrigin("t"); got.RootRunID != "" || got.RootOrkflow != nil || got.RootOrigin != nil {
		t.Fatalf("a run that records no root named one: %+v", got)
	}
	if got := originState("ork-1", "flow", nil).newChildOrigin("t"); got.RootOrigin != nil || got.RootRunID != "ork-1" {
		t.Fatalf("origin of a run with none = %+v", got)
	}
}

// A shared child's Orkflow is named with the namespace that owns it.
func TestNewChildOrigin_NamesASharedOrkflowsOwner(t *testing.T) {
	s := originState("ork-shared", "deploy", &RunOrigin{Kind: RunOriginKindParentRun, ID: "ork-root", RootRunID: "ork-root"})
	s.sharedOwner = "platform"
	if got := s.newChildOrigin("t").ParentOrkflow; got == nil || got.Namespace != "platform" {
		t.Fatalf("parent orkflow = %+v", got)
	}
}

// The largest origin a child can hold fits what the submit reserves for it:
// each text at its bound, in the byte JSON escapes longest (one byte as six).
func TestNewChildOrigin_FitsItsReserve(t *testing.T) {
	long := strings.Repeat("<", 256)
	name := strings.Repeat("n", 200)
	root := &RunOrigin{Kind: "RUN_ORIGIN_KIND_TRIGGER", ID: name, Reference: long, Description: long, Principal: long,
		PrincipalVerified: true, AuthMethod: "service_account"}
	s := originState("ork-0123456789", name, root)
	s.sharedOwner = name
	s.wf.Version, s.wf.DefinitionDigest = name, "sha256:"+strings.Repeat("a", 64)
	encoded, err := stablejson.Marshal(s.newChildOrigin(name))
	must.OK(t, err)
	if len(encoded) > ChildOriginMaxBytes {
		t.Fatalf("the largest origin is %d bytes, over the %d reserved", len(encoded), ChildOriginMaxBytes)
	}
}

// What a child returns is recorded in its parent's history once for every
// composed task, and the parent reads none of the lineage, so none is sent.
func TestCompactChildResult_CarriesNoLineage(t *testing.T) {
	root := &RunOrigin{Kind: "RUN_ORIGIN_KIND_TRIGGER", ID: "trg-1", Reference: "PR-7", Principal: "dana"}
	origin := originState("ork-root", "release", root).newChildOrigin("train").Proto()
	run := &orkstackv1.Run{Id: "ork-child", Origin: origin}
	got := compactChildResult(run).GetOrigin()
	if got.GetKind() != origin.GetKind() || got.GetId() != "ork-root" || got.GetRootRunId() != "ork-root" ||
		got.GetParentTaskId() != "" || got.GetParentOrkflow() != nil || got.GetRootOrkflow() != nil || got.GetRootOrigin() != nil {
		t.Fatalf("origin of the result = %v", got)
	}
	if origin.GetRootOrigin() == nil {
		t.Fatal("the run's own origin was changed")
	}
}

// A child continues its parent's trace, under its parent's run span, and a
// parent with no trace gives its child none. The planned copy is untouched.
func TestChildStartPlan_ContinuesTheParentsTrace(t *testing.T) {
	const parent = "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01"
	s := originState("ork-root", "release", nil)
	s.plan.Trace = &TraceContext{Traceparent: parent, Tracestate: "vendor=1"}
	planned := &ChildRunPlan{}
	got := s.childStartPlan("train", planned)
	want := &TraceContext{Traceparent: runspan.ChildTraceparent(parent, "ork-root"), Tracestate: "vendor=1"}
	if !reflect.DeepEqual(got.Trace, want) || planned.Plan.Trace != nil {
		t.Fatalf("child trace = %+v, want %+v; planned %+v", got.Trace, want, planned.Plan.Trace)
	}
	if run := s.snapshot(); run.GetTraceId() != "0af7651916cd43dd8448eb211c80319c" || run.GetTraceparent() != parent {
		t.Fatalf("trace_id = %q, traceparent = %q", run.GetTraceId(), run.GetTraceparent())
	}
	for _, none := range []*TraceContext{nil, {Traceparent: "malformed", Tracestate: "vendor=1"}} {
		s.plan.Trace = none
		if got := s.childStartPlan("train", planned); got.Trace != nil {
			t.Fatalf("child of %+v got %+v", none, got.Trace)
		}
		if run := s.snapshot(); run.GetTraceId() != "" || run.GetTraceparent() != "" {
			t.Fatalf("trace of %+v = %q %q", none, run.GetTraceId(), run.GetTraceparent())
		}
	}
}
