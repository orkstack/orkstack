package api

import (
	"context"
	"slices"
	"testing"

	"connectrpc.com/connect"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/gen/go/orkstack/v1/orkstackv1connect"
	"github.com/orkstack/orkstack/internal/authn"
	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/internal/testsupport/must"
)

// TestForceCancelAsk_ClaimsOnlyWhatAdmitted: what admitted the request is
// signed with it, and a request served although nothing did claims nothing.
func TestForceCancelAsk_ClaimsOnlyWhatAdmitted(t *testing.T) {
	run := &orkstackv1.Run{Id: "ork-r", OrkflowName: "pay"}
	for decision, admitted := range map[string]bool{DecisionAllowed: true, DecisionWouldDeny: false, DecisionDenied: false} {
		ctx := withDecision(context.Background(), &accessDecision{Permission: "run.cancel.force", Scope: "orkflow:default/pay", Rule: "ops", Generation: 7, Decision: decision})
		ask := forceCancelAsk(ctx, connect.NewRequest(&orkstackv1.ForceCancelRunRequest{RunId: "ork-r", Reason: "stuck"}), run, 0)
		claims := ask.Permission == "run.cancel.force" && ask.Scope == "orkflow:default/pay" && ask.Rule == "ops" && ask.PolicyGeneration == 7
		nothing := ask.Permission == "" && ask.Scope == "" && ask.Rule == "" && ask.PolicyGeneration == 0
		if claims != admitted || nothing == admitted {
			t.Errorf("%s: %+v", decision, ask)
		}
	}
}

// TestForceCancelBelow_Redacted: a run below of an Orkflow the caller may not
// read has only its state, and what is open after is named only where the
// caller may read it.
func TestForceCancelBelow_Redacted(t *testing.T) {
	readable := func(orkflow string) (bool, error) { return orkflow == "pay", nil }
	c := &engine.ForceCancelCascade{State: engine.FCCascadePartial, Excluded: map[string]string{"z": "unreadable"}, After: &engine.FCAfter{Count: 3, RunIDs: []string{"a", "b", "q"}},
		Results: []engine.FCBelowResult{{FCBelow: engine.FCBelow{RunID: "a", Orkflow: "pay"}, State: engine.FCStateFailed}, {FCBelow: engine.FCBelow{RunID: "b", ParentRunID: "a", Orkflow: "secret", Depth: 2}, State: engine.FCStateFailed, RequestID: "fc-1-b", Cause: "why"}}}
	got, err := forceCancelCascadeProto(c, readable)
	must.OK(t, err)
	hidden := &orkstackv1.ForceCancelRunBelow{Redacted: true, State: orkstackv1.ForceCancelState_FORCE_CANCEL_STATE_FAILED}
	if got.GetRuns()[0].GetRunId() != "a" || got.GetRuns()[1].String() != hidden.String() || !slices.Equal(got.GetOpenAfter().GetRunIds(), []string{"a"}) || got.GetOpenAfter().GetCount() != 3 ||
		got.GetExclusions()[0].GetRunId() != "" || got.GetExclusions()[0].GetCause() != "unreadable" {
		t.Errorf("%v", got)
	}
}

// TestForceCancelRun_ReachesBelow: acting on, listing or stopping what is below
// is decided at the namespace, and the refusal says so; the run alone, at its
// Orkflow, and a refusal there is a run that does not exist.
func TestForceCancelRun_ReachesBelow(t *testing.T) {
	const procedure = orkstackv1connect.RunServiceForceCancelRunProcedure
	row := accessRows[procedure]
	for name, req := range map[string]*orkstackv1.ForceCancelRunRequest{"one": {RunId: "r"}, "cascade": {RunId: "r", Cascade: true}, "preview": {RunId: "r", Preview: true}, "stop": {RunId: "r", Stop: true}} {
		var asked []string
		for _, c := range row.checks {
			if !c.when(req) {
				continue
			}
			at := "run"
			if c.res != nil {
				at = "namespace"
			}
			if c.named != (c.res != nil) {
				t.Errorf("%s: %s at the %s is named %t", name, c.perm, at, c.named)
			}
			asked = append(asked, string(c.perm)+"@"+at)
		}
		want := []string{"run.cancel.force@namespace", "run.read@namespace"}
		if name == "one" {
			want = []string{"run.cancel.force@run", "run.read@run"}
		}
		if !slices.Equal(asked, want) {
			t.Errorf("%s asks %v, want %v", name, asked, want)
		}
	}
	s := newAccessService(t)
	s.access, _ = accessFor(t, authn.ModeEnforce, nil)
	for req, code := range map[*orkstackv1.ForceCancelRunRequest]connect.Code{{RunId: "r", Cascade: true}: connect.CodePermissionDenied, {RunId: "r"}: connect.CodeNotFound} {
		if _, err := s.access.decide(asAlice(authn.ModeEnforce), s, procedure, req); connect.CodeOf(err) != code {
			t.Errorf("%v is refused %v, want %s", req, err, code)
		}
	}
}
