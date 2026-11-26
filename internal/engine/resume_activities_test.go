package engine

import (
	"context"
	"errors"
	"strings"
	"testing"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/pkg/artifacts"
	"go.temporal.io/sdk/temporal"
)

func TestVerifyResumePlan_RunsOnlyThePlanTheResetNames(t *testing.T) {
	key := ResumePlanKey{Scope: 42, N: 1, PlanDigest: testDigest}
	facts := ResetFacts{BaseRunID: "base", Reason: key.String(), ScheduledEventID: 8, StartedEventID: 9}
	record := func(edit func(*orkstackv1.ResumePlanRecord)) artifacts.RecordRead[*orkstackv1.ResumePlanRecord] {
		r := &orkstackv1.ResumePlanRecord{BaseExecutionRunId: "base", RScheduledEventId: 8, RStartedEventId: 9,
			Block: &orkstackv1.ResumeBlock{Scope: 42, SeqBase: 42, Units: []byte{1, 2, 3}}}
		r.PlanDigest = ResumePlanDigestOf(r)
		if edit != nil {
			edit(r)
		}
		return artifacts.RecordRead[*orkstackv1.ResumePlanRecord]{Answer: artifacts.RecordFound, Value: r}
	}
	linked := artifacts.RecordRead[artifacts.ResumeRecord]{Answer: artifacts.RecordFound, Value: artifacts.ResumeRecord{Scope: 42, N: 1}}
	for _, tc := range []struct {
		name   string
		read   artifacts.RecordRead[*orkstackv1.ResumePlanRecord]
		digest string
		newest uint64
		link   artifacts.RecordRead[artifacts.ResumeRecord]
		want   string // a fragment of the reason; "" runs the plan
	}{
		{"the plan", record(nil), testDigest, 42, linked, ""},
		{"no record", artifacts.RecordRead[*orkstackv1.ResumePlanRecord]{Answer: artifacts.RecordNone}, "", 42, linked, "no record"},
		{"another digest", record(nil), strings.Repeat("cd", 32), 42, linked, "hashes to"},
		{"another base", record(func(r *orkstackv1.ResumePlanRecord) { r.BaseExecutionRunId = "other" }), testDigest, 42, linked, `of the ending of "other"`},
		// resume-plan-key-at-wrong-reset-point-invalid
		{"another reset point", record(func(r *orkstackv1.ResumePlanRecord) { r.RStartedEventId = 15 }), testDigest, 42, linked, "starts from point 8/15"},
		{"a block of another scope", record(func(r *orkstackv1.ResumePlanRecord) { r.Block.Scope = 7 }), testDigest, 42, linked, "scope 7"},
		// A stored digest is never trusted: it is the plan's own, or the plan is not run.
		{"a digest of another plan", record(func(r *orkstackv1.ResumePlanRecord) { r.PlanDigest = strings.Repeat("ef", 32) }), testDigest, 42, linked, "not the one it was confirmed as"},
		{"no digest", record(func(r *orkstackv1.ResumePlanRecord) { r.PlanDigest = "" }), testDigest, 42, linked, "not the one it was confirmed as"},
		{"other units than it was confirmed as", record(func(r *orkstackv1.ResumePlanRecord) { r.Block.Units = []byte{1, 2, 4} }), testDigest, 42, linked, "not the one it was confirmed as"},
		{"another sequence", record(func(r *orkstackv1.ResumePlanRecord) { r.Block.SeqBase = 43 }), testDigest, 42, linked, "not the one it was confirmed as"},
		{"a newer resume", record(nil), testDigest, 50, linked, "newest resume is plan 50"},
		{"a later plan of the scope", record(nil), testDigest, 42,
			artifacts.RecordRead[artifacts.ResumeRecord]{Answer: artifacts.RecordFound, Value: artifacts.ResumeRecord{Scope: 42, N: 2}}, "plan 42.002"},
		{"no identity record", record(nil), testDigest, 0, artifacts.RecordRead[artifacts.ResumeRecord]{Answer: artifacts.RecordNone}, "cannot be read"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := verifyResumePlan(key, facts, tc.read, tc.digest, tc.newest, tc.link)
			if tc.want == "" && got != "" || tc.want != "" && !strings.Contains(got, tc.want) {
				t.Fatalf("verifyResumePlan = %q, want %q", got, tc.want)
			}
		})
	}
}

// A reason that claims to be a plan key is never read as a reset by hand.
func TestPlanKeyOf_OnlyAReasonOfNoVersionIsNoResume(t *testing.T) {
	for _, tc := range []struct {
		reason string
		resume bool
		final  bool
	}{
		{"ork-resume:v1:42.000:" + testDigest, true, false},
		{"reset by an operator", false, false},
		{"", false, false},
		{"ork-resume:v2:42.000:" + testDigest, false, true},
		{"ork-resume:v1:42.000:" + strings.ToUpper(testDigest), false, true},
		{"ork-resume:v1:42.999:" + testDigest, false, true},
	} {
		_, resume, err := planKeyOf("ork-1", tc.reason)
		var app *temporal.ApplicationError
		isFinal := errors.As(err, &app) && app.NonRetryable() && app.Type() == ReasonResumePlanInvalid
		if resume != tc.resume || isFinal != tc.final || (err != nil) != tc.final {
			t.Errorf("planKeyOf(%q) = %v, %v; want resume %v, final %v", tc.reason, resume, err, tc.resume, tc.final)
		}
	}
}

func TestUnansweredPlanRead_RetriesOnlyWhatAnotherTryCanChange(t *testing.T) {
	var app *temporal.ApplicationError
	for _, err := range []error{&artifacts.RecordStoreUnanswered{Class: artifacts.UnansweredUnavailable, Err: errors.New("down")}, context.DeadlineExceeded} {
		if got := unansweredPlanRead(err); errors.As(got, &app) && app.NonRetryable() {
			t.Errorf("%v became final", err)
		}
	}
	if got := unansweredPlanRead(artifacts.ErrInvalidRecordKey); !errors.As(got, &app) || !app.NonRetryable() || app.Type() != ReasonResumePlanInvalid {
		t.Errorf("an invalid key = %v, want a final RESUME_PLAN_INVALID", got)
	}
}
