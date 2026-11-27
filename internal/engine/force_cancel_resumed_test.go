package engine

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/testsupport/must"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// fakePlans answers ReadResumePlan from one record, or an error.
type fakePlans struct {
	rec    *orkstackv1.ResumePlanRecord
	answer artifacts.RecordAnswer
	digest string
	err    error
	asked  []ResumePlanKey
}

func (f *fakePlans) ReadResumePlan(_ context.Context, _, _ string, scope uint64, n int) (artifacts.RecordRead[*orkstackv1.ResumePlanRecord], string, error) {
	f.asked = append(f.asked, ResumePlanKey{Scope: scope, N: n})
	if f.err != nil {
		return artifacts.RecordRead[*orkstackv1.ResumePlanRecord]{}, "", f.err
	}
	return artifacts.RecordRead[*orkstackv1.ResumePlanRecord]{Answer: f.answer, Value: f.rec}, f.digest, nil
}

// fakeResets answers ResetKeyOf with one reason, or an error.
type fakeResets struct {
	reason string
	err    error
}

func (f fakeResets) ResetKeyOf(context.Context, string, string) (string, bool, error) {
	return f.reason, f.reason != "", f.err
}

// TestForceCancelResumedFrom: what an ended execution was resumed from is read
// from the plan record of the resume that made it, held to its key's digest,
// and what cannot be told is unknown, never a guess.
func TestForceCancelResumedFrom(t *testing.T) {
	ctx := context.Background()
	digest := strings.Repeat("a", 64)
	key := ResumePlanKey{Scope: 9, N: 2, PlanDigest: digest}.String()
	forced := &orkstackv1.ResumePlanRecord{ForceCanceledBy: &orkstackv1.ResumeForceCanceledBy{}}
	found := func(rec *orkstackv1.ResumePlanRecord) *fakePlans {
		return &fakePlans{rec: rec, answer: artifacts.RecordFound, digest: digest}
	}
	for _, tt := range []struct {
		name   string
		resets resetKeyReader
		exec   string
		plans  *fakePlans
		want   string
	}{
		{"made by no reset", fakeResets{}, "exec-1", found(forced), FCResumedNone},
		{"made by a reset by hand alone", fakeResets{reason: "by hand"}, "exec-1", found(forced), FCResumedNone},
		{"a resume of a failed run", fakeResets{reason: key}, "exec-1", found(&orkstackv1.ResumePlanRecord{}), FCResumedFailed},
		{"a resume of a force-cancelled run", fakeResets{reason: key}, "exec-1", found(forced), FCResumedForceCanceled},
		{"its record does not answer", fakeResets{reason: key}, "exec-1", &fakePlans{err: errors.New("store is down")}, FCResumedUnknown},
		{"its record is gone", fakeResets{reason: key}, "exec-1", &fakePlans{answer: artifacts.RecordNone}, FCResumedUnknown},
		{"its record does not read as one", fakeResets{reason: key}, "exec-1", &fakePlans{answer: artifacts.RecordInvalid}, FCResumedUnknown},
		{"its record is another plan's", fakeResets{reason: key}, "exec-1", &fakePlans{rec: forced, answer: artifacts.RecordFound, digest: strings.Repeat("b", 64)}, FCResumedUnknown},
		{"its start does not answer", fakeResets{err: errors.New("cluster is down")}, "exec-1", found(forced), FCResumedUnknown},
		{"the cluster holds no such execution", fakeResets{err: ErrResumeExecutionNotFound}, "exec-1", found(forced), FCResumedUnknown},
		{"no execution is known", fakeResets{reason: key}, "", found(forced), FCResumedUnknown},
		{"no reset reader", nil, "exec-1", found(forced), FCResumedUnknown},
	} {
		t.Run(tt.name, func(t *testing.T) {
			a := &ForceCancelActivities{resets: tt.resets, plans: tt.plans}
			if got := a.resumedFrom(ctx, "default", "ork-r", tt.exec); got != tt.want {
				t.Errorf("= %q, want %q (asked %v)", got, tt.want, tt.plans.asked)
			}
			if want := (ResumePlanKey{Scope: 9, N: 2}); tt.want != FCResumedNone && len(tt.plans.asked) > 0 && tt.plans.asked[0] != want {
				t.Errorf("asked %v, want %v", tt.plans.asked, want)
			}
		})
	}
	t.Run("a step with no time left reads nothing", func(t *testing.T) {
		plans := found(forced)
		a := &ForceCancelActivities{resets: fakeResets{reason: key}, plans: plans}
		short, cancel := context.WithTimeout(ctx, resumedFromSpare/2)
		defer cancel()
		if got := a.resumedFrom(short, "default", "ork-r", "exec-1"); got != FCResumedUnknown || len(plans.asked) != 0 {
			t.Errorf("= %q after %d reads", got, len(plans.asked))
		}
	})
}

// TestForceCancelAudit_CountsWhatTheEndedRunWasResumedFrom: the completed
// record of an ending is counted with what the run it ended was resumed from,
// and written whatever that read answers.
func TestForceCancelAudit_CountsWhatTheEndedRunWasResumedFrom(t *testing.T) {
	ctx, keys := context.Background(), recordKeys(t)
	d := sampleDecision()
	msg, err := SignForceCancelDecision(keys, d)
	must.OK(t, err)
	key := ResumePlanKey{Scope: 9, N: 2, PlanDigest: strings.Repeat("a", 64)}.String()
	for name, tt := range map[string]struct {
		plans *fakePlans
		want  string
	}{
		"readable":        {&fakePlans{rec: &orkstackv1.ResumePlanRecord{ForceCanceledBy: &orkstackv1.ResumeForceCanceledBy{}}, answer: artifacts.RecordFound, digest: strings.Repeat("a", 64)}, "force_canceled/force_canceled"},
		"does not answer": {&fakePlans{err: errors.New("store is down")}, "force_canceled/unknown"},
	} {
		t.Run(name, func(t *testing.T) {
			a, store := fcActivities(t, keys, d)
			a.resets = fakeResets{reason: key}
			a.plans = tt.plans
			var c counted
			a.counter = &c
			in := FCAuditInput{Input: ForceCancelInput{Decision: msg}, Kind: forceCancelCompleted,
				Result: ForceCancelResult{State: FCStateForceCanceled, Terminated: true, Released: true, TerminalPublished: true, Execution: "exec-1"}}
			must.OK(t, a.Audit(ctx, in))
			if c[tt.want] != 1 || len(c) != 1 {
				t.Errorf("counted %v, want %s", c, tt.want)
			}
			page, err := store.ListAuditRecords(ctx, artifacts.AuditQuery{Since: d.IssuedAt.Add(-time.Hour), Until: d.IssuedAt.Add(time.Hour), Limit: 20})
			must.OK(t, err)
			if len(page.Records) != 1 {
				t.Errorf("%d records written", len(page.Records))
			}
		})
	}
}
