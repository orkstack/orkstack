package api_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"connectrpc.com/connect"
	enumspb "go.temporal.io/api/enums/v1"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/api"
	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/internal/testsupport/must"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// resets makes the cluster apply a reset of the base as the server does: the
// base names the run it started, which is the run's current execution and
// carries the plan's key; and, when carriesOn, the run goes on.
func (f *resumeFixture) resets(carriesOn bool) { f.cluster.onReset = f.applies(carriesOn) }

// applies is what resets does with one reset.
func (f *resumeFixture) applies(carriesOn bool) func(engine.ResumeReset) (string, error) {
	return func(r engine.ResumeReset) (string, error) {
		f.cluster.mu.Lock()
		id := fmt.Sprintf("exec-%d", len(f.cluster.resets)+1)
		base := f.cluster.executions[r.BaseExecutionRunID]
		base.ResetRunID = id
		f.cluster.executions[r.BaseExecutionRunID] = base
		next := engine.ResumeExecution{ExecutionRunID: id, Status: enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING}
		f.cluster.executions[""], f.cluster.executions[id] = next, next
		if f.cluster.resetKeys == nil {
			f.cluster.resetKeys = map[string]string{}
		}
		f.cluster.resetKeys[id] = r.Reason
		f.cluster.mu.Unlock()
		f.reader.mu.Lock()
		f.run.Status = orkstackv1.Status_STATUS_RUNNING
		f.reader.mu.Unlock()
		if carriesOn {
			key, _ := engine.ParseResumePlanKey(r.Reason)
			f.runReader.set(int64(key.Scope))
		}
		return id, nil
	}
}

func (f *resumeFixture) confirm(t *testing.T, change func(*orkstackv1.ResumeRunRequest)) (*orkstackv1.ResumeRunResponse, error) {
	t.Helper()
	req := &orkstackv1.ResumeRunRequest{ExpectedTerminalSequence: 9, Reason: "flaky network"}
	if change != nil {
		change(req)
	}
	return f.resume(req)
}

// preview is the plan a person saw before confirming it.
func (f *resumeFixture) preview(t *testing.T) *orkstackv1.ResumeRunResponse {
	t.Helper()
	got, err := f.resume(&orkstackv1.ResumeRunRequest{DryRun: true})
	if err != nil {
		t.Fatal(err)
	}
	return got
}

func quickConfirm(f *resumeFixture) { f.settings.ConfirmTimeout = 200 * time.Millisecond }

// TestResumeRun_Confirm: a confirm writes the plan it would carry out, and
// identity's scope, then resets the base once, to R, under that plan's key,
// and answers when the run goes on.
func TestResumeRun_Confirm(t *testing.T) {
	f := newResumeFixture(t, nil)
	f.resets(true)
	preview := f.preview(t)
	req := connect.NewRequest(&orkstackv1.ResumeRunRequest{RunId: "ork-r", ExpectedTerminalSequence: 9, ExpectedPlanDigest: preview.GetPlanDigest(), Reason: "flaky network"})
	req.Header().Set(api.AuditPrincipalHeader, "ada")
	resp, err := f.svc.ResumeRun(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	got := resp.Msg
	if got.GetAlreadyResumed() || got.GetSeqBase() != 10 || got.GetPlanDigest() != preview.GetPlanDigest() || got.GetKeptCount() != 1 {
		t.Fatalf("answer = %v", got)
	}
	r := got.GetResume()
	if r.GetSequence() != 10 || r.GetResumedAfterSequence() != 9 || r.GetScope() != 10 || r.GetPrincipal() != "ada" || r.GetPrincipalVerified() ||
		r.GetReason() != "flaky network" || r.GetSupersededReason() != "RUN_FAILED" || r.GetFromState() != orkstackv1.ResumeFromState_RESUME_FROM_STATE_FAILED ||
		!r.GetAt().AsTime().Equal(f.clock.Now()) {
		t.Fatalf("resume = %v", r)
	}
	if len(f.cluster.resets) != 1 {
		t.Fatalf("%d resets, want exactly one", len(f.cluster.resets))
	}
	reset := f.cluster.resets[0]
	key, ok := engine.ParseResumePlanKey(reset.Reason)
	if !ok || key.Scope != 10 || key.N != 0 || reset.BaseExecutionRunID != "exec-1" || reset.RunID != "ork-r" || reset.WorkflowTaskFinishEventID == 0 {
		t.Fatalf("reset = %+v", reset)
	}
	read, digest, err := f.records.ReadResumePlan(context.Background(), "default", "ork-r", 10, 0)
	if err != nil || read.Answer != artifacts.RecordFound || digest != key.PlanDigest {
		t.Fatalf("the plan's record = %v %s, %v; want the one the reset names", read.Answer, digest, err)
	}
	rec := read.Value
	if rec.GetBaseExecutionRunId() != "exec-1" || rec.GetBaseTerminalSequence() != 9 || rec.GetPlanDigest() != preview.GetPlanDigest() ||
		rec.GetRScheduledEventId() == 0 || rec.GetRStartedEventId() == 0 || rec.GetPreviousScope() != 0 || rec.GetBlock().GetScope() != 10 ||
		rec.GetBlock().GetSeqBase() != 10 || rec.GetBlock().GetResumesBefore() != 0 || !reset.Deadline.Equal(rec.GetExpiresAt().AsTime()) ||
		!rec.GetExpiresAt().AsTime().After(rec.GetAt().AsTime()) {
		t.Fatalf("record = %v, reset deadline %s", rec, reset.Deadline)
	}
	scope, link, err := f.records.NewestResume(context.Background(), "default", "ork-r")
	if err != nil || scope != 10 || link.Value.N != 0 || len(link.Value.Tasks) != 1 || link.Value.Tasks[0] != "b" {
		t.Fatalf("identity's newest resume = %d %+v, %v; want scope 10 of task b", scope, link, err)
	}
}

// TestResumeRun_RepeatedConfirm: the same confirm sent again answers the
// resume it made, without a second reset; another plan's, or one of another
// ending, is told the run changed.
func TestResumeRun_RepeatedConfirm(t *testing.T) {
	f := newResumeFixture(t, nil)
	f.resets(true)
	digest := f.preview(t).GetPlanDigest()
	same := func(r *orkstackv1.ResumeRunRequest) { r.ExpectedPlanDigest = digest }
	first, err := f.confirm(t, same)
	if err != nil {
		t.Fatal(err)
	}
	again, err := f.confirm(t, same)
	if err != nil || !again.GetAlreadyResumed() || !proto.Equal(again.GetResume(), first.GetResume()) || again.GetSeqBase() != 10 {
		t.Fatalf("again = %v, %v; want the resume already made", again, err)
	}
	for name, change := range map[string]func(*orkstackv1.ResumeRunRequest){
		"another plan":   func(r *orkstackv1.ResumeRunRequest) { r.ExpectedPlanDigest = strings.Repeat("ab", 32) },
		"another task":   func(r *orkstackv1.ResumeRunRequest) { r.FromTaskId = "a" },
		"another ending": func(r *orkstackv1.ResumeRunRequest) { r.ExpectedTerminalSequence = 7 },
	} {
		_, err := f.confirm(t, change)
		if r := refusalOf(t, err); r.GetReason() != "RUN_CHANGED" {
			t.Errorf("%s: refusal = %v, want RUN_CHANGED", name, r)
		}
	}
	if len(f.cluster.resets) != 1 {
		t.Fatalf("%d resets, want exactly one", len(f.cluster.resets))
	}
	// A preview of a running run is of no resumable state.
	_, err = f.resume(&orkstackv1.ResumeRunRequest{DryRun: true})
	if r := refusalOf(t, err); r.GetReason() != "STATE_NOT_RESUMABLE" {
		t.Fatalf("preview = %v", r)
	}
}

// TestResumeRun_NotCarriedOn: the reset was made and the run did not go on in
// the wait: unconfirmed, with a cause that says the reset is known.
func TestResumeRun_NotCarriedOn(t *testing.T) {
	f := newResumeFixture(t, quickConfirm)
	f.resets(false)
	_, err := f.confirm(t, nil)
	r := refusalOf(t, err)
	if r.GetReason() != "RESUME_UNCONFIRMED" || r.GetCause() != "not_carried_on" || r.GetRetryAfterSeconds() <= 0 ||
		r.GetClass() != orkstackv1.ResumeRefusalClass_RESUME_REFUSAL_CLASS_WAIT {
		t.Fatalf("refusal = %v", r)
	}
	// It goes on later: the same request is then answered.
	f.runReader.set(10)
	again, err := f.confirm(t, nil)
	if err != nil || !again.GetAlreadyResumed() || len(f.cluster.resets) != 1 {
		t.Fatalf("again = %v, %v, %d resets", again, err, len(f.cluster.resets))
	}
}

// TestResumeRun_LostResetResponseNoSecondReset: a reset whose answer was lost
// is never sent again while it may still be applied: unconfirmed, then found
// by the base's own record of it.
func TestResumeRun_LostResetResponseNoSecondReset(t *testing.T) {
	f := newResumeFixture(t, nil)
	f.resets(true)
	apply := f.cluster.onReset
	f.cluster.onReset = func(r engine.ResumeReset) (string, error) {
		_, _ = apply(r)
		return "", fmt.Errorf("%w: connection reset", engine.ErrResetOutcomeUnknown)
	}
	_, err := f.confirm(t, nil)
	r := refusalOf(t, err)
	if r.GetReason() != "RESUME_UNCONFIRMED" || r.GetCause() != "reset_outcome_unknown" || r.GetRetryAfterSeconds() < 60 {
		t.Fatalf("refusal = %v", r)
	}
	again, err := f.confirm(t, nil)
	if err != nil || !again.GetAlreadyResumed() || len(f.cluster.resets) != 1 {
		t.Fatalf("again = %v, %v, %d resets; want the one already applied", again, err, len(f.cluster.resets))
	}
}

// TestResumeRun_LostResetSentAgainOnlyAfterItsWindow: a reset that was never
// applied holds its run until nothing sent under its deadline can land any
// more; then the same plan is sent once more, under a fresh deadline, and
// only while it is still the plan.
func TestResumeRun_LostResetSentAgainOnlyAfterItsWindow(t *testing.T) {
	f := newResumeFixture(t, nil)
	lost := func(engine.ResumeReset) (string, error) { return "", engine.ErrResetOutcomeUnknown }
	f.cluster.onReset = lost
	_, err := f.confirm(t, nil)
	refusalOf(t, err)
	first := f.cluster.resets[0]
	held := func(when string) {
		t.Helper()
		_, err := f.confirm(t, nil)
		if r := refusalOf(t, err); r.GetReason() != "RESUME_IN_PROGRESS" || r.GetRetryAfterSeconds() <= 0 {
			t.Fatalf("%s: refusal = %v, want RESUME_IN_PROGRESS with when", when, r)
		}
	}
	held("at once")
	f.clock.set(first.Deadline.Add(60*time.Second - time.Millisecond))
	held("just before its window ends")
	f.clock.set(first.Deadline.Add(60 * time.Second))
	_, err = f.confirm(t, nil)
	if r := refusalOf(t, err); r.GetReason() != "RESUME_UNCONFIRMED" || len(f.cluster.resets) != 2 {
		t.Fatalf("refusal = %v, %d resets; want the plan sent again", r, len(f.cluster.resets))
	}
	second := f.cluster.resets[1]
	if second.Reason != first.Reason || !second.Deadline.After(f.clock.Now()) {
		t.Fatalf("sent %q until %s, then %q until %s; want the same plan under a fresh deadline", first.Reason, first.Deadline, second.Reason, second.Deadline)
	}
	held("after the second")
	f.clock.set(second.Deadline.Add(60 * time.Second))
	f.resets(true)
	got, err := f.confirm(t, nil)
	if err != nil || got.GetAlreadyResumed() || got.GetSeqBase() != 10 || len(f.cluster.resets) != 3 {
		t.Fatalf("third = %v, %v", got, err)
	}
	if refs, err := f.records.ListResumePlans(context.Background(), "default", "ork-r"); err != nil || len(refs) != 1 {
		t.Fatalf("plans = %v, %v; want the first alone", refs, err)
	}
}

// TestResumeRun_Stranded: a lost plan that is no longer what would be planned
// can be neither sent again nor replaced, and the answer is final.
func TestResumeRun_Stranded(t *testing.T) {
	for name, change := range map[string]func(*testing.T, *resumeFixture) func(*orkstackv1.ResumeRunRequest){
		"its sequence moved": func(t *testing.T, f *resumeFixture) func(*orkstackv1.ResumeRunRequest) { f.snapshot(t, 11); return nil },
		"another task is asked for": func(*testing.T, *resumeFixture) func(*orkstackv1.ResumeRunRequest) {
			return func(r *orkstackv1.ResumeRunRequest) { r.FromTaskId = "a" }
		},
	} {
		t.Run(name, func(t *testing.T) {
			f := newResumeFixture(t, nil)
			f.cluster.onReset = func(engine.ResumeReset) (string, error) { return "", engine.ErrResetOutcomeUnknown }
			_, _ = f.confirm(t, nil)
			f.clock.set(f.cluster.resets[0].Deadline.Add(60 * time.Second))
			_, err := f.confirm(t, change(t, f))
			if r := refusalOf(t, err); r.GetReason() != "RESUME_PLAN_STRANDED" ||
				r.GetClass() != orkstackv1.ResumeRefusalClass_RESUME_REFUSAL_CLASS_PERMANENT || len(f.cluster.resets) != 1 {
				t.Fatalf("refusal = %v, %d resets", r, len(f.cluster.resets))
			}
		})
	}
}

// unlinkedRecords is a record store that stops answering before identity's
// link is written, as a crash between the two writes leaves it.
type unlinkedRecords struct{ *artifacts.RecordStore }

func (unlinkedRecords) WriteResume(context.Context, artifacts.ResumeRecord) error {
	return errors.New("connection refused")
}

// TestResumeRun_InterruptedBeforeTheLink: a plan that was recorded and whose
// resume never began resets nothing, and is followed by another plan once its
// own time has passed.
func TestResumeRun_InterruptedBeforeTheLink(t *testing.T) {
	var real *artifacts.RecordStore
	f := newResumeFixture(t, func(f *resumeFixture) {
		real = f.records.(*artifacts.RecordStore)
		f.records = unlinkedRecords{real}
	})
	_, err := f.confirm(t, nil)
	if r := refusalOf(t, err); r.GetReason() != "RESUME_CHECK_UNAVAILABLE" || len(f.cluster.resets) != 0 {
		t.Fatalf("refusal = %v, %d resets", r, len(f.cluster.resets))
	}
	read, _, err := real.ReadResumePlan(context.Background(), "default", "ork-r", 10, 0)
	if err != nil || read.Answer != artifacts.RecordFound {
		t.Fatalf("record = %v, %v", read.Answer, err)
	}
	_, err = f.confirm(t, nil)
	if r := refusalOf(t, err); r.GetReason() != "RESUME_IN_PROGRESS" {
		t.Fatalf("refusal = %v, want RESUME_IN_PROGRESS", r)
	}
	next := newResumeFixture(t, func(g *resumeFixture) { g.records = real })
	next.clock.set(read.Value.GetExpiresAt().AsTime().Add(60 * time.Second))
	next.resets(true)
	got, err := next.confirm(t, nil)
	if err != nil || len(next.cluster.resets) != 1 {
		t.Fatalf("= %v, %v", got, err)
	}
	if key, _ := engine.ParseResumePlanKey(next.cluster.resets[0].Reason); key.Scope != 10 || key.N != 1 {
		t.Fatalf("reset under %+v, want plan 1 of scope 10", key)
	}
}

// TestResumeRun_BaseMoves: a base that is reset or replaced between the plan
// and the reset is never reset again.
func TestResumeRun_BaseMoves(t *testing.T) {
	for name, move := range map[string]func(*resumeFixture){
		"reset by hand": func(f *resumeFixture) {
			e := f.cluster.executions["exec-1"]
			e.ResetRunID = "exec-9"
			f.cluster.executions["exec-1"] = e
		},
		"no longer current": func(f *resumeFixture) {
			f.cluster.executions[""] = engine.ResumeExecution{ExecutionRunID: "exec-9", Status: enumspb.WORKFLOW_EXECUTION_STATUS_FAILED}
		},
	} {
		t.Run(name, func(t *testing.T) {
			f := newResumeFixture(t, nil)
			f.resets(true)
			// After the plan is made and recorded: as the claim is taken.
			f.claims.onClaim = func() { move(f) }
			_, err := f.confirm(t, nil)
			if r := refusalOf(t, err); r.GetReason() != "RUN_CHANGED" || len(f.cluster.resets) != 0 {
				t.Fatalf("refusal = %v, %d resets; want RUN_CHANGED and none", r, len(f.cluster.resets))
			}
		})
	}
}

// TestResumeRun_ClaimHeldElsewhere: another request that claimed this plan's
// reset holds it, and this one resets nothing.
func TestResumeRun_ClaimHeldElsewhere(t *testing.T) {
	f := newResumeFixture(t, nil)
	f.resets(true)
	f.claims.answer = &engine.Claim{RunID: "another-request", Snapshot: []byte(f.clock.Now().Add(time.Minute).Format(time.RFC3339Nano))}
	_, err := f.confirm(t, nil)
	if r := refusalOf(t, err); r.GetReason() != "RESUME_IN_PROGRESS" || len(f.cluster.resets) != 0 {
		t.Fatalf("refusal = %v, %d resets", r, len(f.cluster.resets))
	}
	// A claim store that does not answer resets nothing either.
	g := newResumeFixture(t, nil)
	g.resets(true)
	g.claims.err = errors.New("unreachable")
	_, err = g.confirm(t, nil)
	if r := refusalOf(t, err); r.GetReason() != "RESUME_CHECK_UNAVAILABLE" || len(g.cluster.resets) != 0 {
		t.Fatalf("refusal = %v, %d resets", r, len(g.cluster.resets))
	}
}

// TestResumeRun_ResetRefused: a reset the cluster refused before applying it
// is said as such, and is not one whose outcome is unknown.
func TestResumeRun_ResetRefused(t *testing.T) {
	f := newResumeFixture(t, nil)
	f.cluster.onReset = func(engine.ResumeReset) (string, error) { return "", errors.New("not allowed") }
	_, err := f.confirm(t, nil)
	if connect.CodeOf(err) != connect.CodeInternal || strings.Contains(err.Error(), "not allowed") {
		t.Fatalf("= %v, want Internal, with the cluster's own words in the log alone", err)
	}
	gone := newResumeFixture(t, nil)
	gone.cluster.onReset = func(engine.ResumeReset) (string, error) { return "", engine.ErrResumeExecutionNotFound }
	if _, err := gone.confirm(t, nil); refusalOf(t, err).GetReason() != "HISTORY_UNAVAILABLE" {
		t.Fatalf("a base that is gone = %v, want HISTORY_UNAVAILABLE", err)
	}
}

// TestResumeRun_ResumedAgain: the resume of a run already resumed names the
// resume before it, in its plan and in identity's scopes.
func TestResumeRun_ResumedAgain(t *testing.T) {
	f := newResumeFixture(t, nil)
	f.resets(true)
	if _, err := f.confirm(t, nil); err != nil {
		t.Fatal(err)
	}
	// The resumed run fails at sequence 15.
	f.reader.mu.Lock()
	f.run.Status = orkstackv1.Status_STATUS_FAILED
	f.run.TaskRuns[1].ResultEpoch = 10
	f.reader.mu.Unlock()
	f.runReader.set(15)
	ended := f.cluster.executions[""]
	ended.Status, ended.CloseTime = enumspb.WORKFLOW_EXECUTION_STATUS_FAILED, resumeEpoch.Add(2*time.Hour)
	f.cluster.executions[""], f.cluster.executions[ended.ExecutionRunID] = ended, ended
	f.cluster.histories[ended.ExecutionRunID] = failedHistory(t, "a", "b")
	got, err := f.confirm(t, func(r *orkstackv1.ResumeRunRequest) { r.ExpectedTerminalSequence = 15 })
	if err != nil || got.GetSeqBase() != 16 || got.GetResumesUsed() != 1 {
		t.Fatalf("second = %v, %v", got, err)
	}
	read, _, err := f.records.ReadResumePlan(context.Background(), "default", "ork-r", 16, 0)
	if err != nil || read.Value.GetPreviousScope() != 10 || read.Value.GetBlock().GetPreviousSeqBase() != 10 || read.Value.GetBlock().GetResumesBefore() != 1 {
		t.Fatalf("second plan = %v, %v", read.Value, err)
	}
	if scope, _, err := f.records.NewestResume(context.Background(), "default", "ork-r"); err != nil || scope != 16 {
		t.Fatalf("newest = %d, %v", scope, err)
	}
}

// TestResumeRun_ResumeEndedAtOnce: a reset that was made and whose run could
// not carry its plan out is never answered as a resume that worked, whether
// its ending was published or not.
func TestResumeRun_ResumeEndedAtOnce(t *testing.T) {
	for name, tc := range map[string]struct {
		code     string
		sequence int64
	}{
		"with its event": {engine.ReasonResumePlanInvalid, 10},
		"with no event":  {engine.ReasonResumePlanUnreadable, 9},
	} {
		t.Run(name, func(t *testing.T) {
			f := newResumeFixture(t, nil)
			f.resets(false)
			apply := f.cluster.onReset
			f.cluster.onReset = func(r engine.ResumeReset) (string, error) {
				id, err := apply(r)
				f.reader.mu.Lock()
				f.run.Status, f.run.Error = orkstackv1.Status_STATUS_FAILED, &orkstackv1.Error{Code: tc.code}
				f.reader.mu.Unlock()
				f.runReader.set(tc.sequence)
				return id, err
			}
			for range 2 {
				_, err := f.confirm(t, nil)
				r := refusalOf(t, err)
				if r.GetReason() != "RUN_CHANGED" || r.GetCause() != "resume_ended" || !strings.Contains(r.GetMessage(), tc.code) {
					t.Fatalf("refusal = %v, want RUN_CHANGED resume_ended naming %s", r, tc.code)
				}
			}
			if len(f.cluster.resets) != 1 {
				t.Fatalf("%d resets", len(f.cluster.resets))
			}
		})
	}
}

// TestResumeRun_ResumeAgainAfterAPlanThatCouldNotRun: a run whose resume could
// not carry its plan out is as its base was: it can be resumed again, and the
// resume that never got going counts as none.
func TestResumeRun_ResumeAgainAfterAPlanThatCouldNotRun(t *testing.T) {
	for _, code := range []string{engine.ReasonResumePlanInvalid, engine.ReasonResumePlanUnreadable} {
		t.Run(code, func(t *testing.T) {
			f := newResumeFixture(t, nil)
			f.resets(true)
			if _, err := f.confirm(t, nil); err != nil {
				t.Fatal(err)
			}
			f.reader.mu.Lock()
			f.run.Status, f.run.Error = orkstackv1.Status_STATUS_FAILED, &orkstackv1.Error{Code: code}
			f.reader.mu.Unlock()
			ended := f.cluster.executions[""]
			ended.Status, ended.CloseTime = enumspb.WORKFLOW_EXECUTION_STATUS_FAILED, resumeEpoch.Add(2*time.Hour)
			f.cluster.executions[""], f.cluster.executions[ended.ExecutionRunID] = ended, ended
			// It dispatched nothing; its rows are its base's, which did.
			f.cluster.histories[ended.ExecutionRunID] = failedHistory(t)
			for _, row := range f.run.TaskRuns {
				row.Attempt = 1
			}
			preview := f.preview(t)
			if preview.GetResumesUsed() != 0 || preview.GetTerminalSequence() != 10 {
				t.Fatalf("preview = %v, want none counted", preview)
			}
			got, err := f.confirm(t, func(r *orkstackv1.ResumeRunRequest) {
				r.ExpectedTerminalSequence, r.ExpectedPlanDigest = 10, preview.GetPlanDigest()
			})
			if err != nil || got.GetAlreadyResumed() || got.GetSeqBase() != 11 {
				t.Fatalf("second = %v, %v", got, err)
			}
		})
	}
}

// TestResumeRun_ResetUnderAPlanWhoseRecordIsGone: a run reset under a plan
// that can no longer be read is resumed from the scope that reset named, not
// refused for good.
func TestResumeRun_ResetUnderAPlanWhoseRecordIsGone(t *testing.T) {
	f := newResumeFixture(t, func(f *resumeFixture) {
		f.cluster.resetKeys = map[string]string{"exec-1": engine.ResumePlanKey{Scope: 4, N: 0, PlanDigest: strings.Repeat("ab", 32)}.String()}
	})
	if err := f.records.WriteResume(context.Background(), artifacts.ResumeRecord{Namespace: "default", RunID: "ork-r", Scope: 4, Tasks: []string{"b"}, ReopenedAwaits: []string{}}); err != nil {
		t.Fatal(err)
	}
	f.resets(true)
	if _, err := f.confirm(t, nil); err != nil {
		t.Fatal(err)
	}
	read, _, err := f.records.ReadResumePlan(context.Background(), "default", "ork-r", 10, 0)
	if err != nil || read.Value.GetPreviousScope() != 4 {
		t.Fatalf("plan = %v, %v; want the scope before to be 4", read.Value, err)
	}
}

// TestResumeRun_ResetByHandAfterAResume: a run resumed and then reset by hand
// carries on from the resume identity knows as the newest.
func TestResumeRun_ResetByHandAfterAResume(t *testing.T) {
	f := newResumeFixture(t, func(f *resumeFixture) {
		f.cluster.resetKeys = map[string]string{"exec-1": "an operator's reset"}
	})
	old := &orkstackv1.ResumePlanRecord{Format: artifacts.ResumePlanFormat, Namespace: "default", RunId: "ork-r", Scope: 4, BaseExecutionRunId: "exec-0",
		ExpiresAt: timestamppb.New(resumeEpoch), Block: &orkstackv1.ResumeBlock{SeqBase: 4, Scope: 4}}
	if _, err := f.records.WriteResumePlan(context.Background(), old); err != nil {
		t.Fatal(err)
	}
	if err := f.records.WriteResume(context.Background(), artifacts.ResumeRecord{Namespace: "default", RunID: "ork-r", Scope: 4, Tasks: []string{"b"}, ReopenedAwaits: []string{}}); err != nil {
		t.Fatal(err)
	}
	f.resets(true)
	if _, err := f.confirm(t, nil); err != nil {
		t.Fatal(err)
	}
	if scope, _, err := f.records.NewestResume(context.Background(), "default", "ork-r"); err != nil || scope != 10 {
		t.Fatalf("newest = %d, %v", scope, err)
	}
}

// TestResumeRun_ScopeRecordsLost: a run whose plan is there and whose scope is
// not is refused for good, never told to wait for what cannot come.
func TestResumeRun_ScopeRecordsLost(t *testing.T) {
	digest := ""
	f := newResumeFixture(t, func(f *resumeFixture) {
		rec := &orkstackv1.ResumePlanRecord{Format: artifacts.ResumePlanFormat, Namespace: "default", RunId: "ork-r", Scope: 4, BaseExecutionRunId: "exec-0",
			Block: &orkstackv1.ResumeBlock{SeqBase: 4, Scope: 4}}
		var err error
		if digest, err = f.records.WriteResumePlan(context.Background(), rec); err != nil {
			t.Fatal(err)
		}
		f.cluster.resetKeys = map[string]string{"exec-1": engine.ResumePlanKey{Scope: 4, N: 0, PlanDigest: digest}.String()}
	})
	_, err := f.confirm(t, nil)
	if r := refusalOf(t, err); r.GetReason() != "STATE_UNREADABLE" || r.GetClass() != orkstackv1.ResumeRefusalClass_RESUME_REFUSAL_CLASS_PERMANENT || len(f.cluster.resets) != 0 {
		t.Fatalf("refusal = %v, %d resets", r, len(f.cluster.resets))
	}
}

// resumeServer is a cluster that applies a reset only before its deadline, as
// the frontend does, and only when it is delivered: a test holds each sending
// and delivers it when it chooses.
type resumeServer struct {
	f       *resumeFixture
	held    []engine.ResumeReset
	applied int
	// lose makes each sending's answer lost, held for later delivery.
	lose bool
}

func (s *resumeServer) deliver(r engine.ResumeReset) (string, error) {
	if !s.f.clock.Now().Before(r.Deadline) {
		return "", engine.ErrResetOutcomeUnknown // past its deadline: not applied
	}
	s.applied++
	return s.apply(r)
}

func (s *resumeServer) apply(r engine.ResumeReset) (string, error) {
	return s.f.applies(true)(r)
}

func (s *resumeServer) receive(r engine.ResumeReset) (string, error) {
	if s.lose {
		s.held = append(s.held, r)
		return "", engine.ErrResetOutcomeUnknown
	}
	return s.deliver(r)
}

// TestResumeRun_OneResetHoweverSendsInterleave: a plan sent twice is applied
// once, wherever the first sending is delivered: while it still holds its run,
// as the second is prepared, as it is claimed, as it is sent, and after.
func TestResumeRun_OneResetHoweverSendsInterleave(t *testing.T) {
	for name, at := range map[string]string{
		"in its window": "window", "as the second is claimed": "claim", "as the second is sent": "send", "after the second": "after", "never": "never",
	} {
		t.Run(name, func(t *testing.T) {
			f := newResumeFixture(t, nil)
			s := &resumeServer{f: f, lose: true}
			f.cluster.onReset = s.receive
			_, err := f.confirm(t, nil)
			if r := refusalOf(t, err); r.GetReason() != "RESUME_UNCONFIRMED" {
				t.Fatalf("first = %v", r)
			}
			s.lose = false
			first := s.held[0]
			if at == "window" {
				// Delivered in time, its answer lost: the base says so.
				if _, err := s.deliver(first); err != nil {
					t.Fatal(err)
				}
			}
			f.clock.set(first.Deadline.Add(60 * time.Second))
			if at == "claim" {
				f.claims.onClaim = func() { _, _ = s.deliver(first) }
			}
			if at == "send" {
				f.cluster.onReset = func(r engine.ResumeReset) (string, error) {
					_, _ = s.deliver(first)
					return s.receive(r)
				}
			}
			if _, err := f.confirm(t, nil); err != nil {
				t.Fatalf("second = %v", err)
			}
			if at == "after" {
				_, _ = s.deliver(first)
			}
			if s.applied != 1 {
				t.Fatalf("%d resets applied, want exactly one", s.applied)
			}
			again, err := f.confirm(t, nil)
			if err != nil || !again.GetAlreadyResumed() || s.applied != 1 {
				t.Fatalf("again = %v, %v, %d applied", again, err, s.applied)
			}
		})
	}
}

// TestResumeRun_FirstSendLandsAsTheSecondIsPrepared: were a sending to land
// past its window after all, the base's own record of it is seen before the
// second is sent: nothing is reset twice.
func TestResumeRun_FirstSendLandsAsTheSecondIsPrepared(t *testing.T) {
	f := newResumeFixture(t, nil)
	s := &resumeServer{f: f, lose: true}
	f.cluster.onReset = s.receive
	_, _ = f.confirm(t, nil)
	s.lose = false
	f.clock.set(s.held[0].Deadline.Add(60 * time.Second))
	f.claims.onClaim = func() { s.applied++; _, _ = s.apply(s.held[0]) }
	_, err := f.confirm(t, nil)
	if r := refusalOf(t, err); r.GetReason() != "RUN_CHANGED" || s.applied != 1 || len(f.cluster.resets) != 1 {
		t.Fatalf("second = %v, %d applied, %d sent", r, s.applied, len(f.cluster.resets))
	}
	f.claims.onClaim = nil
	again, err := f.confirm(t, nil)
	if err != nil || !again.GetAlreadyResumed() || s.applied != 1 {
		t.Fatalf("again = %v, %v", again, err)
	}
}

// TestResumeFixture_AReadRunIsItsReaders: a run the fixture's reader answered
// stays as it was read while a reset changes the run, as the real reader's
// does, which builds one for each read.
func TestResumeFixture_AReadRunIsItsReaders(t *testing.T) {
	f := newResumeFixture(t, nil)
	f.resets(true)
	read, sequence, err := f.runReader.GetRunAndSequence(context.Background(), "ork-r")
	must.OK(t, err)
	alone, err := f.runReader.GetRun(context.Background(), "ork-r")
	must.OK(t, err)
	done := make(chan error, 1)
	go func() {
		_, err := f.cluster.ResetForResume(context.Background(), engine.ResumeReset{BaseExecutionRunID: "exec-1",
			Reason: engine.ResumePlanKey{Scope: 10, PlanDigest: strings.Repeat("a", 64)}.String()})
		done <- err
	}()
	during := []orkstackv1.Status{read.GetStatus(), alone.GetStatus()} // unguarded, as a request reads its run
	must.OK(t, <-done)
	for i, run := range []*orkstackv1.Run{read, alone} {
		if run.GetStatus() != orkstackv1.Status_STATUS_FAILED || during[i] != orkstackv1.Status_STATUS_FAILED {
			t.Errorf("run %d was read %v, and is %v after a reset", i, during[i], run.GetStatus())
		}
	}
	if sequence != 9 {
		t.Errorf("sequence = %d", sequence)
	}
	now, sequence, err := f.runReader.GetRunAndSequence(context.Background(), "ork-r")
	must.OK(t, err)
	if now.GetStatus() != orkstackv1.Status_STATUS_RUNNING || sequence != 10 {
		t.Errorf("read again = %v at %d, want it running at 10", now.GetStatus(), sequence)
	}
}

// TestResumeRun_ConcurrentConfirmsOneReset: confirms made at the same moment
// reset the run once; every other is told to wait, or answered by the one
// that did it.
func TestResumeRun_ConcurrentConfirmsOneReset(t *testing.T) {
	f := newResumeFixture(t, func(f *resumeFixture) { f.settings.ConfirmsPerMinute = 100 })
	s := &resumeServer{f: f}
	f.cluster.onReset = s.receive
	const callers = 8
	results := make(chan error, callers)
	start := make(chan struct{})
	for range callers {
		go func() {
			<-start
			_, err := f.resume(&orkstackv1.ResumeRunRequest{ExpectedTerminalSequence: 9})
			results <- err
		}()
	}
	close(start)
	made := 0
	for range callers {
		err := <-results
		if err == nil {
			made++
			continue
		}
		if r := refusalOf(t, err); r.GetReason() != "RESUME_IN_PROGRESS" && r.GetReason() != "RUN_CHANGED" && r.GetReason() != "STATE_NOT_RESUMABLE" {
			t.Errorf("refusal = %v", r)
		}
	}
	if s.applied != 1 || made == 0 {
		t.Fatalf("%d resets applied and %d confirms answered, want one and at least one", s.applied, made)
	}
}
