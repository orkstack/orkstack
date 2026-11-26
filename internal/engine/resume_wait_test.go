package engine_test

import (
	"slices"
	"strings"
	"testing"
	"time"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/internal/testsupport/must"
	"github.com/stretchr/testify/mock"
)

const waitAgain = orkstackv1.ResumeDisposition_RESUME_DISPOSITION_WAIT

const resumeWaitDocument = `
apiVersion: orkstack/v1
kind: Orkflow
name: resume-wait
tasks:
  - id: build
    uses: shell
    with: {op: exec, command: "echo build"}
  - id: approve
    needs: [build]
    await: {timeout_seconds: 3600}
  - id: settle
    needs: [build]
    sleep: {seconds: 600}
  - id: deploy
    uses: shell
    needs: [approve, settle]
    with: {op: exec, command: "echo deploy"}
`

func (h *resetHarness) waitPlan(t *testing.T, document string) {
	t.Helper()
	h.resume(t, document, nil, unit("build", -1, keepIt), unit("approve", -1, waitAgain), unit("settle", -1, waitAgain), unit("deploy", -1, rerunIt))
}

// An await and a sleep that wait again do so from the resume, in full, and
// only once the results kept are in: an answer before that is refused as any
// answer to an await not open yet, and the one after it goes on.
func TestResetRun_AnAwaitAndASleepWaitAgain(t *testing.T) {
	h := newResetHarness(t)
	h.waitPlan(t, resumeWaitDocument)
	h.signalAt(30*time.Minute, completion("approve", `{"ok":true}`, "ana@example.com"))
	must.OKf(t, h.runResetOf(t, resumeWaitDocument), "the resumed run")

	var kept, awaiting, sleeping *orkstackv1.Event
	for _, e := range eventsOf(h.harness, "ork-reset-1") {
		switch e.GetType() {
		case orkstackv1.EventType_EVENT_TYPE_RUN_RESULTS_KEPT:
			kept = e
		case orkstackv1.EventType_EVENT_TYPE_TASK_AWAITING:
			awaiting = e
		case orkstackv1.EventType_EVENT_TYPE_TASK_SLEEPING:
			sleeping = e
		}
	}
	if kept == nil || awaiting == nil || sleeping == nil || awaiting.GetSequence() <= kept.GetSequence() || sleeping.GetSequence() <= kept.GetSequence() {
		t.Fatalf("kept %v, awaiting %v, sleeping %v: want both to wait after the results kept", kept, awaiting, sleeping)
	}
	run := h.ended(t)
	approve, nap := taskRunOfRun(run, "approve"), taskRunOfRun(run, "settle")
	if approve.GetStatus() != orkstackv1.Status_STATUS_SUCCEEDED || approve.GetAwait().GetResolvedBy() != "ana@example.com" ||
		approve.GetAwait().GetDeadline().AsTime().Sub(approve.GetStartedAt().AsTime()) != time.Hour || approve.GetAdopted() || approve.GetResultEpoch() != 42 {
		t.Errorf("approve = %v, want resolved in a fresh hour, under scope 42", approve)
	}
	if nap.GetStatus() != orkstackv1.Status_STATUS_SUCCEEDED || nap.GetFinishedAt().AsTime().Sub(nap.GetStartedAt().AsTime()) < 10*time.Minute {
		t.Errorf("settle = %v, want slept in full", nap)
	}
	if n := len(h.taskCalls("build")); n != 0 || len(h.taskCalls("deploy")) != 1 {
		t.Errorf("build ran %d times, deploy %d", n, len(h.taskCalls("deploy")))
	}
}

// An answer that comes while the kept results are read back answers no wait:
// the await is not open, and the run says it was ignored.
func TestResetRun_AnAnswerDuringTheReadBackIsIgnored(t *testing.T) {
	h := newResetHarness(t)
	h.waitPlan(t, resumeWaitDocument)
	h.adopting = func() error {
		h.env.SignalWorkflow(engine.ResolveAwaitSignal, completion("approve", `{"ok":true}`, "early@example.com"))
		return nil
	}
	h.signalAt(30*time.Minute, completion("approve", `{"ok":true}`, "ana@example.com"))
	must.OKf(t, h.runResetOf(t, resumeWaitDocument), "the resumed run")
	if row := taskRunOfRun(h.ended(t), "approve"); row.GetAwait().GetResolvedBy() != "ana@example.com" {
		t.Fatalf("approve = %v, want the answer given while it was open", row.GetAwait())
	}
	ignored := slices.ContainsFunc(eventsFor(h.harness, "ork-reset-1", "approve"), func(e *orkstackv1.Event) bool {
		return strings.Contains(e.GetMessage(), "resolution ignored: task is PENDING") && strings.Contains(e.GetMessage(), "early@example.com")
	})
	if !ignored {
		t.Fatal("no event says the early answer was ignored")
	}
}

// A run timeout bounds a wait that opens again by its fresh clock.
func TestResetRun_ARunTimeoutEndsAnAwaitThatWaitsAgain(t *testing.T) {
	h := newResetHarness(t)
	document := strings.Replace(resumeWaitDocument, "\nname: ", "\ntimeout: 30m\nname: ", 1)
	h.waitPlan(t, document)
	h.runTimeout = &engine.RunTimeoutPlan{HandlerGraceSeconds: 60, ResolutionsPerPass: engine.DefaultResolutionsPerPass}
	if err := h.runResetOf(t, document); err == nil {
		t.Fatal("the run outlived its timeout")
	}
	run := h.ended(t)
	if row := taskRunOfRun(run, "approve"); run.GetError().GetCode() != engine.RunTimedOutCode || row.GetError().GetCode() != engine.TaskStoppedByRunTimeoutCode {
		t.Fatalf("run = %v, approve = %v, want both stopped at the run's timeout", run.GetError(), row.GetError())
	}
}

// One that nobody answers again times out, at the fresh deadline.
func TestResetRun_AnAwaitThatWaitsAgainTimesOut(t *testing.T) {
	h := newResetHarness(t)
	h.waitPlan(t, resumeWaitDocument)
	if err := h.runResetOf(t, resumeWaitDocument); err == nil {
		t.Fatal("the run went on without an answer")
	}
	if row := taskRunOfRun(h.ended(t), "approve"); row.GetError().GetCode() != engine.AwaitTimedOutCode {
		t.Fatalf("approve = %v, want timed out", row)
	}
}

// A read-back failure ends what was to wait again as any row not started, and
// opens nothing.
func TestResetRun_AReadBackFailureOpensNoWait(t *testing.T) {
	h := newResetHarness(t)
	h.resume(t, resumeWaitDocument, []string{"build"}, unit("build", -1, keepIt), unit("approve", -1, waitAgain), unit("settle", -1, waitAgain), unit("deploy", -1, rerunIt))
	if err := h.runResetOf(t, resumeWaitDocument); err == nil {
		t.Fatal("the run succeeded without its kept result")
	}
	for _, id := range []string{"approve", "settle"} {
		if row := taskRunOfRun(h.ended(t), id); row.GetStatus() != orkstackv1.Status_STATUS_CANCELED || row.GetStartedAt() != nil {
			t.Errorf("%s = %v, want canceled, never started", id, row)
		}
	}
}

// A resume gives the run's timeout a fresh clock: the base's deadline has
// passed when it goes on, and the new one is a whole timeout from there.
func TestResetRun_AResumeRestartsTheRunTimeout(t *testing.T) {
	h := newResetHarness(t)
	document := strings.Replace(resumeTwoTaskDocument, "\nname: ", "\ntimeout: 3h\nname: ", 1)
	h.keepBuildRerunDeploy(t, true)
	h.pack(t, document, keptUnit("build"), rerunUnit("deploy"))
	h.runTimeout = &engine.RunTimeoutPlan{HandlerGraceSeconds: 60, ResolutionsPerPass: engine.DefaultResolutionsPerPass}
	publish := engine.NewEventActivities(h.bus, h.store).WithSnapshotStore(h.snapshots).PublishEvent
	args := []any{mock.Anything, mock.Anything, mock.Anything, mock.Anything}
	// RUN_CREATED and RUN_STARTED each take longer than the timeout.
	h.env.OnActivity(engine.ActivityPublishEvent, args...).After(4 * time.Hour).Return(publish).Twice()
	h.env.OnActivity(engine.ActivityPublishEvent, args...).Return(publish)
	var atResumed *orkstackv1.RunTimeout
	h.onSnapshot = func(run *orkstackv1.Run, sequence int64) error {
		if sequence == 42 {
			atResumed = run.GetTimeout()
		}
		return nil
	}
	must.OKf(t, h.runResetOf(t, document), "the resumed run")

	var started, resumed time.Time
	for _, e := range eventsOf(h.harness, "ork-reset-1") {
		switch e.GetType() {
		case orkstackv1.EventType_EVENT_TYPE_RUN_STARTED:
			started = e.GetTimestamp().AsTime()
		case orkstackv1.EventType_EVENT_TYPE_RUN_RESUMED:
			resumed = e.GetTimestamp().AsTime()
		}
	}
	deadline := h.ended(t).GetTimeout().GetDeadline().AsTime()
	if resumed.Sub(started) < 4*time.Hour || deadline.Before(resumed.Add(3*time.Hour)) {
		t.Fatalf("started %s, resumed %s, deadline %s: want three hours from the resume", started, resumed, deadline)
	}
	if atResumed == nil || atResumed.GetDeadline() != nil || atResumed.GetSeconds() != 3*3600 {
		t.Fatalf("RUN_RESUMED's timeout = %v, want no deadline yet", atResumed)
	}
}

// A resumed run that fails before its clock restarts has its hard end from
// then, not from the base's deadline.
func TestResetRun_AReadBackFailureUnderARunTimeout(t *testing.T) {
	h := newResetHarness(t)
	document := strings.Replace(resumeTwoTaskDocument, "\nname: ", "\ntimeout: 3h\nname: ", 1)
	h.keepBuildRerunDeploy(t, false)
	h.pack(t, document, keptUnit("build"), rerunUnit("deploy"))
	h.runTimeout = &engine.RunTimeoutPlan{HandlerGraceSeconds: 60, ResolutionsPerPass: engine.DefaultResolutionsPerPass}
	if err := h.runResetOf(t, document); err == nil {
		t.Fatal("the run succeeded without its kept result")
	}
	run := h.ended(t)
	if run.GetError().GetCode() == engine.RunTimedOutCode || run.GetTimeout().GetDeadline() != nil || run.GetTimeout().GetHardEndReached() {
		t.Fatalf("run = %v, timeout %v: want its read-back's failure, with no deadline", run.GetError(), run.GetTimeout())
	}
}
