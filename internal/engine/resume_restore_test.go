package engine_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
	"google.golang.org/protobuf/proto"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/internal/testsupport/must"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

const leaveIt = orkstackv1.ResumeDisposition_RESUME_DISPOSITION_UNSPECIFIED

// resumeCarriedDocument carries on once build is done, with approve and settle
// waiting.
const resumeCarriedDocument = `
apiVersion: orkstack/v1
kind: Orkflow
name: resume-carried
tasks:
  - id: build
    uses: shell
    with: {op: exec, command: "echo build"}
  - id: approve
    await: {timeout_seconds: 3600}
  - id: settle
    sleep: {seconds: 600}
  - id: late
    uses: shell
    needs: [build, settle]
    with: {op: exec, command: "echo late"}
  - id: deploy
    uses: shell
    needs: [approve, settle, late]
    with: {op: exec, command: "echo deploy"}
`

// carriedRun is a run that carried on: its stores, and what its next history
// starts from.
type carriedRun struct {
	t         *testing.T
	snapshots *testSnapshotStore
	values    *memValues
	wf        *orkstackv1.Orkflow
	input     *orkstackv1.Payload
	plan      engine.RunPlan
	carry     *orkstackv1.RunContinuation
}

// carryOn runs document until it carries on, after its first task.
func carryOn(t *testing.T, document string, input map[string]any, plan func(*engine.RunPlan)) *carriedRun {
	t.Helper()
	c := &carriedRun{t: t, snapshots: newTestSnapshotStore(), values: &memValues{objects: map[string][]byte{}}}
	var h *harness
	h = newHarnessShared(t, nil, c.snapshots, c.values, func(_ context.Context, in *orkstackv1.ExecuteTaskInput) (*orkstackv1.ExecuteTaskResult, error) {
		// Past the budget once build ends, so it carries on there.
		h.env.SetCurrentHistoryLength(100)
		return succeeded(in.TaskId, map[string]any{"ok": true}), nil
	})
	h.env.SetStartWorkflowOptions(client.StartWorkflowOptions{ID: "ork-reset-1"})
	p := continuedPlan(50)
	p.CommitRoot, p.Cache = "mem://commits", &engine.CachePlan{MaxTTLSeconds: 604800}
	if plan != nil {
		plan(&p)
	}
	h.env.ExecuteWorkflow(engine.RunWorkflowType, "ork-reset-1", compile(t, document), payload(t, input), p, (*orkstackv1.RunContinuation)(nil))
	c.next(h.env.GetWorkflowError())
	return c
}

// next takes what a history that carried on handed the next.
func (c *carriedRun) next(err error) {
	c.t.Helper()
	var can *workflow.ContinueAsNewError
	if !errors.As(err, &can) {
		c.t.Fatalf("the run ended %v, want it to carry on", err)
	}
	var id string
	must.OK(c.t, converter.GetDefaultDataConverter().FromPayloads(can.Input, &id, &c.wf, &c.input, &c.plan, &c.carry))
	// What is resumed has room to end where it is.
	roomy := engine.DefaultContinuationBudget(1<<20, 1<<40, 5*time.Minute)
	c.plan.Continuation = &roomy
}

// reset is a harness for the reset of c's last history, its plan's base having
// ended at 41.
func (c *carriedRun) reset() *resetHarness {
	c.t.Helper()
	h := newResetHarnessOn(c.t, c.snapshots, c.values)
	h.carried = true
	h.env.SetStartWorkflowOptions(client.StartWorkflowOptions{ID: "ork-reset-1"})
	h.baseEnded(c.t, c.wf.GetNamespace())
	return h
}

func (c *carriedRun) run(h *resetHarness) error {
	c.t.Helper()
	h.env.ExecuteWorkflow("ResetRunWorkflow", "", c.wf, c.input, c.plan, c.carry)
	if !h.env.IsWorkflowCompleted() {
		c.t.Fatal("the reset run did not complete")
	}
	return h.env.GetWorkflowError()
}

// endedAt42 asserts err is code and answers the run as its snapshot at 42 says.
func (c *carriedRun) endedAt42(h *resetHarness, err error, code string) *orkstackv1.Run {
	c.t.Helper()
	var app *temporal.ApplicationError
	if !errors.As(err, &app) || app.Type() != code {
		c.t.Fatalf("error = %v, want %s", err, code)
	}
	if len(h.calls) != 0 {
		c.t.Fatalf("%d tasks ran", len(h.calls))
	}
	events := eventsOf(h.harness, "ork-reset-1")
	if len(events) != 1 || events[0].GetSequence() != 42 || events[0].GetType() != orkstackv1.EventType_EVENT_TYPE_RUN_FAILED {
		c.t.Fatalf("events = %v, want its ending alone, at 42", events)
	}
	return h.ended(c.t)
}

func (c *carriedRun) row(id string) *orkstackv1.TaskRun {
	c.t.Helper()
	data, ok := c.snapshots.get(c.carry.GetCheckpoint().GetUri())
	if !ok {
		c.t.Fatal("no saved state")
	}
	saved, err := engine.DecodeCheckpoint(data, c.carry.GetCheckpoint())
	must.OK(c.t, err)
	return taskRunOfRun(saved, id)
}

// baseEnded writes the snapshot a run's base ended with, at 41.
func (h *resetHarness) baseEnded(t *testing.T, namespace string) {
	t.Helper()
	data, err := proto.Marshal(&orkstackv1.Run{Id: "ork-reset-1", Status: orkstackv1.Status_STATUS_FAILED,
		Error:    &orkstackv1.Error{Code: "RUN_FAILED", Message: "deploy failed"},
		TaskRuns: []*orkstackv1.TaskRun{{TaskId: "deploy", Status: orkstackv1.Status_STATUS_FAILED, Attempt: 3}}})
	must.OK(t, err)
	_, err = h.snapshots.WriteSnapshot(context.Background(), namespace, "ork-reset-1", 41, data)
	must.OK(t, err)
}

// canceledAt42 asserts the run ended canceled before anything of its resume
// was carried out: one event, RUN_CANCELED at 42, over its base's rows.
func (h *resetHarness) canceledAt42(t *testing.T, err error) {
	t.Helper()
	var canceled *temporal.CanceledError
	if !errors.As(err, &canceled) || len(h.calls) != 0 {
		t.Fatalf("ended %v having run %d tasks, want canceled", err, len(h.calls))
	}
	events := eventsOf(h.harness, "ork-reset-1")
	if n := len(events); n == 0 || events[n-1].GetSequence() != 42 || events[n-1].GetType() != orkstackv1.EventType_EVENT_TYPE_RUN_CANCELED {
		t.Fatalf("events = %v, want RUN_CANCELED at 42 last", events)
	}
	run := h.ended(t)
	if run.GetStatus() != orkstackv1.Status_STATUS_CANCELED || run.GetError().GetCode() != "RUN_CANCELED" || taskRunOfRun(run, "deploy").GetAttempt() != 3 {
		t.Fatalf("ended %v %v with deploy %v, want canceled over its base's rows", run.GetStatus(), run.GetError(), taskRunOfRun(run, "deploy"))
	}
}

// A resume canceled while its plan is loaded ends as one canceled later does,
// whether or not the run had carried on: it says so, once. Only where what it
// set aside cannot be found, or read, does it end with no event.
func TestResetRun_CanceledWhileItsPlanIsLoaded(t *testing.T) {
	key := engine.ResetFacts{Reason: engine.ResumePlanKey{Scope: 42, PlanDigest: strings.Repeat("ab", 32)}.String()}
	for name, tc := range map[string]struct {
		facts, plan, unread, noBase, byHand bool // canceled at each read; what is missing
	}{
		"as its reset is read":            {facts: true},
		"as its plan is read":             {plan: true},
		"and its reset is never read":     {facts: true, unread: true},
		"and its base's snapshot is gone": {plan: true, noBase: true},
		"as a reset made by hand is read": {facts: true, byHand: true},
	} {
		for _, carried := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s, carried on %v", name, carried), func(t *testing.T) {
				c := carryOn(t, resumeCarriedDocument, nil, nil)
				h := newResetHarnessOn(t, c.snapshots, c.values)
				h.env.SetStartWorkflowOptions(client.StartWorkflowOptions{ID: "ork-reset-1"})
				if !tc.noBase {
					h.baseEnded(t, c.wf.GetNamespace())
				}
				calls := 0
				if tc.facts {
					h.env.OnActivity(engine.ActivityReadResetFacts, mock.Anything, mock.Anything).Return(func(context.Context, engine.ReadResetFactsInput) (engine.ResetFacts, error) {
						if calls++; calls == 1 {
							h.env.CancelWorkflow()
						}
						if tc.byHand {
							return engine.ResetFacts{Reason: "by hand"}, nil
						}
						if calls == 1 || tc.unread {
							return engine.ResetFacts{}, errors.New("unavailable")
						}
						return key, nil
					})
				} else {
					h.facts = key
					h.env.OnActivity(engine.ActivityLoadResumePlan, mock.Anything, mock.Anything).Return(func(context.Context, engine.LoadResumePlanInput) (*orkstackv1.ResumePlanRecord, error) {
						h.env.CancelWorkflow()
						return nil, errors.New("unavailable")
					})
				}
				if !carried {
					c.carry = nil
				}
				id := ""
				if !carried {
					id = "ork-reset-1"
				}
				h.env.ExecuteWorkflow("ResetRunWorkflow", id, c.wf, c.input, c.plan, c.carry)
				err := h.env.GetWorkflowError()
				var canceled *temporal.CanceledError
				events := eventsOf(h.harness, "ork-reset-1")
				ends := slices.ContainsFunc(events, func(e *orkstackv1.Event) bool { return e.GetType() == orkstackv1.EventType_EVENT_TYPE_RUN_CANCELED })
				switch {
				case tc.byHand:
					if !errors.As(err, &canceled) || !ends || events[len(events)-1].GetSequence() >= 42 {
						t.Fatalf("ended %v, events %v; want it canceled as any run", err, events)
					}
				case tc.unread || tc.noBase:
					if !errors.As(err, &canceled) || ends {
						t.Fatalf("ended %v, events %v; want canceled, with no event it could number", err, events)
					}
				default:
					h.canceledAt42(t, err)
				}
			})
		}
	}
}

// A cancel that comes as a run whose plan cannot be run ends changes nothing of
// its ending: it is written and said, whichever came first.
func TestResetRun_CanceledAsItEndsUnrunnable(t *testing.T) {
	c := carryOn(t, resumeCarriedDocument, nil, nil)
	h := c.reset()
	h.facts = engine.ResetFacts{Reason: engine.ResumePlanKey{Scope: 42, PlanDigest: strings.Repeat("ab", 32)}.String()}
	write := engine.NewSnapshotActivities(c.snapshots).WriteResumeEnding
	h.env.OnActivity(engine.ActivityWriteResumeEnding, mock.Anything, mock.Anything).Return(func(ctx context.Context, in engine.ResumeEndingInput) (string, error) {
		h.env.CancelWorkflow()
		return write(context.WithoutCancel(ctx), in)
	})
	c.endedAt42(h, c.run(h), engine.ReasonResumePlanInvalid)
}

// A run that had carried on is resumed where its last history started: what
// had ended in its saved state stays, nothing that waited in it waits on, and
// the rest is kept, waits or runs again from the resume.
func TestResetRun_ACarriedRunStartsWhereItsHistoryDid(t *testing.T) {
	c := carryOn(t, resumeCarriedDocument, nil, nil)
	if a, s := c.row("approve"), c.row("settle"); a.GetStatus() != orkstackv1.Status_STATUS_AWAITING || s.GetStatus() != orkstackv1.Status_STATUS_SLEEPING || c.row("build").GetStatus() != orkstackv1.Status_STATUS_SUCCEEDED {
		t.Fatalf("saved approve %v, settle %v: the document no longer carries on waiting", a.GetStatus(), s.GetStatus())
	}
	h := c.reset()
	h.resumeOf(t, c.wf, nil, unit("build", -1, leaveIt), unit("approve", -1, waitAgain), unit("settle", -1, waitAgain), unit("late", -1, rerunIt), unit("deploy", -1, rerunIt))
	h.env.SetStartTime(time.Now().Add(48 * time.Hour)) // long after its wait's deadline
	h.signalAt(30*time.Minute, completion("approve", `{"ok":true}`, "ana@example.com"))
	must.OKf(t, c.run(h), "the resumed run")

	events := eventsOf(h.harness, "ork-reset-1")
	if events[0].GetType() != orkstackv1.EventType_EVENT_TYPE_RUN_RESUMED || events[0].GetSequence() != 42 {
		t.Fatalf("first %v at %d, want RUN_RESUMED at 42", events[0].GetType(), events[0].GetSequence())
	}
	for i, e := range events {
		if e.GetSequence() != int64(42+i) {
			t.Fatalf("event %d is %d, want the stream whole from 42", i, e.GetSequence())
		}
	}
	run := h.ended(t)
	build, approve, nap := taskRunOfRun(run, "build"), taskRunOfRun(run, "approve"), taskRunOfRun(run, "settle")
	if !proto.Equal(build, c.row("build")) {
		t.Errorf("build = %v, want it as it was saved", build)
	}
	if len(h.taskCalls("late")) != 1 || len(h.taskCalls("build")) != 0 || len(h.taskCalls("deploy")) != 1 {
		t.Errorf("ran %d late, %d build, %d deploy", len(h.taskCalls("late")), len(h.taskCalls("build")), len(h.taskCalls("deploy")))
	}
	if approve.GetStatus() != orkstackv1.Status_STATUS_SUCCEEDED || approve.GetResultEpoch() != 42 || approve.GetAwait().GetDeadline().AsTime().Sub(approve.GetStartedAt().AsTime()) != time.Hour {
		t.Errorf("approve = %v, want it answered within a fresh hour", approve)
	}
	if nap.GetFinishedAt().AsTime().Sub(nap.GetStartedAt().AsTime()) < 10*time.Minute {
		t.Errorf("settle = %v, want it slept in full", nap)
	}
	if up := h.taskCalls("deploy")[0].GetUpstream(); len(up) == 0 {
		t.Error("deploy was handed nothing of what it needs")
	}
}

// What waited in the saved state and was answered, or woke, before the run
// ended is kept from its record.
func TestResetRun_ACarriedRunsWaitsAreKeptFromTheirRecords(t *testing.T) {
	c := carryOn(t, resumeCarriedDocument, nil, nil)
	h := c.reset()
	h.resumeOf(t, c.wf, nil, unit("build", -1, leaveIt), recordedUnit("approve", settleIt, h.answered(`{"go":true}`, true)), sleptUnit("settle"),
		unit("late", -1, keepIt), unit("deploy", -1, rerunIt))
	must.OKf(t, c.run(h), "the resumed run")
	run := h.ended(t)
	for _, id := range []string{"approve", "settle"} {
		if row := taskRunOfRun(run, id); row.GetStatus() != orkstackv1.Status_STATUS_SUCCEEDED || !row.GetAdopted() || len(eventsFor(h.harness, "ork-reset-1", id)) != 0 {
			t.Errorf("%s = %v, want it kept and silent", id, row)
		}
	}
	if !taskRunOfRun(run, "late").GetAdopted() || len(h.taskCalls("late")) != 0 || len(h.taskCalls("deploy")) != 1 {
		t.Errorf("late = %v; ran %d late, %d deploy", taskRunOfRun(run, "late"), len(h.taskCalls("late")), len(h.taskCalls("deploy")))
	}
}

// A plan that does not leave exactly what had ended in the saved state is of
// another state, and is not carried out.
func TestResetRun_ACarriedRunsPlanIsOfItsSavedState(t *testing.T) {
	waits := []engine.ResumeUnitPlan{unit("approve", -1, waitAgain), unit("settle", -1, waitAgain)}
	for name, tc := range map[string]struct {
		build, late engine.ResumeUnitPlan
		want        string
	}{
		"runs again what had ended":  {unit("build", -1, rerunIt), unit("late", -1, rerunIt), "had ended before"},
		"keeps what had ended":       {unit("build", -1, keepIt), unit("late", -1, rerunIt), "had ended before"},
		"leaves what had not ended":  {unit("build", -1, leaveIt), unit("late", -1, leaveIt), "had not ended"},
		"leaves what waits, unended": {unit("build", -1, leaveIt), unit("late", -1, rerunIt), "had not ended"},
	} {
		t.Run(name, func(t *testing.T) {
			c := carryOn(t, resumeCarriedDocument, nil, nil)
			h := c.reset()
			units := append([]engine.ResumeUnitPlan{tc.build}, waits...)
			if strings.Contains(name, "waits") {
				units[1] = unit("approve", -1, leaveIt)
			}
			h.resumeOf(t, c.wf, nil, append(units, tc.late, unit("deploy", -1, rerunIt))...)
			run := c.endedAt42(h, c.run(h), engine.ReasonResumePlanInvalid)
			if !strings.Contains(run.GetError().GetMessage(), tc.want) || taskRunOfRun(run, "deploy").GetAttempt() != 3 {
				t.Fatalf("ended %v with deploy %v; want it to say %q, over the base's rows", run.GetError(), taskRunOfRun(run, "deploy"), tc.want)
			}
		})
	}
}

// A carried run whose plan cannot be loaded ends from its base's rows, before
// its definition or its saved state is read.
func TestResetRun_ACarriedRunsPlanThatCannotBeLoaded(t *testing.T) {
	c := carryOn(t, resumeCarriedDocument, nil, nil)
	h := c.reset()
	h.facts = engine.ResetFacts{Reason: engine.ResumePlanKey{Scope: 42, PlanDigest: strings.Repeat("ab", 32)}.String()}
	c.snapshots.remove(c.carry.GetCheckpoint().GetUri())
	c.endedAt42(h, c.run(h), engine.ReasonResumePlanInvalid)
}

// A reset by hand of a carried run restores it as it always has.
func TestResetRun_ACarriedRunResetByHandIsNoResume(t *testing.T) {
	c := carryOn(t, resumeCarriedDocument, nil, nil)
	h := c.reset()
	h.facts = engine.ResetFacts{Reason: "by hand"}
	h.signalAt(time.Minute, completion("approve", `{"ok":true}`, "ana@example.com"))
	must.OKf(t, c.run(h), "the run")
	if events := eventsOf(h.harness, "ork-reset-1"); events[0].GetSequence() != c.carry.GetEventSequence()+1 || len(h.taskCalls("build")) != 0 {
		t.Fatalf("first event %d after %d", events[0].GetSequence(), c.carry.GetEventSequence())
	}
}

// Saved state that fails its check ends the resumed run for good, above every
// event of its base.
func TestResetRun_ACarriedRunsDamagedSavedStateEndsItForGood(t *testing.T) {
	c := carryOn(t, resumeCarriedDocument, nil, nil)
	h := c.reset()
	h.resumeOf(t, c.wf, nil, unit("build", -1, leaveIt), unit("approve", -1, waitAgain), unit("settle", -1, waitAgain), unit("late", -1, keepIt), unit("deploy", -1, rerunIt))
	uri := c.carry.GetCheckpoint().GetUri()
	data, _ := c.snapshots.get(uri)
	data = bytes.Clone(data)
	data[len(data)/2] ^= 0xff
	c.snapshots.put(uri, data)
	c.endedAt42(h, c.run(h), engine.RunStateCorruptCode)
}

// A store that does not answer while the saved state is read ends the resumed
// run as its base ended it, and it can be resumed again.
func TestResetRun_ACarriedRunsSavedStateThatIsNotAnswered(t *testing.T) {
	c := carryOn(t, resumeCarriedDocument, nil, nil)
	h := c.reset()
	h.resumeOf(t, c.wf, nil, unit("build", -1, leaveIt), unit("approve", -1, waitAgain), unit("settle", -1, waitAgain), unit("late", -1, rerunIt), unit("deploy", -1, rerunIt))
	h.env.OnActivity(engine.ActivityReadContinuationChunk, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Return(nil, errors.New("connection refused"))
	run := c.endedAt42(h, c.run(h), engine.ReasonResumePlanUnreadable)
	if deploy := taskRunOfRun(run, "deploy"); deploy.GetAttempt() != 3 || !strings.Contains(run.GetError().GetMessage(), "saved state") ||
		!strings.HasSuffix(run.GetError().GetMessage(), "; nothing ran again") {
		t.Fatalf("ended %v with deploy %v, want its base's rows and what was not read", run.GetError(), deploy)
	}
}

// An ending before RUN_RESUMED comes after every event of the base, whatever
// the plan says its first is.
func TestResetRun_ACarriedRunsEndingIsAboveItsBaseWhateverItsPlanSays(t *testing.T) {
	for _, seqBase := range []uint64{0, 1, 1 << 60} {
		c := carryOn(t, resumeCarriedDocument, nil, nil)
		h := c.reset()
		h.resumeOf(t, c.wf, nil, unit("build", -1, leaveIt), unit("approve", -1, waitAgain), unit("settle", -1, waitAgain), unit("late", -1, rerunIt), unit("deploy", -1, rerunIt))
		h.plan.Block.SeqBase = seqBase
		uri := c.carry.GetCheckpoint().GetUri()
		data, _ := c.snapshots.get(uri)
		data = bytes.Clone(data)
		data[len(data)/2] ^= 0xff
		c.snapshots.put(uri, data)
		c.endedAt42(h, c.run(h), engine.RunStateCorruptCode)
	}
}

// A cancel that comes with a failed check is still a cancel.
func TestResetRun_ACarriedRunCanceledAsItsSavedStateFailsItsCheck(t *testing.T) {
	for name, byReference := range map[string]bool{"saved state": false, "definition": true} {
		t.Run(name, func(t *testing.T) {
			c := carryOn(t, resumeCarriedDocument, nil, nil)
			h := c.reset()
			wf := c.wf
			if byReference {
				wf = c.byReference()
			}
			h.resumeOf(t, wf, nil, unit("build", -1, leaveIt), unit("approve", -1, waitAgain), unit("settle", -1, waitAgain), unit("late", -1, rerunIt), unit("deploy", -1, rerunIt))
			h.env.OnActivity(engine.ActivityReadContinuationChunk, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).
				Return(func(_ context.Context, _ engine.ContinuationLocator, _, _, length int64, _ engine.ContinuationObservationInput) ([]byte, error) {
					h.env.CancelWorkflow()
					return make([]byte, length), nil
				})
			h.canceledAt42(t, c.run(h))
		})
	}
}

// byReference has c's definition travel by reference, as it would when it is
// too large for a start.
func (c *carriedRun) byReference() *orkstackv1.Orkflow {
	c.t.Helper()
	whole := c.wf
	data, ref, err := engine.EncodeDefinitionObject(whole)
	must.OK(c.t, err)
	_, err = c.snapshots.WriteContinuation(context.Background(), whole.GetNamespace(), "ork-reset-1", 0, artifacts.ContinuationKindDefinition, data)
	must.OK(c.t, err)
	c.wf, c.plan.Definition = engine.DefinitionStub(whole), &ref
	c.carry.VersionDecisions[engine.DefinitionByReferenceChange] = 1
	return whole
}

// A definition passed by reference is read after the plan, so each of its
// endings is the resume's, as the saved state's is.
func TestResetRun_ACarriedRunsDefinitionByReference(t *testing.T) {
	for name, tc := range map[string]struct {
		read func(*resetHarness) error // nil reads it
		code string                    // "" resumes
		over bool                      // it ends as its base's rows
	}{
		"read":         {},
		"not answered": {read: func(*resetHarness) error { return errors.New("connection refused") }, code: engine.ReasonResumePlanUnreadable, over: true},
		"damaged": {read: func(*resetHarness) error {
			return temporal.NewNonRetryableApplicationError("its bytes are not its own", engine.RunStateCorruptCode, nil)
		}, code: engine.RunDefinitionCorruptCode},
		"canceled": {read: func(h *resetHarness) error { h.env.CancelWorkflow(); return errors.New("connection refused") }, code: "canceled"},
	} {
		t.Run(name, func(t *testing.T) {
			c := carryOn(t, resumeCarriedDocument, nil, nil)
			h := c.reset()
			h.resumeOf(t, c.byReference(), nil, unit("build", -1, leaveIt), unit("approve", -1, waitAgain), unit("settle", -1, waitAgain), unit("late", -1, rerunIt), unit("deploy", -1, rerunIt))
			if tc.read == nil {
				h.signalAt(time.Minute, completion("approve", `{"ok":true}`, "ana@example.com"))
				must.OKf(t, c.run(h), "the resumed run")
				if len(h.taskCalls("deploy")) != 1 || len(h.taskCalls("build")) != 0 {
					t.Fatalf("ran %d deploy, %d build", len(h.taskCalls("deploy")), len(h.taskCalls("build")))
				}
				return
			}
			h.env.OnActivity(engine.ActivityReadContinuationChunk, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).
				Return(func(context.Context, engine.ContinuationLocator, int64, int64, int64, engine.ContinuationObservationInput) ([]byte, error) {
					return nil, tc.read(h)
				})
			err := c.run(h)
			if tc.code == "canceled" {
				h.canceledAt42(t, err)
				return
			}
			run := c.endedAt42(h, err, tc.code)
			if over := taskRunOfRun(run, "deploy").GetAttempt() == 3; over != tc.over {
				t.Fatalf("ended over its base's rows %v, want %v", over, tc.over)
			}
		})
	}
}

// An operator's cancel while the saved state is read carries nothing of the
// resume out: the run ends canceled, with its base's rows.
func TestResetRun_ACarriedRunCanceledWhileItsSavedStateIsRead(t *testing.T) {
	c := carryOn(t, resumeCarriedDocument, nil, nil)
	h := c.reset()
	h.resumeOf(t, c.wf, nil, unit("build", -1, leaveIt), unit("approve", -1, waitAgain), unit("settle", -1, waitAgain), unit("late", -1, rerunIt), unit("deploy", -1, rerunIt))
	h.env.OnActivity(engine.ActivityReadContinuationChunk, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Return(func(context.Context, engine.ContinuationLocator, int64, int64, int64, engine.ContinuationObservationInput) ([]byte, error) {
			h.env.CancelWorkflow()
			return nil, errors.New("connection refused")
		})
	h.canceledAt42(t, c.run(h))
}

// What an earlier resume had run again past its memo is said anew by the next
// resume's plan, never left from the saved state.
func TestResetRun_ACarriedRunsMemosAreReadAsItsNewPlanSays(t *testing.T) {
	doc := strings.Replace(fan(resumeFanOutDocument, ", max_parallel: 1", "\n    cache: {enabled: true}"), "    each:", "    cache: {enabled: true}\n    each:", 1)
	input := map[string]any{"xs": []any{"a", "b", "c"}}
	h := newResetHarness(t)
	h.behave = func(in *orkstackv1.ExecuteTaskInput) (*orkstackv1.ExecuteTaskResult, error) {
		if in.GetTaskId() == "convert" {
			h.env.SetCurrentHistoryLength(650)
		}
		return succeeded(in.TaskId, map[string]any{"ok": true}), nil
	}
	h.adjust = func(plan *engine.RunPlan) {
		held := holdPlan()
		plan.History, plan.Continuation = held.History, held.Continuation
	}
	fresh := func(u engine.ResumeUnitPlan) engine.ResumeUnitPlan { u.Fresh = true; return u }
	h.env.SetStartWorkflowOptions(client.StartWorkflowOptions{ID: "ork-reset-1"})
	h.resume(t, doc, nil, unit("build", -1, keepIt), fresh(unit("convert", -1, rerunIt)), fresh(unit("merge", -1, rerunIt)))
	c := &carriedRun{t: t, snapshots: h.snapshots, values: h.values}
	c.next(h.runResetWith(t, doc, input))
	if c.carry.GetResume().GetSeqBase() != 42 || !slices.ContainsFunc(c.row("convert").GetItems(), func(i *orkstackv1.TaskRun) bool { return i.GetStatus() == orkstackv1.Status_STATUS_PENDING }) {
		t.Fatalf("carried %v with convert %v", c.carry.GetResume(), c.row("convert").GetItems())
	}

	// The second resume, of the same point, runs the whole fan-out again.
	again := c.reset()
	again.resumeOf(t, c.wf, nil, unit("build", -1, leaveIt), unit("convert", -1, rerunIt), unit("merge", -1, rerunIt))
	again.facts = engine.ResetFacts{Reason: engine.ResumePlanKey{Scope: 60, PlanDigest: strings.Repeat("ab", 32)}.String()}
	again.plan.Scope, again.plan.BaseTerminalSequence, again.plan.Block.SeqBase, again.plan.Block.Scope, again.plan.Block.ResumesBefore = 60, 59, 60, 60, 1
	must.OKf(t, c.run(again), "the second resume")
	if n := len(again.taskCalls("convert")); n != 3 {
		t.Errorf("convert ran %d items, want all 3 from the first", n)
	}
	for _, in := range again.calls {
		if in.GetCache().GetRefresh() || !strings.Contains(in.GetIdempotencyKey(), "@r60") {
			t.Errorf("%s: refresh %v, key %s; want its memo read, under scope 60", invocationName(in), in.GetCache().GetRefresh(), in.GetIdempotencyKey())
		}
	}
	run := again.ended(t)
	if build := taskRunOfRun(run, "build"); !proto.Equal(build, c.row("build")) || !build.GetAdopted() {
		t.Errorf("build = %v, want it as the first resume kept it", build)
	}
	if r := run.GetResumes(); len(r) != 1 || r[0].GetScope() != 60 || run.GetStatus() != orkstackv1.Status_STATUS_SUCCEEDED {
		t.Errorf("run %v, resumes %v", run.GetStatus(), r)
	}
}
