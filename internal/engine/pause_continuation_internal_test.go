package engine

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/workflow"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"

	"github.com/orkstack/orkstack/internal/testsupport/must"
)

// chainOf is a chain of n plain tasks, c1 → … → cn.
func chainOf(n int) string {
	var b strings.Builder
	b.WriteString("\napiVersion: orkstack/v1\nkind: Orkflow\nname: chain\ntasks:\n")
	for i := 1; i <= n; i++ {
		fmt.Fprintf(&b, "  - id: c%d\n    uses: shell\n    with: {op: exec}\n", i)
		if i > 1 {
			fmt.Fprintf(&b, "    needs: [c%d]\n", i-1)
		}
	}
	return b.String()
}

// carriesOn registers what a switch writes and a restore reads.
func (h *timeoutHarness) carriesOn() {
	activities := NewSnapshotActivities(h.store)
	h.env.RegisterActivityWithOptions(activities.WriteEventSegment, activity.RegisterOptions{Name: ActivityWriteEventSegment})
	h.env.RegisterActivityWithOptions(activities.WriteSwitchSnapshot, activity.RegisterOptions{Name: ActivityWriteSwitchSnapshot})
	h.env.RegisterActivityWithOptions(activities.WriteCheckpoint, activity.RegisterOptions{Name: ActivityWriteCheckpoint})
	h.env.RegisterActivityWithOptions(activities.ReadContinuationChunk, activity.RegisterOptions{Name: ActivityReadContinuationChunk})
}

// pausedGeneration is one generation's arguments as its predecessor handed
// them on, and when it was handed them.
type pausedGeneration struct {
	store *memSnapshotStore
	wf    *orkstackv1.Orkflow
	plan  RunPlan
	carry *orkstackv1.RunContinuation
	at    time.Time
	start time.Time
}

// pauseThenSwitch runs a chain whose switch is due from the start, paused
// with a pause of pauseSeconds before its first idle point, until it
// carries on.
func pauseThenSwitch(t *testing.T, pauseSeconds string) *pausedGeneration {
	t.Helper()
	h := newTimeoutHarness(t)
	h.carriesOn()
	h.taskTakes(10*time.Second, 0)
	h.env.RegisterDelayedCallback(func() { h.env.SignalWorkflow(PauseSignal, pauseSignal(true, pauseSeconds)) }, 5*time.Second)
	plan := pausePlan(t)
	budget := DefaultContinuationBudget(1, 1<<40, DefaultContinuationRestoreTimeout)
	plan.Continuation = &budget
	start := h.env.Now()
	err := h.run(t, chainOf(12), plan)
	var can *workflow.ContinueAsNewError
	if !errors.As(err, &can) {
		t.Fatalf("generation 0 ended %v, want it to carry on while paused", err)
	}
	if len(h.sawEvent(eventRunPaused)) != 1 || len(h.sawEvent(eventRunUnpaused)) != 0 {
		t.Fatal("generation 0 carried on unpaused; want it to carry on while paused")
	}
	out := &pausedGeneration{store: h.store, at: h.env.Now(), start: start}
	var runID string
	var input *orkstackv1.Payload
	must.OK(t, converter.GetDefaultDataConverter().FromPayloads(can.Input, &runID, &out.wf, &input, &out.plan, &out.carry))
	// Generation 0's budget of one event made its switch due at once; the
	// next generation gets an ordinary one, so it does not switch again.
	ordinary := DefaultContinuationBudget(DefaultContinuationEvents, DefaultContinuationBytes, DefaultContinuationRestoreTimeout)
	out.plan.Continuation = &ordinary
	return out
}

// next runs the generation in a fresh harness over the run's store, from
// the instant its predecessor handed over.
func (g *pausedGeneration) next(t *testing.T, setup func(h *timeoutHarness)) (*timeoutHarness, error) {
	t.Helper()
	h := newTimeoutHarnessOver(t, g.store)
	h.carriesOn()
	h.env.SetStartTime(g.at)
	h.taskTakes(10*time.Second, 0)
	if setup != nil {
		setup(h)
	}
	h.env.ExecuteWorkflow(RunWorkflowType, "", g.wf, (*orkstackv1.Payload)(nil), g.plan, g.carry)
	if !h.env.IsWorkflowCompleted() {
		t.Fatal("the next generation did not end")
	}
	return h, h.env.GetWorkflowError()
}

// A paused run carries on with its pause: the next generation is PAUSED from
// its first snapshot, starts nothing, and its pause expires at the instant
// it was given, never at a fresh one.
func TestPause_CrossesAContinuationAndExpiresOnTime(t *testing.T) {
	g := pauseThenSwitch(t, "600")
	if got := g.carry.GetPause(); got == nil || got.GetExpiresAt() == nil || got.GetPausedBy() != "alice" {
		t.Fatalf("carry pause %v, want alice's pause with its expiry", got)
	}
	h, err := g.next(t, nil)
	requireCode(t, err, RunPauseTimeoutCode)
	_, run := h.last(t)
	want := g.start.Add(605 * time.Second)
	if ended := run.GetFinishedAt().AsTime(); ended.Before(want) || ended.After(want.Add(time.Second)) {
		t.Fatalf("the run ended at %s, want at its pause's original expiry %s", ended, want)
	}
	if n := len(h.tasks); n != 0 {
		t.Fatalf("the paused generation dispatched %d tasks: %v", n, h.tasks)
	}
}

// After the switch the pause is lifted as before it: the run unpauses, runs
// its remaining tasks and succeeds.
func TestPause_CrossesAContinuationAndUnpauses(t *testing.T) {
	g := pauseThenSwitch(t, "0")
	h, err := g.next(t, func(h *timeoutHarness) {
		h.env.RegisterDelayedCallback(func() { h.env.SignalWorkflow(PauseSignal, pauseSignal(false, "0")) }, time.Hour)
	})
	var can *workflow.ContinueAsNewError
	if err != nil && !errors.As(err, &can) {
		t.Fatalf("the next generation ended %v, want it to run on once unpaused", err)
	}
	if len(h.sawEvent(eventRunUnpaused)) != 1 || len(h.tasks) == 0 {
		t.Fatalf("unpaused %d, dispatched %v; want the unpause to let work start", len(h.sawEvent(eventRunUnpaused)), h.tasks)
	}
	if got := g.carry.GetLastPause(); got != nil {
		t.Fatalf("carry last_pause %v while the pause still lasts", got)
	}
}

// A resume of a run whose last history started paused sets that pause aside:
// the run starts at once, and what the cluster indexed of the pause is cleared
// (E103 §16).
func TestResetRun_ACarriedPauseIsSetAside(t *testing.T) {
	g := pauseThenSwitch(t, "600")
	g.plan.Pause.Indexed, g.plan.CommitRoot = true, "mem://commits"
	key := ResumePlanKey{Scope: 42, PlanDigest: strings.Repeat("ab", 32)}
	block := &orkstackv1.ResumeBlock{SeqBase: 42, Scope: 42, Action: orkstackv1.ResumeAction_RESUME_ACTION_RESUME,
		Readback: readbackOf(defaultReadbackTunables())}
	var units []ResumeUnitPlan
	for _, task := range g.wf.GetTasks() {
		d := dRerun
		if task.GetId() == "c1" {
			d = dNone
		}
		units = append(units, ResumeUnitPlan{Task: task.GetId(), Item: -1, Disposition: d})
	}
	must.OK(t, EncodeResumeUnits(g.wf, block, units, true))
	h := newTimeoutHarnessOver(t, g.store)
	h.carriesOn()
	h.env.RegisterWorkflowWithOptions(ResetRunWorkflow(), workflow.RegisterOptions{Name: "ResetRunWorkflow"})
	h.env.RegisterActivityWithOptions(func(context.Context, ReadResetFactsInput) (ResetFacts, error) {
		return ResetFacts{Reason: key.String()}, nil
	}, activity.RegisterOptions{Name: ActivityReadResetFacts})
	h.env.RegisterActivityWithOptions(func(context.Context, LoadResumePlanInput) (*orkstackv1.ResumePlanRecord, error) {
		return &orkstackv1.ResumePlanRecord{Scope: 42, BaseTerminalSequence: 41, At: timestamppb.New(g.at), Block: block}, nil
	}, activity.RegisterOptions{Name: ActivityLoadResumePlan})
	index := h.recordPauseIndex()
	h.env.SetStartTime(g.at.Add(24 * time.Hour)) // long past the pause's expiry
	h.taskTakes(10*time.Second, 0)
	h.env.ExecuteWorkflow("ResetRunWorkflow", "", g.wf, (*orkstackv1.Payload)(nil), g.plan, g.carry)
	must.OKf(t, h.env.GetWorkflowError(), "the resumed run")
	if len(h.sawEvent(eventRunUnpaused)) != 0 || len(h.sawEvent(eventRunPaused)) != 0 || len(h.tasks) != 11 {
		t.Fatalf("paused %d, unpaused %d, ran %v; want no pause and every other task", len(h.sawEvent(eventRunPaused)), len(h.sawEvent(eventRunUnpaused)), h.tasks)
	}
	if _, run := h.last(t); run.GetPause() != nil || run.GetLastPause() != nil || run.GetStatus() != orkstackv1.Status_STATUS_SUCCEEDED {
		t.Fatalf("ended %v, pause %v, last %v", run.GetStatus(), run.GetPause(), run.GetLastPause())
	}
	if !slices.Equal(index.attr, []string{"unset"}) || !slices.Equal(index.memo, []string{"nil"}) {
		t.Fatalf("wrote %v and %v to the pause index, want each cleared once", index.attr, index.memo)
	}
}

// adoptPauseCarry restores the pause a carry holds, PAUSED and with no
// timer yet, and refuses one it cannot keep or that its saved state does not
// match.
func TestAdoptPauseCarry(t *testing.T) {
	at := time.Date(2026, 11, 15, 10, 0, 0, 0, time.UTC)
	pause := &orkstackv1.RunPause{PausedAt: timestamppb.New(at), ExpiresAt: timestamppb.New(at.Add(time.Hour)), PausedBy: "alice"}
	ended := func(end orkstackv1.PauseEnd, endedAt *timestamppb.Timestamp) *orkstackv1.RunPause {
		p := proto.Clone(pause).(*orkstackv1.RunPause)
		p.End, p.EndedAt = end, endedAt
		return p
	}
	last := ended(orkstackv1.PauseEnd_PAUSE_END_UNPAUSED, timestamppb.New(at.Add(time.Minute)))
	parentPause := &orkstackv1.InheritedPause{ParentRunId: testParentRunID, Since: timestamppb.New(at),
		BackstopAt: timestamppb.New(at.Add(time.Hour))}
	carried := map[string]workflow.Version{pauseCarryVersion: 1}
	var channel workflow.ReceiveChannel = &fakeReceiveChannel{}
	child := RunPlan{Pause: pausePlan(t).Pause, Origin: &RunOrigin{Kind: "RUN_ORIGIN_KIND_PARENT_RUN", ID: testParentRunID}}
	root := RunPlan{Pause: pausePlan(t).Pause}
	// savedAs is the saved state a switch writes beside carry.
	savedAs := func(c *orkstackv1.RunContinuation) *orkstackv1.Run {
		run := &orkstackv1.Run{Status: orkstackv1.Status_STATUS_RUNNING, Pause: c.GetPause(), LastPause: c.GetLastPause(),
			PausedByParent: c.GetPausedByParent()}
		if c.GetPause() != nil || c.GetPausedByParent() != nil {
			run.Status = statusPaused
		}
		return run
	}
	tests := []struct {
		name     string
		versions map[string]workflow.Version
		pauses   workflow.ReceiveChannel
		plan     RunPlan
		carry    *orkstackv1.RunContinuation
		saved    *orkstackv1.Run // nil: savedAs(carry)
		fault    string
		paused   bool
	}{
		{name: "no pause state", carry: &orkstackv1.RunContinuation{}},
		{name: "a pause", versions: carried, pauses: channel, plan: root, carry: &orkstackv1.RunContinuation{Pause: pause}, paused: true},
		{name: "a last pause only", versions: carried, pauses: channel, plan: root,
			carry: &orkstackv1.RunContinuation{LastPause: last, LastCascadeSeq: 3}},
		{name: "a parent's pause", versions: carried, pauses: channel, plan: child,
			carry: &orkstackv1.RunContinuation{PausedByParent: parentPause}, paused: true},
		{name: "without the gate", pauses: channel, plan: root, carry: &orkstackv1.RunContinuation{Pause: pause}, fault: "cannot carry"},
		{name: "a run not pausable", versions: carried, plan: root, carry: &orkstackv1.RunContinuation{Pause: pause}, fault: "cannot carry"},
		{name: "no expiry", versions: carried, pauses: channel, plan: root,
			carry: &orkstackv1.RunContinuation{Pause: &orkstackv1.RunPause{PausedAt: timestamppb.New(at)}}, fault: "expiry"},
		{name: "an expiry past the longest pause", versions: carried, pauses: channel, plan: root,
			carry: &orkstackv1.RunContinuation{Pause: &orkstackv1.RunPause{PausedAt: timestamppb.New(at),
				ExpiresAt: timestamppb.New(at.Add(32 * 24 * time.Hour))}}, fault: "not within"},
		{name: "an expiry before its start", versions: carried, pauses: channel, plan: root,
			carry: &orkstackv1.RunContinuation{Pause: &orkstackv1.RunPause{PausedAt: timestamppb.New(at),
				ExpiresAt: timestamppb.New(at.Add(-time.Minute))}}, fault: "not within"},
		{name: "a last pause that never ended", versions: carried, pauses: channel, plan: root,
			carry: &orkstackv1.RunContinuation{LastPause: ended(orkstackv1.PauseEnd_PAUSE_END_UNSPECIFIED, timestamppb.New(at))},
			fault: "not a way a pause ends"},
		{name: "a last pause of an unknown end", versions: carried, pauses: channel, plan: root,
			carry: &orkstackv1.RunContinuation{LastPause: ended(orkstackv1.PauseEnd(7), timestamppb.New(at))},
			fault: "not a way a pause ends"},
		{name: "a last pause with no end time", versions: carried, pauses: channel, plan: root,
			carry: &orkstackv1.RunContinuation{LastPause: ended(orkstackv1.PauseEnd_PAUSE_END_TIMED_OUT, nil)}, fault: "no valid end"},
		{name: "a parent's pause on a top-level run", versions: carried, pauses: channel, plan: root,
			carry: &orkstackv1.RunContinuation{PausedByParent: &orkstackv1.InheritedPause{Since: timestamppb.New(at),
				BackstopAt: timestamppb.New(at)}}, fault: "not this run's parent"},
		{name: "another run's parent", versions: carried, pauses: channel, plan: child,
			carry: &orkstackv1.RunContinuation{PausedByParent: &orkstackv1.InheritedPause{ParentRunId: "ork-other",
				Since: timestamppb.New(at), BackstopAt: timestamppb.New(at)}}, fault: "not this run's parent"},
		{name: "a negative counter", versions: carried, pauses: channel, plan: root,
			carry: &orkstackv1.RunContinuation{LastCascadeSeq: -1}, fault: "negative"},
		{name: "a saved state paused, a carry holding none", versions: carried, pauses: channel, plan: root,
			carry: &orkstackv1.RunContinuation{}, saved: &orkstackv1.Run{Status: statusPaused, Pause: pause}, fault: "differs"},
		{name: "a carry moving the expiry", versions: carried, pauses: channel, plan: root,
			carry: &orkstackv1.RunContinuation{Pause: pause}, saved: &orkstackv1.Run{Status: statusPaused,
				Pause: &orkstackv1.RunPause{PausedAt: timestamppb.New(at), ExpiresAt: timestamppb.New(at.Add(2 * time.Hour))}},
			fault: "differs"},
		{name: "a saved state paused with no cause", versions: carried, pauses: channel, plan: root,
			carry: &orkstackv1.RunContinuation{}, saved: &orkstackv1.Run{Status: statusPaused}, fault: "holds no"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &runState{versions: tt.versions, pauses: tt.pauses, plan: tt.plan, status: orkstackv1.Status_STATUS_RUNNING}
			saved := tt.saved
			if saved == nil {
				saved = savedAs(tt.carry)
			}
			got := s.adoptPauseCarry(tt.carry, saved)
			if (got != "") != (tt.fault != "") || !strings.Contains(got, tt.fault) {
				t.Fatalf("adoptPauseCarry() = %q, want a fault naming %q", got, tt.fault)
			}
			if tt.fault != "" {
				return
			}
			if s.paused() != tt.paused || (s.status == statusPaused) != tt.paused {
				t.Fatalf("paused %v, status %s; want paused %v", s.paused(), s.status, tt.paused)
			}
			if s.pause != nil && (s.pause.timer != nil || !s.pause.expiresAt.Equal(at.Add(time.Hour))) {
				t.Fatalf("restored pause %+v, want its expiry and no timer yet", s.pause)
			}
			if got := s.lastCascadeSeq; got != tt.carry.GetLastCascadeSeq() {
				t.Fatalf("cascade counter %d, want %d", got, tt.carry.GetLastCascadeSeq())
			}
			if !proto.Equal(s.lastPause.proto(), tt.carry.GetLastPause()) {
				t.Fatalf("restored last pause %v, want %v", s.lastPause.proto(), tt.carry.GetLastPause())
			}
		})
	}
}

// An unpause delivered while the switch's writes block is never lost: the
// switch is abandoned, and the run carries on unpaused, its pause ended.
func TestPause_UnpauseDuringTheSwitchIsKept(t *testing.T) {
	h := newTimeoutHarness(t)
	activities := NewSnapshotActivities(h.store)
	h.env.RegisterActivityWithOptions(activities.WriteEventSegment, activity.RegisterOptions{Name: ActivityWriteEventSegment})
	h.env.RegisterActivityWithOptions(activities.WriteSwitchSnapshot, activity.RegisterOptions{Name: ActivityWriteSwitchSnapshot})
	unpaused := false
	h.env.RegisterActivityWithOptions(func(ctx context.Context, run *orkstackv1.Run, generation, capBytes int64,
		obs ContinuationObservationInput, loss *orkstackv1.RunLossEvents, resume *orkstackv1.RunResumeState, chain string) (*orkstackv1.ContinuationObject, error) {
		if !unpaused {
			unpaused = true
			h.env.SignalWorkflow(PauseSignal, pauseSignal(false, "0"))
		}
		return activities.WriteCheckpoint(ctx, run, generation, capBytes, obs, loss, resume, chain)
	}, activity.RegisterOptions{Name: ActivityWriteCheckpoint})
	h.taskTakes(10*time.Second, 0)
	h.env.RegisterDelayedCallback(func() { h.env.SignalWorkflow(PauseSignal, pauseSignal(true, "0")) }, 5*time.Second)
	plan := pausePlan(t)
	budget := DefaultContinuationBudget(1, 1<<40, DefaultContinuationRestoreTimeout)
	plan.Continuation = &budget
	err := h.run(t, chainOf(12), plan)
	var can *workflow.ContinueAsNewError
	if !errors.As(err, &can) {
		t.Fatalf("generation 0 ended %v, want it to carry on", err)
	}
	var runID string
	var wf *orkstackv1.Orkflow
	var input *orkstackv1.Payload
	var next RunPlan
	var carry *orkstackv1.RunContinuation
	must.OK(t, converter.GetDefaultDataConverter().FromPayloads(can.Input, &runID, &wf, &input, &next, &carry))
	if carry.GetPause() != nil || carry.GetLastPause().GetEnd() != orkstackv1.PauseEnd_PAUSE_END_UNPAUSED {
		t.Fatalf("carry pause %v, last pause %v; want the unpause delivered at the switch kept", carry.GetPause(), carry.GetLastPause())
	}
}

// Two edges of a carried pause: a parent's pause whose backstop runs past
// the longest bound by a few clock milliseconds is kept, and a run that never
// took the carry gate carries none, whatever its saved state holds.
func TestAdoptPauseCarry_Edges(t *testing.T) {
	at := time.Date(2026, 11, 15, 10, 0, 0, 0, time.UTC)
	plan := RunPlan{Pause: pausePlan(t).Pause, Origin: &RunOrigin{Kind: "RUN_ORIGIN_KIND_PARENT_RUN", ID: testParentRunID}}
	p := plan.Pause
	longest := time.Duration(p.MaxTimeoutSeconds+p.InheritedGraceSeconds*p.InheritedGraceMaxLevels) * time.Second
	skewed := &orkstackv1.InheritedPause{ParentRunId: testParentRunID, Since: timestamppb.New(at),
		BackstopAt: timestamppb.New(at.Add(longest + 5*time.Millisecond))}
	s := &runState{versions: map[string]workflow.Version{pauseCarryVersion: 1}, pauses: &fakeReceiveChannel{}, plan: plan}
	if got := s.adoptPauseCarry(&orkstackv1.RunContinuation{PausedByParent: skewed},
		&orkstackv1.Run{Status: statusPaused, PausedByParent: skewed}); got != "" {
		t.Fatalf("a backstop a few milliseconds past the bound was refused: %s", got)
	}
	last := &orkstackv1.RunPause{PausedAt: timestamppb.New(at), ExpiresAt: timestamppb.New(at.Add(time.Hour)),
		End: orkstackv1.PauseEnd_PAUSE_END_UNPAUSED, EndedAt: timestamppb.New(at.Add(time.Minute))}
	gateless := &runState{pauses: &fakeReceiveChannel{}, plan: plan}
	if got := gateless.adoptPauseCarry(&orkstackv1.RunContinuation{}, &orkstackv1.Run{LastPause: last}); got != "" {
		t.Fatalf("a switch before the pause carry was refused for its saved last pause: %s", got)
	}
}
