package engine_test

import (
	"sync"
	"testing"
	"time"

	"go.temporal.io/sdk/workflow"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"

	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/internal/testsupport/must"
)

// cascadePausePlan is a pause plan that cascades (PR2a): every run of the
// fixture, parent and child, accepts pause requests.
func cascadePausePlan() *engine.PausePlan {
	p := engine.DefaultPausePlan(7*24*3600, 31*24*3600)
	p.Cascades = true
	return &p
}

// parentPauseSignal is an API request to the parent run.
func parentPauseSignal(pause bool, requestID string) map[string]any {
	return map[string]any{"format": 1, "pause": pause, "principal": "alice", "reason": "maintenance",
		"requestId": requestID, "timeoutSeconds": "3600"}
}

// pausedChild records the pause requests a mocked child run receives and
// returns once it has been unpaused (or after an hour, so a test whose
// cascade never arrives fails on what it saw rather than timing out).
type pausedChild struct {
	mu       sync.Mutex
	received []*orkstackv1.RunPauseSignal
}

func (c *pausedChild) seen() []*orkstackv1.RunPauseSignal {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]*orkstackv1.RunPauseSignal(nil), c.received...)
}

func (c *pausedChild) run(t *testing.T) func(ctx workflow.Context, _ childStart) (*orkstackv1.Run, error) {
	return func(ctx workflow.Context, _ childStart) (*orkstackv1.Run, error) {
		ch := workflow.GetSignalChannel(ctx, engine.PauseSignal)
		timer := workflow.NewTimer(ctx, time.Hour)
		for done := false; !done; {
			sel := workflow.NewSelector(ctx)
			sel.AddReceive(ch, func(r workflow.ReceiveChannel, _ bool) {
				var sig *orkstackv1.RunPauseSignal
				r.Receive(ctx, &sig)
				c.mu.Lock()
				c.received = append(c.received, sig)
				c.mu.Unlock()
				done = !sig.GetPause()
			})
			sel.AddFuture(timer, func(workflow.Future) { done = true })
			sel.Select(ctx)
		}
		return childOutputs(t, map[string]any{"digest": "sha256:abc", "url": "https://r/abc"}), nil
	}
}

// pausableComposedFixture is composedFixture with both runs pausable.
func pausableComposedFixture(t *testing.T, childPausable bool) (*orkstackv1.Orkflow, engine.RunPlan) {
	t.Helper()
	parent, plan, _ := composedFixture(t, composedChildDocument)
	plan.Pause = cascadePausePlan()
	if childPausable {
		plan.Children["release"].Plan.Pause = cascadePausePlan()
	}
	return parent, plan
}

// A parent's pause reaches its running child as an inherited pause, and its
// unpause lifts it: two rounds, each with its own cascade counter, the
// pause naming the parent, the originating pause's expiry and depth 1.
func TestPause_CascadesToARunningChild(t *testing.T) {
	h := newComposeHarness(t, defaultBehavior)
	parent, plan := pausableComposedFixture(t, true)
	child := &pausedChild{}
	h.mockChild(child.run(t))
	h.env.RegisterDelayedCallback(func() { h.env.SignalWorkflow(engine.PauseSignal, parentPauseSignal(true, "req-1")) }, time.Minute)
	h.env.RegisterDelayedCallback(func() { h.env.SignalWorkflow(engine.PauseSignal, parentPauseSignal(false, "req-2")) }, 2*time.Minute)
	start := h.env.Now()
	h.run("ork-parent", parent, payload(t, nil), plan)
	must.OKf(t, h.env.GetWorkflowError(), "workflow error")

	got := child.seen()
	if len(got) != 2 {
		t.Fatalf("the child received %d pause requests %v, want the pause and the unpause", len(got), got)
	}
	pause, unpause := got[0], got[1]
	if !pause.GetPause() || !pause.GetInherited() || pause.GetParentRunId() != "ork-parent" ||
		pause.GetDepth() != 1 || pause.GetOriginExpiresAt() == nil {
		t.Fatalf("cascaded pause %v, want an inherited pause from ork-parent at depth 1 with its origin's expiry", pause)
	}
	if unpause.GetPause() || !unpause.GetInherited() || unpause.GetCascadeSeq() <= pause.GetCascadeSeq() {
		t.Fatalf("cascaded unpause %v after %v, want an inherited unpause of a later round", unpause, pause)
	}
	want := start.Add(time.Minute + time.Hour)
	if at := pause.GetOriginExpiresAt().AsTime(); at.Sub(want).Abs() > time.Second {
		t.Fatalf("origin expiry %s, want the parent's own pause expiry %s", at, want)
	}
}

// A child whose plan lacks Pause is sent nothing: the parent warns
// CHILD_NOT_PAUSABLE on its run and keeps going.
func TestPause_ChildWithoutPauseIsNotSentAndWarned(t *testing.T) {
	h := newComposeHarness(t, defaultBehavior)
	parent, plan := pausableComposedFixture(t, false)
	child := &pausedChild{}
	h.mockChild(child.run(t))
	h.env.RegisterDelayedCallback(func() { h.env.SignalWorkflow(engine.PauseSignal, parentPauseSignal(true, "req-1")) }, time.Minute)
	h.env.RegisterDelayedCallback(func() { h.env.SignalWorkflow(engine.PauseSignal, parentPauseSignal(false, "req-2")) }, 2*time.Minute)
	h.run("ork-parent", parent, payload(t, nil), plan)
	must.OKf(t, h.env.GetWorkflowError(), "workflow error")
	if got := child.seen(); len(got) != 0 {
		t.Fatalf("a child without Pause received %v", got)
	}
	run := h.snapshotAt(t, orkstackv1.EventType_EVENT_TYPE_RUN_PAUSED, "")
	warnings := run.GetPauseWarnings()
	if len(warnings) != 1 || warnings[0].GetCode() != "CHILD_NOT_PAUSABLE" || warnings[0].GetTaskId() != "release" ||
		warnings[0].GetChildRunId() != plan.Children["release"].RunID {
		t.Fatalf("pause warnings %v, want one CHILD_NOT_PAUSABLE for release", warnings)
	}
}

// A plan from before the cascade (Cascades unset) pauses the parent alone.
func TestPause_PlanWithoutCascadeSendsNothing(t *testing.T) {
	h := newComposeHarness(t, defaultBehavior)
	parent, plan := pausableComposedFixture(t, true)
	plan.Pause.Cascades = false
	child := &pausedChild{}
	h.mockChild(child.run(t))
	h.env.RegisterDelayedCallback(func() { h.env.SignalWorkflow(engine.PauseSignal, parentPauseSignal(true, "req-1")) }, time.Minute)
	h.env.RegisterDelayedCallback(func() { h.env.SignalWorkflow(engine.PauseSignal, parentPauseSignal(false, "req-2")) }, 2*time.Minute)
	h.run("ork-parent", parent, payload(t, nil), plan)
	must.OKf(t, h.env.GetWorkflowError(), "workflow error")
	if got := child.seen(); len(got) != 0 {
		t.Fatalf("a plan without the cascade sent the child %v", got)
	}
}
