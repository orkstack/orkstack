package engine_test

import (
	"fmt"
	"testing"

	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/sdk/converter"
	"google.golang.org/protobuf/proto"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/internal/task"
)

// The shapes PR2a's recordings must have, as pure checks over a history:
// each recorder refuses to write a fixture without its shape, and
// TestCommittedPausePR2Fixtures_HaveTheirShape holds the committed fixtures
// to the same checks, so a fixture's claim is read off its own events.

// pauseSignalEvent is one PauseSignal the run received: its event id, and
// whether it asked for a pause.
type pauseSignalEvent struct {
	id    int64
	pause bool
}

// pauseSignalEvents is every PauseSignal the run received, in history order.
func pauseSignalEvents(h *historypb.History) ([]pauseSignalEvent, error) {
	var out []pauseSignalEvent
	for _, e := range h.GetEvents() {
		attrs := e.GetWorkflowExecutionSignaledEventAttributes()
		if attrs == nil || attrs.GetSignalName() != engine.PauseSignal {
			continue
		}
		// A parent's cascade sends the proto; a client sends its JSON.
		var sig *orkstackv1.RunPauseSignal
		if err := converter.GetDefaultDataConverter().FromPayloads(attrs.GetInput(), &sig); err == nil && sig != nil {
			out = append(out, pauseSignalEvent{id: e.GetEventId(), pause: sig.GetPause()})
			continue
		}
		var raw map[string]any
		if err := converter.GetDefaultDataConverter().FromPayloads(attrs.GetInput(), &raw); err != nil {
			return nil, fmt.Errorf("decode the pause signal at event %d: %w", e.GetEventId(), err)
		}
		pause, ok := raw["pause"].(bool)
		if !ok {
			return nil, fmt.Errorf("the pause signal at event %d carries no pause field", e.GetEventId())
		}
		out = append(out, pauseSignalEvent{id: e.GetEventId(), pause: pause})
	}
	return out, nil
}

// pauseThenUnpause is the event ids of the run's one pause signal and the
// one unpause signal after it.
func pauseThenUnpause(h *historypb.History) (pause, unpause int64, err error) {
	signals, err := pauseSignalEvents(h)
	if err != nil {
		return 0, 0, err
	}
	if len(signals) != 2 || !signals[0].pause || signals[1].pause {
		return 0, 0, fmt.Errorf("pause signals %v, want one pause then one unpause", signals)
	}
	return signals[0].id, signals[1].id, nil
}

// taskSchedules is the event id of every task activity the run scheduled.
func taskSchedules(h *historypb.History) []int64 {
	var out []int64
	for _, e := range h.GetEvents() {
		if attrs := e.GetActivityTaskScheduledEventAttributes(); attrs != nil && attrs.GetActivityType().GetName() == task.ActivityExecuteTask {
			out = append(out, e.GetEventId())
		}
	}
	return out
}

// heldBetween is nil when no task was scheduled in (from, to) and one was
// after to; it is the next task's schedule event id.
func heldBetween(h *historypb.History, from, to int64) (next int64, err error) {
	for _, id := range taskSchedules(h) {
		if id > from && id < to {
			return 0, fmt.Errorf("a task was scheduled at event %d, while the run was paused (events %d to %d)", id, from, to)
		}
		if id > to {
			return id, nil
		}
	}
	return 0, fmt.Errorf("no task was scheduled after the unpause at event %d", to)
}

// checkPinClockShape: the pause cancels a timer started before it (the
// pin's), nothing is scheduled while paused, and the unpause starts a timer
// with less time left than the pin's had, before the next member is
// scheduled.
func checkPinClockShape(h *historypb.History) error {
	pause, unpause, err := pauseThenUnpause(h)
	if err != nil {
		return err
	}
	next, err := heldBetween(h, pause, unpause)
	if err != nil {
		return err
	}
	started := map[int64]*historypb.TimerStartedEventAttributes{}
	var pinTimer *historypb.TimerStartedEventAttributes
	for _, e := range h.GetEvents() {
		if attrs := e.GetTimerStartedEventAttributes(); attrs != nil {
			started[e.GetEventId()] = attrs
		}
		attrs := e.GetTimerCanceledEventAttributes()
		if attrs != nil && e.GetEventId() > pause && e.GetEventId() < unpause && attrs.GetStartedEventId() < pause {
			pinTimer = started[attrs.GetStartedEventId()]
		}
	}
	if pinTimer == nil {
		return fmt.Errorf("the pause (event %d) canceled no timer started before it", pause)
	}
	for id, attrs := range started {
		if id > unpause && id < next && attrs.GetStartToFireTimeout().AsDuration() < pinTimer.GetStartToFireTimeout().AsDuration() {
			return nil
		}
	}
	return fmt.Errorf("the unpause (event %d) re-armed no timer shorter than the pin's %s before the next member (event %d)",
		unpause, pinTimer.GetStartToFireTimeout().AsDuration(), next)
}

// checkCascadeChildShape: the child received its parent's pause and then
// its unpause, and was held between them.
func checkCascadeChildShape(h *historypb.History) error {
	pause, unpause, err := pauseThenUnpause(h)
	if err != nil {
		return err
	}
	_, err = heldBetween(h, pause, unpause)
	return err
}

// checkDeferredG1Shape: the generation received no pause signal of its own
// (it applied the carried one), scheduled nothing before the unpause, and
// ran its remaining work after it.
func checkDeferredG1Shape(h *historypb.History) error {
	signals, err := pauseSignalEvents(h)
	if err != nil {
		return err
	}
	if len(signals) != 1 || signals[0].pause {
		return fmt.Errorf("pause signals %v, want the unpause alone", signals)
	}
	_, err = heldBetween(h, 0, signals[0].id)
	return err
}

// TestCommittedPausePR2Fixtures_HaveTheirShape holds the committed fixtures
// to their recorders' checks.
func TestCommittedPausePR2Fixtures_HaveTheirShape(t *testing.T) {
	for path, check := range map[string]func(*historypb.History) error{
		pausePinFixture:   checkPinClockShape,
		pauseCascadeChild: checkCascadeChildShape,
		pauseDeferredG1:   checkDeferredG1Shape,
	} {
		t.Run(path, func(t *testing.T) {
			requireCommittedFixture(t, path)
			if err := check(loadHistoryFile(t, path)); err != nil {
				t.Fatalf("%s: %v", path, err)
			}
		})
	}
}

// withoutEvent is h without the first event drop accepts after the event
// after names (0: from the start).
func withoutEvent(h *historypb.History, after int64, drop func(*historypb.HistoryEvent) bool) *historypb.History {
	out := &historypb.History{}
	dropped := false
	for _, e := range h.GetEvents() {
		if !dropped && e.GetEventId() > after && drop(e) {
			dropped = true
			continue
		}
		out.Events = append(out.Events, e)
	}
	return out
}

// The checks are not vacuous: each refuses a committed fixture with the
// event its claim rests on removed.
func TestPausePR2ShapeChecks_RefuseAMissingEvent(t *testing.T) {
	pin := loadHistoryFile(t, pausePinFixture)
	pause, unpause, err := pauseThenUnpause(pin)
	if err != nil {
		t.Fatal(err)
	}
	child := loadHistoryFile(t, pauseCascadeChild)
	childPause, _, err := pauseThenUnpause(child)
	if err != nil {
		t.Fatal(err)
	}
	g1 := loadHistoryFile(t, pauseDeferredG1)
	signaled := func(e *historypb.HistoryEvent) bool { return e.GetWorkflowExecutionSignaledEventAttributes() != nil }
	canceled := func(e *historypb.HistoryEvent) bool { return e.GetTimerCanceledEventAttributes() != nil }
	timerStarted := func(e *historypb.HistoryEvent) bool { return e.GetTimerStartedEventAttributes() != nil }
	for name, tc := range map[string]struct {
		check func(*historypb.History) error
		h     *historypb.History
	}{
		"pin: the pause's timer cancel": {checkPinClockShape, withoutEvent(pin, pause, canceled)},
		"pin: the unpause's re-arm":     {checkPinClockShape, withoutEvent(pin, unpause, timerStarted)},
		"pin: the unpause":              {checkPinClockShape, withoutEvent(pin, pause, signaled)},
		"child: the unpause":            {checkCascadeChildShape, withoutEvent(child, childPause, signaled)},
		"g1: the unpause":               {checkDeferredG1Shape, withoutEvent(g1, 0, signaled)},
		"pin: a member while paused":    {checkPinClockShape, scheduledAt(pin, pause+1)},
		"child: a task while paused":    {checkCascadeChildShape, scheduledAt(child, childPause+1)},
		"g1: a task before the unpause": {checkDeferredG1Shape, scheduledAt(g1, 1)},
	} {
		t.Run(name, func(t *testing.T) {
			if err := tc.check(tc.h); err == nil {
				t.Fatal("the check accepted the altered history")
			}
		})
	}
}

// scheduledAt is h with its first task schedule after id renumbered to id,
// as if the task had been scheduled there.
func scheduledAt(h *historypb.History, id int64) *historypb.History {
	out := &historypb.History{}
	moved := false
	for _, e := range h.GetEvents() {
		if attrs := e.GetActivityTaskScheduledEventAttributes(); !moved && e.GetEventId() > id && attrs != nil &&
			attrs.GetActivityType().GetName() == task.ActivityExecuteTask {
			c := proto.Clone(e).(*historypb.HistoryEvent)
			c.EventId = id
			out.Events = append(out.Events, c)
			moved = true
			continue
		}
		out.Events = append(out.Events, e)
	}
	return out
}
