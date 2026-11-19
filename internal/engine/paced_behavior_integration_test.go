//go:build integration

package engine_test

import (
	"context"
	"fmt"
	"strconv"
	"sync"
	"testing"
	"time"

	"go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/worker"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
)

// startPacedCANStack is a versioned CAN stack whose Orker and event worker
// complete their activities one at a time, in the order the run scheduled
// them, each only once its execution is quiescent: no workflow task
// pending, no timer about to fire, and every pending activity parked in the
// pacer. Every completion then lands in a workflow task of its own after
// the previous one's has completed, so a recorded history — the event at which a continuation
// budget is crossed, how many items a fan-out dispatched before it held,
// how many generations a run had — is decided by the plan and the script,
// never by how a loaded host batches task results and detached event
// publishes. Tasks named in keep are behavior's own to run (one that
// heartbeats until it is cancelled): they take no turn and never block one,
// but count as parked.
//
// Every parked activity holds one of its worker's execution slots; the
// stacks pace at most a run's in-flight cap of tasks (16 or the default)
// and its publish window, well under the Orker's 100 slots and the event
// worker's default.
func startPacedCANStack(t *testing.T, ctx context.Context, label string, behavior canBehavior,
	beforeRead func(ctx context.Context, offset int64) error, keep ...string,
) *canStack {
	t.Helper()
	p := &pacer{registry: parkedActivities, keep: map[string]bool{}}
	for _, id := range keep {
		p.keep[id] = true
	}
	return startCANStackWith(t, ctx, label, true, behavior, beforeRead, p)
}

// startPacedCANStackKeeping is startPacedCANStack whose kept attempts are
// chosen per call: keep says, before behavior runs, whether this one attempt
// is behavior's own (one that heartbeats once and then says nothing until
// the cluster times it out), so it holds no turn while it waits; every other
// attempt of the same task is paced.
func startPacedCANStackKeeping(t *testing.T, ctx context.Context, label string, behavior canBehavior,
	keep func(in *orkstackv1.ExecuteTaskInput) bool,
) *canStack {
	t.Helper()
	p := &pacer{registry: parkedActivities, keep: map[string]bool{}, keepCall: keep}
	return startCANStackWith(t, ctx, label, true, behavior, nil, p)
}

type parked struct {
	kept    bool
	attempt int32
}

// pacer is one stack's pacing: which of its tasks it keeps, over the one
// registry of parked activities every stack of the process shares.
type pacer struct {
	*registry
	keep map[string]bool
	// keepCall, when set, also keeps the attempts it says (startPacedCANStackKeeping).
	keepCall func(in *orkstackv1.ExecuteTaskInput) bool
	// register, when set, registers further activities on the stack's Orker
	// worker, which pace them through turn themselves (the line indexer).
	register func(s *canStack, w worker.Worker)
}

// registry is every paced stack's parked activities, by execution. It is
// one per process because the event channel (ork-engine-events) is fixed:
// a publish of one stack's run may be taken by another stack's event
// worker still polling in the same test, and its turn must still count.
type registry struct {
	mu     sync.Mutex
	parked map[string]map[int64]parked // execution → activity sequence number
	turn   map[string]int64            // execution → the activity holding its turn
}

var parkedActivities = &registry{parked: map[string]map[int64]parked{}, turn: map[string]int64{}}

type executeFunc = func(context.Context, *orkstackv1.ExecuteTaskInput) (*orkstackv1.ExecuteTaskResult, error)

type publishFunc = func(context.Context, *orkstackv1.Event, string, string) error

func (p *pacer) execute(s *canStack, inner executeFunc) executeFunc {
	return func(ctx context.Context, in *orkstackv1.ExecuteTaskInput) (*orkstackv1.ExecuteTaskResult, error) {
		release, err := p.turn(ctx, s, p.keep[in.GetTaskId()] || (p.keepCall != nil && p.keepCall(in)))
		if err != nil {
			return nil, err
		}
		defer release()
		return inner(ctx, in)
	}
}

func (p *pacer) publish(s *canStack, inner publishFunc) publishFunc {
	return func(ctx context.Context, event *orkstackv1.Event, snapshotURI, dedupeKey string) error {
		release, err := p.turn(ctx, s, false)
		if err != nil {
			return err
		}
		defer release()
		return inner(ctx, event, snapshotURI, dedupeKey)
	}
}

// turn parks the calling activity and, unless it is kept, waits for its
// turn. The returned release unparks it once its answer is returned.
func (p *pacer) turn(ctx context.Context, s *canStack, kept bool) (func(), error) {
	info := activity.GetInfo(ctx)
	// The SDK numbers activities in command order; an id that is not its
	// number would leave the turns unordered, so it fails the attempt.
	seq, err := strconv.ParseInt(info.ActivityID, 10, 64)
	if err != nil {
		return nil, fmt.Errorf("pacer: activity id %q is not a sequence number: %w", info.ActivityID, err)
	}
	execution := info.WorkflowExecution.ID + "/" + info.WorkflowExecution.RunID
	p.enter(execution, seq, parked{kept: kept, attempt: info.Attempt})
	release := func() { p.leave(execution, seq, info.Attempt) }
	if kept {
		return release, nil
	}
	if err := p.awaitTurn(ctx, s, info, execution, seq); err != nil {
		release()
		return nil, err
	}
	return release, nil
}

func (p *registry) enter(execution string, seq int64, entry parked) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.parked[execution] == nil {
		p.parked[execution] = map[int64]parked{}
	}
	p.parked[execution][seq] = entry
}

// leave removes the entry only while it is still this attempt's, so a late
// return of an earlier attempt never unparks its retry.
func (p *registry) leave(execution string, seq int64, attempt int32) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if entry, ok := p.parked[execution][seq]; ok && entry.attempt == attempt {
		delete(p.parked[execution], seq)
		if held, ok := p.turn[execution]; ok && held == seq {
			delete(p.turn, execution)
		}
	}
	if len(p.parked[execution]) == 0 {
		delete(p.parked, execution)
	}
}

// first reports whether seq is the lowest parked number of execution that
// takes turns.
func (p *registry) first(execution string, seq int64) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	for other, entry := range p.parked[execution] {
		if !entry.kept && other < seq {
			return false
		}
	}
	return true
}

// claim gives seq its execution's turn when no other activity holds it and
// seq is still the lowest parked number that takes turns: first and the
// history read are not one step, so a lower activity that parked between
// them, or one claiming at the same moment, wins instead. The turn is held
// until seq leaves; the next one then waits for its completion to be
// recorded (quiescent).
func (p *registry) claim(execution string, seq int64) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	if _, held := p.turn[execution]; held {
		return false
	}
	for other, entry := range p.parked[execution] {
		if !entry.kept && other < seq {
			return false
		}
	}
	p.turn[execution] = seq
	return true
}

// isParked reports whether the activity scheduled at seq is parked for
// execution.
func (p *registry) isParked(execution string, seq int64) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	_, ok := p.parked[execution][seq]
	return ok
}

// awaitTurn returns once this activity is first and its execution is
// quiescent, heartbeating while it waits so the wait itself is never an
// attempt's timeout. A wait that ends with its context says what it was
// last waiting on.
func (p *pacer) awaitTurn(ctx context.Context, s *canStack, info activity.Info, execution string, seq int64) error {
	ticker := time.NewTicker(20 * time.Millisecond)
	defer ticker.Stop()
	waiting := "its turn"
	for {
		activity.RecordHeartbeat(ctx)
		if p.first(execution, seq) {
			waiting = p.quiescent(ctx, s, info, execution)
			if waiting == "" {
				if p.claim(execution, seq) {
					return nil
				}
				waiting = "another activity's turn"
			}
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("pacer: activity %s of %s still waiting on %s: %w", info.ActivityID, execution, waiting, ctx.Err())
		case <-ticker.C:
		}
	}
}

// quiescent is what execution still waits on before a turn, "" for
// nothing, read from its history alone: a workflow task scheduled and not
// yet completed, an activity scheduled and not yet closed that is not
// parked here, or an open timer about to fire (timerMargin). The history
// is read once per tick and never takes the execution's lock, as a
// describe would; a read that fails is retried on the next tick, bounded
// so the heartbeat above it keeps beating. It decides only when to answer,
// never what.
func (p *pacer) quiescent(ctx context.Context, s *canStack, info activity.Info, execution string) string {
	rctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	workflowTask := false
	activities := map[int64]bool{}
	timers := map[string]time.Time{}
	iter := s.client.GetWorkflowHistory(rctx, info.WorkflowExecution.ID, info.WorkflowExecution.RunID, false,
		enums.HISTORY_EVENT_FILTER_TYPE_ALL_EVENT)
	for iter.HasNext() {
		event, err := iter.Next()
		if err != nil {
			return "a read of the execution's history, last: " + err.Error()
		}
		switch event.GetEventType() {
		case enums.EVENT_TYPE_WORKFLOW_TASK_SCHEDULED:
			workflowTask = true
		case enums.EVENT_TYPE_WORKFLOW_TASK_COMPLETED, enums.EVENT_TYPE_WORKFLOW_TASK_FAILED,
			enums.EVENT_TYPE_WORKFLOW_TASK_TIMED_OUT:
			workflowTask = false
		case enums.EVENT_TYPE_ACTIVITY_TASK_SCHEDULED:
			activities[event.GetEventId()] = true
		case enums.EVENT_TYPE_TIMER_STARTED:
			a := event.GetTimerStartedEventAttributes()
			timers[a.GetTimerId()] = event.GetEventTime().AsTime().Add(a.GetStartToFireTimeout().AsDuration())
		case enums.EVENT_TYPE_TIMER_FIRED:
			delete(timers, event.GetTimerFiredEventAttributes().GetTimerId())
		case enums.EVENT_TYPE_TIMER_CANCELED:
			delete(timers, event.GetTimerCanceledEventAttributes().GetTimerId())
		}
		if closed := closedActivity(event); closed != 0 {
			delete(activities, closed)
		}
	}
	if workflowTask {
		return "a pending workflow task"
	}
	// The SDK's activity id is its scheduled event's id.
	for scheduled := range activities {
		if !p.isParked(execution, scheduled) {
			return fmt.Sprintf("pending activity %d", scheduled)
		}
	}
	horizon := time.Now().Add(timerMargin)
	for id, at := range timers {
		if at.Before(horizon) {
			return fmt.Sprintf("timer %s due at %s", id, at.Format(time.RFC3339Nano))
		}
	}
	return ""
}

// closedActivity is the scheduled event id of the activity event closes, 0
// for any other event.
func closedActivity(event *historypb.HistoryEvent) int64 {
	switch {
	case event.GetActivityTaskCompletedEventAttributes() != nil:
		return event.GetActivityTaskCompletedEventAttributes().GetScheduledEventId()
	case event.GetActivityTaskFailedEventAttributes() != nil:
		return event.GetActivityTaskFailedEventAttributes().GetScheduledEventId()
	case event.GetActivityTaskTimedOutEventAttributes() != nil:
		return event.GetActivityTaskTimedOutEventAttributes().GetScheduledEventId()
	case event.GetActivityTaskCanceledEventAttributes() != nil:
		return event.GetActivityTaskCanceledEventAttributes().GetScheduledEventId()
	}
	return 0
}

// timerMargin is how close to firing an open timer must be to hold a turn:
// more than a turn takes, so a timer fires in a workflow task of its own
// rather than with a completion granted just before it. A timer further out
// (an await's deadline, a run timeout) holds nothing until it nears.
const timerMargin = 2 * time.Second

// waitIdle waits until runID's current execution is running with no
// workflow task and no activity pending: everything it can do without
// outside input is done.
func (s *canStack) waitIdle(runID string, within time.Duration) error {
	ctx, cancel := context.WithTimeout(s.ctx, within)
	defer cancel()
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()
	var last error // set on every pass that does not return
	for {
		described, err := describeExecution(s.client, ctx, runID, "")
		switch {
		case err != nil:
			last = err
		case described.GetWorkflowExecutionInfo().GetStatus() != enums.WORKFLOW_EXECUTION_STATUS_RUNNING:
			return fmt.Errorf("run %s closed %s before it went idle", runID, described.GetWorkflowExecutionInfo().GetStatus())
		case described.GetPendingWorkflowTask() == nil && len(described.GetPendingActivities()) == 0:
			return nil
		default:
			last = fmt.Errorf("workflow task pending %v, %d activities pending",
				described.GetPendingWorkflowTask() != nil, len(described.GetPendingActivities()))
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("run %s did not go idle within %s: %w (last: %v)", runID, within, ctx.Err(), last)
		case <-ticker.C:
		}
	}
}
