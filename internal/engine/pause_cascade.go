package engine

import (
	"fmt"
	"slices"
	"time"

	"go.temporal.io/sdk/workflow"
	"google.golang.org/protobuf/types/known/timestamppb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/payloadcrypt"
)

// The cascade (design §4.11, §18.3, §19 M4/M5, §20.2): a run that becomes
// paused tells each of its in-flight child runs, and its unpause lifts what
// it sent. Each child that becomes paused forwards to its own children, so
// the tree is covered level by level, with no list and no visibility read.
//
// Every send runs in its own coroutine: a send waits until its child has
// started, and neither loop may block on it. Results come back on the
// round's buffered channel, which both loops' selectors read; the pass top
// takes them (takeCascadeResults). A failed send is retried within its round
// on its own timer; a pause that keeps failing is recorded on the run as a
// warning and noted on the composed task, an unpause is noted. Nothing
// re-sends it until the run's next pause or unpause.

// pauseCascadeVersion gates the cascade's commands: asked lazily at the
// first round that has a child to send to, under a plan that pins Cascades,
// so a history recorded before it replays its DefaultVersion branch, which
// pauses the run alone.
const pauseCascadeVersion = "pause-cascade"

// The pinned defaults of a cascade round's retries (PausePlan).
const (
	DefaultCascadeRetries              = 3
	DefaultCascadeRetryIntervalSeconds = 5
)

// The pause warning codes (Run.pause_warnings).
const (
	pauseWarningChildNotPausable = "CHILD_NOT_PAUSABLE"
	pauseWarningChildPauseFailed = "CHILD_PAUSE_FAILED"
)

// cascadeState is the run's cascade: the round counter, what the last pause
// round signalled, the round's channel and cancel, and the sends in flight.
// Every field is generation-local: a run with a child in flight never
// switches, and a run whose sends are live waits for them (canSwitchNow).
type cascadeState struct {
	seq      int64
	cascaded map[string]bool // composed task ids the last pause round targeted
	// results is one channel for every round, so a round's stale result is
	// told by its counter, and every send's end, a canceled one's included,
	// wakes the selectors that read it.
	results  workflow.Channel
	cancel   workflow.CancelFunc
	live     int // coroutines not yet finished, of every round
	warnings []*orkstackv1.PauseWarning
}

// cascadeResultsBuffer is the results channel's room: a send beyond it waits
// in its own coroutine until the pass top drains the channel.
const cascadeResultsBuffer = 256

// cascadeResult is one send's outcome.
type cascadeResult struct {
	TaskID   string
	Round    int64
	Err      string // "" on success
	Closed   bool   // the child had already ended
	Canceled bool   // its round or the run was canceled: nothing to record
}

// cascadeSendForTest, when set, replaces the send to a child: the fault
// cases a real cluster cannot produce on demand (a signal the server
// refuses). TEST-ONLY.
var cascadeSendForTest func(ctx workflow.Context, taskID string, sig *orkstackv1.RunPauseSignal) error

// cascades reports that the run's plan pins the cascade.
func (s *runState) cascades() bool { return s.plan.Pause != nil && s.plan.Pause.Cascades }

// childTargets are the composed tasks whose child run is in flight, in
// declaration order.
func (s *runState) childTargets(inFlight map[string]workflow.Future) []string {
	var out []string
	for _, id := range s.order {
		if s.children[id] == nil {
			continue
		}
		if f, ok := inFlight[id]; ok && f != nil {
			out = append(out, id)
		}
	}
	return out
}

// cascadeRound sends one round to targets: a pause round on becoming paused,
// an unpause round on becoming unpaused. It issues no command unless there
// is a child to send to.
func (s *runState) cascadeRound(ctx workflow.Context, inFlight map[string]workflow.Future, pause bool, req *pauseRequest) {
	if !s.cascades() {
		return
	}
	c := &s.cascade
	var targets []string
	if pause {
		targets = s.childTargets(inFlight)
	} else {
		for _, id := range s.childTargets(inFlight) {
			if c.cascaded[id] {
				targets = append(targets, id)
			}
		}
	}
	if !pause {
		c.cascaded, c.warnings = nil, nil
	}
	if len(targets) == 0 {
		if pause {
			c.cascaded, c.warnings = map[string]bool{}, nil
		}
		// A round with nothing to send still supersedes the last one: its
		// sends end, and their results are not read as this round's.
		if c.cancel != nil {
			c.cancel()
			c.cancel = nil
		}
		c.seq++
		return
	}
	if s.version(ctx, pauseCascadeVersion, 1) < 1 {
		return
	}
	if c.cancel != nil {
		c.cancel() // the previous round's sends end; their results are discarded
	}
	c.seq++
	round := c.seq
	roundCtx, cancel := workflow.WithCancel(ctx)
	c.cancel = cancel
	if c.results == nil {
		c.results = workflow.NewBufferedChannel(ctx, cascadeResultsBuffer)
	}
	if pause {
		c.cascaded, c.warnings = map[string]bool{}, nil
	}
	for _, id := range targets {
		child := s.children[id]
		if child.plan == nil || child.plan.Plan.Pause == nil {
			if pause {
				s.addPauseWarning(ctx, id, child, pauseWarningChildNotPausable,
					fmt.Sprintf("child run %s was started without support for pausing; it keeps running", childRunIDOf(child)))
			}
			continue
		}
		future, ok := inFlight[id].(workflow.ChildWorkflowFuture)
		if !ok {
			continue // never: a composed task's in-flight future is its child's
		}
		if pause {
			c.cascaded[id] = true
		}
		sig := s.cascadeSignal(pause, round, req)
		c.live++
		s.goSend(roundCtx, c.results, id, round, future, sig)
	}
}

// goSend starts the coroutine that sends sig to one child, retrying within
// the round, and reports once, unless its round was canceled.
func (s *runState) goSend(roundCtx workflow.Context, results workflow.Channel, id string, round int64,
	future workflow.ChildWorkflowFuture, sig *orkstackv1.RunPauseSignal,
) {
	p := s.plan.Pause
	workflow.Go(roundCtx, func(g workflow.Context) {
		result := cascadeResult{TaskID: id, Round: round}
		defer func() {
			// Every end is sent, so a loop waiting on the switch wakes once
			// no send is live; then the count drops, with no yield between.
			results.Send(g, result)
			s.cascade.live--
		}()
		for attempt := int64(1); ; attempt++ {
			var err error
			if cascadeSendForTest != nil {
				err = cascadeSendForTest(g, id, sig)
			} else {
				err = future.SignalChildWorkflow(g, PauseSignal, s.sealedForChild(id, sig)).Get(g, nil)
			}
			reading := categoryOf(err)
			if g.Err() != nil || reading.canceled {
				result.Canceled = true // never retried or warned (M5)
				return
			}
			switch {
			case err == nil:
			case reading.unknownRun || future.IsReady():
				result.Closed = true
			case attempt <= p.CascadeRetries:
				// Doubling, at most eight intervals apart.
				backoff := time.Duration(p.CascadeRetryIntervalSeconds) * time.Second << min(attempt-1, 3)
				if sleepErr := workflow.Sleep(g, backoff); sleepErr != nil {
					result.Canceled = true
					return
				}
				continue
			default:
				result.Err = err.Error()
			}
			return
		}
	})
}

// cascadeSignal is the request a round sends: an inherited pause names this
// run, its round, its depth below the pause that began the cascade, and that
// pause's expiry, which every level's backstop is derived from.
func (s *runState) cascadeSignal(pause bool, round int64, req *pauseRequest) *orkstackv1.RunPauseSignal {
	sig := &orkstackv1.RunPauseSignal{Format: 1, Pause: pause, Inherited: true, ParentRunId: s.runID, CascadeSeq: round}
	if !pause {
		return sig
	}
	sig.Depth = 1
	switch {
	case req != nil && req.Inherited:
		// Paused by its parent: pass the originating pause down a level.
		sig.Depth = max(req.Depth, 1) + 1
		if t, err := time.Parse(time.RFC3339Nano, req.OriginExpiresAt); err == nil {
			sig.OriginExpiresAt = timestamppb.New(t)
		}
	case s.pause != nil:
		sig.OriginExpiresAt = timestamppb.New(s.pause.expiresAt)
		sig.Principal, sig.Reason = s.pause.by, s.pause.reason
	}
	return sig
}

// takeCascadeResults applies every result already delivered, never
// blocking: called at the top of each pass of both loops.
func (s *runState) takeCascadeResults(ctx workflow.Context) {
	c := &s.cascade
	if c.results == nil {
		return
	}
	for {
		var r cascadeResult
		if !c.results.ReceiveAsync(&r) {
			return
		}
		if r.Canceled || r.Round < c.seq {
			continue // a newer round supersedes it, or the run ended
		}
		if r.Closed || r.Err == "" {
			continue // a closed child's end is collected as usual
		}
		child := s.children[r.TaskID]
		workflow.GetLogger(ctx).Warn("pause did not reach a child run", "run_id", s.runID,
			"task_id", r.TaskID, "child_run_id", childRunIDOf(child), "error", r.Err)
		if c.cascaded != nil {
			s.addPauseWarning(ctx, r.TaskID, child, pauseWarningChildPauseFailed,
				fmt.Sprintf("could not pause child run %s: %s; it keeps running", childRunIDOf(child), r.Err))
			continue
		}
		// An unpause round: the run is no longer paused, so there is no
		// warning to show; the note on the composed task says it.
		message := fmt.Sprintf("could not unpause child run %s: %s; it stays paused until it is unpaused or its pause times out",
			childRunIDOf(child), r.Err)
		if err := s.emit(ctx, orkstackv1.EventType_EVENT_TYPE_TASK_LOG, r.TaskID, s.taskRuns[r.TaskID].GetStatus(), message); err != nil {
			workflow.GetLogger(ctx).Error("emit unpause failure failed", "run_id", s.runID, "task_id", r.TaskID, "error", err)
		}
	}
}

// addPauseWarning records one way the pause did not reach a child, and
// notes it on the composed task. A failed note is logged, never fatal.
func (s *runState) addPauseWarning(ctx workflow.Context, id string, child *childState, code, message string) {
	s.cascade.warnings = append(s.cascade.warnings, &orkstackv1.PauseWarning{
		TaskId: id, ChildRunId: childRunIDOf(child), Code: code, Message: message,
	})
	workflow.GetLogger(ctx).Warn("pause warning", "run_id", s.runID, "task_id", id, "code", code)
	if s.status != statusPaused {
		return // the round runs before the pause event, whose snapshot carries it
	}
	if err := s.emit(ctx, orkstackv1.EventType_EVENT_TYPE_TASK_LOG, id, s.taskRuns[id].GetStatus(), message); err != nil {
		workflow.GetLogger(ctx).Error("emit pause warning failed", "run_id", s.runID, "task_id", id, "error", err)
	}
}

// pauseWarningsProto is what the snapshot shows: the current pause round's,
// while the run is paused and live.
func (s *runState) pauseWarningsProto() []*orkstackv1.PauseWarning {
	if !s.paused() || s.ended() {
		return nil
	}
	return slices.Clone(s.cascade.warnings)
}

// childRunIDOf is the planned child's run id, or "".
func childRunIDOf(c *childState) string {
	if c == nil || c.plan == nil {
		return ""
	}
	return c.plan.RunID
}

// cascadeIdle reports that no send is live: the switch waits for it.
func (s *runState) cascadeIdle() bool { return s.cascade.live == 0 }

// cascadeChildEnded forgets a child that ended, so no later round targets it.
func (s *runState) cascadeChildEnded(id string) {
	delete(s.cascade.cascaded, id)
}

// sealedForChild is a pause request as a composed task's child run receives
// it: sealed for the child's own id under the tree's key in a sealed run, so
// the child opens it (payload-encryption.md §4.1 rule 2), and the request
// itself in any other.
func (s *runState) sealedForChild(id string, sig any) any {
	c := s.children[id]
	if c == nil || c.plan == nil {
		// No child id to seal for: sealed for this run, which no child
		// opens, never plaintext.
		return payloadcrypt.Seal(s.scope, sig)
	}
	return payloadcrypt.Seal(s.scope.For(payloadcrypt.RunContext(c.plan.RunID)), sig)
}
