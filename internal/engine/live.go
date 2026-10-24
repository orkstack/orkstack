package engine

import (
	"context"
	"errors"
	"fmt"
	"time"

	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	workflowpb "go.temporal.io/api/workflow/v1"
	"google.golang.org/protobuf/types/known/timestamppb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/task"
)

// LiveAttemptReader is the optional half of a RunReader that can describe a
// live execution. It is separate from RunReader, on the terms
// RunIndexTallier is separate in internal/api (stats.go), so the fakes that
// stand in for the read model need not grow: a Service whose reader lacks it
// answers InspectRun with FailedPrecondition, which is the correct answer for
// a process that reads runs from its own memory.
type LiveAttemptReader interface {
	// LiveAttempts describes one execution and returns its pending
	// ExecuteTask attempts. A run the cluster no longer holds is
	// ErrRunNotFound. Closed is true when the execution has finished; the
	// list is then empty and no history was read.
	//
	// now is the caller's clock (the handler's computed_at): heartbeat_stale
	// is measured against it, so the flag and the timestamp a client ages
	// everything by always agree.
	LiveAttempts(ctx context.Context, runID string, now time.Time) (LiveRun, error)
}

// LiveRun is one describe, decoded.
type LiveRun struct {
	Closed   bool
	Attempts []*orkstackv1.LiveAttempt
}

// ErrNoLiveReader reports a reader that cannot describe live executions.
var ErrNoLiveReader = errors.New("engine: run reader cannot inspect live attempts")

var _ LiveAttemptReader = (*TemporalRunReader)(nil)

// scheduledTask is what the ExecuteTask scheduling event says about one
// pending activity: which task it runs and the task's own bounds as
// dispatched. Nothing else of the (possibly large) input is kept.
type scheduledTask struct {
	taskID         string
	timeoutSeconds int32
	graceSeconds   int32
	// item is the fan-out item this activity runs (ExecuteTaskInput.each),
	// or nil for a task's own activity.
	item *int32
}

// LiveAttempts answers InspectRun's cluster half: one describe of the
// execution, then — only when an ExecuteTask attempt is pending — one walk of
// its history to learn which task each pending activity runs.
//
// Nothing is cached and nothing is written. The describe comes FIRST so every
// activity it reports as pending has its scheduling event in the history read
// that follows; the reverse order could miss an activity scheduled between
// the two reads.
func (r *TemporalRunReader) LiveAttempts(ctx context.Context, runID string, now time.Time) (LiveRun, error) {
	if r == nil || r.client == nil {
		return LiveRun{}, ErrNoLiveReader
	}
	// A run id is a single-use workflow id (AGENTS.md §7); the empty Temporal
	// run id selects its CURRENT execution — the newest generation of a run
	// that has continued as new. The history read below names the execution
	// this describe answered for, so a switch between the two reads cannot
	// pair one generation's pending activities with another's history.
	resp, err := r.client.DescribeWorkflowExecution(ctx, runID, "")
	if err != nil {
		var notFound *serviceerror.NotFound
		if errors.As(err, &notFound) {
			return LiveRun{}, fmt.Errorf("engine: run %s: %w", runID, ErrRunNotFound)
		}
		return LiveRun{}, fmt.Errorf("engine: describe run %s: %w", runID, err)
	}
	if resp.GetWorkflowExecutionInfo().GetCloseTime() != nil {
		return LiveRun{Closed: true}, nil
	}

	// PublishEvent, DescribeOrker, ReleasePin and a pin's session-creation
	// activity are pending activities of the same execution and are NOT
	// attempts; only ExecuteTask is.
	var pending []*workflowpb.PendingActivityInfo
	ids := map[string]struct{}{}
	for _, pa := range resp.GetPendingActivities() {
		if pa.GetActivityType().GetName() != task.ActivityExecuteTask {
			continue
		}
		pending = append(pending, pa)
		ids[pa.GetActivityId()] = struct{}{}
	}
	if len(pending) == 0 {
		return LiveRun{}, nil
	}

	execution := resp.GetWorkflowExecutionInfo().GetExecution().GetRunId()
	scheduled, err := r.scheduledTasks(ctx, runID, execution, ids)
	if err != nil {
		return LiveRun{}, err
	}
	attempts := make([]*orkstackv1.LiveAttempt, 0, len(pending))
	for _, pa := range pending {
		st, ok := scheduled[pa.GetActivityId()]
		if !ok {
			// A consistent history read taken after the describe always
			// holds the scheduling event; a miss is an invariant broken,
			// never a row to render or drop.
			return LiveRun{}, fmt.Errorf("engine: run %s: pending activity %q has no scheduling event",
				runID, pa.GetActivityId())
		}
		attempts = append(attempts, r.liveAttempt(pa, st, now))
	}
	return LiveRun{Attempts: attempts}, nil
}

// scheduledTasks walks a run's history once and decodes the ExecuteTaskInput
// of exactly the pending activities named in ids. It is deliberately NOT part
// of scan: scan runs on every GetRun, and decoding every task's input on
// every run read, for fields only InspectRun uses, is the wrong trade.
func (r *TemporalRunReader) scheduledTasks(
	ctx context.Context,
	runID, execution string,
	ids map[string]struct{},
) (map[string]scheduledTask, error) {
	iter := r.client.GetWorkflowHistory(ctx, runID, execution, false,
		enumspb.HISTORY_EVENT_FILTER_TYPE_ALL_EVENT)
	found := make(map[string]scheduledTask, len(ids))
	read := 0
	for iter.HasNext() {
		historyEvent, err := iter.Next()
		if err != nil {
			var notFound *serviceerror.NotFound
			if errors.As(err, &notFound) && read == 0 {
				return nil, fmt.Errorf("engine: run %s: %w", runID, ErrRunNotFound)
			}
			return nil, fmt.Errorf("engine: read history of run %s: %w", runID, err)
		}
		read++
		attrs := historyEvent.GetActivityTaskScheduledEventAttributes()
		if attrs == nil || attrs.GetActivityType().GetName() != task.ActivityExecuteTask {
			continue
		}
		if _, want := ids[attrs.GetActivityId()]; !want {
			continue
		}
		payloads := attrs.GetInput().GetPayloads()
		if len(payloads) == 0 {
			return nil, fmt.Errorf("engine: run %s: scheduling event %d of activity %q has no input",
				runID, historyEvent.GetEventId(), attrs.GetActivityId())
		}
		var in *orkstackv1.ExecuteTaskInput
		if err := r.converter.FromPayload(payloads[0], &in); err != nil {
			return nil, fmt.Errorf("engine: run %s: decode task input at history event %d: %w",
				runID, historyEvent.GetEventId(), err)
		}
		st := scheduledTask{
			taskID:         in.GetTaskId(),
			timeoutSeconds: in.GetTimeoutSeconds(),
			graceSeconds:   in.GetCancellationGracePeriodSeconds(),
		}
		if each := in.GetEach(); each != nil {
			index := each.GetIndex()
			st.item = &index
		}
		found[attrs.GetActivityId()] = st
		if len(found) == len(ids) {
			break
		}
	}
	return found, nil
}

// liveAttemptState maps Temporal's PendingActivityState onto OrkStack's own
// numbering, value by value. PAUSED and PAUSE_REQUESTED have no OrkStack
// member (nothing in OrkStack pauses an activity) and, with anything a later
// Temporal adds, read UNSPECIFIED rather than a number a client cannot name.
func liveAttemptState(state enumspb.PendingActivityState) orkstackv1.LiveAttemptState {
	switch state {
	case enumspb.PENDING_ACTIVITY_STATE_SCHEDULED:
		return orkstackv1.LiveAttemptState_LIVE_ATTEMPT_STATE_SCHEDULED
	case enumspb.PENDING_ACTIVITY_STATE_STARTED:
		return orkstackv1.LiveAttemptState_LIVE_ATTEMPT_STATE_STARTED
	case enumspb.PENDING_ACTIVITY_STATE_CANCEL_REQUESTED:
		return orkstackv1.LiveAttemptState_LIVE_ATTEMPT_STATE_CANCEL_REQUESTED
	case enumspb.PENDING_ACTIVITY_STATE_PAUSED,
		enumspb.PENDING_ACTIVITY_STATE_PAUSE_REQUESTED,
		enumspb.PENDING_ACTIVITY_STATE_UNSPECIFIED:
		return orkstackv1.LiveAttemptState_LIVE_ATTEMPT_STATE_UNSPECIFIED
	default:
		return orkstackv1.LiveAttemptState_LIVE_ATTEMPT_STATE_UNSPECIFIED
	}
}

// liveAttemptStateNamed lists every Temporal value liveAttemptState names in
// its switch, so a test can notice a Temporal upgrade that adds one.
var liveAttemptStateNamed = map[enumspb.PendingActivityState]bool{
	enumspb.PENDING_ACTIVITY_STATE_UNSPECIFIED:      true,
	enumspb.PENDING_ACTIVITY_STATE_SCHEDULED:        true,
	enumspb.PENDING_ACTIVITY_STATE_STARTED:          true,
	enumspb.PENDING_ACTIVITY_STATE_CANCEL_REQUESTED: true,
	enumspb.PENDING_ACTIVITY_STATE_PAUSED:           true,
	enumspb.PENDING_ACTIVITY_STATE_PAUSE_REQUESTED:  true,
}

// liveAttempt derives ONE LiveAttempt from a pending activity and its
// scheduling event — the one normative rule of the live-attempt contract.
//
// The heartbeat is reported only when it belongs to THIS attempt: Temporal
// hands a failed attempt's last detail to the next one and reports it
// through the retry backoff and until the next attempt's first beat, so a
// beat recorded before last_started_time is the previous attempt's and is
// refused. Both timestamps are the Temporal server's, so the comparison is
// between one clock.
//
// SDK worker identity (pid@hostname) is never read: a host name is not a
// user-facing coordinate (AGENTS.md §7.1). orker_id is the Orker's self-name
// from the detail.
func (r *TemporalRunReader) liveAttempt(
	pa *workflowpb.PendingActivityInfo,
	st scheduledTask,
	now time.Time,
) *orkstackv1.LiveAttempt {
	state := liveAttemptState(pa.GetState())
	out := &orkstackv1.LiveAttempt{
		TaskId:                         st.taskID,
		Attempt:                        pa.GetAttempt(),
		State:                          state,
		AttemptScheduledAt:             pa.GetScheduledTime(),
		NextAttemptAt:                  pa.GetNextAttemptScheduleTime(),
		TimeoutSeconds:                 st.timeoutSeconds,
		CancellationGracePeriodSeconds: st.graceSeconds,
		ExpiresAt:                      pa.GetExpirationTime(),
		Item:                           st.item,
	}
	started := state == orkstackv1.LiveAttemptState_LIVE_ATTEMPT_STATE_STARTED ||
		state == orkstackv1.LiveAttemptState_LIVE_ATTEMPT_STATE_CANCEL_REQUESTED
	if !started {
		out.HeartbeatUnavailableReason = orkstackv1.HeartbeatUnavailableReason_HEARTBEAT_UNAVAILABLE_REASON_NOT_STARTED
		return out
	}
	out.AttemptStartedAt = pa.GetLastStartedTime()

	lastBeat, lastStarted := pa.GetLastHeartbeatTime(), pa.GetLastStartedTime()
	details := pa.GetHeartbeatDetails()
	if lastBeat == nil || lastStarted == nil ||
		lastBeat.AsTime().Before(lastStarted.AsTime()) ||
		len(details.GetPayloads()) == 0 {
		out.HeartbeatUnavailableReason = orkstackv1.HeartbeatUnavailableReason_HEARTBEAT_UNAVAILABLE_REASON_NO_HEARTBEAT_YET
		return out
	}
	var progress task.HeartbeatProgress
	if err := r.converter.FromPayloads(details, &progress); err != nil || progress.Phase == "" {
		out.HeartbeatUnavailableReason = orkstackv1.HeartbeatUnavailableReason_HEARTBEAT_UNAVAILABLE_REASON_UNDECODABLE
		return out
	}

	beat := lastBeat.AsTime()
	out.HeartbeatAvailable = true
	out.HeartbeatUnavailableReason = orkstackv1.HeartbeatUnavailableReason_HEARTBEAT_UNAVAILABLE_REASON_UNSPECIFIED
	out.LastHeartbeatAt = lastBeat
	out.HeartbeatStale = now.Sub(beat) > task.HeartbeatStaleAfter
	out.HeartbeatDeadlineAt = timestamppb.New(beat.Add(task.HeartbeatTimeout))
	out.Phase = progress.Phase
	out.ContainerId = progress.ContainerID
	out.BytesLogged = progress.BytesLogged
	out.OrkerId = progress.OrkerID
	return out
}
