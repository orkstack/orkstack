package engine

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/task"
)

// The fleet-census half of TaskRun.pending_reason (E92 §4.9): why a task the
// run has dispatched is not running yet, said from the Orker inventory. Like
// the half a row carries itself (pending_reason.go), it is derived on read and
// never stored, and it claims nothing it cannot know: a census in which a live
// Orker did not answer says nothing, because that Orker could serve the pool.

// RunDispatch is what the cluster says of a live run's dispatch, beside its
// snapshot: the release it is pinned to, whether a pin is opening, and which
// of its tasks wait for an Orker to take them.
type RunDispatch struct {
	// Build is the release the run is pinned to; empty on an unversioned
	// fleet, and for an auto-upgrading run, where any Orker of a pool counts
	// (which can only say less, never something false). On a versioned
	// fleet a pinned run's tasks go only to its own release's Orkers
	// (AGENTS.md §7.2), so only they are counted.
	Build string
	// OpeningPins says a pin of the run is being opened.
	OpeningPins bool
	// Scheduled says one of the run's task activities has waited at least
	// DispatchWaitAfter for an Orker to take it, outside any retry back-off.
	Scheduled bool
	// Waiting is each task, or fan-out item, whose activity so waits (from
	// LiveAttempts). Only a task in it can be said to wait for an Orker: a
	// snapshot's started_at is filled when an attempt ends, so RUNNING with no
	// started_at is also a task whose container runs.
	Waiting map[WaitingAttempt]bool
}

// WaitingAttempt names a task's own activity (Item -1) or one fan-out item.
type WaitingAttempt struct {
	TaskID string
	Item   int32
}

// WaitingFor keys a live attempt.
func WaitingFor(attempt *orkstackv1.LiveAttempt) WaitingAttempt {
	if attempt.Item == nil {
		return WaitingAttempt{TaskID: attempt.GetTaskId(), Item: -1}
	}
	return WaitingAttempt{TaskID: attempt.GetTaskId(), Item: attempt.GetItem()}
}

// WaitsForAnOrker says a live attempt has waited for an Orker to take it: its
// activity is SCHEDULED, outside a retry back-off, since at least
// DispatchWaitAfter before now. InspectRun's capacity question asks the same.
func WaitsForAnOrker(attempt *orkstackv1.LiveAttempt, now time.Time) bool {
	scheduled := attempt.GetAttemptScheduledAt()
	return attempt.GetState() == orkstackv1.LiveAttemptState_LIVE_ATTEMPT_STATE_SCHEDULED &&
		attempt.GetNextAttemptAt() == nil && scheduled != nil && now.Sub(scheduled.AsTime()) >= DispatchWaitAfter
}

// DispatchWaitAfter is how long a dispatched task must have waited before the
// census speaks for it: an Orker's outstanding poll takes a task within
// milliseconds, so a task read in that instant would be called waiting by the
// poll about to take it.
const DispatchWaitAfter = 2 * time.Second

// ApplyCensusPendingReasons sets pending_reason, from the inventory orkers,
// on every row and item of a live run that waits for an Orker and carries no
// reason yet:
//   - a task, or item, whose activity waits for an Orker (dispatch.Waiting),
//     on a pool where no Orker of the run's release takes work;
//   - a pinned task of the one pin that can be opening (dispatch.OpeningPins,
//     and the run's only pin with PENDING members and no member started), in
//     a run not paused, on a pool where no pin host of the run's release takes
//     work.
//
// A reason a row already carries (its own wait to run again) stands.
func ApplyCensusPendingReasons(run *orkstackv1.Run, orkers []FleetOrker, dispatch RunDispatch, now time.Time) {
	if run == nil || isTerminal(run.GetStatus()) {
		return
	}
	census := censusOf(orkers)
	if len(census.unanswered) > 0 {
		return
	}
	opening := ""
	if dispatch.OpeningPins && run.GetPause() == nil {
		opening = theOpeningPin(run)
	}
	for _, row := range run.GetTaskRuns() {
		if row.GetAwait() != nil || row.GetSleep() != nil || row.GetChild() != nil {
			continue
		}
		if row.GetPin() != "" {
			if opening != "" && row.GetPin() == opening &&
				row.GetStatus() == orkstackv1.Status_STATUS_PENDING && row.GetPendingReason() == "" {
				row.PendingReason = census.waitReason(row.GetPool(), dispatch.Build, true)
			}
			continue
		}
		if len(row.GetItems()) > 0 {
			for i, item := range row.GetItems() {
				index := int32(i)
				if item.Item != nil {
					index = item.GetItem()
				}
				pool := item.GetPool()
				if pool == "" {
					pool = row.GetPool()
				}
				if dispatch.Waiting[WaitingAttempt{TaskID: row.GetTaskId(), Item: index}] && unreasoned(item) {
					item.PendingReason = census.waitReason(pool, dispatch.Build, false)
				}
			}
			continue
		}
		if dispatch.Waiting[WaitingAttempt{TaskID: row.GetTaskId(), Item: -1}] && unreasoned(row) {
			row.PendingReason = census.waitReason(row.GetPool(), dispatch.Build, false)
		}
	}
}

// theOpeningPin is the run's one pin that has PENDING members and no member
// that started or ended, "" when there is none or more than one: the cluster
// says a pin is opening, not which.
func theOpeningPin(run *orkstackv1.Run) string {
	started := map[string]bool{}
	waiting := map[string]bool{}
	for _, row := range run.GetTaskRuns() {
		pin := row.GetPin()
		if pin == "" {
			continue
		}
		if row.GetStatus() == orkstackv1.Status_STATUS_PENDING && row.GetStartedAt() == nil {
			waiting[pin] = true
		} else {
			started[pin] = true
		}
	}
	opening := ""
	for pin := range waiting {
		if started[pin] {
			continue
		}
		if opening != "" {
			return ""
		}
		opening = pin
	}
	return opening
}

// unreasoned: a RUNNING row with no reason of its own yet.
func unreasoned(row *orkstackv1.TaskRun) bool {
	return row.GetStatus() == orkstackv1.Status_STATUS_RUNNING && row.GetPendingReason() == ""
}

// waitReason is why a task of pool waits, "" when an Orker of build (any, when
// empty) takes work there — a pin host, when pinned. The pool's other Orkers
// of that build are named by why they take none.
func (c *poolCensus) waitReason(pool, build string, pinned bool) string {
	if pool == "" {
		return ""
	}
	var stoppingN, pausedN, cordonedN int
	for _, orker := range c.byPool[pool] {
		d := orker.Described
		if build != "" && d.GetBuildId() != build {
			continue
		}
		if pinned && d.GetMaxConcurrentPins() <= 0 {
			continue
		}
		switch {
		case !d.GetCordoned():
			return ""
		case d.GetStopping() != nil:
			stoppingN++
		case keepsPoolLive(d):
			pausedN++
		default:
			cordonedN++
		}
	}
	who := "an Orker"
	if pinned {
		who = "a pin host"
	}
	if build != "" {
		who += " of release " + build
	}
	if stoppingN+pausedN+cordonedN == 0 {
		return fmt.Sprintf("waiting for %s on pool %s; none serves it", who, pool)
	}
	return fmt.Sprintf("waiting for %s on pool %s; none takes work (%s)", who, pool, orkersNotTaking(stoppingN, pausedN, cordonedN))
}

// orkersNotTaking names a pool's Orkers that take no work by why.
func orkersNotTaking(stoppingN, pausedN, cordonedN int) string {
	var parts []string
	for _, part := range []struct {
		n    int
		word string
	}{{stoppingN, "stopping"}, {pausedN, "paused"}, {cordonedN, "cordoned"}} {
		if part.n > 0 {
			parts = append(parts, fmt.Sprintf("%d %s", part.n, part.word))
		}
	}
	return strings.Join(parts, ", ")
}

// RunDispatchReader is the describe the census half of pending_reason needs:
// production wires *TemporalRunReader.
type RunDispatchReader interface {
	// RunDispatch describes a live run's current execution once. A run the
	// cluster no longer holds is ErrRunNotFound.
	RunDispatch(ctx context.Context, runID string, now time.Time) (RunDispatch, error)
}

var _ RunDispatchReader = (*TemporalRunReader)(nil)

// RunDispatch reads, in one describe of the run's current execution, the
// release the cluster pins it to and whether one of its pins is opening (the
// SDK's session-creation activity is pending). Only a Pinned execution is
// bound to its release; an auto-upgrading or unversioned one names none. It
// also says whether a task activity has waited for an Orker (Scheduled), so
// the caller reads the history that names it (LiveAttempts) only then.
func (r *TemporalRunReader) RunDispatch(ctx context.Context, runID string, now time.Time) (RunDispatch, error) {
	if r == nil || r.client == nil {
		return RunDispatch{}, ErrNoLiveReader
	}
	resp, err := r.client.DescribeWorkflowExecution(ctx, runID, "")
	if err != nil {
		var notFound *serviceerror.NotFound
		if errors.As(err, &notFound) {
			return RunDispatch{}, fmt.Errorf("engine: run %s: %w", runID, ErrRunNotFound)
		}
		return RunDispatch{}, fmt.Errorf("engine: describe run %s: %w", runID, err)
	}
	var dispatch RunDispatch
	if info := resp.GetWorkflowExecutionInfo().GetVersioningInfo(); info.GetBehavior() == enumspb.VERSIONING_BEHAVIOR_PINNED {
		dispatch.Build = info.GetDeploymentVersion().GetBuildId()
	}
	for _, pending := range resp.GetPendingActivities() {
		switch pending.GetActivityType().GetName() {
		case sessionCreationActivity:
			dispatch.OpeningPins = true
		case task.ActivityExecuteTask:
			scheduled := pending.GetScheduledTime()
			if pending.GetState() == enumspb.PENDING_ACTIVITY_STATE_SCHEDULED && pending.GetNextAttemptScheduleTime() == nil &&
				scheduled != nil && now.Sub(scheduled.AsTime()) >= DispatchWaitAfter {
				dispatch.Scheduled = true
			}
		}
	}
	return dispatch, nil
}

// AwaitsCensus says a live run has a row the census could speak for, from its
// snapshot alone: a RUNNING task no attempt of which has ended yet (it may be
// waiting for an Orker, or running), or a pinned task still PENDING. A run
// with none needs neither the describe nor the inventory.
func AwaitsCensus(run *orkstackv1.Run) bool {
	if run == nil || isTerminal(run.GetStatus()) {
		return false
	}
	for _, row := range run.GetTaskRuns() {
		if row.GetAwait() != nil || row.GetSleep() != nil || row.GetChild() != nil {
			continue
		}
		if row.GetPin() != "" {
			if row.GetStatus() == orkstackv1.Status_STATUS_PENDING && row.GetPendingReason() == "" {
				return true
			}
			continue
		}
		if unreasoned(row) && row.GetStartedAt() == nil {
			return true
		}
		// A fan-out's row is stamped started once any item ends; its other
		// items can still wait.
		for _, item := range row.GetItems() {
			if unreasoned(item) && item.GetStartedAt() == nil {
				return true
			}
		}
	}
	return false
}
