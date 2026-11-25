package engine

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"time"

	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservice/v1"

	"github.com/orkstack/orkstack/internal/task"
)

// ErrForceCancelNotFound is a run with no such force cancel.
var ErrForceCancelNotFound = errors.New("no such force cancel")

// ForceCancelStatus is a force cancel as it is now. It is for display: how the
// run ended is read from the run, never from here.
type ForceCancelStatus struct {
	Decision  ForceCancelDecision
	StartedAt time.Time
	Running   bool
	// State and Cause are its own result's; of one that has none, FCStateFailed
	// and why.
	State, Cause string
	// Cascade is where its cascade stands, nil where it has none.
	Cascade *ForceCancelCascade
	// Unjudged: a newer one of the run could not be judged.
	Unjudged bool
	// Incomplete: it ended the run, or finishes what one did, and lacks
	// something of it.
	Incomplete bool
	// Unaudited: a record of it, or of its cascade, could not be written.
	Unaudited bool
	// Sent: a termination of the run was sent, or may have been.
	Sent bool
}

// ErrForceCancelUnverifiable is a run whose close cannot be verified now: the
// keys could not be read, or this engine does not know its mode yet.
var ErrForceCancelUnverifiable = errors.New("force cancel: how the run closed could not be verified now; ask again")

// The causes of a force cancel that is over with no result of its own.
const (
	FCCauseStoppedOutside = "stopped_from_outside"
	FCCauseTimedOut       = "timed_out"
	// FCCauseBroke: it failed, as none of its own code does on purpose.
	FCCauseBroke = "failed_unexpectedly"
)

// Status is a run's force cancel requestID, or its newest. One whose decision
// does not verify is none.
func (s *ForceCancelStarter) Status(ctx context.Context, namespace, runID, requestID string) (ForceCancelStatus, error) {
	f, d, status, unjudged, err := s.newest(ctx, namespace, runID, requestID, func(ForceCancelDecision, bool) bool { return true })
	if err != nil {
		return ForceCancelStatus{}, err
	}
	out, err := s.status(ctx, f, d, status)
	out.Unjudged = unjudged
	return out, err
}

// Stop stops the newest cascade of a run: the force cancels it has started go
// on. It answers the request id it stopped.
func (s *ForceCancelStarter) Stop(ctx context.Context, namespace, runID string) (string, error) {
	f, d, _, _, err := s.newest(ctx, namespace, runID, "", func(d ForceCancelDecision, running bool) bool { return running && d.Cascade })
	if err != nil {
		return "", err
	}
	if err := s.client.CancelWorkflow(ctx, f.workflowID, f.execution); err != nil {
		return "", fmt.Errorf("force cancel: stop %s: %w", f.workflowID, err)
	}
	return d.RequestID, nil
}

// Below is the open runs below a run now.
func (s *ForceCancelStarter) Below(ctx context.Context, namespace, runID string) (FCSubtree, error) {
	return s.reader.Subtree(ctx, namespace, runID, s.settings.Cascade.bounded())
}

// newest is a run's force cancel requestID, or its newest, that wanted wants.
func (s *ForceCancelStarter) newest(ctx context.Context, namespace, runID, requestID string, wanted func(d ForceCancelDecision, running bool) bool) (
	f fcFlight, d ForceCancelDecision, status enumspb.WorkflowExecutionStatus, newer bool, err error,
) {
	if err := checkForceCancelText(forceCancelText{"run_id", runID, 256}, forceCancelText{"request_id", requestID, 128}); err != nil {
		return f, d, status, false, err
	}
	flights, err := s.named(ctx, namespace, runID, requestID)
	if err != nil {
		return f, d, status, false, err
	}
	var unjudged error
	for _, f := range flights {
		described, err := s.steps().described(ctx, f.workflowID, f.execution)
		var gone *serviceerror.NotFound
		switch {
		case errors.As(err, &gone):
			continue
		case err != nil:
			return f, d, status, false, fmt.Errorf("force cancel: describe %s: %w", f.workflowID, err)
		}
		info := described.GetWorkflowExecutionInfo()
		f.execution, f.startedAt = info.GetExecution().GetRunId(), info.GetStartTime().AsTime()
		in, err := s.startedWith(ctx, f)
		if err != nil {
			unjudged = err
			continue
		}
		d, _, err := s.steps().decide(ctx, in, forceCancelSelf{WorkflowID: f.workflowID, Type: info.GetType().GetName(), StartedAt: f.startedAt, Local: true, Parent: info.GetParentExecution()})
		if err != nil {
			unjudged = err // another may be judged
			continue
		}
		running := info.GetStatus() == enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING
		if d == nil || d.RunID != runID || d.Namespace != namespace || !wanted(*d, running) {
			continue
		}
		return f, *d, info.GetStatus(), unjudged != nil, nil
	}
	if unjudged != nil {
		return f, d, status, false, unjudged
	}
	return f, d, status, false, fmt.Errorf("%w of run %s", ErrForceCancelNotFound, runID)
}

// named are the executions that may be the run's force cancel requestID, or
// any of its force cancels, the newest first.
func (s *ForceCancelStarter) named(ctx context.Context, namespace, runID, requestID string) ([]fcFlight, error) {
	prefix := ForceCancelWorkflowIDPrefix(namespace, runID)
	if requestID != "" {
		return []fcFlight{{workflowID: prefix + requestID}}, nil
	}
	resp, err := s.client.ListWorkflow(ctx, &workflowservice.ListWorkflowExecutionsRequest{Namespace: s.namespace, PageSize: forceCancelFlights,
		Query: fmt.Sprintf("WorkflowType = %q AND WorkflowId STARTS_WITH %q", ForceCancelWorkflowType, prefix)})
	if err != nil {
		return nil, fmt.Errorf("force cancel: list run %s's: %w", runID, err)
	}
	var out []fcFlight
	for _, e := range resp.GetExecutions() {
		if e.GetTaskQueue() == task.DefaultQueue {
			out = append(out, fcFlight{workflowID: e.GetExecution().GetWorkflowId(), execution: e.GetExecution().GetRunId(), startedAt: e.GetStartTime().AsTime()})
		}
	}
	slices.SortStableFunc(out, func(a, b fcFlight) int { return b.startedAt.Compare(a.startedAt) })
	return out, nil
}

// status is f's, started under d.
func (s *ForceCancelStarter) status(ctx context.Context, f fcFlight, d ForceCancelDecision, status enumspb.WorkflowExecutionStatus) (ForceCancelStatus, error) {
	out := ForceCancelStatus{Decision: d, StartedAt: f.startedAt, State: FCStateFailed}
	switch status {
	case enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING:
		out.Running, out.State = true, FCStateRequested
		if d.Cascade {
			out.Cascade = s.cascadeNow(ctx, f)
		}
	case enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED:
		var r ForceCancelResult
		if err := s.client.GetWorkflow(ctx, f.workflowID, f.execution).Get(ctx, &r); err != nil {
			return ForceCancelStatus{}, fmt.Errorf("force cancel: read the result of %s: %w", f.workflowID, err)
		}
		out.State, out.Cause, out.Cascade = r.State, r.Cause, r.Cascade.shown()
		if checkForceCancelText(forceCancelText{"cause", r.Cause, 64}) != nil {
			out.Cause = "" // no engine's
		}
		out.Incomplete, out.Unaudited, out.Sent = r.lacks(), r.Unaudited, r.Terminated
	default:
		out.Cause = closedCause(status)
		// It may have ended the run before it was lost itself.
		ran, closed, err := s.steps().closeOf(ctx, d, "")
		var gone *serviceerror.NotFound
		switch {
		case errors.As(err, &gone):
			// The run is gone from the cluster with how it closed.
		case err != nil:
			return ForceCancelStatus{}, err
		case closed.Verification == ForceCancelUnavailable:
			return ForceCancelStatus{}, fmt.Errorf("%w: run %s", ErrForceCancelUnverifiable, d.RunID)
		case closed.Verification == ForceCancelVerified || closed.Verification == ForceCancelAsserted:
			out.State = FCStateAlreadyEnded
			if closed.Record.RequestID == d.RequestID {
				out.State, out.Incomplete, out.Sent = FCStateForceCanceled, true, true
			}
		case ran != enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING && ran != enumspb.WORKFLOW_EXECUTION_STATUS_TERMINATED:
			out.State = FCStateAlreadyEnded // by itself
		}
	}
	if d.Cascade && out.Cascade == nil {
		out.Cascade = s.cascadeNow(ctx, f) // as far as it came
	}
	if c := out.Cascade; c != nil && !out.Running && c.State != FCCascadeComplete && c.State != FCCascadePartial {
		c.State = FCCascadeStopped
	}
	return out, nil
}

// closedCause is why a force cancel that closed as status says has no result.
func closedCause(status enumspb.WorkflowExecutionStatus) string {
	switch status {
	case enumspb.WORKFLOW_EXECUTION_STATUS_TIMED_OUT:
		return FCCauseTimedOut
	case enumspb.WORKFLOW_EXECUTION_STATUS_FAILED:
		return FCCauseBroke
	}
	return FCCauseStoppedOutside
}

// cascadeNow is where a cascade that runs says it stands.
func (s *ForceCancelStarter) cascadeNow(ctx context.Context, f fcFlight) *ForceCancelCascade {
	ctx, cancel := context.WithTimeout(ctx, forceCancelDefinitionTimeout)
	defer cancel()
	var c ForceCancelCascade
	v, err := s.client.QueryWorkflow(ctx, f.workflowID, f.execution, forceCancelCascadeQuery)
	if err == nil {
		err = v.Get(&c)
	}
	if err != nil {
		slog.Warn("force cancel: where a cascade stands could not be asked", "workflow_id", f.workflowID, "error", err)
		return &ForceCancelCascade{State: FCCascadeRunning, Unread: true}
	}
	out := (&c).shown()
	out.State = FCCascadeRunning // whatever it says of itself, it has not ended
	return out
}

// shown is c as it may be shown: no more than a cascade covers, and no text
// that hides. Whoever answers for a force cancel writes it.
func (c *ForceCancelCascade) shown() *ForceCancelCascade {
	if c == nil {
		return nil
	}
	out := *c
	out.Results = nil
	for _, r := range c.Results[:min(len(c.Results), 2*forceCancelCascadeMost)] {
		if checkForceCancelText(forceCancelText{"run_id", r.RunID, 256}, forceCancelText{"parent_run_id", r.ParentRunID, 256}, forceCancelText{"orkflow", r.Orkflow, 512},
			forceCancelText{"request_id", r.RequestID, 128}, forceCancelText{"cause", r.Cause, 64}) == nil {
			out.Results = append(out.Results, r)
		}
	}
	// Whatever is withheld may be what it left: it is not complete then.
	withheld := len(out.Results) != len(c.Results)
	hidden := false
	for id, cause := range c.Excluded {
		hidden = hidden || checkForceCancelText(forceCancelText{"run_id", id, 256}, forceCancelText{"cause", cause, 64}) != nil
	}
	if hidden || len(c.Excluded) > forceCancelCascadeMost {
		out.Excluded, withheld = nil, true
	}
	if a := c.After; a != nil {
		shown := FCAfter{Count: a.Count}
		for _, id := range a.RunIDs[:min(len(a.RunIDs), forceCancelCascadeMost)] {
			if checkForceCancelText(forceCancelText{"run_id", id, 256}) == nil {
				shown.RunIDs = append(shown.RunIDs, id)
			}
		}
		if checkForceCancelText(forceCancelText{"unconfirmed", a.Unconfirmed, 64}) == nil {
			shown.Unconfirmed = a.Unconfirmed
		}
		out.After = &shown
		withheld = withheld || len(shown.RunIDs) != len(a.RunIDs) || shown.Unconfirmed != a.Unconfirmed
	}
	if checkForceCancelText(forceCancelText{"cause", c.Cause, 64}) != nil {
		out.Cause = ""
	}
	if checkForceCancelText(forceCancelText{"truncated", c.Truncated, 64}) != nil {
		out.Truncated, withheld = "", true
	}
	if withheld && out.State == FCCascadeComplete {
		out.State = FCCascadePartial
	}
	return &out
}
