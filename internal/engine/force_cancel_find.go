package engine

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"time"

	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservice/v1"
	"google.golang.org/protobuf/types/known/timestamppb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/identity"
	"github.com/orkstack/orkstack/internal/jwt"
	"github.com/orkstack/orkstack/internal/task"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// The audit records of force cancels that are not this request's own.
const (
	forceCancelSquatter   = "run.cancel.force.squatter"
	forceCancelSuperseded = "run.cancel.force.superseded"
)

// forceCancelFlights bounds how many open executions named as one run's force
// cancels are looked at: one is the rule, and the rest are somebody's noise.
const forceCancelFlights = 100

// fcFlight is an open ForceCancelWorkflow execution, and its decision where it
// verified.
type fcFlight struct {
	workflowID, execution string
	startedAt             time.Time
	decision              ForceCancelDecision
}

// fcFound is what find saw: force cancels of the run to join, the earliest
// first, those past their time, and executions whose decision is forged, which
// are nobody's. Partial: not all there is was seen; unjudged: one that was seen
// could not be told to be one or not. Unsigned: how many, where the deployment
// signs nothing, are left alone; unknown, how many could not be judged.
type fcFound struct {
	join      []fcFlight
	supersede []fcFlight
	squatters []fcFlight
	partial   bool
	unjudged  bool
	unsigned  int
	unknown   int
}

// find looks for an open force cancel of the run, by the id every one of them
// has, and judges each by the decision it was started with: nothing else says
// what an execution is. One whose decision cannot be judged now is left alone.
// The cluster's index may lag, so two requests may both start: every step of a
// force cancel is made to be repeated.
func (s *ForceCancelStarter) find(ctx context.Context, namespace, runID string) (fcFound, error) {
	resp, err := s.client.ListWorkflow(ctx, &workflowservice.ListWorkflowExecutionsRequest{
		Namespace: s.namespace, PageSize: forceCancelFlights,
		Query: fmt.Sprintf("WorkflowType = %q AND WorkflowId STARTS_WITH %q AND ExecutionStatus = \"Running\"",
			ForceCancelWorkflowType, ForceCancelWorkflowIDPrefix(namespace, runID)),
	})
	if err != nil {
		return fcFound{}, fmt.Errorf("force cancel: list run %s's: %w", runID, err)
	}
	out := fcFound{partial: len(resp.GetNextPageToken()) > 0}
	for _, e := range resp.GetExecutions() {
		flight := fcFlight{workflowID: e.GetExecution().GetWorkflowId(), execution: e.GetExecution().GetRunId(), startedAt: e.GetStartTime().AsTime()}
		if e.GetType().GetName() != ForceCancelWorkflowType {
			continue
		}
		in, err := s.startedWith(ctx, flight)
		if err != nil {
			slog.Warn("force cancel: an execution named as one could not be read", "workflow_id", flight.workflowID, "error", err)
			out.unjudged = true
			out.unknown++
			continue
		}
		if in.Unsigned != nil && s.keys.UnsignedDecision(s.mode()) == identity.UnsignedAsserted {
			// Anyone may have started it, or a colleague: it is neither joined
			// nor ended, and two that are genuine run beside each other.
			slog.Warn("force cancel: an execution nobody signed is left alone", "workflow_id", flight.workflowID)
			out.unsigned++
			continue
		}
		d, err := s.judged(ctx, flight, in)
		switch {
		case err != nil:
			slog.Warn("force cancel: an execution named as one could not be judged now", "workflow_id", flight.workflowID, "error", err)
			out.unjudged = true
			out.unknown++
			continue
		case d == nil && s.bad(ctx, in, flight.startedAt):
			out.squatters = append(out.squatters, flight)
			continue
		case d == nil || d.RunID != runID || d.Namespace != namespace || e.GetTaskQueue() != task.DefaultQueue:
			continue // on another channel no engine's code runs it
		}
		flight.decision = *d
		// Its time is its decision's, and never more than this engine's own
		// force cancels have: an unsigned decision may say anything.
		given := min(d.Grace()+d.Ceiling(), s.timeout(*d)) + s.settings.AttachProgressSlack
		if d.Cascade && s.over(ctx, runID) {
			given = s.timeout(*d) // what is below is still its to do
		}
		if now := s.now(); !now.Before(flight.startedAt.Add(given)) || !now.Before(d.ExpiresAt) {
			out.supersede = append(out.supersede, flight)
			continue
		}
		out.join = append(out.join, flight)
	}
	slices.SortStableFunc(out.join, func(a, b fcFlight) int { return a.startedAt.Compare(b.startedAt) })
	return out, nil
}

// over reports whether a run is known to have ended.
func (s *ForceCancelStarter) over(ctx context.Context, runID string) bool {
	now, err := s.steps().describe(ctx, runID)
	return err == nil && !now.Open
}

// judged is the decision f acts under, as f itself would judge it, nil for
// none. An error is no answer.
func (s *ForceCancelStarter) judged(ctx context.Context, f fcFlight, in ForceCancelInput) (*ForceCancelDecision, error) {
	self := forceCancelSelf{WorkflowID: f.workflowID, Type: ForceCancelWorkflowType, StartedAt: f.startedAt, Local: true}
	if in.Derived != "" || in.Unsigned != nil && in.Unsigned.Of != nil {
		described, err := s.steps().described(ctx, f.workflowID, f.execution)
		if err != nil {
			return nil, fmt.Errorf("force cancel: describe %s: %w", f.workflowID, err)
		}
		self.Parent = described.GetWorkflowExecutionInfo().GetParentExecution()
	}
	d, cause, err := s.steps().decide(ctx, in, self)
	if d == nil && err == nil {
		slog.Debug("force cancel: an execution named as one is not", "workflow_id", f.workflowID, "cause", cause)
	}
	return d, err
}

// bad reports whether in's decision is a forgery: its key is known and its
// signature is wrong, or it is unsigned where callers are verified. A genuine
// decision in another's place, or one under a key not known here, is not.
func (s *ForceCancelStarter) bad(ctx context.Context, in ForceCancelInput, at time.Time) bool {
	if msg := in.Decision + in.Derived; msg != "" && in.Unsigned == nil {
		kind := jwt.KindForceCancelRoot
		if in.Derived != "" {
			kind = jwt.KindForceCancelDerived
		}
		_, _, verdict := s.keys.VerifyDecision(ctx, msg, kind, at, identity.NoBinding)
		return verdict == identity.VerdictBad
	}
	return s.keys.UnsignedDecision(s.mode()) == identity.UnsignedBad
}

// errForceCancelInput is an execution started with what no force cancel is.
var errForceCancelInput = errors.New("not a force cancel's input")

// startedWith is what an execution was started with.
func (s *ForceCancelStarter) startedWith(ctx context.Context, f fcFlight) (ForceCancelInput, error) {
	resp, err := s.client.WorkflowService().GetWorkflowExecutionHistory(ctx, &workflowservice.GetWorkflowExecutionHistoryRequest{
		Namespace: s.namespace, Execution: &commonpb.WorkflowExecution{WorkflowId: f.workflowID, RunId: f.execution}, MaximumPageSize: 1})
	if err != nil {
		return ForceCancelInput{}, fmt.Errorf("force cancel: read the start of %s: %w", f.workflowID, err)
	}
	var in ForceCancelInput
	events := resp.GetHistory().GetEvents()
	if len(events) == 0 {
		return in, fmt.Errorf("force cancel: %s has no start", f.workflowID)
	}
	payloads := events[0].GetWorkflowExecutionStartedEventAttributes().GetInput().GetPayloads()
	if len(payloads) != 1 || DataConverter().FromPayload(payloads[0], &in) != nil {
		return in, errForceCancelInput
	}
	return in, nil
}

// end ends an execution that is not to be waited for, by its own run id, so
// one started since under that id is untouched, and records it; it reports
// whether it is ended. Both are best effort: neither decides anything.
func (s *ForceCancelStarter) end(ctx context.Context, runID, namespace string, f fcFlight, action string) bool {
	reason := "orkstack force cancel superseded"
	if action == forceCancelSquatter {
		reason = "orkstack force cancel squatter"
	}
	_, err := s.client.WorkflowService().TerminateWorkflowExecution(ctx, &workflowservice.TerminateWorkflowExecutionRequest{
		Namespace: s.namespace, WorkflowExecution: &commonpb.WorkflowExecution{WorkflowId: f.workflowID, RunId: f.execution}, Reason: reason})
	var gone *serviceerror.NotFound
	if err != nil && !errors.As(err, &gone) {
		slog.Warn("force cancel: an execution could not be ended", "workflow_id", f.workflowID, "reason", reason, "error", err)
		return false
	}
	if action == forceCancelSquatter {
		s.count(ctx, FCCountSquatter, 1)
	} else {
		s.count(ctx, FCCountSuperseded, 1)
	}
	slog.Warn("force cancel: an execution was ended", "workflow_id", f.workflowID, "reason", reason)
	if s.audit == nil {
		return true
	}
	at := s.now().UTC()
	rec := &orkstackv1.AuditRecord{SchemaVersion: artifacts.AuditSchemaVersion, Id: artifacts.NewAuditID(at), At: timestamppb.New(at),
		Action: action, Namespace: namespace, Target: runID, Succeeded: true, Permission: forceCancelPermission,
		Detail: printable(f.workflowID) + ", started " + f.startedAt.UTC().Format(time.RFC3339)}
	if m := s.mode(); m == "observe" || m == "enforce" {
		rec.SchemaVersion = artifacts.AuditSchemaVersionVerified
	}
	if err := s.audit.PutAuditRecord(ctx, rec); err != nil {
		slog.Warn("force cancel: an ended execution could not be recorded", "workflow_id", f.workflowID, "error", err)
	}
	return true
}
