package api

import (
	"context"
	"errors"
	"time"

	"connectrpc.com/connect"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/internal/telemetry"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// ResumeObserver is told what requests to resume runs came to
// (telemetry.EngineObserver). It returns nothing, so telemetry can never
// change an answer.
type ResumeObserver interface {
	RecordResumeRequest(ctx context.Context, r telemetry.ResumeRequest)
	RecordResumePreflight(ctx context.Context, namespace string, share float64)
	RecordResumeConfirmed(ctx context.Context, r telemetry.ResumeConfirmed)
	RecordResumeAnomaly(ctx context.Context, namespace, kind string)
}

// The anomalies a request to resume a run can meet (ork.run.resume.anomalies).
const (
	anomalyResetRunMismatch    = "reset_run_mismatch"
	anomalyEventsAboveTerminal = "events_above_terminal"
	anomalyContainerAfterBound = "container_after_bound"
)

func (s *Service) resumeAnomaly(ctx context.Context, namespace, kind string) {
	if s.resumeObserver != nil {
		s.resumeObserver.RecordResumeAnomaly(ctx, namespace, kind)
	}
}

// observeResume counts one answer. A preview that planned, and a request that
// named no run or was not one, resumed nothing and count as nothing.
func (s *Service) observeResume(ctx context.Context, msg *orkstackv1.ResumeRunRequest, resp *orkstackv1.ResumeRunResponse, err error) {
	if s.resumeObserver == nil {
		return
	}
	r := telemetry.ResumeRequest{Namespace: artifacts.ResolveNamespace(msg.GetNamespace()), Action: orkstackv1.ResumeAction_RESUME_ACTION_RESUME}
	if msg.GetFromTaskId() != "" {
		r.Action = orkstackv1.ResumeAction_RESUME_ACTION_RETRY_FROM
	}
	refusal := wireRefusal(err)
	switch {
	case err == nil && msg.GetDryRun():
		return
	case err == nil && resp.GetAlreadyResumed():
		r.Outcome = "already"
	case err == nil:
		r.Outcome = "resumed"
	case refusal != nil:
		r.Refusal, r.Outcome = refusal.GetReason(), "refused"
		switch {
		case r.Refusal == string(resumeUnconfirmed):
			r.Outcome = "unconfirmed"
		case r.Refusal == string(engine.RefuseCheckUnavailable):
			r.Outcome = "unavailable"
		case msg.GetDryRun():
			r.Outcome = "preview_refused"
		}
	case connect.CodeOf(err) == connect.CodeUnavailable:
		r.Outcome = "unavailable"
	default:
		return
	}
	s.resumeObserver.RecordResumeRequest(ctx, r)
}

// wireRefusal is the refusal err carries, or nil.
func wireRefusal(err error) *orkstackv1.ResumeRefusal {
	var wire *connect.Error
	if !errors.As(err, &wire) {
		return nil
	}
	for _, d := range wire.Details() {
		if v, derr := d.Value(); derr == nil {
			if r, ok := v.(*orkstackv1.ResumeRefusal); ok {
				return r
			}
		}
	}
	return nil
}

// observePlanned records what carrying a plan out would cost its history.
func (s *Service) observePlanned(ctx context.Context, base *resumeBase, p *resumePlanned) {
	if s.resumeObserver != nil && base.start.Plan.History != nil {
		s.resumeObserver.RecordResumePreflight(ctx, base.namespace, 1-p.preflight.Headroom)
	}
}

// observeConfirmed records a resume that was made.
func (s *Service) observeConfirmed(ctx context.Context, base *resumeBase, p *resumePlanned, started time.Time) {
	if s.resumeObserver == nil {
		return
	}
	tasks := map[string]int{}
	for _, u := range p.plan.Units {
		tasks[string(u.Category)]++
	}
	now := s.now()
	s.resumeObserver.RecordResumeConfirmed(ctx, telemetry.ResumeConfirmed{
		Namespace: base.namespace, Tasks: tasks, Ordinal: p.input.ResumesUsed + 1,
		Took: now.Sub(started), SinceFailure: now.Sub(base.execution.CloseTime),
	})
}
