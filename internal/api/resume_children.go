package api

import (
	"context"
	"errors"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/internal/identity"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// ComposedRecords is identity.ValuesRecorder's RecordComposed.
type ComposedRecords interface {
	RecordComposed(ctx context.Context, q identity.ComposedAsk) (identity.RecordedOutcome, error)
}

// resumeChildRecords records, before the reset, the values of each composed
// task p settles from its child's outputs on record (design §28.3), by the
// steps an engine records any composed task's values with: from the records it
// checks itself, never from the run's history. A task that has its record
// already, as one whose confirm never reset the run has, is not asked again:
// on a run resumed since, its scope is no longer the newest. It is the one
// place the resume writes such a record (decision P14R4-1).
func (s *Service) resumeChildRecords(ctx context.Context, base *resumeBase, p *resumePlanned) error {
	plan := base.start.Plan.Identity
	if plan == nil || len(p.input.Evidence.ChildFromOutputs) == 0 {
		return nil
	}
	id := base.run.GetId()
	if s.composedRecords == nil {
		return resumeUnavailable("this engine records no values of run %s; try again shortly", id)
	}
	input, err := identity.StartInput(base.start.Input)
	if err != nil {
		s.logger.ErrorContext(ctx, "resume: a run's input could not be read for its records", "run_id", id, "error", err)
		return refuseResume(engine.RefuseStateUnreadable, engine.RefusalPermanent, "run %s's input can't be read back. Run again", id)
	}
	rows := map[string]*orkstackv1.TaskRun{}
	for _, row := range base.run.GetTaskRuns() {
		rows[row.GetTaskId()] = row
	}
	for _, u := range p.plan.Units {
		task := u.Unit.Task
		if u.Disposition != engine.DispositionSettle || !p.input.Evidence.ChildFromOutputs[task] {
			continue
		}
		child, answered := base.start.Plan.Children[task], p.input.Evidence.Answered[task]
		if child == nil || child.Plan.Identity == nil || answered == nil || rows[task] == nil || rows[task].GetResultEpoch() < 0 {
			s.logger.ErrorContext(ctx, "resume: a composed task settled from its child has no child, row or scope", "run_id", id, "task_id", task)
			return refuseResume(engine.RefuseStateUnreadable, engine.RefusalPermanent, "task %s of run %s can't be read back. Run again", task, id)
		}
		_, err := s.composedRecords.RecordComposed(ctx, identity.ComposedAsk{
			Namespace: artifacts.ResolveNamespace(base.namespace), RunID: id, AdmissionSHA256: plan.AdmissionSHA256, TaskID: task,
			Scopes: identity.Scopes{Resume: uint64(rows[task].GetResultEpoch()), Root: plan.RootScope}, Input: input,
			ChildAdmissionSHA256: child.Plan.Identity.AdmissionSHA256, ValuesSHA256: answered.GetValuesRef().GetSha256(),
			Spacing: plan.RecordBudget.Spacings(), Closed: true,
		})
		if err != nil {
			return s.childRecordRefused(ctx, id, task, err)
		}
	}
	return nil
}

// childRecordRefused is a resume refused because a composed task's values
// could not be recorded from its child: a refusal of what the records say runs
// the task again, which a resume does not do; anything else was not answered,
// and is a wait.
func (s *Service) childRecordRefused(ctx context.Context, id, task string, err error) error {
	var refusal *identity.ValuesRefusal
	if !errors.As(err, &refusal) {
		s.logger.WarnContext(ctx, "resume: a composed task's values could not be recorded", "run_id", id, "task_id", task, "error", err)
		return resumeUnavailable("task %s of run %s can't be recorded right now; try again shortly", task, id)
	}
	s.logger.ErrorContext(ctx, "resume: a composed task's values were refused a record", "run_id", id, "task_id", task, "error", err)
	r := refuseResume(engine.RefuseComposedTask, engine.RefusalPermanent,
		"task %s runs another Orkflow, and its child's outputs can't be recorded as its values (%s); Retry from a task after %s, or Run again",
		task, refusal.Reason, task)
	r.Cause, r.Tasks = string(engine.ComposedChildOutputsUnavailable), []string{task}
	return r
}
