package api

import (
	"context"
	"time"

	"github.com/orkstack/orkstack/internal/engine"
)

// ResumeEndings are the archive of runs' endings and the marks resumes leave
// (artifacts.RunEndingStore).
type ResumeEndings interface {
	HasEnding(ctx context.Context, runID string, scope uint64) (bool, error)
	GetEnding(ctx context.Context, runID string, scope uint64) ([]byte, error)
	MarkResumed(ctx context.Context, runID string, scope, setAside uint64) error
}

// ResumeArchiver archives a finished run from its history
// (engine.HistoryArchiver).
type ResumeArchiver interface {
	ArchiveFromHistory(ctx context.Context, runID string) error
}

// resumeArchiveBudget bounds archiving the ending a resume sets aside, with
// the confirm's own wait.
const resumeArchiveBudget = 10 * time.Second

// resumeBodyBudget bounds reading that ending's body back, which is as large
// as the run's stream.
const resumeBodyBudget = 30 * time.Second

// causeEndingUnarchived: the ending a resume would set aside is not archived.
const causeEndingUnarchived = "ending_unarchived"

// archiveSetAside makes sure the ending a resume sets aside is archived. Once
// the run is reset its history no longer holds what the run published after
// the reset point, so without that body a part of its stream would be lost. It
// is bounded: a write that does not end in time is a wait, with nothing begun.
func (s *Service) archiveSetAside(ctx context.Context, base *resumeBase) error {
	id := base.run.GetId()
	bounded, cancel := context.WithTimeout(ctx, min(resumeArchiveBudget, s.resumeSettings.ConfirmTimeout))
	defer cancel()
	has, err := s.resumeEndings.HasEnding(bounded, id, base.scope)
	if err == nil && !has {
		if err = s.resumeArchiver.ArchiveFromHistory(bounded, id); err == nil {
			has, err = s.resumeEndings.HasEnding(bounded, id, base.scope)
		}
	}
	switch {
	case ctx.Err() != nil:
		return ctxError(ctx)
	case err == nil && has:
		// Its run is the first line of a body that holds the whole stream.
		reading, cancel := context.WithTimeout(ctx, resumeBodyBudget)
		defer cancel()
		var body []byte
		if body, err = s.resumeEndings.GetEnding(reading, id, base.scope); err == nil {
			return s.refuseAnotherEnding(ctx, base, body)
		}
		if ctx.Err() != nil {
			return ctxError(ctx)
		}
	}
	s.logger.WarnContext(ctx, "resume: the ending a resume would set aside is not archived", "run_id", id, "scope", base.scope, "error", err)
	r := refuseResume(engine.RefuseCheckUnavailable, engine.RefusalWait, "how run %s ended is not archived yet, and a resume sets it aside; try again shortly", id)
	r.Cause, r.RetryAfter = causeEndingUnarchived, resumeCheckRetry
	return r
}

// refuseAnotherEnding refuses a base whose archived ending is another of the
// run's: an execution reset by cluster tooling ends under the scope of the one
// before, whose body stays. What a resume sets aside would then be lost, and
// the run read as the other ending left it.
func (s *Service) refuseAnotherEnding(ctx context.Context, base *resumeBase, body []byte) error {
	archived, err := engine.UnmarshalRunLine(body)
	if err == nil && archived.GetId() == base.run.GetId() && archived.GetStatus() == base.run.GetStatus() &&
		archived.GetError().GetCode() == base.run.GetError().GetCode() &&
		archived.GetFinishedAt().AsTime().Equal(base.run.GetFinishedAt().AsTime()) {
		return nil
	}
	s.logger.WarnContext(ctx, "resume: the run's archived ending is not the one a resume would set aside",
		"run_id", base.run.GetId(), "scope", base.scope, "error", err)
	return refuseResume(engine.RefuseStateUnreadable, engine.RefusalPermanent,
		"the archive holds another ending of run %s in place of this one, so this one can't be set aside. Run again", base.run.GetId())
}
