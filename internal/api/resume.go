package api

import (
	"context"
	"errors"
	"fmt"
	"time"
	"unicode/utf8"

	"connectrpc.com/connect"
	enumspb "go.temporal.io/api/enums/v1"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/archive"
	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

const (
	// maxResumeReasonBytes bounds a resume's reason.
	maxResumeReasonBytes = 1 << 10
	// maxResumeRunIDBytes is past any run id, and maxResumeTaskIDBytes is the
	// task id rule's (AGENTS.md §7).
	maxResumeRunIDBytes  = 255
	maxResumeTaskIDBytes = 200
	// maxAcknowledgedAttempts bounds how many unknown attempts one confirm
	// accepts: a run has at most that many open at its end.
	maxAcknowledgedAttempts = artifacts.MaxOpenAttemptClaims
	// resumeCheckRetry is when a caller is asked to try again after something
	// a check needs did not answer.
	resumeCheckRetry = 15 * time.Second
	// resumeClosingRetry is when a caller is asked to try again for a run
	// whose ending is recorded and whose history has not closed yet.
	resumeClosingRetry = 5 * time.Second
)

// causeRunClosing: the run's ending is recorded, and its history is still
// closing.
const causeRunClosing = "run_closing"

// ResumeRun resumes a failed run under its own id (docs/RESUME.md): a
// preview plans it and says what would rerun, and a confirm records the
// plan, resets the run's closed execution to it once, and waits for the run
// to carry on.
func (s *Service) ResumeRun(
	ctx context.Context,
	req *connect.Request[orkstackv1.ResumeRunRequest],
) (*connect.Response[orkstackv1.ResumeRunResponse], error) {
	resp, err := s.resumeRun(ctx, req)
	s.observeResume(ctx, req.Msg, respMsg(resp), err)
	return resp, err
}

// respMsg is resp's message, nil for none.
func respMsg(resp *connect.Response[orkstackv1.ResumeRunResponse]) *orkstackv1.ResumeRunResponse {
	if resp == nil {
		return nil
	}
	return resp.Msg
}

func (s *Service) resumeRun(
	ctx context.Context,
	req *connect.Request[orkstackv1.ResumeRunRequest],
) (*connect.Response[orkstackv1.ResumeRunResponse], error) {
	msg, started := req.Msg, s.now()
	if err := checkResumeRequest(msg); err != nil {
		return nil, err
	}
	release, err := s.resumeGate.enter(ctx, msg, s.now(), s.resumeSettings.ConfirmsPerMinute)
	if err != nil {
		return nil, s.resumeError(ctx, msg.GetRunId(), err)
	}
	defer release()
	base, err := s.resumeBase(ctx, msg)
	var done *resumeDone
	if errors.As(err, &done) {
		return s.answerResumed(ctx, done)
	}
	if err != nil {
		return nil, s.resumeError(ctx, msg.GetRunId(), err)
	}
	if err := engine.RefuseResumeFromHistory(base.planInput(msg)); err != nil {
		return nil, s.resumeError(ctx, msg.GetRunId(), err)
	}
	warnings, err := s.stillStopping(ctx, base)
	if err != nil {
		return nil, s.resumeError(ctx, msg.GetRunId(), err)
	}
	if !msg.GetDryRun() {
		if err := refuseUnacknowledged(msg.GetRunId(), warnings, msg.GetAcknowledgedUnknownAttempts()); err != nil {
			return nil, s.resumeError(ctx, msg.GetRunId(), err)
		}
	}
	planned, err := s.planResume(ctx, base, msg)
	if err != nil {
		return nil, s.resumeError(ctx, msg.GetRunId(), err)
	}
	s.observePlanned(ctx, base, planned)
	if digest := msg.GetExpectedPlanDigest(); digest != "" && digest != planned.digest {
		return nil, s.resumeError(ctx, msg.GetRunId(), refuseResume(resumePlanChanged, engine.RefusalReplan,
			"what would rerun of run %s has changed since you looked; preview again", msg.GetRunId()))
	}
	if msg.GetDryRun() {
		return connect.NewResponse(resumePreview(base, planned, warnings)), nil
	}
	resp, err := s.confirmResume(ctx, req, base, planned, warnings)
	if err != nil {
		return nil, s.resumeError(ctx, msg.GetRunId(), err)
	}
	s.observeConfirmed(ctx, base, planned, started)
	return resp, nil
}

// checkResumeRequest refuses a request no run could be resumed by, before
// anything of the run is read.
func checkResumeRequest(msg *orkstackv1.ResumeRunRequest) error {
	invalid := func(format string, args ...any) error {
		return connect.NewError(connect.CodeInvalidArgument, fmt.Errorf(format, args...))
	}
	digest := msg.GetExpectedPlanDigest()
	switch {
	case msg.GetRunId() == "":
		return invalid("run_id is required")
	case len(msg.GetRunId()) > maxResumeRunIDBytes:
		return invalid("run_id is %d bytes, above the maximum of %d", len(msg.GetRunId()), maxResumeRunIDBytes)
	case len(msg.GetFromTaskId()) > maxResumeTaskIDBytes || !utf8.ValidString(msg.GetFromTaskId()):
		return invalid("from_task_id is no task's id")
	case len(msg.GetReason()) > maxResumeReasonBytes:
		return invalid("reason is %d bytes, above the maximum of %d", len(msg.GetReason()), maxResumeReasonBytes)
	case !utf8.ValidString(msg.GetReason()):
		return invalid("reason is not valid UTF-8")
	case msg.GetExpectedTerminalSequence() < 0 || !msg.GetDryRun() && msg.GetExpectedTerminalSequence() == 0:
		return invalid("expected_terminal_sequence is required to resume a run: it is the preview's terminal_sequence")
	case digest != "" && !engine.IsResumePlanDigest(digest):
		return invalid("expected_plan_digest is not a preview's plan_digest")
	case len(msg.GetAcknowledgedUnknownAttempts()) > 0 && digest == "":
		return invalid("expected_plan_digest is required with acknowledged_unknown_attempts: an attempt is accepted for one plan")
	case len(msg.GetAcknowledgedUnknownAttempts()) > maxAcknowledgedAttempts:
		return invalid("acknowledged_unknown_attempts lists %d attempts, above the maximum of %d",
			len(msg.GetAcknowledgedUnknownAttempts()), maxAcknowledgedAttempts)
	case msg.GetAcceptSequenceGap() && msg.GetAcceptedHighestSeq() <= 0:
		return invalid("accepted_highest_seq is required with accept_sequence_gap: it is the highest sequence the refusal named")
	// Only a force cancel leaves a stop that was not confirmed, and no
	// force-canceled run is resumed.
	case msg.GetAcceptUnverifiedStop():
		return invalid("accept_unverified_stop is for a run that was force-canceled")
	case !msg.GetAcceptSequenceGap() && msg.GetAcceptedHighestSeq() != 0:
		return invalid("accepted_highest_seq is set without accept_sequence_gap")
	}
	return nil
}

// resumeError is err as ResumeRun answers it: a refusal, the planner's or its
// own, on the wire, and a connect error as it is. Anything else is an
// input the planner could not account for, which plans nothing.
func (s *Service) resumeError(ctx context.Context, runID string, err error) error {
	var planned *engine.ResumeRefusal
	var own *resumeRefusal
	var wire *connect.Error
	switch {
	case errors.As(err, &own):
		return own.connectError()
	case errors.As(err, &planned):
		r := resumeRefusal{ResumeRefusal: *planned}
		if planned.Class == engine.RefusalWait {
			r.RetryAfter = resumeCheckRetry
		}
		return r.connectError()
	case errors.As(err, &wire):
		return wire
	}
	if ctx.Err() != nil {
		return ctxError(ctx)
	}
	s.logger.ErrorContext(ctx, "resume: a resume could not be planned", "run_id", runID, "error", err)
	return connect.NewError(connect.CodeInternal, fmt.Errorf("the resume of run %q could not be planned; the engine's log says why", runID))
}

// resumeBase is the closed execution a resume starts from, and what its
// history says.
type resumeBase struct {
	namespace string
	binding   engine.Binding
	run       *orkstackv1.Run
	// sequence is the run's last event's, its ending.
	sequence   int64
	execution  engine.ResumeExecution
	start      *engine.RunStartRecord
	definition *orkstackv1.Orkflow
	history    *engine.ResumeHistory
	facts      engine.ResumeBaseFacts
	// resumesUsed counts the resumes of the run that got going, and previous
	// is the plan of the last; nil when none did.
	resumesUsed int
	previous    *orkstackv1.ResumePlanRecord
	// scope is the resume scope the base's own reset names; 0 for a base no
	// resume made.
	scope uint64
}

// planInput is the base as the planner reads it, for what msg asks.
func (b *resumeBase) planInput(msg *orkstackv1.ResumeRunRequest) engine.ResumePlanInput {
	in := engine.ResumePlanInput{
		Definition:  b.definition,
		Base:        b.run,
		Class:       engine.ResumeBaseFailed,
		Facts:       b.facts,
		ResumesUsed: b.resumesUsed,
		FromTask:    msg.GetFromTaskId(),
	}
	if msg.GetFromTaskId() != "" {
		in.Action = engine.ResumeActionRetryFrom
	}
	return in
}

// resumeBase reads the run a resume names and the execution it would start
// from, and refuses a run that is in no state to be resumed, cheapest first:
// its status, then the cluster's word on its execution, then its history.
func (s *Service) resumeBase(ctx context.Context, msg *orkstackv1.ResumeRunRequest) (*resumeBase, error) {
	runID := msg.GetRunId()
	ns, err := s.namespace(ctx, msg.GetNamespace())
	if err != nil {
		return nil, err
	}
	binding, err := s.locate(ctx, runID)
	if err != nil {
		return nil, err
	}
	run, sequence, err := s.runAndSequence(ctx, binding, runID, ns.GetName())
	if err != nil {
		return nil, err
	}
	if binding.Resume == nil {
		return nil, connect.NewError(connect.CodeFailedPrecondition, errors.New("this engine resumes no runs"))
	}
	if binding.Reader == nil || s.resumeRecords == nil || s.resumeClaims == nil || s.resumeSnapshots == nil ||
		s.commits == nil || s.artifactStore == nil || !s.listsClaims() {
		return nil, connect.NewError(connect.CodeFailedPrecondition, errors.New("this engine resumes no runs: one of its stores is not configured"))
	}
	if !msg.GetDryRun() {
		if err := s.alreadyResumed(ctx, ns.GetName(), binding, run, sequence, msg); err != nil {
			return nil, err
		}
	}
	if r := engine.RefuseResumeState(run, engine.ResumeBaseFailed); r != nil {
		return nil, r
	}
	b := &resumeBase{namespace: ns.GetName(), binding: binding, run: run, sequence: sequence}
	if b.execution, err = binding.Resume.DescribeResumeExecution(ctx, runID, ""); err != nil {
		return nil, s.historyReadError(ctx, runID, err)
	}
	if r := b.refuseExecution(msg); r != nil {
		return nil, r
	}
	if err := s.refuseRetiring(ctx, ns, run); err != nil {
		return nil, err
	}
	if err := s.readHistory(ctx, b); err != nil {
		return nil, err
	}
	if b.facts, err = b.history.Facts(run); err != nil {
		return nil, err
	}
	return b, nil
}

// readHistory reads what the base's history says: from the last minute's
// reading of the same execution, which is closed and never changes.
func (s *Service) readHistory(ctx context.Context, b *resumeBase) error {
	id := b.run.GetId()
	key := b.namespace + "\x00" + id + "\x00" + b.execution.ExecutionRunID
	if hit := s.resumeGate.read(key, s.now()); hit != nil {
		b.history, b.start, b.definition, b.resumesUsed, b.previous = hit.history, hit.start, hit.definition, hit.resumesUsed, hit.previous
		b.scope = hit.scope
		return nil
	}
	events, err := b.binding.Resume.ResumeExecutionHistory(ctx, id, b.execution.ExecutionRunID)
	if err != nil {
		return s.historyReadError(ctx, id, err)
	}
	if b.history, err = engine.ScanResumeBase(events, nil); err != nil {
		return err
	}
	if err := s.readDefinition(ctx, b); err != nil {
		return err
	}
	if b.resumesUsed, err = s.resumesUsed(ctx, b); err != nil {
		return err
	}
	s.resumeGate.keep(key, b, s.now())
	return nil
}

// ResumeRecords is the store of a resume's plan records and of identity's
// record of its scope (artifacts.RecordStore).
type ResumeRecords interface {
	ListResumePlans(ctx context.Context, namespace, runID string) ([]artifacts.ResumePlanRef, error)
	WriteResumePlan(ctx context.Context, r *orkstackv1.ResumePlanRecord) (string, error)
	WriteResume(ctx context.Context, r artifacts.ResumeRecord) error
	NewestResume(ctx context.Context, namespace, runID string) (uint64, artifacts.RecordRead[artifacts.ResumeRecord], error)
	ReadResumePlan(ctx context.Context, namespace, runID string, scope uint64, n int) (artifacts.RecordRead[*orkstackv1.ResumePlanRecord], string, error)
}

// ResumeSnapshots answers what is stored for a run
// (artifacts.BlobRunSnapshotStore).
type ResumeSnapshots interface {
	HighestSnapshot(ctx context.Context, namespace, runID string) (int64, error)
}

// causeRecordSchema: a record of a format this release does not read.
const causeRecordSchema = "record_schema_unsupported"

// resumesUsed is how many resumes of the run got going: none for an
// execution no resume made, and otherwise the plan that made it and the
// resumes it counts before it. A plan whose record is not the one the reset
// names never ran (the run ended RESUME_PLAN_INVALID), and nothing says what
// came before it, so it counts none: the cap bounds cost, not safety.
func (s *Service) resumesUsed(ctx context.Context, b *resumeBase) (int, error) {
	id := b.run.GetId()
	reason, reset, err := b.binding.Resume.ResetKeyOf(ctx, id, b.execution.ExecutionRunID)
	if err != nil {
		return 0, s.historyReadError(ctx, id, err)
	}
	key, resumed := engine.ParseResumePlanKey(reason)
	if !reset || !resumed {
		return 0, nil
	}
	b.scope = key.Scope
	read, digest, err := s.resumeRecords.ReadResumePlan(ctx, b.namespace, id, key.Scope, key.N)
	switch {
	case err != nil:
		s.logger.WarnContext(ctx, "resume: the plan a run was resumed under could not be read", "run_id", id, "error", err)
		r := refuseResume(engine.RefuseCheckUnavailable, engine.RefusalWait, "this run's resume record can't be read right now; try again shortly")
		r.RetryAfter = resumeCheckRetry
		return 0, r
	case read.Answer == artifacts.RecordUnsupported:
		r := refuseResume(resumeReleaseUnsupport, engine.RefusalPermanent,
			"this run was resumed by a newer release, whose record this release can't read; resume it with that release")
		r.Cause = causeRecordSchema
		return 0, r
	case read.Answer != artifacts.RecordFound || digest != key.PlanDigest:
		s.logger.WarnContext(ctx, "resume: a run was reset under a resume plan whose record does not match; its earlier resumes are not counted", "run_id", id)
		return 0, nil
	}
	b.previous = read.Value
	used := int(read.Value.GetBlock().GetResumesBefore())
	if engine.ResumePlanNeverRan(b.run.GetError().GetCode()) {
		return used, nil
	}
	return used + 1, nil
}

// resumeRetentionMargin is how long before a run is retired it can still be
// resumed: past a resume's longest wait.
const resumeRetentionMargin = 15 * time.Minute

// refuseRetiring refuses a run about to be retired, whose saved results go
// with it.
func (s *Service) refuseRetiring(ctx context.Context, ns *orkstackv1.Namespace, run *orkstackv1.Run) error {
	if s.retention == nil || !s.retentionEnforced || run.GetFinishedAt() == nil {
		return nil
	}
	policy, err := s.retention.Policy(ctx)
	if errors.Is(err, artifacts.ErrNotFound) {
		return nil // no maximum is set yet, so nothing is retired by one
	}
	if err != nil {
		s.logger.WarnContext(ctx, "resume: the retention policy could not be read", "run_id", run.GetId(), "error", err)
		return resumeUnavailable("how long run %s is kept can't be read right now; try again shortly", run.GetId())
	}
	keep := archive.EffectiveRunRetention(ns, policy.GetMaxRunRetention().AsDuration())
	if keep <= 0 || s.now().Before(run.GetFinishedAt().AsTime().Add(keep-resumeRetentionMargin)) {
		return nil
	}
	return refuseResume(resumeRetentionEnding, engine.RefusalPermanent, "run %s is about to be retired. Run again", run.GetId())
}

// refuseExecution refuses an execution a resume cannot start from, or one
// that is not the ending the caller saw.
func (b *resumeBase) refuseExecution(msg *orkstackv1.ResumeRunRequest) *resumeRefusal {
	id := b.run.GetId()
	switch {
	case b.execution.ResetRunID != "":
		return refuseResume(resumeRunChanged, engine.RefusalReplan, "run %s was resumed or reset since you looked; preview again", id)
	case b.execution.Status == enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
		b.execution.Status == enumspb.WORKFLOW_EXECUTION_STATUS_CONTINUED_AS_NEW:
		r := refuseResume(resumeStillStopping, engine.RefusalWait, "run %s has recorded its ending and is still closing; try again in %s",
			id, resumeClosingRetry)
		r.Cause, r.RetryAfter = causeRunClosing, resumeClosingRetry
		return r
	case b.execution.Status != enumspb.WORKFLOW_EXECUTION_STATUS_FAILED:
		return refuseResume(engine.RefuseStateNotResumable, engine.RefusalPermanent,
			"run %s did not end the way its record says, so nothing can be resumed from it. Run again starts a fresh run", id)
	case !msg.GetDryRun() && msg.GetExpectedTerminalSequence() != b.sequence:
		return refuseResume(resumeRunChanged, engine.RefusalReplan, "run %s changed since you looked: it ended again; preview again", id)
	}
	return nil
}

// readDefinition reads the definition the run is pinned to, verified when it
// is stored by reference. One that does not verify is a fact the planner
// refuses on; a store that does not answer is a wait.
func (s *Service) readDefinition(ctx context.Context, b *resumeBase) error {
	id := b.run.GetId()
	start, err := b.binding.Reader.RunStart(ctx, id)
	if err != nil {
		return s.historyReadError(ctx, id, err)
	}
	b.start = start
	b.definition, err = b.binding.Reader.RunDefinition(ctx, id, start)
	switch {
	case err == nil && len(b.definition.GetTasks()) > 0:
		return nil
	case err == nil, errors.Is(err, engine.ErrContinuationObjectInvalid):
		return refuseResume(engine.RefuseStateUnreadable, engine.RefusalPermanent, "this run's saved definition can't be read back. Run again")
	}
	if ctx.Err() != nil {
		return ctxError(ctx)
	}
	s.logger.WarnContext(ctx, "resume: a run's saved definition could not be read", "run_id", id, "error", err)
	r := refuseResume(engine.RefuseCheckUnavailable, engine.RefusalWait, "this run's saved definition can't be read right now; try again shortly")
	r.RetryAfter = resumeCheckRetry
	return r
}

// historyReadError is a failed read of a run's history: gone, which is
// permanent, or a cluster that did not answer.
func (s *Service) historyReadError(ctx context.Context, runID string, err error) error {
	if errors.Is(err, engine.ErrResumeExecutionNotFound) || errors.Is(err, engine.ErrRunNotFound) {
		return refuseResume(resumeHistoryGone, engine.RefusalPermanent, "run %s's history is no longer kept. Run again", runID)
	}
	if ctx.Err() != nil {
		return ctxError(ctx)
	}
	s.logger.WarnContext(ctx, "resume: a run could not be read from the cluster", "run_id", runID, "error", err)
	return connect.NewError(connect.CodeUnavailable, fmt.Errorf("run %q can't be read right now; try again shortly", runID))
}
