package api

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"time"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/types/known/timestamppb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/authn"
	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/internal/runstatus"
	"github.com/orkstack/orkstack/internal/typeid"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

const (
	// resumeResetMargin is added to the confirm's wait for a reset's deadline.
	resumeResetMargin = 30 * time.Second
	// resumeResetWindow is how long past its recorded deadline a reset is
	// waited for before its plan is given up or sent again.
	//
	// The recorded deadline is the one the cluster enforces: the request
	// carries it as the time left when it is sent (gRPC's timeout), the cluster
	// counts that from receipt on its own clock, and it applies no reset after
	// it (spike T10). So the cluster's clock need not agree with an engine's.
	// The window covers, together, what can still put a reset's landing past
	// the recorded instant as another engine reads it: how far the recording
	// engine's clock runs behind the one that compares; how long the request
	// was on its way after its time left was written; and how long the cluster
	// takes to apply what it began in time. Two engines whose clocks differ by
	// a minute or more, or a proxy that drops a request's deadline, are outside
	// it; even then a reset that landed before the last look at the base is
	// seen by it, and no second one is sent.
	resumeResetWindow = 60 * time.Second
	// ResumeClaimTTL is a claim's lifetime: far past any window it guards.
	ResumeClaimTTL = 24 * time.Hour
	// resumeConfirmPoll is how often a confirm looks for the run to go on.
	resumeConfirmPoll = 250 * time.Millisecond
	// maxResumePlans is the highest number a plan of one scope takes, as the
	// records number them.
	maxResumePlans = 998
	// maxResumeSends bounds how often one plan's reset is sent, so reading
	// its claims is bounded; a plan this often lost is stranded.
	maxResumeSends = 100
)

// ResumeClaims hold a plan's reset for one request at a time
// (engine.TemporalIdempotency, with ResumeClaimTTL). A claim is a lease that
// keeps concurrent requests from repeating work; what keeps a reset from
// being made twice is the base's own record of its reset, and the deadline
// each is sent under.
type ResumeClaims interface {
	Lookup(ctx context.Context, key string) (engine.Claim, bool, error)
	Claim(ctx context.Context, key string, candidate engine.Claim) (engine.Claim, error)
}

// resumeSend is one sending of one plan's reset.
type resumeSend struct {
	record *orkstackv1.ResumePlanRecord
	key    engine.ResumePlanKey
	// nth counts the plan's sendings from 0, and deadline is this one's.
	nth      int
	deadline time.Time
}

// confirmResume carries a planned resume out (design §10.2, §30): the plan is
// recorded and identity told of it, the base is reset to it exactly once, and
// the run is waited for. Nothing of the run is touched before the reset.
func (s *Service) confirmResume(
	ctx context.Context, req *connect.Request[orkstackv1.ResumeRunRequest], base *resumeBase, p *resumePlanned, warnings []*orkstackv1.ResumeWarning,
) (*connect.Response[orkstackv1.ResumeRunResponse], error) {
	send, err := s.recordResume(ctx, req, base, p)
	if err != nil {
		return nil, err
	}
	if err := s.baseUnmoved(ctx, base); err != nil {
		return nil, err
	}
	if err := s.claimReset(ctx, base, send); err != nil {
		return nil, err
	}
	// Between the last look at the base and the reset there is nothing but
	// the call.
	if err := s.baseUnmoved(ctx, base); err != nil {
		return nil, err
	}
	resetRun, err := base.binding.Resume.ResetForResume(ctx, engine.ResumeReset{
		RunID: base.run.GetId(), BaseExecutionRunID: base.execution.ExecutionRunID,
		WorkflowTaskFinishEventID: base.history.ResetPoint.CompletedEventID,
		Reason:                    send.key.String(), Deadline: send.deadline,
		DeploymentName: base.execution.DeploymentName, BuildID: base.execution.BuildID,
	})
	switch {
	case errors.Is(err, engine.ErrResetOutcomeUnknown):
		s.logger.WarnContext(ctx, "resume: a reset was sent and its answer was lost", "run_id", base.run.GetId(), "error", err)
		settled := send.deadline.Add(resumeResetWindow)
		r := refuseResume(resumeUnconfirmed, engine.RefusalWait,
			"the resume of run %s was sent and its outcome is not known yet; it will be settled by %s", base.run.GetId(), settled.UTC().Format(time.RFC3339))
		r.Cause, r.RetryAfter = causeResetOutcomeUnknown, settled.Sub(s.now())
		return nil, r
	case errors.Is(err, engine.ErrResumeExecutionNotFound):
		return nil, s.historyReadError(ctx, base.run.GetId(), err)
	case err != nil:
		// A definite refusal, which trying again does not change.
		s.logger.ErrorContext(ctx, "resume: the cluster refused to reset a run", "run_id", base.run.GetId(), "error", err)
		return nil, connect.NewError(connect.CodeInternal, fmt.Errorf("resume run %q: the cluster refused it; the engine's log says why", base.run.GetId()))
	}
	s.checkResetRun(ctx, base, resetRun)
	out := resumePreview(base, p, warnings)
	out.Resume = resumeOf(send.record)
	if err := s.awaitCarriedOn(ctx, base.namespace, base.binding, base.run.GetId(), p.seqBase); err != nil {
		return nil, err
	}
	return connect.NewResponse(out), nil
}

// recordResume answers the plan this confirm sends: a plan already recorded
// for the base and lost, once its window has passed and while it is still the
// plan; or a new one, recorded and told to identity.
func (s *Service) recordResume(ctx context.Context, req *connect.Request[orkstackv1.ResumeRunRequest], base *resumeBase, p *resumePlanned) (*resumeSend, error) {
	id := base.run.GetId()
	refs, err := s.resumeRecords.ListResumePlans(ctx, base.namespace, id)
	if err != nil {
		return nil, s.recordsUnavailable(ctx, id, err)
	}
	newest, link, err := s.resumeRecords.NewestResume(ctx, base.namespace, id)
	if err != nil {
		return nil, s.recordsUnavailable(ctx, id, err)
	}
	switch link.Answer {
	case artifacts.RecordFound, artifacts.RecordNone:
	case artifacts.RecordUnsupported:
		return nil, unreadableRecord(id)
	default:
		return nil, refuseResume(engine.RefuseStateUnreadable, engine.RefusalPermanent, "run %s's resume records can't be read back. Run again", id)
	}
	own := base.scope
	switch {
	case newest > own:
		send, err := s.sendAgain(ctx, base, p, newest, link.Value.N)
		if send != nil || err != nil {
			return send, err
		}
		// The newest resume is of an earlier execution, which was reset since
		// by hand: this resume follows it.
		own = newest
	case newest < own:
		return nil, refuseResume(engine.RefuseStateUnreadable, engine.RefusalPermanent, "run %s's resume records can't be read back. Run again", id)
	}
	scope, n := uint64(p.seqBase), 0
	if scope <= own {
		return nil, refuseResume(engine.RefuseStateUnreadable, engine.RefusalPermanent,
			"run %s's events end below those of an earlier resume, so nothing can follow it. Run again", id)
	}
	for _, ref := range refs {
		if ref.Scope <= uint64(base.sequence) {
			continue // a plan of an earlier ending
		}
		read, _, err := s.resumeRecords.ReadResumePlan(ctx, base.namespace, id, ref.Scope, ref.N)
		if err != nil {
			return nil, s.recordsUnavailable(ctx, id, err)
		}
		if read.Answer == artifacts.RecordUnsupported {
			return nil, unreadableRecord(id)
		}
		if ref.Scope == scope {
			n = max(n, ref.N+1)
		}
		// A record that cannot be read back names no time to wait for, and
		// its number is not used again.
		if read.Answer == artifacts.RecordFound && read.Value.GetBaseExecutionRunId() == base.execution.ExecutionRunID {
			if err := s.inProgress(id, recordedBy(read.Value), read.Value.GetExpiresAt().AsTime()); err != nil {
				return nil, err
			}
		}
	}
	return s.recordNewPlan(ctx, req, base, p, scope, n, own)
}

// recordNewPlan records plan n of scope and tells identity.
func (s *Service) recordNewPlan(
	ctx context.Context, req *connect.Request[orkstackv1.ResumeRunRequest], base *resumeBase, p *resumePlanned, scope uint64, n int, own uint64,
) (*resumeSend, error) {
	id, msg := base.run.GetId(), req.Msg
	if n > maxResumePlans {
		return nil, refuseResume(resumePlanStranded, engine.RefusalPermanent, "resumes of run %s were interrupted too often to make another. Run again", id)
	}
	block, err := engine.BuildResumeBlock(engine.ResumeBlockInput{
		Input: p.input, Plan: p.plan, Verification: p.verified, SeqBase: uint64(p.seqBase), Scope: scope,
		PreviousSeqBase: base.previous.GetBlock().GetSeqBase(),
	})
	if err != nil {
		return nil, err
	}
	asker := authn.Recorded(ctx, req.Header())
	now := s.now().UTC()
	expires := now.Add(s.resumeSettings.ConfirmTimeout + resumeResetMargin)
	rec := planRecord(base, block)
	rec.Scope, rec.N, rec.PreviousScope = scope, int32(n), own
	rec.At, rec.ExpiresAt = timestamppb.New(now), timestamppb.New(expires)
	rec.Principal, rec.PrincipalVerified = boundOriginAssertion(asker.ID), asker.Verified
	rec.Reason, rec.SupersededReason = msg.GetReason(), base.run.GetError().GetCode()
	rec.RequestId, rec.PlanDigest = "resume-"+typeid.New(), p.digest
	if engine.ResumePlanDigestOf(rec) != p.digest {
		return nil, fmt.Errorf("the plan recorded for run %s is not the plan made", id)
	}
	digest, err := s.resumeRecords.WriteResumePlan(ctx, rec)
	switch {
	case errors.Is(err, artifacts.ErrResumePlanDiffers):
		// Another request recorded this number first.
		return nil, s.inProgressUntil(id, "", expires)
	case errors.Is(err, artifacts.ErrInvalidRecordKey):
		return nil, refuseResume(engine.RefuseTooLarge, engine.RefusalPermanent,
			"too many recorded results would have to be tracked to resume run %s; retry from a later task, or Run again", id)
	case err != nil:
		return nil, s.recordsUnavailable(ctx, id, err)
	}
	err = s.resumeRecords.WriteResume(ctx, artifacts.ResumeRecord{
		Namespace: base.namespace, RunID: id, Scope: scope, N: n, PreviousScope: own,
		Tasks: rerunTasks(p.plan), ReopenedAwaits: reopenedAwaits(base.definition, p.plan),
	})
	switch {
	case errors.Is(err, artifacts.ErrResumeLinkElsewhere), errors.Is(err, artifacts.ErrResumeScopeHasSuccessor), errors.Is(err, artifacts.ErrResumeRecordDiffers):
		return nil, refuseResume(resumeRunChanged, engine.RefusalReplan, "run %s was resumed again since you looked; preview again", id)
	case err != nil:
		return nil, s.recordsUnavailable(ctx, id, err)
	}
	return &resumeSend{record: rec, key: engine.ResumePlanKey{Scope: scope, N: n, PlanDigest: digest}, deadline: expires}, nil
}

// sendAgain answers the plan identity's newest link names, when it is the
// base's own and its reset was never applied. Nothing can follow it under
// another number, so it is sent again, as it is, once nothing sent before can
// land; one that is no longer what would be planned is stranded. It answers
// none for a plan of another execution, whose own time has passed.
func (s *Service) sendAgain(ctx context.Context, base *resumeBase, p *resumePlanned, scope uint64, n int) (*resumeSend, error) {
	id := base.run.GetId()
	read, digest, err := s.resumeRecords.ReadResumePlan(ctx, base.namespace, id, scope, n)
	if err != nil {
		return nil, s.recordsUnavailable(ctx, id, err)
	}
	rec := read.Value
	switch {
	case read.Answer == artifacts.RecordUnsupported:
		return nil, unreadableRecord(id)
	case read.Answer != artifacts.RecordFound:
		return nil, refuseResume(engine.RefuseStateUnreadable, engine.RefusalPermanent, "run %s's resume records can't be read back. Run again", id)
	case rec.GetBaseExecutionRunId() != base.execution.ExecutionRunID:
		return nil, s.inProgress(id, recordedBy(rec), rec.GetExpiresAt().AsTime())
	}
	send := &resumeSend{record: rec, key: engine.ResumePlanKey{Scope: scope, N: n, PlanDigest: digest}, deadline: rec.GetExpiresAt().AsTime()}
	for send.nth = 1; send.nth < maxResumeSends; send.nth++ {
		claim, bound, err := s.resumeClaims.Lookup(ctx, resumeClaimKey(base, send))
		if err != nil {
			return nil, s.recordsUnavailable(ctx, id, err)
		}
		if !bound {
			break
		}
		if send.deadline, err = claimDeadline(claim); err != nil {
			return nil, err
		}
	}
	if err := s.inProgress(id, recordedBy(rec), send.deadline); err != nil {
		return nil, err
	}
	if send.nth == maxResumeSends || rec.GetPlanDigest() != p.digest || rec.GetBlock().GetSeqBase() != uint64(p.seqBase) {
		return nil, refuseResume(resumePlanStranded, engine.RefusalPermanent,
			"a resume of run %s was interrupted, and what it would have done has changed since, so it can be neither completed nor replaced. Run again", id)
	}
	send.deadline = s.now().UTC().Add(s.resumeSettings.ConfirmTimeout + resumeResetMargin)
	return send, nil
}

// recordedBy is who asked for a recorded resume, marked as the record says;
// empty when it names nobody.
func recordedBy(rec *orkstackv1.ResumePlanRecord) string {
	if rec.GetPrincipal() == "" {
		return ""
	}
	return principalMark(strconv.Quote(rec.GetPrincipal()), rec.GetPrincipalVerified())
}

// inProgress refuses while a reset sent under deadline may still be applied.
func (s *Service) inProgress(id, principal string, deadline time.Time) error {
	if s.now().Before(deadline.Add(resumeResetWindow)) {
		return s.inProgressUntil(id, principal, deadline)
	}
	return nil
}

func (s *Service) inProgressUntil(id, principal string, deadline time.Time) *resumeRefusal {
	until := deadline.Add(resumeResetWindow)
	by := ""
	if principal != "" {
		by = " by " + principal
	}
	r := refuseResume(resumeInProgress, engine.RefusalWait, "a resume of run %s started%s is not settled; try again after %s",
		id, by, until.UTC().Format(time.RFC3339))
	r.RetryAfter = max(until.Sub(s.now()), time.Second)
	return r
}

func (s *Service) recordsUnavailable(ctx context.Context, id string, err error) *resumeRefusal {
	s.logger.WarnContext(ctx, "resume: a run's resume records could not be read or written", "run_id", id, "error", err)
	return resumeUnavailable("run %s's resume records can't be read or written right now; try again shortly", id)
}

func unreadableRecord(id string) *resumeRefusal {
	r := refuseResume(resumeReleaseUnsupport, engine.RefusalPermanent,
		"run %s was resumed by a newer release, whose records this release can't read; resume it with that release", id)
	r.Cause = causeRecordSchema
	return r
}

// baseUnmoved refuses a base that was reset, or is no longer the run's
// current execution.
func (s *Service) baseUnmoved(ctx context.Context, base *resumeBase) error {
	id := base.run.GetId()
	current, err := base.binding.Resume.DescribeResumeExecution(ctx, id, "")
	if err != nil {
		return s.historyReadError(ctx, id, err)
	}
	own, err := base.binding.Resume.DescribeResumeExecution(ctx, id, base.execution.ExecutionRunID)
	if err != nil {
		return s.historyReadError(ctx, id, err)
	}
	if current.ExecutionRunID != base.execution.ExecutionRunID || own.ResetRunID != "" {
		return refuseResume(resumeRunChanged, engine.RefusalReplan, "run %s was resumed or reset since you looked; preview again", id)
	}
	return nil
}

func resumeClaimKey(base *resumeBase, send *resumeSend) string {
	return fmt.Sprintf("resume-reset:%s/%s/%s/%d.%03d/%d",
		base.namespace, base.run.GetId(), base.execution.ExecutionRunID, send.key.Scope, send.key.N, send.nth)
}

func claimDeadline(c engine.Claim) (time.Time, error) {
	t, err := time.Parse(time.RFC3339Nano, string(c.Snapshot))
	if err != nil {
		return time.Time{}, fmt.Errorf("a resume's claim names no deadline: %w", err)
	}
	return t, nil
}

// claimReset takes the sending's lease for this request alone: a request made
// again, or by anyone else, is told to wait for it.
func (s *Service) claimReset(ctx context.Context, base *resumeBase, send *resumeSend) error {
	mine := engine.Claim{RunID: "resume-" + typeid.New(), Snapshot: []byte(send.deadline.Format(time.RFC3339Nano))}
	got, err := s.resumeClaims.Claim(ctx, resumeClaimKey(base, send), mine)
	if err != nil {
		return s.recordsUnavailable(ctx, base.run.GetId(), err)
	}
	if got.RunID == mine.RunID {
		return nil
	}
	deadline, err := claimDeadline(got)
	if err != nil {
		return err
	}
	return s.inProgressUntil(base.run.GetId(), recordedBy(send.record), deadline)
}

// checkResetRun compares the run the reset answered with the base's own
// record of it, which is the cluster's word.
func (s *Service) checkResetRun(ctx context.Context, base *resumeBase, resetRun string) {
	after, err := base.binding.Resume.DescribeResumeExecution(ctx, base.run.GetId(), base.execution.ExecutionRunID)
	switch {
	case err != nil:
		s.logger.WarnContext(ctx, "resume: the base could not be read after its reset", "run_id", base.run.GetId(), "error", err)
	case after.ResetRunID != resetRun:
		s.logger.ErrorContext(ctx, "resume: the base names another reset run than the reset answered",
			"run_id", base.run.GetId(), "answered", resetRun, "recorded", after.ResetRunID)
		s.resumeAnomaly(ctx, base.namespace, anomalyResetRunMismatch)
	}
}

// awaitCarriedOn waits for the resumed run to publish its first event. A run
// that could not carry its plan out, and so ended as its base had, is no
// resume that worked.
func (s *Service) awaitCarriedOn(ctx context.Context, namespace string, binding engine.Binding, runID string, seqBase int64) error {
	ctx, cancel := context.WithTimeout(ctx, s.resumeSettings.ConfirmTimeout)
	defer cancel()
	tick := time.NewTicker(resumeConfirmPoll)
	defer tick.Stop()
	var unread error
	for {
		run, sequence, err := s.runAndSequence(ctx, binding, runID, namespace)
		if err != nil && ctx.Err() == nil {
			unread = err
		}
		if err == nil {
			unread = nil
			if code := run.GetError().GetCode(); run.GetStatus() == orkstackv1.Status_STATUS_FAILED && engine.ResumePlanNeverRan(code) {
				r := refuseResume(resumeRunChanged, engine.RefusalReplan,
					"run %s was resumed and ended at once (%s): nothing ran again; preview again", runID, code)
				r.Cause = causeResumeEnded
				return r
			}
			if sequence >= seqBase {
				return nil
			}
		}
		select {
		case <-ctx.Done():
			r := refuseResume(resumeUnconfirmed, engine.RefusalWait, "run %s was resumed and has not carried on yet; its release may have stopped", runID)
			if unread != nil {
				s.logger.WarnContext(ctx, "resume: a resumed run could not be read", "run_id", runID, "error", unread)
				r = refuseResume(resumeUnconfirmed, engine.RefusalWait, "run %s was resumed, and whether it has carried on can't be read right now", runID)
			}
			r.Cause, r.RetryAfter = causeNotCarriedOn, resumeCheckRetry
			return r
		case <-tick.C:
		}
	}
}

// resumeDone says the confirm asks for a resume already made.
type resumeDone struct {
	namespace string
	binding   engine.Binding
	runID     string
	record    *orkstackv1.ResumePlanRecord
}

func (d *resumeDone) Error() string { return "run " + d.runID + " was already resumed" }

// alreadyResumed answers, as *resumeDone, a confirm of a resume that was made
// already: the run's current execution is the one a reset under that plan
// started. A confirm of an ending the run has since been resumed from, by
// another plan, is RUN_CHANGED; anything else goes on.
func (s *Service) alreadyResumed(
	ctx context.Context, namespace string, binding engine.Binding, run *orkstackv1.Run, sequence int64, msg *orkstackv1.ResumeRunRequest,
) error {
	id := run.GetId()
	current, err := binding.Resume.DescribeResumeExecution(ctx, id, "")
	if err != nil {
		return s.historyReadError(ctx, id, err)
	}
	reason, reset, err := binding.Resume.ResetKeyOf(ctx, id, current.ExecutionRunID)
	if err != nil {
		return s.historyReadError(ctx, id, err)
	}
	key, resumed := engine.ParseResumePlanKey(reason)
	if !reset || !resumed || (sequence == msg.GetExpectedTerminalSequence() && sequence >= int64(key.Scope)) {
		return nil
	}
	read, digest, err := s.resumeRecords.ReadResumePlan(ctx, namespace, id, key.Scope, key.N)
	switch {
	case err != nil:
		return s.recordsUnavailable(ctx, id, err)
	case read.Answer == artifacts.RecordUnsupported:
		return unreadableRecord(id)
	case read.Answer != artifacts.RecordFound || digest != key.PlanDigest:
		return nil // a plan that never ran; the run's own state answers
	}
	rec := read.Value
	same := rec.GetBaseTerminalSequence() == msg.GetExpectedTerminalSequence() &&
		rec.GetBlock().GetFromTaskId() == msg.GetFromTaskId() &&
		(msg.GetExpectedPlanDigest() == "" || msg.GetExpectedPlanDigest() == rec.GetPlanDigest())
	// An ending that published no event has its base's sequence: a caller who
	// saw that ending names another plan than the one that could not be run.
	if !same && sequence == msg.GetExpectedTerminalSequence() && runstatus.IsTerminal(run.GetStatus()) {
		return nil
	}
	if !same {
		by := ""
		if who := recordedBy(rec); who != "" {
			by = " by " + who
		}
		return refuseResume(resumeRunChanged, engine.RefusalReplan, "run %s changed since you looked: it was resumed%s at %s",
			id, by, rec.GetAt().AsTime().UTC().Format(time.RFC3339))
	}
	return &resumeDone{namespace: namespace, binding: binding, runID: id, record: rec}
}

// answerResumed answers a resume made before: once the run has carried on.
func (s *Service) answerResumed(ctx context.Context, d *resumeDone) (*connect.Response[orkstackv1.ResumeRunResponse], error) {
	seqBase := int64(d.record.GetBlock().GetSeqBase())
	if err := s.awaitCarriedOn(ctx, d.namespace, d.binding, d.runID, seqBase); err != nil {
		return nil, s.resumeError(ctx, d.runID, err)
	}
	return connect.NewResponse(&orkstackv1.ResumeRunResponse{
		RunId: d.runID, Resume: resumeOf(d.record), AlreadyResumed: true, PlanDigest: d.record.GetPlanDigest(), SeqBase: seqBase,
		Action: d.record.GetBlock().GetAction(), ResumesUsed: d.record.GetBlock().GetResumesBefore(),
		TerminalSequence: d.record.GetBaseTerminalSequence(),
	}), nil
}

// resumeOf is the resume a plan's record is of.
func resumeOf(rec *orkstackv1.ResumePlanRecord) *orkstackv1.RunResume {
	return &orkstackv1.RunResume{
		At: rec.GetAt(), Action: rec.GetBlock().GetAction(), FromTaskId: rec.GetBlock().GetFromTaskId(),
		Principal: rec.GetPrincipal(), PrincipalVerified: rec.GetPrincipalVerified(),
		Reason: rec.GetReason(), SupersededReason: rec.GetSupersededReason(),
		ResumedAfterSequence: rec.GetBaseTerminalSequence(), Sequence: int64(rec.GetBlock().GetSeqBase()),
		FromState: orkstackv1.ResumeFromState_RESUME_FROM_STATE_FAILED, Scope: rec.GetScope(),
	}
}

// rerunTasks are the tasks that run again, whole or in part, each once.
func rerunTasks(plan *engine.ResumePlan) []string {
	out := []string{}
	for _, u := range plan.RerunSet() {
		if !slices.Contains(out, u.Task) {
			out = append(out, u.Task)
		}
	}
	return out
}

// reopenedAwaits are the awaited tasks that wait again.
func reopenedAwaits(wf *orkstackv1.Orkflow, plan *engine.ResumePlan) []string {
	await := map[string]bool{}
	for _, t := range wf.GetTasks() {
		await[t.GetId()] = t.GetAwait() != nil
	}
	out := []string{}
	for _, u := range plan.Units {
		if u.Disposition == engine.DispositionWait && await[u.Unit.Task] {
			out = append(out, u.Unit.Task)
		}
	}
	return out
}
