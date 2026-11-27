package api

import (
	"cmp"
	"context"
	"errors"
	"slices"
	"strings"
	"time"

	"connectrpc.com/connect"
	enumspb "go.temporal.io/api/enums/v1"
	"google.golang.org/protobuf/proto"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/internal/identity"
	"github.com/orkstack/orkstack/internal/runstatus"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// maxEventSequence is the largest sequence an event may take: every JSON
// reader holds it exactly, and clients resume a stream by it.
const maxEventSequence = 1<<53 - 1

// resumeSequenceRoom is the sequences a resumed run is left below
// maxEventSequence: more than any run publishes.
const resumeSequenceRoom = 1 << 32

// The causes a refused plan carries (docs/REASONS.md).
const (
	// causeEventsAboveTerminal: the run's stored events go on above its
	// ending by more than a resume passes by.
	causeEventsAboveTerminal = "events_above_terminal"
)

// resumePlanned is a base's resume as it would be carried out now.
type resumePlanned struct {
	input     engine.ResumePlanInput
	plan      *engine.ResumePlan
	verified  engine.ResumeVerification
	seqBase   int64
	highest   int64
	block     *orkstackv1.ResumeBlock
	preflight engine.ResumePreflight
	digest    string
	// warnings are what the plan's admission found worth saying, not
	// refusing: a pin host still holding the run's old slot.
	warnings []*orkstackv1.ResumeWarning
}

// maxLoggedCauses bounds how many saved results one request logs.
const maxLoggedCauses = 10

// planResume is a resume's third phase (design §26.4): the saved results it
// would keep are checked, the plan is made from them, and everything the
// deployment's state now can refuse is asked. It writes nothing.
func (s *Service) planResume(ctx context.Context, base *resumeBase, msg *orkstackv1.ResumeRunRequest) (*resumePlanned, error) {
	p := &resumePlanned{input: base.planInput(msg)}
	var err error
	if base.start.Plan.CommitRoot == "" {
		return nil, refuseResume(engine.RefuseStateUnreadable, engine.RefusalPermanent,
			"run %s does not say where its results are saved. Run again", base.run.GetId())
	}
	var recorded *engine.ResumeRecordedRun
	if base.start.Plan.Identity != nil {
		if recorded, err = s.resumeRecordedRun(ctx, base, p.input.Class == engine.ResumeBaseForceCanceled); err != nil {
			return nil, err
		}
	}
	verifier := engine.NewResumeVerifier(s.commits, s.artifactStore)
	if p.verified, err = verifier.VerifyRecorded(ctx, p.input, base.start.Plan.CommitRoot, recorded); err != nil {
		if ctx.Err() != nil {
			return nil, connect.NewError(connect.CodeUnavailable, err)
		}
		return nil, err
	}
	logged := 0
	for u, cause := range p.verified.Causes {
		if logged++; logged > maxLoggedCauses {
			s.logger.InfoContext(ctx, "resume: more saved results were not found whole", "run_id", base.run.GetId(), "count", len(p.verified.Causes))
			break
		}
		s.logger.InfoContext(ctx, "resume: a saved result was not found whole", "run_id", base.run.GetId(), "task_id", u.Task, "item", u.Item, "error", cause)
	}
	p.input.Evidence = p.verified.Evidence
	if p.plan, err = engine.PlanResume(p.input); err != nil {
		return nil, err
	}
	if err := refuseUnbuilt(base); err != nil {
		return nil, err
	}
	if err := s.planSequence(ctx, base, msg, p); err != nil {
		return nil, err
	}
	// The scope and the plan's number are settled when the plan is recorded.
	p.block, err = engine.BuildResumeBlock(engine.ResumeBlockInput{
		Input: p.input, Plan: p.plan, Verification: p.verified, SeqBase: uint64(p.seqBase), Scope: uint64(p.seqBase),
	})
	if err != nil {
		return nil, err
	}
	if err := engine.CheckItemEpochs(p.input, p.plan, uint64(p.seqBase)); err != nil {
		return nil, err
	}
	if p.preflight, err = engine.PreflightResume(p.input, base.start.Plan, base.history, p.plan, p.verified); err != nil {
		return nil, err
	}
	if err := s.admitResume(ctx, base, p); err != nil {
		return nil, err
	}
	p.digest = engine.ResumePlanDigestOf(planRecord(base, p.block))
	return p, nil
}

// resumeRecordedRun is how base's records are read: a run whose results the
// engine records (design §28). Its resumes are read up to the one base is of:
// one whose plan was recorded and never carried out decides nothing yet. Its
// end is the earlier of its execution's close and its last state's finish,
// after which no answer settles anything.
func (s *Service) resumeRecordedRun(ctx context.Context, base *resumeBase, forceCanceled bool) (*engine.ResumeRecordedRun, error) {
	id := base.run.GetId()
	if s.recordedResults == nil {
		return nil, resumeUnavailable("this engine reads no record of run %s; try again shortly", id)
	}
	end := base.execution.CloseTime
	if at := base.run.GetFinishedAt(); at != nil && (end.IsZero() || at.AsTime().Before(end)) {
		end = at.AsTime()
	}
	if end.IsZero() {
		return nil, resumeUnavailable("when run %s ended can't be read right now; try again shortly", id)
	}
	chain, err := s.recordedResults.ResumeChain(ctx, base.namespace, id)
	if err != nil {
		s.logger.WarnContext(ctx, "resume: a run's resume records could not be read", "run_id", id, "error", err)
		return nil, resumeUnavailable("the resumes of run %s can't be read right now; try again shortly", id)
	}
	switch chain.Answer {
	case artifacts.RecordFound:
	case artifacts.RecordUnsupported:
		return nil, unreadableRecord(id)
	default:
		s.logger.WarnContext(ctx, "resume: a run's resume records are not valid", "run_id", id,
			"answer", chain.Answer.String(), "object", chain.Object, "reason", chain.Reason)
		return nil, refuseResume(engine.RefuseStateUnreadable, engine.RefusalPermanent, "run %s's resume records can't be read back. Run again", id)
	}
	resumes := chain.Value
	for len(resumes) > 0 && resumes[0].Scope > base.scope {
		resumes = resumes[1:]
	}
	children, err := s.resumeChildren(ctx, base, forceCanceled)
	if err != nil {
		return nil, err
	}
	plan := base.start.Plan.Identity
	return &engine.ResumeRecordedRun{Store: s.recordedResults, AdmissionSHA256: plan.AdmissionSHA256, Resumes: resumes, End: end,
		Spacings: plan.RecordBudget.Spacings(), Children: children, Received: base.start.Plan.InputArtifacts}, nil
}

// resumeChildren are the child runs of base's composed tasks that a resume
// may settle from their outputs on record (design §28.3): as the run's plan
// names them, which its state does not decide, and whether the cluster says
// each closed SUCCEEDED. A child still open waits (phase 2 refused it
// already, unless it started since).
func (s *Service) resumeChildren(ctx context.Context, base *resumeBase, forceCanceled bool) (map[string]engine.ResumeChildAsk, error) {
	out := map[string]engine.ResumeChildAsk{}
	for _, row := range base.run.GetTaskRuns() {
		id := row.GetTaskId()
		if !engine.ComposedOnRecordCandidate(row, forceCanceled) {
			continue
		}
		c := base.start.Plan.Children[id]
		if c == nil || c.RunID == "" || c.Plan.Identity == nil {
			continue // read as a task with no admitted child
		}
		ask := engine.ResumeChildAsk{RunID: c.RunID, AdmissionSHA256: c.Plan.Identity.AdmissionSHA256}
		e, err := base.binding.Resume.DescribeResumeExecution(ctx, c.RunID, "")
		switch {
		case errors.Is(err, engine.ErrResumeExecutionNotFound):
			// Past the cluster's retention, or never started: whether it
			// succeeded can't be told.
			s.logger.InfoContext(ctx, "resume: a composed task's child run is not in the cluster", "run_id", base.run.GetId(), "task_id", id, "child_run_id", c.RunID)
			ask.Unknown = true
		case err != nil:
			s.logger.WarnContext(ctx, "resume: a composed task's child run could not be read", "run_id", base.run.GetId(), "child_run_id", c.RunID, "error", err)
			return nil, waitStopping(causeChildrenUnconfirmed, resumeCheckRetry,
				"whether run %s, below run %s, succeeded can't be told right now; try again shortly", c.RunID, base.run.GetId())
		case e.Status == enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING || e.Status == enumspb.WORKFLOW_EXECUTION_STATUS_CONTINUED_AS_NEW:
			return nil, s.childRunning(ctx, base, id, c.RunID, e.StartTime)
		case e.Status == enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED:
			// A run's execution completes whatever its own ending: the run
			// says whether it succeeded.
			run, err := base.binding.Reader.GetRun(ctx, c.RunID)
			if err != nil {
				s.logger.WarnContext(ctx, "resume: a composed task's child run could not be read", "run_id", base.run.GetId(), "child_run_id", c.RunID, "error", err)
				return nil, waitStopping(causeChildrenUnconfirmed, resumeCheckRetry,
					"whether run %s, below run %s, succeeded can't be told right now; try again shortly", c.RunID, base.run.GetId())
			}
			if !runstatus.IsTerminal(run.GetStatus()) {
				// Its execution has completed and its state does not say so yet.
				return nil, waitStopping(causeChildrenUnconfirmed, resumeCheckRetry,
					"whether run %s, below run %s, succeeded can't be told yet; try again shortly", c.RunID, base.run.GetId())
			}
			ask.Succeeded = run.GetStatus() == orkstackv1.Status_STATUS_SUCCEEDED
		}
		out[id] = ask
	}
	return out, nil
}

// refuseUnbuilt refuses a run this release's engine does not resume.
func refuseUnbuilt(base *resumeBase) error {
	unsupported := func(cause, format string, args ...any) error {
		r := refuseResume(resumeReleaseUnsupport, engine.RefusalPermanent, format, args...)
		r.Cause = cause
		return r
	}
	if part, why, unbuilt := engine.ResumeNeedsUnbuilt(base.definition, base.start.Plan); unbuilt {
		return unsupported(string(part), "run %s can't be resumed by this release: %s", base.run.GetId(), why)
	}
	return nil
}

// planSequence sets the sequence the resume begins at: one above everything
// published or stored for the run. A run whose stored events go on above its
// ending by more than max_seq_gap was not written to by its own ending, and
// is resumed above them only as the caller accepts it, at the sequence it saw.
func (s *Service) planSequence(ctx context.Context, base *resumeBase, msg *orkstackv1.ResumeRunRequest, p *resumePlanned) error {
	id := base.run.GetId()
	snapshot, err := s.resumeSnapshots.HighestSnapshot(ctx, base.namespace, id)
	if err != nil {
		s.logger.WarnContext(ctx, "resume: a run's stored snapshots could not be listed", "run_id", id, "error", err)
		return resumeUnavailable("what is stored for run %s can't be read right now; try again shortly", id)
	}
	p.highest = max(base.sequence, base.history.MaxPublishedSequence, snapshot)
	switch {
	case msg.GetAcceptSequenceGap() && msg.GetAcceptedHighestSeq() != p.highest:
		r := refuseResume(resumePlanChanged, engine.RefusalReplan,
			"run %s's stored events now go up to %d; preview again", id, p.highest)
		r.Highest = p.highest
		return r
	case !msg.GetAcceptSequenceGap() && p.highest-base.sequence > s.resumeSettings.MaxSequenceGap:
		r := refuseResume(engine.RefuseStateUnreadable, engine.RefusalReplan,
			"events up to %d above run %s's ending at %d were not written by it; accept the gap to continue above them, or Run again",
			p.highest, id, base.sequence)
		r.Cause, r.Highest = causeEventsAboveTerminal, p.highest
		s.resumeAnomaly(ctx, base.namespace, anomalyEventsAboveTerminal)
		return r
	case p.highest > maxEventSequence-resumeSequenceRoom:
		return refuseResume(resumeSequenceExhaust, engine.RefusalPermanent, "run %s's event sequence is exhausted; Run again", id)
	}
	p.seqBase = p.highest + 1
	return nil
}

// resumeUnavailable is a check that cannot be made now.
func resumeUnavailable(format string, args ...any) *resumeRefusal {
	r := refuseResume(engine.RefuseCheckUnavailable, engine.RefusalWait, format, args...)
	r.RetryAfter = resumeCheckRetry
	return r
}

// waitRefusal is a refusal of the deployment's state now.
func waitRefusal(code engine.ResumeRefusalCode, format string, args ...any) *resumeRefusal {
	r := refuseResume(code, engine.RefusalWait, format, args...)
	r.RetryAfter = resumeAdmissionRetry
	return r
}

// resumeAdmissionRetry is when a caller is asked to try again for what an
// operator, or other runs, must change first.
const resumeAdmissionRetry = time.Minute

// admitResume asks of the resume what a submit of the tasks it runs again
// would be asked (design §9): the namespace's admission, the release the run
// is pinned to, the image policy in force now, and their pools.
func (s *Service) admitResume(ctx context.Context, base *resumeBase, p *resumePlanned) error {
	ns, err := s.namespace(ctx, base.namespace)
	if err != nil {
		return err
	}
	if err := s.admit(ctx, ns, base.definition, base.binding); err != nil {
		var refused *engine.AdmissionRefusedError
		if errors.As(err, &refused) {
			return waitRefusal(engine.ResumeRefusalCode(refused.Reason), "%s", refused.Error())
		}
		return err
	}
	if err := s.releaseRunning(ctx, base); err != nil {
		return err
	}
	if err := s.admitResumeSlot(ctx, base); err != nil {
		return err
	}
	rerun := rerunDefinition(base.definition, p.plan)
	if len(rerun.GetTasks()) == 0 {
		return nil
	}
	needs, holds, err := s.admitResumeIdentity(ctx, ns, base, rerun)
	if err != nil {
		return err
	}
	sel, err := s.imagePolicySelection(ctx, ns)
	if err != nil {
		return err
	}
	if err := s.checkImagePolicy(ctx, ns, sel, rerun); err != nil {
		if connect.CodeOf(err) != connect.CodeInvalidArgument {
			return err
		}
		return refuseResume(resumeImagePolicy, engine.RefusalPermanent, "a task that would run again uses an image no longer allowed: %s", plainError(err))
	}
	resolver := s.resolverFor(base.binding)
	planned, _, err := resolver.Plan(ctx, ns, rerun, false)
	if err != nil {
		return resumePoolError(err)
	}
	// What runs again of a run that holds an identity runs only where every
	// Orker of its pools serves task identity, and declares the providers
	// its grants need, as at a submit.
	if holds {
		if err := resolver.CheckIdentitySupport(ctx, ns, planned, needs); err != nil {
			return resumePoolError(err)
		}
	}
	return s.refusePinLeases(ctx, base, planned, p)
}

// admitResumeIdentity asks of a resume what a submit asks of task identity
// (identity_submit.go), against the namespace's policy, the Orkers and the
// run's starter as they are now: since the run was submitted the namespace
// may have taken back an audience or a grant, or let only an Orkflow's
// current version hold identities; an Orker's storage credential may have
// stopped being confirmed scoped; the binder of the schedule, trigger or
// event source that started it may no longer submit it, or not have
// consented to what it asks for now. The tasks that run again are held to
// the namespace's policy; a run that holds an identity at all (holds), whose
// results the engine records even where only a task it keeps held one, is
// held to scoped Orkers and its starter. A resume runs no composed task
// again (COMPOSED_TASK), so no child run's definition is asked. It answers
// the providers the grants of what runs again need, for
// PoolResolver.CheckIdentitySupport. What is no longer allowed is
// permanent; an Orker not confirmed scoped, a record that cannot be read and
// a store that does not answer are waits.
func (s *Service) admitResumeIdentity(ctx context.Context, ns *orkstackv1.Namespace, base *resumeBase, rerun *orkstackv1.Orkflow) (needs map[string]engine.ProviderNeed, holds bool, err error) {
	origin := base.run.GetOrigin()
	if identity.AsksForIdentity(rerun) {
		if needs, err = s.checkIdentityAtSubmit(ctx, ns, rerun, ns.GetName(), originWord(origin)); err != nil {
			return nil, false, resumeIdentityRefusal("a task that would run again asks for what the namespace no longer allows it", err)
		}
		holds = true
	}
	holds = holds || base.start.Plan.Identity != nil
	if !holds {
		return nil, false, nil
	}
	if err := s.checkStoreScopedForIdentity(ctx); err != nil {
		switch {
		case s.fleet == nil:
			return nil, false, refuseResume(resumeStoreUnscoped, engine.RefusalPermanent, "%s", plainError(err))
		case connect.CodeOf(err) == connect.CodeFailedPrecondition:
			return nil, false, waitRefusal(resumeStoreUnscoped, "%s", plainError(err))
		case connect.CodeOf(err) == connect.CodeUnavailable:
			return nil, false, resumeUnavailable("the Orkers' storage credentials could not be confirmed scoped now: %s; try again shortly", plainError(err))
		}
		return nil, false, err
	}
	byObject, err := startedByAnObject(origin)
	if err != nil {
		return nil, false, resumeIdentityRefusal("the run's start cannot hold an identity", err)
	}
	if byObject {
		admitted, err := s.admittedBinder(ctx, base)
		if err != nil {
			return nil, false, err
		}
		if err := s.judgeResumedStarter(ctx, ns, origin, admitted, base.definition.GetName(), base.definition.GetVersion()); err != nil {
			if reason, ok := starterRefusal(err); ok && reason == reasonStarterNotRecorded {
				return nil, false, refuseResume(resumeIdentityPolicy, engine.RefusalPermanent,
					"run %s holds an identity, and a resume cannot tell which binder of %s %q started it: its admission records none it can still be held to; run it again",
					base.run.GetId(), originWord(origin), cmp.Or(origin.GetDescription(), origin.GetId()))
			}
			return nil, false, resumeIdentityRefusal("the binder that started the run may no longer start it with an identity, so it is not resumed; once that is put right, resume it again, or run it again", err)
		}
	}
	return needs, true, nil
}

// admittedBinder is the binder base's admission recorded at submit, or nil
// where it recorded none, or no admission of it can be read as found: the
// resume then cannot tell its binder, and is refused (judgeResumedStarter).
// A store that does not answer is a wait.
func (s *Service) admittedBinder(ctx context.Context, base *resumeBase) (*artifacts.AdmittedBinder, error) {
	planned := base.start.Plan.Identity
	if planned == nil || planned.AdmissionSHA256 == "" || s.awaitRecords == nil {
		return nil, nil
	}
	read, err := s.awaitRecords.ReadAdmission(ctx, base.namespace, base.run.GetId(), planned.AdmissionSHA256)
	if err != nil {
		return nil, resumeUnavailable("run %s's admission could not be read now; try again shortly", base.run.GetId())
	}
	if read.Answer != artifacts.RecordFound {
		return nil, nil
	}
	return read.Value.Origin.Binder, nil
}

// resumeIdentityRefusal is a submit's identity refusal as a resume's, led by
// lead: what is no longer allowed or no longer there is permanent, and a
// record that cannot be read, or a store that does not answer, is a wait.
func resumeIdentityRefusal(lead string, err error) error {
	switch {
	case errors.Is(err, artifacts.ErrPolicyUnreadable), errors.Is(err, artifacts.ErrIncarnationUnreadable):
		return resumeUnavailable("%s", plainError(err))
	case connect.CodeOf(err) == connect.CodeFailedPrecondition, connect.CodeOf(err) == connect.CodeNotFound,
		connect.CodeOf(err) == connect.CodePermissionDenied:
		return refuseResume(resumeIdentityPolicy, engine.RefusalPermanent, "%s: %s", lead, plainError(err))
	case connect.CodeOf(err) == connect.CodeUnavailable:
		return resumeUnavailable("%s; try again shortly", plainError(err))
	}
	return err
}

// admitResumeSlot asks of the run's concurrency limit what a submit would be
// asked (E88 §7.1), under the plan the run was submitted with: a resumed run
// asks for its slot anew, and may wait in line for it. Best-effort, as a
// submit's is: one that loses the race after it ends on the limit, and has
// used a resume.
func (s *Service) admitResumeSlot(ctx context.Context, base *resumeBase) error {
	slot, ok := base.start.Plan.RunConcurrencySlot()
	if !ok {
		return nil
	}
	err := s.concurrencySupport(ctx, base.definition, base.binding.Starter)
	if err == nil {
		err = s.lineChecks(ctx, base.namespace, slot.GateID, slot.Key, slot.Strategy,
			&orkstackv1.ConcurrencySpec{Limit: int32(slot.Limit), MaxQueued: int32(slot.MaxQueued)})
	}
	if err == nil {
		return nil
	}
	text := plainError(err)
	for _, code := range []string{engine.ConcurrencyLimitReachedCode, engine.ConcurrencyQueueFullCode, engine.ConcurrencyKeysExhaustedCode,
		engine.ConcurrencyUnsupportedCode} {
		if strings.HasPrefix(text, code) {
			return waitRefusal(engine.ResumeRefusalCode(code), "%s", text)
		}
	}
	// The limit could not be read: the cluster's own error is logged, never
	// answered.
	s.logger.WarnContext(ctx, "resume: a run's concurrency limit could not be checked", "run_id", base.run.GetId(), "key", slot.Key, "error", err)
	return resumeUnavailable("whether run %s's concurrency limit can take it can't be checked; try again shortly", base.run.GetId())
}

// plainError is err's own sentence, without its wire code.
func plainError(err error) string {
	var wire *connect.Error
	if errors.As(err, &wire) {
		return wire.Message()
	}
	return err.Error()
}

// resumePoolError is a pool's refusal of the tasks that run again.
func resumePoolError(err error) error {
	var (
		empty   *engine.PoolEmptyError
		noPin   *engine.PoolNoPinHostError
		profile *engine.ProfileUnavailableError
		large   *engine.PoolResourcesUnsatisfiableError

		noIdentity *engine.PoolNoIdentitySupportError
		noProvider *engine.PoolNoProviderError
	)
	switch {
	case errors.As(err, &empty):
		r := waitRefusal(resumePoolUnserved, "no Orker of pool %s is running, so nothing would run %s again", empty.Pool, strings.Join(empty.Tasks, ", "))
		r.Tasks = empty.Tasks
		return r
	case errors.As(err, &noPin):
		r := waitRefusal(resumePoolUnserved, "no Orker of pool %s hosts pins, so nothing would run %s again", noPin.Pool, strings.Join(noPin.Tasks, ", "))
		r.Tasks = noPin.Tasks
		return r
	case errors.As(err, &profile):
		return waitRefusal(resumePoolUnserved, "%s", profile.Error())
	case errors.As(err, &noIdentity):
		r := waitRefusal(resumePoolUnserved, "%s", noIdentity.Error())
		r.Tasks = noIdentity.Tasks
		return r
	case errors.As(err, &noProvider):
		return waitRefusal(resumePoolUnserved, "%s", noProvider.Error())
	case errors.As(err, &large):
		r := refuseResume(resumeTaskOverCap, engine.RefusalPermanent, "task %s asks for more than pool %s allows now", large.Task, large.Pool)
		r.Tasks = []string{large.Task}
		return r
	}
	return poolPlanError(err)
}

// releaseRunning refuses while no engine of the release the run is pinned to
// runs: nothing else carries its resume on. An unversioned run asks nothing.
func (s *Service) releaseRunning(ctx context.Context, base *resumeBase) error {
	build := base.execution.BuildID
	if build == "" {
		return nil
	}
	if s.releases == nil {
		return resumeUnavailable("whether release %s is running can't be checked; try again shortly", build)
	}
	obs, err := s.releases.ObserveReleases(ctx)
	if err != nil {
		s.logger.WarnContext(ctx, "resume: the releases could not be read", "run_id", base.run.GetId(), "error", err)
		return resumeUnavailable("whether release %s is running can't be checked right now; try again shortly", build)
	}
	if obs.EngineReplicas[build] == 0 {
		return waitRefusal(resumeReleaseDown, "the release run %s started on (%s) is not running", base.run.GetId(), build)
	}
	return nil
}

// rerunDefinition is the definition's container tasks that would run again,
// as a submit of them alone would be checked.
func rerunDefinition(wf *orkstackv1.Orkflow, plan *engine.ResumePlan) *orkstackv1.Orkflow {
	again := map[string]bool{}
	for _, u := range plan.RerunSet() {
		again[u.Task] = true
	}
	out := proto.Clone(wf).(*orkstackv1.Orkflow)
	out.Tasks = slices.DeleteFunc(out.Tasks, func(t *orkstackv1.Task) bool { return !again[t.GetId()] })
	for _, t := range out.Tasks {
		t.Needs = slices.DeleteFunc(t.Needs, func(id string) bool { return !again[id] })
	}
	return out
}

// planRecord is the part of a plan's record that is the plan: what a preview
// shows, whoever confirms it and whenever.
func planRecord(base *resumeBase, block *orkstackv1.ResumeBlock) *orkstackv1.ResumePlanRecord {
	return &orkstackv1.ResumePlanRecord{
		Format: artifacts.ResumePlanFormat, Namespace: base.namespace, RunId: base.run.GetId(),
		BaseExecutionRunId: base.execution.ExecutionRunID, BaseTerminalSequence: base.sequence,
		RScheduledEventId: base.history.ResetPoint.ScheduledEventID, RStartedEventId: base.history.ResetPoint.StartedEventID,
		Block: block, ForceCanceledBy: forceCanceledBy(base),
	}
}

// forceCanceledBy is who force-cancelled the base, as its verified record
// names them and the request it was; nil for a run that failed.
func forceCanceledBy(base *resumeBase) *orkstackv1.ResumeForceCanceledBy {
	if base.class != engine.ResumeBaseForceCanceled {
		return nil
	}
	fc := base.forceCancel
	return &orkstackv1.ResumeForceCanceledBy{
		Principal:         fc.Record.Principal,
		PrincipalVerified: fc.Verification == engine.ForceCancelVerified && fc.Record.PrincipalVerified,
		RequestId:         fc.Record.RequestID,
	}
}
