package api

import (
	"context"
	"errors"
	"slices"
	"strings"
	"time"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/proto"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/engine"
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
	// causeConcurrencySlot: the run's tasks are held to a concurrency limit, whose
	// slot a resumed run cannot take yet.
	causeConcurrencySlot = "concurrency_slot"
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
	verifier := engine.NewResumeVerifier(s.commits, s.artifactStore)
	if p.verified, err = verifier.Verify(ctx, p.input, base.start.Plan.CommitRoot); err != nil {
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

// refuseUnbuilt refuses a run this release's engine does not resume.
func refuseUnbuilt(base *resumeBase) error {
	unsupported := func(cause, format string, args ...any) error {
		r := refuseResume(resumeReleaseUnsupport, engine.RefusalPermanent, format, args...)
		r.Cause = cause
		return r
	}
	if engine.ResumeLimitUnbuilt(base.start.Plan) {
		return unsupported(causeConcurrencySlot, "run %s's tasks are held to a concurrency limit, which this release can't resume a run under; Run again", base.run.GetId())
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
	planned, _, err := s.resolverFor(base.binding).Plan(ctx, ns, rerun, false)
	if err != nil {
		return resumePoolError(err)
	}
	return s.refusePinLeases(ctx, base, planned)
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
		Block: block,
	}
}
