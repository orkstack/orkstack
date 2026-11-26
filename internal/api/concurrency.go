package api

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"

	"connectrpc.com/connect"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/dsl"
	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/internal/enumname"
	"github.com/orkstack/orkstack/internal/runstatus"
	"github.com/orkstack/orkstack/internal/task"
	"github.com/orkstack/orkstack/internal/telemetry"
	"github.com/orkstack/orkstack/pkg/artifacts"
	"github.com/orkstack/orkstack/pkg/eventsources"
)

// A run-level concurrency limit at submit (docs/DSL.md "Concurrency
// limits"; E88 §7.1): the key is rendered, the deployment is asked whether
// it can hold a limit at all, and the run's plan pins the slot and every
// number its wait decides with. The key and line checks of §7.1 steps 5-7
// land with the key's own checks; until then a full key queues.

// createSupportCheckTimeout bounds the fleet read create and validate make
// for their warning.
const createSupportCheckTimeout = 10 * time.Second

const (
	minQueueTimeoutSetting = "engine.concurrency.min_queue_timeout"
	maxQueueTimeoutSetting = "engine.concurrency.max_queue_timeout"
)

// busyRefusal marks a submit refusal that says the namespace or the key is
// full for now: a limit's line or keys, or a max_running filled by queued
// runs. A trigger answers it 429 with Retry-After
// (engine.concurrency.retry_after) rather than 503, which stays the
// cluster's own unavailability and rate limiting.
type busyRefusal struct{ err error }

func (b *busyRefusal) Error() string { return b.err.Error() }
func (b *busyRefusal) Unwrap() error { return b.err }

// busy is a ResourceExhausted refusal marked busyRefusal.
func busy(err error) error {
	return connect.NewError(connect.CodeResourceExhausted, &busyRefusal{err: err})
}

// isBusy reports a busyRefusal anywhere in err's chain.
func isBusy(err error) bool {
	var b *busyRefusal
	return errors.As(err, &b)
}

// limitReached marks CONCURRENCY_LIMIT_REACHED with the strategy that
// refused, so a fire tells a skip limit's full key (it skips) from a refuse
// limit's (it is refused).
type limitReached struct {
	strategy, key string
	err           error
}

func (l *limitReached) Error() string { return l.err.Error() }
func (l *limitReached) Unwrap() error { return l.err }

// limitReachedOf is the limitReached in err's chain, nil without one.
func limitReachedOf(err error) *limitReached {
	var l *limitReached
	if errors.As(err, &l) {
		return l
	}
	return nil
}

// checkUnavailable marks a refusal that says a limit could not be checked
// now (CONCURRENCY_CHECK_UNAVAILABLE, CONCURRENCY_UNSUPPORTED), which a fire
// asks again for, then completes without a run rather than failing.
type checkUnavailable struct{ err error }

func (c *checkUnavailable) Error() string { return c.err.Error() }
func (c *checkUnavailable) Unwrap() error { return c.err }

// unavailableCheck is a connect error of code marked checkUnavailable.
func unavailableCheck(code connect.Code, err error) error {
	return connect.NewError(code, &checkUnavailable{err: err})
}

// isCheckUnavailable reports a checkUnavailable anywhere in err's chain.
func isCheckUnavailable(err error) bool {
	var c *checkUnavailable
	return errors.As(err, &c)
}

// concurrencySettingsOf is Config.Concurrency, the defaults for a zero value.
func concurrencySettingsOf(c engine.ConcurrencySettings) engine.ConcurrencySettings {
	if c == (engine.ConcurrencySettings{}) {
		return engine.DefaultConcurrencySettings()
	}
	return c
}

// gateTunablesOf is Config.GateTunables, the defaults for a zero value.
func gateTunablesOf(t engine.GateTunables) engine.GateTunables {
	if t == (engine.GateTunables{}) {
		return engine.DefaultGateTunables()
	}
	return t
}

// concurrencyIndexAnswer is the starter's boot answer to whether the
// cluster indexes what a limit needs; a starter that gives none does not.
func concurrencyIndexAnswer(starter engine.RunStarter) bool {
	indexed, ok := starter.(interface{ ConcurrencyIndexAnswer() bool })
	return ok && indexed.ConcurrencyIndexAnswer()
}

// queueTimeoutOf is a limit's queue_timeout: its own, or the deployment's
// default when it declares none.
func (s *Service) queueTimeoutOf(spec *orkstackv1.ConcurrencySpec) time.Duration {
	if seconds := spec.GetQueueTimeoutSeconds(); seconds > 0 {
		return time.Duration(seconds) * time.Second
	}
	return s.concurrency.DefaultQueueTimeout
}

// concurrencyBoundsError is a limit's declared queue_timeout outside the
// deployment's bounds, naming the setting; nil within them or with none.
func (s *Service) concurrencyBoundsError(wf *orkstackv1.Orkflow) error {
	spec := wf.GetConcurrency()
	if spec == nil || spec.GetQueueTimeoutSeconds() == 0 {
		return nil
	}
	timeout := time.Duration(spec.GetQueueTimeoutSeconds()) * time.Second
	switch {
	case timeout < s.concurrency.MinQueueTimeout:
		return fmt.Errorf("concurrency.queue_timeout is %s; this deployment allows at least %s (%s)",
			timeout, s.concurrency.MinQueueTimeout, minQueueTimeoutSetting)
	case timeout > s.concurrency.MaxQueueTimeout:
		return fmt.Errorf("concurrency.queue_timeout is %s; this deployment allows at most %s (%s)",
			timeout, s.concurrency.MaxQueueTimeout, maxQueueTimeoutSetting)
	}
	return nil
}

// checkConcurrencyBounds is concurrencyBoundsError at validate and create:
// InvalidArgument on the field, as checkTaskCeiling refuses.
func (s *Service) checkConcurrencyBounds(wf *orkstackv1.Orkflow) error {
	if err := s.concurrencyBoundsError(wf); err != nil {
		return connect.NewError(connect.CodeInvalidArgument, &validationFieldError{Field: "concurrency.queue_timeout", Err: err})
	}
	return nil
}

// keyBound reports that the submission carries an idempotency key its first
// submit already bound (§7.1 step 2): its retry returns the original run,
// so the limit's admission checks are skipped for it. The plan is still
// built, because a key claimed and never started starts with this
// submission's. A lookup that fails is no answer: the checks run.
func (s *Service) keyBound(ctx context.Context, namespace string, claim claimCoords) bool {
	if claim.own == "" || s.idempotency == nil {
		return false
	}
	for _, at := range []string{claim.own, claim.other} {
		if at == "" {
			continue
		}
		_, bound, err := s.idempotency.Lookup(ctx, at)
		if err != nil {
			s.logger.Warn("an idempotency key's binding could not be read before the concurrency checks; checking",
				"namespace", namespace, "error", err)
			return false
		}
		if bound {
			return true
		}
	}
	return false
}

// planConcurrency is §7.1 steps 3 and 4 and the plan, for the run and for every
// composed child that declares a limit (planChildLimits): it refuses a key
// that does not render, a queue_timeout the deployment no longer allows and
// a deployment that cannot hold a limit, and pins the run-level slot. A run
// whose Orkflow declares none is untouched and asks nothing.
func (s *Service) planConcurrency(
	ctx context.Context,
	ns *orkstackv1.Namespace,
	planned *orkstackv1.Orkflow,
	plan *engine.RunPlan,
	msg *orkstackv1.SubmitRunRequest,
	origin *orkstackv1.RunOrigin,
	claim claimCoords,
	starter engine.RunStarter,
) error {
	spec := planned.GetConcurrency()
	limitedChild, err := childDeclaresLimit(planned, plan)
	if err != nil {
		return err
	}
	if spec == nil && !limitedChild {
		return nil // nothing of the tree is limited: nothing is asked
	}
	incarnation, err := concurrencyIncarnation(ns)
	if err != nil {
		return err
	}
	if spec != nil {
		if err := s.planRunLimit(ctx, ns, planned, plan, msg, origin, claim, starter, incarnation); err != nil {
			return err
		}
	}
	if err := s.planChildLimits(ns, planned, plan, incarnation); err != nil {
		return err
	}
	if spec != nil {
		return nil
	}
	// The run itself has no limit: what its children's need of the deployment
	// is asked here, once.
	if s.keyBound(ctx, ns.GetName(), claim) {
		return nil
	}
	return s.concurrencySupport(ctx, planned, starter)
}

// planRunLimit is planConcurrency's run-level slot.
func (s *Service) planRunLimit(
	ctx context.Context,
	ns *orkstackv1.Namespace,
	planned *orkstackv1.Orkflow,
	plan *engine.RunPlan,
	msg *orkstackv1.SubmitRunRequest,
	origin *orkstackv1.RunOrigin,
	claim claimCoords,
	starter engine.RunStarter,
	incarnation string,
) error {
	spec := planned.GetConcurrency()
	if err := s.concurrencyBoundsError(planned); err != nil {
		return connect.NewError(connect.CodeFailedPrecondition, refused(refusalPause, "CONCURRENCY_LIMIT_INVALID", fmt.Errorf(
			"orkflow %s@%s: %w; create a new version within the bounds", planned.GetName(), planned.GetVersion(), err)))
	}
	if plan.History == nil {
		// pinRunTreeLimits always runs first; a plan without its history
		// limit would carry no reserve for the release.
		return connect.NewError(connect.CodeInternal, errors.New("plan a concurrency limit: the run's history limit is not pinned"))
	}
	namespace := ns.GetName()
	owner := artifacts.ResolveNamespace(planned.GetNamespace())
	key, label := engine.DefaultRunConcurrencyKey(owner, planned.GetName()), spec.GetKey()
	if label == "" {
		label = key
	} else {
		input := engine.ApplyInputDefaults(planned, msg.GetInput())
		rendered, err := dsl.RenderConcurrencyKey(label, input.GetData().AsMap(), dsl.ConcurrencyKeyRun{
			Orkflow: planned.GetName(), Version: planned.GetVersion(),
			OriginKind: enumname.Of(origin.GetKind()), OriginID: origin.GetId(),
		})
		if err != nil {
			// The key is rendered from the run's input: a message's values are
			// its own.
			return connect.NewError(connect.CodeInvalidArgument, refused(refusalReject, eventsources.ReasonMessageConcurrencyKeyInvalid, err))
		}
		key = rendered
	}
	gateID := engine.GateID(namespace, incarnation, key)
	strategy, err := planStrategy(spec.GetOnLimit())
	if err != nil {
		return err
	}
	if !s.keyBound(ctx, namespace, claim) {
		err := s.concurrencySupport(ctx, planned, starter)
		if err == nil {
			err = s.lineChecks(ctx, namespace, gateID, key, strategy, spec)
		}
		if err != nil {
			s.observeRefusedArrival(ctx, namespace, planned.GetName(), label, strategy, origin, err)
			return err
		}
	}
	run := 0
	limit := s.concurrencyPlanOf(namespace, incarnation, planned.GetName())
	limit.Slots = []engine.ConcurrencySlotPlan{{
		GateID: gateID, Key: key, Label: label,
		Limit: int(spec.GetLimit()), Strategy: strategy,
		QueueTimeoutSeconds: int64(s.queueTimeoutOf(spec) / time.Second), MaxQueued: int(spec.GetMaxQueued()),
	}}
	limit.Run = &run
	plan.Concurrency = &limit
	plan.History = engine.ReserveConcurrencyRelease(plan.History, &limit)
	return nil
}

// runIndexGates readies ListRuns' concurrency filters (queued=, queued_on=,
// holding=), which only the cluster's index of open runs can answer: a
// deployment that does not index concurrency limits, or that has no read
// model, refuses them, and otherwise a key stands for the gate submit plans
// for it in this namespace's incarnation.
func runIndexGates(index *engine.RunIndexQuery, ns *orkstackv1.Namespace, binding engine.Binding) error {
	if binding.Reader == nil || !concurrencyIndexAnswer(binding.Starter) {
		return connect.NewError(connect.CodeFailedPrecondition, fmt.Errorf(
			"this deployment does not index concurrency limits, so it cannot list runs by queued=, queued_on= or holding=; "+
				"docs/DEPLOY.md, \"Underneath: the cluster commands\", says what a concurrency limit needs the cluster to index"))
	}
	incarnation, err := concurrencyIncarnation(ns)
	if err != nil {
		return err
	}
	namespace := ns.GetName()
	index.GateOf = func(key string) string { return engine.GateID(namespace, incarnation, key) }
	return nil
}

// applyLiveQueued reads where a run that waits for its concurrency slot
// stands now (Run.queued's position, holders and note are "read live by
// GetRun only"): the namespace and one question to its gate, together within
// censusReadTimeout, GetRun's bound on what it reads beyond the run itself.
// A limit that cannot be read, or whose gate is not running, leaves the
// place unknown and says so (NOT_ANSWERING: the run asks the gate again,
// which starts it again), logs why at most once a minute, and never fails the
// read. A run that is not queued, or has ended, asks nothing.
func (s *Service) applyLiveQueued(ctx context.Context, run *orkstackv1.Run) {
	snapshot := run.GetQueued()
	if snapshot == nil || s.concurrencyReader == nil || runstatus.IsTerminal(run.GetStatus()) {
		return
	}
	now := time.Now()
	notAnswering := func(why string, err error) {
		run.Queued = engine.NotAnsweringQueued(snapshot, now.Add(s.concurrency.RequeueIntervalMax))
		if ctx.Err() == nil && s.queuedWarn.allow(now, capacityWarnInterval) {
			s.logger.Warn("get run: "+why+", so the run's place in line is unknown (said at most once a minute)",
				"run_id", run.GetId(), "key", snapshot.GetKey(), "error", err)
		}
	}
	rctx, cancel := context.WithTimeout(ctx, censusReadTimeout)
	defer cancel()
	ns, err := s.namespace(rctx, run.GetNamespace())
	if err != nil {
		notAnswering("the run's namespace could not be read", err)
		return
	}
	incarnation, err := concurrencyIncarnation(ns)
	if err != nil {
		notAnswering("the run's namespace has no incarnation for its limit", err)
		return
	}
	gate := engine.GateID(ns.GetName(), incarnation, snapshot.GetKey())
	line, err := s.concurrencyReader.GateLine(rctx, gate)
	switch {
	case err != nil:
		notAnswering("its limit "+gate+" did not answer", err)
		return
	case !line.Present:
		notAnswering("its limit "+gate+" is not running", nil)
		return
	}
	run.Queued = engine.LiveQueued(snapshot, run.GetId(), line, s.concurrency.APIHoldersShown, now)
}

// applyChildQueued says of each composed task whose child run waits for its
// own concurrency slot where that run stands, as GetRun on the child would
// (TaskRun.queued). GetRun is polled, so it is bounded: a run with no running
// composed task whose child has a limit reads nothing, and at most
// api_holders_shown children are asked. A child the caller may not read, or
// that could not be read, is left as it is, and never fails the read.
func (s *Service) applyChildQueued(ctx context.Context, run *orkstackv1.Run) {
	if s.concurrencyReader == nil || runstatus.IsTerminal(run.GetStatus()) {
		return
	}
	rows := map[string]*orkstackv1.TaskRun{}
	var children []string
	for _, row := range run.GetTaskRuns() {
		child := row.GetChild()
		if row.GetStatus() != orkstackv1.Status_STATUS_RUNNING || !child.GetConcurrencyLimited() || child.GetRunId() == "" {
			continue
		}
		if len(children) == s.concurrency.APIHoldersShown {
			break
		}
		rows[child.GetRunId()] = row
		children = append(children, child.GetRunId())
	}
	if len(children) == 0 {
		return
	}
	failed := func(what string, err error) {
		if ctx.Err() == nil && s.queuedWarn.allow(time.Now(), capacityWarnInterval) {
			s.logger.Warn("get run: "+what+", so whether its child runs wait for a concurrency slot is not shown (said at most once a minute)",
				"run_id", run.GetId(), "error", err)
		}
	}
	rctx, cancel := context.WithTimeout(ctx, censusReadTimeout)
	defer cancel()
	waiting, err := s.concurrencyReader.WaitingRuns(rctx, children)
	if err != nil {
		failed("a child run could not be described", err)
	}
	for _, id := range waiting {
		child, err := s.LookupRunIn(rctx, id, run.GetNamespace())
		if err != nil {
			failed("a child run could not be read", err)
			continue
		}
		may, err := s.mayReadRun(rctx, child)
		if err != nil {
			failed("whether the caller may read a child run could not be decided", err)
			continue
		}
		if !may {
			continue
		}
		s.applyLiveQueued(rctx, child)
		rows[id].Queued = child.GetQueued()
	}
}

// concurrencyIncarnation is the namespace's incarnation at its gates
// (§3.1): "" for the never-stored default, else its created_at in RFC 3339
// UTC to the nanosecond (decision D13), so two incarnations a second apart
// stay apart. Every stored namespace has one; without it two incarnations
// would share their gates, so its absence refuses.
func concurrencyIncarnation(ns *orkstackv1.Namespace) (string, error) {
	if ns.GetName() == artifacts.DefaultNamespace {
		return "", nil
	}
	if ns.GetCreatedAt() == nil {
		return "", connect.NewError(connect.CodeInternal, fmt.Errorf(
			"plan a concurrency limit: namespace %q records no creation time", ns.GetName()))
	}
	return ns.GetCreatedAt().AsTime().UTC().Format(time.RFC3339Nano), nil
}

// lineChecks are §7.1 steps 5-7 for a queue limit, each a lock-free read,
// best-effort by a bounded overshoot (the gate holds the limit itself):
//   - a key no gate holds is a new active key, refused
//     CONCURRENCY_KEYS_EXHAUSTED at the namespace's bound (§3.6);
//   - a run that would queue (its key at its limit) is refused
//     CONCURRENCY_QUEUE_FULL when its line is at max_queued or the gate's
//     max_entries, or its namespace's queued runs at
//     max_queued_runs_per_namespace.
//
// A read that cannot be made refuses CONCURRENCY_CHECK_UNAVAILABLE,
// scope=key for the gate's line and scope=namespace for a count. A holder
// that has ended still counts until the gate's own check frees it: the
// verify a full key sends lands with the strategies that refuse at a full
// key.
func (s *Service) lineChecks(ctx context.Context, namespace, gateID, key, strategy string, spec *orkstackv1.ConcurrencySpec) error {
	unavailable := func(scope, what string, err error) error {
		s.logger.Warn("concurrency check could not read the cluster", "scope", scope, "key", key, "error", err)
		return unavailableCheck(connect.CodeUnavailable, fmt.Errorf("%s (scope=%s): %s could not be read; try again: %w",
			engine.ConcurrencyCheckUnavailableCode, scope, what, err))
	}
	full := func(cause, format string, args ...any) error {
		return busy(fmt.Errorf("%s (cause=%s): %s", engine.ConcurrencyQueueFullCode, cause, fmt.Sprintf(format, args...)))
	}
	reader := s.concurrencyReader
	if reader == nil {
		return unavailableCheck(connect.CodeUnavailable, fmt.Errorf(
			"%s (scope=key): this engine is not wired to a cluster, so the line for key %s cannot be read",
			engine.ConcurrencyCheckUnavailableCode, key))
	}
	line, err := reader.GateLine(ctx, gateID)
	if err != nil {
		return unavailable("key", "the line for key "+key, err)
	}
	// A rebuilding gate does not yet know its holders, so it cannot say a
	// refuse or skip limit's key is full (a queue run waits in its line).
	if strategy != engine.ConcurrencyStrategyQueue && line.Rebuilding {
		return unavailableCheck(connect.CodeUnavailable, fmt.Errorf(
			"%s (scope=key): the limit on key %s is rebuilding its record of running runs, so it cannot say whether the key is full; try again",
			engine.ConcurrencyCheckUnavailableCode, key))
	}
	// A gate that lingers empty, or has just closed, holds nothing: a run
	// on its key opens it again, as a new key would.
	if !line.Present || line.Holders+line.Queued == 0 {
		active, err := reader.ActiveGates(ctx, namespace)
		if err != nil {
			return unavailable("namespace", fmt.Sprintf("namespace %q's active keys", namespace), err)
		}
		if bound := int64(s.concurrency.MaxActiveKeysPerNamespace); active >= bound {
			return busy(fmt.Errorf(
				"%s: namespace %q already has %d keys with runs holding or queued, its bound "+
					"(engine.concurrency.max_active_keys_per_namespace); submit again once one is free",
				engine.ConcurrencyKeysExhaustedCode, namespace, active))
		}
		return nil
	}
	if strategy != engine.ConcurrencyStrategyQueue {
		return s.fullKeyCheck(ctx, gateID, key, strategy, spec, line)
	}
	if line.Holders < int(spec.GetLimit()) {
		return nil // it would hold a slot at once
	}
	if bound := int(spec.GetMaxQueued()); bound > 0 && line.Queued >= bound {
		return full("max_queued", "the line for key %s already has %d runs waiting, its max_queued; submit again later",
			key, line.Queued)
	}
	if bound := s.gateTunables.MaxEntries; line.Holders+line.Queued >= bound {
		return full("max_entries", "key %s already has %d runs holding or waiting, the most one key holds "+
			"(engine.concurrency.max_entries); submit again later", key, line.Holders+line.Queued)
	}
	queued, err := reader.QueuedRuns(ctx, namespace)
	if err != nil {
		return unavailable("namespace", fmt.Sprintf("namespace %q's queued runs", namespace), err)
	}
	if bound := int64(s.concurrency.MaxQueuedRunsPerNamespace); queued >= bound {
		return full("namespace", "namespace %q already has %d runs waiting on concurrency limits, its bound "+
			"(engine.concurrency.max_queued_runs_per_namespace); submit again later", namespace, queued)
	}
	return nil
}

// observeRefusedArrival counts a submit the limit refused as an arrival
// (§9.9.2): refused, or skipped for a schedule's fire under skip; full for a
// full line or keys; unavailable or unsupported when the limit could not be
// checked or held.
func (s *Service) observeRefusedArrival(ctx context.Context, namespace, orkflow, label, strategy string, origin *orkstackv1.RunOrigin, err error) {
	if s.concurrencyMetrics == nil {
		return
	}
	var outcome string
	switch {
	case limitReachedOf(err) != nil && strategy == engine.ConcurrencyStrategySkip &&
		origin.GetKind() == orkstackv1.RunOriginKind_RUN_ORIGIN_KIND_SCHEDULE:
		outcome = "skipped"
	case limitReachedOf(err) != nil:
		outcome = "refused"
	case isBusy(err):
		outcome = "full"
	case isCheckUnavailable(err):
		outcome = "unavailable"
	case strings.Contains(err.Error(), engine.ConcurrencyUnsupportedCode):
		outcome = "unsupported"
	default:
		return
	}
	s.concurrencyMetrics.Arrival(ctx, telemetry.ConcurrencyArrival{Namespace: namespace, Orkflow: orkflow,
		Limit: engine.CensusLimitLabel(label), Level: "run", Strategy: strategy, Outcome: outcome})
}

// planStrategy is a declared strategy's name in the plan; one this release
// cannot hold was refused at validate, so meeting one here is a definition
// stored by another release.
func planStrategy(s orkstackv1.ConcurrencyStrategy) (string, error) {
	switch s {
	case orkstackv1.ConcurrencyStrategy_CONCURRENCY_STRATEGY_QUEUE:
		return engine.ConcurrencyStrategyQueue, nil
	case orkstackv1.ConcurrencyStrategy_CONCURRENCY_STRATEGY_REFUSE:
		return engine.ConcurrencyStrategyRefuse, nil
	case orkstackv1.ConcurrencyStrategy_CONCURRENCY_STRATEGY_SKIP:
		return engine.ConcurrencyStrategySkip, nil
	}
	return "", connect.NewError(connect.CodeFailedPrecondition, refused(refusalPause, "CONCURRENCY_LIMIT_INVALID", fmt.Errorf(
		"concurrency.on_limit %s is not one this release holds; create a version with queue, refuse or skip", enumname.Of(s))))
}

// fullKeyCheck is §7.1 step 6 for refuse and skip: a key at its limit
// refuses CONCURRENCY_LIMIT_REACHED. A holder the cluster reports ended is
// counted free, and the gate is asked to check its holders, so the run's
// own acquire finds the slot; a describe that fails cannot say the key is
// full.
func (s *Service) fullKeyCheck(ctx context.Context, gateID, key, strategy string, spec *orkstackv1.ConcurrencySpec, line engine.GateLine) error {
	limit := int(spec.GetLimit())
	if line.Holders < limit {
		return nil
	}
	holding := line.Holders
	if len(line.HolderRuns) > 0 {
		closed, err := s.concurrencyReader.ClosedRuns(ctx, line.HolderRuns)
		if err != nil {
			// Whether the key is full turns on holders that may have
			// ended: the check cannot be made, never "full".
			s.logger.Warn("the runs holding a full concurrency key could not be described", "key", key, "error", err)
			return unavailableCheck(connect.CodeUnavailable, fmt.Errorf(
				"%s (scope=key): the runs holding key %s could not be checked; try again: %w",
				engine.ConcurrencyCheckUnavailableCode, key, err))
		}
		if len(closed) > 0 {
			if err := s.concurrencyReader.Verify(ctx, gateID); err != nil {
				s.logger.Warn("a concurrency limit could not be asked to check its holders; its own pass frees them",
					"key", key, "error", err)
			}
			holding -= len(closed)
		}
	}
	if holding < limit {
		return nil
	}
	shown := line.HolderRuns
	if n := s.concurrency.APIHoldersShown; n > 0 && len(shown) > n {
		shown = shown[:n]
	}
	text := fmt.Sprintf("%s: key %s is at its limit (%d of %d running: %s)",
		engine.ConcurrencyLimitReachedCode, key, line.Holders, limit, strings.Join(shown, ", "))
	for _, run := range line.StalledRuns {
		text += fmt.Sprintf("; run %s is not making progress; an operator must end it", run)
	}
	return connect.NewError(connect.CodeFailedPrecondition, &limitReached{strategy: strategy, key: key, err: errors.New(text)})
}

// childDeclaresLimit reports whether a composed child of the tree declares a
// limit.
func childDeclaresLimit(planned *orkstackv1.Orkflow, plan *engine.RunPlan) (bool, error) {
	limited := false
	if err := engine.ForEachChild(planned, plan, func(_ string, _ *engine.ChildRunPlan, def *orkstackv1.Orkflow) error {
		limited = limited || def.GetConcurrency() != nil
		return nil
	}); err != nil {
		return false, connect.NewError(connect.CodeInternal, fmt.Errorf("read the planned child runs: %w", err))
	}
	return limited, nil
}

// planChildLimits pins the run-level limit of every composed child that
// declares one (E88 §5.4, §22.3), its key a template its parent renders when
// it starts it. A key that needs no rendering is held to its ancestors' here,
// before a run exists (CONCURRENCY_KEY_CONFLICT).
func (s *Service) planChildLimits(ns *orkstackv1.Namespace, planned *orkstackv1.Orkflow, plan *engine.RunPlan, incarnation string) error {
	var walk func(wf *orkstackv1.Orkflow, plan *engine.RunPlan, ancestors []string) error
	walk = func(wf *orkstackv1.Orkflow, plan *engine.RunPlan, ancestors []string) error {
		for _, t := range wf.GetTasks() {
			child := plan.Children[t.GetId()]
			if t.GetOrkflow() == nil || child == nil {
				continue
			}
			def, err := child.Definition()
			if err != nil {
				return connect.NewError(connect.CodeInternal, fmt.Errorf("read the planned child runs: %w", err))
			}
			below := ancestors
			if spec := def.GetConcurrency(); spec != nil {
				if err := s.concurrencyBoundsError(def); err != nil {
					return connect.NewError(connect.CodeFailedPrecondition, refused(refusalPause, "CONCURRENCY_LIMIT_INVALID", fmt.Errorf(
						"orkflow %s@%s, composed by task %s: %w; create a new version within the bounds", def.GetName(), def.GetVersion(), t.GetId(), err)))
				}
				strategy, err := planStrategy(spec.GetOnLimit())
				if err != nil {
					return err
				}
				key, label := engine.DefaultRunConcurrencyKey(artifacts.ResolveNamespace(def.GetNamespace()), def.GetName()), spec.GetKey()
				if label == "" {
					label = key
				} else {
					key = label
				}
				child.Concurrency = &engine.ChildConcurrency{
					Slot: engine.ConcurrencySlotPlan{Key: key, Label: label, Limit: int(spec.GetLimit()), Strategy: strategy,
						QueueTimeoutSeconds: int64(s.queueTimeoutOf(spec) / time.Second), MaxQueued: int(spec.GetMaxQueued())},
					Plan:          s.concurrencyPlanOf(ns.GetName(), incarnation, def.GetName()),
					MaxActiveKeys: int64(s.concurrency.MaxActiveKeysPerNamespace),
				}
				if !strings.Contains(key, "{{") {
					gate := engine.GateID(ns.GetName(), incarnation, key)
					if slices.Contains(ancestors, gate) {
						return connect.NewError(connect.CodeInvalidArgument, refused(refusalPause, engine.ConcurrencyKeyConflictCode, fmt.Errorf(
							"%s: task %s composes %s@%s, whose concurrency key %s is one a run above it holds: the child would wait for a slot its own parent keeps",
							engine.ConcurrencyKeyConflictCode, t.GetId(), def.GetName(), def.GetVersion(), key)))
					}
					below = append(slices.Clone(ancestors), gate)
				}
			}
			if err := walk(def, &child.Plan, below); err != nil {
				return err
			}
		}
		return nil
	}
	return walk(planned, plan, plan.Concurrency.GateIDs())
}

// concurrencyPlanOf is a run's concurrency plan with no slot: what its limits
// are decided with.
func (s *Service) concurrencyPlanOf(namespace, incarnation, orkflow string) engine.ConcurrencyPlan {
	settings, gate := s.concurrency, s.gateTunables
	return engine.ConcurrencyPlan{
		Orkflow: orkflow,
		// A fresh run's incarnation at its gates (decision D2).
		SeqBase: 1, GateQueue: task.DefaultQueue, Namespace: namespace, Incarnation: incarnation,
		AcquireTimeout: gate.AcquireTimeout, AcquireBackoffMin: settings.AcquireBackoffMin, AcquireBackoffMax: settings.AcquireBackoffMax,
		RequeueInterval: settings.RequeueInterval, RequeueBackoffFactor: settings.RequeueBackoffFactor,
		RequeueIntervalMax: settings.RequeueIntervalMax, PerSlotBytes: settings.PerSlotBytes,
		EventHoldersShown: settings.EventHoldersShown, Gate: gate,
	}
}

// concurrencySupport is §7.1 step 4: CONCURRENCY_UNSUPPORTED naming every
// cause that applies (cause=index: the cluster does not index the limit's
// attributes; cause=fleet: an engine process polling the engine's channel
// runs a release without concurrency-1), and CONCURRENCY_CHECK_UNAVAILABLE
// when the fleet cannot be read. It fails closed.
func (s *Service) concurrencySupport(ctx context.Context, wf *orkstackv1.Orkflow, starter engine.RunStarter) error {
	var causes []string
	if !concurrencyIndexAnswer(starter) {
		causes = append(causes, "cause=index: the cluster does not index the attributes a limit is kept by "+
			"(docs/DEPLOY.md, \"Search attributes\")")
	}
	if s.capabilities == nil && len(causes) > 0 {
		return unsupported(wf, causes, false)
	}
	if s.capabilities == nil {
		return unavailableCheck(connect.CodeUnavailable, fmt.Errorf(
			"%s (scope=fleet): orkflow %s@%s declares a concurrency limit, and this engine "+
				"is not wired to a cluster, so whether every engine process can hold one is unknown",
			engine.ConcurrencyCheckUnavailableCode, wf.GetName(), wf.GetVersion()))
	}
	lacking, err := s.capabilities.MissingCapability(ctx, engine.CapabilityConcurrency, engine.ScopeWorkflow)
	if err != nil {
		s.logger.Warn("concurrency check could not read the engine processes",
			"orkflow", wf.GetName(), "version", wf.GetVersion(), "error", err)
		return unavailableCheck(connect.CodeUnavailable, fmt.Errorf(
			"%s (scope=fleet): orkflow %s@%s declares a concurrency limit, and the cluster's "+
				"list of engine processes could not be read; try again", engine.ConcurrencyCheckUnavailableCode, wf.GetName(), wf.GetVersion()))
	}
	indexMissing := len(causes) > 0
	if len(lacking) > 0 {
		s.logger.Warn("submit refused: engine processes run a release without concurrency limits",
			"missing_capability", engine.CapabilityConcurrency, "identities", lacking,
			"orkflow", wf.GetName(), "version", wf.GetVersion())
		causes = append(causes, fmt.Sprintf("cause=fleet: %s a release that cannot hold a limit; finish the rollout first",
			processCount(len(lacking))))
	}
	if len(causes) == 0 {
		return nil
	}
	return unsupported(wf, causes, !indexMissing)
}

// unsupported is CONCURRENCY_UNSUPPORTED naming every cause. A fleet
// mid-rollout is the only cause (fleetOnly) that clears by itself, so it is
// Unavailable (a trigger's 503, a fire's retry); a cluster that does not
// index the limit is FailedPrecondition.
func unsupported(wf *orkstackv1.Orkflow, causes []string, fleetOnly bool) error {
	// A rollout clears by itself, so a fire asks again (checkUnavailable);
	// an unindexed cluster does not, so it is a refusal.
	mark := func(err error) error {
		return connect.NewError(connect.CodeFailedPrecondition, refused(refusalPause, "CONCURRENCY_UNSUPPORTED", err))
	}
	if fleetOnly {
		mark = func(err error) error {
			return unavailableCheck(connect.CodeUnavailable, refused(refusalWait, waitRollout, err))
		}
	}
	return mark(fmt.Errorf(
		"%s: orkflow %s@%s declares a concurrency limit this deployment cannot hold (%s)",
		engine.ConcurrencyUnsupportedCode, wf.GetName(), wf.GetVersion(), strings.Join(causes, "; ")))
}

// concurrencyIssues are create's and validate's warnings for a limit the
// deployment could not hold now (§7.1, "At create"): the Orkflow is stored,
// and every cause the submit would refuse with is said. A WARNING only: a
// deployment finishing its rollout, or indexing the attributes, holds it
// later. An Orkflow with no limit asks nothing.
func (s *Service) concurrencyIssues(ctx context.Context, ns *orkstackv1.Namespace, wf *orkstackv1.Orkflow) []*orkstackv1.ValidationIssue {
	if wf.GetConcurrency() == nil {
		return nil
	}
	binding, err := s.binding(ns.GetName())
	if err != nil {
		s.logger.Warn("create could not check whether the deployment can hold a concurrency limit",
			"namespace", ns.GetName(), "orkflow", wf.GetName(), "error", err)
	} else {
		// A warning is worth a short wait only: a slow cluster leaves the
		// create answered, with the check said to be unmade.
		wctx, cancel := context.WithTimeout(ctx, createSupportCheckTimeout)
		err = s.concurrencySupport(wctx, wf, binding.Starter)
		cancel()
	}
	if err == nil {
		return nil
	}
	var connectErr *connect.Error
	message := err.Error()
	if errors.As(err, &connectErr) {
		message = connectErr.Message()
	}
	return []*orkstackv1.ValidationIssue{composeIssue("concurrency",
		"a run of this Orkflow would be refused now: "+message, orkstackv1.ValidationSeverity_VALIDATION_SEVERITY_WARNING)}
}
