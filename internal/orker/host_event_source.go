package orker

import (
	"context"
	"errors"
	"log/slog"
	"strings"
	"sync"
	"time"

	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/worker"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/orker/servicehost"
	"github.com/orkstack/orkstack/internal/orker/sourcehost"
	"github.com/orkstack/orkstack/pkg/artifacts"
	"github.com/orkstack/orkstack/pkg/eventsources"
)

// SourceHosting is what an Orker hosts event sources with.
type SourceHosting struct {
	// Engine answers who this Orker's credential is, and Conn is its host service.
	Engine Whoamier
	Conn   *sourcehost.HostConn
	// Records holds receipts and staged parts, Logs and Attempts what an
	// instance's attempt leaves.
	Records  sourcehost.RecordStore
	Logs     InstanceLogStore
	Attempts InstanceAttemptStore
	// Deployment names the deployment in a service's context.
	Deployment string
}

// SetHosting is what this Orker hosts event sources with; it is set before its
// source worker starts.
func (a *Activities) SetHosting(h SourceHosting) { a.hosting, a.engine = &h, h.Engine }

// InstanceAttemptStore keeps instances' attempt records.
type InstanceAttemptStore interface {
	PutInstanceAttempt(ctx context.Context, a artifacts.InstanceAttempt, record eventsources.AttemptRecord) error
}

const (
	// instanceTick is how often a running instance's bounds are looked at.
	instanceTick = time.Second
	// loopStalledAfter is how old the instance's own look at its lanes may be.
	// Nothing its loop does takes a third as long.
	loopStalledAfter = 30 * time.Second
	// confirmAgain is how soon a confirmation that did not accept is asked
	// again; a start's first few are sooner.
	confirmAgain = 30 * time.Second
	// rotateBefore is how long before its activity's bound an instance begins to
	// stop: what its stop may take, and to spare.
	rotateBefore = 3 * time.Minute
	// confirmUnavailableMax is the bound where the activity's input sets none.
	confirmUnavailableMax = time.Hour
	// leaseSeconds is the lease an Orker would like a message held under.
	leaseSeconds = 60
	// instanceEnds is how long an instance told to end may take before its
	// container is stopped without it.
	instanceEnds = 25 * time.Second
	// recordTimeout bounds an attempt's record.
	recordTimeout = 10 * time.Second
)

// The phases an attempt of an instance goes through (its heartbeat's and its
// record's).
const (
	phaseStart     = "start"
	phaseConfirm   = "confirm"
	phasePolicy    = "policy"
	phaseImage     = "image"
	phaseWorkspace = "workspace"
	phaseRun       = "run"
	phaseStop      = "stop"
)

// evicted is why this Orker ends an instance itself.
type evicted struct{ reason, detail string }

func (e *evicted) Error() string { return e.reason + ": " + e.detail }

// hostedInstance is one attempt of an instance, as its Orker keeps it.
type hostedInstance struct {
	label string
	end   context.CancelCauseFunc
}

// host registers an attempt, so that what evicts the Orker's instances finds it.
func (s *serviceHostState) host(h *hostedInstance) (leave func()) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.hosted == nil {
		s.hosted = map[*hostedInstance]bool{}
	}
	s.hosted[h] = true
	return func() {
		s.mu.Lock()
		defer s.mu.Unlock()
		delete(s.hosted, h)
	}
}

// hostedNow is how many instances this Orker has taken and not given back.
func (s *serviceHostState) hostedNow() int32 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return int32(len(s.hosted))
}

// evict ends every instance this Orker hosts for a reason of the Orker's, and
// returns how many there were. One taken after it asks mayHost itself.
func (a *Activities) evictInstances(reason, detail string) int {
	a.services.mu.Lock()
	defer a.services.mu.Unlock()
	for h := range a.services.hosted {
		h.end(&evicted{reason, detail})
	}
	return len(a.services.hosted)
}

// mayHost is why this Orker takes no instance now, nil where it does.
func (a *Activities) mayHost() *instanceFailure {
	if _, stopping := a.cordon.Stopping(); stopping {
		return instanceFail(eventsources.ReasonOrkerStopped, "this Orker is stopping")
	}
	if unfit := a.unfitToHost(); len(unfit) > 0 {
		return instanceFail(eventsources.ReasonServiceHostUnfit, "this Orker hosts no service: %v", unfit)
	}
	if cordoned, reason, _ := a.cordon.State(); cordoned {
		return instanceFail(eventsources.ReasonOrkerCordoned, "this Orker is cordoned: %s", reason)
	}
	return nil
}

// mayTake is why this Orker does not take the instance, nil where it does: it is of a
// namespace it does not serve, of which it reads nothing, or it takes none now.
func (h *instanceHost) mayTake() *instanceFailure {
	if ns, served := h.in.GetNamespace(), h.a.dispatcher.Namespaces; !served.Serves(ns) {
		return instanceFail(eventsources.ReasonNamespaceNotServed, "this Orker serves %s (%s), and not %q", strings.Join(served.Names(), ", "), NamespacesVar, ns)
	}
	return h.a.mayHost()
}

// HostEventSource runs one attempt of one instance of an event source: it takes it,
// confirms it with the engine, starts its container and converses with it until
// one of them ends it.
func (a *Activities) HostEventSource(ctx context.Context, in *orkstackv1.HostEventSourceInput) (*orkstackv1.HostEventSourceResult, error) {
	if a.hosting == nil {
		return instanceOutcome(in, activity.GetInfo(ctx).Attempt, instanceEnding{instanceFailure: *instanceFail(eventsources.ReasonServiceHostUnfit, "this Orker hosts no event source")})
	}
	h := a.newInstanceHost(in, activity.GetInfo(ctx).Attempt, earlierDetails(ctx))
	if deadline, ok := ctx.Deadline(); ok {
		h.rotateAt = deadline.Add(-rotateBefore)
	}
	e := h.host(ctx, h.heartbeater(ctx))
	if e.Cancelled {
		return nil, temporal.NewCanceledError()
	}
	return instanceOutcome(in, h.attempt, e)
}

// earlierDetails is what the activity's attempt before this one last said of
// itself.
func earlierDetails(ctx context.Context) eventsources.HeartbeatDetails {
	if !activity.HasHeartbeatDetails(ctx) {
		return eventsources.HeartbeatDetails{}
	}
	var raw []byte
	err := activity.GetHeartbeatDetails(ctx, &raw)
	d, read := eventsources.ReadHeartbeatDetails(raw)
	if err != nil || !read {
		activity.GetLogger(ctx).Warn("what this activity's attempt before this one last said is not read; how long it waited is counted from now", "error", err)
		return eventsources.HeartbeatDetails{}
	}
	return d
}

// instanceHost is one attempt of one instance.
type instanceHost struct {
	a       *Activities
	in      *orkstackv1.HostEventSourceInput
	attempt int32
	taken   time.Time
	// rotateAt is when its activity's bound makes it stop, none if it has none.
	rotateAt time.Time
	door     *sourcehost.DoorClient
	log      *instanceLog

	mu           sync.Mutex
	judgment     sourcehost.Judgment
	phase        string
	endedIn      string
	refusedSince time.Time
	spec         *eventsources.ServiceSpec
	judge        *sourcehost.Judge
	instance     *sourcehost.Instance
	started      time.Time
	readyAt      time.Time
	readyFor     time.Duration
	exitCode     *int32
	// laneSaid is when a lane's failure of each kind was last said.
	laneSaid map[string]time.Time
}

func (a *Activities) newInstanceHost(in *orkstackv1.HostEventSourceInput, attempt int32, earlier eventsources.HeartbeatDetails) *instanceHost {
	unavailable := time.Duration(in.GetConfirmUnavailableMaxSeconds()) * time.Second
	if unavailable <= 0 {
		unavailable = confirmUnavailableMax
	}
	now := time.Now()
	h := &instanceHost{a: a, in: in, attempt: attempt, taken: now, phase: phaseStart, laneSaid: map[string]time.Time{},
		judgment: sourcehost.Judgment{Started: now, WaitMax: a.sourceHost.ConfirmWaitMax, UnavailableMax: unavailable, HeldSince: earlier.HeldSince}}
	if earlier.Epoch != in.GetEpoch() {
		// Not this activity's.
		h.judgment.HeldSince = time.Time{}
	}
	h.door = &sourcehost.DoorClient{Conn: a.hosting.Conn, Now: time.Now, MaxDeliveries: h.maxDeliveries,
		Instance: &orkstackv1.InstanceRef{Namespace: in.GetNamespace(), Name: in.GetName(), EventSourceId: in.GetEventSourceId(),
			Index: in.GetIndex(), Epoch: in.GetEpoch(), Attempt: attempt, ServiceDigest: in.GetServiceDigest()}}
	return h
}

func (h *instanceHost) maxDeliveries() int {
	if in := h.running(); in != nil {
		n, _ := in.DeliveryLimit()
		return n
	}
	return 0
}

func (h *instanceHost) running() *sourcehost.Instance {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.instance
}

func (h *instanceHost) enter(phase string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.phase = phase
	if phase != phaseStop {
		h.endedIn = phase
	}
}

// ref names the attempt's objects.
func (h *instanceHost) ref() artifacts.InstanceAttempt {
	return artifacts.InstanceAttempt{SourceID: h.in.GetEventSourceId(), Index: int(h.in.GetIndex()), Epoch: h.in.GetEpoch(), Attempt: int(h.attempt)}
}

// heartbeater is the attempt's heartbeat loop, whose detail is the instance's own.
func (h *instanceHost) heartbeater(ctx context.Context) *Heartbeater {
	if made := h.a.dispatcher.heartbeaters; made != nil {
		return made(ctx)
	}
	b := NewHeartbeater(ctx)
	info, beats := activity.GetInfo(ctx), activity.GetClient(ctx)
	b.send = func(call context.Context, _ HeartbeatProgress) error {
		return beats.RecordActivityHeartbeatWithOptions(call, client.RecordActivityHeartbeatOptions{TaskToken: info.TaskToken, Details: []any{h.details()},
			Namespace: info.Namespace, WorkflowID: info.WorkflowExecution.ID, ActivityType: info.ActivityType.Name,
			WorkflowType: info.WorkflowType.Name, TaskQueue: info.TaskQueue})
	}
	b.deliver = func(call context.Context, _ HeartbeatProgress) { activity.RecordHeartbeat(call, h.details()) }
	return b
}

// details is what the attempt says of itself now.
func (h *instanceHost) details() []byte {
	// Asked without the lock, which what it shows is held back for needs.
	var s sourcehost.Snapshot
	in := h.running()
	if in != nil {
		s = in.Snapshot()
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	d := eventsources.HeartbeatDetails{Epoch: h.in.GetEpoch(), Phase: h.phase, State: sourcehost.StateStarting, Since: h.taken,
		ConfirmedAt: h.judgment.AcceptedAt(), RefusedSince: h.refusedSince}
	d.Held, d.HeldSince = h.judgment.Held()
	if d.HeldSince.IsZero() {
		// Not judged yet: what an earlier attempt waited on stands.
		d.HeldSince = h.judgment.HeldSince
	}
	if h.spec != nil {
		d.Image = h.spec.Image
	}
	if h.judge != nil {
		d.Judging = h.judge.Revision()
	}
	if in != nil {
		d.State, d.Reason, d.Since = s.State, s.Reason, s.Since
		d.Lanes, d.Broker, d.Warnings, d.LastAcceptedAt, d.LastPollOkAt = s.Lanes, s.Broker, s.Warnings, s.LastAcceptedAt, s.LastPollOkAt
	}
	return d.Encode()
}

// host runs the attempt and says how it ended.
func (h *instanceHost) host(ctx context.Context, beat *Heartbeater) instanceEnding {
	attempt, end := context.WithCancelCause(ctx)
	defer end(nil)
	// Registered before this Orker's own state is asked: what changes it evicts
	// the ones it finds.
	leave := h.a.services.host(&hostedInstance{label: h.ref().SourceID, end: end})
	defer leave()
	if f := h.mayTake(); f != nil {
		h.logger().Info("an event source's instance was handed back before it started", "reason", f.Reason, "detail", f.Detail)
		return instanceEnding{instanceFailure: *f}
	}
	if h.a.dispatcher.HeartbeatInterval > 0 {
		beat.interval = h.a.dispatcher.HeartbeatInterval
	}
	beat.orkerID, beat.onLost, beat.logger = h.a.dispatcher.OrkerID, end, h.logger()
	// Its loop outlives the attempt's context: an instance is stopped under it.
	beat.Start(context.WithoutCancel(ctx))
	defer beat.Stop()
	if err := beat.AwaitFirstAnswer(attempt); err != nil {
		return h.endedBy(attempt)
	}
	e := h.run(attempt)
	h.a.services.attemptEnded(e.Reason, h.a.hostPool())
	h.enter(phaseStop)
	h.record(ctx, e)
	level := slog.LevelInfo
	if e.failed() {
		level = slog.LevelWarn
	}
	h.logger().Log(ctx, level, "an event source's instance ended", "reason", e.Reason, "cause", e.Cause, "detail", e.Detail, "rotated", e.Rotated, "cancelled", e.Cancelled)
	return e
}

// failed reports an end nobody asked for.
func (e instanceEnding) failed() bool {
	row, ok := eventsources.LookupInstanceEnd(e.Reason)
	return e.Reason != "" && (!ok || row.Retried == eventsources.RetryBackoff || row.Completes != orkstackv1.InstanceEnd_INSTANCE_END_STOPPED && row.Retried == eventsources.RetryNone)
}

func (h *instanceHost) logger() *slog.Logger {
	return h.a.dispatcher.logger().With("namespace", h.in.GetNamespace(), "event_source", h.in.GetName(), "event_source_id", h.in.GetEventSourceId(),
		"instance", h.in.GetIndex(), "epoch", h.in.GetEpoch(), "attempt", h.attempt)
}

// endedBy is how the end of its context ends the attempt.
func (h *instanceHost) endedBy(ctx context.Context) instanceEnding {
	cause := context.Cause(ctx)
	if e, ok := errors.AsType[*evicted](cause); ok {
		return instanceEnding{instanceFailure: instanceFailure{Reason: e.reason, Detail: e.detail}}
	}
	if _, lost := errors.AsType[*heartbeatUnanswered](cause); lost {
		// The cluster has given it to another by now, or is about to: what is
		// said here is heard by nobody.
		return instanceEnding{instanceFailure: *instanceFail(eventsources.ReasonServiceHostStalled, "%v", cause)}
	}
	switch {
	case errors.Is(cause, worker.ErrWorkerShutdown):
		return instanceEnding{instanceFailure: *instanceFail(eventsources.ReasonOrkerStopped, "this Orker is stopping")}
	case errors.Is(cause, context.DeadlineExceeded):
		return instanceEnding{Rotated: true}
	}
	return instanceEnding{Cancelled: true}
}

// judged is how the judgment ends the attempt.
func judged(e sourcehost.JudgmentEnd) instanceEnding {
	out := instanceEnding{instanceFailure: instanceFailure{Reason: e.Reason}, Unread: e.Unread}
	if e.Held != "" {
		out.Detail = "the engine's judgment waited on " + e.Held
	}
	return out
}

// run takes the attempt through its phases.
func (h *instanceHost) run(ctx context.Context) instanceEnding {
	failed := func(f *instanceFailure) instanceEnding {
		if ctx.Err() != nil && f.Reason != eventsources.ReasonServiceStopStuck {
			return h.endedBy(ctx)
		}
		return instanceEnding{instanceFailure: *f}
	}
	var first sourcehost.Confirmed
	for {
		// A pull can take longer than a confirmation holds: what it was given is
		// then asked for again, and its image is on this host.
		var e instanceEnding
		var ok bool
		h.enter(phaseConfirm)
		if first, e, ok = h.firstConfirmation(ctx); !ok {
			return e
		}
		confirmed := time.Now()
		if f := h.prepare(ctx, first); f != nil {
			return failed(f)
		}
		if time.Since(confirmed) < sourcehost.ConfirmEvery {
			break
		}
		h.mu.Lock()
		h.judgment = sourcehost.Judgment{Started: time.Now(), WaitMax: h.judgment.WaitMax, UnavailableMax: h.judgment.UnavailableMax}
		h.mu.Unlock()
	}
	spec := first.Service
	h.enter(phaseWorkspace)
	serviceContext, err := servicehost.BuildContext(*spec, h.a.hosting.Deployment, int(h.in.GetIndex()), int(h.in.GetInstances()), instanceRun(h.in.GetEpoch(), h.attempt))
	if err != nil {
		return failed(instanceFail(eventsources.ReasonServiceHostStalled, "its context: %v", err))
	}
	h.log = newInstanceLog(context.WithoutCancel(ctx), h.a.hosting.Logs, h.ref(), h.a.dispatcher.LogOpts, h.a.sourceHost.LogBytesPerHour, func(err error) {
		h.logger().Warn("an instance's log could not be stored", "error", err)
	})
	defer h.log.Close()
	c, f := h.a.startService(ctx, serviceStart{name: instanceName(h.ref().SourceID, h.in.GetIndex(), h.in.GetEpoch(), h.attempt), spec: spec,
		context: serviceContext, image: spec.Image, log: h.log, openAnswer: h.a.openAnswer()})
	if f != nil {
		return failed(f)
	}
	h.enter(phaseRun)
	instance := h.newInstance(c, first)
	c.users = instance.Finished()
	e := h.converse(ctx, c, instance)
	h.enter(phaseStop)
	stuck := c.stop(context.WithoutCancel(ctx), spec.Limits.StopSeconds)
	h.mu.Lock()
	h.exitCode = c.exitCode()
	h.mu.Unlock()
	if stuck != nil {
		// What ended it is in the log; the container that is there is what its
		// source must go on without.
		h.logger().Error("an instance's container could not be stopped", "ended", e.Reason, "detail", e.Detail)
		return instanceEnding{instanceFailure: *stuck}
	}
	return e
}

// prepare takes what a confirmation gave to where its container can start: this
// Orker's own checks of it, and its image.
func (h *instanceHost) prepare(ctx context.Context, first sourcehost.Confirmed) *instanceFailure {
	spec := first.Service
	h.mu.Lock()
	h.spec = spec
	h.mu.Unlock()
	h.enter(phasePolicy)
	if f := h.a.checkInstancePolicy(ctx, spec, first.ImagePolicy); f != nil {
		return f
	}
	if asked, reserved := spec.Resources, h.in.GetResources(); asked.CPU != reserved.GetCpu() || asked.Memory != reserved.GetMemory() {
		// What it holds of this Orker is what its activity asked for.
		f := instanceFail(eventsources.ReasonEngineUnavailable, "its spec asks for %s, %s, and its activity reserved %s, %s",
			asked.CPU, asked.Memory, reserved.GetCpu(), reserved.GetMemory())
		f.Cause = ReasonResourceAdmissionMismatch
		return f
	}
	h.enter(phaseImage)
	if _, err := h.a.dispatcher.Runtime.Pull(ctx, spec.Image); err != nil {
		return instanceFail(eventsources.ReasonImagePullFailed, "pull %s: %v", spec.Image, err)
	}
	return h.readLabels(ctx, spec)
}

// readLabels reads what the image says it implements, which the container is run
// under: a digest's labels are its own, and the spec's versions were read from them.
func (h *instanceHost) readLabels(ctx context.Context, spec *eventsources.ServiceSpec) *instanceFailure {
	labels := map[string]string{}
	for _, key := range []string{eventsources.ContractLabel, eventsources.ProfileLabel} {
		value, ok, err := h.a.dispatcher.Runtime.ImageLabel(ctx, spec.Image, key)
		if err != nil {
			return instanceFail(eventsources.ReasonServiceHostStalled, "read %s of its image: %v", key, err)
		}
		if ok {
			labels[key] = value
		}
	}
	if _, err := eventsources.ReadServiceLabels(map[string]string{eventsources.ContractLabel: labels[eventsources.ContractLabel],
		eventsources.ProfileLabel: eventsources.ProfileEventSource + "/1"}); err != nil {
		return instanceFail(eventsources.ReasonServiceContractUnknown, "%v", err)
	}
	img, err := eventsources.ReadServiceLabels(labels)
	if err != nil {
		return instanceFail(eventsources.ReasonServiceProfileUnknown, "%v", err)
	}
	spec.Contracts, spec.Profiles = img.Contracts, img.Profiles
	return nil
}

// openAnswer opens a started container's answer filesystem.
func (a *Activities) openAnswer() func(int, string) (*servicehost.AnswerDir, error) {
	if a.answers != nil {
		return a.answers
	}
	return servicehost.OpenContainerAnswer
}

// firstConfirmation asks the engine whether the attempt may run, until it answers
// that or its judgment ends the wait.
func (h *instanceHost) firstConfirmation(ctx context.Context) (sourcehost.Confirmed, instanceEnding, bool) {
	for wait := time.Second; ; wait = min(2*wait, confirmAgain) {
		c, err := h.door.ConfirmInstance(ctx, 0, true)
		if ctx.Err() != nil {
			return c, h.endedBy(ctx), false
		}
		if e, over := h.observe(c, err); over {
			return c, judged(e), false
		}
		if err == nil && c.Kind == sourcehost.ConfirmAccepted {
			return c, instanceEnding{}, true
		}
		select {
		case <-ctx.Done():
			return c, h.endedBy(ctx), false
		case <-time.After(wait):
		}
	}
}

// observe takes a confirmation's answer into the attempt's judgment.
func (h *instanceHost) observe(c sourcehost.Confirmed, err error) (sourcehost.JudgmentEnd, bool) {
	if err != nil {
		h.logger().Warn("an instance's confirmation got no answer", "error", err)
	}
	if err == nil && c.Kind == sourcehost.ConfirmAccepted {
		h.a.services.setPolicy(c.ImagePolicy)
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	h.judgment.Observe(time.Now(), c, err)
	return h.judgment.End(time.Now())
}

// over is how the judgment ends the attempt now.
func (h *instanceHost) over() (sourcehost.JudgmentEnd, bool) {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.judgment.End(time.Now())
}

// held is what this Orker holds the instance back for.
func (h *instanceHost) held() string {
	if _, outage := h.a.cordon.Causes(); outage != nil {
		return "outage"
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.judgment.Waiting(time.Now()) {
		return "outage"
	}
	return ""
}

// newInstance is the conversation with a started container.
func (h *instanceHost) newInstance(c *serviceContainer, first sourcehost.Confirmed) *sourcehost.Instance {
	spec := first.Service
	atMostOnce := spec.Delivery == eventsources.DeliveryWord(orkstackv1.DeliveryClass_DELIVERY_CLASS_AT_MOST_ONCE)
	instance := &sourcehost.Instance{Door: h.door, Steps: c.answer, Asks: c.ask, Now: time.Now, Started: c.started, Hold: h.held,
		Config: sourcehost.InstanceConfig{Limits: spec.Limits, Park: time.Duration(eventsources.ParkSeconds(spec.Limits.HoldSeconds)) * time.Second, AtMostOnce: atMostOnce}}
	if h.attempt == 1 {
		instance.Config.Trial = h.in.GetTrial()
	}
	records := &sourcehost.Records{Store: h.a.hosting.Records, Instance: h.door.Instance, OrkerID: h.a.dispatcher.OrkerID, Now: time.Now,
		Provider: func() string { return instance.Snapshot().Provider }, Limit: instance.DeliveryLimit}
	judge := sourcehost.NewJudge(first.Source, first.Revision, h.door, records, time.Now)
	judge.MaxMessageBytes, judge.AtMostOnce = spec.Limits.MaxBodyBytes, atMostOnce
	judge.Ref = sourcehost.SourceRef{ID: spec.Source.ID, Name: spec.Source.Name, Image: spec.Image}
	instance.Judge = judge
	seconds := func(n int) time.Duration { return time.Duration(n) * time.Second }
	for k := range int(spec.Concurrency) {
		instance.Drivers = append(instance.Drivers, &sourcehost.LaneDriver{Index: k, Steps: c.answer, Asks: c.ask, Judge: judge, Recorder: records.Lane(k), Now: time.Now,
			Parse: h.a.services.parseShare(h.a.sourceHost.ParseBytes), Failed: h.laneFailed,
			Limits: sourcehost.LaneLimits{PollWait: seconds(spec.Limits.PollWaitSeconds), Answer: seconds(spec.Limits.AnswerSeconds), Settle: seconds(spec.Limits.SettleSeconds),
				Hold: seconds(spec.Limits.HoldSeconds), LeaseSeconds: leaseSeconds, MaxBodyBytes: spec.Limits.MaxBodyBytes}})
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	h.instance, h.judge, h.started = instance, judge, c.started
	return instance
}

// laneFailed says a failure a lane went on from.
func (h *instanceHost) laneFailed(kind string, err error) {
	if !h.due(kind) {
		return
	}
	h.logger().Warn("an instance's lane; the same of this instance is said once a minute", "kind", kind, "error", err)
	h.log.Orker("%s: %v", kind, errorKind(err))
}

// due reports whether a lane's failure of a kind is said now.
func (h *instanceHost) due(kind string) bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	if now := time.Now(); now.Sub(h.laneSaid[kind]) >= time.Minute {
		h.laneSaid[kind] = now
		return true
	}
	return false
}

// parseShare is this Orker's one share of the bodies being read.
func (s *serviceHostState) parseShare(total int64) *sourcehost.ParseShare {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.parse == nil {
		s.parse = sourcehost.NewParseShare(total)
	}
	return s.parse
}

// converse runs the conversation until it ends or the attempt does, and returns
// once nothing of it goes on but a lane that hangs.
func (h *instanceHost) converse(ctx context.Context, c *serviceContainer, instance *sourcehost.Instance) instanceEnding {
	talking, hangUp := context.WithCancel(context.WithoutCancel(ctx))
	defer hangUp()
	ended := make(chan sourcehost.Ending, 1)
	go func() { ended <- instance.Run(talking) }()
	go func() {
		select {
		case <-c.exited:
			instance.Exited(c.ending(talking.Err() != nil, !instance.Snapshot().Reported))
		case <-instance.Finished():
		}
	}()
	again := make(chan struct{}, 1)
	var wg sync.WaitGroup
	defer wg.Wait()
	watched, stopWatching := context.WithCancel(ctx)
	defer stopWatching()
	refused := make(chan *instanceFailure, 1)
	wg.Go(func() { h.confirmations(watched, again, refused) })
	reports := h.reports(watched, again, &wg)
	tick := time.NewTicker(instanceTick)
	defer tick.Stop()
	// stop ends the conversation and says how, where it did not end of itself
	// meanwhile.
	stop := func(how instanceEnding) instanceEnding {
		hangUp()
		select {
		case e := <-ended:
			// What trying again cannot change is said whenever it is found. Any
			// other failure of what is being stopped is not why it ended.
			if row, _ := eventsources.LookupInstanceEnd(e.Reason); row.Completes == orkstackv1.InstanceEnd_INSTANCE_END_UNCHANGEABLE {
				return h.said(e)
			}
			if e.Detail != "" {
				h.logger().Info("what an event source's instance ended with as it was stopped", "reason", e.Reason, "detail", e.Detail, "stop", e.Stop, "paused", e.Paused)
			}
		case <-time.After(instanceEnds):
			h.logger().Error("an instance's own loop did not end; its container is stopped without it")
		}
		how.ReadyFor = h.ready(instance)
		return how
	}
	for {
		select {
		case e := <-ended:
			return h.said(e)
		case <-ctx.Done():
			return stop(h.endedBy(ctx))
		case f := <-refused:
			return stop(instanceEnding{instanceFailure: *f})
		case <-tick.C:
		}
		if e, over := h.over(); over {
			return stop(judged(e))
		}
		if what := h.door.NotPermitted(); what != "" {
			return stop(instanceEnding{instanceFailure: *instanceFail(eventsources.ReasonHostNotPermitted, "%s", what)})
		}
		now, s := time.Now(), instance.Snapshot()
		h.ready(instance)
		reports(now, s)
		switch {
		case !s.Checked.IsZero() && now.Sub(s.Checked) > loopStalledAfter, s.Checked.IsZero() && now.Sub(c.started) > loopStalledAfter:
			return stop(instanceEnding{instanceFailure: *instanceFail(eventsources.ReasonServiceHostStalled, "this Orker's own loop has not looked at its lanes since %s", s.Checked)})
		case h.late(now, s):
			return stop(instanceEnding{instanceFailure: *instanceFail(eventsources.ReasonServiceNotReadyInTime, "it said what it was within %d s and never became ready", h.in.GetReadyWithinSeconds())})
		case !h.rotateAt.IsZero() && !now.Before(h.rotateAt):
			return stop(instanceEnding{Rotated: true})
		}
	}
}

// late reports a replacement on trial that ran as long as it may with a status and
// without becoming ready.
func (h *instanceHost) late(now time.Time, s sourcehost.Snapshot) bool {
	within := time.Duration(h.in.GetReadyWithinSeconds()) * time.Second
	h.mu.Lock()
	defer h.mu.Unlock()
	return within > 0 && s.Reported && h.readyAt.IsZero() && now.Sub(h.started) >= within
}

// ready keeps since when, and for how long at most, the instance has been ready.
func (h *instanceHost) ready(instance *sourcehost.Instance) time.Duration {
	s := instance.Snapshot()
	h.mu.Lock()
	defer h.mu.Unlock()
	if s.ReadySince.IsZero() {
		return h.readyFor
	}
	if h.readyAt.IsZero() {
		h.readyAt = s.ReadySince
	}
	h.readyFor = max(h.readyFor, time.Since(s.ReadySince))
	return h.readyFor
}

// said is how the instance's own ending ends the attempt.
func (h *instanceHost) said(e sourcehost.Ending) instanceEnding {
	out := instanceEnding{instanceFailure: instanceFailure{Reason: e.Reason, Detail: e.Detail}, ReadyFor: h.ready(h.running())}
	switch {
	case e.Reason != "":
	case e.Stop != "":
		out.Reason = e.Stop
		if e.Paused != "" {
			out.Detail = e.Detail + ": " + e.Paused
		}
	default:
		// Nobody asked it to end, and it says nothing of why.
		out.instanceFailure = *instanceFail(eventsources.ReasonServiceExited, "%s", e.Detail)
	}
	return out
}

// confirmations confirms the running instance every five minutes, sooner where it
// was not accepted or it is asked again, and takes the judging spec and the image
// policy each answers with.
func (h *instanceHost) confirmations(ctx context.Context, again <-chan struct{}, refused chan<- *instanceFailure) {
	h.mu.Lock()
	wait := max(sourcehost.ConfirmEvery-time.Since(h.judgment.AcceptedAt()), 0)
	h.mu.Unlock()
	for {
		select {
		case <-ctx.Done():
			return
		case <-again:
		case <-time.After(wait):
		}
		c, err := h.door.ConfirmInstance(ctx, h.judge.Revision(), false)
		if ctx.Err() != nil {
			return
		}
		h.observe(c, err)
		if wait = confirmAgain; err != nil || c.Kind != sourcehost.ConfirmAccepted {
			continue
		}
		wait = sourcehost.ConfirmEvery
		if c.Source != nil && c.Revision > h.judge.Revision() {
			h.judge.Swap(c.Source, c.Revision, time.Now())
		}
		switch f := h.a.dispatcher.checkInstanceImage(ctx, h.spec, c.ImagePolicy); {
		case f == nil || ctx.Err() != nil:
		case f.Reason == eventsources.ReasonImageNotPermitted:
			refused <- f
			return
		default:
			h.logger().Warn("an instance's image could not be judged again; it runs on", "detail", f.Detail)
		}
	}
}

// reports tells the engine when the instance is first ready, when its first check
// says its configuration is wrong, and when it has been healthy for ten minutes,
// each once, until it is answered.
func (h *instanceHost) reports(ctx context.Context, again chan<- struct{}, wg *sync.WaitGroup) func(time.Time, sourcehost.Snapshot) {
	told := map[orkstackv1.InstanceReport]bool{}
	tell := func(what orkstackv1.InstanceReport, reason string) {
		if told[what] {
			return
		}
		told[what] = true
		wg.Go(func() {
			for wait := time.Second; ; wait = min(2*wait, confirmAgain) {
				stop, err := h.door.Report(ctx, what, reason)
				if err != nil && ctx.Err() == nil && wait == time.Second {
					h.logger().Warn("an instance's report got no answer; it is made again until it does", "report", what.String(), "error", err)
				}
				if err == nil {
					if stop {
						// Why, a confirmation says.
						select {
						case again <- struct{}{}:
						default:
						}
					}
					return
				}
				select {
				case <-ctx.Done():
					return
				case <-time.After(wait):
				}
			}
		})
	}
	return func(now time.Time, s sourcehost.Snapshot) {
		switch {
		case s.State == sourcehost.StateInvalid && !told[orkstackv1.InstanceReport_INSTANCE_REPORT_READY]:
			tell(orkstackv1.InstanceReport_INSTANCE_REPORT_INVALID, s.Reason)
		case s.ReadySince.IsZero():
		default:
			tell(orkstackv1.InstanceReport_INSTANCE_REPORT_READY, "")
			if now.Sub(s.ReadySince) >= healthyAfter {
				tell(orkstackv1.InstanceReport_INSTANCE_REPORT_HEALTHY, "")
			}
		}
	}
}

// record leaves the attempt's record.
func (h *instanceHost) record(ctx context.Context, e instanceEnding) {
	h.mu.Lock()
	r := eventsources.AttemptRecord{Namespace: h.in.GetNamespace(), Name: h.in.GetName(), ID: h.in.GetEventSourceId(), Index: h.in.GetIndex(),
		Epoch: h.in.GetEpoch(), Attempt: h.attempt, OrkerID: h.a.dispatcher.OrkerID, BuildID: h.a.dispatcher.BuildID, Pool: h.a.pool,
		Service: h.in.GetServiceDigest(), Phase: h.endedIn, TakenAt: h.taken, StartedAt: h.started, ReadyAt: h.readyAt, EndedAt: time.Now(),
		Reason: e.Reason, Cause: eventsources.InstanceCause(e.Cause), ExitCode: h.exitCode}
	r.Held, r.HeldSince = h.judgment.Held()
	if h.spec != nil {
		r.Image = h.spec.Image
	}
	if h.judge != nil {
		r.Judging = h.judge.Revision()
	}
	h.mu.Unlock()
	if h.log != nil {
		r.LogBytes = h.log.Bytes()
	}
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), recordTimeout)
	defer cancel()
	if err := h.a.hosting.Attempts.PutInstanceAttempt(ctx, h.ref(), r); err != nil {
		h.logger().Warn("an instance's attempt record could not be written", "error", err)
	}
}
