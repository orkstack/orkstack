package orker

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"go.temporal.io/api/serviceerror"
	"go.temporal.io/sdk/activity"
	"google.golang.org/protobuf/types/known/timestamppb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
)

// An Orker that is itself stopped stops what it runs carefully
// (docs/OCI_ACTIVITY_CONTRACT.md §6.3). Every ExecuteTask attempt is entered in
// a registry when it is admitted, at the SDK seam, and moves through stages by
// compare-and-swap:
//
//	admitted    the attempt runs, and has started no container
//	revocable   its container was started
//	committing  it has a result of its own to report: a commit it adopted, a
//	            memo hit it is writing, or a container that ended by itself
//	handingBack the Orker's stop took it back
//
// The stop's sweep (handBack) moves every admitted attempt, and every revocable
// one that is not a pinned member, to handingBack and cancels its work with
// errHandBack; an attempt's own CAS fails once handingBack is set, and the
// sweep's fails once the attempt is committing. So exactly one side decides:
// a committing attempt completes and reports its own result, and a handed-back
// one gets the cancellation sequence, heartbeated throughout, and is reported
// ORKER_STOPPED by the Orker itself, before the process stops its workers.
const (
	stageAdmitted int32 = iota
	stageRevocable
	stageCommitting
	stageHandingBack
)

// errHandBack is the cause an attempt's work is cancelled with when its
// Orker's stop takes it back. It is never a cancel of the task, nor a lost
// heartbeat.
var errHandBack = errors.New("the orker is stopping and hands this attempt back")

// handBackReportDeadline bounds how long a handed-back attempt keeps trying to
// report itself. It is the commit tail's cap: a report the cluster has not
// taken by then is left to the cluster, which times the attempt out once its
// heartbeats stop. The process manager's own stop timeout, or a third signal,
// bounds it sooner.
const handBackReportDeadline = 10 * time.Minute

// reportBackoff is the spacing of a report's retries: from the first to the
// last, then the last again.
var reportBackoff = []time.Duration{time.Second, 2 * time.Second, 5 * time.Second, 10 * time.Second}

// attemptStage is one admitted attempt's entry in the registry.
type attemptStage struct {
	reg    *stopRegistry
	stage  atomic.Int32
	pinned atomic.Bool
	// handBack is done, with errHandBack as its cause, once the sweep took
	// the attempt back. The attempt's work context follows it.
	handBack       context.Context
	cancelHandBack context.CancelCauseFunc
}

// setPinned says the attempt is a pinned member. It is set before the
// attempt's first CAS, so a sweep that sees it revocable sees it pinned.
func (s *attemptStage) setPinned() {
	if s != nil {
		s.pinned.Store(true)
	}
}

// toRevocable is the attempt's CAS once its container started. False means
// the sweep took it back first.
func (s *attemptStage) toRevocable() bool {
	if s == nil {
		return true
	}
	ok := s.stage.CompareAndSwap(stageAdmitted, stageRevocable)
	s.reg.changed()
	return ok
}

// toCommitting is the attempt's CAS once it has a result of its own to
// report, before it writes anything. False means the sweep took it back
// first, and the attempt writes nothing.
func (s *attemptStage) toCommitting() bool {
	if s == nil {
		return true
	}
	ok := s.stage.CompareAndSwap(stageAdmitted, stageCommitting) ||
		s.stage.CompareAndSwap(stageRevocable, stageCommitting)
	s.reg.changed()
	return ok
}

// handedBack reports whether the sweep took the attempt back.
func (s *attemptStage) handedBack() bool {
	return s != nil && s.stage.Load() == stageHandingBack
}

// owed reports whether the stop waits for this attempt before it stops the
// task worker: every unpinned attempt, and every pinned one that is not
// running a container. A running pinned member keeps its old path: the
// worker's Stop ends it, and its pin fails PIN_HOST_LOST.
func (s *attemptStage) owed() bool {
	return !s.pinned.Load() || s.stage.Load() != stageRevocable
}

// stopRegistry is every ExecuteTask attempt this Orker has admitted and not
// yet returned from.
type stopRegistry struct {
	mu       sync.Mutex
	entries  map[*attemptStage]struct{}
	stopping bool
	// change is closed, and replaced, whenever an entry leaves or moves.
	change chan struct{}
}

func newStopRegistry() *stopRegistry {
	return &stopRegistry{entries: map[*attemptStage]struct{}{}, change: make(chan struct{})}
}

// admit enters one attempt. An attempt admitted once the stop has begun is
// taken back at once: it starts nothing.
func (r *stopRegistry) admit() *attemptStage {
	if r == nil {
		return nil
	}
	s := &attemptStage{reg: r}
	s.handBack, s.cancelHandBack = context.WithCancelCause(context.Background())
	r.mu.Lock()
	defer r.mu.Unlock()
	r.entries[s] = struct{}{}
	if r.stopping {
		s.stage.Store(stageHandingBack)
		s.cancelHandBack(errHandBack)
	}
	return s
}

// leave removes an attempt once it has returned.
func (r *stopRegistry) leave(s *attemptStage) {
	if r == nil || s == nil {
		return
	}
	r.mu.Lock()
	delete(r.entries, s)
	r.mu.Unlock()
	s.cancelHandBack(nil)
	r.changed()
}

func (r *stopRegistry) changed() {
	if r == nil {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	close(r.change)
	r.change = make(chan struct{})
}

// handBack is the stop's sweep: from now on every attempt admitted is taken
// back before it starts, and every admitted attempt, and every revocable one
// that is not a pinned member, is taken back now. It returns how many it took.
func (r *stopRegistry) handBack() int {
	if r == nil {
		return 0
	}
	r.mu.Lock()
	r.stopping = true
	var taken []*attemptStage
	for s := range r.entries {
		if s.stage.CompareAndSwap(stageAdmitted, stageHandingBack) ||
			!s.pinned.Load() && s.stage.CompareAndSwap(stageRevocable, stageHandingBack) {
			taken = append(taken, s)
		}
	}
	r.mu.Unlock()
	for _, s := range taken {
		s.cancelHandBack(errHandBack)
	}
	r.changed()
	return len(taken)
}

// owed counts the attempts the stop still waits for, and returns the channel
// that is closed at the next change.
func (r *stopRegistry) owed() (int, <-chan struct{}) {
	r.mu.Lock()
	defer r.mu.Unlock()
	n := 0
	for s := range r.entries {
		if s.owed() {
			n++
		}
	}
	return n, r.change
}

// awaitReported returns once every attempt the stop waits for has returned,
// having reported itself, or when ctx is done.
func (r *stopRegistry) awaitReported(ctx context.Context) error {
	if r == nil {
		return nil
	}
	for {
		n, change := r.owed()
		if n == 0 {
			return nil
		}
		select {
		case <-change:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

type stageKey struct{}

// withStage carries the attempt's registry entry to the dispatcher.
func withStage(ctx context.Context, s *attemptStage) context.Context {
	return context.WithValue(ctx, stageKey{}, s)
}

// stageFrom is the attempt's registry entry, or nil outside an Orker (orkctl
// task run, orkctl activity test), where nothing is ever handed back.
func stageFrom(ctx context.Context) *attemptStage {
	s, _ := ctx.Value(stageKey{}).(*attemptStage)
	return s
}

type startKey struct{}

// handBackWork is the attempt's work context: ctx, also cancelled with
// errHandBack when the stop takes the attempt back. The context the work
// derives from is kept in it, for the one call the sweep must never interrupt:
// a container's start (startContext).
func handBackWork(ctx context.Context, s *attemptStage) (context.Context, func()) {
	if s == nil {
		return ctx, func() {}
	}
	work, cancel := context.WithCancelCause(context.WithValue(ctx, startKey{}, ctx))
	stop := context.AfterFunc(s.handBack, func() { cancel(context.Cause(s.handBack)) })
	return work, func() {
		stop()
		cancel(nil)
	}
}

// startContext is the context a container's start runs on: the attempt's, which
// a cancel of the task or a lost heartbeat still ends, but which the stop's
// sweep never does. A start in flight is left to return, and the attempt's CAS
// then decides.
func startContext(ctx context.Context) context.Context {
	if parent, ok := ctx.Value(startKey{}).(context.Context); ok {
		return parent
	}
	return ctx
}

// completer reports an attempt's outcome to the cluster on the attempt's
// behalf: CompleteActivity by task token.
type completer func(ctx context.Context, result any, err error) error

// activityCompleter is the activity's own completer, or nil outside one.
func activityCompleter(ctx context.Context) completer {
	if !activity.IsActivity(ctx) {
		return nil
	}
	info := activity.GetInfo(ctx)
	client := activity.GetClient(ctx)
	return func(call context.Context, result any, err error) error {
		return client.CompleteActivity(call, info.TaskToken, result, err)
	}
}

// reportSync reports an attempt's outcome itself and returns once the cluster
// has it, or answered that it has nothing to take (NotFound: the attempt was
// already completed, cancelled, or timed out), or the deadline passed. The
// attempt's heartbeats carry on meanwhile, so the cluster does not time it out
// while it is being reported. It reports whether the cluster answered, and
// the last error it gave.
func reportSync(ctx context.Context, complete completer, result any, err error, deadline time.Duration,
	pause func(context.Context, time.Duration) bool) (bool, error) {
	if complete == nil {
		return false, nil
	}
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), deadline)
	defer cancel()
	for i := 0; ; i++ {
		call, done := context.WithTimeout(ctx, controlTimeout)
		last := complete(call, result, err)
		done()
		var notFound *serviceerror.NotFound
		switch {
		case last == nil:
			return true, nil
		case errors.As(last, &notFound):
			return true, last
		}
		if !pause(ctx, reportBackoff[min(i, len(reportBackoff)-1)]) {
			return false, last
		}
	}
}

// sleepCtx waits d, or until ctx is done, and reports whether it waited d.
func sleepCtx(ctx context.Context, d time.Duration) bool {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-timer.C:
		return true
	case <-ctx.Done():
		return false
	}
}

// report sends an attempt's outcome to the cluster on its behalf. sent is
// false where there is no cluster to tell (orkctl task run, a test without
// one), and the caller then returns the outcome as any attempt does; landed
// says the cluster took it, or answered that the attempt had already ended.
func (d *OCIDispatcher) report(
	ctx context.Context, in *orkstackv1.ExecuteTaskInput, result any, err error,
) (sent, landed bool) {
	complete := activityCompleter(ctx)
	if d.completers != nil {
		complete = d.completers(ctx)
	}
	if complete == nil {
		return false, false
	}
	pause := d.reportPause
	if pause == nil {
		pause = sleepCtx
	}
	landed, last := reportSync(ctx, complete, result, err, handBackReportDeadline, pause)
	switch {
	case !landed:
		d.logger().Error("the cluster never took this attempt's report; it times the attempt out once its heartbeats stop",
			"run", in.GetRunId(), "task", in.GetTaskId(), "error", last)
	case last != nil:
		d.logger().Info("the cluster had nothing to take for this attempt's report: it had already ended",
			"run", in.GetRunId(), "task", in.GetTaskId(), "answer", last)
	}
	return true, landed
}

// handBackUnstarted ends an attempt that reached this Orker once its stop had
// begun. It started nothing and leaves nothing: no log, heartbeat, claim,
// record or container. An unpinned one is reported ORKER_STOPPED; a pinned
// member is refused unreported, and its pin fails PIN_HOST_LOST when the
// Orker's worker stops.
func (d *OCIDispatcher) handBackUnstarted(
	ctx context.Context, in *orkstackv1.ExecuteTaskInput, attempt int32,
) (*orkstackv1.ExecuteTaskResult, error) {
	d.logger().Info("an attempt reached this orker as it stopped; handing it back unstarted",
		"run", in.GetRunId(), "task", in.GetTaskId(), "attempt", attempt, "pinned", in.GetPin() != nil)
	if in.GetPin() != nil {
		return nil, activity.ErrResultPending
	}
	failure := Fail(ReasonOrkerStopped,
		"orker %s was stopping when this attempt reached it, so it started nothing and handed the attempt "+
			"back; it runs again on another Orker if its retry policy allows", d.OrkerID)
	if sent, _ := d.report(ctx, in, nil, failure.TemporalError()); !sent {
		return nil, failure.TemporalError()
	}
	return nil, activity.ErrResultPending
}

// handBackStarted ends an attempt the stop's sweep took back after it began:
// its work was cancelled with errHandBack, and a container it started got the
// cancellation sequence while its heartbeats went on. Whatever else it
// collected came from that teardown and is replaced. An unpinned attempt is
// reported ORKER_STOPPED by the Orker itself, then recorded; a pinned member
// (taken back before its container started) is recorded and not reported:
// its pin fails PIN_HOST_LOST.
func (d *OCIDispatcher) handBackStarted(
	ctx context.Context, in *orkstackv1.ExecuteTaskInput, startedAt *timestamppb.Timestamp,
	end func(AttemptOutcome),
) (*orkstackv1.ExecuteTaskResult, error) {
	if in.GetPin() != nil {
		failure := Fail(ReasonOrkerStopped,
			"orker %s was stopping before this pinned member started its container, so it started none; "+
				"its pin fails PIN_HOST_LOST", d.OrkerID).StampStart(startedAt)
		end(AttemptOutcome{Reason: failure.Reason, Err: failure.TemporalError()})
		return nil, activity.ErrResultPending
	}
	failure := Fail(ReasonOrkerStopped,
		"orker %s was stopped while this attempt ran, so it handed the attempt back: SIGTERM to a running "+
			"container, SIGKILL after the task's %ds grace, heartbeating throughout. It runs again on another "+
			"Orker if its retry policy allows", d.OrkerID, graceSecondsOf(in)).StampStart(startedAt)
	// The report comes first and the record after it, so a slow store never
	// delays what the run waits for.
	sent, landed := d.report(ctx, in, nil, failure.TemporalError())
	recorded := failure
	if sent && !landed {
		recorded = Fail(ReasonReportUndelivered,
			"orker %s handed this attempt back as it stopped and could not report it to the cluster within %s; "+
				"the cluster times the attempt out once its heartbeats stop", d.OrkerID, handBackReportDeadline).
			StampStart(startedAt)
	}
	end(AttemptOutcome{Reason: recorded.Reason, Err: recorded.TemporalError()})
	if !sent {
		return nil, failure.TemporalError()
	}
	return nil, activity.ErrResultPending
}
