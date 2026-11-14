package orker

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"go.temporal.io/api/serviceerror"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/worker"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/testsupport/must"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// TestAttemptStage_TheSweepTakesBackWhatHasNoResultOfItsOwn: the stop's sweep
// takes back every admitted attempt and every running unpinned one, cancelling
// its work with errHandBack; it leaves a committing attempt and a running
// pinned member alone.
func TestAttemptStage_TheSweepTakesBackWhatHasNoResultOfItsOwn(t *testing.T) {
	r := newStopRegistry()
	admitted := r.admit()
	running := r.admit()
	running.toRevocable()
	committing := r.admit()
	committing.toCommitting()
	pinned := r.admit()
	pinned.setPinned()
	pinned.toRevocable()
	pinnedAdmitted := r.admit()
	pinnedAdmitted.setPinned()

	if got := r.handBack(); got != 3 {
		t.Fatalf("handBack took %d attempts, want 3 (admitted, running unpinned, admitted pinned)", got)
	}
	for name, s := range map[string]*attemptStage{"admitted": admitted, "running": running, "pinned admitted": pinnedAdmitted} {
		if !s.handedBack() {
			t.Errorf("%s: not handed back", name)
		}
		if cause := context.Cause(s.handBack); !errors.Is(cause, errHandBack) {
			t.Errorf("%s: hand-back cause = %v, want errHandBack", name, cause)
		}
	}
	for name, s := range map[string]*attemptStage{"committing": committing, "running pinned": pinned} {
		if s.handedBack() || s.handBack.Err() != nil {
			t.Errorf("%s: handed back, want it left alone", name)
		}
	}
}

// TestAttemptStage_EveryTransitionRechecksHandingBack: once the sweep took an
// attempt back, none of its own transitions succeeds; once an attempt is
// committing, the sweep never takes it.
func TestAttemptStage_EveryTransitionRechecksHandingBack(t *testing.T) {
	r := newStopRegistry()
	a, b := r.admit(), r.admit()
	b.toRevocable()
	r.handBack()
	if a.toRevocable() {
		t.Error("an attempt taken back while admitted moved to revocable")
	}
	if a.toCommitting() || b.toCommitting() {
		t.Error("an attempt taken back moved to committing")
	}

	r = newStopRegistry()
	c := r.admit()
	if !c.toCommitting() {
		t.Fatal("an admitted attempt could not move to committing")
	}
	r.handBack()
	if c.handedBack() {
		t.Error("the sweep took back a committing attempt")
	}
}

// TestAttemptStage_AnArrivalAfterTheStopIsTakenBackAtOnce: an attempt admitted
// once the stop began is taken back before it starts.
func TestAttemptStage_AnArrivalAfterTheStopIsTakenBackAtOnce(t *testing.T) {
	r := newStopRegistry()
	r.handBack()
	s := r.admit()
	if !s.handedBack() || !errors.Is(context.Cause(s.handBack), errHandBack) {
		t.Fatal("an attempt admitted after the stop began was not taken back")
	}
}

// TestStop_AwaitHandedBackWaitsForEveryUnpinnedAndCommittingAttempt: the stop
// waits for every unpinned attempt and every committing one to return, and not
// for a running pinned member, which the task worker's Stop ends.
func TestStop_AwaitHandedBackWaitsForEveryUnpinnedAndCommittingAttempt(t *testing.T) {
	r := newStopRegistry()
	unpinned := r.admit()
	committingPinned := r.admit()
	committingPinned.setPinned()
	committingPinned.toCommitting()
	runningPinned := r.admit()
	runningPinned.setPinned()
	runningPinned.toRevocable()
	r.handBack()

	done := make(chan error, 1)
	go func() { done <- r.awaitReported(context.Background()) }()
	r.leave(unpinned)
	select {
	case <-done:
		t.Fatal("the stop stopped waiting while a committing pinned member was still reporting")
	default:
	}
	r.leave(committingPinned)
	select {
	case err := <-done:
		must.OKf(t, err, "awaitReported")
	case <-time.After(5 * time.Second):
		t.Fatal("the stop kept waiting for a running pinned member")
	}
	if n, _ := r.owed(); n != 0 {
		t.Errorf("owed = %d, want 0 with only a running pinned member left", n)
	}
}

// reportedReason reports whether err is a retryable activity failure whose
// detail names reason, as the engine reads it.
func reportedReason(err error, reason string) bool {
	var app *temporal.ApplicationError
	if !errors.As(err, &app) || app.NonRetryable() {
		return false
	}
	var detail *orkstackv1.ActivityFailureDetail
	return app.Details(&detail) == nil && detail.GetReasonCode() == reason
}

// fakeCompleter records each report and answers with answers in turn, then nil.
type fakeCompleter struct {
	mu      sync.Mutex
	answers []error
	reports []error
	results []any
	beats   *atomic.Int64
}

func (c *fakeCompleter) complete(_ context.Context, result any, err error) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.reports = append(c.reports, err)
	c.results = append(c.results, result)
	if len(c.answers) == 0 {
		return nil
	}
	answer := c.answers[0]
	c.answers = c.answers[1:]
	return answer
}

// stoppingDispatcher is a dispatcher whose heartbeats a test counts and whose
// reports it reads.
func stoppingDispatcher(t *testing.T, runtime ContainerRuntime, attempts *artifacts.MemoryAttemptStore) (*OCIDispatcher, *fakeCompleter) {
	t.Helper()
	d := newAttemptDispatcher(t, runtime, attempts)
	beats := &atomic.Int64{}
	reports := &fakeCompleter{beats: beats}
	d.heartbeaters = func(context.Context) *Heartbeater {
		return &Heartbeater{
			interval:   2 * time.Millisecond,
			ackTimeout: time.Minute,
			send: func(context.Context, HeartbeatProgress) error {
				beats.Add(1)
				return nil
			},
		}
	}
	d.completers = func(context.Context) completer { return reports.complete }
	d.reportPause = func(context.Context, time.Duration) bool { return true }
	return d, reports
}

// executeSweptRunning runs Execute as an admitted attempt and runs the stop's
// sweep once its container has started.
func executeSweptRunning(d *OCIDispatcher, runtime *heldRuntime, in *orkstackv1.ExecuteTaskInput) (*orkstackv1.ExecuteTaskResult, error) {
	r := newStopRegistry()
	stage := r.admit()
	defer r.leave(stage)
	go func() {
		<-runtime.started
		r.handBack()
	}()
	return d.Execute(withStage(context.Background(), stage), in)
}

// TestShutdown_AHandedBackAttemptReportsORKER_STOPPEDItself: a running
// unpinned attempt the stop takes back gets the cancellation sequence (SIGTERM
// first), heartbeats throughout, and the Orker reports it ORKER_STOPPED — a
// retryable InfrastructureError — itself, then records it, and Execute
// returns ErrResultPending so the SDK sends nothing more.
func TestShutdown_AHandedBackAttemptReportsORKER_STOPPEDItself(t *testing.T) {
	attempts := artifacts.NewMemoryAttemptStore()
	held := newHeldRuntime()
	runtime := &beatingTeardown{heldRuntime: held}
	d, reports := stoppingDispatcher(t, runtime, attempts)
	runtime.beats = reports.beats
	in := baseExportInput("handed-back")

	result, err := executeSweptRunning(d, held, in)
	if !errors.Is(err, activity.ErrResultPending) || result != nil {
		t.Fatalf("Execute = (%v, %v), want (nil, ErrResultPending) once the Orker reported it", result, err)
	}
	if runtime.first != SIGTERM {
		t.Errorf("the container's first signal = %v, want SIGTERM", runtime.first)
	}
	if len(reports.reports) != 1 {
		t.Fatalf("reported %d times, want 1", len(reports.reports))
	}
	if !reportedReason(reports.reports[0], ReasonOrkerStopped) {
		t.Fatalf("reported %v, want a retryable ORKER_STOPPED", reports.reports[0])
	}
	if !runtime.beatenThrough.Load() {
		t.Error("no heartbeat went out while the container was being stopped: the attempt stopped beating when it was taken back")
	}
	record := onlyAttempt(t, attempts, in)
	if record.GetError().GetCode() != ReasonOrkerStopped ||
		record.GetOutcome() != orkstackv1.AttemptOutcome_ATTEMPT_OUTCOME_FAILED {
		t.Errorf("record = %v %v, want FAILED ORKER_STOPPED", record.GetOutcome(), record.GetError())
	}
}

// beatingTeardown is a held container that, once signalled, ends only after
// the attempt has sent three more heartbeats, or after five seconds.
type beatingTeardown struct {
	*heldRuntime
	beats         *atomic.Int64
	beatenThrough atomic.Bool
}

func (r *beatingTeardown) Wait(ctx context.Context, id ContainerID) (ExitStatus, error) {
	status, err := r.heldRuntime.Wait(ctx, id)
	from := r.beats.Load()
	deadline := time.Now().Add(5 * time.Second)
	for r.beats.Load() < from+3 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	r.beatenThrough.Store(r.beats.Load() >= from+3)
	return status, err
}

// trapExitZeroRuntime is a container that traps SIGTERM and exits 0, having
// written values.json.
type trapExitZeroRuntime struct{ *heldRuntime }

func (r trapExitZeroRuntime) Wait(ctx context.Context, id ContainerID) (ExitStatus, error) {
	_, _ = r.heldRuntime.Wait(ctx, id)
	return ExitStatus{Code: 0}, nil
}

// TestAttemptStage_SigtermTrapExitZeroIsHandedBack: a workload that traps
// SIGTERM and exits 0 after the sweep is handed back, and nothing it left is
// committed.
func TestAttemptStage_SigtermTrapExitZeroIsHandedBack(t *testing.T) {
	attempts := artifacts.NewMemoryAttemptStore()
	held := newHeldRuntime()
	d, reports := stoppingDispatcher(t, trapExitZeroRuntime{held}, attempts)
	commits := artifacts.NewMemoryCommitStore()
	d.Commits = commits
	in := baseExportInput("trap-exit-zero")
	in.CommitUri = "mem://commits/" + in.GetRunId() + "/" + in.GetTaskId() + "/commit.json"

	_, err := executeSweptRunning(d, held, in)
	if !errors.Is(err, activity.ErrResultPending) {
		t.Fatalf("Execute = %v, want ErrResultPending", err)
	}
	if len(reports.reports) != 1 || !reportedReason(reports.reports[0], ReasonOrkerStopped) {
		t.Fatalf("reports = %v, want one ORKER_STOPPED", reports.reports)
	}
	if _, err := commits.Get(context.Background(), in.GetCommitUri()); !errors.Is(err, artifacts.ErrNotFound) {
		t.Fatalf("a commit was written for a handed-back attempt: Get = %v, want ErrNotFound", err)
	}
}

// TestShutdown_ACommittingAttemptIsNotTakenBack: an attempt whose container
// ended by itself before the sweep reports its own result, through the SDK as
// any attempt does.
func TestShutdown_ACommittingAttemptIsNotTakenBack(t *testing.T) {
	attempts := artifacts.NewMemoryAttemptStore()
	d, reports := stoppingDispatcher(t, &phaseCapturingRuntime{exitCode: 0}, attempts)
	in := baseExportInput("committed-before-sweep")
	r := newStopRegistry()
	stage := r.admit()
	defer r.leave(stage)

	result, err := d.Execute(withStage(context.Background(), stage), in)
	must.OKf(t, err, "Execute")
	r.handBack()
	if result == nil || stage.handedBack() {
		t.Fatalf("result %v, handed back %v: a finished attempt was taken back", result, stage.handedBack())
	}
	if len(reports.reports) != 0 {
		t.Errorf("the Orker reported a finished attempt itself: %v", reports.reports)
	}
}

// TestShutdown_AnArrivalAfterTheStopStartsNothing: an attempt that reaches the
// Orker after its stop began starts nothing — no container, no record — and is
// reported ORKER_STOPPED.
func TestShutdown_AnArrivalAfterTheStopStartsNothing(t *testing.T) {
	attempts := artifacts.NewMemoryAttemptStore()
	runtime := newHeldRuntime()
	d, reports := stoppingDispatcher(t, runtime, attempts)
	in := baseExportInput("after-the-stop")
	r := newStopRegistry()
	r.handBack()
	stage := r.admit()
	defer r.leave(stage)

	_, err := d.Execute(withStage(context.Background(), stage), in)
	if !errors.Is(err, activity.ErrResultPending) {
		t.Fatalf("Execute = %v, want ErrResultPending", err)
	}
	select {
	case <-runtime.started:
		t.Fatal("an attempt that reached a stopping Orker started a container")
	default:
	}
	if len(reports.reports) != 1 || !reportedReason(reports.reports[0], ReasonOrkerStopped) {
		t.Fatalf("reports = %v, want one ORKER_STOPPED", reports.reports)
	}
	records, err := attempts.ListAttempts(context.Background(), in.GetRunId(), in.GetTaskId(), artifacts.NoItem)
	must.OKf(t, err, "ListAttempts")
	if len(records) != 0 {
		t.Errorf("an attempt that started nothing left %d records", len(records))
	}
}

// TestShutdown_APinnedArrivalAfterTheStopIsRefusedUnreported: a pinned member
// that reaches a stopping Orker starts nothing and is not reported: its pin
// fails PIN_HOST_LOST when the worker stops.
func TestShutdown_APinnedArrivalAfterTheStopIsRefusedUnreported(t *testing.T) {
	runtime := newHeldRuntime()
	d, reports := stoppingDispatcher(t, runtime, artifacts.NewMemoryAttemptStore())
	in := baseExportInput("pinned-after-the-stop")
	in.Pin = &orkstackv1.PinContext{Name: "build"}
	r := newStopRegistry()
	r.handBack()
	stage := r.admit()
	defer r.leave(stage)

	if _, err := d.Execute(withStage(context.Background(), stage), in); !errors.Is(err, activity.ErrResultPending) {
		t.Fatalf("Execute = %v, want ErrResultPending", err)
	}
	if len(reports.reports) != 0 {
		t.Errorf("a refused pinned member was reported: %v", reports.reports)
	}
}

// stopAfterCommit is a commit store whose Create, once it has written the
// commit, plays the task worker's Stop: it cancels the attempt with
// worker.ErrWorkerShutdown.
type stopAfterCommit struct {
	artifacts.CommitStore
	stop func()
}

func (s *stopAfterCommit) Create(ctx context.Context, uri string, c *orkstackv1.ActivityCommit) (artifacts.CommitRecord, error) {
	record, err := s.CommitStore.Create(ctx, uri, c)
	s.stop()
	return record, err
}

// TestShutdown_CommittedAttemptReportsItsResult: the worker's Stop cancelling
// an attempt whose commit is written never turns it into a failure: the Orker
// reports its result itself.
func TestShutdown_CommittedAttemptReportsItsResult(t *testing.T) {
	attempts := artifacts.NewMemoryAttemptStore()
	d, reports := stoppingDispatcher(t, &phaseCapturingRuntime{exitCode: 0}, attempts)
	ctx, cancel := context.WithCancelCause(context.Background())
	defer cancel(nil)
	d.Commits = &stopAfterCommit{CommitStore: artifacts.NewMemoryCommitStore(),
		stop: func() { cancel(worker.ErrWorkerShutdown) }}
	in := baseExportInput("committed-then-stopped")
	in.CommitUri = "mem://commits/" + in.GetRunId() + "/" + in.GetTaskId() + "/commit.json"

	_, err := d.Execute(ctx, in)
	if !errors.Is(err, activity.ErrResultPending) {
		t.Fatalf("Execute = %v, want ErrResultPending once the Orker reported the result", err)
	}
	if len(reports.reports) != 1 || reports.reports[0] != nil {
		t.Fatalf("reports = %v, want one success", reports.reports)
	}
	result, ok := reports.results[0].(*orkstackv1.ExecuteTaskResult)
	if !ok || result.GetCommit() == nil {
		t.Fatalf("reported result = %v, want the committed result", reports.results[0])
	}
}

// TestReport_RetriesThenNotFoundIsBenign: a report retries an unanswered
// cluster, and NotFound (the attempt already ended) ends it as answered.
func TestReport_RetriesThenNotFoundIsBenign(t *testing.T) {
	c := &fakeCompleter{answers: []error{errors.New("unavailable"), serviceerror.NewNotFound("gone")}}
	landed, last := reportSync(context.Background(), c.complete, nil, errors.New("x"), time.Minute,
		func(context.Context, time.Duration) bool { return true })
	if !landed || len(c.reports) != 2 {
		t.Fatalf("landed %v after %d tries (last %v), want landed after 2", landed, len(c.reports), last)
	}
}

// TestReport_GivesUpAtDeadlineWithRecord: a report the cluster never takes
// gives up at its deadline, and the record says REPORT_UNDELIVERED.
func TestReport_GivesUpAtDeadlineWithRecord(t *testing.T) {
	attempts := artifacts.NewMemoryAttemptStore()
	runtime := newHeldRuntime()
	d, reports := stoppingDispatcher(t, runtime, attempts)
	reports.answers = []error{errors.New("unavailable"), errors.New("unavailable")}
	tries := 0
	d.reportPause = func(context.Context, time.Duration) bool {
		tries++
		return tries < 2
	}
	in := baseExportInput("undelivered")

	if _, err := executeSweptRunning(d, runtime, in); !errors.Is(err, activity.ErrResultPending) {
		t.Fatalf("Execute = %v, want ErrResultPending", err)
	}
	if got := onlyAttempt(t, attempts, in).GetError().GetCode(); got != ReasonReportUndelivered {
		t.Errorf("record code = %q, want REPORT_UNDELIVERED", got)
	}
}

// TestShutdown_TheWorkersStopDoesNotCutAHandBackShort: the Orker stops its
// task worker right after the sweep, so it polls no more. That Stop cancels
// every activity's context; an unpinned attempt outlives it, still gets the
// cancellation sequence, and still reports itself ORKER_STOPPED.
func TestShutdown_TheWorkersStopDoesNotCutAHandBackShort(t *testing.T) {
	attempts := artifacts.NewMemoryAttemptStore()
	runtime := newHeldRuntime()
	d, reports := stoppingDispatcher(t, runtime, attempts)
	in := baseExportInput("worker-stopped")
	r := newStopRegistry()
	stage := r.admit()
	defer r.leave(stage)
	ctx, cancel := context.WithCancelCause(withStage(context.Background(), stage))
	defer cancel(nil)
	go func() {
		<-runtime.started
		r.handBack()
		cancel(worker.ErrWorkerShutdown)
	}()

	if _, err := d.Execute(ctx, in); !errors.Is(err, activity.ErrResultPending) {
		t.Fatalf("Execute = %v, want ErrResultPending", err)
	}
	if runtime.first != SIGTERM {
		t.Errorf("the container's first signal = %v, want SIGTERM", runtime.first)
	}
	if len(reports.reports) != 1 || !reportedReason(reports.reports[0], ReasonOrkerStopped) {
		t.Fatalf("reports = %v, want one ORKER_STOPPED", reports.reports)
	}
}

// TestShutdown_AnAttemptFinishingAfterTheWorkersStopReportsItsResult: an
// unpinned attempt that commits after the task worker stopped reports its
// result itself, since the SDK sends nothing for it.
func TestShutdown_AnAttemptFinishingAfterTheWorkersStopReportsItsResult(t *testing.T) {
	d, reports := stoppingDispatcher(t, &phaseCapturingRuntime{exitCode: 0}, artifacts.NewMemoryAttemptStore())
	r := newStopRegistry()
	stage := r.admit()
	defer r.leave(stage)
	ctx, cancel := context.WithCancelCause(withStage(context.Background(), stage))
	defer cancel(nil)
	d.Commits = &stopAfterCommit{CommitStore: artifacts.NewMemoryCommitStore(),
		stop: func() { cancel(worker.ErrWorkerShutdown) }}
	in := baseExportInput("finished-after-stop")
	in.CommitUri = "mem://commits/" + in.GetRunId() + "/" + in.GetTaskId() + "/commit.json"

	if _, err := d.Execute(ctx, in); !errors.Is(err, activity.ErrResultPending) {
		t.Fatalf("Execute = %v, want ErrResultPending once the Orker reported the result", err)
	}
	if len(reports.reports) != 1 || reports.reports[0] != nil {
		t.Fatalf("reports = %v, want one success", reports.reports)
	}
	if result, ok := reports.results[0].(*orkstackv1.ExecuteTaskResult); !ok || result.GetCommit() == nil {
		t.Fatalf("reported %v, want the committed result", reports.results[0])
	}
}

// TestDetach_TheWorkersStopIsForwardedUnlessKept: the detached context
// follows every cancel of the original but the worker's Stop, and that one too
// when keep says no longer.
func TestDetach_TheWorkersStopIsForwardedUnlessKept(t *testing.T) {
	for _, tc := range []struct {
		name    string
		cause   error
		keep    bool
		wantEnd bool
	}{
		{"the worker's stop, kept", worker.ErrWorkerShutdown, true, false},
		{"the worker's stop, not kept", worker.ErrWorkerShutdown, false, true},
		{"a server's cancel", temporal.NewCanceledError(), true, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithCancelCause(context.Background())
			detached, release := detachFromWorkerStop(ctx, func() bool { return tc.keep })
			defer release()
			if sdkContextOf(detached) != ctx {
				t.Fatal("the detached context does not carry the original")
			}
			cancel(tc.cause)
			select {
			case <-detached.Done():
				if !tc.wantEnd {
					t.Fatalf("ended with %v, want it to outlive the worker's stop", context.Cause(detached))
				}
				if !errors.Is(context.Cause(detached), tc.cause) {
					t.Errorf("cause = %v, want %v", context.Cause(detached), tc.cause)
				}
			case <-time.After(200 * time.Millisecond):
				if tc.wantEnd {
					t.Fatal("the cancel never reached the detached context")
				}
			}
		})
	}
}

// TestShutdown_AServerCancelAfterTheWorkersStopIsReportedCancelled: once the
// worker stopped, the SDK's cancel no longer reaches the attempt; a server's
// cancel still ends it, through its heartbeat, and the Orker reports it
// cancelled, never ORKER_STOPPED.
func TestShutdown_AServerCancelAfterTheWorkersStopIsReportedCancelled(t *testing.T) {
	runtime := newHeldRuntime()
	d, reports := stoppingDispatcher(t, runtime, artifacts.NewMemoryAttemptStore())
	var cancelled atomic.Bool
	d.heartbeaters = func(context.Context) *Heartbeater {
		return &Heartbeater{
			interval:   2 * time.Millisecond,
			ackTimeout: time.Minute,
			send: func(context.Context, HeartbeatProgress) error {
				if cancelled.Load() {
					return temporal.NewCanceledError()
				}
				return nil
			},
		}
	}
	in := baseExportInput("cancel-after-stop")
	r := newStopRegistry()
	stage := r.admit()
	defer r.leave(stage)
	ctx, cancel := context.WithCancelCause(withStage(context.Background(), stage))
	defer cancel(nil)
	go func() {
		<-runtime.started
		cancel(worker.ErrWorkerShutdown)
		cancelled.Store(true)
	}()

	if _, err := d.Execute(ctx, in); !errors.Is(err, activity.ErrResultPending) {
		t.Fatalf("Execute = %v, want ErrResultPending", err)
	}
	if runtime.first != SIGTERM {
		t.Errorf("the container's first signal = %v, want SIGTERM", runtime.first)
	}
	if len(reports.reports) != 1 || !temporal.IsCanceledError(reports.reports[0]) {
		t.Fatalf("reports = %v, want one cancel", reports.reports)
	}
}
