package orker

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"go.temporal.io/api/serviceerror"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/worker"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// heartbeatRPCFailures are the causes the SDK cancels a running attempt with
// when a heartbeat RPC fails a status it counts as retryable: it assumes the
// server is about to time the activity out. None of them means anybody asked
// for the task to stop.
var heartbeatRPCFailures = map[string]error{
	"the deadline of a stalled frontend": context.DeadlineExceeded,
	"an unreachable frontend":            serviceerror.NewUnavailable("connection refused"),
	"a frontend shedding load":           serviceerror.NewResourceExhausted(0, "too many requests"),
	"wrapped":                            fmt.Errorf("record heartbeat: %w", context.DeadlineExceeded),
}

// TestExecute_ALostHeartbeatIsNotACancel: an attempt the SDK cancelled because
// its heartbeat RPC failed reports a retryable InfrastructureError naming the
// loss, on the record and to the run. Reported as a cancel it says something
// nobody did, and it reaches the run as a bare "context canceled" with no
// reason (AGENTS.md §4, §15).
func TestExecute_ALostHeartbeatIsNotACancel(t *testing.T) {
	for name, cause := range heartbeatRPCFailures {
		t.Run(name, func(t *testing.T) {
			attempts := artifacts.NewMemoryAttemptStore()
			dispatcher := newAttemptDispatcher(t, &phaseCapturingRuntime{exitCode: 0}, attempts)
			in := baseExportInput("heartbeat-lost")

			ctx, cancel := context.WithCancelCause(context.Background())
			cancel(cause)
			result, err := dispatcher.Execute(ctx, in)
			if result != nil {
				t.Fatalf("Execute returned a result %v for an attempt it stopped", result)
			}

			var appErr *temporal.ApplicationError
			if !errors.As(err, &appErr) {
				t.Fatalf("Execute error = %v (%T), want an ApplicationError naming the loss", err, err)
			}
			if appErr.Type() != CategoryInfrastructureError || appErr.NonRetryable() {
				t.Errorf("error type %q nonRetryable=%v, want a retryable %s",
					appErr.Type(), appErr.NonRetryable(), CategoryInfrastructureError)
			}
			var detail *orkstackv1.ActivityFailureDetail
			if detailErr := appErr.Details(&detail); detailErr != nil {
				t.Fatalf("read the failure detail: %v", detailErr)
			}
			if detail.GetReasonCode() != ReasonHeartbeatLost {
				t.Errorf("reason on the wire = %q, want %s", detail.GetReasonCode(), ReasonHeartbeatLost)
			}

			records, listErr := attempts.ListAttempts(context.Background(), in.GetRunId(), in.GetTaskId(), artifacts.NoItem)
			if listErr != nil {
				t.Fatalf("ListAttempts: %v", listErr)
			}
			if len(records) != 1 {
				t.Fatalf("stored %d records, want 1", len(records))
			}
			record := records[0]
			if record.GetOutcome() != orkstackv1.AttemptOutcome_ATTEMPT_OUTCOME_FAILED {
				t.Errorf("outcome = %v, want FAILED: nobody cancelled this task", record.GetOutcome())
			}
			got := record.GetError()
			if got.GetCode() != ReasonHeartbeatLost ||
				got.GetCategory() != orkstackv1.FailureCategory_FAILURE_CATEGORY_INFRASTRUCTURE_ERROR || !got.GetRetryable() {
				t.Errorf("record's error = %v, want %s, a retryable INFRASTRUCTURE_ERROR", got, ReasonHeartbeatLost)
			}
			for _, want := range []string{"orker-attempt-test", "the cluster did not answer", cause.Error()} {
				if !strings.Contains(got.GetMessage(), want) {
					t.Errorf("message %q does not say %q", got.GetMessage(), want)
				}
			}
		})
	}
}

// TestExecute_ACancelNobodyLostStaysACancel: every cancel that does mean stop
// keeps its own ending. The server's cancel is CANCELED and never retried, an
// activity the server no longer knows is CANCELED too (nothing can hear a
// completion for it), and a stopping Orker tells the run nothing.
func TestExecute_ACancelNobodyLostStaysACancel(t *testing.T) {
	tests := map[string]struct {
		cause       error
		wantOutcome orkstackv1.AttemptOutcome
		wantPending bool
	}{
		"the server's cancel": {
			cause:       &temporal.CanceledError{},
			wantOutcome: orkstackv1.AttemptOutcome_ATTEMPT_OUTCOME_CANCELED,
		},
		"a plain cancel": {
			cause:       context.Canceled,
			wantOutcome: orkstackv1.AttemptOutcome_ATTEMPT_OUTCOME_CANCELED,
		},
		"an activity the server no longer knows": {
			cause:       serviceerror.NewNotFound("activity not found"),
			wantOutcome: orkstackv1.AttemptOutcome_ATTEMPT_OUTCOME_CANCELED,
		},
		"a namespace that went away": {
			cause:       serviceerror.NewNamespaceNotFound("gone"),
			wantOutcome: orkstackv1.AttemptOutcome_ATTEMPT_OUTCOME_CANCELED,
		},
		"a paused activity": {
			cause:       activity.ErrActivityPaused,
			wantOutcome: orkstackv1.AttemptOutcome_ATTEMPT_OUTCOME_CANCELED,
		},
		"a reset activity": {
			cause:       activity.ErrActivityReset,
			wantOutcome: orkstackv1.AttemptOutcome_ATTEMPT_OUTCOME_CANCELED,
		},
		"a stopping orker": {
			cause:       worker.ErrWorkerShutdown,
			wantOutcome: orkstackv1.AttemptOutcome_ATTEMPT_OUTCOME_FAILED,
			wantPending: true,
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			attempts := artifacts.NewMemoryAttemptStore()
			dispatcher := newAttemptDispatcher(t, &phaseCapturingRuntime{exitCode: 0}, attempts)
			in := baseExportInput("still-a-cancel")

			ctx, cancel := context.WithCancelCause(context.Background())
			cancel(tc.cause)
			_, err := dispatcher.Execute(ctx, in)
			if tc.wantPending {
				if !errors.Is(err, activity.ErrResultPending) {
					t.Errorf("Execute error = %v, want ErrResultPending", err)
				}
			} else if !errors.Is(err, context.Canceled) {
				t.Errorf("Execute error = %v, want the context's own cancel", err)
			}

			records, listErr := attempts.ListAttempts(context.Background(), in.GetRunId(), in.GetTaskId(), artifacts.NoItem)
			if listErr != nil {
				t.Fatalf("ListAttempts: %v", listErr)
			}
			if len(records) != 1 {
				t.Fatalf("stored %d records, want 1", len(records))
			}
			if got := records[0].GetOutcome(); got != tc.wantOutcome {
				t.Errorf("outcome = %v, want %v", got, tc.wantOutcome)
			}
			if records[0].GetError().GetCode() == ReasonHeartbeatLost {
				t.Errorf("record names %s for a cancel that lost nothing", ReasonHeartbeatLost)
			}
		})
	}
}

// committingThenCancelling writes the commit and then cancels the attempt, the
// window in which the attempt's work is already durable.
type committingThenCancelling struct {
	artifacts.CommitStore
	cancel func()
}

func (s committingThenCancelling) Create(
	ctx context.Context, uri string, c *orkstackv1.ActivityCommit,
) (artifacts.CommitRecord, error) {
	record, err := s.CommitStore.Create(ctx, uri, c)
	s.cancel()
	return record, err
}

// TestExecute_AHeartbeatLostAfterTheCommitStillReportsTheResult: a commit is
// the attempt's durable outcome (AGENTS.md §4), so a heartbeat lost after it
// costs nothing. Throwing the result away would fail a task with no retry:
// whose work is done, for a blip that has already passed.
func TestExecute_AHeartbeatLostAfterTheCommitStillReportsTheResult(t *testing.T) {
	attempts := artifacts.NewMemoryAttemptStore()
	dispatcher := newAttemptDispatcher(t, &phaseCapturingRuntime{exitCode: 0}, attempts)
	ctx, cancel := context.WithCancelCause(context.Background())
	dispatcher.Commits = committingThenCancelling{
		CommitStore: artifacts.NewMemoryCommitStore(),
		cancel:      func() { cancel(context.DeadlineExceeded) },
	}
	in := baseExportInput("heartbeat-lost-after-commit")
	in.CommitUri = "memory://commits/heartbeat-lost-after-commit"

	result, err := dispatcher.Execute(ctx, in)
	if err != nil {
		t.Fatalf("Execute = %v, want the committed result", err)
	}
	if result.GetCommit().GetUri() != in.GetCommitUri() {
		t.Errorf("result names commit %q, want %q", result.GetCommit().GetUri(), in.GetCommitUri())
	}
	records, listErr := attempts.ListAttempts(context.Background(), in.GetRunId(), in.GetTaskId(), artifacts.NoItem)
	if listErr != nil {
		t.Fatalf("ListAttempts: %v", listErr)
	}
	if len(records) != 1 || records[0].GetOutcome() != orkstackv1.AttemptOutcome_ATTEMPT_OUTCOME_SUCCEEDED {
		t.Errorf("records = %v, want one SUCCEEDED", records)
	}
}

// TestExecute_ACancelAfterTheCommitStaysACancel: the rule above is about a
// lost heartbeat only. A cancel the server delivered is CANCELED whatever was
// already written (§6.3).
func TestExecute_ACancelAfterTheCommitStaysACancel(t *testing.T) {
	attempts := artifacts.NewMemoryAttemptStore()
	dispatcher := newAttemptDispatcher(t, &phaseCapturingRuntime{exitCode: 0}, attempts)
	ctx, cancel := context.WithCancelCause(context.Background())
	dispatcher.Commits = committingThenCancelling{
		CommitStore: artifacts.NewMemoryCommitStore(),
		cancel:      func() { cancel(&temporal.CanceledError{}) },
	}
	in := baseExportInput("cancel-after-commit")
	in.CommitUri = "memory://commits/cancel-after-commit"

	if _, err := dispatcher.Execute(ctx, in); !errors.Is(err, context.Canceled) {
		t.Fatalf("Execute = %v, want the context's own cancel", err)
	}
}

// cancellingStore cancels the attempt the first time anything is stored: the
// window in which a failed attempt uploads what its container left.
type cancellingStore struct {
	artifacts.Store
	once   sync.Once
	cancel func()
}

func (s *cancellingStore) Put(ctx context.Context, name string, r io.Reader, opts artifacts.PutOptions) (artifacts.ArtifactRef, error) {
	s.once.Do(s.cancel)
	return s.Store.Put(ctx, name, r, opts)
}

// stoppedRuntime is a container its runtime stopped from outside.
type stoppedRuntime struct{ phaseCapturingRuntime }

func (r *stoppedRuntime) Wait(context.Context, ContainerID) (ExitStatus, error) {
	return ExitStatus{Code: 143, StoppedFromOutside: true}, nil
}

// TestExecute_AContainersOwnEndingOutlivesAHeartbeatLostAfterIt: a container
// that ended before the heartbeat went unanswered said how it ended itself, and
// nothing the loss stopped produced that. Its reason stays, so a failure the
// workload caused is never reported as an infrastructure blip. A cancel the
// server delivered is still a cancel.
func TestExecute_AContainersOwnEndingOutlivesAHeartbeatLostAfterIt(t *testing.T) {
	for name, tt := range map[string]struct {
		runtime ContainerRuntime
		cause   error
		want    string
	}{
		"its own non-zero exit":         {runtime: &phaseCapturingRuntime{exitCode: 3}, cause: context.DeadlineExceeded, want: ReasonContainerExitNonzero},
		"a stop through its runtime":    {runtime: &stoppedRuntime{}, cause: context.DeadlineExceeded, want: ReasonContainerStoppedByRuntime},
		"the loop's own stop":           {runtime: &phaseCapturingRuntime{exitCode: 3}, cause: &heartbeatUnanswered{}, want: ReasonContainerExitNonzero},
		"the server's cancel, not lost": {runtime: &phaseCapturingRuntime{exitCode: 3}, cause: temporal.NewCanceledError()},
	} {
		t.Run(name, func(t *testing.T) {
			attempts := artifacts.NewMemoryAttemptStore()
			dispatcher := newAttemptDispatcher(t, tt.runtime, attempts)
			ctx, cancel := context.WithCancelCause(context.Background())
			dispatcher.Store = &cancellingStore{Store: dispatcher.Store, cancel: func() { cancel(tt.cause) }}
			in := baseExportInput("own-ending-" + strings.ReplaceAll(name, " ", "-"))

			_, err := dispatcher.Execute(ctx, in)
			records, listErr := attempts.ListAttempts(context.Background(), in.GetRunId(), in.GetTaskId(), artifacts.NoItem)
			if listErr != nil || len(records) != 1 {
				t.Fatalf("ListAttempts = %v, %v, want one record", records, listErr)
			}
			if tt.want == "" {
				if !errors.Is(err, context.Canceled) || records[0].GetOutcome() != orkstackv1.AttemptOutcome_ATTEMPT_OUTCOME_CANCELED {
					t.Errorf("Execute = %v, outcome %v, want a cancel", err, records[0].GetOutcome())
				}
				return
			}
			var appErr *temporal.ApplicationError
			if !errors.As(err, &appErr) {
				t.Fatalf("Execute error = %v (%T), want an ApplicationError", err, err)
			}
			var detail *orkstackv1.ActivityFailureDetail
			if detailErr := appErr.Details(&detail); detailErr != nil {
				t.Fatalf("read the failure detail: %v", detailErr)
			}
			if detail.GetReasonCode() != tt.want {
				t.Errorf("reason on the wire = %q, want %s", detail.GetReasonCode(), tt.want)
			}
			if got := records[0].GetError().GetCode(); got != tt.want {
				t.Errorf("record's reason = %q, want %s", got, tt.want)
			}
		})
	}
}

// heartbeatClock is a clock a test moves.
type heartbeatClock struct{ at time.Time }

func (c *heartbeatClock) now() time.Time { return c.at }

// reportingHeartbeater is a loop whose report a test calls itself, on a cluster
// that answers with answer.
func reportingHeartbeater(answer *error) (h *Heartbeater, clock *heartbeatClock, delivered, lost *[]error) {
	clock = &heartbeatClock{at: time.Date(2026, 11, 6, 12, 0, 0, 0, time.UTC)}
	delivered, lost = &[]error{}, &[]error{}
	h = &Heartbeater{
		send:       func(context.Context, HeartbeatProgress) error { return *answer },
		deliver:    func(context.Context, HeartbeatProgress) { *delivered = append(*delivered, *answer) },
		onLost:     func(err error) { *lost = append(*lost, err) },
		ackTimeout: 30 * time.Second,
		now:        clock.now,
		lastAck:    clock.at,
	}
	return h, clock, delivered, lost
}

// TestHeartbeater_AnUnansweredHeartbeatTouchesNothingUntilTheTimeout: a cluster
// that does not answer has decided nothing, however long it takes, until the
// attempt's heartbeat timeout.
func TestHeartbeater_AnUnansweredHeartbeatTouchesNothingUntilTheTimeout(t *testing.T) {
	for name, cause := range heartbeatRPCFailures {
		t.Run(name, func(t *testing.T) {
			answer := cause
			h, clock, delivered, lost := reportingHeartbeater(&answer)
			for _, after := range []time.Duration{0, 5 * time.Second, 15 * time.Second, 29 * time.Second} {
				clock.at = h.lastAck.Add(after)
				h.report(context.Background())
			}
			if len(*lost) != 0 || len(*delivered) != 0 {
				t.Fatalf("inside the timeout: lost %v, delivered %v", *lost, *delivered)
			}

			clock.at = h.lastAck.Add(30 * time.Second)
			h.report(context.Background())
			if len(*lost) != 1 || !errors.Is((*lost)[0], cause) {
				t.Fatalf("at the timeout: lost %v, want one naming %v", *lost, cause)
			}
			if len(*delivered) != 0 {
				t.Errorf("handed the SDK an answer nobody gave: %v", *delivered)
			}
		})
	}
}

// TestHeartbeater_AnAnswerRestartsTheTimeout: the timeout runs from the last
// answer, as the cluster's own does, not from the attempt's start.
func TestHeartbeater_AnAnswerRestartsTheTimeout(t *testing.T) {
	var answer error = context.DeadlineExceeded
	h, clock, _, lost := reportingHeartbeater(&answer)
	start := clock.at
	clock.at = start.Add(25 * time.Second)
	h.report(context.Background())

	answer = nil
	clock.at = start.Add(26 * time.Second)
	h.report(context.Background())

	answer = context.DeadlineExceeded
	clock.at = start.Add(50 * time.Second)
	h.report(context.Background())
	if len(*lost) != 0 {
		t.Fatalf("lost %v, 24 s after an answer", *lost)
	}
	clock.at = start.Add(56 * time.Second)
	h.report(context.Background())
	if len(*lost) != 1 {
		t.Fatalf("lost %v, 30 s after an answer; want one", *lost)
	}
}

// TestHeartbeater_TheServersDecisionsGoToTheSDK: what the server decided is
// handed on, at once, and never counts as a heartbeat gone unanswered.
func TestHeartbeater_TheServersDecisionsGoToTheSDK(t *testing.T) {
	for name, decision := range map[string]error{
		"a cancel":              temporal.NewCanceledError(),
		"a pause":               activity.ErrActivityPaused,
		"a reset":               activity.ErrActivityReset,
		"an activity it lost":   serviceerror.NewNotFound("activity not found"),
		"a namespace elsewhere": serviceerror.NewNamespaceNotActive("ns", "here", "there"),
		"a namespace it lost":   serviceerror.NewNamespaceNotFound("ns"),
	} {
		t.Run(name, func(t *testing.T) {
			answer := decision
			h, clock, delivered, lost := reportingHeartbeater(&answer)
			clock.at = h.lastAck.Add(time.Hour)
			h.report(context.Background())
			if len(*delivered) != 1 {
				t.Errorf("delivered %v, want the decision once", *delivered)
			}
			if len(*lost) != 0 {
				t.Errorf("lost %v: a decision is an answer", *lost)
			}
			if want := temporal.IsCanceledError(decision); h.serverCanceled.Load() != want {
				t.Errorf("serverCanceled = %v, want %v", h.serverCanceled.Load(), want)
			}
		})
	}
}

// TestHeartbeater_StopAbandonsAHeartbeatInFlight: an attempt never waits on a
// silent cluster to finish, and the abandoned heartbeat says nothing about the
// cluster. Each RPC has a third of the timeout, so three go unanswered before it.
func TestHeartbeater_StopAbandonsAHeartbeatInFlight(t *testing.T) {
	inFlight := make(chan time.Duration, 1)
	clock := &heartbeatClock{at: time.Date(2026, 11, 6, 12, 0, 0, 0, time.UTC)}
	lost := make(chan error, 1)
	h := &Heartbeater{
		send: func(call context.Context, _ HeartbeatProgress) error {
			deadline, _ := call.Deadline()
			inFlight <- time.Until(deadline)
			clock.at = clock.at.Add(time.Hour)
			<-call.Done()
			return call.Err()
		},
		onLost:     func(err error) { lost <- err },
		ackTimeout: 30 * time.Second,
		now:        clock.now,
	}
	h.Start(context.Background())
	if left := <-inFlight; left > 10*time.Second || left < 5*time.Second {
		t.Errorf("the RPC had %s, want about a third of the timeout", left)
	}
	h.Stop()
	select {
	case err := <-lost:
		t.Errorf("a stopped loop reported the cluster lost: %v", err)
	default:
	}
}

// heldRuntime holds its container until it is signalled, and says when it started.
type heldRuntime struct {
	phaseCapturingRuntime
	started  chan struct{}
	signaled chan Signal
	first    Signal // what stopped it; read after Execute returns
	// completedCanceled says the Orker reported the attempt cancelled itself.
	completedCanceled atomic.Bool
	once              sync.Once
}

func newHeldRuntime() *heldRuntime {
	return &heldRuntime{started: make(chan struct{}), signaled: make(chan Signal, 4)}
}

func (r *heldRuntime) Start(context.Context, ContainerID, io.Writer, io.Writer) error {
	r.once.Do(func() { close(r.started) })
	return nil
}

func (r *heldRuntime) Signal(_ context.Context, _ ContainerID, signal Signal) error {
	r.signaled <- signal
	return nil
}

func (r *heldRuntime) Wait(context.Context, ContainerID) (ExitStatus, error) {
	r.first = <-r.signaled
	return ExitStatus{Code: 143}, nil
}

// clusterAnswering is an attempt's loop on a cluster that acknowledges heartbeats
// until the container runs and answers answer after, ticking fast, with deliver
// for the SDK.
func clusterAnswering(r *heldRuntime, answer error, ackTimeout time.Duration, deliver func(context.Context, HeartbeatProgress)) func(context.Context) *Heartbeater {
	return func(context.Context) *Heartbeater {
		return &Heartbeater{
			interval: 5 * time.Millisecond,
			send: func(context.Context, HeartbeatProgress) error {
				select {
				case <-r.started:
					return answer
				default:
					return nil
				}
			},
			deliver:    deliver,
			ackTimeout: ackTimeout,
			completeCanceled: func(context.Context) error {
				r.completedCanceled.Store(true)
				return nil
			},
		}
	}
}

// TestExecute_NoAckForTheTimeoutStopsTheContainerAndFailsLost: past the heartbeat
// timeout the container is stopped with SIGTERM, as a cancelled one is, and the
// attempt fails with a retryable reason, never a cancel.
func TestExecute_NoAckForTheTimeoutStopsTheContainerAndFailsLost(t *testing.T) {
	attempts := artifacts.NewMemoryAttemptStore()
	runtime := newHeldRuntime()
	dispatcher := newAttemptDispatcher(t, runtime, attempts)
	dispatcher.heartbeaters = clusterAnswering(runtime, context.DeadlineExceeded, 50*time.Millisecond, nil)
	in := baseExportInput("heartbeat-no-ack")

	result, err := dispatcher.Execute(context.Background(), in)
	if result != nil {
		t.Fatalf("Execute returned a result %v for an attempt it stopped", result)
	}
	var appErr *temporal.ApplicationError
	if !errors.As(err, &appErr) || appErr.Type() != CategoryInfrastructureError || appErr.NonRetryable() {
		t.Fatalf("Execute error = %v, want a retryable %s", err, CategoryInfrastructureError)
	}
	var detail *orkstackv1.ActivityFailureDetail
	if detailErr := appErr.Details(&detail); detailErr != nil {
		t.Fatalf("read the failure detail: %v", detailErr)
	}
	if detail.GetReasonCode() != ReasonHeartbeatLost {
		t.Errorf("reason = %q, want %s", detail.GetReasonCode(), ReasonHeartbeatLost)
	}
	if runtime.first != SIGTERM {
		t.Errorf("the container was stopped with %v, want SIGTERM", runtime.first)
	}
	if !strings.Contains(err.Error(), context.DeadlineExceeded.Error()) {
		t.Errorf("message %q does not say what the heartbeat met", err)
	}
}

// TestExecute_TheServersCancelStaysACancel, both ways it reaches the attempt. The
// SDK's heartbeat is answered, so the context carries the server's own cause and
// the SDK reports the cancel. Or a new stall eats that heartbeat, so the SDK
// cancels with the stall as its cause and would report a returned cancel as a
// failure: the Orker reports it itself and returns the SDK nothing to send.
func TestExecute_TheServersCancelStaysACancel(t *testing.T) {
	for name, tt := range map[string]struct {
		cause          error
		wantResultHeld bool
	}{
		"delivered":                   {cause: temporal.NewCanceledError()},
		"eaten by a stall of its own": {cause: context.DeadlineExceeded, wantResultHeld: true},
	} {
		t.Run(name, func(t *testing.T) {
			attempts := artifacts.NewMemoryAttemptStore()
			runtime := newHeldRuntime()
			dispatcher := newAttemptDispatcher(t, runtime, attempts)
			parent, cancel := context.WithCancelCause(context.Background())
			defer cancel(nil)
			dispatcher.heartbeaters = clusterAnswering(runtime, temporal.NewCanceledError(), time.Minute,
				func(context.Context, HeartbeatProgress) { cancel(tt.cause) })
			in := baseExportInput("heartbeat-cancel")

			result, err := dispatcher.Execute(parent, in)
			if result != nil {
				t.Fatalf("Execute returned a result %v for a cancelled attempt", result)
			}
			if tt.wantResultHeld {
				if !errors.Is(err, activity.ErrResultPending) || !runtime.completedCanceled.Load() {
					t.Errorf("Execute error = %v, reported cancelled = %v; want the Orker to report it and the SDK nothing",
						err, runtime.completedCanceled.Load())
				}
			} else if !errors.Is(err, context.Canceled) || runtime.completedCanceled.Load() {
				t.Errorf("Execute error = %v, reported cancelled = %v; want the SDK to report it",
					err, runtime.completedCanceled.Load())
			}
			if runtime.first != SIGTERM {
				t.Errorf("the container was stopped with %v, want SIGTERM", runtime.first)
			}
			records, listErr := attempts.ListAttempts(context.Background(), in.GetRunId(), in.GetTaskId(), artifacts.NoItem)
			if listErr != nil {
				t.Fatalf("ListAttempts: %v", listErr)
			}
			if len(records) != 1 || records[0].GetOutcome() != orkstackv1.AttemptOutcome_ATTEMPT_OUTCOME_CANCELED {
				t.Errorf("records %v, want one CANCELED", records)
			}
		})
	}
}

// TestHeartbeater_TheStopIsNotLateByAnRPC: a heartbeat near the timeout has only
// what is left of it, so an RPC begun then cannot carry the stop much past it.
func TestHeartbeater_TheStopIsNotLateByAnRPC(t *testing.T) {
	for _, tt := range []struct {
		sinceAck time.Duration
		want     time.Duration
	}{
		{0, 10 * time.Second},
		{20 * time.Second, 10 * time.Second},
		{25 * time.Second, 5 * time.Second},
		{29500 * time.Millisecond, time.Second},
		{time.Hour, time.Second},
	} {
		clock := &heartbeatClock{at: time.Date(2026, 11, 6, 12, 0, 0, 0, time.UTC)}
		h := &Heartbeater{ackTimeout: 30 * time.Second, now: clock.now, lastAck: clock.at}
		clock.at = clock.at.Add(tt.sinceAck)
		if got := h.rpcDeadline(); got != tt.want {
			t.Errorf("%s after an answer: deadline %s, want %s", tt.sinceAck, got, tt.want)
		}
	}
}

// TestHeartbeatLost_TheLoopsStopIsLostWhateverItWraps: the error a heartbeat met
// must not read as a decision: a client's own cancel looks like the plain parent
// cancel it is told apart from.
func TestHeartbeatLost_TheLoopsStopIsLostWhateverItWraps(t *testing.T) {
	for _, met := range []error{context.Canceled, serviceerror.NewNotFound("x"), temporal.NewCanceledError()} {
		ctx, cancel := context.WithCancelCause(context.Background())
		cancel(&heartbeatUnanswered{after: time.Minute, err: met})
		if lost, _ := heartbeatLost(ctx); !lost {
			t.Errorf("a stop after meeting %v did not read as lost", met)
		}
	}
}

// sdkRouteHeartbeater is the loop for a test in the SDK's activity environment,
// whose mock service answers a heartbeat and nothing else: the client
// NewHeartbeater beats through asks the service for its capabilities first. A
// real cluster's route is the stall tests' (internal/engine).
func sdkRouteHeartbeater(ctx context.Context) *Heartbeater {
	return &Heartbeater{send: func(_ context.Context, progress HeartbeatProgress) error {
		activity.RecordHeartbeat(ctx, progress)
		return nil
	}}
}
