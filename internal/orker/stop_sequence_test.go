package orker

import (
	"context"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"go.temporal.io/sdk/activity"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/task"
)

// stopDeadline bounds a stop sequence a test expects to end: a hang, never a
// measured duration.
const stopDeadline = 10 * time.Second

// runStop begins a's stop, which sets its cordon before it returns, and runs
// the rest of it on its own goroutine, returning its outcome channel.
func runStop(a *Activities, seq StopSequence) <-chan StopOutcome {
	stop := a.BeginStop(seq)
	out := make(chan StopOutcome, 1)
	go func() { out <- stop.Finish() }()
	return out
}

func awaitStop(t *testing.T, out <-chan StopOutcome) StopOutcome {
	t.Helper()
	select {
	case got := <-out:
		return got
	case <-time.After(stopDeadline):
		t.Fatal("the stop sequence did not end")
		return StopOutcome{}
	}
}

// TestStop_AnIdleOrkerEndsAtOnce: with nothing running and no pin open, a stop
// with a day's grace sweeps nothing and stops its worker without waiting.
func TestStop_AnIdleOrkerEndsAtOnce(t *testing.T) {
	a := newCordonActivities(t, t.TempDir())
	var stopped atomic.Int32
	got := awaitStop(t, runStop(a, StopSequence{
		Grace:      24 * time.Hour,
		StopWorker: func() { stopped.Add(1) },
	}))
	if got.Ended != GraceEndedIdle || got.HandedBack != 0 {
		t.Fatalf("outcome = %+v; want an idle end with nothing handed back", got)
	}
	if stopped.Load() != 1 {
		t.Fatalf("the worker was stopped %d times; want once", stopped.Load())
	}
	if _, ok := a.Cordon().Stopping(); !ok {
		t.Fatal("the stop did not set the stopping cordon")
	}
}

// TestStop_TheGraceWaitsForRunningWorkAndThePinIdle: a running task and an
// open pin keep the grace going; once both end, the stop ends idle and hands
// nothing back.
func TestStop_TheGraceWaitsForRunningWorkAndThePinIdle(t *testing.T) {
	a := newCordonActivities(t, t.TempDir())
	a.running.add()
	a.pins.add("s1", "ork-r", time.Now())
	var stopped atomic.Bool
	out := runStop(a, StopSequence{Grace: 24 * time.Hour, StopWorker: func() { stopped.Store(true) }})

	// Positive control: the stop has begun (its cordon is set) while the
	// worker is still running.
	if _, ok := a.Cordon().Stopping(); !ok {
		t.Fatal("the stop did not set the stopping cordon")
	}
	if stopped.Load() {
		t.Fatal("the worker was stopped while a task ran inside the grace")
	}
	a.running.done()
	a.pins.remove("s1")
	got := awaitStop(t, out)
	if got.Ended != GraceEndedIdle || got.HandedBack != 0 || !stopped.Load() {
		t.Fatalf("outcome = %+v stopped=%v; want an idle end, nothing handed back, the worker stopped", got, stopped.Load())
	}
}

// TestStop_ADeliveryDuringTheGraceRunsInFlightAndT1SweepsThenStops: during
// the grace a task an open poll delivers runs on the stopping Orker, as any
// cordon's in-flight task does, spending no attempt; at t1 the sweep takes the
// running attempt back and the worker is stopped at once, before that attempt
// has reported.
func TestStop_ADeliveryDuringTheGraceRunsInFlightAndT1SweepsThenStops(t *testing.T) {
	a := newCordonActivities(t, t.TempDir())
	env := activityEnv(a)
	ran := 0
	env.RegisterActivityWithOptions(func(context.Context, *orkstackv1.ExecuteTaskInput) error {
		ran++
		return nil
	}, activity.RegisterOptions{Name: task.ActivityExecuteTask})

	// One unpinned attempt runs: admitted, its container started.
	a.running.add()
	running := a.stops.admit()
	if !running.toRevocable() {
		t.Fatal("the running attempt could not reach revocable")
	}
	workerStopped := make(chan struct{})
	reported := make(chan struct{})
	go func() {
		// The attempt reports only once the worker has stopped: were the
		// sweep to wait for its report before stopping the worker, this
		// would never end.
		<-running.handBack.Done()
		<-workerStopped
		// Reported first: the stop waits for the attempt to leave the
		// registry (AwaitHandedBack), so a close after the leave could
		// still be pending when the stop returns.
		close(reported)
		a.stops.leave(running)
		a.running.done()
	}()

	endGrace := make(chan struct{})
	out := runStop(a, StopSequence{
		Grace:    24 * time.Hour,
		EndGrace: endGrace,
		StopWorker: func() {
			if !a.stops.isStopping() {
				t.Error("the worker was stopped before the sweep")
			}
			close(workerStopped)
		},
	})

	// A poll still open at the signal delivers a task inside the grace: it
	// runs here, in flight.
	if _, err := env.ExecuteActivity(task.ActivityExecuteTask, &orkstackv1.ExecuteTaskInput{RunId: "ork-r", TaskId: "late"}); err != nil {
		t.Fatalf("a delivery during the grace = %v; want it run in flight", err)
	}
	if ran != 1 {
		t.Fatalf("a task delivered during the grace ran %d times; want once", ran)
	}
	select {
	case <-workerStopped:
		t.Fatal("the worker was stopped during the grace")
	default:
	}

	close(endGrace) // the second signal
	got := awaitStop(t, out)
	if got.Ended != GraceEndedBySignal || got.HandedBack != 1 {
		t.Fatalf("outcome = %+v; want the grace ended by a signal and one attempt handed back", got)
	}
	select {
	case <-reported:
	default:
		t.Fatal("the stop ended before the handed-back attempt reported")
	}
}

// TestStop_ZeroGraceHandsBackAtOnce: with no grace, t1 is t0.
func TestStop_ZeroGraceHandsBackAtOnce(t *testing.T) {
	a := newCordonActivities(t, t.TempDir())
	a.running.add()
	running := a.stops.admit()
	go func() {
		<-running.handBack.Done()
		a.stops.leave(running)
		a.running.done()
	}()
	var stopped atomic.Bool
	got := awaitStop(t, runStop(a, StopSequence{Grace: 0, StopWorker: func() { stopped.Store(true) }}))
	if got.Ended != GraceEndedTimeout || got.HandedBack != 1 || !stopped.Load() {
		t.Fatalf("outcome = %+v stopped=%v; want the grace's end, one handed back, the worker stopped", got, stopped.Load())
	}
}

func TestParseStopGrace(t *testing.T) {
	cases := []struct {
		raw     string
		want    time.Duration
		refused string // a substring of the refusal, or "" for none
	}{
		{raw: "", want: DefaultStopGrace},
		{raw: "0s", want: 0},
		{raw: "60s", want: time.Minute},
		{raw: "744h", want: MaxStopGrace},
		{raw: "745h", refused: "744h"},
		{raw: "31d", refused: "744h"},
		{raw: "-1s", refused: "744h"},
		{raw: "soon", refused: "not a duration"},
	}
	for _, tc := range cases {
		t.Run(tc.raw, func(t *testing.T) {
			got, err := ParseStopGrace(tc.raw)
			if tc.refused != "" {
				if err == nil || !strings.Contains(err.Error(), tc.refused) || !strings.Contains(err.Error(), StopGraceVar) {
					t.Fatalf("ParseStopGrace(%q) = (%v, %v); want a refusal naming %s and %q", tc.raw, got, err, StopGraceVar, tc.refused)
				}
				return
			}
			if err != nil || got != tc.want {
				t.Fatalf("ParseStopGrace(%q) = (%v, %v); want %v", tc.raw, got, err, tc.want)
			}
		})
	}
}

// TestStop_ADeliveryDuringTheGraceKeepsTheStopUntilItFinishes: a task an open
// poll delivers to an idle stopping Orker holds the grace like any running
// task; when it finishes the stop ends idle, having handed nothing back.
func TestStop_ADeliveryDuringTheGraceKeepsTheStopUntilItFinishes(t *testing.T) {
	a := newCordonActivities(t, t.TempDir())
	env := activityEnv(a)
	release := make(chan struct{})
	started := make(chan struct{})
	env.RegisterActivityWithOptions(func(context.Context, *orkstackv1.ExecuteTaskInput) error {
		close(started)
		<-release
		return nil
	}, activity.RegisterOptions{Name: task.ActivityExecuteTask})
	// One task runs at t0, so the stop does not end idle before the delivery.
	a.running.add()
	var stopped atomic.Bool
	out := runStop(a, StopSequence{Grace: 24 * time.Hour, StopWorker: func() { stopped.Store(true) }})
	delivered := make(chan error, 1)
	go func() {
		_, err := env.ExecuteActivity(task.ActivityExecuteTask, &orkstackv1.ExecuteTaskInput{RunId: "ork-r", TaskId: "late"})
		delivered <- err
	}()
	<-started
	a.running.done() // the task that ran at t0 ends
	select {
	case got := <-out:
		t.Fatalf("the stop ended %+v while a task delivered during the grace still ran", got)
	default:
	}
	if stopped.Load() {
		t.Fatal("the worker was stopped while a task delivered during the grace ran")
	}
	close(release)
	if err := <-delivered; err != nil {
		t.Fatalf("the delivered task = %v; want it to finish", err)
	}
	got := awaitStop(t, out)
	if got.Ended != GraceEndedIdle || got.HandedBack != 0 || !stopped.Load() {
		t.Fatalf("outcome = %+v stopped=%v; want an idle end with nothing handed back", got, stopped.Load())
	}
}

// TestStopRegistry_NamesTheAttemptsItWaitsFor: the stop's wait says which
// attempts it still owes a report for, so a long wait is never anonymous.
func TestStopRegistry_NamesTheAttemptsItWaitsFor(t *testing.T) {
	r := newStopRegistry()
	owed := r.admit()
	owed.setLabel("run ork-a task build attempt 1")
	r.admit() // admitted, not yet named
	pinned := r.admit()
	pinned.setLabel("run ork-b task test attempt 1")
	pinned.setPinned()
	if !pinned.toRevocable() {
		t.Fatal("the pinned member could not reach revocable")
	}
	got := r.owedLabels(10)
	if len(got) != 1 || got[0] != "run ork-a task build attempt 1" {
		t.Fatalf("owedLabels = %v; want only the unpinned attempt (a running pinned member is left to the worker's stop)", got)
	}
}
