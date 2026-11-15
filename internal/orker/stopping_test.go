package orker

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/worker"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/task"
	"github.com/orkstack/orkstack/internal/testsupport/must"
)

// The stopping cause is the cordon an Orker sets itself on its first stop
// signal (docs/OCI_ACTIVITY_CONTRACT.md §6.3): it ends only with the process.

func TestCordon_StoppingCause(t *testing.T) {
	t0 := time.Date(2026, 11, 14, 9, 0, 0, 0, time.UTC)
	ends := t0.Add(24 * time.Hour)
	done, cancel := context.WithCancel(context.Background())
	cancel()

	t.Run("it cordons, names itself and keeps its grace end", func(t *testing.T) {
		c := NewCordon()
		c.SetStopping(t0, ends)
		cordoned, reason, since := c.State()
		if !cordoned || reason != stoppingReason || !since.Equal(t0) {
			t.Fatalf("State() = (%v, %q, %v); want (true, %q, %v)", cordoned, reason, since, stoppingReason, t0)
		}
		got, ok := c.Stopping()
		if !ok || !got.Since.Equal(t0) || !got.GraceEndsAt.Equal(ends) {
			t.Fatalf("Stopping() = (%+v, %v); want since %v, grace ends %v", got, ok, t0, ends)
		}
		if err := c.Wait(done); !errors.Is(err, context.Canceled) {
			t.Fatalf("Wait while stopping = %v; want it to block until its context ends", err)
		}
	})
	t.Run("a second call keeps the first instant", func(t *testing.T) {
		c := NewCordon()
		c.SetStopping(t0, ends)
		c.SetStopping(t0.Add(time.Minute), t0.Add(time.Hour))
		if got, _ := c.Stopping(); !got.Since.Equal(t0) || !got.GraceEndsAt.Equal(ends) {
			t.Fatalf("Stopping() after a second call = %+v; want the first", got)
		}
	})
	t.Run("clearing the other causes never ends it", func(t *testing.T) {
		c := NewCordon()
		c.Set("cve", t0)
		c.SetStoreOutage("store down", t0)
		c.SetStopping(t0, ends)
		c.Clear()
		c.ClearStoreOutage()
		if cordoned, reason, _ := c.State(); !cordoned || reason != stoppingReason {
			t.Fatalf("State() after clearing the others = (%v, %q); want stopping", cordoned, reason)
		}
		if err := c.Wait(done); !errors.Is(err, context.Canceled) {
			t.Fatalf("Wait = %v; want it to block", err)
		}
	})
	t.Run("an operator's reason is named first", func(t *testing.T) {
		c := NewCordon()
		c.SetStopping(t0, ends)
		c.Set("cve", t0.Add(time.Second))
		if _, reason, _ := c.State(); reason != "cve" {
			t.Fatalf("reason = %q; want the operator's", reason)
		}
	})
	t.Run("a cordon that is not stopping says so", func(t *testing.T) {
		if _, ok := NewCordon().Stopping(); ok {
			t.Fatal("a new cordon says it is stopping")
		}
	})
}

func TestDescribeOrker_ReportsStopping(t *testing.T) {
	a := newCordonActivities(t, t.TempDir())
	ctx := context.Background()
	t0 := time.Now()
	a.Cordon().SetStopping(t0, t0.Add(time.Hour))
	got, err := a.DescribeOrker(ctx)
	must.OK(t, err)
	if !got.GetCordoned() || got.GetStopping() == nil {
		t.Fatalf("a stopping Orker: cordoned=%v stopping=%v; want both", got.GetCordoned(), got.GetStopping())
	}
	// When the stop began and when its grace ends, as the cordon holds them.
	if since := got.GetStopping().GetSince(); since == nil || !since.AsTime().Equal(t0) {
		t.Fatalf("stopping.since = %v; want %v", since, t0)
	}
	if end := got.GetStopping().GetGraceEndsAt(); end == nil || !end.AsTime().Equal(t0.Add(time.Hour)) {
		t.Fatalf("stopping.grace_ends_at = %v; want %v", end, t0.Add(time.Hour))
	}
	// Not an operator's cordon: the census keeps its pool live.
	if got.GetCordonReason() != "" || got.GetCordonedAt() != nil {
		t.Fatalf("a stopping Orker reports an operator's cordon: reason=%q at=%v", got.GetCordonReason(), got.GetCordonedAt())
	}
	a.Cordon().Set("cve", t0)
	got, err = a.DescribeOrker(ctx)
	must.OK(t, err)
	if got.GetCordonReason() != "cve" || got.GetStopping() == nil {
		t.Fatalf("operator and stopping: reason=%q stopping=%v; want both", got.GetCordonReason(), got.GetStopping())
	}
}

// TestCordonInterceptor_StoppingRunsWhatAnOpenPollDelivers is what a poll
// still open at the first signal delivers: like any cordon's in-flight task,
// it runs on the stopping Orker within the grace, counted as running, rather
// than being handed back to spend one of the task's own attempts. A member of
// a pin opened before the signal runs too.
func TestCordonInterceptor_StoppingRunsWhatAnOpenPollDelivers(t *testing.T) {
	a := newCordonActivities(t, t.TempDir())
	env := activityEnv(a)
	ran := 0
	during := int32(-1)
	env.RegisterActivityWithOptions(func(context.Context, *orkstackv1.ExecuteTaskInput) error {
		ran++
		during = a.RunningTasks()
		return nil
	}, activity.RegisterOptions{Name: task.ActivityExecuteTask})
	a.Cordon().SetStopping(time.Now(), time.Now().Add(time.Hour))

	if _, err := env.ExecuteActivity(task.ActivityExecuteTask, &orkstackv1.ExecuteTaskInput{RunId: "ork-r", TaskId: "build"}); err != nil {
		t.Fatalf("a task delivered during the grace was refused: %v", err)
	}
	if ran != 1 || during != 1 {
		t.Fatalf("ran=%d running while it ran=%d; want it run once and counted as running", ran, during)
	}
	pinned := &orkstackv1.ExecuteTaskInput{RunId: "ork-r", TaskId: "test", Pin: &orkstackv1.PinContext{}}
	if _, err := env.ExecuteActivity(task.ActivityExecuteTask, pinned); err != nil {
		t.Fatalf("a member of a pin opened before the signal was refused: %v", err)
	}
	if ran != 2 {
		t.Fatalf("the pinned member ran %d times in all; want 2", ran)
	}
}

// TestCordon_PinCreationBoundFromScheduledTime is the stopping pin host's
// refusal: retryable while now is before min(max(grace end, t0 + 15 s),
// scheduled + 5 min) and within 45 attempts, then final.
func TestCordon_PinCreationBoundFromScheduledTime(t *testing.T) {
	t0 := time.Date(2026, 11, 14, 9, 0, 0, 0, time.UTC)
	cases := []struct {
		name      string
		grace     time.Duration
		now       time.Duration // after t0
		scheduled time.Duration // after t0, may be negative
		attempt   int32
		retryable bool
	}{
		{name: "long grace, a fresh creation", grace: 24 * time.Hour, now: time.Minute, scheduled: 30 * time.Second, attempt: 1, retryable: true},
		{name: "long grace, five minutes after it was scheduled", grace: 24 * time.Hour, now: 10 * time.Minute, scheduled: 5*time.Minute - time.Second, attempt: 3},
		{name: "long grace, just inside five minutes", grace: 24 * time.Hour, now: 10 * time.Minute, scheduled: 5*time.Minute + time.Second, attempt: 3, retryable: true},
		{name: "grace 0, inside the 15 s floor", grace: 0, now: 10 * time.Second, scheduled: 0, attempt: 1, retryable: true},
		{name: "grace 0, past the 15 s floor", grace: 0, now: 16 * time.Second, scheduled: 0, attempt: 2},
		{name: "a short grace past its end", grace: 30 * time.Second, now: 31 * time.Second, scheduled: 0, attempt: 2},
		{name: "a short grace before its end", grace: 30 * time.Second, now: 29 * time.Second, scheduled: 0, attempt: 2, retryable: true},
		{name: "scheduled before t0 counts from its schedule", grace: 24 * time.Hour, now: time.Minute, scheduled: -4*time.Minute - time.Second, attempt: 2},
		{name: "the 45th refusal still retries", grace: 24 * time.Hour, now: time.Minute, scheduled: 0, attempt: 45, retryable: true},
		{name: "the 46th is final", grace: 24 * time.Hour, now: time.Minute, scheduled: 0, attempt: 46},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			stopping := StoppingCause{Since: t0, GraceEndsAt: t0.Add(tc.grace)}
			got, _ := stoppingPinRefusal(stopping, t0.Add(tc.now), t0.Add(tc.scheduled), tc.attempt)
			if got != tc.retryable {
				t.Fatalf("retryable = %v; want %v", got, tc.retryable)
			}
		})
	}
	t.Run("an unknown schedule counts from t0", func(t *testing.T) {
		stopping := StoppingCause{Since: t0, GraceEndsAt: t0.Add(24 * time.Hour)}
		if ok, _ := stoppingPinRefusal(stopping, t0.Add(4*time.Minute), time.Time{}, 1); !ok {
			t.Fatal("a creation with no schedule was refused finally inside five minutes of t0")
		}
		if ok, _ := stoppingPinRefusal(stopping, t0.Add(6*time.Minute), time.Time{}, 1); ok {
			t.Fatal("a creation with no schedule was retried past five minutes of t0")
		}
	})
}

// TestCordon_CreationDeliveredAtT0IsRefused: every creation that reaches a
// stopping pin host is refused, retryably and spaced, naming the stop.
func TestCordon_CreationDeliveredAtT0IsRefused(t *testing.T) {
	a := newCordonActivities(t, t.TempDir())
	env := activityEnv(a)
	ran := 0
	env.RegisterActivityWithOptions(func(context.Context) error {
		ran++
		return nil
	}, activity.RegisterOptions{Name: sessionCreationActivityName})
	a.Cordon().SetStopping(time.Now(), time.Now())

	_, err := env.ExecuteActivity(sessionCreationActivityName)
	var appErr *temporal.ApplicationError
	if !errors.As(err, &appErr) {
		t.Fatalf("a stopping pin host answered %v; want an ApplicationError", err)
	}
	if appErr.Type() != ReasonOrkerCordoned || appErr.NonRetryable() {
		t.Fatalf("refusal type=%q nonRetryable=%v; want retryable %s", appErr.Type(), appErr.NonRetryable(), ReasonOrkerCordoned)
	}
	if appErr.NextRetryDelay() != stoppingPinRetryDelay {
		t.Fatalf("refusal retry delay = %v; want %v", appErr.NextRetryDelay(), stoppingPinRetryDelay)
	}
	if !strings.Contains(appErr.Error(), "stopping") || !strings.Contains(appErr.Error(), cordonOrkerID) {
		t.Fatalf("refusal %q names neither the Orker nor the stop", appErr.Error())
	}
	if ran != 0 {
		t.Fatal("the creation activity ran on a stopping pin host")
	}
}

// TestActivities_AwaitIdle is the grace's early end: no running task and no
// open pin.
func TestActivities_AwaitIdle(t *testing.T) {
	a := newCordonActivities(t, t.TempDir())
	if err := a.AwaitIdle(context.Background()); err != nil {
		t.Fatalf("an idle Orker's wait = %v; want it to return at once", err)
	}
	a.running.add()
	a.pins.add("s1", "ork-r", time.Now())
	idle := make(chan error, 1)
	go func() { idle <- a.AwaitIdle(context.Background()) }()

	a.running.done()
	// Still a pin open: the wait goes on. A done context proves it would
	// block without a sub-second window.
	done, cancel := context.WithCancel(context.Background())
	cancel()
	if err := a.AwaitIdle(done); !errors.Is(err, context.Canceled) {
		t.Fatalf("AwaitIdle with a pin open = %v; want it to block", err)
	}
	a.pins.remove("s1")
	select {
	case err := <-idle:
		if err != nil {
			t.Fatalf("AwaitIdle = %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("AwaitIdle did not return once the last task and pin ended")
	}
}

// TestHeartbeater_BeatStoppingCarriesStoppingSince: the t0 beat goes at once,
// by task token, and every beat after it carries the instant of t0.
func TestHeartbeater_BeatStoppingCarriesStoppingSince(t *testing.T) {
	t0 := time.Date(2026, 11, 14, 9, 0, 0, 0, time.UTC)
	sent := make(chan HeartbeatProgress, 16)
	h := &Heartbeater{
		ackTimeout: time.Minute,
		interval:   time.Hour, // only the first beat and the t0 beat go
		send: func(_ context.Context, p HeartbeatProgress) error {
			sent <- p
			return nil
		},
	}
	h.Start(context.Background())
	defer h.Stop()
	<-sent // the loop's first beat
	if err := h.beatStopping(t0); err != nil {
		t.Fatalf("beatStopping = %v", err)
	}
	select {
	case p := <-sent:
		if p.StoppingSince != t0.Format(time.RFC3339Nano) {
			t.Fatalf("the t0 beat carried stopping_since %q; want %q", p.StoppingSince, t0.Format(time.RFC3339Nano))
		}
	default:
		t.Fatal("beatStopping returned before its beat was sent")
	}
	if got := h.progress().StoppingSince; got != t0.Format(time.RFC3339Nano) {
		t.Fatalf("a later beat carries stopping_since %q; want t0", got)
	}
	failing := &Heartbeater{send: func(context.Context, HeartbeatProgress) error { return errors.New("unreachable") }}
	if err := failing.beatStopping(t0); err == nil {
		t.Fatal("a t0 beat the cluster did not take reported success")
	}
}

// TestStop_T0BeatCarriesStoppingSinceWithinTwoSeconds: BeginStop sends the t0
// beat of every running unpinned attempt, including one whose loop starts
// after the stop began, and of no pinned member.
func TestStop_T0BeatCarriesStoppingSinceWithinTwoSeconds(t *testing.T) {
	a := newCordonActivities(t, t.TempDir())
	beats := make(chan string, 8)
	beater := func(name string) func(time.Time) error {
		return func(time.Time) error { beats <- name; return nil }
	}
	early := a.stops.admit()
	early.setStoppingBeat(beater("early"))
	pinned := a.stops.admit()
	pinned.setPinned()
	failing := a.stops.admit()
	failing.setStoppingBeat(func(time.Time) error { beats <- "failing"; return errors.New("unreachable") })
	late := a.stops.admit()

	a.BeginStop(StopSequence{Grace: time.Hour})
	got := map[string]bool{}
	for range 2 {
		got[<-beats] = true
	}
	if !got["early"] || !got["failing"] {
		t.Fatalf("t0 beats = %v; want early and failing", got)
	}
	late.setStoppingBeat(beater("late"))
	select {
	case name := <-beats:
		if name != "late" {
			t.Fatalf("beat %q; want late", name)
		}
	case <-time.After(stopDeadline):
		t.Fatal("an attempt whose loop started after t0 sent no t0 beat")
	}
	select {
	case name := <-beats:
		t.Fatalf("an extra t0 beat %q (a pinned member sends none)", name)
	default:
	}
}

// TestHeartbeater_AStoppingBeatAfterTheLoopEndedIsNotAFailure: an attempt that
// ended while its t0 beat was on its way has nothing to tell the cluster, so
// the stop must not warn that the beat was not acknowledged.
func TestHeartbeater_AStoppingBeatAfterTheLoopEndedIsNotAFailure(t *testing.T) {
	h := &Heartbeater{
		ackTimeout: time.Minute,
		interval:   time.Hour,
		send: func(call context.Context, _ HeartbeatProgress) error {
			<-call.Done()
			return call.Err()
		},
	}
	h.Start(context.Background())
	sent := make(chan error, 1)
	go func() { sent <- h.beatStopping(time.Now()) }()
	h.Stop()
	select {
	case err := <-sent:
		if err != nil {
			t.Fatalf("beatStopping after the loop ended = %v; want nil", err)
		}
	case <-time.After(stopDeadline):
		t.Fatal("beatStopping did not return once the loop ended")
	}
}

// TestStoppingPinRefusal_NamesTheBoundThatEndedIt: a final refusal says which
// bound ended the retries.
func TestStoppingPinRefusal_NamesTheBoundThatEndedIt(t *testing.T) {
	t0 := time.Date(2026, 11, 14, 9, 0, 0, 0, time.UTC)
	long := StoppingCause{Since: t0, GraceEndsAt: t0.Add(24 * time.Hour)}
	none := StoppingCause{Since: t0, GraceEndsAt: t0}
	cases := []struct {
		name     string
		stopping StoppingCause
		now      time.Duration
		attempt  int32
		want     string
	}{
		{name: "refusals", stopping: long, now: time.Minute, attempt: 46, want: "refused 45 times"},
		{name: "five minutes", stopping: long, now: 6 * time.Minute, attempt: 2, want: "five minutes"},
		{name: "the grace", stopping: none, now: 20 * time.Second, attempt: 2, want: "its stop grace has ended"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			retryable, why := stoppingPinRefusal(tc.stopping, t0.Add(tc.now), t0, tc.attempt)
			if retryable || !strings.Contains(why, tc.want) {
				t.Fatalf("stoppingPinRefusal = (%v, %q); want final, naming %q", retryable, why, tc.want)
			}
		})
	}
}

// TestHeartbeater_AServerDecisionOnTheStoppingBeatIsNotAFailure: a cancel or a
// run the cluster no longer knows answers the t0 beat as any beat; the loop
// acts on it, and the stop must not warn that the beat was lost.
func TestHeartbeater_AServerDecisionOnTheStoppingBeatIsNotAFailure(t *testing.T) {
	h := &Heartbeater{send: func(context.Context, HeartbeatProgress) error { return temporal.NewCanceledError() }}
	if err := h.beatStopping(time.Now()); err != nil {
		t.Fatalf("beatStopping answered by a cancel = %v; want nil", err)
	}
}

// cordonDuringReserve is the fixed-size supplier the task worker uses, whose
// reservation is granted only after a cordon landed while it waited, as a slot
// freed mid-stop is.
type cordonDuringReserve struct {
	worker.SlotSupplier
	cordon *Cordon
}

func (s *cordonDuringReserve) ReserveSlot(ctx context.Context, info worker.SlotReservationInfo) (*worker.SlotPermit, error) {
	s.cordon.SetStopping(time.Now(), time.Now().Add(time.Hour))
	return s.SlotSupplier.ReserveSlot(ctx, info)
}

// TestCordonSlotSupplier_AGrantAfterTheCordonLandedIsGivenBack: a reservation
// that waited for a free slot while the cordon landed does not poll: the slot
// goes back and the reservation waits on the cordon, so a stopping Orker
// polls for no task its open polls did not already hold.
func TestCordonSlotSupplier_AGrantAfterTheCordonLandedIsGivenBack(t *testing.T) {
	c := NewCordon()
	fixed, err := worker.NewFixedSizeSlotSupplier(1)
	must.OK(t, err)
	inner := &cordonDuringReserve{SlotSupplier: fixed, cordon: c}
	s := &cordonSlotSupplier{inner: inner, cordon: c}
	ctx, cancel := context.WithCancel(context.Background())
	got := make(chan error, 1)
	go func() {
		permit, err := s.ReserveSlot(ctx, nil)
		if permit != nil {
			err = errors.New("a permit was handed out after the cordon landed")
		}
		got <- err
	}()
	cancel()
	select {
	case err := <-got:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("ReserveSlot = %v; want it to wait on the cordon until its context ends", err)
		}
	case <-time.After(stopDeadline):
		t.Fatal("ReserveSlot did not return")
	}
	// The one slot went back: it can be taken again.
	if fixed.TryReserveSlot(nil) == nil {
		t.Fatal("the slot granted after the cordon was not given back")
	}
}
