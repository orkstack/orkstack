package orker

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/interceptor"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/worker"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/task"
)

// newCordonActivities is an Activities with no store and no runtime:
// everything the cordon touches is process memory and the work root.
func newCordonActivities(t *testing.T, workRoot string) *Activities {
	t.Helper()
	return NewActivitiesWithOptions(nil, Options{
		OrkerID:  "orker-test",
		Pool:     "build",
		MaxPins:  2,
		WorkRoot: workRoot,
		Runtime:  &reapRecorder{labelled: map[string]int{}},
	})
}

func TestCordon_SetClearAndState(t *testing.T) {
	now := time.Date(2026, 9, 22, 8, 52, 10, 0, time.UTC)
	cases := []struct {
		name   string
		apply  func(c *Cordon)
		want   bool
		reason string
		since  time.Time
	}{
		{name: "a new cordon is clear", apply: func(*Cordon) {}},
		{name: "set", apply: func(c *Cordon) { c.Set("cve", now) }, want: true, reason: "cve", since: now},
		{name: "set twice replaces reason and since", apply: func(c *Cordon) {
			c.Set("first", now)
			c.Set("second", now.Add(time.Minute))
		}, want: true, reason: "second", since: now.Add(time.Minute)},
		{name: "set then clear", apply: func(c *Cordon) { c.Set("cve", now); c.Clear() }},
		{name: "clearing a clear cordon is a no-op", apply: func(c *Cordon) { c.Clear(); c.Clear() }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c := NewCordon()
			tc.apply(c)
			cordoned, reason, since := c.State()
			if cordoned != tc.want || reason != tc.reason || !since.Equal(tc.since) {
				t.Fatalf("State() = (%v, %q, %v); want (%v, %q, %v)",
					cordoned, reason, since, tc.want, tc.reason, tc.since)
			}
		})
	}

	t.Run("Wait returns at once when clear", func(t *testing.T) {
		if err := NewCordon().Wait(context.Background()); err != nil {
			t.Fatalf("Wait on a clear cordon = %v", err)
		}
	})
	t.Run("Wait blocks while set and wakes on Clear", func(t *testing.T) {
		c := NewCordon()
		c.Set("cve", now)
		done := make(chan error, 1)
		go func() { done <- c.Wait(context.Background()) }()
		select {
		case err := <-done:
			t.Fatalf("Wait returned %v while cordoned", err)
		case <-time.After(50 * time.Millisecond):
		}
		c.Clear()
		select {
		case err := <-done:
			if err != nil {
				t.Fatalf("Wait after Clear = %v", err)
			}
		case <-time.After(time.Second):
			t.Fatal("Wait did not wake on Clear")
		}
	})
	t.Run("Wait returns context.Canceled on cancel", func(t *testing.T) {
		c := NewCordon()
		c.Set("cve", now)
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan error, 1)
		go func() { done <- c.Wait(ctx) }()
		cancel()
		select {
		case err := <-done:
			if !errors.Is(err, context.Canceled) {
				t.Fatalf("Wait after cancel = %v; want context.Canceled", err)
			}
		case <-time.After(time.Second):
			t.Fatal("Wait did not return on cancel")
		}
	})
}

func TestCordonSlotSupplier_BlocksReservationWhileCordoned(t *testing.T) {
	inner, err := worker.NewFixedSizeSlotSupplier(1)
	if err != nil {
		t.Fatal(err)
	}
	c := NewCordon()
	s := &cordonSlotSupplier{inner: inner, cordon: c}
	if s.MaxSlots() != 1 {
		t.Fatalf("MaxSlots() = %d; want the inner supplier's 1", s.MaxSlots())
	}

	c.Set("cve", time.Now())
	if permit := s.TryReserveSlot(nil); permit != nil {
		t.Fatal("TryReserveSlot handed out a permit while cordoned")
	}
	got := make(chan *worker.SlotPermit, 1)
	go func() {
		permit, err := s.ReserveSlot(context.Background(), nil)
		if err != nil {
			t.Errorf("ReserveSlot: %v", err)
		}
		got <- permit
	}()
	select {
	case <-got:
		t.Fatal("ReserveSlot returned while cordoned")
	case <-time.After(100 * time.Millisecond):
	}
	c.Clear()
	var permit *worker.SlotPermit
	select {
	case permit = <-got:
	case <-time.After(100 * time.Millisecond):
		t.Fatal("ReserveSlot did not return a permit within 100ms of Clear")
	}
	if permit == nil {
		t.Fatal("ReserveSlot returned a nil permit")
	}
	// The one slot is held, so the next try is refused by the INNER supplier;
	// released, it is reusable.
	if s.TryReserveSlot(nil) != nil {
		t.Fatal("a fixed(1) supplier handed out a second slot")
	}
	s.ReleaseSlot(nil)
	if s.TryReserveSlot(nil) == nil {
		t.Fatal("a released permit was not reusable")
	}

	c.Set("again", time.Now())
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := s.ReserveSlot(ctx, nil); !errors.Is(err, context.Canceled) {
		t.Fatalf("ReserveSlot on a stopped worker while cordoned = %v; want context.Canceled", err)
	}
}

func TestRefusePinCreation_Table(t *testing.T) {
	cases := []struct {
		cordoned        bool
		attempt         int32
		refuse, retries bool
	}{
		{cordoned: false, attempt: 1},
		{cordoned: false, attempt: 1000},
		{cordoned: true, attempt: 1, refuse: true, retries: true},
		{cordoned: true, attempt: 45, refuse: true, retries: true},
		{cordoned: true, attempt: 46, refuse: true, retries: false},
	}
	for _, tc := range cases {
		refuse, retryable := refusePinCreation(tc.cordoned, tc.attempt)
		if refuse != tc.refuse || (refuse && retryable != tc.retries) {
			t.Errorf("refusePinCreation(%v, %d) = (%v, %v); want (%v, %v)",
				tc.cordoned, tc.attempt, refuse, retryable, tc.refuse, tc.retries)
		}
	}
}

// TestCordonRefusalBound_MatchesThePinCreationTimeout sums the SDK's session
// creation backoff (session.go: 1s × 1.1ⁿ⁻¹, capped at 10s) over the refusal
// bound: an all-cordoned pool must fail a pin in about the time a saturated
// one does, engine.pinCreationTimeout (5m, spelled here as a literal because
// the Orker never imports the engine).
func TestCordonRefusalBound_MatchesThePinCreationTimeout(t *testing.T) {
	const pinCreationTimeout = 5 * time.Minute
	var total time.Duration
	interval := float64(time.Second)
	for n := 1; n <= cordonPinRefusalAttempts; n++ {
		step := time.Duration(interval)
		if step > 10*time.Second {
			step = 10 * time.Second
		}
		total += step
		interval *= 1.1
	}
	if total < pinCreationTimeout-30*time.Second || total > pinCreationTimeout+30*time.Second {
		t.Fatalf("%d refused attempts take %v of backoff; want within 30s of %v",
			cordonPinRefusalAttempts, total, pinCreationTimeout)
	}
}

// activityEnv is a TestActivityEnvironment carrying the cordon's interceptor,
// which is how the shipped worker carries it.
func activityEnv(a *Activities) *testsuite.TestActivityEnvironment {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestActivityEnvironment()
	env.SetWorkerOptions(worker.Options{Interceptors: []interceptor.WorkerInterceptor{a.WorkerInterceptor()}})
	return env
}

func TestCordonInterceptor_RefusesSessionCreationWhileCordoned(t *testing.T) {
	a := newCordonActivities(t, t.TempDir())
	env := activityEnv(a)
	ran := 0
	openDuring := int32(-1)
	env.RegisterActivityWithOptions(func(ctx context.Context) error {
		ran++
		described, err := a.DescribeOrker(ctx)
		if err != nil {
			return err
		}
		openDuring = described.GetOpenPins()
		return nil
	}, activity.RegisterOptions{Name: sessionCreationActivityName})

	a.Cordon().Set("cve", time.Now())
	_, err := env.ExecuteActivity(sessionCreationActivityName)
	var appErr *temporal.ApplicationError
	if !errors.As(err, &appErr) {
		t.Fatalf("a cordoned host answered %v; want an ApplicationError", err)
	}
	if appErr.Type() != ReasonOrkerCordoned || appErr.NonRetryable() {
		t.Fatalf("refusal type=%q nonRetryable=%v; want retryable %s", appErr.Type(), appErr.NonRetryable(), ReasonOrkerCordoned)
	}
	if !strings.Contains(appErr.Error(), "cve") || !strings.Contains(appErr.Error(), "orker-test") {
		t.Fatalf("refusal %q names neither the Orker nor the reason", appErr.Error())
	}
	if ran != 0 {
		t.Fatal("the creation activity ran on a cordoned host")
	}

	a.Cordon().Clear()
	if _, err := env.ExecuteActivity(sessionCreationActivityName); err != nil {
		t.Fatalf("an uncordoned host refused a pin: %v", err)
	}
	if ran != 1 || openDuring != 1 {
		t.Fatalf("ran=%d open_pins during creation=%d; want 1 and 1", ran, openDuring)
	}
	described, _ := a.DescribeOrker(context.Background())
	if described.GetOpenPins() != 0 || len(described.GetPins()) != 0 {
		t.Fatalf("open_pins after the session ended = %d (%v); want 0", described.GetOpenPins(), described.GetPins())
	}
}

func TestCordonInterceptor_LetsOtherActivitiesThrough(t *testing.T) {
	a := newCordonActivities(t, t.TempDir())
	env := activityEnv(a)
	ran := map[string]bool{}
	for _, name := range []string{task.ActivityExecuteTask, task.ActivityDescribeOrker, task.ActivityReleasePin, task.ActivityCordonOrker, "internalSessionCompletionActivity"} {
		name := name
		env.RegisterActivityWithOptions(func(context.Context) error {
			ran[name] = true
			return nil
		}, activity.RegisterOptions{Name: name})
	}
	a.Cordon().Set("cve", time.Now())
	for name := range map[string]bool{task.ActivityExecuteTask: true, task.ActivityDescribeOrker: true, task.ActivityReleasePin: true, task.ActivityCordonOrker: true, "internalSessionCompletionActivity": true} {
		if _, err := env.ExecuteActivity(name); err != nil {
			t.Errorf("%s refused while cordoned: %v", name, err)
		}
		if !ran[name] {
			t.Errorf("%s did not run while cordoned", name)
		}
	}
}

func TestCordonInterceptor_CountsRunningExecuteTasks(t *testing.T) {
	a := newCordonActivities(t, t.TempDir())
	env := activityEnv(a)
	during := int32(-1)
	// A test function under the shipped name: the count is taken at the SDK
	// seam, so it does not matter which function is registered.
	env.RegisterActivityWithOptions(func(ctx context.Context) error {
		described, err := a.DescribeOrker(ctx)
		if err != nil {
			return err
		}
		during = described.GetRunningTasks()
		return nil
	}, activity.RegisterOptions{Name: task.ActivityExecuteTask})
	if _, err := env.ExecuteActivity(task.ActivityExecuteTask); err != nil {
		t.Fatal(err)
	}
	if during != 1 {
		t.Fatalf("running_tasks while ExecuteTask ran = %d; want 1", during)
	}
	described, _ := a.DescribeOrker(context.Background())
	if described.GetRunningTasks() != 0 {
		t.Fatalf("running_tasks after = %d; want 0", described.GetRunningTasks())
	}
}

func TestDescribeOrker_ReportsCordonAndLoad(t *testing.T) {
	a := newCordonActivities(t, t.TempDir())
	ctx := context.Background()
	after, err := a.CordonOrker(ctx, &orkstackv1.CordonOrkerInput{Cordoned: true, Reason: "cve"})
	if err != nil {
		t.Fatal(err)
	}
	if !after.GetCordoned() || after.GetCordonReason() != "cve" || after.GetCordonedAt() == nil {
		t.Fatalf("after cordon: cordoned=%v reason=%q at=%v", after.GetCordoned(), after.GetCordonReason(), after.GetCordonedAt())
	}
	if after.GetOrkerId() != "orker-test" || after.GetPool() != "build" || after.GetMaxConcurrentPins() != 2 {
		t.Fatalf("the post-state lost the identity: %v", after)
	}
	if after.GetRunningTasks() != 0 || after.GetOpenPins() != 0 {
		t.Fatalf("an idle Orker reports load: %v", after)
	}
	cleared, err := a.CordonOrker(ctx, &orkstackv1.CordonOrkerInput{Cordoned: false, Reason: "ignored"})
	if err != nil {
		t.Fatal(err)
	}
	if cleared.GetCordoned() || cleared.GetCordonReason() != "" || cleared.GetCordonedAt() != nil {
		t.Fatalf("after uncordon: cordoned=%v reason=%q at=%v", cleared.GetCordoned(), cleared.GetCordonReason(), cleared.GetCordonedAt())
	}
}

func TestCordonOrker_RefusesAnEmptyReason(t *testing.T) {
	for _, reason := range []string{"", "   ", strings.Repeat("x", 257), "a\nb", "a\x00b"} {
		a := newCordonActivities(t, t.TempDir())
		_, err := a.CordonOrker(context.Background(), &orkstackv1.CordonOrkerInput{Cordoned: true, Reason: reason})
		var appErr *temporal.ApplicationError
		if !errors.As(err, &appErr) || appErr.Type() != ReasonOrkerCordonInvalid || !appErr.NonRetryable() {
			t.Errorf("reason %q: err = %v; want non-retryable %s", reason, err, ReasonOrkerCordonInvalid)
		}
		if cordoned, _, _ := a.Cordon().State(); cordoned {
			t.Errorf("reason %q: the flag was set by a refused cordon", reason)
		}
	}
}

func writeLease(t *testing.T, root, dir string, lease *pinLease, raw string) {
	t.Helper()
	path := filepath.Join(root, dir)
	if err := os.MkdirAll(path, 0o700); err != nil {
		t.Fatal(err)
	}
	if lease != nil {
		encoded, err := json.Marshal(lease)
		if err != nil {
			t.Fatal(err)
		}
		raw = string(encoded)
	}
	if raw != "" {
		mustWrite(t, filepath.Join(path, pinLeaseFileName), raw)
	}
}

func TestListOpenPinLeases_ReadsLeasesAndSkipsUndecodable(t *testing.T) {
	root := t.TempDir()
	expires := time.Date(2026, 9, 22, 11, 26, 51, 0, time.UTC)
	writeLease(t, root, "ork-pin-ork-a-build-00000000-0000-0000-0000-000000000001",
		&pinLease{ExpiresAt: expires.Format(time.RFC3339Nano), RunID: "ork-a", Pin: "build"}, "")
	writeLease(t, root, "ork-pin-ork-b-test-00000000-0000-0000-0000-000000000002",
		&pinLease{ExpiresAt: expires.Add(time.Hour).Format(time.RFC3339Nano), RunID: "ork-b", Pin: "test"}, "")
	writeLease(t, root, "ork-pin-ork-c-x-00000000-0000-0000-0000-000000000003", nil, "")          // no lease
	writeLease(t, root, "ork-pin-ork-d-x-00000000-0000-0000-0000-000000000004", nil, "{not json") // undecodable
	writeLease(t, root, "not-a-pin", &pinLease{ExpiresAt: expires.Format(time.RFC3339Nano), RunID: "ork-z", Pin: "z"}, "")

	leases, err := ListOpenPinLeases(root)
	if err != nil {
		t.Fatal(err)
	}
	if len(leases) != 2 {
		t.Fatalf("ListOpenPinLeases = %+v; want the two decodable leases", leases)
	}
	byRun := map[string]PinLeaseInfo{}
	for _, lease := range leases {
		byRun[lease.RunID] = lease
	}
	if got := byRun["ork-a"]; got.Pin != "build" || !got.ExpiresAt.Equal(expires) {
		t.Fatalf("ork-a lease = %+v", got)
	}
	if got := byRun["ork-b"]; got.Pin != "test" {
		t.Fatalf("ork-b lease = %+v", got)
	}

	// DescribeOrker joins them onto the interceptor's map by run id, and a
	// session with no directory yet is still counted, undecorated.
	a := newCordonActivities(t, root)
	since := time.Date(2026, 9, 22, 9, 0, 0, 0, time.UTC)
	a.pins.add("creation-1", "ork-a", since)
	a.pins.add("creation-2", "ork-new", since.Add(time.Minute))
	described, err := a.DescribeOrker(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if described.GetOpenPins() != 2 || len(described.GetPins()) != 2 {
		t.Fatalf("open_pins=%d pins=%v; want 2 and 2", described.GetOpenPins(), described.GetPins())
	}
	first, second := described.GetPins()[0], described.GetPins()[1]
	if first.GetRunId() != "ork-a" || first.GetPin() != "build" || !first.GetExpiresAt().AsTime().Equal(expires) ||
		!first.GetSince().AsTime().Equal(since) {
		t.Fatalf("the joined pin = %v", first)
	}
	if second.GetRunId() != "ork-new" || second.GetPin() != "" || second.GetExpiresAt() != nil {
		t.Fatalf("a session with no directory = %v; want run id only, pin and expires_at unset", second)
	}
}

func TestBoot_RefusesAnInvalidOrkerID(t *testing.T) {
	a := NewActivitiesWithOptions(nil, Options{
		OrkerID:  "a b",
		WorkRoot: t.TempDir(),
		Runtime:  &reapRecorder{labelled: map[string]int{}},
	})
	defer a.Close()
	err := a.Boot(context.Background())
	if err == nil || !strings.Contains(err.Error(), "ORKER_ID_INVALID") || !strings.Contains(err.Error(), OrkerIDVar) {
		t.Fatalf("Boot with ORK_ORKER_ID=%q = %v; want ORKER_ID_INVALID naming %s", "a b", err, OrkerIDVar)
	}
}
