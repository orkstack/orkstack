package orker

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"go.temporal.io/sdk/worker"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
)

// fakeStoreClock is the injected clock: it moves only when a test says so.
type fakeStoreClock struct{ at time.Time }

func (c *fakeStoreClock) now() time.Time { return c.at }

// recordingStoreObserver records what the telemetry seam was told.
type recordingStoreObserver struct {
	mu      sync.Mutex
	entered int
	left    []time.Duration
}

func (o *recordingStoreObserver) StoreOutageEntered() {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.entered++
}

func (o *recordingStoreObserver) StoreOutageLeft(lasted time.Duration) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.left = append(o.left, lasted)
}

var errStoreDown = errors.New("connection refused to the store at http://secret-endpoint:9000")

func TestStoreWatch_CordonsAfterConsecutiveFailuresAndResumesAfterConsecutiveSuccesses(t *testing.T) {
	const bad, good = false, true
	cases := []struct {
		name   string
		checks []bool
		want   bool // the outage holds after every check
	}{
		{"no check has run", nil, false},
		{"healthy", []bool{good, good, good}, false},
		{"one failure is a blip", []bool{bad}, false},
		{"one short of the threshold", []bool{bad, bad}, false},
		{"at the threshold", []bool{bad, bad, bad}, true},
		{"a good check resets the failures", []bool{bad, bad, good, bad, bad}, false},
		{"failures in a row after a blip", []bool{bad, good, bad, bad, bad}, true},
		{"one good check does not resume", []bool{bad, bad, bad, good}, true},
		{"resumes at the recovery threshold", []bool{bad, bad, bad, good, good}, false},
		{"a failure resets the recovery", []bool{bad, bad, bad, good, bad, good}, true},
		{"the recovery counts again from the failure", []bool{bad, bad, bad, good, bad, good, good}, false},
		{"a second outage needs its own failures", []bool{bad, bad, bad, good, good, bad, bad}, false},
		{"a second outage cordons again", []bool{bad, bad, bad, good, good, bad, bad, bad}, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cordon := NewCordon()
			clock := &fakeStoreClock{at: time.Date(2026, 11, 4, 9, 0, 0, 0, time.UTC)}
			healthy := true
			watch := newTestStoreWatch(cordon, clock, nil, func(context.Context) error {
				if healthy {
					return nil
				}
				return errStoreDown
			})
			for _, check := range tc.checks {
				healthy = check
				watch.Step(context.Background())
			}
			_, outage := cordon.Causes()
			if got := outage != nil; got != tc.want {
				t.Fatalf("after %v the outage holds = %v, want %v", tc.checks, got, tc.want)
			}
			if cordoned, _, _ := cordon.State(); cordoned != tc.want {
				t.Fatalf("after %v cordoned = %v, want %v", tc.checks, cordoned, tc.want)
			}
		})
	}
}

func TestStoreWatch_SaysWhyAndSinceWhen_AndKeepsTheSinceWhileItHolds(t *testing.T) {
	cordon := NewCordon()
	clock := &fakeStoreClock{at: time.Date(2026, 11, 4, 9, 0, 0, 0, time.UTC)}
	observer := &recordingStoreObserver{}
	check := func(context.Context) error { return errStoreDown }
	watch := newTestStoreWatch(cordon, clock, observer, func(ctx context.Context) error { return check(ctx) })

	watch.Step(context.Background())
	clock.at = clock.at.Add(time.Minute)
	watch.Step(context.Background())
	clock.at = clock.at.Add(time.Minute)
	entered := clock.at
	watch.Step(context.Background())
	clock.at = clock.at.Add(time.Hour)
	watch.Step(context.Background()) // still failing: since does not move

	_, outage := cordon.Causes()
	if outage == nil {
		t.Fatal("the outage does not hold")
	}
	if !outage.Since.Equal(entered) {
		t.Fatalf("since = %s, want the moment it was entered, %s", outage.Since, entered)
	}
	if !strings.HasPrefix(outage.Reason, "object storage unreachable") {
		t.Fatalf("reason = %q, want it to say object storage is unreachable", outage.Reason)
	}
	if strings.Contains(outage.Reason, "secret-endpoint") {
		t.Fatalf("reason = %q carries the store's raw error into the inventory", outage.Reason)
	}
	if observer.entered != 1 {
		t.Fatalf("entered told %d times, want once for one outage", observer.entered)
	}

	check = func(context.Context) error { return nil }
	watch.Step(context.Background())
	clock.at = clock.at.Add(time.Minute)
	watch.Step(context.Background())
	if _, outage := cordon.Causes(); outage != nil {
		t.Fatalf("the outage still holds after recovery: %+v", outage)
	}
	if want := []time.Duration{time.Hour + time.Minute}; len(observer.left) != 1 || observer.left[0] != want[0] {
		t.Fatalf("left told %v, want %v", observer.left, want)
	}
}

// The two causes are independent. Every row drives both through a real Cordon
// and reads what each answer says afterwards.
func TestCordon_TwoCausesAreIndependent(t *testing.T) {
	now := time.Date(2026, 11, 4, 9, 0, 0, 0, time.UTC)
	const set, clear, outage, recover = "set", "clear", "outage", "recover"
	cases := []struct {
		name         string
		steps        []string
		wantCordoned bool
		wantOperator bool
		wantOutage   bool
	}{
		{"boots clear", nil, false, false, false},
		{"operator alone", []string{set}, true, true, false},
		{"outage alone", []string{outage}, true, false, true},
		{"recovery never clears an operator's cordon", []string{set, outage, recover}, true, true, false},
		{"recovery with an operator's cordon set during the outage", []string{outage, set, recover}, true, true, false},
		{"an operator's uncordon does not override an outage", []string{outage, clear}, true, false, true},
		{"an operator's uncordon while both hold leaves the outage", []string{set, outage, clear}, true, false, true},
		{"both cleared, operator last", []string{set, outage, recover, clear}, false, false, false},
		{"both cleared, outage last", []string{set, outage, clear, recover}, false, false, false},
		{"recovery with no outage is a no-op", []string{recover}, false, false, false},
		{"uncordon with no cordon is a no-op", []string{clear}, false, false, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cordon := NewCordon()
			for _, step := range tc.steps {
				switch step {
				case set:
					cordon.Set("patching", now)
				case clear:
					cordon.Clear()
				case outage:
					cordon.SetStoreOutage("object storage unreachable", now)
				case recover:
					cordon.ClearStoreOutage()
				}
			}
			cordoned, _, _ := cordon.State()
			operatorCause, outageCause := cordon.Causes()
			if cordoned != tc.wantCordoned {
				t.Errorf("cordoned = %v, want %v", cordoned, tc.wantCordoned)
			}
			if (operatorCause != nil) != tc.wantOperator {
				t.Errorf("operator cause = %+v, want held=%v", operatorCause, tc.wantOperator)
			}
			if (outageCause != nil) != tc.wantOutage {
				t.Errorf("outage cause = %+v, want held=%v", outageCause, tc.wantOutage)
			}
		})
	}
}

// A reservation blocked on the cordon wakes only when the LAST cause clears.
func TestCordon_WaitWakesOnlyWhenEveryCauseHasCleared(t *testing.T) {
	now := time.Date(2026, 11, 4, 9, 0, 0, 0, time.UTC)
	cordon := NewCordon()
	cordon.Set("patching", now)
	cordon.SetStoreOutage("object storage unreachable", now)

	woke := make(chan error, 1)
	go func() { woke <- cordon.Wait(context.Background()) }()

	cordon.ClearStoreOutage()
	cordon.Set("patching still", now) // a round trip through the lock while the waiter blocks
	select {
	case err := <-woke:
		t.Fatalf("Wait returned %v with the operator's cordon still held", err)
	default:
	}
	cordon.Clear()
	if err := <-woke; err != nil {
		t.Fatalf("Wait after every cause cleared = %v", err)
	}
}

// A check that never answers must still count as failed: a hung store is the
// outage this exists for, and a watch stuck inside its own check would report
// nothing at all.
func TestStoreWatch_ACheckThatNeverAnswersCountsAsFailed(t *testing.T) {
	cordon := NewCordon()
	clock := &fakeStoreClock{at: time.Date(2026, 11, 4, 9, 0, 0, 0, time.UTC)}
	release := make(chan struct{})
	defer close(release)
	watch := newTestStoreWatch(cordon, clock, nil, func(context.Context) error {
		<-release // ignores its context, as a syscall on a hung mount does
		return nil
	})
	watch.cfg.Timeout = time.Millisecond

	for range 3 {
		watch.Step(context.Background())
	}
	_, outage := cordon.Causes()
	if outage == nil {
		t.Fatal("three checks that never answered did not cordon the Orker")
	}
	if !strings.Contains(outage.Reason, "did not answer") {
		t.Fatalf("reason = %q, want it to say the check did not answer", outage.Reason)
	}
}

// At most one check is ever outstanding, however long one hangs.
func TestStoreWatch_NeverStacksChecksBehindOneThatHangs(t *testing.T) {
	var mu sync.Mutex
	started := 0
	release := make(chan struct{})
	cordon := NewCordon()
	clock := &fakeStoreClock{at: time.Date(2026, 11, 4, 9, 0, 0, 0, time.UTC)}
	watch := newTestStoreWatch(cordon, clock, nil, func(context.Context) error {
		mu.Lock()
		started++
		mu.Unlock()
		<-release
		return nil
	})
	watch.cfg.Timeout = time.Millisecond
	for range 5 {
		watch.Step(context.Background())
	}
	mu.Lock()
	got := started
	mu.Unlock()
	close(release)
	if got != 1 {
		t.Fatalf("%d checks were started behind one that hung, want 1", got)
	}
}

func TestStoreWatch_ACheckCutShortByShutdownCountsForNothing(t *testing.T) {
	cordon := NewCordon()
	clock := &fakeStoreClock{at: time.Date(2026, 11, 4, 9, 0, 0, 0, time.UTC)}
	watch := newTestStoreWatch(cordon, clock, nil, func(ctx context.Context) error { return ctx.Err() })
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	for range 5 {
		watch.Step(ctx)
	}
	if _, outage := cordon.Causes(); outage != nil {
		t.Fatalf("a process stopping cordoned itself for an outage: %+v", outage)
	}
}

func TestStoreWatch_RunChecksOnItsCadenceAndStopsWithItsContext(t *testing.T) {
	cordon := NewCordon()
	clock := &fakeStoreClock{at: time.Date(2026, 11, 4, 9, 0, 0, 0, time.UTC)}
	checked := make(chan struct{}, 16)
	watch := newTestStoreWatch(cordon, clock, nil, func(context.Context) error {
		select {
		case checked <- struct{}{}:
		default:
		}
		return nil
	})
	watch.cfg.Interval = time.Millisecond
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { watch.Run(ctx); close(done) }()
	<-checked
	<-checked
	cancel()
	<-done
}

// The inventory says which cause holds, each with its own reason and since,
// and an operator's cordon reads exactly as it did before there were two.
func TestDescribeOrker_ReportsEachCauseOnItsOwnFields(t *testing.T) {
	at := time.Date(2026, 11, 4, 9, 0, 0, 0, time.UTC)
	later := at.Add(time.Hour)
	cases := []struct {
		name         string
		apply        func(*Cordon)
		wantCordoned bool
		wantReason   string
		wantOutage   string
	}{
		{"ready", func(*Cordon) {}, false, "", ""},
		{"operator", func(c *Cordon) { c.Set("patching", at) }, true, "patching", ""},
		{"outage", func(c *Cordon) { c.SetStoreOutage("object storage unreachable", later) }, true, "", "object storage unreachable"},
		{"both", func(c *Cordon) {
			c.Set("patching", at)
			c.SetStoreOutage("object storage unreachable", later)
		}, true, "patching", "object storage unreachable"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			activities := NewActivitiesWithOptions(nil, Options{WorkRoot: t.TempDir(), OrkerID: "orker-a", Runtime: nil})
			activities.dispatcher.Runtime = nil
			tc.apply(activities.Cordon())
			got, err := activities.DescribeOrker(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			if got.GetCordoned() != tc.wantCordoned || got.GetCordonReason() != tc.wantReason {
				t.Fatalf("cordoned=%v reason=%q, want %v %q", got.GetCordoned(), got.GetCordonReason(), tc.wantCordoned, tc.wantReason)
			}
			if got.GetStoreOutage().GetReason() != tc.wantOutage {
				t.Fatalf("store outage = %q, want %q", got.GetStoreOutage().GetReason(), tc.wantOutage)
			}
			if tc.wantOutage != "" && !got.GetStoreOutage().GetSince().AsTime().Equal(later) {
				t.Fatalf("outage since = %s, want %s", got.GetStoreOutage().GetSince().AsTime(), later)
			}
			if tc.wantReason != "" && !got.GetCordonedAt().AsTime().Equal(at) {
				t.Fatalf("cordoned_at = %s, want the operator's own %s", got.GetCordonedAt().AsTime(), at)
			}
			if tc.wantReason == "" && got.GetCordonedAt() != nil {
				t.Fatalf("cordoned_at = %s with no operator's cordon held", got.GetCordonedAt().AsTime())
			}
		})
	}
}

// The operator's uncordon, delivered as the API delivers it, leaves an outage.
func TestCordonOrkerActivity_UncordonLeavesAStoreOutage(t *testing.T) {
	activities := NewActivitiesWithOptions(nil, Options{WorkRoot: t.TempDir(), OrkerID: "orker-a"})
	activities.dispatcher.Runtime = nil
	activities.Cordon().SetStoreOutage("object storage unreachable", time.Now())
	got, err := activities.CordonOrker(context.Background(), &orkstackv1.CordonOrkerInput{Cordoned: false})
	if err != nil {
		t.Fatal(err)
	}
	if !got.GetCordoned() || got.GetStoreOutage() == nil {
		t.Fatalf("uncordon overrode an outage still failing: cordoned=%v outage=%v", got.GetCordoned(), got.GetStoreOutage())
	}
}

// A pin refused during an outage says why: the message reaches the task's
// PIN_UNAVAILABLE.
func TestCordonState_NamesTheOutageWhenItIsTheOnlyCause(t *testing.T) {
	cordon := NewCordon()
	cordon.SetStoreOutage("object storage unreachable: the last check failed", time.Now())
	if _, reason, _ := cordon.State(); !strings.Contains(reason, "object storage unreachable") {
		t.Fatalf("State reason = %q", reason)
	}
}

// The store outage uses the operator's seams: while it holds, a poller reserves
// no slot, and a recovery lets the blocked reservation through.
func TestCordonSlotSupplier_HoldsPollsThroughAStoreOutage(t *testing.T) {
	inner, err := worker.NewFixedSizeSlotSupplier(1)
	if err != nil {
		t.Fatal(err)
	}
	cordon := NewCordon()
	supplier := &cordonSlotSupplier{inner: inner, cordon: cordon}
	cordon.SetStoreOutage("object storage unreachable", time.Now())
	if permit := supplier.TryReserveSlot(nil); permit != nil {
		t.Fatal("an Orker in a store outage reserved a slot eagerly")
	}
	expired, cancel := context.WithTimeout(context.Background(), time.Millisecond)
	defer cancel()
	if _, err := supplier.ReserveSlot(expired, nil); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("ReserveSlot during an outage = %v, want it to wait until its context ended", err)
	}
	cordon.ClearStoreOutage()
	permit, err := supplier.ReserveSlot(context.Background(), nil)
	if err != nil || permit == nil {
		t.Fatalf("ReserveSlot after recovery = %v, %v", permit, err)
	}
}

func TestStoreWatchConfig_Validate(t *testing.T) {
	good := StoreWatchConfig{Enabled: true, Interval: 15 * time.Second, Timeout: 5 * time.Second, FailureThreshold: 3, RecoveryThreshold: 2}
	cases := []struct {
		name   string
		mutate func(*StoreWatchConfig)
		bad    string
	}{
		{"defaults", func(*StoreWatchConfig) {}, ""},
		{"timeout equal to the interval", func(c *StoreWatchConfig) { c.Timeout = c.Interval }, ""},
		{"interval too short", func(c *StoreWatchConfig) { c.Interval = 100 * time.Millisecond }, StoreCheckIntervalVar},
		{"interval too long", func(c *StoreWatchConfig) { c.Interval = 2 * time.Hour }, StoreCheckIntervalVar},
		{"timeout zero", func(c *StoreWatchConfig) { c.Timeout = 0 }, StoreCheckTimeoutVar},
		{"timeout above the interval", func(c *StoreWatchConfig) { c.Timeout = time.Minute }, StoreCheckTimeoutVar},
		{"failure threshold zero", func(c *StoreWatchConfig) { c.FailureThreshold = 0 }, StoreCheckFailuresVar},
		{"recovery threshold zero", func(c *StoreWatchConfig) { c.RecoveryThreshold = 0 }, StoreCheckRecoveriesVar},
		{"off ignores the rest", func(c *StoreWatchConfig) { *c = StoreWatchConfig{Enabled: false} }, ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := good
			tc.mutate(&cfg)
			err := cfg.Validate()
			if tc.bad == "" {
				if err != nil {
					t.Fatalf("Validate() = %v", err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), tc.bad) {
				t.Fatalf("Validate() = %v, want it to name %s", err, tc.bad)
			}
		})
	}
}

func newTestStoreWatch(cordon *Cordon, clock *fakeStoreClock, observer StoreOutageObserver, check StoreCheck) *StoreWatch {
	return &StoreWatch{
		cordon: cordon,
		check:  check,
		cfg: StoreWatchConfig{
			Enabled: true, Interval: time.Minute, Timeout: time.Minute,
			FailureThreshold: 3, RecoveryThreshold: 2,
		},
		now:      clock.now,
		observer: observer,
	}
}
