package provider_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/orkstack/orkstack/internal/provider"
	"github.com/orkstack/orkstack/internal/provider/internal/scripted"
	"github.com/orkstack/orkstack/internal/task"
	"github.com/orkstack/orkstack/pkg/logstream"
)

// fakeClock is the engine's two clocks under the test's hand. The refresh
// schedule, the hold limit, the grace and expiry are all read from it, so
// every case runs in no wall time.
type fakeClock struct {
	mu    sync.Mutex
	boot  time.Duration
	wall  time.Time
	fired []func()
}

func newFakeClock() *fakeClock {
	return &fakeClock{boot: time.Hour, wall: time.Date(2026, 11, 17, 9, 0, 0, 0, time.UTC)}
}

func (c *fakeClock) Boot() time.Duration { c.mu.Lock(); defer c.mu.Unlock(); return c.boot }
func (c *fakeClock) Wall() time.Time     { c.mu.Lock(); defer c.mu.Unlock(); return c.wall }

func (c *fakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	c.boot += d
	c.wall = c.wall.Add(d)
	fired := c.fired
	c.mu.Unlock()
	for _, f := range fired {
		f()
	}
}

// budget is a setup budget measured on the fake clock: once the clock passes
// it, the context is done with DeadlineExceeded, as a real setup budget is.
type budget struct {
	context.Context
	clock *fakeClock
	until time.Duration
	once  sync.Once
	done  chan struct{}
}

func newBudget(clock *fakeClock, d time.Duration) *budget {
	b := &budget{Context: context.Background(), clock: clock, until: clock.Boot() + d, done: make(chan struct{})}
	clock.mu.Lock()
	clock.fired = append(clock.fired, func() {
		if clock.Boot() >= b.until {
			b.once.Do(func() { close(b.done) })
		}
	})
	clock.mu.Unlock()
	return b
}

func (b *budget) Done() <-chan struct{} { return b.done }
func (b *budget) Err() error {
	select {
	case <-b.done:
		return context.DeadlineExceeded
	default:
		return nil
	}
}
func (b *budget) Deadline() (time.Time, bool) { return time.Time{}, false }

// refusingMasker is the real masker that a test can make refuse every value:
// the masker's bound being reached by a provider's grown values is a fault
// case the real masker reaches only with thousands of forms.
type refusingMasker struct {
	*logstream.Masker
	mu     sync.Mutex
	refuse bool
}

func (m *refusingMasker) Add(holder string, value []byte) (bool, error) {
	m.mu.Lock()
	refuse := m.refuse
	m.mu.Unlock()
	if refuse {
		return false, &logstream.MaskCapacityError{}
	}
	return m.Masker.Add(holder, value)
}

type rig struct {
	closes int
	t      *testing.T
	clock  *fakeClock
	p      *scripted.Provider
	masker *refusingMasker
	engine *provider.Engine
	root   string
	stops  chan provider.Failure
	mu     sync.Mutex
	lines  []string
	tokens int
	refuse *provider.TokenRefused
}

type rigOption func(*provider.EngineConfig)

func withInstanceMaxTTL(d time.Duration) rigOption {
	return func(c *provider.EngineConfig) { c.Attempt.Instance = true; c.InstanceMaxTTL = d }
}

func withSessionLife(r *rig, d time.Duration) { r.p.SetSessionLifetime(d) }

func withCallTimeout(d time.Duration) rigOption {
	return func(c *provider.EngineConfig) { c.CallTimeout = d }
}

func withPolicy(p provider.RefreshPolicy) rigOption {
	return func(c *provider.EngineConfig) { c.Credentials[0].Policy = p }
}

func asInstance() rigOption {
	return func(c *provider.EngineConfig) { c.Attempt.Instance = true }
}

func newRig(t *testing.T, life time.Duration, renewals int, opts ...rigOption) *rig {
	t.Helper()
	r := &rig{t: t, clock: newFakeClock(), stops: make(chan provider.Failure, 4)}
	r.p = scripted.New("kv", "vault.internal")
	r.p.Admit("payments-app")
	r.p.SetLifetime(life, renewals)
	r.masker = &refusingMasker{Masker: logstream.NewMasker()}
	r.root = filepath.Join(tempRoot(t), "credentials")
	parsed, err := r.p.Parse(provider.Ref{Kind: provider.KindCredential, Locator: "creds/payments-rw"})
	if err != nil {
		t.Fatal(err)
	}
	cfg := provider.EngineConfig{
		Attempt: provider.Attempt{Namespace: "payments", RunID: "ork-1", TaskID: "migrate", Attempt: 1},
		Grants: map[string]provider.Grant{"vault-app": {Provider: r.p, Principal: "payments-app", Token: func(context.Context) (string, time.Duration, error) {
			r.mu.Lock()
			defer r.mu.Unlock()
			if r.refuse != nil {
				return "", 0, r.refuse
			}
			r.tokens++
			return fmt.Sprintf("identity-token-%04d", r.tokens), time.Hour, nil
		}}},
		Credentials: []provider.CredentialSpec{{Name: "db", Grant: "vault-app", Ref: parsed}},
		Root:        r.root,
		Masker:      r.masker,
		Clock:       r.clock,
		Unit:        func() float64 { return 0.5 },
		Sleep: func(ctx context.Context, d time.Duration) error {
			r.clock.Advance(d)
			return ctx.Err()
		},
		Hooks: provider.Hooks{
			TaskLine: func(line string) { r.mu.Lock(); r.lines = append(r.lines, line); r.mu.Unlock() },
			Stop:     func(f provider.Failure) { r.stops <- f },
			Closed:   func(string, error) { r.mu.Lock(); r.closes++; r.mu.Unlock() },
		},
	}
	for _, opt := range opts {
		opt(&cfg)
	}
	r.engine, err = provider.NewEngine(cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(r.engine.Close)
	return r
}

func (r *rig) setup() {
	r.t.Helper()
	ctx := context.Background()
	if err := r.engine.Open(ctx); err != nil {
		r.t.Fatalf("Open: %v", err)
	}
	if err := r.engine.Issue(ctx); err != nil {
		r.t.Fatalf("Issue: %v", err)
	}
}

// tickAt advances the clock by d, runs every tick that falls due on the way,
// and runs the watchdog after each, as Run and its ticker would.
func (r *rig) tickFor(d time.Duration) {
	r.t.Helper()
	end := r.clock.Boot() + d
	for {
		next := r.engine.Tick(context.Background())
		r.engine.Check()
		now := r.clock.Boot()
		if now >= end {
			return
		}
		step := end - now
		if next >= 0 && next < step {
			step = next
		}
		if step > provider.WatchdogInterval {
			step = provider.WatchdogInterval
		}
		if step <= 0 {
			step = time.Second
		}
		r.clock.Advance(step)
	}
}

func (r *rig) file(name string) string {
	r.t.Helper()
	b, err := os.ReadFile(filepath.Join(r.root, "db", name))
	if err != nil {
		r.t.Fatalf("read %s: %v", name, err)
	}
	return string(b)
}

// leapFor is tickFor from one due time to the next, for a run of days.
func (r *rig) leapFor(d time.Duration) {
	r.t.Helper()
	end := r.clock.Boot() + d
	for {
		next := r.engine.Tick(context.Background())
		r.engine.Check()
		now := r.clock.Boot()
		if now >= end {
			return
		}
		step := end - now
		if next >= 0 && next < step {
			step = max(next, time.Second)
		}
		r.clock.Advance(step)
	}
}

func (r *rig) masked(value string) bool { return r.masker.Mask(value) != value }

func (r *rig) stopped() (provider.Failure, bool) {
	select {
	case f := <-r.stops:
		return f, true
	case <-time.After(2 * time.Second):
		return provider.Failure{}, false
	}
}

func (r *rig) notStopped() {
	r.t.Helper()
	select {
	case f := <-r.stops:
		r.t.Fatalf("the container was stopped: %+v", f)
	case <-time.After(50 * time.Millisecond):
	}
}

func (r *rig) linesWith(s string) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	n := 0
	for _, line := range r.lines {
		if strings.Contains(line, s) {
			n++
		}
	}
	return n
}

func TestEngine_SetupPutsTheBundleInPlaceMasked(t *testing.T) {
	r := newRig(t, 30*time.Minute, 0)
	r.setup()
	password := r.file("password")
	if password != "password-serial-0001-secret-0" || r.file("username") != "user-serial-0001" {
		t.Fatalf("files = %q, %q", password, r.file("username"))
	}
	if !r.masked(password) {
		t.Error("the password is not masked once it is in place")
	}
	if r.masked("user-serial-0001") {
		t.Error("a plain file is masked")
	}
	if got := r.file("expiration"); got != "2026-11-17T09:30:00Z\n" {
		t.Errorf("expiration = %q", got)
	}
	if states := r.engine.States(); len(states) != 1 || states[0].Condition != "fresh" {
		t.Errorf("States() = %+v", states)
	}
}

// TestEngine_RefreshesAtAThirdRenewingWhileAllowed: nothing is asked before a
// third of the bundle's life; a renewal is used while the provider extends
// it, then a new bundle is issued.
func TestEngine_RefreshesAtAThirdRenewingWhileAllowed(t *testing.T) {
	r := newRig(t, 30*time.Minute, 1)
	r.setup()
	r.tickFor(9 * time.Minute)
	if r.file("password") != "password-serial-0001-secret-0" || r.p.Issued() != 1 {
		t.Fatal("refreshed before a third of the life")
	}
	r.tickFor(3 * time.Minute) // 12 min: past 10 + 1.25 (the draw is the middle)
	if r.file("password") != "password-serial-0001-secret-1" || r.p.Issued() != 1 {
		t.Fatalf("not renewed at a third: %q, issued %d", r.file("password"), r.p.Issued())
	}
	r.tickFor(12 * time.Minute)
	if r.p.Issued() != 2 || r.file("password") != "password-serial-0002-secret-0" {
		t.Fatalf("not re-issued once renewal stopped: %q, issued %d", r.file("password"), r.p.Issued())
	}
	r.notStopped()
}

// TestEngine_MaskerBeforeRename: at the moment the new files become current,
// the masker already holds their values.
func TestEngine_MaskerBeforeRename(t *testing.T) {
	r := newRig(t, 30*time.Minute, 0)
	r.setup()
	checked := false
	provider.SetAfterWrite(r.engine, "db", func() error {
		checked = true
		if !r.masked("password-serial-0002-secret-0") {
			t.Error("the new value is not masked when its files are about to become current")
		}
		return nil
	})
	r.tickFor(13 * time.Minute)
	if !checked {
		t.Fatal("no refresh reached the hook")
	}
}

// TestEngine_AReplacedValueStaysMaskedUntilItsExpiryPlusTheGrace: a process
// may hold and print the old value until it expires; past that and the grace
// it leaves the masker, and the value in place stays.
func TestEngine_AReplacedValueStaysMaskedUntilItsExpiryPlusTheGrace(t *testing.T) {
	r := newRig(t, 30*time.Minute, 0)
	r.setup()
	old := "password-serial-0001-secret-0"
	r.tickFor(13 * time.Minute)
	if r.p.Issued() != 2 {
		t.Fatalf("issued %d", r.p.Issued())
	}
	r.tickFor(20 * time.Minute) // 33 min: the old bundle expired at 30, the grace runs to 35
	if !r.masked(old) {
		t.Fatal("the replaced value left the masker before its expiry plus the grace")
	}
	r.tickFor(3 * time.Minute) // 36 min
	if r.masked(old) {
		t.Error("the replaced value is still masked past its expiry plus the grace")
	}
	if !r.masked(r.file("password")) {
		t.Error("the value in place is not masked")
	}
}

func TestEngine_UnavailableUntilExpiryStopsARequiredCredential(t *testing.T) {
	r := newRig(t, 30*time.Minute, 0)
	r.setup()
	r.p.Down()
	r.tickFor(29 * time.Minute)
	r.notStopped()
	if r.linesWith("refresh failed") != 1 {
		t.Errorf("%d lines say the refresh failed; want one at the first failure", r.linesWith("refresh failed"))
	}
	if states := r.engine.States(); states[0].Condition != "retrying" {
		t.Errorf("States() = %+v; want retrying", states)
	}
	r.tickFor(2 * time.Minute)
	f, ok := r.stopped()
	if !ok {
		t.Fatal("a required credential that expired unrefreshed did not stop the container")
	}
	if f.Reason != provider.ReasonCredentialRefreshFailed || f.Category != task.CategoryInfrastructureError || !f.Retryable {
		t.Errorf("stopped with %+v; want CREDENTIAL_REFRESH_FAILED, InfrastructureError, retried", f.Outcome)
	}
	if !strings.Contains(f.Detail, "credential db") || !strings.Contains(f.Detail, "did not answer") {
		t.Errorf("detail %q does not name the credential and the cause", f.Detail)
	}
}

func TestEngine_AnOutageThatEndsBeforeExpiryFailsNothing(t *testing.T) {
	r := newRig(t, 30*time.Minute, 0)
	r.setup()
	r.p.Down()
	r.tickFor(20 * time.Minute)
	r.p.Up()
	r.tickFor(20 * time.Minute)
	r.notStopped()
	if r.linesWith("refreshed again") != 1 {
		t.Errorf("no line said the credential recovered")
	}
	if r.p.Issued() < 2 {
		t.Errorf("issued %d; the refresh did not land after the outage", r.p.Issued())
	}
}

func TestEngine_ABestEffortCredentialKeepsItsFilesAtExpiry(t *testing.T) {
	r := newRig(t, 30*time.Minute, 0, withPolicy(provider.RefreshBestEffort))
	r.setup()
	r.p.Down()
	r.tickFor(40 * time.Minute)
	r.notStopped()
	if r.linesWith("expired") != 1 {
		t.Errorf("%d lines say it expired; want one", r.linesWith("expired"))
	}
	if r.file("password") != "password-serial-0001-secret-0" {
		t.Error("the files of an expired best_effort credential were removed")
	}
	r.p.Up()
	r.tickFor(time.Minute)
	if r.p.Issued() != 2 {
		t.Errorf("the loop stopped trying a best_effort credential: issued %d", r.p.Issued())
	}
}

func TestEngine_ARefusalBelievedOverThirtySecondsStopsNow(t *testing.T) {
	for _, tc := range []struct {
		name     string
		opts     []rigOption
		category string
		retry    bool
	}{
		{"a task", nil, task.CategoryContractViolation, false},
		{"an event source's instance", []rigOption{asInstance()}, task.CategoryInfrastructureError, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := newRig(t, 30*time.Minute, 0, tc.opts...)
			r.setup()
			r.p.Script("credential", scripted.Refused("issue"), scripted.Refused("issue"), scripted.Refused("issue"), scripted.Refused("issue"))
			r.tickFor(11*time.Minute + 20*time.Second)
			r.notStopped()
			r.tickFor(40 * time.Second)
			f, ok := r.stopped()
			if !ok {
				t.Fatal("a believed refusal did not stop the container")
			}
			if f.Reason != provider.ReasonCredentialRefused || f.Category != tc.category || f.Retryable != tc.retry {
				t.Errorf("stopped with %+v", f.Outcome)
			}
			issued := r.p.Issued()
			r.tickFor(time.Hour)
			if r.p.Issued() != issued {
				t.Error("the loop asked again after a believed refusal")
			}
		})
	}
}

func TestEngine_TwoRefusalsThenAnAnswerFailNothing(t *testing.T) {
	r := newRig(t, 30*time.Minute, 0)
	r.setup()
	r.p.Script("credential", scripted.Refused("issue"), scripted.Refused("issue"))
	r.tickFor(20 * time.Minute)
	r.notStopped()
	if r.p.Issued() != 2 {
		t.Errorf("issued %d; the third try should have landed", r.p.Issued())
	}
}

// TestEngine_AWriteFaultRetriesWithTheBundleItHolds: the provider is asked
// once, the write is retried with what it gave, and nothing more is issued;
// a fault past the hold limit drops it and starts from a fresh token.
func TestEngine_AWriteFaultRetriesWithTheBundleItHolds(t *testing.T) {
	r := newRig(t, 30*time.Minute, 0)
	r.setup()
	var fault sync.Mutex
	failing := true
	provider.SetAfterWrite(r.engine, "db", func() error {
		fault.Lock()
		defer fault.Unlock()
		if failing {
			return errors.New("no space left on device")
		}
		return nil
	})
	r.tickFor(11*time.Minute + 20*time.Second) // due at 11:15
	if r.p.Issued() != 2 {
		t.Fatalf("issued %d after the first try", r.p.Issued())
	}
	r.tickFor(40 * time.Second)
	if r.p.Issued() != 2 {
		t.Fatalf("issued %d; a write fault asked the provider again", r.p.Issued())
	}
	fault.Lock()
	failing = false
	fault.Unlock()
	r.tickFor(15 * time.Second)
	if got := r.file("password"); got != "password-serial-0002-secret-0" {
		t.Fatalf("after the fault ended the file is %q; want the bundle that was held", got)
	}
	r.notStopped()
}

func TestEngine_AWriteFaultPastTheHoldLimitDropsTheBundle(t *testing.T) {
	r := newRig(t, 30*time.Minute, 0)
	r.setup()
	var fault sync.Mutex
	failing := true
	provider.SetAfterWrite(r.engine, "db", func() error {
		fault.Lock()
		defer fault.Unlock()
		if failing {
			return errors.New("no space left on device")
		}
		return nil
	})
	r.tickFor(11*time.Minute + 20*time.Second)
	held := "password-serial-0002-secret-0"
	if !r.masked(held) {
		t.Fatal("the held bundle is not masked while it is held")
	}
	r.tickFor(2 * time.Minute)
	if r.p.Issued() < 3 {
		t.Fatalf("issued %d; past the hold limit the next try starts afresh", r.p.Issued())
	}
	if !r.masked(held) {
		t.Error("a dropped bundle left the masker at once; its files were readable while it was held")
	}
	fault.Lock()
	failing = false
	fault.Unlock()
	r.tickFor(2 * time.Minute)
	if got := r.file("password"); got == held || !strings.HasPrefix(got, "password-serial-") {
		t.Errorf("after the fault ended the file is %q", got)
	}
	r.notStopped()
	r.tickFor(35 * time.Minute) // past the dropped bundle's expiry plus the grace
	if r.masked(held) {
		t.Error("a dropped bundle's values are masked past its expiry plus the grace")
	}
}

// TestEngine_AMaskerThatWillNotTakeTheValuesStopsAskingForThatCredential:
// the new files are not put in place, the provider is asked no more, and at
// expiry a required credential stops the container, naming the cause.
func TestEngine_AMaskerThatWillNotTakeTheValuesStopsAskingForThatCredential(t *testing.T) {
	r := newRig(t, 30*time.Minute, 0)
	r.setup()
	r.masker.mu.Lock()
	r.masker.refuse = true
	r.masker.mu.Unlock()
	r.tickFor(13 * time.Minute)
	if r.file("password") != "password-serial-0001-secret-0" {
		t.Fatal("files the masker refused were put in place")
	}
	if r.linesWith("refreshed no more") != 1 {
		t.Errorf("no line said the credential is refreshed no more")
	}
	issued := r.p.Issued()
	r.tickFor(10 * time.Minute)
	if r.p.Issued() != issued {
		t.Errorf("the loop asked the provider again after the masker refused")
	}
	r.tickFor(10 * time.Minute)
	f, ok := r.stopped()
	if !ok || f.Reason != provider.ReasonCredentialRefreshFailed || !strings.Contains(f.Detail, "masker") {
		t.Fatalf("at expiry: %+v, %v; want CREDENTIAL_REFRESH_FAILED naming the masker", f, ok)
	}
}

func TestEngine_ARevokedIdentityStopsTheContainerWithItsOwnReason(t *testing.T) {
	r := newRig(t, 30*time.Minute, 0)
	r.setup()
	// A provider that mints a fresh token for each issue (aws) passes the
	// engine's refusal through; the scripted one answers with it.
	r.p.Script("credential", &provider.TokenRefused{Reason: "IDENTITY_REVOKED"})
	r.tickFor(13 * time.Minute)
	f, ok := r.stopped()
	if !ok || f.Reason != "IDENTITY_REVOKED" || f.Category != task.CategoryContractViolation {
		t.Fatalf("stopped with %+v, %v; want IDENTITY_REVOKED", f, ok)
	}
}

func TestEngine_SetupRefusals(t *testing.T) {
	t.Run("a login refused three times over thirty seconds", func(t *testing.T) {
		r := newRig(t, 30*time.Minute, 0)
		r.p.Script("login", scripted.Refused("login"), scripted.Refused("login"), scripted.Refused("login"))
		start := r.clock.Boot()
		err := r.engine.Open(context.Background())
		var f *provider.Failure
		if !errors.As(err, &f) || f.Reason != provider.ReasonCredentialRefused {
			t.Fatalf("Open = %v; want CREDENTIAL_REFUSED", err)
		}
		if waited := r.clock.Boot() - start; waited < provider.RefusalWindow {
			t.Errorf("believed after %s; a refusal stands %s first", waited, provider.RefusalWindow)
		}
	})
	t.Run("a missing secret, at once", func(t *testing.T) {
		r := newRig(t, 30*time.Minute, 0)
		if err := r.engine.Open(context.Background()); err != nil {
			t.Fatal(err)
		}
		parsed, _ := r.p.Parse(provider.Ref{Kind: provider.KindSecret, Locator: "secret/data/none#x"})
		start := r.clock.Boot()
		_, err := r.engine.Secret(context.Background(), "db-url", "vault-app", parsed)
		var f *provider.Failure
		if !errors.As(err, &f) || f.Reason != provider.ReasonSecretNotFound || r.clock.Boot() != start {
			t.Fatalf("Secret = %v after %s; want SECRET_NOT_FOUND at once", err, r.clock.Boot()-start)
		}
	})
	t.Run("a refused secret", func(t *testing.T) {
		r := newRig(t, 30*time.Minute, 0)
		if err := r.engine.Open(context.Background()); err != nil {
			t.Fatal(err)
		}
		r.p.SetSecret("secret/data/db", []byte("s3cr3t-value"))
		r.p.Script("secret", scripted.Refused("read"), scripted.Refused("read"), scripted.Refused("read"))
		parsed, _ := r.p.Parse(provider.Ref{Kind: provider.KindSecret, Locator: "secret/data/db"})
		_, err := r.engine.Secret(context.Background(), "db-url", "vault-app", parsed)
		var f *provider.Failure
		if !errors.As(err, &f) || f.Reason != provider.ReasonSecretAccessDenied {
			t.Fatalf("Secret = %v; want SECRET_ACCESS_DENIED", err)
		}
	})
	t.Run("a secret read and masked", func(t *testing.T) {
		r := newRig(t, 30*time.Minute, 0)
		if err := r.engine.Open(context.Background()); err != nil {
			t.Fatal(err)
		}
		r.p.SetSecret("secret/data/db", []byte("s3cr3t-value"))
		parsed, _ := r.p.Parse(provider.Ref{Kind: provider.KindSecret, Locator: "secret/data/db#url"})
		v, err := r.engine.Secret(context.Background(), "db-url", "vault-app", parsed)
		if err != nil || string(v.Bytes) != "s3cr3t-value" || !r.masked("s3cr3t-value") {
			t.Fatalf("Secret = %q, %v; masked %v", v.Bytes, err, r.masked("s3cr3t-value"))
		}
	})
	t.Run("an outage past the setup budget", func(t *testing.T) {
		r := newRig(t, 30*time.Minute, 0)
		r.p.Down()
		err := r.engine.Open(newBudget(r.clock, 2*time.Minute))
		var f *provider.Failure
		if !errors.As(err, &f) || f.Reason != provider.ReasonProviderUnavailable || !f.Retryable {
			t.Fatalf("Open = %v; want PROVIDER_UNAVAILABLE, retried", err)
		}
	})
	t.Run("a cancellation", func(t *testing.T) {
		r := newRig(t, 30*time.Minute, 0)
		r.p.Down()
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		if err := r.engine.Open(ctx); !errors.Is(err, context.Canceled) {
			t.Fatalf("Open = %v; want the cancellation itself", err)
		}
	})
}

func TestEngine_CloseEndsEveryLogin(t *testing.T) {
	r := newRig(t, 30*time.Minute, 0)
	r.setup()
	if r.p.Open() != 1 {
		t.Fatalf("%d sessions open", r.p.Open())
	}
	r.engine.Close()
	r.engine.Close()
	if r.p.Open() != 0 {
		t.Errorf("%d sessions open after Close", r.p.Open())
	}
}

// TestEngine_AHungCallDoesNotDelayTheWatchdog: while a refresh's call is
// stuck in the provider, the watchdog still sees the expiry and stops the
// container.
func TestEngine_AHungCallDoesNotDelayTheWatchdog(t *testing.T) {
	r := newRig(t, 30*time.Minute, 0)
	r.setup()
	release := make(chan struct{})
	r.p.Hang("credential", release)
	r.clock.Advance(13 * time.Minute)
	done := make(chan struct{})
	go func() {
		r.engine.Tick(context.Background())
		close(done)
	}()
	r.clock.Advance(20 * time.Minute)
	r.engine.Check()
	if _, ok := r.stopped(); !ok {
		t.Fatal("the watchdog did not stop the container while a call hung")
	}
	close(release)
	<-done
}

// TestEngine_AReplacedLoginClosesOnlyAfterItsBundlesAreReplaced: a login at
// its end is replaced, every credential of the grant is issued again from the
// new one, and only then is the old one closed, since a Vault lease dies with
// the token that made it.
func TestEngine_AReplacedLoginClosesOnlyAfterItsBundlesAreReplaced(t *testing.T) {
	r := newRig(t, 2*time.Hour, 0)
	withSessionLife(r, 30*time.Minute)
	r.setup()
	if r.p.Open() != 1 {
		t.Fatalf("%d sessions open after setup", r.p.Open())
	}
	r.p.Script("credential", scripted.Unavailable("issue"))
	r.tickFor(11*time.Minute + 20*time.Second) // the login is due at 11:15
	if r.p.Open() != 2 {
		t.Fatalf("%d sessions open; want the new login beside the old", r.p.Open())
	}
	if r.p.RevokedSerial("serial-0001") || r.file("password") != "password-serial-0001-secret-0" {
		t.Fatal("the old login closed before its bundle was replaced")
	}
	r.tickFor(time.Minute)
	if r.file("password") != "password-serial-0002-secret-0" {
		t.Fatalf("the bundle was not issued again from the new login: %q", r.file("password"))
	}
	// A process may still hold what the old login issued, until it expires.
	r.tickFor(100 * time.Minute)
	if r.p.RevokedSerial("serial-0001") {
		t.Error("the old login closed while its bundle had not expired")
	}
	r.tickFor(10 * time.Minute) // past the two hours of the first bundle
	if !r.p.RevokedSerial("serial-0001") {
		t.Error("the old login is open once its bundle expired")
	}
	r.notStopped()
}

// TestEngine_AnInstancesReplacedLoginClosesAtOnce: what an event source's old
// login issued is revoked once the new one's works (providers.md §6).
func TestEngine_AnInstancesReplacedLoginClosesAtOnce(t *testing.T) {
	r := newRig(t, 2*time.Hour, 0, asInstance())
	withSessionLife(r, 30*time.Minute)
	r.setup()
	r.tickFor(12 * time.Minute)
	if r.p.Open() != 1 || !r.p.RevokedSerial("serial-0001") || r.file("password") != "password-serial-0002-secret-0" {
		t.Errorf("%d open, file %q", r.p.Open(), r.file("password"))
	}
}

// capRig is a task that has run 24 hours less a little: its login was renewed
// all along and its credential with it, so nothing has been asked of the
// engine since it started.
func capRig(t *testing.T, opts ...rigOption) *rig {
	t.Helper()
	r := newRig(t, 8*time.Hour, 1000, opts...)
	withSessionLife(r, 30*time.Minute)
	r.p.SetLoginRenewals(1000, 30*time.Minute)
	r.setup()
	r.leapFor(23*time.Hour + 55*time.Minute)
	if len(r.p.Tokens()) != 1 || r.p.Issued() != 1 {
		t.Fatalf("%d logins, %d issued before the cap; want one of each", len(r.p.Tokens()), r.p.Issued())
	}
	r.notStopped()
	return r
}

// TestEngine_ATasksLoginIsReplacedAt24Hours: at 24 hours since it was made a
// task's login is replaced, however often it was renewed. The task runs on:
// nothing stops it, its files are swapped by one rename to a whole new set, the
// value it held stays masked while it may still print it, and what the old
// login issued is not revoked before it expires.
func TestEngine_ATasksLoginIsReplacedAt24Hours(t *testing.T) {
	r := capRig(t)
	dir := filepath.Join(r.root, "db")
	before, err := os.Readlink(filepath.Join(dir, "current"))
	if err != nil {
		t.Fatal(err)
	}
	old := r.file("password")
	opened, err := os.Open(filepath.Join(dir, "password"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = opened.Close() }()
	r.tickFor(6 * time.Minute)
	if len(r.p.Tokens()) != 2 || r.p.Issued() != 2 {
		t.Fatalf("%d logins, %d issued past the cap; want a new login and a new bundle", len(r.p.Tokens()), r.p.Issued())
	}
	after, err := os.Readlink(filepath.Join(dir, "current"))
	if err != nil || after == before {
		t.Fatalf("current is %q (%v); it was %q", after, err, before)
	}
	if r.file("password") == old || !strings.Contains(r.file("password"), "serial-0002") {
		t.Fatalf("the file holds %q; it held %q", r.file("password"), old)
	}
	if held, err := io.ReadAll(opened); err != nil || string(held) != old {
		t.Fatalf("a process that had the old file open reads %q, %v; want %q", held, err, old)
	}
	if _, err := os.Stat(filepath.Join(dir, before, "password")); err != nil {
		t.Fatalf("the replaced set is gone: %v", err)
	}
	if !r.masked(old) || !r.masked(r.file("password")) {
		t.Error("the replaced value, or the new one, is not masked")
	}
	if r.p.RevokedSerial("serial-0001") {
		t.Error("what the old login issued was revoked while a process may hold it")
	}
	r.notStopped()
	r.leapFor(24 * time.Hour)
	if len(r.p.Tokens()) != 3 {
		t.Errorf("%d logins after two days; want one a day", len(r.p.Tokens()))
	}
	r.notStopped()
}

// TestEngine_ANewLoginRefusedAtTheCap: what a new login's answer does at the
// cap is the refresh table's, and nothing else: a refusal stops a required
// credential's container with its own reason and leaves a best_effort one;
// no answer leaves both running on what they hold.
func TestEngine_ANewLoginRefusedAtTheCap(t *testing.T) {
	refused := []error{scripted.Refused("login"), scripted.Refused("login"), scripted.Refused("login"), scripted.Refused("login")}
	down := make([]error, 2000)
	for i := range down {
		down[i] = scripted.Unavailable("login")
	}
	for name, tc := range map[string]struct {
		policy provider.RefreshPolicy
		answer []error
		stops  bool
	}{
		"required, refused":      {provider.RefreshRequired, refused, true},
		"best_effort, refused":   {provider.RefreshBestEffort, refused, false},
		"required, no answer":    {provider.RefreshRequired, down[:5], false},
		"best_effort, no answer": {provider.RefreshBestEffort, down[:5], false},
	} {
		t.Run(name, func(t *testing.T) {
			r := capRig(t, withPolicy(tc.policy))
			held := r.file("password")
			r.p.Script("login", tc.answer...)
			r.tickFor(7 * time.Minute)
			if !tc.stops {
				r.notStopped()
				if len(r.p.Tokens()) < 2 || !strings.Contains(r.file("password"), "password-") {
					t.Errorf("%d logins asked; the task holds %q, it held %q", len(r.p.Tokens()), r.file("password"), held)
				}
				return
			}
			if f, ok := r.stopped(); !ok || f.Reason != provider.ReasonCredentialRefused {
				t.Errorf("stopped %v with %+v; want CREDENTIAL_REFUSED", ok, f)
			}
		})
	}
	// Unanswered to the end, the credential's own expiry decides.
	r := capRig(t)
	r.p.Script("login", down...)
	r.leapFor(9 * time.Hour)
	if f, ok := r.stopped(); !ok || f.Reason != provider.ReasonCredentialRefreshFailed {
		t.Errorf("stopped %v with %+v; want CREDENTIAL_REFRESH_FAILED at the credential's expiry", ok, f)
	}
}

// TestEngine_ALoginIsRenewedInPlace: a login that can be extended is, with no
// token and no new bundle, until its provider will not; then it is replaced
// as one that cannot be.
func TestEngine_ALoginIsRenewedInPlace(t *testing.T) {
	r := newRig(t, 4*time.Hour, 0)
	withSessionLife(r, 30*time.Minute)
	r.p.SetLoginRenewals(2, 30*time.Minute)
	r.setup()
	r.tickFor(25 * time.Minute) // due at 11:15, and again as long after
	if r.p.Open() != 1 || len(r.p.Tokens()) != 1 || r.p.Issued() != 1 {
		t.Fatalf("%d open, %d logins, %d issued while the login could be renewed; want one of each", r.p.Open(), len(r.p.Tokens()), r.p.Issued())
	}
	r.tickFor(15 * time.Minute)
	if len(r.p.Tokens()) != 2 || r.file("password") != "password-serial-0002-secret-0" || r.p.RevokedSerial("serial-0001") {
		t.Fatalf("at its maximum: %d open, %d logins, file %q; want the login and its bundle replaced", r.p.Open(), len(r.p.Tokens()), r.file("password"))
	}
	r.notStopped()
}

// TestEngine_ALoginThatIsNotRenewed: an answer that is not a longer life has
// the engine log in again at once, with a fresh token, so a refusal is never
// believed from a renewal; one that did not come is asked again.
func TestEngine_ALoginThatIsNotRenewed(t *testing.T) {
	for name, tc := range map[string]struct {
		life   time.Duration
		answer error
		logins int
	}{
		"refused":                  {30 * time.Minute, scripted.Refused("renew-login"), 2},
		"at its maximum":           {30 * time.Minute, provider.ErrReissue, 2},
		"no longer than what left": {10 * time.Minute, nil, 2},
		"a life of nothing":        {0, nil, 2},
		"below a life":             {-time.Minute, nil, 2},
		"a panic":                  {30 * time.Minute, scripted.Panic(), 2},
		"no answer":                {30 * time.Minute, scripted.Unavailable("renew-login"), 1},
	} {
		t.Run(name, func(t *testing.T) {
			r := newRig(t, 4*time.Hour, 0)
			withSessionLife(r, 30*time.Minute)
			r.p.SetLoginRenewals(100, tc.life)
			r.setup()
			if tc.answer != nil {
				r.p.Script("renew-login", tc.answer)
			}
			r.tickFor(13 * time.Minute)
			if got := len(r.p.Tokens()); got != tc.logins {
				t.Errorf("%d logins; want %d", got, tc.logins)
			}
			r.notStopped()
		})
	}
}

// TestEngine_ARenewalNeverAnsweredIsALoginAtItsEnd: a login that ended while its
// renewal was not answered is never renewed again: the engine logs in.
func TestEngine_ARenewalNeverAnsweredIsALoginAtItsEnd(t *testing.T) {
	r := newRig(t, 4*time.Hour, 0)
	withSessionLife(r, 30*time.Minute)
	r.p.SetLoginRenewals(100, 30*time.Minute)
	r.setup()
	down := make([]error, 200)
	for i := range down {
		down[i] = scripted.Unavailable("renew-login")
	}
	r.p.Script("renew-login", down...)
	r.tickFor(32 * time.Minute)
	if got := len(r.p.Tokens()); got != 2 {
		t.Errorf("%d logins once the login had ended; want 2", got)
	}
	r.notStopped()
}

// TestEngine_AnInstancesLoginIsNotRenewedPastTheCap: however often an event
// source's login is renewed, it is replaced from a fresh token at the cap
// since it was made, since a renewal asks the engine nothing.
func TestEngine_AnInstancesLoginIsNotRenewedPastTheCap(t *testing.T) {
	r := newRig(t, 8*time.Hour, 0, withInstanceMaxTTL(time.Hour))
	withSessionLife(r, 30*time.Minute)
	r.p.SetLoginRenewals(100, 30*time.Minute)
	r.setup()
	r.tickFor(55 * time.Minute)
	if got := len(r.p.Tokens()); got != 1 {
		t.Fatalf("%d logins before the cap; renewals should have served", got)
	}
	r.tickFor(7 * time.Minute)
	if got := len(r.p.Tokens()); got != 2 {
		t.Fatalf("%d logins past the cap; want a fresh one at it", got)
	}
	r.tickFor(time.Hour)
	if got := len(r.p.Tokens()); got < 3 {
		t.Errorf("%d logins two hours in; the cap is from the last login", got)
	}
}

// TestEngine_ALoginThatReadASecretIsKept: a secret is read once and never
// refreshed, so the login it was read through is not revoked from a task when
// it is replaced; the attempt's end closes it.
func TestEngine_ALoginThatReadASecretIsKept(t *testing.T) {
	r := newRig(t, 2*time.Hour, 0)
	withSessionLife(r, 30*time.Minute)
	r.p.SetSecret("secret/db", []byte("a-static-value"))
	r.setup()
	if _, err := r.engine.Secret(context.Background(), "db-url", "vault-app", provider.Parsed{Ref: provider.Ref{Kind: provider.KindSecret, Locator: "secret/db"}}); err != nil {
		t.Fatal(err)
	}
	r.leapFor(6 * time.Hour) // past every bundle of the first login
	if r.p.RevokedSerial("serial-0001") {
		t.Error("the login that read the secret was closed while the task may hold it")
	}
	r.engine.Close()
	if r.p.Open() != 0 || !r.p.RevokedSerial("serial-0001") {
		t.Errorf("%d sessions open after Close", r.p.Open())
	}
}

// TestEngine_ReplacedLoginsAreBounded: a login that cannot be renewed is
// replaced often, and its month-long bundles would keep every one open.
func TestEngine_ReplacedLoginsAreBounded(t *testing.T) {
	r := newRig(t, 30*24*time.Hour, 0)
	withSessionLife(r, 30*time.Minute)
	r.p.SetSecret("secret/db", []byte("a-static-value"))
	r.setup()
	if _, err := r.engine.Secret(context.Background(), "db-url", "vault-app", provider.Parsed{Ref: provider.Ref{Kind: provider.KindSecret, Locator: "secret/db"}}); err != nil {
		t.Fatal(err)
	}
	r.leapFor(12 * time.Hour) // some 64 logins
	if got := r.p.Open(); len(r.p.Tokens()) < 40 || got > 34 {
		t.Fatalf("%d sessions open after %d logins; want at most the bound, the current one and one in place", got, len(r.p.Tokens()))
	}
	if r.p.RevokedSerial("serial-0001") || !r.p.RevokedSerial("serial-0002") {
		t.Error("past the bound the login that read the secret goes before an older one's bundles")
	}
	r.notStopped()
	r.engine.Close()
	if r.p.Open() != 0 {
		t.Errorf("%d sessions open after Close", r.p.Open())
	}
}

func TestEngine_AFailedLoginIsRetried(t *testing.T) {
	r := newRig(t, 2*time.Hour, 0)
	withSessionLife(r, 30*time.Minute)
	r.setup()
	r.p.Script("login", scripted.Unavailable("login"), scripted.Unavailable("login"))
	r.tickFor(11*time.Minute + 20*time.Second)
	if r.p.Open() != 1 {
		t.Fatalf("%d sessions open after a failed login", r.p.Open())
	}
	r.tickFor(2 * time.Minute)
	if len(r.p.Tokens()) != 4 || r.file("password") != "password-serial-0002-secret-0" {
		t.Errorf("after the retry: %d open, file %q; want the new login's bundle", r.p.Open(), r.file("password"))
	}
}

// TestEngine_AnInstanceLogsInAgainAtTheCap: for an event source's instance,
// a renewable bundle is renewed only until the cap, then a new login from a
// fresh token issues a new one.
func TestEngine_AnInstanceLogsInAgainAtTheCap(t *testing.T) {
	r := newRig(t, 30*time.Minute, 100, withInstanceMaxTTL(time.Hour))
	r.setup()
	r.tickFor(55 * time.Minute)
	if r.p.Issued() != 1 {
		t.Fatalf("issued %d before the cap; renewals should have served", r.p.Issued())
	}
	r.tickFor(20 * time.Minute)
	if r.p.Issued() < 2 {
		t.Fatalf("issued %d past the cap; a new bundle from a fresh login was due", r.p.Issued())
	}
	if tokens := len(r.p.Tokens()); tokens < 2 {
		t.Errorf("%d logins; the cap asks for a fresh token", tokens)
	}
}

func TestEngine_CloseClosesReplacedLoginsToo(t *testing.T) {
	r := newRig(t, 2*time.Hour, 0)
	withSessionLife(r, 30*time.Minute)
	r.setup()
	r.p.Script("credential", scripted.Unavailable("issue"), scripted.Unavailable("issue"))
	r.tickFor(11*time.Minute + 20*time.Second)
	if r.p.Open() != 2 {
		t.Fatalf("%d sessions open", r.p.Open())
	}
	r.engine.Close()
	if r.p.Open() != 0 {
		t.Errorf("%d sessions open after Close", r.p.Open())
	}
}

// TestEngine_AValueThatNeverExpiresStaysMaskedOnceReplaced: a bundle with no
// lifetime, replaced when its login was, stays masked for the attempt.
func TestEngine_AValueThatNeverExpiresStaysMaskedOnceReplaced(t *testing.T) {
	r := newRig(t, 0, 0)
	withSessionLife(r, 30*time.Minute)
	r.setup()
	old := r.file("password")
	r.tickFor(3 * time.Hour)
	if r.file("password") == old {
		t.Fatal("the bundle was not replaced with its login")
	}
	if !r.masked(old) {
		t.Error("a replaced value that never expires left the masker")
	}
}

// TestEngine_NoProviderMessageReachesAFailureOrALine: an error that is not a
// *provider.Error, or wraps one in a message, is described by its class and
// code alone.
func TestEngine_NoProviderMessageReachesAFailureOrALine(t *testing.T) {
	const leak = "hvs.CAESIJ-secret-token"
	r := newRig(t, 30*time.Minute, 0)
	raw := errors.New("POST https://vault.internal/v1/auth/jwt/login?role=prod-admin: 403 body={\"token\":\"" + leak + "\"}")
	r.p.Script("login", raw, raw, raw, raw, raw, raw, raw, raw)
	err := r.engine.Open(newBudget(r.clock, 2*time.Minute))
	if err == nil || strings.Contains(err.Error(), leak) || strings.Contains(err.Error(), "prod-admin") {
		t.Fatalf("Open = %v; it must fail and say nothing of the provider's message", err)
	}

	r = newRig(t, 30*time.Minute, 0)
	r.setup()
	wrapped := fmt.Errorf("issuing for role prod-admin with %s: %w", leak, scripted.Unavailable("issue"))
	r.p.Script("credential", wrapped, wrapped, wrapped)
	r.tickFor(13 * time.Minute)
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.lines) == 0 {
		t.Fatal("no line said the refresh failed")
	}
	for _, line := range r.lines {
		if strings.Contains(line, leak) || strings.Contains(line, "prod-admin") {
			t.Errorf("a task line holds the provider's message: %q", line)
		}
	}
}

// TestEngine_AnUnbelievedRefusalAtTheBudgetsEndIsUnavailable: a refusal not
// yet believed when the setup budget runs out ends the attempt retried, as an
// outage would.
func TestEngine_AnUnbelievedRefusalAtTheBudgetsEndIsUnavailable(t *testing.T) {
	r := newRig(t, 30*time.Minute, 0)
	r.p.Script("login", scripted.Refused("login"), scripted.Refused("login"))
	err := r.engine.Open(newBudget(r.clock, 20*time.Second))
	var f *provider.Failure
	if !errors.As(err, &f) || f.Reason != provider.ReasonProviderUnavailable || !f.Retryable {
		t.Fatalf("Open = %v; want PROVIDER_UNAVAILABLE, retried", err)
	}
}

// TestEngine_AnInstancesMissingSecretIsStillAContractViolation: only a
// refusal becomes a retried infrastructure error for an instance.
func TestEngine_AnInstancesMissingSecretIsStillAContractViolation(t *testing.T) {
	r := newRig(t, 30*time.Minute, 0, asInstance())
	if err := r.engine.Open(context.Background()); err != nil {
		t.Fatal(err)
	}
	parsed, _ := r.p.Parse(provider.Ref{Kind: provider.KindSecret, Locator: "secret/data/none"})
	_, err := r.engine.Secret(context.Background(), "db-url", "vault-app", parsed)
	var f *provider.Failure
	if !errors.As(err, &f) || f.Reason != provider.ReasonSecretNotFound || f.Category != task.CategoryContractViolation || f.Retryable {
		t.Fatalf("Secret = %+v; want SECRET_NOT_FOUND, a ContractViolation", err)
	}
}

// TestEngine_OneGrantsFailureEndsTheOthersSetup: a believed refusal on one
// grant does not wait for another grant's outage to use up the budget.
func TestEngine_OneGrantsFailureEndsTheOthersSetup(t *testing.T) {
	r := newRig(t, 30*time.Minute, 0)
	other := scripted.New("sts", "sts.amazonaws.com")
	other.Down()
	parsed, _ := r.p.Parse(provider.Ref{Kind: provider.KindCredential, Locator: "creds/payments-rw"})
	engine, err := provider.NewEngine(provider.EngineConfig{
		Grants: map[string]provider.Grant{
			"a-refused": {Provider: r.p, Principal: "nobody", Token: func(context.Context) (string, time.Duration, error) { return "tok-a-0001", time.Hour, nil }},
			"b-down":    {Provider: other, Principal: "role", Token: func(context.Context) (string, time.Duration, error) { return "tok-b-0001", time.Hour, nil }},
		},
		Credentials: []provider.CredentialSpec{{Name: "db", Grant: "a-refused", Ref: parsed}},
		Root:        r.root,
		Masker:      r.masker,
		Clock:       r.clock,
		Unit:        func() float64 { return 0.5 },
		Sleep: func(ctx context.Context, d time.Duration) error {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(time.Millisecond):
			}
			r.clock.Advance(d / 2) // two grants sleep; the clock moves once for both
			return ctx.Err()
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close()
	budget := newBudget(r.clock, time.Hour)
	err = engine.Open(budget)
	var f *provider.Failure
	if !errors.As(err, &f) || f.Reason != provider.ReasonCredentialRefused {
		t.Fatalf("Open = %v; want the refused grant's CREDENTIAL_REFUSED", err)
	}
	if budget.Err() != nil {
		t.Error("Open waited out the budget for the grant that was down")
	}
}

// TestEngine_AShortSecretIsSaidToBeUnmasked: a value the masker cannot hold
// (under its minimum) is not silently unmasked: one line says so.
func TestEngine_AShortSecretIsSaidToBeUnmasked(t *testing.T) {
	r := newRig(t, 30*time.Minute, 0)
	if err := r.engine.Open(context.Background()); err != nil {
		t.Fatal(err)
	}
	r.p.SetSecret("secret/data/pin", []byte("1234"))
	parsed, _ := r.p.Parse(provider.Ref{Kind: provider.KindSecret, Locator: "secret/data/pin"})
	if _, err := r.engine.Secret(context.Background(), "pin", "vault-app", parsed); err != nil {
		t.Fatal(err)
	}
	if r.linesWith("is not masked") != 1 {
		t.Errorf("no line said the short secret is not masked: %q", r.lines)
	}
}

// TestEngine_ARefusalWithoutAReasonsShapeIsNotBelieved: a TokenRefused whose
// reason is not a reason code is "could not be asked", retried, and never
// stored as a reason of its own.
func TestEngine_ARefusalWithoutAReasonsShapeIsNotBelieved(t *testing.T) {
	r := newRig(t, 30*time.Minute, 0)
	r.setup()
	forged := &provider.TokenRefused{Reason: "SEE https://evil.example/\nIDENTITY_REVOKED"}
	r.p.Script("credential", forged)
	r.tickFor(13 * time.Minute)
	r.notStopped()
	if r.linesWith("refresh failed") != 1 {
		t.Errorf("a malformed refusal was not treated as a failed refresh")
	}
}

// TestEngine_APanickingProviderFailsItsCallOnly: a provider that panics in a
// call fails that call as unavailable; the engine and the process go on.
func TestEngine_APanickingProviderFailsItsCallOnly(t *testing.T) {
	r := newRig(t, 30*time.Minute, 0)
	r.setup()
	r.p.Script("credential", scripted.Panic())
	r.tickFor(13 * time.Minute)
	r.notStopped()
	if r.linesWith("refresh failed") != 1 {
		t.Errorf("the panic was not a failed refresh")
	}
	r.tickFor(30 * time.Second)
	if r.p.Issued() < 2 {
		t.Errorf("the loop did not go on after a panicking call")
	}
}

// eventually waits, in real time, for cond: a late answer arrives on the
// abandoned call's own goroutine.
func eventually(t *testing.T, cond func() bool) bool {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return true
		}
		time.Sleep(5 * time.Millisecond)
	}
	return cond()
}

// TestEngine_ALoginThatAnswersLateIsClosed: a login given up on at its call
// timeout, at setup and at a relogin, that answers afterwards is closed once
// and never used.
func TestEngine_ALoginThatAnswersLateIsClosed(t *testing.T) {
	t.Run("at a relogin", func(t *testing.T) {
		r := newRig(t, 2*time.Hour, 0, withCallTimeout(50*time.Millisecond))
		withSessionLife(r, 30*time.Minute)
		r.setup()
		release := make(chan struct{})
		r.p.Hang("login", release)
		r.tickFor(11*time.Minute + 20*time.Second) // the relogin is given up on
		close(release)
		if !eventually(t, func() bool { r.mu.Lock(); defer r.mu.Unlock(); return r.closes == 1 }) {
			t.Fatal("the late login was not closed")
		}
		if r.p.Open() != 1 {
			t.Errorf("%d sessions open; want the first login alone", r.p.Open())
		}
		if r.file("password") != "password-serial-0001-secret-0" {
			t.Error("a bundle was issued from the abandoned login")
		}
	})
	t.Run("at setup", func(t *testing.T) {
		r := newRig(t, 2*time.Hour, 0, withCallTimeout(50*time.Millisecond))
		release := make(chan struct{})
		r.p.Hang("login", release)
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		go func() { time.Sleep(200 * time.Millisecond); close(release) }()
		if err := r.engine.Open(ctx); err != nil {
			t.Fatalf("Open = %v; the retry after the abandoned login should succeed", err)
		}
		if !eventually(t, func() bool { r.mu.Lock(); defer r.mu.Unlock(); return r.closes == 1 }) {
			t.Fatal("the late login was not closed")
		}
		if r.p.Open() != 1 {
			t.Errorf("%d sessions open; want the retry's login alone", r.p.Open())
		}
	})
}

func TestEngine_AProviderThatPanicsInCloseDoesNotTakeTheOrkerDown(t *testing.T) {
	r := newRig(t, 30*time.Minute, 0)
	r.setup()
	r.p.PanicOnClose()
	r.engine.Close()
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closes != 1 {
		t.Errorf("%d closes counted; the panicking close is a failed close", r.closes)
	}
}
