package orker

import (
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"sync"
	"testing"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// phaseObserverCtxKey is the value fakePhaseObserver.Begin stamps onto the
// context it returns, so a test can prove that ctx — not the one Execute was
// originally called with — is what reaches the container runtime.
type phaseObserverCtxKey struct{}

// fakePhaseObserver is the smallest PhaseObserver: it records Begin's
// arguments, every Phase call in order, and the single End call, and it
// marks the context it returns from Begin so a fake runtime can prove that
// exact context is what Create and Start see.
type fakePhaseObserver struct {
	mu      sync.Mutex
	begins  int
	beginIn *orkstackv1.ExecuteTaskInput
	attempt int32
	phases  []string
	ends    int
	outcome AttemptOutcome
}

func (f *fakePhaseObserver) Begin(
	ctx context.Context, in *orkstackv1.ExecuteTaskInput, attempt int32,
) (context.Context, AttemptObserver) {
	f.mu.Lock()
	f.begins++
	f.beginIn = in
	f.attempt = attempt
	f.mu.Unlock()
	return context.WithValue(ctx, phaseObserverCtxKey{}, "from-begin"), &fakeAttemptObserver{f: f}
}

func (f *fakePhaseObserver) recorded() (int, []string, int, AttemptOutcome) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.begins, append([]string(nil), f.phases...), f.ends, f.outcome
}

type fakeAttemptObserver struct{ f *fakePhaseObserver }

func (a *fakeAttemptObserver) Phase(name string) {
	a.f.mu.Lock()
	defer a.f.mu.Unlock()
	a.f.phases = append(a.f.phases, name)
}

func (a *fakeAttemptObserver) End(outcome AttemptOutcome) {
	a.f.mu.Lock()
	defer a.f.mu.Unlock()
	a.f.ends++
	a.f.outcome = outcome
}

// phaseCapturingRuntime is a ContainerRuntime that writes a well-formed
// values.json into whatever the output mount's host source is (so a
// success case can reach the commit step), exits with a configurable code,
// and records the value fakePhaseObserver stamped onto ctx at Create and
// Start — the seam TestPhaseObserver_BeginsCtxReachesTheRuntime asserts.
type phaseCapturingRuntime struct {
	exitCode int32

	mu             sync.Mutex
	createCtxValue any
	startCtxValue  any
}

func (r *phaseCapturingRuntime) Pull(context.Context, string) (PullInfo, error) {
	return PullInfo{Platform: "linux/amd64", Delivery: DeliveryRegistry}, nil
}

func (r *phaseCapturingRuntime) LoadLayout(context.Context, string, string) (PullInfo, error) {
	return PullInfo{}, errors.New("no embedded layout in this test")
}

func (r *phaseCapturingRuntime) Create(ctx context.Context, spec ContainerSpec) (ContainerID, error) {
	r.mu.Lock()
	r.createCtxValue = ctx.Value(phaseObserverCtxKey{})
	r.mu.Unlock()
	for _, m := range spec.Mounts {
		if m.Destination == ContainerOutput {
			if err := os.WriteFile(filepath.Join(m.Source, "values.json"), []byte(`{}`), 0o644); err != nil {
				return "", err
			}
		}
	}
	return "fake-container", nil
}

func (r *phaseCapturingRuntime) Start(ctx context.Context, _ ContainerID, _, _ io.Writer) error {
	r.mu.Lock()
	r.startCtxValue = ctx.Value(phaseObserverCtxKey{})
	r.mu.Unlock()
	return nil
}

func (r *phaseCapturingRuntime) Signal(context.Context, ContainerID, Signal) error { return nil }

func (r *phaseCapturingRuntime) Wait(context.Context, ContainerID) (ExitStatus, error) {
	return ExitStatus{Code: r.exitCode}, nil
}

func (r *phaseCapturingRuntime) Remove(context.Context, ContainerID) error { return nil }

func (r *phaseCapturingRuntime) Reap(context.Context, string) (int, error) { return 0, nil }

func (r *phaseCapturingRuntime) Describe(context.Context) (RuntimeInfo, error) {
	return RuntimeInfo{}, nil
}

func (r *phaseCapturingRuntime) ctxValues() (create, start any) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.createCtxValue, r.startCtxValue
}

// newPhaseDispatcher builds a dispatcher wired to a phaseCapturingRuntime, an
// in-memory artifact store and no commit store — reusing the TEST-ONLY shape
// newExportDispatcher (tasklog_export_test.go) already establishes for this
// package, plus the PhaseObserver seam under test.
func newPhaseDispatcher(t *testing.T, runtime *phaseCapturingRuntime, obs PhaseObserver) *OCIDispatcher {
	t.Helper()
	store, err := artifacts.NewLocalStore(t.TempDir())
	if err != nil {
		t.Fatalf("local artifact store: %v", err)
	}
	return &OCIDispatcher{
		Runtime:  runtime,
		Store:    store,
		Refs:     artifacts.MemoryRefPredictor{},
		Logs:     &recordingLogStore{},
		Policy:   DefaultSecurityPolicy(),
		WorkRoot: t.TempDir(),
		OrkerID:  "orker-phase-test",
		BuildID:  "test",
		Phases:   obs,
	}
}

// wantPhaseSequence is the §7.1 order a successful attempt walks, exactly as
// Execute and execute call beat.Phase.
var wantPhaseSequence = []string{
	"start", "commit-check", "workspace", "stage-inputs", "image", "render",
	"create", "run", "exit", "validate", "commit",
}

// TestPhaseObserver_SuccessWalksTheFullSequence is the §7.1 order, held as
// data: Begin is called once with the exact input and attempt, Phase sees the
// complete sequence in order, and End is called exactly once with an empty
// Reason.
func TestPhaseObserver_SuccessWalksTheFullSequence(t *testing.T) {
	obs := &fakePhaseObserver{}
	runtime := &phaseCapturingRuntime{exitCode: 0}
	dispatcher := newPhaseDispatcher(t, runtime, obs)
	in := baseExportInput("phase-success")

	result, err := dispatcher.Execute(context.Background(), in)
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if result == nil {
		t.Fatal("Execute returned a nil result on success")
	}

	begins, phases, ends, outcome := obs.recorded()
	if begins != 1 {
		t.Fatalf("Begin called %d times, want 1", begins)
	}
	if obs.beginIn != in {
		t.Error("Begin did not receive the same ExecuteTaskInput Execute was given")
	}
	if obs.attempt != 1 {
		t.Errorf("Begin attempt = %d, want 1 (no Temporal activity context)", obs.attempt)
	}
	if len(phases) != len(wantPhaseSequence) {
		t.Fatalf("phase sequence = %q, want %q", phases, wantPhaseSequence)
	}
	for i, want := range wantPhaseSequence {
		if phases[i] != want {
			t.Errorf("phase %d = %q, want %q (full sequence %q)", i, phases[i], want, phases)
		}
	}
	if ends != 1 {
		t.Fatalf("End called %d times, want exactly 1", ends)
	}
	if outcome.Reason != "" {
		t.Errorf("End outcome.Reason = %q, want empty on success", outcome.Reason)
	}
	if outcome.Canceled {
		t.Error("End outcome.Canceled = true on a success")
	}
}

// TestPhaseObserver_FailureCarriesTheReasonAndExitCode covers a container
// that exits non-zero: End is still called exactly once, but with
// ReasonContainerExitNonzero and the exit code the runtime reported.
func TestPhaseObserver_FailureCarriesTheReasonAndExitCode(t *testing.T) {
	obs := &fakePhaseObserver{}
	runtime := &phaseCapturingRuntime{exitCode: 7}
	dispatcher := newPhaseDispatcher(t, runtime, obs)
	in := baseExportInput("phase-failure")

	if _, err := dispatcher.Execute(context.Background(), in); err == nil {
		t.Fatal("Execute: want a failure (the fake container exits 7), got nil")
	}

	_, _, ends, outcome := obs.recorded()
	if ends != 1 {
		t.Fatalf("End called %d times, want exactly 1", ends)
	}
	if outcome.Reason != ReasonContainerExitNonzero {
		t.Errorf("End outcome.Reason = %q, want %q", outcome.Reason, ReasonContainerExitNonzero)
	}
	if outcome.ExitCode != 7 {
		t.Errorf("End outcome.ExitCode = %d, want 7", outcome.ExitCode)
	}
	if outcome.Canceled {
		t.Error("End outcome.Canceled = true on a container failure")
	}
}

// TestPhaseObserver_NilMeansUnchanged is what the Phases doc comment
// promises: with none configured, Execute's behavior is exactly what it was
// before the seam existed.
func TestPhaseObserver_NilMeansUnchanged(t *testing.T) {
	runtime := &phaseCapturingRuntime{exitCode: 0}
	dispatcher := newPhaseDispatcher(t, runtime, nil)
	in := baseExportInput("phase-nil")

	result, err := dispatcher.Execute(context.Background(), in)
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if result == nil {
		t.Fatal("Execute returned a nil result on success")
	}
}

// panicCreateRuntime is a ContainerRuntime whose Create panics instead of
// returning an error — the shape a runtime library bug takes, as opposed to
// every other fake in this package, which reports failure the contract's own
// way. It exists to prove Execute's panic-recovery defer, not the §7.1
// sequence: Start/Wait/etc. are never reached and their bodies do not matter.
type panicCreateRuntime struct{}

func (panicCreateRuntime) Pull(context.Context, string) (PullInfo, error) {
	return PullInfo{Platform: "linux/amd64", Delivery: DeliveryRegistry}, nil
}

func (panicCreateRuntime) LoadLayout(context.Context, string, string) (PullInfo, error) {
	return PullInfo{}, errors.New("no embedded layout in this test")
}

func (panicCreateRuntime) Create(context.Context, ContainerSpec) (ContainerID, error) {
	panic("boom: container runtime create panicked")
}

func (panicCreateRuntime) Start(context.Context, ContainerID, io.Writer, io.Writer) error {
	return nil
}

func (panicCreateRuntime) Signal(context.Context, ContainerID, Signal) error { return nil }

func (panicCreateRuntime) Wait(context.Context, ContainerID) (ExitStatus, error) {
	return ExitStatus{}, nil
}

func (panicCreateRuntime) Remove(context.Context, ContainerID) error { return nil }

func (panicCreateRuntime) Reap(context.Context, string) (int, error) { return 0, nil }

func (panicCreateRuntime) Describe(context.Context) (RuntimeInfo, error) {
	return RuntimeInfo{}, nil
}

// TestPhaseObserver_PanicStillEndsExactlyOnceAndClosesTheLogs is Fix 2's
// behavior test: a panic out of the ContainerRuntime — not a returned error —
// must still produce exactly one AttemptObserver.End, tagged ReasonPanic with
// a non-nil Err, before Execute re-panics with the ORIGINAL value unchanged
// (Temporal's own activity panic handling must see exactly what the runtime
// panicked with). It also proves STEP 1's teardown still runs on this path:
// the TaskLogExporter's Close is called exactly once.
//
// The chunk sink's own Close is deferred immediately after the sink is
// opened — BEFORE the exporter's — so in the LIFO unwind that follows the
// recovered-and-rethrown panic it runs AFTER the exporter's Close, with
// nothing in between that can itself panic. The exporter's Close having run
// exactly once is therefore proof the sink's Close ran too.
func TestPhaseObserver_PanicStillEndsExactlyOnceAndClosesTheLogs(t *testing.T) {
	obs := &fakePhaseObserver{}
	exporter := &recordingTaskLogExporter{}
	attempts := artifacts.NewMemoryAttemptStore()
	store, err := artifacts.NewLocalStore(t.TempDir())
	if err != nil {
		t.Fatalf("local artifact store: %v", err)
	}
	dispatcher := &OCIDispatcher{
		Runtime:  panicCreateRuntime{},
		Store:    store,
		Refs:     artifacts.MemoryRefPredictor{},
		Logs:     &recordingLogStore{},
		Policy:   DefaultSecurityPolicy(),
		WorkRoot: t.TempDir(),
		OrkerID:  "orker-panic-test",
		BuildID:  "test",
		Phases:   obs,
		TaskLogs: exporter,
		Attempts: attempts,
	}
	in := baseExportInput("phase-panic")

	func() {
		defer func() {
			r := recover()
			if r == nil {
				t.Fatal("Execute: want a re-panic from the fake runtime's Create, got none")
			}
			if s, ok := r.(string); !ok || s != "boom: container runtime create panicked" {
				t.Errorf("recovered panic = %v, want the original panic value unchanged", r)
			}
		}()
		dispatcher.Execute(context.Background(), in)
	}()

	_, _, ends, outcome := obs.recorded()
	if ends != 1 {
		t.Fatalf("End called %d times, want exactly 1", ends)
	}
	if outcome.Reason != ReasonPanic {
		t.Errorf("End outcome.Reason = %q, want %q", outcome.Reason, ReasonPanic)
	}
	if outcome.Err == nil {
		t.Error("End outcome.Err = nil, want the panic wrapped as a non-nil error")
	}

	opens, events := exporter.recorded()
	if opens != 1 {
		t.Fatalf("Open called %d times, want 1", opens)
	}
	closeCount := 0
	for _, ev := range events {
		if ev.close {
			closeCount++
		}
	}
	if closeCount != 1 {
		t.Fatalf("exporter Close called %d times, want exactly 1 — a panic must not skip STEP 1's teardown", closeCount)
	}

	// O9: the record the panic path writes still closes its phase list — the
	// attempt died in "create", and the span it died in ends at the record's
	// finished_at rather than being left open.
	record := onlyAttempt(t, attempts, in)
	assertClosedSpanList(t, record)
	if last := record.GetPhases()[len(record.GetPhases())-1].GetPhase(); last != "create" {
		t.Errorf("the panic path's last span = %q, want create (where the runtime panicked)", last)
	}
}

// TestPhaseObserver_BeginsCtxReachesTheRuntime proves the ctx Begin returns —
// not the one Execute was called with — is what flows to the container
// runtime's Create and Start: child work and (§ Part B) the trace-context
// file must see whatever Begin attached, such as a span.
func TestPhaseObserver_BeginsCtxReachesTheRuntime(t *testing.T) {
	obs := &fakePhaseObserver{}
	runtime := &phaseCapturingRuntime{exitCode: 0}
	dispatcher := newPhaseDispatcher(t, runtime, obs)
	in := baseExportInput("phase-ctx")

	if _, err := dispatcher.Execute(context.Background(), in); err != nil {
		t.Fatalf("Execute: %v", err)
	}

	create, start := runtime.ctxValues()
	if create != "from-begin" {
		t.Errorf("Create saw ctx value %v, want %q", create, "from-begin")
	}
	if start != "from-begin" {
		t.Errorf("Start saw ctx value %v, want %q", start, "from-begin")
	}
}
