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

	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/task"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// TestEmission_TypesAListedInterruption is the Orker half of E92 §4.7: an
// interruption reason becomes the error type OrkInterruption:<REASON> only
// when the engine listed that type in the attempt's NonRetryableErrorTypes
// and the Orker may emit it (its compiled set, and each reason's own
// condition). A listed reason it may not emit is declined, and the failure
// is today's category-typed error; with nothing listed, nothing changes.
func TestEmission_TypesAListedInterruption(t *testing.T) {
	stopped := func() *Failure { return Fail(ReasonOrkerStopped, "handed back") }
	neverStartedStore := func() *Failure {
		f := Fail(ReasonAttemptStoreUnavailable, "claim refused")
		f.NeverStarted = true
		return f
	}
	startedStore := func() *Failure { return Fail(ReasonAttemptStoreUnavailable, "store gone") }
	byRuntime := func() *Failure { return Fail(ReasonContainerStoppedByRuntime, "daemon stopped") }
	runtimeWentAway := func() *Failure {
		f := Fail(ReasonContainerStoppedByRuntime, "daemon stopped, and its stream ended")
		f.RuntimeWentAway = true
		return f
	}
	neverStartedDrift := func() *Failure {
		f := Fail(ReasonProfileDrifted, "profile drifted")
		f.NeverStarted = true
		return f
	}
	startedDrift := func() *Failure { return Fail(ReasonProfileDrifted, "profile drifted") }
	neverStartedUnknown := func() *Failure {
		f := Fail(ReasonRuntimeStateUnknown, "start and inspect both failed")
		f.NeverStarted = true
		return f
	}
	listed := func(reasons ...string) []string {
		out := []string{task.CategoryContractViolation}
		for _, r := range reasons {
			out = append(out, InterruptionType(r))
		}
		return out
	}
	cases := []struct {
		name     string
		failure  *Failure
		listed   []string
		stopping bool
		wantType string
		emitted  int // +1 emitted, -1 declined, 0 neither
	}{
		{"nothing listed: today's error", stopped(), nil, false, CategoryInfrastructureError, 0},
		{"another reason listed: today's error", stopped(), listed(ReasonAttemptStoreUnavailable), false, CategoryInfrastructureError, 0},
		{"ORKER_STOPPED listed: typed", stopped(), listed(ReasonOrkerStopped), false, InterruptionType(ReasonOrkerStopped), 1},
		{"a never-started store fault listed: typed", neverStartedStore(), listed(ReasonAttemptStoreUnavailable), false, InterruptionType(ReasonAttemptStoreUnavailable), 1},
		{"a store fault after its start is declined", startedStore(), listed(ReasonAttemptStoreUnavailable), false, CategoryInfrastructureError, -1},
		{"stopped by its runtime while stopping: typed", byRuntime(), listed(ReasonContainerStoppedByRuntime), true, InterruptionType(ReasonContainerStoppedByRuntime), 1},
		{"stopped by its runtime, not stopping, is declined", byRuntime(), listed(ReasonContainerStoppedByRuntime), false, CategoryInfrastructureError, -1},
		{"stopped as its runtime went away, not stopping: typed", runtimeWentAway(), listed(ReasonContainerStoppedByRuntime), false, InterruptionType(ReasonContainerStoppedByRuntime), 1},
		{"stopped as its runtime went away, nothing listed: today's error", runtimeWentAway(), nil, false, CategoryInfrastructureError, 0},
		{"a never-started profile drift listed: typed", neverStartedDrift(), listed(ReasonProfileDrifted), false, InterruptionType(ReasonProfileDrifted), 1},
		{"a profile drift not marked never-started is declined", startedDrift(), listed(ReasonProfileDrifted), false, CategoryInfrastructureError, -1},
		{"a never-started drift whose container started is declined", neverStartedDrift().stampFor(listed(ReasonProfileDrifted), true, 0), listed(ReasonProfileDrifted), false, CategoryInfrastructureError, -1},
		{"a runtime state unknown listed: typed", neverStartedUnknown(), listed(ReasonRuntimeStateUnknown), false, InterruptionType(ReasonRuntimeStateUnknown), 1},
		{"a runtime state unknown not marked never-started is declined", Fail(ReasonRuntimeStateUnknown, "unknown"), listed(ReasonRuntimeStateUnknown), false, CategoryInfrastructureError, -1},
		{"a reason outside the compiled set is declined", Fail(ReasonContainerExitNonzero, "exit 1"), listed(ReasonContainerExitNonzero), false, CategoryActivityFailed, -1},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := 0
			err := tc.failure.EmittedError(tc.listed, tc.stopping, func(emitted bool) {
				if emitted {
					got = 1
				} else {
					got = -1
				}
			})
			var app *temporal.ApplicationError
			if !errors.As(err, &app) {
				t.Fatalf("EmittedError = %v, want an ApplicationError", err)
			}
			if app.Type() != tc.wantType {
				t.Fatalf("type = %q, want %q", app.Type(), tc.wantType)
			}
			if got != tc.emitted {
				t.Fatalf("observed %d, want %d (+1 emitted, -1 declined, 0 neither)", got, tc.emitted)
			}
			// The detail still carries the reason the engine reads.
			var detail *orkstackv1.ActivityFailureDetail
			if err := app.Details(&detail); err != nil || detail.GetReasonCode() != tc.failure.Reason {
				t.Fatalf("detail = %v, %v; want reason %s", detail, err, tc.failure.Reason)
			}
			// A typed interruption ends the execution early: never retried by
			// Temporal, whatever the reason's own retryability.
			if tc.emitted == 1 && !app.NonRetryable() {
				t.Fatalf("a typed interruption is retryable; want it to end the execution")
			}
		})
	}
}

// TestEmission_HeartbeatLossIsUnreportedOnlyWhenListed: ORKER_HEARTBEAT_LOST
// goes unreported, left to the cluster's heartbeat timeout, only when the
// engine listed TemporalTimeout:Heartbeat (E92 §4.7).
func TestEmission_HeartbeatLossIsUnreportedOnlyWhenListed(t *testing.T) {
	for _, tc := range []struct {
		listed  []string
		timeout time.Duration
		want    bool
	}{
		{nil, 30 * time.Second, false},
		{[]string{InterruptionType(ReasonOrkerStopped)}, 30 * time.Second, false},
		{[]string{HeartbeatTimeoutType}, 30 * time.Second, true},
		// No heartbeat timeout: nothing would ever time it out, so it reports.
		{[]string{HeartbeatTimeoutType}, 0, false},
	} {
		if got := leaveHeartbeatLossUnreported(tc.listed, tc.timeout); got != tc.want {
			t.Errorf("leaveHeartbeatLossUnreported(%v, %v) = %v, want %v", tc.listed, tc.timeout, got, tc.want)
		}
	}
}

// TestExecute_AClaimFaultIsTypedWhenItsPolicyListsIt is the wiring of E92
// PR 6's typed emission: an attempt reads the types its own retry policy lists
// (activity.Info.RetryPolicy), so a claim the store refuses, which started no
// container, ends its execution as OrkInterruption:ATTEMPT_STORE_UNAVAILABLE
// when listed, on one attempt, and as today's error when not.
func TestExecute_AClaimFaultIsTypedWhenItsPolicyListsIt(t *testing.T) {
	saved := claimBackoff
	claimBackoff = []time.Duration{time.Millisecond, time.Millisecond, time.Millisecond}
	t.Cleanup(func() { claimBackoff = saved })

	for _, tc := range []struct {
		name     string
		listed   []string
		wantType string
	}{
		{"listed", []string{InterruptionType(ReasonAttemptStoreUnavailable)}, InterruptionType(ReasonAttemptStoreUnavailable)},
		{"not listed", nil, CategoryInfrastructureError},
	} {
		t.Run(tc.name, func(t *testing.T) {
			store := &failingClaims{MemoryAttemptStore: artifacts.NewMemoryAttemptStore()}
			runtime := &creatingRuntime{ContainerRuntime: &phaseCapturingRuntime{exitCode: 0}}
			d := newAttemptDispatcher(t, runtime, store.MemoryAttemptStore)
			d.Claims = store
			d.heartbeaters = sdkRouteHeartbeater
			// The SDK's test environment leaves the retry policy out of the
			// activity's info (P0-2 measured the real round trip), so the
			// attempt is told what its policy lists.
			listed := tc.listed
			d.listed = func(context.Context) []string { return listed }
			heard := &interruptionRecorder{}
			d.Interruptions = heard
			probe := func(ctx workflow.Context, in *orkstackv1.ExecuteTaskInput) error {
				ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
					StartToCloseTimeout: time.Minute,
					RetryPolicy: &temporal.RetryPolicy{
						MaximumAttempts:        1,
						InitialInterval:        time.Millisecond,
						NonRetryableErrorTypes: listed,
					},
				})
				return workflow.ExecuteActivity(ctx, "ExecuteTask", in).Get(ctx, nil)
			}
			var suite testsuite.WorkflowTestSuite
			env := suite.NewTestWorkflowEnvironment()
			env.SetTestTimeout(60 * time.Second)
			env.SetWorkerOptions(worker.Options{DeadlockDetectionTimeout: testDeadlockDetectionTimeout})
			env.RegisterWorkflowWithOptions(probe, workflow.RegisterOptions{Name: "probe"})
			env.RegisterActivityWithOptions(d.Execute, activity.RegisterOptions{Name: "ExecuteTask"})
			env.ExecuteWorkflow("probe", baseExportInput("claim"))

			if !env.IsWorkflowCompleted() {
				t.Fatal("workflow did not complete")
			}
			var app *temporal.ApplicationError
			if err := env.GetWorkflowError(); !errors.As(err, &app) {
				t.Fatalf("workflow error = %v, want the attempt's ApplicationError", err)
			}
			if app.Type() != tc.wantType {
				t.Fatalf("the attempt ended as %q, want %q", app.Type(), tc.wantType)
			}
			// Its detail says it never started, never that it started, and
			// quotes the store's error.
			var detail *orkstackv1.ActivityFailureDetail
			if err := app.Details(&detail); err != nil {
				t.Fatalf("detail: %v", err)
			}
			// Only an engine that listed an interruption gets the new fields
			// (TestMixedFleet_…): listed, the detail says never started and
			// quotes the store; not listed, it carries none of them.
			asked := len(tc.listed) > 0
			if asked && (!detail.GetNeverStarted() || detail.GetStarted() || detail.GetStoreError() == "") {
				t.Fatalf("detail never_started=%v started=%v store_error=%q; want true, false and the store's error",
					detail.GetNeverStarted(), detail.GetStarted(), detail.GetStoreError())
			}
			if !asked && (detail.GetNeverStarted() || detail.GetStarted() || detail.GetStoreError() != "" || detail.Ordinal != nil) {
				t.Fatalf("an engine that listed nothing got the new fields: %v", detail)
			}
			if want := len(tc.listed); heard.emitted != want || heard.declined != 0 {
				t.Fatalf("counted %d emitted and %d declined; want %d and 0", heard.emitted, heard.declined, want)
			}
			runtime.mu.Lock()
			created := len(runtime.labels)
			runtime.mu.Unlock()
			if created != 0 {
				t.Fatalf("a claim fault created %d containers; want none", created)
			}
		})
	}
}

// TestStampAttempt_NeverBothStartedAndNeverStarted: started (the container
// was observed started) and never_started (the attempt is a never-started
// interruption) mean different things, and the detail carries only three
// combinations: started; never started; neither (a failure before any
// container that is no interruption, or an older Orker). Both is never sent,
// even for a failure wrongly marked never-started after its container ran.
func TestStampAttempt_NeverBothStartedAndNeverStarted(t *testing.T) {
	type combo struct{ started, neverStarted bool }
	allowed := map[combo]bool{{true, false}: true, {false, true}: true, {false, false}: true}
	for _, started := range []bool{false, true} {
		for _, marked := range []bool{false, true} {
			f := Fail(ReasonOrkerStopped, "x")
			f.NeverStarted = marked
			d := f.StampAttempt(started, 3).Detail
			got := combo{d.GetStarted(), d.GetNeverStarted()}
			if !allowed[got] {
				t.Fatalf("started=%v marked=%v sent %+v", started, marked, got)
			}
			if got.started != started {
				t.Fatalf("started=%v sent started %v", started, got.started)
			}
			if started && f.NeverStarted {
				t.Fatal("a container that ran left the failure marked never started, so it could still be emitted as one")
			}
			if d.GetOrdinal() != 3 {
				t.Fatalf("ordinal = %d, want 3", d.GetOrdinal())
			}
		}
	}
	// A refused claim names the store's error; nothing else does.
	claim := neverStarted(FailCause(ReasonAttemptStoreUnavailable, errors.New("SlowDown"), "claim refused"))
	if got := claim.StampAttempt(false, 0).Detail.GetStoreError(); got != "SlowDown" {
		t.Fatalf("a refused claim's store_error = %q, want SlowDown", got)
	}
	if got := Fail(ReasonOrkerStopped, "x").StampAttempt(false, 0).Detail.GetStoreError(); got != "" {
		t.Fatalf("a hand-back carries store_error %q", got)
	}
	if Fail(ReasonOrkerStopped, "x").StampAttempt(false, 0).Detail.Ordinal != nil {
		t.Fatal("an attempt with no ordinal sends one")
	}
}

// interruptionRecorder counts what an attempt's emission decided.
type interruptionRecorder struct{ emitted, declined int }

func (r *interruptionRecorder) InterruptionEmission(emitted bool) {
	if emitted {
		r.emitted++
	} else {
		r.declined++
	}
}

// TestMixedFleet_ANewDetailFieldReachesOnlyAnEngineThatAskedForIt: an engine
// that predates PR 6 decodes ActivityFailureDetail with the SDK's default
// protojson converter, which refuses unknown fields, so a detail carrying
// never_started, started, ordinal or store_error would lose its whole
// detail there (its reason code, artifacts, Orker). The Orker sends those
// fields only to an attempt whose retry policy lists an interruption type,
// which only an engine that knows them does. An Orker that predates PR 6
// sends none, which a new engine reads as not started.
func TestMixedFleet_ANewDetailFieldReachesOnlyAnEngineThatAskedForIt(t *testing.T) {
	newKeys := []string{`"neverStarted"`, `"started"`, `"ordinal"`, `"storeError"`}
	for _, tc := range []struct {
		name   string
		listed []string
		asked  bool
	}{
		{"an engine that lists nothing (one before PR 6)", nil, false},
		{"an engine that lists only categories", []string{task.CategoryContractViolation}, false},
		{"an engine that lists an interruption", []string{InterruptionType(ReasonOrkerStopped)}, true},
		{"an engine that lists the heartbeat timeout", []string{HeartbeatTimeoutType}, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ran := Fail(ReasonContainerExitNonzero, "exit 1").stampFor(tc.listed, true, 4)
			refused := neverStarted(FailCause(ReasonAttemptStoreUnavailable, errors.New("SlowDown"), "claim refused")).
				stampFor(tc.listed, false, 0)
			body := ""
			for _, f := range []*Failure{ran, refused} {
				payload, err := converter.GetDefaultDataConverter().ToPayload(f.Detail)
				if err != nil {
					t.Fatal(err)
				}
				body += string(payload.GetData())
			}
			for _, key := range newKeys {
				if got := strings.Contains(body, key); got != tc.asked {
					t.Fatalf("the details %s carry %s = %v; want %v", body, key, got, tc.asked)
				}
			}
		})
	}
}

// TestExecute_AProfileDriftIsTypedWhenItsPolicyListsIt: an attempt that meets
// a drifted profile at step 3c started no container, so it ends its execution
// as OrkInterruption:PROFILE_DRIFTED when its policy lists it, a never-started
// interruption the engine runs again elsewhere (E92 §4.7, E91 P3), and as
// today's error when not.
func TestExecute_AProfileDriftIsTypedWhenItsPolicyListsIt(t *testing.T) {
	for _, tc := range []struct {
		name     string
		listed   []string
		wantType string
	}{
		{"listed", []string{InterruptionType(ReasonProfileDrifted)}, InterruptionType(ReasonProfileDrifted)},
		{"not listed", nil, CategoryInfrastructureError},
	} {
		t.Run(tc.name, func(t *testing.T) {
			runtime := &creatingRuntime{ContainerRuntime: &phaseCapturingRuntime{exitCode: 0}}
			d := newAttemptDispatcher(t, runtime, artifacts.NewMemoryAttemptStore())
			d.heartbeaters = sdkRouteHeartbeater
			profiles, _ := gateState(t)
			// Latched: every attempt after a drift meets it, whatever the node
			// says now.
			profiles.setDrift("sensor", orkstackv1.ProfileCordonReason_PROFILE_CORDON_REASON_DEVICE_CHANGED, "188:199 is now 188:0", time.Now())
			d.profiles = profiles
			listed := tc.listed
			d.listed = func(context.Context) []string { return listed }
			probe := func(ctx workflow.Context, in *orkstackv1.ExecuteTaskInput) error {
				ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
					StartToCloseTimeout: time.Minute,
					RetryPolicy: &temporal.RetryPolicy{
						MaximumAttempts:        1,
						InitialInterval:        time.Millisecond,
						NonRetryableErrorTypes: listed,
					},
				})
				return workflow.ExecuteActivity(ctx, "ExecuteTask", in).Get(ctx, nil)
			}
			var suite testsuite.WorkflowTestSuite
			env := suite.NewTestWorkflowEnvironment()
			env.SetTestTimeout(60 * time.Second)
			env.SetWorkerOptions(worker.Options{DeadlockDetectionTimeout: testDeadlockDetectionTimeout})
			env.RegisterWorkflowWithOptions(probe, workflow.RegisterOptions{Name: "probe"})
			env.RegisterActivityWithOptions(d.Execute, activity.RegisterOptions{Name: "ExecuteTask"})
			in := baseExportInput("drift")
			in.Devices = sensorRef(gateIdentity)
			env.ExecuteWorkflow("probe", in)

			if !env.IsWorkflowCompleted() {
				t.Fatal("workflow did not complete")
			}
			var app *temporal.ApplicationError
			if err := env.GetWorkflowError(); !errors.As(err, &app) {
				t.Fatalf("workflow error = %v, want the attempt's ApplicationError", err)
			}
			if app.Type() != tc.wantType {
				t.Fatalf("the attempt ended as %q, want %q", app.Type(), tc.wantType)
			}
			var detail *orkstackv1.ActivityFailureDetail
			if err := app.Details(&detail); err != nil || detail.GetReasonCode() != ReasonProfileDrifted {
				t.Fatalf("detail = %v, %v; want reason %s", detail, err, ReasonProfileDrifted)
			}
			if asked := len(tc.listed) > 0; asked && (!detail.GetNeverStarted() || detail.GetStarted()) {
				t.Fatalf("detail never_started=%v started=%v; want true and false", detail.GetNeverStarted(), detail.GetStarted())
			} else if !asked && (detail.GetNeverStarted() || detail.GetStarted() || detail.Ordinal != nil) {
				// An engine that listed nothing decodes the detail strictly.
				t.Fatalf("an engine that listed nothing got the new fields: %v", detail)
			}
			runtime.mu.Lock()
			created := len(runtime.labels)
			runtime.mu.Unlock()
			if created != 0 {
				t.Fatalf("a drifted profile created %d containers; want none", created)
			}
		})
	}
}

// stateUnknownRuntime is a daemon that fails between a container's start and
// the inspect that would say whether it started: the start call fails, and so
// does every observation after it. It is a hand-written fake because that is a
// fault a real Docker or Podman daemon cannot be made to produce on demand
// (AGENTS.md §11.1, step 3); the CLI half of the same fault, an inspect that
// cannot answer, is proven against the runtime's own parsing in
// TestDockerRuntime_StartWhoseStateCannotBeObservedIsUnknown.
type stateUnknownRuntime struct {
	phaseCapturingRuntime
	mu      sync.Mutex
	signals []Signal
	removed int
}

func (r *stateUnknownRuntime) Start(context.Context, ContainerID, io.Writer, io.Writer) error {
	return fmt.Errorf("start container fake-container: observe its state: %w", ErrRuntimeStateUnknown)
}

func (r *stateUnknownRuntime) Signal(_ context.Context, _ ContainerID, sig Signal) error {
	r.mu.Lock()
	r.signals = append(r.signals, sig)
	r.mu.Unlock()
	return errors.New("cannot connect to the container daemon")
}

// Wait answers at once, as `docker wait` does for a container still created:
// an exit nobody may take as proof that the container ended.
func (r *stateUnknownRuntime) Wait(context.Context, ContainerID) (ExitStatus, error) {
	return ExitStatus{}, nil
}

func (r *stateUnknownRuntime) Remove(context.Context, ContainerID) error {
	r.mu.Lock()
	r.removed++
	r.mu.Unlock()
	return nil
}

// TestExecute_AStartWhoseStateIsUnknownIsANeverStartedFault is R8-1 (E92
// §4.2): a start that failed and whose container no inspect can describe is
// RUNTIME_STATE_UNKNOWN, a never-started fault that says the container did not
// start (so the engine resets no never-started counter and a broken daemon
// cannot loop forever), after the cancellation sequence and the container's
// removal.
func TestExecute_AStartWhoseStateIsUnknownIsANeverStartedFault(t *testing.T) {
	for _, tc := range []struct {
		name     string
		listed   []string
		wantType string
	}{
		{"listed", []string{InterruptionType(ReasonRuntimeStateUnknown)}, InterruptionType(ReasonRuntimeStateUnknown)},
		{"not listed", nil, CategoryInfrastructureError},
	} {
		t.Run(tc.name, func(t *testing.T) {
			runtime := &stateUnknownRuntime{}
			d := newAttemptDispatcher(t, runtime, artifacts.NewMemoryAttemptStore())
			d.heartbeaters = sdkRouteHeartbeater
			listed := tc.listed
			d.listed = func(context.Context) []string { return listed }
			probe := func(ctx workflow.Context, in *orkstackv1.ExecuteTaskInput) error {
				ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
					StartToCloseTimeout: time.Minute,
					RetryPolicy: &temporal.RetryPolicy{
						MaximumAttempts:        1,
						InitialInterval:        time.Millisecond,
						NonRetryableErrorTypes: listed,
					},
				})
				return workflow.ExecuteActivity(ctx, "ExecuteTask", in).Get(ctx, nil)
			}
			var suite testsuite.WorkflowTestSuite
			env := suite.NewTestWorkflowEnvironment()
			env.SetTestTimeout(60 * time.Second)
			env.SetWorkerOptions(worker.Options{DeadlockDetectionTimeout: testDeadlockDetectionTimeout})
			env.RegisterWorkflowWithOptions(probe, workflow.RegisterOptions{Name: "probe"})
			env.RegisterActivityWithOptions(d.Execute, activity.RegisterOptions{Name: "ExecuteTask"})
			in := baseExportInput("unknown")
			in.CancellationGracePeriodSeconds = 1
			env.ExecuteWorkflow("probe", in)

			if !env.IsWorkflowCompleted() {
				t.Fatal("workflow did not complete")
			}
			var app *temporal.ApplicationError
			if err := env.GetWorkflowError(); !errors.As(err, &app) {
				t.Fatalf("workflow error = %v, want the attempt's ApplicationError", err)
			}
			if app.Type() != tc.wantType {
				t.Fatalf("the attempt ended as %q, want %q", app.Type(), tc.wantType)
			}
			var detail *orkstackv1.ActivityFailureDetail
			if err := app.Details(&detail); err != nil || detail.GetReasonCode() != ReasonRuntimeStateUnknown {
				t.Fatalf("detail = %v, %v; want reason %s", detail, err, ReasonRuntimeStateUnknown)
			}
			if asked := len(tc.listed) > 0; asked && (!detail.GetNeverStarted() || detail.GetStarted()) {
				t.Fatalf("detail never_started=%v started=%v; want true and false: an unknown start resets nothing",
					detail.GetNeverStarted(), detail.GetStarted())
			} else if !asked && (detail.GetNeverStarted() || detail.GetStarted() || detail.Ordinal != nil) {
				t.Fatalf("an engine that listed nothing got the new fields: %v", detail)
			}
			runtime.mu.Lock()
			signals, removed := append([]Signal(nil), runtime.signals...), runtime.removed
			runtime.mu.Unlock()
			if fmt.Sprint(signals) != fmt.Sprint([]Signal{SIGTERM, SIGKILL}) {
				t.Fatalf("signals = %v; want the whole cancellation sequence, SIGTERM then SIGKILL after the grace, "+
					"for a container that may be running", signals)
			}
			if removed == 0 {
				t.Fatal("the container was never removed; its removal is mandatory")
			}
		})
	}
}

// driftAfterPullRuntime is creatingRuntime whose Pull marks the moment the
// attempt has passed its profile check (step 3c) and not yet reached the
// recheck before create: the device changes then.
type driftAfterPullRuntime struct {
	creatingRuntime
	pulled atomic.Bool
}

func (r *driftAfterPullRuntime) Pull(ctx context.Context, pinned string) (PullInfo, error) {
	r.pulled.Store(true)
	return r.creatingRuntime.Pull(ctx, pinned)
}

// TestExecute_ADriftAtTheRecheckBeforeCreateCreatesNothing: a device that
// passes the profile check and then changes while the image is pulled is
// caught by the recheck just before create. The attempt creates no container,
// cordons the Orker and is a never-started PROFILE_DRIFTED, typed when its
// policy lists it (E91 §4.4, E92 R5E91-H3).
func TestExecute_ADriftAtTheRecheckBeforeCreateCreatesNothing(t *testing.T) {
	runtime := &driftAfterPullRuntime{creatingRuntime: creatingRuntime{ContainerRuntime: &phaseCapturingRuntime{exitCode: 0}}}
	d := newAttemptDispatcher(t, runtime, artifacts.NewMemoryAttemptStore())
	d.heartbeaters = sdkRouteHeartbeater
	profiles, cordon := gateState(t)
	d.profiles = profiles
	var inspected atomic.Int32
	d.inspectDevice = func(p string) (hostDevice, error) {
		inspected.Add(1)
		if runtime.pulled.Load() {
			return hostDevice{Resolved: "/dev/ttyUSB199", Char: true, Major: 188, Minor: 0, GID: 1000}, nil
		}
		return sameNode(p)
	}
	listed := []string{InterruptionType(ReasonProfileDrifted)}
	d.listed = func(context.Context) []string { return listed }
	probe := func(ctx workflow.Context, in *orkstackv1.ExecuteTaskInput) error {
		ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
			StartToCloseTimeout: time.Minute,
			RetryPolicy: &temporal.RetryPolicy{
				MaximumAttempts:        1,
				InitialInterval:        time.Millisecond,
				NonRetryableErrorTypes: listed,
			},
		})
		return workflow.ExecuteActivity(ctx, "ExecuteTask", in).Get(ctx, nil)
	}
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	env.SetTestTimeout(60 * time.Second)
	env.SetWorkerOptions(worker.Options{DeadlockDetectionTimeout: testDeadlockDetectionTimeout})
	env.RegisterWorkflowWithOptions(probe, workflow.RegisterOptions{Name: "probe"})
	env.RegisterActivityWithOptions(d.Execute, activity.RegisterOptions{Name: "ExecuteTask"})
	in := baseExportInput("drift-at-recheck")
	in.Devices = sensorRef(gateIdentity)
	env.ExecuteWorkflow("probe", in)

	if !runtime.pulled.Load() {
		t.Fatal("the attempt never pulled: the drift was caught before the recheck, so this proves nothing")
	}
	if inspected.Load() != 2 {
		t.Fatalf("the device was inspected %d times; want once at the check and once at the recheck", inspected.Load())
	}
	var app *temporal.ApplicationError
	if err := env.GetWorkflowError(); !errors.As(err, &app) || app.Type() != InterruptionType(ReasonProfileDrifted) {
		t.Fatalf("workflow error = %v; want a typed %s", env.GetWorkflowError(), ReasonProfileDrifted)
	}
	var detail *orkstackv1.ActivityFailureDetail
	if err := app.Details(&detail); err != nil || !detail.GetNeverStarted() || detail.GetStarted() {
		t.Fatalf("detail = %v, %v; want never started", detail, err)
	}
	if created := len(runtime.creates()); created != 0 {
		t.Fatalf("a drift at the recheck created %d containers; want none", created)
	}
	if cordoned, _, _ := cordon.State(); !cordoned {
		t.Fatal("a drift at the recheck left the Orker uncordoned")
	}
}

// driftAtCreateRuntime is a runtime during whose Create a profile's device
// changes: the moment between the recheck before create and the start, which
// a real daemon cannot be made to open on demand. It records whether Start
// was called and what was removed.
type driftAtCreateRuntime struct {
	creatingRuntime
	drift   func()
	started bool
	removed []ContainerID
}

func (r *driftAtCreateRuntime) Create(ctx context.Context, spec ContainerSpec) (ContainerID, error) {
	id, err := r.creatingRuntime.Create(ctx, spec)
	r.drift()
	return id, err
}

func (r *driftAtCreateRuntime) Start(ctx context.Context, id ContainerID, stdout, stderr io.Writer) error {
	r.started = true
	return r.creatingRuntime.Start(ctx, id, stdout, stderr)
}

func (r *driftAtCreateRuntime) Remove(ctx context.Context, id ContainerID) error {
	r.removed = append(r.removed, id)
	return r.creatingRuntime.Remove(ctx, id)
}

// TestProfileDrifted_NeverStartedOnlyBeforeStartIsCalled: Docker resolves a
// CDI device at start (design Q18), so the profile checks run once more just
// before it. A drift found there fails PROFILE_DRIFTED marked never-started,
// with Start never called and the created container removed; once Start is
// asked for, an attempt counts as started (design Q18's start-error-after-exec).
func TestProfileDrifted_NeverStartedOnlyBeforeStartIsCalled(t *testing.T) {
	listed := []string{InterruptionType(ReasonProfileDrifted)}
	profiles, _ := gateState(t)
	runtime := &driftAtCreateRuntime{
		creatingRuntime: creatingRuntime{ContainerRuntime: &phaseCapturingRuntime{exitCode: 0}},
		drift: func() {
			profiles.setDrift("sensor", orkstackv1.ProfileCordonReason_PROFILE_CORDON_REASON_DEVICE_CHANGED, "renumbered after create", time.Now())
		},
	}
	d := newAttemptDispatcher(t, runtime, artifacts.NewMemoryAttemptStore())
	d.heartbeaters = sdkRouteHeartbeater
	d.profiles = profiles
	d.inspectDevice = sameNode
	d.listed = func(context.Context) []string { return listed }
	probe := func(ctx workflow.Context, in *orkstackv1.ExecuteTaskInput) error {
		ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
			StartToCloseTimeout: time.Minute,
			RetryPolicy: &temporal.RetryPolicy{MaximumAttempts: 1, InitialInterval: time.Millisecond,
				NonRetryableErrorTypes: listed},
		})
		return workflow.ExecuteActivity(ctx, "ExecuteTask", in).Get(ctx, nil)
	}
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	env.SetTestTimeout(60 * time.Second)
	env.SetWorkerOptions(worker.Options{DeadlockDetectionTimeout: testDeadlockDetectionTimeout})
	env.RegisterWorkflowWithOptions(probe, workflow.RegisterOptions{Name: "probe"})
	env.RegisterActivityWithOptions(d.Execute, activity.RegisterOptions{Name: "ExecuteTask"})
	in := baseExportInput("drift-at-start")
	in.Devices = sensorRef(gateIdentity)
	env.ExecuteWorkflow("probe", in)

	var app *temporal.ApplicationError
	if err := env.GetWorkflowError(); !errors.As(err, &app) || app.Type() != InterruptionType(ReasonProfileDrifted) {
		t.Fatalf("workflow error = %v, want the typed PROFILE_DRIFTED interruption", err)
	}
	var detail *orkstackv1.ActivityFailureDetail
	if err := app.Details(&detail); err != nil || !detail.GetNeverStarted() || detail.GetStarted() {
		t.Fatalf("detail = %v, %v; want never_started and not started", detail, err)
	}
	if runtime.started {
		t.Fatal("Start was called for a container whose profile drifted before it")
	}
	if len(runtime.creates()) != 1 || len(runtime.removed) != 1 {
		t.Fatalf("created %d, removed %v; want the created container removed", len(runtime.creates()), runtime.removed)
	}
}
