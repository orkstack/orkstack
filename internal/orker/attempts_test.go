package orker

import (
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/workflow"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// retryingRuntime fails the FIRST container it runs and succeeds on every one
// after it — the shape of the `flaky` fixture, without a daemon. It is the
// smallest thing that produces two Temporal attempts of one activity, which is
// the whole point: an attempt record is only interesting when there is more
// than one attempt to tell apart.
type retryingRuntime struct {
	mu    sync.Mutex
	runs  int
	specs []ContainerSpec
}

func (r *retryingRuntime) Pull(context.Context, string) (PullInfo, error) {
	return PullInfo{
		Platform:       "linux/amd64",
		PlatformDigest: "sha256:" + repeat64('b'),
		Delivery:       DeliveryRegistry,
	}, nil
}

func (r *retryingRuntime) LoadLayout(context.Context, string, string) (PullInfo, error) {
	return PullInfo{}, errors.New("no embedded layout in this test")
}

func (r *retryingRuntime) Create(_ context.Context, spec ContainerSpec) (ContainerID, error) {
	r.mu.Lock()
	r.runs++
	r.specs = append(r.specs, spec)
	r.mu.Unlock()
	for _, m := range spec.Mounts {
		if m.Destination == ContainerOutput {
			// Every attempt writes a well-formed values.json, so the FIRST
			// attempt's failure is the container's exit status and nothing
			// else: a VALUES_MISSING would be a different story about the
			// same run.
			if err := os.WriteFile(filepath.Join(m.Source, "values.json"), []byte(`{}`), 0o644); err != nil {
				return "", err
			}
		}
	}
	return "fake-container", nil
}

func (r *retryingRuntime) Start(context.Context, ContainerID, io.Writer, io.Writer) error { return nil }

func (r *retryingRuntime) Signal(context.Context, ContainerID, Signal) error { return nil }

func (r *retryingRuntime) Wait(context.Context, ContainerID) (ExitStatus, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.runs == 1 {
		return ExitStatus{Code: 1}, nil
	}
	return ExitStatus{Code: 0}, nil
}

func (r *retryingRuntime) Remove(context.Context, ContainerID) error { return nil }

func (r *retryingRuntime) Reap(context.Context, string) (int, error) { return 0, nil }

func (r *retryingRuntime) Describe(context.Context) (RuntimeInfo, error) {
	return RuntimeInfo{}, nil
}

func repeat64(c byte) string {
	out := make([]byte, 64)
	for i := range out {
		out[i] = c
	}
	return string(out)
}

// newAttemptDispatcher is newPhaseDispatcher's sibling for the attempt-record
// tests: same TEST-ONLY shape (in-memory artifact store, no commit store), plus
// the AttemptStore under test.
func newAttemptDispatcher(t *testing.T, runtime ContainerRuntime, attempts artifacts.AttemptStore) *OCIDispatcher {
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
		Attempts: attempts,
		Policy:   DefaultSecurityPolicy(),
		WorkRoot: t.TempDir(),
		OrkerID:  "orker-attempt-test",
		BuildID:  "build-attempt-test",
	}
}

// attemptProbeWorkflow is the smallest workflow that can produce a RETRY: one
// activity, one retry policy. Temporal's own test environment increments
// activity.GetInfo(ctx).Attempt across those retries, which is the number the
// dispatcher reads and the record is keyed by — so this is how a unit test
// gets a real attempt 2 without a Temporal server or a Docker daemon.
func attemptProbeWorkflow(ctx workflow.Context, in *orkstackv1.ExecuteTaskInput) error {
	ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: time.Minute,
		RetryPolicy: &temporal.RetryPolicy{
			MaximumAttempts: 2,
			InitialInterval: time.Millisecond,
		},
	})
	return workflow.ExecuteActivity(ctx, "ExecuteTask", in).Get(ctx, nil)
}

// TestAttemptRecords_OneRecordPerAttemptOfARetriedTask is Phase 1's whole
// point, held as data.
//
// A task that fails once and then succeeds leaves NOTHING behind today outside
// the Orker process: Temporal hands the workflow the final result and discards
// attempt 1's error when it schedules attempt 2. After this change the store
// holds one record per attempt — the failed one with its reason code, category,
// exit code and the §7.1 phase it died in, and the successful one after it —
// so a reader can say "attempt 1 failed in `run` with CONTAINER_EXIT_NONZERO,
// attempt 2 succeeded" without having been there.
func TestAttemptRecords_OneRecordPerAttemptOfARetriedTask(t *testing.T) {
	attempts := artifacts.NewMemoryAttemptStore()
	runtime := &retryingRuntime{}
	dispatcher := newAttemptDispatcher(t, runtime, attempts)
	in := baseExportInput("flaky")

	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	env.SetTestTimeout(60 * time.Second)
	env.RegisterWorkflow(attemptProbeWorkflow)
	env.RegisterActivityWithOptions(dispatcher.Execute, activity.RegisterOptions{Name: "ExecuteTask"})

	env.ExecuteWorkflow(attemptProbeWorkflow, in)

	if !env.IsWorkflowCompleted() {
		t.Fatal("workflow did not complete")
	}
	if err := env.GetWorkflowError(); err != nil {
		t.Fatalf("workflow error = %v, want the second attempt to succeed", err)
	}
	if runtime.runs != 2 {
		t.Fatalf("the runtime ran %d containers, want 2 (fail once, then succeed)", runtime.runs)
	}

	records, err := attempts.ListAttempts(context.Background(), in.GetRunId(), in.GetTaskId(), artifacts.NoItem)
	if err != nil {
		t.Fatalf("ListAttempts: %v", err)
	}
	if len(records) != 2 {
		t.Fatalf("stored %d attempt records, want one per attempt (2): %+v", len(records), records)
	}

	first, second := records[0], records[1]
	if first.GetAttempt() != 1 || second.GetAttempt() != 2 {
		t.Fatalf("attempt numbers = %d, %d; want 1, 2 ascending",
			first.GetAttempt(), second.GetAttempt())
	}

	// The failed attempt: the row that does not exist anywhere else.
	if first.GetOutcome() != orkstackv1.AttemptOutcome_ATTEMPT_OUTCOME_FAILED {
		t.Errorf("attempt 1 outcome = %v, want FAILED", first.GetOutcome())
	}
	if got := first.GetError().GetCode(); got != ReasonContainerExitNonzero {
		t.Errorf("attempt 1 error code = %q, want %q", got, ReasonContainerExitNonzero)
	}
	if got := first.GetError().GetCategory(); got != orkstackv1.FailureCategory_FAILURE_CATEGORY_ACTIVITY_FAILED {
		t.Errorf("attempt 1 error category = %v, want ACTIVITY_FAILED", got)
	}
	if !first.GetError().GetRetryable() {
		t.Error("attempt 1 error is not retryable, but the task was in fact retried")
	}
	if first.GetExitCode() != 1 {
		t.Errorf("attempt 1 exit_code = %d, want 1", first.GetExitCode())
	}
	if first.GetPhase() != "exit" {
		// The phase the attempt ENDED in. A non-zero status is classified at
		// STEP 11, so "exit" is where this failure was decided — and saying
		// "run" instead would be a different story about the same code.
		t.Errorf("attempt 1 phase = %q, want %q", first.GetPhase(), "exit")
	}
	if len(first.GetSystemArtifacts()) == 0 {
		t.Error("attempt 1 recorded no system artifacts; a failed attempt still publishes its streams")
	}
	if first.GetImage().GetPlatformDigest() == "" {
		t.Error("attempt 1 recorded no platform digest; the image as EXECUTED is what the record is for")
	}

	// The successful attempt gets a row too, so the timeline is complete
	// without a reader having to synthesize its last row from TaskRun.
	if second.GetOutcome() != orkstackv1.AttemptOutcome_ATTEMPT_OUTCOME_SUCCEEDED {
		t.Errorf("attempt 2 outcome = %v, want SUCCEEDED", second.GetOutcome())
	}
	if second.GetError() != nil {
		t.Errorf("attempt 2 carries an error: %v", second.GetError())
	}
	if second.GetExitCode() != 0 {
		t.Errorf("attempt 2 exit_code = %d, want 0", second.GetExitCode())
	}
	if second.GetPhase() != "commit" {
		t.Errorf("attempt 2 phase = %q, want %q", second.GetPhase(), "commit")
	}

	// Full context on every row (§2.2 open question 12): "which Orker, which
	// pool, which image" is the retry question, so the record answers it
	// without a log search.
	for _, record := range records {
		if record.GetSchemaVersion() != artifacts.AttemptSchemaVersion {
			t.Errorf("attempt %d schema_version = %d, want %d",
				record.GetAttempt(), record.GetSchemaVersion(), artifacts.AttemptSchemaVersion)
		}
		if record.GetRunId() != in.GetRunId() || record.GetTaskId() != in.GetTaskId() {
			t.Errorf("attempt %d coordinates = %q/%q", record.GetAttempt(), record.GetRunId(), record.GetTaskId())
		}
		if record.GetOrkerId() != "orker-attempt-test" || record.GetBuildId() != "build-attempt-test" {
			t.Errorf("attempt %d orker/build = %q/%q", record.GetAttempt(), record.GetOrkerId(), record.GetBuildId())
		}
		if record.GetPool() != "main" {
			t.Errorf("attempt %d pool = %q, want %q", record.GetAttempt(), record.GetPool(), "main")
		}
		if record.GetStartedAt() == nil || record.GetFinishedAt() == nil {
			t.Errorf("attempt %d is missing a timestamp: %v .. %v",
				record.GetAttempt(), record.GetStartedAt(), record.GetFinishedAt())
		}
		if record.GetCommitAdopted() {
			t.Errorf("attempt %d claims an adopted commit; this test configures no commit store",
				record.GetAttempt())
		}
	}
}

// failingAttemptStore is an AttemptStore that never accepts anything.
type failingAttemptStore struct {
	mu    sync.Mutex
	calls int
}

func (s *failingAttemptStore) PutAttempt(context.Context, *orkstackv1.TaskAttempt) error {
	s.mu.Lock()
	s.calls++
	s.mu.Unlock()
	return errors.New("the object store is down")
}

func (s *failingAttemptStore) ListAttempts(
	context.Context, string, string, artifacts.Item,
) ([]*orkstackv1.TaskAttempt, error) {
	return nil, errors.New("the object store is down")
}

// TestAttemptRecords_AreBestEffort is the rule that makes this feature safe to
// turn on: an attempt record that cannot be written is a log line, never a task
// failure and never a changed outcome. Retry VISIBILITY must not become a new
// way for work to fail.
func TestAttemptRecords_AreBestEffort(t *testing.T) {
	store := &failingAttemptStore{}
	runtime := &phaseCapturingRuntime{exitCode: 0}
	dispatcher := newAttemptDispatcher(t, runtime, store)

	result, err := dispatcher.Execute(context.Background(), baseExportInput("best-effort"))
	if err != nil {
		t.Fatalf("Execute: %v — a failed attempt-record write changed the task's outcome", err)
	}
	if result == nil {
		t.Fatal("Execute returned no result")
	}
	if store.calls != 1 {
		t.Fatalf("PutAttempt called %d times, want exactly 1 per attempt", store.calls)
	}
}

// TestAttemptRecords_NilStoreWritesNothing is what the Attempts doc comment
// promises, and it is the configuration every test in this package that
// predates the field still runs in.
func TestAttemptRecords_NilStoreWritesNothing(t *testing.T) {
	dispatcher := newAttemptDispatcher(t, &phaseCapturingRuntime{exitCode: 0}, nil)
	if _, err := dispatcher.Execute(context.Background(), baseExportInput("nil-attempts")); err != nil {
		t.Fatalf("Execute with no attempt store: %v", err)
	}
}

// TestAttemptRecords_CancellationIsStillRecorded covers the path the detached
// context exists for: the attempt whose ctx is ALREADY done is exactly the one
// an operator wants the record of, so the write must not ride on it.
func TestAttemptRecords_CancellationIsStillRecorded(t *testing.T) {
	attempts := artifacts.NewMemoryAttemptStore()
	dispatcher := newAttemptDispatcher(t, &phaseCapturingRuntime{exitCode: 0}, attempts)
	in := baseExportInput("canceled")

	ctx, cancel := context.WithCancel(context.Background())
	// Cancelled before Execute even starts, so ctx.Err() is non-nil at every
	// point the record could be written on.
	cancel()
	if _, err := dispatcher.Execute(ctx, in); err == nil {
		t.Fatal("Execute on a cancelled context returned no error")
	}

	records, err := attempts.ListAttempts(context.Background(), in.GetRunId(), in.GetTaskId(), artifacts.NoItem)
	if err != nil {
		t.Fatalf("ListAttempts: %v", err)
	}
	if len(records) != 1 {
		t.Fatalf("stored %d records for a cancelled attempt, want 1", len(records))
	}
	if got := records[0].GetOutcome(); got != orkstackv1.AttemptOutcome_ATTEMPT_OUTCOME_CANCELED {
		t.Errorf("outcome = %v, want CANCELED", got)
	}
	if records[0].GetError() != nil {
		t.Errorf("a cancelled attempt carries an error: %v", records[0].GetError())
	}
}

// TestFailureErrorProto_MatchesTheEngineShape pins the two producers of one
// Error together: what the Orker writes onto an attempt record must say the
// same thing, in the same fields, that the engine's recordFailure writes onto
// TaskRun.error for the same failure once it has crossed Temporal.
func TestFailureErrorProto_MatchesTheEngineShape(t *testing.T) {
	failure := Fail(ReasonValuesMissing, "task %q: no values.json", "gamma").
		WithDetail(&orkstackv1.ActivityFailureDetail{
			ExitCode:  0,
			ScanNotes: []string{"ARTIFACT_NOT_A_FILE: /orkstack/output/artifacts/link is a symlink"},
		})

	got := failure.ErrorProto("gamma")
	if got.GetCode() != ReasonValuesMissing {
		t.Errorf("code = %q, want the §8.2 reason %q", got.GetCode(), ReasonValuesMissing)
	}
	if got.GetCategory() != orkstackv1.FailureCategory_FAILURE_CATEGORY_CONTRACT_VIOLATION {
		t.Errorf("category = %v, want CONTRACT_VIOLATION", got.GetCategory())
	}
	if got.GetRetryable() {
		t.Error("retryable = true for a contract violation")
	}
	if got.GetMessage() != failure.Error() {
		t.Errorf("message = %q, want Failure.Error() %q", got.GetMessage(), failure.Error())
	}
	// Scan notes are notes ON the workload's error, appended to the task's own
	// name — the exact shape recordFailure builds.
	if got.GetDetails() != `task "gamma"; ARTIFACT_NOT_A_FILE: /orkstack/output/artifacts/link is a symlink` {
		t.Errorf("details = %q", got.GetDetails())
	}

	// A nil Failure has no error to render, and a caller on the success path
	// must not have to check first.
	var absent *Failure
	if absent.ErrorProto("gamma") != nil {
		t.Error("a nil Failure rendered an error")
	}
	if absent.GetDetail() != nil {
		t.Error("a nil Failure rendered a detail")
	}
}

// reservationDispatcher is newAttemptDispatcher with a policy whose defaults
// differ from the package defaults, so a record that carried the package
// constants instead of the policy's values would be caught.
func reservationDispatcher(t *testing.T, runtime ContainerRuntime, attempts artifacts.AttemptStore) *OCIDispatcher {
	t.Helper()
	dispatcher := newAttemptDispatcher(t, runtime, attempts)
	dispatcher.Policy.DefaultCPU = "1"
	dispatcher.Policy.DefaultMemory = "2Gi"
	return dispatcher
}

func assertReservation(t *testing.T, label string, record *orkstackv1.TaskAttempt, cpu float64, memory int64) {
	t.Helper()
	got := record.GetResources()
	if got == nil {
		t.Fatalf("%s: record carries no reservation, want {%v, %d}", label, cpu, memory)
	}
	if got.GetCpu() != cpu || got.GetMemoryBytes() != memory {
		t.Fatalf("%s: reservation = {%v, %d}, want {%v, %d}", label, got.GetCpu(), got.GetMemoryBytes(), cpu, memory)
	}
}

// TestAttemptRecord_CarriesTheReservationTheContainerIsGiven: the record says
// what the Orker granted — its default for a task that asked for nothing, the
// request otherwise — whatever the attempt's outcome, because the Orker's
// slot was held for the whole attempt either way.
func TestAttemptRecord_CarriesTheReservationTheContainerIsGiven(t *testing.T) {
	type outcome struct {
		name    string
		run     func(t *testing.T, d *OCIDispatcher, in *orkstackv1.ExecuteTaskInput)
		runtime func() ContainerRuntime
	}
	succeed := func(t *testing.T, d *OCIDispatcher, in *orkstackv1.ExecuteTaskInput) {
		if _, err := d.Execute(context.Background(), in); err != nil {
			t.Fatalf("Execute: %v", err)
		}
	}
	fail := func(t *testing.T, d *OCIDispatcher, in *orkstackv1.ExecuteTaskInput) {
		if _, err := d.Execute(context.Background(), in); err == nil {
			t.Fatal("Execute: want a failure for exit 8")
		}
	}
	cancel := func(t *testing.T, d *OCIDispatcher, in *orkstackv1.ExecuteTaskInput) {
		ctx, stop := context.WithCancel(context.Background())
		stop()
		if _, err := d.Execute(ctx, in); err == nil {
			t.Fatal("Execute on a cancelled context returned no error")
		}
	}
	panics := func(t *testing.T, d *OCIDispatcher, in *orkstackv1.ExecuteTaskInput) {
		defer func() {
			if recover() == nil {
				t.Fatal("Execute: want a re-panic from the runtime")
			}
		}()
		_, _ = d.Execute(context.Background(), in)
	}
	outcomes := []outcome{
		{"succeeded", succeed, func() ContainerRuntime { return &phaseCapturingRuntime{exitCode: 0} }},
		{"failed", fail, func() ContainerRuntime { return &phaseCapturingRuntime{exitCode: 8} }},
		{"canceled", cancel, func() ContainerRuntime { return &phaseCapturingRuntime{exitCode: 0} }},
		{"panic", panics, func() ContainerRuntime { return panicCreateRuntime{} }},
	}
	requests := []struct {
		name   string
		req    *orkstackv1.ResourceRequest
		cpu    float64
		memory int64
	}{
		{"no request", nil, 1, 2 << 30},
		{"500m and 512Mi", &orkstackv1.ResourceRequest{Cpu: "500m", Memory: "512Mi"}, 0.5, 512 << 20},
	}
	for _, req := range requests {
		for _, oc := range outcomes {
			t.Run(req.name+"/"+oc.name, func(t *testing.T) {
				attempts := artifacts.NewMemoryAttemptStore()
				dispatcher := reservationDispatcher(t, oc.runtime(), attempts)
				in := baseExportInput("reserve-" + oc.name)
				in.Resources = req.req
				oc.run(t, dispatcher, in)
				assertReservation(t, oc.name, onlyAttempt(t, attempts, in), req.cpu, req.memory)
			})
		}
	}
}

// TestAttemptRecord_ARefusedRequestCarriesNoReservation: an attempt the
// policy refused reserved nothing, and its record must not say otherwise.
func TestAttemptRecord_ARefusedRequestCarriesNoReservation(t *testing.T) {
	attempts := artifacts.NewMemoryAttemptStore()
	dispatcher := reservationDispatcher(t, &phaseCapturingRuntime{exitCode: 0}, attempts)
	in := baseExportInput("reserve-refused")
	in.Resources = &orkstackv1.ResourceRequest{Cpu: "64"}
	if _, err := dispatcher.Execute(context.Background(), in); err == nil {
		t.Fatal("Execute: want a RESOURCE_REFUSED failure for cpu above the cap")
	}
	record := onlyAttempt(t, attempts, in)
	if record.GetOutcome() != orkstackv1.AttemptOutcome_ATTEMPT_OUTCOME_FAILED {
		t.Errorf("outcome = %v, want FAILED", record.GetOutcome())
	}
	if got := record.GetError().GetCode(); got != ReasonResourceRefused {
		t.Errorf("error code = %q, want %q", got, ReasonResourceRefused)
	}
	if record.GetResources() != nil {
		t.Errorf("a refused request recorded a reservation: %v", record.GetResources())
	}
}

// TestAttemptRecord_AnAdoptedCommitCarriesTheReservation: an attempt that
// adopted a commit held the Orker's slot for its commit check, so its record
// carries the reservation as any attempt's does.
func TestAttemptRecord_AnAdoptedCommitCarriesTheReservation(t *testing.T) {
	dispatcher := reservationDispatcher(t, &phaseCapturingRuntime{exitCode: 0}, artifacts.NewMemoryAttemptStore())
	dispatcher.Commits = artifacts.NewMemoryCommitStore()
	in := baseExportInput("reserve-adopt")
	in.CommitUri = "mem://commits/" + in.GetRunId() + "/" + in.GetTaskId() + "/commit.json"
	if _, err := dispatcher.Execute(context.Background(), in); err != nil {
		t.Fatalf("first Execute: %v", err)
	}
	adoptStore := artifacts.NewMemoryAttemptStore()
	dispatcher.Attempts = adoptStore
	second, err := dispatcher.Execute(context.Background(), in)
	if err != nil {
		t.Fatalf("second Execute: %v", err)
	}
	if !second.GetCommitAdopted() {
		t.Fatal("the second attempt did not adopt the first attempt's commit")
	}
	record := onlyAttempt(t, adoptStore, in)
	if !record.GetCommitAdopted() {
		t.Error("the adopting record does not say commit_adopted")
	}
	assertReservation(t, "adopted", record, 1, 2<<30)
}
