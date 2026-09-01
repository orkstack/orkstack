package engine_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/workflow"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/internal/task"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

const dispatchDocument = `
name: dispatch
version: 1.0.0
tasks:
  - id: build
    uses: shell
    with: {command: "echo build"}
`

// TestRunWorkflow_BuildsTheActivityInputFromTheResolvedTask is the engine half
// of the wire: what the Orker actually receives.
func TestRunWorkflow_BuildsTheActivityInputFromTheResolvedTask(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	h := newHarness(t, &suite, func(in *orkstackv1.ExecuteTaskInput) (*orkstackv1.ExecuteTaskResult, error) {
		return succeeded(in.TaskId, map[string]any{"ok": true}), nil
	})

	wf := compile(t, dispatchDocument)
	// The image is the APPLY-TIME resolution; the workflow never resolves a
	// tag, because workflow code may not reach a registry and a run must
	// execute what its definition pinned (§4.1).
	wf.Tasks[0].Image = &orkstackv1.ImageRef{
		Reference:   "ghcr.io/orkstack/activity-shell:0.1",
		Pinned:      "ghcr.io/orkstack/activity-shell@sha256:" + strings.Repeat("a", 64),
		IndexDigest: "sha256:" + strings.Repeat("a", 64),
	}
	wf.Tasks[0].Command = []string{"/bin/sh", "-c", "echo build"}
	wf.Tasks[0].Env = map[string]string{"GREETING": "hello"}
	wf.Tasks[0].Network = orkstackv1.NetworkMode_NETWORK_MODE_NONE
	wf.Tasks[0].Resources = &orkstackv1.ResourceRequest{Cpu: "2", Memory: "4Gi"}

	plan := defaultPlan()
	plan.CommitRoot = "s3://orkstack-artifacts"
	plan.Limits = engine.DefaultLimits()
	plan.TransferBudgetSeconds = 300

	h.env.ExecuteWorkflow(engine.RunWorkflow, "ork-dispatch-1", wf, payload(t, nil), plan)
	if err := h.env.GetWorkflowError(); err != nil {
		t.Fatalf("workflow error: %v", err)
	}

	in, ok := h.inputFor("build")
	if !ok {
		t.Fatal("build was never dispatched")
	}

	// The image travels verbatim. Nothing here re-resolves it.
	if in.GetImage().GetPinned() != wf.Tasks[0].GetImage().GetPinned() {
		t.Errorf("image.pinned = %q, want the definition's %q",
			in.GetImage().GetPinned(), wf.Tasks[0].GetImage().GetPinned())
	}

	// The idempotency key is "{namespace}/{run}/{task}" and is stable across
	// retries, which is what lets an external system implement idempotency
	// against it (AGENTS.md §3, §4).
	if got, want := in.GetIdempotencyKey(), "default/ork-dispatch-1/build"; got != want {
		t.Errorf("idempotency_key = %q, want %q", got, want)
	}

	// The commit URI is composed exactly from the plan's root plus the shared
	// key renderer, so the Orker uses the value it was SENT rather than
	// re-deriving one (§5.2).
	key, err := artifacts.CommitKey("default", "ork-dispatch-1", "build")
	if err != nil {
		t.Fatal(err)
	}
	if got, want := in.GetCommitUri(), "s3://orkstack-artifacts/"+key; got != want {
		t.Errorf("commit_uri = %q, want exactly %q", got, want)
	}

	// The `with:` block travels RAW. Rendering is the Orker's job because
	// `artifact "<task>/<name>"` resolves to a container path only it knows
	// (OQ-1).
	if got := in.GetWith().AsMap()["command"]; got != "echo build" {
		t.Errorf("with.command = %v, want the un-rendered definition value", got)
	}

	// The contract limits travel so the Orker enforces the deployment's
	// numbers rather than its own.
	if in.GetLimits().GetValuesMaxBytes() != engine.DefaultValuesMaxBytes {
		t.Errorf("limits.values_max_bytes = %d, want %d",
			in.GetLimits().GetValuesMaxBytes(), engine.DefaultValuesMaxBytes)
	}
	if in.GetLimits().GetCommitMaxBytes() != engine.DefaultCommitMaxBytes {
		t.Errorf("limits.commit_max_bytes = %d, want %d",
			in.GetLimits().GetCommitMaxBytes(), engine.DefaultCommitMaxBytes)
	}

	// The execution shape the author asked for.
	if strings.Join(in.GetCommand(), " ") != "/bin/sh -c echo build" {
		t.Errorf("command = %v, want the definition's argv", in.GetCommand())
	}
	if in.GetEnv()["GREETING"] != "hello" {
		t.Errorf("env = %v, want the workload's own variables", in.GetEnv())
	}
	if in.GetNetwork() != orkstackv1.NetworkMode_NETWORK_MODE_NONE {
		t.Errorf("network = %v, want NONE", in.GetNetwork())
	}
	if in.GetResources().GetMemory() != "4Gi" {
		t.Errorf("resources = %v, want the request the author wrote", in.GetResources())
	}
}

func TestRunWorkflow_CommitURIIsStableAcrossReplayEquivalentExecutions(t *testing.T) {
	execute := func() string {
		var suite testsuite.WorkflowTestSuite
		h := newHarness(t, &suite, func(in *orkstackv1.ExecuteTaskInput) (*orkstackv1.ExecuteTaskResult, error) {
			return succeeded(in.TaskId, nil), nil
		})
		plan := defaultPlan()
		plan.CommitRoot = "s3://orkstack-artifacts/"
		h.env.ExecuteWorkflow(engine.RunWorkflow, "ork-stable-uri", compile(t, dispatchDocument), payload(t, nil), plan)
		if err := h.env.GetWorkflowError(); err != nil {
			t.Fatalf("workflow error: %v", err)
		}
		in, ok := h.inputFor("build")
		if !ok {
			t.Fatal("build was never dispatched")
		}
		return in.GetCommitUri()
	}

	first, replayEquivalent := execute(), execute()
	key, err := artifacts.CommitKey("default", "ork-stable-uri", "build")
	if err != nil {
		t.Fatal(err)
	}
	if want := "s3://orkstack-artifacts/" + key; first != want || replayEquivalent != want {
		t.Fatalf("commit URIs = %q and %q, want deterministic %q", first, replayEquivalent, want)
	}
}

func TestRunWorkflow_UsesThePlansInputLimitBeforeScheduling(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	h := newHarness(t, &suite, func(in *orkstackv1.ExecuteTaskInput) (*orkstackv1.ExecuteTaskResult, error) {
		return succeeded(in.TaskId, nil), nil
	})
	plan := defaultPlan()
	plan.Limits = engine.DefaultLimits()
	plan.Limits.ValuesMaxBytes = 1

	h.env.ExecuteWorkflow(engine.RunWorkflow, "ork-plan-limit", compile(t, dispatchDocument), payload(t, nil), plan)
	err := h.env.GetWorkflowError()
	if err == nil || !strings.Contains(err.Error(), task.ReasonInputTooLarge) {
		t.Fatalf("workflow error = %v, want %s from the plan's one-byte limit", err, task.ReasonInputTooLarge)
	}
	if _, ok := h.inputFor("build"); ok {
		t.Fatal("activity was scheduled even though its input exceeded the replay-pinned plan limit")
	}
}

// TestRunWorkflow_RecordsTheCommitAndExitCodeOnTheTaskRun is what the read
// model gains: an execution is explainable after the fact.
func TestRunWorkflow_RecordsTheCommitAndExitCodeOnTheTaskRun(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	h := newHarness(t, &suite, func(in *orkstackv1.ExecuteTaskInput) (*orkstackv1.ExecuteTaskResult, error) {
		result := succeeded(in.TaskId, map[string]any{"built": true})
		result.Commit = &orkstackv1.CommitRef{
			Uri:    in.GetCommitUri(),
			Sha256: strings.Repeat("c", 64),
			Etag:   `"etag"`,
		}
		result.SystemArtifacts = []*orkstackv1.ArtifactRef{
			{Name: "stdout.log", Uri: "s3://b/stdout"},
			{Name: "stderr.log", Uri: "s3://b/stderr"},
		}
		result.Attempt = 3
		return result, nil
	})

	plan := defaultPlan()
	plan.CommitRoot = "s3://orkstack-artifacts"

	h.env.ExecuteWorkflow(engine.RunWorkflow, "ork-dispatch-2", compile(t, dispatchDocument), payload(t, nil), plan)
	if err := h.env.GetWorkflowError(); err != nil {
		t.Fatalf("workflow error: %v", err)
	}

	var run *orkstackv1.Run
	if err := h.env.GetWorkflowResult(&run); err != nil {
		t.Fatalf("decode the run: %v", err)
	}
	taskRun := run.GetTaskRuns()[0]

	if taskRun.GetCommit().GetSha256() != strings.Repeat("c", 64) {
		t.Errorf("TaskRun.commit = %v, want the durable completion record", taskRun.GetCommit())
	}
	if taskRun.GetValuesRef().GetUri() == "" {
		t.Error("TaskRun.values_ref is empty; it is the authoritative reference to the result")
	}
	if len(taskRun.GetSystemArtifacts()) != 2 {
		t.Errorf("TaskRun.system_artifacts = %v, want stdout.log and stderr.log",
			taskRun.GetSystemArtifacts())
	}
	// The attempt is the one that produced the result, so a fresh and an
	// adopted result report identically (§2.2 ExecuteTaskResult.attempt).
	if taskRun.GetAttempt() != 3 {
		t.Errorf("TaskRun.attempt = %d, want the producing attempt 3", taskRun.GetAttempt())
	}
	// The inline copy is decoded for DISPLAY. values_ref stays authoritative.
	if got := taskRun.GetOutput().GetData().AsMap()["built"]; got != true {
		t.Errorf("TaskRun.output = %v, want the decoded inline values", taskRun.GetOutput())
	}
}

// TestRunWorkflow_ReadsTheOrkersFailureAsDataNotAsAString is §8.3.
//
// The engine used to flatten every failure into TASK_FAILED with a stringified
// Go error. It now reads the ApplicationError type — which IS the category —
// and the ActivityFailureDetail, so the UI can say "contract violation — this
// image did not write values.json" instead of rendering an error string.
func TestRunWorkflow_ReadsTheOrkersFailureAsDataNotAsAString(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	h := newHarness(t, &suite, func(in *orkstackv1.ExecuteTaskInput) (*orkstackv1.ExecuteTaskResult, error) {
		detail := &orkstackv1.ActivityFailureDetail{
			ReasonCode: "VALUES_MISSING",
			Category:   orkstackv1.FailureCategory_FAILURE_CATEGORY_CONTRACT_VIOLATION,
			ExitCode:   0,
			Artifacts:  []*orkstackv1.ArtifactRef{{Name: "report.txt", Uri: "s3://b/report"}},
			SystemArtifacts: []*orkstackv1.ArtifactRef{
				{Name: "stderr.log", Uri: "s3://b/stderr"},
			},
			ScanNotes: []string{"ARTIFACT_NOT_A_FILE: /orkstack/output/artifacts/link is a symlink"},
			Attempt:   1,
		}
		return nil, temporal.NewNonRetryableApplicationError(
			"VALUES_MISSING: the container exited 0 but wrote no values.json",
			task.CategoryContractViolation, nil, detail)
	})

	h.env.ExecuteWorkflow(engine.RunWorkflow, "ork-dispatch-3", compile(t, dispatchDocument), payload(t, nil), defaultPlan())
	if err := h.env.GetWorkflowError(); err == nil {
		t.Fatal("the run succeeded despite its only task failing")
	}

	// A failed run's snapshot travels on the EVENT stream, which is where
	// every reader gets it: the workflow's own return value carries the
	// error. The projection activity stored it in the harness's run store.
	run, ok := h.store.Get("ork-dispatch-3")
	if !ok {
		t.Fatal("the failed run was never projected, so no reader could see why it failed")
	}
	var taskRun *orkstackv1.TaskRun
	for _, candidate := range run.GetTaskRuns() {
		if candidate.GetTaskId() == "build" {
			taskRun = candidate
		}
	}
	if taskRun == nil {
		t.Fatalf("the projected run has no build task: %v", run.GetTaskRuns())
	}
	failure := taskRun.GetError()

	if failure.GetCode() != "VALUES_MISSING" {
		t.Errorf("Error.code = %q, want the §8.2 reason code VALUES_MISSING", failure.GetCode())
	}
	if failure.GetCategory() != orkstackv1.FailureCategory_FAILURE_CATEGORY_CONTRACT_VIOLATION {
		t.Errorf("Error.category = %v, want CONTRACT_VIOLATION", failure.GetCategory())
	}
	if failure.GetRetryable() {
		t.Error("Error.retryable is true for a contract violation")
	}
	// A failed attempt is as inspectable as a successful one.
	if len(taskRun.GetArtifacts()) != 1 {
		t.Errorf("a failed TaskRun has artifacts %v; what the task produced must stay reachable",
			taskRun.GetArtifacts())
	}
	if len(taskRun.GetSystemArtifacts()) != 1 {
		t.Errorf("a failed TaskRun has system artifacts %v; its stderr must stay reachable",
			taskRun.GetSystemArtifacts())
	}
	// Scan notes are notes ON the error, never a replacement for it.
	if !strings.Contains(failure.GetDetails(), "ARTIFACT_NOT_A_FILE") {
		t.Errorf("Error.details = %q, want the scan note attached", failure.GetDetails())
	}
	if !strings.Contains(failure.GetMessage(), "VALUES_MISSING") {
		t.Errorf("Error.message = %q, want the workload's own reason", failure.GetMessage())
	}
}

// TestRunWorkflow_SetsTheHeartbeatAndCancellationOptions is acceptance
// criterion 5 and §6.3's first requirement.
//
// Without HeartbeatTimeout the SDK throttles heartbeats and cancellation
// cannot reach a running container at all; without WaitForCancellation the
// workflow may mark a task terminal before the Orker finishes killing and
// cleaning up. Both are invisible in a system that is not being cancelled,
// which is why they are asserted rather than assumed.
func TestRunWorkflow_SetsTheHeartbeatAndCancellationOptions(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	env.SetTestTimeout(30 * time.Second)
	env.RegisterWorkflowWithOptions(engine.RunWorkflow, workflow.RegisterOptions{Name: engine.RunWorkflowType})
	env.RegisterActivityWithOptions(
		func(actCtx context.Context, in *orkstackv1.ExecuteTaskInput) (*orkstackv1.ExecuteTaskResult, error) {
			info := activity.GetInfo(actCtx)
			if info.HeartbeatTimeout <= 0 {
				t.Errorf("HeartbeatTimeout is %v; Temporal delivers cancellation ONLY through a "+
					"heartbeat response, so without it cancellation cannot reach a running container",
					info.HeartbeatTimeout)
			}
			// Assert against the SEAM's constant, not a literal. A literal
			// here asserted the engine's own value against itself, so the two
			// halves could drift apart without anything failing.
			if info.HeartbeatTimeout != task.HeartbeatTimeout {
				t.Errorf("HeartbeatTimeout = %v, want task.HeartbeatTimeout %v, which is the "+
					"number the Orker derives its tick rate from",
					info.HeartbeatTimeout, task.HeartbeatTimeout)
			}
			// The Orker must tick at least three times inside the window, or
			// Temporal declares a live activity dead.
			if task.HeartbeatInterval*3 > task.HeartbeatTimeout {
				t.Errorf("the Orker ticks every %v inside a %v timeout; a live activity "+
					"would be declared dead", task.HeartbeatInterval, task.HeartbeatTimeout)
			}
			// StartToClose has to leave room for staging, the pull, hashing
			// and uploads on top of the task's own wall clock (§6.3).
			if info.StartToCloseTimeout <= 300*time.Second {
				t.Errorf("StartToCloseTimeout = %v, want the task timeout plus the grace period "+
					"plus the plan's transfer budget", info.StartToCloseTimeout)
			}
			return succeeded(in.TaskId, map[string]any{"ok": true}), nil
		},
		activity.RegisterOptions{Name: task.ActivityExecuteTask},
	)
	snapshots := newTestSnapshotStore()
	env.RegisterActivityWithOptions(
		engine.NewSnapshotActivities(snapshots).WriteSnapshot,
		activity.RegisterOptions{Name: engine.ActivityWriteSnapshot},
	)
	env.RegisterActivityWithOptions(
		engine.NewEventActivities(engine.NewEventBus(), engine.NewRunStore()).WithSnapshotStore(snapshots).PublishEvent,
		activity.RegisterOptions{Name: engine.ActivityPublishEvent},
	)

	plan := defaultPlan()
	plan.TransferBudgetSeconds = 600

	env.ExecuteWorkflow(engine.RunWorkflow, "ork-dispatch-4", compile(t, dispatchDocument), payload(t, nil), plan)
	if err := env.GetWorkflowError(); err != nil {
		t.Fatalf("workflow error: %v", err)
	}
}

// TestNonRetryableCategories_AreWhatTheEngineTellsTemporal pins the list the
// retry policy carries. The constructor's flag is the primary mechanism; this
// list is what makes the intent visible to a reader of the policy (§8.1).
func TestNonRetryableCategories_AreWhatTheEngineTellsTemporal(t *testing.T) {
	got := strings.Join(task.NonRetryableCategories(), ",")
	const want = "ContractViolation,TimedOut"
	if got != want {
		t.Errorf("NonRetryableCategories = %q, want %q. TimedOut is terminal by default because "+
			"acceptance criterion 7 groups timeout with the permanent failures; flipping it back "+
			"is a one-line change here (§17 C1)", got, want)
	}
}

// TestCheckInputSize_RefusesAnOversizedActivityInput is the engine's own
// INPUT_TOO_LARGE gate.
//
// AGENTS.md §2.2 caps a serialized Temporal payload at 256 KiB. Checking it
// here, deterministically and before scheduling, turns a Temporal-side
// rejection nobody can attribute into a clean ContractViolation naming the
// task.
func TestCheckInputSize_RefusesAnOversizedActivityInput(t *testing.T) {
	small := &orkstackv1.ExecuteTaskInput{TaskId: "small", RunId: "r"}
	if err := task.CheckInputSize(small); err != nil {
		t.Fatalf("a small input was refused: %v", err)
	}

	// A `with:` block that grew past the wire budget is the realistic way
	// this happens.
	oversized := &orkstackv1.ExecuteTaskInput{
		TaskId: "oversized",
		RunId:  "r",
		Env:    map[string]string{"BLOB": strings.Repeat("x", task.MaxActivityInputBytes)},
	}
	err := task.CheckInputSize(oversized)
	if err == nil {
		t.Fatal("an input over 256 KiB was accepted")
	}
	if !strings.Contains(err.Error(), task.ReasonInputTooLarge) {
		t.Errorf("error = %v, want it to name %s", err, task.ReasonInputTooLarge)
	}
	if !strings.Contains(err.Error(), "artifact") {
		t.Errorf("error = %v, want it to say what to do instead (move the bytes into an artifact)", err)
	}
}
