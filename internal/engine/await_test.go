package engine_test

import (
	"errors"
	"strings"
	"testing"
	"time"

	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/testsuite"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/engine"
)

// awaitedDocument is a → await b → c: the narrative's shape, small.
const awaitedDocument = `
apiVersion: orkstack/v1
kind: Orkflow
name: awaited
tasks:
  - id: a
    uses: shell
    with: {command: "echo a"}
  - id: b
    needs: [a]
    await:
      timeout_seconds: 3600
      callback: true
      schema:
        model_uri: {type: string, required: true}
  - id: c
    uses: shell
    needs: [b]
    with: {command: "echo {{ .tasks.b.output.model_uri }}"}
`

const awaitedConditionDocument = `
apiVersion: orkstack/v1
kind: Orkflow
name: awaited-condition
tasks:
  - id: b
    await: {timeout_seconds: 600}
  - id: c
    uses: shell
    needs: [b]
    condition: "{{ .tasks.b.output.approved }}"
    with: {command: "echo c"}
`

const awaitedSiblingDocument = `
apiVersion: orkstack/v1
kind: Orkflow
name: awaited-sibling
tasks:
  - id: a
    uses: shell
    with: {command: "echo a"}
  - id: b
    needs: [a]
    await: {timeout_seconds: 3600}
  - id: c
    uses: shell
    needs: [a]
    with: {command: "exit 1"}
`

const awaitedSkippedDocument = `
apiVersion: orkstack/v1
kind: Orkflow
name: awaited-skipped
inputs:
  gate: {type: boolean, default: false}
tasks:
  - id: b
    condition: "{{ .inputs.gate }}"
    await: {timeout_seconds: 600}
  - id: c
    uses: shell
    needs: [b]
    with: {command: "echo c"}
`

const awaitOnlyDocument = `
apiVersion: orkstack/v1
kind: Orkflow
name: await-only
tasks:
  - id: first
    await: {timeout_seconds: 600}
  - id: second
    needs: [first]
    await: {timeout_seconds: 600}
`

// valuesRef is a stored values.json reference as a door would hand it over.
func valuesRef(task string, body []byte) *orkstackv1.ArtifactRef {
	return &orkstackv1.ArtifactRef{
		Name:      "values.json",
		Uri:       "memory://orkflows/awaited/run/" + task + "/values.json/abc",
		Sha256:    "abc",
		SizeBytes: int64(len(body)),
	}
}

func completion(task string, body string, principal string) *orkstackv1.AwaitSignal {
	return &orkstackv1.AwaitSignal{
		TaskId:     task,
		Values:     valuesRef(task, []byte(body)),
		ValuesJson: []byte(body),
		Via:        orkstackv1.AwaitResolvedVia_AWAIT_RESOLVED_VIA_API,
		Principal:  principal,
	}
}

func okTask(in *orkstackv1.ExecuteTaskInput) (*orkstackv1.ExecuteTaskResult, error) {
	return &orkstackv1.ExecuteTaskResult{TaskId: in.TaskId, ValuesJson: valuesJSON(map[string]any{})}, nil
}

// signalAt delivers sig to the run under test after d of workflow time.
func (h *harness) signalAt(d time.Duration, sig *orkstackv1.AwaitSignal) {
	h.env.RegisterDelayedCallback(func() {
		h.env.SignalWorkflow(engine.ResolveAwaitSignal, sig)
	}, d)
}

func eventsOf(h *harness, runID string) []*orkstackv1.Event {
	return h.bus.Events(runID, 0)
}

func eventsFor(h *harness, runID, taskID string) []*orkstackv1.Event {
	var out []*orkstackv1.Event
	for _, e := range eventsOf(h, runID) {
		if e.GetTaskId() == taskID {
			out = append(out, e)
		}
	}
	return out
}

func eventTypes(events []*orkstackv1.Event) []string {
	out := make([]string, 0, len(events))
	for _, e := range events {
		out = append(out, strings.TrimPrefix(e.GetType().String(), "EVENT_TYPE_"))
	}
	return out
}

func runOf(t *testing.T, h *harness, runID string) *orkstackv1.Run {
	t.Helper()
	run, ok := h.store.Get(runID)
	if !ok {
		t.Fatalf("%s: run read model was not populated", runID)
	}
	return run
}

func TestRunWorkflow_AwaitOpensWithoutAnActivityAndCompletesOnResolution(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	h := newHarness(t, &suite, okTask)
	body := `{"model_uri": "s3://models/88421"}`
	h.signalAt(time.Minute, completion("b", body, "alice"))

	h.env.ExecuteWorkflow(engine.RunWorkflow, "run-await-complete", compile(t, awaitedDocument), payload(t, nil), defaultPlan(), (*orkstackv1.RunContinuation)(nil))
	if !h.env.IsWorkflowCompleted() {
		t.Fatal("workflow did not complete")
	}
	if err := h.env.GetWorkflowError(); err != nil {
		t.Fatalf("workflow error: %v", err)
	}
	if got := strings.Join(h.taskOrder(), ","); got != "a,c" {
		t.Fatalf("ExecuteTask called for %s, want a,c — an await starts no activity", got)
	}

	events := eventsFor(h, "run-await-complete", "b")
	if got := strings.Join(eventTypes(events), ","); got != "TASK_AWAITING,TASK_RESOLVED,TASK_SUCCEEDED" {
		t.Fatalf("b's events = %s", got)
	}
	awaiting := events[0]
	if awaiting.GetStatus() != orkstackv1.Status_STATUS_AWAITING {
		t.Fatalf("TASK_AWAITING status = %v", awaiting.GetStatus())
	}
	run := runOf(t, h, "run-await-complete")
	b := taskRunOf(t, run, "b")
	deadline := b.GetAwait().GetDeadline().AsTime()
	if !strings.Contains(awaiting.GetMessage(), deadline.UTC().Format(time.RFC3339)) ||
		!strings.Contains(awaiting.GetMessage(), "; accepts a callback") {
		t.Fatalf("TASK_AWAITING message %q does not name the deadline %s and the callback", awaiting.GetMessage(), deadline)
	}
	if got := events[1].GetMessage(); got != "await resolved via api by alice" {
		t.Fatalf("TASK_RESOLVED message = %q", got)
	}

	if b.GetStatus() != orkstackv1.Status_STATUS_SUCCEEDED {
		t.Fatalf("b status = %v", b.GetStatus())
	}
	opened := awaiting.GetTimestamp().AsTime()
	if !b.GetScheduledAt().AsTime().Equal(opened) || !b.GetStartedAt().AsTime().Equal(opened) {
		t.Fatalf("scheduled_at %v / started_at %v, want both the open instant %v",
			b.GetScheduledAt().AsTime(), b.GetStartedAt().AsTime(), opened)
	}
	if !deadline.Equal(opened.Add(time.Hour)) {
		t.Fatalf("deadline %v, want the open instant + 1h (%v)", deadline, opened.Add(time.Hour))
	}
	if b.GetFinishedAt() == nil || !b.GetFinishedAt().AsTime().Equal(events[1].GetTimestamp().AsTime()) {
		t.Fatalf("finished_at %v, want the resolution's instant", b.GetFinishedAt())
	}
	if a := b.GetAwait(); !a.GetCallback() || a.GetResolvedVia() != orkstackv1.AwaitResolvedVia_AWAIT_RESOLVED_VIA_API || a.GetResolvedBy() != "alice" {
		t.Fatalf("await = %v", a)
	}
	if b.GetValuesRef().GetUri() != valuesRef("b", []byte(body)).GetUri() {
		t.Fatalf("values_ref = %v", b.GetValuesRef())
	}
	if b.GetOutput().GetData().GetFields()["model_uri"].GetStringValue() != "s3://models/88421" {
		t.Fatalf("output = %v", b.GetOutput())
	}
	if b.GetPool() != "" || b.GetPin() != "" || b.GetOrkerId() != "" || b.GetAttempt() != 0 || b.GetCommit() != nil {
		t.Fatalf("an await carries container fields: %v", b)
	}
	if taskRunOf(t, run, "a").GetAwait() != nil {
		t.Fatal("a container task carries an await")
	}

	in, ok := h.inputFor("c")
	if !ok {
		t.Fatal("c was never dispatched")
	}
	var upstream *orkstackv1.UpstreamRef
	for _, ref := range in.GetUpstream() {
		if ref.GetTaskId() == "b" {
			upstream = ref
		}
	}
	if upstream == nil || upstream.GetValues().GetUri() != b.GetValuesRef().GetUri() || upstream.GetCommitUri() != "" {
		t.Fatalf("c's upstream for b = %v, want the values ref and an empty commit_uri", upstream)
	}
}

func TestRunWorkflow_AwaitOutputDrivesADownstreamCondition(t *testing.T) {
	for _, tc := range []struct {
		body string
		want orkstackv1.Status
	}{
		{`{"approved": false}`, orkstackv1.Status_STATUS_SKIPPED},
		{`{"approved": true}`, orkstackv1.Status_STATUS_SUCCEEDED},
	} {
		t.Run(tc.body, func(t *testing.T) {
			var suite testsuite.WorkflowTestSuite
			h := newHarness(t, &suite, okTask)
			h.signalAt(time.Minute, completion("b", tc.body, ""))
			h.env.ExecuteWorkflow(engine.RunWorkflow, "run-await-condition", compile(t, awaitedConditionDocument), payload(t, nil), defaultPlan(), (*orkstackv1.RunContinuation)(nil))
			if err := h.env.GetWorkflowError(); err != nil {
				t.Fatalf("workflow error: %v", err)
			}
			if got := statusOf(runOf(t, h, "run-await-condition"), "c"); got != tc.want {
				t.Fatalf("c = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestRunWorkflow_AwaitDeadlineFailsTheRunWithAwaitTimedOut(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	h := newHarness(t, &suite, okTask)
	h.env.ExecuteWorkflow(engine.RunWorkflow, "run-await-timeout", compile(t, awaitedDocument), payload(t, nil), defaultPlan(), (*orkstackv1.RunContinuation)(nil))
	if !h.env.IsWorkflowCompleted() {
		t.Fatal("workflow did not complete")
	}
	var appErr *temporal.ApplicationError
	if err := h.env.GetWorkflowError(); !errors.As(err, &appErr) || appErr.Type() != engine.AwaitTimedOutCode {
		t.Fatalf("workflow error = %v, want an %s application error", err, engine.AwaitTimedOutCode)
	}
	run := runOf(t, h, "run-await-timeout")
	if run.GetStatus() != orkstackv1.Status_STATUS_FAILED || run.GetError().GetCode() != "RUN_FAILED" {
		t.Fatalf("run = %v / %v", run.GetStatus(), run.GetError())
	}
	b := taskRunOf(t, run, "b")
	if b.GetStatus() != orkstackv1.Status_STATUS_FAILED {
		t.Fatalf("b status = %v", b.GetStatus())
	}
	e := b.GetError()
	if e.GetCode() != engine.AwaitTimedOutCode || e.GetCategory() != orkstackv1.FailureCategory_FAILURE_CATEGORY_TIMED_OUT || e.GetRetryable() {
		t.Fatalf("b error = %v", e)
	}
	if !strings.Contains(e.GetMessage(), `await "b" was not resolved within 1h0m0s (deadline `) {
		t.Fatalf("message = %q", e.GetMessage())
	}
	if !b.GetFinishedAt().AsTime().Equal(b.GetAwait().GetDeadline().AsTime()) {
		t.Fatalf("finished_at %v, want the deadline %v", b.GetFinishedAt().AsTime(), b.GetAwait().GetDeadline().AsTime())
	}
	if b.GetAwait().GetResolvedVia() != orkstackv1.AwaitResolvedVia_AWAIT_RESOLVED_VIA_UNSPECIFIED {
		t.Fatalf("a timeout recorded a resolver: %v", b.GetAwait())
	}
	if got := strings.Join(eventTypes(eventsFor(h, "run-await-timeout", "b")), ","); got != "TASK_AWAITING,TASK_FAILED,RUN_FAILED" {
		t.Fatalf("b's events = %s", got)
	}
	if statusOf(run, "c") != orkstackv1.Status_STATUS_CANCELED {
		t.Fatalf("c = %v, want CANCELED", statusOf(run, "c"))
	}
}

func TestRunWorkflow_AwaitRejectionFailsTheRunWithItsReason(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	h := newHarness(t, &suite, okTask)
	h.signalAt(time.Minute, &orkstackv1.AwaitSignal{
		TaskId:    "b",
		Reject:    true,
		Reason:    "upstream job cancelled",
		Via:       orkstackv1.AwaitResolvedVia_AWAIT_RESOLVED_VIA_CALLBACK,
		Principal: "ci-bot",
	})
	h.env.ExecuteWorkflow(engine.RunWorkflow, "run-await-reject", compile(t, awaitedDocument), payload(t, nil), defaultPlan(), (*orkstackv1.RunContinuation)(nil))
	var appErr *temporal.ApplicationError
	if err := h.env.GetWorkflowError(); !errors.As(err, &appErr) || appErr.Type() != engine.AwaitRejectedCode {
		t.Fatalf("workflow error = %v, want an %s application error", err, engine.AwaitRejectedCode)
	}
	run := runOf(t, h, "run-await-reject")
	b := taskRunOf(t, run, "b")
	e := b.GetError()
	if b.GetStatus() != orkstackv1.Status_STATUS_FAILED || e.GetCode() != engine.AwaitRejectedCode ||
		e.GetCategory() != orkstackv1.FailureCategory_FAILURE_CATEGORY_ACTIVITY_FAILED || e.GetRetryable() {
		t.Fatalf("b = %v / %v", b.GetStatus(), e)
	}
	if e.GetMessage() != `await "b" was rejected: upstream job cancelled` {
		t.Fatalf("message = %q", e.GetMessage())
	}
	if e.GetDetails() != `task "b"; rejected via callback by ci-bot` {
		t.Fatalf("details = %q", e.GetDetails())
	}
	if b.GetAwait().GetResolvedVia() != orkstackv1.AwaitResolvedVia_AWAIT_RESOLVED_VIA_CALLBACK || b.GetAwait().GetResolvedBy() != "ci-bot" {
		t.Fatalf("await = %v", b.GetAwait())
	}
	if run.GetStatus() != orkstackv1.Status_STATUS_FAILED || !strings.Contains(run.GetError().GetMessage(), "upstream job cancelled") {
		t.Fatalf("run = %v / %v", run.GetStatus(), run.GetError())
	}
	events := eventsFor(h, "run-await-reject", "b")
	if got := strings.Join(eventTypes(events), ","); got != "TASK_AWAITING,TASK_RESOLVED,TASK_FAILED,RUN_FAILED" {
		t.Fatalf("b's events = %s", got)
	}
	if events[1].GetStatus() != orkstackv1.Status_STATUS_FAILED ||
		events[1].GetMessage() != "await rejected via callback by ci-bot: upstream job cancelled" {
		t.Fatalf("TASK_RESOLVED = %v %q", events[1].GetStatus(), events[1].GetMessage())
	}
}

func TestRunWorkflow_ResolutionForATaskNotAwaitingIsLoggedAndIgnored(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	h := newHarness(t, &suite, okTask)
	first := completion("b", `{"model_uri":"first"}`, "alice")
	second := completion("b", `{"model_uri":"second"}`, "bob")
	second.Values.Uri = "memory://second"
	h.env.RegisterDelayedCallback(func() {
		h.env.SignalWorkflow(engine.ResolveAwaitSignal, first)
		h.env.SignalWorkflow(engine.ResolveAwaitSignal, second)
	}, time.Minute)
	h.env.ExecuteWorkflow(engine.RunWorkflow, "run-await-twice", compile(t, awaitedDocument), payload(t, nil), defaultPlan(), (*orkstackv1.RunContinuation)(nil))
	if err := h.env.GetWorkflowError(); err != nil {
		t.Fatalf("workflow error: %v", err)
	}
	events := eventsFor(h, "run-await-twice", "b")
	resolved, ignored := 0, 0
	for _, e := range events {
		switch e.GetType() {
		case orkstackv1.EventType_EVENT_TYPE_TASK_RESOLVED:
			resolved++
		case orkstackv1.EventType_EVENT_TYPE_TASK_LOG:
			ignored++
			if e.GetMessage() != "resolution ignored: task is SUCCEEDED (via api by bob)" ||
				e.GetStatus() != orkstackv1.Status_STATUS_SUCCEEDED {
				t.Fatalf("ignored event = %v %q", e.GetStatus(), e.GetMessage())
			}
		}
	}
	if resolved != 1 || ignored != 1 {
		t.Fatalf("TASK_RESOLVED x%d, ignored x%d; want one of each: %v", resolved, ignored, eventTypes(events))
	}
	b := taskRunOf(t, runOf(t, h, "run-await-twice"), "b")
	if b.GetValuesRef().GetUri() != first.GetValues().GetUri() || b.GetAwait().GetResolvedBy() != "alice" {
		t.Fatalf("b was changed by the losing resolution: %v", b)
	}
}

func TestRunWorkflow_CancelWhileAwaitingEndsCanceled(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	h := newHarness(t, &suite, okTask)
	cancelled := 0
	h.env.SetOnTimerCanceledListener(func(string) { cancelled++ })
	h.env.RegisterDelayedCallback(h.env.CancelWorkflow, time.Minute)
	h.env.ExecuteWorkflow(engine.RunWorkflow, "run-await-cancel", compile(t, awaitedDocument), payload(t, nil), defaultPlan(), (*orkstackv1.RunContinuation)(nil))
	if !h.env.IsWorkflowCompleted() {
		t.Fatal("workflow did not complete")
	}
	run := runOf(t, h, "run-await-cancel")
	if run.GetStatus() != orkstackv1.Status_STATUS_CANCELED || run.GetError().GetCode() != "RUN_CANCELED" {
		t.Fatalf("run = %v / %v", run.GetStatus(), run.GetError())
	}
	b := taskRunOf(t, run, "b")
	if b.GetStatus() != orkstackv1.Status_STATUS_CANCELED || b.GetFinishedAt() == nil {
		t.Fatalf("b = %v finished %v", b.GetStatus(), b.GetFinishedAt())
	}
	if b.GetAwait().GetDeadline() == nil || b.GetAwait().GetResolvedVia() != orkstackv1.AwaitResolvedVia_AWAIT_RESOLVED_VIA_UNSPECIFIED {
		t.Fatalf("await = %v", b.GetAwait())
	}
	found := false
	for _, e := range eventsFor(h, "run-await-cancel", "b") {
		if e.GetType() == orkstackv1.EventType_EVENT_TYPE_TASK_LOG &&
			e.GetMessage() == "task canceled: run ended while this task was awaiting external completion" &&
			e.GetStatus() == orkstackv1.Status_STATUS_CANCELED {
			found = true
		}
	}
	if !found {
		t.Fatalf("no cancellation sentence for b: %v", eventTypes(eventsFor(h, "run-await-cancel", "b")))
	}
	if cancelled == 0 {
		t.Fatal("the deadline timer was never cancelled")
	}
}

func TestRunWorkflow_SiblingFailureCancelsAnOpenAwait(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	h := newHarness(t, &suite, func(in *orkstackv1.ExecuteTaskInput) (*orkstackv1.ExecuteTaskResult, error) {
		if in.TaskId == "c" {
			return nil, temporal.NewNonRetryableApplicationError("c exploded", "ActivityFailed", nil)
		}
		return okTask(in)
	})
	h.env.ExecuteWorkflow(engine.RunWorkflow, "run-await-sibling", compile(t, awaitedSiblingDocument), payload(t, nil), defaultPlan(), (*orkstackv1.RunContinuation)(nil))
	if !h.env.IsWorkflowCompleted() {
		t.Fatal("workflow did not complete")
	}
	run := runOf(t, h, "run-await-sibling")
	if run.GetStatus() != orkstackv1.Status_STATUS_FAILED || !strings.Contains(run.GetError().GetMessage(), "c exploded") {
		t.Fatalf("run = %v / %v", run.GetStatus(), run.GetError())
	}
	b := taskRunOf(t, run, "b")
	if b.GetStatus() != orkstackv1.Status_STATUS_CANCELED || b.GetFinishedAt() == nil {
		t.Fatalf("b = %v", b)
	}
}

func TestRunWorkflow_SkippedAwaitNeverOpens(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	h := newHarness(t, &suite, okTask)
	timers := 0
	h.env.SetOnTimerScheduledListener(func(string, time.Duration) { timers++ })
	h.env.ExecuteWorkflow(engine.RunWorkflow, "run-await-skipped", compile(t, awaitedSkippedDocument), payload(t, nil), defaultPlan(), (*orkstackv1.RunContinuation)(nil))
	if err := h.env.GetWorkflowError(); err != nil {
		t.Fatalf("workflow error: %v", err)
	}
	run := runOf(t, h, "run-await-skipped")
	b := taskRunOf(t, run, "b")
	if b.GetStatus() != orkstackv1.Status_STATUS_SKIPPED || b.GetAwait() == nil || b.GetAwait().GetDeadline() != nil {
		t.Fatalf("b = %v", b)
	}
	if statusOf(run, "c") != orkstackv1.Status_STATUS_SKIPPED {
		t.Fatalf("c = %v, want the upstream skip", statusOf(run, "c"))
	}
	if timers != 0 {
		t.Fatalf("%d timers scheduled for a skipped await", timers)
	}
	for _, e := range eventsOf(h, "run-await-skipped") {
		if e.GetType() == orkstackv1.EventType_EVENT_TYPE_TASK_AWAITING {
			t.Fatalf("a skipped await emitted TASK_AWAITING: %v", e)
		}
	}
}

func TestRunWorkflow_AwaitOnlyOrkflowSucceeds(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	h := newHarness(t, &suite, func(in *orkstackv1.ExecuteTaskInput) (*orkstackv1.ExecuteTaskResult, error) {
		t.Errorf("ExecuteTask called for %q in an await-only orkflow", in.TaskId)
		return okTask(in)
	})
	h.signalAt(time.Minute, completion("first", `{"step":1}`, ""))
	h.signalAt(2*time.Minute, completion("second", `{"step":2}`, ""))
	// No route at all: an await-only orkflow plans with empty Routes.
	h.env.ExecuteWorkflow(engine.RunWorkflow, "run-await-only", compile(t, awaitOnlyDocument), payload(t, nil),
		engine.RunPlan{Namespace: "default", Routes: map[string]string{}}, (*orkstackv1.RunContinuation)(nil))
	if err := h.env.GetWorkflowError(); err != nil {
		t.Fatalf("workflow error: %v", err)
	}
	run := runOf(t, h, "run-await-only")
	if run.GetStatus() != orkstackv1.Status_STATUS_SUCCEEDED {
		t.Fatalf("run = %v / %v", run.GetStatus(), run.GetError())
	}
	if len(h.invocations()) != 0 {
		t.Fatalf("%d activities dispatched", len(h.invocations()))
	}
	leaf := run.GetOutput().GetData().GetFields()["second"].GetStructValue().GetFields()["step"].GetNumberValue()
	if leaf != 2 {
		t.Fatalf("run output = %v, want the leaf await's values", run.GetOutput())
	}
}

// A run with no await registers no signal channel, schedules no timer, and
// snapshots no TaskRun.await: the command stream the golden fixture holds.
func TestRunWorkflow_NoAwaitNoTimerAndNoAwaitState(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	h := newHarness(t, &suite, okTask)
	timers := 0
	h.env.SetOnTimerScheduledListener(func(string, time.Duration) { timers++ })
	h.env.ExecuteWorkflow(engine.RunWorkflow, "run-no-await", compile(t, linearDocument), payload(t, nil), defaultPlan(), (*orkstackv1.RunContinuation)(nil))
	if err := h.env.GetWorkflowError(); err != nil {
		t.Fatalf("workflow error: %v", err)
	}
	if timers != 0 {
		t.Fatalf("%d timers in a run with no await", timers)
	}
	for _, tr := range runOf(t, h, "run-no-await").GetTaskRuns() {
		if tr.GetAwait() != nil {
			t.Fatalf("task %q carries await state", tr.GetTaskId())
		}
	}
}
