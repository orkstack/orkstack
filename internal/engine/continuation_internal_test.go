package engine

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/workflow"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// refusalHarness runs RunWorkflow under a test environment with the real
// WriteFailedRunSnapshot over an in-memory store and a recording publisher.
type refusalHarness struct {
	env    *testsuite.TestWorkflowEnvironment
	store  *memSnapshotStore
	mu     sync.Mutex
	events []*orkstackv1.Event
	uris   []string
}

const refusalWorkflowID = "ork-1700000000-42"

func newRefusalHarness(t *testing.T) *refusalHarness {
	t.Helper()
	var suite testsuite.WorkflowTestSuite
	h := &refusalHarness{env: suite.NewTestWorkflowEnvironment(), store: newMemSnapshotStore()}
	h.env.RegisterWorkflowWithOptions(RunWorkflow, workflow.RegisterOptions{Name: RunWorkflowType})
	activities := NewSnapshotActivities(h.store)
	h.env.RegisterActivityWithOptions(activities.WriteSnapshot, activity.RegisterOptions{Name: ActivityWriteSnapshot})
	h.env.RegisterActivityWithOptions(activities.WriteFailedRunSnapshot, activity.RegisterOptions{Name: ActivityWriteFailedRunSnapshot})
	h.env.RegisterActivityWithOptions(func(_ context.Context, event *orkstackv1.Event, uri, _ string) error {
		h.mu.Lock()
		defer h.mu.Unlock()
		h.events = append(h.events, event)
		h.uris = append(h.uris, uri)
		return nil
	}, activity.RegisterOptions{Name: ActivityPublishEvent})
	h.env.SetStartWorkflowOptions(client.StartWorkflowOptions{ID: refusalWorkflowID})
	return h
}

// refusalPlan is a plan pinRunTreeLimits would build with continuation on:
// a history limit and a valid budget, so a carry reaches its version check.
func refusalPlan() RunPlan {
	budget := DefaultContinuationBudget(DefaultContinuationEvents, DefaultContinuationBytes, DefaultContinuationRestoreTimeout)
	return RunPlan{
		Namespace:    "team-a",
		History:      &HistoryLimit{MaxEvents: DefaultHistoryMaxEvents, MaxBytes: DefaultHistoryMaxBytes, Reserve: DefaultHistoryReserve},
		Continuation: &budget,
	}
}

// unknownCarryVersion is a carry format a newer release would write.
const unknownCarryVersion = ContinuationCarryVersion + 1

// refusalOrkflow is a definition a newer release would have planned.
func refusalOrkflow() *orkstackv1.Orkflow {
	return &orkstackv1.Orkflow{Name: "pipeline", Version: "v1", Namespace: "team-a", Tasks: []*orkstackv1.Task{
		{Id: "build", Image: &orkstackv1.ImageRef{Reference: "alpine"}}, {Id: "fan", Image: &orkstackv1.ImageRef{Reference: "alpine"}}, {Id: "publish", Image: &orkstackv1.ImageRef{Reference: "alpine"}},
	}}
}

// switchSnapshotRun is the exact state a switch would have written: build
// SUCCEEDED, a fan-out with item 0 SUCCEEDED (after its last stride snapshot)
// and items 1–2 PENDING, publish PENDING.
func switchSnapshotRun() *orkstackv1.Run {
	run := checkpointRunForTest()
	run.Id = refusalWorkflowID
	run.Namespace = "team-a"
	return run
}

// carryFor is a version-v carry of generation 1 whose switch snapshot is
// stored (when snapshot is non-nil) under this run's own key.
func (h *refusalHarness) carryFor(t *testing.T, version int32, snapshot *orkstackv1.Run, namespace, runID string) *orkstackv1.RunContinuation {
	t.Helper()
	carry := &orkstackv1.RunContinuation{
		Version:       version,
		Generation:    1,
		EventSequence: 41,
		CreatedAt:     timestamppb.New(time.Date(2026, 10, 1, 12, 0, 0, 0, time.UTC)),
		StartedAt:     timestamppb.New(time.Date(2026, 10, 1, 12, 0, 1, 0, time.UTC)),
	}
	if snapshot != nil {
		data, obj := gzipObjectForTest(t, snapshot, namespace, runID, 0, artifacts.ContinuationKindSnapshot)
		h.store.put(obj.GetUri(), data)
		carry.Snapshot = obj
	}
	return carry
}

func (h *refusalHarness) terminal(t *testing.T) (*orkstackv1.Event, *orkstackv1.Run) {
	t.Helper()
	h.mu.Lock()
	defer h.mu.Unlock()
	if len(h.events) != 1 {
		t.Fatalf("published %d events, want exactly the terminal one: %v", len(h.events), h.events)
	}
	data, ok := h.store.get(h.uris[0])
	if !ok {
		t.Fatalf("terminal event names snapshot %q, which was never written", h.uris[0])
	}
	run := &orkstackv1.Run{}
	if err := proto.Unmarshal(data, run); err != nil {
		t.Fatal(err)
	}
	return h.events[0], run
}

func requireRunStateError(t *testing.T, err error, code string) {
	t.Helper()
	var applicationErr *temporal.ApplicationError
	if !errors.As(err, &applicationErr) || applicationErr.Type() != code || !applicationErr.NonRetryable() {
		t.Fatalf("workflow error = %v, want a non-retryable %s", err, code)
	}
}

func TestRunWorkflow_RefusesACarryItCannotContinue(t *testing.T) {
	h := newRefusalHarness(t)
	carry := h.carryFor(t, unknownCarryVersion, switchSnapshotRun(), "team-a", refusalWorkflowID)
	h.env.ExecuteWorkflow(RunWorkflowType, "", refusalOrkflow(), (*orkstackv1.Payload)(nil), refusalPlan(), carry)
	requireRunStateError(t, h.env.GetWorkflowError(), RunStateUnsupportedCode)

	event, run := h.terminal(t)
	if event.GetType() != orkstackv1.EventType_EVENT_TYPE_RUN_FAILED || event.GetSequence() != 42 ||
		event.GetRunId() != refusalWorkflowID || event.GetStatus() != orkstackv1.Status_STATUS_FAILED {
		t.Fatalf("terminal event = %v, want RUN_FAILED at sequence 42 of %s", event, refusalWorkflowID)
	}
	if !strings.Contains(event.GetMessage(), fmt.Sprintf("saved-state format %d", unknownCarryVersion)) || !strings.Contains(event.GetMessage(), "roll the engines forward") {
		t.Fatalf("terminal message %q does not explain the refusal", event.GetMessage())
	}
	for _, word := range []string{"generation", "Temporal", "execution", "continue as new", "checkpoint", "segment"} {
		if strings.Contains(event.GetMessage(), word) {
			t.Fatalf("terminal message %q names %q, an internal word", event.GetMessage(), word)
		}
	}
	if !strings.Contains(h.uris[0], "/42.pb") {
		t.Fatalf("terminal snapshot %q is not the sequence-42 key", h.uris[0])
	}
	if run.GetError().GetCode() != RunStateUnsupportedCode ||
		run.GetError().GetCategory() != orkstackv1.FailureCategory_FAILURE_CATEGORY_CONTRACT_VIOLATION ||
		run.GetStatus() != orkstackv1.Status_STATUS_FAILED || run.GetFinishedAt() == nil {
		t.Fatalf("terminal snapshot header = %v %v %v", run.GetStatus(), run.GetError(), run.GetFinishedAt())
	}
	if !proto.Equal(run.GetCreatedAt(), carry.GetCreatedAt()) || !proto.Equal(run.GetStartedAt(), carry.GetStartedAt()) {
		t.Fatalf("terminal snapshot times = %v %v, want the carried ones", run.GetCreatedAt(), run.GetStartedAt())
	}
	rows := map[string]*orkstackv1.TaskRun{}
	for _, row := range run.GetTaskRuns() {
		rows[row.GetTaskId()] = row
	}
	if len(rows) != 3 {
		t.Fatalf("terminal snapshot holds %d task rows, want the switch snapshot's 3", len(rows))
	}
	if got := rows["build"].GetStatus(); got != orkstackv1.Status_STATUS_SUCCEEDED {
		t.Fatalf("build = %v, want SUCCEEDED kept", got)
	}
	for _, id := range []string{"fan", "publish"} {
		row := rows[id]
		if row.GetStatus() != orkstackv1.Status_STATUS_CANCELED || row.GetError().GetCode() != RunStateUnsupportedCode ||
			!strings.HasPrefix(row.GetError().GetMessage(), "the run stopped before this task finished: ") {
			t.Fatalf("%s = %v %v, want CANCELED with the run's reason", id, row.GetStatus(), row.GetError())
		}
	}
}

func TestEndUnrestorable_KeepsRowsThatWereTerminalAtTheSwitch(t *testing.T) {
	h := newRefusalHarness(t)
	h.env.ExecuteWorkflow(RunWorkflowType, "", refusalOrkflow(), (*orkstackv1.Payload)(nil), refusalPlan(),
		h.carryFor(t, unknownCarryVersion, switchSnapshotRun(), "team-a", refusalWorkflowID))
	requireRunStateError(t, h.env.GetWorkflowError(), RunStateUnsupportedCode)
	_, run := h.terminal(t)
	var items []*orkstackv1.TaskRun
	for _, row := range run.GetTaskRuns() {
		if row.GetTaskId() == "fan" {
			items = row.GetItems()
		}
	}
	if len(items) != 3 {
		t.Fatalf("fan-out has %d items, want 3", len(items))
	}
	// Item 0 succeeded after the fan-out's last stride snapshot: the switch
	// snapshot is the exact state, so it stays SUCCEEDED.
	if items[0].GetStatus() != orkstackv1.Status_STATUS_SUCCEEDED || items[0].GetError() != nil {
		t.Fatalf("item 0 = %v %v, want SUCCEEDED untouched", items[0].GetStatus(), items[0].GetError())
	}
	for _, item := range items[1:] {
		if item.GetStatus() != orkstackv1.Status_STATUS_CANCELED || item.GetFinishedAt() == nil {
			t.Fatalf("item %d = %v, want CANCELED with a finish time", item.GetItem(), item.GetStatus())
		}
	}
}

func TestEndUnrestorable_NeverReadsAPlanPointer(t *testing.T) {
	h := newRefusalHarness(t)
	// No checkpoint, no snapshot, no timestamps, a negative sequence; the
	// plan has no History, no Continuation, no Telemetry, no Origin.
	carry := &orkstackv1.RunContinuation{Version: 1, EventSequence: -7}
	h.env.ExecuteWorkflow(RunWorkflowType, "", refusalOrkflow(), (*orkstackv1.Payload)(nil), RunPlan{}, carry)
	requireRunStateError(t, h.env.GetWorkflowError(), RunStateCorruptCode)
	event, run := h.terminal(t)
	if event.GetSequence() != 1 || event.GetType() != orkstackv1.EventType_EVENT_TYPE_RUN_FAILED {
		t.Fatalf("terminal event = %v, want RUN_FAILED at sequence 1", event)
	}
	if len(run.GetTaskRuns()) != 0 || run.GetId() != refusalWorkflowID {
		t.Fatalf("terminal snapshot = %v, want the header of %s alone", run, refusalWorkflowID)
	}
}

func TestRunWorkflow_CarriedGenerationTakesItsRunIDFromTheWorkflowID(t *testing.T) {
	h := newRefusalHarness(t)
	// Even a non-empty argument is ignored once a carry is present.
	h.env.ExecuteWorkflow(RunWorkflowType, "ork-not-this-one", refusalOrkflow(), (*orkstackv1.Payload)(nil),
		refusalPlan(), h.carryFor(t, unknownCarryVersion, nil, "", ""))
	requireRunStateError(t, h.env.GetWorkflowError(), RunStateUnsupportedCode)
	event, run := h.terminal(t)
	if event.GetRunId() != refusalWorkflowID || run.GetId() != refusalWorkflowID {
		t.Fatalf("published under %q (snapshot %q), want the workflow id %q", event.GetRunId(), run.GetId(), refusalWorkflowID)
	}
	if !strings.Contains(h.uris[0], "/"+refusalWorkflowID+"/") {
		t.Fatalf("terminal snapshot %q is not under the workflow id's prefix", h.uris[0])
	}
}

func TestRunWorkflow_EmptyRunIDWithoutACarryIsStillInvalidRun(t *testing.T) {
	h := newRefusalHarness(t)
	h.env.ExecuteWorkflow(RunWorkflowType, "", refusalOrkflow(), (*orkstackv1.Payload)(nil), RunPlan{Namespace: "team-a"})
	requireRunStateError(t, h.env.GetWorkflowError(), "INVALID_RUN")
	h.mu.Lock()
	defer h.mu.Unlock()
	if len(h.events) != 0 {
		t.Fatalf("an INVALID_RUN start published %d events", len(h.events))
	}
}

func TestEndUnrestorable_IgnoresASwitchSnapshotOfAnotherRun(t *testing.T) {
	for name, carry := range map[string]func(h *refusalHarness) *orkstackv1.RunContinuation{
		"another run": func(h *refusalHarness) *orkstackv1.RunContinuation {
			return h.carryFor(t, unknownCarryVersion, switchSnapshotRun(), "team-a", "ork-1700000000-43")
		},
		"another namespace": func(h *refusalHarness) *orkstackv1.RunContinuation {
			return h.carryFor(t, unknownCarryVersion, switchSnapshotRun(), "team-b", refusalWorkflowID)
		},
	} {
		t.Run(name, func(t *testing.T) {
			h := newRefusalHarness(t)
			c := carry(h)
			h.env.ExecuteWorkflow(RunWorkflowType, "", refusalOrkflow(), (*orkstackv1.Payload)(nil), refusalPlan(), c)
			requireRunStateError(t, h.env.GetWorkflowError(), RunStateUnsupportedCode)
			_, run := h.terminal(t)
			if len(run.GetTaskRuns()) != 0 {
				t.Fatalf("terminal snapshot adopted %d rows of another run's object", len(run.GetTaskRuns()))
			}
			for _, uri := range h.store.reads {
				if uri == c.GetSnapshot().GetUri() {
					t.Fatalf("another run's object %q was read", uri)
				}
			}
		})
	}
}

func TestEndUnrestorable_IgnoresASwitchSnapshotThatFailsVerification(t *testing.T) {
	h := newRefusalHarness(t)
	carry := h.carryFor(t, unknownCarryVersion, switchSnapshotRun(), "team-a", refusalWorkflowID)
	// The object at the key was altered after it was written.
	data, _ := h.store.get(carry.GetSnapshot().GetUri())
	altered := append([]byte(nil), data...)
	altered[len(altered)-1] ^= 0xff
	h.store.put(carry.GetSnapshot().GetUri(), altered)
	h.env.ExecuteWorkflow(RunWorkflowType, "", refusalOrkflow(), (*orkstackv1.Payload)(nil), refusalPlan(), carry)
	requireRunStateError(t, h.env.GetWorkflowError(), RunStateUnsupportedCode)
	_, run := h.terminal(t)
	if len(run.GetTaskRuns()) != 0 {
		t.Fatalf("terminal snapshot adopted %d rows of an object that failed verification", len(run.GetTaskRuns()))
	}
}

// TestEndUnrestorable_CanceledRunEndsCanceled: the cancel path Stage 2's
// bootstrap takes (RUN_CANCELED) ends the run CANCELED, never as a
// RUN_STATE_* code.
func TestEndUnrestorable_CanceledRunEndsCanceled(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	store := newMemSnapshotStore()
	var published []*orkstackv1.Event
	env.RegisterActivityWithOptions(NewSnapshotActivities(store).WriteFailedRunSnapshot,
		activity.RegisterOptions{Name: ActivityWriteFailedRunSnapshot})
	env.RegisterActivityWithOptions(func(_ context.Context, event *orkstackv1.Event, _, _ string) error {
		published = append(published, event)
		return nil
	}, activity.RegisterOptions{Name: ActivityPublishEvent})
	env.RegisterWorkflowWithOptions(func(ctx workflow.Context) error {
		s := &runState{runID: "ork-c", wf: refusalOrkflow()}
		_, err := s.endUnrestorable(ctx, &orkstackv1.RunContinuation{EventSequence: 3}, "RUN_CANCELED", "run canceled")
		return err
	}, workflow.RegisterOptions{Name: "cancel-probe"})
	env.ExecuteWorkflow("cancel-probe")
	if err := env.GetWorkflowError(); !temporal.IsCanceledError(errors.Unwrap(err)) && !temporal.IsCanceledError(err) {
		t.Fatalf("workflow error = %v, want a cancellation", err)
	}
	if len(published) != 1 || published[0].GetType() != orkstackv1.EventType_EVENT_TYPE_RUN_CANCELED || published[0].GetSequence() != 4 {
		t.Fatalf("published %v, want RUN_CANCELED at sequence 4", published)
	}
}

// TestEndUnrestorable_CategoryIsTheReadersForEveryCode: a carried
// generation that ends with a RUN_STATE_* or RUN_DEFINITION_* code records
// the category the reader gives the same code from the execution's close
// (closedExecution), and the one generation 0 records for it, so the
// terminal snapshot, the archive body and the history-only read agree. A
// RUN_DEFINITION_UNAVAILABLE that ends generation 1 is InfrastructureError,
// never collapsed into ContractViolation (AGENTS.md §4).
func TestEndUnrestorable_CategoryIsTheReadersForEveryCode(t *testing.T) {
	for _, tc := range []struct {
		code string
		want orkstackv1.FailureCategory
	}{
		{RunStateUnavailableCode, orkstackv1.FailureCategory_FAILURE_CATEGORY_INFRASTRUCTURE_ERROR},
		{RunStateCorruptCode, orkstackv1.FailureCategory_FAILURE_CATEGORY_CONTRACT_VIOLATION},
		{RunStateUnsupportedCode, orkstackv1.FailureCategory_FAILURE_CATEGORY_CONTRACT_VIOLATION},
		{RunDefinitionUnavailableCode, orkstackv1.FailureCategory_FAILURE_CATEGORY_INFRASTRUCTURE_ERROR},
		{RunDefinitionCorruptCode, orkstackv1.FailureCategory_FAILURE_CATEGORY_CONTRACT_VIOLATION},
	} {
		t.Run(tc.code, func(t *testing.T) {
			var suite testsuite.WorkflowTestSuite
			env := suite.NewTestWorkflowEnvironment()
			env.RegisterActivityWithOptions(NewSnapshotActivities(newMemSnapshotStore()).WriteFailedRunSnapshot,
				activity.RegisterOptions{Name: ActivityWriteFailedRunSnapshot})
			env.RegisterActivityWithOptions(func(context.Context, *orkstackv1.Event, string, string) error { return nil },
				activity.RegisterOptions{Name: ActivityPublishEvent})
			var ended *orkstackv1.Run
			env.RegisterWorkflowWithOptions(func(ctx workflow.Context) error {
				s := &runState{runID: "ork-c", wf: refusalOrkflow()}
				run, err := s.endUnrestorable(ctx, &orkstackv1.RunContinuation{Generation: 1, EventSequence: 3},
					tc.code, "the run has stopped")
				ended = run
				return err
			}, workflow.RegisterOptions{Name: "category-probe"})
			env.ExecuteWorkflow("category-probe")
			var appErr *temporal.ApplicationError
			if err := env.GetWorkflowError(); !errors.As(err, &appErr) || appErr.Type() != tc.code {
				t.Fatalf("workflow error = %v, want an ApplicationError of type %s", err, tc.code)
			}
			if got := ended.GetError(); got.GetCode() != tc.code || got.GetCategory() != tc.want {
				t.Fatalf("the run ended %v, want %s %v", got, tc.code, tc.want)
			}
			if got := runLevelFailureCategory(tc.code); got != ended.GetError().GetCategory() {
				t.Fatalf("the reader maps %s to %v, the ended run records %v", tc.code, got, ended.GetError().GetCategory())
			}
		})
	}
}

func TestChildFailure_MapsRunStateCodes(t *testing.T) {
	for _, tc := range []struct {
		code string
		want orkstackv1.FailureCategory
	}{
		{RunStateUnavailableCode, orkstackv1.FailureCategory_FAILURE_CATEGORY_INFRASTRUCTURE_ERROR},
		{RunStateCorruptCode, orkstackv1.FailureCategory_FAILURE_CATEGORY_CONTRACT_VIOLATION},
		{RunStateUnsupportedCode, orkstackv1.FailureCategory_FAILURE_CATEGORY_CONTRACT_VIOLATION},
		{RunDefinitionUnavailableCode, orkstackv1.FailureCategory_FAILURE_CATEGORY_INFRASTRUCTURE_ERROR},
		{RunDefinitionCorruptCode, orkstackv1.FailureCategory_FAILURE_CATEGORY_CONTRACT_VIOLATION},
	} {
		status, code, category := childFailure(temporal.NewNonRetryableApplicationError("its saved state …", tc.code, nil))
		if status != orkstackv1.Status_STATUS_FAILED || code != ChildRunFailedCode || category != tc.want {
			t.Fatalf("childFailure(%s) = %v %s %v, want FAILED CHILD_RUN_FAILED %v", tc.code, status, code, category, tc.want)
		}
	}
	// A child canceled during its own start keeps today's mapping.
	if status, code, _ := childFailure(temporal.NewCanceledError()); status != orkstackv1.Status_STATUS_CANCELED || code != ChildRunCanceledCode {
		t.Fatalf("childFailure(canceled) = %v %s, want CANCELED %s", status, code, ChildRunCanceledCode)
	}
}

// TestSavedStateSize_TellsTwoSizesApart: the reason a run gives for not
// carrying on names its saved state's size and the cap in units that tell
// them apart — MiB from 1 MiB up, KiB below it, where a cap near
// continuation.bytes' 16 KiB floor would otherwise read "0.1 MiB, more than
// the 0.1 MiB".
func TestSavedStateSize_TellsTwoSizesApart(t *testing.T) {
	for _, tc := range []struct {
		bytes int64
		want  string
	}{
		{9_751_757, "9.3 MiB"},
		{8 << 20, "8.0 MiB"},
		{1 << 20, "1.0 MiB"},
		{(1 << 20) - 1, "1024.0 KiB"},
		{98_982, "96.7 KiB"},
		{96_934, "94.7 KiB"},
		{16 << 10, "16.0 KiB"},
	} {
		if got := savedStateSize(tc.bytes); got != tc.want {
			t.Errorf("savedStateSize(%d) = %q, want %q", tc.bytes, got, tc.want)
		}
	}
}
