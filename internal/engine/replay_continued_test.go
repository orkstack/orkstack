package engine_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	commonpb "go.temporal.io/api/common/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
	"google.golang.org/protobuf/proto"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/internal/task"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// The continued-run replay fixtures (testdata/history/README.md): a run recorded
// across a switch — generation 0, which crossed its budget while its
// fan-out refilled, held at its hold line and switched with its await open,
// and generation 1, which read its saved state back and finished — and a
// fan-out held at a 16-task in-flight cap.
const (
	continuedG0History  = "testdata/history/run-continued-g0-v1.json"
	continuedG1History  = "testdata/history/run-continued-g1-v1.json"
	continuedObjects    = "testdata/history/run-continued-g0-v1.objects"
	inFlightCapHistory  = "testdata/history/run-in-flight-cap-v1.json"
	continuedRecordHint = "record them with ORK_RECORD_GOLDEN_HISTORY=1 go test -tags=integration ./internal/engine/ " +
		"-run TestIntegration_RecordContinuedRunHistories"
)

func requireContinuedFixtures(t *testing.T) {
	t.Helper()
	for _, path := range []string{continuedG0History, continuedG1History, inFlightCapHistory,
		filepath.Join(continuedObjects, "checkpoint.pb.gz")} {
		if _, err := os.Stat(path); err != nil {
			if os.Getenv("ORK_REQUIRE_CONTINUED_FIXTURES") != "" {
				t.Fatalf("the continued replay fixture %s is missing (%v); %s", path, err, continuedRecordHint)
			}
			t.Skipf("%s is not recorded (%v): %s", path, err, continuedRecordHint)
		}
	}
}

// startArgs decodes an execution's five RunWorkflow arguments.
func startArgs(t *testing.T, recorded *historypb.History) (string, *orkstackv1.Orkflow, *orkstackv1.Payload, engine.RunPlan, *orkstackv1.RunContinuation) {
	t.Helper()
	attrs := recorded.GetEvents()[0].GetWorkflowExecutionStartedEventAttributes()
	var (
		runID string
		wf    *orkstackv1.Orkflow
		input *orkstackv1.Payload
		plan  engine.RunPlan
		carry *orkstackv1.RunContinuation
	)
	if err := converter.GetDefaultDataConverter().FromPayloads(attrs.GetInput(), &runID, &wf, &input, &plan, &carry); err != nil {
		t.Fatalf("decode the recorded start: %v", err)
	}
	return runID, wf, input, plan, carry
}

// continuedRunID is the run id: generation 0's own argument.
func continuedRunID(t *testing.T) string {
	runID, _, _, _, _ := startArgs(t, loadHistoryFile(t, continuedG0History))
	if runID == "" {
		t.Fatal("generation 0's run-id argument is empty")
	}
	return runID
}

// replayAs replays recorded as the execution of runID: a carried generation
// reads its run id from its workflow id.
func replayAs(t *testing.T, recorded *historypb.History, runID string) error {
	t.Helper()
	replayer := worker.NewWorkflowReplayer()
	replayer.RegisterWorkflowWithOptions(engine.RunWorkflow, workflow.RegisterOptions{Name: engine.RunWorkflowType})
	return replayer.ReplayWorkflowHistoryWithOptions(nil, recorded, worker.ReplayWorkflowHistoryOptions{
		OriginalExecution: workflow.Execution{ID: runID, RunID: "replay"},
	})
}

// TestRunWorkflow_ReplaysCommittedContinuedHistories is the command half: the
// replayer sets the history length from every recorded WorkflowTaskStarted,
// so today's body reproduces generation 0's budget crossing, its lazy
// continue-as-new marker, the held refill, the three writes, the memo upsert
// and the continue-as-new ending; and generation 1's chunk reads in their
// batches with their yield timers, the recreated await timer, the fan-out
// resumed at its next index, and the resolution and completion.
func TestRunWorkflow_ReplaysCommittedContinuedHistories(t *testing.T) {
	requireContinuedFixtures(t)
	runID := continuedRunID(t)
	for _, path := range []string{continuedG0History, continuedG1History} {
		if err := replayAs(t, loadHistoryFile(t, path), runID); err != nil {
			t.Fatalf("replaying %s against current code failed: %v", path, err)
		}
	}
}

// TestRunWorkflow_ReplaysCommittedInFlightCapHistory: the lazy
// run-in-flight-cap marker and every held refill replay.
func TestRunWorkflow_ReplaysCommittedInFlightCapHistory(t *testing.T) {
	requireContinuedFixtures(t)
	recorded := loadHistoryFile(t, inFlightCapHistory)
	runID, _, _, _, _ := startArgs(t, recorded)
	if err := replayAs(t, recorded, runID); err != nil {
		t.Fatalf("replaying %s against current code failed: %v", inFlightCapHistory, err)
	}
}

// versionMarkers is every Version marker's change id in a history.
func versionMarkers(t *testing.T, recorded *historypb.History) map[string]int64 {
	t.Helper()
	out := map[string]int64{}
	for _, event := range recorded.GetEvents() {
		attrs := event.GetMarkerRecordedEventAttributes()
		if attrs == nil || attrs.GetMarkerName() != "Version" {
			continue
		}
		var change string
		var version int
		_ = converter.GetDefaultDataConverter().FromPayloads(attrs.GetDetails()["change-id"], &change)
		_ = converter.GetDefaultDataConverter().FromPayloads(attrs.GetDetails()["version"], &version)
		out[change] = event.GetEventId()
		_ = version
	}
	return out
}

// publishedEvents is every event a history's PublishEvent activities carried,
// by sequence, as recorded JSON; before, when positive, keeps only those
// scheduled before that event id.
func publishedEvents(t *testing.T, recorded *historypb.History, before int64) map[int64]map[string]any {
	t.Helper()
	out := map[int64]map[string]any{}
	for _, event := range recorded.GetEvents() {
		attrs := event.GetActivityTaskScheduledEventAttributes()
		if attrs == nil || attrs.GetActivityType().GetName() != engine.ActivityPublishEvent {
			continue
		}
		if before > 0 && event.GetEventId() >= before {
			continue
		}
		published := recordedPayload(t, attrs.GetInput(), 0)
		var sequence int64
		fmt.Sscan(fmt.Sprint(published["sequence"]), &sequence)
		out[sequence] = published
	}
	return out
}

// reexecution is one re-executed generation's sent payloads.
type reexecution struct {
	mu       sync.Mutex
	inputs   map[string]map[string]any
	events   map[int64]map[string]any
	writes   int
	runError error
}

// reexecute runs RunWorkflow from args in a test environment — history
// length held at 0 — feeding back the recorded ExecuteTask results, the
// recorded resolutions and, for a carried generation, the recorded chunk
// bytes in order; every continuation write is counted.
func reexecute(t *testing.T, runID string, started time.Time, args []any, results map[string]*orkstackv1.ExecuteTaskResult,
	signals []*orkstackv1.AwaitSignal, offsets []time.Duration, chunks [][]byte, count int32,
) *reexecution {
	t.Helper()
	out := &reexecution{inputs: map[string]map[string]any{}, events: map[int64]map[string]any{}}
	var suite testsuite.WorkflowTestSuite
	env := generousDeadlock(suite.NewTestWorkflowEnvironment())
	// The recorded execution's own start, so a deadline rendered in an
	// event's message is the recorded one.
	env.SetStartTime(started)
	env.SetTestTimeout(60 * time.Second)
	env.SetStartWorkflowOptions(client.StartWorkflowOptions{ID: runID})
	env.RegisterWorkflowWithOptions(engine.RunWorkflow, workflow.RegisterOptions{Name: engine.RunWorkflowType})
	env.RegisterActivityWithOptions(func(_ context.Context, in *orkstackv1.ExecuteTaskInput) (*orkstackv1.ExecuteTaskResult, error) {
		out.mu.Lock()
		defer out.mu.Unlock()
		key := scheduleKey(in)
		out.inputs[key] = canonicalPayload(t, in)
		if result, ok := results[key]; ok {
			return result, nil
		}
		return &orkstackv1.ExecuteTaskResult{TaskId: in.GetTaskId(), ValuesJson: valuesJSON(map[string]any{})}, nil
	}, activity.RegisterOptions{Name: task.ActivityExecuteTask})
	env.RegisterActivityWithOptions(func(context.Context, *orkstackv1.ArtifactRef, []string, string, int64) (int32, error) {
		return count, nil
	}, activity.RegisterOptions{Name: engine.ActivityCountEachItems})
	env.RegisterActivityWithOptions(func(_ context.Context, run *orkstackv1.Run, sequence int64) (string, error) {
		return fmt.Sprintf("memory://snapshots/%s/%d.pb", run.GetId(), sequence), nil
	}, activity.RegisterOptions{Name: engine.ActivityWriteSnapshot})
	env.RegisterActivityWithOptions(func(_ context.Context, event *orkstackv1.Event, _, _ string) error {
		out.mu.Lock()
		defer out.mu.Unlock()
		out.events[event.GetSequence()] = canonicalPayload(t, event)
		return nil
	}, activity.RegisterOptions{Name: engine.ActivityPublishEvent})
	var next int
	env.RegisterActivityWithOptions(func(context.Context, engine.ContinuationLocator, int64, int64, int64, engine.ContinuationObservationInput) ([]byte, error) {
		out.mu.Lock()
		defer out.mu.Unlock()
		if next >= len(chunks) {
			return nil, errors.New("current code read more chunks than the recorded run")
		}
		next++
		return chunks[next-1], nil
	}, activity.RegisterOptions{Name: engine.ActivityReadContinuationChunk})
	refuse := func(name string) {
		env.RegisterActivityWithOptions(func(context.Context) error {
			out.mu.Lock()
			out.writes++
			out.mu.Unlock()
			return errors.New(name + " is not expected")
		}, activity.RegisterOptions{Name: name})
	}
	refuse(engine.ActivityWriteEventSegment)
	refuse(engine.ActivityWriteSwitchSnapshot)
	refuse(engine.ActivityWriteCheckpoint)
	env.RegisterActivityWithOptions(func(context.Context, string, string, *orkstackv1.ContinuationObject, *orkstackv1.Run, int64) (string, error) {
		return "memory://failed.pb", nil
	}, activity.RegisterOptions{Name: engine.ActivityWriteFailedRunSnapshot})
	for i, sig := range signals {
		sig := sig
		env.RegisterDelayedCallback(func() { env.SignalWorkflow(engine.ResolveAwaitSignal, sig) }, offsets[i])
	}
	env.ExecuteWorkflow(engine.RunWorkflowType, args...)
	if !env.IsWorkflowCompleted() {
		t.Fatal("the re-execution did not complete")
	}
	out.runError = env.GetWorkflowError()
	return out
}

// recordedChunks is a carried generation's chunk reads, as recorded in its
// ReadContinuationChunk markers, in order.
func recordedChunks(t *testing.T, recorded *historypb.History) [][]byte {
	t.Helper()
	var out [][]byte
	for _, payloads := range recordedMarkers(t, recorded)[engine.ActivityReadContinuationChunk] {
		var data []byte
		if err := converter.GetDefaultDataConverter().FromPayloads(payloads, &data); err != nil {
			t.Fatalf("decode a recorded chunk: %v", err)
		}
		out = append(out, data)
	}
	return out
}

func eachCount(inputs map[string]map[string]any) int32 {
	var count int32
	for _, in := range inputs {
		if each, ok := in["each"].(map[string]any); ok {
			if n, ok := each["count"].(float64); ok {
				count = int32(n)
			}
		}
	}
	return count
}

// TestContinuedHistory_PayloadsMatchWhatCurrentCodeSends is the payload half
// (the replayer never looks inside a payload):
//
//   - generation 0's prefix: re-executed with the history length held at 0,
//     so its budget never crosses, every ExecuteTask input and every event
//     published before the recorded continue-as-new marker equals the
//     recorded one;
//   - generation 0's carry, checked against the recorded history and the
//     three companion objects the recording read back from MinIO;
//   - generation 1, fully re-executed from its recorded five arguments with
//     its recorded chunks fed back: every input and every event equals the
//     recorded one, and no continuation path is taken;
//   - the in-flight cap, fully re-executed.
func TestContinuedHistory_PayloadsMatchWhatCurrentCodeSends(t *testing.T) {
	requireContinuedFixtures(t)
	g0 := loadHistoryFile(t, continuedG0History)
	g1 := loadHistoryFile(t, continuedG1History)
	runID := continuedRunID(t)
	inputs0, results0 := recordedItemPayloads(t, g0)
	inputs1, results1 := recordedItemPayloads(t, g1)
	results := map[string]*orkstackv1.ExecuteTaskResult{}
	for k, v := range results0 {
		results[k] = v
	}
	for k, v := range results1 {
		results[k] = v
	}
	signals, _ := recordedResolutions(t, g1)

	t.Run("g0 prefix", func(t *testing.T) {
		marker := versionMarkers(t, g0)[engine.ContinueAsNewChange]
		if marker == 0 {
			t.Fatal("generation 0 records no continue-as-new marker")
		}
		id, wf, input, plan, _ := startArgs(t, g0)
		got := reexecute(t, runID, startedAt(g0), []any{id, wf, input, plan, (*orkstackv1.RunContinuation)(nil)}, results,
			signals, []time.Duration{time.Minute}, nil, eachCount(inputs0))
		before := map[string]map[string]any{}
		for _, event := range g0.GetEvents() {
			attrs := event.GetActivityTaskScheduledEventAttributes()
			if attrs == nil || attrs.GetActivityType().GetName() != task.ActivityExecuteTask || event.GetEventId() >= marker {
				continue
			}
			var in *orkstackv1.ExecuteTaskInput
			if err := converter.GetDefaultDataConverter().FromPayload(attrs.GetInput().GetPayloads()[0], &in); err != nil {
				t.Fatal(err)
			}
			before[scheduleKey(in)] = inputs0[scheduleKey(in)]
		}
		if len(before) == 0 {
			t.Fatal("no task was dispatched before the marker")
		}
		for key, recorded := range before {
			if diff := payloadDiff(recorded, got.inputs[key]); diff != "" {
				t.Errorf("the ExecuteTask input for %q differs:\n%s", key, diff)
			}
		}
		// Concurrent items complete in either order, so the prefix is held
		// as a multiset (sequence and timestamps aside), within everything
		// the uninterrupted re-execution published.
		sent := eventCounts(got.events)
		for key, n := range eventCounts(publishedEvents(t, g0, marker)) {
			if sent[key] < n {
				t.Errorf("the recorded prefix published this event %d time(s), current code %d:\n  %s", n, sent[key], key)
			}
		}
	})

	t.Run("g0 carry", func(t *testing.T) {
		carry := continuedCarryFromHistory(t, g0)
		id, wf, input, plan, _ := startArgs(t, g0)
		published := publishedEvents(t, g0, 0)
		var highest int64
		for sequence := range published {
			highest = max(highest, sequence)
		}
		if carry.GetEventSequence() != highest {
			t.Fatalf("carried sequence %d, the highest published %d", carry.GetEventSequence(), highest)
		}
		started := g0.GetEvents()[0].GetEventTime().AsTime()
		if !carry.GetCreatedAt().AsTime().Equal(started) {
			t.Fatalf("carried created_at %v, the run's start %v", carry.GetCreatedAt().AsTime(), started)
		}
		markers := versionMarkers(t, g0)
		if len(carry.GetVersionDecisions()) != len(markers) {
			t.Fatalf("carried decisions %v, recorded markers %v", carry.GetVersionDecisions(), markers)
		}
		for change := range markers {
			if _, ok := carry.GetVersionDecisions()[change]; !ok {
				t.Fatalf("the carry lacks the recorded decision %s", change)
			}
		}
		read := func(name string, obj *orkstackv1.ContinuationObject) []byte {
			data, err := os.ReadFile(filepath.Join(continuedObjects, name))
			if err != nil {
				t.Fatal(err)
			}
			if int64(len(data)) != obj.GetSizeBytes() || artifacts.ContinuationSHA256(data) != obj.GetSha256() {
				t.Fatalf("%s does not match its reference", name)
			}
			return data
		}
		seg := carry.GetSegments()[0]
		segment, err := engine.DecodeEventSegment(read("segment.pb.gz", seg.GetObject()), seg.GetObject())
		if err != nil {
			t.Fatal(err)
		}
		if seg.GetFirstSequence() != 1 || seg.GetLastSequence() != highest || seg.GetEventCount() != int64(len(published)) ||
			int64(len(segment.GetEvents())) != seg.GetEventCount() {
			t.Fatalf("segment %d–%d with %d events (%d decoded), want 1–%d with %d", seg.GetFirstSequence(),
				seg.GetLastSequence(), seg.GetEventCount(), len(segment.GetEvents()), highest, len(published))
		}
		for _, event := range segment.GetEvents() {
			recorded := published[event.GetSequence()]
			if diff := payloadDiff(recorded, canonicalPayload(t, event)); diff != "" {
				t.Fatalf("segment event %d differs from the recorded publish:\n%s", event.GetSequence(), diff)
			}
		}
		checkpoint, err := engine.DecodeCheckpoint(read("checkpoint.pb.gz", carry.GetCheckpoint()), carry.GetCheckpoint())
		if err != nil {
			t.Fatal(err)
		}
		if !strings.Contains(carry.GetSnapshot().GetUri(), "/continuations/000000-snapshot-"+carry.GetSnapshot().GetSha256()+".pb.gz") {
			t.Fatalf("snapshot %q", carry.GetSnapshot().GetUri())
		}
		snapshot, err := engine.DecodeSwitchSnapshot(read("switch-snapshot.pb.gz", carry.GetSnapshot()), carry.GetSnapshot())
		if err != nil {
			t.Fatal(err)
		}
		if !proto.Equal(&orkstackv1.Run{TaskRuns: snapshot.GetTaskRuns()}, &orkstackv1.Run{TaskRuns: checkpoint.GetTaskRuns()}) {
			t.Fatal("the switch snapshot's rows differ from the checkpoint's")
		}
		for key := range results0 {
			id := strings.SplitN(key, "#", 2)[0]
			if row := taskRunOfRun(checkpoint, id); row == nil {
				t.Fatalf("the checkpoint has no row for %s", id)
			}
		}
		// Payloads 1–3 of the switch are generation 0's own start payloads,
		// and payload 0 is empty.
		attrs := continuedEventOf(t, g0)
		var (
			nextID    string
			nextWf    *orkstackv1.Orkflow
			nextInput *orkstackv1.Payload
			nextPlan  engine.RunPlan
		)
		if err := converter.GetDefaultDataConverter().FromPayloads(attrs.GetInput(), &nextID, &nextWf, &nextInput, &nextPlan); err != nil {
			t.Fatal(err)
		}
		wantPlan, _ := json.Marshal(plan)
		gotPlan, _ := json.Marshal(nextPlan)
		if nextID != "" || !proto.Equal(nextWf, wf) || !proto.Equal(nextInput, input) || string(gotPlan) != string(wantPlan) || id == "" {
			t.Fatalf("the switch's arguments are not generation 0's: run id %q", nextID)
		}
	})

	t.Run("g1", func(t *testing.T) {
		id, wf, input, plan, carry := startArgs(t, g1)
		if id != "" || carry == nil {
			t.Fatalf("generation 1 started with run id %q and carry %v", id, carry)
		}
		_, offsets := recordedResolutions(t, g1)
		got := reexecute(t, runID, startedAt(g1), []any{id, wf, input, plan, carry}, results, signals, offsets, recordedChunks(t, g1), 0)
		if got.runError != nil {
			t.Fatalf("the re-executed generation 1 failed: %v", got.runError)
		}
		if got.writes != 0 {
			t.Fatal("the re-executed generation 1 took a continuation path its recording never took")
		}
		compareAll(t, inputs1, got.inputs, publishedEvents(t, g1, 0), got.events)
	})

	t.Run("in-flight cap", func(t *testing.T) {
		recorded := loadHistoryFile(t, inFlightCapHistory)
		id, wf, input, plan, _ := startArgs(t, recorded)
		inputs, capResults := recordedItemPayloads(t, recorded)
		got := reexecute(t, id, startedAt(recorded), []any{id, wf, input, plan, (*orkstackv1.RunContinuation)(nil)}, capResults, nil, nil, nil, eachCount(inputs))
		if got.runError != nil {
			t.Fatalf("the re-executed in-flight-cap run failed: %v", got.runError)
		}
		compareAll(t, inputs, got.inputs, publishedEvents(t, recorded, 0), got.events)
	})
}

func compareAll(t *testing.T, recordedInputs, sentInputs map[string]map[string]any,
	recordedEvents, sentEvents map[int64]map[string]any,
) {
	t.Helper()
	keys := make([]string, 0, len(recordedInputs))
	for key := range recordedInputs {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		if diff := payloadDiff(recordedInputs[key], sentInputs[key]); diff != "" {
			t.Errorf("the ExecuteTask input for %q differs:\n%s", key, diff)
		}
	}
	for key := range sentInputs {
		if _, ok := recordedInputs[key]; !ok {
			t.Errorf("current code executes %q, which the recording never scheduled", key)
		}
	}
	// The sequences current code published are contiguous from the first
	// recorded one, and the events — sequence and timestamps aside, since
	// concurrent items and a resolution land in either order — are the
	// recorded ones, each as many times (compareEventPayloads' rule).
	var first int64 = -1
	for sequence := range recordedEvents {
		if first < 0 || sequence < first {
			first = sequence
		}
	}
	for i := int64(0); i < int64(len(sentEvents)); i++ {
		if _, ok := sentEvents[first+i]; !ok {
			t.Errorf("current code published no event %d: its %d events are not contiguous from %d", first+i, len(sentEvents), first)
		}
	}
	recorded, sent := eventCounts(recordedEvents), eventCounts(sentEvents)
	for key, n := range recorded {
		if sent[key] != n {
			t.Errorf("the recording published this event %d time(s), current code %d:\n  %s", n, sent[key], key)
		}
	}
	for key, n := range sent {
		if _, ok := recorded[key]; !ok {
			t.Errorf("current code publishes an event the recording never did (%d time(s)):\n  %s", n, key)
		}
	}
}

// eventCounts is events as a multiset, sequence and timestamps aside — an
// await's deadline in its message is a timestamp too, read from the clock
// the execution's first workflow task started at.
func eventCounts(events map[int64]map[string]any) map[string]int {
	list := make([]map[string]any, 0, len(events))
	for _, event := range events {
		normalized := map[string]any{}
		for key, value := range event {
			if text, ok := value.(string); ok && key == "message" {
				value = rfc3339.ReplaceAllString(text, "<time>")
			}
			normalized[key] = value
		}
		list = append(list, normalized)
	}
	return countPayloads(list, "sequence", "timestamp")
}

// startedAt is a recorded execution's start.
func startedAt(recorded *historypb.History) time.Time {
	return recorded.GetEvents()[0].GetEventTime().AsTime()
}

// continuedEventOf is a generation's WorkflowExecutionContinuedAsNew.
func continuedEventOf(t *testing.T, recorded *historypb.History) *historypb.WorkflowExecutionContinuedAsNewEventAttributes {
	t.Helper()
	for _, event := range recorded.GetEvents() {
		if attrs := event.GetWorkflowExecutionContinuedAsNewEventAttributes(); attrs != nil {
			return attrs
		}
	}
	t.Fatal("the history does not continue as new")
	return nil
}

// continuedCarryFromHistory is the carry a generation handed on: its
// continue-as-new's fifth argument.
func continuedCarryFromHistory(t *testing.T, recorded *historypb.History) *orkstackv1.RunContinuation {
	t.Helper()
	var carry *orkstackv1.RunContinuation
	if err := converter.GetDefaultDataConverter().FromPayload(continuedEventOf(t, recorded).GetInput().GetPayloads()[4], &carry); err != nil {
		t.Fatal(err)
	}
	return carry
}

// TestContinuedHistory_CatchesASwitchWithoutItsGate: every committed history
// that predates the writer, replayed against a body that switches at every
// natural idle point with the continue-as-new version check removed, must
// fail: those histories replay only because the gate sends them down the
// old path.
func TestContinuedHistory_CatchesASwitchWithoutItsGate(t *testing.T) {
	budget := engine.DefaultContinuationBudget(1, 1<<40, time.Hour)
	limit := engine.HistoryLimit{MaxEvents: 1 << 40, MaxBytes: 1 << 50, Reserve: engine.DefaultHistoryReserve}
	defer engine.ForceContinuationForTest(budget, limit)()
	caught := 0
	for _, path := range committedFixtures(t) {
		if recordedWithTheWriter(path) || isRefusedRouteHistory(path) {
			continue
		}
		recorded := loadHistoryFile(t, path)
		runID, _, _, _ := goldenWorkflowArgs(t, recorded)
		if err := replayAs(t, recorded, runID); err == nil {
			t.Errorf("%s replayed against a body that switches without its gate: it cannot see a switch", path)
			continue
		}
		caught++
	}
	if caught == 0 {
		t.Fatal("no fixture was checked")
	}
}

// TestContinuedHistory_CatchesATamperedCheckpoint: one TaskRun altered in the
// recorded chunk bytes is never adopted — RUN_STATE_CORRUPT — so the
// re-executed generation 1 diverges from its recording.
func TestContinuedHistory_CatchesATamperedCheckpoint(t *testing.T) {
	requireContinuedFixtures(t)
	g1 := loadHistoryFile(t, continuedG1History)
	id, wf, input, plan, carry := startArgs(t, g1)
	chunks := recordedChunks(t, g1)
	chunks[len(chunks)/2] = append([]byte(nil), chunks[len(chunks)/2]...)
	chunks[len(chunks)/2][0] ^= 0xff
	_, results := recordedItemPayloads(t, g1)
	signals, offsets := recordedResolutions(t, g1)
	got := reexecute(t, continuedRunID(t), startedAt(g1), []any{id, wf, input, plan, carry}, results, signals, offsets, chunks, 0)
	requireRunStateErrorExternal(t, got.runError, engine.RunStateCorruptCode)
}

func requireRunStateErrorExternal(t *testing.T, err error, code string) {
	t.Helper()
	if err == nil || !strings.Contains(err.Error(), code) {
		t.Fatalf("workflow error = %v, want %s", err, code)
	}
}

// TestContinuedHistory_CatchesAnOffByOneSequence: a carry whose sequence is
// one off — its segment shifted to agree, so it passes its own consistency
// check — publishes every event of generation 1 one sequence later, and the
// payload half sees it.
func TestContinuedHistory_CatchesAnOffByOneSequence(t *testing.T) {
	requireContinuedFixtures(t)
	g1 := loadHistoryFile(t, continuedG1History)
	id, wf, input, plan, carry := startArgs(t, g1)
	carry.EventSequence++
	carry.Segments[len(carry.Segments)-1].LastSequence++
	inputs, results := recordedItemPayloads(t, g1)
	signals, offsets := recordedResolutions(t, g1)
	got := reexecute(t, continuedRunID(t), startedAt(g1), []any{id, wf, input, plan, carry}, results, signals, offsets, recordedChunks(t, g1), 0)
	recordedEvents := publishedEvents(t, g1, 0)
	shifted := false
	for sequence := range recordedEvents {
		if _, ok := got.events[sequence]; !ok {
			shifted = true
		}
	}
	if !shifted {
		t.Fatalf("an off-by-one carry published the recorded sequences (%d inputs): the check cannot see it", len(inputs))
	}
}

// TestContinuedHistory_CatchesAChangedPinnedBatchSize: generation 1's
// history with its plan's chunks_per_task rewritten to 4 must fail to
// replay — the pinned value, not a code constant, decides the batches.
func TestContinuedHistory_CatchesAChangedPinnedBatchSize(t *testing.T) {
	requireContinuedFixtures(t)
	g1 := loadHistoryFile(t, continuedG1History)
	attrs := g1.GetEvents()[0].GetWorkflowExecutionStartedEventAttributes()
	plan := map[string]any{}
	if err := json.Unmarshal(attrs.GetInput().GetPayloads()[3].GetData(), &plan); err != nil {
		t.Fatal(err)
	}
	continuation := plan["continuation"].(map[string]any)
	if continuation["chunks_per_task"].(float64) <= 4 {
		t.Fatalf("the fixture pins %v chunks per task: too few to see a change to 4", continuation["chunks_per_task"])
	}
	if chunks := len(recordedChunks(t, g1)); chunks <= 4 {
		t.Fatalf("the fixture reads %d chunks: too few to see a change to 4", chunks)
	}
	continuation["chunks_per_task"] = 4
	data, err := json.Marshal(plan)
	if err != nil {
		t.Fatal(err)
	}
	attrs.GetInput().GetPayloads()[3] = &commonpb.Payload{Metadata: attrs.GetInput().GetPayloads()[3].GetMetadata(), Data: data}
	if err := replayAs(t, g1, continuedRunID(t)); err == nil {
		t.Fatal("generation 1 replayed with 4 chunks per task: the fixture cannot see its pinned batch size")
	}
}
