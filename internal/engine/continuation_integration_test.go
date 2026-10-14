//go:build integration

package engine_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/api/temporalproto"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/internal/task"
	"github.com/orkstack/orkstack/internal/testsupport/proof"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// canStack is one engine replica and one Orker over a private namespace and
// private queues, with the run's snapshots and continuation objects in real
// object storage (MinIO), owned by the test: the run worker with every
// continuation local activity, an Orker whose ExecuteTask is the test's
// behavior (values committed into an in-process store a fan-out counts
// from), and the archiving projection worker.
type canStack struct {
	t          *testing.T
	ctx        context.Context
	client     client.Client
	runQueue   string
	orkerQueue string
	deployment task.Deployment
	snapshots  *artifacts.S3RunSnapshotStore
	values     *memValues
	runs       *engine.RunStore
	bus        *engine.EventBus
	archive    *engine.RunArchiver
	reader     *engine.TemporalRunReader
	starter    *engine.TemporalStarter
}

type canBehavior func(ctx context.Context, s *canStack, in *orkstackv1.ExecuteTaskInput) (*orkstackv1.ExecuteTaskResult, error)

// startCANStack builds the stack; versioned runs every worker under one
// Worker Deployment Version, as the fixture recorders do.
func startCANStack(t *testing.T, ctx context.Context, label string, versioned bool, behavior canBehavior,
	extra ...func(w worker.Worker),
) *canStack {
	t.Helper()
	cfg := s3Config()
	requireMinIO(t, cfg)
	var c client.Client
	if versioned {
		c = dialVersioningCluster(t, ctx, versioningNamespace())
	} else {
		address := temporalAddress()
		c = dialRoutingCluster(t, ctx, address, routingNamespace())
	}
	snapshots, err := artifacts.NewS3RunSnapshotStore(ctx, cfg)
	if err != nil {
		t.Fatalf("snapshot store: %v", err)
	}
	suffix := fmt.Sprintf("%d-%d", os.Getpid(), time.Now().UnixNano())
	s := &canStack{
		t: t, ctx: ctx, client: c,
		runQueue:   "ork-" + label + "-run-" + suffix,
		orkerQueue: "ork-" + label + "-tasks-" + suffix,
		snapshots:  snapshots,
		values:     &memValues{objects: map[string][]byte{}},
		runs:       engine.NewRunStore(),
		bus:        engine.NewEventBus(),
	}
	options := func(o worker.Options) worker.Options { return o }
	if versioned {
		s.deployment = task.Deployment{Name: "ork-" + label + "-" + suffix, BuildID: "v1"}
		options = s.deployment.Apply
	}
	local, err := artifacts.NewLocalRunArchive(t.TempDir())
	if err != nil {
		t.Fatalf("local run archive: %v", err)
	}
	s.archive = engine.NewRunArchiver(local)
	s.reader = engine.NewTemporalRunReader(c).WithSnapshotStore(snapshots)
	s.starter = engine.NewTemporalStarterWithOrkerQueue(c, s.runQueue, s.orkerQueue)

	runWorker := worker.New(c, s.runQueue, options(worker.Options{
		DisableRegistrationAliasing: true,
		LocalActivityWorkerOnly:     true,
	}))
	engine.RegisterWorkflows(runWorker)
	registerRunActivities(runWorker, engine.NewSnapshotActivities(snapshots), s.values)
	for _, register := range extra {
		register(runWorker)
	}
	orker := worker.New(c, s.orkerQueue, options(worker.Options{
		DisableRegistrationAliasing:        true,
		DisableWorkflowWorker:              true,
		EnableSessionWorker:                true,
		MaxConcurrentSessionExecutionSize:  4,
		MaxConcurrentActivityExecutionSize: 100,
	}))
	orker.RegisterActivityWithOptions(func(actx context.Context, in *orkstackv1.ExecuteTaskInput) (*orkstackv1.ExecuteTaskResult, error) {
		return behavior(actx, s, in)
	}, activity.RegisterOptions{Name: task.ActivityExecuteTask})
	orker.RegisterActivityWithOptions(func(context.Context, *orkstackv1.ReleasePinInput) error { return nil },
		activity.RegisterOptions{Name: task.ActivityReleasePin})
	events := worker.New(c, engine.EventQueue, worker.Options{DisableRegistrationAliasing: true, DisableWorkflowWorker: true})
	events.RegisterActivityWithOptions(
		engine.NewArchivingEventActivities(s.bus, s.runs, s.archive).WithEventSource(s.reader).WithSnapshotStore(snapshots).PublishEvent,
		activity.RegisterOptions{Name: engine.ActivityPublishEvent})
	for name, w := range map[string]worker.Worker{"run": runWorker, "orker": orker, "events": events} {
		if err := w.Start(); err != nil {
			t.Fatalf("start %s worker: %v", name, err)
		}
		t.Cleanup(w.Stop)
	}
	if versioned {
		setCurrentVersion(t, ctx, c, s.deployment, s.runQueue, s.orkerQueue)
	}
	return s
}

// registerRunActivities registers every local activity the run worker hosts,
// as cmd/ork does.
func registerRunActivities(w worker.Worker, snapshots *engine.SnapshotActivities, values *memValues) {
	w.RegisterActivityWithOptions(snapshots.WriteSnapshot, activity.RegisterOptions{Name: engine.ActivityWriteSnapshot})
	w.RegisterActivityWithOptions(snapshots.WriteFailedRunSnapshot, activity.RegisterOptions{Name: engine.ActivityWriteFailedRunSnapshot})
	w.RegisterActivityWithOptions(snapshots.WriteEventSegment, activity.RegisterOptions{Name: engine.ActivityWriteEventSegment})
	w.RegisterActivityWithOptions(snapshots.WriteSwitchSnapshot, activity.RegisterOptions{Name: engine.ActivityWriteSwitchSnapshot})
	w.RegisterActivityWithOptions(snapshots.WriteCheckpoint, activity.RegisterOptions{Name: engine.ActivityWriteCheckpoint})
	w.RegisterActivityWithOptions(snapshots.ReadContinuationChunk, activity.RegisterOptions{Name: engine.ActivityReadContinuationChunk})
	w.RegisterActivityWithOptions(engine.NewEachActivities(values).CountEachItems, activity.RegisterOptions{Name: engine.ActivityCountEachItems})
	compose := engine.NewComposeActivities(&memComposeStore{memValues: values})
	w.RegisterActivityWithOptions(compose.RenderComposedValues, activity.RegisterOptions{Name: engine.ActivityRenderComposedValues})
	w.RegisterActivityWithOptions(compose.StoreComposedValues, activity.RegisterOptions{Name: engine.ActivityStoreComposedValues})
}

// plan is the stack's routing plan with a history limit and, when budget is
// set, a continuation budget and the default in-flight cap.
func (s *canStack) plan(maxEvents int64, budget *engine.ContinuationBudget) engine.RunPlan {
	plan := rawPlan(s.orkerQueue)
	plan.CommitRoot = "mem://commits"
	plan.History = &engine.HistoryLimit{MaxEvents: maxEvents, MaxBytes: engine.DefaultHistoryMaxBytes, Reserve: engine.DefaultHistoryReserve}
	plan.Continuation = budget
	limit := engine.DefaultInFlightLimit(engine.DefaultRunMaxInFlight)
	plan.InFlight = &limit
	return plan
}

// history is one execution's whole history.
func (s *canStack) history(runID, execution string) *historypb.History {
	s.t.Helper()
	recorded := &historypb.History{}
	iter := s.client.GetWorkflowHistory(s.ctx, runID, execution, false, enums.HISTORY_EVENT_FILTER_TYPE_ALL_EVENT)
	for iter.HasNext() {
		event, err := iter.Next()
		if err != nil {
			s.t.Fatalf("read history of %s/%s: %v", runID, execution, err)
		}
		recorded.Events = append(recorded.Events, event)
	}
	return recorded
}

// waitForSwitch blocks until runID's chain has more than n executions.
func (s *canStack) waitForSwitch(runID string, n int, within time.Duration) []string {
	s.t.Helper()
	deadline := time.Now().Add(within)
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for {
		described, err := s.client.DescribeWorkflowExecution(s.ctx, runID, "")
		if err == nil {
			if executions := s.chainOf(runID, described.GetWorkflowExecutionInfo().GetExecution().GetRunId()); len(executions) > n {
				return executions
			}
		}
		if time.Now().After(deadline) {
			s.t.Fatalf("run %s did not switch past %d executions within %s (last error %v)", runID, n, within, err)
		}
		<-ticker.C
	}
}

// chainOf is the chain of executions ending at current, oldest first, read
// from each one's carry.
func (s *canStack) chainOf(runID, current string) []string {
	s.t.Helper()
	carry := carryOf(s.t, s.history(runID, current))
	var out []string
	for _, seg := range carry.GetSegments() {
		out = append(out, seg.GetExecutionId())
	}
	return append(out, current)
}

// carryOf decodes the carry an execution started from (nil for generation 0).
func carryOf(t *testing.T, h *historypb.History) *orkstackv1.RunContinuation {
	t.Helper()
	attrs := h.GetEvents()[0].GetWorkflowExecutionStartedEventAttributes()
	payloads := attrs.GetInput().GetPayloads()
	if len(payloads) < 5 {
		return nil
	}
	var carry *orkstackv1.RunContinuation
	if err := converter.GetDefaultDataConverter().FromPayload(payloads[4], &carry); err != nil {
		t.Fatalf("decode the carry: %v", err)
	}
	return carry
}

// continuedCarryOf decodes the carry an execution handed its successor, from
// its WorkflowExecutionContinuedAsNew event's fifth argument.
func continuedCarryOf(t *testing.T, h *historypb.History) *orkstackv1.RunContinuation {
	t.Helper()
	for _, event := range h.GetEvents() {
		attrs := event.GetWorkflowExecutionContinuedAsNewEventAttributes()
		if attrs == nil {
			continue
		}
		var carry *orkstackv1.RunContinuation
		if err := converter.GetDefaultDataConverter().FromPayload(attrs.GetInput().GetPayloads()[4], &carry); err != nil {
			t.Fatalf("decode the handed-on carry: %v", err)
		}
		return carry
	}
	return nil
}

// canFixture is the continued run's recorded shape (testdata/history/README.md).
func canFixtureDocument() string {
	return `
name: continued
version: "1.0.0"
tasks:
  - id: approve
    needs: []
    await: {timeout_seconds: 3600}
  - id: a
    uses: shell
    needs: []
    with: {op: exec}
  - id: b
    uses: shell
    needs: [a]
    with: {op: exec}
  - id: list
    uses: shell
    needs: []
    with: {op: exec}
  - id: convert
    uses: shell
    needs: [list]
    each: {over: "{{ .tasks.list.output.parts }}", as: part, max_items: 50, max_parallel: 2}
    with: {op: exec, name: "{{ .each.part.name }}"}
  - id: merge
    uses: shell
    needs: [convert, approve]
    with: {op: exec}
`
}

// canFixtureBehavior commits list's array for the fan-out to count, and
// answers every other task at once.
func canFixtureBehavior(_ context.Context, s *canStack, in *orkstackv1.ExecuteTaskInput) (*orkstackv1.ExecuteTaskResult, error) {
	if in.GetTaskId() == "list" {
		data := []byte(partsJSON(6))
		return &orkstackv1.ExecuteTaskResult{TaskId: "list", Attempt: 1, OrkerId: "fixture",
			ValuesJson: data, ValuesInlined: true, ValuesRef: s.values.put("mem://values/"+in.GetRunId()+"/list", data)}, nil
	}
	if in.GetEach() != nil {
		r := itemResult(in)
		r.OrkerId = "fixture"
		return r, nil
	}
	return &orkstackv1.ExecuteTaskResult{TaskId: in.GetTaskId(), Attempt: 1, OrkerId: "fixture",
		ValuesJson: valuesJSON(map[string]any{"task": in.GetTaskId()})}, nil
}

// The continued fixtures (testdata/history/README.md).
const (
	continuedG0Fixture      = "testdata/history/run-continued-g0-v1.json"
	continuedG1Fixture      = "testdata/history/run-continued-g1-v1.json"
	continuedObjectsDir     = "testdata/history/run-continued-g0-v1.objects"
	inFlightCapFixture      = "testdata/history/run-in-flight-cap-v1.json"
	continuedSegmentObject  = "segment.pb.gz"
	continuedSnapshotObject = "switch-snapshot.pb.gz"
	continuedCheckObject    = "checkpoint.pb.gz"
)

// recordedEvents is the decoded detail of one generation's history.
type recordedShape struct {
	marker       int64 // the continue-as-new version marker's event id, 0 if none
	listDone     int64 // list's completion event id
	lastDispatch int64 // the last ExecuteTask scheduled
	items        int   // fan-out items scheduled
	switched     bool
	signaled     bool
	awaiting     bool // TASK_AWAITING published
	completed    bool
}

func shapeOf(t *testing.T, h *historypb.History) recordedShape {
	t.Helper()
	var shape recordedShape
	scheduled := map[int64]string{}
	for _, event := range h.GetEvents() {
		id := event.GetEventId()
		if attrs := event.GetMarkerRecordedEventAttributes(); attrs != nil && attrs.GetMarkerName() == "Version" {
			var change string
			if payload := attrs.GetDetails()["change-id"]; payload != nil {
				_ = converter.GetDefaultDataConverter().FromPayloads(payload, &change)
			}
			if change == engine.ContinueAsNewChange {
				shape.marker = id
			}
		}
		if attrs := event.GetActivityTaskScheduledEventAttributes(); attrs != nil {
			switch attrs.GetActivityType().GetName() {
			case task.ActivityExecuteTask:
				var in *orkstackv1.ExecuteTaskInput
				if err := converter.GetDefaultDataConverter().FromPayload(attrs.GetInput().GetPayloads()[0], &in); err != nil {
					t.Fatal(err)
				}
				scheduled[id] = in.GetTaskId()
				shape.lastDispatch = id
				if in.GetEach() != nil {
					shape.items++
				}
			case engine.ActivityPublishEvent:
				published := recordedPayload(t, attrs.GetInput(), 0)
				if published["type"] == orkstackv1.EventType_EVENT_TYPE_TASK_AWAITING.String() {
					shape.awaiting = true
				}
			}
		}
		if attrs := event.GetActivityTaskCompletedEventAttributes(); attrs != nil && scheduled[attrs.GetScheduledEventId()] == "list" {
			shape.listDone = id
		}
		switch {
		case event.GetWorkflowExecutionContinuedAsNewEventAttributes() != nil:
			shape.switched = true
		case event.GetWorkflowExecutionSignaledEventAttributes() != nil:
			shape.signaled = true
		case event.GetWorkflowExecutionCompletedEventAttributes() != nil:
			shape.completed = true
		}
	}
	return shape
}

// recordContinued runs the fixture definition once under (maxEvents,
// events), resolves its await after the switch, and returns both
// generations' histories with the shape check's verdict.
func recordContinued(t *testing.T, s *canStack, maxEvents, events int64) (runID string, g0, g1 *historypb.History, carry *orkstackv1.RunContinuation, verdict error) {
	t.Helper()
	budget := engine.DefaultContinuationBudget(events, 1<<20, time.Minute)
	// Small chunks, so the next generation's bootstrap reads its saved state
	// in more than one batch and the fixture pins the yield between them.
	budget.ChunkBytes = 64
	plan := s.plan(maxEvents, &budget)
	limit := engine.DefaultInFlightLimit(16)
	plan.InFlight = &limit
	runID = fmt.Sprintf("ork-continued-%d", time.Now().UnixNano())
	if err := s.starter.StartRun(s.ctx, runID, compile(t, canFixtureDocument()), nil, plan); err != nil {
		t.Fatalf("StartRun: %v", err)
	}
	chain := s.waitForSwitchOrEnd(runID, 90*time.Second)
	if len(chain) < 2 {
		return runID, nil, nil, nil, errors.New("the run ended without switching")
	}
	if err := s.client.SignalWorkflow(s.ctx, runID, "", engine.ResolveAwaitSignal, completion("approve", `{"approved":true}`, "fixture")); err != nil {
		t.Fatalf("resolve approve: %v", err)
	}
	if err := s.client.GetWorkflow(s.ctx, runID, "").Get(s.ctx, nil); err != nil {
		return runID, nil, nil, nil, fmt.Errorf("the run failed: %w", err)
	}
	described, err := s.client.DescribeWorkflowExecution(s.ctx, runID, "")
	if err != nil {
		t.Fatal(err)
	}
	chain = s.chainOf(runID, described.GetWorkflowExecutionInfo().GetExecution().GetRunId())
	if len(chain) != 2 {
		return runID, nil, nil, nil, fmt.Errorf("%d generations, want exactly 2", len(chain))
	}
	g0, g1 = s.history(runID, chain[0]), s.history(runID, chain[1])
	carry = continuedCarryOf(t, g0)
	first, second := shapeOf(t, g0), shapeOf(t, g1)
	switch {
	case first.marker == 0 || first.listDone == 0 || first.marker < first.listDone:
		verdict = fmt.Errorf("the budget crossed at event %d, before list finished at %d", first.marker, first.listDone)
	case first.items == 0 || first.items >= 6 || first.lastDispatch < first.marker:
		verdict = fmt.Errorf("generation 0 dispatched %d items, the last at %d (marker %d): want the fan-out refilling past "+
			"the crossing and held before its end", first.items, first.lastDispatch, first.marker)
	case !first.awaiting || first.signaled || !first.switched:
		verdict = fmt.Errorf("generation 0: awaiting %v, signaled %v, switched %v", first.awaiting, first.signaled, first.switched)
	case !second.signaled || !second.completed || second.marker != 0 || second.switched:
		verdict = fmt.Errorf("generation 1: signaled %v, completed %v, marker %d, switched %v", second.signaled,
			second.completed, second.marker, second.switched)
	case first.items+second.items != 6:
		verdict = fmt.Errorf("%d + %d items dispatched, want 6", first.items, second.items)
	}
	return runID, g0, g1, carry, verdict
}

// waitForSwitchOrEnd returns runID's chain once it has switched, or once it
// has closed without switching.
func (s *canStack) waitForSwitchOrEnd(runID string, within time.Duration) []string {
	s.t.Helper()
	deadline := time.Now().Add(within)
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for {
		described, err := s.client.DescribeWorkflowExecution(s.ctx, runID, "")
		if err == nil {
			info := described.GetWorkflowExecutionInfo()
			chain := s.chainOf(runID, info.GetExecution().GetRunId())
			if len(chain) > 1 || info.GetStatus() != enums.WORKFLOW_EXECUTION_STATUS_RUNNING {
				return chain
			}
		}
		if time.Now().After(deadline) {
			s.t.Fatalf("run %s neither switched nor ended within %s (last error %v)", runID, within, err)
		}
		<-ticker.C
	}
}

// replayRecorded replays one recorded generation against current code, as
// the execution it was (its workflow id is the run id a carried generation
// reads).
func replayRecorded(h *historypb.History, runID string) error {
	replayer, err := worker.NewWorkflowReplayerWithOptions(worker.WorkflowReplayerOptions{})
	if err != nil {
		return err
	}
	replayer.RegisterWorkflowWithOptions(engine.RunWorkflow, workflow.RegisterOptions{Name: engine.RunWorkflowType})
	return replayer.ReplayWorkflowHistoryWithOptions(nil, h, worker.ReplayWorkflowHistoryOptions{
		OriginalExecution: workflow.Execution{ID: runID, RunID: "replay"},
	})
}

func encodeHistory(t *testing.T, h *historypb.History) []byte {
	t.Helper()
	encoded, err := temporalproto.CustomJSONMarshalOptions{Indent: "  "}.Marshal(h)
	if err != nil {
		t.Fatalf("encode history: %v", err)
	}
	return append(encoded, '\n')
}

// TestIntegration_RecordContinuedRunHistories records Stage 2's three replay
// fixtures from REAL runs on a real cluster, by a versioned worker, with the
// run's continuation objects in MinIO: a run that crosses its budget while
// its fan-out refills (no natural idle point), holds at its hold line,
// switches with its await open, and is resolved after the switch
// (run-continued-g0-v1.json, run-continued-g1-v1.json, and generation 0's
// three objects byte for byte); and a 24-item fan-out under a 16-task
// in-flight cap (run-in-flight-cap-v1.json). It never skips: by default the
// recordings land under ORK_PROOF_DIR and the committed fixtures are left
// alone, so every proof shows today's code still records and replays these
// paths on the cluster. Only ORK_RECORD_GOLDEN_HISTORY=1 also writes the
// fixtures, which are meant to age, not to be refreshed. It FAILS unless
// the recorded shapes land and the recordings replay.
func TestIntegration_RecordContinuedRunHistories(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()
	s := startCANStack(t, ctx, "continued-rec", true, canFixtureBehavior)
	write := os.Getenv("ORK_RECORD_GOLDEN_HISTORY") != ""

	// The values that land the shape depend on the cluster's own event
	// accounting; each candidate is tried in turn, and the values committed
	// are the ones that landed.
	type candidate struct{ maxEvents, events int64 }
	var (
		runID           string
		g0, g1          *historypb.History
		carry           *orkstackv1.RunContinuation
		landed          *candidate
		tried           []string
		candidates      = []candidate{{400, 50}, {440, 60}, {480, 70}, {360, 50}, {520, 80}, {560, 90}, {600, 100}, {320, 50}}
		maxEventsOverly = os.Getenv("ORK_CONTINUED_FIXTURE_MAX_EVENTS")
	)
	if maxEventsOverly != "" {
		var c candidate
		if _, err := fmt.Sscanf(maxEventsOverly, "%d/%d", &c.maxEvents, &c.events); err != nil {
			t.Fatalf("ORK_CONTINUED_FIXTURE_MAX_EVENTS=%q, want MAX/EVENTS", maxEventsOverly)
		}
		candidates = []candidate{c}
	}
	for i := range candidates {
		c := candidates[i]
		var verdict error
		runID, g0, g1, carry, verdict = recordContinued(t, s, c.maxEvents, c.events)
		if verdict == nil {
			landed = &c
			break
		}
		tried = append(tried, fmt.Sprintf("%d/%d: %v", c.maxEvents, c.events, verdict))
	}
	if landed == nil {
		t.Fatalf("no candidate landed the continued shape:\n%s", strings.Join(tried, "\n"))
	}
	t.Logf("the continued shape landed at history.max_events=%d continuation.events=%d (tried before: %v)",
		landed.maxEvents, landed.events, tried)

	for name, h := range map[string]*historypb.History{"g0": g0, "g1": g1} {
		if err := replayRecorded(h, runID); err != nil {
			t.Fatalf("generation %s just recorded does not replay against current code: %v", name, err)
		}
	}
	objects := map[string][]byte{}
	for name, obj := range map[string]*orkstackv1.ContinuationObject{
		continuedSegmentObject: carry.GetSegments()[0].GetObject(), continuedSnapshotObject: carry.GetSnapshot(),
		continuedCheckObject: carry.GetCheckpoint(),
	} {
		data, err := s.snapshots.ReadSnapshot(ctx, obj.GetUri())
		if err != nil {
			t.Fatalf("read %s back from MinIO: %v", name, err)
		}
		if artifacts.ContinuationSHA256(data) != obj.GetSha256() {
			t.Fatalf("%s read back does not hash to its reference", name)
		}
		objects[name] = data
	}

	// The in-flight cap: a 24-item fan-out without max_parallel, capped at 16.
	capRunID := fmt.Sprintf("ork-in-flight-cap-%d", time.Now().UnixNano())
	capPlan := s.plan(engine.DefaultHistoryMaxEvents, nil)
	capLimit := engine.DefaultInFlightLimit(16)
	capPlan.InFlight = &capLimit
	if err := s.starter.StartRun(ctx, capRunID, compile(t, inFlightCapFixtureDocument), payload(t, map[string]any{"parts": guardItems(24)}), capPlan); err != nil {
		t.Fatalf("StartRun: %v", err)
	}
	if err := s.client.GetWorkflow(ctx, capRunID, "").Get(ctx, nil); err != nil {
		t.Fatalf("the in-flight-cap run failed: %v", err)
	}
	capHistory := s.history(capRunID, "")
	capMarker := false
	for _, event := range capHistory.GetEvents() {
		if attrs := event.GetMarkerRecordedEventAttributes(); attrs != nil && attrs.GetMarkerName() == "Version" {
			var change string
			if payload := attrs.GetDetails()["change-id"]; payload != nil {
				_ = converter.GetDefaultDataConverter().FromPayloads(payload, &change)
			}
			capMarker = capMarker || change == engine.InFlightCapChange
		}
	}
	if !capMarker {
		t.Fatal("the in-flight-cap run recorded no run-in-flight-cap marker: it never held")
	}
	if err := replayRecorded(capHistory, capRunID); err != nil {
		t.Fatalf("the in-flight-cap history does not replay: %v", err)
	}

	files := map[string][]byte{
		continuedG0Fixture: encodeHistory(t, g0), continuedG1Fixture: encodeHistory(t, g1),
		inFlightCapFixture: encodeHistory(t, capHistory),
	}
	for name, data := range objects {
		files[filepath.Join(continuedObjectsDir, name)] = data
	}
	written := "the proof directory only (ORK_RECORD_GOLDEN_HISTORY unset)"
	for path, data := range files {
		proof.Write(t, "continuation", filepath.Base(path), data)
		if write {
			if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(path, data, 0o644); err != nil {
				t.Fatalf("write %s: %v", path, err)
			}
		}
	}
	if write {
		written = "testdata/history"
	}
	proof.WriteJSON(t, "continuation", "recorded-fixtures.json", map[string]any{
		"run_id": runID, "max_events": landed.maxEvents, "continuation_events": landed.events,
		"g0_events": len(g0.GetEvents()), "g1_events": len(g1.GetEvents()), "in_flight_cap_events": len(capHistory.GetEvents()),
		"checkpoint_bytes": carry.GetCheckpoint().GetSizeBytes(), "tried": tried,
	})
	t.Logf("recorded %d + %d events of run %s and %d of %s into %s (deployment %s), replayed clean",
		len(g0.GetEvents()), len(g1.GetEvents()), runID, len(capHistory.GetEvents()), capRunID, written, s.deployment)
}

const inFlightCapFixtureDocument = `
name: capped
version: "1.0.0"
inputs:
  parts: {type: array}
tasks:
  - id: spread
    uses: shell
    each: {over: "{{ .inputs.parts }}", max_items: 50}
    with: {op: exec, part: "{{ .each.item }}"}
  - id: after
    uses: shell
    needs: [spread]
    with: {op: exec}
`

var _ = json.Marshal
