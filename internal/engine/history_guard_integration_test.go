//go:build integration

package engine_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"os"
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
	"google.golang.org/protobuf/encoding/protojson"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/internal/task"
	"github.com/orkstack/orkstack/internal/testsupport/proof"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// guardStack is one engine replica and one pin-hosting Orker over a private
// namespace and private queues, owned by the test: a versioned-or-not run
// worker, the projection worker archiving into a local run archive, and an
// Orker whose ExecuteTask is the test's behavior.
type guardStack struct {
	t          *testing.T
	ctx        context.Context
	client     client.Client
	runQueue   string
	orkerQueue string
	runs       *engine.RunStore
	snapshots  *testSnapshotStore
	archive    *engine.RunArchiver
	reader     *engine.TemporalRunReader
	starter    *engine.TemporalStarter
}

// guardBehavior answers one ExecuteTask on the stack's Orker.
type guardBehavior func(ctx context.Context, c client.Client, in *orkstackv1.ExecuteTaskInput) (*orkstackv1.ExecuteTaskResult, error)

func startGuardStack(t *testing.T, ctx context.Context, behavior guardBehavior) *guardStack {
	t.Helper()
	address := temporalAddress()
	conn, err := net.DialTimeout("tcp", address, 2*time.Second)
	if err != nil {
		infraAbsent(t, "temporal is not reachable at %s: %v", address, err)
	}
	_ = conn.Close()
	c := dialRoutingCluster(t, ctx, address, routingNamespace())

	suffix := fmt.Sprintf("%d-%d", os.Getpid(), time.Now().UnixNano())
	s := &guardStack{
		t: t, ctx: ctx, client: c,
		runQueue:   "ork-guard-run-" + suffix,
		orkerQueue: "ork-guard-tasks-" + suffix,
		runs:       engine.NewRunStore(),
		snapshots:  newTestSnapshotStore(),
	}
	local, err := artifacts.NewLocalRunArchive(t.TempDir())
	if err != nil {
		t.Fatalf("local run archive: %v", err)
	}
	s.archive = engine.NewRunArchiver(local)
	s.reader = engine.NewTemporalRunReader(c).WithSnapshotStore(s.snapshots)
	s.starter = engine.NewTemporalStarterWithOrkerQueue(c, s.runQueue, s.orkerQueue)

	runWorker := worker.New(c, s.runQueue, worker.Options{
		DisableRegistrationAliasing: true,
		LocalActivityWorkerOnly:     true,
	})
	engine.RegisterWorkflows(runWorker)
	runWorker.RegisterActivityWithOptions(engine.NewSnapshotActivities(s.snapshots).WriteSnapshot,
		activity.RegisterOptions{Name: engine.ActivityWriteSnapshot})
	compose := engine.NewComposeActivities(&memComposeStore{memValues: &memValues{objects: map[string][]byte{}}})
	runWorker.RegisterActivityWithOptions(compose.RenderComposedValues,
		activity.RegisterOptions{Name: engine.ActivityRenderComposedValues})
	runWorker.RegisterActivityWithOptions(compose.StoreComposedValues,
		activity.RegisterOptions{Name: engine.ActivityStoreComposedValues})

	orker := worker.New(c, s.orkerQueue, worker.Options{
		DisableRegistrationAliasing:        true,
		DisableWorkflowWorker:              true,
		EnableSessionWorker:                true,
		MaxConcurrentSessionExecutionSize:  4,
		MaxConcurrentActivityExecutionSize: 400,
	})
	orker.RegisterActivityWithOptions(func(actx context.Context, in *orkstackv1.ExecuteTaskInput) (*orkstackv1.ExecuteTaskResult, error) {
		return behavior(actx, c, in)
	}, activity.RegisterOptions{Name: task.ActivityExecuteTask})
	orker.RegisterActivityWithOptions(func(context.Context, *orkstackv1.ReleasePinInput) error { return nil },
		activity.RegisterOptions{Name: task.ActivityReleasePin})

	events := worker.New(c, engine.EventQueue, worker.Options{
		DisableRegistrationAliasing: true,
		DisableWorkflowWorker:       true,
	})
	events.RegisterActivityWithOptions(
		engine.NewArchivingEventActivities(engine.NewEventBus(), s.runs, s.archive).WithSnapshotStore(s.snapshots).PublishEvent,
		activity.RegisterOptions{Name: engine.ActivityPublishEvent})

	for name, w := range map[string]worker.Worker{"run": runWorker, "orker": orker, "events": events} {
		if err := w.Start(); err != nil {
			t.Fatalf("start %s worker: %v", name, err)
		}
		t.Cleanup(w.Stop)
	}
	return s
}

// plan is the stack's routing plan with the given history limit.
func (s *guardStack) limitedPlan(maxEvents, maxBytes int64) engine.RunPlan {
	plan := rawPlan(s.orkerQueue)
	plan.History = &engine.HistoryLimit{MaxEvents: maxEvents, MaxBytes: maxBytes, Reserve: engine.DefaultHistoryReserve}
	return plan
}

// run starts one run and waits for it to close, returning its workflow error.
func (s *guardStack) startAndWait(runID, document string, input *orkstackv1.Payload, plan engine.RunPlan) error {
	s.t.Helper()
	return s.startDefinitionAndWait(runID, compile(s.t, document), input, plan)
}

func (s *guardStack) startDefinitionAndWait(runID string, wf *orkstackv1.Orkflow, input *orkstackv1.Payload, plan engine.RunPlan) error {
	s.t.Helper()
	if err := s.starter.StartRun(s.ctx, runID, wf, input, plan); err != nil {
		s.t.Fatalf("StartRun %s: %v", runID, err)
	}
	return s.client.GetWorkflow(s.ctx, runID, "").Get(s.ctx, nil)
}

// closed is what the cluster says about a closed run: its status, its
// history's length and size, and — for a run the guard stopped — the
// failure's type and the guard's own accounting at the crossing.
type closedRun struct {
	Status      string                       `json:"temporal_status"`
	Length      int64                        `json:"history_length"`
	SizeBytes   int64                        `json:"history_size_bytes"`
	FailureType string                       `json:"failure_type,omitempty"`
	Blockers    string                       `json:"blockers,omitempty"`
	Crossing    engine.HistoryCrossingDetail `json:"crossing"`
	MarkerEvent int64                        `json:"guard_marker_event_id,omitempty"`
}

func (s *guardStack) describeClosed(runID string) closedRun {
	s.t.Helper()
	described, err := s.client.DescribeWorkflowExecution(s.ctx, runID, "")
	if err != nil {
		s.t.Fatalf("describe %s: %v", runID, err)
	}
	info := described.GetWorkflowExecutionInfo()
	out := closedRun{
		Status:    info.GetStatus().String(),
		Length:    info.GetHistoryLength(),
		SizeBytes: info.GetHistorySizeBytes(),
	}
	for _, event := range s.fullHistory(runID).GetEvents() {
		if attrs := event.GetMarkerRecordedEventAttributes(); attrs != nil && attrs.GetMarkerName() == "Version" {
			var change string
			if payload := attrs.GetDetails()["change-id"]; payload != nil {
				_ = converter.GetDefaultDataConverter().FromPayloads(payload, &change)
			}
			if change == engine.HistoryGuardChange {
				out.MarkerEvent = event.GetEventId()
			}
		}
		attrs := event.GetWorkflowExecutionFailedEventAttributes()
		if attrs == nil {
			continue
		}
		app := attrs.GetFailure().GetApplicationFailureInfo()
		out.FailureType = app.GetType()
		if out.FailureType == engine.RunHistoryLimitCode {
			if err := converter.GetDefaultDataConverter().FromPayloads(app.GetDetails(), &out.Blockers, &out.Crossing); err != nil {
				s.t.Fatalf("decode the guard's failure details: %v", err)
			}
		}
	}
	return out
}

func (s *guardStack) fullHistory(runID string) *historypb.History {
	s.t.Helper()
	recorded := &historypb.History{}
	iter := s.client.GetWorkflowHistory(s.ctx, runID, "", false, enums.HISTORY_EVENT_FILTER_TYPE_ALL_EVENT)
	for iter.HasNext() {
		event, err := iter.Next()
		if err != nil {
			s.t.Fatalf("read history of %s: %v", runID, err)
		}
		recorded.Events = append(recorded.Events, event)
	}
	return recorded
}

// requireStoppedByTheGuard asserts the run the guard stopped: Temporal Failed
// with RUN_HISTORY_LIMIT, never terminated; its history below both limits;
// and what its failure path appended within the reserve the guard kept back.
func (s *guardStack) requireStoppedByTheGuard(runID string, limit *engine.HistoryLimit) closedRun {
	s.t.Helper()
	c := s.describeClosed(runID)
	if c.Status != enums.WORKFLOW_EXECUTION_STATUS_FAILED.String() || c.FailureType != engine.RunHistoryLimitCode {
		s.t.Fatalf("run %s closed %s with failure type %q, want Failed with %s", runID, c.Status, c.FailureType,
			engine.RunHistoryLimitCode)
	}
	if c.Length >= limit.MaxEvents || c.SizeBytes >= limit.MaxBytes {
		s.t.Fatalf("run %s's history is %d events and %d bytes, not below its %d-event, %d-byte limit",
			runID, c.Length, c.SizeBytes, limit.MaxEvents, limit.MaxBytes)
	}
	if grown := c.Length - c.Crossing.Events; grown > c.Crossing.ReserveEvents {
		s.t.Fatalf("run %s's failure path appended %d events, over the %d the guard kept back", runID, grown,
			c.Crossing.ReserveEvents)
	}
	if grown := c.SizeBytes - c.Crossing.Bytes; grown > c.Crossing.ReserveBytes {
		s.t.Fatalf("run %s's failure path appended %d bytes, over the %d the guard kept back", runID, grown,
			c.Crossing.ReserveBytes)
	}
	if run, ok := s.runs.Get(runID); !ok || run.GetError().GetCode() != engine.RunHistoryLimitCode {
		s.t.Fatalf("run %s's published error = %v, want %s", runID, run.GetError(), engine.RunHistoryLimitCode)
	}
	return c
}

// blockUntilCanceled is a container that runs until its run stops it.
func blockUntilCanceled(ctx context.Context) (*orkstackv1.ExecuteTaskResult, error) {
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-ticker.C:
			activity.RecordHeartbeat(ctx, "running")
		}
	}
}

// guardBehaviorByPrefix blocks a task (or an item) whose id starts with
// "block", "spread" or "hold", and answers every other at once.
func guardBehaviorByPrefix(ctx context.Context, c client.Client, in *orkstackv1.ExecuteTaskInput) (*orkstackv1.ExecuteTaskResult, error) {
	id := in.GetTaskId()
	switch {
	case id == "hold":
		// The await resolved at the crossing: its resolution arrives exactly
		// while the run is stopping, as this blocker is stopped.
		_, err := blockUntilCanceled(ctx)
		signalCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = c.SignalWorkflow(signalCtx, in.GetRunId(), "", engine.ResolveAwaitSignal,
			&orkstackv1.AwaitSignal{TaskId: "approve", Reject: true, Reason: "too late"})
		return nil, err
	case strings.HasPrefix(id, "block"), strings.HasPrefix(id, "spread"), strings.HasPrefix(id, "p1"):
		return blockUntilCanceled(ctx)
	}
	return succeeded(id, map[string]any{"value": "v-" + id}), nil
}

// chain is n plain tasks, each needing the one before.
func guardChain(prefix string, n int, first string) string {
	var b strings.Builder
	for i := 0; i < n; i++ {
		needs := first
		if i > 0 {
			needs = fmt.Sprintf("%s%02d", prefix, i-1)
		}
		fmt.Fprintf(&b, "  - id: %s%02d\n    uses: shell\n    needs: [%s]\n    with: {op: exec}\n", prefix, i, needs)
	}
	return b.String()
}

// TestIntegration_RunHistoryLimitFailsExplicitly: a long chain under a
// 600-event limit stops RUN_HISTORY_LIMIT on a real cluster: GetRun says
// FAILED with the code, the archive body ends RUN_FAILED, and Temporal records
// the execution Failed — not Terminated.
func TestIntegration_RunHistoryLimitFailsExplicitly(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	s := startGuardStack(t, ctx, guardBehaviorByPrefix)
	runID := fmt.Sprintf("ork-history-limit-%d", time.Now().UnixNano())
	plan := s.limitedPlan(600, engine.DefaultHistoryMaxBytes)
	err := s.startAndWait(runID, "name: long-chain\ntasks:\n"+guardChain("c", 60, ""), nil, plan)
	if err == nil {
		t.Fatal("a 60-task chain finished under a 600-event history limit")
	}
	c := s.requireStoppedByTheGuard(runID, plan.History)

	run, err := s.reader.GetRun(ctx, runID)
	if err != nil {
		t.Fatalf("GetRun: %v", err)
	}
	if run.GetStatus() != orkstackv1.Status_STATUS_FAILED || run.GetError().GetCode() != engine.RunHistoryLimitCode {
		t.Fatalf("GetRun = %v / %v, want FAILED %s", run.GetStatus(), run.GetError(), engine.RunHistoryLimitCode)
	}
	body, err := s.archive.LoadRunBody(ctx, runID)
	if err != nil {
		t.Fatalf("archive body: %v", err)
	}
	archived, archivedEvents, err := engine.UnmarshalRunArchive(body)
	if err != nil {
		t.Fatalf("decode archive: %v", err)
	}
	last := archivedEvents[len(archivedEvents)-1]
	if last.GetType() != orkstackv1.EventType_EVENT_TYPE_RUN_FAILED || archived.GetError().GetCode() != engine.RunHistoryLimitCode {
		t.Fatalf("archive ends %v with error %v, want RUN_FAILED with %s", last.GetType(), archived.GetError(),
			engine.RunHistoryLimitCode)
	}
	if c.Status == enums.WORKFLOW_EXECUTION_STATUS_TERMINATED.String() {
		t.Fatal("the cluster terminated the run")
	}
	encoded, err := protojson.Marshal(run)
	if err != nil {
		t.Fatal(err)
	}
	proof.WriteJSON(t, "history-limit", "run.json", map[string]any{
		"run_id":          runID,
		"run":             json.RawMessage(encoded),
		"temporal_status": "Failed",
		"closed":          c,
		"archive_last":    last.GetType().String(),
	})
	t.Logf("E0 history limit: run %s FAILED %s; Temporal %s; %d events (limit %d); crossing at %d events with %d kept back",
		runID, run.GetError().GetCode(), c.Status, c.Length, plan.History.MaxEvents, c.Crossing.Events, c.Crossing.ReserveEvents)
}

// guardShape is one shape of TestIntegration_HistoryGuardHoldsAcrossShapes.
type guardShape struct {
	name      string
	document  string
	input     *orkstackv1.Payload
	maxEvents int64
	maxBytes  int64
	children  func(t *testing.T, plan *engine.RunPlan)
	// guarded is the run the guard must stop: the run itself, or its child.
	guarded func(plan engine.RunPlan, runID string) string
	// landed fails the test when the shape did not land as designed.
	landed func(t *testing.T, s *guardStack, runID string, c closedRun)
}

const guardShapeChildDocument = `
name: shape-child
version: "1.0.0"
tasks:
  - id: work
    uses: shell
    with: {op: exec}
`

const shapeChildDigest = "sha256:" + "5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a"

// composeChild plans one child run for task taskID of definition document.
func composeChild(t *testing.T, s *guardStack, plan *engine.RunPlan, taskID, childDocument string) {
	t.Helper()
	child := compile(t, childDocument)
	child.DefinitionDigest = shapeChildDigest
	childPlan := rawPlan(s.orkerQueue)
	cp, err := engine.NewChildRunPlan(child, childPlan, false, false)
	if err != nil {
		t.Fatal(err)
	}
	cp.RunID = fmt.Sprintf("ork-%d-%s", time.Now().UnixNano(), taskID)
	if plan.Children == nil {
		plan.Children = map[string]*engine.ChildRunPlan{}
	}
	plan.Children[taskID] = cp
}

func pinDigest(wf *orkstackv1.Orkflow) {
	for _, tk := range wf.GetTasks() {
		if tk.GetOrkflow() != nil {
			tk.Orkflow.DefinitionDigest = shapeChildDigest
		}
	}
}

// TestIntegration_HistoryGuardHoldsAcrossShapes is the evidence for every
// reserve constant: for each shape the run (or, composed, its child) stops
// RUN_HISTORY_LIMIT, its final history is below both limits, and what its
// failure path appended after the crossing is within the reserve the guard
// kept back — in events and in bytes. shapes.json records predicted and
// measured per shape.
func TestIntegration_HistoryGuardHoldsAcrossShapes(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	defer cancel()
	s := startGuardStack(t, ctx, guardBehaviorByPrefix)
	bigInput := payload(t, map[string]any{"blob": strings.Repeat("x", 128<<10)})

	var blockers64 strings.Builder
	for i := 0; i < 64; i++ {
		fmt.Fprintf(&blockers64, "  - id: block%02d\n    uses: shell\n    needs: []\n    with: {op: exec}\n", i)
	}
	var flat24 strings.Builder
	for i := 0; i < 24; i++ {
		fmt.Fprintf(&flat24, "  - id: block%02d\n    uses: shell\n    needs: []\n    with: {op: exec, blob: \"{{ .inputs.blob }}\"}\n", i)
	}
	shapes := []guardShape{
		{
			name:      "64 tasks in flight",
			document:  "name: shape-in-flight\ntasks:\n" + blockers64.String() + guardChain("q", 60, ""),
			maxEvents: 3000, maxBytes: engine.DefaultHistoryMaxBytes,
			landed: func(t *testing.T, _ *guardStack, _ string, c closedRun) {
				if c.Crossing.InFlight != 64 {
					t.Fatalf("the guard crossed with %d in flight, want all 64 blockers", c.Crossing.InFlight)
				}
			},
		},
		{
			name: "an open 3-member pin",
			document: "name: shape-pin\npins:\n  build: {}\ntasks:\n" +
				"  - id: p1\n    uses: shell\n    pin: build\n    run_as_orker_user: true\n    needs: []\n    with: {op: exec}\n" +
				"  - id: p2\n    uses: shell\n    pin: build\n    run_as_orker_user: true\n    needs: [p1]\n    with: {op: exec}\n" +
				"  - id: p3\n    uses: shell\n    pin: build\n    run_as_orker_user: true\n    needs: [p2]\n    with: {op: exec}\n" +
				guardChain("q", 60, ""),
			maxEvents: 1000, maxBytes: engine.DefaultHistoryMaxBytes,
			landed: func(t *testing.T, _ *guardStack, _ string, c closedRun) {
				if !strings.Contains(c.Blockers, "pins open: build") {
					t.Fatalf("the guard crossed with %q, want pin build open", c.Blockers)
				}
			},
		},
		{
			name: "a 200-item fan-out without max_parallel",
			document: "name: shape-fan\ninputs:\n  parts: {type: array}\ntasks:\n" +
				"  - id: spread\n    uses: shell\n    each: {over: \"{{ .inputs.parts }}\", max_items: 200}\n    with: {op: exec, part: \"{{ .each.item }}\"}\n",
			input:     payload(t, map[string]any{"parts": guardItems(200)}),
			maxEvents: 2000, maxBytes: engine.DefaultHistoryMaxBytes,
			landed: func(t *testing.T, s *guardStack, runID string, c closedRun) {
				run, _ := s.runs.Get(runID)
				node := taskRunOfRun(run, "spread")
				dispatched := 0
				for _, item := range node.GetItems() {
					if item.GetScheduledAt() != nil {
						dispatched++
					}
				}
				if dispatched == 0 || dispatched >= 200 || !strings.Contains(c.Blockers, "spread[") {
					t.Fatalf("%d of 200 items dispatched, blockers %q: the crossing was not inside the dispatch", dispatched, c.Blockers)
				}
			},
		},
		{
			name: "three failure handlers: plain, composed and an await",
			document: "name: shape-handlers\ntasks:\n" + guardChain("q", 60, "") +
				"  - id: hp\n    uses: shell\n    needs: [q59]\n    join: all_done\n    with: {op: exec}\n" +
				"  - id: hc\n    uses: orkflow:shape-child@1.0.0\n    needs: [q59]\n    join: all_done\n" +
				"  - id: ha\n    await: {timeout_seconds: 2}\n    needs: [q59]\n    join: all_done\n",
			maxEvents: 1500, maxBytes: engine.DefaultHistoryMaxBytes,
			children: func(t *testing.T, plan *engine.RunPlan) { composeChild(t, s, plan, "hc", guardShapeChildDocument) },
			landed: func(t *testing.T, s *guardStack, runID string, _ closedRun) {
				run, _ := s.runs.Get(runID)
				for _, id := range []string{"hp", "hc", "ha"} {
					tr := taskRunOfRun(run, id)
					if tr.GetScheduledAt() == nil {
						t.Fatalf("handler %s never started: %v / %v", id, tr.GetStatus(), tr.GetError())
					}
				}
			},
		},
		{
			name: "an await resolved at the crossing",
			document: "name: shape-await\ntasks:\n" +
				"  - id: approve\n    await: {timeout_seconds: 3600}\n" +
				"  - id: hold\n    uses: shell\n    needs: []\n    with: {op: exec}\n" + guardChain("q", 60, ""),
			maxEvents: 1000, maxBytes: engine.DefaultHistoryMaxBytes,
			landed: func(t *testing.T, s *guardStack, runID string, c closedRun) {
				if !strings.Contains(c.Blockers, "awaiting: approve") {
					t.Fatalf("the guard crossed with %q, want the await open", c.Blockers)
				}
				run, _ := s.runs.Get(runID)
				if got := taskRunOfRun(run, "approve").GetStatus(); got != orkstackv1.Status_STATUS_CANCELED {
					t.Fatalf("approve = %v, want CANCELED", got)
				}
			},
		},
		{
			name: "a composed task whose child reaches its own guard",
			document: "name: shape-parent\ntasks:\n" +
				"  - id: build\n    uses: shell\n    with: {op: exec}\n" +
				"  - id: release\n    uses: orkflow:shape-child@1.0.0\n    needs: [build]\n",
			maxEvents: 600, maxBytes: engine.DefaultHistoryMaxBytes,
			children: func(t *testing.T, plan *engine.RunPlan) {
				composeChild(t, s, plan, "release", "name: shape-child\nversion: \"1.0.0\"\ntasks:\n"+guardChain("c", 40, ""))
			},
			guarded: func(plan engine.RunPlan, _ string) string { return plan.Children["release"].RunID },
			landed: func(t *testing.T, s *guardStack, runID string, _ closedRun) {
				parent, _ := s.runs.Get(runID)
				release := taskRunOfRun(parent, "release")
				if release.GetError().GetCode() != engine.ChildRunFailedCode ||
					release.GetError().GetCategory() != orkstackv1.FailureCategory_FAILURE_CATEGORY_INFRASTRUCTURE_ERROR {
					t.Fatalf("release = %v, want CHILD_RUN_FAILED InfrastructureError", release.GetError())
				}
			},
		},
		{
			name:      "bytes: a 24-task flat stage with a 128 KiB input",
			document:  "name: shape-bytes\ninputs:\n  blob: {type: string}\ntasks:\n" + flat24.String(),
			input:     bigInput,
			maxEvents: engine.DefaultHistoryMaxEvents, maxBytes: 2 << 20,
			landed: func(t *testing.T, _ *guardStack, _ string, c closedRun) {
				if c.Crossing.Events+c.Crossing.ReserveEvents >= c.Crossing.MaxEvents ||
					c.Crossing.Bytes+c.Crossing.ReserveBytes < c.Crossing.MaxBytes {
					t.Fatalf("the crossing %+v was not the bytes dimension's", c.Crossing)
				}
			},
		},
	}

	var recorded []map[string]any
	for i, shape := range shapes {
		t.Run(shape.name, func(t *testing.T) {
			s.t = t
			runID := fmt.Sprintf("ork-shape-%d-%d", i, time.Now().UnixNano())
			wf := compile(t, shape.document)
			pinDigest(wf)
			plan := s.limitedPlan(shape.maxEvents, shape.maxBytes)
			if shape.children != nil {
				shape.children(t, &plan)
			}
			input := shape.input
			if input == nil {
				input = &orkstackv1.Payload{}
			}
			if err := s.startDefinitionAndWait(runID, wf, input, plan); err == nil {
				t.Fatalf("shape %q finished; it was meant to reach its history limit", shape.name)
			}
			guarded := runID
			if shape.guarded != nil {
				guarded = shape.guarded(plan, runID)
			}
			c := s.requireStoppedByTheGuard(guarded, plan.History)
			shape.landed(t, s, runID, c)
			recorded = append(recorded, map[string]any{
				"shape":            shape.name,
				"run_id":           guarded,
				"max_events":       shape.maxEvents,
				"max_bytes":        shape.maxBytes,
				"final_length":     c.Length,
				"final_size_bytes": c.SizeBytes,
				"crossing":         c.Crossing,
				// The guard's promise: the final history stays under these.
				"promised_max_events": c.Crossing.Events + c.Crossing.ReserveEvents,
				"promised_max_bytes":  c.Crossing.Bytes + c.Crossing.ReserveBytes,
				// From the start of the workflow task that crossed: what that
				// task had issued plus the reserve, against what was appended.
				"predicted_events":      c.Crossing.Events - c.Crossing.RawEvents + c.Crossing.ReserveEvents,
				"measured_events":       c.Length - c.Crossing.RawEvents,
				"predicted_bytes":       c.Crossing.Bytes - c.Crossing.RawBytes + c.Crossing.ReserveBytes,
				"measured_bytes":        c.SizeBytes - c.Crossing.RawBytes,
				"blockers":              c.Blockers,
				"temporal_status":       c.Status,
				"guard_marker_event_id": c.MarkerEvent,
			})
			t.Logf("E0 shape %q: final %d events (< %d), %d bytes (< %d); after the crossing task's start "+
				"predicted %d events / %d bytes, measured %d / %d",
				shape.name, c.Length, shape.maxEvents, c.SizeBytes, shape.maxBytes,
				c.Crossing.Events-c.Crossing.RawEvents+c.Crossing.ReserveEvents,
				c.Crossing.Bytes-c.Crossing.RawBytes+c.Crossing.ReserveBytes,
				c.Length-c.Crossing.RawEvents, c.SizeBytes-c.Crossing.RawBytes)
		})
	}
	proof.WriteJSON(t, "history-guard", "shapes.json", recorded)
}

// historySizeProbe is a workflow of its own, used only by
// TestIntegration_ServerReportsHistorySize: three activities, then what the
// SDK says the history's size is.
func historySizeProbe(ctx workflow.Context) (int64, error) {
	actx := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{StartToCloseTimeout: 10 * time.Second})
	for i := 0; i < 3; i++ {
		if err := workflow.ExecuteActivity(actx, "HistorySizeProbeStep", i).Get(ctx, nil); err != nil {
			return 0, err
		}
	}
	return int64(workflow.GetInfo(ctx).GetCurrentHistorySize()), nil
}

// TestIntegration_ServerReportsHistorySize: the bytes half of the guard
// depends on the server filling WorkflowTaskStarted's history size. It FAILS,
// never skips, when the value is 0.
func TestIntegration_ServerReportsHistorySize(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	address := temporalAddress()
	conn, err := net.DialTimeout("tcp", address, 2*time.Second)
	if err != nil {
		infraAbsent(t, "temporal is not reachable at %s: %v", address, err)
	}
	_ = conn.Close()
	c := dialRoutingCluster(t, ctx, address, routingNamespace())
	queue := fmt.Sprintf("ork-history-size-%d-%d", os.Getpid(), time.Now().UnixNano())
	w := worker.New(c, queue, worker.Options{DisableRegistrationAliasing: true})
	w.RegisterWorkflowWithOptions(historySizeProbe, workflow.RegisterOptions{Name: "HistorySizeProbe"})
	w.RegisterActivityWithOptions(func(context.Context, int) error { return nil },
		activity.RegisterOptions{Name: "HistorySizeProbeStep"})
	if err := w.Start(); err != nil {
		t.Fatal(err)
	}
	defer w.Stop()
	run, err := c.ExecuteWorkflow(ctx, client.StartWorkflowOptions{TaskQueue: queue}, "HistorySizeProbe")
	if err != nil {
		t.Fatal(err)
	}
	var size int64
	if err := run.Get(ctx, &size); err != nil {
		t.Fatal(err)
	}
	if size <= 0 {
		t.Fatalf("the server reported a history size of %d after three activities: the guard's bytes "+
			"dimension cannot see history growth on this cluster", size)
	}
	t.Logf("E0 history size: the server reports %d bytes after three activities", size)
}

// TestIntegration_LargeRunResultIsCompact: a 200-task run — whose whole-run
// result used to be about 305 KB — completes, and its WorkflowExecutionCompleted
// result is under 2 KiB.
func TestIntegration_LargeRunResultIsCompact(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	s := startGuardStack(t, ctx, guardBehaviorByPrefix)
	var doc strings.Builder
	doc.WriteString("name: wide\ntasks:\n")
	for i := 0; i < 200; i++ {
		fmt.Fprintf(&doc, "  - id: t%03d\n    uses: shell\n    needs: []\n    with: {op: exec}\n", i)
	}
	runID := fmt.Sprintf("ork-compact-%d", time.Now().UnixNano())
	if err := s.startAndWait(runID, doc.String(), nil, s.limitedPlan(engine.DefaultHistoryMaxEvents, engine.DefaultHistoryMaxBytes)); err != nil {
		t.Fatalf("the 200-task run failed: %v", err)
	}
	var resultBytes int
	for _, event := range s.fullHistory(runID).GetEvents() {
		if attrs := event.GetWorkflowExecutionCompletedEventAttributes(); attrs != nil {
			for _, p := range attrs.GetResult().GetPayloads() {
				resultBytes += len(p.GetData())
			}
		}
	}
	if resultBytes == 0 || resultBytes >= 2048 {
		t.Fatalf("completed_result_bytes = %d, want a result under 2 KiB", resultBytes)
	}
	run, _ := s.runs.Get(runID)
	if len(run.GetTaskRuns()) != 200 {
		t.Fatalf("the published run carries %d task rows, want 200", len(run.GetTaskRuns()))
	}
	proof.WriteJSON(t, "history-limit", "large-run-result.json", map[string]any{
		"run_id": runID, "tasks": 200, "completed_result_bytes": resultBytes,
	})
	t.Logf("E0 compact result: completed_result_bytes=%d for 200 tasks", resultBytes)
}

// historyLimitFixture is the guard's replay fixture. Only
// TestIntegration_RecordHistoryLimitRunHistory writes it.
const historyLimitFixture = "testdata/history/run-history-limit-v1.json"

// historyLimitFixtureDocument is the recorded shape: a 30-item fan-out
// followed by a 40-task chain, under a 600-event limit. The reserve the
// pending chain keeps back (8 events each) brings the line forward, so the
// guard crosses inside the fan-out's first dispatch batch — the exact,
// per-command count is what decides where.
func historyLimitFixtureDocument() string {
	return "name: history-limit\nversion: \"1.0.0\"\ninputs:\n  parts: {type: array}\ntasks:\n" +
		"  - id: spread\n    uses: shell\n    each: {over: \"{{ .inputs.parts }}\", max_items: 30}\n" +
		"    with: {op: exec, part: \"{{ .each.item }}\"}\n" +
		guardChain("c", 40, "spread")
}

// TestIntegration_RecordHistoryLimitRunHistory records run-history-limit-v1.json
// from a REAL run on a real cluster, by a versioned worker, as the golden
// recorder does. Opt-in (ORK_RECORD_GOLDEN_HISTORY=1); the fixture is meant to
// age, not to be refreshed. The recording FAILS, never skips, unless the run
// ended RUN_HISTORY_LIMIT and Temporal recorded it Failed below its limit.
func TestIntegration_RecordHistoryLimitRunHistory(t *testing.T) {
	if os.Getenv("ORK_RECORD_GOLDEN_HISTORY") == "" {
		t.Skip("set ORK_RECORD_GOLDEN_HISTORY=1 to re-record " + historyLimitFixture +
			" — the committed fixture is meant to age, not to be refreshed")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	namespace := versioningNamespace()
	c := dialVersioningCluster(t, ctx, namespace)

	suffix := fmt.Sprintf("%d-%d", os.Getpid(), time.Now().UnixNano())
	runQueue := "ork-limit-run-" + suffix
	orkerQueue := "ork-limit-tasks-" + suffix
	deployment := task.Deployment{Name: "ork-limit-" + suffix, BuildID: "v1"}

	runWorker := worker.New(c, runQueue, deployment.Apply(worker.Options{
		DisableRegistrationAliasing: true,
		LocalActivityWorkerOnly:     true,
	}))
	engine.RegisterWorkflows(runWorker)
	snapshots := newTestSnapshotStore()
	runWorker.RegisterActivityWithOptions(engine.NewSnapshotActivities(snapshots).WriteSnapshot,
		activity.RegisterOptions{Name: engine.ActivityWriteSnapshot})
	orkerWorker := worker.New(c, orkerQueue, deployment.Apply(worker.Options{
		DisableRegistrationAliasing: true,
		DisableWorkflowWorker:       true,
	}))
	orkerWorker.RegisterActivityWithOptions(func(actx context.Context, in *orkstackv1.ExecuteTaskInput) (*orkstackv1.ExecuteTaskResult, error) {
		return guardBehaviorByPrefix(actx, c, in)
	}, activity.RegisterOptions{Name: task.ActivityExecuteTask})
	runs := engine.NewRunStore()
	eventWorker := worker.New(c, engine.EventQueue, worker.Options{
		DisableRegistrationAliasing: true,
		DisableWorkflowWorker:       true,
	})
	eventWorker.RegisterActivityWithOptions(
		engine.NewEventActivities(engine.NewEventBus(), runs).WithSnapshotStore(snapshots).PublishEvent,
		activity.RegisterOptions{Name: engine.ActivityPublishEvent})
	for name, w := range map[string]worker.Worker{"run": runWorker, "orker": orkerWorker, "event": eventWorker} {
		if err := w.Start(); err != nil {
			t.Fatalf("start %s worker: %v", name, err)
		}
		defer w.Stop()
	}
	setCurrentVersion(t, ctx, c, deployment, runQueue, orkerQueue)

	runID := fmt.Sprintf("ork-limit-%d", time.Now().UnixNano())
	plan := rawPlan(orkerQueue)
	plan.History = &engine.HistoryLimit{MaxEvents: 600, MaxBytes: engine.DefaultHistoryMaxBytes, Reserve: engine.DefaultHistoryReserve}
	starter := engine.NewTemporalStarterWithOrkerQueue(c, runQueue, orkerQueue)
	if err := starter.StartRun(ctx, runID, compile(t, historyLimitFixtureDocument()),
		payload(t, map[string]any{"parts": guardItems(30)}), plan); err != nil {
		t.Fatalf("StartRun: %v", err)
	}
	if err := c.GetWorkflow(ctx, runID, "").Get(ctx, nil); err == nil {
		t.Fatal("the recorded run finished; it must end RUN_HISTORY_LIMIT")
	}
	s := &guardStack{t: t, ctx: ctx, client: c, runs: runs}
	closedRun := s.requireStoppedByTheGuard(runID, plan.History)
	if closedRun.MarkerEvent == 0 {
		t.Fatal("the recorded history holds no run-history-guard marker")
	}

	recorded := s.fullHistory(runID)
	encoded, err := temporalproto.CustomJSONMarshalOptions{Indent: "  "}.Marshal(recorded)
	if err != nil {
		t.Fatalf("encode history: %v", err)
	}
	if err := os.WriteFile(historyLimitFixture, append(encoded, '\n'), 0o644); err != nil {
		t.Fatalf("write %s: %v", historyLimitFixture, err)
	}
	t.Logf("recorded %d history events for run %s into %s (deployment version %s); crossing %+v; blockers %q",
		len(recorded.Events), runID, historyLimitFixture, deployment, closedRun.Crossing, closedRun.Blockers)
}
