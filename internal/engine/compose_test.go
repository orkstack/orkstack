package engine_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
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
	"google.golang.org/protobuf/types/known/structpb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/internal/task"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// memComposeStore is memValues with the Put a composed task's values need.
type memComposeStore struct {
	*memValues
	mu      sync.Mutex
	puts    []artifacts.PutOptions
	putErr  error
	reads   []string
	badHash bool
}

func (m *memComposeStore) Get(ctx context.Context, uri string) (io.ReadCloser, error) {
	m.mu.Lock()
	m.reads = append(m.reads, uri)
	m.mu.Unlock()
	return m.memValues.Get(ctx, uri)
}

func (m *memComposeStore) Put(_ context.Context, name string, r io.Reader, opts artifacts.PutOptions) (artifacts.ArtifactRef, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.putErr != nil {
		return artifacts.ArtifactRef{}, m.putErr
	}
	data, err := io.ReadAll(r)
	if err != nil {
		return artifacts.ArtifactRef{}, err
	}
	sum := sha256.Sum256(data)
	digest := hex.EncodeToString(sum[:])
	uri := "mem://composed/" + digest
	m.memValues.mu.Lock()
	m.memValues.objects[uri] = data
	m.memValues.mu.Unlock()
	m.puts = append(m.puts, opts)
	if m.badHash {
		digest = strings.Repeat("0", 64)
	}
	return artifacts.ArtifactRef{Name: name, URI: uri, ContentType: opts.ContentType,
		SizeBytes: int64(len(data)), SHA256: digest, Metadata: opts.Metadata}, nil
}

func (m *memComposeStore) readURIs() []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]string(nil), m.reads...)
}

// childStart is one mocked child run's start, as the child saw it.
type childStart struct {
	workflowID string
	parentID   string
	runID      string
	wf         *orkstackv1.Orkflow
	input      *orkstackv1.Payload
	plan       engine.RunPlan
}

// composeHarness runs a parent RunWorkflow with every activity mocked, the
// two compose activities real over memComposeStore, and every published
// event and snapshot recorded.
type composeHarness struct {
	env       *testsuite.TestWorkflowEnvironment
	bus       *engine.EventBus
	store     *engine.RunStore
	snapshots *testSnapshotStore
	values    *memComposeStore

	mu        sync.Mutex
	calls     []*orkstackv1.ExecuteTaskInput
	published []published
	renders   []engine.ComposeRenderRequest
	stores    []engine.ComposeStoreRequest
	children  []childStart
}

func newComposeHarness(t *testing.T, behavior func(*composeHarness, *orkstackv1.ExecuteTaskInput) (*orkstackv1.ExecuteTaskResult, error)) *composeHarness {
	t.Helper()
	var suite testsuite.WorkflowTestSuite
	h := &composeHarness{
		env:       suite.NewTestWorkflowEnvironment(),
		bus:       engine.NewEventBus(),
		store:     engine.NewRunStore(),
		snapshots: newTestSnapshotStore(),
		values:    &memComposeStore{memValues: &memValues{objects: map[string][]byte{}}},
	}
	h.env.SetTestTimeout(60 * time.Second)
	// The interpreter under a second type name, so a test that fakes the
	// CHILD run (registered as RunWorkflowType by mockChild or realChild)
	// still runs the real parent.
	h.env.RegisterWorkflowWithOptions(engine.RunWorkflow, workflow.RegisterOptions{Name: parentWorkflowType})
	h.env.RegisterActivityWithOptions(engine.NewSnapshotActivities(h.snapshots).WriteSnapshot,
		activity.RegisterOptions{Name: engine.ActivityWriteSnapshot})
	publish := engine.NewEventActivities(h.bus, h.store).WithSnapshotStore(h.snapshots).PublishEvent
	h.env.RegisterActivityWithOptions(func(ctx context.Context, event *orkstackv1.Event, uri, key string) error {
		h.mu.Lock()
		h.published = append(h.published, published{event: event, snapshotURI: uri})
		h.mu.Unlock()
		return publish(ctx, event, uri, key)
	}, activity.RegisterOptions{Name: engine.ActivityPublishEvent})
	h.env.RegisterActivityWithOptions(engine.NewEachActivities(h.values).CountEachItems,
		activity.RegisterOptions{Name: engine.ActivityCountEachItems})
	compose := engine.NewComposeActivities(h.values)
	h.env.RegisterActivityWithOptions(func(ctx context.Context, req engine.ComposeRenderRequest) (json.RawMessage, error) {
		h.mu.Lock()
		h.renders = append(h.renders, req)
		h.mu.Unlock()
		return compose.RenderComposedValues(ctx, req)
	}, activity.RegisterOptions{Name: engine.ActivityRenderComposedValues})
	h.env.RegisterActivityWithOptions(func(ctx context.Context, req engine.ComposeStoreRequest) (*orkstackv1.ArtifactRef, error) {
		h.mu.Lock()
		h.stores = append(h.stores, req)
		h.mu.Unlock()
		return compose.StoreComposedValues(ctx, req)
	}, activity.RegisterOptions{Name: engine.ActivityStoreComposedValues})
	h.env.RegisterActivityWithOptions(func(_ context.Context, in *orkstackv1.ExecuteTaskInput) (*orkstackv1.ExecuteTaskResult, error) {
		h.mu.Lock()
		h.calls = append(h.calls, in)
		h.mu.Unlock()
		return behavior(h, in)
	}, activity.RegisterOptions{Name: task.ActivityExecuteTask})
	return h
}

// parentWorkflowType is RunWorkflow registered under a name no child is
// started with.
const parentWorkflowType = "ParentRunWorkflow"

// run executes the parent under parentWorkflowType.
func (h *composeHarness) run(runID string, wf *orkstackv1.Orkflow, input *orkstackv1.Payload, plan engine.RunPlan) {
	h.env.SetStartWorkflowOptions(client.StartWorkflowOptions{ID: runID})
	h.env.ExecuteWorkflow(parentWorkflowType, runID, wf, input, plan)
}

// committed is a task result whose values.json really is in the store, so a
// render can read and verify it.
func (h *composeHarness) committed(in *orkstackv1.ExecuteTaskInput, values map[string]any) *orkstackv1.ExecuteTaskResult {
	raw := valuesJSON(values)
	result := &orkstackv1.ExecuteTaskResult{
		TaskId:    in.GetTaskId(),
		Attempt:   1,
		ValuesRef: h.values.put("mem://values/"+in.GetRunId()+"/"+in.GetTaskId(), raw),
		Commit:    &orkstackv1.CommitRef{Uri: "mem://commits/" + in.GetTaskId()},
	}
	if len(raw) <= 4096 {
		result.ValuesJson, result.ValuesInlined = raw, true
	}
	return result
}

// mockChild answers every child run with fn, recording what it was started
// with.
func (h *composeHarness) mockChild(fn func(ctx workflow.Context, start childStart) (*orkstackv1.Run, error)) {
	h.env.RegisterWorkflowWithOptions(
		func(ctx workflow.Context, runID string, wf *orkstackv1.Orkflow, input *orkstackv1.Payload, plan engine.RunPlan) (*orkstackv1.Run, error) {
			info := workflow.GetInfo(ctx)
			start := childStart{workflowID: info.WorkflowExecution.ID, runID: runID, wf: wf, input: input, plan: plan}
			if parent := info.ParentWorkflowExecution; parent != nil {
				start.parentID = parent.ID
			}
			h.mu.Lock()
			h.children = append(h.children, start)
			h.mu.Unlock()
			return fn(ctx, start)
		}, workflow.RegisterOptions{Name: engine.RunWorkflowType})
}

// realChild runs every child as the real RunWorkflow.
func (h *composeHarness) realChild() {
	h.env.RegisterWorkflowWithOptions(engine.RunWorkflow, workflow.RegisterOptions{Name: engine.RunWorkflowType})
}

func (h *composeHarness) startedChildren() []childStart {
	h.mu.Lock()
	defer h.mu.Unlock()
	return append([]childStart(nil), h.children...)
}

func (h *composeHarness) inputFor(taskID string) *orkstackv1.ExecuteTaskInput {
	h.mu.Lock()
	defer h.mu.Unlock()
	for _, in := range h.calls {
		if in.GetTaskId() == taskID {
			return in
		}
	}
	return nil
}

func (h *composeHarness) snapshotAt(t *testing.T, eventType orkstackv1.EventType, taskID string) *orkstackv1.Run {
	t.Helper()
	h.mu.Lock()
	defer h.mu.Unlock()
	for _, p := range h.published {
		if p.event.GetType() == eventType && p.event.GetTaskId() == taskID {
			data, err := h.snapshots.ReadSnapshot(context.Background(), p.snapshotURI)
			if err != nil {
				t.Fatalf("read snapshot %s: %v", p.snapshotURI, err)
			}
			run := &orkstackv1.Run{}
			if err := proto.Unmarshal(data, run); err != nil {
				t.Fatalf("decode snapshot: %v", err)
			}
			return run
		}
	}
	t.Fatalf("no %s event for task %q", eventType, taskID)
	return nil
}

func (h *composeHarness) events(runID, taskID string) []*orkstackv1.Event {
	var out []*orkstackv1.Event
	for _, e := range h.bus.Events(runID, 0) {
		if e.GetTaskId() == taskID {
			out = append(out, e)
		}
	}
	return out
}

const composedParentDocument = `
name: parent
version: "1.0.0"
inputs:
  image: { type: string, default: "ghcr.io/acme/app:1" }
tasks:
  - id: build
    uses: shell
    with: { op: eval, command: ["true"] }
  - id: release
    uses: orkflow:release-block@2.1.0
    needs: [build]
    with:
      image: '{{ .tasks.build.output.value }}'
      note: fixed
  - id: announce
    uses: shell
    needs: [release]
    with: { op: exec, command: ["echo", "{{ .tasks.release.output.digest }}"] }
`

const composedChildDocument = `
name: release-block
version: "2.1.0"
inputs:
  image: { type: string, required: true }
outputs:
  digest: '{{ .tasks.sign.output.value }}'
  url: 'https://records.example/{{ .tasks.sign.output.value }}'
tasks:
  - id: sign
    uses: shell
    with: { op: eval, command: ["true"] }
`

const childDigest = "sha256:" + "c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1"

// composedFixture is the parent and its planned child: the release task
// pins childDigest, which the child definition carries.
func composedFixture(t *testing.T, childDoc string) (*orkstackv1.Orkflow, engine.RunPlan, *orkstackv1.Orkflow) {
	t.Helper()
	parent := compile(t, composedParentDocument)
	for _, task := range parent.GetTasks() {
		if task.GetOrkflow() != nil {
			task.Orkflow.DefinitionDigest = childDigest
		}
	}
	child := compile(t, childDoc)
	child.DefinitionDigest = childDigest
	childPlan := defaultPlan()
	childPlan.CommitRoot = "mem://commits"
	cp, err := engine.NewChildRunPlan(child, childPlan, true, true)
	if err != nil {
		t.Fatal(err)
	}
	cp.RunID = "ork-1758790021000000000-4081623940183294"
	plan := defaultPlan()
	plan.CommitRoot = "mem://commits"
	plan.Children = map[string]*engine.ChildRunPlan{"release": cp}
	return parent, plan, child
}

func defaultBehavior(h *composeHarness, in *orkstackv1.ExecuteTaskInput) (*orkstackv1.ExecuteTaskResult, error) {
	return h.committed(in, map[string]any{"value": "img@" + in.GetTaskId()}), nil
}

func childOutputs(t *testing.T, values map[string]any) *orkstackv1.Run {
	t.Helper()
	return &orkstackv1.Run{Status: orkstackv1.Status_STATUS_SUCCEEDED, Output: payload(t, values)}
}

func taskRunOfRun(run *orkstackv1.Run, id string) *orkstackv1.TaskRun {
	for _, tr := range run.GetTaskRuns() {
		if tr.GetTaskId() == id {
			return tr
		}
	}
	return nil
}

func TestRunWorkflow_ComposedTaskStartsOneChildRun(t *testing.T) {
	h := newComposeHarness(t, defaultBehavior)
	parent, plan, child := composedFixture(t, composedChildDocument)
	h.mockChild(func(workflow.Context, childStart) (*orkstackv1.Run, error) {
		return childOutputs(t, map[string]any{"digest": "sha256:abc", "url": "https://r/abc"}), nil
	})
	h.run("ork-parent", parent, payload(t, nil), plan)
	if err := h.env.GetWorkflowError(); err != nil {
		t.Fatalf("workflow error: %v", err)
	}

	starts := h.startedChildren()
	if len(starts) != 1 {
		t.Fatalf("%d child starts, want exactly 1", len(starts))
	}
	got := starts[0]
	cp := plan.Children["release"]
	if got.workflowID != cp.RunID || got.runID != cp.RunID || got.parentID != "ork-parent" {
		t.Fatalf("child start = id %q run %q parent %q", got.workflowID, got.runID, got.parentID)
	}
	wantDef := proto.Clone(child).(*orkstackv1.Orkflow)
	wantDef.SourceYaml = ""
	if !proto.Equal(got.wf, wantDef) {
		t.Fatalf("child definition differs:\n got %v\nwant %v", got.wf, wantDef)
	}
	if got.input.GetData().AsMap()["image"] != "img@build" || got.input.GetData().AsMap()["note"] != "fixed" {
		t.Fatalf("child input = %v", got.input.GetData().AsMap())
	}
	wantPlan := cp.Plan
	wantPlan.Origin = &engine.RunOrigin{Kind: "RUN_ORIGIN_KIND_PARENT_RUN", ID: "ork-parent"}
	gotJSON, _ := json.Marshal(got.plan)
	wantJSON, _ := json.Marshal(wantPlan)
	if !bytes.Equal(gotJSON, wantJSON) {
		t.Fatalf("child plan:\n got %s\nwant %s", gotJSON, wantJSON)
	}

	var types []string
	for _, e := range h.events("ork-parent", "release") {
		types = append(types, e.GetType().String()+"|"+e.GetMessage())
	}
	want := []string{
		"EVENT_TYPE_TASK_SCHEDULED|child run requested for orkflow release-block@2.1.0",
		"EVENT_TYPE_TASK_STARTED|child run " + cp.RunID + " started",
		"EVENT_TYPE_TASK_SUCCEEDED|child run " + cp.RunID + " succeeded",
	}
	if strings.Join(types, "\n") != strings.Join(want, "\n") {
		t.Fatalf("events:\n%s\nwant\n%s", strings.Join(types, "\n"), strings.Join(want, "\n"))
	}
	scheduled := taskRunOfRun(h.snapshotAt(t, orkstackv1.EventType_EVENT_TYPE_TASK_SCHEDULED, "release"), "release")
	if scheduled.GetChild().GetRunId() != "" || scheduled.GetChild().GetOrkflowName() != "release-block" {
		t.Fatalf("TASK_SCHEDULED child = %v", scheduled.GetChild())
	}
	started := taskRunOfRun(h.snapshotAt(t, orkstackv1.EventType_EVENT_TYPE_TASK_STARTED, "release"), "release")
	if started.GetChild().GetRunId() != cp.RunID || started.GetScheduledAt() == nil || started.GetStartedAt() == nil {
		t.Fatalf("TASK_STARTED row = %v", started)
	}

	if len(h.stores) != 1 || string(h.stores[0].Values) != `{"digest":"sha256:abc","url":"https://r/abc"}` {
		t.Fatalf("stores = %+v", h.stores)
	}
	meta := h.values.puts[0].Metadata
	if meta[artifacts.MetaTaskID] != "release" || meta[artifacts.MetaRunID] != "ork-parent" || meta["artifact"] != "values.json" ||
		h.values.puts[0].ContentType != "application/json" {
		t.Fatalf("put options = %+v", h.values.puts[0])
	}

	announce := h.inputFor("announce")
	var ref *orkstackv1.UpstreamRef
	for _, up := range announce.GetUpstream() {
		if up.GetTaskId() == "release" {
			ref = up
		}
	}
	if ref == nil || ref.GetCommitUri() != "" || ref.GetValues().GetUri() == "" {
		t.Fatalf("announce's release upstream = %v", ref)
	}
	if h.inputFor("release") != nil {
		t.Fatal("a composed task reached an Orker")
	}
	run, _ := h.store.Get("ork-parent")
	row := taskRunOfRun(run, "release")
	if row.GetStatus() != orkstackv1.Status_STATUS_SUCCEEDED || row.GetValuesRef() == nil ||
		row.GetOutput().GetData().AsMap()["digest"] != "sha256:abc" || row.GetPool() != "" || row.GetAttempt() != 0 {
		t.Fatalf("release row = %v", row)
	}
}

func TestRunWorkflow_ChildInputIsRenderedFromFullUpstreamValues(t *testing.T) {
	big := strings.Repeat("x", 10*1024)
	h := newComposeHarness(t, func(h *composeHarness, in *orkstackv1.ExecuteTaskInput) (*orkstackv1.ExecuteTaskResult, error) {
		if in.GetTaskId() == "build" {
			return h.committed(in, map[string]any{"value": big}), nil
		}
		return h.committed(in, map[string]any{}), nil
	})
	doc := strings.Replace(composedParentDocument, "  - id: release\n", `  - id: other
    uses: shell
    with: { op: eval, command: ["true"] }
  - id: release
`, 1)
	doc = strings.Replace(doc, "needs: [build]\n    with:\n      image", "needs: [build, other]\n    with:\n      image", 1)
	parent := compile(t, doc)
	_, plan, _ := composedFixture(t, composedChildDocument)
	for _, task := range parent.GetTasks() {
		if task.GetOrkflow() != nil {
			task.Orkflow.DefinitionDigest = childDigest
		}
	}
	h.mockChild(func(workflow.Context, childStart) (*orkstackv1.Run, error) {
		return childOutputs(t, map[string]any{"digest": "d", "url": "u"}), nil
	})
	h.run("ork-parent", parent, payload(t, nil), plan)
	if err := h.env.GetWorkflowError(); err != nil {
		t.Fatalf("workflow error: %v", err)
	}
	starts := h.startedChildren()
	if len(starts) != 1 || starts[0].input.GetData().AsMap()["image"] != big {
		t.Fatalf("the child did not receive the full 10 KiB value")
	}
	if len(h.renders) == 0 || len(h.renders[0].Upstream) != 1 || h.renders[0].Upstream[0].TaskID != "build" {
		t.Fatalf("the render read %+v; want build only", h.renders)
	}
}

func TestRunWorkflow_ChildInputRenderFailureStartsNoChild(t *testing.T) {
	h := newComposeHarness(t, func(h *composeHarness, in *orkstackv1.ExecuteTaskInput) (*orkstackv1.ExecuteTaskResult, error) {
		return h.committed(in, map[string]any{"other": "x"}), nil
	})
	parent, plan, _ := composedFixture(t, composedChildDocument)
	h.mockChild(func(workflow.Context, childStart) (*orkstackv1.Run, error) {
		t.Error("a child was started")
		return childOutputs(t, nil), nil
	})
	h.run("ork-parent", parent, payload(t, nil), plan)
	if h.env.GetWorkflowError() == nil {
		t.Fatal("the run succeeded")
	}
	run, _ := h.store.Get("ork-parent")
	row := taskRunOfRun(run, "release")
	if row.GetStatus() != orkstackv1.Status_STATUS_FAILED || row.GetError().GetCode() != engine.ChildInputRenderFailedCode ||
		row.GetError().GetCategory() != orkstackv1.FailureCategory_FAILURE_CATEGORY_CONTRACT_VIOLATION ||
		row.GetChild().GetRunId() != "" {
		t.Fatalf("release row = %v", row)
	}
	if run.GetStatus() != orkstackv1.Status_STATUS_FAILED {
		t.Fatalf("run status = %v", run.GetStatus())
	}
	if len(h.startedChildren()) != 0 {
		t.Fatal("a child was started")
	}
}

func TestRunWorkflow_ChildWithoutOutputsGivesEmptyValues(t *testing.T) {
	h := newComposeHarness(t, defaultBehavior)
	noOutputs := strings.Replace(composedChildDocument,
		"outputs:\n  digest: '{{ .tasks.sign.output.value }}'\n  url: 'https://records.example/{{ .tasks.sign.output.value }}'\n", "", 1)
	parent, plan, _ := composedFixture(t, noOutputs)
	// announce reads digest, which a child with no outputs cannot give.
	parent.Tasks[2].With, _ = structpb.NewStruct(map[string]any{"op": "exec", "command": []any{"true"}})
	h.mockChild(func(workflow.Context, childStart) (*orkstackv1.Run, error) {
		// The child's leaf aggregation is not the task's values.
		return childOutputs(t, map[string]any{"sign": map[string]any{"value": "x"}}), nil
	})
	h.run("ork-parent", parent, payload(t, nil), plan)
	if err := h.env.GetWorkflowError(); err != nil {
		t.Fatalf("workflow error: %v", err)
	}
	if len(h.stores) != 1 || string(h.stores[0].Values) != `{}` {
		t.Fatalf("stored %+v; want {}", h.stores)
	}
	run, _ := h.store.Get("ork-parent")
	if out := taskRunOfRun(run, "release").GetOutput().GetData().AsMap(); len(out) != 0 {
		t.Fatalf("release output = %v", out)
	}
}

// grandchildError is what a parent sees when its child's own composed task
// failed: the child's collectChild error, with the category as its type.
func applicationFailure(category, message string) error {
	return temporal.NewNonRetryableApplicationError(message, category, nil)
}

func TestRunWorkflow_ChildFailureCarriesTheChildsCategory(t *testing.T) {
	cases := []struct {
		name     string
		err      error
		status   orkstackv1.Status
		code     string
		category orkstackv1.FailureCategory
		inner    string
	}{
		{"activity failed", applicationFailure(task.CategoryActivityFailed, "task sign exited 1"),
			orkstackv1.Status_STATUS_FAILED, engine.ChildRunFailedCode, orkstackv1.FailureCategory_FAILURE_CATEGORY_ACTIVITY_FAILED, "task sign exited 1"},
		{"contract violation", applicationFailure(task.CategoryContractViolation, "no values.json"),
			orkstackv1.Status_STATUS_FAILED, engine.ChildRunFailedCode, orkstackv1.FailureCategory_FAILURE_CATEGORY_CONTRACT_VIOLATION, "no values.json"},
		{"timed out", applicationFailure(task.CategoryTimedOut, "container timed out"),
			orkstackv1.Status_STATUS_FAILED, engine.ChildRunFailedCode, orkstackv1.FailureCategory_FAILURE_CATEGORY_TIMED_OUT, "container timed out"},
		{"await rejected", applicationFailure(engine.AwaitRejectedCode, `await "gate" was rejected: no`),
			orkstackv1.Status_STATUS_FAILED, engine.ChildRunFailedCode, orkstackv1.FailureCategory_FAILURE_CATEGORY_ACTIVITY_FAILED, "was rejected"},
		{"await timed out", applicationFailure(engine.AwaitTimedOutCode, `await "gate" was not resolved`),
			orkstackv1.Status_STATUS_FAILED, engine.ChildRunFailedCode, orkstackv1.FailureCategory_FAILURE_CATEGORY_TIMED_OUT, "was not resolved"},
		{"grandchild failed", applicationFailure(task.CategoryInfrastructureError, "child run ork-g (x@1) failed: lost"),
			orkstackv1.Status_STATUS_FAILED, engine.ChildRunFailedCode, orkstackv1.FailureCategory_FAILURE_CATEGORY_INFRASTRUCTURE_ERROR, "child run ork-g (x@1) failed: lost"},
		{"unknown type", errors.New("boom"),
			orkstackv1.Status_STATUS_FAILED, engine.ChildRunFailedCode, orkstackv1.FailureCategory_FAILURE_CATEGORY_UNSPECIFIED, "boom"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			h := newComposeHarness(t, defaultBehavior)
			parent, plan, _ := composedFixture(t, composedChildDocument)
			h.mockChild(func(workflow.Context, childStart) (*orkstackv1.Run, error) { return nil, tc.err })
			h.run("ork-parent", parent, payload(t, nil), plan)
			if h.env.GetWorkflowError() == nil {
				t.Fatal("the run succeeded")
			}
			run, _ := h.store.Get("ork-parent")
			row := taskRunOfRun(run, "release")
			if row.GetStatus() != tc.status || row.GetError().GetCode() != tc.code || row.GetError().GetCategory() != tc.category {
				t.Fatalf("release row error = %v (status %v)", row.GetError(), row.GetStatus())
			}
			cp := plan.Children["release"]
			want := fmt.Sprintf("child run %s (release-block@2.1.0) failed: ", cp.RunID)
			if !strings.HasPrefix(row.GetError().GetMessage(), want) || !strings.Contains(row.GetError().GetMessage(), tc.inner) {
				t.Fatalf("message = %q", row.GetError().GetMessage())
			}
			for _, msg := range []string{row.GetError().GetMessage(), run.GetError().GetMessage()} {
				if strings.Contains(msg, "child workflow execution error") || strings.Contains(msg, "workflowID") {
					t.Fatalf("a message names the SDK wrapper: %q", msg)
				}
			}
			if !strings.HasPrefix(row.GetError().GetDetails(), "child run "+cp.RunID+"; open it with orkctl run get "+cp.RunID) {
				t.Fatalf("details = %q", row.GetError().GetDetails())
			}
			if row.GetChild().GetRunId() != cp.RunID {
				t.Fatalf("child.run_id = %q", row.GetChild().GetRunId())
			}
			if run.GetStatus() != orkstackv1.Status_STATUS_FAILED || run.GetError().GetMessage() != row.GetError().GetMessage() {
				t.Fatalf("run = %v / %v", run.GetStatus(), run.GetError())
			}
		})
	}
}

func TestRunWorkflow_ChildCanceledOnItsOwnCancelsTheParent(t *testing.T) {
	h := newComposeHarness(t, defaultBehavior)
	parent, plan, _ := composedFixture(t, composedChildDocument)
	h.mockChild(func(workflow.Context, childStart) (*orkstackv1.Run, error) {
		return nil, temporal.NewCanceledError("canceled by operator")
	})
	h.run("ork-parent", parent, payload(t, nil), plan)
	if h.env.GetWorkflowError() == nil {
		t.Fatal("the run succeeded")
	}
	run, _ := h.store.Get("ork-parent")
	row := taskRunOfRun(run, "release")
	cp := plan.Children["release"]
	if row.GetStatus() != orkstackv1.Status_STATUS_CANCELED || row.GetError().GetCode() != engine.ChildRunCanceledCode ||
		row.GetError().GetCategory() != orkstackv1.FailureCategory_FAILURE_CATEGORY_CANCELLED {
		t.Fatalf("release row = %v", row)
	}
	want := fmt.Sprintf("child run %s (release-block@2.1.0) was canceled", cp.RunID)
	if run.GetStatus() != orkstackv1.Status_STATUS_CANCELED || run.GetError().GetCode() != "RUN_CANCELED" ||
		run.GetError().GetMessage() != want {
		t.Fatalf("run = %v / %v", run.GetStatus(), run.GetError())
	}
	if statusOf(run, "announce") != orkstackv1.Status_STATUS_CANCELED {
		t.Fatalf("announce = %v", statusOf(run, "announce"))
	}
}

func TestRunWorkflow_ChildStartRefusedIsChildStartFailed(t *testing.T) {
	h := newComposeHarness(t, defaultBehavior)
	parent, plan, _ := composedFixture(t, composedChildDocument)
	// An id the cluster already holds: a second execution with the child's id
	// is started first, so the parent's start is refused.
	h.mockChild(func(workflow.Context, childStart) (*orkstackv1.Run, error) {
		return childOutputs(t, map[string]any{"digest": "d", "url": "u"}), nil
	})
	cp := plan.Children["release"]
	// Two composed tasks sharing one planned id: the second start is a
	// duplicate the cluster refuses.
	parent.Tasks = append(parent.Tasks, &orkstackv1.Task{
		Id: "again", Uses: parent.Tasks[1].GetUses(), Needs: []string{"release"},
		Orkflow: proto.Clone(parent.Tasks[1].GetOrkflow()).(*orkstackv1.OrkflowRef),
	})
	plan.Children["again"] = cp
	h.run("ork-parent", parent, payload(t, nil), plan)
	if h.env.GetWorkflowError() == nil {
		t.Fatal("the run succeeded")
	}
	run, _ := h.store.Get("ork-parent")
	row := taskRunOfRun(run, "again")
	if row.GetStatus() != orkstackv1.Status_STATUS_FAILED || row.GetError().GetCode() != engine.ChildStartFailedCode ||
		row.GetError().GetCategory() != orkstackv1.FailureCategory_FAILURE_CATEGORY_INFRASTRUCTURE_ERROR ||
		row.GetChild().GetRunId() != "" {
		t.Fatalf("again row = %v", row)
	}
	if !strings.Contains(row.GetError().GetMessage(), "child run "+cp.RunID+" of orkflow release-block@2.1.0 could not be started") {
		t.Fatalf("message = %q", row.GetError().GetMessage())
	}
}

func TestRunWorkflow_SkippedComposedTaskStartsNoChild(t *testing.T) {
	h := newComposeHarness(t, defaultBehavior)
	parent, plan, _ := composedFixture(t, composedChildDocument)
	parent.Tasks[1].Condition = `{{ eq .inputs.image "never" }}`
	h.mockChild(func(workflow.Context, childStart) (*orkstackv1.Run, error) {
		t.Error("a child was started")
		return childOutputs(t, nil), nil
	})
	h.run("ork-parent", parent, payload(t, nil), plan)
	if err := h.env.GetWorkflowError(); err != nil {
		t.Fatalf("workflow error: %v", err)
	}
	run, _ := h.store.Get("ork-parent")
	row := taskRunOfRun(run, "release")
	if row.GetStatus() != orkstackv1.Status_STATUS_SKIPPED || row.GetChild().GetRunId() != "" || row.GetScheduledAt() != nil {
		t.Fatalf("release row = %v", row)
	}
	if statusOf(run, "announce") != orkstackv1.Status_STATUS_SKIPPED {
		t.Fatalf("announce = %v", statusOf(run, "announce"))
	}
	if len(h.startedChildren()) != 0 {
		t.Fatal("a child was started")
	}
}

func TestRunWorkflow_ComposedTaskWithoutAPlannedChildIsInvalidComposition(t *testing.T) {
	cases := map[string]func(plan *engine.RunPlan){
		"nil entry": func(plan *engine.RunPlan) { plan.Children = nil },
		"undecodable definition": func(plan *engine.RunPlan) {
			cp := *plan.Children["release"]
			cp.Orkflow = json.RawMessage(`{"tasks":"not a list"}`)
			plan.Children = map[string]*engine.ChildRunPlan{"release": &cp}
		},
		"digest mismatch": func(plan *engine.RunPlan) {
			cp := *plan.Children["release"]
			def, _ := cp.Definition()
			def.DefinitionDigest = "sha256:other"
			moved, _ := engine.NewChildRunPlan(def, cp.Plan, false, false)
			moved.RunID = cp.RunID
			plan.Children = map[string]*engine.ChildRunPlan{"release": moved}
		},
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			h := newComposeHarness(t, defaultBehavior)
			parent, plan, _ := composedFixture(t, composedChildDocument)
			mutate(&plan)
			h.run("ork-parent", parent, payload(t, nil), plan)
			if h.env.GetWorkflowError() == nil {
				t.Fatal("the run succeeded")
			}
			run, _ := h.store.Get("ork-parent")
			row := taskRunOfRun(run, "release")
			if row.GetStatus() != orkstackv1.Status_STATUS_FAILED || row.GetError().GetCode() != engine.InvalidCompositionCode ||
				row.GetError().GetCategory() != orkstackv1.FailureCategory_FAILURE_CATEGORY_CONTRACT_VIOLATION {
				t.Fatalf("release row = %v", row)
			}
		})
	}
}

const outputsDocument = `
name: block
version: "1.0.0"
inputs:
  image: { type: string, default: "img" }
outputs:
  digest: '{{ .tasks.sign.output.value }}'
  image: '{{ .inputs.image }}'
  const: fixed
tasks:
  - id: sign
    uses: shell
    with: { op: eval, command: ["true"] }
  - id: leaf
    uses: shell
    needs: [sign]
    with: { op: eval, command: ["true"] }
`

func TestRunWorkflow_DeclaredOutputsBecomeRunOutput(t *testing.T) {
	h := newComposeHarness(t, defaultBehavior)
	plan := defaultPlan()
	h.run("ork-outputs", compile(t, outputsDocument), payload(t, nil), plan)
	if err := h.env.GetWorkflowError(); err != nil {
		t.Fatalf("workflow error: %v", err)
	}
	run := mustWorkflowResult(t, h.env)
	got := run.GetOutput().GetData().AsMap()
	if len(got) != 3 || got["digest"] != "img@sign" || got["image"] != "img" || got["const"] != "fixed" {
		t.Fatalf("output = %v", got)
	}
}

func TestRunWorkflow_OutputsRenderFailureFailsTheRun(t *testing.T) {
	h := newComposeHarness(t, defaultBehavior)
	doc := strings.Replace(outputsDocument, "{{ .tasks.sign.output.value }}", "{{ .tasks.sign.output.missing }}", 1)
	h.run("ork-outputs", compile(t, doc), payload(t, nil), defaultPlan())
	err := h.env.GetWorkflowError()
	if err == nil {
		t.Fatal("the run succeeded")
	}
	run, _ := h.store.Get("ork-outputs")
	if run.GetStatus() != orkstackv1.Status_STATUS_FAILED || run.GetError().GetCode() != engine.OutputsRenderFailedCode ||
		run.GetError().GetCategory() != orkstackv1.FailureCategory_FAILURE_CATEGORY_CONTRACT_VIOLATION ||
		run.GetError().GetDetails() != "outputs.digest" ||
		!strings.HasPrefix(run.GetError().GetMessage(), "the run's tasks finished but its declared outputs could not be produced: ") {
		t.Fatalf("run = %v / %v", run.GetStatus(), run.GetError())
	}
	for _, id := range []string{"sign", "leaf"} {
		if statusOf(run, id) != orkstackv1.Status_STATUS_SUCCEEDED {
			t.Fatalf("%s = %v; every task keeps its status", id, statusOf(run, id))
		}
	}
	events := h.bus.Events("ork-outputs", 0)
	if last := events[len(events)-1]; last.GetType() != orkstackv1.EventType_EVENT_TYPE_RUN_FAILED {
		t.Fatalf("last event = %v", last.GetType())
	}
}

func TestRunWorkflow_OutputsOverTheBoundFailTheRun(t *testing.T) {
	h := newComposeHarness(t, func(h *composeHarness, in *orkstackv1.ExecuteTaskInput) (*orkstackv1.ExecuteTaskResult, error) {
		return h.committed(in, map[string]any{"value": strings.Repeat("y", 900)}), nil
	})
	plan := defaultPlan()
	plan.Limits.ValuesMaxBytes = 1024
	doc := strings.Replace(outputsDocument, "  const: fixed\n", "  again: '{{ .tasks.sign.output.value }}'\n", 1)
	h.run("ork-outputs", compile(t, doc), payload(t, nil), plan)
	if h.env.GetWorkflowError() == nil {
		t.Fatal("the run succeeded")
	}
	run, _ := h.store.Get("ork-outputs")
	if run.GetStatus() != orkstackv1.Status_STATUS_FAILED || run.GetError().GetCode() != engine.OutputsTooLargeCode {
		t.Fatalf("run = %v / %v", run.GetStatus(), run.GetError())
	}
}

func TestRunWorkflow_NoOutputsKeepsLeafAggregation(t *testing.T) {
	h := newComposeHarness(t, defaultBehavior)
	doc := strings.Replace(outputsDocument,
		"outputs:\n  digest: '{{ .tasks.sign.output.value }}'\n  image: '{{ .inputs.image }}'\n  const: fixed\n", "", 1)
	h.run("ork-leaves", compile(t, doc), payload(t, nil), defaultPlan())
	if err := h.env.GetWorkflowError(); err != nil {
		t.Fatalf("workflow error: %v", err)
	}
	run := mustWorkflowResult(t, h.env)
	got := run.GetOutput().GetData().AsMap()
	leaf, _ := got["leaf"].(map[string]any)
	if len(got) != 1 || leaf["value"] != "img@leaf" {
		t.Fatalf("output = %v", got)
	}
	if len(h.renders) != 0 {
		t.Fatal("a render ran for an Orkflow with no outputs")
	}
}

func TestRunWorkflow_ChildRunReturnsACompactResult(t *testing.T) {
	h := newComposeHarness(t, defaultBehavior)
	plan := defaultPlan()
	plan.Origin = &engine.RunOrigin{Kind: "RUN_ORIGIN_KIND_PARENT_RUN", ID: "ork-parent"}
	h.run("ork-child", compile(t, outputsDocument), payload(t, nil), plan)
	if err := h.env.GetWorkflowError(); err != nil {
		t.Fatalf("workflow error: %v", err)
	}
	run := mustWorkflowResult(t, h.env)
	if len(run.GetTaskRuns()) != 0 || run.GetInput() != nil || run.GetCreatedAt() != nil {
		t.Fatalf("a child result carried more than it should: %v", run)
	}
	if run.GetId() != "ork-child" || run.GetOrkflowName() != "block" || run.GetStatus() != orkstackv1.Status_STATUS_SUCCEEDED ||
		run.GetOrigin().GetKind() != orkstackv1.RunOriginKind_RUN_ORIGIN_KIND_PARENT_RUN ||
		run.GetOutput().GetData().AsMap()["digest"] != "img@sign" {
		t.Fatalf("compact result = %v", run)
	}
	// The snapshot is the whole run, unaffected.
	snapshot, _ := h.store.Get("ork-child")
	if len(snapshot.GetTaskRuns()) != 2 {
		t.Fatalf("snapshot task runs = %d", len(snapshot.GetTaskRuns()))
	}
}

func TestRunWorkflow_TopLevelResultIsUnchanged(t *testing.T) {
	h := newComposeHarness(t, defaultBehavior)
	h.run("ork-top", compile(t, outputsDocument), payload(t, nil), defaultPlan())
	if err := h.env.GetWorkflowError(); err != nil {
		t.Fatalf("workflow error: %v", err)
	}
	run := mustWorkflowResult(t, h.env)
	snapshot, _ := h.store.Get("ork-top")
	if len(run.GetTaskRuns()) != 2 || run.GetCreatedAt() == nil || !proto.Equal(run, snapshot) {
		t.Fatalf("the top-level result is not the snapshot:\n%v\n%v", run, snapshot)
	}
}

func TestRunWorkflow_RealChildInTheTestEnvironment(t *testing.T) {
	h := newComposeHarness(t, defaultBehavior)
	h.realChild()
	parent, plan, _ := composedFixture(t, composedChildDocument)
	h.run("ork-parent", parent, payload(t, nil), plan)
	if err := h.env.GetWorkflowError(); err != nil {
		t.Fatalf("workflow error: %v", err)
	}
	cp := plan.Children["release"]
	child, ok := h.store.Get(cp.RunID)
	if !ok {
		t.Fatal("the child's snapshots were not projected under its own id")
	}
	if child.GetId() != cp.RunID || child.GetOrigin().GetKind() != orkstackv1.RunOriginKind_RUN_ORIGIN_KIND_PARENT_RUN ||
		child.GetOrigin().GetId() != "ork-parent" || child.GetStatus() != orkstackv1.Status_STATUS_SUCCEEDED {
		t.Fatalf("child = %v", child)
	}
	if got := child.GetInput().GetData().AsMap()["image"]; got != "img@build" {
		t.Fatalf("child input image = %v", got)
	}
	parentRun, _ := h.store.Get("ork-parent")
	out := taskRunOfRun(parentRun, "release").GetOutput().GetData().AsMap()
	if out["digest"] != "img@sign" || out["url"] != "https://records.example/img@sign" {
		t.Fatalf("release output = %v", out)
	}
	announce := h.inputFor("announce")
	if announce == nil || announce.GetRunId() != "ork-parent" {
		t.Fatal("announce did not run in the parent")
	}
}

func TestComposeActivities_RenderComposedValues(t *testing.T) {
	store := &memComposeStore{memValues: &memValues{objects: map[string][]byte{}}}
	ref := store.put("mem://v/build", []byte(`{"value":"sha256:x","n":12345678901234567890}`))
	a := engine.NewComposeActivities(store)
	req := engine.ComposeRenderRequest{
		Label: `task "release": with`, CodePrefix: "CHILD_INPUT",
		Templates: json.RawMessage(`{"image":"{{ .tasks.build.output.value }}","n":"{{ .tasks.build.output.n }}","k":7}`),
		Inputs:    json.RawMessage(`{}`),
		Upstream:  []engine.ComposeUpstream{{TaskID: "build", URI: ref.GetUri(), SHA256: ref.GetSha256()}},
		MaxBytes:  1024,
	}
	out, err := a.RenderComposedValues(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	if string(out) != `{"image":"sha256:x","k":7,"n":"12345678901234567890"}` {
		t.Fatalf("rendered %s", out)
	}

	codeOf := func(err error) (string, bool) {
		var app *temporal.ApplicationError
		var detail *orkstackv1.ActivityFailureDetail
		if errors.As(err, &app) && app.Details(&detail) == nil {
			return detail.GetReasonCode(), app.NonRetryable()
		}
		return "", false
	}
	mutated := req
	store.objects[ref.GetUri()] = []byte(`{"value":"tampered"}`)
	if code, nonRetryable := codeOf(func() error { _, err := a.RenderComposedValues(context.Background(), mutated); return err }()); code != "CHILD_INPUT_UNAVAILABLE" || !nonRetryable {
		t.Fatalf("mutated bytes = %q (non-retryable %v)", code, nonRetryable)
	}
	store.put(ref.GetUri(), []byte(`{"value":"sha256:x","n":12345678901234567890}`))

	small := req
	small.MaxBytes = 10
	if code, _ := codeOf(func() error { _, err := a.RenderComposedValues(context.Background(), small); return err }()); code != "CHILD_INPUT_UNAVAILABLE" {
		t.Fatalf("over-bound read = %q", code)
	}
	tooLarge := req
	tooLarge.Upstream = nil
	tooLarge.Templates = json.RawMessage(`{"x":"` + strings.Repeat("z", 100) + `"}`)
	tooLarge.MaxBytes = 50
	tooLarge.CodePrefix = "OUTPUTS"
	if code, _ := codeOf(func() error { _, err := a.RenderComposedValues(context.Background(), tooLarge); return err }()); code != "OUTPUTS_TOO_LARGE" {
		t.Fatalf("over-bound result = %q", code)
	}
	failing := req
	failing.Templates = json.RawMessage(`{"x":"{{ .tasks.build.output.nope }}"}`)
	if code, _ := codeOf(func() error { _, err := a.RenderComposedValues(context.Background(), failing); return err }()); code != "CHILD_INPUT_RENDER_FAILED" {
		t.Fatalf("render failure = %q", code)
	}
	missing := req
	missing.Upstream = []engine.ComposeUpstream{{TaskID: "build", URI: "mem://nowhere"}}
	_, err = a.RenderComposedValues(context.Background(), missing)
	if err == nil {
		t.Fatal("a missing object rendered")
	}
	var app *temporal.ApplicationError
	if errors.As(err, &app) && app.NonRetryable() {
		t.Fatalf("a read error is non-retryable: %v", err)
	}
}

func TestComposeActivities_StoreComposedValues(t *testing.T) {
	store := &memComposeStore{memValues: &memValues{objects: map[string][]byte{}}}
	a := engine.NewComposeActivities(store)
	req := engine.ComposeStoreRequest{Orkflow: "parent", RunID: "ork-p", TaskID: "release", Values: json.RawMessage(`{"digest":"d"}`)}
	ref, err := a.StoreComposedValues(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256([]byte(`{"digest":"d"}`))
	if ref.GetSha256() != hex.EncodeToString(sum[:]) || ref.GetName() != "values.json" || ref.GetContentType() != "application/json" ||
		ref.GetSizeBytes() != int64(len(`{"digest":"d"}`)) {
		t.Fatalf("ref = %v", ref)
	}
	meta := store.puts[0].Metadata
	if meta[artifacts.MetaOrkflow] != "parent" || meta[artifacts.MetaRunID] != "ork-p" || meta[artifacts.MetaTaskID] != "release" ||
		meta["artifact"] != "values.json" {
		t.Fatalf("metadata = %v", meta)
	}
	store.badHash = true
	if _, err := a.StoreComposedValues(context.Background(), req); err == nil {
		t.Fatal("a store that misreports the sha256 was accepted")
	}
}

func TestChildRunPlan_RoundTrip(t *testing.T) {
	child := compile(t, composedChildDocument)
	child.DefinitionDigest = childDigest
	cp, err := engine.NewChildRunPlan(child, defaultPlan(), true, false)
	if err != nil {
		t.Fatal(err)
	}
	if cp.RunID != "" || !cp.NamespaceIndexed || cp.OriginIndexed {
		t.Fatalf("plan = %+v", cp)
	}
	if bytes.ContainsAny(cp.Orkflow, " \n") && !bytes.Contains(cp.Orkflow, []byte("{{ .tasks")) {
		t.Fatalf("not compact: %s", cp.Orkflow)
	}
	got, err := cp.Definition()
	if err != nil {
		t.Fatal(err)
	}
	want := proto.Clone(child).(*orkstackv1.Orkflow)
	want.SourceYaml = ""
	if got.GetSourceYaml() != "" || !proto.Equal(got, want) {
		t.Fatalf("round trip:\n got %v\nwant %v", got, want)
	}
	if child.GetSourceYaml() == "" {
		t.Fatal("NewChildRunPlan mutated its input")
	}
}

// TestRunPlan_EncodesIdenticallyWithoutChildren pins the encoding of a plan
// with no children: the field is omitempty, so the golden history keeps
// matching.
func TestRunPlan_EncodesIdenticallyWithoutChildren(t *testing.T) {
	plan := engine.RunPlan{Namespace: "default", Routes: map[string]string{"main": "ork-tasks"}, CommitRoot: "s3://b"}
	got, err := json.Marshal(plan)
	if err != nil {
		t.Fatal(err)
	}
	const want = `{"namespace":"default","routes":{"main":"ork-tasks"},"commit_root":"s3://b","limits":{"values_max_bytes":0,"artifact_max_count":0,"artifact_max_bytes":0,"log_max_bytes":0,"commit_max_bytes":0},"transfer_budget_seconds":0}`
	if string(got) != want {
		t.Fatalf("plan encodes as\n%s\nwant\n%s", got, want)
	}
}

func TestChildWorkflowOptions(t *testing.T) {
	child := compile(t, composedChildDocument)
	cp, _ := engine.NewChildRunPlan(child, defaultPlan(), true, true)
	cp.RunID = "ork-child-1"
	childPlan := cp.Plan
	childPlan.Origin = &engine.RunOrigin{Kind: "RUN_ORIGIN_KIND_PARENT_RUN", ID: "ork-parent"}
	opts := engine.ChildWorkflowOptionsForTest("ork-tasks", cp, child, childPlan)
	if opts.WorkflowID != "ork-child-1" || opts.TaskQueue != "ork-tasks" {
		t.Fatalf("id/queue = %q %q", opts.WorkflowID, opts.TaskQueue)
	}
	if opts.WorkflowIDReusePolicy.String() != "RejectDuplicate" && opts.WorkflowIDReusePolicy.String() != "WORKFLOW_ID_REUSE_POLICY_REJECT_DUPLICATE" {
		t.Fatalf("reuse policy = %v", opts.WorkflowIDReusePolicy)
	}
	if opts.ParentClosePolicy.String() != "RequestCancel" && opts.ParentClosePolicy.String() != "PARENT_CLOSE_POLICY_REQUEST_CANCEL" {
		t.Fatalf("parent close policy = %v", opts.ParentClosePolicy)
	}
	if !opts.WaitForCancellation || opts.RetryPolicy != nil || opts.WorkflowExecutionTimeout != 0 ||
		opts.WorkflowRunTimeout != 0 || opts.CronSchedule != "" {
		t.Fatalf("options = %+v", opts)
	}
	wantMemo := engine.RunStartMemo(child, childPlan)
	if fmt.Sprint(opts.Memo) != fmt.Sprint(wantMemo) || opts.Memo[engine.MemoOriginKind] != "parent-run" {
		t.Fatalf("memo = %v, want %v", opts.Memo, wantMemo)
	}
	wantAttrs := temporal.NewSearchAttributes(engine.RunStartSearchAttributes(child, childPlan, true, true)...)
	if opts.TypedSearchAttributes.Size() != wantAttrs.Size() || wantAttrs.Size() != 3 {
		t.Fatalf("search attributes = %d, want %d (3)", opts.TypedSearchAttributes.Size(), wantAttrs.Size())
	}
	id, ok := opts.TypedSearchAttributes.GetKeyword(task.SearchKeyOrkflowOriginID)
	if !ok || id != "ork-parent" {
		t.Fatalf("origin id attribute = %q %v", id, ok)
	}
}

func TestRunWorkflow_ParentCancelCancelsTheChildAndWaits(t *testing.T) {
	h := newComposeHarness(t, defaultBehavior)
	h.env.SetDetachedChildWait(false)
	parent, plan, _ := composedFixture(t, composedChildDocument)
	var sawCancel bool
	var childClosed time.Time
	h.mockChild(func(ctx workflow.Context, _ childStart) (*orkstackv1.Run, error) {
		err := workflow.Sleep(ctx, time.Hour)
		if !temporal.IsCanceledError(err) {
			return nil, err
		}
		sawCancel = true
		// The child's own grace: its containers get SIGTERM and take time.
		cleanup, _ := workflow.NewDisconnectedContext(ctx)
		_ = workflow.Sleep(cleanup, 30*time.Second)
		childClosed = workflow.Now(cleanup)
		return nil, err
	})
	h.env.RegisterDelayedCallback(h.env.CancelWorkflow, time.Minute)
	h.run("ork-parent", parent, payload(t, nil), plan)
	if !temporal.IsCanceledError(h.env.GetWorkflowError()) {
		t.Fatalf("workflow error = %v, want a cancellation", h.env.GetWorkflowError())
	}
	if !sawCancel {
		t.Fatal("cancelling the parent did not cancel its child")
	}
	run, _ := h.store.Get("ork-parent")
	row := taskRunOfRun(run, "release")
	if row.GetStatus() != orkstackv1.Status_STATUS_CANCELED || row.GetError().GetCode() != engine.ChildRunCanceledCode {
		t.Fatalf("release row = %v", row)
	}
	if run.GetStatus() != orkstackv1.Status_STATUS_CANCELED || run.GetError().GetCode() != "RUN_CANCELED" {
		t.Fatalf("run = %v %v", run.GetStatus(), run.GetError())
	}
	events := h.bus.Events("ork-parent", 0)
	terminal := events[len(events)-1]
	if terminal.GetType() != orkstackv1.EventType_EVENT_TYPE_RUN_CANCELED {
		t.Fatalf("last event = %v", terminal.GetType())
	}
	if terminal.GetTimestamp().AsTime().Before(childClosed) {
		t.Fatalf("the parent's terminal event (%v) precedes its child's close (%v)",
			terminal.GetTimestamp().AsTime(), childClosed)
	}
}

func TestRunWorkflow_SiblingFailureCancelsTheRunningChild(t *testing.T) {
	h := newComposeHarness(t, func(h *composeHarness, in *orkstackv1.ExecuteTaskInput) (*orkstackv1.ExecuteTaskResult, error) {
		if in.GetTaskId() == "other" {
			return nil, temporal.NewNonRetryableApplicationError("other exited 1", task.CategoryActivityFailed, nil)
		}
		return defaultBehavior(h, in)
	})
	h.env.SetDetachedChildWait(false)
	doc := strings.Replace(composedParentDocument, "  - id: announce\n", `  - id: other
    uses: shell
    needs: [build]
    with: { op: exec, command: ["false"] }
  - id: announce
`, 1)
	parent := compile(t, doc)
	for _, task := range parent.GetTasks() {
		if task.GetOrkflow() != nil {
			task.Orkflow.DefinitionDigest = childDigest
		}
	}
	_, plan, _ := composedFixture(t, composedChildDocument)
	var sawCancel bool
	h.mockChild(func(ctx workflow.Context, _ childStart) (*orkstackv1.Run, error) {
		err := workflow.Sleep(ctx, time.Hour)
		sawCancel = temporal.IsCanceledError(err)
		return nil, err
	})
	h.run("ork-parent", parent, payload(t, nil), plan)
	if h.env.GetWorkflowError() == nil {
		t.Fatal("the run succeeded")
	}
	if !sawCancel {
		t.Fatal("the running child was not cancelled when a sibling failed")
	}
	run, _ := h.store.Get("ork-parent")
	if row := taskRunOfRun(run, "release"); row.GetStatus() != orkstackv1.Status_STATUS_CANCELED ||
		row.GetError().GetCode() != engine.ChildRunCanceledCode {
		t.Fatalf("release row = %v", row)
	}
	if run.GetStatus() != orkstackv1.Status_STATUS_FAILED || !strings.Contains(run.GetError().GetMessage(), "other exited 1") {
		t.Fatalf("run = %v %v", run.GetStatus(), run.GetError())
	}
}

const composedOnlyDocument = `
name: composed-only
tasks:
  - id: release
    uses: orkflow:release-block@2.1.0
`

const mixedComposedDocument = `
name: mixed-composed
tasks:
  - id: crunch
    uses: shell
    pool: gpu
    with: {command: "echo crunch"}
  - id: release
    uses: orkflow:release-block@2.1.0
    needs: [crunch]
`

func TestPoolResolverPlan_SkipsComposedTasks(t *testing.T) {
	t.Run("a composed-only definition plans with empty routes and no probe", func(t *testing.T) {
		resolver := &engine.PoolResolver{Probe: failingProbe{t}}
		planned, plan, err := resolver.Plan(context.Background(), nil, compile(t, composedOnlyDocument), false)
		if err != nil {
			t.Fatalf("Plan: %v", err)
		}
		if len(plan.Routes) != 0 || planned.GetTasks()[0].GetPool() != "" {
			t.Fatalf("routes = %v, pool %q; want none", plan.Routes, planned.GetTasks()[0].GetPool())
		}
	})
	t.Run("a mixed definition routes only the container task", func(t *testing.T) {
		resolver := &engine.PoolResolver{Probe: everyPoolLive{}}
		planned, plan, err := resolver.Plan(context.Background(), nil, compile(t, mixedComposedDocument), false)
		if err != nil {
			t.Fatalf("Plan: %v", err)
		}
		if len(plan.Routes) != 1 || plan.Routes["gpu"] == "" || planned.GetTasks()[1].GetPool() != "" {
			t.Fatalf("routes = %v, composed pool %q", plan.Routes, planned.GetTasks()[1].GetPool())
		}
	})
}

func TestCheckAllowedPools_SkipsComposedTasks(t *testing.T) {
	ns := &orkstackv1.Namespace{Name: "payments", AllowedPools: []string{"batch"}}
	resolver := &engine.PoolResolver{}
	if err := resolver.CheckAllowedPools(ns, compile(t, composedOnlyDocument)); err != nil {
		t.Fatalf("CheckAllowedPools refused a composed-only orkflow: %v", err)
	}
}

const simulateComposedDocument = `
name: plan-composed
tasks:
  - id: build
    uses: shell
    with: {command: "echo"}
  - id: release
    uses: orkflow:release-block@2.1.0
    needs: [build]
  - id: announce
    uses: shell
    needs: [release]
    condition: '{{ eq .tasks.release.output.digest "go" }}'
    with: {command: "echo"}
`

func TestSimulate_ComposedTaskIsOnePlanTaskAndStubsItsOutputs(t *testing.T) {
	wf := compile(t, simulateComposedDocument)
	plan := simulate(t, wf, nil, assumedPayloads(t, map[string]map[string]any{"release": {"digest": "go"}}))
	release := planTaskOf(t, plan, "release")
	if release.GetOutcome() != orkstackv1.PlanOutcome_PLAN_OUTCOME_RUNS || release.GetWave() != 2 ||
		release.GetOrkflow().GetName() != "release-block" || release.GetOrkflow().GetVersion() != "2.1.0" {
		t.Fatalf("release = %v", release)
	}
	if planTaskOf(t, plan, "announce").GetOutcome() != orkstackv1.PlanOutcome_PLAN_OUTCOME_RUNS {
		t.Fatalf("announce did not read the stubbed output")
	}
	if len(plan.GetTasks()) != 3 {
		t.Fatalf("the plan recursed into the child: %d tasks", len(plan.GetTasks()))
	}
	unstubbed := simulate(t, wf, nil, nil)
	if got := planTaskOf(t, unstubbed, "announce"); got.GetOutcome() != orkstackv1.PlanOutcome_PLAN_OUTCOME_SKIPPED {
		t.Fatalf("announce without a stub = %v", got.GetOutcome())
	}
}

// TestSimulate_AgreesWithTheInterpreterOverAComposedTask is E1 over a
// composed task: the interpreter, whose mocked child returns the stub as its
// declared outputs, decides every task as the plan does.
func TestSimulate_AgreesWithTheInterpreterOverAComposedTask(t *testing.T) {
	for _, digest := range []string{"go", "stop"} {
		t.Run(digest, func(t *testing.T) {
			wf := compile(t, simulateComposedDocument)
			wf.Tasks[1].Orkflow.DefinitionDigest = childDigest
			plan := simulate(t, wf, nil, assumedPayloads(t, map[string]map[string]any{"release": {"digest": digest}}))

			h := newComposeHarness(t, defaultBehavior)
			_, runPlan, _ := composedFixture(t, composedChildDocument)
			h.mockChild(func(workflow.Context, childStart) (*orkstackv1.Run, error) {
				return childOutputs(t, map[string]any{"digest": digest, "url": "u"}), nil
			})
			h.run("ork-plan-composed", wf, payload(t, nil), runPlan)
			if err := h.env.GetWorkflowError(); err != nil {
				t.Fatalf("workflow error: %v", err)
			}
			run, _ := h.store.Get("ork-plan-composed")
			for _, task := range plan.GetTasks() {
				want := orkstackv1.Status_STATUS_SUCCEEDED
				if task.GetOutcome() == orkstackv1.PlanOutcome_PLAN_OUTCOME_SKIPPED {
					want = orkstackv1.Status_STATUS_SKIPPED
				}
				if got := statusOf(run, task.GetTaskId()); got != want {
					t.Errorf("%s planned %v, interpreter %v", task.GetTaskId(), task.GetOutcome(), got)
				}
			}
			if started := len(h.startedChildren()); started != 1 {
				t.Fatalf("RUNS for the composed task, but %d children started", started)
			}
		})
	}
}
