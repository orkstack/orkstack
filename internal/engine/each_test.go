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
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/workflow"
	"google.golang.org/protobuf/proto"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/internal/task"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// memValues is the in-memory store CountEachItems reads committed
// values.json objects from in a workflow test.
type memValues struct {
	mu      sync.Mutex
	objects map[string][]byte
}

func (m *memValues) Get(_ context.Context, uri string) (io.ReadCloser, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	data, ok := m.objects[uri]
	if !ok {
		return nil, fmt.Errorf("%s: %w", uri, artifacts.ErrNotFound)
	}
	return io.NopCloser(bytes.NewReader(data)), nil
}

func (m *memValues) put(uri string, data []byte) *orkstackv1.ArtifactRef {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.objects[uri] = append([]byte(nil), data...)
	sum := sha256.Sum256(data)
	return &orkstackv1.ArtifactRef{Name: "values.json", Uri: uri, Sha256: hex.EncodeToString(sum[:]), SizeBytes: int64(len(data))}
}

type published struct {
	event       *orkstackv1.Event
	snapshotURI string
}

// eachHarness is newHarness for fan-out tests: the task behavior sees the
// activity context, CountEachItems is registered over memValues, and every
// PublishEvent (event and snapshot URI), every count and every timer is
// recorded.
type eachHarness struct {
	env       *testsuite.TestWorkflowEnvironment
	bus       *engine.EventBus
	store     *engine.RunStore
	snapshots *testSnapshotStore
	values    *memValues

	mu        sync.Mutex
	calls     []*orkstackv1.ExecuteTaskInput
	published []published
	counts    int
	timers    []time.Duration
}

func newEachHarness(t *testing.T, behavior func(context.Context, *orkstackv1.ExecuteTaskInput) (*orkstackv1.ExecuteTaskResult, error)) *eachHarness {
	t.Helper()
	var suite testsuite.WorkflowTestSuite
	h := &eachHarness{
		env:       suite.NewTestWorkflowEnvironment(),
		bus:       engine.NewEventBus(),
		store:     engine.NewRunStore(),
		snapshots: newTestSnapshotStore(),
		values:    &memValues{objects: map[string][]byte{}},
	}
	h.env.SetTestTimeout(120 * time.Second)
	h.env.RegisterWorkflowWithOptions(engine.RunWorkflow, workflow.RegisterOptions{Name: engine.RunWorkflowType})
	h.env.RegisterActivityWithOptions(engine.NewSnapshotActivities(h.snapshots).WriteSnapshot,
		activity.RegisterOptions{Name: engine.ActivityWriteSnapshot})
	publish := engine.NewEventActivities(h.bus, h.store).WithSnapshotStore(h.snapshots).PublishEvent
	h.env.RegisterActivityWithOptions(func(ctx context.Context, event *orkstackv1.Event, uri, key string) error {
		h.mu.Lock()
		h.published = append(h.published, published{event: event, snapshotURI: uri})
		h.mu.Unlock()
		return publish(ctx, event, uri, key)
	}, activity.RegisterOptions{Name: engine.ActivityPublishEvent})
	count := engine.NewEachActivities(h.values).CountEachItems
	h.env.RegisterActivityWithOptions(func(ctx context.Context, ref *orkstackv1.ArtifactRef, path []string, over string, maxBytes int64) (int32, error) {
		h.mu.Lock()
		h.counts++
		h.mu.Unlock()
		return count(ctx, ref, path, over, maxBytes)
	}, activity.RegisterOptions{Name: engine.ActivityCountEachItems})
	h.env.RegisterActivityWithOptions(func(ctx context.Context, in *orkstackv1.ExecuteTaskInput) (*orkstackv1.ExecuteTaskResult, error) {
		h.mu.Lock()
		h.calls = append(h.calls, in)
		h.mu.Unlock()
		return behavior(ctx, in)
	}, activity.RegisterOptions{Name: task.ActivityExecuteTask})
	h.env.SetOnTimerScheduledListener(func(_ string, d time.Duration) {
		h.mu.Lock()
		h.timers = append(h.timers, d)
		h.mu.Unlock()
	})
	return h
}

func (h *eachHarness) run(t *testing.T, runID, document string, input map[string]any) *orkstackv1.Run {
	t.Helper()
	plan := defaultPlan()
	plan.CommitRoot = "mem://commits"
	h.env.ExecuteWorkflow(engine.RunWorkflow, runID, compile(t, document), payload(t, input), plan, (*orkstackv1.RunContinuation)(nil))
	if !h.env.IsWorkflowCompleted() {
		t.Fatal("workflow did not complete")
	}
	run, ok := h.store.Get(runID)
	if !ok {
		t.Fatalf("%s: run read model was not populated", runID)
	}
	return run
}

func (h *eachHarness) itemCalls(taskID string) []*orkstackv1.ExecuteTaskInput {
	h.mu.Lock()
	defer h.mu.Unlock()
	var out []*orkstackv1.ExecuteTaskInput
	for _, in := range h.calls {
		if in.GetTaskId() == taskID {
			out = append(out, in)
		}
	}
	return out
}

func (h *eachHarness) eventsFor(runID, taskID string) []*orkstackv1.Event {
	var out []*orkstackv1.Event
	for _, e := range h.bus.Events(runID, 0) {
		if e.GetTaskId() == taskID {
			out = append(out, e)
		}
	}
	return out
}

func (h *eachHarness) yieldTimers() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	n := 0
	for _, d := range h.timers {
		if d == time.Millisecond {
			n++
		}
	}
	return n
}

// listTask answers `list` with parts committed in memValues, and every item
// with success.
func listThenItems(h *eachHarness, parts string) func(context.Context, *orkstackv1.ExecuteTaskInput) (*orkstackv1.ExecuteTaskResult, error) {
	return func(_ context.Context, in *orkstackv1.ExecuteTaskInput) (*orkstackv1.ExecuteTaskResult, error) {
		if in.GetTaskId() == "list" {
			data := []byte(parts)
			return &orkstackv1.ExecuteTaskResult{TaskId: "list", ValuesJson: data, ValuesInlined: true,
				ValuesRef: h.values.put("mem://values/list", data)}, nil
		}
		return itemResult(in), nil
	}
}

func itemResult(in *orkstackv1.ExecuteTaskInput) *orkstackv1.ExecuteTaskResult {
	return &orkstackv1.ExecuteTaskResult{
		TaskId:    in.GetTaskId(),
		Attempt:   1,
		OrkerId:   "orker-1",
		ValuesRef: &orkstackv1.ArtifactRef{Uri: fmt.Sprintf("mem://values/%s/%d", in.GetTaskId(), in.GetEach().GetIndex())},
		Commit:    &orkstackv1.CommitRef{Uri: in.GetCommitUri()},
	}
}

func partsJSON(n int) string {
	parts := make([]string, n)
	for i := range parts {
		parts[i] = fmt.Sprintf(`{"name":"p%d"}`, i)
	}
	return `{"parts":[` + strings.Join(parts, ",") + `]}`
}

const eachTaskDocument = `
name: fan
tasks:
  - id: list
    uses: shell
    with: {op: exec}
  - id: convert
    uses: shell
    needs: [list]
    each: {over: "{{ .tasks.list.output.parts }}", as: part, max_items: 50%s}
    with: {op: exec, name: "{{ .each.part.name }}"}
  - id: merge
    uses: shell
    needs: [convert]
    with: {op: exec}
`

func TestEachArrayLength(t *testing.T) {
	var root any
	if err := json.Unmarshal([]byte(`{"a":{"b":[1,2,3]},"n":null,"o":{},"s":"x","num":4,"e":[]}`), &root); err != nil {
		t.Fatal(err)
	}
	before, _ := json.Marshal(root)
	if n, err := engine.EachArrayLength(root, []string{"a", "b"}, "o"); err != nil || n != 3 {
		t.Fatalf("nested = %d, %v", n, err)
	}
	if n, err := engine.EachArrayLength(root, []string{"e"}, "o"); err != nil || n != 0 {
		t.Fatalf("empty = %d, %v", n, err)
	}
	if _, err := engine.EachArrayLength(root, []string{"missing"}, "o"); err == nil || err.Code != engine.EachOverMissingCode || !strings.Contains(err.Message, `"missing"`) {
		t.Fatalf("missing = %v", err)
	}
	if _, err := engine.EachArrayLength(root, []string{"s", "x"}, "o"); err == nil || err.Code != engine.EachOverMissingCode || !strings.Contains(err.Message, "a string") {
		t.Fatalf("non-object on the way = %v", err)
	}
	for key, kind := range map[string]string{"n": "null", "o": "an object", "s": "a string", "num": "a number"} {
		if _, err := engine.EachArrayLength(root, []string{key}, "o"); err == nil || err.Code != engine.EachOverNotArrayCode || !strings.Contains(err.Message, kind) {
			t.Fatalf("%s = %v, want NOT_ARRAY naming %s", key, err, kind)
		}
	}
	// json.Number elements count like any other.
	dec := json.NewDecoder(strings.NewReader(`{"x":[12345678901234567890, 1.5]}`))
	dec.UseNumber()
	var numbers any
	_ = dec.Decode(&numbers)
	if n, err := engine.EachArrayLength(numbers, []string{"x"}, "o"); err != nil || n != 2 {
		t.Fatalf("json.Number = %d, %v", n, err)
	}
	after, _ := json.Marshal(root)
	if !bytes.Equal(before, after) {
		t.Fatal("EachArrayLength mutated its root")
	}
}

func TestEachActivities_CountEachItems(t *testing.T) {
	values := &memValues{objects: map[string][]byte{}}
	activities := engine.NewEachActivities(values)
	ref := values.put("mem://v", []byte(`{"parts":[1,2,3]}`))
	n, err := activities.CountEachItems(context.Background(), ref, []string{"parts"}, "o", 1024)
	if err != nil || n != 3 {
		t.Fatalf("count = %d, %v", n, err)
	}
	reasonOf := func(err error) (string, bool) {
		var appErr *temporal.ApplicationError
		if !errors.As(err, &appErr) {
			return "", false
		}
		var detail *orkstackv1.ActivityFailureDetail
		if appErr.Details(&detail) != nil {
			return "", appErr.NonRetryable()
		}
		return detail.GetReasonCode(), appErr.NonRetryable()
	}
	// A mutated object is non-retryable EACH_EXPANSION_FAILED.
	values.objects["mem://v"] = []byte(`{"parts":[1,2,3,4]}`)
	_, err = activities.CountEachItems(context.Background(), ref, []string{"parts"}, "o", 1024)
	if code, nonRetryable := reasonOf(err); code != engine.EachExpansionFailedCode || !nonRetryable {
		t.Fatalf("mutated object = %v (%s)", err, code)
	}
	// Bytes over the bound are refused.
	big := values.put("mem://big", []byte(`{"parts":[`+strings.Repeat("1,", 100)+`1]}`))
	if _, err := activities.CountEachItems(context.Background(), big, []string{"parts"}, "o", 16); err == nil {
		t.Fatal("bytes over the bound were counted")
	}
	// Contract errors carry the reason-code detail.
	notArray := values.put("mem://na", []byte(`{"parts":{"a":1}}`))
	_, err = activities.CountEachItems(context.Background(), notArray, []string{"parts"}, "o", 1024)
	if code, nonRetryable := reasonOf(err); code != engine.EachOverNotArrayCode || !nonRetryable {
		t.Fatalf("not an array = %v (%s)", err, code)
	}
	missing := values.put("mem://m", []byte(`{"other":[]}`))
	_, err = activities.CountEachItems(context.Background(), missing, []string{"parts"}, "o", 1024)
	if code, _ := reasonOf(err); code != engine.EachOverMissingCode {
		t.Fatalf("missing key = %v (%s)", err, code)
	}
	// A read error is returned as-is: retried by the local activity's policy.
	_, err = activities.CountEachItems(context.Background(), &orkstackv1.ArtifactRef{Uri: "mem://gone"}, nil, "o", 1024)
	if _, nonRetryable := reasonOf(err); err == nil || nonRetryable {
		t.Fatalf("a read error = %v, want a retryable error", err)
	}
}

func TestRunWorkflow_EachOverAnUpstreamOutputRunsOneActivityPerItem(t *testing.T) {
	var h *eachHarness
	h = newEachHarness(t, func(ctx context.Context, in *orkstackv1.ExecuteTaskInput) (*orkstackv1.ExecuteTaskResult, error) {
		return listThenItems(h, partsJSON(5))(ctx, in)
	})
	run := h.run(t, "run-each-5", fmt.Sprintf(eachTaskDocument, ""), nil)
	if run.GetStatus() != orkstackv1.Status_STATUS_SUCCEEDED {
		t.Fatalf("run = %v %v", run.GetStatus(), run.GetError())
	}
	if h.counts != 1 {
		t.Fatalf("CountEachItems ran %d times, want 1", h.counts)
	}
	calls := h.itemCalls("convert")
	if len(calls) != 5 {
		t.Fatalf("%d convert activities, want 5", len(calls))
	}
	seen := map[int32]bool{}
	for _, in := range calls {
		e := in.GetEach()
		want := &orkstackv1.EachContext{Index: e.GetIndex(), Count: 5, As: "part",
			Source: &orkstackv1.EachSource{TaskId: "list", Path: []string{"parts"}}}
		if !proto.Equal(e, want) {
			t.Fatalf("each = %v, want %v", e, want)
		}
		seen[e.GetIndex()] = true
		if in.GetIdempotencyKey() != fmt.Sprintf("default/run-each-5/convert/%d", e.GetIndex()) {
			t.Fatalf("idempotency key = %q", in.GetIdempotencyKey())
		}
		if !strings.HasSuffix(in.GetCommitUri(), fmt.Sprintf("/convert/%05d/commit.json", e.GetIndex())) {
			t.Fatalf("commit uri = %q", in.GetCommitUri())
		}
	}
	if len(seen) != 5 {
		t.Fatalf("items dispatched = %v", seen)
	}
	types := eventTypes(h.eventsFor("run-each-5", "convert"))
	want := []string{"TASK_SCHEDULED", "TASK_STARTED", "TASK_LOG", "TASK_LOG", "TASK_LOG", "TASK_LOG", "TASK_LOG", "TASK_SUCCEEDED"}
	if fmt.Sprint(types) != fmt.Sprint(want) {
		t.Fatalf("convert events = %v, want %v", types, want)
	}
	for _, e := range h.eventsFor("run-each-5", "convert") {
		if (e.GetType() == orkstackv1.EventType_EVENT_TYPE_TASK_LOG) != (e.Item != nil) {
			t.Fatalf("event %v item = %v: only the item TASK_LOGs carry an item", e.GetType(), e.Item)
		}
	}
	node := taskRunOf(t, run, "convert")
	if !node.GetEach().GetExpanded() || node.GetEach().GetCount() != 5 || len(node.GetItems()) != 5 {
		t.Fatalf("node each = %v, %d items", node.GetEach(), len(node.GetItems()))
	}
	if node.GetScheduledAt() == nil || node.GetAttempt() != 0 || node.GetOrkerId() != "" || node.GetCommit() != nil || node.GetValuesRef() != nil || node.Item != nil {
		t.Fatalf("node row carries an item's fields: %v", node)
	}
	for i, item := range node.GetItems() {
		if item.GetItem() != int32(i) || item.Item == nil || item.GetStatus() != orkstackv1.Status_STATUS_SUCCEEDED ||
			item.GetOrkerId() != "orker-1" || item.GetCommit() == nil || item.GetOutput() != nil || item.GetEach() != nil || item.GetPool() == "" {
			t.Fatalf("item %d = %v", i, item)
		}
	}
	if taskRunOf(t, run, "list").GetEach() != nil {
		t.Fatal("a task that is not a fan-out carries each")
	}
}

const eachInputDocument = `
name: fan-input
inputs:
  regions: {type: array}
tasks:
  - id: convert
    uses: shell
    each: {over: "{{ .inputs.regions }}", max_items: %d%s}
    with: {op: exec, r: "{{ .each.item }}"%s}
`

func TestRunWorkflow_EachOverAnInputCountsWithoutAnActivity(t *testing.T) {
	h := newEachHarness(t, func(_ context.Context, in *orkstackv1.ExecuteTaskInput) (*orkstackv1.ExecuteTaskResult, error) {
		return itemResult(in), nil
	})
	run := h.run(t, "run-each-input", fmt.Sprintf(eachInputDocument, 10, "", ""), map[string]any{"regions": []any{"eu", "us", "ap"}})
	if run.GetStatus() != orkstackv1.Status_STATUS_SUCCEEDED {
		t.Fatalf("run = %v %v", run.GetStatus(), run.GetError())
	}
	if h.counts != 0 {
		t.Fatalf("an input source ran CountEachItems %d times", h.counts)
	}
	if len(h.itemCalls("convert")) != 3 {
		t.Fatalf("%d items, want 3", len(h.itemCalls("convert")))
	}
}

func TestRunWorkflow_EachRespectsMaxParallel(t *testing.T) {
	var h *eachHarness
	h = newEachHarness(t, func(ctx context.Context, in *orkstackv1.ExecuteTaskInput) (*orkstackv1.ExecuteTaskResult, error) {
		return listThenItems(h, partsJSON(6))(ctx, in)
	})
	run := h.run(t, "run-each-mp", fmt.Sprintf(eachTaskDocument, ", max_parallel: 2"), nil)
	if run.GetStatus() != orkstackv1.Status_STATUS_SUCCEEDED {
		t.Fatalf("run = %v %v", run.GetStatus(), run.GetError())
	}
	// Every snapshot the run wrote: never more than 2 items RUNNING, and the
	// items that left PENDING are always a prefix — dispatch is ascending.
	checked := 0
	for _, p := range h.published {
		if p.snapshotURI == "" {
			continue
		}
		data, err := h.snapshots.ReadSnapshot(context.Background(), p.snapshotURI)
		if err != nil {
			t.Fatal(err)
		}
		snap := &orkstackv1.Run{}
		if err := proto.Unmarshal(data, snap); err != nil {
			t.Fatal(err)
		}
		node := taskRunOf(t, snap, "convert")
		running, pendingSeen := 0, false
		for _, item := range node.GetItems() {
			switch item.GetStatus() {
			case orkstackv1.Status_STATUS_RUNNING:
				running++
				if pendingSeen {
					t.Fatalf("item %d dispatched before a lower index", item.GetItem())
				}
			case orkstackv1.Status_STATUS_PENDING:
				pendingSeen = true
			default:
				if pendingSeen {
					t.Fatalf("item %d settled before a lower index was dispatched", item.GetItem())
				}
			}
		}
		if running > 2 {
			t.Fatalf("snapshot %s has %d items running, max_parallel is 2", p.snapshotURI, running)
		}
		checked++
	}
	if checked == 0 {
		t.Fatal("no snapshot was checked")
	}
	if !strings.Contains(h.eventsFor("run-each-mp", "convert")[0].GetMessage(), "at most 2 at a time") {
		t.Fatalf("TASK_SCHEDULED = %q", h.eventsFor("run-each-mp", "convert")[0].GetMessage())
	}
}

func TestRunWorkflow_EachDispatchesInBoundedBatches(t *testing.T) {
	regions := make([]any, 40)
	for i := range regions {
		regions[i] = float64(i)
	}
	// max_items 128: S = 128, an item input bound of 128 KiB, and a batch of
	// 2 MiB / 128 KiB = 16. 40 items: 16, yield, 16, yield, 8.
	h := newEachHarness(t, func(_ context.Context, in *orkstackv1.ExecuteTaskInput) (*orkstackv1.ExecuteTaskResult, error) {
		return itemResult(in), nil
	})
	run := h.run(t, "run-each-batches", fmt.Sprintf(eachInputDocument, 128, "", ""), map[string]any{"regions": regions})
	if run.GetStatus() != orkstackv1.Status_STATUS_SUCCEEDED || len(h.itemCalls("convert")) != 40 {
		t.Fatalf("run = %v, %d items", run.GetStatus(), len(h.itemCalls("convert")))
	}
	if got := h.yieldTimers(); got != 2 {
		t.Fatalf("%d yield timers, want 2 (16 | 16 | 8)", got)
	}
	bounded := newEachHarness(t, func(_ context.Context, in *orkstackv1.ExecuteTaskInput) (*orkstackv1.ExecuteTaskResult, error) {
		return itemResult(in), nil
	})
	run = bounded.run(t, "run-each-batches-mp", fmt.Sprintf(eachInputDocument, 128, ", max_parallel: 10", ""), map[string]any{"regions": regions})
	if run.GetStatus() != orkstackv1.Status_STATUS_SUCCEEDED {
		t.Fatalf("run = %v", run.GetStatus())
	}
	if got := bounded.yieldTimers(); got != 0 {
		t.Fatalf("max_parallel 10 scheduled %d yield timers, want 0", got)
	}
}

func TestRunWorkflow_EachZeroItemsSucceedsWithoutAnActivity(t *testing.T) {
	var h *eachHarness
	h = newEachHarness(t, func(ctx context.Context, in *orkstackv1.ExecuteTaskInput) (*orkstackv1.ExecuteTaskResult, error) {
		return listThenItems(h, `{"parts":[]}`)(ctx, in)
	})
	run := h.run(t, "run-each-0", fmt.Sprintf(eachTaskDocument, ""), nil)
	if run.GetStatus() != orkstackv1.Status_STATUS_SUCCEEDED {
		t.Fatalf("run = %v %v", run.GetStatus(), run.GetError())
	}
	if len(h.itemCalls("convert")) != 0 {
		t.Fatal("a fan-out of 0 items dispatched an activity")
	}
	node := taskRunOf(t, run, "convert")
	if !node.GetEach().GetExpanded() || node.GetEach().GetCount() != 0 || node.GetScheduledAt() != nil || node.GetFinishedAt() == nil {
		t.Fatalf("node = %v", node)
	}
	if types := eventTypes(h.eventsFor("run-each-0", "convert")); fmt.Sprint(types) != "[TASK_SUCCEEDED]" {
		t.Fatalf("events = %v", types)
	}
	merge, ok := firstCall(h, "merge")
	if !ok {
		t.Fatal("merge never ran")
	}
	ref := upstreamRefOf(merge, "convert")
	if ref == nil || ref.GetEach() == nil || ref.GetEach().GetCount() != 0 {
		t.Fatalf("merge's convert ref = %v, want EachUpstream{count: 0}", ref)
	}
}

func firstCall(h *eachHarness, taskID string) (*orkstackv1.ExecuteTaskInput, bool) {
	calls := h.itemCalls(taskID)
	if len(calls) == 0 {
		return nil, false
	}
	return calls[0], true
}

func upstreamRefOf(in *orkstackv1.ExecuteTaskInput, taskID string) *orkstackv1.UpstreamRef {
	for _, ref := range in.GetUpstream() {
		if ref.GetTaskId() == taskID {
			return ref
		}
	}
	return nil
}

func TestRunWorkflow_EachExpansionRefusalsFailBeforeDispatch(t *testing.T) {
	for name, tc := range map[string]struct {
		values string
		code   string
		extra  string // appended to the fan-out's each: block, as eachTaskDocument's %s
	}{
		"TooManyItems":    {partsJSON(51), engine.EachTooManyItemsCode, ""},
		"OverNotAnArray":  {`{"parts":{"a":1}}`, engine.EachOverNotArrayCode, ""},
		"OverMissingKey":  {`{"other":[]}`, engine.EachOverMissingCode, ""},
		"OverNullIsNotOK": {`{"parts":null}`, engine.EachOverNotArrayCode, ""},
		// A hand-built plan with no route for the fan-out's pool: unreachable
		// through SubmitRun, whose plan is resolved from the same definition,
		// but the node must still fail with the routing reason and no item,
		// never read expanded and then SUCCEEDED with none.
		"UnroutablePool": {partsJSON(3), "INVALID_POOL", ""},
	} {
		t.Run(name, func(t *testing.T) {
			var h *eachHarness
			h = newEachHarness(t, func(ctx context.Context, in *orkstackv1.ExecuteTaskInput) (*orkstackv1.ExecuteTaskResult, error) {
				return listThenItems(h, tc.values)(ctx, in)
			})
			doc := fmt.Sprintf(eachTaskDocument, tc.extra)
			if tc.code == "INVALID_POOL" {
				doc = strings.Replace(doc, "    needs: [list]\n", "    needs: [list]\n    pool: nosuch\n", 1)
				if !strings.Contains(doc, "pool: nosuch") {
					t.Fatal("the document did not take the pool")
				}
			}
			run := h.run(t, "run-each-refused", doc, nil)
			if run.GetStatus() != orkstackv1.Status_STATUS_FAILED || run.GetError().GetCode() != "RUN_FAILED" {
				t.Fatalf("run = %v %v", run.GetStatus(), run.GetError())
			}
			node := taskRunOf(t, run, "convert")
			if node.GetError().GetCode() != tc.code ||
				node.GetError().GetCategory() != orkstackv1.FailureCategory_FAILURE_CATEGORY_CONTRACT_VIOLATION ||
				node.GetError().GetRetryable() || node.GetEach().GetExpanded() || len(node.GetItems()) != 0 {
				t.Fatalf("node = %v", node)
			}
			if len(h.itemCalls("convert")) != 0 {
				t.Fatal("a refused expansion dispatched an item")
			}
			if run.GetError().GetMessage() != node.GetError().GetMessage() {
				t.Fatalf("run message %q is not the node's %q", run.GetError().GetMessage(), node.GetError().GetMessage())
			}
			if node.GetStatus() != orkstackv1.Status_STATUS_FAILED {
				t.Fatalf("node status = %v, want FAILED", node.GetStatus())
			}
			if types := eventTypes(h.eventsFor("run-each-refused", "convert")); fmt.Sprint(types) != "[TASK_FAILED RUN_FAILED]" {
				t.Fatalf("convert events = %v, want TASK_FAILED then the RUN_FAILED naming it", types)
			}
			if tc.code == "INVALID_POOL" && !strings.Contains(node.GetError().GetMessage(), `pool "nosuch"`) {
				t.Fatalf("routing refusal = %q, want it to name the pool", node.GetError().GetMessage())
			}
			if tc.code == engine.EachTooManyItemsCode && !strings.Contains(node.GetError().GetMessage(),
				"each.over resolved to 51 items, over max_items 50 declared at tasks[1].each.max_items") {
				t.Fatalf("too many = %q", node.GetError().GetMessage())
			}
		})
	}
}

// Named aliases of the table above, so `go test -run` finds each scenario by
// name.
func TestRunWorkflow_EachTooManyItemsFailsBeforeDispatch(t *testing.T) {
	TestRunWorkflow_EachExpansionRefusalsFailBeforeDispatch(t)
}

func TestRunWorkflow_EachItemFailureFailsTheNodeAndCancelsTheRest(t *testing.T) {
	var h *eachHarness
	var started sync.WaitGroup
	started.Add(2)
	h = newEachHarness(t, func(ctx context.Context, in *orkstackv1.ExecuteTaskInput) (*orkstackv1.ExecuteTaskResult, error) {
		if in.GetTaskId() == "list" {
			return listThenItems(h, partsJSON(6))(ctx, in)
		}
		switch in.GetEach().GetIndex() {
		case 0, 1:
			started.Done()
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(30 * time.Second):
				return nil, errors.New("never canceled")
			}
		case 2:
			started.Wait()
			return nil, temporal.NewNonRetryableApplicationError("item exploded", task.CategoryActivityFailed, nil,
				&orkstackv1.ActivityFailureDetail{ReasonCode: "CONTAINER_EXIT_NONZERO",
					Category: orkstackv1.FailureCategory_FAILURE_CATEGORY_ACTIVITY_FAILED, Attempt: 1})
		}
		return nil, fmt.Errorf("item %d was dispatched past max_parallel", in.GetEach().GetIndex())
	})
	run := h.run(t, "run-each-fail", fmt.Sprintf(eachTaskDocument, ", max_parallel: 3"), nil)
	if run.GetStatus() != orkstackv1.Status_STATUS_FAILED {
		t.Fatalf("run = %v", run.GetStatus())
	}
	node := taskRunOf(t, run, "convert")
	if node.GetStatus() != orkstackv1.Status_STATUS_FAILED || node.GetError().GetCode() != "CONTAINER_EXIT_NONZERO" ||
		!strings.HasPrefix(node.GetError().GetMessage(), "item 2 of 6 failed:") ||
		node.GetError().GetCategory() != orkstackv1.FailureCategory_FAILURE_CATEGORY_ACTIVITY_FAILED {
		t.Fatalf("node error = %v", node.GetError())
	}
	if !strings.HasPrefix(run.GetError().GetMessage(), "item 2 of 6 failed:") {
		t.Fatalf("run error = %v", run.GetError())
	}
	items := node.GetItems()
	for _, i := range []int{0, 1} {
		if items[i].GetStatus() != orkstackv1.Status_STATUS_CANCELED || items[i].GetScheduledAt() == nil {
			t.Fatalf("in-flight item %d = %v", i, items[i])
		}
	}
	for _, i := range []int{3, 4, 5} {
		if items[i].GetStatus() != orkstackv1.Status_STATUS_CANCELED || items[i].GetScheduledAt() != nil || items[i].GetStartedAt() != nil {
			t.Fatalf("never-started item %d = %v", i, items[i])
		}
	}
	itemEvents := map[int32]bool{}
	summaries := 0
	for _, e := range h.eventsFor("run-each-fail", "convert") {
		if e.Item != nil {
			itemEvents[e.GetItem()] = true
		} else if e.GetType() == orkstackv1.EventType_EVENT_TYPE_TASK_LOG {
			summaries++
			// Item 2 failed; 0 and 1 were in flight (max_parallel 3); 3, 4
			// and 5 were never dispatched. The node is FAILED, so the verb
			// is "failed".
			const want = "task failed: run ended with 5 of 6 items unfinished (2 were running, 3 never started)"
			if e.GetMessage() != want || e.GetStatus() != orkstackv1.Status_STATUS_FAILED {
				t.Fatalf("summary = %q (%v), want %q (FAILED)", e.GetMessage(), e.GetStatus(), want)
			}
		}
	}
	for _, i := range []int32{0, 1, 2} {
		if !itemEvents[i] {
			t.Fatalf("item %d has no event (%v)", i, itemEvents)
		}
	}
	for _, i := range []int32{3, 4, 5} {
		if itemEvents[i] {
			t.Fatalf("never-started item %d has an event", i)
		}
	}
	if summaries != 1 {
		t.Fatalf("%d node summaries, want 1", summaries)
	}
	if merge := taskRunOf(t, run, "merge"); merge.GetStatus() != orkstackv1.Status_STATUS_CANCELED {
		t.Fatalf("merge = %v", merge.GetStatus())
	}
}

func TestRunWorkflow_EachItemRetriesAlone(t *testing.T) {
	var h *eachHarness
	var mu sync.Mutex
	attempts := map[int32]int{}
	h = newEachHarness(t, func(ctx context.Context, in *orkstackv1.ExecuteTaskInput) (*orkstackv1.ExecuteTaskResult, error) {
		if in.GetTaskId() == "list" {
			return listThenItems(h, partsJSON(5))(ctx, in)
		}
		if in.GetEach() == nil {
			return itemResult(in), nil
		}
		mu.Lock()
		attempts[in.GetEach().GetIndex()]++
		n := attempts[in.GetEach().GetIndex()]
		mu.Unlock()
		if in.GetEach().GetIndex() == 3 && n == 1 {
			return nil, temporal.NewApplicationError("transient s3 5xx", task.CategoryInfrastructureError)
		}
		result := itemResult(in)
		result.Attempt = int32(n)
		return result, nil
	})
	doc := strings.Replace(fmt.Sprintf(eachTaskDocument, ""), "    with: {op: exec, name:", "    retry: {maximum_attempts: 3}\n    with: {op: exec, name:", 1)
	run := h.run(t, "run-each-retry", doc, nil)
	if run.GetStatus() != orkstackv1.Status_STATUS_SUCCEEDED {
		t.Fatalf("run = %v %v", run.GetStatus(), run.GetError())
	}
	for i := int32(0); i < 5; i++ {
		want := 1
		if i == 3 {
			want = 2
		}
		if attempts[i] != want {
			t.Fatalf("item %d ran %d times, want %d (attempts %v)", i, attempts[i], want, attempts)
		}
	}
	if got := taskRunOf(t, run, "convert").GetItems()[3].GetAttempt(); got != 2 {
		t.Fatalf("item 3 attempt = %d", got)
	}
}

func TestRunWorkflow_DirectDependentGetsOneEachUpstreamRef(t *testing.T) {
	var h *eachHarness
	h = newEachHarness(t, func(ctx context.Context, in *orkstackv1.ExecuteTaskInput) (*orkstackv1.ExecuteTaskResult, error) {
		return listThenItems(h, partsJSON(5))(ctx, in)
	})
	doc := fmt.Sprintf(eachTaskDocument, "") + `  - id: report
    uses: shell
    needs: [merge]
    with: {op: exec}
`
	run := h.run(t, "run-each-fanin", doc, nil)
	if run.GetStatus() != orkstackv1.Status_STATUS_SUCCEEDED {
		t.Fatalf("run = %v %v", run.GetStatus(), run.GetError())
	}
	merge, _ := firstCall(h, "merge")
	ref := upstreamRefOf(merge, "convert")
	want := &orkstackv1.UpstreamRef{TaskId: "convert", Each: &orkstackv1.EachUpstream{Count: 5, CommitRoot: "mem://commits"}}
	if !proto.Equal(ref, want) {
		t.Fatalf("merge's convert ref = %v, want %v", ref, want)
	}
	n := 0
	for _, r := range merge.GetUpstream() {
		if r.GetTaskId() == "convert" {
			n++
		}
	}
	if n != 1 {
		t.Fatalf("merge carries %d refs for convert", n)
	}
	report, _ := firstCall(h, "report")
	if upstreamRefOf(report, "convert") != nil {
		t.Fatal("a task two hops down received a ref for the fan-out")
	}
	if upstreamRefOf(report, "list") == nil {
		t.Fatal("the closure rule for ordinary tasks changed")
	}
}

func TestRunWorkflow_EachItemInputBoundIsShared(t *testing.T) {
	big := strings.Repeat("x", 20000)
	for _, tc := range []struct {
		maxItems int
		fails    bool
	}{{10, false}, {1000, true}} {
		h := newEachHarness(t, func(_ context.Context, in *orkstackv1.ExecuteTaskInput) (*orkstackv1.ExecuteTaskResult, error) {
			return itemResult(in), nil
		})
		run := h.run(t, "run-each-bound", fmt.Sprintf(eachInputDocument, tc.maxItems, "", `, big: "`+big+`"`), map[string]any{"regions": []any{"eu"}})
		if !tc.fails {
			if run.GetStatus() != orkstackv1.Status_STATUS_SUCCEEDED {
				t.Fatalf("total %d: run = %v %v", tc.maxItems, run.GetStatus(), run.GetError())
			}
			continue
		}
		node := taskRunOf(t, run, "convert")
		if run.GetStatus() != orkstackv1.Status_STATUS_FAILED || node.GetError().GetCode() != task.ReasonInputTooLarge {
			t.Fatalf("total %d: run = %v, node = %v", tc.maxItems, run.GetStatus(), node.GetError())
		}
		for _, want := range []string{"item 0 of 1 failed", "16777-byte per-item limit", "declare 1000 items", "share a 16 MiB history budget"} {
			if !strings.Contains(node.GetError().GetMessage(), want) {
				t.Fatalf("node message %q lacks %q", node.GetError().GetMessage(), want)
			}
		}
		if len(h.itemCalls("convert")) != 0 {
			t.Fatal("an over-bound item reached an Orker")
		}
	}
}

// An item that settles and frees a slot whose refill the engine refuses
// (INPUT_TOO_LARGE) still gets its one TASK_LOG event:
// every item the node records SUCCEEDED has a SUCCEEDED item event. The
// with: value is sized so items 0-9 sit exactly at the shared per-item bound
// and item 10, whose idempotency key is one digit longer, is one byte over,
// so the refusal can only happen on a refill.
func TestRunWorkflow_EachRefusedRefillStillEmitsTheSettledItem(t *testing.T) {
	const runID = "run-each-refill"
	const limit = 16777 // 16 MiB / 1000 declared items, under the plan's values limit
	regions := make([]any, 12)
	for i := range regions {
		regions[i] = "r"
	}
	document := func(bigLen int) string {
		return fmt.Sprintf(eachInputDocument, 1000, ", max_parallel: 10", `, big: "`+strings.Repeat("x", bigLen)+`"`)
	}

	// Calibrate: the size of item 9's input with a known with: value.
	const probe = 15000
	calibrate := newEachHarness(t, func(_ context.Context, in *orkstackv1.ExecuteTaskInput) (*orkstackv1.ExecuteTaskResult, error) {
		return itemResult(in), nil
	})
	if run := calibrate.run(t, runID, document(probe), map[string]any{"regions": regions}); run.GetStatus() != orkstackv1.Status_STATUS_SUCCEEDED {
		t.Fatalf("calibration run = %v %v", run.GetStatus(), run.GetError())
	}
	var size9 int64
	for _, in := range calibrate.itemCalls("convert") {
		if in.GetEach().GetIndex() == 9 {
			n, err := task.InputSize(in)
			if err != nil {
				t.Fatal(err)
			}
			size9 = n
		}
	}
	if size9 == 0 || size9 > limit {
		t.Fatalf("calibration: item 9's input is %d bytes", size9)
	}

	// The first max_parallel (10) items are dispatched together; item 10's
	// refill is refused when the first of them settles, and fail-fast then
	// cancels the rest. An item cancelled before its activity goroutine ran
	// would never reach this mock, so no item settles until all ten have
	// arrived: the count below then measures dispatch, not goroutine
	// scheduling. The deadline only stops a regression from hanging the test;
	// it then fails on the count.
	var (
		arrivedMu sync.Mutex
		arrived   int
	)
	allArrived := make(chan struct{})
	h := newEachHarness(t, func(ctx context.Context, in *orkstackv1.ExecuteTaskInput) (*orkstackv1.ExecuteTaskResult, error) {
		arrivedMu.Lock()
		arrived++
		if arrived == 10 {
			close(allArrived)
		}
		arrivedMu.Unlock()
		select {
		case <-allArrived:
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(10 * time.Second):
		}
		return itemResult(in), nil
	})
	run := h.run(t, runID, document(probe+int(limit-size9)), map[string]any{"regions": regions})
	node := taskRunOf(t, run, "convert")
	if run.GetStatus() != orkstackv1.Status_STATUS_FAILED || node.GetError().GetCode() != task.ReasonInputTooLarge ||
		!strings.HasPrefix(node.GetError().GetMessage(), "item 10 of 12 failed") {
		t.Fatalf("run = %v, node = %v", run.GetStatus(), node.GetError())
	}
	if got := len(h.itemCalls("convert")); got != 10 {
		t.Fatalf("%d items reached an Orker, want the first 10", got)
	}
	itemEvents := map[int32]orkstackv1.Status{}
	for _, e := range h.eventsFor(runID, "convert") {
		if e.Item != nil {
			if _, dup := itemEvents[e.GetItem()]; dup {
				t.Fatalf("item %d has two events", e.GetItem())
			}
			itemEvents[e.GetItem()] = e.GetStatus()
		}
	}
	succeeded := 0
	for _, item := range node.GetItems() {
		if item.GetStatus() != orkstackv1.Status_STATUS_SUCCEEDED {
			continue
		}
		succeeded++
		if itemEvents[item.GetItem()] != orkstackv1.Status_STATUS_SUCCEEDED {
			t.Fatalf("item %d is SUCCEEDED with no SUCCEEDED event (events %v)", item.GetItem(), itemEvents)
		}
	}
	if succeeded == 0 {
		t.Fatal("no item settled before the refusal")
	}
	if itemEvents[10] != orkstackv1.Status_STATUS_FAILED {
		t.Fatalf("refused item 10 event = %v", itemEvents[10])
	}
}

func TestRunWorkflow_EachItemArtifactLimitIsShared(t *testing.T) {
	for _, tc := range []struct {
		maxItems int
		want     int32
	}{{10, 256}, {1000, 16}} {
		h := newEachHarness(t, func(_ context.Context, in *orkstackv1.ExecuteTaskInput) (*orkstackv1.ExecuteTaskResult, error) {
			return itemResult(in), nil
		})
		h.run(t, "run-each-artifacts", fmt.Sprintf(eachInputDocument, tc.maxItems, "", ""), map[string]any{"regions": []any{"eu", "us"}})
		for _, in := range h.itemCalls("convert") {
			if got := in.GetLimits().GetArtifactMaxCount(); got != tc.want {
				t.Fatalf("total %d: artifact_max_count = %d, want %d", tc.maxItems, got, tc.want)
			}
		}
	}
}

func TestRunWorkflow_EachSnapshotsFollowTheStride(t *testing.T) {
	regions := make([]any, 100)
	for i := range regions {
		regions[i] = float64(i)
	}
	h := newEachHarness(t, func(_ context.Context, in *orkstackv1.ExecuteTaskInput) (*orkstackv1.ExecuteTaskResult, error) {
		return itemResult(in), nil
	})
	run := h.run(t, "run-each-stride", fmt.Sprintf(eachInputDocument, 100, "", ""), map[string]any{"regions": regions})
	if run.GetStatus() != orkstackv1.Status_STATUS_SUCCEEDED {
		t.Fatalf("run = %v", run.GetStatus())
	}
	fresh, empty := 0, 0
	for _, p := range h.published {
		if p.event.Item == nil {
			if p.snapshotURI == "" {
				t.Fatalf("node/run event %v published without a snapshot", p.event.GetType())
			}
			continue
		}
		if p.snapshotURI == "" {
			empty++
		} else {
			fresh++
		}
	}
	if fresh != 20 || empty != 80 {
		t.Fatalf("item events: %d fresh, %d without a snapshot; want 20 and 80", fresh, empty)
	}
	node := taskRunOf(t, run, "convert")
	for _, item := range node.GetItems() {
		if item.GetStatus() != orkstackv1.Status_STATUS_SUCCEEDED {
			t.Fatalf("the terminal snapshot is not exact: item %d is %v", item.GetItem(), item.GetStatus())
		}
	}
}

func TestRunWorkflow_EachResultCarriesNoItems(t *testing.T) {
	h := newEachHarness(t, func(_ context.Context, in *orkstackv1.ExecuteTaskInput) (*orkstackv1.ExecuteTaskResult, error) {
		return itemResult(in), nil
	})
	stored := h.run(t, "run-each-result", fmt.Sprintf(eachInputDocument, 10, "", ""), map[string]any{"regions": []any{"eu", "us"}})
	// The workflow's result carries no task row at all (compactRunResult), so
	// no item can reach it however many a fan-out declares.
	result := mustWorkflowResult(t, h.env)
	if len(result.GetTaskRuns()) != 0 {
		t.Fatalf("the workflow result carries task rows: %v", result.GetTaskRuns())
	}
	if got := taskRunOf(t, stored, "convert"); got.GetEach().GetCount() != 2 {
		t.Fatalf("the last snapshot's node row = %v", got)
	}
	if len(taskRunOf(t, stored, "convert").GetItems()) != 2 {
		t.Fatal("the last snapshot lost its items")
	}
}

func TestRunWorkflow_EachLeafContributesNoRunOutput(t *testing.T) {
	h := newEachHarness(t, func(_ context.Context, in *orkstackv1.ExecuteTaskInput) (*orkstackv1.ExecuteTaskResult, error) {
		r := itemResult(in)
		r.ValuesJson = []byte(`{"x":1}`)
		return r, nil
	})
	run := h.run(t, "run-each-leaf", fmt.Sprintf(eachInputDocument, 10, "", ""), map[string]any{"regions": []any{"eu"}})
	if _, present := run.GetOutput().GetData().GetFields()["convert"]; present {
		t.Fatalf("a fan-out leaf contributed to Run.output: %v", run.GetOutput())
	}
}

func TestRunWorkflow_SkippedFanOutNeverCounts(t *testing.T) {
	var h *eachHarness
	h = newEachHarness(t, func(ctx context.Context, in *orkstackv1.ExecuteTaskInput) (*orkstackv1.ExecuteTaskResult, error) {
		return listThenItems(h, partsJSON(3))(ctx, in)
	})
	doc := strings.Replace(fmt.Sprintf(eachTaskDocument, ""), "    needs: [list]\n    each:", "    needs: [list]\n    condition: \"false\"\n    each:", 1)
	run := h.run(t, "run-each-skip", doc, nil)
	if run.GetStatus() != orkstackv1.Status_STATUS_SUCCEEDED {
		t.Fatalf("run = %v %v", run.GetStatus(), run.GetError())
	}
	node := taskRunOf(t, run, "convert")
	if node.GetStatus() != orkstackv1.Status_STATUS_SKIPPED || node.GetEach().GetExpanded() || len(node.GetItems()) != 0 || node.GetEach() == nil {
		t.Fatalf("node = %v", node)
	}
	if h.counts != 0 {
		t.Fatal("a skipped fan-out counted its array")
	}
}

func TestRunWorkflow_CancelDuringFanOutEndsCanceled(t *testing.T) {
	var h *eachHarness
	var once sync.Once
	h = newEachHarness(t, func(ctx context.Context, in *orkstackv1.ExecuteTaskInput) (*orkstackv1.ExecuteTaskResult, error) {
		if in.GetEach().GetIndex() == 0 {
			return itemResult(in), nil
		}
		once.Do(h.env.CancelWorkflow)
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(30 * time.Second):
			return nil, errors.New("never canceled")
		}
	})
	regions := []any{"a", "b", "c", "d"}
	run := h.run(t, "run-each-cancel", fmt.Sprintf(eachInputDocument, 10, ", max_parallel: 2", ""), map[string]any{"regions": regions})
	if run.GetStatus() != orkstackv1.Status_STATUS_CANCELED || run.GetError().GetCode() != "RUN_CANCELED" {
		t.Fatalf("run = %v %v", run.GetStatus(), run.GetError())
	}
	node := taskRunOf(t, run, "convert")
	if node.GetStatus() != orkstackv1.Status_STATUS_CANCELED {
		t.Fatalf("node = %v", node.GetStatus())
	}
	for _, item := range node.GetItems() {
		if item.GetStatus() == orkstackv1.Status_STATUS_PENDING || item.GetStatus() == orkstackv1.Status_STATUS_RUNNING {
			t.Fatalf("item %d left %v", item.GetItem(), item.GetStatus())
		}
	}
}

func TestRunWorkflow_EachOnAHandBuiltPinnedTaskIsInvalidEach(t *testing.T) {
	h := newEachHarness(t, func(_ context.Context, in *orkstackv1.ExecuteTaskInput) (*orkstackv1.ExecuteTaskResult, error) {
		return itemResult(in), nil
	})
	wf := compile(t, fmt.Sprintf(eachInputDocument, 10, "", ""))
	wf.Pins = []*orkstackv1.Pin{{Name: "b"}}
	wf.Tasks[0].Pin = "b"
	plan := defaultPlan()
	h.env.ExecuteWorkflow(engine.RunWorkflow, "run-each-pin", wf, payload(t, map[string]any{"regions": []any{"eu"}}), plan, (*orkstackv1.RunContinuation)(nil))
	run, _ := h.store.Get("run-each-pin")
	node := taskRunOf(t, run, "convert")
	if run.GetStatus() != orkstackv1.Status_STATUS_FAILED || node.GetError().GetCode() != engine.InvalidEachCode {
		t.Fatalf("run = %v, node = %v", run.GetStatus(), node.GetError())
	}
	if len(h.itemCalls("convert")) != 0 {
		t.Fatal("an invalid fan-out dispatched")
	}
}

func TestWithoutItems(t *testing.T) {
	run := &orkstackv1.Run{Id: "r", TaskRuns: []*orkstackv1.TaskRun{
		{TaskId: "a"},
		{TaskId: "b", Each: &orkstackv1.TaskEach{Expanded: true, Count: 1},
			Items: []*orkstackv1.TaskRun{{TaskId: "b", Item: proto.Int32(0)}}},
	}}
	out := engine.WithoutItems(run)
	if len(out.GetTaskRuns()[1].GetItems()) != 0 || out.GetTaskRuns()[1].GetEach().GetCount() != 1 {
		t.Fatalf("WithoutItems = %v", out)
	}
	if len(run.GetTaskRuns()[1].GetItems()) != 1 {
		t.Fatal("WithoutItems mutated its input")
	}
	plain := &orkstackv1.Run{Id: "p", TaskRuns: []*orkstackv1.TaskRun{{TaskId: "a"}}}
	if !proto.Equal(engine.WithoutItems(plain), plain) {
		t.Fatal("WithoutItems changed a run with no fan-out")
	}
}

func TestPublishEvent_EmptySnapshotURIPublishesWithoutReading(t *testing.T) {
	bus := engine.NewEventBus()
	store := engine.NewRunStore()
	reads := &countingSnapshots{testSnapshotStore: newTestSnapshotStore()}
	activities := engine.NewEventActivities(bus, store).WithSnapshotStore(reads)
	event := &orkstackv1.Event{RunId: "r", Sequence: 7, Type: orkstackv1.EventType_EVENT_TYPE_TASK_LOG, TaskId: "convert", Item: proto.Int32(3)}
	if err := activities.PublishEvent(context.Background(), event, "", "r#7"); err != nil {
		t.Fatalf("PublishEvent: %v", err)
	}
	if got := bus.Events("r", 0); len(got) != 1 || got[0].GetItem() != 3 {
		t.Fatalf("bus = %v", got)
	}
	if _, ok := store.Get("r"); ok {
		t.Fatal("the store was written for an event with no snapshot")
	}
	if reads.reads != 0 {
		t.Fatalf("the snapshot store was read %d times", reads.reads)
	}
}

type countingSnapshots struct {
	*testSnapshotStore
	reads int
}

func (c *countingSnapshots) ReadSnapshot(ctx context.Context, uri string) ([]byte, error) {
	c.reads++
	return c.testSnapshotStore.ReadSnapshot(ctx, uri)
}
