package engine

import (
	"context"
	"errors"
	"slices"
	"strings"
	"testing"

	"github.com/stretchr/testify/mock"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	workflowpb "go.temporal.io/api/workflow/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/converter"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/dsl"
	"github.com/orkstack/orkstack/internal/payloadcrypt"
	"github.com/orkstack/orkstack/internal/stablejson"
	"github.com/orkstack/orkstack/internal/task"
	"github.com/orkstack/orkstack/internal/testsupport/must"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// keysClient is a cluster with active keys in use, whose one gate answers no
// query: none runs (the zero value), or it cannot be read. An index that fails
// is a fault a real cluster does not produce on demand.
type keysClient struct {
	client.Client
	active   int64
	queryErr error
	countErr error
}

func (f *keysClient) QueryWorkflow(context.Context, string, string, string, ...any) (converter.EncodedValue, error) {
	if f.queryErr != nil {
		return nil, f.queryErr
	}
	return nil, serviceerror.NewNotFound("none")
}

func (f *keysClient) CountWorkflow(context.Context, *workflowservice.CountWorkflowExecutionsRequest) (*workflowservice.CountWorkflowExecutionsResponse, error) {
	return &workflowservice.CountWorkflowExecutionsResponse{Count: f.active}, f.countErr
}

func childLimit(key string) ChildConcurrency {
	plan := *concPlan().Concurrency
	plan.Slots, plan.Run = nil, nil
	return ChildConcurrency{MaxActiveKeys: 3, Plan: plan,
		Slot: ConcurrencySlotPlan{Key: key, Label: key, Limit: 2, Strategy: gateStrategyRefuse, QueueTimeoutSeconds: 60}}
}

// TestRenderConcurrencyKeys: a child's key is rendered from its inputs and
// where it came from, and refused when it does not render, is an ancestor's,
// or would be one key too many; a check the cluster cannot answer is tried
// again, without the cluster's words.
func TestRenderConcurrencyKeys(t *testing.T) {
	render := func(f *keysClient, key string, ancestors ...string) (ConcurrencySlotPlan, error) {
		return (&GateActivities{client: f}).RenderConcurrencyKeys(context.Background(), RenderConcurrencyKeysInput{
			Limit: childLimit(key), Inputs: stablejson.RawJSON(`{"env":"prod","n":3,"big":2500000}`), Orkflow: "inner", Version: "1.0.0",
			OriginKind: RunOriginKindParentRun, OriginID: "ork-parent", Ancestors: ancestors})
	}
	slot, err := render(&keysClient{active: 2}, "db:{{ .inputs.env }}-{{ .inputs.n }}:{{ .run.orkflow }}")
	must.OK(t, err)
	want := childLimit("x").Slot
	want.Key, want.Label, want.GateID = "db:prod-3:inner", "db:{{ .inputs.env }}-{{ .inputs.n }}:{{ .run.orkflow }}", GateID("team-a", "", "db:prod-3:inner")
	if slot != want {
		t.Fatalf("slot %+v; want %+v", slot, want)
	}
	// A child that declares no key has its Orkflow's own.
	own := DefaultRunConcurrencyKey("default", "inner")
	slot, err = render(&keysClient{}, own)
	must.OK(t, err)
	if slot.Key != own || slot.GateID != GateID("team-a", "", own) {
		t.Fatalf("slot %+v; want the Orkflow's own key", slot)
	}
	// A number reads as a submit renders it, from the run's own input.
	slot, err = render(&keysClient{}, "n{{ .inputs.big }}")
	must.OK(t, err)
	submitted, err := dsl.RenderConcurrencyKey("n{{ .inputs.big }}", map[string]any{"big": float64(2500000)}, dsl.ConcurrencyKeyRun{})
	must.OK(t, err)
	if slot.Key != submitted {
		t.Errorf("key %q, and %q at a submit", slot.Key, submitted)
	}
	for name, c := range map[string]struct {
		f         *keysClient
		key, code string
		ancestors []string
	}{
		"an input it does not have": {f: &keysClient{}, key: "db:{{ .inputs.missing }}", code: TaskConcurrencyKeyInvalidCode},
		"a key the rule refuses":    {f: &keysClient{}, key: "-{{ .inputs.env }}", code: TaskConcurrencyKeyInvalidCode},
		"an ancestor's":             {f: &keysClient{}, key: "db:{{ .inputs.env }}", code: TaskConcurrencyKeyConflictCode, ancestors: []string{"ork-conc-z", GateID("team-a", "", "db:prod")}},
		"one key too many":          {f: &keysClient{active: 3}, key: "db", code: TaskConcurrencyKeysExhaustedCode},
	} {
		t.Run(name, func(t *testing.T) {
			_, err := render(c.f, c.key, c.ancestors...)
			code, category, message := renderFailure("TASK_CONCURRENCY_CHECK", err)
			if code != c.code || !categoryOf(err).nonRetryable() || message == "" || category == orkstackv1.FailureCategory_FAILURE_CATEGORY_INFRASTRUCTURE_ERROR {
				t.Fatalf("= %v (%s %v); want %s, never retried", err, code, category, c.code)
			}
		})
	}
	for name, f := range map[string]*keysClient{
		"the gate":  {queryErr: serviceerror.NewUnavailable("raw cluster words")},
		"the index": {countErr: serviceerror.NewUnavailable("raw cluster words")},
	} {
		_, err := render(f, "db")
		code, category, message := renderFailure("TASK_CONCURRENCY_CHECK", err)
		if code != TaskConcurrencyCheckUnavailableCode || category != orkstackv1.FailureCategory_FAILURE_CATEGORY_INFRASTRUCTURE_ERROR ||
			categoryOf(err).nonRetryable() || strings.Contains(message, "raw cluster words") {
			t.Errorf("%s: %v (%s %v %q); want unavailable, retried, in its own words", name, err, code, category, message)
		}
	}
}

// childHarness runs a parent held to key "deploy" that composes a child whose
// own limit is limit, and records what renders the child's key.
func childHarness(t *testing.T, limit *ChildConcurrency, render func(RenderConcurrencyKeysInput) (ConcurrencySlotPlan, error), answers ...AcquireConcurrencyResult) (*concRunHarness, RunPlan, *orkstackv1.Orkflow, *[]RenderConcurrencyKeysInput) {
	t.Helper()
	h := newConcRunHarness(t)
	h.answers = answers
	asked := &[]RenderConcurrencyKeysInput{}
	h.env.RegisterActivityWithOptions(func(_ context.Context, in RenderConcurrencyKeysInput) (ConcurrencySlotPlan, error) {
		*asked = append(*asked, in)
		return render(in)
	}, activity.RegisterOptions{Name: ActivityRenderConcurrencyKeys})
	store, err := artifacts.NewLocalStore(t.TempDir())
	must.OK(t, err)
	compose := NewComposeActivities(store)
	h.env.RegisterActivityWithOptions(compose.RenderComposedValues, activity.RegisterOptions{Name: ActivityRenderComposedValues})
	h.env.RegisterActivityWithOptions(compose.StoreComposedValues, activity.RegisterOptions{Name: ActivityStoreComposedValues})
	child := concOrkflow()
	child.Name, child.DefinitionDigest = "inner", "sha256:"+strings.Repeat("c", 64)
	cp, err := NewChildRunPlan(child, RunPlan{Namespace: "team-a", Routes: map[string]string{"main": "ork-tasks"}}, false, false)
	must.OK(t, err)
	cp.RunID, cp.Concurrency = "ork-child", limit
	parent := &orkstackv1.Orkflow{Name: "outer", Version: "v1", Namespace: "team-a", Tasks: []*orkstackv1.Task{{Id: "inner",
		Orkflow: &orkstackv1.OrkflowRef{Name: "inner", Version: "v1", DefinitionDigest: child.DefinitionDigest}}}}
	plan := concPlan()
	plan.History = &HistoryLimit{MaxEvents: DefaultHistoryMaxEvents, MaxBytes: DefaultHistoryMaxBytes, Reserve: DefaultHistoryReserve}
	plan.Children = map[string]*ChildRunPlan{"inner": cp}
	plan.ConcurrencyAncestors = []string{"ork-conc-above"}
	return h, plan, parent, asked
}

// TestComposedChild_TakesItsOwnSlot: the parent renders the child's key from
// its inputs, names every gate the child may not ask for, and starts it with
// the slot; the child asks for it as a run does, under its own id, and releases
// it at its end. A child with no limit renders nothing and asks for nothing.
func TestComposedChild_TakesItsOwnSlot(t *testing.T) {
	limit := childLimit("db:{{ .inputs.env }}")
	slot := limit.Slot
	slot.Key, slot.GateID = "db:prod", "ork-conc-child"
	h, plan, parent, asked := childHarness(t, &limit, func(RenderConcurrencyKeysInput) (ConcurrencySlotPlan, error) { return slot, nil }, granted(1), granted(5))
	var released []GateReleaseAll
	h.env.OnSignalExternalWorkflow(mock.Anything, "ork-conc-child", "", GateSignalReleaseAll, mock.Anything).Return(
		func(_, _, _, _ string, arg any) error {
			released = append(released, payloadcrypt.Unplain(arg).(GateReleaseAll))
			return nil
		})
	h.runOrkflow(plan, parent)
	must.OK(t, h.env.GetWorkflowError())
	if len(*asked) != 1 {
		t.Fatalf("rendered %d times, want once", len(*asked))
	}
	if in := (*asked)[0]; in.Orkflow != "inner" || in.OriginKind != RunOriginKindParentRun || in.OriginID != concRunID ||
		!slices.Equal(in.Ancestors, []string{"ork-conc-above", "ork-conc-test"}) || in.Limit.Slot.Key != "db:{{ .inputs.env }}" {
		t.Fatalf("rendered %+v", in)
	}
	if len(h.acquires) != 2 {
		t.Fatalf("%d acquires, want the parent's and the child's", len(h.acquires))
	}
	if a := h.acquires[1]; a.GateID != "ork-conc-child" || a.Request.Run != "ork-child" || a.Request.Strategy != gateStrategyRefuse || a.Request.Limit != 2 ||
		a.Start.Key != "db:prod" || a.Start.Namespace != "team-a" {
		t.Fatalf("the child's acquire = %+v", a)
	}
	if !h.last.GetTaskRuns()[0].GetChild().GetConcurrencyLimited() {
		t.Error("the composed task does not say its child has a limit")
	}
	if len(released) != 1 || released[0] != (GateReleaseAll{Run: "ork-child", Below: 2}) || h.ran != 1 {
		t.Fatalf("the child released %+v after %d tasks", released, h.ran)
	}

	h, plan, parent, asked = childHarness(t, nil, nil, granted(1))
	h.runOrkflow(plan, parent)
	must.OK(t, h.env.GetWorkflowError())
	if len(*asked) != 0 || len(h.acquires) != 1 || h.ran != 1 || h.last.GetTaskRuns()[0].GetChild().GetConcurrencyLimited() {
		t.Fatalf("a child with no limit rendered %d keys and asked %d gates", len(*asked), len(h.acquires)-1)
	}
}

// TestComposedChild_ARefusedKeyFailsItsTask: the composed task fails with the
// refusal's code and reason, and the child never starts.
func TestComposedChild_ARefusedKeyFailsItsTask(t *testing.T) {
	for name, c := range map[string]struct {
		err  error
		slot ConcurrencySlotPlan
		code string
	}{
		"refused":      {err: composeActivityError(TaskConcurrencyKeyConflictCode, orkstackv1.FailureCategory_FAILURE_CATEGORY_CONTRACT_VIOLATION, "an ancestor's"), code: TaskConcurrencyKeyConflictCode},
		"unavailable":  {err: errors.New("down"), code: TaskConcurrencyCheckUnavailableCode},
		"not rendered": {code: TaskConcurrencyCheckUnavailableCode},
	} {
		t.Run(name, func(t *testing.T) {
			limit := childLimit("db")
			h, plan, parent, asked := childHarness(t, &limit, func(RenderConcurrencyKeysInput) (ConcurrencySlotPlan, error) { return c.slot, c.err }, granted(1))
			h.runOrkflow(plan, parent)
			if h.env.GetWorkflowError() == nil || h.ran != 0 || len(h.acquires) != 1 {
				t.Fatalf("ended %v after %d tasks and %d acquires; want failed, and the child never started", h.env.GetWorkflowError(), h.ran, len(h.acquires))
			}
			row := h.last.GetTaskRuns()[0]
			if row.GetError().GetCode() != c.code || !strings.Contains(row.GetError().GetMessage(), "child orkflow inner") || row.GetChild().GetRunId() != "" {
				t.Fatalf("task %v; want %s naming the child, and no run", row, c.code)
			}
			if name == "unavailable" && len(*asked) != 3 {
				t.Errorf("tried %d times, want 3", len(*asked))
			}
		})
	}
}

// TestReserveConcurrencyRelease: a limit's release adds to its own copy of the
// reserve, never to the one the runs of its tree share.
func TestReserveConcurrencyRelease(t *testing.T) {
	shared := &HistoryLimit{MaxEvents: 100, MaxBytes: 1 << 20, Reserve: DefaultHistoryReserve}
	fixed := shared.Reserve.Fixed
	got := ReserveConcurrencyRelease(shared, concPlan().Concurrency)
	if shared.Reserve.Fixed != fixed || got == shared || got.Reserve.Fixed <= fixed {
		t.Fatalf("shared %d, own %d; want the shared one unchanged and its own above %d", shared.Reserve.Fixed, got.Reserve.Fixed, fixed)
	}
	if ReserveConcurrencyRelease(nil, concPlan().Concurrency) != nil || ReserveConcurrencyRelease(shared, nil) != shared {
		t.Fatal("no limit, or no slot, adds nothing")
	}
}

// TestComposedChild_ACancelDuringTheCheckCancelsItsTask: a run canceled while
// the child's key is checked ends its composed task canceled, never as a check
// that could not be made.
func TestComposedChild_ACancelDuringTheCheckCancelsItsTask(t *testing.T) {
	limit := childLimit("db")
	var h *concRunHarness
	h, plan, parent, _ := childHarness(t, &limit, func(RenderConcurrencyKeysInput) (ConcurrencySlotPlan, error) {
		h.env.CancelWorkflow()
		return ConcurrencySlotPlan{}, errors.New("down")
	}, granted(1))
	h.runOrkflow(plan, parent)
	row := h.last.GetTaskRuns()[0]
	if row.GetStatus() != orkstackv1.Status_STATUS_CANCELED || row.GetError().GetCode() != ChildRunCanceledCode ||
		h.last.GetStatus() != orkstackv1.Status_STATUS_CANCELED || len(h.acquires) != 1 {
		t.Fatalf("task %v in run %v after %d acquires; want both canceled, and no child", row, h.last.GetStatus(), len(h.acquires))
	}
}

// TestComposedChild_AncestorsReachThroughAChildWithNoLimit: a grandchild may
// ask for no gate of a run above it, though the child between them has no
// limit of its own.
func TestComposedChild_AncestorsReachThroughAChildWithNoLimit(t *testing.T) {
	limit := childLimit("db")
	slot := limit.Slot
	slot.GateID = "ork-conc-child"
	h, plan, parent, asked := childHarness(t, nil, func(RenderConcurrencyKeysInput) (ConcurrencySlotPlan, error) { return slot, nil }, granted(1), granted(2))
	h.env.OnSignalExternalWorkflow(mock.Anything, "ork-conc-child", "", GateSignalReleaseAll, mock.Anything).Return(nil)
	inner := plan.Children["inner"]
	inner.Concurrency = &limit
	mid := &orkstackv1.Orkflow{Name: "mid", Version: "v1", Namespace: "team-a", DefinitionDigest: "sha256:" + strings.Repeat("d", 64), Tasks: parent.Tasks}
	midPlan := RunPlan{Namespace: "team-a", Routes: map[string]string{"main": "ork-tasks"}, Children: map[string]*ChildRunPlan{"inner": inner}}
	mp, err := NewChildRunPlan(mid, midPlan, false, false)
	must.OK(t, err)
	mp.RunID = "ork-mid"
	plan.Children = map[string]*ChildRunPlan{"mid": mp}
	parent.Tasks = []*orkstackv1.Task{{Id: "mid", Orkflow: &orkstackv1.OrkflowRef{Name: "mid", Version: "v1", DefinitionDigest: mid.DefinitionDigest}}}
	h.runOrkflow(plan, parent)
	must.OK(t, h.env.GetWorkflowError())
	if len(*asked) != 1 || !slices.Equal((*asked)[0].Ancestors, []string{"ork-conc-above", "ork-conc-test"}) || (*asked)[0].OriginID != "ork-mid" {
		t.Fatalf("rendered %+v; want once, for the middle run, refusing the gates above it", *asked)
	}
}

// waitingClient answers each describe by run id.
type waitingClient struct {
	client.Client
	answers map[string]*workflowpb.WorkflowExecutionInfo
	errs    map[string]error
}

func (f *waitingClient) DescribeWorkflow(_ context.Context, run, _ string) (*client.WorkflowExecutionDescription, error) {
	return &client.WorkflowExecutionDescription{RawResponse: &workflowservice.DescribeWorkflowExecutionResponse{
		WorkflowExecutionInfo: f.answers[run]}}, f.errs[run]
}

// TestWaitingRuns: only a running execution that says it is queued waits; a
// run that could not be described is reported, and hides no other.
func TestWaitingRuns(t *testing.T) {
	info := func(status enumspb.WorkflowExecutionStatus, queued string) *workflowpb.WorkflowExecutionInfo {
		p, err := converter.GetDefaultDataConverter().ToPayload(queued)
		must.OK(t, err)
		return &workflowpb.WorkflowExecutionInfo{Status: status, SearchAttributes: &commonpb.SearchAttributes{
			IndexedFields: map[string]*commonpb.Payload{task.SearchAttrOrkflowRunQueued: p}}}
	}
	running := enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING
	f := &waitingClient{
		answers: map[string]*workflowpb.WorkflowExecutionInfo{"b": info(running, "true"), "a": info(running, "true"), "granted": info(running, "false"),
			"plain": {Status: running}, "ended": info(enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED, "true")},
		errs: map[string]error{"gone": serviceerror.NewNotFound("none")},
	}
	ask := []string{"b", "granted", "gone", "plain", "ended", "a"}
	got, err := NewTemporalConcurrencyReader(f, 0).WaitingRuns(context.Background(), ask)
	if err != nil || !slices.Equal(got, []string{"b", "a"}) {
		t.Fatalf("waiting %v, %v; want b and a", got, err)
	}
	f.errs["granted"] = serviceerror.NewUnavailable("down")
	if got, err := NewTemporalConcurrencyReader(f, 0).WaitingRuns(context.Background(), append(ask, "nothing")); err == nil || !slices.Equal(got, []string{"b", "a"}) {
		t.Fatalf("waiting %v, %v; want b and a, and the runs that could not be described", got, err)
	}
}

// TestRenderConcurrencyTaskKeys: each of a child's tasks' keys is rendered, and
// one that is its own run's, or an ancestor's, is refused.
func TestRenderConcurrencyTaskKeys(t *testing.T) {
	limit := childLimit("")
	limit.Tasks = map[string]ConcurrencySlotPlan{"b": childLimit("push:{{ .inputs.env }}").Slot, "a": childLimit(DefaultTaskConcurrencyKey("team-a", "inner", "a")).Slot}
	render := func(runGate string, ancestors ...string) (ChildTaskSlots, error) {
		return (&GateActivities{client: &keysClient{}}).RenderConcurrencyTaskKeys(context.Background(), RenderConcurrencyKeysInput{
			Limit: limit, Inputs: stablejson.RawJSON(`{"env":"prod"}`), Orkflow: "inner", Version: "1.0.0",
			OriginKind: RunOriginKindParentRun, OriginID: "ork-parent", Ancestors: ancestors, RunGate: runGate})
	}
	got, err := render("")
	must.OK(t, err)
	if got.Tasks["b"].Key != "push:prod" || got.Tasks["b"].GateID != GateID("team-a", "", "push:prod") ||
		got.Tasks["a"].Key != "ork:task:team-a/inner:a" || len(got.Tasks) != 2 {
		t.Fatalf("slots %+v", got)
	}
	for _, c := range [][]string{{GateID("team-a", "", "push:prod")}, {"", GateID("team-a", "", "push:prod")}} {
		_, err := render(c[0], c[1:]...)
		if code, _, _ := renderFailure("TASK_CONCURRENCY_CHECK", err); code != TaskConcurrencyKeyConflictCode {
			t.Errorf("run gate %q, ancestors %v = %v; want %s", c[0], c[1:], err, TaskConcurrencyKeyConflictCode)
		}
	}
}

// TestComposedChild_StartsWithItsTasksSlots: the parent renders the child's
// tasks' keys after its own and starts it with every slot, its own first.
func TestComposedChild_StartsWithItsTasksSlots(t *testing.T) {
	limit := childLimit("db")
	own := limit.Slot
	own.GateID = "ork-conc-child"
	limit.Tasks = map[string]ConcurrencySlotPlan{"apply": childLimit("push").Slot}
	h, plan, parent, asked := childHarness(t, &limit, func(in RenderConcurrencyKeysInput) (ConcurrencySlotPlan, error) { return own, nil }, granted(1), granted(5), granted(6))
	var tasked []RenderConcurrencyKeysInput
	h.env.RegisterActivityWithOptions(func(_ context.Context, in RenderConcurrencyKeysInput) (ChildTaskSlots, error) {
		tasked = append(tasked, in)
		push := in.Limit.Tasks["apply"]
		push.GateID = "ork-conc-push"
		return ChildTaskSlots{Tasks: map[string]ConcurrencySlotPlan{"apply": push}}, nil
	}, activity.RegisterOptions{Name: ActivityRenderConcurrencyTaskKeys})
	h.env.OnSignalExternalWorkflow(mock.Anything, mock.Anything, "", mock.Anything, mock.Anything).Return(nil)
	h.runOrkflow(plan, parent)
	must.OK(t, h.env.GetWorkflowError())
	if len(*asked) != 1 || len(tasked) != 1 || tasked[0].RunGate != "ork-conc-child" {
		t.Fatalf("rendered %d own keys and %+v", len(*asked), tasked)
	}
	var gates []string
	for _, a := range h.acquires {
		gates = append(gates, a.GateID)
	}
	if !slices.Equal(gates, []string{"ork-conc-test", "ork-conc-child", "ork-conc-push"}) {
		t.Fatalf("asked %v; want the parent's, the child's own, then its task's", gates)
	}
}
