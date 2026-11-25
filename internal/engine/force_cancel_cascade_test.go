package engine

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/api/serviceerror"
	workflowpb "go.temporal.io/api/workflow/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/client"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/timestamppb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/testsupport/must"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// belowRun is a run as its own start and the cluster's record of it have it.
type belowRun struct {
	namespace, parent string
	grace             int32
	closed, unread    bool
	err               error
	// child is the execution of parent it is a child of, "exec-of-" its own
	// where unset, "none" for none; typ, its workflow type; earlier, the run
	// as an earlier execution of its id started it.
	child, typ string
	earlier    *belowRun
}

// belowCluster holds runs, and the force cancels in flight of them.
type belowCluster struct {
	workflowservice.WorkflowServiceClient
	runs    map[string]belowRun
	flights []fcInFlight
}

func (c *belowCluster) start(id, exec string) (*historypb.HistoryEvent, error) {
	for _, f := range c.flights {
		if f.id == id {
			input, err := DataConverter().ToPayloads(f.input)
			return &historypb.HistoryEvent{Attributes: &historypb.HistoryEvent_WorkflowExecutionStartedEventAttributes{
				WorkflowExecutionStartedEventAttributes: &historypb.WorkflowExecutionStartedEventAttributes{Input: input}}}, err
		}
	}
	r, ok := c.runs[id]
	if exec == "earlier" && r.earlier != nil {
		r = *r.earlier
	} else if exec != "" && exec != "exec-of-"+id {
		ok = false
	}
	switch {
	case !ok:
		return nil, serviceerror.NewNotFound("none")
	case r.err != nil:
		return nil, r.err
	}
	plan := RunPlan{}
	if r.parent != "" {
		plan.Origin = &RunOrigin{Kind: RunOriginKindParentRun, ID: r.parent}
	}
	if r.parent == "schedule" {
		plan.Origin.Kind = "RUN_ORIGIN_KIND_SCHEDULE"
	}
	wf := &orkstackv1.Orkflow{Name: "pay", Namespace: r.namespace, Tasks: []*orkstackv1.Task{{Id: "t", CancellationGracePeriodSeconds: r.grace}}}
	args := []any{id, wf, nil, plan}
	if r.unread {
		args = args[:2]
	}
	input, err := DataConverter().ToPayloads(args...)
	started := &historypb.WorkflowExecutionStartedEventAttributes{Input: input, WorkflowType: &commonpb.WorkflowType{Name: RunWorkflowType}}
	if r.typ != "" {
		started.WorkflowType.Name = r.typ
	}
	switch {
	case r.parent == "" || r.parent == "schedule" || r.child == "none":
	case r.child != "":
		started.ParentWorkflowExecution = &commonpb.WorkflowExecution{WorkflowId: r.parent, RunId: r.child}
	default:
		started.ParentWorkflowExecution = &commonpb.WorkflowExecution{WorkflowId: r.parent, RunId: "exec-of-" + r.parent}
	}
	return &historypb.HistoryEvent{EventId: 1, Attributes: &historypb.HistoryEvent_WorkflowExecutionStartedEventAttributes{WorkflowExecutionStartedEventAttributes: started}}, err
}

func (c *belowCluster) GetWorkflowExecutionHistory(_ context.Context, in *workflowservice.GetWorkflowExecutionHistoryRequest, _ ...grpc.CallOption) (*workflowservice.GetWorkflowExecutionHistoryResponse, error) {
	e, err := c.start(in.GetExecution().GetWorkflowId(), in.GetExecution().GetRunId())
	return &workflowservice.GetWorkflowExecutionHistoryResponse{History: &historypb.History{Events: []*historypb.HistoryEvent{e}}}, err
}

type oneStart struct {
	event *historypb.HistoryEvent
	err   error
}

func (o *oneStart) HasNext() bool                          { return true }
func (o *oneStart) Next() (*historypb.HistoryEvent, error) { return o.event, o.err }

// belowReads is the same cluster as a run's reader reads it.
type belowReads struct {
	runReadClient
	*belowCluster
}

func (c belowReads) GetWorkflowHistory(_ context.Context, id, _ string, _ bool, _ enumspb.HistoryEventFilterType) client.HistoryEventIterator {
	e, err := c.start(id, "")
	return &oneStart{e, err}
}

// belowClient is the same cluster as an SDK client: runs are described side by
// side, which one mocked call's answer cannot be.
type belowClient struct {
	client.Client
	c *belowCluster
}

func (b belowClient) WorkflowService() workflowservice.WorkflowServiceClient { return b.c }

func (b belowClient) ListWorkflow(_ context.Context, in *workflowservice.ListWorkflowExecutionsRequest) (*workflowservice.ListWorkflowExecutionsResponse, error) {
	out := &workflowservice.ListWorkflowExecutionsResponse{}
	for _, f := range b.c.flights {
		if strings.Contains(in.GetQuery(), fmt.Sprintf("%q", f.id[:len("ork-force-")+33])) {
			out.Executions = append(out.Executions, &workflowpb.WorkflowExecutionInfo{Execution: &commonpb.WorkflowExecution{WorkflowId: f.id, RunId: f.exec},
				Type: &commonpb.WorkflowType{Name: ForceCancelWorkflowType}, StartTime: timestamppb.New(f.started), TaskQueue: "ork-tasks"})
		}
	}
	return out, nil
}

func (b belowClient) DescribeWorkflow(_ context.Context, id, _ string) (*client.WorkflowExecutionDescription, error) {
	r, ok := b.c.runs[id]
	if !ok {
		return nil, serviceerror.NewNotFound("none")
	}
	info := &workflowpb.WorkflowExecutionInfo{Execution: &commonpb.WorkflowExecution{WorkflowId: id, RunId: "exec-of-" + id},
		Type: &commonpb.WorkflowType{Name: RunWorkflowType}, Status: enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING}
	if r.closed {
		info.Status = enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED
	}
	return &client.WorkflowExecutionDescription{RawResponse: &workflowservice.DescribeWorkflowExecutionResponse{WorkflowExecutionInfo: info}}, r.err
}

// cascadeActivities are activities running in a cascading force cancel of
// ork-r over c, and its input.
func cascadeActivities(t *testing.T, keys ForceCancelKeys, mode string, c *belowCluster) (*ForceCancelActivities, ForceCancelInput, ForceCancelDecision) {
	t.Helper()
	d := sampleDecision()
	d.Cascade = true
	settings := DefaultForceCancelSettings()
	a := NewForceCancelActivities(ForceCancelDeps{Client: belowClient{c: c}, ClusterNamespace: "ork-main", Keys: keys, Mode: func() string { return mode }, Settings: settings,
		Reader: &TemporalRunReader{client: belowReads{belowCluster: c}, converter: DataConverter()}})
	var in ForceCancelInput
	if mode == "asserted" {
		d.claimNothing()
		in.Unsigned = &d
	} else {
		msg, err := SignForceCancelDecision(keys, d)
		must.OK(t, err)
		in.Decision = msg
	}
	in.Settings, in.ScanEvents, in.PageSize, in.RecordTimeout = &settings, 2000, 250, 10*time.Second
	started := time.Now()
	a.self = func(context.Context) (forceCancelSelf, error) {
		return forceCancelSelf{WorkflowID: d.WorkflowID(), RunID: "coordinator", Type: ForceCancelWorkflowType, StartedAt: started, Local: true}, nil
	}
	return a, in, d
}

// tree is ork-r over a over b, and what is beside them.
func tree() map[string]belowRun {
	return map[string]belowRun{
		"ork-r": {namespace: "payments"}, "a": {namespace: "payments", parent: "ork-r"}, "b": {namespace: "payments", parent: "a", grace: 240},
		"closed": {namespace: "payments", parent: "a", closed: true}, "other-tree": {namespace: "payments", parent: "elsewhere"}, "elsewhere": {namespace: "payments"},
		"other-ns": {namespace: "billing", parent: "ork-r"}, "orphan": {namespace: "payments", parent: "gone"},
		// Started by a run of another tree that has closed, whose id someone
		// started again since, saying it is below.
		"taken": {namespace: "payments", parent: "ork-r", earlier: &belowRun{namespace: "payments", parent: "elsewhere"}}, "of-taken": {namespace: "payments", parent: "taken", child: "earlier"},
		"plain": {namespace: "payments", parent: "ork-r", child: "none"}, "typed": {namespace: "payments", parent: "ork-r", typ: "Other"}, "unread": {namespace: "payments", parent: "ork-r", unread: true},
		"scheduled": {namespace: "payments", parent: "schedule"}, "loop": {namespace: "payments", parent: "loop"},
	}
}

// TestForceCancelDerive: a run below has a decision of its own, which names it
// and nothing else and is good in one child of one execution; a run that is
// not below, by its own start and its parents', has none, whoever says it is.
func TestForceCancelDerive(t *testing.T) {
	ctx, keys := context.Background(), recordKeys(t)
	a, in, d := cascadeActivities(t, keys, "enforce", &belowCluster{runs: tree()})
	for id, want := range map[string]string{"ork-r": FCCauseNotBelow, "other-tree": FCCauseNotBelow, "elsewhere": FCCauseNotBelow, "other-ns": FCCauseNotBelow, "scheduled": FCCauseNotBelow,
		"loop": FCCauseNotBelow, "of-taken": FCCauseNotBelow, "plain": FCCauseNotBelow, "typed": FCCauseNotBelow, "orphan": FCCauseChainUnreadable, "unread": FCCauseChainUnreadable} {
		got, err := a.Derive(ctx, FCBelowStep{Input: in, RunID: id})
		if err != nil || got.Cause != want || got.WorkflowID != "" {
			t.Errorf("%s: %+v, %v; want %s", id, got, err, want)
		}
	}
	for _, id := range []string{"closed", "gone"} {
		if got, err := a.Derive(ctx, FCBelowStep{Input: in, RunID: id}); err != nil || !got.Closed || got.WorkflowID != "" {
			t.Errorf("%s: %+v, %v", id, got, err)
		}
	}
	a.now = func() time.Time { return d.ExpiresAt.Add(time.Second) }
	if got, err := a.Derive(ctx, FCBelowStep{Input: in, RunID: "a"}); err != nil || got.Cause != FCCauseDecisionStale || got.WorkflowID != "" {
		t.Errorf("once its own decision has expired: %+v, %v", got, err)
	}
	a.now = time.Now
	for id, grace := range map[string]time.Duration{"a": time.Minute, "b": 4*time.Minute + 30*time.Second} {
		got, err := a.Derive(ctx, FCBelowStep{Input: in, RunID: id})
		must.OK(t, err)
		again, err := a.Derive(ctx, FCBelowStep{Input: in, RunID: id})
		must.OK(t, err)
		if got.WorkflowID == again.WorkflowID {
			t.Errorf("%s: two starts have one id %s", id, got.WorkflowID)
		}
		if got.Input.Derived == "" || got.Input.Decision != "" || got.Input.Unsigned != nil || got.Input.Settings != nil || got.Timeout < 3*time.Hour || got.Bound < 40*time.Minute {
			t.Errorf("%s: %+v", id, got)
		}
		self := forceCancelSelf{WorkflowID: got.WorkflowID, Type: ForceCancelWorkflowType, StartedAt: time.Now(), Local: true,
			Parent: &commonpb.WorkflowExecution{WorkflowId: d.WorkflowID(), RunId: "coordinator"}}
		own, cause, err := a.decide(ctx, got.Input, self)
		if err != nil || own == nil {
			t.Fatalf("%s: its own decision is refused: %q, %v", id, cause, err)
		}
		if own.RunID != id || own.TargetExecutionRunID != "exec-of-"+id || own.Grace() != grace || own.Cascade || !strings.HasPrefix(own.RequestID, d.RequestID+"-") || own.Principal != d.Principal ||
			!own.PrincipalVerified || own.Reason != d.Reason || own.ExpiresAt.After(d.ExpiresAt) || own.Permission != d.Permission || own.Scope != d.Scope || own.Of == nil || own.Of.RunID != "ork-r" {
			t.Errorf("%s: %+v", id, own)
		}
		for name, change := range map[string]func(*ForceCancelInput, *forceCancelSelf){
			"no parent": func(_ *ForceCancelInput, s *forceCancelSelf) { s.Parent = nil },
			"another parent": func(_ *ForceCancelInput, s *forceCancelSelf) {
				s.Parent = &commonpb.WorkflowExecution{WorkflowId: "ork-force-x", RunId: "coordinator"}
			},
			"another of its executions": func(_ *ForceCancelInput, s *forceCancelSelf) {
				s.Parent = &commonpb.WorkflowExecution{WorkflowId: d.WorkflowID(), RunId: "other"}
			},
			"another id":                func(_ *ForceCancelInput, s *forceCancelSelf) { s.WorkflowID += "0" },
			"as one a caller asked for": func(in *ForceCancelInput, _ *forceCancelSelf) { in.Decision, in.Derived = in.Derived, "" },
			"beside the root's":         func(i *ForceCancelInput, _ *forceCancelSelf) { i.Decision = in.Decision },
			"the root's in its place":   func(i *ForceCancelInput, s *forceCancelSelf) { i.Derived, s.WorkflowID = in.Decision, d.WorkflowID() },
		} {
			input, as := got.Input, self
			change(&input, &as)
			if own, cause, err := a.decide(ctx, input, as); own != nil || err != nil || cause != FCCauseDecisionUnverified {
				t.Errorf("%s, %s: %+v, %q, %v", id, name, own, cause, err)
			}
		}
	}
	t.Run("a longer grace asked for is every one's", func(t *testing.T) {
		long := d
		long.GraceSeconds = 500
		forged := DefaultForceCancelSettings()
		forged.Grace, forged.MaxGrace = time.Second, time.Second
		msg, err := SignForceCancelDecision(keys, long)
		must.OK(t, err)
		in := in
		in.Decision, in.Settings = msg, &forged
		got, err := a.Derive(ctx, FCBelowStep{Input: in, RunID: "a"})
		must.OK(t, err)
		own, _, _ := a.decide(ctx, got.Input, forceCancelSelf{WorkflowID: got.WorkflowID, Type: ForceCancelWorkflowType, StartedAt: time.Now(), Local: true,
			Parent: &commonpb.WorkflowExecution{WorkflowId: d.WorkflowID(), RunId: "coordinator"}})
		if own == nil || own.Grace() != 500*time.Second {
			t.Errorf("%+v", own)
		}
	})
	t.Run("one another caller's force cancel is of", func(t *testing.T) {
		theirs := sampleDecision()
		theirs.RequestID, theirs.RunID, theirs.TargetExecutionRunID, theirs.CeilingSeconds = "fc-9", "a", "exec-of-a", 2400
		msg, err := SignForceCancelDecision(keys, theirs)
		must.OK(t, err)
		c := &belowCluster{runs: tree(), flights: []fcInFlight{{id: theirs.WorkflowID(), exec: "e", started: time.Now(), input: ForceCancelInput{Decision: msg}}}}
		a, in, _ := cascadeActivities(t, keys, "enforce", c)
		got, err := a.Derive(ctx, FCBelowStep{Input: in, RunID: "a"})
		if err != nil || !got.Attached || got.RequestID != "fc-9" || got.WorkflowID != "" || got.Bound < 40*time.Minute {
			t.Errorf("%+v, %v", got, err)
		}
	})
	t.Run("where nothing is signed", func(t *testing.T) {
		keyless := NewKeylessEngine(keysDoc{}, func() string { return "asserted" })
		a, in, d := cascadeActivities(t, keyless, "asserted", &belowCluster{runs: tree()})
		got, err := a.Derive(ctx, FCBelowStep{Input: in, RunID: "a"})
		must.OK(t, err)
		self := forceCancelSelf{WorkflowID: got.WorkflowID, Type: ForceCancelWorkflowType, StartedAt: time.Now(), Local: true,
			Parent: &commonpb.WorkflowExecution{WorkflowId: d.WorkflowID(), RunId: "coordinator"}}
		if own, cause, err := a.decide(ctx, got.Input, self); own == nil || own.PrincipalVerified || own.Rule != "" {
			t.Errorf("%+v, %q, %v", own, cause, err)
		}
		self.Parent = nil
		if own, _, _ := a.decide(ctx, got.Input, self); own != nil {
			t.Error("good with no parent")
		}
	})
	t.Run("what cannot be read is asked again", func(t *testing.T) {
		runs := tree()
		runs["a"] = belowRun{err: errors.New("down")}
		a, in, _ := cascadeActivities(t, keys, "enforce", &belowCluster{runs: runs})
		if got, err := a.Derive(ctx, FCBelowStep{Input: in, RunID: "b"}); err == nil {
			t.Errorf("%+v", got)
		}
	})
}

// TestForceCancelCascadeSteps_NeedACascade: no step of a cascade is taken for a
// decision that has none, or with no settings to take it with.
func TestForceCancelCascadeSteps_NeedACascade(t *testing.T) {
	ctx, keys := context.Background(), recordKeys(t)
	a, in, d := cascadeActivities(t, keys, "enforce", &belowCluster{runs: tree()})
	unsettled := in
	unsettled.Settings = nil
	d.Cascade = false
	msg, err := SignForceCancelDecision(keys, d)
	must.OK(t, err)
	plain := in
	plain.Decision = msg
	forged := in
	forged.Decision = forged.Decision[:len(forged.Decision)-6] + "AAAAAA"
	for name, in := range map[string]ForceCancelInput{"no settings": unsettled, "no cascade": plain, "forged": forged} {
		_, below := a.Below(ctx, in)
		_, derive := a.Derive(ctx, FCBelowStep{Input: in, RunID: "a"})
		_, open := a.Open(ctx, FCOpenStep{Input: in, RunIDs: []string{"a"}})
		_, after := a.After(ctx, FCOpenStep{Input: in})
		summary := a.Summary(ctx, FCSummaryStep{Input: in})
		for step, err := range map[string]error{"below": below, "derive": derive, "open": open, "after": after, "summary": summary} {
			if !refused(err) {
				t.Errorf("%s, %s: %v", name, step, err)
			}
		}
	}
}

// TestForceCancelBelow: a search's answer names at most what a cascade covers,
// and counts the rest.
func TestForceCancelBelow(t *testing.T) {
	c := &treeClient{runs: []treeRun{{id: "ork-r", namespace: "payments"}, {id: "gone", parent: "ork-r", root: "ork-r", namespace: "payments", closed: true}}}
	for i := range 4 {
		c.runs = append(c.runs, treeRun{id: fmt.Sprintf("a%d", i), parent: "ork-r", root: "ork-r", namespace: "payments", orkflow: "pay"})
	}
	c.runs = append(c.runs, treeRun{id: "orphan", parent: "gone", root: "ork-r", namespace: "payments", orkflow: "pay"}, treeRun{id: "odd", parent: "ork-r", namespace: "billing"})
	a, in, _ := cascadeActivities(t, recordKeys(t), "enforce", &belowCluster{runs: tree()})
	a.reader = &TemporalRunReader{client: c, converter: DataConverter(), originIndexed: true, rootIndexed: true}
	in.Settings.Cascade.Max = 3
	got, err := a.Below(context.Background(), in)
	must.OK(t, err)
	if len(got.Rows) != 3 || got.Reached != 1 || got.Unreached != 1 || !reflect.DeepEqual(got.Excluded, map[string]string{"odd": FCExcludedNamespace}) {
		t.Fatalf("%+v", got)
	}
	if got := firstByName(map[string]string{"c": "1", "a": "2", "b": "3"}, 2); !reflect.DeepEqual(got, map[string]string{"a": "2", "b": "3"}) {
		t.Errorf("%v", got)
	}
}

// TestForceCancelAfter: what is open at the end is confirmed run by run, the
// index's word and the cascade's own, and what cannot be told is said.
func TestForceCancelAfter(t *testing.T) {
	ctx, keys := context.Background(), recordKeys(t)
	after := func(t *testing.T, change func(*treeClient, *belowCluster, *ForceCancelSettings), known ...string) FCAfter {
		t.Helper()
		index := &treeClient{runs: []treeRun{{id: "ork-r", namespace: "payments"}, {id: "a", parent: "ork-r", namespace: "payments"}, {id: "closed", parent: "ork-r", namespace: "payments"}}}
		cluster := &belowCluster{runs: tree()}
		a, in, _ := cascadeActivities(t, keys, "enforce", cluster)
		if change != nil {
			change(index, cluster, in.Settings)
		}
		a.reader = &TemporalRunReader{client: index, converter: DataConverter(), originIndexed: true}
		got, err := a.After(ctx, FCOpenStep{Input: in, RunIDs: known})
		must.OK(t, err)
		return got
	}
	for name, tt := range map[string]struct {
		change func(*treeClient, *belowCluster, *ForceCancelSettings)
		known  []string
		want   FCAfter
	}{
		"a run the index has not yet closed is not open": {nil, nil, FCAfter{Count: 1, RunIDs: []string{"a"}}},
		"one only the cascade knows of":                  {nil, []string{"b", "a", "gone"}, FCAfter{Count: 2, RunIDs: []string{"a", "b"}}},
		"no more are named than it covers":               {func(_ *treeClient, _ *belowCluster, s *ForceCancelSettings) { s.Cascade.Max = 1 }, []string{"b"}, FCAfter{Count: 2, RunIDs: []string{"a"}}},
		"a search cut short":                             {func(_ *treeClient, _ *belowCluster, s *ForceCancelSettings) { s.Cascade.Scan = 1 }, nil, FCAfter{Count: 1, RunIDs: []string{"a"}, Unconfirmed: FCCauseTooManyRuns}},
		"a search that fails":                            {func(i *treeClient, _ *belowCluster, _ *ForceCancelSettings) { i.err = errors.New("down") }, nil, FCAfter{Unconfirmed: FCCauseCouldNotRead}},
		"a search out of time": {func(i *treeClient, _ *belowCluster, s *ForceCancelSettings) {
			i.block, s.Cascade.ListTimeout = true, time.Millisecond
		}, nil, FCAfter{RunIDs: []string{}, Unconfirmed: FCCauseTookTooLong}},
		"a run that cannot be described": {func(_ *treeClient, c *belowCluster, _ *ForceCancelSettings) {
			c.runs["a"] = belowRun{err: errors.New("down")}
		}, nil,
			FCAfter{RunIDs: []string{}, Unconfirmed: FCCauseCouldNotRead}},
	} {
		t.Run(name, func(t *testing.T) {
			got := after(t, tt.change, tt.known...)
			if got.Count != tt.want.Count || got.Unconfirmed != tt.want.Unconfirmed || !slices.Equal(got.RunIDs, tt.want.RunIDs) {
				t.Fatalf("%+v, want %+v", got, tt.want)
			}
		})
	}
}

// TestForceCancelOpen: which runs are open; one that cannot be told may be.
func TestForceCancelOpen(t *testing.T) {
	c := &belowCluster{runs: tree()}
	a, in, _ := cascadeActivities(t, recordKeys(t), "enforce", c)
	got, err := a.Open(context.Background(), FCOpenStep{Input: in, RunIDs: []string{"b", "closed", "gone", "a"}})
	if err != nil || !slices.Equal(got, []string{"a", "b"}) {
		t.Errorf("%v, %v", got, err)
	}
	c.runs["a"] = belowRun{err: errors.New("down")}
	if got, err := a.Open(context.Background(), FCOpenStep{Input: in, RunIDs: []string{"a", "closed", "b"}}); err != nil || !slices.Equal(got, []string{"a", "b"}) {
		t.Errorf("%v, %v", got, err)
	}
}

// TestForceCancelSummary: one record of a cascade, of its verified decision.
func TestForceCancelSummary(t *testing.T) {
	ctx, keys := context.Background(), recordKeys(t)
	a, in, d := cascadeActivities(t, keys, "enforce", &belowCluster{runs: tree()})
	if err := a.Summary(ctx, FCSummaryStep{Input: in}); err == nil {
		t.Error("recorded nowhere")
	}
	store := artifacts.NewMemoryAuditStore()
	a.audit = store
	if got := (ForceCancelCascade{State: "a\nb", Results: []FCBelowResult{{State: "x\ny"}}}).detail(); got != "unknown, unknown=1, what is open after it was not looked at" {
		t.Errorf("%q", got)
	}
	cascade := ForceCancelCascade{State: FCCascadePartial, Covered: 2, NotStarted: 1, Excluded: map[string]string{"z": FCExcludedNamespace}, After: &FCAfter{Count: 3, Unconfirmed: FCCauseTooManyRuns},
		Results: []FCBelowResult{{State: FCStateForceCanceled}, {State: FCStateCanceled}, {State: FCStateForceCanceled}, {State: FCBelowPending}}}
	for range 2 {
		must.OK(t, a.Summary(ctx, FCSummaryStep{Input: in, Cascade: cascade}))
	}
	page, err := store.ListAuditRecords(ctx, artifacts.AuditQuery{Since: d.IssuedAt.Add(-time.Hour), Until: d.IssuedAt.Add(time.Hour), Limit: 10})
	must.OK(t, err)
	if len(page.Records) != 1 {
		t.Fatalf("recorded %v", page.Records)
	}
	rec := page.Records[0]
	if rec.GetAction() != "run.cancel.force.cascade" || rec.GetSucceeded() || rec.GetPrincipal() != d.Principal || !rec.GetPrincipalVerified() || rec.GetTarget() != "ork-r" ||
		!strings.Contains(rec.GetDetail(), "partial, canceled=1, force_canceled=2, pending=1, left to their parents' cancel=2, not started=1, excluded=1, open after it=3, unconfirmed (too_many_runs)") {
		t.Errorf("%v", rec)
	}
}
