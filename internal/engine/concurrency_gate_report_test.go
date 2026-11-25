package engine

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	workflowpb "go.temporal.io/api/workflow/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/workflow"

	"github.com/orkstack/orkstack/internal/stablejson"
)

// TestGateApplyFindings_ReportsWhyEachEntryWasTakenBack: each entry a pass
// frees is counted once under its limit and why; one it keeps is not.
func TestGateApplyFindings_ReportsWhyEachEntryWasTakenBack(t *testing.T) {
	g := newTestGate()
	for _, r := range []string{"gone", "closed", "moved", "keyless", "alive", "unread"} {
		req := acq(r, 10, gateStrategyQueue, gateT0)
		if req.Label = "a"; r == "closed" {
			req.Label = "b"
		}
		g.decideAcquire(req, gateT0, false)
	}
	for i := range g.Holders {
		if g.Holders[i].Run == "keyless" {
			g.Holders[i].MissingSince = gateT0
		}
	}
	out := g.applyFindings([]gateRunFinding{
		{Run: "gone", NotFound: true}, {Run: "closed"}, {Run: "moved", Open: true, MemoSeq: 2},
		{Run: "keyless", Open: true, MemoSeq: 1}, alive("alive", 1), {Run: "unread", Err: "unavailable"},
	}, gateT0.Add(time.Minute))
	a, b := CensusLimitLabel("a"), CensusLimitLabel("b")
	want := map[gateReclaimKey]int{{a, gateReclaimNotFound}: 1, {b, gateReclaimClosed}: 1, {a, gateReclaimStale}: 1, {a, gateReclaimMissingSlot}: 1}
	if !reflect.DeepEqual(out.Reclaimed, want) {
		t.Fatalf("reclaimed = %v, want %v", out.Reclaimed, want)
	}
}

// TestGateLimitsOf_NamesEachRunsLimitsOnce: a run that holds twice under one
// limit and waits under another names each once, and a run not asked about
// is left out.
func TestGateLimitsOf_NamesEachRunsLimitsOnce(t *testing.T) {
	g := newTestGate()
	for _, e := range []struct {
		ix    int
		label string
	}{{0, "a"}, {1, "a"}, {2, "b"}} {
		req := acq("r", 1, gateStrategyQueue, gateT0)
		req.TaskIx, req.Label = e.ix, e.label
		g.decideAcquire(req, gateT0, false)
	}
	g.decideAcquire(acq("other", 9, gateStrategyQueue, gateT0), gateT0, false)
	want := map[string][]string{"r": {CensusLimitLabel("a"), CensusLimitLabel("b")}}
	if got := g.limitsOf([]string{"r"}); !reflect.DeepEqual(got, want) {
		t.Fatalf("limits = %v, want %v", got, want)
	}
}

// TestGateDropExpiredQueued_ReportsWhatItDropped: a place in line past its
// bound is reported by its limit, and one within it is not.
func TestGateDropExpiredQueued_ReportsWhatItDropped(t *testing.T) {
	g := heldGate(t, "holder")
	for i, r := range []string{"late", "later"} {
		req := acq(r, 1, gateStrategyQueue, gateT0)
		req.Label, req.WaitUntil = "a", gateT0.Add(time.Duration(i+1)*time.Minute)
		g.decideAcquire(req, gateT0, false)
	}
	want := map[gateReclaimKey]int{{CensusLimitLabel("a"), gateReclaimExpired}: 1}
	if got := g.dropExpiredQueued(gateT0.Add(time.Minute)); !reflect.DeepEqual(got, want) {
		t.Fatalf("dropped = %v, want %v", got, want)
	}
	if got := g.dropExpiredQueued(gateT0.Add(time.Minute)); len(got) != 0 {
		t.Fatalf("dropped again = %v, want nothing", got)
	}
}

// reclaimedBy totals reports' counts by outcome, and fails one that carries
// anything else: a gate reports only what it took back.
func reclaimedBy(t *testing.T, reports []ConcurrencyGateReport) map[string]int {
	t.Helper()
	out := map[string]int{}
	for _, r := range reports {
		if r.Namespace != "default" || r.Started || len(r.VerifyErrors)+len(r.Rebuilt) > 0 || len(r.Reclaimed) == 0 {
			t.Fatalf("report %+v, want only what was taken back, in namespace default", r)
		}
		for _, c := range r.Reclaimed {
			out[c.Outcome] += c.Entries
		}
	}
	return out
}

// TestGateWorkflow_ReportsWhatAPassTookBack: a pass that takes a slot back
// reports it once; one that could not check a run, and one with nothing to
// say, report nothing. Every check is told each run's limits.
func TestGateWorkflow_ReportsWhatAPassTookBack(t *testing.T) {
	h := newGateHarness(t)
	var first DescribeConcurrencyEntriesInput
	h.describe = func(in DescribeConcurrencyEntriesInput) ([]gateRunFinding, error) {
		if h.passes == 1 {
			first = in
		}
		var out []gateRunFinding
		for _, r := range in.Runs {
			f := gateRunFinding{Run: r, Open: true, MemoSeq: 1, Keys: map[int]bool{gateRunLevel: true}}
			switch {
			case r == "gone":
				f = gateRunFinding{Run: r, NotFound: true}
			case h.passes == 2:
				f = gateRunFinding{Run: r, Err: "unavailable"}
			}
			out = append(out, f)
		}
		return out, nil
	}
	tunables := DefaultGateTunables()
	tunables.HolderSweepInterval = time.Minute
	h.acquireAt(time.Second, runAcquire("gone", 2, gateStrategyQueue))
	h.acquireAt(time.Second, runAcquire("kept", 2, gateStrategyQueue))
	h.signalAt(3*time.Minute+30*time.Second, GateSignalRelease, GateRelease{Run: "kept", SeqBase: 1, TaskIx: gateRunLevel})
	h.run(GatePreviousClean, tunables)
	h.closedEmpty()
	if got := reclaimedBy(t, h.reports); h.passes < 3 || len(h.reports) != 1 || !reflect.DeepEqual(got, map[string]int{gateReclaimNotFound: 1}) {
		t.Fatalf("%d passes, reports = %+v; want one slot taken back, once", h.passes, h.reports)
	}
	limit := []string{CensusLimitLabel("deploy")}
	if want := map[string][]string{"gone": limit, "kept": limit}; first.Namespace != "default" || !reflect.DeepEqual(first.Limits, want) {
		t.Fatalf("the check was asked with %+v, want namespace default and limits %v", first, want)
	}
}

// TestGateWorkflow_TellsARebuildWhatItHasRead: each page is asked with the
// entries read so far and the most the gate takes, so the page that ends the
// attempt can count it; a start, and a rebuild, report nothing themselves.
func TestGateWorkflow_TellsARebuildWhatItHasRead(t *testing.T) {
	h := newGateHarness(t)
	memo, err := stablejson.Marshal(gateMemoEntry{Proto: 1, Held: true, Limit: 2, Strategy: gateStrategyQueue, ArrivedAt: h.env.Now(), Seq: 1, SeqBase: 1})
	if err != nil {
		t.Fatal(err)
	}
	var asked []ListConcurrencyEntriesInput
	h.list = func(in ListConcurrencyEntriesInput) (ListConcurrencyEntriesResult, error) {
		if asked = append(asked, in); len(asked) == 1 {
			return ListConcurrencyEntriesResult{NextToken: []byte("2"), Records: []gateRunRecord{
				{Run: "old", MemoSeq: 1, Entries: map[int]stablejson.RawJSON{gateRunLevel: memo}}}}, nil
		}
		return ListConcurrencyEntriesResult{}, nil
	}
	h.signalAt(time.Minute, GateSignalRelease, GateRelease{Run: "old", SeqBase: 1, TaskIx: gateRunLevel})
	tunables := DefaultGateTunables()
	h.run(GatePreviousAbsent, tunables)
	h.closedEmpty()
	if len(asked) != 2 || asked[0].Seen != 0 || asked[1].Seen != 1 || asked[1].MaxEntries != tunables.MaxEntries || asked[1].Namespace != "default" {
		t.Fatalf("pages asked with %+v, want 0 then 1 entries seen of %d in namespace default", asked, tunables.MaxEntries)
	}
	if len(h.reports) != 0 {
		t.Fatalf("a start and a rebuild reported %+v, want nothing", h.reports)
	}
}

// TestGateWorkflow_ReportsAnExpiredPlaceBeforeItCloses: a place in line
// dropped outside a pass is reported even when the gate then closes.
func TestGateWorkflow_ReportsAnExpiredPlaceBeforeItCloses(t *testing.T) {
	h := newGateHarness(t)
	memo, err := stablejson.Marshal(gateMemoEntry{Proto: 1, Limit: 1, Strategy: gateStrategyQueue, Label: "a",
		ArrivedAt: h.env.Now(), Seq: 1, SeqBase: 1, WaitUntil: h.env.Now().Add(time.Second)})
	if err != nil {
		t.Fatal(err)
	}
	h.list = func(ListConcurrencyEntriesInput) (ListConcurrencyEntriesResult, error) {
		return ListConcurrencyEntriesResult{Records: []gateRunRecord{{Run: "late", MemoSeq: 1, Entries: map[int]stablejson.RawJSON{gateRunLevel: memo}}}}, nil
	}
	h.run(GatePreviousUnclean, DefaultGateTunables()) // it settles, so nothing is granted first
	h.closedEmpty()
	if got := reclaimedBy(t, h.reports); !reflect.DeepEqual(got, map[string]int{gateReclaimExpired: 1}) {
		t.Fatalf("reclaimed = %v, want the expired place", got)
	}
}

// TestGateWorkflow_ReportsAnExpiredPlaceBeforeItContinues: the counts are no
// part of the carry, so they are reported before it.
func TestGateWorkflow_ReportsAnExpiredPlaceBeforeItContinues(t *testing.T) {
	h := newGateHarness(t)
	tunables := DefaultGateTunables()
	tunables.ContinueUpdates = 0
	carry := &gateState{Tunables: tunables, Phase: gatePhaseActive, Seq: 2, Labels: []string{"a"}}
	carry.Holders = []gateEntry{{Run: "holder", TaskIx: gateRunLevel, SeqBase: 1, Limit: 1, Strategy: gateStrategyQueue, Proto: gateProto, Seq: 1, GrantedAt: h.env.Now()}}
	carry.Queue = []gateEntry{{Run: "late", TaskIx: gateRunLevel, SeqBase: 1, Limit: 1, Strategy: gateStrategyQueue, Proto: gateProto, Seq: 2,
		ArrivedAt: h.env.Now(), WaitUntil: h.env.Now().Add(time.Second)}}
	h.acquireAt(2*time.Second, runAcquire("next", 1, gateStrategyQueue))
	h.env.ExecuteWorkflow(ConcurrencyGateWorkflowType, GateStart{GateID: GateID("default", "", "deploy"), Namespace: "default", Tunables: tunables, Carry: carry})
	var can *workflow.ContinueAsNewError
	if err := h.env.GetWorkflowError(); !errors.As(err, &can) {
		t.Fatalf("the gate ended %v, want it carried on", err)
	}
	if reclaimed := reclaimedBy(t, h.reports); h.passes != 0 || !reflect.DeepEqual(reclaimed, map[string]int{gateReclaimExpired: 1}) {
		t.Fatalf("%d passes, reclaimed = %v; want the expired place, with no pass", h.passes, reclaimed)
	}
}

// TestGateWorkflow_AReportThatFailsChangesNothing: a gate decides the same
// whether or not its report could be made, and asks once.
func TestGateWorkflow_AReportThatFailsChangesNothing(t *testing.T) {
	h := newGateHarness(t)
	h.reportErr = errors.New("no meter")
	h.describe = func(in DescribeConcurrencyEntriesInput) ([]gateRunFinding, error) {
		var out []gateRunFinding
		for _, r := range in.Runs {
			out = append(out, gateRunFinding{Run: r, NotFound: r == "gone"})
		}
		return out, nil
	}
	h.acquireAt(time.Second, runAcquire("gone", 1, gateStrategyQueue))
	out := h.acquireAt(time.Minute, runAcquire("next", 1, gateStrategyRefuse))
	h.signalAt(2*time.Minute, GateSignalRelease, GateRelease{Run: "next", SeqBase: 1, TaskIx: gateRunLevel})
	h.run(GatePreviousClean, DefaultGateTunables())
	h.closedEmpty()
	if out.answer.Answer != gateAnswerGranted || len(h.reports) != 1 {
		t.Fatalf("answer %+v after %d reports, want granted after one", out, len(h.reports))
	}
}

type gateReportRecorder struct{ got []ConcurrencyGateReport }

func (r *gateReportRecorder) ObserveGateReport(_ context.Context, rep ConcurrencyGateReport) {
	r.got = append(r.got, rep)
}

// TestReportConcurrencyGate_HandsTheReportToItsObserver, and without one
// does nothing.
func TestReportConcurrencyGate_HandsTheReportToItsObserver(t *testing.T) {
	rec := &gateReportRecorder{}
	rep := ConcurrencyGateReport{Namespace: "acme", Started: true}
	if err := (&GateActivities{}).WithGateReportObserver(rec).ReportConcurrencyGate(context.Background(), rep); err != nil {
		t.Fatal(err)
	}
	if err := (&GateActivities{}).ReportConcurrencyGate(context.Background(), rep); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(rec.got, []ConcurrencyGateReport{rep}) {
		t.Fatalf("observed %+v, want the report once", rec.got)
	}
}

// gateReadClient is a cluster whose describes and lists fail, or answer as
// the test says: a fault a real cluster does not produce on demand, so it is
// faked here.
type gateReadClient struct {
	client.Client
	err  error
	list *workflowservice.ListWorkflowExecutionsResponse
}

func (f *gateReadClient) DescribeWorkflow(context.Context, string, string) (*client.WorkflowExecutionDescription, error) {
	return nil, f.err
}

func (f *gateReadClient) ListWorkflow(context.Context, *workflowservice.ListWorkflowExecutionsRequest) (*workflowservice.ListWorkflowExecutionsResponse, error) {
	return f.list, f.err
}

// TestDescribeConcurrencyEntries_CountsTheRunsItCouldNotCheck: a run that
// could not be checked is counted once under each of its limits, a run that
// is gone is not an error, and a check cut short by a stopping worker counts
// nothing.
func TestDescribeConcurrencyEntries_CountsTheRunsItCouldNotCheck(t *testing.T) {
	in := DescribeConcurrencyEntriesInput{GateID: "g", Namespace: "acme", Parallel: 2, Runs: []string{"r1", "r2"},
		Limits: map[string][]string{"r1": {"a#1", "b#2"}, "r2": {"a#1"}}}
	stopping, cancel := context.WithCancel(context.Background())
	cancel()
	late, lcancel := context.WithTimeout(context.Background(), 0)
	defer lcancel()
	counted := []ConcurrencyGateReport{{Namespace: "acme", VerifyErrors: []GateVerifyErrors{{Limit: "a#1", Calls: 2}, {Limit: "b#2", Calls: 1}}}}
	for name, c := range map[string]struct {
		ctx  context.Context
		err  error
		want []ConcurrencyGateReport
	}{
		"unreachable":          {context.Background(), serviceerror.NewUnavailable("frontend down"), counted},
		"past its deadline":    {late, context.DeadlineExceeded, counted},
		"gone":                 {context.Background(), serviceerror.NewNotFound("no such run"), nil},
		"the worker is ending": {stopping, context.Canceled, nil},
	} {
		rec := &gateReportRecorder{}
		a := NewGateActivities(&gateReadClient{err: c.err}).WithGateReportObserver(rec)
		if _, err := a.DescribeConcurrencyEntries(c.ctx, in); err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		if !reflect.DeepEqual(rec.got, c.want) {
			t.Errorf("%s: counted %+v, want %+v", name, rec.got, c.want)
		}
	}
}

// TestListConcurrencyEntries_CountsEachRebuildAttemptOnce: the page that ends
// an attempt counts it: the last one, one that cannot be read, or one past
// the most the gate takes; a page in between counts nothing.
func TestListConcurrencyEntries_CountsEachRebuildAttemptOnce(t *testing.T) {
	page := func(token string) *workflowservice.ListWorkflowExecutionsResponse {
		return &workflowservice.ListWorkflowExecutionsResponse{NextPageToken: []byte(token), Executions: []*workflowpb.WorkflowExecutionInfo{{
			Execution: &commonpb.WorkflowExecution{WorkflowId: "r"},
			Memo:      &commonpb.Memo{Fields: map[string]*commonpb.Payload{gateMemoKey("g", gateRunLevel): {Data: []byte("{}")}}},
		}}}
	}
	for name, c := range map[string]struct {
		client gateReadClient
		seen   int
		want   string
	}{
		"the last page":    {gateReadClient{list: page("")}, 1, GateRebuiltOK},
		"a page before it": {gateReadClient{list: page("more")}, 1, ""},
		"past max_entries": {gateReadClient{list: page("more")}, 2, GateRebuiltFailedAttempt},
		"past it, last":    {gateReadClient{list: page("")}, 2, GateRebuiltFailedAttempt},
		"unreadable":       {gateReadClient{err: errors.New("the index is down")}, 0, GateRebuiltFailedAttempt},
	} {
		rec := &gateReportRecorder{}
		a := NewGateActivities(&c.client).WithGateReportObserver(rec)
		_, _ = a.ListConcurrencyEntries(context.Background(), ListConcurrencyEntriesInput{GateID: "g", Namespace: "acme", Seen: c.seen, MaxEntries: 2})
		var want []ConcurrencyGateReport
		if c.want != "" {
			want = []ConcurrencyGateReport{{Namespace: "acme", Rebuilt: []GateRebuilt{{Outcome: c.want, Times: 1}}}}
		}
		if !reflect.DeepEqual(rec.got, want) {
			t.Errorf("%s: counted %+v, want %+v", name, rec.got, want)
		}
	}
}

// gateStartClient is a cluster with no running gate: the Update finds none,
// and the start with it is taken, or refused with startErr, a fault a real
// cluster does not produce on demand.
type gateStartClient struct {
	gateReadClient
	previous *client.WorkflowExecutionDescription
	startErr error
}

func (f *gateStartClient) UpdateWorkflow(context.Context, client.UpdateWorkflowOptions) (client.WorkflowUpdateHandle, error) {
	return nil, serviceerror.NewNotFound("no gate")
}

func (f *gateStartClient) DescribeWorkflow(context.Context, string, string) (*client.WorkflowExecutionDescription, error) {
	if f.previous == nil {
		return nil, serviceerror.NewNotFound("no gate")
	}
	return f.previous, nil
}

func (f *gateStartClient) NewWithStartWorkflowOperation(client.StartWorkflowOptions, any, ...any) client.WithStartWorkflowOperation {
	return nil
}

func (f *gateStartClient) UpdateWithStartWorkflow(context.Context, client.UpdateWithStartWorkflowOptions) (client.WorkflowUpdateHandle, error) {
	if f.startErr != nil {
		return nil, f.startErr
	}
	return gateUpdateHandle{ans: gateAnswer{Proto: 1, Answer: gateAnswerGranted, Seq: 1}}, nil
}

// TestAcquireConcurrency_CountsAGateItStarts: a start is counted once, with
// its rebuild skipped after a clean end; one the cluster refused is not.
func TestAcquireConcurrency_CountsAGateItStarts(t *testing.T) {
	ended := func(status enumspb.WorkflowExecutionStatus) *client.WorkflowExecutionDescription {
		return &client.WorkflowExecutionDescription{RawResponse: &workflowservice.DescribeWorkflowExecutionResponse{
			WorkflowExecutionInfo: &workflowpb.WorkflowExecutionInfo{Status: status}}}
	}
	started := ConcurrencyGateReport{Namespace: "acme", Started: true}
	skipped := started
	skipped.Rebuilt = []GateRebuilt{{Outcome: GateRebuiltSkipped, Times: 1}}
	for name, c := range map[string]struct {
		client gateStartClient
		want   []ConcurrencyGateReport
	}{
		"none before":    {gateStartClient{}, []ConcurrencyGateReport{started}},
		"a clean end":    {gateStartClient{previous: ended(enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED)}, []ConcurrencyGateReport{skipped}},
		"an unclean end": {gateStartClient{previous: ended(enumspb.WORKFLOW_EXECUTION_STATUS_TERMINATED)}, []ConcurrencyGateReport{started}},
		"refused":        {gateStartClient{startErr: serviceerror.NewPermissionDenied("no", "")}, nil},
	} {
		rec := &gateReportRecorder{}
		a := NewGateActivities(&c.client).WithGateReportObserver(rec)
		if _, err := a.AcquireConcurrency(context.Background(), AcquireConcurrencyInput{GateID: "g", Start: GateStart{Namespace: "acme"}}); err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		if !reflect.DeepEqual(rec.got, c.want) {
			t.Errorf("%s: counted %+v, want %+v", name, rec.got, c.want)
		}
	}
}
