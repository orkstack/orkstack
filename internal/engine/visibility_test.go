package engine

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	workflowpb "go.temporal.io/api/workflow/v1"
	"go.temporal.io/sdk/converter"
	"google.golang.org/protobuf/types/known/timestamppb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/task"
)

// A chain is never invisible to a liveness question, and settling it is
// cheap. These tests drive the readers over the query-evaluating index
// (statsIndex) with the records a chain leaves while the cluster's index
// catches up with a switch.

var visibilityNow = time.Date(2026, 10, 13, 12, 0, 0, 0, time.UTC)

const (
	running   = enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING
	continued = enumspb.WORKFLOW_EXECUTION_STATUS_CONTINUED_AS_NEW
	completed = enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED
)

// record is one visibility record of workflow id: started at start, closed at
// closed (zero = open), its run created at created (zero = no created_at memo,
// a run that never switched).
type record struct {
	id      string
	status  enumspb.WorkflowExecutionStatus
	start   time.Time
	closed  time.Time
	created time.Time
	ns      string
	version string
}

func (r record) indexed(t *testing.T) indexedExecution {
	t.Helper()
	dc := converter.GetDefaultDataConverter()
	payload := func(v string) *commonpb.Payload {
		p, err := dc.ToPayload(v)
		if err != nil {
			t.Fatal(err)
		}
		return p
	}
	ns := r.ns
	if ns == "" {
		ns = "payments"
	}
	info := &workflowpb.WorkflowExecutionInfo{
		Execution: &commonpb.WorkflowExecution{WorkflowId: r.id, RunId: fmt.Sprintf("%s@%d", r.id, r.start.Unix())},
		Type:      &commonpb.WorkflowType{Name: RunWorkflowType},
		Status:    r.status,
		StartTime: timestamppb.New(r.start),
		Memo: &commonpb.Memo{Fields: map[string]*commonpb.Payload{
			MemoNamespace: payload(ns), MemoOrkflowName: payload("reconcile"),
		}},
	}
	if !r.closed.IsZero() {
		info.CloseTime = timestamppb.New(r.closed)
	}
	if !r.created.IsZero() {
		info.Memo.Fields[MemoCreatedAt] = payload(r.created.UTC().Format(time.RFC3339Nano))
	}
	return indexedExecution{info: info, namespace: ns, version: r.version}
}

func indexOf(t *testing.T, records ...record) *statsIndex {
	t.Helper()
	index := &statsIndex{describes: map[string]*workflowpb.WorkflowExecutionInfo{}}
	for _, r := range records {
		index.records = append(index.records, r.indexed(t))
	}
	return index
}

func (s *statsIndex) indexLater(t *testing.T, records ...record) {
	t.Helper()
	for _, r := range records {
		s.idRecords = append(s.idRecords, r.indexed(t))
	}
}

func (s *statsIndex) describeAs(id string, status enumspb.WorkflowExecutionStatus) {
	info := &workflowpb.WorkflowExecutionInfo{Execution: &commonpb.WorkflowExecution{WorkflowId: id}, Status: status}
	if status != running {
		info.CloseTime = timestamppb.New(visibilityNow)
	}
	s.describes[id] = info
}

func readerOverIndex(index *statsIndex) *TemporalRunReader {
	r := newRunReader(index, converter.GetDefaultDataConverter())
	r.now = func() time.Time { return visibilityNow }
	return r
}

// switched is a chain's generation 0 record, closed ContinuedAsNew minutes
// ago, created at created.
func switched(id string, created time.Time, minutesAgo int) record {
	return record{id: id, status: continued, start: created, created: created,
		closed: visibilityNow.Add(-time.Duration(minutesAgo) * time.Minute)}
}

func successor(id string, created time.Time, status enumspb.WorkflowExecutionStatus) record {
	r := record{id: id, status: status, start: visibilityNow.Add(-time.Minute), created: created}
	if status != running {
		r.closed = visibilityNow.Add(-30 * time.Second)
	}
	return r
}

func TestListQuery_NeverExcludesContinuedAsNewUnconditionally(t *testing.T) {
	reader := readerOverIndex(&statsIndex{}).WithNamespaceIndex(true).WithOriginIndex(true)
	for _, status := range []orkstackv1.Status{
		orkstackv1.Status_STATUS_UNSPECIFIED, orkstackv1.Status_STATUS_RUNNING, orkstackv1.Status_STATUS_SUCCEEDED,
		orkstackv1.Status_STATUS_FAILED, orkstackv1.Status_STATUS_CANCELED,
	} {
		for _, q := range []RunQuery{
			{Status: status},
			{Status: status, Namespace: "payments"},
			{Status: status, OrkflowName: "x", OriginKind: orkstackv1.RunOriginKind_RUN_ORIGIN_KIND_TRIGGER, OriginID: "trg-1"},
		} {
			query, _, _ := reader.listQuery(q)
			if strings.Contains(query, `!= "ContinuedAsNew"`) {
				t.Fatalf("%v: query %q excludes ContinuedAsNew", q.Status, query)
			}
			if status == orkstackv1.Status_STATUS_RUNNING && !strings.Contains(query, liveRunPredicate(visibilityNow)) {
				t.Fatalf("RUNNING query %q does not carry the live-run predicate", query)
			}
		}
	}
}

func TestListRuns_ChainSeenOnlyAsContinuedAsNewIsRunning(t *testing.T) {
	created := visibilityNow.Add(-3 * time.Hour)
	index := indexOf(t, switched("ork-chain", created, 2))
	runs, err := readerOverIndex(index).ListRuns(context.Background(), RunQuery{Exhaustive: true})
	if err != nil {
		t.Fatalf("ListRuns: %v", err)
	}
	if len(runs) != 1 {
		t.Fatalf("listed %d runs, want the chain", len(runs))
	}
	run := runs[0]
	if run.GetStatus() != orkstackv1.Status_STATUS_RUNNING || run.GetFinishedAt() != nil ||
		!run.GetCreatedAt().AsTime().Equal(created) {
		t.Fatalf("chain = %v %v %v, want RUNNING, unfinished, created at the memo's time", run.GetStatus(), run.GetFinishedAt(), run.GetCreatedAt())
	}
}

func TestListRuns_CreatedAtFromTheContinuationMemo(t *testing.T) {
	created := visibilityNow.Add(-26 * time.Hour)
	index := indexOf(t, successor("ork-chain", created, running), record{id: "ork-plain", status: running, start: visibilityNow.Add(-time.Hour)})
	runs, err := readerOverIndex(index).ListRuns(context.Background(), RunQuery{Complete: true, Limit: 10})
	if err != nil {
		t.Fatal(err)
	}
	if len(runs) != 2 || runs[1].GetId() != "ork-chain" || !runs[1].GetCreatedAt().AsTime().Equal(created) {
		t.Fatalf("runs = %v, want the chain last, at its memo created_at", runs)
	}
	if !runs[0].GetCreatedAt().AsTime().Equal(visibilityNow.Add(-time.Hour)) {
		t.Fatalf("a run without the memo is created at its StartTime: %v", runs[0].GetCreatedAt())
	}
}

func TestListRuns_RunningFilterSettlesAContinuedAsNewOnlyChain(t *testing.T) {
	created := visibilityNow.Add(-time.Hour)
	list := func(t *testing.T, index *statsIndex) ([]*orkstackv1.Run, error) {
		t.Helper()
		return readerOverIndex(index).ListRuns(context.Background(),
			RunQuery{Status: orkstackv1.Status_STATUS_RUNNING, Complete: true, Limit: 10})
	}
	t.Run("successor now indexed and running: listed", func(t *testing.T) {
		index := indexOf(t, switched("ork-chain", created, 1))
		index.indexLater(t, successor("ork-chain", created, running))
		runs, err := list(t, index)
		if err != nil || len(runs) != 1 || runs[0].GetStatus() != orkstackv1.Status_STATUS_RUNNING {
			t.Fatalf("runs = %v, %v", runs, err)
		}
		if index.count("describe") != 0 {
			t.Fatal("a chain whose successor is indexed was described")
		}
	})
	t.Run("final record completed: not listed, no describe", func(t *testing.T) {
		index := indexOf(t, switched("ork-chain", created, 1))
		index.indexLater(t, successor("ork-chain", created, completed))
		runs, err := list(t, index)
		if err != nil || len(runs) != 0 || index.count("describe") != 0 {
			t.Fatalf("runs = %v, %v, %d describes; want none, none", runs, err, index.count("describe"))
		}
	})
	for _, tc := range []struct {
		name   string
		status enumspb.WorkflowExecutionStatus
		want   int
	}{{"still mid-switch, described running: listed", running, 1}, {"still mid-switch, described completed: not listed", completed, 0}} {
		t.Run(tc.name, func(t *testing.T) {
			index := indexOf(t, switched("ork-chain", created, 1))
			index.describeAs("ork-chain", tc.status)
			runs, err := list(t, index)
			if err != nil || len(runs) != tc.want || index.count("describe") != 1 {
				t.Fatalf("runs = %v, %v, %d describes", runs, err, index.count("describe"))
			}
		})
	}
	t.Run("a describe error fails the list", func(t *testing.T) {
		index := indexOf(t, switched("ork-chain", created, 1))
		index.describeErr = errors.New("frontend unavailable")
		if _, err := list(t, index); err == nil {
			t.Fatal("ListRuns succeeded over an unsettled chain")
		}
	})
}

func TestResolveChains_DescribesOnlyChainsStillCaughtMidSwitch(t *testing.T) {
	created := visibilityNow.Add(-time.Hour)
	var walk []record
	index := &statsIndex{describes: map[string]*workflowpb.WorkflowExecutionInfo{}}
	for i := 0; i < 100; i++ {
		id := fmt.Sprintf("ork-c%03d", i)
		walk = append(walk, switched(id, created, 5))
		switch {
		case i < 60:
			index.indexLater(t, successor(id, created, completed))
		case i < 99:
			index.indexLater(t, successor(id, created, running))
		default:
			index.describeAs(id, running)
		}
	}
	var infos []*workflowpb.WorkflowExecutionInfo
	for _, r := range walk {
		infos = append(infos, r.indexed(t).info)
	}
	live, settled, err := readerOverIndex(index).resolveChains(context.Background(), infos)
	if err != nil {
		t.Fatal(err)
	}
	if len(live) != 40 || len(settled) != 100 {
		t.Fatalf("live %d, settled %d; want 40 live of 100", len(live), len(settled))
	}
	if index.count("list") != 1 || index.count("describe") != 1 {
		t.Fatalf("%d lists, %d describes; want exactly 1 and 1", index.count("list"), index.count("describe"))
	}
}

func TestResolveChains_FallsBackToOneListPerID(t *testing.T) {
	created := visibilityNow.Add(-time.Hour)
	index := &statsIndex{describes: map[string]*workflowpb.WorkflowExecutionInfo{}, rejectIN: true}
	var infos []*workflowpb.WorkflowExecutionInfo
	for i, status := range []enumspb.WorkflowExecutionStatus{completed, running, running} {
		id := fmt.Sprintf("ork-c%d", i)
		infos = append(infos, switched(id, created, 5).indexed(t).info)
		index.indexLater(t, successor(id, created, status))
	}
	reader := readerOverIndex(index)
	live, _, err := reader.resolveChains(context.Background(), infos)
	if err != nil {
		t.Fatal(err)
	}
	if len(live) != 2 {
		t.Fatalf("live = %d, want 2", len(live))
	}
	// One refused IN, then one list per id; later calls skip IN for good.
	if index.count("list") != 4 || !reader.chainINUnsupported.Load() {
		t.Fatalf("%d lists (IN unsupported %v), want 1 + 3", index.count("list"), reader.chainINUnsupported.Load())
	}
	if _, _, err := reader.resolveChains(context.Background(), infos); err != nil {
		t.Fatal(err)
	}
	if index.count("list") != 7 {
		t.Fatalf("%d lists after a second settle, want 3 more and no IN", index.count("list"))
	}
}

func TestListRuns_DeduplicatesAChainByLatestStart(t *testing.T) {
	created := visibilityNow.Add(-2 * time.Hour)
	index := indexOf(t,
		record{id: "ork-chain", status: running, start: created, created: created}, // generation 0, not yet updated
		switched("ork-chain", created, 1),                                         // the same generation, updated
		successor("ork-chain", created, running),                                  // generation 1
	)
	runs, err := readerOverIndex(index).ListRuns(context.Background(), RunQuery{Complete: true, Limit: 10})
	if err != nil || len(runs) != 1 {
		t.Fatalf("runs = %v, %v; want one row", runs, err)
	}
	// On a tie a non-ContinuedAsNew record wins.
	tie := []*workflowpb.WorkflowExecutionInfo{
		switched("ork-t", created, 1).indexed(t).info,
		record{id: "ork-t", status: running, start: created, created: created}.indexed(t).info,
	}
	if got := dedupeChains(tie); len(got) != 1 || got[0].GetStatus() != running {
		t.Fatalf("tie kept %v", got)
	}
}

func TestListRuns_DedupeRunsBeforeEveryTrim(t *testing.T) {
	// The chain is the newest run, so a duplicate would survive every trim.
	created := visibilityNow.Add(-10 * time.Minute)
	index := indexOf(t,
		successor("ork-chain", created, running),
		record{id: "ork-a", status: running, start: visibilityNow.Add(-50 * time.Minute)},
		record{id: "ork-b", status: running, start: visibilityNow.Add(-40 * time.Minute)},
		record{id: "ork-c", status: running, start: visibilityNow.Add(-30 * time.Minute)},
		record{id: "ork-chain", status: running, start: created, created: created},
	)
	index.pageLimit = 1
	runs, err := readerOverIndex(index).ListRuns(context.Background(), RunQuery{Complete: true, Limit: 4})
	if err != nil {
		t.Fatal(err)
	}
	seen := map[string]int{}
	for _, run := range runs {
		seen[run.GetId()]++
	}
	if seen["ork-chain"] != 1 || len(runs) != 4 {
		t.Fatalf("runs = %v, want the chain once among 4", sequencesOfIDs(runs))
	}
}

func sequencesOfIDs(runs []*orkstackv1.Run) []string {
	out := make([]string, 0, len(runs))
	for _, run := range runs {
		out = append(out, run.GetId())
	}
	return out
}

func TestListRuns_CompletePageOrdersAnOldChainAmongNewerRuns(t *testing.T) {
	created := visibilityNow.Add(-5 * time.Hour)
	index := indexOf(t,
		successor("ork-old-chain", created, running), // newest StartTime, oldest run
		record{id: "ork-new", status: running, start: visibilityNow.Add(-2 * time.Hour)},
		record{id: "ork-mid", status: running, start: visibilityNow.Add(-3 * time.Hour)},
	)
	reader := readerOverIndex(index)
	runs, err := reader.ListRuns(context.Background(), RunQuery{Complete: true, Limit: 2})
	if err != nil {
		t.Fatal(err)
	}
	if got := sequencesOfIDs(runs); strings.Join(got, ",") != "ork-new,ork-mid" {
		t.Fatalf("first page = %v, want the newer runs, the old chain on the next", got)
	}
	next, err := reader.ListRuns(context.Background(), RunQuery{Complete: true, Limit: 2,
		BeforeCreated: runs[1].GetCreatedAt().AsTime(), BeforeID: runs[1].GetId()})
	if err != nil {
		t.Fatal(err)
	}
	if got := sequencesOfIDs(next); strings.Join(got, ",") != "ork-old-chain" {
		t.Fatalf("second page = %v, want the chain placed by its created_at", got)
	}
}

func TestListRuns_DeletionGateCountsAChainOnce(t *testing.T) {
	created := visibilityNow.Add(-2 * time.Hour)
	index := indexOf(t, switched("ork-chain", created, 1), successor("ork-chain", created, running),
		record{id: "ork-done", status: completed, start: created, closed: created.Add(time.Minute)})
	runs, err := readerOverIndex(index).ListRuns(context.Background(), RunQuery{OrkflowName: "reconcile", Exhaustive: true})
	if err != nil {
		t.Fatal(err)
	}
	active := 0
	for _, run := range runs {
		if run.GetStatus() == orkstackv1.Status_STATUS_RUNNING {
			active++
		}
	}
	if len(runs) != 2 || active != 1 {
		t.Fatalf("runs = %v, want the chain once (RUNNING) and the finished run", sequencesOfIDs(runs))
	}
}

func TestLiveRunCount_AsksOpenBeforeRecent(t *testing.T) {
	index := indexOf(t, record{id: "ork-a", status: running, start: visibilityNow.Add(-time.Hour)},
		record{id: "ork-b", status: running, start: visibilityNow.Add(-time.Hour)})
	got, err := readerOverIndex(index).liveRunCount(context.Background(), fmt.Sprintf("WorkflowType = %q", RunWorkflowType), 0)
	if err != nil || got != 2 {
		t.Fatalf("liveRunCount = %d, %v; want 2", got, err)
	}
	if strings.Join(index.calls, ",") != "count,count" {
		t.Fatalf("calls = %v, want two counts and nothing else", index.calls)
	}
	if !strings.Contains(index.counts[0], openExecutionsPredicate()) || !strings.Contains(index.counts[1], `ExecutionStatus = "ContinuedAsNew" AND CloseTime >=`) {
		t.Fatalf("counts = %q, want open first, then recent continuations", index.counts)
	}
}

func TestLiveRunCount_ChainSeenOnlyAsContinuedAsNewIsCounted(t *testing.T) {
	created := visibilityNow.Add(-time.Hour)
	chain := switched("ork-chain", created, 1)
	chain.version = "orkstack:v2"
	chain.ns = "payments"
	ctx := context.Background()
	t.Run("RunningCount", func(t *testing.T) {
		for _, indexed := range []bool{true, false} {
			index := indexOf(t, chain)
			index.describeAs("ork-chain", running)
			got, err := readerOverIndex(index).WithNamespaceIndex(indexed).RunningCount(ctx, "payments", 1)
			if err != nil || got != 1 {
				t.Fatalf("indexed=%v: RunningCount = %d, %v; want 1, never 0", indexed, got, err)
			}
		}
	})
	t.Run("RunningRuns", func(t *testing.T) {
		index := indexOf(t, chain)
		index.describeAs("ork-chain", running)
		got, err := readerOverIndex(index).RunningRuns(ctx)
		if err != nil || got != 1 {
			t.Fatalf("RunningRuns = %d, %v; want 1", got, err)
		}
	})
	t.Run("ReleaseRuns", func(t *testing.T) {
		index := indexOf(t, chain)
		index.describeAs("ork-chain", running)
		got, runs, err := readerOverIndex(index).ReleaseRuns(ctx, "orkstack", "v2", 10)
		if err != nil || got != 1 || len(runs) != 1 || runs[0].GetStatus() != orkstackv1.Status_STATUS_RUNNING {
			t.Fatalf("ReleaseRuns = %d %v, %v; want 1 RUNNING", got, runs, err)
		}
	})
}

func TestLiveRunCount_ExactWhenNothingIsOpen(t *testing.T) {
	created := visibilityNow.Add(-time.Hour)
	index := indexOf(t, switched("ork-mid", created, 2), switched("ork-indexed", created, 2), switched("ork-done", created, 2))
	index.describeAs("ork-mid", running)
	index.indexLater(t, successor("ork-indexed", created, running), successor("ork-done", created, completed))
	got, err := readerOverIndex(index).liveRunCount(context.Background(), fmt.Sprintf("WorkflowType = %q", RunWorkflowType), 0)
	if err != nil || got != 2 {
		t.Fatalf("liveRunCount = %d, %v; want 2 (mid-switch and newly indexed; the finished one not)", got, err)
	}
}

func TestLiveRunCount_ReleaseCountsNeverWalkTheLivePopulation(t *testing.T) {
	created := visibilityNow.Add(-time.Hour)
	var records []record
	for i := 0; i < 5000; i++ {
		records = append(records, record{id: fmt.Sprintf("ork-%04d", i), status: running, start: visibilityNow.Add(-time.Minute)})
	}
	for i := 0; i < 3; i++ {
		records = append(records, switched(fmt.Sprintf("ork-chain-%d", i), created, 10))
	}
	index := indexOf(t, records...)
	for i := 0; i < 3; i++ {
		index.indexLater(t, successor(fmt.Sprintf("ork-chain-%d", i), created, completed))
	}
	got, err := readerOverIndex(index).RunningRuns(context.Background())
	if err != nil || got != 5000 {
		t.Fatalf("RunningRuns = %d, %v; want 5000", got, err)
	}
	if index.count("list") != 2 {
		t.Fatalf("%d lists: %q; want the 3 recent records and one by-id batch", index.count("list"), index.queries)
	}
	for _, query := range index.queries {
		if strings.Contains(query, "NOT IN") {
			t.Fatalf("the live population was listed: %q", query)
		}
	}
}

func TestRunningCount_ConfirmsAtTheCeilingByDistinctWorkflowID(t *testing.T) {
	created := visibilityNow.Add(-time.Hour)
	records := []record{
		{id: "ork-chain", status: running, start: created, created: created}, // not yet updated
		successor("ork-chain", created, running),
	}
	for i := 0; i < 4; i++ {
		records = append(records, record{id: fmt.Sprintf("ork-%d", i), status: running, start: visibilityNow.Add(-time.Minute)})
	}
	ctx := context.Background()
	// The index counts 6 open records for 5 distinct runs.
	index := indexOf(t, records...)
	reader := readerOverIndex(index).WithNamespaceIndex(true)
	got, err := reader.RunningCount(ctx, "payments", 6)
	if err != nil || got != 5 {
		t.Fatalf("RunningCount at ceiling 6 = %d, %v; want 5 distinct (no refusal)", got, err)
	}
	if index.count("list") == 0 {
		t.Fatal("at the ceiling no walk confirmed the count")
	}
	index = indexOf(t, records...)
	reader = readerOverIndex(index).WithNamespaceIndex(true)
	if got, err := reader.RunningCount(ctx, "payments", 10); err != nil || got != 6 || index.count("list") != 0 {
		t.Fatalf("below the ceiling = %d, %v, %d lists; want the count and no list", got, err, index.count("list"))
	}
	// A failing settle is returned, never a number.
	index = indexOf(t, append(records, switched("ork-x", created, 1))...)
	index.describeErr = errors.New("frontend unavailable")
	if got, err := readerOverIndex(index).WithNamespaceIndex(true).RunningCount(ctx, "payments", 6); err == nil {
		t.Fatalf("RunningCount = %d with a failing describe", got)
	}
}

func TestRunningCount_UpperBoundBelowTheCeilingSkipsTheWalk(t *testing.T) {
	created := visibilityNow.Add(-time.Hour)
	index := indexOf(t, record{id: "ork-a", status: running, start: visibilityNow.Add(-time.Minute)},
		switched("ork-chain", created, 3))
	got, err := readerOverIndex(index).WithNamespaceIndex(true).RunningCount(context.Background(), "payments", 5)
	if err != nil || got != 2 || index.count("list") != 0 || index.count("describe") != 0 {
		t.Fatalf("RunningCount = %d, %v, %d lists; want the upper bound 2 and no list", got, err, index.count("list"))
	}
}

func TestRunningCount_UnindexedWalkCountsAChainOnce(t *testing.T) {
	created := visibilityNow.Add(-time.Hour)
	index := indexOf(t, switched("ork-chain", created, 2), successor("ork-chain", created, running),
		record{id: "ork-other", status: running, start: visibilityNow.Add(-time.Minute), ns: "billing"})
	got, err := readerOverIndex(index).RunningCount(context.Background(), "payments", 10)
	if err != nil || got != 1 {
		t.Fatalf("RunningCount = %d, %v; want the chain once", got, err)
	}
	for _, query := range index.queries {
		if strings.Contains(query, task.SearchAttrOrkflowNamespace) {
			t.Fatalf("unindexed walk pushed the namespace: %q", query)
		}
	}
}

func TestRunningExecutions_ListsAContinuedAsNewOnlyChainOnce(t *testing.T) {
	created := visibilityNow.Add(-time.Hour)
	index := indexOf(t, switched("ork-chain", created, 2))
	index.describeAs("ork-chain", running)
	runs, err := readerOverIndex(index).runningExecutions(context.Background(),
		StatsWindow{Namespace: "payments", Since: visibilityNow.Add(-24 * time.Hour), Until: visibilityNow})
	if err != nil || len(runs) != 1 || runs[0].GetStatus() != orkstackv1.Status_STATUS_RUNNING {
		t.Fatalf("running = %v, %v; want the chain once", runs, err)
	}
}

func TestReleaseRuns_PageIsDeduplicatedAndSettled(t *testing.T) {
	created := visibilityNow.Add(-time.Hour)
	recs := []record{switched("ork-chain", created, 2), successor("ork-chain", created, running),
		switched("ork-done", created, 2), {id: "ork-a", status: running, start: visibilityNow.Add(-time.Minute)}}
	for i := range recs {
		recs[i].version = "orkstack:v2"
	}
	index := indexOf(t, recs...)
	index.indexLater(t, successor("ork-done", created, completed))
	count, runs, err := readerOverIndex(index).ReleaseRuns(context.Background(), "orkstack", "v2", 10)
	if err != nil {
		t.Fatal(err)
	}
	if got := sequencesOfIDs(runs); len(got) != 2 || count < 2 {
		t.Fatalf("count %d, page %v; want the chain once and ork-a, the finished chain dropped", count, got)
	}
}

func TestStatsWindow_MeasuresChainsFromCreatedAt(t *testing.T) {
	since, until := visibilityNow.Add(-6*time.Hour), visibilityNow.Add(-3*time.Hour)
	inside := since.Add(time.Hour)
	index := indexOf(t,
		// A plain run created and finished inside: 10 minutes.
		record{id: "ork-plain", status: completed, start: inside, closed: inside.Add(10 * time.Minute)},
		// A chain created inside whose final generation also started inside
		// (Query A): created_at to close, 2 hours.
		switched("ork-a", inside, 200),
		record{id: "ork-a", status: completed, start: inside.Add(30 * time.Minute), created: inside, closed: inside.Add(2 * time.Hour)},
		// A chain created inside whose final generation started after the
		// window (Query B): created_at to close, 5 hours.
		switched("ork-b", inside, 100),
		// A chain created BEFORE the window whose final generation started
		// inside: not the window's run.
		record{id: "ork-old", status: completed, start: inside, created: since.Add(-time.Hour), closed: inside.Add(time.Minute)},
	)
	index.indexLater(t, record{id: "ork-b", status: completed, start: until.Add(time.Hour), created: inside, closed: inside.Add(5 * time.Hour)})
	durations, err := readerOverIndex(index).windowRunDurations(context.Background(), StatsWindow{Since: since, Until: until})
	if err != nil {
		t.Fatal(err)
	}
	want := []time.Duration{10 * time.Minute, 2 * time.Hour, 5 * time.Hour}
	if fmt.Sprint(durations) != fmt.Sprint(want) {
		t.Fatalf("durations = %v, want %v", durations, want)
	}
	// Query A excludes closed generations; B settles by one batched listing.
	if !strings.Contains(index.queries[0], `ExecutionStatus != "ContinuedAsNew"`) {
		t.Fatalf("query A = %q", index.queries[0])
	}
	if index.count("describe") != 0 {
		t.Fatalf("%d describes, want none: the final record was indexed", index.count("describe"))
	}
}
