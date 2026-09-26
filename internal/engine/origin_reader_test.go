package engine

import (
	"context"
	"strings"
	"testing"

	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	workflowpb "go.temporal.io/api/workflow/v1"
	"go.temporal.io/sdk/converter"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/task"
)

// withOriginMemo adds the two origin memo keys StartRun writes.
func withOriginMemo(t *testing.T, info *workflowpb.WorkflowExecutionInfo, kind, id string) *workflowpb.WorkflowExecutionInfo {
	t.Helper()
	dc := converter.GetDefaultDataConverter()
	kindPayload, err := dc.ToPayload(kind)
	if err != nil {
		t.Fatal(err)
	}
	idPayload, err := dc.ToPayload(id)
	if err != nil {
		t.Fatal(err)
	}
	info.Memo.Fields[MemoOriginKind] = kindPayload
	info.Memo.Fields[MemoOriginID] = idPayload
	return info
}

func TestListQuery_PushesOriginDownOnlyWhenIndexed(t *testing.T) {
	trigger := orkstackv1.RunOriginKind_RUN_ORIGIN_KIND_TRIGGER
	reader := newRunReader(&fakeReadClient{}, nil)

	query, _, originScoped := reader.listQuery(RunQuery{OriginKind: trigger, OriginID: "trg-1"})
	if originScoped || strings.Contains(query, task.SearchAttrOrkflowOriginKind) {
		t.Fatalf("without the index the query %q pushed the origin down (scoped=%v)", query, originScoped)
	}

	reader.WithOriginIndex(true)
	query, _, originScoped = reader.listQuery(RunQuery{OriginKind: trigger, OriginID: "trg-1"})
	want := `WorkflowType = "RunWorkflow" AND OrkflowOriginKind = "trigger" AND OrkflowOriginId = "trg-1"`
	if !originScoped || query != want {
		t.Fatalf("indexed kind+id query = %q (scoped=%v), want %q", query, originScoped, want)
	}

	query, _, originScoped = reader.listQuery(RunQuery{OriginKind: trigger})
	if !originScoped || !strings.Contains(query, `OrkflowOriginKind = "trigger"`) || strings.Contains(query, task.SearchAttrOrkflowOriginId) {
		t.Fatalf("kind-only query = %q (scoped=%v), want the kind predicate and no id predicate", query, originScoped)
	}

	query, _, originScoped = reader.listQuery(RunQuery{})
	if originScoped || strings.Contains(query, "Origin") {
		t.Fatalf("an unfiltered query = %q (scoped=%v), want no origin predicate", query, originScoped)
	}
}

func TestListRuns_OriginFilterWithoutIndexFailsClosed(t *testing.T) {
	// Every record is another origin's: only the budget can end the walk.
	client := &endlessReadClient{page: []*workflowpb.WorkflowExecutionInfo{
		withOriginMemo(t, visibilityRecord(t, "ork-other", "hello-ork", "1.0.0",
			enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING, readerEpoch), "submit", ""),
	}}
	_, err := newRunReader(client, nil).ListRuns(context.Background(), RunQuery{
		OriginKind: orkstackv1.RunOriginKind_RUN_ORIGIN_KIND_TRIGGER, OriginID: "trg-1", Limit: 10,
	})
	if err == nil {
		t.Fatal("an unindexed origin filter past its budget returned a list, want a failure")
	}
	for _, name := range []string{task.SearchAttrOrkflowOriginKind, task.SearchAttrOrkflowOriginId} {
		if !strings.Contains(err.Error(), name) {
			t.Errorf("error %q does not name %s", err, name)
		}
	}
	if client.calls != maxRunScanPages {
		t.Fatalf("the walk read %d pages, want the fail-closed %d", client.calls, maxRunScanPages)
	}

	// The same list with the index answers from what the cluster selected.
	indexed := &endlessReadClient{}
	if _, err := newRunReader(indexed, nil).WithOriginIndex(true).ListRuns(context.Background(), RunQuery{
		OriginKind: orkstackv1.RunOriginKind_RUN_ORIGIN_KIND_TRIGGER, Limit: 10,
	}); err != nil {
		t.Fatalf("indexed origin list: %v", err)
	}
	if !strings.Contains(indexed.queries[0], `OrkflowOriginKind = "trigger"`) {
		t.Fatalf("indexed query = %q", indexed.queries[0])
	}
}

func TestListedRun_ReadsOriginMemo(t *testing.T) {
	reader := newRunReader(&fakeReadClient{}, nil)
	run := reader.listedRun(withOriginMemo(t, visibilityRecord(t, "ork-1", "hello-ork", "1.0.0",
		enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING, readerEpoch), "trigger", "trg-1"))
	want := &orkstackv1.RunOrigin{Kind: orkstackv1.RunOriginKind_RUN_ORIGIN_KIND_TRIGGER, Id: "trg-1"}
	if !proto.Equal(run.GetOrigin(), want) {
		t.Fatalf("listed origin = %v, want %v", run.GetOrigin(), want)
	}
	legacy := reader.listedRun(visibilityRecord(t, "ork-2", "hello-ork", "1.0.0",
		enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING, readerEpoch))
	if legacy.GetOrigin() != nil {
		t.Fatalf("a memo with no kind listed origin %v, want none", legacy.GetOrigin())
	}
	bogus := reader.listedRun(withOriginMemo(t, visibilityRecord(t, "ork-3", "hello-ork", "1.0.0",
		enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING, readerEpoch), "unspecified", "x"))
	if bogus.GetOrigin() != nil {
		t.Fatalf("an unrecognized kind slug listed origin %v, want none", bogus.GetOrigin())
	}
}

func startedEventWithPlan(t *testing.T, runID string, wf *orkstackv1.Orkflow, plan any) *historypb.HistoryEvent {
	t.Helper()
	return &historypb.HistoryEvent{
		EventId:   1,
		EventTime: timestamppb.New(readerEpoch),
		EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED,
		Attributes: &historypb.HistoryEvent_WorkflowExecutionStartedEventAttributes{
			WorkflowExecutionStartedEventAttributes: &historypb.WorkflowExecutionStartedEventAttributes{
				WorkflowType: &commonpb.WorkflowType{Name: RunWorkflowType},
				Input:        payloadsOf(t, runID, wf, &orkstackv1.Payload{}, plan),
			},
		},
	}
}

func TestStartedRun_ReadsOriginOffThePlan(t *testing.T) {
	const runID = "ork-origin-pending"
	wf := &orkstackv1.Orkflow{Name: "hello-ork", Version: "0.1.0"}
	origin := &RunOrigin{Kind: "RUN_ORIGIN_KIND_RUN_AGAIN", ID: "ork-source", Reference: "INC-1", Principal: "ops"}

	reader, _ := readerOver([]*historypb.HistoryEvent{
		startedEventWithPlan(t, runID, wf, RunPlan{Namespace: "default", Origin: origin}),
	}, runID)
	run, err := reader.GetRun(context.Background(), runID)
	if err != nil {
		t.Fatalf("GetRun: %v", err)
	}
	if !proto.Equal(run.GetOrigin(), origin.Proto()) {
		t.Fatalf("pending-window origin = %v, want %v", run.GetOrigin(), origin.Proto())
	}

	// A fourth argument that is not a plan leaves the origin unset.
	reader, _ = readerOver([]*historypb.HistoryEvent{
		startedEventWithPlan(t, runID, wf, "not a plan"),
	}, runID)
	run, err = reader.GetRun(context.Background(), runID)
	if err != nil {
		t.Fatalf("GetRun: %v", err)
	}
	if run.GetOrigin() != nil || run.GetOrkflowName() != "hello-ork" {
		t.Fatalf("an undecodable plan read origin %v (orkflow %q), want none and the orkflow intact",
			run.GetOrigin(), run.GetOrkflowName())
	}

	// A plan with no origin — every run before origins existed — reads none.
	reader, _ = readerOver([]*historypb.HistoryEvent{
		startedEventWithPlan(t, runID, wf, RunPlan{Namespace: "default"}),
	}, runID)
	run, _ = reader.GetRun(context.Background(), runID)
	if run.GetOrigin() != nil {
		t.Fatalf("a plan with no origin read %v, want none", run.GetOrigin())
	}
}

func TestMatchesRunQuery_OriginNeverMatchesARunWithoutOne(t *testing.T) {
	legacy := &orkstackv1.Run{Id: "ork-legacy", CreatedAt: timestamppb.New(readerEpoch)}
	trigger := &orkstackv1.Run{Id: "ork-t", CreatedAt: timestamppb.New(readerEpoch), Origin: &orkstackv1.RunOrigin{
		Kind: orkstackv1.RunOriginKind_RUN_ORIGIN_KIND_TRIGGER, Id: "trg-1",
	}}
	for number := range orkstackv1.RunOriginKind_name {
		kind := orkstackv1.RunOriginKind(number)
		if kind == orkstackv1.RunOriginKind_RUN_ORIGIN_KIND_UNSPECIFIED {
			continue
		}
		if matchesRunQuery(legacy, RunQuery{OriginKind: kind}) {
			t.Fatalf("a run with no origin matched %v", kind)
		}
	}
	if !matchesRunQuery(legacy, RunQuery{}) {
		t.Fatal("a run with no origin must still match a query with no origin filter")
	}
	if !matchesRunQuery(trigger, RunQuery{OriginKind: orkstackv1.RunOriginKind_RUN_ORIGIN_KIND_TRIGGER, OriginID: "trg-1"}) {
		t.Fatal("the trigger run did not match its own kind and id")
	}
	if matchesRunQuery(trigger, RunQuery{OriginKind: orkstackv1.RunOriginKind_RUN_ORIGIN_KIND_TRIGGER, OriginID: "trg-"}) {
		t.Fatal("an origin id matched as a prefix; it is exact")
	}
	if matchesRunQuery(trigger, RunQuery{OriginKind: orkstackv1.RunOriginKind_RUN_ORIGIN_KIND_SUBMIT}) {
		t.Fatal("the trigger run matched submit")
	}
}

func TestMergeRunSnapshot_KeepsTheListedOriginWhenTheSnapshotHasNone(t *testing.T) {
	listed := &orkstackv1.Run{Id: "ork-1", Origin: &orkstackv1.RunOrigin{Kind: orkstackv1.RunOriginKind_RUN_ORIGIN_KIND_SUBMIT}}
	merged := mergeRunSnapshot(listed, &orkstackv1.Run{Id: "ork-1"})
	if merged.GetOrigin().GetKind() != orkstackv1.RunOriginKind_RUN_ORIGIN_KIND_SUBMIT {
		t.Fatalf("merged origin = %v, want the listed one", merged.GetOrigin())
	}
	full := &orkstackv1.RunOrigin{Kind: orkstackv1.RunOriginKind_RUN_ORIGIN_KIND_SUBMIT, Reference: "r"}
	merged = mergeRunSnapshot(listed, &orkstackv1.Run{Id: "ork-1", Origin: full})
	if !proto.Equal(merged.GetOrigin(), full) {
		t.Fatalf("merged origin = %v, want the snapshot's", merged.GetOrigin())
	}
}
