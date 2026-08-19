package engine

import (
	"context"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"

	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	workflowpb "go.temporal.io/api/workflow/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/converter"
	"google.golang.org/protobuf/types/known/timestamppb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/task"
)

// A namespace is a tenancy boundary, and these tests are about what that costs
// to answer. Scoping a list by filtering the pages a bounded walk happened to
// reach makes one tenant's visibility a function of another tenant's traffic:
// the walk spends its budget on runs it discards, and the quiet namespace is
// told it has none. The defect was measured exactly that way — one live run in
// `alpha`, a hundred in `bravo`, and `ListRuns(alpha, limit 5)` returning zero
// after reading every page it was allowed.

// visibilityIndex is Temporal's visibility store, in a slice: records newest
// first, returned a page at a time, and — the part that matters — FILTERED BY
// THE QUERY, because that is the whole question here. A double that ignored
// the query could not tell a predicate the cluster applies from a filter the
// reader applies afterwards, which is the difference under test.
type visibilityIndex struct {
	records []indexedRun
	// queries records every query it was asked, so a test can say what the
	// reader pushed down rather than infer it.
	queries []string
	// pages counts the requests, which is how "it read the whole index" and
	// "the cluster answered in one page" are told apart.
	pages int
}

// indexedRun is one visibility record plus the search attribute the cluster
// indexed it under. A run started before the index existed has none, and is
// therefore not selectable — only filterable.
type indexedRun struct {
	info      *workflowpb.WorkflowExecutionInfo
	namespace string
}

var namespacePredicate = regexp.MustCompile(
	task.SearchAttrOrkflowNamespace + ` = "([^"]*)"`)

func (v *visibilityIndex) GetWorkflowHistory(
	context.Context, string, string, bool, enumspb.HistoryEventFilterType,
) client.HistoryEventIterator {
	return &fakeHistory{}
}

func (v *visibilityIndex) ListWorkflow(
	_ context.Context,
	request *workflowservice.ListWorkflowExecutionsRequest,
) (*workflowservice.ListWorkflowExecutionsResponse, error) {
	v.pages++
	v.queries = append(v.queries, request.GetQuery())

	var wantNamespace string
	if match := namespacePredicate.FindStringSubmatch(request.GetQuery()); match != nil {
		wantNamespace = match[1]
	}
	if !strings.Contains(request.GetQuery(), fmt.Sprintf("WorkflowType = %q", RunWorkflowType)) {
		return nil, fmt.Errorf("visibility: unsupported query %q", request.GetQuery())
	}

	// The cluster applies the predicate, then pages what is left.
	matching := make([]*workflowpb.WorkflowExecutionInfo, 0, len(v.records))
	for _, record := range v.records {
		if wantNamespace != "" && record.namespace != wantNamespace {
			continue
		}
		matching = append(matching, record.info)
	}

	size := int(request.GetPageSize())
	if size <= 0 {
		size = DefaultRunPageSize
	}
	from := 0
	if token := request.GetNextPageToken(); len(token) > 0 {
		parsed, err := strconv.Atoi(string(token))
		if err != nil {
			return nil, fmt.Errorf("visibility: bad page token %q", token)
		}
		from = parsed
	}
	if from > len(matching) {
		from = len(matching)
	}
	to := from + size
	if to > len(matching) {
		to = len(matching)
	}
	response := &workflowservice.ListWorkflowExecutionsResponse{Executions: matching[from:to]}
	if to < len(matching) {
		response.NextPageToken = []byte(strconv.Itoa(to))
	}
	return response, nil
}

// namespacedRuns builds count live runs of one namespace, indexed as StartRun
// stamps them.
func namespacedRuns(t *testing.T, namespace string, count int) []indexedRun {
	t.Helper()
	dc := converter.GetDefaultDataConverter()
	namePayload, err := dc.ToPayload("deploy")
	if err != nil {
		t.Fatalf("encode memo name: %v", err)
	}
	namespacePayload, err := dc.ToPayload(namespace)
	if err != nil {
		t.Fatalf("encode memo namespace: %v", err)
	}
	runs := make([]indexedRun, 0, count)
	for i := 0; i < count; i++ {
		runs = append(runs, indexedRun{
			namespace: namespace,
			info: &workflowpb.WorkflowExecutionInfo{
				Execution: &commonpb.WorkflowExecution{
					WorkflowId: fmt.Sprintf("ork-%s-%d", namespace, i),
				},
				Type:      &commonpb.WorkflowType{Name: RunWorkflowType},
				Status:    enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
				StartTime: timestamppb.New(readerEpoch.Add(time.Duration(i) * time.Second)),
				Memo: &commonpb.Memo{Fields: map[string]*commonpb.Payload{
					MemoOrkflowName: namePayload,
					MemoNamespace:   namespacePayload,
				}},
			},
		})
	}
	return runs
}

// TestTemporalRunReader_ScopesAListInTheQueryNotAfterIt is the fix stated as
// the cluster's job: the namespace goes into the query, so every record the
// walk reads already belongs to the tenant that asked.
func TestTemporalRunReader_ScopesAListInTheQueryNotAfterIt(t *testing.T) {
	index := &visibilityIndex{}
	index.records = append(index.records, namespacedRuns(t, "bravo", 100)...)
	index.records = append(index.records, namespacedRuns(t, "alpha", 1)...)
	reader := newRunReader(index, converter.GetDefaultDataConverter()).WithNamespaceIndex(true)

	runs, err := reader.ListRuns(context.Background(), RunQuery{Namespace: "alpha", Limit: 5})
	if err != nil {
		t.Fatalf("ListRuns: %v", err)
	}
	if len(runs) != 1 || runs[0].GetId() != "ork-alpha-0" {
		t.Fatalf("ListRuns(alpha) = %v, want alpha's one run: a namespace's own runs "+
			"cannot depend on how busy its neighbours are", runs)
	}
	want := fmt.Sprintf("WorkflowType = %q AND %s = %q",
		RunWorkflowType, task.SearchAttrOrkflowNamespace, "alpha")
	if index.queries[0] != want {
		t.Errorf("ListRuns queried %q, want %q", index.queries[0], want)
	}
	if index.pages != 1 {
		t.Errorf("the cluster was asked %d times for one matching run; a pushed-down "+
			"predicate answers in one page", index.pages)
	}

	// The neighbour is not shown, which is the half that already held.
	for _, run := range runs {
		if run.GetNamespace() != "alpha" {
			t.Errorf("listing alpha returned a run of namespace %q", run.GetNamespace())
		}
	}
}

// TestTemporalRunReader_AnswersAQuietNamespaceOnAClusterWithoutTheIndex is the
// other half: a cluster that would not register the search attribute still
// answers for the tenant, by reading the index to the end rather than to the
// end of a budget another tenant spent.
func TestTemporalRunReader_AnswersAQuietNamespaceOnAClusterWithoutTheIndex(t *testing.T) {
	const limit = 5
	index := &visibilityIndex{}
	// Every page of the bounded walk, filled with someone else's runs, and the
	// tenant's own single run behind all of them.
	index.records = append(index.records, namespacedRuns(t, "bravo", maxRunListPages*limit)...)
	index.records = append(index.records, namespacedRuns(t, "alpha", 1)...)
	// No WithNamespaceIndex: this reader cannot push the namespace down.
	reader := newRunReader(index, converter.GetDefaultDataConverter())

	runs, err := reader.ListRuns(context.Background(), RunQuery{Namespace: "alpha", Limit: limit})
	if err != nil {
		t.Fatalf("ListRuns: %v", err)
	}
	if len(runs) != 1 || runs[0].GetId() != "ork-alpha-0" {
		t.Fatalf("ListRuns(alpha) = %v, want alpha's one run: namespace bravo's %d live "+
			"runs must not decide whether alpha can see its own",
			runs, maxRunListPages*limit)
	}
	if index.pages <= maxRunListPages {
		t.Errorf("the walk stopped after %d pages; it has to read past the %d-page budget, "+
			"because the budget is what the neighbour's traffic consumed",
			index.pages, maxRunListPages)
	}
	for _, query := range index.queries {
		if strings.Contains(query, task.SearchAttrOrkflowNamespace) {
			t.Errorf("query %q names an index this cluster does not have", query)
		}
	}
}

// TestTemporalRunReader_ReportsAnUnscopedListItCouldNotFinish keeps the
// fallback honest. Reading the whole index is bounded too, and a bound that is
// reached must not look like an answer: a short list a caller cannot tell from
// a complete one is the same defect wearing a bigger number.
func TestTemporalRunReader_ReportsAnUnscopedListItCouldNotFinish(t *testing.T) {
	index := &visibilityIndex{records: namespacedRuns(t, "bravo", maxRunScanPages+1)}
	reader := newRunReader(index, converter.GetDefaultDataConverter())

	runs, err := reader.ListRuns(context.Background(), RunQuery{Namespace: "alpha", Limit: 1})
	if err == nil {
		t.Fatalf("ListRuns = %v, want a failure: the walk ran out of pages before it had "+
			"read the whole index, so the empty list is not an answer", runs)
	}
	for _, want := range []string{"alpha", task.SearchAttrOrkflowNamespace} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error %q does not name %q; a caller cannot act on it", err, want)
		}
	}
}

// A run's namespace is not a label on a read; it is what AUTHORIZES one.
// internal/api resolves a run id inside the namespace the caller asked for and
// refuses it in any other, so a projection that lost the field resolves to
// `default` and hands one tenant's run to another — readable, watchable and
// cancellable. The fold below is where that can happen: a run's projection is
// built from many snapshots written over its life, and a snapshot that omits a
// field must never erase it. CreatedAt, Input and the orkflow's name and
// version were already carried for exactly that reason; the namespace is the
// same shape of field and the only one with a boundary attached.

// TestTemporalRunReader_ARunKeepsItsNamespaceWhenALaterSnapshotOmitsIt reads a
// history whose lifecycle snapshots carry no namespace — the shape a build
// that does not fill the field writes, and the shape every history recorded
// before the field existed already has. The run's own start payload carries
// the definition, so the namespace is in history; the question is whether the
// fold keeps it.
func TestTemporalRunReader_ARunKeepsItsNamespaceWhenALaterSnapshotOmitsIt(t *testing.T) {
	const runID = "ork-tenancy-1"
	wf := &orkstackv1.Orkflow{Name: "deploy", Version: "1.4.0", Namespace: "payments"}
	history := []*historypb.HistoryEvent{startedEvent(t, runID, wf)}
	for i, stage := range []struct {
		eventType orkstackv1.EventType
		status    orkstackv1.Status
	}{
		{orkstackv1.EventType_EVENT_TYPE_RUN_CREATED, orkstackv1.Status_STATUS_PENDING},
		{orkstackv1.EventType_EVENT_TYPE_RUN_STARTED, orkstackv1.Status_STATUS_RUNNING},
		{orkstackv1.EventType_EVENT_TYPE_RUN_SUCCEEDED, orkstackv1.Status_STATUS_SUCCEEDED},
	} {
		sequence := int64(i + 1)
		history = append(history, publishEventScheduled(t, sequence+1, &orkstackv1.Event{
			RunId:     runID,
			Sequence:  sequence,
			Type:      stage.eventType,
			Status:    stage.status,
			Message:   stage.eventType.String(),
			Timestamp: timestamppb.New(readerEpoch.Add(time.Duration(sequence) * time.Second)),
		}, &orkstackv1.Run{
			// No Namespace: this is the snapshot under test.
			Id:             runID,
			OrkflowName:    wf.GetName(),
			OrkflowVersion: wf.GetVersion(),
			Status:         stage.status,
		}))
	}
	reader, _ := readerOver(history, runID)

	run, err := reader.GetRun(context.Background(), runID)
	if err != nil {
		t.Fatalf("GetRun: %v", err)
	}
	if run.GetNamespace() != "payments" {
		t.Fatalf("GetRun(%s).Namespace = %q, want %q: a snapshot that omits the namespace "+
			"moves the run to the default namespace, where another tenant may read and "+
			"cancel it", runID, run.GetNamespace(), "payments")
	}
	// The neighbouring identity fields are the control: they prove the run was
	// actually folded rather than answered from the start event alone.
	if run.GetOrkflowName() != "deploy" || run.GetStatus() != orkstackv1.Status_STATUS_SUCCEEDED {
		t.Fatalf("GetRun(%s) = %v, want the succeeded run of orkflow deploy", runID, run)
	}
}

// TestMergeRunSnapshot_KeepsTheIdentityALaterSnapshotOmits states the same
// claim over the fold itself, field by field, so a field added to the run
// without being carried here fails one named row rather than a whole read.
func TestMergeRunSnapshot_KeepsTheIdentityALaterSnapshotOmits(t *testing.T) {
	base := &orkstackv1.Run{
		Id:             "ork-tenancy-2",
		Namespace:      "payments",
		OrkflowName:    "deploy",
		OrkflowVersion: "1.4.0",
		Status:         orkstackv1.Status_STATUS_PENDING,
		Input:          &orkstackv1.Payload{},
		CreatedAt:      timestamppb.New(readerEpoch),
	}
	// The later snapshot carries only what it observed: a status change.
	merged := mergeRunSnapshot(base, &orkstackv1.Run{
		Id:     base.GetId(),
		Status: orkstackv1.Status_STATUS_RUNNING,
	})

	for _, field := range []struct {
		name string
		got  any
		want any
	}{
		{"namespace", merged.GetNamespace(), base.GetNamespace()},
		{"orkflow name", merged.GetOrkflowName(), base.GetOrkflowName()},
		{"orkflow version", merged.GetOrkflowVersion(), base.GetOrkflowVersion()},
		{"created at", merged.GetCreatedAt().AsTime(), base.GetCreatedAt().AsTime()},
		{"input present", merged.GetInput() != nil, true},
	} {
		if field.got != field.want {
			t.Errorf("merged %s = %v, want %v — a snapshot that omits a field must not "+
				"erase it", field.name, field.got, field.want)
		}
	}
	if merged.GetStatus() != orkstackv1.Status_STATUS_RUNNING {
		t.Errorf("merged status = %v, want RUNNING: the newer snapshot still wins on what "+
			"it does carry", merged.GetStatus())
	}
}
