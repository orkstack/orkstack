package engine

import (
	"context"
	"fmt"
	"regexp"
	"strings"
	"testing"
	"time"

	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	workflowpb "go.temporal.io/api/workflow/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/converter"
	"google.golang.org/protobuf/types/known/timestamppb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// ONE WINDOW, ONE POPULATION.
//
// A stats answer is assembled from two records — the archive's index keys and
// the cluster's execution index — and nothing forces them to agree about which
// runs a window contains. The archive stamps a run by when it was CREATED
// (RunIndexTime), so runs_by_status, success_rate and most_failing count the
// runs a window created; percentiles queried by `CloseTime` would count the
// runs it CLOSED instead. A response carrying both, with nothing in it saying
// so, reads as `succeeded 0 / failed 0 / rate unavailable` beside `p50 58m
// over 12 finished runs` for `orkctl stats --since 15m` over hour-long runs.
//
// The tests below hold the two sources to one set of runs, over one corpus,
// with the same window applied to both.

// statsWindowStart is the corpus's window: 09:00 (inclusive) to 12:00
// (exclusive) of one fixed day.
var (
	statsWindowStart = time.Date(2026, 5, 1, 9, 0, 0, 0, time.UTC)
	statsWindowEnd   = time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC)
)

// statsIndex is Temporal's visibility store as a stats question sees it:
// records, and a query language it APPLIES rather than records.
//
// Applying it is the point. A double that ignored the query could not tell a
// population selected by start time from one selected by close time, which is
// the whole difference under test.
type statsIndex struct {
	records []indexedExecution
	queries []string
	// counts records every CountWorkflow query, apart from the list queries
	// above, so a test can say which of the two questions was asked.
	counts []string
	// countErr is returned by CountWorkflow when set.
	countErr error
	// endless always offers another page, so a walk that does not stop on its
	// own budget never stops at all.
	endless bool
}

// indexedExecution is one visibility record and the namespace memo the cluster
// indexed it under.
type indexedExecution struct {
	info      *workflowpb.WorkflowExecutionInfo
	namespace string
}

var (
	startAtLeast  = regexp.MustCompile(`StartTime >= "([^"]*)"`)
	startBefore   = regexp.MustCompile(`StartTime < "([^"]*)"`)
	closeAtLeast  = regexp.MustCompile(`CloseTime >= "([^"]*)"`)
	closeBefore   = regexp.MustCompile(`CloseTime < "([^"]*)"`)
	statusEqualTo = regexp.MustCompile(`ExecutionStatus = "([^"]*)"`)
)

func (s *statsIndex) GetWorkflowHistory(
	context.Context, string, string, bool, enumspb.HistoryEventFilterType,
) client.HistoryEventIterator {
	return &fakeHistory{}
}

func (s *statsIndex) ListWorkflow(
	_ context.Context,
	request *workflowservice.ListWorkflowExecutionsRequest,
) (*workflowservice.ListWorkflowExecutionsResponse, error) {
	query := request.GetQuery()
	s.queries = append(s.queries, query)
	matching, err := s.match(query)
	if err != nil {
		return nil, err
	}
	response := &workflowservice.ListWorkflowExecutionsResponse{Executions: matching}
	if s.endless {
		response.NextPageToken = []byte("next")
	}
	return response, nil
}

// CountWorkflow answers how many records the query selects, applying the
// same predicates ListWorkflow does.
func (s *statsIndex) CountWorkflow(
	_ context.Context,
	request *workflowservice.CountWorkflowExecutionsRequest,
) (*workflowservice.CountWorkflowExecutionsResponse, error) {
	s.counts = append(s.counts, request.GetQuery())
	if s.countErr != nil {
		return nil, s.countErr
	}
	matching, err := s.match(request.GetQuery())
	if err != nil {
		return nil, err
	}
	return &workflowservice.CountWorkflowExecutionsResponse{Count: int64(len(matching))}, nil
}

// match applies the query language the stats questions use.
func (s *statsIndex) match(query string) ([]*workflowpb.WorkflowExecutionInfo, error) {
	if !strings.Contains(query, fmt.Sprintf("WorkflowType = %q", RunWorkflowType)) {
		return nil, fmt.Errorf("visibility: unsupported query %q", query)
	}
	var wantNamespace string
	if match := namespacePredicate.FindStringSubmatch(query); match != nil {
		wantNamespace = match[1]
	}
	bound := func(pattern *regexp.Regexp) (time.Time, bool) {
		match := pattern.FindStringSubmatch(query)
		if match == nil {
			return time.Time{}, false
		}
		parsed, err := time.Parse(statsTimeLayout, match[1])
		if err != nil {
			return time.Time{}, false
		}
		return parsed, true
	}
	matching := make([]*workflowpb.WorkflowExecutionInfo, 0, len(s.records))
	for _, record := range s.records {
		info := record.info
		if wantNamespace != "" && record.namespace != wantNamespace {
			continue
		}
		if want := statusEqualTo.FindStringSubmatch(query); want != nil {
			running := info.GetStatus() == enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING
			if (want[1] == executionStatusRunning) != running {
				continue
			}
		}
		if at, ok := bound(startAtLeast); ok &&
			info.GetStartTime().AsTime().Before(at) {
			continue
		}
		if at, ok := bound(startBefore); ok &&
			!info.GetStartTime().AsTime().Before(at) {
			continue
		}
		if at, ok := bound(closeAtLeast); ok &&
			(info.GetCloseTime() == nil || info.GetCloseTime().AsTime().Before(at)) {
			continue
		}
		if at, ok := bound(closeBefore); ok &&
			(info.GetCloseTime() == nil || !info.GetCloseTime().AsTime().Before(at)) {
			continue
		}
		matching = append(matching, info)
	}
	return matching, nil
}

// statsCorpusRun is one run of the corpus, described the way both records see
// it: when it was created, when it ended (zero = still going), and how it went.
type statsCorpusRun struct {
	id        string
	namespace string
	orkflow   string
	createdAt time.Time
	closedAt  time.Time
	status    orkstackv1.Status
}

// statsCorpus is the corpus both sources are asked about. Every row is a case
// the two populations disagreed on before this was one population.
func statsCorpus() []statsCorpusRun {
	return []statsCorpusRun{
		// Created inside, ended inside: counted and sampled by anybody.
		{
			id: "ork-inside", namespace: "alpha", orkflow: "deploy",
			createdAt: statsWindowStart.Add(time.Hour),
			closedAt:  statsWindowStart.Add(90 * time.Minute),
			status:    orkstackv1.Status_STATUS_SUCCEEDED,
		},
		// THE TRAP. Created an hour BEFORE the window and closed inside it.
		// The archive counts it in the earlier window; a percentile keyed on
		// close time counted it here, in a window whose run counts have never
		// heard of it.
		{
			id: "ork-earlier", namespace: "alpha", orkflow: "deploy",
			createdAt: statsWindowStart.Add(-time.Hour),
			closedAt:  statsWindowStart.Add(30 * time.Minute),
			status:    orkstackv1.Status_STATUS_FAILED,
		},
		// A second one, so that the two sources cannot agree by accident: with
		// a close-time population the cluster samples three runs where the
		// archive counts two, and the cross-source test below says so.
		{
			id: "ork-earlier-2", namespace: "alpha", orkflow: "deploy",
			createdAt: statsWindowStart.Add(-2 * time.Hour),
			closedAt:  statsWindowStart.Add(2 * time.Hour),
			status:    orkstackv1.Status_STATUS_SUCCEEDED,
		},
		// THE OTHER HALF OF THE TRAP. Created inside, closed after the window
		// ends — the shape of every long run, and the reason a 15-minute
		// window over hour-long runs sampled by close time reports nothing finished.
		{
			id: "ork-late", namespace: "alpha", orkflow: "deploy",
			createdAt: statsWindowEnd.Add(-30 * time.Minute),
			closedAt:  statsWindowEnd.Add(30 * time.Minute),
			status:    orkstackv1.Status_STATUS_SUCCEEDED,
		},
		// Created inside and still going: counted as running by both halves,
		// sampled by neither.
		{
			id: "ork-running", namespace: "alpha", orkflow: "deploy",
			createdAt: statsWindowEnd.Add(-15 * time.Minute),
			status:    orkstackv1.Status_STATUS_RUNNING,
		},
		// Another tenant's run, inside the window in every other respect.
		{
			id: "ork-beta", namespace: "beta", orkflow: "deploy",
			createdAt: statsWindowStart.Add(time.Hour),
			closedAt:  statsWindowStart.Add(time.Hour + 10*time.Minute),
			status:    orkstackv1.Status_STATUS_SUCCEEDED,
		},
	}
}

// statsIndexOver renders the corpus as the cluster's execution index holds it.
func statsIndexOver(t *testing.T, corpus []statsCorpusRun) *statsIndex {
	t.Helper()
	dc := converter.GetDefaultDataConverter()
	payload := func(value string) *commonpb.Payload {
		encoded, err := dc.ToPayload(value)
		if err != nil {
			t.Fatalf("encode memo %q: %v", value, err)
		}
		return encoded
	}
	index := &statsIndex{}
	for _, run := range corpus {
		info := &workflowpb.WorkflowExecutionInfo{
			Execution: &commonpb.WorkflowExecution{WorkflowId: run.id},
			Type:      &commonpb.WorkflowType{Name: RunWorkflowType},
			Status:    enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
			StartTime: timestamppb.New(run.createdAt),
			Memo: &commonpb.Memo{Fields: map[string]*commonpb.Payload{
				MemoOrkflowName: payload(run.orkflow),
				MemoNamespace:   payload(run.namespace),
			}},
		}
		if !run.closedAt.IsZero() {
			info.Status = enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED
			if run.status == orkstackv1.Status_STATUS_FAILED {
				info.Status = enumspb.WORKFLOW_EXECUTION_STATUS_FAILED
			}
			info.CloseTime = timestamppb.New(run.closedAt)
		}
		index.records = append(index.records, indexedExecution{
			info: info, namespace: run.namespace,
		})
	}
	return index
}

// TestExecutions_SamplesTheDurationsOfRunsTheWindowCREATED is the population
// rule, stated over the record that breaks it when sampling goes by close time.
func TestExecutions_SamplesTheDurationsOfRunsTheWindowCREATED(t *testing.T) {
	index := statsIndexOver(t, statsCorpus())
	reader := newRunReader(index, converter.GetDefaultDataConverter())

	executions, err := reader.Executions(context.Background(), StatsWindow{
		Namespace: "alpha",
		Since:     statsWindowStart,
		Until:     statsWindowEnd,
	})
	if err != nil {
		t.Fatalf("Executions: %v", err)
	}

	// ork-inside took 30 minutes; ork-late took an hour. ork-earlier's two
	// hours are absent: it belongs to the window that created it.
	want := []time.Duration{30 * time.Minute, time.Hour}
	if len(executions.Durations) != len(want) {
		t.Fatalf("sampled %v, want %v — the percentiles are over a different set of "+
			"runs from the counts beside them", executions.Durations, want)
	}
	for i, sample := range executions.Durations {
		if sample != want[i] {
			t.Errorf("sample %d = %v, want %v", i, sample, want[i])
		}
	}

	// The trap, named: a run that CLOSED inside the window but was created
	// before it contributes nothing, because nothing else in the answer counts
	// it either.
	for _, sample := range executions.Durations {
		if sample == 90*time.Minute {
			t.Error("ork-earlier was sampled: it closed inside the window but was " +
				"created before it, so runs_by_status has never counted it")
		}
	}

	// The bound the cluster was asked to apply is the one the population is
	// defined by. A query naming CloseTime is the defect, whatever the answer
	// happened to be on this corpus.
	var durationQuery string
	for _, query := range index.queries {
		if strings.Contains(query, "StartTime") || strings.Contains(query, "CloseTime") {
			durationQuery = query
		}
	}
	if durationQuery == "" {
		t.Fatal("no time-bounded query reached the index; the window was not pushed down")
	}
	if strings.Contains(durationQuery, "CloseTime") {
		t.Errorf("the duration query is %q: it selects the runs the window CLOSED, "+
			"and every other number in the answer counts the runs it CREATED",
			durationQuery)
	}

	if len(executions.Running) != 1 || executions.Running[0].GetId() != "ork-running" {
		t.Errorf("Running = %v, want the one run still going", executions.Running)
	}
}

// TestExecutions_SamplesTheSameRunsTheArchiveCounts is the claim itself, over
// both records at once: one corpus, one window, two sources, one population.
//
// It is what the API-level correctness test could not say. That one archives a
// corpus and hands a fake reader a list of durations chosen by hand, so the
// two numbers it asserts cannot contradict each other however far the sources
// drift apart. Here both halves read the same corpus.
func TestExecutions_SamplesTheSameRunsTheArchiveCounts(t *testing.T) {
	corpus := statsCorpus()
	store, err := artifacts.NewLocalRunArchive(t.TempDir())
	if err != nil {
		t.Fatalf("open local run archive: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })

	// The archive holds what an engine archives: a run, once it has finished.
	archiver := NewRunArchiver(store)
	for _, run := range corpus {
		if run.closedAt.IsZero() {
			continue
		}
		err := archiver.Archive(context.Background(), &orkstackv1.Run{
			Id:          run.id,
			Namespace:   run.namespace,
			OrkflowName: run.orkflow,
			Status:      run.status,
			CreatedAt:   timestamppb.New(run.createdAt),
			FinishedAt:  timestamppb.New(run.closedAt),
		}, nil)
		if err != nil {
			t.Fatalf("archive %s: %v", run.id, err)
		}
	}

	window := StatsWindow{
		Namespace: "alpha",
		Since:     statsWindowStart,
		Until:     statsWindowEnd,
	}
	tally, err := archiver.TallyRuns(context.Background(), ArchiveQuery{
		Namespace: window.Namespace,
		Since:     window.Since,
		Until:     window.Until,
	})
	if err != nil {
		t.Fatalf("TallyRuns: %v", err)
	}
	reader := newRunReader(statsIndexOver(t, corpus), converter.GetDefaultDataConverter())
	executions, err := reader.Executions(context.Background(), window)
	if err != nil {
		t.Fatalf("Executions: %v", err)
	}

	if got, want := tally.Finished(), int64(len(executions.Durations)); got != want {
		t.Fatalf("the archive counts %d finished runs in this window and the cluster "+
			"samples %d durations over the same corpus; a dashboard would print both "+
			"numbers side by side as one population", got, want)
	}
	if got := tally.Finished(); got != 2 {
		t.Errorf("the window counts %d finished runs, want 2 (ork-inside, ork-late)", got)
	}
	if got := tally.ByStatus[StatusKeySucceeded]; got != 2 {
		t.Errorf("succeeded = %d, want 2 — ork-earlier's failure belongs to the "+
			"window that created it", got)
	}
}
