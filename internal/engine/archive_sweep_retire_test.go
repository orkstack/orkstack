package engine

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"strings"
	"testing"
	"time"

	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	workflowpb "go.temporal.io/api/workflow/v1"
	"go.temporal.io/api/workflowservice/v1"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	archivecleanup "github.com/orkstack/orkstack/internal/archive"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// sweepCluster is the cluster the retention path talks to. It holds one run,
// and retiring the run forgets it, as the cluster would.
type sweepCluster struct {
	client  *fakeReadClient
	runID   string
	closed  time.Time
	retired int
}

func (c *sweepCluster) NamespaceRetention(context.Context, string) (time.Duration, error) {
	return 96 * time.Hour, nil
}

func (c *sweepCluster) SetNamespaceRetention(_ context.Context, _ string, d time.Duration) (time.Duration, error) {
	return d, nil
}

func (c *sweepCluster) LookupWorkflow(context.Context, string, string) (bool, time.Time, error) {
	if c.retired > 0 {
		return false, time.Time{}, nil
	}
	return true, c.closed, nil
}

func (c *sweepCluster) DeleteWorkflow(context.Context, string, string) error {
	c.retired++
	delete(c.client.history, c.runID)
	c.client.executions = nil
	return nil
}

type fixedPolicy struct{ max time.Duration }

func (p fixedPolicy) Get(context.Context) (*orkstackv1.RetentionPolicy, error) {
	return &orkstackv1.RetentionPolicy{MaxRunRetention: durationpb.New(p.max)}, nil
}

func (p fixedPolicy) Put(_ context.Context, policy *orkstackv1.RetentionPolicy, _ artifacts.PutConditions) (*orkstackv1.RetentionPolicy, error) {
	return policy, nil
}

type noNamespaces struct{}

func (noNamespaces) List(context.Context) ([]*orkstackv1.Namespace, error) { return nil, nil }

// TestRetention_ARunThatDidNotArchiveItselfIsArchivedAndRetired is the whole
// path for a run terminated from outside, over the real sweep, the real
// cleanup and a shared archive. The sweep archives it from its history; the
// retention walk, which could not see it before, retires it on time; and with
// the cluster's copy gone, a replica that never saw the run answers for it
// exactly as the live read did.
func TestRetention_ARunThatDidNotArchiveItselfIsArchivedAndRetired(t *testing.T) {
	ctx := context.Background()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	const runID, temporalNamespace = "ork-terminated", "ork-main"
	const retention = 72 * time.Hour
	closedAt := archiveClosedAt

	events, snapshots := helloRunEndingWith(t, runID, terminatedClose())
	reader, client := readerOver(events, runID)
	reader = reader.WithSnapshotStore(snapshots)
	client.executions = []*workflowpb.WorkflowExecutionInfo{{
		Execution: &commonpb.WorkflowExecution{WorkflowId: runID},
		Status:    enumspb.WORKFLOW_EXECUTION_STATUS_TERMINATED,
		CloseTime: timestamppb.New(closedAt),
	}}
	liveRun, err := reader.GetRun(ctx, runID)
	if err != nil {
		t.Fatalf("GetRun: %v", err)
	}
	liveEvents, err := reader.Events(ctx, runID, 0)
	if err != nil {
		t.Fatalf("Events: %v", err)
	}

	store, err := artifacts.NewLocalRunArchive(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	cluster := &sweepCluster{client: client, runID: runID, closed: closedAt}
	config := archivecleanup.CleanupConfig{TemporalNamespace: temporalNamespace, Interval: time.Hour, BatchSize: 10}
	controller := archivecleanup.NewRetentionController(fixedPolicy{max: retention}, cluster, temporalNamespace, 24*time.Hour, logger)
	cleanerAt := func(now time.Time) *archivecleanup.Cleaner {
		return archivecleanup.NewCleaner(config, store, cluster, noNamespaces{}, controller, logger).
			WithClock(func() time.Time { return now })
	}
	pastRetention := closedAt.Add(retention + time.Hour)

	// Past its retention with no archive, the walk cannot see it.
	if _, err := cleanerAt(pastRetention).RunOnce(ctx); err != nil {
		t.Fatalf("cleanup before the sweep: %v", err)
	}
	if cluster.retired != 0 {
		t.Fatal("cleanup retired a run with no archive")
	}

	archiver := NewRunArchiver(store)
	result, err := archivecleanup.NewSweeper(config, reader, store, NewHistoryArchiver(reader, archiver), cluster, nil, logger).
		WithClock(func() time.Time { return closedAt.Add(2 * time.Hour) }).RunOnce(ctx)
	if err != nil || result.Archived != 1 {
		t.Fatalf("sweep = %+v (%v), want the run archived", result, err)
	}
	if !strings.Contains(client.listQuery, `"Terminated"`) || strings.Contains(client.listQuery, "ContinuedAsNew") {
		t.Fatalf("the sweep listed %q, want finished runs and no execution that only carried on", client.listQuery)
	}

	// Not yet past its retention, the run stays. Past it, it is retired.
	if _, err := cleanerAt(closedAt.Add(time.Hour)).RunOnce(ctx); err != nil || cluster.retired != 0 {
		t.Fatalf("cleanup before retention: %v, retired %d", err, cluster.retired)
	}
	if _, err := cleanerAt(pastRetention).RunOnce(ctx); err != nil || cluster.retired != 1 {
		t.Fatalf("cleanup past retention: %v, retired %d, want the run retired", err, cluster.retired)
	}

	// The cluster has forgotten it, so only the archive can answer now.
	if _, err := reader.GetRun(ctx, runID); !errors.Is(err, ErrRunNotFound) {
		t.Fatalf("GetRun after retirement = %v, want not found", err)
	}
	replica := NewRunArchiver(store)
	run, err := replica.LoadRun(ctx, runID)
	if err != nil {
		t.Fatalf("LoadRun: %v", err)
	}
	if !proto.Equal(run, liveRun) {
		t.Fatalf("after retirement the run reads\n%v\nand before it read\n%v", run, liveRun)
	}
	stored, err := replica.LoadRunEvents(ctx, runID)
	if err != nil {
		t.Fatalf("LoadRunEvents: %v", err)
	}
	if eventTypesOf(stored) != eventTypesOf(liveEvents) {
		t.Fatalf("after retirement the stream reads %s, before it read %s", eventTypesOf(stored), eventTypesOf(liveEvents))
	}
}

// pagedList serves finished executions a page at a time.
type pagedList struct {
	*fakeReadClient
	pages [][]*workflowpb.WorkflowExecutionInfo
	sizes []int32
}

func (p *pagedList) ListWorkflow(
	_ context.Context,
	request *workflowservice.ListWorkflowExecutionsRequest,
) (*workflowservice.ListWorkflowExecutionsResponse, error) {
	page := 0
	if len(request.GetNextPageToken()) > 0 {
		page = int(request.GetNextPageToken()[0])
	}
	p.sizes = append(p.sizes, request.GetPageSize())
	p.listQuery = request.GetQuery()
	response := &workflowservice.ListWorkflowExecutionsResponse{Executions: p.pages[page]}
	if page+1 < len(p.pages) {
		response.NextPageToken = []byte{byte(page + 1)}
	}
	return response, nil
}

func finishedExecution(runID string, closedAt time.Time) *workflowpb.WorkflowExecutionInfo {
	return &workflowpb.WorkflowExecutionInfo{Execution: &commonpb.WorkflowExecution{WorkflowId: runID},
		CloseTime: timestamppb.New(closedAt)}
}

// TestEachFinishedRun_WalksEveryPageAndStopsWhenAsked: every page of the index
// is walked, a visit that fails ends the walk with its error, and a listing
// that fails is an error rather than an empty answer.
func TestEachFinishedRun_WalksEveryPageAndStopsWhenAsked(t *testing.T) {
	ctx := context.Background()
	cutoff := readerEpoch.Add(time.Hour)
	list := &pagedList{fakeReadClient: &fakeReadClient{}, pages: [][]*workflowpb.WorkflowExecutionInfo{
		{finishedExecution("ork-1", readerEpoch), finishedExecution("ork-2", readerEpoch.Add(time.Minute))},
		{finishedExecution("ork-3", readerEpoch.Add(2*time.Minute))},
	}}
	reader := &TemporalRunReader{client: list}
	var seen []string
	if err := reader.EachFinishedRun(ctx, cutoff, 2, func(runID string, closedAt time.Time) error {
		if closedAt.IsZero() {
			t.Fatalf("run %s has no close time", runID)
		}
		seen = append(seen, runID)
		return nil
	}); err != nil {
		t.Fatalf("EachFinishedRun: %v", err)
	}
	if got := strings.Join(seen, " "); got != "ork-1 ork-2 ork-3" {
		t.Fatalf("visited %s", got)
	}
	if len(list.sizes) != 2 || list.sizes[0] != 2 {
		t.Fatalf("page sizes = %v, want two pages of 2", list.sizes)
	}
	for _, want := range []string{`WorkflowType = "RunWorkflow"`, `CloseTime < "` + statsTime(cutoff) + `"`} {
		if !strings.Contains(list.listQuery, want) {
			t.Fatalf("query %q does not ask %s", list.listQuery, want)
		}
	}

	stop := errors.New("stop")
	visits := 0
	if err := reader.EachFinishedRun(ctx, cutoff, 2, func(string, time.Time) error {
		visits++
		return stop
	}); !errors.Is(err, stop) || visits != 1 {
		t.Fatalf("a failed visit: %v after %d visits, want it back after 1", err, visits)
	}

	refused := &TemporalRunReader{client: failingList{&fakeReadClient{}}}
	if err := refused.EachFinishedRun(ctx, cutoff, 2, func(string, time.Time) error { return nil }); err == nil {
		t.Fatal("a listing that failed read as no finished runs")
	}
}

type failingList struct{ *fakeReadClient }

func (failingList) ListWorkflow(
	context.Context,
	*workflowservice.ListWorkflowExecutionsRequest,
) (*workflowservice.ListWorkflowExecutionsResponse, error) {
	return nil, errors.New("the cluster is unreachable")
}
