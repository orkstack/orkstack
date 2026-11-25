package engine

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	workflowpb "go.temporal.io/api/workflow/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/converter"
	"google.golang.org/protobuf/types/known/timestamppb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/task"
	"github.com/orkstack/orkstack/internal/testsupport/must"
)

// pausedRecord is one visibility record of run id with the given execution
// status, its OrkflowRunPaused attribute set to paused when paused is not
// empty.
func pausedRecord(t *testing.T, id string, status enumspb.WorkflowExecutionStatus, paused string) *workflowpb.WorkflowExecutionInfo {
	t.Helper()
	info := &workflowpb.WorkflowExecutionInfo{
		Execution: &commonpb.WorkflowExecution{WorkflowId: id, RunId: id + "-exec"},
		Type:      &commonpb.WorkflowType{Name: RunWorkflowType},
		Status:    status,
		StartTime: timestamppb.New(time.Date(2026, 11, 17, 10, 0, 0, 0, time.UTC)),
	}
	if status != enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING {
		info.CloseTime = timestamppb.New(time.Date(2026, 11, 17, 11, 0, 0, 0, time.UTC))
	}
	if paused != "" {
		payload, err := converter.GetDefaultDataConverter().ToPayload(paused)
		must.OKf(t, err, "encode the attribute")
		info.SearchAttributes = &commonpb.SearchAttributes{IndexedFields: map[string]*commonpb.Payload{
			task.SearchAttrOrkflowRunPaused: payload,
		}}
	}
	return info
}

// A listing row is PAUSED only for a live record whose attribute says so: a
// closed record keeps the value it had when the run ended (a cancel or a
// failure while paused leaves it), and that run is not paused.
func TestListedRun_PausedOnlyWhileLive(t *testing.T) {
	for _, tc := range []struct {
		name   string
		status enumspb.WorkflowExecutionStatus
		paused string
		want   orkstackv1.Status
	}{
		{"an open paused run", enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING, "true", orkstackv1.Status_STATUS_PAUSED},
		{"an open run with no attribute", enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING, "", orkstackv1.Status_STATUS_RUNNING},
		{"an open run with another value", enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING, "false", orkstackv1.Status_STATUS_RUNNING},
		{"a generation that switched while paused", enumspb.WORKFLOW_EXECUTION_STATUS_CONTINUED_AS_NEW, "true", orkstackv1.Status_STATUS_PAUSED},
		{"a run canceled while paused", enumspb.WORKFLOW_EXECUTION_STATUS_CANCELED, "true", orkstackv1.Status_STATUS_CANCELED},
		{"a run that failed while paused", enumspb.WORKFLOW_EXECUTION_STATUS_FAILED, "true", orkstackv1.Status_STATUS_FAILED},
		{"a run terminated while paused", enumspb.WORKFLOW_EXECUTION_STATUS_TERMINATED, "true", orkstackv1.Status_STATUS_CANCELED},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := newRunReader(&endlessReadClient{}, converter.GetDefaultDataConverter())
			got := r.listedRun(pausedRecord(t, "ork-a", tc.status, tc.paused)).GetStatus()
			if got != tc.want {
				t.Fatalf("listed status = %s, want %s", got, tc.want)
			}
		})
	}
}

// Where the cluster indexes the attribute, status=PAUSED narrows to the live
// predicate AND the attribute, and status=RUNNING keeps the live predicate
// alone and drops paused rows over the page.
func TestTemporalRunReader_PausedListWhereIndexed(t *testing.T) {
	page := []*workflowpb.WorkflowExecutionInfo{
		pausedRecord(t, "ork-paused", enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING, "true"),
		pausedRecord(t, "ork-running", enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING, ""),
		pausedRecord(t, "ork-canceled", enumspb.WORKFLOW_EXECUTION_STATUS_CANCELED, "true"),
	}
	for _, tc := range []struct {
		name      string
		status    orkstackv1.Status
		wantQuery string
		wantIDs   []string
	}{
		{"paused", orkstackv1.Status_STATUS_PAUSED, `AND OrkflowRunPaused = "true"`, []string{"ork-paused"}},
		{"running excludes paused", orkstackv1.Status_STATUS_RUNNING, "", []string{"ork-running"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			client := &endlessReadClient{page: page}
			runs, err := newRunReader(client, converter.GetDefaultDataConverter()).
				WithPausedIndex(true).
				ListRuns(context.Background(), RunQuery{Status: tc.status, Limit: 1})
			must.OKf(t, err, "ListRuns")
			if len(client.queries) == 0 {
				t.Fatal("ListRuns issued no visibility query")
			}
			query := client.queries[0]
			if !strings.Contains(query, "ExecutionStatus NOT IN") {
				t.Fatalf("query %q lacks the live predicate", query)
			}
			if tc.wantQuery != "" && !strings.Contains(query, tc.wantQuery) {
				t.Fatalf("query %q, want it to contain %q", query, tc.wantQuery)
			}
			if tc.wantQuery == "" && strings.Contains(query, task.SearchAttrOrkflowRunPaused) {
				t.Fatalf("query %q names the paused attribute; a RUNNING question must not depend on it", query)
			}
			var ids []string
			for _, run := range runs {
				ids = append(ids, run.GetId())
			}
			if strings.Join(ids, ",") != strings.Join(tc.wantIDs, ",") {
				t.Fatalf("listed %v, want %v", ids, tc.wantIDs)
			}
		})
	}
}

// Where the cluster does not index the attribute, status=PAUSED is refused
// before the cluster is asked: an empty answer would read as "no run is
// paused".
func TestTemporalRunReader_PausedListRefusedWhereUnindexed(t *testing.T) {
	client := &endlessReadClient{}
	_, err := newRunReader(client, converter.GetDefaultDataConverter()).
		ListRuns(context.Background(), RunQuery{Status: orkstackv1.Status_STATUS_PAUSED, Limit: 1})
	if !errors.Is(err, ErrPausedFilterUnavailable) {
		t.Fatalf("ListRuns = %v, want ErrPausedFilterUnavailable", err)
	}
	if client.calls != 0 {
		t.Fatalf("issued %d visibility calls, want 0", client.calls)
	}
}

// Every live question settles chains caught between two records, so a
// paused run that is switching is never lost from a PAUSED list.
func TestListSettlesChains_EveryLiveStatus(t *testing.T) {
	for status := range orkstackv1.Status_name {
		s := orkstackv1.Status(status)
		want := s == orkstackv1.Status_STATUS_RUNNING || s == orkstackv1.Status_STATUS_PAUSED ||
			s == orkstackv1.Status_STATUS_PENDING || s == orkstackv1.Status_STATUS_AWAITING ||
			s == orkstackv1.Status_STATUS_SLEEPING
		if got := listSettlesChains(s); got != want {
			t.Errorf("listSettlesChains(%s) = %t, want %t", s, got, want)
		}
	}
}

// PAUSED is decodable from visibility only where the attribute is indexed.
func TestReaderDecodes_PausedOnlyWhereIndexed(t *testing.T) {
	r := newRunReader(&endlessReadClient{}, converter.GetDefaultDataConverter())
	if r.decodes(orkstackv1.Status_STATUS_PAUSED) {
		t.Fatal("an unindexed reader decodes PAUSED")
	}
	if !r.WithPausedIndex(true).decodes(orkstackv1.Status_STATUS_PAUSED) {
		t.Fatal("an indexed reader does not decode PAUSED")
	}
	for _, s := range []orkstackv1.Status{orkstackv1.Status_STATUS_PENDING, orkstackv1.Status_STATUS_SKIPPED} {
		if r.decodes(s) {
			t.Errorf("decodes %s", s)
		}
	}
}

// A release's paused runs are asked only where the cluster indexes them.
func TestReleasePausedRuns_AsksNothingWhereUnindexed(t *testing.T) {
	client := &endlessReadClient{}
	n, ok, err := newRunReader(client, converter.GetDefaultDataConverter()).ReleasePausedRuns(context.Background(), "orkstack", "v1")
	if err != nil || ok || n != 0 || client.calls != 0 {
		t.Fatalf("ReleasePausedRuns = %d, %t, %v after %d calls; want 0, false, nil after none", n, ok, err, client.calls)
	}
}

// A release's queued runs are asked only where the cluster indexes them.
func TestReleaseQueuedRuns_AsksNothingWhereUnindexed(t *testing.T) {
	client := &endlessReadClient{}
	n, ok, err := newRunReader(client, converter.GetDefaultDataConverter()).ReleaseQueuedRuns(context.Background(), "orkstack", "v1")
	if err != nil || ok || n != 0 || client.calls != 0 {
		t.Fatalf("ReleaseQueuedRuns = %d, %t, %v after %d calls; want 0, false, nil after none", n, ok, err, client.calls)
	}
}

// The census groups live paused runs by namespace and keeps the soonest own
// pause's end; a closed record still carrying the attribute is not paused.
func TestPausedRuns_GroupsByNamespace(t *testing.T) {
	conv := converter.GetDefaultDataConverter()
	memo := func(info *workflowpb.WorkflowExecutionInfo, ns, expires string) *workflowpb.WorkflowExecutionInfo {
		fields := map[string]*commonpb.Payload{}
		for k, v := range map[string]string{MemoNamespace: ns, MemoPauseExpiresAt: expires} {
			if v == "" {
				continue
			}
			p, err := conv.ToPayload(v)
			must.OKf(t, err, "memo")
			fields[k] = p
		}
		info.Memo = &commonpb.Memo{Fields: fields}
		return info
	}
	client := &endlessReadClient{page: []*workflowpb.WorkflowExecutionInfo{
		memo(pausedRecord(t, "ork-a", enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING, "true"), "payments", "2026-11-20T10:00:00Z"),
		memo(pausedRecord(t, "ork-b", enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING, "true"), "payments", "2026-11-19T10:00:00Z"),
		memo(pausedRecord(t, "ork-c", enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING, "true"), "", ""),
		memo(pausedRecord(t, "ork-d", enumspb.WORKFLOW_EXECUTION_STATUS_CANCELED, "true"), "payments", "2026-11-18T10:00:00Z"),
	}}
	got, err := newRunReader(&onePageClient{client}, conv).WithPausedIndex(true).PausedRuns(context.Background())
	must.OKf(t, err, "PausedRuns")
	soonest := time.Date(2026, 11, 19, 10, 0, 0, 0, time.UTC)
	if got["payments"].Count != 2 || !got["payments"].Soonest.Equal(soonest) || got["default"].Count != 1 || !got["default"].Soonest.IsZero() || len(got) != 2 {
		t.Fatalf("PausedRuns = %+v", got)
	}
}

// onePageClient answers its page once, with no next page.
type onePageClient struct{ *endlessReadClient }

func (c *onePageClient) ListWorkflow(ctx context.Context, req *workflowservice.ListWorkflowExecutionsRequest) (*workflowservice.ListWorkflowExecutionsResponse, error) {
	resp, err := c.endlessReadClient.ListWorkflow(ctx, req)
	if resp != nil {
		resp.NextPageToken = nil
	}
	return resp, err
}
