package api_test

import (
	"context"
	"regexp"
	"strings"
	"testing"
	"time"

	"connectrpc.com/connect"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	workflowpb "go.temporal.io/api/workflow/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/converter"
	"google.golang.org/protobuf/types/known/timestamppb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/api"
	"github.com/orkstack/orkstack/internal/engine"
)

// chainIndexClient is a cluster whose visibility index holds a run that
// continued as new and is, for the indexing lag, visible ONLY as its closed
// generation's ContinuedAsNew record. It answers the engine reader's
// questions (open and recent counts, the live listing, the by-id listing, a
// describe) over that record, so the API's decisions are made by the real
// engine.TemporalRunReader. Every other client.Client method is unused.
type chainIndexClient struct {
	client.Client
	records []*workflowpb.WorkflowExecutionInfo
	// live is what DescribeWorkflowExecution says of the chain's current
	// execution.
	live bool
}

var idsIn = regexp.MustCompile(`WorkflowId IN \(([^)]*)\)`)

func (c *chainIndexClient) WorkflowService() workflowservice.WorkflowServiceClient { return nil }

func (c *chainIndexClient) match(query string) []*workflowpb.WorkflowExecutionInfo {
	var out []*workflowpb.WorkflowExecutionInfo
	for _, info := range c.records {
		continued := info.GetStatus() == enumspb.WORKFLOW_EXECUTION_STATUS_CONTINUED_AS_NEW
		switch {
		case idsIn.MatchString(query):
			if !strings.Contains(idsIn.FindStringSubmatch(query)[1], `"`+info.GetExecution().GetWorkflowId()+`"`) {
				continue
			}
		case strings.Contains(query, "NOT IN") && !strings.Contains(query, " OR ("):
			if continued {
				continue // open executions only
			}
		case strings.Contains(query, `ExecutionStatus = "ContinuedAsNew"`) && !strings.Contains(query, " OR ("):
			if !continued {
				continue
			}
		}
		out = append(out, info)
	}
	return out
}

func (c *chainIndexClient) CountWorkflow(_ context.Context, in *workflowservice.CountWorkflowExecutionsRequest) (*workflowservice.CountWorkflowExecutionsResponse, error) {
	return &workflowservice.CountWorkflowExecutionsResponse{Count: int64(len(c.match(in.GetQuery())))}, nil
}

func (c *chainIndexClient) ListWorkflow(_ context.Context, in *workflowservice.ListWorkflowExecutionsRequest) (*workflowservice.ListWorkflowExecutionsResponse, error) {
	return &workflowservice.ListWorkflowExecutionsResponse{Executions: c.match(in.GetQuery())}, nil
}

func (c *chainIndexClient) DescribeWorkflowExecution(_ context.Context, id, _ string) (*workflowservice.DescribeWorkflowExecutionResponse, error) {
	info := &workflowpb.WorkflowExecutionInfo{Execution: &commonpb.WorkflowExecution{WorkflowId: id},
		Status: enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING}
	if !c.live {
		info.Status = enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED
		info.CloseTime = timestamppb.Now()
	}
	return &workflowservice.DescribeWorkflowExecutionResponse{WorkflowExecutionInfo: info}, nil
}

// continuedOnly is the index holding only generation 0's ContinuedAsNew
// record of run ork-chain, of orkflow deploy, closed a minute ago.
func continuedOnly(t *testing.T, closedAgo time.Duration) *chainIndexClient {
	t.Helper()
	dc := converter.GetDefaultDataConverter()
	payload := func(v string) *commonpb.Payload {
		p, err := dc.ToPayload(v)
		if err != nil {
			t.Fatal(err)
		}
		return p
	}
	created := time.Now().Add(-3 * time.Hour).UTC()
	return &chainIndexClient{live: true, records: []*workflowpb.WorkflowExecutionInfo{{
		Execution: &commonpb.WorkflowExecution{WorkflowId: "ork-chain", RunId: "exec-0"},
		Type:      &commonpb.WorkflowType{Name: engine.RunWorkflowType},
		Status:    enumspb.WORKFLOW_EXECUTION_STATUS_CONTINUED_AS_NEW,
		StartTime: timestamppb.New(created),
		CloseTime: timestamppb.New(time.Now().Add(-closedAgo)),
		Memo: &commonpb.Memo{Fields: map[string]*commonpb.Payload{
			engine.MemoOrkflowName: payload("deploy"),
			engine.MemoNamespace:   payload("default"),
			engine.MemoCreatedAt:   payload(created.Format(time.RFC3339Nano)),
		}},
	}}}
}

func TestDeleteOrkflow_RefusedWhileAChainIsSeenOnlyAsContinuedAsNew(t *testing.T) {
	now := time.Now()
	svc, store := deletionService(t, now, nil, nil)
	// The deletion gate asks the real reader, over an index that shows the
	// live chain only as the generation that closed.
	reader := engine.NewTemporalRunReader(continuedOnly(t, time.Minute))
	svc = api.NewService(api.Config{Orkflows: store, Reader: reader, Now: func() time.Time { return now }})
	_, err := deleteDeploy(svc)
	assertRunSafetyResult(t, store, err, connect.CodeFailedPrecondition)
	if !strings.Contains(err.Error(), "1 active") {
		t.Fatalf("refusal %v does not count the live chain as active", err)
	}
}

// chainReleases is a ReleaseCluster whose run counts come from the real
// engine reader; everything else is the stub's.
type chainReleases struct {
	*stubReleases
	reader *engine.TemporalRunReader
}

func (c *chainReleases) ReleaseRuns(ctx context.Context, build string, limit int) (int64, []*orkstackv1.Run, error) {
	return c.reader.ReleaseRuns(ctx, "orkstack", build, limit)
}

func (c *chainReleases) RunningRuns(ctx context.Context) (int64, error) {
	return c.reader.RunningRuns(ctx)
}

func TestReleaseDrained_FalseWhileAChainIsSeenOnlyAsContinuedAsNew(t *testing.T) {
	stub := rolloutReleases()
	cluster := &chainReleases{stubReleases: stub, reader: engine.NewTemporalRunReader(continuedOnly(t, time.Minute))}
	svc := releaseService(cluster, releaseFleet())
	response, err := svc.ListReleases(context.Background(), connect.NewRequest(&orkstackv1.ListReleasesRequest{}))
	if err != nil {
		t.Fatalf("ListReleases: %v", err)
	}
	var candidate *orkstackv1.Release
	for _, release := range response.Msg.GetReleases() {
		if release.GetBuildId() == "v2" {
			candidate = release
		}
	}
	if candidate == nil {
		t.Fatalf("no candidate in %v", response.Msg.GetReleases())
	}
	// For a candidate the count is the only authority: a chain caught
	// between its records must keep it from reading as drained.
	if candidate.GetRunsInFlight() != 1 || candidate.GetDrained() {
		t.Fatalf("candidate runs_in_flight = %d, drained = %v; want 1 and not drained",
			candidate.GetRunsInFlight(), candidate.GetDrained())
	}
}

func TestAdmission_PassesMaxRunningAsTheCeiling(t *testing.T) {
	f := newAdmissionFixture(t)
	f.setCap(t, 7)
	if _, err := f.submit("", false); err != nil {
		t.Fatalf("submit: %v", err)
	}
	f.reader.mu.Lock()
	defer f.reader.mu.Unlock()
	if len(f.reader.runningCeilings) != 1 || f.reader.runningCeilings[0] != 7 {
		t.Fatalf("RunningCount ceilings = %v, want [7]: the namespace's max_running", f.reader.runningCeilings)
	}
}
