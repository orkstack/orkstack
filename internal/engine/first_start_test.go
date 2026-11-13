package engine

import (
	"context"
	"os"
	"testing"

	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/temporalproto"
	workflowpb "go.temporal.io/api/workflow/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/converter"
	"google.golang.org/protobuf/types/known/timestamppb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
)

func startedWithPlan(t *testing.T, runID string, plan RunPlan) *historypb.HistoryEvent {
	t.Helper()
	return &historypb.HistoryEvent{
		EventId:   1,
		EventTime: timestamppb.New(readerEpoch),
		EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED,
		Attributes: &historypb.HistoryEvent_WorkflowExecutionStartedEventAttributes{
			WorkflowExecutionStartedEventAttributes: &historypb.WorkflowExecutionStartedEventAttributes{
				WorkflowType: &commonpb.WorkflowType{Name: RunWorkflowType},
				Input: payloadsOf(t, runID, &orkstackv1.Orkflow{Name: "hello"},
					&orkstackv1.Payload{}, plan),
			},
		},
	}
}

func describeWithFirst(first string) *workflowservice.DescribeWorkflowExecutionResponse {
	return &workflowservice.DescribeWorkflowExecutionResponse{
		WorkflowExecutionInfo: &workflowpb.WorkflowExecutionInfo{FirstRunId: first},
	}
}

// FirstStart is rule 7's cluster read (workload-identity.md §3.1): a definite
// "not held" only when the cluster answers that it has no such execution, and
// every other failure an error, never a "no" and never "not the claim's own".
func TestFirstStart(t *testing.T) {
	const runID = "ork-x"
	ctx := context.Background()
	chain := func(first map[string]*fakeHistory) *fakeReadClient {
		return &fakeReadClient{
			describes:   map[string]*workflowservice.DescribeWorkflowExecutionResponse{runID: describeWithFirst("e1")},
			generations: map[string]map[string]*fakeHistory{runID: first},
			current:     map[string]string{runID: "e2"},
		}
	}

	t.Run("not held", func(t *testing.T) {
		r := newRunReader(&fakeReadClient{}, converter.GetDefaultDataConverter())
		got, err := r.FirstStart(ctx, runID)
		if err != nil || got.Held {
			t.Fatalf("FirstStart = %+v, %v; want not held, nil", got, err)
		}
	})

	t.Run("held: reads the FIRST execution's start, not the current one's", func(t *testing.T) {
		fc := chain(map[string]*fakeHistory{
			"e1": {events: []*historypb.HistoryEvent{startedWithPlan(t, runID, RunPlan{IdempotencyFingerprint: "fp-1"})}},
			"e2": {events: []*historypb.HistoryEvent{startedWithPlan(t, "", RunPlan{IdempotencyFingerprint: "forged"})}},
		})
		r := newRunReader(fc, converter.GetDefaultDataConverter())
		got, err := r.FirstStart(ctx, runID)
		if err != nil || !got.Held || got.Fingerprint != "fp-1" {
			t.Fatalf("FirstStart = %+v, %v; want held with fp-1", got, err)
		}
		if len(fc.historyExecutions) != 1 || fc.historyExecutions[0] != "e1" {
			t.Fatalf("history read executions %v, want [e1]", fc.historyExecutions)
		}
	})

	t.Run("held with no fingerprint", func(t *testing.T) {
		fc := chain(map[string]*fakeHistory{
			"e1": {events: []*historypb.HistoryEvent{startedWithPlan(t, runID, RunPlan{})}},
		})
		got, err := newRunReader(fc, converter.GetDefaultDataConverter()).FirstStart(ctx, runID)
		if err != nil || !got.Held || got.Fingerprint != "" {
			t.Fatalf("FirstStart = %+v, %v; want held, empty fingerprint", got, err)
		}
	})

	errorCases := map[string]*fakeReadClient{
		"describe fails": {describeErr: serviceerror.NewUnavailable("down")},
		"empty first execution": {
			describes: map[string]*workflowservice.DescribeWorkflowExecutionResponse{runID: describeWithFirst("")},
		},
		"first history gone": chain(map[string]*fakeHistory{}),
		"history read fails": chain(map[string]*fakeHistory{
			"e1": {err: serviceerror.NewDeadlineExceeded("slow")},
		}),
		"first event is not a start": chain(map[string]*fakeHistory{
			"e1": {events: []*historypb.HistoryEvent{{EventId: 1, EventType: enumspb.EVENT_TYPE_TIMER_FIRED}}},
		}),
	}
	for name, fc := range errorCases {
		t.Run(name, func(t *testing.T) {
			got, err := newRunReader(fc, converter.GetDefaultDataConverter()).FirstStart(ctx, runID)
			if err == nil {
				t.Fatalf("FirstStart = %+v, nil; want an error", got)
			}
		})
	}
}

// FirstStart reads the fingerprint a real keyed start recorded: the fixture's
// own first event, served as the cluster would serve it.
func TestFirstStart_ReadsTheKeyedFixture(t *testing.T) {
	const path = "testdata/history/run-keyed-v1.json"
	raw, err := os.ReadFile(path)
	if err != nil {
		if os.Getenv("ORK_REQUIRE_KEYED_FIXTURES") != "" {
			t.Fatalf("the keyed replay fixture %s is missing: %v", path, err)
		}
		t.Skipf("%s is not recorded yet (%v)", path, err)
	}
	recorded := &historypb.History{}
	if err := (temporalproto.CustomJSONUnmarshalOptions{}).Unmarshal(raw, recorded); err != nil {
		t.Fatalf("decode %s: %v", path, err)
	}
	fc := &fakeReadClient{
		describes:   map[string]*workflowservice.DescribeWorkflowExecutionResponse{"ork-k": describeWithFirst("e1")},
		generations: map[string]map[string]*fakeHistory{"ork-k": {"e1": {events: recorded.GetEvents()}}},
	}
	got, err := newRunReader(fc, converter.GetDefaultDataConverter()).FirstStart(context.Background(), "ork-k")
	if err != nil || !got.Held || got.Fingerprint == "" {
		t.Fatalf("FirstStart of the keyed fixture = %+v, %v; want held with its fingerprint", got, err)
	}
}
