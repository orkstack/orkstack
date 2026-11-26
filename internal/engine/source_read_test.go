package engine

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"

	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	workflowpb "go.temporal.io/api/workflow/v1"
	"go.temporal.io/api/workflowservice/v1"

	"github.com/orkstack/orkstack/internal/held"
)

// describeAnswer answers one describe: a cluster's answer that a test cannot
// make a real workflow give (a memo that does not decode, another kind's
// workflow under the id).
type describeAnswer struct {
	resp *workflowservice.DescribeWorkflowExecutionResponse
	err  error
}

func (d describeAnswer) DescribeWorkflowExecution(context.Context, string, string) (*workflowservice.DescribeWorkflowExecutionResponse, error) {
	return d.resp, d.err
}

func jsonPayload(body string) *commonpb.Payload {
	return &commonpb.Payload{Metadata: map[string][]byte{"encoding": []byte("json/plain")}, Data: []byte(body)}
}

func sourceDescribe(edit func(*workflowservice.DescribeWorkflowExecutionResponse)) describeAnswer {
	resp := &workflowservice.DescribeWorkflowExecutionResponse{
		WorkflowExecutionInfo: &workflowpb.WorkflowExecutionInfo{
			Status: enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING, Type: &commonpb.WorkflowType{Name: SourceWorkflowType},
			Memo: &commonpb.Memo{Fields: map[string]*commonpb.Payload{
				"now": jsonPayload(`{"load":"ok","slot":{"0":{"current":{"epoch":7,"digest":"d7","admissionSave":"s1"},"replacement":{"epoch":8,"digest":"d8"}},` +
					`"3":{"current":{"epoch":6,"digest":"d7"}}}}`),
				"carry": jsonPayload(`{"next_epoch":9,"revoke":{"save":"s1","cause":"x"}}`),
			}},
		},
		PendingActivities: []*workflowpb.PendingActivityInfo{
			{ActivityId: "evs/0/7", State: enumspb.PENDING_ACTIVITY_STATE_STARTED, Attempt: 2, LastWorkerIdentity: "orker-a"},
			{ActivityId: "evs/0/8", State: enumspb.PENDING_ACTIVITY_STATE_SCHEDULED, Attempt: 1},
			{ActivityId: "evs/3/6", State: enumspb.PENDING_ACTIVITY_STATE_CANCEL_REQUESTED, Attempt: 1, LastWorkerIdentity: "orker-b"},
		},
	}
	if edit != nil {
		edit(resp)
	}
	return describeAnswer{resp: resp}
}

// TestSourceReads_Describe: what the held check reads of a source's workflow,
// and nothing that is not its own.
func TestSourceReads_Describe(t *testing.T) {
	got, err := SourceReads{Client: sourceDescribe(nil)}.Describe(context.Background(), "evs-0123456789abcdef")
	want := held.Workflow{Open: true,
		Pending: []held.Pending{
			{ActivityID: held.ActivityID(0, 7), State: held.Started, Attempt: 2, LastWorker: "orker-a"},
			{ActivityID: held.ActivityID(0, 8), State: held.Scheduled, Attempt: 1},
			{ActivityID: held.ActivityID(3, 6), State: held.CancelRequested, Attempt: 1, LastWorker: "orker-b"}},
		Memo: held.Memo{Slots: map[int32]held.Slot{
			0: {Current: held.SlotEpoch{Epoch: 7, Digest: "d7", AdmissionSave: "s1"}, Replacement: &held.SlotEpoch{Epoch: 8, Digest: "d8"}},
			3: {Current: held.SlotEpoch{Epoch: 6, Digest: "d7"}}},
			Revoke: &held.Revoke{Save: "s1"}}}
	if err != nil || !reflect.DeepEqual(got, want) {
		t.Fatalf("%+v, %v; want %+v", got, err, want)
	}

	closed := []enumspb.WorkflowExecutionStatus{enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED, enumspb.WORKFLOW_EXECUTION_STATUS_TERMINATED,
		enumspb.WORKFLOW_EXECUTION_STATUS_CONTINUED_AS_NEW, enumspb.WORKFLOW_EXECUTION_STATUS_UNSPECIFIED}
	answers := map[string]describeAnswer{
		"no such workflow": {err: serviceerror.NewNotFound("no such execution")},
		"another kind's": sourceDescribe(func(r *workflowservice.DescribeWorkflowExecutionResponse) {
			r.WorkflowExecutionInfo.Type.Name = RunWorkflowType
		}),
		"an answer with nothing": {resp: &workflowservice.DescribeWorkflowExecutionResponse{}},
	}
	for _, status := range closed {
		answers[status.String()] = sourceDescribe(func(r *workflowservice.DescribeWorkflowExecutionResponse) { r.WorkflowExecutionInfo.Status = status })
	}
	for name, a := range answers {
		if got, err := (SourceReads{Client: a}).Describe(context.Background(), "evs-0123456789abcdef"); err != nil || !reflect.DeepEqual(got, held.Workflow{}) {
			t.Errorf("%s: %+v, %v; want one that is not open", name, got, err)
		}
	}
}

// A workflow that has written no memo yet has no slots; one whose memo cannot
// be decoded, and a describe that failed, are no answer.
func TestSourceReads_AMemoThatIsNotThereOrNotReadable(t *testing.T) {
	got, err := SourceReads{Client: sourceDescribe(func(r *workflowservice.DescribeWorkflowExecutionResponse) {
		r.WorkflowExecutionInfo.Memo = nil
	})}.Describe(context.Background(), "evs-0123456789abcdef")
	if err != nil || !got.Open || len(got.Memo.Slots) != 0 || got.Memo.Revoke != nil {
		t.Fatalf("%+v, %v", got, err)
	}
	for _, field := range []string{"now", "carry"} {
		_, err := SourceReads{Client: sourceDescribe(func(r *workflowservice.DescribeWorkflowExecutionResponse) {
			r.WorkflowExecutionInfo.Memo.Fields[field] = jsonPayload(`{"slot":[`)
		})}.Describe(context.Background(), "evs-0123456789abcdef")
		if err == nil || !strings.Contains(err.Error(), field) {
			t.Errorf("a %s that does not decode: %v", field, err)
		}
	}
	for _, failure := range []error{serviceerror.NewUnavailable("later"), context.DeadlineExceeded} {
		if _, err := (SourceReads{Client: describeAnswer{err: failure}}).Describe(context.Background(), "evs-0123456789abcdef"); !errors.Is(err, failure) {
			t.Errorf("%v came back as %v", failure, err)
		}
	}
}
