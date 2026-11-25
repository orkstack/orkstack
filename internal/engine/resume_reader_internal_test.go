package engine

import (
	"testing"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	enumspb "go.temporal.io/api/enums/v1"
	failurepb "go.temporal.io/api/failure/v1"
	historypb "go.temporal.io/api/history/v1"
)

// A reset run that cannot read its reset fails with no terminal event
// (R4-2): a reader takes its reason from the closed execution, never the
// generic RUN_FAILED, so GetRun, the run page and orkctl show it.
func TestClosedExecution_KeepsAResetRunsOwnEnding(t *testing.T) {
	for code, category := range map[string]orkstackv1.FailureCategory{
		ReasonResumePlanUnreadable:    orkstackv1.FailureCategory_FAILURE_CATEGORY_INFRASTRUCTURE_ERROR,
		ReasonResumePlanInvalid:       orkstackv1.FailureCategory_FAILURE_CATEGORY_CONTRACT_VIOLATION,
		RunConcurrencyUnsupportedCode: orkstackv1.FailureCategory_FAILURE_CATEGORY_CONTRACT_VIOLATION,
	} {
		event := &historypb.HistoryEvent{EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_FAILED,
			Attributes: &historypb.HistoryEvent_WorkflowExecutionFailedEventAttributes{
				WorkflowExecutionFailedEventAttributes: &historypb.WorkflowExecutionFailedEventAttributes{
					Failure: &failurepb.Failure{Message: "the reset that made this run could not be read",
						FailureInfo: &failurepb.Failure_ApplicationFailureInfo{
							ApplicationFailureInfo: &failurepb.ApplicationFailureInfo{Type: code}}}}}}
		status, failure, closed := closedExecution(event, false)
		if !closed || status != orkstackv1.Status_STATUS_FAILED || failure.GetCode() != code || failure.GetCategory() != category ||
			failure.GetMessage() != "the reset that made this run could not be read" {
			t.Fatalf("%s = %v %v, want FAILED %s %v with its message", code, status, failure, code, category)
		}
	}
	// Canceled while it loaded its plan: no event either, and the closed
	// execution reads CANCELED RUN_CANCELED with its message.
	canceled := &historypb.HistoryEvent{EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_CANCELED}
	status, failure, closed := closedExecution(canceled, false)
	if !closed || status != orkstackv1.Status_STATUS_CANCELED || failure.GetCode() != "RUN_CANCELED" || failure.GetMessage() == "" {
		t.Fatalf("a reset run canceled at R = %v %v, want CANCELED RUN_CANCELED with its message", status, failure)
	}
}
