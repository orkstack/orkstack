package engine

import (
	"testing"

	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/sdk/temporal"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
)

// A superseded run closes canceled with its code in the close's details
// (design §6.1). Whoever reads only the close, a reader whose terminal
// snapshot never landed or a parent whose child it was, still names the
// supersede: never an operator's RUN_CANCELED.
func TestSuperseded_TheCloseKeepsItsReason(t *testing.T) {
	const sentence = "run ork-newer replaced this run on key deploy"
	details, err := DataConverter().ToPayloads(RunSupersededCode, sentence)
	if err != nil {
		t.Fatal(err)
	}
	event := &historypb.HistoryEvent{
		EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_CANCELED,
		Attributes: &historypb.HistoryEvent_WorkflowExecutionCanceledEventAttributes{
			WorkflowExecutionCanceledEventAttributes: &historypb.WorkflowExecutionCanceledEventAttributes{Details: details},
		},
	}
	status, failure, ok := closedExecution(event, false)
	if !ok || status != orkstackv1.Status_STATUS_CANCELED || failure.GetCode() != RunSupersededCode ||
		failure.GetMessage() != sentence || failure.GetCategory() != orkstackv1.FailureCategory_FAILURE_CATEGORY_CANCELLED {
		t.Errorf("closedExecution(superseded) = %v %v %v, want CANCELED %s %q CANCELLED", status, failure, ok, RunSupersededCode, sentence)
	}

	// An operator's cancel, which carries no such details, reads as before.
	plain := &historypb.HistoryEvent{
		EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_CANCELED,
		Attributes: &historypb.HistoryEvent_WorkflowExecutionCanceledEventAttributes{
			WorkflowExecutionCanceledEventAttributes: &historypb.WorkflowExecutionCanceledEventAttributes{},
		},
	}
	if _, failure, _ := closedExecution(plain, false); failure.GetCode() != "RUN_CANCELED" || failure.GetDetails() != "" {
		t.Errorf("closedExecution(cancel) = %v, want RUN_CANCELED with no details", failure)
	}

	// Details it cannot read are said, never dropped.
	foreign, err := DataConverter().ToPayloads("SOMETHING_ELSE")
	if err != nil {
		t.Fatal(err)
	}
	odd := &historypb.HistoryEvent{
		EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_CANCELED,
		Attributes: &historypb.HistoryEvent_WorkflowExecutionCanceledEventAttributes{
			WorkflowExecutionCanceledEventAttributes: &historypb.WorkflowExecutionCanceledEventAttributes{Details: foreign},
		},
	}
	if _, failure, _ := closedExecution(odd, false); failure.GetCode() != "RUN_CANCELED" || failure.GetDetails() == "" {
		t.Errorf("closedExecution(unreadable details) = %v, want RUN_CANCELED naming the unread details", failure)
	}

	// A parent's composed task whose child was replaced fails: nobody
	// canceled the parent.
	status, code, category := childFailure(temporal.NewCanceledError(RunSupersededCode, sentence))
	if status != orkstackv1.Status_STATUS_FAILED || code != ChildRunFailedCode ||
		category != orkstackv1.FailureCategory_FAILURE_CATEGORY_CANCELLED {
		t.Errorf("childFailure(superseded) = %v %s %v, want FAILED %s CANCELLED", status, code, category, ChildRunFailedCode)
	}
	if got := childInnerMessage(temporal.NewCanceledError(RunSupersededCode, sentence)); got != sentence {
		t.Errorf("childInnerMessage(superseded) = %q, want %q", got, sentence)
	}
	if status, code, _ := childFailure(temporal.NewCanceledError()); status != orkstackv1.Status_STATUS_CANCELED || code != ChildRunCanceledCode {
		t.Errorf("childFailure(cancel) = %v %s, want CANCELED %s", status, code, ChildRunCanceledCode)
	}
}
