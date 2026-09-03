package engine

import (
	"context"
	"errors"
	"testing"
	"time"

	"go.temporal.io/api/serviceerror"
)

func TestLookupWorkflow_ReportsAFoundRunningExecution(t *testing.T) {
	reader := readerWithRetention(&fakeWorkflowServiceAdmin{describeExecutionFound: true})

	found, closedAt, err := reader.LookupWorkflow(context.Background(), "ork-main", "run-1")
	if err != nil {
		t.Fatalf("LookupWorkflow: %v", err)
	}
	if !found {
		t.Fatal("found = false, want true")
	}
	if !closedAt.IsZero() {
		t.Errorf("closedAt = %s, want the zero value for a still-running execution", closedAt)
	}
}

func TestLookupWorkflow_ReportsAFoundClosedExecutionsCloseTime(t *testing.T) {
	closed := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	reader := readerWithRetention(&fakeWorkflowServiceAdmin{
		describeExecutionFound: true,
		describeExecutionClose: closed,
	})

	found, closedAt, err := reader.LookupWorkflow(context.Background(), "ork-main", "run-1")
	if err != nil {
		t.Fatalf("LookupWorkflow: %v", err)
	}
	if !found {
		t.Fatal("found = false, want true")
	}
	if !closedAt.Equal(closed) {
		t.Errorf("closedAt = %s, want %s", closedAt, closed)
	}
}

func TestLookupWorkflow_ReportsAnAbsentExecutionAsNotFoundWithoutError(t *testing.T) {
	reader := readerWithRetention(&fakeWorkflowServiceAdmin{describeExecutionFound: false})

	found, closedAt, err := reader.LookupWorkflow(context.Background(), "ork-main", "run-1")
	if err != nil {
		t.Fatalf("LookupWorkflow: %v", err)
	}
	if found {
		t.Fatal("found = true, want false for a NotFound execution")
	}
	if !closedAt.IsZero() {
		t.Errorf("closedAt = %s, want the zero value", closedAt)
	}
}

func TestLookupWorkflow_ReportsATransportFailureNeverAsAbsence(t *testing.T) {
	boom := errors.New("temporal unavailable")
	reader := readerWithRetention(&fakeWorkflowServiceAdmin{describeExecutionErr: boom})

	found, _, err := reader.LookupWorkflow(context.Background(), "ork-main", "run-1")
	if !errors.Is(err, boom) {
		t.Fatalf("err = %v, want it to wrap %v", err, boom)
	}
	if found {
		t.Fatal("found = true on a transport failure, want false")
	}
}

func TestLookupWorkflow_DistinguishesNotFoundFromOtherServiceErrors(t *testing.T) {
	namespaceGone := serviceerror.NewNamespaceNotFound("gone")
	reader := readerWithRetention(&fakeWorkflowServiceAdmin{describeExecutionErr: namespaceGone})

	found, _, err := reader.LookupWorkflow(context.Background(), "ork-main", "run-1")
	if found {
		t.Fatal("found = true, want false")
	}
	if !errors.Is(err, namespaceGone) {
		t.Fatalf("err = %v, want it to wrap %v (a namespace going missing is not execution absence)", err, namespaceGone)
	}
}

func TestLookupWorkflow_WithNoClientReportsItsOwnMisconfiguration(t *testing.T) {
	reader := &TemporalRunReader{}
	if _, _, err := reader.LookupWorkflow(context.Background(), "ork-main", "run-1"); err == nil {
		t.Fatal("LookupWorkflow with no client succeeded, want an error")
	}
}

func TestDeleteWorkflow_CallsDeleteWorkflowExecutionForTheGivenNamespaceAndWorkflow(t *testing.T) {
	fake := &fakeWorkflowServiceAdmin{}
	reader := readerWithRetention(fake)

	if err := reader.DeleteWorkflow(context.Background(), "ork-main", "run-1"); err != nil {
		t.Fatalf("DeleteWorkflow: %v", err)
	}
	if len(fake.deleteExecutionCalls) != 1 {
		t.Fatalf("delete calls = %d, want 1", len(fake.deleteExecutionCalls))
	}
	call := fake.deleteExecutionCalls[0]
	if call.GetNamespace() != "ork-main" {
		t.Errorf("namespace = %q, want ork-main", call.GetNamespace())
	}
	if call.GetWorkflowExecution().GetWorkflowId() != "run-1" {
		t.Errorf("workflow id = %q, want run-1", call.GetWorkflowExecution().GetWorkflowId())
	}
}

func TestDeleteWorkflow_ReportsAnAlreadyAbsentExecutionAsSuccess(t *testing.T) {
	reader := readerWithRetention(&fakeWorkflowServiceAdmin{
		deleteExecutionErr: serviceerror.NewNotFound("gone"),
	})

	if err := reader.DeleteWorkflow(context.Background(), "ork-main", "run-1"); err != nil {
		t.Fatalf("DeleteWorkflow on an already-absent execution = %v, want nil (the outcome asked for is the outcome)", err)
	}
}

func TestDeleteWorkflow_ReportsAnyOtherFailureUntouched(t *testing.T) {
	boom := errors.New("temporal unavailable")
	reader := readerWithRetention(&fakeWorkflowServiceAdmin{deleteExecutionErr: boom})

	if err := reader.DeleteWorkflow(context.Background(), "ork-main", "run-1"); !errors.Is(err, boom) {
		t.Fatalf("err = %v, want it to wrap %v", err, boom)
	}
}

func TestDeleteWorkflow_WithNoClientReportsItsOwnMisconfiguration(t *testing.T) {
	reader := &TemporalRunReader{}
	if err := reader.DeleteWorkflow(context.Background(), "ork-main", "run-1"); err == nil {
		t.Fatal("DeleteWorkflow with no client succeeded, want an error")
	}
}
