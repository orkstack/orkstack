package engine

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/sdk/converter"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
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

// deletionReader is a reader whose cluster holds run-1 as one closed
// execution, exec-0, started with no carry.
func deletionReader(t *testing.T, fake *fakeWorkflowServiceAdmin) *TemporalRunReader {
	t.Helper()
	fake.describeExecutionFound = true
	fake.describeExecutionRunID = "exec-0"
	reader := readerWithRetention(fake)
	reader.client = &fakeReadClient{history: map[string]*fakeHistory{
		"run-1": {events: []*historypb.HistoryEvent{startedEvent(t, "run-1", &orkstackv1.Orkflow{Name: "x"})}},
	}}
	reader.converter = converter.GetDefaultDataConverter()
	return reader
}

func TestDeleteWorkflow_CallsDeleteWorkflowExecutionForTheGivenNamespaceAndWorkflow(t *testing.T) {
	fake := &fakeWorkflowServiceAdmin{}
	reader := deletionReader(t, fake)

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
	if call.GetWorkflowExecution().GetWorkflowId() != "run-1" || call.GetWorkflowExecution().GetRunId() != "exec-0" {
		t.Errorf("deleted %v, want run-1 by its own execution id exec-0", call.GetWorkflowExecution())
	}
}

func TestDeleteWorkflow_ReportsAnAlreadyAbsentExecutionAsSuccess(t *testing.T) {
	// Gone before the describe: nothing to delete.
	reader := readerWithRetention(&fakeWorkflowServiceAdmin{describeExecutionFound: false})
	if err := reader.DeleteWorkflow(context.Background(), "ork-main", "run-1"); err != nil {
		t.Fatalf("DeleteWorkflow on an absent run = %v, want nil", err)
	}
	// Gone between the describe and the delete.
	reader = deletionReader(t, &fakeWorkflowServiceAdmin{deleteExecutionErr: serviceerror.NewNotFound("gone")})
	if err := reader.DeleteWorkflow(context.Background(), "ork-main", "run-1"); err != nil {
		t.Fatalf("DeleteWorkflow on an already-absent execution = %v, want nil (the outcome asked for is the outcome)", err)
	}
}

func TestDeleteWorkflow_ReportsAnyOtherFailureUntouched(t *testing.T) {
	boom := errors.New("temporal unavailable")
	reader := deletionReader(t, &fakeWorkflowServiceAdmin{deleteExecutionErr: boom})
	if err := reader.DeleteWorkflow(context.Background(), "ork-main", "run-1"); !errors.Is(err, boom) {
		t.Fatalf("err = %v, want it to wrap %v", err, boom)
	}
	reader = readerWithRetention(&fakeWorkflowServiceAdmin{describeExecutionErr: boom})
	if err := reader.DeleteWorkflow(context.Background(), "ork-main", "run-1"); !errors.Is(err, boom) {
		t.Fatalf("describe failure: err = %v, want it to wrap %v", err, boom)
	}
}

// chainDeletion is a closed three-generation chain: exec-0 and exec-1 are
// named by the current generation's carry, exec-2 is current.
func chainDeletion(t *testing.T, fake *fakeWorkflowServiceAdmin) (*TemporalRunReader, *chainFixture) {
	t.Helper()
	f := newChain(t, chainSpec{closed: []int{5, 4}, current: 2, finish: true})
	fake.describeExecutionFound = true
	fake.describeExecutionRunID = f.current
	reader := readerWithRetention(fake)
	reader.client = f.client
	reader.converter = converter.GetDefaultDataConverter()
	return reader, f
}

func TestDeleteWorkflow_DeletesEveryGenerationOldestFirst(t *testing.T) {
	fake := &fakeWorkflowServiceAdmin{}
	reader, f := chainDeletion(t, fake)
	if err := reader.DeleteWorkflow(context.Background(), "ork-main", f.runID); err != nil {
		t.Fatalf("DeleteWorkflow: %v", err)
	}
	var got []string
	for _, call := range fake.deleteExecutionCalls {
		if call.GetWorkflowExecution().GetWorkflowId() != f.runID {
			t.Fatalf("deleted another workflow: %v", call.GetWorkflowExecution())
		}
		got = append(got, call.GetWorkflowExecution().GetRunId())
	}
	if want := []string{"exec-0", "exec-1", "exec-2"}; strings.Join(got, ",") != strings.Join(want, ",") {
		t.Fatalf("deleted %v, want %v: every generation, oldest first, the current one by its own id", got, want)
	}
	if got := f.client.historyExecutions; len(got) != 1 || got[0] != f.current {
		t.Fatalf("history reads = %v, want the current generation's start only", got)
	}
}

func TestDeleteWorkflow_ToleratesAlreadyExpiredGenerations(t *testing.T) {
	fake := &fakeWorkflowServiceAdmin{deleteGone: map[string]bool{"exec-0": true}}
	reader, f := chainDeletion(t, fake)
	if err := reader.DeleteWorkflow(context.Background(), "ork-main", f.runID); err != nil {
		t.Fatalf("DeleteWorkflow with an expired generation = %v, want nil", err)
	}
	if len(fake.deleteExecutionCalls) != 3 {
		t.Fatalf("delete calls = %d, want 3", len(fake.deleteExecutionCalls))
	}
}

func TestDeleteWorkflow_WithNoClientReportsItsOwnMisconfiguration(t *testing.T) {
	reader := &TemporalRunReader{}
	if err := reader.DeleteWorkflow(context.Background(), "ork-main", "run-1"); err == nil {
		t.Fatal("DeleteWorkflow with no client succeeded, want an error")
	}
}
