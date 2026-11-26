package engine

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"go.temporal.io/api/serviceerror"
	"go.temporal.io/sdk/converter"
	"google.golang.org/protobuf/proto"

	enumspb "go.temporal.io/api/enums/v1"
	failurepb "go.temporal.io/api/failure/v1"
	historypb "go.temporal.io/api/history/v1"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/testsupport/must"
)

// startedAs is the first event of execution, reset from the run's original.
func startedAs(t *testing.T, original string) *historypb.HistoryEvent {
	t.Helper()
	e := startedEvent(t, "ork-r", &orkstackv1.Orkflow{Name: "wf"})
	e.GetWorkflowExecutionStartedEventAttributes().OriginalExecutionRunId = original
	return e
}

// resetFrom is the reset of base that made execution own, for reason.
func resetFrom(id int64, base, own, reason string) *historypb.HistoryEvent {
	return &historypb.HistoryEvent{EventId: id, EventType: enumspb.EVENT_TYPE_WORKFLOW_TASK_FAILED,
		Attributes: &historypb.HistoryEvent_WorkflowTaskFailedEventAttributes{WorkflowTaskFailedEventAttributes: &historypb.WorkflowTaskFailedEventAttributes{
			Cause: enumspb.WORKFLOW_TASK_FAILED_CAUSE_RESET_WORKFLOW, BaseRunId: base, NewRunId: own, Failure: &failurepb.Failure{Message: reason},
		}}}
}

// resumedTwice is a run resumed twice: exec-0 failed, exec-1 resumed it under
// scope 7 and failed, exec-2 resumed that under scope 20.
func resumedTwice(t *testing.T, fake *fakeWorkflowServiceAdmin) (*TemporalRunReader, *fakeReadClient) {
	t.Helper()
	key := func(scope uint64) string {
		return ResumePlanKey{Scope: scope, PlanDigest: strings.Repeat("a", 64)}.String()
	}
	fake.describeExecutionFound, fake.describeExecutionRunID = true, "exec-2"
	reader := readerWithRetention(fake)
	client := &fakeReadClient{current: map[string]string{"ork-r": "exec-2"}, generations: map[string]map[string]*fakeHistory{"ork-r": {
		"exec-0": {events: []*historypb.HistoryEvent{startedAs(t, "exec-0")}},
		"exec-1": {events: []*historypb.HistoryEvent{startedAs(t, "exec-0"), resetFrom(5, "exec-0", "exec-1", key(7))}},
		"exec-2": {events: []*historypb.HistoryEvent{startedAs(t, "exec-0"), resetFrom(5, "exec-1", "exec-2", key(20))}},
	}}}
	reader.client, reader.converter = client, converter.GetDefaultDataConverter()
	return reader, client
}

// TestDeleteWorkflow_DeletesWhatAResumeSetAside: the executions a run's resumes
// set aside are retired with it, oldest first, the current one last.
func TestDeleteWorkflow_DeletesWhatAResumeSetAside(t *testing.T) {
	deleted := func(f *fakeWorkflowServiceAdmin) string {
		var got []string
		for _, call := range f.deleteExecutionCalls {
			got = append(got, call.GetWorkflowExecution().GetRunId())
		}
		return strings.Join(got, ",")
	}
	closed := readerEpoch.Add(time.Hour)
	fake := &fakeWorkflowServiceAdmin{describeExecutionClose: closed}
	reader, _ := resumedTwice(t, fake)
	must.OKf(t, reader.DeleteWorkflow(context.Background(), "ork-main", "ork-r"), "DeleteWorkflow")
	if got := deleted(fake); got != "exec-0,exec-1,exec-2" {
		t.Fatalf("deleted %s, want every execution, oldest first", got)
	}
	// One the cluster let go of ends the lineage it would name.
	fake = &fakeWorkflowServiceAdmin{describeExecutionClose: closed}
	reader, client := resumedTwice(t, fake)
	delete(client.generations["ork-r"], "exec-1")
	must.OKf(t, reader.DeleteWorkflow(context.Background(), "ork-main", "ork-r"), "DeleteWorkflow")
	if got := deleted(fake); got != "exec-1,exec-2" {
		t.Fatalf("deleted %s", got)
	}
	// A history that cannot be read deletes nothing: what it names would stay.
	fake = &fakeWorkflowServiceAdmin{describeExecutionClose: closed}
	reader, client = resumedTwice(t, fake)
	boom := errors.New("unavailable")
	client.generations["ork-r"]["exec-1"] = &fakeHistory{err: boom}
	if err := reader.DeleteWorkflow(context.Background(), "ork-main", "ork-r"); !errors.Is(err, boom) || len(fake.deleteExecutionCalls) != 0 {
		t.Fatalf("= %v, deleted %s; want the error and nothing deleted", err, deleted(fake))
	}
	// Histories that name each other end.
	fake = &fakeWorkflowServiceAdmin{describeExecutionClose: closed}
	reader, client = resumedTwice(t, fake)
	client.generations["ork-r"]["exec-1"].events[1] = resetFrom(5, "exec-2", "exec-1", "by hand")
	must.OKf(t, reader.DeleteWorkflow(context.Background(), "ork-main", "ork-r"), "DeleteWorkflow")
	if got := deleted(fake); got != "exec-1,exec-2" {
		t.Fatalf("deleted %s, want each once", got)
	}
	// A run that runs is refused, and nothing of it is removed: the cluster
	// would end it, and a resume may have started it since it was looked up.
	fake = &fakeWorkflowServiceAdmin{}
	reader, _ = resumedTwice(t, fake)
	if err := reader.DeleteWorkflow(context.Background(), "ork-main", "ork-r"); err == nil || len(fake.deleteExecutionCalls) != 0 {
		t.Fatalf("a running run: %v, deleted %s", err, deleted(fake))
	}
}

// TestNewestEnding: the scope of a run's current execution is the newest its
// history names: its own reset's, or one a reset by hand copied.
func TestNewestEnding(t *testing.T) {
	ctx := context.Background()
	closed := readerEpoch.Add(time.Hour)
	fake := &fakeWorkflowServiceAdmin{describeExecutionClose: closed}
	reader, client := resumedTwice(t, fake)
	client.generations["ork-r"]["exec-2"].events = append([]*historypb.HistoryEvent{startedAs(t, "exec-0"),
		resetFrom(4, "exec-0", "exec-1", ResumePlanKey{Scope: 7, PlanDigest: strings.Repeat("a", 64)}.String())},
		client.generations["ork-r"]["exec-2"].events[1:]...)
	scope, at, found, err := reader.NewestEnding(ctx, "ork-main", "ork-r")
	if err != nil || !found || scope != 20 || !at.Equal(closed) {
		t.Fatalf("= %d %v %v %v; want scope 20", scope, at, found, err)
	}
	// A run no resume made reads its first event alone.
	plain, plainClient := resumedTwice(t, &fakeWorkflowServiceAdmin{})
	plain.admin.(*fakeWorkflowServiceAdmin).describeExecutionRunID = "exec-0"
	plainClient.generations["ork-r"]["exec-0"].events = append(plainClient.generations["ork-r"]["exec-0"].events,
		resetFrom(5, "exec-9", "exec-0", "not its own making"))
	if scope, at, found, err = plain.NewestEnding(ctx, "ork-main", "ork-r"); err != nil || !found || scope != 0 || !at.IsZero() {
		t.Fatalf("= %d %v %v %v; want scope 0, running", scope, at, found, err)
	}
	// A reset by hand of a run before any resume of it names no scope.
	hand, handClient := resumedTwice(t, &fakeWorkflowServiceAdmin{})
	handClient.generations["ork-r"]["exec-2"].events[1] = resetFrom(5, "exec-1", "exec-2", "by hand")
	if scope, _, found, err = hand.NewestEnding(ctx, "ork-main", "ork-r"); err != nil || !found || scope != 0 {
		t.Fatalf("= %d %v %v; want scope 0", scope, found, err)
	}
	// One of an execution a resume made stays in that resume's ending, as the
	// reader has it: its numbers go on from that scope.
	hand, handClient = resumedTwice(t, &fakeWorkflowServiceAdmin{})
	handClient.generations["ork-r"]["exec-2"].events = []*historypb.HistoryEvent{startedAs(t, "exec-0"),
		handClient.generations["ork-r"]["exec-1"].events[1], resetFrom(9, "exec-1", "exec-2", "by hand")}
	if scope, _, found, err = hand.NewestEnding(ctx, "ork-main", "ork-r"); err != nil || !found || scope != 7 {
		t.Fatalf("= %d %v %v; want scope 7", scope, found, err)
	}
	// A run that carried on after its resume is of the resume its carry names,
	// and one that carried on and was never resumed is of none.
	for want, resume := range map[uint64]*orkstackv1.RunResumeCarry{20: {SeqBase: 20}, 0: nil} {
		fake := &fakeWorkflowServiceAdmin{}
		carried, carriedClient := resumedTwice(t, fake)
		fake.describeExecutionRunID = "exec-3"
		carriedClient.generations["ork-r"]["exec-3"] = &fakeHistory{events: []*historypb.HistoryEvent{startedCarrying(t, "exec-3",
			&orkstackv1.RunContinuation{Segments: []*orkstackv1.EventSegmentRef{{ExecutionId: "exec-2"}}, Resume: resume})}}
		if scope, _, found, err = carried.NewestEnding(ctx, "ork-main", "ork-r"); err != nil || !found || scope != want {
			t.Fatalf("= %d %v %v; want scope %d", scope, found, err, want)
		}
	}
	gone := readerWithRetention(&fakeWorkflowServiceAdmin{})
	if _, _, found, err = gone.NewestEnding(ctx, "ork-main", "ork-r"); err != nil || found {
		t.Fatalf("= %v %v; want not found", found, err)
	}
	down := readerWithRetention(&fakeWorkflowServiceAdmin{describeExecutionErr: serviceerror.NewUnavailable("down")})
	if _, _, _, err = down.NewestEnding(ctx, "ork-main", "ork-r"); err == nil {
		t.Fatal("a cluster that does not answer answered")
	}
	unreadable, unreadableClient := resumedTwice(t, &fakeWorkflowServiceAdmin{})
	unreadableClient.generations["ork-r"]["exec-2"] = &fakeHistory{err: errors.New("down")}
	if _, _, _, err = unreadable.NewestEnding(ctx, "ork-main", "ork-r"); err == nil {
		t.Fatal("a history that cannot be read answered")
	}
}

// startedCarrying is the start of execution, carried on with carry.
func startedCarrying(t *testing.T, execution string, carry *orkstackv1.RunContinuation) *historypb.HistoryEvent {
	t.Helper()
	start := startedAs(t, execution)
	payload, err := converter.GetDefaultDataConverter().ToPayload(carry)
	must.OK(t, err)
	attrs := start.GetWorkflowExecutionStartedEventAttributes()
	for len(attrs.GetInput().GetPayloads()) < 4 {
		attrs.Input.Payloads = append(attrs.Input.Payloads, payload)
	}
	attrs.Input.Payloads = append(attrs.Input.Payloads[:4], payload)
	return start
}

// TestDeleteWorkflow_AResumedRunOfManyHistories: how often it carried on is no
// count of its resets.
func TestDeleteWorkflow_AResumedRunOfManyHistories(t *testing.T) {
	fake := &fakeWorkflowServiceAdmin{describeExecutionClose: readerEpoch.Add(time.Hour)}
	reader, client := resumedTwice(t, fake)
	carry := &orkstackv1.RunContinuation{Segments: []*orkstackv1.EventSegmentRef{{ExecutionId: "exec-2"}}, Resume: &orkstackv1.RunResumeCarry{SeqBase: 20}}
	const last = 100
	for i := 3; i <= last; i++ {
		id := fmt.Sprintf("exec-%d", i)
		client.generations["ork-r"][id] = &fakeHistory{events: []*historypb.HistoryEvent{startedCarrying(t, id, carry)}}
		carry = proto.Clone(carry).(*orkstackv1.RunContinuation)
		carry.Segments = append(carry.Segments, &orkstackv1.EventSegmentRef{ExecutionId: id})
	}
	fake.describeExecutionRunID = fmt.Sprintf("exec-%d", last)
	must.OKf(t, reader.DeleteWorkflow(context.Background(), "ork-main", "ork-r"), "DeleteWorkflow")
	if n := len(fake.deleteExecutionCalls); n != last+1 {
		t.Fatalf("deleted %d executions, want all %d", n, last+1)
	}
}

// TestDeleteWorkflow_AResumedRunThatCarriedOn: every execution of it goes: the
// histories it carried on from, and the endings their own resets set aside.
func TestDeleteWorkflow_AResumedRunThatCarriedOn(t *testing.T) {
	fake := &fakeWorkflowServiceAdmin{describeExecutionClose: readerEpoch.Add(time.Hour)}
	reader, client := resumedTwice(t, fake)
	fake.describeExecutionRunID = "exec-3"
	client.generations["ork-r"]["exec-3"] = &fakeHistory{events: []*historypb.HistoryEvent{startedCarrying(t, "exec-3",
		&orkstackv1.RunContinuation{Segments: []*orkstackv1.EventSegmentRef{{ExecutionId: "exec-2"}}, Resume: &orkstackv1.RunResumeCarry{SeqBase: 20}})}}
	must.OKf(t, reader.DeleteWorkflow(context.Background(), "ork-main", "ork-r"), "DeleteWorkflow")
	var got []string
	for _, call := range fake.deleteExecutionCalls {
		got = append(got, call.GetWorkflowExecution().GetRunId())
	}
	if strings.Join(got, ",") != "exec-0,exec-1,exec-2,exec-3" {
		t.Fatalf("deleted %v, want every execution, oldest first", got)
	}
}
