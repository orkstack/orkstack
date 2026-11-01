package engine

import (
	"context"
	"strings"
	"testing"
	"time"
	"unicode/utf8"

	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/sdk/converter"
	"google.golang.org/protobuf/types/known/timestamppb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
)

// cancelRequestedEvent is the cluster's record of one cancellation request,
// its cause the reason the request gave.
func cancelRequestedEvent(eventID int64, cause string) *historypb.HistoryEvent {
	return &historypb.HistoryEvent{
		EventId:   eventID,
		EventTime: timestamppb.New(readerEpoch.Add(5 * time.Second)),
		EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_CANCEL_REQUESTED,
		Attributes: &historypb.HistoryEvent_WorkflowExecutionCancelRequestedEventAttributes{
			WorkflowExecutionCancelRequestedEventAttributes: &historypb.WorkflowExecutionCancelRequestedEventAttributes{
				Cause:    cause,
				Identity: "engine-a",
			},
		},
	}
}

// mustCancelCause is the cause CancelRun gives for principal.
func mustCancelCause(t *testing.T, principal string) string {
	t.Helper()
	cause, err := encodeCancelRequest(principal)
	if err != nil {
		t.Fatalf("encodeCancelRequest(%q): %v", principal, err)
	}
	return cause
}

// canceledRun is one run's history up to its own RUN_CANCELED: created,
// started, the cancellation request (cause, when not empty), and the terminal
// event with its CANCELED snapshot. closed appends the execution's own
// close. The store holds every snapshot it references.
func canceledRun(t *testing.T, runID, cause string, closed bool) ([]*historypb.HistoryEvent, *countingSnapshotStore) {
	t.Helper()
	wf := &orkstackv1.Orkflow{Name: "cancel-ork", Version: "1.0.0"}
	store := &countingSnapshotStore{objects: map[string][]byte{}}
	events := []*historypb.HistoryEvent{startedEvent(t, runID, wf)}
	snapshot := func(sequence int64, status orkstackv1.Status, runErr *orkstackv1.Error) string {
		uri := snapshotURIFor(runID, sequence)
		store.objects[uri] = mustMarshalRun(t, &orkstackv1.Run{
			Id: runID, OrkflowName: wf.GetName(), OrkflowVersion: wf.GetVersion(),
			Status: status, Error: runErr, CreatedAt: timestamppb.New(readerEpoch),
		})
		return uri
	}
	publish := func(eventID, sequence int64, eventType orkstackv1.EventType, status orkstackv1.Status, uri string) {
		events = append(events, publishEventWithURI(t, eventID, &orkstackv1.Event{
			RunId: runID, Sequence: sequence, Type: eventType, Status: status,
			Timestamp: timestamppb.New(readerEpoch.Add(time.Duration(sequence) * time.Second)),
		}, uri))
	}
	publish(2, 1, orkstackv1.EventType_EVENT_TYPE_RUN_CREATED, orkstackv1.Status_STATUS_PENDING,
		snapshot(1, orkstackv1.Status_STATUS_PENDING, nil))
	publish(3, 2, orkstackv1.EventType_EVENT_TYPE_RUN_STARTED, orkstackv1.Status_STATUS_RUNNING,
		snapshot(2, orkstackv1.Status_STATUS_RUNNING, nil))
	if cause != "" {
		events = append(events, cancelRequestedEvent(4, cause))
	}
	publish(5, 3, orkstackv1.EventType_EVENT_TYPE_RUN_CANCELED, orkstackv1.Status_STATUS_CANCELED,
		snapshot(3, orkstackv1.Status_STATUS_CANCELED, &orkstackv1.Error{Code: runCanceledCode, Message: "run canceled"}))
	if closed {
		events = append(events, closedEvent(6, enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_CANCELED,
			readerEpoch.Add(10*time.Second)))
	}
	return events, store
}

func TestCancelPrincipal_ReadsOnlyWhatCancelRunWrote(t *testing.T) {
	atBound := strings.Repeat("b", CanceledByMaxBytes)
	overBound := strings.Repeat("a", CanceledByMaxBytes+1)
	tests := []struct {
		name  string
		cause string
		want  string
	}{
		{"a record CancelRun wrote", mustCancelCause(t, "dana@acme"), "dana@acme"},
		{"a record at the bound", mustCancelCause(t, atBound), atBound},
		{"a record with no principal", mustCancelCause(t, ""), ""},
		{"no cause: a parent run's request, or an earlier release's", "", ""},
		{"an operator's own reason text", "stopping it, it is stuck", ""},
		{"a JSON object that is not a record", `{"reason":"stuck"}`, ""},
		{"a record of another format", `{"format":2,"principal":"dana@acme"}`, ""},
		{"a record with no format", `{"principal":"dana@acme"}`, ""},
		{"a later release's extra field is ignored", `{"format":1,"principal":"dana@acme","reason":"stuck"}`, "dana@acme"},
		{"a principal over the bound was not written by CancelRun", `{"format":1,"principal":"` + overBound + `"}`, ""},
		{"truncated JSON", `{"format":1,"principal":"dana`, ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := cancelPrincipal(tt.cause); got != tt.want {
				t.Fatalf("cancelPrincipal(%q) = %q, want %q", tt.cause, got, tt.want)
			}
		})
	}
}

func TestApplyCanceledBy_OnlyARunACancellationEnded(t *testing.T) {
	canceled := &orkstackv1.Error{Code: runCanceledCode}
	tests := []struct {
		name      string
		run       *orkstackv1.Run
		principal string
		want      string
	}{
		{"canceled by the request", &orkstackv1.Run{Status: orkstackv1.Status_STATUS_CANCELED, Error: canceled}, "dana@acme", "dana@acme"},
		{"no principal recorded", &orkstackv1.Run{Status: orkstackv1.Status_STATUS_CANCELED, Error: canceled}, "", ""},
		{"terminated from outside", &orkstackv1.Run{Status: orkstackv1.Status_STATUS_CANCELED,
			Error: &orkstackv1.Error{Code: "RUN_TERMINATED"}}, "dana@acme", ""},
		{"still draining after the request", &orkstackv1.Run{Status: orkstackv1.Status_STATUS_RUNNING}, "dana@acme", ""},
		{"a failing run stays failed", &orkstackv1.Run{Status: orkstackv1.Status_STATUS_FAILED,
			Error: &orkstackv1.Error{Code: "RUN_FAILED"}}, "dana@acme", ""},
		{"succeeded", &orkstackv1.Run{Status: orkstackv1.Status_STATUS_SUCCEEDED}, "dana@acme", ""},
		{"canceled with no error recorded", &orkstackv1.Run{Status: orkstackv1.Status_STATUS_CANCELED}, "dana@acme", ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			applyCanceledBy(tt.run, tt.principal)
			if got := tt.run.GetCanceledBy(); got != tt.want {
				t.Fatalf("canceled_by = %q, want %q", got, tt.want)
			}
		})
	}
	applyCanceledBy(nil, "dana@acme") // a nil run is no run: nothing to set, no panic
}

// GetRun names who canceled a run from the run's own record of the request
// that canceled it, on every path a canceled run is read through.
func TestTemporalRunReader_GetRunNamesWhoCanceledTheRun(t *testing.T) {
	const runID = "ork-canceled-by"
	tests := []struct {
		name   string
		cause  string
		closed bool
		want   string
	}{
		{"closed, CancelRun recorded a principal", mustCancelCause(t, "dana@acme"), true, "dana@acme"},
		// The stream delivers RUN_CANCELED before the execution closes; a
		// read in that window must not show the name only to drop it later.
		{"terminal event published, execution still open", mustCancelCause(t, "dana@acme"), false, "dana@acme"},
		{"CancelRun with no header", "", true, ""},
		{"an operator's own reason text", "operator: stuck", true, ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			history, store := canceledRun(t, runID, tt.cause, tt.closed)
			reader, _ := readerOver(history, runID)
			reader.WithSnapshotStore(store)
			run, err := reader.GetRun(context.Background(), runID)
			if err != nil {
				t.Fatalf("GetRun: %v", err)
			}
			if run.GetStatus() != orkstackv1.Status_STATUS_CANCELED {
				t.Fatalf("status = %s, want CANCELED", run.GetStatus())
			}
			if got := run.GetCanceledBy(); got != tt.want {
				t.Fatalf("canceled_by = %q, want %q", got, tt.want)
			}
		})
	}
}

// The first request of an execution is the one that took effect, so a
// second recorded one never replaces it, named or not.
func TestTemporalRunReader_TheFirstRequestIsTheOneNamed(t *testing.T) {
	tests := []struct {
		name          string
		first, second string
		want          string
	}{
		{"named, then another name", mustCancelCause(t, "dana@acme"), mustCancelCause(t, "eve@acme"), "dana@acme"},
		{"nameless, then a name", "", mustCancelCause(t, "eve@acme"), ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			const runID = "ork-canceled-twice"
			history, store := canceledRun(t, runID, tt.first, true)
			if tt.first == "" {
				// canceledRun omits an empty cause's event; a nameless request is
				// still a request.
				history = append(history[:3], append([]*historypb.HistoryEvent{cancelRequestedEvent(4, "")}, history[3:]...)...)
			}
			second := cancelRequestedEvent(4, tt.second)
			history = append(history[:4], append([]*historypb.HistoryEvent{second}, history[4:]...)...)
			reader, _ := readerOver(history, runID)
			reader.WithSnapshotStore(store)
			run, err := reader.GetRun(context.Background(), runID)
			if err != nil {
				t.Fatalf("GetRun: %v", err)
			}
			if got := run.GetCanceledBy(); got != tt.want {
				t.Fatalf("canceled_by = %q, want %q", got, tt.want)
			}
		})
	}
}

// A run whose cancellation was requested and that was then terminated —
// stuck, so an operator ended it from outside — was not ended by the
// request: it names nobody.
func TestTemporalRunReader_ATerminatedRunNamesNobody(t *testing.T) {
	const runID = "ork-canceled-then-terminated"
	history, store := canceledRun(t, runID, mustCancelCause(t, "dana@acme"), false)
	// Drop the run's own RUN_CANCELED: it never got that far.
	history = append(history[:len(history)-1],
		closedEvent(6, enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_TERMINATED, readerEpoch.Add(time.Minute)))
	reader, _ := readerOver(history, runID)
	reader.WithSnapshotStore(store)
	run, err := reader.GetRun(context.Background(), runID)
	if err != nil {
		t.Fatalf("GetRun: %v", err)
	}
	if run.GetError().GetCode() != "RUN_TERMINATED" || run.GetCanceledBy() != "" {
		t.Fatalf("run = %s/%s by %q, want RUN_TERMINATED by nobody",
			run.GetStatus(), run.GetError().GetCode(), run.GetCanceledBy())
	}
}

// A request recorded after the run published its own end did not end it,
// and the archive write — which reads at that publish — never sees it: it
// names nobody, so GetRun and the archived run agree.
func TestTemporalRunReader_ARequestAfterTheRunsEndNamesNobody(t *testing.T) {
	const runID = "ork-canceled-late"
	history, store := canceledRun(t, runID, "", false)
	history = append(history,
		cancelRequestedEvent(6, mustCancelCause(t, "dana@acme")),
		closedEvent(7, enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_CANCELED, readerEpoch.Add(time.Minute)))
	reader, _ := readerOver(history, runID)
	reader.WithSnapshotStore(store)
	run, err := reader.GetRun(context.Background(), runID)
	if err != nil {
		t.Fatalf("GetRun: %v", err)
	}
	if run.GetCanceledBy() != "" {
		t.Fatalf("canceled_by = %q, want nobody: the request came after the run's end", run.GetCanceledBy())
	}
	reader, _ = readerOver(history, runID)
	if read, err := reader.EventsForArchive(context.Background(), runID); err != nil || read.CanceledBy != "" {
		t.Fatalf("EventsForArchive = %q, %v; want nobody", read.CanceledBy, err)
	}
}

// A run that published its own RUN_CANCELED and was then terminated before
// its execution closed keeps the end it recorded, and with it who canceled
// it (closeRun keeps a terminal snapshot).
func TestTemporalRunReader_ATerminationAfterTheRunsEndKeepsTheName(t *testing.T) {
	const runID = "ork-canceled-then-cut"
	history, store := canceledRun(t, runID, mustCancelCause(t, "dana@acme"), false)
	history = append(history,
		closedEvent(6, enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_TERMINATED, readerEpoch.Add(time.Minute)))
	reader, _ := readerOver(history, runID)
	reader.WithSnapshotStore(store)
	run, err := reader.GetRun(context.Background(), runID)
	if err != nil {
		t.Fatalf("GetRun: %v", err)
	}
	if run.GetError().GetCode() != runCanceledCode || run.GetCanceledBy() != "dana@acme" {
		t.Fatalf("run = %s by %q, want RUN_CANCELED by dana@acme", run.GetError().GetCode(), run.GetCanceledBy())
	}
}

// Only the request of the execution that ends the run counts: one recorded
// in an execution that then continued as new is not the successor's.
func TestTemporalRunReader_ARequestOfAnEarlierExecutionNamesNobody(t *testing.T) {
	const runID = "ork-canceled-across"
	first, _ := canceledRun(t, runID, "", false)
	switched := []*historypb.HistoryEvent{first[0], cancelRequestedEvent(2, mustCancelCause(t, "dana@acme")), {
		EventId:   3,
		EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_CONTINUED_AS_NEW,
		Attributes: &historypb.HistoryEvent_WorkflowExecutionContinuedAsNewEventAttributes{
			WorkflowExecutionContinuedAsNewEventAttributes: &historypb.WorkflowExecutionContinuedAsNewEventAttributes{
				NewExecutionRunId: "second",
			},
		},
	}}
	second, store := canceledRun(t, runID, "", true)
	fake := &fakeReadClient{
		generations: map[string]map[string]*fakeHistory{runID: {
			"first":  {events: switched},
			"second": {events: second},
		}},
		current: map[string]string{runID: "first"},
	}
	reader := newRunReader(fake, converter.GetDefaultDataConverter()).WithSnapshotStore(store)
	run, err := reader.GetRun(context.Background(), runID)
	if err != nil {
		t.Fatalf("GetRun: %v", err)
	}
	if run.GetStatus() != orkstackv1.Status_STATUS_CANCELED || run.GetCanceledBy() != "" {
		t.Fatalf("run = %s by %q, want CANCELED by nobody", run.GetStatus(), run.GetCanceledBy())
	}
}

// The archive write's walk reads who canceled the run, so the archived run
// can say it without a second read.
func TestTemporalRunReader_EventsForArchiveReadsWhoCanceled(t *testing.T) {
	const runID = "ork-archive-canceled-by"
	history, _ := canceledRun(t, runID, mustCancelCause(t, "dana@acme"), false)
	reader, _ := readerOver(history, runID)
	read, err := reader.EventsForArchive(context.Background(), runID)
	if err != nil {
		t.Fatalf("EventsForArchive: %v", err)
	}
	if read.CanceledBy != "dana@acme" || len(read.Events) != 3 {
		t.Fatalf("read %d events by %q, want 3 by dana@acme", len(read.Events), read.CanceledBy)
	}
}

// FuzzCancelPrincipal: a cancellation's cause is text OrkStack does not
// control — anyone with cluster access can cancel with any reason — so the
// reader of it never panics, and returns a principal only bounded and valid
// UTF-8. What CancelRun writes for a principal it can write reads back as it
// was written.
func FuzzCancelPrincipal(f *testing.F) {
	for _, seed := range []string{
		"",
		"stopping it",
		`{"reason":"stuck"}`,
		`{"format":1,"principal":"dana@acme"}`,
		`{"format":2,"principal":"dana@acme"}`,
		`{"format":1,"principal":"dana@acme","reason":"x"}`,
		`{"format":1,"principal":"dana`,
		`{"format":"1","principal":1}`,
	} {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, cause string) {
		got := cancelPrincipal(cause)
		if got != "" && (len(got) > CanceledByMaxBytes || !utf8.ValidString(got)) {
			t.Fatalf("cancelPrincipal(%q) = %q: over the bound or not UTF-8", cause, got)
		}
		// The text as a principal: what CancelRun writes for it, when it is
		// one CancelRun can write, reads back unchanged.
		if len(cause) <= CanceledByMaxBytes && utf8.ValidString(cause) {
			written, err := encodeCancelRequest(cause)
			if err != nil {
				t.Fatalf("encodeCancelRequest(%q): %v", cause, err)
			}
			if back := cancelPrincipal(written); back != cause {
				t.Fatalf("round trip of %q read back %q", cause, back)
			}
		}
	})
}

// PublishEvent holds a canceled run's end back only while the workflow will
// retry it; the two numbers are one.
func TestPublishAttempts_MatchTheRetryPolicy(t *testing.T) {
	if got := eventActivityOptions().RetryPolicy.MaximumAttempts; got != publishAttempts {
		t.Fatalf("the projection activity gets %d attempts, PublishEvent assumes %d", got, publishAttempts)
	}
}
