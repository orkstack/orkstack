package engine

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/converter"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/anypb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
)

// The cluster can answer a history page with CurrentBranchChanged: the page
// token the read was following names a branch or state transition the
// execution has moved past. A followed stream that ended there reported a run
// that had succeeded as unavailable. These tests prove the read restarts on
// that answer alone — no gap, no duplicate, a bounded number of times in a
// row — and that every other error still ends it as before.

// branchChangedMessage is the cluster's own text for the answer.
const branchChangedMessage = "Current and request branch tokens, or current and request versioned transitions, don't match."

// branchChangedError is the answer as the SDK hands it over: the status the
// cluster sent, decoded by the API's serviceerror, which has no type for the
// server-internal detail and so yields an InvalidArgument that keeps it.
func branchChangedError(t *testing.T) error {
	t.Helper()
	wire := status.New(codes.InvalidArgument, branchChangedMessage).Proto()
	wire.Details = append(wire.Details, &anypb.Any{
		TypeUrl: "type.googleapis.com/temporal.server.api.errordetails.v1.CurrentBranchChangedFailure",
		Value:   []byte{0x0a, 0x02, 0x01, 0x02}, // current_branch_token: 0x0102
	})
	err := serviceerror.FromStatus(status.FromProto(wire))
	var invalid *serviceerror.InvalidArgument
	if !errors.As(err, &invalid) {
		t.Fatalf("decoded answer = %T, want *serviceerror.InvalidArgument", err)
	}
	return err
}

// otherDetailError is an InvalidArgument carrying some other detail.
func otherDetailError(t *testing.T) error {
	t.Helper()
	wire := status.New(codes.InvalidArgument, branchChangedMessage).Proto()
	wire.Details = append(wire.Details, &anypb.Any{
		TypeUrl: "type.googleapis.com/temporal.server.api.errordetails.v1.TaskAlreadyStartedFailure",
	})
	return serviceerror.FromStatus(status.FromProto(wire))
}

func TestHistoryBranchChanged_MatchesOnlyTheClusterDetail(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want bool
	}{
		{"the cluster's answer", branchChangedError(t), true},
		{"the answer, wrapped", fmt.Errorf("page: %w", branchChangedError(t)), true},
		// The same text without the detail is not the answer: the match is
		// on the detail's type, never the message.
		{"the text alone", serviceerror.NewInvalidArgument(branchChangedMessage), false},
		{"another detail", otherDetailError(t), false},
		{"unavailable", serviceerror.NewUnavailable("frontend unreachable"), false},
		{"not found", serviceerror.NewNotFound("workflow execution not found"), false},
		{"a plain error", errors.New(branchChangedMessage), false},
		{"nil", nil, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := historyBranchChanged(tc.err); got != tc.want {
				t.Fatalf("historyBranchChanged(%v) = %v, want %v", tc.err, got, tc.want)
			}
		})
	}
}

// historyCut ends one history read: after events have been served, the
// iterator fails with err.
type historyCut struct {
	after int
	err   error
}

// interruptingReadClient serves a fakeReadClient's histories, cutting the
// i-th history read at cuts[i]; a read past the list is served whole.
// beforeRead, when set, runs before each read with its 0-based index.
type interruptingReadClient struct {
	*fakeReadClient
	cuts       []historyCut
	reads      int
	beforeRead func(read int)
}

func (c *interruptingReadClient) GetWorkflowHistory(
	ctx context.Context,
	workflowID, execution string,
	isLongPoll bool,
	filter enumspb.HistoryEventFilterType,
) client.HistoryEventIterator {
	read := c.reads
	c.reads++
	if c.beforeRead != nil {
		c.beforeRead(read)
	}
	whole := c.fakeReadClient.GetWorkflowHistory(ctx, workflowID, execution, isLongPoll, filter).(*fakeHistory)
	if read >= len(c.cuts) {
		return whole
	}
	cut := c.cuts[read]
	return &fakeHistory{events: whole.events[:min(cut.after, len(whole.events))], err: cut.err}
}

// cutsOf is n cuts at the given points, each failing with err.
func cutsOf(err error, after ...int) []historyCut {
	cuts := make([]historyCut, 0, len(after))
	for _, n := range after {
		cuts = append(cuts, historyCut{after: n, err: err})
	}
	return cuts
}

// helloReaderCut is a reader over helloRun (history: started, then sequences
// 1–4 at event ids 2–5, the last RUN_SUCCEEDED) whose reads are cut.
func helloReaderCut(t *testing.T, runID string, cuts []historyCut) (*TemporalRunReader, *interruptingReadClient) {
	t.Helper()
	history, store := helloRun(t, runID)
	fake := &interruptingReadClient{
		fakeReadClient: &fakeReadClient{history: map[string]*fakeHistory{runID: {events: history}}},
		cuts:           cuts,
	}
	return newRunReader(fake, converter.GetDefaultDataConverter()).WithSnapshotStore(store), fake
}

func streamAll(t *testing.T, reader *TemporalRunReader, runID string, from int64) ([]*orkstackv1.Event, error) {
	t.Helper()
	var got []*orkstackv1.Event
	err := reader.StreamEvents(context.Background(), runID, from, func(e *orkstackv1.Event) error {
		got = append(got, e)
		return nil
	})
	return got, err
}

func TestTemporalRunReader_StreamRestartsAHistoryThatChangedUnderIt(t *testing.T) {
	const runID = "ork-restart-1"
	branch := branchChangedError(t)
	cases := []struct {
		name string
		from int64
		cuts []historyCut
	}{
		{"before the first event", 0, cutsOf(branch, 0)},
		{"after the started event", 0, cutsOf(branch, 1)},
		{"mid-stream", 0, cutsOf(branch, 3)},
		{"right before the terminal event", 0, cutsOf(branch, 4)},
		{"mid-stream, resumed from a sequence", 2, cutsOf(branch, 3)},
		{"before the resume point", 3, cutsOf(branch, 2)},
		// Three restarts in a row is the bound, and each restart here
		// handles nothing new before failing again.
		{"the bound, in a row", 0, cutsOf(branch, 3, 3, 3, 3)[:maxHistoryRestarts]},
		// A restart that handles a new event resets the count, so a read
		// that keeps moving is restarted past the bound.
		{"more than the bound, each after progress", 0, cutsOf(branch, 1, 2, 3, 4)},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			reader, fake := helloReaderCut(t, runID, tc.cuts)
			got, err := streamAll(t, reader, runID, tc.from)
			if err != nil {
				t.Fatalf("StreamEvents: %v", err)
			}
			requireContiguous(t, got, tc.from+1, 4)
			if last := got[len(got)-1]; last.GetType() != orkstackv1.EventType_EVENT_TYPE_RUN_SUCCEEDED {
				t.Fatalf("stream ended on %v, want the terminal event", last)
			}
			if want := len(tc.cuts) + 1; fake.reads != want {
				t.Fatalf("history reads = %d, want %d: one per restart and the first", fake.reads, want)
			}
			for i, execution := range fake.historyExecutions {
				if execution != "" {
					t.Fatalf("read %d named execution %q, want the same (current) execution each time", i, execution)
				}
			}
			if !fake.longPolled {
				t.Fatal("the restarted read did not long-poll")
			}
		})
	}
}

func TestTemporalRunReader_StreamEndsWhenTheHistoryKeepsChanging(t *testing.T) {
	const runID = "ork-restart-2"
	branch := branchChangedError(t)
	// Every read fails at the same point: no restart handles anything new.
	reader, fake := helloReaderCut(t, runID, cutsOf(branch, 3, 3, 3, 3, 3, 3, 3, 3))
	got, err := streamAll(t, reader, runID, 0)
	if err == nil {
		t.Fatal("StreamEvents succeeded, want the persistent answer to end it")
	}
	if !historyBranchChanged(err) {
		t.Fatalf("error = %v, want it to wrap the cluster's answer", err)
	}
	if errors.Is(err, ErrRunNotFound) {
		t.Fatalf("error = %v: a run whose history keeps changing is not missing", err)
	}
	if want := fmt.Sprintf("changed under the read %d times in a row", maxHistoryRestarts+1); !strings.Contains(err.Error(), want) {
		t.Fatalf("error = %q, want it to say %q", err, want)
	}
	if want := maxHistoryRestarts + 1; fake.reads != want {
		t.Fatalf("history reads = %d, want %d", fake.reads, want)
	}
	// What was handled before the first failure was delivered once, and
	// nothing after it.
	requireContiguous(t, got, 1, 2)
}

func TestTemporalRunReader_OtherHistoryErrorsStillEndTheStream(t *testing.T) {
	const runID = "ork-restart-3"
	cases := []struct {
		name string
		err  error
	}{
		{"unavailable", serviceerror.NewUnavailable("frontend unreachable")},
		{"the text without the detail", serviceerror.NewInvalidArgument(branchChangedMessage)},
		{"another detail", otherDetailError(t)},
		{"not found after events", serviceerror.NewNotFound("workflow execution not found")},
		{"a plain error", errors.New("connection reset")},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			reader, fake := helloReaderCut(t, runID, cutsOf(tc.err, 3))
			got, err := streamAll(t, reader, runID, 0)
			if err == nil || !strings.Contains(err.Error(), "read history of run "+runID) {
				t.Fatalf("StreamEvents error = %v, want the read to end with it", err)
			}
			if !errors.Is(err, tc.err) {
				t.Fatalf("error = %v, want it to wrap %v", err, tc.err)
			}
			if errors.Is(err, ErrRunNotFound) {
				t.Fatalf("error = %v: a run whose events were read is never not found", err)
			}
			if fake.reads != 1 {
				t.Fatalf("history reads = %d, want 1: no other error restarts the read", fake.reads)
			}
			requireContiguous(t, got, 1, 2)
		})
	}
}

// A plain read (GetRun, Events) pages the same way and restarts the same way.
func TestTemporalRunReader_EventsRestartAHistoryThatChangedUnderIt(t *testing.T) {
	const runID = "ork-restart-4"
	reader, fake := helloReaderCut(t, runID, cutsOf(branchChangedError(t), 3))
	events, err := reader.Events(context.Background(), runID, 0)
	if err != nil {
		t.Fatalf("Events: %v", err)
	}
	requireContiguous(t, events, 1, 4)
	if fake.reads != 2 {
		t.Fatalf("history reads = %d, want 2", fake.reads)
	}
}

// A restart of the current execution that lands on its successor — the run
// continued as new between the failed read and the restart — reads the
// successor from its start: its carry replays the rest of the earlier
// generation from its segment, and what was already delivered is not
// delivered again.
func TestTemporalRunReader_RestartThatLandsOnTheSuccessorHasNoGap(t *testing.T) {
	f := newChain(t, chainSpec{closed: []int{5}, current: 3, finish: true})
	f.readFrom(0)
	fake := &interruptingReadClient{
		fakeReadClient: f.client,
		// Generation 0's history: started, then sequences 1–5. Cut after 3.
		cuts: cutsOf(branchChangedError(t), 4),
		beforeRead: func(read int) {
			if read == 1 {
				f.readFrom(1)
			}
		},
	}
	reader := newRunReader(fake, converter.GetDefaultDataConverter()).WithSnapshotStore(f.store)
	got, err := streamAll(t, reader, f.runID, 0)
	if err != nil {
		t.Fatalf("StreamEvents: %v", err)
	}
	requireContiguous(t, got, 1, 9)
	if f.segmentReads() != 1 {
		t.Fatalf("segment reads = %d, want 1: the rest of generation 0 comes from its segment", f.segmentReads())
	}
	if want := []string{"", ""}; strings.Join(fake.historyExecutions, ",") != strings.Join(want, ",") {
		t.Fatalf("history reads = %v, want %v", fake.historyExecutions, want)
	}
}
