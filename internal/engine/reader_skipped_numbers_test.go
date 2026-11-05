package engine

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	"google.golang.org/protobuf/types/known/timestamppb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
)

// A run uses an event number without publishing an event when object storage
// refuses that event's record (emit). A live read shows the numbers and goes
// on to the run's end; the archive records them as a gap.

// skippedRun is a run that published exactly these numbers and ended
// RUN_SUCCEEDED at the last of them.
func skippedRun(t *testing.T, runID string, sequences ...int64) ([]*historypb.HistoryEvent, *countingSnapshotStore) {
	t.Helper()
	wf := &orkstackv1.Orkflow{Name: "hello-ork", Version: "0.1.0"}
	store := &countingSnapshotStore{objects: map[string][]byte{}}
	events := []*historypb.HistoryEvent{startedEvent(t, runID, wf)}
	for i, sequence := range sequences {
		eventType, status := orkstackv1.EventType_EVENT_TYPE_TASK_LOG, orkstackv1.Status_STATUS_RUNNING
		if i == len(sequences)-1 {
			eventType, status = orkstackv1.EventType_EVENT_TYPE_RUN_SUCCEEDED, orkstackv1.Status_STATUS_SUCCEEDED
		}
		uri := snapshotURIFor(runID, sequence)
		store.objects[uri] = mustMarshalRun(t, &orkstackv1.Run{Id: runID, OrkflowName: wf.GetName(), Status: status,
			CreatedAt: timestamppb.New(readerEpoch)})
		events = append(events, publishEventWithURI(t, int64(len(events)+1), &orkstackv1.Event{
			RunId: runID, Sequence: sequence, Type: eventType, Status: status,
			Timestamp: timestamppb.New(readerEpoch.Add(time.Duration(sequence) * time.Second)),
		}, uri))
	}
	return append(events, closedEvent(int64(len(events)+1), enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED,
		readerEpoch.Add(time.Minute))), store
}

// liveReadsAgree reads the stream from a number both ways a live read can,
// holds them to one answer, and returns it.
func liveReadsAgree(t *testing.T, reader *TemporalRunReader, runID string, from int64) []*orkstackv1.Event {
	t.Helper()
	var streamed []*orkstackv1.Event
	if err := reader.StreamEvents(context.Background(), runID, from, func(e *orkstackv1.Event) error {
		streamed = append(streamed, e)
		return nil
	}); err != nil {
		t.Fatalf("StreamEvents from %d: %v", from, err)
	}
	read, err := reader.Events(context.Background(), runID, from)
	if err != nil {
		t.Fatalf("Events from %d: %v", from, err)
	}
	if eventTypesOf(streamed) != eventTypesOf(read) {
		t.Fatalf("from %d: StreamEvents served %s, Events %s", from, eventTypesOf(streamed), eventTypesOf(read))
	}
	return streamed
}

func gapMessages(events []*orkstackv1.Event) []string {
	var out []string
	for _, e := range events {
		if e.GetType() == orkstackv1.EventType_EVENT_TYPE_STREAM_GAP {
			out = append(out, e.GetMessage())
		}
	}
	return out
}

func gapRanges(gaps []EventGap) string {
	out := make([]string, 0, len(gaps))
	for _, gap := range gaps {
		out = append(out, fmt.Sprintf("%d–%d", gap.First, gap.Last))
	}
	return "[" + strings.Join(out, " ") + "]"
}

// TestSkippedNumbers_ALiveReadShowsThemAndReachesTheEnd: a live read over
// numbers the run never published says which, and still reaches the run's
// terminal event; the numbers it has passed are not said again on a resume.
func TestSkippedNumbers_ALiveReadShowsThemAndReachesTheEnd(t *testing.T) {
	const runID = "ork-skipped"
	events, store := skippedRun(t, runID, 1, 2, 5, 6, 8)
	reader, _ := readerOver(events, runID)
	reader = reader.WithSnapshotStore(store)
	const gapOfThreeAndFour = "events 3–4 of this run were never published"
	const gapOfSeven = "events 7–7 of this run were never published"
	for _, tc := range []struct {
		from   int64
		served string
		gaps   []string
	}{
		{0, "[1:TASK_LOG 2:TASK_LOG 4:STREAM_GAP 5:TASK_LOG 6:TASK_LOG 7:STREAM_GAP 8:RUN_SUCCEEDED]",
			[]string{gapOfThreeAndFour, gapOfSeven}},
		// A resume inside the numbers names only what it has not passed.
		{3, "[4:STREAM_GAP 5:TASK_LOG 6:TASK_LOG 7:STREAM_GAP 8:RUN_SUCCEEDED]",
			[]string{"events 4–4 of this run were never published", gapOfSeven}},
		{4, "[5:TASK_LOG 6:TASK_LOG 7:STREAM_GAP 8:RUN_SUCCEEDED]", []string{gapOfSeven}},
		{7, "[8:RUN_SUCCEEDED]", nil},
	} {
		t.Run(fmt.Sprintf("from %d", tc.from), func(t *testing.T) {
			served := liveReadsAgree(t, reader, runID, tc.from)
			if got := eventTypesOf(served); got != tc.served {
				t.Fatalf("served %s, want %s", got, tc.served)
			}
			messages := gapMessages(served)
			if len(messages) != len(tc.gaps) {
				t.Fatalf("gaps said %q, want %q", messages, tc.gaps)
			}
			for i, want := range tc.gaps {
				if !strings.HasPrefix(messages[i], want) {
					t.Fatalf("gap %d said %q, want %q", i, messages[i], want)
				}
			}
			for _, e := range served {
				if e.GetRunId() != runID || e.GetTimestamp() == nil {
					t.Fatalf("event %v does not say its run and when", e)
				}
			}
		})
	}
	run, err := reader.GetRun(context.Background(), runID)
	if err != nil || run.GetStatus() != orkstackv1.Status_STATUS_SUCCEEDED {
		t.Fatalf("GetRun = %v (%v), want the run SUCCEEDED: skipped numbers change no outcome", run, err)
	}
}

// TestSkippedNumbers_AnArchiveRecordsThemAsAGap: the archive holds the events
// the run published and one gap per range, and a read of it over a gap is the
// existing DataLoss answer. The gap event a live read serves is not in it.
func TestSkippedNumbers_AnArchiveRecordsThemAsAGap(t *testing.T) {
	ctx := context.Background()
	const runID = "ork-skipped-archive"
	events, store := skippedRun(t, runID, 1, 2, 5, 6, 8)
	reader, _ := readerOver(events, runID)
	reader = reader.WithSnapshotStore(store)
	read, err := reader.EventsForArchive(ctx, runID)
	if err != nil {
		t.Fatalf("EventsForArchive: %v", err)
	}
	if got := eventTypesOf(read.Events); got != "[1:TASK_LOG 2:TASK_LOG 5:TASK_LOG 6:TASK_LOG 8:RUN_SUCCEEDED]" {
		t.Fatalf("EventsForArchive gave %s", got)
	}
	if got := gapRanges(read.Gaps); got != "[3–4 7–7]" {
		t.Fatalf("EventsForArchive recorded gaps %s, want [3–4 7–7]", got)
	}
	for _, gap := range read.Gaps {
		if !gap.Unpublished() {
			t.Fatalf("gap %d–%d reads as lost (%q), want never published", gap.First, gap.Last, gap.Reason)
		}
	}
	archiver := newLocalArchiver(t)
	if err := NewHistoryArchiver(reader, archiver).ArchiveFromHistory(ctx, runID); err != nil {
		t.Fatalf("ArchiveFromHistory: %v", err)
	}
	stored, err := archiver.LoadRunEvents(ctx, runID)
	var lost *ArchivedEventsLostError
	if !errors.As(err, &lost) || gapRanges(lost.Gaps) != "[3–4 7–7]" {
		t.Fatalf("LoadRunEvents = %v, want both ranges named", err)
	}
	if got := eventTypesOf(stored); got != eventTypesOf(read.Events) {
		t.Fatalf("the archive holds %s", got)
	}
}

// TestSkippedNumbers_AcrossTheRunsParts: a long run's stream comes from event
// segments and the current history. A number skipped anywhere in it — inside
// a segment, as the last number of one, or filling a whole segment — reads the
// same as one skipped in a single history.
func TestSkippedNumbers_AcrossTheRunsParts(t *testing.T) {
	ctx := context.Background()
	for _, tc := range []struct {
		name string
		spec chainSpec
		// from is where the live read starts.
		from   int64
		served string
		gaps   string
	}{
		{
			name:   "inside a segment",
			spec:   chainSpec{closed: []int{5}, current: 2, finish: true, skip: map[int64]bool{3: true}},
			served: "[1:RUN_CREATED 2:RUN_STARTED 3:STREAM_GAP 4:TASK_LOG 5:TASK_LOG 6:TASK_LOG 7:TASK_LOG 8:RUN_SUCCEEDED]",
			gaps:   "[3–3]",
		},
		{
			name:   "the last number of a segment",
			spec:   chainSpec{closed: []int{5}, current: 2, finish: true, skip: map[int64]bool{5: true}},
			served: "[1:RUN_CREATED 2:RUN_STARTED 3:TASK_LOG 4:TASK_LOG 5:STREAM_GAP 6:TASK_LOG 7:TASK_LOG 8:RUN_SUCCEEDED]",
			gaps:   "[5–5]",
		},
		{
			name:   "a whole segment",
			spec:   chainSpec{closed: []int{2, 2}, current: 1, finish: true, skip: map[int64]bool{3: true, 4: true}},
			served: "[1:RUN_CREATED 2:RUN_STARTED 4:STREAM_GAP 5:TASK_LOG 6:RUN_SUCCEEDED]",
			gaps:   "[3–4]",
		},
		{
			name:   "in the current history, after the carried numbers",
			spec:   chainSpec{closed: []int{2}, current: 3, finish: true, skip: map[int64]bool{4: true}},
			served: "[1:RUN_CREATED 2:RUN_STARTED 3:TASK_LOG 4:STREAM_GAP 5:TASK_LOG 6:RUN_SUCCEEDED]",
			gaps:   "[4–4]",
		},
		{
			name: "a read that resumes after the segment holding them does not name them",
			spec: chainSpec{closed: []int{5}, current: 2, finish: true, skip: map[int64]bool{3: true}},
			from: 4, served: "[5:TASK_LOG 6:TASK_LOG 7:TASK_LOG 8:RUN_SUCCEEDED]",
			gaps: "[3–3]",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newChain(t, tc.spec)
			served := liveReadsAgree(t, f.reader, f.runID, tc.from)
			if got := eventTypesOf(served); got != tc.served {
				t.Fatalf("served %s, want %s", got, tc.served)
			}
			read, err := f.reader.EventsForArchive(ctx, f.runID)
			if err != nil {
				t.Fatalf("EventsForArchive: %v", err)
			}
			if got := gapRanges(read.Gaps); got != tc.gaps {
				t.Fatalf("EventsForArchive recorded gaps %s, want %s", got, tc.gaps)
			}
			if got := len(gapMessages(read.Events)); got != 0 {
				t.Fatalf("EventsForArchive holds %d gap events: they belong to a live read", got)
			}
		})
	}
}
