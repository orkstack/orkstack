package engine

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"testing"

	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/api/temporalproto"
	"go.temporal.io/sdk/converter"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
)

// runCreatedFirstHistory reads a committed run-created-first fixture and
// returns its run id and events.
func runCreatedFirstHistory(t *testing.T, name string) (string, []*historypb.HistoryEvent) {
	t.Helper()
	path := "testdata/history/" + name
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	recorded := &historypb.History{}
	if err := (temporalproto.CustomJSONUnmarshalOptions{DiscardUnknown: true}).Unmarshal(raw, recorded); err != nil {
		t.Fatalf("decode %s: %v", path, err)
	}
	var runID string
	payloads := recorded.GetEvents()[0].GetWorkflowExecutionStartedEventAttributes().GetInput().GetPayloads()
	if err := converter.GetDefaultDataConverter().FromPayload(payloads[0], &runID); err != nil || runID == "" {
		t.Fatalf("%s: read the run id: %v", path, err)
	}
	return runID, recorded.GetEvents()
}

// withoutPublishes is a recorded history with its last n PublishEvent
// schedulings removed (every one when n is negative): the same ending with
// those events' snapshot writes refused, so it published none of them. It is
// the one derivation the reader's rows make from the committed fixtures, and
// it is exact for what the reader reads. The reader takes an ending's events
// from the PublishEvent schedulings (isPublishEvent) and the execution's
// close, and nothing else of the run's commands; a refused write differs from
// a taken one in the failed write's marker, which the reader never looks at,
// and in the PublishEvent that a taken write goes on to schedule. The same
// endings on a real cluster are the rows of
// TestIntegration_ARefusedRunCreatedReadsBackTheDefinitionCode, which read the
// real histories back through the reader and the API.
func withoutPublishes(events []*historypb.HistoryEvent, n int) []*historypb.HistoryEvent {
	var publishes []int
	for i, e := range events {
		if attrs := e.GetActivityTaskScheduledEventAttributes(); attrs != nil && attrs.GetActivityType().GetName() == ActivityPublishEvent {
			publishes = append(publishes, i)
		}
	}
	if n >= 0 && n < len(publishes) {
		publishes = publishes[len(publishes)-n:]
	}
	drop := map[int]bool{}
	for _, i := range publishes {
		drop[i] = true
	}
	var out []*historypb.HistoryEvent
	for i, e := range events {
		if !drop[i] {
			out = append(out, e)
		}
	}
	return out
}

func eventTypesOf(events []*orkstackv1.Event) string {
	parts := make([]string, 0, len(events))
	for _, e := range events {
		parts = append(parts, fmt.Sprintf("%d:%s", e.GetSequence(), strings.TrimPrefix(e.GetType().String(), "EVENT_TYPE_")))
	}
	return "[" + strings.Join(parts, " ") + "]"
}

// TestTemporalRunReader_AnEndingBeforeStartIsServedAndArchivedWithItsGap is
// what the reader serves for an ending before start (emitRunCreatedFirst),
// from the recorded histories, to a live event read (StreamEvents and
// Events) and to the archive (EventsForArchive):
//
//   - RUN_CREATED's second write took: both events, from 1, and no gap;
//   - neither RUN_CREATED write took, the terminal event's did: the archive
//     holds the terminal event at 2 and records 1 as a gap; a live read from
//     0 fails naming the same gap, and one from 1 serves the event;
//   - a build before the gate (legacy) left the same stream, read the same;
//   - RUN_CREATED took and the terminal write did not: RUN_CREATED, then the
//     execution's close read as the terminal event at 2;
//   - nothing took: the execution's close, at 1, read as the run's one
//     event.
//
// In the last two the run published no terminal event, so in a deployment
// nothing archives it (PublishedTerminalEvent is false); EventsForArchive
// still says what the archive would hold.
func TestTemporalRunReader_AnEndingBeforeStartIsServedAndArchivedWithItsGap(t *testing.T) {
	for _, tc := range []struct {
		name, fixture string
		// strip is how many of the history's last publishes to remove (-1
		// every one).
		strip int
		// served is what an event read from 0 serves, or "" when it fails
		// naming gap; then fromOne is what one from 1 serves.
		served, fromOne string
		archived, gaps  string
		// published is whether the run published its terminal event, which
		// is what archives a run (PublishedTerminalEvent).
		published bool
	}{
		{"its second write took", "run-created-first-v1.json", 0,
			"[1:RUN_CREATED 2:RUN_FAILED]", "[2:RUN_FAILED]", "[1:RUN_CREATED 2:RUN_FAILED]", "[]", true},
		{"neither RUN_CREATED write took", "run-created-first-gap-v1.json", 0,
			"", "[2:RUN_FAILED]", "[2:RUN_FAILED]", "[1–1]", true},
		{"a build before the gate", "run-created-first-legacy-v1.json", 0,
			"", "[2:RUN_FAILED]", "[2:RUN_FAILED]", "[1–1]", true},
		{"RUN_CREATED took and the terminal did not", "run-created-first-v1.json", 1,
			"[1:RUN_CREATED 2:RUN_FAILED]", "[2:RUN_FAILED]", "[1:RUN_CREATED 2:RUN_FAILED]", "[]", false},
		{"nothing took", "run-created-first-gap-v1.json", -1,
			"[1:RUN_FAILED]", "[]", "[1:RUN_FAILED]", "[]", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			runID, events := runCreatedFirstHistory(t, tc.fixture)
			if tc.strip != 0 {
				events = withoutPublishes(events, tc.strip)
			}
			reader := &TemporalRunReader{
				client:    &fakeReadClient{history: map[string]*fakeHistory{runID: {events: events}}},
				converter: converter.GetDefaultDataConverter(),
			}
			stream := func(from int64) ([]*orkstackv1.Event, error) {
				var streamed []*orkstackv1.Event
				err := reader.StreamEvents(context.Background(), runID, from, func(e *orkstackv1.Event) error {
					streamed = append(streamed, e)
					return nil
				})
				return streamed, err
			}
			for name, read := range map[string]func(from int64) ([]*orkstackv1.Event, error){
				"StreamEvents": stream,
				"Events": func(from int64) ([]*orkstackv1.Event, error) {
					return reader.Events(context.Background(), runID, from)
				},
			} {
				got, err := read(0)
				var unpublished *UnpublishedEventsError
				switch {
				case tc.served == "" && (!errors.As(err, &unpublished) || !errors.Is(err, ErrEventsUnpublished) ||
					len(unpublished.Gaps) != 1 || unpublished.Gaps[0].First != 1 || unpublished.Gaps[0].Last != 1 ||
					!unpublished.Gaps[0].Unpublished()):
					t.Fatalf("%s from 0 = %s (%v), want it to fail naming 1–1 as never published", name, eventTypesOf(got), err)
				case tc.served == "" && len(got) != 0:
					t.Fatalf("%s from 0 served %s before failing, want nothing: the gap is the start", name, eventTypesOf(got))
				case tc.served != "" && (err != nil || eventTypesOf(got) != tc.served):
					t.Fatalf("%s from 0 = %s (%v), want %s", name, eventTypesOf(got), err, tc.served)
				}
				got, err = read(1)
				if err != nil || eventTypesOf(got) != tc.fromOne {
					t.Fatalf("%s from 1 = %s (%v), want %s", name, eventTypesOf(got), err, tc.fromOne)
				}
			}
			read, err := reader.EventsForArchive(context.Background(), runID)
			if err != nil {
				t.Fatalf("EventsForArchive: %v", err)
			}
			archived, gaps := read.Events, read.Gaps
			if got := eventTypesOf(archived); got != tc.archived {
				t.Fatalf("EventsForArchive gave %s, want %s", got, tc.archived)
			}
			ranges := make([]string, 0, len(gaps))
			for _, gap := range gaps {
				ranges = append(ranges, fmt.Sprintf("%d–%d", gap.First, gap.Last))
				if !gap.Unpublished() {
					t.Errorf("gap %d–%d gives the reason %q, want the unpublished one", gap.First, gap.Last, gap.Reason)
				}
			}
			if got := "[" + strings.Join(ranges, " ") + "]"; got != tc.gaps {
				t.Fatalf("EventsForArchive recorded gaps %s, want %s", got, tc.gaps)
			}
			if published, err := reader.PublishedTerminalEvent(context.Background(), runID); err != nil || published != tc.published {
				t.Fatalf("PublishedTerminalEvent = %v (%v), want %v", published, err, tc.published)
			}
		})
	}
}

// TestScanState_OnlyTheStartOfAFirstExecutionCanBeUnpublished: the reader
// names a gap below the first event a run's first execution published, and
// nowhere else. A number consumed later without an event is not checked, an
// execution that started from a carry has its earlier events in segments and
// begins wherever the run had got to, and a run that published RUN_CREATED
// first has no gap to name. The table runs the decision on its own: the
// recorded histories reach it only through the endings above, and no history
// a cluster records holds a mid-stream burn, so each guard that keeps the
// decision to the start of a first execution is held here. The carried guard
// is the documented rule, not a second line of defence the reader needs
// today: adoptCarry raises the scan's last sequence to the carry's before a
// carried execution's first event is delivered, so for every carry a run
// writes (its earlier events end at 1 or later) the last-sequence guard
// already refuses it, and removing this one fails no read of a real history.
func TestScanState_OnlyTheStartOfAFirstExecutionCanBeUnpublished(t *testing.T) {
	for _, tc := range []struct {
		name string
		// state: what the scan had done before this event.
		carried      bool
		lastSequence int64
		seen         []int64
		// first is the event's sequence.
		first int64
		// gap is the numbers named as never published, "" for none.
		gap string
	}{
		{name: "RUN_CREATED was not published and the next event is 2", first: 2, gap: "1–1"},
		{name: "the first event published is 5", first: 5, gap: "1–4"},
		{name: "the first event is RUN_CREATED at 1", first: 1},
		{name: "an event numbered 0 is no later than 1", first: 0},
		{name: "a number burned after the run began is not checked", lastSequence: 3, seen: []int64{1, 2, 3}, first: 5},
		{name: "events up to 3 are known to have been published, though none was delivered", lastSequence: 3, first: 5},
		{name: "an event was delivered already, whatever its number", seen: []int64{0}, first: 5},
		{name: "a generation that started from a carry begins where the run had got to", carried: true, first: 12},
	} {
		t.Run(tc.name, func(t *testing.T) {
			st := newScanState(false)
			st.carried, st.lastSequence = tc.carried, tc.lastSequence
			for _, sequence := range tc.seen {
				st.seen[sequence] = struct{}{}
			}
			gap, ok := st.unpublishedStart(&orkstackv1.Event{Sequence: tc.first})
			got := ""
			if ok {
				got = fmt.Sprintf("%d–%d", gap.First, gap.Last)
				if !gap.Unpublished() {
					t.Errorf("the gap %s gives the reason %q, want the unpublished one", got, gap.Reason)
				}
			}
			if got != tc.gap {
				t.Fatalf("unpublishedStart(%d) named %q, want %q", tc.first, got, tc.gap)
			}
		})
	}
}
