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

// withoutPublishes is a history with its last n PublishEvent schedulings
// removed (every one when n is negative): the same ending with those
// events' snapshot writes refused, so it published none of them.
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
			archived, gaps, err := reader.EventsForArchive(context.Background(), runID)
			if err != nil {
				t.Fatalf("EventsForArchive: %v", err)
			}
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
