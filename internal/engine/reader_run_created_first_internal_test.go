package engine

import (
	"context"
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

// withoutPublishes is a history with every PublishEvent scheduling removed:
// the same ending with every one of its snapshot writes refused, so it
// published nothing.
func withoutPublishes(events []*historypb.HistoryEvent) []*historypb.HistoryEvent {
	var out []*historypb.HistoryEvent
	for _, e := range events {
		if attrs := e.GetActivityTaskScheduledEventAttributes(); attrs != nil && attrs.GetActivityType().GetName() == ActivityPublishEvent {
			continue
		}
		out = append(out, e)
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
// from the recorded histories, live (StreamEvents) and to the archive
// (EventsForArchive):
//
//   - RUN_CREATED's second write took: both events, from 1, and no gap;
//   - neither RUN_CREATED write took, the terminal event's did: the terminal
//     event alone, at 2, and the archive records 1 as an explicit gap;
//   - a build before the gate (legacy) left the same stream, and its archive
//     records the same gap;
//   - nothing took: the execution's close, at 1, read as the run's one
//     event — a run that publishes no terminal event is never archived, so
//     there is no archive for a gap to belong to.
func TestTemporalRunReader_AnEndingBeforeStartIsServedAndArchivedWithItsGap(t *testing.T) {
	for _, tc := range []struct {
		name, fixture string
		strip         bool
		served        string
		gaps          string
	}{
		{"its second write took", "run-created-first-v1.json", false, "[1:RUN_CREATED 2:RUN_FAILED]", "[]"},
		{"neither RUN_CREATED write took", "run-created-first-gap-v1.json", false, "[2:RUN_FAILED]", "[1–1]"},
		{"a build before the gate", "run-created-first-legacy-v1.json", false, "[2:RUN_FAILED]", "[1–1]"},
		{"nothing took", "run-created-first-gap-v1.json", true, "[1:RUN_FAILED]", "[]"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			runID, events := runCreatedFirstHistory(t, tc.fixture)
			if tc.strip {
				events = withoutPublishes(events)
			}
			reader := &TemporalRunReader{
				client:    &fakeReadClient{history: map[string]*fakeHistory{runID: {events: events}}},
				converter: converter.GetDefaultDataConverter(),
			}
			var streamed []*orkstackv1.Event
			if err := reader.StreamEvents(context.Background(), runID, 0, func(e *orkstackv1.Event) error {
				streamed = append(streamed, e)
				return nil
			}); err != nil {
				t.Fatalf("StreamEvents: %v", err)
			}
			if got := eventTypesOf(streamed); got != tc.served {
				t.Fatalf("StreamEvents served %s, want %s", got, tc.served)
			}
			archived, gaps, err := reader.EventsForArchive(context.Background(), runID)
			if err != nil {
				t.Fatalf("EventsForArchive: %v", err)
			}
			if got := eventTypesOf(archived); got != tc.served {
				t.Fatalf("EventsForArchive gave %s, want %s", got, tc.served)
			}
			ranges := make([]string, 0, len(gaps))
			for _, gap := range gaps {
				ranges = append(ranges, fmt.Sprintf("%d–%d", gap.First, gap.Last))
				if !strings.Contains(gap.Reason, "never written") {
					t.Errorf("gap %d–%d gives the reason %q, want it to say the events were never written",
						gap.First, gap.Last, gap.Reason)
				}
			}
			if got := "[" + strings.Join(ranges, " ") + "]"; got != tc.gaps {
				t.Fatalf("EventsForArchive recorded gaps %s, want %s", got, tc.gaps)
			}
		})
	}
}
