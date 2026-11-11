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
	"github.com/orkstack/orkstack/internal/enumname"

	"github.com/orkstack/orkstack/internal/testsupport/must"
)

// runCreatedFirstHistory reads a committed run-created-first fixture and
// returns its run id and events.
func runCreatedFirstHistory(t *testing.T, name string) (string, []*historypb.HistoryEvent) {
	t.Helper()
	path := "testdata/history/" + name
	raw, err := os.ReadFile(path)
	must.OKf(t, err, "read %s", path)
	recorded := &historypb.History{}
	must.OKf(t, (temporalproto.CustomJSONUnmarshalOptions{DiscardUnknown: true}).Unmarshal(raw, recorded), "decode %s", path)
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
		parts = append(parts, fmt.Sprintf("%d:%s", e.GetSequence(), enumname.Of(e.GetType())))
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
//     0 serves a STREAM_GAP naming 1 and then the event, and one from 1 only
//     the event;
//   - a build before the gate (legacy) left the same stream, read the same;
//   - RUN_CREATED took and the terminal write did not: RUN_CREATED, then the
//     execution's close read as the terminal event at 2;
//   - nothing took: the execution's close, at 1, read as the run's one
//     event.
//
// In the last two the run published no terminal event, so its archive comes
// from its history (ArchiveFromHistory), which holds what EventsForArchive says.
func TestTemporalRunReader_AnEndingBeforeStartIsServedAndArchivedWithItsGap(t *testing.T) {
	for _, tc := range []struct {
		name, fixture string
		// strip is how many of the history's last publishes to remove (-1
		// every one).
		strip int
		// served is what an event read from 0 serves; fromOne, one from 1.
		served, fromOne string
		archived, gaps  string
	}{
		{"its second write took", "run-created-first-v1.json", 0,
			"[1:RUN_CREATED 2:RUN_FAILED]", "[2:RUN_FAILED]", "[1:RUN_CREATED 2:RUN_FAILED]", "[]"},
		{"neither RUN_CREATED write took", "run-created-first-gap-v1.json", 0,
			"[1:STREAM_GAP 2:RUN_FAILED]", "[2:RUN_FAILED]", "[2:RUN_FAILED]", "[1–1]"},
		{"a build before the gate", "run-created-first-legacy-v1.json", 0,
			"[1:STREAM_GAP 2:RUN_FAILED]", "[2:RUN_FAILED]", "[2:RUN_FAILED]", "[1–1]"},
		{"RUN_CREATED took and the terminal did not", "run-created-first-v1.json", 1,
			"[1:RUN_CREATED 2:RUN_FAILED]", "[2:RUN_FAILED]", "[1:RUN_CREATED 2:RUN_FAILED]", "[]"},
		{"nothing took", "run-created-first-gap-v1.json", -1,
			"[1:RUN_FAILED]", "[]", "[1:RUN_FAILED]", "[]"},
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
			if got := eventTypesOf(liveReadsAgree(t, reader, runID, 0)); got != tc.served {
				t.Fatalf("a live read from 0 served %s, want %s", got, tc.served)
			}
			if got := eventTypesOf(liveReadsAgree(t, reader, runID, 1)); got != tc.fromOne {
				t.Fatalf("a live read from 1 served %s, want %s", got, tc.fromOne)
			}
			read, err := reader.EventsForArchive(context.Background(), runID)
			must.OKf(t, err, "EventsForArchive")
			if got := eventTypesOf(read.Events); got != tc.archived {
				t.Fatalf("EventsForArchive gave %s, want %s", got, tc.archived)
			}
			for _, gap := range read.Gaps {
				if !gap.Unpublished() {
					t.Errorf("gap %d–%d gives the reason %q, want the unpublished one", gap.First, gap.Last, gap.Reason)
				}
			}
			if got := gapRanges(read.Gaps); got != tc.gaps {
				t.Fatalf("EventsForArchive recorded gaps %s, want %s", got, tc.gaps)
			}
		})
	}
}
