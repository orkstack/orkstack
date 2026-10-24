package engine_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"google.golang.org/protobuf/types/known/timestamppb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// gappedEvents is a stream whose events 3–5 were lost: 1, 2, then 6, 7.
func gappedEvents(runID string) []*orkstackv1.Event {
	var out []*orkstackv1.Event
	for _, seq := range []int64{1, 2, 6, 7} {
		out = append(out, &orkstackv1.Event{RunId: runID, Sequence: seq, Type: orkstackv1.EventType_EVENT_TYPE_TASK_LOG})
	}
	out[len(out)-1].Type = orkstackv1.EventType_EVENT_TYPE_RUN_SUCCEEDED
	return out
}

func TestMarshalRunArchive_WritesGapLinesInSequenceOrder(t *testing.T) {
	run := sampleRun(t, "ork-gap", orkstackv1.Status_STATUS_SUCCEEDED, at(t, "2026-10-01T00:00:00Z"))
	body, err := engine.MarshalRunArchive(run, gappedEvents("ork-gap"),
		engine.EventGap{First: 3, Last: 5, Reason: "segment object missing"})
	if err != nil {
		t.Fatal(err)
	}
	var kinds []string
	for _, line := range bytes.Split(bytes.TrimSpace(body), []byte("\n")) {
		var record struct {
			Kind  string `json:"kind"`
			Event struct {
				Sequence string `json:"sequence"`
			} `json:"event"`
			Gap *engine.EventGap `json:"gap"`
		}
		if err := json.Unmarshal(line, &record); err != nil {
			t.Fatal(err)
		}
		switch record.Kind {
		case engine.ArchiveKindEvent:
			kinds = append(kinds, record.Event.Sequence)
		case engine.ArchiveKindGap:
			kinds = append(kinds, "gap")
			if record.Gap.First != 3 || record.Gap.Last != 5 || record.Gap.Reason != "segment object missing" {
				t.Fatalf("gap line = %+v", record.Gap)
			}
		default:
			kinds = append(kinds, record.Kind)
		}
	}
	if got := strings.Join(kinds, ","); got != "run,1,2,gap,6,7" {
		t.Fatalf("lines = %s, want run,1,2,gap,6,7", got)
	}
	if !bytes.Contains(body, []byte(`{"kind":"gap","gap":{"first":3,"last":5,"reason":"segment object missing"}}`)) {
		t.Fatalf("gap line is not the documented shape:\n%s", body)
	}
}

func TestUnmarshalRunArchive_SkipsGapLines(t *testing.T) {
	run := sampleRun(t, "ork-gap", orkstackv1.Status_STATUS_SUCCEEDED, at(t, "2026-10-01T00:00:00Z"))
	body, err := engine.MarshalRunArchive(run, gappedEvents("ork-gap"), engine.EventGap{First: 3, Last: 5, Reason: "x"})
	if err != nil {
		t.Fatal(err)
	}
	loaded, events, err := engine.UnmarshalRunArchive(body)
	if err != nil || loaded.GetId() != "ork-gap" || len(events) != 4 {
		t.Fatalf("UnmarshalRunArchive = %v, %d events, %v", loaded.GetId(), len(events), err)
	}
	if line, err := engine.UnmarshalRunLine(body); err != nil || line.GetId() != "ork-gap" {
		t.Fatalf("UnmarshalRunLine = %v, %v", line, err)
	}
}

func TestUnmarshalRunArchiveGaps(t *testing.T) {
	run := sampleRun(t, "ork-gap", orkstackv1.Status_STATUS_SUCCEEDED, at(t, "2026-10-01T00:00:00Z"))
	gaps := []engine.EventGap{{First: 3, Last: 5, Reason: "segment object missing"}, {First: 8, Last: 9, Reason: "segment failed verification"}}
	body, err := engine.MarshalRunArchive(run, gappedEvents("ork-gap"), gaps...)
	if err != nil {
		t.Fatal(err)
	}
	got, err := engine.UnmarshalRunArchiveGaps(body)
	if err != nil || len(got) != 2 || got[0] != gaps[0] || got[1] != gaps[1] {
		t.Fatalf("gaps = %+v, %v; want %+v", got, err, gaps)
	}
	whole, err := engine.MarshalRunArchive(run, gappedEvents("ork-gap"))
	if err != nil {
		t.Fatal(err)
	}
	if got, err := engine.UnmarshalRunArchiveGaps(whole); err != nil || got != nil {
		t.Fatalf("an ungapped body = %+v, %v; want none", got, err)
	}
}

// gapSource is a RunEventSource whose archive read reports a lost range, or
// fails.
type gapSource struct {
	events []*orkstackv1.Event
	gaps   []engine.EventGap
	err    error
}

func (s *gapSource) Events(context.Context, string, int64) ([]*orkstackv1.Event, error) {
	return s.events, s.err
}

func (s *gapSource) EventsForArchive(context.Context, string) ([]*orkstackv1.Event, []engine.EventGap, error) {
	return s.events, s.gaps, s.err
}

func publishTerminal(t *testing.T, source *gapSource) (*artifacts.LocalRunArchive, *engine.RunArchiver, error) {
	t.Helper()
	archiver, store, _ := newArchiveFixture(t)
	activities, snapshots := eventActivitiesWithSnapshots(
		engine.NewArchivingEventActivities(engine.NewEventBus(), engine.NewRunStore(), archiver).WithEventSource(source))
	created := at(t, "2026-10-01T00:00:00Z")
	finished := sampleRun(t, "ork-gap", orkstackv1.Status_STATUS_SUCCEEDED, created)
	err := activities.PublishEvent(context.Background(), &orkstackv1.Event{
		RunId: "ork-gap", Sequence: 7, Type: orkstackv1.EventType_EVENT_TYPE_RUN_SUCCEEDED,
		Status: orkstackv1.Status_STATUS_SUCCEEDED, Timestamp: timestamppb.New(created),
	}, snapshots.putRun(t, finished), "ork-gap#7")
	return store, archiver, err
}

func TestPublishEvent_ArchivesWithAGapWhenASegmentIsLost(t *testing.T) {
	store, archiver, err := publishTerminal(t, &gapSource{events: gappedEvents("ork-gap"),
		gaps: []engine.EventGap{{First: 3, Last: 5, Reason: "segment object missing"}}})
	if err != nil {
		t.Fatalf("PublishEvent: %v", err)
	}
	body, err := store.GetRun(context.Background(), "ork-gap")
	if err != nil {
		t.Fatalf("the run was not archived: %v", err)
	}
	gaps, err := engine.UnmarshalRunArchiveGaps(body)
	if err != nil || len(gaps) != 1 || gaps[0].First != 3 {
		t.Fatalf("archived gaps = %+v, %v", gaps, err)
	}
	events, err := archiver.LoadRunEvents(context.Background(), "ork-gap")
	var lost *engine.ArchivedEventsLostError
	if !errors.As(err, &lost) || !errors.Is(err, engine.ErrArchivedEventsLost) || len(events) != 4 ||
		!strings.Contains(err.Error(), "3–5 (segment object missing)") {
		t.Fatalf("LoadRunEvents = %d events, %v; want the events and the loss naming 3–5", len(events), err)
	}
}

func TestPublishEvent_RetriesWhenASegmentIsTransientlyUnavailable(t *testing.T) {
	store, _, err := publishTerminal(t, &gapSource{err: engine.ErrEventSegmentUnavailable})
	if err == nil {
		t.Fatal("PublishEvent succeeded over an unreadable segment; the activity must retry")
	}
	if _, err := store.GetRun(context.Background(), "ork-gap"); !errors.Is(err, artifacts.ErrNotFound) {
		t.Fatalf("a run was archived from a partial view: %v", err)
	}
}
