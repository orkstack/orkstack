package engine

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/sdk/converter"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// A run the workflow never archived — terminated from outside, or whose
// ending object storage refused — is archived from its history instead. These
// tests hold what that archive says: what a live read of the run said, or the
// same with what could not be recovered stated.

var archiveClosedAt = readerEpoch.Add(time.Minute)

func newLocalArchiver(t *testing.T) *RunArchiver {
	t.Helper()
	store, err := artifacts.NewLocalRunArchive(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	return NewRunArchiver(store)
}

// withTaskRow rewrites the record the store holds for one event, so the run
// has a task to show at that event.
func withTaskRow(t *testing.T, store *countingSnapshotStore, runID string, sequence int64, status orkstackv1.Status) {
	t.Helper()
	uri := snapshotURIFor(runID, sequence)
	run := &orkstackv1.Run{}
	if err := proto.Unmarshal(store.objects[uri], run); err != nil {
		t.Fatal(err)
	}
	run.TaskRuns = []*orkstackv1.TaskRun{{TaskId: "work", Status: status}}
	store.objects[uri] = mustMarshalRun(t, run)
}

// helloRunEndingWith is helloRun without its terminal publish, closed by
// closing: the run that never archived itself. Its records of events 2 and 3
// show its task.
func helloRunEndingWith(t *testing.T, runID string, closing *historypb.HistoryEvent) ([]*historypb.HistoryEvent, *countingSnapshotStore) {
	t.Helper()
	events, store := helloRun(t, runID)
	withTaskRow(t, store, runID, 2, orkstackv1.Status_STATUS_PENDING)
	withTaskRow(t, store, runID, 3, orkstackv1.Status_STATUS_RUNNING)
	events = withoutPublishes(events, 1)
	closing.EventId = int64(len(events) + 1)
	if closing.EventTime == nil {
		closing.EventTime = timestamppb.New(archiveClosedAt)
	}
	return append(events, closing), store
}

func terminatedClose() *historypb.HistoryEvent {
	return closedEvent(0, enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_TERMINATED, archiveClosedAt)
}

// TestArchiveFromHistory_SaysWhatALiveReadSaid: for every ending that leaves a
// run with no archive, the archive built from its history answers what the
// run's live reads answered, so reading the run after it is retired changes
// nothing. Each row also says what the live read was, so a row cannot pass by
// both reads being empty.
func TestArchiveFromHistory_SaysWhatALiveReadSaid(t *testing.T) {
	for _, tc := range []struct {
		name  string
		build func(t *testing.T, runID string) (*TemporalRunReader, string)
		// status, code and lastEvent are what the run must read as.
		status    orkstackv1.Status
		code      string
		lastEvent orkstackv1.EventType
		// tasks is how many task rows the run must show.
		tasks      int
		canceledBy string
	}{
		{
			name: "terminated from outside",
			build: func(t *testing.T, runID string) (*TemporalRunReader, string) {
				events, store := helloRunEndingWith(t, runID, terminatedClose())
				reader, _ := readerOver(events, runID)
				return reader.WithSnapshotStore(store), runID
			},
			status: orkstackv1.Status_STATUS_CANCELED, code: "RUN_TERMINATED",
			lastEvent: orkstackv1.EventType_EVENT_TYPE_RUN_CANCELED, tasks: 1,
		},
		{
			name: "its ending refused by object storage",
			build: func(t *testing.T, runID string) (*TemporalRunReader, string) {
				events, store := helloRunEndingWith(t, runID, failedClose(""))
				reader, _ := readerOver(events, runID)
				return reader.WithSnapshotStore(store), runID
			},
			status: orkstackv1.Status_STATUS_FAILED, code: "RUN_FAILED",
			lastEvent: orkstackv1.EventType_EVENT_TYPE_RUN_FAILED, tasks: 1,
		},
		{
			name: "its ending published, its archive write refused",
			build: func(t *testing.T, runID string) (*TemporalRunReader, string) {
				events, store := helloRun(t, runID)
				withTaskRow(t, store, runID, 4, orkstackv1.Status_STATUS_SUCCEEDED)
				events = append(events, closedEvent(int64(len(events)+1),
					enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED, archiveClosedAt))
				reader, _ := readerOver(events, runID)
				return reader.WithSnapshotStore(store), runID
			},
			status: orkstackv1.Status_STATUS_SUCCEEDED, lastEvent: orkstackv1.EventType_EVENT_TYPE_RUN_SUCCEEDED, tasks: 1,
		},
		{
			name: "canceled by someone, its archive write refused",
			build: func(t *testing.T, runID string) (*TemporalRunReader, string) {
				events, store := canceledRun(t, runID, mustCancelCause(t, "dana@acme"), true)
				reader, _ := readerOver(events, runID)
				return reader.WithSnapshotStore(store), runID
			},
			status: orkstackv1.Status_STATUS_CANCELED, code: runCanceledCode,
			lastEvent: orkstackv1.EventType_EVENT_TYPE_RUN_CANCELED, canceledBy: "dana@acme",
		},
		{
			name: "terminated in a later part of a long run",
			build: func(t *testing.T, _ string) (*TemporalRunReader, string) {
				f := newChain(t, chainSpec{closed: []int{5, 4}, current: 2, closeWith: closedEvent(9,
					enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_TERMINATED, chainCreatedAt.Add(time.Hour))})
				return f.reader, f.runID
			},
			status: orkstackv1.Status_STATUS_CANCELED, code: "RUN_TERMINATED",
			lastEvent: orkstackv1.EventType_EVENT_TYPE_RUN_CANCELED, tasks: len(chainState().GetTaskRuns()),
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			reader, runID := tc.build(t, "ork-from-history")
			liveRun, err := reader.GetRun(ctx, runID)
			if err != nil {
				t.Fatalf("GetRun: %v", err)
			}
			liveEvents, err := reader.Events(ctx, runID, 0)
			if err != nil {
				t.Fatalf("Events: %v", err)
			}

			archiver := newLocalArchiver(t)
			if err := NewHistoryArchiver(reader, archiver).ArchiveFromHistory(ctx, runID); err != nil {
				t.Fatalf("ArchiveFromHistory: %v", err)
			}

			run, err := archiver.LoadRun(ctx, runID)
			if err != nil {
				t.Fatalf("LoadRun: %v", err)
			}
			if !proto.Equal(run, liveRun) {
				t.Fatalf("the archive's run differs from the live read:\narchive %v\nlive    %v", run, liveRun)
			}
			events, err := archiver.LoadRunEvents(ctx, runID)
			if err != nil {
				t.Fatalf("LoadRunEvents: %v", err)
			}
			if eventTypesOf(events) != eventTypesOf(liveEvents) {
				t.Fatalf("the archive's events are %s, the live read's %s", eventTypesOf(events), eventTypesOf(liveEvents))
			}
			if run.GetStatus() != tc.status || run.GetError().GetCode() != tc.code || len(run.GetTaskRuns()) != tc.tasks ||
				run.GetCanceledBy() != tc.canceledBy || run.GetFinishedAt() == nil {
				t.Fatalf("run = %s %q with %d tasks, by %q, finished %v; want %s %q with %d tasks, by %q, finished",
					run.GetStatus(), run.GetError().GetCode(), len(run.GetTaskRuns()), run.GetCanceledBy(), run.GetFinishedAt(),
					tc.status, tc.code, tc.tasks, tc.canceledBy)
			}
			if last := events[len(events)-1]; last.GetType() != tc.lastEvent {
				t.Fatalf("the archive's stream ends %s, want %s", last.GetType(), tc.lastEvent)
			}
		})
	}
}

// TestArchiveFromHistory_AnArchivedRunIsListedAndRetired: the run gets the
// index row every archive has, under its namespace, so it is listed and the
// retention walk finds it.
func TestArchiveFromHistory_AnArchivedRunIsListed(t *testing.T) {
	ctx := context.Background()
	const runID = "ork-listed"
	events, store := helloRunEndingWith(t, runID, terminatedClose())
	reader, _ := readerOver(events, runID)
	reader = reader.WithSnapshotStore(store)
	archiver := newLocalArchiver(t)
	if err := NewHistoryArchiver(reader, archiver).ArchiveFromHistory(ctx, runID); err != nil {
		t.Fatalf("ArchiveFromHistory: %v", err)
	}
	listed, err := archiver.ListRuns(ctx, ArchiveQuery{Status: orkstackv1.Status_STATUS_CANCELED})
	if err != nil {
		t.Fatalf("ListRuns: %v", err)
	}
	if len(listed) != 1 || listed[0].GetId() != runID {
		t.Fatalf("listed %v, want the one run", listed)
	}
}

// TestArchiveFromHistory_StatesWhatItCouldNotRecover: a record of the run's
// tasks that is gone, or that does not decode, can never come back, so the
// archive is written without it and the run says so. The run's outcome comes
// from its history, and its events are whole.
func TestArchiveFromHistory_StatesWhatItCouldNotRecover(t *testing.T) {
	const runID = "ork-record-lost"
	garbage := []byte{0xff, 0xff, 0xff}
	for _, tc := range []struct {
		name   string
		build  func(t *testing.T) ([]*historypb.HistoryEvent, *countingSnapshotStore)
		damage func(store *countingSnapshotStore)
		// status and code are the run's; says is what its error must say.
		status orkstackv1.Status
		code   string
		says   string
		tasks  int
		events string
	}{
		{
			name: "the newest record is gone, the one before it is not",
			build: func(t *testing.T) ([]*historypb.HistoryEvent, *countingSnapshotStore) {
				return helloRunEndingWith(t, runID, terminatedClose())
			},
			damage: func(store *countingSnapshotStore) { delete(store.objects, snapshotURIFor(runID, 3)) },
			status: orkstackv1.Status_STATUS_CANCELED, code: "RUN_TERMINATED", says: "as of an earlier event", tasks: 1,
			events: "[1:RUN_CREATED 2:RUN_STARTED 3:TASK_SUCCEEDED 4:RUN_CANCELED]",
		},
		{
			name: "both are gone",
			build: func(t *testing.T) ([]*historypb.HistoryEvent, *countingSnapshotStore) {
				return helloRunEndingWith(t, runID, terminatedClose())
			},
			damage: func(store *countingSnapshotStore) {
				delete(store.objects, snapshotURIFor(runID, 3))
				delete(store.objects, snapshotURIFor(runID, 2))
			},
			status: orkstackv1.Status_STATUS_CANCELED, code: "RUN_TERMINATED", says: "not shown",
			events: "[1:RUN_CREATED 2:RUN_STARTED 3:TASK_SUCCEEDED 4:RUN_CANCELED]",
		},
		{
			name: "neither decodes",
			build: func(t *testing.T) ([]*historypb.HistoryEvent, *countingSnapshotStore) {
				return helloRunEndingWith(t, runID, failedClose(""))
			},
			damage: func(store *countingSnapshotStore) {
				store.objects[snapshotURIFor(runID, 3)] = garbage
				store.objects[snapshotURIFor(runID, 2)] = garbage
			},
			status: orkstackv1.Status_STATUS_FAILED, code: "RUN_FAILED", says: "not shown",
			events: "[1:RUN_CREATED 2:RUN_STARTED 3:TASK_SUCCEEDED 4:RUN_FAILED]",
		},
		{
			name: "a run that succeeded, whose records are gone, still succeeded and says what is missing",
			build: func(t *testing.T) ([]*historypb.HistoryEvent, *countingSnapshotStore) {
				events, store := helloRun(t, runID)
				return append(events, closedEvent(int64(len(events)+1),
					enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED, archiveClosedAt)), store
			},
			damage: func(store *countingSnapshotStore) {
				delete(store.objects, snapshotURIFor(runID, 4))
				delete(store.objects, snapshotURIFor(runID, 3))
			},
			status: orkstackv1.Status_STATUS_SUCCEEDED, code: RunRecordIncompleteCode, says: "not shown",
			events: "[1:RUN_CREATED 2:RUN_STARTED 3:TASK_SUCCEEDED 4:RUN_SUCCEEDED]",
		},
		{
			name: "no record was ever written",
			build: func(t *testing.T) ([]*historypb.HistoryEvent, *countingSnapshotStore) {
				events, store := helloRunEndingWith(t, runID, terminatedClose())
				return withoutPublishes(events, -1), store
			},
			damage: func(*countingSnapshotStore) {},
			status: orkstackv1.Status_STATUS_CANCELED, code: "RUN_TERMINATED", says: "was ever written",
			events: "[1:RUN_CANCELED]",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			events, store := tc.build(t)
			tc.damage(store)
			reader, _ := readerOver(events, runID)
			reader = reader.WithSnapshotStore(store)
			archiver := newLocalArchiver(t)
			if err := NewHistoryArchiver(reader, archiver).ArchiveFromHistory(ctx, runID); err != nil {
				t.Fatalf("ArchiveFromHistory: %v", err)
			}
			run, err := archiver.LoadRun(ctx, runID)
			if err != nil {
				t.Fatalf("LoadRun: %v", err)
			}
			said := run.GetError().GetMessage() + " " + run.GetError().GetDetails()
			if run.GetStatus() != tc.status || run.GetError().GetCode() != tc.code || !strings.Contains(said, tc.says) ||
				len(run.GetTaskRuns()) != tc.tasks {
				t.Fatalf("run = %s %q saying %q with %d tasks, want %s %q saying %q with %d tasks",
					run.GetStatus(), run.GetError().GetCode(), said, len(run.GetTaskRuns()),
					tc.status, tc.code, tc.says, tc.tasks)
			}
			// What the run is called and when it finished come from its history.
			if run.GetOrkflowName() != "hello-ork" || run.GetFinishedAt() == nil || run.GetCreatedAt() == nil {
				t.Fatalf("run is called %q, created %v, finished %v", run.GetOrkflowName(), run.GetCreatedAt(), run.GetFinishedAt())
			}
			archived, err := archiver.LoadRunEvents(ctx, runID)
			if err != nil {
				t.Fatalf("LoadRunEvents: %v", err)
			}
			if got := eventTypesOf(archived); got != tc.events {
				t.Fatalf("archived events = %s, want %s", got, tc.events)
			}
		})
	}
}

// TestArchiveFromHistory_AnOverlayThatIsGoneIsLost: the newest record of a
// large run is an overlay. One that is gone can never be assembled, so it is
// lost as any other record is, and the one before it is used.
func TestArchiveFromHistory_AnOverlayThatIsGoneIsLost(t *testing.T) {
	ctx := context.Background()
	const runID = "ork-overlay-lost"
	events, store := helloRunEndingWith(t, runID, terminatedClose())
	events = withOverlayURI(t, events, 3, runID)
	reader, _ := readerOver(events, runID)
	reader = reader.WithSnapshotStore(store)
	archiver := newLocalArchiver(t)
	if err := NewHistoryArchiver(reader, archiver).ArchiveFromHistory(ctx, runID); err != nil {
		t.Fatalf("ArchiveFromHistory: %v", err)
	}
	run, err := archiver.LoadRun(ctx, runID)
	if err != nil {
		t.Fatalf("LoadRun: %v", err)
	}
	if len(run.GetTaskRuns()) != 1 || !strings.Contains(run.GetError().GetDetails(), "as of an earlier event") {
		t.Fatalf("run = %v, want its tasks as of an earlier event", run)
	}
}

// withOverlayURI points the publish of one event at an overlay object.
func withOverlayURI(t *testing.T, events []*historypb.HistoryEvent, sequence int64, runID string) []*historypb.HistoryEvent {
	t.Helper()
	dc := converter.GetDefaultDataConverter()
	out := make([]*historypb.HistoryEvent, 0, len(events))
	for _, e := range events {
		attrs := e.GetActivityTaskScheduledEventAttributes()
		if attrs == nil {
			out = append(out, e)
			continue
		}
		var event *orkstackv1.Event
		if err := dc.FromPayload(attrs.GetInput().GetPayloads()[0], &event); err != nil {
			t.Fatal(err)
		}
		if event.GetSequence() != sequence {
			out = append(out, e)
			continue
		}
		overlayKey, err := artifacts.RunSnapshotOverlayKey("default", runID, sequence)
		if err != nil {
			t.Fatal(err)
		}
		out = append(out, publishEventWithURI(t, e.GetEventId(), event, "s3://test/"+overlayKey))
	}
	return out
}

// TestArchiveFromHistory_AStoreThatIsDownWritesNothing: a read that may heal
// never decides that a record is lost. The archive is immutable, so writing
// one during an outage would keep the outage's holes for good; the call fails
// and the next pass tries again.
func TestArchiveFromHistory_AStoreThatIsDownWritesNothing(t *testing.T) {
	const runID = "ork-store-down"
	down := errors.New("object storage is down")
	for _, tc := range []struct {
		name    string
		overlay bool
	}{
		{name: "a full record"},
		{name: "an overlay", overlay: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			events, _ := helloRunEndingWith(t, runID, terminatedClose())
			if tc.overlay {
				events = withOverlayURI(t, events, 3, runID)
			}
			reader, _ := readerOver(events, runID)
			reader = reader.WithSnapshotStore(&failingSnapshotStore{err: down})
			archiver := newLocalArchiver(t)
			if err := NewHistoryArchiver(reader, archiver).ArchiveFromHistory(ctx, runID); err == nil ||
				errors.Is(err, artifacts.ErrNotFound) {
				t.Fatalf("ArchiveFromHistory = %v, want a failure that is not NotFound", err)
			}
			if _, err := archiver.LoadRun(ctx, runID); !errors.Is(err, artifacts.ErrNotFound) {
				t.Fatalf("LoadRun = %v, want no archive", err)
			}
		})
	}
}

// TestArchiveFromHistory_ALostSegmentIsAGap: an earlier part of a long run's
// stream that is gone is recorded as a gap, as the workflow's own archive
// write records it, and a segment a store outage hid is a failure instead.
func TestArchiveFromHistory_ALostSegmentIsAGap(t *testing.T) {
	ctx := context.Background()
	terminated := func() *historypb.HistoryEvent {
		return closedEvent(9, enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_TERMINATED, chainCreatedAt.Add(time.Hour))
	}
	f := newChain(t, chainSpec{closed: []int{5, 4}, current: 2, closeWith: terminated()})
	delete(f.store.objects, f.carry.GetSegments()[0].GetObject().GetUri())
	archiver := newLocalArchiver(t)
	if err := NewHistoryArchiver(f.reader, archiver).ArchiveFromHistory(ctx, f.runID); err != nil {
		t.Fatalf("ArchiveFromHistory: %v", err)
	}
	events, err := archiver.LoadRunEvents(ctx, f.runID)
	var lost *ArchivedEventsLostError
	if !errors.As(err, &lost) || len(lost.Gaps) != 1 || lost.Gaps[0].First != 1 || lost.Gaps[0].Last != 5 || lost.Gaps[0].Unpublished() {
		t.Fatalf("LoadRunEvents = %v, want events 1–5 named as lost", err)
	}
	if first := events[0].GetSequence(); first != 6 {
		t.Fatalf("the archive's stream starts at %d, want 6", first)
	}

	down := newChain(t, chainSpec{closed: []int{5}, current: 1, closeWith: terminated()})
	down.store.fail[down.carry.GetSegments()[0].GetObject().GetUri()] = errors.New("store is down")
	empty := newLocalArchiver(t)
	if err := NewHistoryArchiver(down.reader, empty).ArchiveFromHistory(ctx, down.runID); err == nil {
		t.Fatal("ArchiveFromHistory wrote an archive while a segment was only unreadable")
	}
	if _, err := empty.LoadRun(ctx, down.runID); !errors.Is(err, artifacts.ErrNotFound) {
		t.Fatalf("LoadRun = %v, want no archive", err)
	}
}

// TestArchiveFromHistory_ACarryNamingAnotherRunsObjectIsNotRead: saved-state
// references that fail verification leave the earlier events a gap and the
// run's tasks stated as lost. Nothing is read through them.
func TestArchiveFromHistory_ACarryNamingAnotherRunsObjectIsNotRead(t *testing.T) {
	ctx := context.Background()
	f := newChain(t, chainSpec{closed: []int{5}, current: 0, closeWith: closedEvent(9,
		enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_TERMINATED, chainCreatedAt.Add(time.Hour))})
	carry := proto.Clone(f.carry).(*orkstackv1.RunContinuation)
	const other = "ork-1700000000-8"
	carry.Checkpoint.Uri = strings.Replace(carry.GetCheckpoint().GetUri(), "/"+chainRunID+"/", "/"+other+"/", 1)
	f.client.generations[f.runID][f.current].events[0] = f.started(carry)

	archiver := newLocalArchiver(t)
	if err := NewHistoryArchiver(f.reader, archiver).ArchiveFromHistory(ctx, f.runID); err != nil {
		t.Fatalf("ArchiveFromHistory: %v", err)
	}
	for _, uri := range f.store.reads {
		if strings.Contains(uri, other) {
			t.Fatalf("the store was read at another run's object: %s", uri)
		}
	}
	run, err := archiver.LoadRun(ctx, f.runID)
	if err != nil {
		t.Fatalf("LoadRun: %v", err)
	}
	if run.GetStatus() != orkstackv1.Status_STATUS_CANCELED || !strings.Contains(run.GetError().GetDetails(), "not shown") {
		t.Fatalf("run = %v, want CANCELED with its tasks stated as lost", run)
	}
	if _, err := archiver.LoadRunEvents(ctx, f.runID); !errors.Is(err, ErrArchivedEventsLost) {
		t.Fatalf("LoadRunEvents = %v, want the earlier events named as lost", err)
	}
}

// TestArchiveFromHistory_RefusesARunThatHasNotFinished: an archive says a run
// ended. Whatever a listing claimed, a run whose history holds no close is not
// archived, and a run the cluster no longer holds reads as NotFound, which is
// how the sweep tells it from a failure.
func TestArchiveFromHistory_RefusesARunThatHasNotFinished(t *testing.T) {
	ctx := context.Background()
	const runID = "ork-still-running"
	events, store := helloRun(t, runID)
	reader, _ := readerOver(withoutPublishes(events, 1), runID)
	reader = reader.WithSnapshotStore(store)
	archiver := newLocalArchiver(t)
	err := NewHistoryArchiver(reader, archiver).ArchiveFromHistory(ctx, runID)
	if err == nil || errors.Is(err, artifacts.ErrNotFound) {
		t.Fatalf("ArchiveFromHistory = %v, want a failure that is not NotFound", err)
	}
	if _, err := archiver.LoadRun(ctx, runID); !errors.Is(err, artifacts.ErrNotFound) {
		t.Fatalf("LoadRun = %v, want no archive", err)
	}
	if err := NewHistoryArchiver(reader, archiver).ArchiveFromHistory(ctx, "ork-gone"); !errors.Is(err, artifacts.ErrNotFound) {
		t.Fatalf("ArchiveFromHistory of a run the cluster does not hold = %v, want NotFound", err)
	}
}
