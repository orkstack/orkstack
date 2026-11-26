package engine

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/testsuite"
	"google.golang.org/protobuf/types/known/timestamppb"

	enumspb "go.temporal.io/api/enums/v1"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/testsupport/must"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

func endingRun(status orkstackv1.Status, finished time.Duration) *orkstackv1.Run {
	return &orkstackv1.Run{Id: "ork-r", Namespace: "default", OrkflowName: "wf", Status: status,
		CreatedAt: timestamppb.New(readerEpoch), FinishedAt: timestamppb.New(readerEpoch.Add(finished))}
}

// TestRunArchiver_EveryEnding: a resumed run's second ending is archived
// beside its first, and is what the run reads as from then on: by id, in a
// list, for its retention, and as what a later resume sets aside.
func TestRunArchiver_EveryEnding(t *testing.T) {
	ctx := context.Background()
	store, err := artifacts.NewLocalRunArchive(t.TempDir())
	must.OKf(t, err, "archive")
	t.Cleanup(func() { _ = store.Close() })
	a := NewRunArchiver(store)
	must.OKf(t, a.ArchiveEnding(ctx, endingRun(orkstackv1.Status_STATUS_FAILED, time.Minute), 0, setAside(6)), "first")
	must.OKf(t, a.ArchiveEnding(ctx, endingRun(orkstackv1.Status_STATUS_SUCCEEDED, time.Hour), 7, append(setAside(6), carriedOn(7, 6)...)), "second")

	run, err := a.LoadRun(ctx, "ork-r")
	if err != nil || run.GetStatus() != orkstackv1.Status_STATUS_SUCCEEDED {
		t.Fatalf("LoadRun = %v, %v; want the newest ending", run.GetStatus(), err)
	}
	events, err := a.LoadRunEvents(ctx, "ork-r")
	if err != nil || len(events) != 9 {
		t.Fatalf("%d events, %v; want the whole stream", len(events), err)
	}
	if at, err := a.RunFinishedAt(ctx, "ork-r"); err != nil || !at.Equal(readerEpoch.Add(time.Hour)) {
		t.Fatalf("finished at %v, %v; want the newest ending's", at, err)
	}
	for status, want := range map[orkstackv1.Status]int{
		orkstackv1.Status_STATUS_UNSPECIFIED: 1, orkstackv1.Status_STATUS_SUCCEEDED: 1, orkstackv1.Status_STATUS_FAILED: 0,
	} {
		runs, err := a.ListRuns(ctx, ArchiveQuery{Namespace: "default", Status: status})
		if err != nil || len(runs) != want || (want == 1 && runs[0].GetStatus() != orkstackv1.Status_STATUS_SUCCEEDED) {
			t.Errorf("list of %v = %v, %v; want %d", status, runs, err, want)
		}
	}
	for before, want := range map[uint64]int{7: 6, 20: 9} {
		events, _, err := a.EarlierEnding(ctx, "ork-r", before)
		if err != nil || len(events) != want {
			t.Errorf("the ending before %d has %d events, %v; want %d", before, len(events), err, want)
		}
	}
	if _, _, err := a.EarlierEnding(ctx, "ork-none", 7); !errors.Is(err, artifacts.ErrNotFound) {
		t.Errorf("a run with none = %v, want ErrNotFound", err)
	}
	if _, _, err := (*RunArchiver)(nil).EarlierEnding(ctx, "ork-r", 7); !errors.Is(err, artifacts.ErrNotFound) {
		t.Errorf("no archive = %v, want ErrNotFound", err)
	}
}

// TestRunArchiver_ARowWithNoBodyIsNotListed: a list never shows an ending
// whose archive is not whole, nor an earlier one in its place.
func TestRunArchiver_ARowWithNoBodyIsNotListed(t *testing.T) {
	ctx := context.Background()
	store, err := artifacts.NewLocalRunArchive(t.TempDir())
	must.OKf(t, err, "archive")
	t.Cleanup(func() { _ = store.Close() })
	a := NewRunArchiver(rowsWithoutBody{store})
	must.OKf(t, a.ArchiveEnding(ctx, endingRun(orkstackv1.Status_STATUS_FAILED, time.Minute), 0, setAside(6)), "first")
	must.OKf(t, a.ArchiveEnding(ctx, endingRun(orkstackv1.Status_STATUS_SUCCEEDED, time.Hour), 7, carriedOn(7, 6)), "second")
	runs, err := a.ListRuns(ctx, ArchiveQuery{Namespace: "default"})
	if err != nil || len(runs) != 0 {
		t.Fatalf("list = %v, %v; want no run", runs, err)
	}
}

// rowsWithoutBody is an archive whose later endings lost their bodies.
type rowsWithoutBody struct{ *artifacts.LocalRunArchive }

func (r rowsWithoutBody) GetEnding(ctx context.Context, runID string, scope uint64) ([]byte, error) {
	if scope > 0 {
		return nil, artifacts.ErrNotFound
	}
	return r.LocalRunArchive.GetEnding(ctx, runID, scope)
}

// TestHistoryArchiver_AResumedRunsEnding: the sweep archives a resumed run's
// ending under its scope, whole, beside the ending it set aside.
func TestHistoryArchiver_AResumedRunsEnding(t *testing.T) {
	ctx := context.Background()
	store, err := artifacts.NewLocalRunArchive(t.TempDir())
	must.OKf(t, err, "archive")
	t.Cleanup(func() { _ = store.Close() })
	a := NewRunArchiver(store)
	must.OKf(t, a.ArchiveEnding(ctx, endingRun(orkstackv1.Status_STATUS_FAILED, time.Minute), 0, setAside(6)), "first")
	reader, _ := resumedHistory(t, 7, enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED, carriedOn(7, 6)...)
	must.OKf(t, NewHistoryArchiver(reader.WithEndings(a), a).ArchiveFromHistory(ctx, "ork-r"), "archive from history")
	if has, err := store.HasEnding(ctx, "ork-r", 7); err != nil || !has {
		t.Fatalf("HasEnding(7) = %v, %v", has, err)
	}
	events, err := a.LoadRunEvents(ctx, "ork-r")
	if err != nil || eventTypesOf(events) != "[1:TASK_LOG 2:TASK_LOG 3:TASK_LOG 4:TASK_LOG 5:TASK_LOG 6:RUN_FAILED 7:RUN_RESUMED 8:RUN_RESULTS_KEPT 9:RUN_SUCCEEDED]" {
		t.Fatalf("events = %s, %v", eventTypesOf(events), err)
	}
	if body, err := store.GetEnding(ctx, "ork-r", 0); err != nil || len(body) == 0 {
		t.Fatalf("the first ending's body = %d bytes, %v; want it kept", len(body), err)
	}
}

// TestPublishEvent_ArchivesAResumedRunsEnding: the terminal publish archives
// under the ending's scope too.
func TestPublishEvent_ArchivesAResumedRunsEnding(t *testing.T) {
	ctx := context.Background()
	store, err := artifacts.NewLocalRunArchive(t.TempDir())
	must.OKf(t, err, "archive")
	t.Cleanup(func() { _ = store.Close() })
	a := NewRunArchiver(store)
	must.OKf(t, a.ArchiveEnding(ctx, endingRun(orkstackv1.Status_STATUS_FAILED, time.Minute), 0, setAside(6)), "first")
	reader, snapshots := resumedHistory(t, 7, enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED, carriedOn(7, 6)...)
	activities := NewArchivingEventActivities(NewEventBus(), NewRunStore(), a).
		WithEventSource(reader.WithEndings(a)).WithSnapshotStore(snapshots)
	last := carriedOn(7, 6)[2]
	must.OKf(t, activities.PublishEvent(ctx, last, snapshotURIFor("ork-r", last.GetSequence()), "k"), "publish")
	if has, err := store.HasEnding(ctx, "ork-r", 7); err != nil || !has {
		t.Fatalf("HasEnding(7) = %v, %v", has, err)
	}
}

// TestPublishEvent_AResumedRunLeavesNoRowOfItsEarlierEnding: once a run goes
// on, no list says it ended; its earlier ending stays readable, and a publish
// repeated after the run ended again removes nothing.
func TestPublishEvent_AResumedRunLeavesNoRowOfItsEarlierEnding(t *testing.T) {
	ctx := context.Background()
	store, err := artifacts.NewLocalRunArchive(t.TempDir())
	must.OKf(t, err, "archive")
	t.Cleanup(func() { _ = store.Close() })
	a := NewRunArchiver(store)
	must.OKf(t, a.ArchiveEnding(ctx, endingRun(orkstackv1.Status_STATUS_FAILED, time.Minute), 0, setAside(6)), "first")
	must.OKf(t, store.MarkResumed(ctx, "ork-r", 7, 0), "mark")
	_, snapshots := resumedHistory(t, 7, enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED, carriedOn(7, 6)...)
	running := endingRun(orkstackv1.Status_STATUS_RUNNING, 0)
	running.FinishedAt = nil
	snapshots.objects[snapshotURIFor("ork-r", 7)] = mustMarshalRun(t, running)
	resumed := func() {
		t.Helper()
		activities := NewArchivingEventActivities(NewEventBus(), NewRunStore(), a).WithSnapshotStore(snapshots)
		must.OKf(t, activities.PublishEvent(ctx, carriedOn(7, 6)[0], snapshotURIFor("ork-r", 7), "k"), "publish")
	}
	listed := func() int {
		t.Helper()
		runs, err := a.ListRuns(ctx, ArchiveQuery{Namespace: "default"})
		must.OKf(t, err, "list")
		return len(runs)
	}
	if listed() != 1 {
		t.Fatal("the mark alone took the run from the list")
	}
	resumed()
	if n := listed(); n != 0 {
		t.Fatalf("%d runs listed, want none: the run goes on", n)
	}
	if run, err := a.LoadRun(ctx, "ork-r"); err != nil || run.GetStatus() != orkstackv1.Status_STATUS_FAILED {
		t.Fatalf("its earlier ending = %v, %v; want it kept", run, err)
	}
	must.OKf(t, a.ArchiveEnding(ctx, endingRun(orkstackv1.Status_STATUS_FAILED, time.Hour), 7, carriedOn(7, 6)), "second")
	resumed()
	if n := listed(); n != 1 {
		t.Fatalf("%d runs listed after a repeated publish, want the run as it ended again", n)
	}
}

// TestPublishEvent_RetiringRowsIsRetried: an archive that cannot retire the
// rows fails the publish while it has attempts left, the event announced once
// however often, and never on the last: the run goes on.
func TestPublishEvent_RetiringRowsIsRetried(t *testing.T) {
	store, err := artifacts.NewLocalRunArchive(t.TempDir())
	must.OKf(t, err, "archive")
	t.Cleanup(func() { _ = store.Close() })
	bus := NewEventBus()
	snapshots := &countingSnapshotStore{objects: map[string][]byte{
		snapshotURIFor("ork-r", 7): mustMarshalRun(t, endingRun(orkstackv1.Status_STATUS_RUNNING, 0)),
	}}
	activities := NewArchivingEventActivities(bus, NewRunStore(), NewRunArchiver(failingEndings{store})).WithSnapshotStore(snapshots)
	// Outside an activity every attempt is the last.
	must.OKf(t, activities.PublishEvent(context.Background(), carriedOn(7, 6)[0], snapshotURIFor("ork-r", 7), "k"), "the last attempt")
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestActivityEnvironment()
	env.RegisterActivityWithOptions(activities.PublishEvent, activity.RegisterOptions{Name: ActivityPublishEvent})
	if _, err := env.ExecuteActivity(ActivityPublishEvent, carriedOn(7, 6)[0], snapshotURIFor("ork-r", 7), "k"); err == nil {
		t.Fatal("a first attempt that could not retire the rows was not retried")
	}
	if got := bus.Events("ork-r", 0); len(got) != 1 {
		t.Fatalf("%d events, want it published once", len(got))
	}
}

type failingEndings struct{ *artifacts.LocalRunArchive }

func (failingEndings) RunEndings(context.Context, string) (artifacts.RunEndings, error) {
	return artifacts.RunEndings{}, errors.New("down")
}

// TestRunArchiver_AnOlderEndingNeverStandsForTheOneSetAside: where the ending
// a resume set aside is gone, an older one's state is not the run's, and its
// events end in a gap.
func TestRunArchiver_AnOlderEndingNeverStandsForTheOneSetAside(t *testing.T) {
	ctx := context.Background()
	store, err := artifacts.NewLocalRunArchive(t.TempDir())
	must.OKf(t, err, "archive")
	t.Cleanup(func() { _ = store.Close() })
	a := NewRunArchiver(store)
	endings := artifacts.RunArchive(store).(artifacts.RunEndingStore)
	first := endingRun(orkstackv1.Status_STATUS_FAILED, time.Minute)
	first.TaskRuns = []*orkstackv1.TaskRun{{TaskId: "build", Status: orkstackv1.Status_STATUS_SUCCEEDED}}
	must.OKf(t, a.ArchiveEnding(ctx, first, 0, setAside(6)), "first")
	must.OKf(t, endings.MarkResumed(ctx, "ork-r", 20, 0), "the mark of the resume being read")
	if run, err := a.EarlierRun(ctx, "ork-r", 20); err != nil || run.GetId() != "ork-r" {
		t.Fatalf("with the ending set aside there = %v, %v", run, err)
	}
	// A mark in between whose reset never ran, or ran and was reset by hand,
	// changes nothing: the resume's own mark names what it set aside.
	must.OKf(t, endings.MarkResumed(ctx, "ork-r", 7, 0), "a mark in between")
	if run, err := a.EarlierRun(ctx, "ork-r", 20); err != nil || run.GetId() != "ork-r" {
		t.Fatalf("with a mark in between = %v, %v", run, err)
	}
	if _, gaps, err := a.EarlierEnding(ctx, "ork-r", 20); err != nil || len(gaps) != 0 {
		t.Fatalf("with a mark in between, gaps %v, %v", gaps, err)
	}
	// The ending under 7 is what this one set aside, and it is gone.
	must.OKf(t, endings.MarkResumed(ctx, "ork-r", 30, 7), "the mark of a resume of the ending that is gone")
	if run, err := a.EarlierRun(ctx, "ork-r", 30); !errors.Is(err, artifacts.ErrNotFound) {
		t.Fatalf("with it gone = %v, %v", run, err)
	}
	if listed, err := a.EarlierResumes(ctx, "ork-r", 30); !errors.Is(err, artifacts.ErrNotFound) {
		t.Fatalf("with it gone, listed %v, %v", listed, err)
	}
	events, gaps, err := a.EarlierEnding(ctx, "ork-r", 30)
	if err != nil || len(events) != 6 || len(gaps) != 1 || gaps[0] != (EventGap{First: 7, Last: 29, Reason: earlierEndingLost}) {
		t.Fatalf("%d events, gaps %v, %v", len(events), gaps, err)
	}
	// Without a mark that names one, none is guessed.
	if _, err := a.EarlierRun(ctx, "ork-r", 40); !errors.Is(err, artifacts.ErrNotFound) {
		t.Fatalf("with no mark of its own = %v", err)
	}

	// End to end: a run resumed under 30 that ended with no event reads, live
	// and archived, with how it ended and what is missing, and no row of an
	// older ending; under 20 it reads as what it set aside left it.
	for scope, whole := range map[uint64]bool{20: true, 30: false} {
		reader, _ := resumedHistory(t, scope, enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_CANCELED)
		reader = reader.WithEndings(a)
		live, err := reader.GetRun(ctx, "ork-r")
		must.OKf(t, err, "live under %d", scope)
		archived, read, err := reader.readForArchive(ctx, "ork-r", false)
		must.OKf(t, err, "archived under %d", scope)
		for where, run := range map[string]*orkstackv1.Run{"live": live, "archived": archived} {
			says := run.GetError().GetMessage() + run.GetError().GetDetails()
			if run.GetStatus() != orkstackv1.Status_STATUS_CANCELED || (len(run.GetTaskRuns()) == 1) != whole || strings.Contains(says, recordSetAsideSentence) == whole {
				t.Errorf("%s under %d = %v", where, scope, run)
			}
		}
		if last := read.Events[len(read.Events)-1]; last.GetSequence() != int64(scope) || (len(read.Gaps) == 0) != whole {
			t.Errorf("under %d it ends %v with gaps %v", scope, last, read.Gaps)
		}
	}
}
