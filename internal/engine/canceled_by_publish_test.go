package engine_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/workflow"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/internal/task"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// canceledSource is a RunEventSource whose archive read names who canceled
// the run, after failing the first failures reads.
type canceledSource struct {
	events     []*orkstackv1.Event
	canceledBy string
	failures   int
	reads      int
}

func (s *canceledSource) Events(context.Context, string, int64) ([]*orkstackv1.Event, error) {
	return s.events, nil
}

func (s *canceledSource) EventsForArchive(context.Context, string) (engine.ArchiveRead, error) {
	s.reads++
	if s.reads <= s.failures {
		return engine.ArchiveRead{}, errors.New("history is unreachable")
	}
	return engine.ArchiveRead{Events: s.events, CanceledBy: s.canceledBy}, nil
}

// publishCanceled publishes a canceled run's RUN_CANCELED through the
// archiving projection activity, with a webhook notifier attached.
func publishCanceled(t *testing.T, source *canceledSource) (
	*engine.EventActivities, string, *artifacts.LocalRunArchive, *recordingNotifier, *engine.RunStore,
) {
	t.Helper()
	archiver, store, _ := newArchiveFixture(t)
	notifier := &recordingNotifier{}
	runs := engine.NewRunStore()
	activities, snapshots := eventActivitiesWithSnapshots(
		engine.NewArchivingEventActivities(engine.NewEventBus(), runs, archiver).
			WithEventSource(source).WithRunNotifier(notifier))
	created := at(t, "2026-10-01T00:00:00Z")
	run := sampleRun(t, "ork-canceled", orkstackv1.Status_STATUS_CANCELED, created)
	run.Error = &orkstackv1.Error{Code: "RUN_CANCELED", Message: "run canceled"}
	return activities, snapshots.putRun(t, run), store, notifier, runs
}

func canceledEvent(t *testing.T) *orkstackv1.Event {
	t.Helper()
	return &orkstackv1.Event{
		RunId: "ork-canceled", Sequence: 3, Type: orkstackv1.EventType_EVENT_TYPE_RUN_CANCELED,
		Status: orkstackv1.Status_STATUS_CANCELED, Timestamp: timestamppb.New(at(t, "2026-10-01T00:00:00Z")),
	}
}

// A canceled run's end names who canceled it on every surface the
// projection writes: the run store, the webhook delivery and the archive,
// from ONE read of the run's record.
func TestPublishEvent_ACanceledRunNamesWhoCanceledIt(t *testing.T) {
	source := &canceledSource{events: []*orkstackv1.Event{{RunId: "ork-canceled", Sequence: 3,
		Type: orkstackv1.EventType_EVENT_TYPE_RUN_CANCELED}}, canceledBy: "dana@acme"}
	activities, uri, store, notifier, runs := publishCanceled(t, source)
	if err := activities.PublishEvent(context.Background(), canceledEvent(t), uri, "ork-canceled#3"); err != nil {
		t.Fatalf("PublishEvent: %v", err)
	}
	if source.reads != 1 {
		t.Fatalf("the run's record was read %d times, want once", source.reads)
	}
	if stored, _ := runs.Get("ork-canceled"); stored.GetCanceledBy() != "dana@acme" {
		t.Fatalf("stored run canceled_by = %q, want dana@acme", stored.GetCanceledBy())
	}
	if len(notifier.runs) != 1 || notifier.runs[0].GetCanceledBy() != "dana@acme" {
		t.Fatalf("webhook runs = %v, want one naming dana@acme", notifier.runs)
	}
	body, err := store.GetRun(context.Background(), "ork-canceled")
	if err != nil {
		t.Fatalf("the run was not archived: %v", err)
	}
	archived, _, err := engine.UnmarshalRunArchive(body)
	if err != nil || archived.GetCanceledBy() != "dana@acme" {
		t.Fatalf("archived canceled_by = %q, %v; want dana@acme", archived.GetCanceledBy(), err)
	}
}

// A record that cannot be read on the last attempt — a direct call has no
// retry behind it, so it is its own last — costs the announcement the name,
// never the announcement: the webhook goes out, the activity returns the
// archive read's error, and its retry — the read healed — archives the run
// with the name.
func TestPublishEvent_AnUnreadableRecordStillAnnouncesTheEnd(t *testing.T) {
	source := &canceledSource{events: []*orkstackv1.Event{{RunId: "ork-canceled", Sequence: 3,
		Type: orkstackv1.EventType_EVENT_TYPE_RUN_CANCELED}}, canceledBy: "dana@acme", failures: 2}
	activities, uri, store, notifier, _ := publishCanceled(t, source)
	if err := activities.PublishEvent(context.Background(), canceledEvent(t), uri, "ork-canceled#3"); err == nil {
		t.Fatal("PublishEvent succeeded without archiving; the activity must retry")
	}
	if len(notifier.runs) != 1 || notifier.runs[0].GetCanceledBy() != "" {
		t.Fatalf("webhook runs = %v, want the end announced once, without the name it could not read", notifier.runs)
	}
	if _, err := store.GetRun(context.Background(), "ork-canceled"); !errors.Is(err, artifacts.ErrNotFound) {
		t.Fatalf("a run was archived without its record: %v", err)
	}
	// The activity's retry: the bus has the event already, so nothing is
	// announced twice, and the archive gets the name.
	if err := activities.PublishEvent(context.Background(), canceledEvent(t), uri, "ork-canceled#3"); err != nil {
		t.Fatalf("retried PublishEvent: %v", err)
	}
	if len(notifier.runs) != 1 {
		t.Fatalf("the retry announced the end again: %d deliveries", len(notifier.runs))
	}
	body, err := store.GetRun(context.Background(), "ork-canceled")
	if err != nil {
		t.Fatalf("the retry did not archive the run: %v", err)
	}
	if archived, _, err := engine.UnmarshalRunArchive(body); err != nil || archived.GetCanceledBy() != "dana@acme" {
		t.Fatalf("archived canceled_by = %q, %v; want dana@acme", archived.GetCanceledBy(), err)
	}
}

// Every other terminal event keeps today's one read, after publishing.
func TestPublishEvent_OnlyACanceledEndReadsTheRecordFirst(t *testing.T) {
	source := &canceledSource{events: gappedEvents("ork-gap"), canceledBy: "dana@acme", failures: 1}
	archiver, store, _ := newArchiveFixture(t)
	notifier := &recordingNotifier{}
	activities, snapshots := eventActivitiesWithSnapshots(
		engine.NewArchivingEventActivities(engine.NewEventBus(), engine.NewRunStore(), archiver).
			WithEventSource(source).WithRunNotifier(notifier))
	created := at(t, "2026-10-01T00:00:00Z")
	finished := sampleRun(t, "ork-gap", orkstackv1.Status_STATUS_SUCCEEDED, created)
	err := activities.PublishEvent(context.Background(), &orkstackv1.Event{
		RunId: "ork-gap", Sequence: 7, Type: orkstackv1.EventType_EVENT_TYPE_RUN_SUCCEEDED,
		Status: orkstackv1.Status_STATUS_SUCCEEDED, Timestamp: timestamppb.New(created),
	}, snapshots.putRun(t, finished), "ork-gap#7")
	if err == nil || source.reads != 1 || len(notifier.runs) != 1 {
		t.Fatalf("PublishEvent = %v after %d reads and %d deliveries; want the archive read's error after one read and one delivery",
			err, source.reads, len(notifier.runs))
	}
	if _, err := store.GetRun(context.Background(), "ork-gap"); !errors.Is(err, artifacts.ErrNotFound) {
		t.Fatalf("archived from a failed read: %v", err)
	}
	if err := activities.PublishEvent(context.Background(), &orkstackv1.Event{
		RunId: "ork-gap", Sequence: 7, Type: orkstackv1.EventType_EVENT_TYPE_RUN_SUCCEEDED,
		Status: orkstackv1.Status_STATUS_SUCCEEDED, Timestamp: timestamppb.New(created),
	}, snapshots.putRun(t, finished), "ork-gap#7"); err != nil {
		t.Fatalf("retried PublishEvent: %v", err)
	}
	body, _ := store.GetRun(context.Background(), "ork-gap")
	if archived, _, err := engine.UnmarshalRunArchive(body); err != nil || archived.GetCanceledBy() != "" {
		t.Fatalf("a succeeded run archived canceled_by = %q, %v; want nobody", archived.GetCanceledBy(), err)
	}
}

// A real run canceled while its task runs ends RUN_CANCELED with a snapshot
// the projection names who canceled it on, and the run itself writes no name:
// RunWorkflow is unchanged, so no recorded history can replay differently.
// The run's record is what names the principal (here, the source the archive
// read answers from; on a cluster, the run's own cancellation request).
func TestRunWorkflow_ACanceledRunIsNamedByTheProjectionNotTheRun(t *testing.T) {
	stored, archived, announced := runCanceledUnderTestEnv(t, &canceledSource{canceledBy: "dana@acme"})
	for surface, by := range map[string]string{"stored": stored, "archived": archived, "announced": announced} {
		if by != "dana@acme" {
			t.Fatalf("%s canceled_by = %q, want dana@acme", surface, by)
		}
	}
}

// A record that cannot be read on the first attempts is read on a retry
// before anything is announced, so the webhook names who canceled; only on
// the activity's last attempt does the end go out without the name — and the
// archive, read once more, still has it.
func TestRunWorkflow_AFailedReadIsRetriedBeforeTheEndIsAnnounced(t *testing.T) {
	tests := []struct {
		name          string
		failures      int
		wantAnnounced string
	}{
		{"the first attempt's read fails", 1, "dana@acme"},
		{"every attempt's first read fails", 3, ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			source := &canceledSource{canceledBy: "dana@acme", failures: tt.failures}
			_, archived, announced := runCanceledUnderTestEnv(t, source)
			if announced != tt.wantAnnounced || archived != "dana@acme" {
				t.Fatalf("announced %q, archived %q; want announced %q, archived dana@acme",
					announced, archived, tt.wantAnnounced)
			}
		})
	}
}

// runCanceledUnderTestEnv runs a real RunWorkflow canceled while its task
// runs, through the real archiving projection over source, and returns who
// the stored run, the archived run and the RUN_CANCELED delivery name.
func runCanceledUnderTestEnv(t *testing.T, source *canceledSource) (stored, archived, announced string) {
	t.Helper()
	var suite testsuite.WorkflowTestSuite
	env := generousDeadlock(suite.NewTestWorkflowEnvironment())
	env.SetTestTimeout(30 * time.Second)
	env.RegisterWorkflowWithOptions(engine.RunWorkflow, workflow.RegisterOptions{Name: engine.RunWorkflowType})
	canceled := make(chan struct{})
	env.RegisterActivityWithOptions(
		func(actCtx context.Context, _ *orkstackv1.ExecuteTaskInput) (*orkstackv1.ExecuteTaskResult, error) {
			env.CancelWorkflow()
			return runUntilCanceled(actCtx, canceled)
		},
		activity.RegisterOptions{Name: task.ActivityExecuteTask},
	)
	snapshots := newTestSnapshotStore()
	env.RegisterActivityWithOptions(engine.NewSnapshotActivities(snapshots).WriteSnapshot,
		activity.RegisterOptions{Name: engine.ActivityWriteSnapshot})
	archiver, archive, _ := newArchiveFixture(t)
	runs := engine.NewRunStore()
	notifier := &recordingNotifier{}
	env.RegisterActivityWithOptions(
		engine.NewArchivingEventActivities(engine.NewEventBus(), runs, archiver).
			WithEventSource(source).WithSnapshotStore(snapshots).WithRunNotifier(notifier).PublishEvent,
		activity.RegisterOptions{Name: engine.ActivityPublishEvent})

	env.ExecuteWorkflow(engine.RunWorkflow, "ork-canceled-env", compile(t, dispatchDocument), payload(t, nil),
		defaultPlan(), (*orkstackv1.RunContinuation)(nil))
	if !env.IsWorkflowCompleted() {
		t.Fatal("the run did not end")
	}
	select {
	case <-canceled:
	case <-time.After(30 * time.Second):
		t.Fatal("the task was never canceled")
	}

	run, ok := runs.Get("ork-canceled-env")
	if !ok || run.GetStatus() != orkstackv1.Status_STATUS_CANCELED || run.GetError().GetCode() != "RUN_CANCELED" {
		t.Fatalf("stored run = %v / %v, want CANCELED / RUN_CANCELED", run.GetStatus(), run.GetError())
	}
	body, err := archive.GetRun(context.Background(), "ork-canceled-env")
	if err != nil {
		t.Fatalf("the run was not archived: %v", err)
	}
	archivedRun, _, err := engine.UnmarshalRunArchive(body)
	if err != nil {
		t.Fatalf("decode the archived run: %v", err)
	}
	deliveries := 0
	for i, event := range notifier.events {
		if event.GetType() == orkstackv1.EventType_EVENT_TYPE_RUN_CANCELED {
			deliveries++
			announced = notifier.runs[i].GetCanceledBy()
		}
	}
	if deliveries != 1 {
		t.Fatalf("the end was announced %d times, want once", deliveries)
	}
	// Every snapshot the run itself wrote names nobody.
	snapshots.mu.Lock()
	written := make(map[string][]byte, len(snapshots.objects))
	for uri, data := range snapshots.objects {
		written[uri] = data
	}
	snapshots.mu.Unlock()
	for uri, data := range written {
		var snapshot orkstackv1.Run
		if err := proto.Unmarshal(data, &snapshot); err != nil {
			t.Fatalf("snapshot %s: %v", uri, err)
		}
		if snapshot.GetCanceledBy() != "" {
			t.Fatalf("the run wrote canceled_by %q into %s; only the projection may", snapshot.GetCanceledBy(), uri)
		}
	}
	return run.GetCanceledBy(), archivedRun.GetCanceledBy(), announced
}
