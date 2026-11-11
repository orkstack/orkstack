package engine_test

import (
	"context"
	"sort"
	"sync"
	"testing"
	"time"

	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/workflow"
	"google.golang.org/protobuf/proto"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/internal/task"
)

// cancelBesideDocument is a task and one that needs it: the operator's cancel
// reaches the run after build's completion, with deploy still to run.
const cancelBesideDocument = `
apiVersion: orkstack/v1
kind: Orkflow
name: beside
version: 1.0.0
tasks:
  - id: build
    uses: shell
    with: {command: "echo build"}
  - id: deploy
    uses: shell
    needs: [build]
    with: {command: "echo deploy"}
`

// publishedSource is a RunEventSource over the events a test run published,
// as the cluster's history reader is over the recorded ones: every number up
// to the last published that holds no event is an unpublished gap.
type publishedSource struct {
	mu     sync.Mutex
	events map[int64]*orkstackv1.Event
}

func (s *publishedSource) add(event *orkstackv1.Event) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.events[event.GetSequence()] = proto.Clone(event).(*orkstackv1.Event)
}

func (s *publishedSource) read() ([]*orkstackv1.Event, []engine.EventGap) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var numbers []int64
	for n := range s.events {
		numbers = append(numbers, n)
	}
	sort.Slice(numbers, func(i, j int) bool { return numbers[i] < numbers[j] })
	var (
		events []*orkstackv1.Event
		gaps   []engine.EventGap
		next   int64 = 1
	)
	for _, n := range numbers {
		if n > next {
			gaps = append(gaps, engine.EventGap{First: next, Last: n - 1, Reason: engine.UnpublishedEventsReason})
		}
		events = append(events, s.events[n])
		next = n + 1
	}
	return events, gaps
}

func (s *publishedSource) Events(context.Context, string, int64) ([]*orkstackv1.Event, error) {
	events, _ := s.read()
	return events, nil
}

func (s *publishedSource) EventsForArchive(context.Context, string) (engine.ArchiveRead, error) {
	events, gaps := s.read()
	return engine.ArchiveRead{Events: events, Gaps: gaps}, nil
}

// runCompletionBesideCancel runs cancelBesideDocument, whose operator's cancel
// reaches it after the task's completion has, while the completion's own
// TASK_SUCCEEDED is being written: the snapshot write of that event asks for
// the cancel (continuation chaos C4, where the two reached the run in one
// workflow task and the emit wrote its snapshot on the canceled context). It
// returns what the run published, the archive's events and gaps, and the
// run's error.
func runCompletionBesideCancel(t *testing.T) (published []*orkstackv1.Event, archived []*orkstackv1.Event,
	gaps []engine.EventGap, runErr error,
) {
	t.Helper()
	return runCancelDuringWrite(t, "build", orkstackv1.Status_STATUS_SUCCEEDED)
}

// runCancelDuringWrite runs cancelBesideDocument and asks for the operator's
// cancel from the first snapshot write that shows task in status, so the
// cancel lands while that event is being written.
func runCancelDuringWrite(t *testing.T, taskID string, status orkstackv1.Status) (published []*orkstackv1.Event,
	archived []*orkstackv1.Event, gaps []engine.EventGap, runErr error,
) {
	t.Helper()
	const runID = "ork-completion-beside-cancel"
	var suite testsuite.WorkflowTestSuite
	env := generousDeadlock(suite.NewTestWorkflowEnvironment())
	env.SetTestTimeout(30 * time.Second)
	env.RegisterWorkflowWithOptions(engine.RunWorkflow, workflow.RegisterOptions{Name: engine.RunWorkflowType})
	env.RegisterActivityWithOptions(
		func(_ context.Context, in *orkstackv1.ExecuteTaskInput) (*orkstackv1.ExecuteTaskResult, error) {
			return &orkstackv1.ExecuteTaskResult{TaskId: in.GetTaskId(), ValuesJson: []byte(`{}`)}, nil
		},
		activity.RegisterOptions{Name: task.ActivityExecuteTask},
	)
	snapshots := newTestSnapshotStore()
	write := engine.NewSnapshotActivities(snapshots).WriteSnapshot
	var canceled sync.Once
	env.RegisterActivityWithOptions(
		func(ctx context.Context, run *orkstackv1.Run, sequence int64) (string, error) {
			for _, row := range run.GetTaskRuns() {
				if row.GetTaskId() == taskID && row.GetStatus() == status {
					canceled.Do(env.CancelWorkflow)
				}
			}
			return write(ctx, run, sequence)
		},
		activity.RegisterOptions{Name: engine.ActivityWriteSnapshot})
	source := &publishedSource{events: map[int64]*orkstackv1.Event{}}
	archiver, archive, _ := newArchiveFixture(t)
	projection := engine.NewArchivingEventActivities(engine.NewEventBus(), engine.NewRunStore(), archiver).
		WithEventSource(source).WithSnapshotStore(snapshots)
	env.RegisterActivityWithOptions(
		func(ctx context.Context, event *orkstackv1.Event, snapshotURI, dedupeKey string) error {
			source.add(event)
			return projection.PublishEvent(ctx, event, snapshotURI, dedupeKey)
		},
		activity.RegisterOptions{Name: engine.ActivityPublishEvent})

	env.ExecuteWorkflow(engine.RunWorkflow, runID, compile(t, cancelBesideDocument), payload(t, nil),
		defaultPlan(), (*orkstackv1.RunContinuation)(nil))
	if !env.IsWorkflowCompleted() {
		t.Fatal("the run did not end")
	}
	published, _ = source.read()
	body, err := archive.GetRun(context.Background(), runID)
	if err != nil {
		t.Fatalf("the run was not archived: %v", err)
	}
	if _, archived, err = engine.UnmarshalRunArchive(body); err != nil {
		t.Fatalf("decode the archive: %v", err)
	}
	if gaps, err = engine.UnmarshalRunArchiveGaps(body); err != nil {
		t.Fatalf("decode the archive's gaps: %v", err)
	}
	return published, archived, gaps, env.GetWorkflowError()
}

// TestEmitPastCancel_ACompletionBesideTheCancelIsPublished is continuation
// chaos C4's race in workflow time: a task's completion and the operator's
// cancel reach the run in one workflow task. The task committed, so its
// TASK_SUCCEEDED is written on the event context, which the cancel does not
// reach, and published; the run then ends canceled. Its stream is contiguous
// and its archive holds every event and no gap. Before EmitPastCancelChange
// the success's snapshot write was canceled with the run, its number never
// published, and the archive recorded the hole.
func TestEmitPastCancel_ACompletionBesideTheCancelIsPublished(t *testing.T) {
	published, archived, gaps, runErr := runCompletionBesideCancel(t)
	if runErr == nil || !temporal.IsCanceledError(runErr) {
		t.Fatalf("the run ended %v, want canceled", runErr)
	}
	var types []orkstackv1.EventType
	for i, event := range published {
		if event.GetSequence() != int64(i+1) {
			t.Fatalf("published event %d is numbered %d: the stream has a hole (%v)", i+1, event.GetSequence(), types)
		}
		types = append(types, event.GetType())
	}
	succeeded := false
	for _, event := range published {
		if event.GetType() == orkstackv1.EventType_EVENT_TYPE_TASK_SUCCEEDED && event.GetTaskId() == "build" {
			succeeded = true
		}
	}
	if !succeeded {
		t.Fatalf("the committed task's TASK_SUCCEEDED was never published: %v", types)
	}
	if last := published[len(published)-1]; last.GetType() != orkstackv1.EventType_EVENT_TYPE_RUN_CANCELED {
		t.Fatalf("the stream ends %v, want RUN_CANCELED: %v", last.GetType(), types)
	}
	if len(gaps) != 0 {
		t.Fatalf("the archive records gaps %+v", gaps)
	}
	if len(archived) != len(published) {
		t.Fatalf("the archive holds %d events, the run published %d", len(archived), len(published))
	}
	t.Logf("completion beside the cancel: %v, archived with no gap", types)
}

// TestEmitPastCancel_BeforeTheChangeTheCompletionIsLost is the same race on
// the path of the builds before EmitPastCancelChange: the success's snapshot
// write is canceled with the run, its number is never published, the archive
// records the hole, and the run ends with the storage error as its reason.
// It is what the test above guards against, so the harness provably sees it.
func TestEmitPastCancel_BeforeTheChangeTheCompletionIsLost(t *testing.T) {
	defer engine.DisableEmitPastCancelGateForTest()()
	published, _, gaps, _ := runCompletionBesideCancel(t)
	for _, event := range published {
		if event.GetType() == orkstackv1.EventType_EVENT_TYPE_TASK_SUCCEEDED {
			t.Fatalf("build's TASK_SUCCEEDED (event %d) was published on the old path", event.GetSequence())
		}
	}
	if len(gaps) != 1 || !gaps[0].Unpublished() {
		t.Fatalf("the archive records gaps %+v, want the one unpublished number", gaps)
	}
}

// TestEmitPastCancel_ACanceledDispatchIsNeverAnnounced: the cancel lands
// while deploy's TASK_SCHEDULED is being written. A dispatch's announcement
// still writes on the run's own context, so the cancel stops it and the run
// never announces a task it will not dispatch. Its number stays used — the
// stopped write may still finish its object, so no other event may take the
// key — and the archive records it as an unpublished gap, explicitly; build's
// TASK_SUCCEEDED, written before the cancel, is in the stream, and the run
// ends canceled.
func TestEmitPastCancel_ACanceledDispatchIsNeverAnnounced(t *testing.T) {
	published, archived, gaps, runErr := runCancelDuringWrite(t, "deploy", orkstackv1.Status_STATUS_RUNNING)
	if runErr == nil || !temporal.IsCanceledError(runErr) {
		t.Fatalf("the run ended %v, want canceled", runErr)
	}
	var types []orkstackv1.EventType
	succeeded := false
	for _, event := range published {
		types = append(types, event.GetType())
		if event.GetTaskId() == "deploy" && announcesDispatchForTest(event.GetType()) {
			t.Fatalf("deploy's %v was published, though the run never dispatched it", event.GetType())
		}
		if event.GetTaskId() == "build" && event.GetType() == orkstackv1.EventType_EVENT_TYPE_TASK_SUCCEEDED {
			succeeded = true
		}
	}
	if !succeeded {
		t.Fatalf("build's TASK_SUCCEEDED is missing: %v", types)
	}
	if last := published[len(published)-1]; last.GetType() != orkstackv1.EventType_EVENT_TYPE_RUN_CANCELED {
		t.Fatalf("the stream ends %v, want RUN_CANCELED: %v", last.GetType(), types)
	}
	if len(gaps) != 1 || !gaps[0].Unpublished() || gaps[0].First != gaps[0].Last {
		t.Fatalf("the archive records gaps %+v, want the one number deploy's announcement used", gaps)
	}
	if len(archived) != len(published) {
		t.Fatalf("the archive holds %d events, the run published %d", len(archived), len(published))
	}
	t.Logf("cancel during a dispatch's announcement: %v, gap %+v recorded", types, gaps[0])
}

func announcesDispatchForTest(eventType orkstackv1.EventType) bool {
	return eventType == orkstackv1.EventType_EVENT_TYPE_TASK_SCHEDULED ||
		eventType == orkstackv1.EventType_EVENT_TYPE_TASK_STARTED
}
