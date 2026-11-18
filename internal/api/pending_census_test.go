package api_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/types/known/timestamppb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/api"
	"github.com/orkstack/orkstack/internal/engine"

	"github.com/orkstack/orkstack/internal/testsupport/must"
)

// dispatchFakeReader answers the run's dispatch describe, and counts it.
type dispatchFakeReader struct {
	*fakeReader
	mu        sync.Mutex
	dispatch  engine.RunDispatch
	waiting   []string // task ids whose activity waits for an Orker
	err       error
	describes int
}

func (r *dispatchFakeReader) RunDispatch(context.Context, string, time.Time) (engine.RunDispatch, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.describes++
	return r.dispatch, r.err
}

// LiveAttempts answers the history read: every waiting task the fixture
// names, scheduled a minute ago.
func (r *dispatchFakeReader) LiveAttempts(context.Context, string, time.Time) (engine.LiveRun, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	var attempts []*orkstackv1.LiveAttempt
	for _, id := range r.waiting {
		attempts = append(attempts, &orkstackv1.LiveAttempt{TaskId: id, Attempt: 1,
			State:              orkstackv1.LiveAttemptState_LIVE_ATTEMPT_STATE_SCHEDULED,
			AttemptScheduledAt: timestamppb.New(inspectNow.Add(-time.Minute))})
	}
	return engine.LiveRun{Attempts: attempts}, nil
}

func (r *dispatchFakeReader) count() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.describes
}

// stoppingFleet is pool "build" whose only Orker, of release b1, is stopping.
func stoppingFleet() *stubFleet {
	return &stubFleet{
		presence: []engine.OrkerPresence{{Identity: "orker-b1", LastSeen: fleetSeen}},
		described: map[string]*orkstackv1.DescribeOrkerResult{
			"orker-b1": {OrkerId: "orker-b1", Pool: "build", BuildId: "b1", MaxConcurrent: 4,
				Cordoned: true, Stopping: &orkstackv1.OrkerStopping{}},
		},
	}
}

func newCensusFixture(t *testing.T, run *orkstackv1.Run, dispatch engine.RunDispatch, fleet *stubFleet) (*readerFixture, *dispatchFakeReader) {
	t.Helper()
	reader := &fakeReader{runs: map[string]*orkstackv1.Run{run.GetId(): run}}
	dispatchReader := &dispatchFakeReader{fakeReader: reader, dispatch: dispatch, waiting: []string{"fetch"}}
	fixture := newReaderFixtureWith(t, reader, func(cfg *api.Config) {
		cfg.Reader = dispatchReader
		cfg.Now = func() time.Time { return inspectNow }
		cfg.Fleet = fleet
	})
	return fixture, dispatchReader
}

func getRunOf(t *testing.T, f *readerFixture, runID string) *orkstackv1.Run {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	resp, err := f.runClient().GetRun(ctx, connect.NewRequest(&orkstackv1.GetRunRequest{RunId: runID}))
	must.OKf(t, err, "GetRun")
	return resp.Msg.GetRun()
}

func dispatchedRun(id string) *orkstackv1.Run {
	run := inspectRun(id, orkstackv1.Status_STATUS_RUNNING, "fetch")
	run.GetTaskRuns()[0].Pool = "build"
	run.GetTaskRuns()[0].ScheduledAt = timestamppb.New(inspectNow.Add(-time.Minute))
	return run
}

// TestGetRun_ATaskNoOrkerOfItsReleaseTakesSaysWhy: GetRun reads the run's
// release and the replica's Orker inventory, and a task its release's Orkers
// will not take says so (E92 §4.9).
func TestGetRun_ATaskNoOrkerOfItsReleaseTakesSaysWhy(t *testing.T) {
	run := dispatchedRun("ork-census-1")
	f, _ := newCensusFixture(t, run, engine.RunDispatch{Build: "b1", Scheduled: true}, stoppingFleet())
	got := getRunOf(t, f, run.GetId())
	want := "waiting for an Orker of release b1 on pool build; none takes work (1 stopping)"
	if reason := got.GetTaskRuns()[0].GetPendingReason(); reason != want {
		t.Fatalf("pending_reason = %q, want %q", reason, want)
	}
}

// TestGetRun_AReasonThatCannotBeKnownIsOmitted: an unreadable inventory and
// an unreadable dispatch each leave the reason unset, never guessed, and the
// read still answers.
func TestGetRun_AReasonThatCannotBeKnownIsOmitted(t *testing.T) {
	t.Run("the inventory cannot be read", func(t *testing.T) {
		run := dispatchedRun("ork-census-2")
		fleet := stoppingFleet()
		fleet.observeEr = errors.New("frontend unavailable")
		f, _ := newCensusFixture(t, run, engine.RunDispatch{Scheduled: true}, fleet)
		if reason := getRunOf(t, f, run.GetId()).GetTaskRuns()[0].GetPendingReason(); reason != "" {
			t.Fatalf("pending_reason = %q over an unreadable inventory", reason)
		}
	})
	t.Run("the run's dispatch cannot be read", func(t *testing.T) {
		run := dispatchedRun("ork-census-3")
		f, reader := newCensusFixture(t, run, engine.RunDispatch{}, stoppingFleet())
		reader.err = errors.New("frontend unavailable")
		if reason := getRunOf(t, f, run.GetId()).GetTaskRuns()[0].GetPendingReason(); reason != "" {
			t.Fatalf("pending_reason = %q without the run's release", reason)
		}
	})
}

// TestGetRun_ARunWithNoTaskWaitingForAnOrkerReadsNothingMore: a run whose
// snapshot holds no task that could wait costs GetRun nothing; one whose
// describe shows no task activity waiting (its containers run) costs one
// describe and says nothing, however its pool's Orkers stand.
func TestGetRun_ARunWithNoTaskWaitingForAnOrkerReadsNothingMore(t *testing.T) {
	t.Run("no row could wait", func(t *testing.T) {
		run := dispatchedRun("ork-census-4")
		run.GetTaskRuns()[0].Status = orkstackv1.Status_STATUS_SUCCEEDED
		fleet := stoppingFleet()
		f, reader := newCensusFixture(t, run, engine.RunDispatch{}, fleet)
		getRunOf(t, f, run.GetId())
		if reader.count() != 0 || fleet.observeOrkers != 0 {
			t.Fatalf("describes = %d, inventory reads = %d; want none", reader.count(), fleet.observeOrkers)
		}
	})
	t.Run("its task's container runs", func(t *testing.T) {
		run := dispatchedRun("ork-census-5")
		fleet := stoppingFleet()
		f, reader := newCensusFixture(t, run, engine.RunDispatch{Build: "b1"}, fleet)
		got := getRunOf(t, f, run.GetId())
		if reason := got.GetTaskRuns()[0].GetPendingReason(); reason != "" {
			t.Fatalf("pending_reason = %q for a task whose container runs on a stopping Orker", reason)
		}
		if reader.count() != 1 || fleet.observeOrkers != 0 {
			t.Fatalf("describes = %d, inventory reads = %d; want one describe and no inventory", reader.count(), fleet.observeOrkers)
		}
	})
}
