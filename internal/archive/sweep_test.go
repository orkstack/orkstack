package archive

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/orkstack/orkstack/pkg/artifacts"

	"github.com/orkstack/orkstack/internal/testsupport/must"
)

var sweepNow = time.Date(2026, 11, 4, 12, 0, 0, 0, time.UTC)

const (
	sweepInterval = time.Hour
	sweepTTL      = 96 * time.Hour
)

// fakeRuns is the cluster's index of finished executions.
type fakeRuns struct {
	closed     map[string]time.Time
	order      []string
	err        error
	askedFor   []time.Time
	pageSizes  []int
	afterVisit func()
}

func (f *fakeRuns) add(runID string, closedAgo time.Duration) {
	if f.closed == nil {
		f.closed = map[string]time.Time{}
	}
	f.closed[runID] = sweepNow.Add(-closedAgo)
	f.order = append(f.order, runID)
}

func (f *fakeRuns) EachFinishedRun(
	_ context.Context, closedBefore time.Time, pageSize int, visit func(string, time.Time) error,
) error {
	f.askedFor = append(f.askedFor, closedBefore)
	f.pageSizes = append(f.pageSizes, pageSize)
	if f.err != nil {
		return f.err
	}
	for _, id := range f.order {
		if f.closed[id].Before(closedBefore) {
			if err := visit(id, f.closed[id]); err != nil {
				return err
			}
			if f.afterVisit != nil {
				f.afterVisit()
			}
		}
	}
	return nil
}

// fakeStore is the shared archive and the history behind it: an archive write
// makes the body appear.
type fakeStore struct {
	bodies   map[string]bool
	hasErr   error
	hasCalls int
	// stalls makes HasRun wait until its context ends, as a store that accepts
	// the request and never answers does.
	stalls   bool
	writeErr map[string]error
	written  []string
}

func (f *fakeStore) HasRun(ctx context.Context, runID string) (bool, error) {
	f.hasCalls++
	if f.stalls {
		<-ctx.Done()
		return false, ctx.Err()
	}
	if f.hasErr != nil {
		return false, f.hasErr
	}
	return f.bodies[runID], nil
}

func (f *fakeStore) ArchiveFromHistory(_ context.Context, runID string) error {
	if err := f.writeErr[runID]; err != nil {
		return err
	}
	if f.bodies == nil {
		f.bodies = map[string]bool{}
	}
	f.bodies[runID] = true
	f.written = append(f.written, runID)
	return nil
}

type recordedSweeps struct{ results []SweepResult }

func (r *recordedSweeps) ObserveSweep(_ context.Context, result SweepResult) {
	r.results = append(r.results, result)
}

type sweepFixture struct {
	runs     *fakeRuns
	store    *fakeStore
	cluster  *fakeCluster
	observed *recordedSweeps
	sweeper  *Sweeper
}

func newSweepFixture() *sweepFixture {
	f := &sweepFixture{
		runs: &fakeRuns{}, store: &fakeStore{bodies: map[string]bool{}}, cluster: newFakeCluster(), observed: &recordedSweeps{},
	}
	f.cluster.retention = sweepTTL
	f.sweeper = NewSweeper(CleanupConfig{TemporalNamespace: "ork-main", Interval: sweepInterval, BatchSize: 25},
		f.runs, f.store, f.store, f.cluster, f.observed, testLogger()).
		WithClock(func() time.Time { return sweepNow })
	return f
}

func sameStrings(a, b []string) bool {
	return strings.Join(a, ",") == strings.Join(b, ",")
}

// TestSweeper_ArchivesAFinishedRunWithNoArchive: a run with no archive that
// has been closed a cleanup interval is archived, a run with one is left
// alone, and a run closed more recently is not looked at: its own terminal
// publish may still be finishing. A second pass has nothing left to do.
func TestSweeper_ArchivesAFinishedRunWithNoArchive(t *testing.T) {
	f := newSweepFixture()
	f.runs.add("ork-unarchived", 2*time.Hour)
	f.runs.add("ork-archived", 2*time.Hour)
	f.runs.add("ork-just-closed", 10*time.Minute)
	f.store.bodies["ork-archived"] = true

	result, err := f.sweeper.RunOnce(context.Background())
	must.OKf(t, err, "RunOnce")
	if !sameStrings(f.store.written, []string{"ork-unarchived"}) {
		t.Fatalf("archived %v, want the one unarchived run", f.store.written)
	}
	if result.Examined != 2 || result.Archived != 1 || result.Unconfirmed != 0 || !result.EarliestExpiry.IsZero() {
		t.Fatalf("result = %+v, want 2 examined, 1 archived, none unconfirmed", result)
	}
	if want := sweepNow.Add(-sweepInterval); !f.runs.askedFor[0].Equal(want) || f.runs.pageSizes[0] != 25 {
		t.Fatalf("asked for runs closed before %v in pages of %d, want %v and 25", f.runs.askedFor[0], f.runs.pageSizes[0], want)
	}

	result, err = f.sweeper.RunOnce(context.Background())
	must.OKf(t, err, "second RunOnce")
	if len(f.store.written) != 1 || result.Archived != 0 {
		t.Fatalf("the second pass archived %v (%+v), want nothing more", f.store.written, result)
	}
}

// TestSweeper_AStoreThatIsDownIsSaidAndHealsOnTheNextPass: with object storage
// down nothing can be archived or even confirmed, so every finished run counts
// as unconfirmed and the pass says when the first of them expires. It asks the
// store once, not once per run, and writes nothing. The next pass after the
// store returns archives what it could not.
func TestSweeper_AStoreThatIsDownIsSaidAndHealsOnTheNextPass(t *testing.T) {
	f := newSweepFixture()
	f.runs.add("ork-newer", 2*time.Hour)
	f.runs.add("ork-oldest", 90*time.Hour)
	f.runs.add("ork-older", 40*time.Hour)
	f.store.hasErr = errInjected

	result, err := f.sweeper.RunOnce(context.Background())
	if !errors.Is(err, errInjected) {
		t.Fatalf("RunOnce = %v, want the store's failure", err)
	}
	if f.store.hasCalls != 1 || len(f.store.written) != 0 {
		t.Fatalf("asked the store %d times and wrote %v, want one question and no write", f.store.hasCalls, f.store.written)
	}
	wantExpiry := sweepNow.Add(-90 * time.Hour).Add(sweepTTL)
	if result.Examined != 3 || result.Unconfirmed != 3 || !result.EarliestExpiry.Equal(wantExpiry) || !result.StoreFailed {
		t.Fatalf("result = %+v, want 3 unconfirmed, the earliest expiring at %v, and the store's failure said", result, wantExpiry)
	}
	if len(f.observed.results) != 1 || f.observed.results[0].Unconfirmed != 3 {
		t.Fatalf("observed %+v, want the risk reported", f.observed.results)
	}

	f.store.hasErr = nil
	result, err = f.sweeper.RunOnce(context.Background())
	must.OKf(t, err, "RunOnce after the store returned")
	if result.Archived != 3 || result.Unconfirmed != 0 || result.StoreFailed {
		t.Fatalf("result = %+v, want all 3 archived and the store answering", result)
	}
	if last := f.observed.results[len(f.observed.results)-1]; last.Unconfirmed != 0 {
		t.Fatalf("observed %+v, want the risk cleared", last)
	}
}

// TestSweeper_AStoreThatStallsStillEndsThePass: a store that accepts a request
// and never answers must not hold the pass, because the pass's report is what
// says the runs are at risk. The context passed in here has no deadline.
func TestSweeper_AStoreThatStallsStillEndsThePass(t *testing.T) {
	f := newSweepFixture()
	f.sweeper.questionBudget = time.Millisecond
	f.runs.add("ork-1", 2*time.Hour)
	f.runs.add("ork-2", 3*time.Hour)
	f.store.stalls = true

	done := make(chan SweepResult, 1)
	go func() {
		result, _ := f.sweeper.RunOnce(context.Background())
		done <- result
	}()
	select {
	case result := <-done:
		if result.Unconfirmed != 2 || f.store.hasCalls != 1 || result.EarliestExpiry.IsZero() {
			t.Fatalf("result = %+v after %d questions, want 2 unconfirmed and dated after one", result, f.store.hasCalls)
		}
	case <-time.After(30 * time.Second):
		t.Fatal("a store that stalls held the pass")
	}
}

// TestSweeper_SaysWhenItsIntervalLeavesNoTime: a run waits out one interval and
// then the next pass, so two intervals at or past the cluster's retention can
// let a run go before a pass looks at it. Nothing else would ever report that
// run: it leaves the listing unseen.
func TestSweeper_SaysWhenItsIntervalLeavesNoTime(t *testing.T) {
	for _, tc := range []struct {
		interval time.Duration
		tooLong  bool
	}{
		{interval: time.Hour},
		{interval: 47 * time.Hour},
		{interval: 48 * time.Hour, tooLong: true},
		{interval: 100 * time.Hour, tooLong: true},
	} {
		t.Run(tc.interval.String(), func(t *testing.T) {
			f := newSweepFixture()
			f.sweeper.config.Interval = tc.interval
			result, err := f.sweeper.RunOnce(context.Background())
			if err != nil || result.IntervalTooLong != tc.tooLong {
				t.Fatalf("result = %+v (%v), want IntervalTooLong %v under a retention of %s", result, err, tc.tooLong, sweepTTL)
			}
		})
	}
}

// TestSweeper_ARunThatCannotBeArchivedStopsNoOther: one run whose archive
// cannot be written is reported, with its expiry, and the rest of the pass
// goes on.
func TestSweeper_ARunThatCannotBeArchivedStopsNoOther(t *testing.T) {
	f := newSweepFixture()
	f.runs.add("ork-bad", 50*time.Hour)
	f.runs.add("ork-good", 3*time.Hour)
	f.store.writeErr = map[string]error{"ork-bad": errInjected}

	result, err := f.sweeper.RunOnce(context.Background())
	if !errors.Is(err, errInjected) || !strings.Contains(err.Error(), "ork-bad") {
		t.Fatalf("RunOnce = %v, want the failure, naming the run", err)
	}
	if !sameStrings(f.store.written, []string{"ork-good"}) {
		t.Fatalf("archived %v, want the other run", f.store.written)
	}
	if want := sweepNow.Add(-50 * time.Hour).Add(sweepTTL); result.Archived != 1 || result.Failed != 1 ||
		result.Unconfirmed != 1 || !result.EarliestExpiry.Equal(want) {
		t.Fatalf("result = %+v, want 1 archived, 1 failed and unconfirmed, expiring at %v", result, want)
	}
}

// TestSweeper_ARunGoneBeforeItCouldBeArchivedIsSaid: a run the index listed
// and the cluster no longer holds had no archive when it went. Nothing can
// bring it back; the pass says so rather than treating it as nothing.
func TestSweeper_ARunGoneBeforeItCouldBeArchivedIsSaid(t *testing.T) {
	f := newSweepFixture()
	f.runs.add("ork-gone", 97*time.Hour)
	f.store.writeErr = map[string]error{"ork-gone": fmt.Errorf("read: %w", artifacts.ErrNotFound)}

	result, err := f.sweeper.RunOnce(context.Background())
	if err == nil || !strings.Contains(err.Error(), "ork-gone") {
		t.Fatalf("RunOnce = %v, want it named", err)
	}
	if result.Expired != 1 || result.Unconfirmed != 0 || result.Failed != 0 {
		t.Fatalf("result = %+v, want 1 expired and nothing still at risk", result)
	}
}

// TestSweeper_AClusterRetentionItCannotReadStillSweeps: the cluster's retention
// only dates the risk. Not being able to read it archives as ever, and reports
// no expiry rather than an invented one.
func TestSweeper_AClusterRetentionItCannotReadStillSweeps(t *testing.T) {
	f := newSweepFixture()
	f.cluster.retentionErr = errInjected
	f.runs.add("ork-bad", 50*time.Hour)
	f.runs.add("ork-good", 3*time.Hour)
	f.store.writeErr = map[string]error{"ork-bad": errors.New("refused")}

	result, _ := f.sweeper.RunOnce(context.Background())
	if result.Archived != 1 || result.Unconfirmed != 1 || !result.EarliestExpiry.IsZero() || result.IntervalTooLong {
		t.Fatalf("result = %+v, want 1 archived, 1 unconfirmed, no expiry and no verdict on the interval", result)
	}
}

// TestSweeper_AClusterThatCannotListIsSaid: a pass that could not read the
// index knows nothing about any run, and says that rather than a clean zero.
func TestSweeper_AClusterThatCannotListIsSaid(t *testing.T) {
	f := newSweepFixture()
	f.runs.err = errInjected

	result, err := f.sweeper.RunOnce(context.Background())
	if !errors.Is(err, errInjected) {
		t.Fatalf("RunOnce = %v, want the failure", err)
	}
	if !result.ListFailed || len(f.observed.results) != 1 || !f.observed.results[0].ListFailed {
		t.Fatalf("result = %+v, observed %+v, want the failed listing said", result, f.observed.results)
	}
}

// TestSweeper_RunPassesAtOnceThenEveryIntervalUntilCanceled: the first pass
// runs when the worker starts, and the worker ends with its context.
func TestSweeper_RunPassesAtOnceThenEveryIntervalUntilCanceled(t *testing.T) {
	passes := make(chan struct{}, 8)
	runs := &fakeRuns{}
	store := &fakeStore{bodies: map[string]bool{}}
	cluster := newFakeCluster()
	sweeper := NewSweeper(CleanupConfig{TemporalNamespace: "ork-main", Interval: time.Millisecond, BatchSize: 1},
		runs, store, store, cluster, sweepObserverFunc(func(context.Context, SweepResult) { passes <- struct{}{} }), testLogger())

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		sweeper.Run(ctx)
		close(done)
	}()
	for range 2 {
		select {
		case <-passes:
		case <-time.After(10 * time.Second):
			t.Fatal("the worker did not pass")
		}
	}
	cancel()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("the worker did not end with its context")
	}
}

type sweepObserverFunc func(context.Context, SweepResult)

func (f sweepObserverFunc) ObserveSweep(ctx context.Context, r SweepResult) { f(ctx, r) }
