package archive

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/orkstack/orkstack/internal/testsupport/must"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// endingStore is what is archived of runs' endings.
type endingStore struct {
	endings map[string]artifacts.RunEndings
	// bodies holds the scopes archived of each run; 0 is its first ending.
	bodies  map[string]map[uint64]bool
	listErr error
	err     error
	asked   int
}

func (s *endingStore) RunEndings(_ context.Context, runID string) (artifacts.RunEndings, error) {
	return s.endings[runID], s.listErr
}

func (s *endingStore) HasEnding(_ context.Context, runID string, scope uint64) (bool, error) {
	s.asked++
	return s.bodies[runID][scope], s.err
}

func (s *endingStore) EachResumedRun(_ context.Context, visit func(artifacts.RunEndings) error) error {
	if s.listErr != nil {
		return s.listErr
	}
	for _, e := range s.endings {
		if err := visit(e); err != nil {
			return err
		}
	}
	return nil
}

// endingCluster is the newest ending the cluster holds of each run.
type endingCluster struct {
	newest map[string]clusterEnding
	err    error
	asked  []string
}

type clusterEnding struct {
	scope  uint64
	closed time.Time // zero: still running
}

func (c *endingCluster) NewestEnding(_ context.Context, namespace, runID string) (uint64, time.Time, bool, error) {
	c.asked = append(c.asked, namespace+"/"+runID)
	if c.err != nil {
		return 0, time.Time{}, false, c.err
	}
	e, ok := c.newest[runID]
	return e.scope, e.closed, ok, nil
}

func marked(runID string, marked, archived []uint64) artifacts.RunEndings {
	return artifacts.RunEndings{RunID: runID, Marked: marked, Archived: archived}
}

// TestNewestEndings_HasRun: a run's archive is whole when its NEWEST ending has
// a body. The cluster says which that is, and is asked only about a run a
// resume marked: the mark alone decides nothing.
func TestNewestEndings_HasRun(t *testing.T) {
	closed := sweepNow.Add(-2 * time.Hour)
	for name, tc := range map[string]struct {
		endings artifacts.RunEndings
		bodies  map[uint64]bool
		cluster map[string]clusterEnding
		want    bool
		asked   int
	}{
		"never resumed":                   {bodies: map[uint64]bool{0: true}, want: true},
		"never resumed, not archived":     {},
		"resumed, its ending archived":    {endings: marked("r", []uint64{7}, []uint64{7}), bodies: map[uint64]bool{0: true, 7: true}, cluster: map[string]clusterEnding{"r": {7, closed}}, want: true, asked: 1},
		"resumed, its ending not":         {endings: marked("r", []uint64{7}, nil), bodies: map[uint64]bool{0: true}, cluster: map[string]clusterEnding{"r": {7, closed}}, asked: 1},
		"resumed and running":             {endings: marked("r", []uint64{7}, nil), bodies: map[uint64]bool{0: true}, cluster: map[string]clusterEnding{"r": {scope: 7}}, asked: 1},
		"a mark whose reset never ran":    {endings: marked("r", []uint64{7}, nil), bodies: map[uint64]bool{0: true}, cluster: map[string]clusterEnding{"r": {0, closed}}, want: true, asked: 1},
		"a mark after an archived ending": {endings: marked("r", []uint64{7, 20}, []uint64{7}), bodies: map[uint64]bool{0: true, 7: true}, cluster: map[string]clusterEnding{"r": {7, closed}}, want: true, asked: 1},
		"gone from the cluster":           {endings: marked("r", []uint64{7}, []uint64{7}), bodies: map[uint64]bool{0: true, 7: true}, want: true, asked: 1},
	} {
		t.Run(name, func(t *testing.T) {
			store := &endingStore{endings: map[string]artifacts.RunEndings{"r": tc.endings}, bodies: map[string]map[uint64]bool{"r": tc.bodies}}
			cluster := &endingCluster{newest: tc.cluster}
			has, err := NewNewestEndings(store, cluster, "ork-main").HasRun(context.Background(), "r")
			if err != nil || has != tc.want || len(cluster.asked) != tc.asked {
				t.Fatalf("HasRun = %v, %v, asked %v; want %v asking %d", has, err, cluster.asked, tc.want, tc.asked)
			}
		})
	}
	// No answer is an error, never false.
	for name, tc := range map[string]struct {
		store   *endingStore
		cluster *endingCluster
	}{
		"listing": {&endingStore{listErr: errors.New("down")}, &endingCluster{}},
		"body":    {&endingStore{err: errors.New("down")}, &endingCluster{}},
		"cluster": {&endingStore{endings: map[string]artifacts.RunEndings{"r": marked("r", []uint64{7}, nil)}}, &endingCluster{err: errors.New("down")}},
	} {
		if has, err := NewNewestEndings(tc.store, tc.cluster, "ork-main").HasRun(context.Background(), "r"); err == nil || has {
			t.Errorf("%s: HasRun = %v, %v; want an error", name, has, err)
		}
	}
}

// resumedSweep is a sweep of one run that failed and was then resumed: the
// cluster's index lists both of its closed executions.
func resumedSweep(t *testing.T, e clusterEnding) (*sweepFixture, *endingStore, *endingCluster) {
	t.Helper()
	f := newSweepFixture()
	store := &endingStore{
		endings: map[string]artifacts.RunEndings{"ork-r": marked("ork-r", []uint64{7}, nil)},
		bodies:  map[string]map[uint64]bool{"ork-r": {0: true}},
	}
	cluster := &endingCluster{newest: map[string]clusterEnding{"ork-r": e}}
	f.runs.add("ork-r", 40*time.Hour) // the base's close
	f.runs.order = append(f.runs.order, "ork-r")
	f.store.bodies["ork-r"] = true // the first ending's body
	f.sweeper.WithEndings(store, NewNewestEndings(store, cluster, "ork-main"))
	return f, store, cluster
}

// TestSweeper_ArchivesAResumedRunsNewestEnding: a resumed run has its first
// ending's body, so the sweep asks about the newest, archives it once though
// the cluster lists both executions, and dates its risk by its own close.
func TestSweeper_ArchivesAResumedRunsNewestEnding(t *testing.T) {
	ctx := context.Background()
	ended := sweepNow.Add(-2 * time.Hour)
	f, _, cluster := resumedSweep(t, clusterEnding{7, ended})
	result, err := f.sweeper.RunOnce(ctx)
	must.OKf(t, err, "sweep")
	if result.Archived != 1 || !sameStrings(f.store.written, []string{"ork-r"}) || len(cluster.asked) != 1 {
		t.Fatalf("result %+v, wrote %v, asked %v; want the newest ending archived once", result, f.store.written, cluster.asked)
	}

	failing, _, _ := resumedSweep(t, clusterEnding{7, ended})
	failing.store.writeErr = map[string]error{"ork-r": errors.New("refused")}
	result, _ = failing.sweeper.RunOnce(ctx)
	if result.Failed != 1 || result.Unconfirmed != 1 || !result.EarliestExpiry.Equal(ended.Add(sweepTTL)) {
		t.Fatalf("result %+v, want the risk dated by the newest ending's close", result)
	}

	archived, store, _ := resumedSweep(t, clusterEnding{7, ended})
	store.bodies["ork-r"][7] = true
	if result, err = archived.sweeper.RunOnce(ctx); err != nil || result.Archived != 0 || result.Unconfirmed != 0 {
		t.Fatalf("result %+v, %v; want nothing to do", result, err)
	}

	running, _, _ := resumedSweep(t, clusterEnding{scope: 7})
	if result, err = running.sweeper.RunOnce(ctx); err != nil || result.Archived != 0 || result.Unconfirmed != 0 {
		t.Fatalf("a running run: result %+v, %v; want nothing to do", result, err)
	}

	recent, _, _ := resumedSweep(t, clusterEnding{7, sweepNow.Add(-time.Minute)})
	if result, err = recent.sweeper.RunOnce(ctx); err != nil || result.Archived != 0 || result.Unconfirmed != 0 {
		t.Fatalf("an ending closed a moment ago: result %+v, %v; want it left to its own publish", result, err)
	}
}

// TestSweeper_AResumedRunsLastChance is its newest ending's: an earlier one the
// cluster is about to delete is no reason to archive the newest unverified.
func TestSweeper_AResumedRunsLastChance(t *testing.T) {
	for name, tc := range map[string]struct {
		ago  time.Duration
		last bool
	}{"long before it": {2 * time.Hour, false}, "at it": {sweepTTL - 2*time.Hour, true}} {
		f, _, _ := resumedSweep(t, clusterEnding{7, sweepNow.Add(-tc.ago)})
		f.runs.add("ork-r", sweepTTL-time.Minute) // the base is about to go
		if result, err := f.sweeper.RunOnce(context.Background()); err != nil || result.Archived != 1 || (len(f.store.lastResort) == 1) != tc.last {
			t.Errorf("%s: %+v, %v, as a last resort %v", name, result, err, f.store.lastResort)
		}
	}
}

// TestSweeper_AResumedRunThatCannotBeAskedAbout: no answer is unconfirmed,
// never archived, and stops no other run.
func TestSweeper_AResumedRunThatCannotBeAskedAbout(t *testing.T) {
	ctx := context.Background()
	f, _, cluster := resumedSweep(t, clusterEnding{})
	cluster.err = errors.New("the cluster is down")
	f.runs.add("ork-s", 3*time.Hour)
	result, err := f.sweeper.RunOnce(ctx)
	if err == nil || result.Unconfirmed != 1 || result.Archived != 1 || !sameStrings(f.store.written, []string{"ork-s"}) {
		t.Fatalf("result %+v, %v, wrote %v; want ork-r unconfirmed and ork-s archived", result, err, f.store.written)
	}
	down, store, _ := resumedSweep(t, clusterEnding{})
	store.listErr = errors.New("the store is down")
	result, err = down.sweeper.RunOnce(ctx)
	if err == nil || !result.StoreFailed || result.Unconfirmed == 0 || len(down.store.written) != 0 {
		t.Fatalf("result %+v, %v; want every run unconfirmed", result, err)
	}
}

// TestEndingRecords: a run marked as resumed after its newest archived ending
// is not retired by the artifact GC.
func TestEndingRecords(t *testing.T) {
	ctx := context.Background()
	for name, tc := range map[string]struct {
		endings artifacts.RunEndings
		want    bool
	}{
		"never resumed":         {marked("r", nil, nil), false},
		"resumed, not archived": {marked("r", []uint64{7}, nil), true},
		"resumed and archived":  {marked("r", []uint64{7}, []uint64{7}), false},
		"resumed again since":   {marked("r", []uint64{7, 20}, []uint64{7}), true},
	} {
		got, err := NewEndingRecords(&endingStore{endings: map[string]artifacts.RunEndings{"r": tc.endings}}).ResumedAfter(ctx, "r", sweepNow)
		if err != nil || got != tc.want {
			t.Errorf("%s: %v, %v; want %v", name, got, err, tc.want)
		}
	}
	if _, err := NewEndingRecords(&endingStore{listErr: errors.New("down")}).ResumedAfter(ctx, "r", sweepNow); err == nil {
		t.Error("a store that does not answer answered")
	}
}

// TestNewestEndings_Resumed: whether a resume marked the run, without asking
// the cluster.
func TestNewestEndings_Resumed(t *testing.T) {
	cluster := &endingCluster{err: errors.New("down")}
	n := NewNewestEndings(&endingStore{endings: map[string]artifacts.RunEndings{"r": marked("r", []uint64{7}, nil)}}, cluster, "ork-main")
	for run, want := range map[string]bool{"r": true, "s": false} {
		if got, err := n.Resumed(context.Background(), run); err != nil || got != want {
			t.Errorf("%s: %v, %v; want %v", run, got, err, want)
		}
	}
	if len(cluster.asked) != 0 {
		t.Error("the cluster was asked")
	}
}
