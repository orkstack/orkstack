package archive

import (
	"context"
	"fmt"
	"sort"
	"testing"
	"time"

	"google.golang.org/protobuf/types/known/durationpb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// TestRunOnce_NeverRetiresAMissingExecutionBeforeItsOwnRetention is E100: an
// archived run whose execution the cluster does not hold (a restored or
// damaged cluster, or a replica pointed at the wrong Temporal namespace) is
// not retired while its own namespace's run retention, counted from when it
// FINISHED, has not passed. Another namespace's short retention pulls it into
// the pass's prefilter; that must decide nothing.
func TestRunOnce_NeverRetiresAMissingExecutionBeforeItsOwnRetention(t *testing.T) {
	now := time.Date(2026, 11, 12, 12, 0, 0, 0, time.UTC)
	for _, tc := range []struct {
		name     string
		created  time.Time
		finished time.Time
		retired  int
	}{
		{name: "finished inside its retention", created: now.Add(-10 * 24 * time.Hour), finished: now.Add(-9 * 24 * time.Hour)},
		{name: "created long ago, finished just now", created: now.Add(-40 * 24 * time.Hour), finished: now.Add(-time.Hour)},
		{name: "finished past its retention", created: now.Add(-40 * 24 * time.Hour), finished: now.Add(-31 * 24 * time.Hour), retired: 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			store := newFakeArchive(
				artifacts.RunIndexEntry{RunID: "kept", Namespace: "long", Time: tc.created},
			)
			store.finished = map[string]time.Time{"kept": tc.finished}
			cluster := newFakeCluster() // the execution is not found
			namespaces := &fakeNamespaces{docs: []*orkstackv1.Namespace{
				{Name: "long", RunRetention: durationpb.New(30 * 24 * time.Hour)},
				{Name: "short", RunRetention: durationpb.New(time.Hour)},
			}}
			stats, err := testCleaner(store, cluster, namespaces, 60*24*time.Hour, now).RunOnce(context.Background())
			if err != nil {
				t.Fatalf("RunOnce: %v", err)
			}
			if got := store.deletes["kept"]; got != tc.retired {
				t.Fatalf("storage deleted %d times, want %d", got, tc.retired)
			}
			if stats.RunsCleaned != tc.retired {
				t.Fatalf("RunsCleaned = %d, want %d", stats.RunsCleaned, tc.retired)
			}
			if _, marked := store.states["kept"]; marked != (tc.retired == 1) {
				t.Fatalf("marked retired = %v, want %v", marked, tc.retired == 1)
			}
		})
	}
}

// TestRunOnce_NeverRetiresAMissingExecutionItCannotDate: with no execution,
// the archive body is the only record of when the run finished. A body that
// is missing or unreadable dates nothing, so the run is kept.
func TestRunOnce_NeverRetiresAMissingExecutionItCannotDate(t *testing.T) {
	now := time.Date(2026, 11, 12, 12, 0, 0, 0, time.UTC)
	for _, tc := range []struct {
		name      string
		noBody    bool
		finishErr error
		finished  time.Time
	}{
		{name: "no body", noBody: true},
		{name: "the store does not answer", finishErr: errInjected},
		{name: "the body records no finish", finished: time.Time{}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			store := newFakeArchive(artifacts.RunIndexEntry{RunID: "ork-1", Namespace: "default", Time: now.Add(-100 * time.Hour)})
			store.noBody = map[string]bool{"ork-1": tc.noBody}
			store.finishErr = map[string]error{"ork-1": tc.finishErr}
			if !tc.noBody && tc.finishErr == nil {
				store.finished = map[string]time.Time{"ork-1": tc.finished}
			}
			cluster := newFakeCluster()
			stats, _ := testCleaner(store, cluster, nil, 72*time.Hour, now).RunOnce(context.Background())
			if store.deletes["ork-1"] != 0 || stats.RunsCleaned != 0 {
				t.Fatalf("a run with no execution and no finish time was retired: stats=%+v", stats)
			}
			if _, marked := store.states["ork-1"]; marked {
				t.Fatalf("a run with no execution and no finish time was marked retired")
			}
		})
	}
}

// TestRunOnce_HoldsMissingRetirementsWhenTooManyAreMissingEarly: one pass
// that finds more than a tenth of the runs it looked up missing before their
// retention is looking at the wrong cluster, or a damaged one. It retires no
// missing execution at all then, even one already due; at a tenth or fewer
// the due ones are retired as usual.
func TestRunOnce_HoldsMissingRetirementsWhenTooManyAreMissingEarly(t *testing.T) {
	now := time.Date(2026, 11, 12, 12, 0, 0, 0, time.UTC)
	for _, tc := range []struct {
		name    string
		early   int
		retired int
	}{
		{name: "one in ten missing early", early: 1, retired: 9},
		{name: "two in ten missing early", early: 2, retired: 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var entries []artifacts.RunIndexEntry
			finished := map[string]time.Time{}
			for i := 0; i < 10; i++ {
				id := fmt.Sprintf("run-%d", i)
				entries = append(entries, artifacts.RunIndexEntry{RunID: id, Namespace: "default", Time: now.Add(-100 * time.Hour)})
				finished[id] = now.Add(-90 * time.Hour)
				if i < tc.early {
					finished[id] = now.Add(-time.Hour)
				}
			}
			store := newFakeArchive(entries...)
			store.finished = finished
			cluster := newFakeCluster()
			stats, err := testCleaner(store, cluster, nil, 72*time.Hour, now).RunOnce(context.Background())
			if err != nil {
				t.Fatalf("RunOnce: %v", err)
			}
			if stats.RunsCleaned != tc.retired {
				t.Fatalf("RunsCleaned = %d, want %d", stats.RunsCleaned, tc.retired)
			}
			for i := 0; i < tc.early; i++ {
				if store.deletes[fmt.Sprintf("run-%d", i)] != 0 {
					t.Fatalf("run-%d, inside its retention, was retired", i)
				}
			}
		})
	}
}

// TestRunOnce_FoundExecutionsAreNotHeldByMissingOnes: the hold is about
// missing executions only. A run the cluster still holds, closed past its
// retention, is retired whatever the share of missing ones.
func TestRunOnce_FoundExecutionsAreNotHeldByMissingOnes(t *testing.T) {
	now := time.Date(2026, 11, 12, 12, 0, 0, 0, time.UTC)
	old := now.Add(-100 * time.Hour)
	store := newFakeArchive(
		artifacts.RunIndexEntry{RunID: "found", Namespace: "default", Time: old},
		artifacts.RunIndexEntry{RunID: "missing", Namespace: "default", Time: old},
	)
	store.finished = map[string]time.Time{"missing": now.Add(-time.Hour)}
	cluster := newFakeCluster()
	cluster.found["found"] = true
	cluster.closeTime["found"] = old
	stats, err := testCleaner(store, cluster, nil, 72*time.Hour, now).RunOnce(context.Background())
	if err != nil {
		t.Fatalf("RunOnce: %v", err)
	}
	if stats.RunsCleaned != 1 || store.deletes["found"] != 1 || store.deletes["missing"] != 0 {
		t.Fatalf("stats=%+v deletes=%v", stats, store.deletes)
	}
}

// recordingObserver keeps every pass's tally.
type recordingObserver struct{ passes []CleanupStats }

func (r *recordingObserver) ObserveCleanup(_ context.Context, s CleanupStats) {
	r.passes = append(r.passes, s)
}

// TestRunOnce_CountsMissingExecutions: a run kept because its execution went
// missing early, and a due one held behind it, each reach the pass's tally,
// which Run hands to the observer.
func TestRunOnce_CountsMissingExecutions(t *testing.T) {
	now := time.Date(2026, 11, 12, 12, 0, 0, 0, time.UTC)
	old := now.Add(-100 * time.Hour)
	store := newFakeArchive(
		artifacts.RunIndexEntry{RunID: "early", Namespace: "default", Time: old},
		artifacts.RunIndexEntry{RunID: "due", Namespace: "default", Time: old},
	)
	store.finished = map[string]time.Time{"early": now.Add(-time.Hour)}
	stats, err := testCleaner(store, newFakeCluster(), nil, 72*time.Hour, now).RunOnce(context.Background())
	if err != nil {
		t.Fatalf("RunOnce: %v", err)
	}
	want := CleanupStats{RunsScanned: 2, Processed: 2, LookedUp: 2, MissingNotDue: 1, MissingHeld: 1}
	if stats != want {
		t.Fatalf("stats = %+v, want %+v", stats, want)
	}

	observer := &recordingObserver{}
	cleaner := testCleaner(store, newFakeCluster(), nil, 72*time.Hour, now).WithObserver(observer)
	ticks := make(chan time.Time)
	cleaner.ticker = func(time.Duration) (<-chan time.Time, func()) { return ticks, func() {} }
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { cleaner.Run(ctx); close(done) }()
	ticks <- now // received only once the first pass has finished
	ticks <- now
	cancel()
	<-done
	if len(observer.passes) < 2 || observer.passes[0] != want {
		t.Fatalf("observed %+v, want the first pass %+v", observer.passes, want)
	}
}

// TestRun_AFailedPassWaitsForTheTickerBeforeDraining: during an outage every
// lookup fails, and a failed run still fills the batch. Run must not drain
// the index at once against a cluster that does not answer: it waits for the
// next tick, then resumes where the failed pass stopped.
func TestRun_AFailedPassWaitsForTheTickerBeforeDraining(t *testing.T) {
	now := time.Date(2026, 11, 12, 12, 0, 0, 0, time.UTC)
	var entries []artifacts.RunIndexEntry
	cluster := newFakeCluster()
	for i := 0; i < 7; i++ {
		id := fmt.Sprintf("run-%d", i)
		entries = append(entries, artifacts.RunIndexEntry{RunID: id, Namespace: "default", Time: now.Add(time.Duration(-100-i) * time.Hour)})
		cluster.lookupErr[id] = errInjected
	}
	store := newFakeArchive(entries...)
	cluster.retention = 96 * time.Hour
	namespaces := &fakeNamespaces{started: make(chan struct{})}
	cleaner := NewCleaner(CleanupConfig{TemporalNamespace: "temporal-runtime", Interval: time.Hour, RetentionBuffer: 24 * time.Hour, BatchSize: 2},
		store, cluster, namespaces, newTestController(cluster, 72*time.Hour), testLogger()).WithClock(func() time.Time { return now })
	ticks := make(chan time.Time)
	cleaner.ticker = func(time.Duration) (<-chan time.Time, func()) { return ticks, func() {} }

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { cleaner.Run(ctx); close(done) }()
	<-namespaces.started // the first pass
	// Run is one goroutine: after the failed first pass it either starts
	// another pass at once or waits on its ticker, never both.
	select {
	case <-namespaces.started:
		stopRun(cancel, namespaces.started, done)
		t.Fatalf("a failed pass drained at once instead of waiting for the ticker")
	case ticks <- now:
	}
	<-namespaces.started // the ticked pass: Run still makes progress
	stopRun(cancel, namespaces.started, done)

	// The first pass examined the first two runs in key order; the ticked
	// one resumes right after the second.
	keys := make([]string, 0, len(entries))
	for _, entry := range store.entries {
		key, err := artifacts.RunIndexKeyForEntry(entry)
		if err != nil {
			t.Fatal(err)
		}
		keys = append(keys, key)
	}
	sort.Strings(keys)
	if q := store.queries[len(store.queries)-1]; q.StartAfterKey != keys[1] {
		t.Fatalf("the ticked pass resumed after %q, want %q, where the failed one stopped", q.StartAfterKey, keys[1])
	}
}

// TestRun_OneFailingRunDoesNotSlowTheDrain: a pass in which one run failed
// among runs that did not is no outage, so the drain goes on at once.
func TestRun_OneFailingRunDoesNotSlowTheDrain(t *testing.T) {
	now := time.Date(2026, 11, 12, 12, 0, 0, 0, time.UTC)
	var entries []artifacts.RunIndexEntry
	cluster := newFakeCluster()
	for i := 0; i < 7; i++ {
		entries = append(entries, artifacts.RunIndexEntry{RunID: fmt.Sprintf("run-%d", i), Namespace: "default", Time: now.Add(time.Duration(-100-i) * time.Hour)})
	}
	store := newFakeArchive(entries...)
	cluster.lookupErr["run-6"] = errInjected // the first in key order, which is by time
	cluster.retention = 96 * time.Hour
	namespaces := &fakeNamespaces{started: make(chan struct{})}
	cleaner := NewCleaner(CleanupConfig{TemporalNamespace: "temporal-runtime", Interval: time.Hour, RetentionBuffer: 24 * time.Hour, BatchSize: 3},
		store, cluster, namespaces, newTestController(cluster, 72*time.Hour), testLogger()).WithClock(func() time.Time { return now })
	ticks := make(chan time.Time)
	cleaner.ticker = func(time.Duration) (<-chan time.Time, func()) { return ticks, func() {} }

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { cleaner.Run(ctx); close(done) }()
	for pass := 1; pass <= 3; pass++ {
		select {
		case <-namespaces.started:
		case ticks <- now:
			stopRun(cancel, namespaces.started, done)
			t.Fatalf("Run waited on the ticker before pass %d: one failing run slowed the drain", pass)
		}
	}
	ticks <- now // the walk reached the end: Run waits on the ticker
	stopRun(cancel, namespaces.started, done)
	if store.deletes["run-6"] != 0 || len(store.deletes) != 6 {
		t.Fatalf("deletes = %v, want the six runs that did not fail", store.deletes)
	}
}

// stopRun cancels a Run and lets any pass it starts meanwhile through the
// started handshake, until Run has returned.
func stopRun(cancel context.CancelFunc, started <-chan struct{}, done <-chan struct{}) {
	cancel()
	for {
		select {
		case <-started:
		case <-done:
			return
		}
	}
}

// TestRunOnce_HoldOutlastsThePassThatTripsIt: a pass sees one batch, so a
// cluster that reported too many runs missing early in one batch is not
// trusted for the batches after it either. The hold stands until a whole walk
// of the index has had no batch over the share.
func TestRunOnce_HoldOutlastsThePassThatTripsIt(t *testing.T) {
	now := time.Date(2026, 11, 12, 12, 0, 0, 0, time.UTC)
	// Key order is time order within one namespace and orkflow: the two
	// oldest runs come first, and the first is missing early.
	store := newFakeArchive(
		artifacts.RunIndexEntry{RunID: "early", Namespace: "default", Time: now.Add(-104 * time.Hour)},
		artifacts.RunIndexEntry{RunID: "due-1", Namespace: "default", Time: now.Add(-103 * time.Hour)},
		artifacts.RunIndexEntry{RunID: "due-2", Namespace: "default", Time: now.Add(-102 * time.Hour)},
		artifacts.RunIndexEntry{RunID: "due-3", Namespace: "default", Time: now.Add(-101 * time.Hour)},
	)
	store.finished = map[string]time.Time{"early": now.Add(-time.Hour)}
	cleaner := testCleaner(store, newFakeCluster(), nil, 72*time.Hour, now)
	cleaner.config.BatchSize = 2
	ctx := context.Background()

	// walk runs passes from startAfter to the end of the index, as Run's
	// drain does, and sums them.
	walk := func(startAfter string) (total CleanupStats) {
		t.Helper()
		for {
			stats, next, err := cleaner.pass(ctx, startAfter)
			if err != nil {
				t.Fatalf("pass after %q: %v", startAfter, err)
			}
			total.RunsCleaned += stats.RunsCleaned
			total.MissingNotDue += stats.MissingNotDue
			total.MissingHeld += stats.MissingHeld
			if next == "" {
				return total
			}
			startAfter = next
		}
	}

	first, resume, err := cleaner.pass(ctx, "")
	if err != nil || first.MissingHeld != 1 || resume == "" {
		t.Fatalf("first pass = %+v, resume %q, %v; want due-1 held and a drain", first, resume, err)
	}
	if rest := walk(resume); rest.MissingNotDue != 0 || rest.MissingHeld != 2 || rest.RunsCleaned != 0 {
		t.Fatalf("rest of the walk = %+v; want due-2 and due-3 held behind the first batch", rest)
	}
	// The next walk still holds: its first batch is over the share again.
	if again := walk(""); again.RunsCleaned != 0 {
		t.Fatalf("next walk = %+v; the early run is still missing, want nothing retired", again)
	}

	// The early run's execution is found again (the right cluster is back):
	// one whole clean walk lifts the hold, and the walk after it retires.
	cluster := cleaner.cluster.(*fakeCluster)
	cluster.found["early"] = true
	cluster.closeTime["early"] = now.Add(-time.Hour)
	if clean := walk(""); clean.RunsCleaned != 0 || clean.MissingHeld != 3 {
		t.Fatalf("clean walk = %+v; want the hold to stand until it ends", clean)
	}
	retired := walk("").RunsCleaned
	if retired != 3 || store.deletes["early"] != 0 {
		t.Fatalf("retired %d after the hold lifted, deletes %v; want the three due runs", retired, store.deletes)
	}
}

// TestRunOnce_RefusesAPolicyWithNoPositiveMaximum: only a write checks the
// stored retention policy. One read back with no maximum, a zero one or a
// negative one would make every finished run due at once; the pass deletes
// nothing and changes no backstop.
func TestRunOnce_RefusesAPolicyWithNoPositiveMaximum(t *testing.T) {
	now := time.Date(2026, 11, 12, 12, 0, 0, 0, time.UTC)
	for _, tc := range []struct {
		name   string
		policy *orkstackv1.RetentionPolicy
	}{
		{name: "no maximum", policy: &orkstackv1.RetentionPolicy{}},
		{name: "zero", policy: &orkstackv1.RetentionPolicy{MaxRunRetention: durationpb.New(0)}},
		{name: "negative", policy: &orkstackv1.RetentionPolicy{MaxRunRetention: durationpb.New(-time.Hour)}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			store := newFakeArchive(artifacts.RunIndexEntry{RunID: "fresh", Namespace: "default", Time: now.Add(-time.Minute)})
			cluster := newFakeCluster()
			cluster.found["fresh"] = true
			cluster.closeTime["fresh"] = now.Add(-time.Second)
			cluster.retention = 96 * time.Hour
			retention := NewRetentionController(&fakePolicyStore{policy: tc.policy}, cluster, "temporal-runtime", 24*time.Hour, testLogger())
			cleaner := NewCleaner(CleanupConfig{TemporalNamespace: "temporal-runtime", Interval: time.Hour, RetentionBuffer: 24 * time.Hour, BatchSize: 100},
				store, cluster, &fakeNamespaces{}, retention, testLogger()).WithClock(func() time.Time { return now })
			stats, err := cleaner.RunOnce(context.Background())
			if err == nil || stats.Errors != 1 {
				t.Fatalf("RunOnce = %+v, %v; want a refused pass", stats, err)
			}
			if len(store.deletes) != 0 || cluster.deleteCalls["fresh"] != 0 || len(cluster.setRetentionCalls) != 0 {
				t.Fatalf("a pass under no positive maximum acted: deletes=%v executions=%v backstop=%v",
					store.deletes, cluster.deleteCalls, cluster.setRetentionCalls)
			}
		})
	}
}
