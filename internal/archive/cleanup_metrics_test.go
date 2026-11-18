package archive

import (
	"context"
	"fmt"
	"maps"
	"testing"
	"time"

	"google.golang.org/protobuf/types/known/durationpb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// detailOf runs one pass from the start of the index and returns what an
// observer is handed.
func detailOf(t *testing.T, c *Cleaner) CleanupPass {
	t.Helper()
	p, _, _ := c.passDetailed(context.Background(), "")
	return p
}

// TestCleanupPass_TalliesEachRunOnceByItsNamespaceAndOutcome: every run a
// pass examines against the cluster and decides on ends in exactly one
// outcome, counted under the run's own namespace, and the storage a
// retirement reclaimed is counted there too.
func TestCleanupPass_TalliesEachRunOnceByItsNamespaceAndOutcome(t *testing.T) {
	now := time.Date(2026, 11, 17, 12, 0, 0, 0, time.UTC)
	old := now.Add(-100 * time.Hour)
	store := newFakeArchive(
		artifacts.RunIndexEntry{RunID: "retired", Namespace: "alpha", Time: old},
		artifacts.RunIndexEntry{RunID: "delete-fails", Namespace: "alpha", Time: old},
		artifacts.RunIndexEntry{RunID: "no-body", Namespace: "beta", Time: old},
		artifacts.RunIndexEntry{RunID: "running", Namespace: "beta", Time: old},
		artifacts.RunIndexEntry{RunID: "missing-early", Namespace: "beta", Time: old},
		artifacts.RunIndexEntry{RunID: "not-due", Namespace: "beta", Time: old},
		artifacts.RunIndexEntry{RunID: "default-ns", Time: old},
	)
	store.deleteErr["delete-fails"] = errInjected
	store.noBody = map[string]bool{"no-body": true}
	store.finished = map[string]time.Time{"missing-early": now.Add(-time.Hour)}
	cluster := newFakeCluster()
	for id, closed := range map[string]time.Time{
		"retired": old, "delete-fails": old, "no-body": old, "not-due": now.Add(-time.Hour), "default-ns": old,
	} {
		cluster.found[id] = true
		cluster.closeTime[id] = closed
	}
	cluster.found["running"] = true // closeTime left zero

	p := detailOf(t, testCleaner(store, cluster, nil, 72*time.Hour, now))

	want := map[string]CleanupNamespaceTally{
		"alpha": {Runs: map[string]int{CleanupRunRetired: 1, CleanupRunFailed: 1}, Objects: 2, Bytes: 20},
		"beta": {Runs: map[string]int{
			CleanupRunNotArchived: 1, CleanupRunStillRunning: 1, CleanupRunNotFoundNotDue: 1,
		}},
		"default": {Runs: map[string]int{CleanupRunRetired: 1}, Objects: 2, Bytes: 20},
	}
	if len(p.Namespaces) != len(want) {
		t.Fatalf("namespaces = %+v, want %+v", p.Namespaces, want)
	}
	for ns, w := range want {
		got := p.Namespaces[ns]
		if !maps.Equal(got.Runs, w.Runs) || got.Objects != w.Objects || got.Bytes != w.Bytes {
			t.Errorf("namespace %q = %+v, want %+v", ns, got, w)
		}
	}
	for ns, tally := range p.Namespaces {
		for outcome := range tally.Runs {
			if !ValidCleanupRunOutcome(outcome) {
				t.Errorf("namespace %q carries outcome %q outside the closed set", ns, outcome)
			}
		}
	}
}

// TestCleanupPass_CountsEachFailureUnderTheStepThatFailed: every failure a
// pass counts in Errors is also counted under the one step that failed, so
// an operator can tell a store that refuses deletes from a cluster that does
// not answer.
func TestCleanupPass_CountsEachFailureUnderTheStepThatFailed(t *testing.T) {
	now := time.Date(2026, 11, 17, 12, 0, 0, 0, time.UTC)
	old := now.Add(-100 * time.Hour)
	type fakes struct {
		store      *fakeArchive
		cluster    *fakeCluster
		namespaces *fakeNamespaces
		policies   *fakePolicyStore
	}
	for _, tc := range []struct {
		name string
		step string
		set  func(f fakes)
	}{
		{name: "the retention policy cannot be read", step: CleanupStepPolicy,
			set: func(f fakes) { f.policies.getErr = errInjected }},
		{name: "the namespaces cannot be listed", step: CleanupStepNamespaces,
			set: func(f fakes) { f.namespaces.listErr = errInjected }},
		{name: "the backstop cannot be reconciled", step: CleanupStepBackstop,
			set: func(f fakes) { f.cluster.retention, f.cluster.setRetentionErr = time.Hour, errInjected }},
		{name: "the index cannot be listed", step: CleanupStepList,
			set: func(f fakes) { f.store.listErr = errInjected }},
		{name: "a cleanup state cannot be read", step: CleanupStepState,
			set: func(f fakes) { f.store.stateErr = map[string]error{"ork-1": errInjected} }},
		{name: "the cluster does not answer a lookup", step: CleanupStepLookup,
			set: func(f fakes) { f.cluster.lookupErr["ork-1"] = errInjected }},
		{name: "a missing execution cannot be dated", step: CleanupStepLookup,
			set: func(f fakes) { f.store.finishErr = map[string]error{"ork-1": errInjected} }},
		{name: "the archive body cannot be checked", step: CleanupStepArchiveCheck,
			set: func(f fakes) {
				f.cluster.found["ork-1"], f.cluster.closeTime["ork-1"] = true, old
				f.store.hasErr = map[string]error{"ork-1": errInjected}
			}},
		{name: "the execution cannot be retired", step: CleanupStepRetire,
			set: func(f fakes) {
				f.cluster.found["ork-1"], f.cluster.closeTime["ork-1"] = true, old
				f.cluster.deleteErr["ork-1"] = errInjected
			}},
		{name: "a still-running execution is refused", step: CleanupStepRetire,
			set: func(f fakes) { f.cluster.found["ork-1"] = true }},
		{name: "the storage cannot be deleted", step: CleanupStepDelete,
			set: func(f fakes) { f.store.deleteErr["ork-1"] = errInjected }},
		{name: "the run cannot be marked cleaned", step: CleanupStepMark,
			set: func(f fakes) { f.store.markErr["ork-1"] = errInjected }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := fakes{
				store:      newFakeArchive(artifacts.RunIndexEntry{RunID: "ork-1", Namespace: "default", Time: old}),
				cluster:    newFakeCluster(),
				namespaces: &fakeNamespaces{},
				policies:   &fakePolicyStore{policy: &orkstackv1.RetentionPolicy{MaxRunRetention: durationpb.New(72 * time.Hour)}},
			}
			f.cluster.retention = 96 * time.Hour
			tc.set(f)
			retention := NewRetentionController(f.policies, f.cluster, "temporal-runtime", 24*time.Hour, testLogger())
			c := NewCleaner(CleanupConfig{TemporalNamespace: "temporal-runtime", Interval: time.Hour, RetentionBuffer: 24 * time.Hour, BatchSize: 100},
				f.store, f.cluster, f.namespaces, retention, testLogger()).WithFinishReader(f.store).WithClock(func() time.Time { return now })

			p := detailOf(t, c)
			if p.StepErrors[tc.step] != 1 {
				t.Fatalf("step errors = %v, want one under %q", p.StepErrors, tc.step)
			}
			total := 0
			for step, n := range p.StepErrors {
				if !ValidCleanupStep(step) {
					t.Errorf("step %q is outside the closed set", step)
				}
				total += n
			}
			if total != p.Errors {
				t.Fatalf("step errors sum to %d, Errors = %d: every failure is counted under one step", total, p.Errors)
			}
		})
	}
}

// TestCleanupPass_OutcomeSaysWhetherThePassCompletedDrainedOrFailed: a pass
// that reached the end of the index completed; one that stopped at a full
// batch drains on; one that returned an error failed.
func TestCleanupPass_OutcomeSaysWhetherThePassCompletedDrainedOrFailed(t *testing.T) {
	now := time.Date(2026, 11, 17, 12, 0, 0, 0, time.UTC)
	old := now.Add(-100 * time.Hour)
	for _, tc := range []struct {
		name  string
		batch int
		fail  bool
		want  string
	}{
		{name: "the whole index in one batch", batch: 100, want: CleanupPassCompleted},
		{name: "a full batch with more behind it", batch: 1, want: CleanupPassDrain},
		{name: "a pass that failed", batch: 100, fail: true, want: CleanupPassFailed},
	} {
		t.Run(tc.name, func(t *testing.T) {
			store := newFakeArchive(
				artifacts.RunIndexEntry{RunID: "ork-1", Namespace: "default", Time: old},
				artifacts.RunIndexEntry{RunID: "ork-2", Namespace: "default", Time: old},
			)
			if tc.fail {
				store.listErr = errInjected
			}
			c := testCleaner(store, newFakeCluster(), nil, 72*time.Hour, now)
			c.config.BatchSize = tc.batch
			if got := detailOf(t, c).Outcome; got != tc.want {
				t.Fatalf("outcome = %q, want %q", got, tc.want)
			}
		})
	}
}

// TestCleanupPass_RetentionLagIsTheMostOverdueRunAWholeWalkLeft: the lag is
// reported only by the pass that ends a walk of the whole index, drained
// across passes or not, and per namespace it is how far past its own due
// time the most overdue run that walk left unretired is. A run the walk
// retired is no lag, however late.
func TestCleanupPass_RetentionLagIsTheMostOverdueRunAWholeWalkLeft(t *testing.T) {
	now := time.Date(2026, 11, 17, 12, 0, 0, 0, time.UTC)
	old := now.Add(-200 * time.Hour)
	store := newFakeArchive(
		artifacts.RunIndexEntry{RunID: "a-retired", Namespace: "alpha", Time: old},
		artifacts.RunIndexEntry{RunID: "a-no-body", Namespace: "alpha", Time: old},
		artifacts.RunIndexEntry{RunID: "b-failing", Namespace: "beta", Time: old},
		artifacts.RunIndexEntry{RunID: "b-later", Namespace: "beta", Time: old},
	)
	store.noBody = map[string]bool{"a-no-body": true}
	store.deleteErr["b-failing"] = errInjected
	store.deleteErr["b-later"] = errInjected
	cluster := newFakeCluster()
	for id, at := range map[string]time.Time{
		"a-retired": now.Add(-190 * time.Hour), // 118h overdue, but retired
		"a-no-body": now.Add(-82 * time.Hour),  // 10h overdue
		"b-failing": now.Add(-92 * time.Hour),  // 20h overdue
		"b-later":   now.Add(-77 * time.Hour),  // 5h overdue
	} {
		cluster.found[id], cluster.closeTime[id] = true, at
	}
	c := testCleaner(store, cluster, nil, 72*time.Hour, now)
	c.config.BatchSize = 1

	ctx := context.Background()
	var passes []CleanupPass
	for resume := ""; ; {
		p, next, _ := c.passDetailed(ctx, resume)
		passes = append(passes, p)
		if next == "" {
			break
		}
		if len(passes) > 10 {
			t.Fatal("the walk did not end")
		}
		resume = next
	}
	if len(passes) < 2 {
		t.Fatalf("the walk took %d passes, want it drained", len(passes))
	}
	last := passes[len(passes)-1]
	for i, p := range passes[:len(passes)-1] {
		if p.WalkComplete || p.RetentionLag != nil {
			t.Fatalf("pass %d of %d reported a walk's end: %+v", i+1, len(passes), p)
		}
	}
	if !last.WalkComplete {
		t.Fatalf("the last pass did not report the walk's end: %+v", last)
	}
	want := map[string]time.Duration{"alpha": 10 * time.Hour, "beta": 20 * time.Hour}
	if !maps.Equal(last.RetentionLag, want) {
		t.Fatalf("retention lag = %v, want %v", last.RetentionLag, want)
	}

	// The next walk starts afresh: once every run is retired, none lags.
	delete(store.deleteErr, "b-failing")
	delete(store.deleteErr, "b-later")
	store.noBody = nil
	c.config.BatchSize = 100
	again := detailOf(t, c)
	if !again.WalkComplete || len(again.RetentionLag) != 0 {
		t.Fatalf("a walk that retired every due run reported lag: %+v", again)
	}
}

// TestCleanupPass_RunHandsTheObserverEveryPass: Run hands its observer each
// pass, with its outcome and how long it took.
func TestCleanupPass_RunHandsTheObserverEveryPass(t *testing.T) {
	now := time.Date(2026, 11, 17, 12, 0, 0, 0, time.UTC)
	var entries []artifacts.RunIndexEntry
	for i := range 3 {
		entries = append(entries, artifacts.RunIndexEntry{RunID: fmt.Sprintf("ork-%d", i), Namespace: "default", Time: now.Add(-100 * time.Hour)})
	}
	store := newFakeArchive(entries...)
	observer := &recordingObserver{}
	clock := now
	c := testCleaner(store, newFakeCluster(), nil, 72*time.Hour, now).WithObserver(observer)
	c.config.BatchSize = 2
	c.now = func() time.Time { clock = clock.Add(time.Second); return clock }
	ticks := make(chan time.Time)
	c.ticker = func(time.Duration) (<-chan time.Time, func()) { return ticks, func() {} }
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { c.Run(ctx); close(done) }()
	ticks <- now // received only once the drain has ended
	cancel()
	<-done

	if len(observer.details) < 2 {
		t.Fatalf("observer saw %d passes, want the drain and its end", len(observer.details))
	}
	first, second := observer.details[0], observer.details[1]
	if first.Outcome != CleanupPassDrain || second.Outcome != CleanupPassCompleted || !second.WalkComplete {
		t.Fatalf("passes = %+v, %+v; want a drain, then the walk's end", first, second)
	}
	if first.Duration <= 0 {
		t.Fatalf("a pass's duration = %v, want it measured", first.Duration)
	}
}

// TestCleanupPass_AWalkThatCouldNotReadARunsLagIsUnknown: a walk in which a
// run's state or execution could not be read cannot tell whether that run is
// overdue, so it reports its lag as unknown, never as none.
func TestCleanupPass_AWalkThatCouldNotReadARunsLagIsUnknown(t *testing.T) {
	now := time.Date(2026, 11, 17, 12, 0, 0, 0, time.UTC)
	old := now.Add(-100 * time.Hour)
	for _, tc := range []struct {
		name string
		set  func(*fakeArchive, *fakeCluster)
	}{
		{name: "the cluster does not answer", set: func(_ *fakeArchive, c *fakeCluster) { c.lookupErr["ork-1"] = errInjected }},
		{name: "the cleanup state cannot be read", set: func(s *fakeArchive, _ *fakeCluster) {
			s.stateErr = map[string]error{"ork-1": errInjected}
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			store := newFakeArchive(artifacts.RunIndexEntry{RunID: "ork-1", Namespace: "default", Time: old})
			cluster := newFakeCluster()
			tc.set(store, cluster)
			p := detailOf(t, testCleaner(store, cluster, nil, 72*time.Hour, now))
			if !p.WalkComplete || !p.LagUnknown || p.RetentionLag != nil {
				t.Fatalf("pass = %+v; want a complete walk whose lag is unknown", p)
			}
		})
	}
}

// TestCleanupPass_APassStoppedByShutdownIsNotObserved: the engine stopping
// is no failure of cleanup's, so the pass it stopped never reaches the
// observer.
func TestCleanupPass_APassStoppedByShutdownIsNotObserved(t *testing.T) {
	now := time.Date(2026, 11, 17, 12, 0, 0, 0, time.UTC)
	store := newFakeArchive(artifacts.RunIndexEntry{RunID: "ork-1", Namespace: "default", Time: now.Add(-100 * time.Hour)})
	observer := &recordingObserver{}
	c := testCleaner(store, newFakeCluster(), nil, 72*time.Hour, now).WithObserver(observer)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	c.runAndLog(ctx, "")
	if len(observer.details) != 0 {
		t.Fatalf("a pass stopped by shutdown was observed: %+v", observer.details)
	}
}
