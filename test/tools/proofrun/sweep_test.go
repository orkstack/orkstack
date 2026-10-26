package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"
)

// sweepFixture is a runner whose jobs left buckets behind: one file per label,
// holding the bucket it names, as s3bucket.Finish writes it.
type sweepFixture struct {
	r   *runner
	log *strings.Builder
	dir string
}

func newSweepFixture(t *testing.T, buckets map[string]string, sweepable ...string) *sweepFixture {
	t.Helper()
	dir := t.TempDir()
	f := &sweepFixture{log: &strings.Builder{}, dir: dir}
	f.r = &runner{sweep: true, jobDir: dir, log: f.log, sweepable: map[string]bool{}}
	for label, bucket := range buckets {
		if err := os.WriteFile(f.r.bucketFile(label), []byte(bucket+"\n"), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	for _, label := range sweepable {
		f.r.sweepable[label] = true
	}
	return f
}

// files are the hand-off files still in the job directory, by label.
func (f *sweepFixture) files(t *testing.T) []string {
	t.Helper()
	matches, err := filepath.Glob(filepath.Join(f.dir, "*.bucket"))
	if err != nil {
		t.Fatal(err)
	}
	var labels []string
	for _, m := range matches {
		labels = append(labels, strings.TrimSuffix(filepath.Base(m), ".bucket"))
	}
	slices.Sort(labels)
	return labels
}

// recorder is a remover that records what it was asked to remove.
type recorder struct {
	mu      sync.Mutex
	removed []string
	fail    map[string]error
}

func (c *recorder) remove(_ context.Context, bucket string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if err := c.fail[bucket]; err != nil {
		return err
	}
	c.removed = append(c.removed, bucket)
	return nil
}

func (c *recorder) names() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := append([]string(nil), c.removed...)
	slices.Sort(out)
	return out
}

func TestSweepBuckets_RemovesOnlyThePassingProcessesBuckets(t *testing.T) {
	f := newSweepFixture(t, map[string]string{
		"ork-001":    "orkstack-ork-11-100",
		"ork-002":    "orkstack-ork-12-200", // a process whose tests did not all pass
		"orkctl-001": "orkstack-orkctl-13-300",
	}, "ork-001", "orkctl-001")
	var rec recorder

	f.r.sweepBuckets(context.Background(), rec.remove)

	if got, want := rec.names(), []string{"orkstack-ork-11-100", "orkstack-orkctl-13-300"}; !slices.Equal(got, want) {
		t.Fatalf("removed %q, want %q: only a passing process's bucket is the sweep's to remove", got, want)
	}
	if got, want := f.files(t), []string{"ork-002"}; !slices.Equal(got, want) {
		t.Errorf("hand-off files left = %q, want %q: a removed bucket's file goes, a kept one stays", got, want)
	}
	out := f.log.String()
	for _, want := range []string{
		"bucket sweep: kept s3://orkstack-ork-12-200 of ork-002: not every test of it passed",
		"bucket sweep: removed 2 of 2 passing job buckets in ",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("log is missing %q:\n%s", want, out)
		}
	}
}

func TestSweepBuckets_ABucketItCannotRemoveIsLoggedAndLeft(t *testing.T) {
	f := newSweepFixture(t, map[string]string{
		"ork-001": "orkstack-ork-11-100",
		"ork-002": "orkstack-ork-12-200",
		"ork-003": "orkstack-ork-13-300",
	}, "ork-001", "ork-002", "ork-003")
	rec := recorder{fail: map[string]error{"orkstack-ork-12-200": errors.New("store unavailable")}}

	// Sweeping never fails the proof: it returns nothing, and a removal it
	// cannot make is a line and a file that stays.
	f.r.sweepBuckets(context.Background(), rec.remove)

	if got, want := rec.names(), []string{"orkstack-ork-11-100", "orkstack-ork-13-300"}; !slices.Equal(got, want) {
		t.Fatalf("removed %q, want %q: one failure must not stop the others", got, want)
	}
	if got, want := f.files(t), []string{"ork-002"}; !slices.Equal(got, want) {
		t.Errorf("hand-off files left = %q, want %q", got, want)
	}
	out := f.log.String()
	for _, want := range []string{
		"bucket sweep: left s3://orkstack-ork-12-200 of ork-002: store unavailable",
		"bucket sweep: removed 2 of 3 passing job buckets in ",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("log is missing %q:\n%s", want, out)
		}
	}
}

func TestSweepBuckets_AStoppedRunRemovesNothing(t *testing.T) {
	f := newSweepFixture(t, map[string]string{
		"ork-001": "orkstack-ork-11-100",
		"ork-002": "orkstack-ork-12-200",
	}, "ork-001", "ork-002")
	var rec recorder
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	f.r.sweepBuckets(ctx, rec.remove)

	if got := rec.names(); len(got) != 0 {
		t.Fatalf("a stopped run removed %q", got)
	}
	if got, want := f.files(t), []string{"ork-001", "ork-002"}; !slices.Equal(got, want) {
		t.Errorf("hand-off files = %q, want %q: scripts/proof-clean.sh needs them", got, want)
	}
	want := "bucket sweep: skipped (proofrun was stopped); 2 passing job buckets are named in " + f.dir + "/*.bucket"
	if !strings.Contains(f.log.String(), want) {
		t.Errorf("log is missing %q:\n%s", want, f.log.String())
	}
}

func TestSweepBuckets_WithoutASweepNothingIsTouched(t *testing.T) {
	f := newSweepFixture(t, map[string]string{"ork-001": "orkstack-ork-11-100"}, "ork-001")
	f.r.sweep = false
	var rec recorder

	f.r.sweepBuckets(context.Background(), rec.remove)

	if got := rec.names(); len(got) != 0 {
		t.Fatalf("a runner that does not sweep removed %q", got)
	}
	if f.log.Len() != 0 {
		t.Errorf("a runner that does not sweep logged:\n%s", f.log.String())
	}
	if got := f.files(t); len(got) != 1 {
		t.Errorf("hand-off files = %q, want the one that was there", got)
	}
}

// A hand-off file is read from disk, so what it names is checked before it is
// removed: only a test process's own bucket is ever the sweep's to remove.
func TestSweepBuckets_ANameThatIsNotATestProcessesBucketIsNeverRemoved(t *testing.T) {
	f := newSweepFixture(t, map[string]string{
		"ork-001": "orkstack-artifacts", // the development stack's bucket
		"ork-002": "",
	}, "ork-001", "ork-002")
	var rec recorder

	f.r.sweepBuckets(context.Background(), rec.remove)

	if got := rec.names(); len(got) != 0 {
		t.Fatalf("removed %q: a name that is no test process's bucket must never reach the store", got)
	}
	if got, want := f.files(t), []string{"ork-001", "ork-002"}; !slices.Equal(got, want) {
		t.Errorf("hand-off files = %q, want %q", got, want)
	}
	if !strings.Contains(f.log.String(), "bucket sweep: kept ") || !strings.Contains(f.log.String(), "not the name of a bucket a test process makes") {
		t.Errorf("log does not say why they were kept:\n%s", f.log.String())
	}
}

func TestSweepBuckets_RemovesSeveralAtOnceAndNoMoreThanTheLimit(t *testing.T) {
	buckets := map[string]string{}
	var labels []string
	for i := 0; i < 3*bucketSweepers; i++ {
		label := fmt.Sprintf("ork-%03d", i)
		labels = append(labels, label)
		buckets[label] = fmt.Sprintf("orkstack-ork-%d-7", 100+i)
	}
	f := newSweepFixture(t, buckets, labels...)

	// The remover holds every call until bucketSweepers are in flight at once,
	// so the sweep can only finish if it really removes that many together;
	// the peak shows it never removes more.
	var (
		mu       sync.Mutex
		inFlight int
		peak     int
	)
	allIn := make(chan struct{})
	var once sync.Once
	remove := func(ctx context.Context, _ string) error {
		mu.Lock()
		inFlight++
		peak = max(peak, inFlight)
		if inFlight == bucketSweepers {
			once.Do(func() { close(allIn) })
		}
		mu.Unlock()
		select {
		case <-allIn:
		case <-time.After(30 * time.Second):
			return errors.New("the sweep never had its removal limit in flight at once")
		}
		mu.Lock()
		inFlight--
		mu.Unlock()
		return nil
	}

	f.r.sweepBuckets(context.Background(), remove)

	if peak != bucketSweepers {
		t.Errorf("peak removals in flight = %d, want %d", peak, bucketSweepers)
	}
	if got := f.files(t); len(got) != 0 {
		t.Errorf("hand-off files left = %q, want none", got)
	}
}

// passes is what runJob does for a process of label whose one test passed:
// the process names bucket in its hand-off file, the runner notes it
// sweepable and hands it to the sweepers.
func passes(t *testing.T, ctx context.Context, r *runner, label, bucket string) {
	t.Helper()
	if err := os.WriteFile(r.bucketFile(label), []byte(bucket+"\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	c := newCollector()
	c.results[testKey{"p", "TestA"}] = &entry{Outcome: outcomePass}
	r.noteSweepable(job{ID: label, Package: "p", Tests: []string{"TestA"}}, nil, c)
	r.handOff(ctx, label)
}

// eventually polls cond until it holds or a deadline passes.
func eventually(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("%s: not within 30s", what)
		}
		time.Sleep(time.Millisecond)
	}
}

// liveBuckets is a store that counts the buckets alive at once: a process
// makes one when it starts, and remove takes it away once gate is closed.
type liveBuckets struct {
	mu      sync.Mutex
	live    map[string]bool
	peak    int
	removed []string
	gate    chan struct{}
}

func (l *liveBuckets) make(bucket string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.live[bucket] = true
	l.peak = max(l.peak, len(l.live))
}

func (l *liveBuckets) count() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return len(l.live)
}

func (l *liveBuckets) remove(ctx context.Context, bucket string) error {
	select {
	case <-l.gate:
	case <-ctx.Done():
		return ctx.Err()
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	delete(l.live, bucket)
	l.removed = append(l.removed, bucket)
	return nil
}

// A phase never holds more buckets than its slots, the sweep's backlog and the
// removals in flight: a slot whose process ended hands its bucket over before
// it starts the next one, and waits while the backlog is full. With every
// removal held, the slots fill exactly that bound and stop there; released,
// every bucket goes, and the phase's end has nothing left to remove.
func TestSweeper_LiveBucketsNeverExceedTheSlotsTheBacklogAndTheRemovals(t *testing.T) {
	const slots = 3
	bound := slots + bucketSweepBacklog + bucketSweepers
	jobs := 3 * bound
	f := newSweepFixture(t, nil)
	store := &liveBuckets{live: map[string]bool{}, gate: make(chan struct{})}
	ctx := context.Background()
	f.r.startSweeper(ctx, store.remove)

	var (
		mu      sync.Mutex
		started int
	)
	next := make(chan int)
	var wg sync.WaitGroup
	for s := 0; s < slots; s++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range next {
				label, bucket := fmt.Sprintf("ork-%03d", i), fmt.Sprintf("orkstack-ork-%d-7", 100+i)
				mu.Lock()
				started++
				mu.Unlock()
				store.make(bucket)
				passes(t, ctx, f.r, label, bucket)
			}
		}()
	}
	feed := make(chan struct{})
	go func() {
		defer close(feed)
		for i := 0; i < jobs; i++ {
			next <- i
		}
		close(next)
	}()

	// Held removals: the slots stop once the bound is alive, and no process
	// starts beyond it.
	eventually(t, "the slots filling the bound", func() bool { return store.count() >= bound })
	mu.Lock()
	if started != bound {
		t.Errorf("%d processes started with every removal held, want %d (slots + backlog + removals)", started, bound)
	}
	mu.Unlock()

	close(store.gate)
	<-feed
	wg.Wait()
	f.r.sweepBuckets(ctx, store.remove)

	if store.peak != bound {
		t.Errorf("peak live buckets = %d, want %d: never more than the slots, the backlog and the removals", store.peak, bound)
	}
	if got := store.count(); got != 0 {
		t.Errorf("%d buckets alive after the phase, want none", got)
	}
	if got := f.files(t); len(got) != 0 {
		t.Errorf("hand-off files left = %q, want none", got)
	}
	want := fmt.Sprintf("bucket sweep: removed %d of %d passing job buckets in ", jobs, jobs)
	if !strings.Contains(f.log.String(), want) {
		t.Errorf("log is missing %q:\n%s", want, f.log.String())
	}
}

// A passing process's bucket is removed while the phase runs, before the
// phase's end, and that end counts it without removing it again.
func TestSweeper_RemovesAPassingBucketWhileThePhaseRuns(t *testing.T) {
	f := newSweepFixture(t, nil)
	var rec recorder
	ctx := context.Background()
	f.r.startSweeper(ctx, rec.remove)

	passes(t, ctx, f.r, "ork-001", "orkstack-ork-11-100")
	eventually(t, "the bucket removed while the phase runs", func() bool { return len(rec.names()) == 1 })
	eventually(t, "its hand-off file removed", func() bool { return len(f.files(t)) == 0 })

	f.r.sweepBuckets(ctx, rec.remove)

	if got, want := rec.names(), []string{"orkstack-ork-11-100"}; !slices.Equal(got, want) {
		t.Fatalf("removed %q, want %q once", got, want)
	}
	out := f.log.String()
	for _, want := range []string{
		"bucket sweep: 1 passing job buckets were removed while the phase ran",
		"bucket sweep: removed 1 of 1 passing job buckets in ",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("log is missing %q:\n%s", want, out)
		}
	}
}

// A process whose tests did not all pass is never handed to the sweepers: its
// bucket stays for the post-mortem.
func TestSweeper_AFailedProcessesBucketIsNeverHandedOver(t *testing.T) {
	f := newSweepFixture(t, map[string]string{"ork-001": "orkstack-ork-11-100"})
	var rec recorder
	ctx := context.Background()
	f.r.startSweeper(ctx, rec.remove)
	c := newCollector()
	c.results[testKey{"p", "TestA"}] = &entry{Outcome: outcomeFail}
	f.r.noteSweepable(job{ID: "ork-001", Package: "p", Tests: []string{"TestA"}}, nil, c)
	f.r.handOff(ctx, "ork-001")

	f.r.sweepBuckets(ctx, rec.remove)

	if got := rec.names(); len(got) != 0 {
		t.Fatalf("removed %q: a failing process's bucket is kept", got)
	}
	if !strings.Contains(f.log.String(), "bucket sweep: kept s3://orkstack-ork-11-100 of ork-001: not every test of it passed") {
		t.Errorf("log does not say it was kept:\n%s", f.log.String())
	}
}

// A removal that fails while the phase runs is logged once and its file kept:
// the phase's end counts it among the passing buckets and does not try it
// again.
func TestSweeper_ARemovalThatFailedWhileThePhaseRanIsNotTriedAgain(t *testing.T) {
	f := newSweepFixture(t, nil)
	calls := map[string]int{}
	var mu sync.Mutex
	remove := func(_ context.Context, bucket string) error {
		mu.Lock()
		defer mu.Unlock()
		calls[bucket]++
		if bucket == "orkstack-ork-12-200" {
			return errors.New("store unavailable")
		}
		return nil
	}
	ctx := context.Background()
	f.r.startSweeper(ctx, remove)
	passes(t, ctx, f.r, "ork-001", "orkstack-ork-11-100")
	passes(t, ctx, f.r, "ork-002", "orkstack-ork-12-200")

	f.r.sweepBuckets(ctx, remove)

	mu.Lock()
	defer mu.Unlock()
	if calls["orkstack-ork-11-100"] != 1 || calls["orkstack-ork-12-200"] != 1 {
		t.Fatalf("removal calls = %v, want one each", calls)
	}
	if got, want := f.files(t), []string{"ork-002"}; !slices.Equal(got, want) {
		t.Errorf("hand-off files left = %q, want %q", got, want)
	}
	out := f.log.String()
	for _, want := range []string{
		"bucket sweep: left s3://orkstack-ork-12-200 of ork-002: store unavailable",
		"bucket sweep: removed 1 of 2 passing job buckets in ",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("log is missing %q:\n%s", want, out)
		}
	}
	if n := strings.Count(out, "left s3://orkstack-ork-12-200"); n != 1 {
		t.Errorf("the failed removal is logged %d times, want once:\n%s", n, out)
	}
}

// A run that was stopped hands nothing more over and removes nothing more,
// even with its backlog full: its slots never wait on the sweepers, and its
// hand-off files stay for scripts/proof-clean.sh.
func TestSweeper_AStoppedRunNeitherWaitsNorRemoves(t *testing.T) {
	f := newSweepFixture(t, nil)
	var rec recorder
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	f.r.startSweeper(ctx, rec.remove)

	for i := 0; i < bucketSweepBacklog+bucketSweepers+2; i++ {
		passes(t, ctx, f.r, fmt.Sprintf("ork-%03d", i), fmt.Sprintf("orkstack-ork-%d-7", 100+i))
	}
	f.r.sweepBuckets(ctx, rec.remove)

	if got := rec.names(); len(got) != 0 {
		t.Fatalf("a stopped run removed %q", got)
	}
	if got := len(f.files(t)); got != bucketSweepBacklog+bucketSweepers+2 {
		t.Errorf("%d hand-off files left, want every one", got)
	}
	if !strings.Contains(f.log.String(), "bucket sweep: skipped (proofrun was stopped)") {
		t.Errorf("log does not say the sweep was skipped:\n%s", f.log.String())
	}
}

func TestNoteSweepable(t *testing.T) {
	j := job{ID: "ork-001", Package: "p", Tests: []string{"TestA", "TestB"}}
	outcomes := func(a, b string) *collector {
		c := newCollector()
		for name, outcome := range map[string]string{"TestA": a, "TestB": b} {
			if outcome != "" {
				c.results[testKey{"p", name}] = &entry{Package: "p", Test: name, Outcome: outcome}
			}
		}
		return c
	}
	cases := []struct {
		name   string
		runErr error
		local  *collector
		want   bool
	}{
		{"every test passed", nil, outcomes(outcomePass, outcomePass), true},
		{"a skip is not a failure", nil, outcomes(outcomePass, outcomeSkip), true},
		{"a test failed", nil, outcomes(outcomePass, outcomeFail), false},
		{"a quarantined failure is a failure", nil, outcomes(outcomePass, outcomeQuarantinedFail), false},
		{"a test has no outcome", nil, outcomes(outcomePass, outcomeNone), false},
		{"a test was never reported", nil, outcomes(outcomePass, ""), false},
		{"the process failed", errors.New("exit status 1"), outcomes(outcomePass, outcomePass), false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			r := &runner{sweep: true}
			r.noteSweepable(j, tc.runErr, tc.local)
			if got := r.sweepable[jobLabel(j)]; got != tc.want {
				t.Fatalf("sweepable = %v, want %v", got, tc.want)
			}
		})
	}
}

// The attempt a crash re-runs in a fresh process is its own process with its
// own bucket, so it is its own label.
func TestNoteSweepable_ARerunIsItsOwnLabel(t *testing.T) {
	r := &runner{sweep: true}
	c := newCollector()
	c.results[testKey{"p", "TestA"}] = &entry{Outcome: outcomePass}
	r.noteSweepable(job{ID: "ork-001", Package: "p", Tests: []string{"TestA"}, Attempt: 2}, nil, c)
	if !r.sweepable["ork-001.2"] || r.sweepable["ork-001"] {
		t.Fatalf("sweepable = %v, want only ork-001.2", r.sweepable)
	}
}

const bucketSweepSuite = modulePath + "/test/tools/proofrun/testdata/bucketsweep"

// runBucketSweepSuite runs testdata/bucketsweep through `proofrun integration`
// with bucketRemover replaced by a recorder, and returns the exit code, the
// buckets the sweep removed, the markers the process itself left, and the
// hand-off files that remain.
func runBucketSweepSuite(t *testing.T, fail bool) (code int, swept, onTheSlot, files []string) {
	t.Helper()
	quiet(t)
	out := t.TempDir()
	costs := filepath.Join(out, "costs.tsv")
	if err := os.WriteFile(costs, []byte(bucketSweepSuite+"\tTestPasses\t5\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	markers := t.TempDir()
	t.Setenv("BUCKETSWEEP_DIR", markers)
	if fail {
		t.Setenv("BUCKETSWEEP_FAIL", "1")
	}
	var rec recorder
	prev := bucketRemover
	bucketRemover = func() func(context.Context, string) error { return rec.remove }
	t.Cleanup(func() { bucketRemover = prev })

	code = cmdIntegration([]string{"-out", out, "-jobs", "1", "-costs", costs, "-allow", "", "./testdata/bucketsweep"})

	m, _ := filepath.Glob(filepath.Join(markers, "removed-on-the-slot-*"))
	for _, p := range m {
		onTheSlot = append(onTheSlot, filepath.Base(p))
	}
	m, _ = filepath.Glob(filepath.Join(out, "integration", "*.bucket"))
	for _, p := range m {
		files = append(files, filepath.Base(p))
	}
	return code, rec.names(), onTheSlot, files
}

// A proof's passing test process leaves its bucket for the sweep instead of
// removing it on its slot, and the runner removes it once, after the phase.
// A failing one keeps its bucket for the post-mortem, and the sweep never sees
// it.
func TestIntegrationRunner_SweepsPassingProcessesBucketsAfterThePhase(t *testing.T) {
	if testing.Short() {
		t.Skip("compiles a test binary")
	}
	named := regexp.MustCompile(`^orkstack-bucketsweep-[0-9]+-[0-9]+$`)

	t.Run("a pass is swept once and never removed on the slot", func(t *testing.T) {
		code, swept, onTheSlot, files := runBucketSweepSuite(t, false)
		if code != 0 {
			t.Fatalf("exit %d, want 0", code)
		}
		if len(onTheSlot) != 0 {
			t.Errorf("the test process removed its own bucket on its slot: %q", onTheSlot)
		}
		if len(swept) != 1 || !named.MatchString(swept[0]) {
			t.Errorf("swept %q, want the one bucket the process named", swept)
		}
		if len(files) != 0 {
			t.Errorf("hand-off files left after the sweep: %q", files)
		}
	})

	t.Run("a failure keeps its bucket and the sweep never sees it", func(t *testing.T) {
		code, swept, onTheSlot, files := runBucketSweepSuite(t, true)
		if code != 1 {
			t.Fatalf("exit %d, want 1", code)
		}
		if len(swept) != 0 || len(onTheSlot) != 0 || len(files) != 0 {
			t.Errorf("a failing process's bucket was touched: swept %q, on its slot %q, hand-off files %q", swept, onTheSlot, files)
		}
	})
}

// A CI shard sweeps too: it runs the processes of its jobs and removes the
// passing ones' buckets when they have all ended.
func TestIntegrationShard_SweepsPassingProcessesBucketsAfterItsJobs(t *testing.T) {
	if testing.Short() {
		t.Skip("compiles a test binary")
	}
	quiet(t)
	planned := t.TempDir()
	if code := cmdIntegration([]string{"-out", planned, "-plan-only", "-jobs", "1", "-costs", "", "-allow", "", "-quarantine", "",
		"-nightly", "", "-host-speed", "", "./testdata/bucketsweep"}); code != 0 {
		t.Fatalf("plan-only exit %d", code)
	}
	p, err := readPlan(filepath.Join(planned, "integration-plan.json"))
	if err != nil {
		t.Fatal(err)
	}
	stubTree(t, p.Tree)

	markers := t.TempDir()
	t.Setenv("BUCKETSWEEP_DIR", markers)
	var rec recorder
	prev := bucketRemover
	bucketRemover = func() func(context.Context, string) error { return rec.remove }
	t.Cleanup(func() { bucketRemover = prev })

	code, l := runShardOf(t, p, 1, "-bin", filepath.Join(planned, "bin", "test"), "-namespace-prefix", "sweep")
	if code != 0 {
		t.Fatalf("exit %d: %v", code, l.Problems)
	}
	if got := rec.names(); len(got) != 1 || !regexp.MustCompile(`^orkstack-bucketsweep-[0-9]+-[0-9]+$`).MatchString(got[0]) {
		t.Errorf("the shard swept %q, want the one bucket its process named", got)
	}
	if m, _ := filepath.Glob(filepath.Join(markers, "removed-on-the-slot-*")); len(m) != 0 {
		t.Errorf("the test process removed its own bucket on its slot: %q", m)
	}
}
