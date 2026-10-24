package api_test

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/api"
	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// These tests describe the dashboard's numbers, and every one of them is about
// where a number came from as much as what it is.
//
// The archive here is a REAL artifacts.LocalRunArchive on a temp directory,
// written through the production archiver, so the index keys under test are
// the keys production writes and the scan under test is the scan production
// runs. A map standing in for it would have proved that a map can be counted.

// statsClock is a movable clock, so a TTL can be passed without sleeping.
type statsClock struct {
	mu  sync.Mutex
	now time.Time
}

func newStatsClock(at time.Time) *statsClock { return &statsClock{now: at} }

func (c *statsClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *statsClock) advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
}

// bodyReadPanics is a run archive that cannot be read for bodies.
//
// It is the whole of the keys-only claim. "The aggregate reads no run body" is
// a promise about a code path, and the only way to hold code to it is to make
// the forbidden call impossible to survive.
type bodyReadPanics struct {
	inner artifacts.RunArchive
}

func (b bodyReadPanics) PutRun(ctx context.Context, rec artifacts.RunRecord) error {
	return b.inner.PutRun(ctx, rec)
}

func (b bodyReadPanics) GetRun(context.Context, string) ([]byte, error) {
	panic("GetStats fetched a run body; the aggregate must read index keys only")
}

func (b bodyReadPanics) ListRuns(
	ctx context.Context,
	q artifacts.RunQuery,
) ([]artifacts.RunIndexEntry, error) {
	return b.inner.ListRuns(ctx, q)
}

func (b bodyReadPanics) CleanupState(ctx context.Context, runID string) (artifacts.RunCleanupState, error) {
	return b.inner.CleanupState(ctx, runID)
}

func (b bodyReadPanics) DeleteRunArtifacts(ctx context.Context, namespace, runID string) (artifacts.CleanupResult, error) {
	return b.inner.DeleteRunArtifacts(ctx, namespace, runID)
}

func (b bodyReadPanics) DeleteUnreferencedObjects(ctx context.Context, objectIDs []string) (artifacts.CleanupResult, error) {
	return b.inner.DeleteUnreferencedObjects(ctx, objectIDs)
}

func (b bodyReadPanics) MarkArtifactsCleaned(ctx context.Context, runID string, at time.Time) error {
	return b.inner.MarkArtifactsCleaned(ctx, runID, at)
}

// statsFixture is one service under test, with the two sources it reads.
type statsFixture struct {
	svc     *api.Service
	reader  *fakeReader
	archive artifacts.RunArchive
	clock   *statsClock
	runs    *engine.RunStore
	// seeded numbers the runs this fixture archives, so two runs of one
	// orkflow at one instant do not collide on an index key. It is a counter,
	// not an ordering: nothing here reads meaning out of a run id.
	seeded int
	// corpus is every run this fixture has seeded, so the CLUSTER half of an
	// answer can be derived from the same runs the ARCHIVE half counts.
	//
	// It exists because the two halves are one population (stats.proto,
	// GetStatsResponse) and a double that invented its own durations could
	// not fail when they part company — which is exactly what happened: the
	// counts were over the runs a window created and the percentiles over the
	// runs it closed, and this file archived seven runs, handed the reader ten
	// durations and asserted nothing that related the two.
	corpus []seededRun
}

// seededRun is what the fixture remembers about a run it archived: when it was
// created, and how long it took.
type seededRun struct {
	namespace string
	createdAt time.Time
	took      time.Duration
}

// newStatsFixture wires a service the way `ork` does: a durable archive, a
// cluster-backed reader, a namespace registry, and NOTHING answering from
// process memory.
func newStatsFixture(t *testing.T, at time.Time, namespaces ...string) *statsFixture {
	t.Helper()
	store, err := artifacts.NewLocalRunArchive(t.TempDir())
	if err != nil {
		t.Fatalf("open local run archive: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })

	docs := registry{"default": {Name: "default"}}
	for _, name := range namespaces {
		docs[name] = &orkstackv1.Namespace{Name: name}
	}
	clock := newStatsClock(at)
	reader := &fakeReader{executions: map[string]engine.Executions{}}
	runs := engine.NewRunStore()
	archive := bodyReadPanics{inner: store}
	svc := api.NewService(api.Config{
		Reader:     reader,
		Runs:       runs,
		Archive:    engine.NewRunArchiver(archive),
		Namespaces: docs,
		Starter:    &fakeStarter{},
		Now:        clock.Now,
	})
	return &statsFixture{svc: svc, reader: reader, archive: archive, clock: clock, runs: runs}
}

// archiveRun writes one finished run through the production archiver, which is
// what puts a real index key in the tree. The run took a minute.
func (f *statsFixture) archiveRun(
	t *testing.T,
	namespace, orkflow string,
	status orkstackv1.Status,
	at time.Time,
) string {
	t.Helper()
	return f.archiveRunTaking(t, namespace, orkflow, status, at, time.Minute)
}

// archiveRunTaking is archiveRun with the run's duration named, for a test
// that asserts a percentile. The duration is remembered here as well as
// written to the archive, so the cluster half of the answer can be derived
// from the same run rather than invented beside it.
func (f *statsFixture) archiveRunTaking(
	t *testing.T,
	namespace, orkflow string,
	status orkstackv1.Status,
	at time.Time,
	took time.Duration,
) string {
	t.Helper()
	f.seeded++
	id := fmt.Sprintf("ork-%d-%d", at.UnixNano(), f.seeded)
	run := &orkstackv1.Run{
		Id:          id,
		Namespace:   namespace,
		OrkflowName: orkflow,
		Status:      status,
		CreatedAt:   timestamppb.New(at),
		FinishedAt:  timestamppb.New(at.Add(took)),
	}
	if err := engine.NewRunArchiver(f.archive).Archive(context.Background(), run, nil); err != nil {
		t.Fatalf("archive run %s: %v", id, err)
	}
	f.corpus = append(f.corpus, seededRun{namespace: namespace, createdAt: at, took: took})
	return id
}

// executionsIn is what the cluster's index says about one window, DERIVED FROM
// THE SAME RUNS the archive holds.
//
// It applies the production rule and no other (engine.windowRunDurations): a
// duration is sampled for a run the window CREATED and that has finished. A
// fixture that listed durations by hand could not fail when the two halves of
// an answer stopped being about one set of runs, which is the defect this
// models away.
func (f *statsFixture) executionsIn(
	namespace string,
	since, until time.Time,
	running ...*orkstackv1.Run,
) engine.Executions {
	sampled := []time.Duration{}
	for _, run := range f.corpus {
		if run.namespace != namespace ||
			run.createdAt.Before(since) || !run.createdAt.Before(until) {
			continue
		}
		sampled = append(sampled, run.took)
	}
	sort.Slice(sampled, func(i, j int) bool { return sampled[i] < sampled[j] })
	return engine.Executions{Running: running, Durations: sampled}
}

func (f *statsFixture) stats(
	t *testing.T,
	handler *api.StatsHandler,
	namespace string,
	since, until time.Time,
) *orkstackv1.GetStatsResponse {
	t.Helper()
	req := &orkstackv1.GetStatsRequest{Namespace: namespace}
	if !since.IsZero() {
		req.Since = timestamppb.New(since)
	}
	if !until.IsZero() {
		req.Until = timestamppb.New(until)
	}
	got, err := handler.GetStats(context.Background(), connect.NewRequest(req))
	if err != nil {
		t.Fatalf("GetStats(%s): %v", namespace, err)
	}
	return got.Msg
}

func durations(values ...time.Duration) []time.Duration { return values }

func liveRun(id, namespace, orkflow string, at time.Time) *orkstackv1.Run {
	return &orkstackv1.Run{
		Id:          id,
		Namespace:   namespace,
		OrkflowName: orkflow,
		Status:      orkstackv1.Status_STATUS_RUNNING,
		CreatedAt:   timestamppb.New(at),
	}
}

// TestGetStats_TwoSeededNamespacesReportExactCountsRateAndPercentiles is the
// correctness test.
//
// Two namespaces are seeded with a KNOWN set of runs, both holding an orkflow
// called "deploy" — the same name in both, which is legal and is exactly the
// case a namespace-blind scan would get wrong. Every number below is asserted
// exactly, and each namespace's numbers are asserted to exclude the other's
// entirely rather than merely to differ.
//
// The percentiles are asserted over the SAME runs the counts count. Both
// halves of the answer are derived from one seeded corpus (executionsIn), so a
// build whose two sources drew from two populations fails here — as one did:
// the counts were over the runs the window created, the percentiles over the
// runs it closed, and the two were rendered side by side as one set.
func TestGetStats_TwoSeededNamespacesReportExactCountsRateAndPercentiles(t *testing.T) {
	now := time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC)
	fixture := newStatsFixture(t, now, "alpha", "beta")
	since, until := now.Add(-24*time.Hour), now
	inWindow := now.Add(-3 * time.Hour)

	// alpha: 7 finished runs — 4 succeeded, 2 failed, 1 canceled — taking one
	// to seven minutes, so a percentile over them is a value one of them took.
	minutes := func(n int) time.Duration { return time.Duration(n) * time.Minute }
	for i := 0; i < 3; i++ {
		fixture.archiveRunTaking(t, "alpha", "deploy",
			orkstackv1.Status_STATUS_SUCCEEDED, inWindow, minutes(i+1))
	}
	for i := 0; i < 2; i++ {
		fixture.archiveRunTaking(t, "alpha", "deploy",
			orkstackv1.Status_STATUS_FAILED, inWindow, minutes(i+4))
	}
	fixture.archiveRunTaking(t, "alpha", "nightly",
		orkstackv1.Status_STATUS_SUCCEEDED, inWindow, minutes(6))
	fixture.archiveRunTaking(t, "alpha", "nightly",
		orkstackv1.Status_STATUS_CANCELED, inWindow, minutes(7))
	// One alpha run OUTSIDE the window. It must not be counted: a window that
	// is really a filter over everything is not a window. Nor may it be
	// SAMPLED — it took an hour, which would move both percentiles.
	fixture.archiveRunTaking(t, "alpha", "deploy", orkstackv1.Status_STATUS_FAILED,
		since.Add(-time.Hour), time.Hour)

	// beta: 6 finished runs — 1 succeeded, 5 failed — under the SAME orkflow
	// name alpha uses.
	for i := 0; i < 6; i++ {
		status := orkstackv1.Status_STATUS_FAILED
		if i == 0 {
			status = orkstackv1.Status_STATUS_SUCCEEDED
		}
		fixture.archiveRunTaking(t, "beta", "deploy", status, inWindow, minutes(i+1))
	}

	fixture.reader.executions["alpha"] = fixture.executionsIn("alpha", since, until,
		liveRun("ork-alpha-live-1", "alpha", "deploy", inWindow),
		liveRun("ork-alpha-live-2", "alpha", "nightly", inWindow))
	fixture.reader.executions["beta"] = fixture.executionsIn("beta", since, until,
		liveRun("ork-beta-live-1", "beta", "deploy", inWindow))

	handler := api.NewStatsHandler(fixture.svc, api.DefaultStatsCacheTTL)

	alpha := fixture.stats(t, handler, "alpha", since, until)
	assertCounts(t, "alpha", alpha, map[string]int64{
		"running": 2, "succeeded": 4, "failed": 2, "canceled": 1,
	})
	assertRate(t, "alpha", alpha, 4.0/7.0)
	// Nearest rank over the seven the window created: p50 is the 4th, p95 the
	// 7th. The hour-long run outside the window is in neither.
	assertPercentiles(t, "alpha", alpha, 7, 4*time.Minute, 7*time.Minute)
	assertMostFailing(t, "alpha", alpha, []failure{{"deploy", 2, 5}})
	assertRunning(t, "alpha", alpha, []string{"ork-alpha-live-1", "ork-alpha-live-2"})

	beta := fixture.stats(t, handler, "beta", since, until)
	assertCounts(t, "beta", beta, map[string]int64{
		"running": 1, "succeeded": 1, "failed": 5, "canceled": 0,
	})
	assertRate(t, "beta", beta, 1.0/6.0)
	// Nearest rank over 6 samples: p50 is the 3rd, p95 the 6th.
	assertPercentiles(t, "beta", beta, 6, 3*time.Minute, 6*time.Minute)
	assertMostFailing(t, "beta", beta, []failure{{"deploy", 5, 6}})
	assertRunning(t, "beta", beta, []string{"ork-beta-live-1"})

	// ONE WINDOW, ONE POPULATION, stated as a relation between two numbers a
	// dashboard prints beside each other: the runs the percentiles were taken
	// over are the runs the rate is a rate over. Nothing here can be read off
	// one source alone, which is why the fixture derives both from one corpus.
	for _, answer := range []struct {
		namespace string
		stats     *orkstackv1.GetStatsResponse
	}{{"alpha", alpha}, {"beta", beta}} {
		counts := answer.stats.GetRunsByStatus()
		finished := counts["succeeded"] + counts["failed"] + counts["canceled"]
		if got := answer.stats.GetDurationSampleCount(); got != finished {
			t.Errorf("%s: the percentiles are over %d runs and the success rate over "+
				"%d; one answer is carrying two populations",
				answer.namespace, got, finished)
		}
	}

	// The exclusion, stated as its own claim. Both namespaces hold an orkflow
	// called "deploy"; if the scan were namespace-blind, alpha's deploy row
	// would read 7 failed of 11 and its counts would be the sum of both.
	if got := alpha.GetRunsByStatus()["failed"] + beta.GetRunsByStatus()["failed"]; got != 7 {
		t.Fatalf("the two namespaces report %d failures between them, want 7", got)
	}
	for _, row := range alpha.GetMostFailing() {
		if row.GetTotal() > 5 {
			t.Errorf("alpha's %s row counts %d runs, more than alpha has; it is counting beta's",
				row.GetOrkflowName(), row.GetTotal())
		}
	}
	if alpha.GetDurationSampleCount() == beta.GetDurationSampleCount() {
		t.Errorf("both namespaces sampled %d durations; the window is not scoped",
			alpha.GetDurationSampleCount())
	}
}

// TestGetStats_ACachedAnswerEqualsTheUncachedOne holds the cache to the only
// promise that makes one acceptable here: it is an optimisation, so it cannot
// change an answer.
//
// The comparison is against a recomputation at the SAME instant, so the two
// are equal field for field including computed_at. Anything the cache rounded,
// widened or merged would show up as a difference.
func TestGetStats_ACachedAnswerEqualsTheUncachedOne(t *testing.T) {
	now := time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC)
	fixture := newStatsFixture(t, now, "payments")
	since, until := now.Add(-24*time.Hour), now
	fixture.archiveRun(t, "payments", "charge", orkstackv1.Status_STATUS_SUCCEEDED,
		now.Add(-time.Hour))
	fixture.archiveRun(t, "payments", "charge", orkstackv1.Status_STATUS_FAILED,
		now.Add(-time.Hour))
	fixture.reader.executions["payments"] = engine.Executions{
		Durations: durations(2*time.Second, 4*time.Second),
	}

	cached := api.NewStatsHandler(fixture.svc, 5*time.Second)
	first := fixture.stats(t, cached, "payments", since, until)
	if got := fixture.reader.executionCalls(); got != 1 {
		t.Fatalf("the first answer consulted the cluster %d times, want 1", got)
	}

	second := fixture.stats(t, cached, "payments", since, until)
	if !proto.Equal(first, second) {
		t.Fatalf("a cached answer differs from the one it cached:\n%v\n%v", first, second)
	}
	if got := fixture.reader.executionCalls(); got != 1 {
		t.Fatalf("the cached answer consulted the cluster again (%d calls); "+
			"the cache is not preventing the scan it exists to prevent", got)
	}

	// The same question, at the same instant, with no cache at all.
	uncached := api.NewStatsHandler(fixture.svc, 0)
	fresh := fixture.stats(t, uncached, "payments", since, until)
	if !proto.Equal(first, fresh) {
		t.Fatalf("the cached answer is not the answer:\n cached %v\n fresh  %v", first, fresh)
	}
	if got := fixture.reader.executionCalls(); got != 2 {
		t.Fatalf("the uncached handler served from a cache (%d cluster calls, want 2)", got)
	}

	// Past the TTL the record is read again, and the answer moves with it.
	fixture.archiveRun(t, "payments", "charge", orkstackv1.Status_STATUS_FAILED,
		now.Add(-time.Hour))
	fixture.clock.advance(6 * time.Second)
	later := fixture.stats(t, cached, "payments",
		fixture.clock.Now().Add(-24*time.Hour), fixture.clock.Now())
	if later.GetRunsByStatus()["failed"] != 2 {
		t.Errorf("after the TTL the answer still reports %d failures, want 2; "+
			"the cache outlived its own lifetime",
			later.GetRunsByStatus()["failed"])
	}
	if !later.GetComputedAt().AsTime().After(first.GetComputedAt().AsTime()) {
		t.Errorf("computed_at did not move past the TTL (%s then %s)",
			first.GetComputedAt().AsTime(), later.GetComputedAt().AsTime())
	}
}

// TestGetStats_ACachedAnswerIsNeverAnotherNamespaces guards the one way a
// cache turns into a tenancy failure: a key that leaves out who asked.
func TestGetStats_ACachedAnswerIsNeverAnotherNamespaces(t *testing.T) {
	now := time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC)
	fixture := newStatsFixture(t, now, "alpha", "beta")
	since, until := now.Add(-24*time.Hour), now
	fixture.archiveRun(t, "alpha", "deploy", orkstackv1.Status_STATUS_SUCCEEDED,
		now.Add(-time.Hour))
	fixture.archiveRun(t, "beta", "deploy", orkstackv1.Status_STATUS_FAILED,
		now.Add(-time.Hour))

	handler := api.NewStatsHandler(fixture.svc, time.Minute)
	alpha := fixture.stats(t, handler, "alpha", since, until)
	beta := fixture.stats(t, handler, "beta", since, until)

	if alpha.GetRunsByStatus()["succeeded"] != 1 || alpha.GetRunsByStatus()["failed"] != 0 {
		t.Errorf("alpha reports %v, want one success and no failure", alpha.GetRunsByStatus())
	}
	if beta.GetRunsByStatus()["failed"] != 1 || beta.GetRunsByStatus()["succeeded"] != 0 {
		t.Errorf("beta was served alpha's answer: %v", beta.GetRunsByStatus())
	}
}

// TestGetStats_ADifferentWindowIsADifferentAnswer is the other half of the key:
// a client that pins a window gets the window it pinned, warm cache or not.
func TestGetStats_ADifferentWindowIsADifferentAnswer(t *testing.T) {
	now := time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC)
	fixture := newStatsFixture(t, now, "alpha")
	fixture.archiveRun(t, "alpha", "deploy", orkstackv1.Status_STATUS_SUCCEEDED,
		now.Add(-20*time.Hour))
	fixture.archiveRun(t, "alpha", "deploy", orkstackv1.Status_STATUS_FAILED,
		now.Add(-time.Hour))

	handler := api.NewStatsHandler(fixture.svc, time.Minute)
	day := fixture.stats(t, handler, "alpha", now.Add(-24*time.Hour), now)
	if got := day.GetRunsByStatus()["succeeded"] + day.GetRunsByStatus()["failed"]; got != 2 {
		t.Fatalf("the day window counts %d runs, want 2", got)
	}
	hour := fixture.stats(t, handler, "alpha", now.Add(-2*time.Hour), now)
	if got := hour.GetRunsByStatus()["succeeded"] + hour.GetRunsByStatus()["failed"]; got != 1 {
		t.Fatalf("the two-hour window counts %d runs, want 1; a warm cache answered "+
			"a window nobody asked about", got)
	}
}

// TestGetStats_ReadsIndexKeysAndNeverARunBody is the keys-only guarantee.
//
// The archive under it panics on any body read, so this test does not observe
// a cost — it observes that the forbidden call is not made at all. Without the
// panic the same aggregate would still be computable by reading every body,
// and the dashboard's cost would scale with how much output the tasks wrote.
func TestGetStats_ReadsIndexKeysAndNeverARunBody(t *testing.T) {
	now := time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC)
	fixture := newStatsFixture(t, now, "alpha")
	for i := 0; i < 25; i++ {
		fixture.archiveRun(t, "alpha", "deploy", orkstackv1.Status_STATUS_SUCCEEDED,
			now.Add(-time.Duration(i+1)*time.Minute))
	}
	handler := api.NewStatsHandler(fixture.svc, 0)

	got := fixture.stats(t, handler, "alpha", now.Add(-24*time.Hour), now)
	if got.GetRunsByStatus()["succeeded"] != 25 {
		t.Fatalf("counted %d successes, want 25", got.GetRunsByStatus()["succeeded"])
	}

	// The control: the panic is reachable, so the test above proves an absence
	// rather than a store that cannot panic.
	defer func() {
		if recovered := recover(); recovered == nil {
			t.Fatal("the archive under this test does not panic on a body read, " +
				"so the assertion above proves nothing")
		}
	}()
	_, _ = fixture.archive.GetRun(context.Background(), "ork-anything")
}

// TestGetStats_IgnoresTheRunsThisProcessRemembers is AGENTS.md §7 as a test.
//
// The in-process RunStore is filled with a run neither source knows about. A
// replica that answered from its own memory would count it, and two replicas
// would then report different numbers for the same namespace — which is the
// failure the whole read model exists to prevent.
func TestGetStats_IgnoresTheRunsThisProcessRemembers(t *testing.T) {
	now := time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC)
	fixture := newStatsFixture(t, now, "alpha")
	fixture.archiveRun(t, "alpha", "deploy", orkstackv1.Status_STATUS_SUCCEEDED,
		now.Add(-time.Hour))

	fixture.runs.Save(&orkstackv1.Run{
		Id:          "ork-only-this-process-saw-this",
		Namespace:   "alpha",
		OrkflowName: "deploy",
		Status:      orkstackv1.Status_STATUS_FAILED,
		CreatedAt:   timestamppb.New(now.Add(-time.Hour)),
		FinishedAt:  timestamppb.New(now.Add(-time.Hour)),
	})

	handler := api.NewStatsHandler(fixture.svc, 0)
	got := fixture.stats(t, handler, "alpha", now.Add(-24*time.Hour), now)
	if got.GetRunsByStatus()["failed"] != 0 {
		t.Fatalf("a run only this process remembers was counted (%v); "+
			"a second replica would answer differently", got.GetRunsByStatus())
	}
	if got.GetRunsByStatus()["succeeded"] != 1 {
		t.Fatalf("counted %v, want the one archived success", got.GetRunsByStatus())
	}
}

// TestGetStats_AnUnanswerableRateSaysSoInsteadOfZero. Zero per cent success is
// the most alarming number a dashboard can show, and it is the number an empty
// namespace would produce if "no data" were spelled 0.
func TestGetStats_AnUnanswerableRateSaysSoInsteadOfZero(t *testing.T) {
	now := time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC)
	fixture := newStatsFixture(t, now, "quiet")
	handler := api.NewStatsHandler(fixture.svc, 0)

	got := fixture.stats(t, handler, "quiet", now.Add(-24*time.Hour), now)
	if got.GetSuccessRateDefined() {
		t.Error("an empty namespace reports a defined success rate")
	}
	if got.GetDurationSampleCount() != 0 {
		t.Errorf("duration_sample_count is %d over no runs", got.GetDurationSampleCount())
	}
	if got.GetP50Duration() != nil || got.GetP95Duration() != nil {
		t.Errorf("percentiles over no samples are set (%v, %v); a client would render 0s",
			got.GetP50Duration().AsDuration(), got.GetP95Duration().AsDuration())
	}
	for _, key := range []string{"running", "succeeded", "failed", "canceled"} {
		if _, ok := got.GetRunsByStatus()[key]; !ok {
			t.Errorf("runs_by_status has no %q key; a blank reads as unmeasured, not as zero", key)
		}
	}
}

// TestGetStats_CoverageNamesWhatThePercentilesActuallyCover. A 30-day window
// over a 3-day record answers for 3 days; the honest part is saying so.
func TestGetStats_CoverageNamesWhatThePercentilesActuallyCover(t *testing.T) {
	now := time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC)
	fixture := newStatsFixture(t, now, "alpha")
	handler := api.NewStatsHandler(fixture.svc, 0)

	long := fixture.stats(t, handler, "alpha", now.Add(-30*24*time.Hour), now)
	if got, want := long.GetDurationCoverageSince().AsTime(),
		now.Add(-api.StatsDurationRetention); !got.Equal(want) {
		t.Errorf("a 30-day window claims duration coverage from %s, want %s", got, want)
	}
	short := fixture.stats(t, handler, "alpha", now.Add(-time.Hour), now)
	if got, want := short.GetDurationCoverageSince().AsTime(), now.Add(-time.Hour); !got.Equal(want) {
		t.Errorf("an hour window claims coverage from %s, want its own start %s", got, want)
	}
}

// TestGetStats_AWarmCacheStillRefusesAWindowThatStoppedBeingOne.
//
// A cached answer must not outlive the validity of the question. Pinning
// `since` and leaving `until` unset makes the window's end the clock, so a
// clock that steps BACKWARDS — an NTP correction, which happens — turns a
// perfectly good question into a window that ends before it starts. The
// request is byte-identical, so it has the same cache key, and a cache
// consulted before the request is validated hands back numbers for a window
// that no longer exists.
//
// This is why the order is resolve-and-validate, then look up.
func TestGetStats_AWarmCacheStillRefusesAWindowThatStoppedBeingOne(t *testing.T) {
	now := time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC)
	fixture := newStatsFixture(t, now, "alpha")
	fixture.archiveRun(t, "alpha", "deploy", orkstackv1.Status_STATUS_SUCCEEDED,
		now.Add(-time.Hour))
	handler := api.NewStatsHandler(fixture.svc, time.Hour)

	request := &orkstackv1.GetStatsRequest{
		Namespace: "alpha",
		Since:     timestamppb.New(now.Add(-time.Hour)),
	}
	if _, err := handler.GetStats(context.Background(), connect.NewRequest(request)); err != nil {
		t.Fatalf("the window was refused while it was still a window: %v", err)
	}

	// The clock steps back behind the window's own start.
	fixture.clock.advance(-2 * time.Hour)

	got, err := handler.GetStats(context.Background(), connect.NewRequest(request))
	if err == nil {
		t.Fatalf("a warm cache answered a window that ends before it starts: %v", got.Msg)
	}
	if code := connect.CodeOf(err); code != connect.CodeInvalidArgument {
		t.Errorf("refused with %v, want InvalidArgument", code)
	}
}

// TestGetStats_AnEpochWindowIsNotAnUnsetOne. A key that stored a bound as a
// bare integer cannot tell "not sent" from 1970, and the two resolve to
// different windows.
func TestGetStats_AnEpochWindowIsNotAnUnsetOne(t *testing.T) {
	now := time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC)
	fixture := newStatsFixture(t, now, "alpha")
	fixture.archiveRun(t, "alpha", "deploy", orkstackv1.Status_STATUS_SUCCEEDED,
		now.Add(-time.Hour))
	handler := api.NewStatsHandler(fixture.svc, time.Minute)

	unset := fixture.stats(t, handler, "alpha", time.Time{}, time.Time{})
	if want := now.Add(-api.DefaultStatsWindow); !unset.GetWindowSince().AsTime().Equal(want) {
		t.Fatalf("the unset window starts at %s, want %s", unset.GetWindowSince().AsTime(), want)
	}
	epoch := fixture.stats(t, handler, "alpha", time.Unix(0, 0).UTC(), time.Time{})
	if !epoch.GetWindowSince().AsTime().Equal(time.Unix(0, 0).UTC()) {
		t.Errorf("an explicit epoch window was answered from the unset one's entry: %s",
			epoch.GetWindowSince().AsTime())
	}
}

// TestGetStats_RefusesAWindowWithNoTimeInIt. Answering it with zeros would
// hand back numbers that read exactly like a healthy, idle namespace.
//
// Both spellings of a window are covered, because both can name nothing: a
// pair of instants that do not straddle any time, and a lookback that is zero
// or reaches forwards.
func TestGetStats_RefusesAWindowWithNoTimeInIt(t *testing.T) {
	now := time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC)
	fixture := newStatsFixture(t, now, "alpha")
	handler := api.NewStatsHandler(fixture.svc, 0)

	for _, testCase := range []struct {
		name string
		req  *orkstackv1.GetStatsRequest
	}{
		{"equal instants", &orkstackv1.GetStatsRequest{
			Namespace: "alpha",
			Since:     timestamppb.New(now),
			Until:     timestamppb.New(now),
		}},
		{"reversed instants", &orkstackv1.GetStatsRequest{
			Namespace: "alpha",
			Since:     timestamppb.New(now),
			Until:     timestamppb.New(now.Add(-time.Hour)),
		}},
		{"a lookback of no time at all", &orkstackv1.GetStatsRequest{
			Namespace: "alpha", Lookback: durationpb.New(0),
		}},
		{"a lookback that reaches forwards", &orkstackv1.GetStatsRequest{
			Namespace: "alpha", Lookback: durationpb.New(-time.Hour),
		}},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			got, err := handler.GetStats(context.Background(), connect.NewRequest(testCase.req))
			if err == nil {
				t.Fatalf("a window of %s was answered: %v", testCase.name, got.Msg)
			}
			if code := connect.CodeOf(err); code != connect.CodeInvalidArgument {
				t.Errorf("refused with %v, want InvalidArgument", code)
			}
		})
	}
}

// TestGetStats_AnEmptyWindowMeansTheLastDay, resolved from the clock and
// echoed back, so a client can label its own numbers.
func TestGetStats_AnEmptyWindowMeansTheLastDay(t *testing.T) {
	now := time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC)
	fixture := newStatsFixture(t, now, "alpha")
	fixture.archiveRun(t, "alpha", "deploy", orkstackv1.Status_STATUS_SUCCEEDED,
		now.Add(-2*time.Hour))
	fixture.archiveRun(t, "alpha", "deploy", orkstackv1.Status_STATUS_SUCCEEDED,
		now.Add(-30*time.Hour))
	handler := api.NewStatsHandler(fixture.svc, 0)

	got := fixture.stats(t, handler, "alpha", time.Time{}, time.Time{})
	if !got.GetWindowUntil().AsTime().Equal(now) {
		t.Errorf("window_until is %s, want the moment it was computed %s",
			got.GetWindowUntil().AsTime(), now)
	}
	if want := now.Add(-api.DefaultStatsWindow); !got.GetWindowSince().AsTime().Equal(want) {
		t.Errorf("window_since is %s, want %s", got.GetWindowSince().AsTime(), want)
	}
	if got.GetRunsByStatus()["succeeded"] != 1 {
		t.Errorf("the default window counted %d successes, want the one inside a day",
			got.GetRunsByStatus()["succeeded"])
	}
}

// TestGetStats_RefusesANamespaceNothingRegistered keeps the stats surface on
// the same boundary every other namespace-carrying request passes through.
func TestGetStats_RefusesANamespaceNothingRegistered(t *testing.T) {
	now := time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC)
	fixture := newStatsFixture(t, now, "alpha")
	handler := api.NewStatsHandler(fixture.svc, time.Minute)

	for _, testCase := range []struct {
		name      string
		namespace string
		want      connect.Code
	}{
		{"unregistered", "ghost", connect.CodeNotFound},
		{"not a slug", "Alpha", connect.CodeInvalidArgument},
	} {
		_, err := handler.GetStats(context.Background(),
			connect.NewRequest(&orkstackv1.GetStatsRequest{Namespace: testCase.namespace}))
		if err == nil {
			t.Fatalf("%s namespace %q was answered", testCase.name, testCase.namespace)
		}
		if code := connect.CodeOf(err); code != testCase.want {
			t.Errorf("%s namespace refused with %v, want %v", testCase.name, code, testCase.want)
		}
	}
}

// TestGetStats_ReportsPoolHealthFromTheCluster: the same report ListPools
// gives, read from the cluster rather than from anything stored.
func TestGetStats_ReportsPoolHealthFromTheCluster(t *testing.T) {
	now := time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC)
	store, err := artifacts.NewLocalRunArchive(t.TempDir())
	if err != nil {
		t.Fatalf("open local run archive: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })
	clock := newStatsClock(now)
	reader := &fakeReader{executions: map[string]engine.Executions{}}
	svc := api.NewService(api.Config{
		Reader:  reader,
		Archive: engine.NewRunArchiver(bodyReadPanics{inner: store}),
		Namespaces: registry{"gpu-shop": {
			Name:         "gpu-shop",
			DefaultPool:  "main",
			AllowedPools: []string{"main", "gpu"},
		}},
		Starter:    &fakeStarter{},
		Pools:      livePools{poolQueue(t, "main"): 2, poolQueue(t, "gpu"): 1},
		PoolOrkers: poolOrkers{poolQueue(t, "gpu"): {}},
		Now:        clock.Now,
	})
	handler := api.NewStatsHandler(svc, 0)

	got, err := handler.GetStats(context.Background(),
		connect.NewRequest(&orkstackv1.GetStatsRequest{Namespace: "gpu-shop"}))
	if err != nil {
		t.Fatalf("GetStats: %v", err)
	}
	live := map[string]int32{}
	for _, pool := range got.Msg.GetPools() {
		live[pool.GetName()] = pool.GetLiveOrkerCount()
	}
	if live["main"] != 2 || live["gpu"] != 1 {
		t.Errorf("pool health is %v, want main=2 gpu=1", live)
	}
}

// TestGetStats_SaysWhatIsMissingWhenNoRecordIsWired. An engine with no archive
// could still count live runs, and a success rate over live runs alone is not
// one — so it says what to configure instead of answering.
//
// Both ways of having no archive are covered, and the second is the one that
// bites: engine.NewRunArchiver(nil) is a TYPED nil, so the interface holding
// it is not nil and the assertion succeeds. An implementation that answered
// that with an empty tally would report a namespace with no finished runs —
// which is what a healthy, idle namespace looks like.
func TestGetStats_SaysWhatIsMissingWhenNoRecordIsWired(t *testing.T) {
	now := time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC)
	for _, testCase := range []struct {
		name    string
		archive api.RunArchive
	}{
		{name: "no archive at all"},
		{name: "an archiver over no store", archive: engine.NewRunArchiver(nil)},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			clock := newStatsClock(now)
			svc := api.NewService(api.Config{
				Reader:  &fakeReader{executions: map[string]engine.Executions{}},
				Archive: testCase.archive,
				Starter: &fakeStarter{},
				Now:     clock.Now,
			})
			handler := api.NewStatsHandler(svc, 0)

			got, err := handler.GetStats(context.Background(),
				connect.NewRequest(&orkstackv1.GetStatsRequest{}))
			if err == nil {
				t.Fatalf("an engine with no run archive answered a stats request: %v", got.Msg)
			}
			if code := connect.CodeOf(err); code != connect.CodeFailedPrecondition {
				t.Errorf("refused with %v, want FailedPrecondition", code)
			}
			if !strings.Contains(err.Error(), "ORK_ARTIFACT_BACKEND") {
				t.Errorf("error %q does not name what to configure", err)
			}
		})
	}
}

// TestStatsCacheTTLFromEnv: the knob an operator turns, including off.
func TestStatsCacheTTLFromEnv(t *testing.T) {
	for _, testCase := range []struct {
		name  string
		set   bool
		value string
		want  time.Duration
	}{
		{name: "unset", want: api.DefaultStatsCacheTTL},
		{name: "empty", set: true, want: api.DefaultStatsCacheTTL},
		{name: "a duration", set: true, value: "30s", want: 30 * time.Second},
		{name: "off", set: true, value: "0", want: 0},
		{name: "a typo falls back", set: true, value: "five seconds",
			want: api.DefaultStatsCacheTTL},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			if testCase.set {
				t.Setenv(api.StatsCacheTTLVar, testCase.value)
			}
			if got := api.StatsCacheTTLFromEnv(); got != testCase.want {
				t.Errorf("StatsCacheTTLFromEnv() = %v, want %v", got, testCase.want)
			}
		})
	}
}

// ---- assertions -----------------------------------------------------------

type failure struct {
	orkflow string
	failed  int64
	total   int64
}

func assertCounts(
	t *testing.T,
	namespace string,
	got *orkstackv1.GetStatsResponse,
	want map[string]int64,
) {
	t.Helper()
	for status, count := range want {
		if got.GetRunsByStatus()[status] != count {
			t.Errorf("%s: runs_by_status[%q] = %d, want %d (whole map %v)",
				namespace, status, got.GetRunsByStatus()[status], count, got.GetRunsByStatus())
		}
	}
	if len(got.GetRunsByStatus()) != len(want) {
		t.Errorf("%s: runs_by_status has %d keys (%v), want exactly %d",
			namespace, len(got.GetRunsByStatus()), got.GetRunsByStatus(), len(want))
	}
}

func assertRate(t *testing.T, namespace string, got *orkstackv1.GetStatsResponse, want float64) {
	t.Helper()
	if !got.GetSuccessRateDefined() {
		t.Fatalf("%s: success_rate is undefined over finished runs", namespace)
	}
	if diff := got.GetSuccessRate() - want; diff > 1e-9 || diff < -1e-9 {
		t.Errorf("%s: success_rate = %v, want %v", namespace, got.GetSuccessRate(), want)
	}
}

func assertPercentiles(
	t *testing.T,
	namespace string,
	got *orkstackv1.GetStatsResponse,
	samples int64,
	p50, p95 time.Duration,
) {
	t.Helper()
	if got.GetDurationSampleCount() != samples {
		t.Errorf("%s: duration_sample_count = %d, want %d",
			namespace, got.GetDurationSampleCount(), samples)
	}
	if got.GetP50Duration().AsDuration() != p50 {
		t.Errorf("%s: p50 = %v, want %v", namespace, got.GetP50Duration().AsDuration(), p50)
	}
	if got.GetP95Duration().AsDuration() != p95 {
		t.Errorf("%s: p95 = %v, want %v", namespace, got.GetP95Duration().AsDuration(), p95)
	}
}

func assertMostFailing(
	t *testing.T,
	namespace string,
	got *orkstackv1.GetStatsResponse,
	want []failure,
) {
	t.Helper()
	if len(got.GetMostFailing()) != len(want) {
		t.Fatalf("%s: most_failing has %d rows (%v), want %d",
			namespace, len(got.GetMostFailing()), got.GetMostFailing(), len(want))
	}
	for i, row := range got.GetMostFailing() {
		if row.GetOrkflowName() != want[i].orkflow ||
			row.GetFailed() != want[i].failed ||
			row.GetTotal() != want[i].total {
			t.Errorf("%s: most_failing[%d] = %s %d/%d, want %s %d/%d",
				namespace, i, row.GetOrkflowName(), row.GetFailed(), row.GetTotal(),
				want[i].orkflow, want[i].failed, want[i].total)
		}
	}
}

func assertRunning(
	t *testing.T,
	namespace string,
	got *orkstackv1.GetStatsResponse,
	want []string,
) {
	t.Helper()
	ids := make([]string, 0, len(got.GetRunning()))
	for _, run := range got.GetRunning() {
		ids = append(ids, run.GetId())
	}
	if strings.Join(ids, ",") != strings.Join(want, ",") {
		t.Errorf("%s: running = %v, want %v", namespace, ids, want)
	}
	if int64(len(ids)) != got.GetRunsByStatus()["running"] {
		t.Errorf("%s: runs_by_status[running] = %d but the list holds %d; "+
			"the count and the list are supposed to be the same set",
			namespace, got.GetRunsByStatus()["running"], len(ids))
	}
}
