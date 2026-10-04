package api_test

import (
	"context"
	"errors"
	"fmt"
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

// These tests describe the one stats read that opens run BODIES, and every
// one of them is about the bound on that: which bodies, how many, and what
// happens when one cannot be read. The archive is a real LocalRunArchive
// written through the production archiver, wrapped so a test can count,
// forbid, hide or break individual body reads.

// countingBodies is a run archive that counts body reads, panics on a read of
// any run a test forbids, and can make one body absent or unreadable.
type countingBodies struct {
	inner artifacts.RunArchive

	mu        sync.Mutex
	reads     map[string]int
	forbidden map[string]bool
	absent    map[string]bool
	broken    map[string]error
	// entries, when set, replaces what ListRuns returns, for a population too
	// big to seed.
	entries []artifacts.RunIndexEntry
}

func newCountingBodies(inner artifacts.RunArchive) *countingBodies {
	return &countingBodies{
		inner: inner, reads: map[string]int{}, forbidden: map[string]bool{},
		absent: map[string]bool{}, broken: map[string]error{},
	}
}

func (c *countingBodies) PutRun(ctx context.Context, rec artifacts.RunRecord) error {
	return c.inner.PutRun(ctx, rec)
}

func (c *countingBodies) GetRun(ctx context.Context, runID string) ([]byte, error) {
	c.mu.Lock()
	c.reads[runID]++
	forbidden, absent, broken := c.forbidden[runID], c.absent[runID], c.broken[runID]
	c.mu.Unlock()
	if forbidden {
		panic(fmt.Sprintf("the failure breakdown read the body of %s, which did not fail", runID))
	}
	if absent {
		return nil, fmt.Errorf("body of %s: %w", runID, artifacts.ErrNotFound)
	}
	if broken != nil {
		return nil, broken
	}
	return c.inner.GetRun(ctx, runID)
}

func (c *countingBodies) ListRuns(ctx context.Context, q artifacts.RunQuery) ([]artifacts.RunIndexEntry, error) {
	if c.entries != nil {
		return c.entries, nil
	}
	return c.inner.ListRuns(ctx, q)
}

func (c *countingBodies) CleanupState(ctx context.Context, runID string) (artifacts.RunCleanupState, error) {
	return c.inner.CleanupState(ctx, runID)
}

func (c *countingBodies) DeleteRunArtifacts(ctx context.Context, namespace, runID string) (artifacts.CleanupResult, error) {
	return c.inner.DeleteRunArtifacts(ctx, namespace, runID)
}

func (c *countingBodies) DeleteUnreferencedObjects(ctx context.Context, objectIDs []string) (artifacts.CleanupResult, error) {
	return c.inner.DeleteUnreferencedObjects(ctx, objectIDs)
}

func (c *countingBodies) MarkArtifactsCleaned(ctx context.Context, runID string, at time.Time) error {
	return c.inner.MarkArtifactsCleaned(ctx, runID, at)
}

func (c *countingBodies) totalReads() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	total := 0
	for _, n := range c.reads {
		total += n
	}
	return total
}

// attemptsPanic is an attempt store nobody may read.
type attemptsPanic struct{}

func (attemptsPanic) PutAttempt(context.Context, *orkstackv1.TaskAttempt) error { return nil }
func (attemptsPanic) ListAttempts(context.Context, string, string, artifacts.Item) ([]*orkstackv1.TaskAttempt, error) {
	panic("the failure breakdown read an attempt record without being asked for image or phase")
}

// attemptsBroken is an attempt store that cannot be reached.
type attemptsBroken struct{}

func (attemptsBroken) PutAttempt(context.Context, *orkstackv1.TaskAttempt) error { return nil }
func (attemptsBroken) ListAttempts(context.Context, string, string, artifacts.Item) ([]*orkstackv1.TaskAttempt, error) {
	return nil, errors.New("attempt bucket unreachable")
}

// breakdownFixture is one service reading a real archive through a counting
// wrapper, and whichever attempt store a test wires.
type breakdownFixture struct {
	svc      *api.Service
	reader   *fakeReader
	bodies   *countingBodies
	clock    *statsClock
	runs     *engine.RunStore
	attempts *artifacts.MemoryAttemptStore
	seeded   int
}

type breakdownOption func(*api.Config, *breakdownFixture)

// withAttempts replaces the default in-memory attempt store; nil wires none.
func withAttempts(store artifacts.AttemptStore) breakdownOption {
	return func(cfg *api.Config, f *breakdownFixture) {
		cfg.Attempts = store
		f.attempts = nil
	}
}

func newBreakdownFixture(t *testing.T, at time.Time, namespaces []string, opts ...breakdownOption) *breakdownFixture {
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
	fixture := &breakdownFixture{
		reader:   &fakeReader{executions: map[string]engine.Executions{}},
		bodies:   newCountingBodies(store),
		clock:    newStatsClock(at),
		runs:     engine.NewRunStore(),
		attempts: artifacts.NewMemoryAttemptStore(),
	}
	cfg := api.Config{
		Reader:     fixture.reader,
		Runs:       fixture.runs,
		Archive:    engine.NewRunArchiver(fixture.bodies),
		Attempts:   fixture.attempts,
		Namespaces: docs,
		Starter:    &fakeStarter{},
		Now:        fixture.clock.Now,
	}
	for _, opt := range opts {
		opt(&cfg, fixture)
	}
	fixture.svc = api.NewService(cfg)
	return fixture
}

// taskFailure is one failed task, as a snapshot records it.
type taskFailure struct {
	task     string
	code     string
	category orkstackv1.FailureCategory
	pool     string
	orker    string
}

// archive writes one finished run. A FAILED run's first failure is its
// primary task; a failure with no task id makes a run-level failure.
func (f *breakdownFixture) archive(
	t *testing.T,
	namespace, orkflow string,
	status orkstackv1.Status,
	at time.Time,
	fail taskFailure,
) string {
	t.Helper()
	f.seeded++
	id := fmt.Sprintf("ork-%d-%d", at.UnixNano(), f.seeded)
	run := &orkstackv1.Run{
		Id: id, Namespace: namespace, OrkflowName: orkflow, Status: status,
		CreatedAt: timestamppb.New(at), FinishedAt: timestamppb.New(at.Add(time.Minute)),
	}
	if status == orkstackv1.Status_STATUS_FAILED {
		if fail.task == "" {
			run.Error = &orkstackv1.Error{Code: fail.code, Category: fail.category}
		} else {
			run.Error = &orkstackv1.Error{Code: "RUN_FAILED"}
			run.TaskRuns = []*orkstackv1.TaskRun{{
				TaskId: fail.task, Status: orkstackv1.Status_STATUS_FAILED, Attempt: 1,
				Pool: fail.pool, OrkerId: fail.orker,
				FinishedAt: timestamppb.New(at.Add(time.Minute)),
				Error:      &orkstackv1.Error{Code: fail.code, Category: fail.category},
			}}
		}
	}
	if err := engine.NewRunArchiver(f.bodies.inner).Archive(context.Background(), run, nil); err != nil {
		t.Fatalf("archive run %s: %v", id, err)
	}
	return id
}

// attempt files the record an Orker would have written for attempt 1.
func (f *breakdownFixture) attempt(t *testing.T, runID, task, image, phase string) {
	t.Helper()
	if err := f.attempts.PutAttempt(context.Background(), &orkstackv1.TaskAttempt{
		SchemaVersion: artifacts.AttemptSchemaVersion, RunId: runID, TaskId: task, Attempt: 1,
		Outcome: orkstackv1.AttemptOutcome_ATTEMPT_OUTCOME_FAILED, Phase: phase,
		Image: &orkstackv1.ImageRef{Pinned: image},
	}); err != nil {
		t.Fatalf("put attempt record: %v", err)
	}
}

func breakdownRequest(namespace string, since, until time.Time, dims ...orkstackv1.FailureGroupBy) *orkstackv1.GetFailureBreakdownRequest {
	return &orkstackv1.GetFailureBreakdownRequest{
		Namespace: namespace, Since: timestamppb.New(since), Until: timestamppb.New(until), GroupBy: dims,
	}
}

func (f *breakdownFixture) breakdown(
	t *testing.T,
	handler *api.StatsHandler,
	req *orkstackv1.GetFailureBreakdownRequest,
) *orkstackv1.GetFailureBreakdownResponse {
	t.Helper()
	got, err := handler.GetFailureBreakdown(context.Background(), connect.NewRequest(req))
	if err != nil {
		t.Fatalf("GetFailureBreakdown(%s): %v", req.GetNamespace(), err)
	}
	return got.Msg
}

var (
	pullFailed = taskFailure{task: "ingest", code: "IMAGE_PULL_FAILED",
		category: orkstackv1.FailureCategory_FAILURE_CATEGORY_INFRASTRUCTURE_ERROR, pool: "gpu", orker: "orker-g1"}
	exitNonzero = taskFailure{task: "reconcile", code: "CONTAINER_EXIT_NONZERO",
		category: orkstackv1.FailureCategory_FAILURE_CATEGORY_ACTIVITY_FAILED, pool: "main", orker: "orker-m1"}
	valuesMissing = taskFailure{task: "report", code: "VALUES_MISSING",
		category: orkstackv1.FailureCategory_FAILURE_CATEGORY_CONTRACT_VIOLATION, pool: "main", orker: "orker-m1"}
	startFailed = taskFailure{code: "RUN_START_FAILED",
		category: orkstackv1.FailureCategory_FAILURE_CATEGORY_INFRASTRUCTURE_ERROR}
)

// seedPayments is the research narrative: a registry outage on one pool, a
// bad promotion, a start failure — beside runs that succeeded and were
// canceled, whose bodies must never be opened.
func (f *breakdownFixture) seedPayments(t *testing.T, now time.Time) (failed []string) {
	t.Helper()
	for i := 0; i < 5; i++ {
		failed = append(failed, f.archive(t, "payments", "payments", orkstackv1.Status_STATUS_FAILED,
			now.Add(-time.Duration(i+1)*time.Minute), pullFailed))
	}
	for i := 0; i < 2; i++ {
		failed = append(failed, f.archive(t, "payments", "reconcile", orkstackv1.Status_STATUS_FAILED,
			now.Add(-time.Duration(i+10)*time.Minute), exitNonzero))
	}
	failed = append(failed, f.archive(t, "payments", "nightly", orkstackv1.Status_STATUS_FAILED,
		now.Add(-3*time.Hour), startFailed))
	for i := 0; i < 4; i++ {
		ok := f.archive(t, "payments", "payments", orkstackv1.Status_STATUS_SUCCEEDED,
			now.Add(-time.Duration(i+1)*time.Minute), taskFailure{})
		f.bodies.forbidden[ok] = true
	}
	canceled := f.archive(t, "payments", "reconcile", orkstackv1.Status_STATUS_CANCELED,
		now.Add(-time.Minute), taskFailure{})
	f.bodies.forbidden[canceled] = true
	// Outside the window, and in another namespace: neither is the population.
	f.bodies.forbidden[f.archive(t, "payments", "payments", orkstackv1.Status_STATUS_FAILED,
		now.Add(-48*time.Hour), pullFailed)] = true
	f.bodies.forbidden[f.archive(t, "beta", "payments", orkstackv1.Status_STATUS_FAILED,
		now.Add(-time.Minute), pullFailed)] = true
	return failed
}

func sumRunCount(groups []*orkstackv1.FailureGroup) int64 {
	var sum int64
	for _, group := range groups {
		sum += group.GetRunCount()
	}
	return sum
}

func TestGetFailureBreakdown_ReconcilesWithGetStatsFailedCount(t *testing.T) {
	now := time.Date(2026, 9, 22, 10, 51, 0, 0, time.UTC)
	fixture := newBreakdownFixture(t, now, []string{"payments", "beta"})
	fixture.seedPayments(t, now)
	handler := api.NewStatsHandler(fixture.svc, 0)
	since, until := now.Add(-24*time.Hour), now

	stats, err := handler.GetStats(context.Background(), connect.NewRequest(&orkstackv1.GetStatsRequest{
		Namespace: "payments", Since: timestamppb.New(since), Until: timestamppb.New(until),
	}))
	if err != nil {
		t.Fatalf("GetStats: %v", err)
	}
	got := fixture.breakdown(t, handler, breakdownRequest("payments", since, until))

	failed := stats.Msg.GetRunsByStatus()["failed"]
	if failed != 8 {
		t.Fatalf("GetStats counts %d failed runs, want the 8 seeded", failed)
	}
	if got.GetFailedRunsInWindow() != failed {
		t.Errorf("failed_runs_in_window = %d, GetStats says %d", got.GetFailedRunsInWindow(), failed)
	}
	if sum := sumRunCount(got.GetGroups()); sum != got.GetFailedRunsGrouped() || sum != got.GetFailedRunsScanned() || sum != failed {
		t.Errorf("sum(run_count)=%d grouped=%d scanned=%d, want all %d",
			sum, got.GetFailedRunsGrouped(), got.GetFailedRunsScanned(), failed)
	}
	if len(got.GetGroups()) != 3 || got.GetGroupsTotal() != 3 {
		t.Fatalf("got %d groups, want 3: %v", len(got.GetGroups()), got.GetGroups())
	}
	worst := got.GetGroups()[0]
	if worst.GetOrkflowName() != "payments" || worst.GetTaskId() != "ingest" ||
		worst.GetCode() != "IMAGE_PULL_FAILED" || worst.GetRunCount() != 5 {
		t.Errorf("worst group = %v, want payments/ingest/IMAGE_PULL_FAILED ×5", worst)
	}
	runLevel := got.GetGroups()[2]
	if runLevel.GetTaskId() != "" || runLevel.GetCode() != "RUN_START_FAILED" {
		t.Errorf("the run-level failure grouped as %v", runLevel)
	}
	wantDims := []orkstackv1.FailureGroupBy{dimOrkflowAPI, dimTaskAPI, dimCodeAPI, dimCategoryAPI}
	if fmt.Sprint(got.GetGroupBy()) != fmt.Sprint(wantDims) {
		t.Errorf("group_by echoed %v, want the resolved default %v", got.GetGroupBy(), wantDims)
	}
	if got.GetNamespace() != "payments" || !got.GetWindowSince().AsTime().Equal(since) ||
		!got.GetWindowUntil().AsTime().Equal(until) || !got.GetComputedAt().AsTime().Equal(now) {
		t.Errorf("the question answered is not the one asked: %s %s %s %s", got.GetNamespace(),
			got.GetWindowSince().AsTime(), got.GetWindowUntil().AsTime(), got.GetComputedAt().AsTime())
	}
}

const (
	dimOrkflowAPI  = orkstackv1.FailureGroupBy_FAILURE_GROUP_BY_ORKFLOW
	dimTaskAPI     = orkstackv1.FailureGroupBy_FAILURE_GROUP_BY_TASK
	dimCodeAPI     = orkstackv1.FailureGroupBy_FAILURE_GROUP_BY_CODE
	dimCategoryAPI = orkstackv1.FailureGroupBy_FAILURE_GROUP_BY_CATEGORY
	dimImageAPI    = orkstackv1.FailureGroupBy_FAILURE_GROUP_BY_IMAGE
	dimPhaseAPI    = orkstackv1.FailureGroupBy_FAILURE_GROUP_BY_PHASE
	dimPoolAPI     = orkstackv1.FailureGroupBy_FAILURE_GROUP_BY_POOL
)

func TestGetFailureBreakdown_ReadsOneBodyPerFailedRunAndNoOther(t *testing.T) {
	now := time.Date(2026, 9, 22, 10, 51, 0, 0, time.UTC)
	fixture := newBreakdownFixture(t, now, []string{"payments", "beta"})
	failed := fixture.seedPayments(t, now)
	handler := api.NewStatsHandler(fixture.svc, 0)

	fixture.breakdown(t, handler, breakdownRequest("payments", now.Add(-24*time.Hour), now))
	if got := fixture.bodies.totalReads(); got != len(failed) {
		t.Fatalf("read %d bodies, want exactly the %d failed runs in the window", got, len(failed))
	}
	for _, id := range failed {
		if n := fixture.bodies.reads[id]; n != 1 {
			t.Errorf("run %s body read %d times, want once", id, n)
		}
	}

	// The control: a forbidden body does panic, so the absence above is real.
	defer func() {
		if recover() == nil {
			t.Fatal("a forbidden body read did not panic; the test proves nothing")
		}
	}()
	for id := range fixture.bodies.forbidden {
		_, _ = fixture.bodies.GetRun(context.Background(), id)
	}
}

func TestGetFailureBreakdown_NeverTouchesTheAttemptStoreUnlessImageOrPhaseIsRequested(t *testing.T) {
	now := time.Date(2026, 9, 22, 10, 51, 0, 0, time.UTC)
	fixture := newBreakdownFixture(t, now, []string{"payments", "beta"}, withAttempts(attemptsPanic{}))
	fixture.seedPayments(t, now)
	handler := api.NewStatsHandler(fixture.svc, 0)

	for _, dims := range [][]orkstackv1.FailureGroupBy{
		nil,
		{dimPoolAPI, orkstackv1.FailureGroupBy_FAILURE_GROUP_BY_ORKER, dimCodeAPI},
	} {
		got := fixture.breakdown(t, handler, breakdownRequest("payments", now.Add(-24*time.Hour), now, dims...))
		if got.GetAttemptEvidenceRead() || got.GetAttemptEvidenceMissing() != 0 {
			t.Errorf("group by %v: attempt_evidence_read=%v missing=%d, want false and 0",
				dims, got.GetAttemptEvidenceRead(), got.GetAttemptEvidenceMissing())
		}
	}

	// The control: the store does panic on a read, so the absence above is
	// real. (Called directly: the handler reads from worker goroutines, where
	// a panic ends the test binary rather than this test.)
	defer func() {
		if recover() == nil {
			t.Fatal("the attempt store under this test does not panic; the test proves nothing")
		}
	}()
	_, _ = attemptsPanic{}.ListAttempts(context.Background(), "ork-anything", "task", artifacts.NoItem)
}

func TestGetFailureBreakdown_ReclaimedAttemptRecordsAreCountedNotBlank(t *testing.T) {
	now := time.Date(2026, 9, 22, 10, 51, 0, 0, time.UTC)
	fixture := newBreakdownFixture(t, now, []string{"payments"})
	kept := fixture.archive(t, "payments", "reconcile", orkstackv1.Status_STATUS_FAILED,
		now.Add(-2*time.Minute), exitNonzero)
	fixture.attempt(t, kept, "reconcile", "ghcr.io/acme/reconcile@sha256:ab12", "exit")
	// Its attempt record was reclaimed: nothing is filed for it.
	fixture.archive(t, "payments", "reconcile", orkstackv1.Status_STATUS_FAILED,
		now.Add(-time.Minute), exitNonzero)
	handler := api.NewStatsHandler(fixture.svc, 0)

	got := fixture.breakdown(t, handler, breakdownRequest("payments", now.Add(-time.Hour), now,
		dimTaskAPI, dimImageAPI, dimPhaseAPI))
	if !got.GetAttemptEvidenceRead() || got.GetAttemptEvidenceMissing() != 1 {
		t.Fatalf("attempt_evidence_read=%v missing=%d, want true and 1",
			got.GetAttemptEvidenceRead(), got.GetAttemptEvidenceMissing())
	}
	if len(got.GetGroups()) != 2 {
		t.Fatalf("got %d groups, want the recorded image and the unavailable one: %v",
			len(got.GetGroups()), got.GetGroups())
	}
	var unavailable, known *orkstackv1.FailureGroup
	for _, group := range got.GetGroups() {
		if group.GetImagePinned() == "" {
			unavailable = group
		} else {
			known = group
		}
	}
	if unavailable == nil || unavailable.GetAttemptEvidenceMissing() != 1 || unavailable.GetPhase() != "" {
		t.Errorf("the reclaimed run's group = %v, want empty image and phase with missing 1", unavailable)
	}
	if known == nil || known.GetImagePinned() != "ghcr.io/acme/reconcile@sha256:ab12" ||
		known.GetPhase() != "exit" || known.GetAttemptEvidenceMissing() != 0 {
		t.Errorf("the recorded run's group = %v", known)
	}
}

func TestGetFailureBreakdown_NoAttemptStoreIsFailedPreconditionOnlyWhenImageOrPhaseIsRequested(t *testing.T) {
	now := time.Date(2026, 9, 22, 10, 51, 0, 0, time.UTC)
	fixture := newBreakdownFixture(t, now, []string{"payments", "beta"}, withAttempts(nil))
	fixture.seedPayments(t, now)
	handler := api.NewStatsHandler(fixture.svc, 0)

	if got := fixture.breakdown(t, handler, breakdownRequest("payments", now.Add(-24*time.Hour), now)); len(got.GetGroups()) != 3 {
		t.Fatalf("the default grouping without an attempt store answered %d groups, want 3", len(got.GetGroups()))
	}
	for _, dim := range []orkstackv1.FailureGroupBy{dimImageAPI, dimPhaseAPI} {
		got, err := handler.GetFailureBreakdown(context.Background(),
			connect.NewRequest(breakdownRequest("payments", now.Add(-24*time.Hour), now, dimTaskAPI, dim)))
		if err == nil {
			t.Fatalf("grouping by %v with no attempt store answered %v", dim, got.Msg)
		}
		if connect.CodeOf(err) != connect.CodeFailedPrecondition ||
			!strings.Contains(err.Error(), "task attempt store is not configured") {
			t.Errorf("grouping by %v refused with %v, want FailedPrecondition naming the attempt store", dim, err)
		}
	}
}

func TestGetFailureBreakdown_AnUnreadableBodyFailsTheAnswer(t *testing.T) {
	now := time.Date(2026, 9, 22, 10, 51, 0, 0, time.UTC)
	fixture := newBreakdownFixture(t, now, []string{"payments", "beta"})
	failed := fixture.seedPayments(t, now)
	fixture.bodies.broken[failed[3]] = errors.New("503 SlowDown")
	handler := api.NewStatsHandler(fixture.svc, 0)

	got, err := handler.GetFailureBreakdown(context.Background(),
		connect.NewRequest(breakdownRequest("payments", now.Add(-24*time.Hour), now)))
	if err == nil {
		t.Fatalf("a census with an unreadable body answered: %v", got.Msg)
	}
	if connect.CodeOf(err) != connect.CodeUnavailable || !strings.Contains(err.Error(), failed[3]) {
		t.Errorf("refused with %v, want Unavailable naming %s", err, failed[3])
	}

	// An attempt store that cannot be reached is the same: "could not read"
	// is not "reclaimed".
	broken := newBreakdownFixture(t, now, []string{"payments", "beta"}, withAttempts(attemptsBroken{}))
	broken.seedPayments(t, now)
	_, err = api.NewStatsHandler(broken.svc, 0).GetFailureBreakdown(context.Background(),
		connect.NewRequest(breakdownRequest("payments", now.Add(-24*time.Hour), now, dimImageAPI)))
	if connect.CodeOf(err) != connect.CodeUnavailable {
		t.Errorf("an unreachable attempt store answered %v, want Unavailable", err)
	}
}

func TestGetFailureBreakdown_AnIndexRowWithoutABodyIsScannedShortAndSaysSo(t *testing.T) {
	now := time.Date(2026, 9, 22, 10, 51, 0, 0, time.UTC)
	fixture := newBreakdownFixture(t, now, []string{"payments", "beta"})
	failed := fixture.seedPayments(t, now)
	fixture.bodies.absent[failed[0]] = true
	handler := api.NewStatsHandler(fixture.svc, 0)

	got := fixture.breakdown(t, handler, breakdownRequest("payments", now.Add(-24*time.Hour), now))
	if got.GetFailedRunsInWindow() != int64(len(failed)) || got.GetFailedRunsScanned() != int64(len(failed)-1) {
		t.Fatalf("in_window=%d scanned=%d, want %d and %d", got.GetFailedRunsInWindow(),
			got.GetFailedRunsScanned(), len(failed), len(failed)-1)
	}
	if got.GetFailedRunsGrouped() != got.GetFailedRunsScanned() {
		t.Errorf("grouped=%d, want scanned %d", got.GetFailedRunsGrouped(), got.GetFailedRunsScanned())
	}
}

func TestGetFailureBreakdown_OverBudgetScanIsRefusedNotTruncated(t *testing.T) {
	now := time.Date(2026, 9, 22, 10, 51, 0, 0, time.UTC)
	fixture := newBreakdownFixture(t, now, []string{"payments"})
	entries := make([]artifacts.RunIndexEntry, api.FailureBreakdownMaxScan+1)
	for i := range entries {
		entries[i] = artifacts.RunIndexEntry{RunID: fmt.Sprintf("ork-%d", i), Namespace: "payments",
			Orkflow: "payments", Status: "failed", Time: now.Add(-time.Minute)}
	}
	fixture.bodies.entries = entries
	handler := api.NewStatsHandler(fixture.svc, 0)

	got, err := handler.GetFailureBreakdown(context.Background(),
		connect.NewRequest(breakdownRequest("payments", now.Add(-24*time.Hour), now)))
	if err == nil {
		t.Fatalf("an over-budget scan answered: %d groups", len(got.Msg.GetGroups()))
	}
	if connect.CodeOf(err) != connect.CodeResourceExhausted {
		t.Errorf("refused with %v, want ResourceExhausted", connect.CodeOf(err))
	}
	for _, want := range []string{fmt.Sprint(api.FailureBreakdownMaxScan + 1), fmt.Sprint(api.FailureBreakdownMaxScan), "narrow the window"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error %q does not say %q", err, want)
		}
	}
	if n := fixture.bodies.totalReads(); n != 0 {
		t.Errorf("an over-budget scan read %d bodies before refusing, want 0", n)
	}
}

func TestGetFailureBreakdown_ACachedAnswerEqualsTheUncachedOne(t *testing.T) {
	now := time.Date(2026, 9, 22, 10, 51, 0, 0, time.UTC)
	fixture := newBreakdownFixture(t, now, []string{"payments", "beta"})
	failed := fixture.seedPayments(t, now)
	since, until := now.Add(-24*time.Hour), now

	cached := api.NewStatsHandler(fixture.svc, 5*time.Second)
	first := fixture.breakdown(t, cached, breakdownRequest("payments", since, until))
	second := fixture.breakdown(t, cached, breakdownRequest("payments", since, until))
	if !proto.Equal(first, second) {
		t.Fatalf("a cached answer differs from the one it cached:\n%v\n%v", first, second)
	}
	if got := fixture.bodies.totalReads(); got != len(failed) {
		t.Fatalf("the cached answer read bodies again (%d reads, want %d)", got, len(failed))
	}

	fresh := fixture.breakdown(t, api.NewStatsHandler(fixture.svc, 0), breakdownRequest("payments", since, until))
	if !proto.Equal(first, fresh) {
		t.Fatalf("the cached answer is not the answer:\n cached %v\n fresh  %v", first, fresh)
	}

	// Editing what the cache handed out must not edit what it hands out next.
	second.Groups[0].RunCount = 999
	if third := fixture.breakdown(t, cached, breakdownRequest("payments", since, until)); !proto.Equal(first, third) {
		t.Fatalf("a caller's edit reached the cache")
	}

	// Past the TTL the record is read again, and the answer moves with it.
	fixture.archive(t, "payments", "reconcile", orkstackv1.Status_STATUS_FAILED, now.Add(-time.Minute), exitNonzero)
	fixture.clock.advance(6 * time.Second)
	later := fixture.breakdown(t, cached, breakdownRequest("payments", since, until))
	if later.GetFailedRunsInWindow() != first.GetFailedRunsInWindow()+1 {
		t.Errorf("after the TTL the answer counts %d failures, want %d",
			later.GetFailedRunsInWindow(), first.GetFailedRunsInWindow()+1)
	}
}

func TestGetFailureBreakdown_ADifferentGroupByOrCategoryOrLimitIsADifferentAnswer(t *testing.T) {
	now := time.Date(2026, 9, 22, 10, 51, 0, 0, time.UTC)
	fixture := newBreakdownFixture(t, now, []string{"payments", "beta"})
	fixture.seedPayments(t, now)
	handler := api.NewStatsHandler(fixture.svc, time.Hour)
	since, until := now.Add(-24*time.Hour), now

	base := fixture.breakdown(t, handler, breakdownRequest("payments", since, until))
	if len(base.GetGroups()) != 3 {
		t.Fatalf("base answer has %d groups, want 3", len(base.GetGroups()))
	}

	byPool := fixture.breakdown(t, handler, breakdownRequest("payments", since, until, dimPoolAPI))
	if fmt.Sprint(byPool.GetGroupBy()) != fmt.Sprint([]orkstackv1.FailureGroupBy{dimPoolAPI}) ||
		byPool.GetGroups()[0].GetPool() != "gpu" {
		t.Errorf("a pool grouping was answered from the default one's entry: %v", byPool)
	}

	reordered := fixture.breakdown(t, handler, breakdownRequest("payments", since, until,
		dimCategoryAPI, dimCodeAPI, dimTaskAPI, dimOrkflowAPI))
	if fmt.Sprint(reordered.GetGroupBy()) == fmt.Sprint(base.GetGroupBy()) {
		t.Errorf("a reordered grouping echoed the default order %v", reordered.GetGroupBy())
	}

	categoryReq := breakdownRequest("payments", since, until)
	categoryReq.Category = orkstackv1.FailureCategory_FAILURE_CATEGORY_ACTIVITY_FAILED
	byCategory := fixture.breakdown(t, handler, categoryReq)
	if len(byCategory.GetGroups()) != 1 || byCategory.GetFailedRunsGrouped() != 2 {
		t.Errorf("a category filter was answered from the unfiltered entry: %v", byCategory)
	}

	limitReq := breakdownRequest("payments", since, until)
	limitReq.Limit = 1
	limited := fixture.breakdown(t, handler, limitReq)
	if len(limited.GetGroups()) != 1 || limited.GetGroupsTotal() != 3 {
		t.Errorf("a limit of 1 answered %d of %d groups, want 1 of 3", len(limited.GetGroups()), limited.GetGroupsTotal())
	}

	orkflowReq := breakdownRequest("payments", since, until)
	orkflowReq.OrkflowName = "reconcile"
	byOrkflow := fixture.breakdown(t, handler, orkflowReq)
	if byOrkflow.GetFailedRunsInWindow() != 2 {
		t.Errorf("an orkflow filter was answered from the whole namespace's entry: %v", byOrkflow)
	}
}

func TestGetFailureBreakdown_ACachedAnswerIsNeverAnotherNamespaces(t *testing.T) {
	now := time.Date(2026, 9, 22, 10, 51, 0, 0, time.UTC)
	fixture := newBreakdownFixture(t, now, []string{"alpha", "beta"})
	fixture.archive(t, "alpha", "deploy", orkstackv1.Status_STATUS_FAILED, now.Add(-time.Hour), exitNonzero)
	fixture.archive(t, "beta", "deploy", orkstackv1.Status_STATUS_FAILED, now.Add(-time.Hour), pullFailed)
	fixture.archive(t, "beta", "deploy", orkstackv1.Status_STATUS_FAILED, now.Add(-time.Hour), pullFailed)
	handler := api.NewStatsHandler(fixture.svc, time.Hour)
	since, until := now.Add(-24*time.Hour), now

	alpha := fixture.breakdown(t, handler, breakdownRequest("alpha", since, until))
	beta := fixture.breakdown(t, handler, breakdownRequest("beta", since, until))
	if alpha.GetFailedRunsInWindow() != 1 || alpha.GetGroups()[0].GetCode() != "CONTAINER_EXIT_NONZERO" {
		t.Errorf("alpha = %v", alpha)
	}
	if beta.GetFailedRunsInWindow() != 2 || beta.GetGroups()[0].GetCode() != "IMAGE_PULL_FAILED" ||
		beta.GetNamespace() != "beta" {
		t.Errorf("beta was served alpha's answer: %v", beta)
	}
}

func TestGetFailureBreakdown_AWarmCacheStillRefusesAWindowThatStoppedBeingOne(t *testing.T) {
	now := time.Date(2026, 9, 22, 10, 51, 0, 0, time.UTC)
	fixture := newBreakdownFixture(t, now, []string{"alpha"})
	fixture.archive(t, "alpha", "deploy", orkstackv1.Status_STATUS_FAILED, now.Add(-30*time.Minute), exitNonzero)
	handler := api.NewStatsHandler(fixture.svc, time.Hour)
	request := &orkstackv1.GetFailureBreakdownRequest{Namespace: "alpha", Since: timestamppb.New(now.Add(-time.Hour))}
	if _, err := handler.GetFailureBreakdown(context.Background(), connect.NewRequest(request)); err != nil {
		t.Fatalf("the window was refused while it was still a window: %v", err)
	}
	fixture.clock.advance(-2 * time.Hour)
	got, err := handler.GetFailureBreakdown(context.Background(), connect.NewRequest(request))
	if err == nil {
		t.Fatalf("a warm cache answered a window that ends before it starts: %v", got.Msg)
	}
	if connect.CodeOf(err) != connect.CodeInvalidArgument {
		t.Errorf("refused with %v, want InvalidArgument", connect.CodeOf(err))
	}
}

func TestGetFailureBreakdown_RefusesAWindowWithNoTimeInIt(t *testing.T) {
	now := time.Date(2026, 9, 22, 10, 51, 0, 0, time.UTC)
	fixture := newBreakdownFixture(t, now, []string{"alpha"})
	handler := api.NewStatsHandler(fixture.svc, 0)
	for name, req := range map[string]*orkstackv1.GetFailureBreakdownRequest{
		"equal instants":     {Namespace: "alpha", Since: timestamppb.New(now), Until: timestamppb.New(now)},
		"reversed instants":  {Namespace: "alpha", Since: timestamppb.New(now), Until: timestamppb.New(now.Add(-time.Hour))},
		"a zero lookback":    {Namespace: "alpha", Lookback: durationpb.New(0)},
		"a forward lookback": {Namespace: "alpha", Lookback: durationpb.New(-time.Hour)},
	} {
		t.Run(name, func(t *testing.T) {
			got, err := handler.GetFailureBreakdown(context.Background(), connect.NewRequest(req))
			if err == nil {
				t.Fatalf("a window of %s was answered: %v", name, got.Msg)
			}
			if connect.CodeOf(err) != connect.CodeInvalidArgument {
				t.Errorf("refused with %v, want InvalidArgument", connect.CodeOf(err))
			}
		})
	}
}

func TestGetFailureBreakdown_RefusesAnUnspecifiedOrDuplicateGroupByAndALimitPastTheCap(t *testing.T) {
	now := time.Date(2026, 9, 22, 10, 51, 0, 0, time.UTC)
	fixture := newBreakdownFixture(t, now, []string{"alpha"})
	handler := api.NewStatsHandler(fixture.svc, 0)
	since, until := now.Add(-time.Hour), now
	pastCap := breakdownRequest("alpha", since, until)
	pastCap.Limit = api.FailureBreakdownMaxLimit + 1
	negative := breakdownRequest("alpha", since, until)
	negative.Limit = -1
	for name, testCase := range map[string]struct {
		req  *orkstackv1.GetFailureBreakdownRequest
		want string
	}{
		"unspecified": {breakdownRequest("alpha", since, until, dimTaskAPI,
			orkstackv1.FailureGroupBy_FAILURE_GROUP_BY_UNSPECIFIED), "FAILURE_GROUP_BY_UNSPECIFIED"},
		"duplicate": {breakdownRequest("alpha", since, until, dimTaskAPI, dimCodeAPI, dimTaskAPI), "FAILURE_GROUP_BY_TASK"},
		"unknown":   {breakdownRequest("alpha", since, until, orkstackv1.FailureGroupBy(42)), "42"},
		"past cap":  {pastCap, fmt.Sprint(api.FailureBreakdownMaxLimit)},
		"negative":  {negative, "-1"},
	} {
		t.Run(name, func(t *testing.T) {
			got, err := handler.GetFailureBreakdown(context.Background(), connect.NewRequest(testCase.req))
			if err == nil {
				t.Fatalf("answered %v", got.Msg)
			}
			if connect.CodeOf(err) != connect.CodeInvalidArgument || !strings.Contains(err.Error(), testCase.want) {
				t.Errorf("refused with %v, want InvalidArgument naming %q", err, testCase.want)
			}
		})
	}
	// Zero is the default, not a refusal.
	if got := fixture.breakdown(t, handler, breakdownRequest("alpha", since, until)); got.GetGroups() == nil && got.GetGroupsTotal() != 0 {
		t.Errorf("limit 0 was not the default: %v", got)
	}
}

func TestGetFailureBreakdown_IgnoresTheRunsThisProcessRemembers(t *testing.T) {
	now := time.Date(2026, 9, 22, 10, 51, 0, 0, time.UTC)
	fixture := newBreakdownFixture(t, now, []string{"alpha"})
	fixture.archive(t, "alpha", "deploy", orkstackv1.Status_STATUS_FAILED, now.Add(-time.Hour), exitNonzero)
	fixture.runs.Save(&orkstackv1.Run{
		Id: "ork-only-this-process-saw-this", Namespace: "alpha", OrkflowName: "deploy",
		Status:    orkstackv1.Status_STATUS_FAILED,
		CreatedAt: timestamppb.New(now.Add(-time.Hour)), FinishedAt: timestamppb.New(now.Add(-time.Hour)),
		Error: &orkstackv1.Error{Code: "ONLY_IN_MEMORY"},
	})
	got := fixture.breakdown(t, api.NewStatsHandler(fixture.svc, 0), breakdownRequest("alpha", now.Add(-24*time.Hour), now))
	if got.GetFailedRunsInWindow() != 1 || len(got.GetGroups()) != 1 || got.GetGroups()[0].GetCode() == "ONLY_IN_MEMORY" {
		t.Fatalf("a run only this process remembers was counted: %v", got)
	}
}

func TestGetFailureBreakdown_OrkflowNarrowsThePopulationNotJustTheGroups(t *testing.T) {
	now := time.Date(2026, 9, 22, 10, 51, 0, 0, time.UTC)
	fixture := newBreakdownFixture(t, now, []string{"payments", "beta"})
	fixture.seedPayments(t, now)
	req := breakdownRequest("payments", now.Add(-24*time.Hour), now)
	req.OrkflowName = "reconcile"
	got := fixture.breakdown(t, api.NewStatsHandler(fixture.svc, 0), req)
	if got.GetFailedRunsInWindow() != 2 || got.GetFailedRunsScanned() != 2 {
		t.Fatalf("in_window=%d scanned=%d, want 2 and 2 — the population, not only the groups",
			got.GetFailedRunsInWindow(), got.GetFailedRunsScanned())
	}
	if got := fixture.bodies.totalReads(); got != 2 {
		t.Fatalf("read %d bodies, want only reconcile's 2", got)
	}
	if len(got.GetGroups()) != 1 || got.GetGroups()[0].GetOrkflowName() != "reconcile" {
		t.Fatalf("groups = %v, want reconcile's one", got.GetGroups())
	}
}

func TestGetStats_WindowResolutionIsSharedWithTheBreakdown(t *testing.T) {
	now := time.Date(2026, 9, 22, 10, 51, 0, 0, time.UTC)
	fixture := newBreakdownFixture(t, now, []string{"alpha"})
	handler := api.NewStatsHandler(fixture.svc, 0)
	pinned := now.Add(-3 * time.Hour)
	for name, window := range map[string]struct {
		since, until *timestamppb.Timestamp
		lookback     *durationpb.Duration
	}{
		"empty":                 {},
		"lookback":              {lookback: durationpb.New(6 * time.Hour)},
		"lookback before until": {lookback: durationpb.New(time.Hour), until: timestamppb.New(pinned)},
		"since only":            {since: timestamppb.New(pinned)},
		"since wins":            {since: timestamppb.New(pinned), lookback: durationpb.New(time.Minute)},
		"both pinned":           {since: timestamppb.New(pinned), until: timestamppb.New(now.Add(-time.Hour))},
	} {
		t.Run(name, func(t *testing.T) {
			stats, err := handler.GetStats(context.Background(), connect.NewRequest(&orkstackv1.GetStatsRequest{
				Namespace: "alpha", Since: window.since, Until: window.until, Lookback: window.lookback,
			}))
			if err != nil {
				t.Fatalf("GetStats: %v", err)
			}
			breakdown := fixture.breakdown(t, handler, &orkstackv1.GetFailureBreakdownRequest{
				Namespace: "alpha", Since: window.since, Until: window.until, Lookback: window.lookback,
			})
			if !proto.Equal(stats.Msg.GetWindowSince(), breakdown.GetWindowSince()) ||
				!proto.Equal(stats.Msg.GetWindowUntil(), breakdown.GetWindowUntil()) {
				t.Errorf("GetStats answered [%s, %s), the breakdown [%s, %s)",
					stats.Msg.GetWindowSince().AsTime(), stats.Msg.GetWindowUntil().AsTime(),
					breakdown.GetWindowSince().AsTime(), breakdown.GetWindowUntil().AsTime())
			}
		})
	}
}

func TestGetFailureBreakdown_SaysWhatIsMissingWhenNoArchiveIsWired(t *testing.T) {
	now := time.Date(2026, 9, 22, 10, 51, 0, 0, time.UTC)
	for name, archive := range map[string]api.RunArchive{
		"no archive at all":         nil,
		"an archiver over no store": engine.NewRunArchiver(nil),
	} {
		t.Run(name, func(t *testing.T) {
			svc := api.NewService(api.Config{
				Reader: &fakeReader{executions: map[string]engine.Executions{}}, Archive: archive,
				Starter: &fakeStarter{}, Now: newStatsClock(now).Now,
			})
			got, err := api.NewStatsHandler(svc, 0).GetFailureBreakdown(context.Background(),
				connect.NewRequest(&orkstackv1.GetFailureBreakdownRequest{}))
			if err == nil {
				t.Fatalf("an engine with no run archive answered: %v", got.Msg)
			}
			if connect.CodeOf(err) != connect.CodeFailedPrecondition || !strings.Contains(err.Error(), "ORK_ARTIFACT_BACKEND") {
				t.Errorf("refused with %v, want FailedPrecondition naming ORK_ARTIFACT_BACKEND", err)
			}
		})
	}
}

func TestGetFailureBreakdown_RefusesANamespaceNothingRegistered(t *testing.T) {
	now := time.Date(2026, 9, 22, 10, 51, 0, 0, time.UTC)
	fixture := newBreakdownFixture(t, now, []string{"alpha"})
	handler := api.NewStatsHandler(fixture.svc, time.Hour)
	_, err := handler.GetFailureBreakdown(context.Background(),
		connect.NewRequest(breakdownRequest("ghost", now.Add(-time.Hour), now)))
	if connect.CodeOf(err) != connect.CodeNotFound {
		t.Fatalf("an unregistered namespace answered %v, want NotFound", err)
	}
}
