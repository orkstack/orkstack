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

// These tests describe the flake index: the one stats read that opens EVERY
// finished run's body in a window and one attempt listing per dispatched
// task. Each is about what it counts, what it refuses to count, and the bound
// on what it reads. The archive is a real LocalRunArchive written through the
// production archiver and the attempt store a real MemoryAttemptStore, both
// wrapped so a test can count, forbid, hide or break individual reads.

// flakeAttempts is an attempt store that counts listings, panics on a
// listing for a task a test says was never dispatched, and can hide
// (reclaim) or break one run's records.
type flakeAttempts struct {
	inner *artifacts.MemoryAttemptStore

	mu           sync.Mutex
	listings     map[string]int
	undispatched map[string]bool // "run/task"
	reclaimed    map[string]bool // run id
	broken       map[string]error
}

func newFlakeAttempts() *flakeAttempts {
	return &flakeAttempts{
		inner: artifacts.NewMemoryAttemptStore(), listings: map[string]int{},
		undispatched: map[string]bool{}, reclaimed: map[string]bool{}, broken: map[string]error{},
	}
}

func (f *flakeAttempts) PutAttempt(ctx context.Context, attempt *orkstackv1.TaskAttempt) error {
	return f.inner.PutAttempt(ctx, attempt)
}

func (f *flakeAttempts) ListAttempts(ctx context.Context, runID, taskID string, _ artifacts.Item) ([]*orkstackv1.TaskAttempt, error) {
	key := runID + "/" + taskID
	f.mu.Lock()
	f.listings[key]++
	undispatched, reclaimed, broken := f.undispatched[key], f.reclaimed[runID], f.broken[runID]
	f.mu.Unlock()
	if undispatched {
		panic(fmt.Sprintf("the flake index listed attempts of %s, which was never dispatched", key))
	}
	if broken != nil {
		return nil, broken
	}
	if reclaimed {
		return nil, nil
	}
	return f.inner.ListAttempts(ctx, runID, taskID, artifacts.NoItem)
}

func (f *flakeAttempts) totalListings() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	total := 0
	for _, n := range f.listings {
		total += n
	}
	return total
}

// flakeFixture is a breakdownFixture whose attempt store is a flakeAttempts.
type flakeFixture struct {
	*breakdownFixture
	store *flakeAttempts
}

func newFlakeFixture(t *testing.T, at time.Time, namespaces []string) *flakeFixture {
	t.Helper()
	store := newFlakeAttempts()
	return &flakeFixture{
		breakdownFixture: newBreakdownFixture(t, at, namespaces, withAttempts(store)),
		store:            store,
	}
}

// flakyTask is one task of an archived run: its pool ("" = never
// dispatched), its snapshot attempt, and the records the Orker filed.
type flakyTask struct {
	id      string
	pool    string
	attempt int32
	records []*orkstackv1.TaskAttempt
}

// retried is a task that failed on attempt 1 on orker-a and succeeded on
// attempt 2 on orker-b.
func retried(id string) flakyTask {
	return flakyTask{id: id, pool: "main", attempt: 2, records: []*orkstackv1.TaskAttempt{
		{Attempt: 1, Outcome: orkstackv1.AttemptOutcome_ATTEMPT_OUTCOME_FAILED, OrkerId: "orker-a", Phase: "exit",
			Error: &orkstackv1.Error{Code: "CONTAINER_EXIT_NONZERO", Category: orkstackv1.FailureCategory_FAILURE_CATEGORY_ACTIVITY_FAILED}},
		{Attempt: 2, Outcome: orkstackv1.AttemptOutcome_ATTEMPT_OUTCOME_SUCCEEDED, OrkerId: "orker-b", Phase: "commit"},
	}}
}

// clean is a task that succeeded on its first attempt.
func clean(id string) flakyTask {
	return flakyTask{id: id, pool: "main", attempt: 1, records: []*orkstackv1.TaskAttempt{
		{Attempt: 1, Outcome: orkstackv1.AttemptOutcome_ATTEMPT_OUTCOME_SUCCEEDED, OrkerId: "orker-a", Phase: "commit"},
	}}
}

// unrecorded is a dispatched task whose records were never written.
func unrecorded(id string) flakyTask { return flakyTask{id: id, pool: "main", attempt: 1} }

// notDispatched is a task that never reached an Orker.
func notDispatched(id string) flakyTask { return flakyTask{id: id} }

// run archives one finished run with its tasks and files their records.
func (f *flakeFixture) run(t *testing.T, namespace, orkflow string, status orkstackv1.Status, at time.Time, tasks ...flakyTask) string {
	t.Helper()
	f.seeded++
	id := fmt.Sprintf("ork-%d-%d", at.UnixNano(), f.seeded)
	run := &orkstackv1.Run{
		Id: id, Namespace: namespace, OrkflowName: orkflow, Status: status,
		CreatedAt: timestamppb.New(at), FinishedAt: timestamppb.New(at.Add(time.Minute)),
	}
	for _, task := range tasks {
		taskStatus := orkstackv1.Status_STATUS_SUCCEEDED
		if task.pool == "" {
			taskStatus = orkstackv1.Status_STATUS_SKIPPED
			f.store.undispatched[id+"/"+task.id] = true
		}
		run.TaskRuns = append(run.TaskRuns, &orkstackv1.TaskRun{
			TaskId: task.id, Pool: task.pool, Attempt: task.attempt, Status: taskStatus,
		})
		for _, record := range task.records {
			record := proto.Clone(record).(*orkstackv1.TaskAttempt)
			record.SchemaVersion = artifacts.AttemptSchemaVersion
			record.RunId, record.TaskId = id, task.id
			if err := f.store.PutAttempt(context.Background(), record); err != nil {
				t.Fatalf("put attempt: %v", err)
			}
		}
	}
	if err := engine.NewRunArchiver(f.bodies.inner).Archive(context.Background(), run, nil); err != nil {
		t.Fatalf("archive run %s: %v", id, err)
	}
	return id
}

func flakeRequest(namespace string, since, until time.Time, dims ...orkstackv1.FailureGroupBy) *orkstackv1.GetFlakeIndexRequest {
	return &orkstackv1.GetFlakeIndexRequest{
		Namespace: namespace, Since: timestamppb.New(since), Until: timestamppb.New(until), GroupBy: dims,
	}
}

func (f *flakeFixture) flakes(t *testing.T, handler *api.StatsHandler, req *orkstackv1.GetFlakeIndexRequest) *orkstackv1.GetFlakeIndexResponse {
	t.Helper()
	got, err := handler.GetFlakeIndex(context.Background(), connect.NewRequest(req))
	if err != nil {
		t.Fatalf("GetFlakeIndex(%s): %v", req.GetNamespace(), err)
	}
	return got.Msg
}

func flakeGroup(t *testing.T, resp *orkstackv1.GetFlakeIndexResponse, orkflow, task string) *orkstackv1.FlakeGroup {
	t.Helper()
	for _, group := range resp.GetGroups() {
		if group.GetOrkflowName() == orkflow && group.GetTaskId() == task {
			return group
		}
	}
	t.Fatalf("no group %s/%s in %v", orkflow, task, resp.GetGroups())
	return nil
}

// dimOrkerAPI is the one dimension the breakdown tests never needed.
const dimOrkerAPI = orkstackv1.FailureGroupBy_FAILURE_GROUP_BY_ORKER

var flakeNow = time.Date(2026, 9, 24, 10, 51, 0, 0, time.UTC)

// seedPaymentsFlakes is a window with every finished status: two succeeded
// runs that retried fetch-rates, one clean succeeded run, one failed run, one
// canceled run with a skipped task — beside a run outside the window and one
// in another namespace, neither of which is the population.
func (f *flakeFixture) seedPaymentsFlakes(t *testing.T) (inWindow []string) {
	t.Helper()
	at := func(minutes int) time.Time { return flakeNow.Add(-time.Duration(minutes) * time.Minute) }
	inWindow = append(inWindow,
		f.run(t, "payments", "reconcile", orkstackv1.Status_STATUS_SUCCEEDED, at(1), retried("fetch-rates"), clean("post")),
		f.run(t, "payments", "reconcile", orkstackv1.Status_STATUS_SUCCEEDED, at(2), retried("fetch-rates"), clean("post")),
		f.run(t, "payments", "reconcile", orkstackv1.Status_STATUS_SUCCEEDED, at(3), clean("fetch-rates"), clean("post")),
		f.run(t, "payments", "reconcile", orkstackv1.Status_STATUS_FAILED, at(4), clean("fetch-rates"), notDispatched("post")),
		f.run(t, "payments", "ingest", orkstackv1.Status_STATUS_CANCELED, at(5), clean("normalize"), notDispatched("load")),
	)
	f.bodies.forbidden[f.run(t, "payments", "reconcile", orkstackv1.Status_STATUS_SUCCEEDED,
		flakeNow.Add(-48*time.Hour), retried("fetch-rates"))] = true
	f.bodies.forbidden[f.run(t, "beta", "reconcile", orkstackv1.Status_STATUS_SUCCEEDED,
		at(1), retried("fetch-rates"))] = true
	return inWindow
}

func TestGetFlakeIndex_PopulationIsEveryFinishedRunTheWindowCreated(t *testing.T) {
	fixture := newFlakeFixture(t, flakeNow, []string{"payments", "beta"})
	fixture.seedPaymentsFlakes(t)
	handler := api.NewStatsHandler(fixture.svc, 0)
	since, until := flakeNow.Add(-24*time.Hour), flakeNow

	stats, err := handler.GetStats(context.Background(), connect.NewRequest(&orkstackv1.GetStatsRequest{
		Namespace: "payments", Since: timestamppb.New(since), Until: timestamppb.New(until),
	}))
	if err != nil {
		t.Fatalf("GetStats: %v", err)
	}
	byStatus := stats.Msg.GetRunsByStatus()
	finished := byStatus["succeeded"] + byStatus["failed"] + byStatus["canceled"]
	got := fixture.flakes(t, handler, flakeRequest("payments", since, until))
	if finished != 5 || got.GetRunsInWindow() != finished {
		t.Fatalf("runs_in_window = %d, GetStats finished = %d, want both 5", got.GetRunsInWindow(), finished)
	}
	if got.GetRunsScanned() != 5 || got.GetRunsDispatched() != 5 {
		t.Errorf("scanned=%d dispatched=%d, want 5 and 5", got.GetRunsScanned(), got.GetRunsDispatched())
	}
	if got.GetNamespace() != "payments" || !got.GetBodyKeyed() ||
		fmt.Sprint(got.GetGroupBy()) != fmt.Sprint([]orkstackv1.FailureGroupBy{dimOrkflowAPI, dimTaskAPI}) {
		t.Errorf("echo: namespace=%q body_keyed=%v group_by=%v", got.GetNamespace(), got.GetBodyKeyed(), got.GetGroupBy())
	}
	if got.GetComputedAt() == nil || !got.GetWindowSince().AsTime().Equal(since) || !got.GetWindowUntil().AsTime().Equal(until) {
		t.Errorf("window echo: [%v, %v) computed %v", got.GetWindowSince(), got.GetWindowUntil(), got.GetComputedAt())
	}
}

func TestGetFlakeIndex_ASucceededRunWithARetriedTaskIsCountedRetried(t *testing.T) {
	fixture := newFlakeFixture(t, flakeNow, []string{"payments"})
	three := flakyTask{id: "fetch-rates", pool: "main", attempt: 3, records: []*orkstackv1.TaskAttempt{
		{Attempt: 1, Outcome: orkstackv1.AttemptOutcome_ATTEMPT_OUTCOME_FAILED, Phase: "pull", OrkerId: "orker-a",
			Error: &orkstackv1.Error{Code: "IMAGE_PULL_FAILED", Category: orkstackv1.FailureCategory_FAILURE_CATEGORY_INFRASTRUCTURE_ERROR}},
		{Attempt: 2, Outcome: orkstackv1.AttemptOutcome_ATTEMPT_OUTCOME_FAILED, Phase: "pull", OrkerId: "orker-a",
			Error: &orkstackv1.Error{Code: "IMAGE_PULL_FAILED", Category: orkstackv1.FailureCategory_FAILURE_CATEGORY_INFRASTRUCTURE_ERROR}},
		{Attempt: 3, Outcome: orkstackv1.AttemptOutcome_ATTEMPT_OUTCOME_SUCCEEDED, Phase: "commit", OrkerId: "orker-b"},
	}}
	id := fixture.run(t, "payments", "reconcile", orkstackv1.Status_STATUS_SUCCEEDED, flakeNow.Add(-time.Minute), three)
	handler := api.NewStatsHandler(fixture.svc, 0)
	since, until := flakeNow.Add(-24*time.Hour), flakeNow

	stats, err := handler.GetStats(context.Background(), connect.NewRequest(&orkstackv1.GetStatsRequest{
		Namespace: "payments", Since: timestamppb.New(since), Until: timestamppb.New(until),
	}))
	if err != nil {
		t.Fatalf("GetStats: %v", err)
	}
	if stats.Msg.GetRunsByStatus()["succeeded"] != 1 || stats.Msg.GetRunsByStatus()["failed"] != 0 {
		t.Fatalf("GetStats sees %v; the retry must be invisible there", stats.Msg.GetRunsByStatus())
	}
	group := flakeGroup(t, fixture.flakes(t, handler, flakeRequest("payments", since, until)), "reconcile", "fetch-rates")
	if group.GetRunsRetried() != 1 || group.GetRunsObserved() != 1 || group.GetRetryRate() != 1 || !group.GetRetryRateDefined() {
		t.Errorf("retried=%d observed=%d rate=%v", group.GetRunsRetried(), group.GetRunsObserved(), group.GetRetryRate())
	}
	if group.GetP50Attempts() != 3 || group.GetP95Attempts() != 3 || group.GetRetryAttempts() != 2 {
		t.Errorf("p50=%d p95=%d retries=%d, want 3/3/2", group.GetP50Attempts(), group.GetP95Attempts(), group.GetRetryAttempts())
	}
	if group.GetDominantCode() != "IMAGE_PULL_FAILED" || group.GetDominantPhase() != "pull" ||
		group.GetDominantCategory() != orkstackv1.FailureCategory_FAILURE_CATEGORY_INFRASTRUCTURE_ERROR || group.GetDominantCount() != 2 {
		t.Errorf("dominant cause is off the non-final records: %v", group)
	}
	if len(group.GetExampleRunIds()) != 1 || group.GetExampleRunIds()[0] != id {
		t.Errorf("examples = %v, want [%s]", group.GetExampleRunIds(), id)
	}
}

func TestGetFlakeIndex_ReadsOneBodyPerRunInTheWindowAndOneListingPerDispatchedTask(t *testing.T) {
	fixture := newFlakeFixture(t, flakeNow, []string{"payments", "beta"})
	inWindow := fixture.seedPaymentsFlakes(t)
	handler := api.NewStatsHandler(fixture.svc, 0)
	got := fixture.flakes(t, handler, flakeRequest("payments", flakeNow.Add(-24*time.Hour), flakeNow))

	if reads := fixture.bodies.totalReads(); int64(reads) != got.GetRunsInWindow() || reads != len(inWindow) {
		t.Errorf("read %d bodies, want one per run in the window (%d)", reads, got.GetRunsInWindow())
	}
	for _, id := range inWindow {
		if n := fixture.bodies.reads[id]; n != 1 {
			t.Errorf("read the body of %s %d times, want once", id, n)
		}
	}
	// 3 runs x 2 dispatched + the failed run's 1 + the canceled run's 1.
	if listings := fixture.store.totalListings(); listings != 8 {
		t.Errorf("listed attempts %d times, want one per dispatched task (8)", listings)
	}
	for key, n := range fixture.store.listings {
		if n != 1 {
			t.Errorf("listed %s %d times, want once", key, n)
		}
	}
}

func TestGetFlakeIndex_ReclaimedAttemptRecordsAreExcludedFromTheDenominatorAndCounted(t *testing.T) {
	fixture := newFlakeFixture(t, flakeNow, []string{"payments"})
	var ids []string
	for i := 0; i < 4; i++ {
		ids = append(ids, fixture.run(t, "payments", "reconcile", orkstackv1.Status_STATUS_SUCCEEDED,
			flakeNow.Add(-time.Duration(i+1)*time.Minute), retried("fetch-rates")))
	}
	fixture.store.reclaimed[ids[2]] = true
	fixture.store.reclaimed[ids[3]] = true
	got := fixture.flakes(t, api.NewStatsHandler(fixture.svc, 0), flakeRequest("payments", flakeNow.Add(-24*time.Hour), flakeNow))
	group := flakeGroup(t, got, "reconcile", "fetch-rates")
	if group.GetRunsObserved() != 2 || group.GetRunsRetried() != 2 || group.GetRetryRate() != 1.0 {
		t.Errorf("observed=%d retried=%d rate=%v, want 2/2/1.0 (never 0.5)",
			group.GetRunsObserved(), group.GetRunsRetried(), group.GetRetryRate())
	}
	if group.GetRunsEvidenceMissing() != 2 || got.GetRunsEvidenceMissing() != 2 || got.GetRunsEvidenceComplete() != 2 {
		t.Errorf("evidence missing: group=%d response=%d complete=%d, want 2/2/2",
			group.GetRunsEvidenceMissing(), got.GetRunsEvidenceMissing(), got.GetRunsEvidenceComplete())
	}
}

func TestGetFlakeIndex_ATaskWithNoEvidenceAnywhereIsUnavailableNotZeroPercent(t *testing.T) {
	fixture := newFlakeFixture(t, flakeNow, []string{"payments"})
	fixture.run(t, "payments", "nightly", orkstackv1.Status_STATUS_SUCCEEDED, flakeNow.Add(-time.Minute), unrecorded("warm-cache"))
	fixture.run(t, "payments", "nightly", orkstackv1.Status_STATUS_SUCCEEDED, flakeNow.Add(-2*time.Minute), unrecorded("warm-cache"))
	got := fixture.flakes(t, api.NewStatsHandler(fixture.svc, 0), flakeRequest("payments", flakeNow.Add(-24*time.Hour), flakeNow))
	group := flakeGroup(t, got, "nightly", "warm-cache")
	if group.GetRetryRateDefined() || group.GetRunsObserved() != 0 || group.GetRunsEvidenceMissing() != 2 {
		t.Errorf("an unobservable task: defined=%v observed=%d missing=%d",
			group.GetRetryRateDefined(), group.GetRunsObserved(), group.GetRunsEvidenceMissing())
	}
	if got.GetRunsEvidenceComplete() != 0 || got.GetRunsEvidenceMissing() != 2 {
		t.Errorf("complete=%d missing=%d, want 0 and 2", got.GetRunsEvidenceComplete(), got.GetRunsEvidenceMissing())
	}
}

func TestGetFlakeIndex_APartiallyObservedRunKeepsItsObservedTasksAndIsMissingForTheResponse(t *testing.T) {
	fixture := newFlakeFixture(t, flakeNow, []string{"payments"})
	fixture.run(t, "payments", "reconcile", orkstackv1.Status_STATUS_SUCCEEDED, flakeNow.Add(-time.Minute),
		retried("fetch-rates"), unrecorded("post"))
	got := fixture.flakes(t, api.NewStatsHandler(fixture.svc, 0), flakeRequest("payments", flakeNow.Add(-24*time.Hour), flakeNow))
	if fetch := flakeGroup(t, got, "reconcile", "fetch-rates"); fetch.GetRunsObserved() != 1 || fetch.GetRunsRetried() != 1 {
		t.Errorf("the observed task's evidence was discarded: %v", fetch)
	}
	if post := flakeGroup(t, got, "reconcile", "post"); post.GetRetryRateDefined() || post.GetRunsEvidenceMissing() != 1 {
		t.Errorf("the unrecorded task: %v", post)
	}
	if got.GetRunsEvidenceMissing() != 1 || got.GetRunsEvidenceComplete() != 0 || got.GetRunsDispatched() != 1 {
		t.Errorf("dispatched=%d complete=%d missing=%d, want 1/0/1",
			got.GetRunsDispatched(), got.GetRunsEvidenceComplete(), got.GetRunsEvidenceMissing())
	}
}

func TestGetFlakeIndex_NoAttemptStoreIsFailedPrecondition(t *testing.T) {
	fixture := newBreakdownFixture(t, flakeNow, []string{"payments"}, withAttempts(nil))
	_, err := api.NewStatsHandler(fixture.svc, time.Hour).GetFlakeIndex(context.Background(),
		connect.NewRequest(flakeRequest("payments", flakeNow.Add(-time.Hour), flakeNow)))
	if connect.CodeOf(err) != connect.CodeFailedPrecondition || !strings.Contains(err.Error(), "task attempt store is not configured") {
		t.Fatalf("an engine with no attempt store answered %v, want FailedPrecondition naming the store", err)
	}
	if n := fixture.bodies.totalReads(); n != 0 {
		t.Errorf("read %d bodies before refusing", n)
	}
}

func TestGetFlakeIndex_AnUnreadableBodyOrAttemptListingFailsTheAnswer(t *testing.T) {
	fixture := newFlakeFixture(t, flakeNow, []string{"payments", "beta"})
	inWindow := fixture.seedPaymentsFlakes(t)
	fixture.bodies.broken[inWindow[2]] = errors.New("503 SlowDown")
	got, err := api.NewStatsHandler(fixture.svc, 0).GetFlakeIndex(context.Background(),
		connect.NewRequest(flakeRequest("payments", flakeNow.Add(-24*time.Hour), flakeNow)))
	if err == nil {
		t.Fatalf("a census with an unreadable body answered: %v", got.Msg)
	}
	if connect.CodeOf(err) != connect.CodeUnavailable || !strings.Contains(err.Error(), inWindow[2]) {
		t.Errorf("refused with %v, want Unavailable naming %s", err, inWindow[2])
	}

	listing := newFlakeFixture(t, flakeNow, []string{"payments", "beta"})
	ids := listing.seedPaymentsFlakes(t)
	listing.store.broken[ids[1]] = errors.New("attempt bucket unreachable")
	got, err = api.NewStatsHandler(listing.svc, 0).GetFlakeIndex(context.Background(),
		connect.NewRequest(flakeRequest("payments", flakeNow.Add(-24*time.Hour), flakeNow)))
	if err == nil {
		t.Fatalf("a census with an unreadable attempt listing answered: %v", got.Msg)
	}
	if connect.CodeOf(err) != connect.CodeUnavailable || !strings.Contains(err.Error(), ids[1]) {
		t.Errorf("refused with %v, want Unavailable naming %s", err, ids[1])
	}
}

func TestGetFlakeIndex_AnIndexRowWithoutABodyIsScannedShortAndSaysSo(t *testing.T) {
	fixture := newFlakeFixture(t, flakeNow, []string{"payments", "beta"})
	inWindow := fixture.seedPaymentsFlakes(t)
	fixture.bodies.absent[inWindow[0]] = true
	got := fixture.flakes(t, api.NewStatsHandler(fixture.svc, 0), flakeRequest("payments", flakeNow.Add(-24*time.Hour), flakeNow))
	if got.GetRunsInWindow() != 5 || got.GetRunsScanned() != 4 {
		t.Fatalf("in_window=%d scanned=%d, want 5 and 4", got.GetRunsInWindow(), got.GetRunsScanned())
	}
	if group := flakeGroup(t, got, "reconcile", "fetch-rates"); group.GetRunsRetried() != 1 {
		t.Errorf("the absent body's retry was counted: %v", group)
	}
}

func TestGetFlakeIndex_OverBudgetScanIsRefusedNotTruncated(t *testing.T) {
	fixture := newFlakeFixture(t, flakeNow, []string{"payments"})
	entries := make([]artifacts.RunIndexEntry, api.FlakeIndexMaxScan+1)
	for i := range entries {
		entries[i] = artifacts.RunIndexEntry{RunID: fmt.Sprintf("ork-%d", i), Namespace: "payments",
			Orkflow: "payments", Status: "succeeded", Time: flakeNow.Add(-time.Minute)}
	}
	fixture.bodies.entries = entries
	got, err := api.NewStatsHandler(fixture.svc, 0).GetFlakeIndex(context.Background(),
		connect.NewRequest(flakeRequest("payments", flakeNow.Add(-24*time.Hour), flakeNow)))
	if err == nil {
		t.Fatalf("an over-budget scan answered: %d groups", len(got.Msg.GetGroups()))
	}
	if connect.CodeOf(err) != connect.CodeResourceExhausted {
		t.Errorf("refused with %v, want ResourceExhausted", connect.CodeOf(err))
	}
	for _, want := range []string{fmt.Sprint(api.FlakeIndexMaxScan + 1), fmt.Sprint(api.FlakeIndexMaxScan), "narrow the window", "name one orkflow"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error %q does not say %q", err, want)
		}
	}
	if n := fixture.bodies.totalReads(); n != 0 {
		t.Errorf("an over-budget scan read %d bodies before refusing, want 0", n)
	}
	if n := fixture.store.totalListings(); n != 0 {
		t.Errorf("an over-budget scan listed attempts %d times before refusing, want 0", n)
	}
}

func TestGetFlakeIndex_ACachedAnswerEqualsTheUncachedOne(t *testing.T) {
	fixture := newFlakeFixture(t, flakeNow, []string{"payments", "beta"})
	inWindow := fixture.seedPaymentsFlakes(t)
	since, until := flakeNow.Add(-24*time.Hour), flakeNow

	cached := api.NewStatsHandler(fixture.svc, 5*time.Second)
	first := fixture.flakes(t, cached, flakeRequest("payments", since, until))
	second := fixture.flakes(t, cached, flakeRequest("payments", since, until))
	if !proto.Equal(first, second) {
		t.Fatalf("a cached answer differs from the one it cached:\n%v\n%v", first, second)
	}
	if got := fixture.bodies.totalReads(); got != len(inWindow) {
		t.Fatalf("the cached answer read bodies again (%d reads, want %d)", got, len(inWindow))
	}
	fresh := fixture.flakes(t, api.NewStatsHandler(fixture.svc, 0), flakeRequest("payments", since, until))
	if !proto.Equal(first, fresh) {
		t.Fatalf("the cached answer is not the answer:\n cached %v\n fresh  %v", first, fresh)
	}
	second.Groups[0].RunsRetried = 999
	if third := fixture.flakes(t, cached, flakeRequest("payments", since, until)); !proto.Equal(first, third) {
		t.Fatalf("a caller's edit reached the cache")
	}
	fixture.run(t, "payments", "reconcile", orkstackv1.Status_STATUS_SUCCEEDED, flakeNow.Add(-time.Minute), retried("fetch-rates"))
	fixture.clock.advance(6 * time.Second)
	later := fixture.flakes(t, cached, flakeRequest("payments", since, until))
	if later.GetRunsInWindow() != first.GetRunsInWindow()+1 {
		t.Errorf("after the TTL the answer counts %d runs, want %d", later.GetRunsInWindow(), first.GetRunsInWindow()+1)
	}
}

func TestGetFlakeIndex_ADifferentGroupByOrOrkflowOrLimitIsADifferentAnswer(t *testing.T) {
	fixture := newFlakeFixture(t, flakeNow, []string{"payments", "beta"})
	fixture.seedPaymentsFlakes(t)
	handler := api.NewStatsHandler(fixture.svc, time.Hour)
	since, until := flakeNow.Add(-24*time.Hour), flakeNow

	base := fixture.flakes(t, handler, flakeRequest("payments", since, until))
	if len(base.GetGroups()) != 3 {
		t.Fatalf("base answer has %d groups, want 3: %v", len(base.GetGroups()), base.GetGroups())
	}
	byOrker := fixture.flakes(t, handler, flakeRequest("payments", since, until, dimTaskAPI, dimOrkerAPI))
	if byOrker.GetBodyKeyed() || byOrker.GetGroups()[0].GetOrkerId() != "orker-a" {
		t.Errorf("an orker grouping was answered from the default one's entry: %v", byOrker)
	}
	byOrkflow := fixture.flakes(t, handler, flakeRequest("payments", since, until, dimOrkflowAPI))
	if len(byOrkflow.GetGroups()) != 2 {
		t.Errorf("an orkflow-only grouping has %d groups, want 2", len(byOrkflow.GetGroups()))
	}
	limitReq := flakeRequest("payments", since, until)
	limitReq.Limit = 1
	limited := fixture.flakes(t, handler, limitReq)
	if len(limited.GetGroups()) != 1 || limited.GetGroupsTotal() != 3 {
		t.Errorf("a limit of 1 answered %d of %d groups, want 1 of 3", len(limited.GetGroups()), limited.GetGroupsTotal())
	}
	orkflowReq := flakeRequest("payments", since, until)
	orkflowReq.OrkflowName = "ingest"
	narrowed := fixture.flakes(t, handler, orkflowReq)
	if narrowed.GetRunsInWindow() != 1 {
		t.Errorf("an orkflow filter was answered from the whole namespace's entry: %v", narrowed)
	}
}

func TestGetFlakeIndex_ACachedAnswerIsNeverAnotherNamespaces(t *testing.T) {
	fixture := newFlakeFixture(t, flakeNow, []string{"alpha", "beta"})
	fixture.run(t, "alpha", "deploy", orkstackv1.Status_STATUS_SUCCEEDED, flakeNow.Add(-time.Hour), retried("build"))
	fixture.run(t, "beta", "deploy", orkstackv1.Status_STATUS_SUCCEEDED, flakeNow.Add(-time.Hour), clean("build"))
	handler := api.NewStatsHandler(fixture.svc, time.Hour)
	since, until := flakeNow.Add(-24*time.Hour), flakeNow
	alpha := fixture.flakes(t, handler, flakeRequest("alpha", since, until))
	beta := fixture.flakes(t, handler, flakeRequest("beta", since, until))
	if alpha.GetGroups()[0].GetRunsRetried() != 1 {
		t.Errorf("alpha = %v", alpha)
	}
	if beta.GetNamespace() != "beta" || beta.GetGroups()[0].GetRunsRetried() != 0 {
		t.Errorf("beta was served alpha's answer: %v", beta)
	}
}

func TestGetFlakeIndex_AWarmCacheStillRefusesAWindowThatStoppedBeingOne(t *testing.T) {
	fixture := newFlakeFixture(t, flakeNow, []string{"alpha"})
	fixture.run(t, "alpha", "deploy", orkstackv1.Status_STATUS_SUCCEEDED, flakeNow.Add(-30*time.Minute), retried("build"))
	handler := api.NewStatsHandler(fixture.svc, time.Hour)
	request := &orkstackv1.GetFlakeIndexRequest{Namespace: "alpha", Since: timestamppb.New(flakeNow.Add(-time.Hour))}
	if _, err := handler.GetFlakeIndex(context.Background(), connect.NewRequest(request)); err != nil {
		t.Fatalf("the window was refused while it was still a window: %v", err)
	}
	fixture.clock.advance(-2 * time.Hour)
	got, err := handler.GetFlakeIndex(context.Background(), connect.NewRequest(request))
	if err == nil {
		t.Fatalf("a warm cache answered a window that ends before it starts: %v", got.Msg)
	}
	if connect.CodeOf(err) != connect.CodeInvalidArgument {
		t.Errorf("refused with %v, want InvalidArgument", connect.CodeOf(err))
	}
}

func TestGetFlakeIndex_RefusesCodeCategoryUnspecifiedDuplicateAndALimitPastTheCap(t *testing.T) {
	fixture := newFlakeFixture(t, flakeNow, []string{"alpha"})
	handler := api.NewStatsHandler(fixture.svc, 0)
	since, until := flakeNow.Add(-time.Hour), flakeNow
	pastCap := flakeRequest("alpha", since, until)
	pastCap.Limit = api.FlakeIndexMaxLimit + 1
	negative := flakeRequest("alpha", since, until)
	negative.Limit = -1
	for name, testCase := range map[string]struct {
		req  *orkstackv1.GetFlakeIndexRequest
		want string
	}{
		"code":        {flakeRequest("alpha", since, until, dimTaskAPI, dimCodeAPI), "FAILURE_GROUP_BY_CODE"},
		"category":    {flakeRequest("alpha", since, until, dimCategoryAPI), "FAILURE_GROUP_BY_CATEGORY"},
		"unspecified": {flakeRequest("alpha", since, until, orkstackv1.FailureGroupBy_FAILURE_GROUP_BY_UNSPECIFIED), "FAILURE_GROUP_BY_UNSPECIFIED"},
		"duplicate":   {flakeRequest("alpha", since, until, dimTaskAPI, dimPoolAPI, dimTaskAPI), "FAILURE_GROUP_BY_TASK"},
		"unknown":     {flakeRequest("alpha", since, until, orkstackv1.FailureGroupBy(42)), "42"},
		"past cap":    {pastCap, fmt.Sprint(api.FlakeIndexMaxLimit)},
		"negative":    {negative, "-1"},
	} {
		t.Run(name, func(t *testing.T) {
			got, err := handler.GetFlakeIndex(context.Background(), connect.NewRequest(testCase.req))
			if err == nil {
				t.Fatalf("answered %v", got.Msg)
			}
			if connect.CodeOf(err) != connect.CodeInvalidArgument || !strings.Contains(err.Error(), testCase.want) {
				t.Errorf("refused with %v, want InvalidArgument naming %q", err, testCase.want)
			}
		})
	}
}

func TestGetFlakeIndex_IgnoresTheRunsThisProcessRemembers(t *testing.T) {
	fixture := newFlakeFixture(t, flakeNow, []string{"alpha"})
	fixture.run(t, "alpha", "deploy", orkstackv1.Status_STATUS_SUCCEEDED, flakeNow.Add(-time.Hour), clean("build"))
	fixture.runs.Save(&orkstackv1.Run{
		Id: "ork-only-this-process-saw-this", Namespace: "alpha", OrkflowName: "deploy",
		Status:    orkstackv1.Status_STATUS_SUCCEEDED,
		CreatedAt: timestamppb.New(flakeNow.Add(-time.Hour)), FinishedAt: timestamppb.New(flakeNow.Add(-time.Hour)),
		TaskRuns: []*orkstackv1.TaskRun{{TaskId: "build", Pool: "main", Attempt: 3}},
	})
	got := fixture.flakes(t, api.NewStatsHandler(fixture.svc, 0), flakeRequest("alpha", flakeNow.Add(-24*time.Hour), flakeNow))
	if got.GetRunsInWindow() != 1 || got.GetGroups()[0].GetRunsRetried() != 0 {
		t.Fatalf("a run only this process remembers was counted: %v", got)
	}
}

func TestGetFlakeIndex_OrkflowNarrowsThePopulationNotJustTheGroups(t *testing.T) {
	fixture := newFlakeFixture(t, flakeNow, []string{"payments", "beta"})
	fixture.seedPaymentsFlakes(t)
	req := flakeRequest("payments", flakeNow.Add(-24*time.Hour), flakeNow)
	req.OrkflowName = "reconcile"
	got := fixture.flakes(t, api.NewStatsHandler(fixture.svc, 0), req)
	if got.GetRunsInWindow() != 4 || got.GetRunsScanned() != 4 {
		t.Fatalf("in_window=%d scanned=%d, want 4 and 4", got.GetRunsInWindow(), got.GetRunsScanned())
	}
	if n := fixture.bodies.totalReads(); n != 4 {
		t.Fatalf("read %d bodies, want only reconcile's 4", n)
	}
	for _, group := range got.GetGroups() {
		if group.GetOrkflowName() != "reconcile" {
			t.Errorf("a group of another orkflow: %v", group)
		}
	}
}

func TestGetFlakeIndex_WindowResolutionIsSharedWithGetStatsAndTheBreakdown(t *testing.T) {
	fixture := newFlakeFixture(t, flakeNow, []string{"alpha"})
	handler := api.NewStatsHandler(fixture.svc, 0)
	pinned := flakeNow.Add(-3 * time.Hour)
	for name, window := range map[string]struct {
		since, until *timestamppb.Timestamp
		lookback     *durationpb.Duration
	}{
		"empty":                 {},
		"lookback":              {lookback: durationpb.New(6 * time.Hour)},
		"lookback before until": {lookback: durationpb.New(time.Hour), until: timestamppb.New(pinned)},
		"since only":            {since: timestamppb.New(pinned)},
		"since wins":            {since: timestamppb.New(pinned), lookback: durationpb.New(time.Minute)},
		"both pinned":           {since: timestamppb.New(pinned), until: timestamppb.New(flakeNow.Add(-time.Hour))},
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
			flakes := fixture.flakes(t, handler, &orkstackv1.GetFlakeIndexRequest{
				Namespace: "alpha", Since: window.since, Until: window.until, Lookback: window.lookback,
			})
			for label, pair := range map[string][2]*timestamppb.Timestamp{
				"breakdown": {breakdown.GetWindowSince(), breakdown.GetWindowUntil()},
				"flakes":    {flakes.GetWindowSince(), flakes.GetWindowUntil()},
			} {
				if !proto.Equal(stats.Msg.GetWindowSince(), pair[0]) || !proto.Equal(stats.Msg.GetWindowUntil(), pair[1]) {
					t.Errorf("GetStats answered [%s, %s), the %s [%s, %s)", stats.Msg.GetWindowSince().AsTime(),
						stats.Msg.GetWindowUntil().AsTime(), label, pair[0].AsTime(), pair[1].AsTime())
				}
			}
		})
	}
}

func TestGetFlakeIndex_SaysWhatIsMissingWhenNoArchiveIsWired(t *testing.T) {
	svc := api.NewService(api.Config{
		Reader: &fakeReader{executions: map[string]engine.Executions{}}, Archive: nil,
		Attempts: artifacts.NewMemoryAttemptStore(),
		Starter:  &fakeStarter{}, Now: newStatsClock(flakeNow).Now,
	})
	_, err := api.NewStatsHandler(svc, 0).GetFlakeIndex(context.Background(),
		connect.NewRequest(&orkstackv1.GetFlakeIndexRequest{}))
	if connect.CodeOf(err) != connect.CodeFailedPrecondition || !strings.Contains(err.Error(), "ORK_ARTIFACT_BACKEND") {
		t.Errorf("refused with %v, want FailedPrecondition naming ORK_ARTIFACT_BACKEND", err)
	}
}

func TestGetFlakeIndex_RefusesANamespaceNothingRegistered(t *testing.T) {
	fixture := newFlakeFixture(t, flakeNow, []string{"alpha"})
	_, err := api.NewStatsHandler(fixture.svc, time.Hour).GetFlakeIndex(context.Background(),
		connect.NewRequest(flakeRequest("ghost", flakeNow.Add(-time.Hour), flakeNow)))
	if connect.CodeOf(err) != connect.CodeNotFound {
		t.Fatalf("an unregistered namespace answered %v, want NotFound", err)
	}
}
