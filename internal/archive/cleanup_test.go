package archive

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sort"
	"testing"
	"time"

	"go.temporal.io/api/serviceerror"

	"github.com/orkstack/orkstack/pkg/artifacts"
)

var errInjected = errors.New("injected")

type fakeTemporal struct {
	retention           time.Duration
	retentionErr        error
	exists              map[string]bool
	errors              map[string]error
	retentionNamespaces []string
	workflowNamespaces  []string
}

func (f *fakeTemporal) NamespaceRetention(_ context.Context, namespace string) (time.Duration, error) {
	f.retentionNamespaces = append(f.retentionNamespaces, namespace)
	return f.retention, f.retentionErr
}

func (f *fakeTemporal) WorkflowExists(_ context.Context, namespace, id string) (bool, error) {
	f.workflowNamespaces = append(f.workflowNamespaces, namespace)
	return f.exists[id], f.errors[id]
}

type fakeArchive struct {
	entries     []artifacts.RunIndexEntry
	states      map[string]artifacts.RunCleanupState
	deleteErr   map[string]error
	markErr     map[string]error
	deletes     map[string]int
	queries     []artifacts.RunQuery
	pages       [][]string
	afterDelete func(runID string)
}

func newFakeArchive(entries ...artifacts.RunIndexEntry) *fakeArchive {
	return &fakeArchive{entries: entries, states: map[string]artifacts.RunCleanupState{}, deleteErr: map[string]error{}, markErr: map[string]error{}, deletes: map[string]int{}}
}
func (f *fakeArchive) PutRun(context.Context, artifacts.RunRecord) error { return nil }
func (f *fakeArchive) GetRun(context.Context, string) ([]byte, error) {
	return nil, artifacts.ErrNotFound
}
func (f *fakeArchive) ListRuns(_ context.Context, q artifacts.RunQuery) ([]artifacts.RunIndexEntry, error) {
	f.queries = append(f.queries, q)
	var entries []artifacts.RunIndexEntry
	for _, entry := range f.entries {
		if q.Matches(entry) {
			entries = append(entries, entry)
		}
	}
	sort.Slice(entries, func(i, j int) bool {
		if entries[i].Time.Equal(entries[j].Time) {
			return entries[i].RunID > entries[j].RunID
		}
		return entries[i].Time.After(entries[j].Time)
	})
	if q.Limit > 0 && len(entries) > q.Limit {
		entries = entries[:q.Limit]
	}
	page := make([]string, len(entries))
	for i, entry := range entries {
		page[i] = entry.RunID
	}
	f.pages = append(f.pages, page)
	return entries, nil
}
func (f *fakeArchive) CleanupState(_ context.Context, id string) (artifacts.RunCleanupState, error) {
	state, ok := f.states[id]
	if !ok {
		return artifacts.RunCleanupState{}, artifacts.ErrNotFound
	}
	return state, nil
}
func (f *fakeArchive) DeleteRunArtifacts(_ context.Context, _, id string) (artifacts.CleanupResult, error) {
	f.deletes[id]++
	if err := f.deleteErr[id]; err != nil {
		return artifacts.CleanupResult{}, err
	}
	if f.afterDelete != nil {
		f.afterDelete(id)
	}
	return artifacts.CleanupResult{ObjectsDeleted: 2, BytesReclaimed: 20}, nil
}
func (f *fakeArchive) DeleteUnreferencedObjects(context.Context, []string) (artifacts.CleanupResult, error) {
	return artifacts.CleanupResult{}, nil
}
func (f *fakeArchive) MarkArtifactsCleaned(_ context.Context, id string, at time.Time) error {
	if err := f.markErr[id]; err != nil {
		return err
	}
	if _, ok := f.states[id]; !ok {
		f.states[id] = artifacts.RunCleanupState{RunID: id, ArtifactsCleaned: true, TemporalDeletedAt: at}
	}
	return nil
}

func testCleaner(store artifacts.RunArchive, temporal WorkflowReader, now time.Time) *Cleaner {
	c := NewCleaner(CleanupConfig{TemporalNamespace: "temporal-runtime", Interval: time.Hour, RetentionBuffer: 24 * time.Hour, BatchSize: 100}, store, temporal, slog.New(slog.NewTextHandler(io.Discard, nil)))
	c.now = func() time.Time { return now }
	return c
}

func TestRunOnce_DeletesOnlyExpiredWorkflowsGoneFromTemporal(t *testing.T) {
	now := time.Date(2026, 8, 31, 12, 0, 0, 0, time.UTC)
	old := now.Add(-97 * time.Hour)
	recent := now.Add(-95 * time.Hour)
	store := newFakeArchive(
		artifacts.RunIndexEntry{RunID: "gone", Namespace: "default", Time: old},
		artifacts.RunIndexEntry{RunID: "exists", Namespace: "default", Time: old},
		artifacts.RunIndexEntry{RunID: "recent", Namespace: "default", Time: recent},
	)
	temporal := &fakeTemporal{retention: 72 * time.Hour, exists: map[string]bool{"exists": true}}
	stats, err := testCleaner(store, temporal, now).RunOnce(context.Background())
	if err != nil {
		t.Fatalf("RunOnce: %v", err)
	}
	if len(store.queries) != 1 || store.queries[0].Limit != 200 || store.queries[0].Until.IsZero() {
		t.Fatalf("bounded queries = %+v", store.queries)
	}
	if stats.RunsCleaned != 1 || stats.ObjectsDeleted != 2 || stats.BytesReclaimed != 20 {
		t.Fatalf("stats = %+v", stats)
	}
	if store.deletes["gone"] != 1 || store.deletes["exists"] != 0 || store.deletes["recent"] != 0 {
		t.Fatalf("deletes = %#v", store.deletes)
	}
	if got := temporal.retentionNamespaces; len(got) != 1 || got[0] != "temporal-runtime" {
		t.Fatalf("retention namespaces = %v", got)
	}
	for _, got := range temporal.workflowNamespaces {
		if got != "temporal-runtime" {
			t.Fatalf("workflow namespace = %q, want temporal-runtime", got)
		}
	}
}

func TestRunOnce_EmptyTemporalNamespaceFailsClosed(t *testing.T) {
	now := time.Now().UTC()
	store := newFakeArchive(artifacts.RunIndexEntry{RunID: "gone", Namespace: "archive-coordinate", Time: now.Add(-100 * time.Hour)})
	cleaner := NewCleaner(CleanupConfig{Interval: time.Hour, RetentionBuffer: 24 * time.Hour, BatchSize: 100}, store,
		&fakeTemporal{retention: 72 * time.Hour, exists: map[string]bool{}}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	stats, err := cleaner.RunOnce(context.Background())
	if err == nil || stats.RunsCleaned != 0 || len(store.deletes) != 0 {
		t.Fatalf("stats=%+v error=%v deletes=%#v", stats, err, store.deletes)
	}
}

func TestRunOnce_NamespaceNotFoundDeletesNothing(t *testing.T) {
	now := time.Now().UTC()
	store := newFakeArchive(artifacts.RunIndexEntry{RunID: "gone", Namespace: "archive-coordinate", Time: now.Add(-100 * time.Hour)})
	temporal := &fakeTemporal{retention: 72 * time.Hour, exists: map[string]bool{}, errors: map[string]error{"gone": serviceerror.NewNamespaceNotFound("temporal namespace missing")}}
	stats, err := testCleaner(store, temporal, now).RunOnce(context.Background())
	if err == nil || stats.Errors != 1 || stats.RunsCleaned != 0 || len(store.deletes) != 0 {
		t.Fatalf("stats=%+v error=%v deletes=%#v", stats, err, store.deletes)
	}
}

func TestRunOnce_IsIdempotent(t *testing.T) {
	now := time.Now().UTC()
	store := newFakeArchive(artifacts.RunIndexEntry{RunID: "gone", Namespace: "default", Time: now.Add(-100 * time.Hour)})
	cleaner := testCleaner(store, &fakeTemporal{retention: 72 * time.Hour, exists: map[string]bool{}}, now)
	first, err := cleaner.RunOnce(context.Background())
	if err != nil || first.RunsCleaned != 1 {
		t.Fatalf("first = %+v, %v", first, err)
	}
	second, err := cleaner.RunOnce(context.Background())
	if err != nil {
		t.Fatalf("second: %v", err)
	}
	if second.RunsCleaned != 0 || second.ObjectsDeleted != 0 || store.deletes["gone"] != 1 {
		t.Fatalf("second = %+v, deletes=%v", second, store.deletes)
	}
}

func TestRunOnce_ContinuesAfterPerRunFailures(t *testing.T) {
	now := time.Now().UTC()
	old := now.Add(-100 * time.Hour)
	store := newFakeArchive(
		artifacts.RunIndexEntry{RunID: "workflow-error", Namespace: "default", Time: old},
		artifacts.RunIndexEntry{RunID: "delete-error", Namespace: "default", Time: old},
		artifacts.RunIndexEntry{RunID: "mark-error", Namespace: "default", Time: old},
		artifacts.RunIndexEntry{RunID: "success", Namespace: "default", Time: old},
	)
	store.deleteErr["delete-error"] = errInjected
	store.markErr["mark-error"] = errInjected
	temporal := &fakeTemporal{retention: 72 * time.Hour, exists: map[string]bool{}, errors: map[string]error{"workflow-error": context.DeadlineExceeded}}
	stats, err := testCleaner(store, temporal, now).RunOnce(context.Background())
	if err == nil || !errors.Is(err, context.DeadlineExceeded) || !errors.Is(err, errInjected) {
		t.Fatalf("error = %v", err)
	}
	if stats.Errors != 3 || stats.RunsCleaned != 1 || !store.states["success"].ArtifactsCleaned {
		t.Fatalf("stats=%+v states=%#v", stats, store.states)
	}
	if store.deletes["success"] != 1 {
		t.Fatalf("later run was not cleaned: %#v", store.deletes)
	}
}

func TestRunOnce_PagesPastAlreadyCleanedRuns(t *testing.T) {
	now := time.Date(2026, 8, 31, 12, 0, 0, 0, time.UTC)
	var entries []artifacts.RunIndexEntry
	for i := 0; i < 7; i++ {
		entries = append(entries, artifacts.RunIndexEntry{RunID: fmt.Sprintf("run-%d", i), Namespace: "default", Time: now.Add(time.Duration(-100-i) * time.Hour)})
	}
	store := newFakeArchive(entries...)
	for i := 0; i < 4; i++ {
		store.states[fmt.Sprintf("run-%d", i)] = artifacts.RunCleanupState{RunID: fmt.Sprintf("run-%d", i), ArtifactsCleaned: true}
	}
	cleaner := NewCleaner(CleanupConfig{TemporalNamespace: "temporal-runtime", Interval: time.Hour, RetentionBuffer: 24 * time.Hour, BatchSize: 2}, store,
		&fakeTemporal{retention: 72 * time.Hour, exists: map[string]bool{}}, slog.New(slog.NewTextHandler(io.Discard, nil))).WithClock(func() time.Time { return now })
	stats, err := cleaner.RunOnce(context.Background())
	if err != nil || stats.RunsCleaned != 2 {
		t.Fatalf("RunOnce = %+v, %v", stats, err)
	}
	if len(store.queries) < 2 {
		t.Fatalf("queries = %+v, want multiple bounded pages", store.queries)
	}
	for _, q := range store.queries {
		if q.Limit != 4 || q.Until.IsZero() {
			t.Fatalf("unbounded query = %+v", q)
		}
	}
	if store.deletes["run-4"] != 1 || store.deletes["run-5"] != 1 {
		t.Fatalf("older eligible runs were not reached: %#v", store.deletes)
	}
}

func TestRunOnce_CompositeCursorMakesPagesDisjoint(t *testing.T) {
	now := time.Date(2026, 8, 31, 12, 0, 0, 0, time.UTC)
	stamp := now.Add(-100 * time.Hour)
	store := newFakeArchive()
	for i := 0; i < 9; i++ {
		id := fmt.Sprintf("cursor-%02d", i)
		store.entries = append(store.entries, artifacts.RunIndexEntry{RunID: id, Namespace: "default", Time: stamp.Add(-time.Duration(i/3) * time.Hour)})
		store.states[id] = artifacts.RunCleanupState{RunID: id, ArtifactsCleaned: true}
	}
	cleaner := NewCleaner(CleanupConfig{TemporalNamespace: "temporal-runtime", Interval: time.Hour, RetentionBuffer: 24 * time.Hour, BatchSize: 2}, store,
		&fakeTemporal{retention: 72 * time.Hour, exists: map[string]bool{}}, slog.New(slog.NewTextHandler(io.Discard, nil))).WithClock(func() time.Time { return now })
	if _, err := cleaner.RunOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(store.pages) < 2 {
		t.Fatalf("pages = %v, want multiple", store.pages)
	}
	seen := make(map[string]struct{})
	for pageNumber, page := range store.pages {
		for _, runID := range page {
			if _, duplicate := seen[runID]; duplicate {
				t.Fatalf("run %q repeated on page %d: %v", runID, pageNumber, store.pages)
			}
			seen[runID] = struct{}{}
		}
	}
}

func TestRunOnce_SharedTimestampTerminates(t *testing.T) {
	now := time.Date(2026, 8, 31, 12, 0, 0, 0, time.UTC)
	stamp := now.Add(-100 * time.Hour)
	store := newFakeArchive()
	for i := 0; i < 6; i++ {
		id := fmt.Sprintf("same-%d", i)
		store.entries = append(store.entries, artifacts.RunIndexEntry{RunID: id, Namespace: "default", Time: stamp})
		store.states[id] = artifacts.RunCleanupState{RunID: id, ArtifactsCleaned: true}
	}
	cleaner := NewCleaner(CleanupConfig{TemporalNamespace: "temporal-runtime", Interval: time.Hour, RetentionBuffer: 24 * time.Hour, BatchSize: 2}, store,
		&fakeTemporal{retention: 72 * time.Hour, exists: map[string]bool{}}, slog.New(slog.NewTextHandler(io.Discard, nil))).WithClock(func() time.Time { return now })
	stats, err := cleaner.RunOnce(context.Background())
	if err != nil || stats.RunsCleaned != 0 || stats.RunsScanned != 6 {
		t.Fatalf("RunOnce = %+v, %v", stats, err)
	}
	if len(store.queries) > 3 {
		t.Fatalf("shared timestamp did not terminate: %d queries", len(store.queries))
	}
	for _, q := range store.queries {
		if q.Limit != 4 || q.Until.IsZero() {
			t.Fatalf("unbounded query = %+v", q)
		}
	}
}

func TestRunOnce_StopsAfterBatchSizeProcessedRuns(t *testing.T) {
	now := time.Date(2026, 8, 31, 12, 0, 0, 0, time.UTC)
	var entries []artifacts.RunIndexEntry
	for i := 0; i < 10; i++ {
		entries = append(entries, artifacts.RunIndexEntry{RunID: fmt.Sprintf("batch-%d", i), Namespace: "default", Time: now.Add(time.Duration(-100-i) * time.Hour)})
	}
	store := newFakeArchive(entries...)
	cleaner := NewCleaner(CleanupConfig{TemporalNamespace: "temporal-runtime", Interval: time.Hour, RetentionBuffer: 24 * time.Hour, BatchSize: 3}, store,
		&fakeTemporal{retention: 72 * time.Hour, exists: map[string]bool{}}, slog.New(slog.NewTextHandler(io.Discard, nil))).WithClock(func() time.Time { return now })
	stats, err := cleaner.RunOnce(context.Background())
	if err != nil || stats.RunsCleaned != 3 || len(store.deletes) != 3 {
		t.Fatalf("RunOnce = %+v, deletes=%#v, error=%v", stats, store.deletes, err)
	}
	for _, q := range store.queries {
		if q.Limit != 6 || q.Until.IsZero() {
			t.Fatalf("unbounded query = %+v", q)
		}
	}
}

func TestCleanupConfigFromEnv(t *testing.T) {
	for _, key := range []string{"ORK_CLEANUP_ENABLED", "ORK_CLEANUP_INTERVAL", "ORK_CLEANUP_RETENTION_BUFFER", "ORK_CLEANUP_BATCH_SIZE"} {
		t.Setenv(key, "")
	}
	if got := CleanupConfigFromEnv(); got != (CleanupConfig{Interval: time.Hour, RetentionBuffer: 24 * time.Hour, BatchSize: 100}) {
		t.Fatalf("defaults = %+v", got)
	}
	t.Setenv("ORK_CLEANUP_ENABLED", "true")
	t.Setenv("ORK_CLEANUP_INTERVAL", "30m")
	t.Setenv("ORK_CLEANUP_RETENTION_BUFFER", "48h")
	t.Setenv("ORK_CLEANUP_BATCH_SIZE", "7")
	if got := CleanupConfigFromEnv(); got != (CleanupConfig{Enabled: true, Interval: 30 * time.Minute, RetentionBuffer: 48 * time.Hour, BatchSize: 7}) {
		t.Fatalf("configured = %+v", got)
	}
	for key, value := range map[string]string{"ORK_CLEANUP_ENABLED": "maybe", "ORK_CLEANUP_INTERVAL": "no", "ORK_CLEANUP_RETENTION_BUFFER": "0", "ORK_CLEANUP_BATCH_SIZE": "-1"} {
		t.Setenv(key, value)
	}
	if got := CleanupConfigFromEnv(); got != (CleanupConfig{Interval: time.Hour, RetentionBuffer: 24 * time.Hour, BatchSize: 100}) {
		t.Fatalf("malformed = %+v", got)
	}
}

func TestRunOnce_RetentionLookupFailureSkipsIteration(t *testing.T) {
	now := time.Now().UTC()
	store := newFakeArchive(artifacts.RunIndexEntry{RunID: "gone", Namespace: "default", Time: now.Add(-100 * time.Hour)})
	stats, err := testCleaner(store, &fakeTemporal{retentionErr: errInjected}, now).RunOnce(context.Background())
	if !errors.Is(err, errInjected) || stats.Errors != 1 || len(store.deletes) != 0 {
		t.Fatalf("stats=%+v error=%v deletes=%#v", stats, err, store.deletes)
	}
}

// TestRunOnce_CleanupStateFollowsSuccessfulDeletion pins the ordering the
// durability contract depends on: the immutable `artifacts_cleaned` sidecar is
// the claim that a run's storage is reclaimed, so it must never be written for
// a run whose deletion failed. A run marked cleaned is never revisited, so a
// premature mark would strand that run's storage forever.
func TestRunOnce_CleanupStateFollowsSuccessfulDeletion(t *testing.T) {
	now := time.Now().UTC()
	old := now.Add(-100 * time.Hour)
	store := newFakeArchive(
		artifacts.RunIndexEntry{RunID: "delete-fails", Namespace: "default", Time: old},
		artifacts.RunIndexEntry{RunID: "delete-succeeds", Namespace: "default", Time: old.Add(-time.Hour)},
	)
	store.deleteErr["delete-fails"] = errInjected
	temporal := &fakeTemporal{retention: 72 * time.Hour, exists: map[string]bool{}}
	stats, err := testCleaner(store, temporal, now).RunOnce(context.Background())
	if !errors.Is(err, errInjected) {
		t.Fatalf("error = %v, want the injected delete failure surfaced", err)
	}
	if _, stateErr := store.CleanupState(context.Background(), "delete-fails"); !errors.Is(stateErr, artifacts.ErrNotFound) {
		t.Fatalf("failed deletion wrote cleanup state: %v", stateErr)
	}
	if !store.states["delete-succeeds"].ArtifactsCleaned {
		t.Fatalf("successful deletion did not write cleanup state: %#v", store.states)
	}
	if stats.RunsCleaned != 1 || stats.Errors != 1 {
		t.Fatalf("stats = %+v", stats)
	}

	// The failed run is still a candidate on the next pass, and converges once
	// the transient failure clears.
	delete(store.deleteErr, "delete-fails")
	second, err := testCleaner(store, temporal, now).RunOnce(context.Background())
	if err != nil || second.RunsCleaned != 1 || !store.states["delete-fails"].ArtifactsCleaned {
		t.Fatalf("retry = %+v, %v, states=%#v", second, err, store.states)
	}
	if store.deletes["delete-succeeds"] != 1 {
		t.Fatalf("already-cleaned run was deleted twice: %#v", store.deletes)
	}
}

// TestRunOnce_CancellationStopsPromptlyAndLeavesStateConsistent is the
// cancellation half of the chaos coverage. Cleanup is a background worker, not
// a Temporal activity, so what is simulated here is precisely a cancelled
// in-flight cleanup iteration — the shape a shutdown or a torn-down engine
// produces. The cleaner must stop rather than grind through the rest of the
// page, and every run it did reach must be either fully cleaned or untouched.
func TestRunOnce_CancellationStopsPromptlyAndLeavesStateConsistent(t *testing.T) {
	now := time.Now().UTC()
	var entries []artifacts.RunIndexEntry
	for i := range 8 {
		entries = append(entries, artifacts.RunIndexEntry{
			RunID: fmt.Sprintf("cancel-%d", i), Namespace: "default", Time: now.Add(time.Duration(-100-i) * time.Hour)})
	}
	store := newFakeArchive(entries...)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	// Cancel as soon as the first run is durably cleaned, so cancellation lands
	// strictly between two runs rather than inside one.
	store.afterDelete = func(string) { cancel() }

	temporal := &fakeTemporal{retention: 72 * time.Hour, exists: map[string]bool{}}
	stats, err := testCleaner(store, temporal, now).RunOnce(ctx)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("error = %v, want context.Canceled", err)
	}
	if stats.RunsCleaned != 1 {
		t.Fatalf("cancellation did not stop promptly: %+v", stats)
	}
	if len(store.deletes) != 1 {
		t.Fatalf("cleanup kept deleting after cancellation: %#v", store.deletes)
	}
	// Consistent, not half-deleted: the one run it reached carries its sidecar,
	// and no other run was touched at all.
	for runID, count := range store.deletes {
		if count != 1 || !store.states[runID].ArtifactsCleaned {
			t.Fatalf("run %q left half-cleaned: deletes=%d state=%+v", runID, count, store.states[runID])
		}
	}

	// Restart and converge. Already-cleaned runs are skipped through the
	// sidecar; the rest are retried. This is resumption, not cross-restart
	// idempotence, and it must not corrupt anything.
	store.afterDelete = nil
	resumed, err := testCleaner(store, temporal, now).RunOnce(context.Background())
	if err != nil {
		t.Fatalf("resume: %v", err)
	}
	if resumed.RunsCleaned != len(entries)-1 {
		t.Fatalf("resume cleaned %d of %d remaining", resumed.RunsCleaned, len(entries)-1)
	}
	for _, entry := range entries {
		if store.deletes[entry.RunID] != 1 || !store.states[entry.RunID].ArtifactsCleaned {
			t.Fatalf("run %q did not converge: deletes=%d state=%+v",
				entry.RunID, store.deletes[entry.RunID], store.states[entry.RunID])
		}
	}
}
