package archive

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/orkstack/orkstack/pkg/artifacts"
)

var errInjected = errors.New("injected")

type fakeTemporal struct {
	retention    time.Duration
	retentionErr error
	exists       map[string]bool
	errors       map[string]error
}

func (f *fakeTemporal) NamespaceRetention(context.Context, string) (time.Duration, error) {
	return f.retention, f.retentionErr
}

func (f *fakeTemporal) WorkflowExists(_ context.Context, _, id string) (bool, error) {
	return f.exists[id], f.errors[id]
}

type fakeArchive struct {
	entries   []artifacts.RunIndexEntry
	states    map[string]artifacts.RunCleanupState
	deleteErr map[string]error
	markErr   map[string]error
	deletes   map[string]int
}

func newFakeArchive(entries ...artifacts.RunIndexEntry) *fakeArchive {
	return &fakeArchive{entries: entries, states: map[string]artifacts.RunCleanupState{}, deleteErr: map[string]error{}, markErr: map[string]error{}, deletes: map[string]int{}}
}
func (f *fakeArchive) PutRun(context.Context, artifacts.RunRecord) error { return nil }
func (f *fakeArchive) GetRun(context.Context, string) ([]byte, error) {
	return nil, artifacts.ErrNotFound
}
func (f *fakeArchive) ListRuns(context.Context, artifacts.RunQuery) ([]artifacts.RunIndexEntry, error) {
	return append([]artifacts.RunIndexEntry(nil), f.entries...), nil
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
	return artifacts.CleanupResult{ObjectsDeleted: 2, BytesReclaimed: 20}, nil
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
	c := NewCleaner(CleanupConfig{Interval: time.Hour, RetentionBuffer: 24 * time.Hour, BatchSize: 100}, store, temporal, slog.New(slog.NewTextHandler(io.Discard, nil)))
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
	if stats.RunsCleaned != 1 || stats.ObjectsDeleted != 2 || stats.BytesReclaimed != 20 {
		t.Fatalf("stats = %+v", stats)
	}
	if store.deletes["gone"] != 1 || store.deletes["exists"] != 0 || store.deletes["recent"] != 0 {
		t.Fatalf("deletes = %#v", store.deletes)
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
