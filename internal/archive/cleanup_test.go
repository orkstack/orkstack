package archive

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math"
	"sort"
	"testing"
	"time"

	"go.temporal.io/api/serviceerror"
	"google.golang.org/protobuf/types/known/durationpb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

var errInjected = errors.New("injected")

func testLogger() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

// fakeCluster stands in for the RunRetirer seam: NamespaceRetention and
// SetNamespaceRetention back the backstop reconcile, LookupWorkflow and
// DeleteWorkflow back active retirement.
type fakeCluster struct {
	// retention is what NamespaceRetention reports as the cluster's CURRENT
	// backstop TTL. Defaulted by newFakeCluster to whatever the test's
	// RetentionController would want, so Reconcile is a no-op unless a test
	// deliberately sets it to something else.
	retention           time.Duration
	retentionErr        error
	setRetentionErr     error
	setRetentionCalls   []time.Duration
	retentionNamespaces []string

	// found reports whether LookupWorkflow finds workflowID. Unset (the zero
	// value, false) is "not found" — the default every paging-focused test
	// below relies on, since a not-found execution needs no DeleteWorkflow
	// call before storage is reclaimed.
	found map[string]bool
	// closeTime is the close time LookupWorkflow reports for a found
	// execution. Unset means still running (the zero time).
	closeTime        map[string]time.Time
	lookupErr        map[string]error
	lookupNamespaces []string

	deleteErr        map[string]error
	deleteCalls      map[string]int
	deleteNamespaces []string
}

func newFakeCluster() *fakeCluster {
	return &fakeCluster{
		found:       map[string]bool{},
		closeTime:   map[string]time.Time{},
		lookupErr:   map[string]error{},
		deleteErr:   map[string]error{},
		deleteCalls: map[string]int{},
	}
}

func (f *fakeCluster) NamespaceRetention(_ context.Context, namespace string) (time.Duration, error) {
	f.retentionNamespaces = append(f.retentionNamespaces, namespace)
	return f.retention, f.retentionErr
}

func (f *fakeCluster) SetNamespaceRetention(_ context.Context, namespace string, d time.Duration) (time.Duration, error) {
	f.setRetentionCalls = append(f.setRetentionCalls, d)
	if f.setRetentionErr != nil {
		return 0, f.setRetentionErr
	}
	f.retention = d
	return d, nil
}

func (f *fakeCluster) LookupWorkflow(_ context.Context, namespace, workflowID string) (bool, time.Time, error) {
	f.lookupNamespaces = append(f.lookupNamespaces, namespace)
	if err := f.lookupErr[workflowID]; err != nil {
		return false, time.Time{}, err
	}
	return f.found[workflowID], f.closeTime[workflowID], nil
}

func (f *fakeCluster) DeleteWorkflow(_ context.Context, namespace, workflowID string) error {
	f.deleteNamespaces = append(f.deleteNamespaces, namespace)
	f.deleteCalls[workflowID]++
	return f.deleteErr[workflowID]
}

// fakePolicyStore is an in-memory artifacts.RetentionPolicyStore.
type fakePolicyStore struct {
	policy *orkstackv1.RetentionPolicy
	getErr error
	putErr error
}

func (f *fakePolicyStore) Get(context.Context) (*orkstackv1.RetentionPolicy, error) {
	if f.getErr != nil {
		return nil, f.getErr
	}
	if f.policy == nil {
		return nil, artifacts.ErrNotFound
	}
	return f.policy, nil
}

func (f *fakePolicyStore) Put(_ context.Context, p *orkstackv1.RetentionPolicy, _ artifacts.PutConditions) (*orkstackv1.RetentionPolicy, error) {
	if f.putErr != nil {
		return nil, f.putErr
	}
	f.policy = p
	return p, nil
}

// fakeNamespaces is an in-memory NamespaceLister.
type fakeNamespaces struct {
	docs    []*orkstackv1.Namespace
	listErr error
	// lists counts List calls: one per cleanup pass.
	lists int
}

func (f *fakeNamespaces) List(context.Context) ([]*orkstackv1.Namespace, error) {
	f.lists++
	if f.listErr != nil {
		return nil, f.listErr
	}
	return f.docs, nil
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
	// keysEnumerated counts index keys this archive walked past, which is what
	// a bounded page has to bound. Returning a short slice after reading the
	// whole index is exactly the regression these tests exist to catch.
	keysEnumerated int
	// ignoreUntil models a backend that honours every filter except Until, to
	// prove cleanup does not trust the query it asked for.
	ignoreUntil bool
}

func newFakeArchive(entries ...artifacts.RunIndexEntry) *fakeArchive {
	// A real archive parses entries out of index keys, so every coordinate is
	// always present. Fill the ones a test did not care to name rather than
	// letting the double return entries no backend could produce.
	for i := range entries {
		if entries[i].Namespace == "" {
			entries[i].Namespace = "default"
		}
		if entries[i].Orkflow == "" {
			entries[i].Orkflow = "flow"
		}
		if entries[i].Status == "" {
			entries[i].Status = "succeeded"
		}
	}
	return &fakeArchive{entries: entries, states: map[string]artifacts.RunCleanupState{}, deleteErr: map[string]error{}, markErr: map[string]error{}, deletes: map[string]int{}}
}
func (f *fakeArchive) PutRun(context.Context, artifacts.RunRecord) error { return nil }
func (f *fakeArchive) GetRun(context.Context, string) ([]byte, error) {
	return nil, artifacts.ErrNotFound
}

// add appends entries through the same normalisation newFakeArchive applies,
// so a test that grows the archive still holds entries a backend could produce.
func (f *fakeArchive) add(entries ...artifacts.RunIndexEntry) {
	seeded := newFakeArchive(entries...)
	f.entries = append(f.entries, seeded.entries...)
}

// ListRuns models both orderings the real backends implement. A KeyOrder query
// walks the index in key order from the cursor and stops at the page boundary,
// counting every key it steps over, so a test can tell a genuinely bounded page
// from a full scan that was truncated afterwards.
func (f *fakeArchive) ListRuns(_ context.Context, q artifacts.RunQuery) ([]artifacts.RunIndexEntry, error) {
	f.queries = append(f.queries, q)
	matches := func(entry artifacts.RunIndexEntry) bool {
		if f.ignoreUntil {
			relaxed := q
			relaxed.Until = time.Time{}
			return relaxed.Matches(entry)
		}
		return q.Matches(entry)
	}

	var entries []artifacts.RunIndexEntry
	if q.KeyOrder {
		type keyed struct {
			key   string
			entry artifacts.RunIndexEntry
		}
		indexed := make([]keyed, 0, len(f.entries))
		for _, entry := range f.entries {
			key, err := artifacts.RunIndexKeyForEntry(entry)
			if err != nil {
				return nil, err
			}
			indexed = append(indexed, keyed{key: key, entry: entry})
		}
		sort.Slice(indexed, func(i, j int) bool { return indexed[i].key < indexed[j].key })
		for _, row := range indexed {
			if q.StartAfterKey != "" && row.key <= q.StartAfterKey {
				continue
			}
			f.keysEnumerated++
			if !matches(row.entry) {
				continue
			}
			entries = append(entries, row.entry)
			if q.Limit > 0 && len(entries) >= q.Limit {
				break
			}
		}
	} else {
		for _, entry := range f.entries {
			f.keysEnumerated++
			if matches(entry) {
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

// newTestController builds a RetentionController over an in-memory policy
// store pre-set to maxRetention, and a 24h enforcement margin — the same
// default DefaultRetentionBuffer uses. cluster's NamespaceRetention is
// defaulted (by the caller, via newFakeCluster) so Reconcile is a no-op
// unless a test wants otherwise.
func newTestController(cluster RunRetirer, maxRetention time.Duration) *RetentionController {
	policies := &fakePolicyStore{policy: &orkstackv1.RetentionPolicy{MaxRunRetention: durationpb.New(maxRetention)}}
	return NewRetentionController(policies, cluster, "temporal-runtime", 24*time.Hour, testLogger())
}

// testCleaner builds a Cleaner with the defaults most tests want: no
// namespace overrides (everything inherits maxRetention), a fixed clock, and
// BatchSize 100.
func testCleaner(store artifacts.RunArchive, cluster *fakeCluster, namespaces NamespaceLister, maxRetention time.Duration, now time.Time) *Cleaner {
	if cluster.retention == 0 {
		cluster.retention = maxRetention + 24*time.Hour // backstop already reconciled; Reconcile no-ops
	}
	if namespaces == nil {
		namespaces = &fakeNamespaces{}
	}
	retention := newTestController(cluster, maxRetention)
	c := NewCleaner(CleanupConfig{TemporalNamespace: "temporal-runtime", Interval: time.Hour, RetentionBuffer: 24 * time.Hour, BatchSize: 100},
		store, cluster, namespaces, retention, testLogger())
	c.now = func() time.Time { return now }
	return c
}

// TestRunOnce_RetiresAFoundClosedExecutionPastItsNamespaceRetention is the
// core active-retirement path: a run whose execution the cluster still
// holds, closed well before its namespace's retention, is explicitly deleted
// from the cluster before its OrkStack storage is reclaimed.
func TestRunOnce_RetiresAFoundClosedExecutionPastItsNamespaceRetention(t *testing.T) {
	now := time.Date(2026, 8, 31, 12, 0, 0, 0, time.UTC)
	old := now.Add(-100 * time.Hour)
	store := newFakeArchive(artifacts.RunIndexEntry{RunID: "retire-me", Namespace: "default", Time: old})
	cluster := newFakeCluster()
	cluster.found["retire-me"] = true
	cluster.closeTime["retire-me"] = old

	stats, err := testCleaner(store, cluster, nil, 72*time.Hour, now).RunOnce(context.Background())
	if err != nil {
		t.Fatalf("RunOnce: %v", err)
	}
	if cluster.deleteCalls["retire-me"] != 1 {
		t.Fatalf("DeleteWorkflow calls = %d, want 1", cluster.deleteCalls["retire-me"])
	}
	if store.deletes["retire-me"] != 1 || stats.RunsCleaned != 1 {
		t.Fatalf("stats = %+v, deletes = %#v", stats, store.deletes)
	}
	if len(cluster.deleteNamespaces) != 1 || cluster.deleteNamespaces[0] != "temporal-runtime" {
		t.Fatalf("delete namespace = %v, want [temporal-runtime]", cluster.deleteNamespaces)
	}
}

// TestRunOnce_ReclaimsStorageWithNoDeleteCallWhenAlreadyGone covers the
// !found path: the backstop or an earlier pass already removed the
// execution, so no DeleteWorkflow call is made, but storage is still
// reclaimed.
func TestRunOnce_ReclaimsStorageWithNoDeleteCallWhenAlreadyGone(t *testing.T) {
	now := time.Date(2026, 8, 31, 12, 0, 0, 0, time.UTC)
	store := newFakeArchive(artifacts.RunIndexEntry{RunID: "already-gone", Namespace: "default", Time: now.Add(-100 * time.Hour)})
	cluster := newFakeCluster() // found defaults to false

	stats, err := testCleaner(store, cluster, nil, 72*time.Hour, now).RunOnce(context.Background())
	if err != nil {
		t.Fatalf("RunOnce: %v", err)
	}
	if cluster.deleteCalls["already-gone"] != 0 {
		t.Fatalf("DeleteWorkflow was called for an execution already absent: %d", cluster.deleteCalls["already-gone"])
	}
	if store.deletes["already-gone"] != 1 || stats.RunsCleaned != 1 {
		t.Fatalf("stats = %+v, deletes = %#v", stats, store.deletes)
	}
}

// TestRunOnce_SkipsAFoundExecutionNotYetPastItsNamespaceRetention proves the
// per-entry cutoff is the execution's own CLOSE time against ITS namespace's
// retention, not merely "old enough to have been listed".
func TestRunOnce_SkipsAFoundExecutionNotYetPastItsNamespaceRetention(t *testing.T) {
	now := time.Date(2026, 8, 31, 12, 0, 0, 0, time.UTC)
	// Created long ago (so the creation-time prefilter includes it) but
	// closed recently — a long-running run.
	store := newFakeArchive(artifacts.RunIndexEntry{RunID: "long-runner", Namespace: "default", Time: now.Add(-1000 * time.Hour)})
	cluster := newFakeCluster()
	cluster.found["long-runner"] = true
	cluster.closeTime["long-runner"] = now.Add(-time.Hour) // closed 1h ago, well inside 72h retention

	stats, err := testCleaner(store, cluster, nil, 72*time.Hour, now).RunOnce(context.Background())
	if err != nil {
		t.Fatalf("RunOnce: %v", err)
	}
	if cluster.deleteCalls["long-runner"] != 0 || store.deletes["long-runner"] != 0 {
		t.Fatalf("a run inside its namespace's retention was retired: deletes=%d cluster_deletes=%d",
			store.deletes["long-runner"], cluster.deleteCalls["long-runner"])
	}
	if stats.RunsCleaned != 0 {
		t.Fatalf("stats = %+v", stats)
	}
}

// TestRunOnce_RefusesAStillRunningArchivedRun covers the impossible-by-
// construction case: an archived run whose execution the cluster reports as
// still open (found, zero close time). Deleting it would TERMINATE a live
// execution, so it is refused rather than risked.
func TestRunOnce_RefusesAStillRunningArchivedRun(t *testing.T) {
	now := time.Date(2026, 8, 31, 12, 0, 0, 0, time.UTC)
	store := newFakeArchive(artifacts.RunIndexEntry{RunID: "still-running", Namespace: "default", Time: now.Add(-100 * time.Hour)})
	cluster := newFakeCluster()
	cluster.found["still-running"] = true // closeTime left zero

	stats, err := testCleaner(store, cluster, nil, 72*time.Hour, now).RunOnce(context.Background())
	if err == nil {
		t.Fatal("RunOnce succeeded, want an error naming the still-running archived run")
	}
	if cluster.deleteCalls["still-running"] != 0 || store.deletes["still-running"] != 0 {
		t.Fatalf("a still-running execution was retired: cluster_deletes=%d archive_deletes=%d",
			cluster.deleteCalls["still-running"], store.deletes["still-running"])
	}
	if stats.Errors != 1 || stats.RunsCleaned != 0 {
		t.Fatalf("stats = %+v", stats)
	}
}

// TestRunOnce_UsesEachNamespacesOwnRetention is the multi-namespace scenario:
// three namespaces under one 72h deployment maximum — one explicit and
// tighter (24h), one explicit and at the ceiling (72h, same as inherit), and
// one that inherits. Only entries past THEIR OWN namespace's retention are
// retired.
func TestRunOnce_UsesEachNamespacesOwnRetention(t *testing.T) {
	now := time.Date(2026, 8, 31, 12, 0, 0, 0, time.UTC)
	store := newFakeArchive(
		// tight (24h): closed 30h ago is past its own retention.
		artifacts.RunIndexEntry{RunID: "tight-expired", Namespace: "tight", Time: now.Add(-200 * time.Hour)},
		// tight (24h): closed 1h ago is still inside its own retention.
		artifacts.RunIndexEntry{RunID: "tight-fresh", Namespace: "tight", Time: now.Add(-200 * time.Hour)},
		// inherit (falls to the 72h maximum): closed 30h ago is NOT past 72h.
		artifacts.RunIndexEntry{RunID: "inherit-fresh", Namespace: "inherits", Time: now.Add(-200 * time.Hour)},
		// inherit: closed 100h ago IS past the 72h maximum.
		artifacts.RunIndexEntry{RunID: "inherit-expired", Namespace: "inherits", Time: now.Add(-200 * time.Hour)},
	)
	cluster := newFakeCluster()
	for id, closed := range map[string]time.Time{
		"tight-expired":   now.Add(-30 * time.Hour),
		"tight-fresh":     now.Add(-1 * time.Hour),
		"inherit-fresh":   now.Add(-30 * time.Hour),
		"inherit-expired": now.Add(-100 * time.Hour),
	} {
		cluster.found[id] = true
		cluster.closeTime[id] = closed
	}
	namespaces := &fakeNamespaces{docs: []*orkstackv1.Namespace{
		{Name: "tight", RunRetention: durationpb.New(24 * time.Hour)},
		{Name: "inherits"}, // no run_retention: falls to the 72h maximum
	}}

	stats, err := testCleaner(store, cluster, namespaces, 72*time.Hour, now).RunOnce(context.Background())
	if err != nil {
		t.Fatalf("RunOnce: %v", err)
	}
	if stats.RunsCleaned != 2 {
		t.Fatalf("runs cleaned = %d, want 2 (tight-expired, inherit-expired): stats=%+v", stats.RunsCleaned, stats)
	}
	for id, wantRetired := range map[string]bool{
		"tight-expired":   true,
		"tight-fresh":     false,
		"inherit-fresh":   false,
		"inherit-expired": true,
	} {
		got := store.deletes[id] == 1
		if got != wantRetired {
			t.Errorf("run %q retired = %v, want %v", id, got, wantRetired)
		}
	}
}

// TestRunOnce_ReconcileFailureIsLoggedButDoesNotBlockRetirement proves the
// backstop reconcile is best-effort: active retirement never depends on the
// cluster's own TTL, so a reconcile failure surfaces as a counted error but
// the pass still retires eligible runs.
func TestRunOnce_ReconcileFailureIsLoggedButDoesNotBlockRetirement(t *testing.T) {
	now := time.Date(2026, 8, 31, 12, 0, 0, 0, time.UTC)
	store := newFakeArchive(artifacts.RunIndexEntry{RunID: "still-retires", Namespace: "default", Time: now.Add(-100 * time.Hour)})
	cluster := newFakeCluster()
	cluster.found["still-retires"] = true
	cluster.closeTime["still-retires"] = now.Add(-100 * time.Hour)
	// Force Reconcile to attempt a write (current != wanted) and fail it.
	cluster.retention = time.Hour
	cluster.setRetentionErr = errInjected

	stats, err := testCleaner(store, cluster, nil, 72*time.Hour, now).RunOnce(context.Background())
	if err != nil {
		t.Fatalf("RunOnce: %v, want the reconcile failure counted rather than returned", err)
	}
	if stats.Errors != 1 {
		t.Fatalf("stats.Errors = %d, want 1 for the reconcile failure", stats.Errors)
	}
	if stats.RunsCleaned != 1 || store.deletes["still-retires"] != 1 {
		t.Fatalf("retirement was blocked by the reconcile failure: stats=%+v deletes=%#v", stats, store.deletes)
	}
	if len(cluster.setRetentionCalls) != 1 {
		t.Fatalf("SetNamespaceRetention calls = %d, want 1", len(cluster.setRetentionCalls))
	}
}

func TestRunOnce_EmptyTemporalNamespaceFailsClosed(t *testing.T) {
	now := time.Now().UTC()
	store := newFakeArchive(artifacts.RunIndexEntry{RunID: "gone", Namespace: "archive-coordinate", Time: now.Add(-100 * time.Hour)})
	cluster := newFakeCluster()
	retention := newTestController(cluster, 72*time.Hour)
	cleaner := NewCleaner(CleanupConfig{Interval: time.Hour, RetentionBuffer: 24 * time.Hour, BatchSize: 100},
		store, cluster, &fakeNamespaces{}, retention, testLogger())
	stats, err := cleaner.RunOnce(context.Background())
	if err == nil || stats.RunsCleaned != 0 || len(store.deletes) != 0 {
		t.Fatalf("stats=%+v error=%v deletes=%#v", stats, err, store.deletes)
	}
}

func TestRunOnce_RetentionPolicyReadFailureSkipsIteration(t *testing.T) {
	now := time.Now().UTC()
	store := newFakeArchive(artifacts.RunIndexEntry{RunID: "gone", Namespace: "default", Time: now.Add(-100 * time.Hour)})
	cluster := newFakeCluster()
	policies := &fakePolicyStore{getErr: errInjected}
	retention := NewRetentionController(policies, cluster, "temporal-runtime", 24*time.Hour, testLogger())
	cleaner := NewCleaner(CleanupConfig{TemporalNamespace: "temporal-runtime", Interval: time.Hour, RetentionBuffer: 24 * time.Hour, BatchSize: 100},
		store, cluster, &fakeNamespaces{}, retention, testLogger())
	cleaner.now = func() time.Time { return now }
	stats, err := cleaner.RunOnce(context.Background())
	if !errors.Is(err, errInjected) || stats.Errors != 1 || len(store.deletes) != 0 {
		t.Fatalf("stats=%+v error=%v deletes=%#v", stats, err, store.deletes)
	}
}

func TestRunOnce_NamespaceListFailureSkipsIteration(t *testing.T) {
	now := time.Now().UTC()
	store := newFakeArchive(artifacts.RunIndexEntry{RunID: "gone", Namespace: "default", Time: now.Add(-100 * time.Hour)})
	cluster := newFakeCluster()
	namespaces := &fakeNamespaces{listErr: errInjected}
	stats, err := testCleaner(store, cluster, namespaces, 72*time.Hour, now).RunOnce(context.Background())
	if !errors.Is(err, errInjected) || stats.Errors != 1 || len(store.deletes) != 0 {
		t.Fatalf("stats=%+v error=%v deletes=%#v", stats, err, store.deletes)
	}
}

func TestRunOnce_NamespaceNotFoundDeletesNothing(t *testing.T) {
	now := time.Now().UTC()
	store := newFakeArchive(artifacts.RunIndexEntry{RunID: "gone", Namespace: "archive-coordinate", Time: now.Add(-100 * time.Hour)})
	cluster := newFakeCluster()
	cluster.lookupErr["gone"] = serviceerror.NewNamespaceNotFound("temporal namespace missing")
	stats, err := testCleaner(store, cluster, nil, 72*time.Hour, now).RunOnce(context.Background())
	if err == nil || stats.Errors != 1 || stats.RunsCleaned != 0 || len(store.deletes) != 0 {
		t.Fatalf("stats=%+v error=%v deletes=%#v", stats, err, store.deletes)
	}
}

func TestRunOnce_IsIdempotent(t *testing.T) {
	now := time.Now().UTC()
	store := newFakeArchive(artifacts.RunIndexEntry{RunID: "gone", Namespace: "default", Time: now.Add(-100 * time.Hour)})
	cluster := newFakeCluster()
	cleaner := testCleaner(store, cluster, nil, 72*time.Hour, now)
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
	cluster := newFakeCluster()
	cluster.lookupErr["workflow-error"] = context.DeadlineExceeded
	stats, err := testCleaner(store, cluster, nil, 72*time.Hour, now).RunOnce(context.Background())
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
	// The walk is in index-key order, and the stamp precedes the run id in a
	// key, so it reaches the OLDEST run first. Marking the four oldest cleaned
	// is what forces it to page before it finds anything left to do.
	for i := 3; i < 7; i++ {
		store.states[fmt.Sprintf("run-%d", i)] = artifacts.RunCleanupState{RunID: fmt.Sprintf("run-%d", i), ArtifactsCleaned: true}
	}
	cluster := newFakeCluster()
	cluster.retention = 96 * time.Hour
	retention := newTestController(cluster, 72*time.Hour)
	cleaner := NewCleaner(CleanupConfig{TemporalNamespace: "temporal-runtime", Interval: time.Hour, RetentionBuffer: 24 * time.Hour, BatchSize: 2},
		store, cluster, &fakeNamespaces{}, retention, testLogger()).WithClock(func() time.Time { return now })
	stats, err := cleaner.RunOnce(context.Background())
	if err != nil || stats.RunsCleaned != 2 {
		t.Fatalf("RunOnce = %+v, %v", stats, err)
	}
	if len(store.queries) < 2 {
		t.Fatalf("queries = %+v, want multiple bounded pages", store.queries)
	}
	for _, q := range store.queries {
		if q.Limit != 4 || q.Until.IsZero() || !q.KeyOrder {
			t.Fatalf("unbounded query = %+v", q)
		}
	}
	if store.deletes["run-2"] != 1 || store.deletes["run-1"] != 1 {
		t.Fatalf("runs past the cleaned page were not reached: %#v", store.deletes)
	}
	// Paging must not re-read what earlier pages already stepped over.
	if store.keysEnumerated != 7 {
		t.Fatalf("keys enumerated = %d, want each of the 7 index keys once", store.keysEnumerated)
	}
}

func TestRunOnce_KeyCursorMakesPagesDisjoint(t *testing.T) {
	now := time.Date(2026, 8, 31, 12, 0, 0, 0, time.UTC)
	stamp := now.Add(-100 * time.Hour)
	store := newFakeArchive()
	for i := 0; i < 9; i++ {
		id := fmt.Sprintf("cursor-%02d", i)
		store.add(artifacts.RunIndexEntry{RunID: id, Namespace: "default", Time: stamp.Add(-time.Duration(i/3) * time.Hour)})
		store.states[id] = artifacts.RunCleanupState{RunID: id, ArtifactsCleaned: true}
	}
	cluster := newFakeCluster()
	cluster.retention = 96 * time.Hour
	retention := newTestController(cluster, 72*time.Hour)
	cleaner := NewCleaner(CleanupConfig{TemporalNamespace: "temporal-runtime", Interval: time.Hour, RetentionBuffer: 24 * time.Hour, BatchSize: 2},
		store, cluster, &fakeNamespaces{}, retention, testLogger()).WithClock(func() time.Time { return now })
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
	if len(seen) != 9 {
		t.Fatalf("pages covered %d runs, want all 9 with no gap: %v", len(seen), store.pages)
	}
	if store.keysEnumerated != 9 {
		t.Fatalf("keys enumerated = %d, want each of the 9 index keys once", store.keysEnumerated)
	}
}

func TestRunOnce_SharedTimestampTerminates(t *testing.T) {
	now := time.Date(2026, 8, 31, 12, 0, 0, 0, time.UTC)
	stamp := now.Add(-100 * time.Hour)
	store := newFakeArchive()
	for i := 0; i < 6; i++ {
		id := fmt.Sprintf("same-%d", i)
		store.add(artifacts.RunIndexEntry{RunID: id, Namespace: "default", Time: stamp})
		store.states[id] = artifacts.RunCleanupState{RunID: id, ArtifactsCleaned: true}
	}
	cluster := newFakeCluster()
	cluster.retention = 96 * time.Hour
	retention := newTestController(cluster, 72*time.Hour)
	cleaner := NewCleaner(CleanupConfig{TemporalNamespace: "temporal-runtime", Interval: time.Hour, RetentionBuffer: 24 * time.Hour, BatchSize: 2},
		store, cluster, &fakeNamespaces{}, retention, testLogger()).WithClock(func() time.Time { return now })
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

// A whole cleanup iteration must cost ONE pass over the run index, no matter
// how many pages it takes. Paging that re-reads the index per page turns a
// linear scan into a quadratic one, and the steady state of a healthy
// deployment — every eligible run already cleaned — is exactly the shape that
// amplifies it. Elapsed time hides this on a warm cache, so this asserts on
// keys enumerated.
func TestRunOnce_WholeIterationEnumeratesTheIndexOnce(t *testing.T) {
	const runs = 2000
	now := time.Date(2026, 8, 31, 12, 0, 0, 0, time.UTC)
	var entries []artifacts.RunIndexEntry
	for i := 0; i < runs; i++ {
		entries = append(entries, artifacts.RunIndexEntry{
			RunID: fmt.Sprintf("scan-%05d", i), Namespace: "default",
			Time: now.Add(time.Duration(-100-i) * time.Hour),
		})
	}
	store := newFakeArchive(entries...)
	for _, entry := range store.entries {
		store.states[entry.RunID] = artifacts.RunCleanupState{RunID: entry.RunID, ArtifactsCleaned: true}
	}
	cluster := newFakeCluster()
	cluster.retention = 96 * time.Hour
	retention := newTestController(cluster, 72*time.Hour)
	cleaner := NewCleaner(CleanupConfig{TemporalNamespace: "temporal-runtime", Interval: time.Hour, RetentionBuffer: 24 * time.Hour, BatchSize: 100},
		store, cluster, &fakeNamespaces{}, retention, testLogger()).WithClock(func() time.Time { return now })

	stats, err := cleaner.RunOnce(context.Background())
	if err != nil {
		t.Fatalf("RunOnce: %v", err)
	}
	if stats.RunsScanned != runs {
		t.Fatalf("runs scanned = %d, want every eligible run visited once (%d)", stats.RunsScanned, runs)
	}
	// One pass, not one pass per page.
	if store.keysEnumerated != runs {
		t.Fatalf("keys enumerated = %d over %d pages, want %d — the index is being re-read per page",
			store.keysEnumerated, len(store.queries), runs)
	}

	// The cost the key-order walk replaced, measured rather than asserted from
	// memory: paging a newest-first query re-reads the whole index per page,
	// because the stamp is the fourth key segment so the newest page cannot be
	// produced without sorting every key first.
	newestFirst := newFakeArchive(entries...)
	pages := 0
	for until := now.Add(-72 * time.Hour); ; pages++ {
		page, err := newestFirst.ListRuns(context.Background(), artifacts.RunQuery{Until: until, Limit: 200})
		if err != nil {
			t.Fatalf("newest-first page: %v", err)
		}
		if len(page) < 200 {
			break
		}
		until = page[len(page)-1].Time
	}
	if newestFirst.keysEnumerated <= store.keysEnumerated {
		t.Fatalf("newest-first paging enumerated %d keys, key-order %d: the comparison is not measuring what it claims",
			newestFirst.keysEnumerated, store.keysEnumerated)
	}
	t.Logf("EVIDENCE runs=%d batch_size=100 key_order_pages=%d key_order_keys_enumerated=%d "+
		"newest_first_pages=%d newest_first_keys_enumerated=%d reduction=%.1fx",
		runs, len(store.queries), store.keysEnumerated,
		pages, newestFirst.keysEnumerated,
		float64(newestFirst.keysEnumerated)/float64(store.keysEnumerated))
}

// Cleanup asks for runs outside the retention window, but the deletion it then
// performs is irreversible and RunArchive is an exported interface, so the
// eligibility of each entry is re-checked rather than trusted from the filter.
func TestRunOnce_RechecksRetentionAgainstEachReturnedEntry(t *testing.T) {
	now := time.Date(2026, 8, 31, 12, 0, 0, 0, time.UTC)
	store := newFakeArchive(
		artifacts.RunIndexEntry{RunID: "expired", Namespace: "default", Time: now.Add(-100 * time.Hour)},
		artifacts.RunIndexEntry{RunID: "still-retained", Namespace: "default", Time: now.Add(-time.Minute)},
	)
	store.ignoreUntil = true // a backend that honours every filter except Until
	cluster := newFakeCluster()
	stats, err := testCleaner(store, cluster, nil, 72*time.Hour, now).RunOnce(context.Background())
	if err != nil {
		t.Fatalf("RunOnce: %v", err)
	}
	if store.deletes["still-retained"] != 0 {
		t.Fatalf("a run inside the retention window was deleted: %#v", store.deletes)
	}
	if _, marked := store.states["still-retained"]; marked {
		t.Fatal("a run inside the retention window was marked cleaned")
	}
	if store.deletes["expired"] != 1 || stats.RunsCleaned != 1 {
		t.Fatalf("the expired run was not cleaned: deletes=%#v stats=%+v", store.deletes, stats)
	}
}

func TestRunOnce_StopsAfterBatchSizeProcessedRuns(t *testing.T) {
	now := time.Date(2026, 8, 31, 12, 0, 0, 0, time.UTC)
	var entries []artifacts.RunIndexEntry
	for i := 0; i < 10; i++ {
		entries = append(entries, artifacts.RunIndexEntry{RunID: fmt.Sprintf("batch-%d", i), Namespace: "default", Time: now.Add(time.Duration(-100-i) * time.Hour)})
	}
	store := newFakeArchive(entries...)
	cluster := newFakeCluster()
	cluster.retention = 96 * time.Hour
	retention := newTestController(cluster, 72*time.Hour)
	cleaner := NewCleaner(CleanupConfig{TemporalNamespace: "temporal-runtime", Interval: time.Hour, RetentionBuffer: 24 * time.Hour, BatchSize: 3},
		store, cluster, &fakeNamespaces{}, retention, testLogger()).WithClock(func() time.Time { return now })
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
	cluster := newFakeCluster()
	cleaner := testCleaner(store, cluster, nil, 72*time.Hour, now)
	stats, err := cleaner.RunOnce(context.Background())
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
	second, err := cleaner.RunOnce(context.Background())
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

	cluster := newFakeCluster()
	cleaner := testCleaner(store, cluster, nil, 72*time.Hour, now)
	stats, err := cleaner.RunOnce(ctx)
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
	resumed, err := testCleaner(store, cluster, nil, 72*time.Hour, now).RunOnce(context.Background())
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

// The page limit is twice the batch size, and the batch size comes from
// ORK_CLEANUP_BATCH_SIZE, which envPositiveInt only checks parses positive. A
// value above MaxInt/2 doubled into a NEGATIVE limit, and a non-positive limit
// means "no limit" to every check downstream — so the wrap silently turned a
// bounded page into an unbounded one, and reached make() with a negative
// capacity, panicking inside the cleanup goroutine.
func TestDoubleSaturating_NeverWrapsNegative(t *testing.T) {
	for _, tc := range []struct {
		in   int
		want int
	}{
		{in: 1, want: 2},
		{in: 100, want: 200},
		{in: math.MaxInt/2 - 1, want: (math.MaxInt/2 - 1) * 2},
		{in: math.MaxInt / 2, want: (math.MaxInt / 2) * 2},
		{in: math.MaxInt/2 + 1, want: math.MaxInt},
		{in: 4611686018427387905, want: math.MaxInt},
		{in: math.MaxInt, want: math.MaxInt},
	} {
		got := doubleSaturating(tc.in)
		if got != tc.want {
			t.Errorf("doubleSaturating(%d) = %d, want %d", tc.in, got, tc.want)
		}
		if got <= 0 {
			t.Errorf("doubleSaturating(%d) = %d, which every limit check reads as unbounded", tc.in, got)
		}
	}
}

// The same property through the worker: an absurd batch size must still
// produce a positive page limit rather than a negative one, and the iteration
// must complete rather than panic.
func TestRunOnce_AbsurdBatchSizeStillAsksForABoundedPage(t *testing.T) {
	now := time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC)
	archived := now.Add(-(72*time.Hour + 48*time.Hour))
	store := newFakeArchive(
		artifacts.RunIndexEntry{RunID: "run-0", Namespace: "default", Orkflow: "flow", Status: "succeeded", Time: archived},
	)
	cluster := newFakeCluster()
	cluster.retention = 96 * time.Hour
	retention := newTestController(cluster, 72*time.Hour)
	cleaner := NewCleaner(CleanupConfig{
		TemporalNamespace: "temporal-runtime", Interval: time.Hour,
		RetentionBuffer: 24 * time.Hour, BatchSize: 4611686018427387905,
	}, store, cluster, &fakeNamespaces{}, retention, testLogger()).WithClock(func() time.Time { return now })

	stats, err := cleaner.RunOnce(context.Background())
	if err != nil {
		t.Fatalf("RunOnce: %v", err)
	}
	if stats.RunsCleaned != 1 {
		t.Fatalf("RunOnce = %+v, want the one expired run cleaned", stats)
	}
	if len(store.queries) == 0 {
		t.Fatal("no query recorded")
	}
	for _, q := range store.queries {
		if q.Limit <= 0 {
			t.Errorf("query limit = %d, want a positive bound: the doubling wrapped", q.Limit)
		}
	}
}

// A pass that processes a full batch leaves a backlog behind it, so Run starts
// the next pass at once, after the last run the previous one examined, and
// returns to the ticker after the first short pass: seven due runs at batch
// two are four passes inside one Interval, each run deleted exactly once.
func TestRun_DrainsABacklogWithoutWaitingForTheTicker(t *testing.T) {
	now := time.Date(2026, 8, 31, 12, 0, 0, 0, time.UTC)
	var entries []artifacts.RunIndexEntry
	for i := 0; i < 7; i++ {
		entries = append(entries, artifacts.RunIndexEntry{RunID: fmt.Sprintf("run-%d", i), Namespace: "default", Time: now.Add(time.Duration(-100-i) * time.Hour)})
	}
	store := newFakeArchive(entries...)
	allDeleted := make(chan struct{})
	deleted := 0
	store.afterDelete = func(string) {
		deleted++
		if deleted == len(entries) {
			close(allDeleted)
		}
	}
	cluster := newFakeCluster()
	cluster.retention = 96 * time.Hour
	namespaces := &fakeNamespaces{}
	cleaner := NewCleaner(CleanupConfig{TemporalNamespace: "temporal-runtime", Interval: time.Hour, RetentionBuffer: 24 * time.Hour, BatchSize: 2},
		store, cluster, namespaces, newTestController(cluster, 72*time.Hour), testLogger()).WithClock(func() time.Time { return now })

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		cleaner.Run(ctx)
		close(done)
	}()
	select {
	case <-allDeleted:
	case <-time.After(10 * time.Second):
		cancel()
		<-done
		t.Fatalf("the backlog did not drain within one Interval: %d of %d runs deleted in %d passes", deleted, len(entries), namespaces.lists)
	}
	cancel()
	<-done

	if namespaces.lists != 4 {
		t.Errorf("passes = %d, want 4 (three full batches of two, then one short)", namespaces.lists)
	}
	for _, entry := range entries {
		if n := store.deletes[entry.RunID]; n != 1 {
			t.Errorf("run %s deleted %d times, want once", entry.RunID, n)
		}
	}
	// Each drained pass resumes after the previous one: every query after the
	// first carries a cursor.
	for i, q := range store.queries[1:] {
		if q.StartAfterKey == "" {
			t.Errorf("query %d of the drain restarted the walk from the start", i+1)
		}
	}
}
