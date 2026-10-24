package engine_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// TestNearestRank_IsSharedByDurationsAndAttemptCounts: one rank formula
// serves run durations and the flake index's attempt counts, so a p95 means
// the same thing on both.
func TestNearestRank_IsSharedByDurationsAndAttemptCounts(t *testing.T) {
	for _, testCase := range []struct {
		n, p, want int
	}{
		{0, 50, 0},
		{1, 50, 1},
		{1, 95, 1},
		{10, 50, 5},
		{10, 95, 10},
		{4, 50, 2},
		{4, 95, 4},
		{10, 0, 1},
		{10, 100, 10},
		{10, 150, 10},
		{5, 50, 3},
	} {
		if got := engine.NearestRank(testCase.n, testCase.p); got != testCase.want {
			t.Errorf("NearestRank(%d, %d) = %d, want %d", testCase.n, testCase.p, got, testCase.want)
		}
		if testCase.n == 0 {
			continue
		}
		durations := make([]time.Duration, testCase.n)
		for i := range durations {
			durations[i] = time.Duration(i+1) * time.Second
		}
		if got := engine.PercentileDuration(durations, testCase.p); got != durations[testCase.want-1] {
			t.Errorf("PercentileDuration over %d samples at p%d = %v, want rank %d",
				testCase.n, testCase.p, got, testCase.want)
		}
	}
}

// TestPercentileDuration_IsTheNearestRankSample. Every value a percentile
// returns here is a duration some run actually took, which is what makes it
// worth paging someone about.
func TestPercentileDuration_IsTheNearestRankSample(t *testing.T) {
	second := time.Second
	ten := []time.Duration{
		1 * second, 2 * second, 3 * second, 4 * second, 5 * second,
		6 * second, 7 * second, 8 * second, 9 * second, 10 * second,
	}
	for _, testCase := range []struct {
		name    string
		samples []time.Duration
		p       int
		want    time.Duration
	}{
		{"empty has no percentile", nil, 50, 0},
		{"one sample is every percentile", []time.Duration{7 * second}, 50, 7 * second},
		{"one sample, p95", []time.Duration{7 * second}, 95, 7 * second},
		{"ten samples, p50 is the fifth", ten, 50, 5 * second},
		{"ten samples, p95 is the tenth", ten, 95, 10 * second},
		{"four samples, p50 is the second",
			[]time.Duration{2 * second, 4 * second, 6 * second, 8 * second}, 50, 4 * second},
		{"four samples, p95 is the fourth",
			[]time.Duration{2 * second, 4 * second, 6 * second, 8 * second}, 95, 8 * second},
		{"p0 is the smallest", ten, 0, 1 * second},
		{"p100 is the largest", ten, 100, 10 * second},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			if got := engine.PercentileDuration(testCase.samples, testCase.p); got != testCase.want {
				t.Errorf("PercentileDuration(%v, %d) = %v, want %v",
					testCase.samples, testCase.p, got, testCase.want)
			}
		})
	}
}

// TestTallyRunIndex_CountsWhatTheKeysSay. The tally is pure over index entries,
// which is what lets the whole aggregate be tested without a store.
func TestTallyRunIndex_CountsWhatTheKeysSay(t *testing.T) {
	entry := func(orkflow, status string) artifacts.RunIndexEntry {
		return artifacts.RunIndexEntry{Namespace: "alpha", Orkflow: orkflow, Status: status}
	}
	tally := engine.TallyRunIndex([]artifacts.RunIndexEntry{
		entry("deploy", "succeeded"),
		entry("deploy", "failed"),
		entry("deploy", "failed"),
		entry("nightly", "succeeded"),
		entry("nightly", "canceled"),
		// The wire spelling of a status is accepted too: the key is the only
		// place this is read from, and one folding rule covers both forms.
		entry("nightly", "STATUS_SUCCEEDED"),
	})

	if got := tally.Finished(); got != 6 {
		t.Errorf("Finished() = %d, want 6", got)
	}
	if got := tally.Succeeded(); got != 3 {
		t.Errorf("Succeeded() = %d, want 3", got)
	}
	if got := tally.ByStatus[engine.StatusKeyFailed]; got != 2 {
		t.Errorf("failed = %d, want 2", got)
	}
	if got := tally.ByStatus[engine.StatusKeyCanceled]; got != 1 {
		t.Errorf("canceled = %d, want 1", got)
	}
	if got := tally.ByOrkflow["deploy"]; got.Failed != 2 || got.Total != 3 {
		t.Errorf("deploy = %d/%d, want 2/3", got.Failed, got.Total)
	}
	if got := tally.ByOrkflow["nightly"]; got.Failed != 0 || got.Total != 3 {
		t.Errorf("nightly = %d/%d, want 0/3", got.Failed, got.Total)
	}
}

// TestTallyRuns_ReadsIndexKeysAndNeverABody is the archiver's half of the
// keys-only claim, one layer below the API's.
//
// The store under it answers listings and panics on a body read, so the test
// observes the absence of the forbidden call rather than a cost.
func TestTallyRuns_ReadsIndexKeysAndNeverABody(t *testing.T) {
	dir := t.TempDir()
	store, err := artifacts.NewLocalRunArchive(dir)
	if err != nil {
		t.Fatalf("open local run archive: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })

	now := time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC)
	writer := engine.NewRunArchiver(store)
	seed := func(namespace, orkflow string, status orkstackv1.Status, at time.Time, n int) {
		t.Helper()
		for i := 0; i < n; i++ {
			run := &orkstackv1.Run{
				Id:          fmt.Sprintf("ork-%s-%s-%s-%d-%d", namespace, orkflow, status, at.UnixNano(), i),
				Namespace:   namespace,
				OrkflowName: orkflow,
				Status:      status,
				CreatedAt:   timestamppb.New(at),
			}
			if err := writer.Archive(context.Background(), run, nil); err != nil {
				t.Fatalf("archive: %v", err)
			}
		}
	}
	seed("alpha", "deploy", orkstackv1.Status_STATUS_SUCCEEDED, now.Add(-time.Hour), 3)
	seed("alpha", "deploy", orkstackv1.Status_STATUS_FAILED, now.Add(-time.Hour), 1)
	seed("alpha", "deploy", orkstackv1.Status_STATUS_FAILED, now.Add(-48*time.Hour), 4)
	seed("beta", "deploy", orkstackv1.Status_STATUS_FAILED, now.Add(-time.Hour), 9)

	// The archiver is rebuilt over a store that refuses to hand back a body.
	sealed := engine.NewRunArchiver(noBodies{inner: store})
	tally, err := sealed.TallyRuns(context.Background(), engine.ArchiveQuery{
		Namespace: "alpha",
		Since:     now.Add(-24 * time.Hour),
		Until:     now,
		// A limit a caller happened to set must not turn a total into a page.
		Limit: 2,
	})
	if err != nil {
		t.Fatalf("TallyRuns: %v", err)
	}
	if got := tally.Finished(); got != 4 {
		t.Errorf("Finished() = %d, want 4 — the window, the namespace or the limit is wrong", got)
	}
	if got := tally.ByStatus[engine.StatusKeySucceeded]; got != 3 {
		t.Errorf("succeeded = %d, want 3", got)
	}
	if got := tally.ByOrkflow["deploy"]; got.Failed != 1 || got.Total != 4 {
		t.Errorf("deploy = %d/%d, want 1/4 (beta's nine failures are not alpha's)",
			got.Failed, got.Total)
	}

	// The control: the store this ran against does panic on a body read, so
	// the assertions above record an absence rather than a store that cannot.
	defer func() {
		if recovered := recover(); recovered == nil {
			t.Fatal("the sealed store does not panic on a body read; the test proves nothing")
		}
	}()
	_, _ = noBodies{inner: store}.GetRun(context.Background(), "ork-anything")
}

// TestListRunIndex_ReadsKeysOnlyIgnoresLimitAndNarrowsToFailed is the failure
// breakdown's population: every FAILED index row in the window, read out of
// keys, however many there are and whatever limit the caller set.
func TestListRunIndex_ReadsKeysOnlyIgnoresLimitAndNarrowsToFailed(t *testing.T) {
	store, err := artifacts.NewLocalRunArchive(t.TempDir())
	if err != nil {
		t.Fatalf("open local run archive: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })

	now := time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC)
	writer := engine.NewRunArchiver(store)
	seed := func(namespace, orkflow string, status orkstackv1.Status, at time.Time, n int) {
		t.Helper()
		for i := 0; i < n; i++ {
			run := &orkstackv1.Run{
				Id:          fmt.Sprintf("ork-%s-%s-%s-%d-%d", namespace, orkflow, status, at.UnixNano(), i),
				Namespace:   namespace,
				OrkflowName: orkflow,
				Status:      status,
				CreatedAt:   timestamppb.New(at),
			}
			if err := writer.Archive(context.Background(), run, nil); err != nil {
				t.Fatalf("archive: %v", err)
			}
		}
	}
	seed("alpha", "deploy", orkstackv1.Status_STATUS_SUCCEEDED, now.Add(-time.Hour), 3)
	seed("alpha", "deploy", orkstackv1.Status_STATUS_CANCELED, now.Add(-time.Hour), 2)
	seed("alpha", "deploy", orkstackv1.Status_STATUS_FAILED, now.Add(-time.Hour), 4)
	seed("alpha", "nightly", orkstackv1.Status_STATUS_FAILED, now.Add(-2*time.Hour), 3)
	seed("alpha", "deploy", orkstackv1.Status_STATUS_FAILED, now.Add(-48*time.Hour), 5)
	seed("beta", "deploy", orkstackv1.Status_STATUS_FAILED, now.Add(-time.Hour), 9)

	sealed := engine.NewRunArchiver(noBodies{inner: store})
	entries, err := sealed.ListRunIndex(context.Background(), engine.ArchiveQuery{
		Namespace: "alpha",
		Since:     now.Add(-24 * time.Hour),
		Until:     now,
		// Neither must narrow a population: the status is always FAILED and
		// a limit a caller happened to set is not a census bound.
		Status: orkstackv1.Status_STATUS_SUCCEEDED,
		Limit:  2,
	})
	if err != nil {
		t.Fatalf("ListRunIndex: %v", err)
	}
	if len(entries) != 7 {
		t.Fatalf("ListRunIndex returned %d entries, want the 7 failed runs of alpha in the window", len(entries))
	}
	for _, entry := range entries {
		if entry.Namespace != "alpha" || artifacts.NormalizeRunStatus(entry.Status) != engine.StatusKeyFailed {
			t.Errorf("entry %+v is not one of alpha's failed runs", entry)
		}
	}

	one, err := sealed.ListRunIndex(context.Background(), engine.ArchiveQuery{
		Namespace: "alpha", OrkflowName: "nightly", Since: now.Add(-24 * time.Hour), Until: now,
	})
	if err != nil || len(one) != 3 {
		t.Fatalf("ListRunIndex(nightly) = %d entries, %v; want 3", len(one), err)
	}

	var absent *engine.RunArchiver
	if _, err := absent.ListRunIndex(context.Background(), engine.ArchiveQuery{Namespace: "alpha"}); err != engine.ErrNoRunArchive {
		t.Fatalf("a nil archiver answered %v, want ErrNoRunArchive", err)
	}
}

// TestListFinishedRunIndex_ReadsKeysOnlyEveryFinishedStatusAndIgnoresLimit is
// the flake index's population: every finished index row in the window,
// whatever its status, read out of keys, however many there are.
func TestListFinishedRunIndex_ReadsKeysOnlyEveryFinishedStatusAndIgnoresLimit(t *testing.T) {
	store, err := artifacts.NewLocalRunArchive(t.TempDir())
	if err != nil {
		t.Fatalf("open local run archive: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })

	now := time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC)
	writer := engine.NewRunArchiver(store)
	seed := func(namespace, orkflow string, status orkstackv1.Status, at time.Time, n int) {
		t.Helper()
		for i := 0; i < n; i++ {
			run := &orkstackv1.Run{
				Id:          fmt.Sprintf("ork-%s-%s-%s-%d-%d", namespace, orkflow, status, at.UnixNano(), i),
				Namespace:   namespace,
				OrkflowName: orkflow,
				Status:      status,
				CreatedAt:   timestamppb.New(at),
			}
			if err := writer.Archive(context.Background(), run, nil); err != nil {
				t.Fatalf("archive: %v", err)
			}
		}
	}
	seed("alpha", "deploy", orkstackv1.Status_STATUS_SUCCEEDED, now.Add(-time.Hour), 3)
	seed("alpha", "deploy", orkstackv1.Status_STATUS_CANCELED, now.Add(-time.Hour), 2)
	seed("alpha", "nightly", orkstackv1.Status_STATUS_FAILED, now.Add(-2*time.Hour), 4)
	seed("alpha", "deploy", orkstackv1.Status_STATUS_FAILED, now.Add(-48*time.Hour), 5)
	seed("beta", "deploy", orkstackv1.Status_STATUS_SUCCEEDED, now.Add(-time.Hour), 9)

	sealed := engine.NewRunArchiver(noBodies{inner: store})
	entries, err := sealed.ListFinishedRunIndex(context.Background(), engine.ArchiveQuery{
		Namespace: "alpha",
		Since:     now.Add(-24 * time.Hour),
		Until:     now,
		Status:    orkstackv1.Status_STATUS_FAILED,
		Limit:     2,
	})
	if err != nil {
		t.Fatalf("ListFinishedRunIndex: %v", err)
	}
	if len(entries) != 9 {
		t.Fatalf("ListFinishedRunIndex returned %d entries, want alpha's 9 finished runs in the window", len(entries))
	}
	tally, err := sealed.TallyRuns(context.Background(), engine.ArchiveQuery{
		Namespace: "alpha", Since: now.Add(-24 * time.Hour), Until: now,
	})
	if err != nil || tally.Finished() != int64(len(entries)) {
		t.Fatalf("TallyRuns counts %d finished (%v), the population is %d", tally.Finished(), err, len(entries))
	}
	one, err := sealed.ListFinishedRunIndex(context.Background(), engine.ArchiveQuery{
		Namespace: "alpha", OrkflowName: "deploy", Since: now.Add(-24 * time.Hour), Until: now,
	})
	if err != nil || len(one) != 5 {
		t.Fatalf("ListFinishedRunIndex(deploy) = %d entries, %v; want 5", len(one), err)
	}
	var absent *engine.RunArchiver
	if _, err := absent.ListFinishedRunIndex(context.Background(), engine.ArchiveQuery{Namespace: "alpha"}); err != engine.ErrNoRunArchive {
		t.Fatalf("a nil archiver answered %v, want ErrNoRunArchive", err)
	}
}

// noBodies answers listings and refuses bodies.
type noBodies struct {
	inner artifacts.RunArchive
}

func (n noBodies) PutRun(ctx context.Context, rec artifacts.RunRecord) error {
	return n.inner.PutRun(ctx, rec)
}

func (n noBodies) GetRun(context.Context, string) ([]byte, error) {
	panic("a tally fetched a run body; it must read index keys only")
}

func (n noBodies) ListRuns(
	ctx context.Context,
	q artifacts.RunQuery,
) ([]artifacts.RunIndexEntry, error) {
	return n.inner.ListRuns(ctx, q)
}

func (n noBodies) CleanupState(ctx context.Context, runID string) (artifacts.RunCleanupState, error) {
	return n.inner.CleanupState(ctx, runID)
}

func (n noBodies) DeleteRunArtifacts(ctx context.Context, namespace, runID string) (artifacts.CleanupResult, error) {
	return n.inner.DeleteRunArtifacts(ctx, namespace, runID)
}

func (n noBodies) DeleteUnreferencedObjects(ctx context.Context, objectIDs []string) (artifacts.CleanupResult, error) {
	return n.inner.DeleteUnreferencedObjects(ctx, objectIDs)
}

func (n noBodies) MarkArtifactsCleaned(ctx context.Context, runID string, at time.Time) error {
	return n.inner.MarkArtifactsCleaned(ctx, runID, at)
}
