package api

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// These tests describe the usage meter: what one measurement reads, what it
// remembers, and why a warm replica answers exactly as a cold one does. The
// fakes are an index lister over a slice, an archive over a map that counts
// LoadRun, and a real MemoryAttemptStore wrapped to count listings and gets.

var usageNow = time.Date(2026, 9, 24, 12, 0, 0, 0, time.UTC)

type usageIndex struct {
	mu      sync.Mutex
	entries []artifacts.RunIndexEntry
	calls   int
	gate    chan struct{} // when set, ListFinishedRunIndex waits on it
	started chan struct{} // when set, closed on the first call
}

func (i *usageIndex) ListFinishedRunIndex(ctx context.Context, q engine.ArchiveQuery) ([]artifacts.RunIndexEntry, error) {
	i.mu.Lock()
	i.calls++
	gate, started := i.gate, i.started
	if started != nil && i.calls == 1 {
		close(started)
	}
	i.mu.Unlock()
	if gate != nil {
		<-gate
	}
	var out []artifacts.RunIndexEntry
	for _, entry := range i.entries {
		if entry.Namespace == q.Namespace && !entry.Time.Before(q.Since) && entry.Time.Before(q.Until) {
			out = append(out, entry)
		}
	}
	return out, nil
}

func (i *usageIndex) listings() int {
	i.mu.Lock()
	defer i.mu.Unlock()
	return i.calls
}

type usageBodies struct {
	mu     sync.Mutex
	runs   map[string]*orkstackv1.Run
	broken map[string]error
	loads  int
	forbid bool
}

func (b *usageBodies) LoadRun(_ context.Context, runID string) (*orkstackv1.Run, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.forbid {
		panic("the usage meter read a body past its scan bound")
	}
	b.loads++
	if err := b.broken[runID]; err != nil {
		return nil, err
	}
	run, ok := b.runs[runID]
	if !ok {
		return nil, fmt.Errorf("run %s: %w", runID, artifacts.ErrNotFound)
	}
	return proto.Clone(run).(*orkstackv1.Run), nil
}

func (b *usageBodies) count() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.loads
}

type usageAttempts struct {
	*artifacts.MemoryAttemptStore
	mu          sync.Mutex
	lists, gets int
	listErr     map[string]error
	getErr      error
	vanished    map[artifacts.AttemptRef]bool
}

func (a *usageAttempts) ListRunAttemptRefs(ctx context.Context, runID string) ([]artifacts.AttemptRef, error) {
	a.mu.Lock()
	a.lists++
	err := a.listErr[runID]
	a.mu.Unlock()
	if err != nil {
		return nil, err
	}
	return a.MemoryAttemptStore.ListRunAttemptRefs(ctx, runID)
}

func (a *usageAttempts) GetAttempt(ctx context.Context, ref artifacts.AttemptRef) (*orkstackv1.TaskAttempt, error) {
	a.mu.Lock()
	a.gets++
	err, vanished := a.getErr, a.vanished[ref]
	a.mu.Unlock()
	if err != nil {
		return nil, err
	}
	if vanished {
		return nil, fmt.Errorf("gone: %w", artifacts.ErrNotFound)
	}
	return a.MemoryAttemptStore.GetAttempt(ctx, ref)
}

func (a *usageAttempts) counts() (lists, gets int) {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.lists, a.gets
}

// usageWorld is one corpus: three finished runs of "analytics" inside the
// last day, one of another namespace, and their attempt records.
type usageWorld struct {
	index    *usageIndex
	bodies   *usageBodies
	attempts *usageAttempts
}

func newUsageWorld(t *testing.T) *usageWorld {
	t.Helper()
	w := &usageWorld{
		index:    &usageIndex{},
		bodies:   &usageBodies{runs: map[string]*orkstackv1.Run{}, broken: map[string]error{}},
		attempts: &usageAttempts{MemoryAttemptStore: artifacts.NewMemoryAttemptStore(), listErr: map[string]error{}, vanished: map[artifacts.AttemptRef]bool{}},
	}
	w.run(t, "analytics", "ork-1-1", "etl", usageNow.Add(-3*time.Hour),
		usageTask{"extract", "main", 1, []usageRecord{{1, 60 * time.Second, 1, 1}}},
		usageTask{"load", "gpu", 2, []usageRecord{{1, 30 * time.Second, 4, 8}, {2, 90 * time.Second, 4, 8}}})
	w.run(t, "analytics", "ork-1-2", "etl", usageNow.Add(-2*time.Hour),
		usageTask{"extract", "main", 1, []usageRecord{{1, 45 * time.Second, 1, 1}}},
		usageTask{"load", "gpu", 1, []usageRecord{{1, 100 * time.Second, 4, 8}}})
	w.run(t, "analytics", "ork-1-3", "report", usageNow.Add(-time.Hour),
		usageTask{"render", "main", 1, []usageRecord{{1, 10 * time.Second, 0.5, 0}}},
		usageTask{"skipped", "", 0, nil})
	w.run(t, "payments", "ork-1-4", "etl", usageNow.Add(-time.Hour),
		usageTask{"extract", "main", 1, []usageRecord{{1, 999 * time.Second, 8, 8}}})
	return w
}

type usageRecord struct {
	attempt int32
	held    time.Duration
	cpu     float64
	gib     int64
}

type usageTask struct {
	id      string
	pool    string
	attempt int32
	records []usageRecord
}

func (w *usageWorld) run(t *testing.T, ns, id, orkflow string, at time.Time, tasks ...usageTask) {
	t.Helper()
	run := &orkstackv1.Run{Id: id, Namespace: ns, OrkflowName: orkflow, Status: orkstackv1.Status_STATUS_SUCCEEDED, CreatedAt: timestamppb.New(at)}
	for _, task := range tasks {
		run.TaskRuns = append(run.TaskRuns, &orkstackv1.TaskRun{TaskId: task.id, Pool: task.pool, Attempt: task.attempt})
		for _, r := range task.records {
			record := &orkstackv1.TaskAttempt{
				SchemaVersion: artifacts.AttemptSchemaVersion, RunId: id, TaskId: task.id, Attempt: r.attempt,
				Outcome:    orkstackv1.AttemptOutcome_ATTEMPT_OUTCOME_SUCCEEDED,
				StartedAt:  timestamppb.New(at),
				FinishedAt: timestamppb.New(at.Add(r.held)),
			}
			if r.cpu > 0 {
				record.Resources = &orkstackv1.ReservedResources{Cpu: r.cpu, MemoryBytes: r.gib << 30}
			}
			if err := w.attempts.PutAttempt(context.Background(), record); err != nil {
				t.Fatalf("PutAttempt: %v", err)
			}
		}
	}
	w.bodies.runs[id] = run
	w.index.entries = append(w.index.entries, artifacts.RunIndexEntry{RunID: id, Namespace: ns, Orkflow: orkflow, Time: at, Status: "succeeded"})
}

func (w *usageWorld) meter(ttl time.Duration) *UsageMeter {
	return &UsageMeter{
		lister: w.index, archive: w.bodies, attempts: w.attempts, ttl: ttl,
		now:     func() time.Time { return usageNow },
		answers: map[usageKey]usageEntry{}, inflight: map[usageKey]*usageCall{},
		memo: newRunUsageMemo(usageMemoMaxRuns),
	}
}

func lastDay() (engine.StatsWindow, usageKey) {
	window := engine.StatsWindow{Namespace: "analytics", Since: usageNow.Add(-24 * time.Hour), Until: usageNow}
	return window, usageKeyFor("analytics", nil, nil, durationpb.New(24*time.Hour), nil)
}

func measureDay(t *testing.T, m *UsageMeter, groupBy ...orkstackv1.FailureGroupBy) *orkstackv1.GetUsageResponse {
	t.Helper()
	window, _ := lastDay()
	resp, err := m.Measure(context.Background(), window, groupBy,
		usageKeyFor("analytics", nil, nil, durationpb.New(24*time.Hour), groupBy))
	if err != nil {
		t.Fatalf("Measure: %v", err)
	}
	return resp
}

func withoutComputedAt(resp *orkstackv1.GetUsageResponse) *orkstackv1.GetUsageResponse {
	out := cloneUsage(resp)
	out.ComputedAt = nil
	return out
}

func TestUsageMeter_MeasuresTheWindowsFinishedRunsFromTheirRecords(t *testing.T) {
	w := newUsageWorld(t)
	resp := measureDay(t, w.meter(0))
	total := resp.GetTotal()
	// extract 60 s + 45 s at {1, 1 GiB}; load 30 + 90 + 100 s at {4, 8 GiB};
	// render 10 s at {0.5, 0 GiB}.
	wantSeconds := 60.0 + 45 + 30 + 90 + 100 + 10
	wantCPU := 105.0 + 4*220 + 5
	wantGiB := 105.0 + 8*220
	if total.GetTaskSeconds() != wantSeconds || total.GetCpuSeconds() != wantCPU || total.GetGibSeconds() != wantGiB {
		t.Fatalf("total = %v / %v / %v, want %v / %v / %v", total.GetTaskSeconds(), total.GetCpuSeconds(),
			total.GetGibSeconds(), wantSeconds, wantCPU, wantGiB)
	}
	if total.GetRuns() != 3 || total.GetTasks() != 5 || total.GetAttempts() != 6 {
		t.Fatalf("total counts = %v", total)
	}
	if resp.GetRunsInWindow() != 3 || resp.GetRunsScanned() != 3 || resp.GetRunsDispatched() != 3 ||
		resp.GetRunsEvidenceComplete() != 3 || resp.GetRunsEvidenceMissing() != 0 {
		t.Fatalf("evidence = %v", resp)
	}
	if !resp.GetCoverageSince().AsTime().Equal(usageNow.Add(-3 * time.Hour)) {
		t.Fatalf("coverage_since = %v", resp.GetCoverageSince().AsTime())
	}
	if resp.GetNamespace() != "analytics" || !resp.GetComputedAt().AsTime().Equal(usageNow) {
		t.Fatalf("namespace/computed_at = %q / %v", resp.GetNamespace(), resp.GetComputedAt().AsTime())
	}
}

func TestUsageMeter_WarmAnswerEqualsColdAnswer(t *testing.T) {
	w := newUsageWorld(t)
	warm := w.meter(0)
	first := measureDay(t, warm, orkstackv1.FailureGroupBy_FAILURE_GROUP_BY_POOL)
	loads := w.bodies.count()
	lists, gets := w.attempts.counts()
	second := measureDay(t, warm, orkstackv1.FailureGroupBy_FAILURE_GROUP_BY_POOL)
	if extra := w.bodies.count() - loads; extra != 0 {
		t.Errorf("the warm measurement made %d LoadRun calls, want 0", extra)
	}
	lists2, gets2 := w.attempts.counts()
	if gets2 != gets {
		t.Errorf("the warm measurement made %d GetAttempt calls, want 0", gets2-gets)
	}
	if lists2-lists != 3 {
		t.Errorf("the warm measurement made %d listings, want one per run (3)", lists2-lists)
	}
	cold := measureDay(t, w.meter(0), orkstackv1.FailureGroupBy_FAILURE_GROUP_BY_POOL)
	for name, resp := range map[string]*orkstackv1.GetUsageResponse{"warm": second, "cold": cold} {
		if !proto.Equal(withoutComputedAt(resp), withoutComputedAt(first)) {
			t.Errorf("%s answer differs from the first:\n%v\n%v", name, resp, first)
		}
	}
}

func TestUsageMeter_AReclaimedRunIsMissingOnAWarmReplicaToo(t *testing.T) {
	w := newUsageWorld(t)
	warm := w.meter(0)
	measureDay(t, warm)
	w.attempts.DeleteRunAttempts("ork-1-1")
	after := measureDay(t, warm)
	if after.GetRunsEvidenceMissing() != 1 || after.GetRunsEvidenceComplete() != 2 {
		t.Fatalf("after reclamation evidence = %d complete, %d missing; want 2, 1",
			after.GetRunsEvidenceComplete(), after.GetRunsEvidenceMissing())
	}
	if got := after.GetTotal().GetTaskSeconds(); got != 155 {
		t.Fatalf("task_seconds = %v, want 155 once ork-1-1's 180 s are gone", got)
	}
	if !after.GetCoverageSince().AsTime().Equal(usageNow.Add(-2 * time.Hour)) {
		t.Fatalf("coverage_since = %v, want it moved past the reclaimed run", after.GetCoverageSince().AsTime())
	}
	cold := measureDay(t, w.meter(0))
	if !proto.Equal(withoutComputedAt(after), withoutComputedAt(cold)) {
		t.Fatalf("warm and cold disagree after reclamation:\n%v\n%v", after, cold)
	}
}

func TestUsageMeter_ALateRecordIsPickedUp(t *testing.T) {
	w := newUsageWorld(t)
	m := w.meter(0)
	before := measureDay(t, m)
	if err := w.attempts.PutAttempt(context.Background(), &orkstackv1.TaskAttempt{
		SchemaVersion: artifacts.AttemptSchemaVersion, RunId: "ork-1-3", TaskId: "render", Attempt: 2,
		StartedAt: timestamppb.New(usageNow), FinishedAt: timestamppb.New(usageNow.Add(4 * time.Second)),
		Resources: &orkstackv1.ReservedResources{Cpu: 1, MemoryBytes: 1 << 30},
	}); err != nil {
		t.Fatalf("PutAttempt: %v", err)
	}
	after := measureDay(t, m)
	if got := after.GetTotal().GetTaskSeconds() - before.GetTotal().GetTaskSeconds(); got != 4 {
		t.Fatalf("a late record added %v task-seconds, want 4", got)
	}
}

func TestUsageMeter_ARecordGoneBetweenListingAndReadIsDropped(t *testing.T) {
	w := newUsageWorld(t)
	w.attempts.vanished[artifacts.AttemptRef{RunID: "ork-1-3", TaskID: "render", Attempt: 1}] = true
	resp := measureDay(t, w.meter(0))
	if resp.GetRunsEvidenceMissing() != 1 || resp.GetTotal().GetAttempts() != 5 {
		t.Fatalf("a vanished record: %d missing, %d attempts; want 1, 5", resp.GetRunsEvidenceMissing(), resp.GetTotal().GetAttempts())
	}
}

func TestUsageMeter_AScanPastItsBoundFailsBeforeReadingABody(t *testing.T) {
	w := newUsageWorld(t)
	w.bodies.forbid = true
	w.index.entries = nil
	for i := range UsageMaxScan + 1 {
		w.index.entries = append(w.index.entries, artifacts.RunIndexEntry{
			RunID: fmt.Sprintf("ork-9-%d", i), Namespace: "analytics", Time: usageNow.Add(-time.Minute),
		})
	}
	window := engine.StatsWindow{Namespace: "analytics", Since: usageNow.Add(-720 * time.Hour), Until: usageNow}
	_, err := w.meter(0).Measure(context.Background(), window, nil, usageKeyFor("analytics", nil, nil, durationpb.New(720*time.Hour), nil))
	var bound *UsageScanBoundError
	if !errors.As(err, &bound) {
		t.Fatalf("Measure = %v, want *UsageScanBoundError", err)
	}
	want := `usage of namespace "analytics" over 720h0m0s would read 10001 finished runs, more than the 10000 a usage read is bounded to`
	if err.Error() != want {
		t.Fatalf("sentence = %q, want %q", err.Error(), want)
	}
}

func TestUsageMeter_AnUnreadableBodyOrListingFailsTheMeasurement(t *testing.T) {
	window, key := lastDay()
	t.Run("body", func(t *testing.T) {
		w := newUsageWorld(t)
		w.bodies.broken["ork-1-2"] = errors.New("bucket unreachable")
		_, err := w.meter(0).Measure(context.Background(), window, nil, key)
		if err == nil || !strings.Contains(err.Error(), `"ork-1-2"`) || !strings.Contains(err.Error(), "bucket unreachable") {
			t.Fatalf("Measure = %v, want a failure naming ork-1-2", err)
		}
	})
	t.Run("listing", func(t *testing.T) {
		w := newUsageWorld(t)
		w.attempts.listErr["ork-1-3"] = errors.New("listing refused")
		_, err := w.meter(0).Measure(context.Background(), window, nil, key)
		if err == nil || !strings.Contains(err.Error(), `"ork-1-3"`) || usageUnmeasurable(err) {
			t.Fatalf("Measure = %v, want an IO failure naming ork-1-3", err)
		}
	})
	t.Run("record", func(t *testing.T) {
		w := newUsageWorld(t)
		w.attempts.getErr = errors.New("get refused")
		if _, err := w.meter(0).Measure(context.Background(), window, nil, key); err == nil || !strings.Contains(err.Error(), "get refused") {
			t.Fatalf("Measure = %v, want the record read's failure", err)
		}
	})
	t.Run("absent body is skipped", func(t *testing.T) {
		w := newUsageWorld(t)
		delete(w.bodies.runs, "ork-1-2")
		resp, err := w.meter(0).Measure(context.Background(), window, nil, key)
		if err != nil {
			t.Fatalf("Measure: %v", err)
		}
		if resp.GetRunsScanned() != 2 || resp.GetRunsInWindow() != 3 {
			t.Fatalf("scanned %d of %d, want 2 of 3", resp.GetRunsScanned(), resp.GetRunsInWindow())
		}
	})
}

func TestUsageMeter_ConcurrentCallersShareOneMeasurement(t *testing.T) {
	w := newUsageWorld(t)
	w.index.gate = make(chan struct{})
	w.index.started = make(chan struct{})
	m := w.meter(0)
	window, key := lastDay()
	var wg sync.WaitGroup
	responses := make([]*orkstackv1.GetUsageResponse, 10)
	errs := make([]error, 10)
	for i := range 10 {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			responses[i], errs[i] = m.Measure(context.Background(), window, nil, key)
		}(i)
	}
	<-w.index.started
	// Hold the one call open until all ten callers have joined it.
	deadline := time.Now().Add(5 * time.Second)
	for {
		m.mu.Lock()
		n, waiters := len(m.inflight), 0
		if call := m.inflight[key]; call != nil {
			waiters = call.waiters
		}
		m.mu.Unlock()
		if n == 1 && waiters == 10 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("%d calls in flight with %d waiters after 5s, want 1 with 10", n, waiters)
		}
		runtime.Gosched()
	}
	close(w.index.gate)
	wg.Wait()
	if got := w.index.listings(); got != 1 {
		t.Fatalf("index listed %d times for ten concurrent callers, want 1", got)
	}
	for i := range 10 {
		if errs[i] != nil || !proto.Equal(responses[i], responses[0]) {
			t.Fatalf("caller %d: %v / %v", i, errs[i], responses[i])
		}
	}
}

func TestUsageMeter_ACallerThatGivesUpDoesNotAbortTheMeasurement(t *testing.T) {
	w := newUsageWorld(t)
	w.index.gate = make(chan struct{})
	w.index.started = make(chan struct{})
	m := w.meter(time.Hour)
	window, key := lastDay()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		_, err := m.Measure(ctx, window, nil, key)
		done <- err
	}()
	<-w.index.started
	cancel()
	err := <-done
	if connect.CodeOf(err) != connect.CodeUnavailable || !strings.Contains(err.Error(), "still being measured; ask again") {
		t.Fatalf("the caller that gave up got %v, want Unavailable 'still being measured'", err)
	}
	close(w.index.gate)
	// The measurement finishes on its own; the next caller gets it from the
	// answer cache without listing the index again.
	deadline := time.Now().Add(5 * time.Second)
	for {
		m.mu.Lock()
		_, cached := m.answers[key]
		m.mu.Unlock()
		if cached {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("the abandoned measurement never completed")
		}
		runtimeGosched()
	}
	if _, err := m.Measure(context.Background(), window, nil, key); err != nil {
		t.Fatalf("second Measure: %v", err)
	}
	if got := w.index.listings(); got != 1 {
		t.Fatalf("index listed %d times, want 1: the second caller should be answered from the cache", got)
	}
}

func TestUsageMeter_ACachedMeasurementCarriesNoBudget(t *testing.T) {
	w := newUsageWorld(t)
	m := w.meter(time.Hour)
	for range 2 {
		resp := measureDay(t, m)
		if resp.GetBudget() != nil || resp.GetBudgetWindow() || resp.GetBudgetReached() {
			t.Fatalf("Measure carries budget fields: %v", resp)
		}
	}
	for _, entry := range m.answers {
		if entry.response.GetBudget() != nil || entry.response.GetBudgetWindow() || entry.response.GetBudgetReached() {
			t.Fatalf("a cached answer carries budget fields: %v", entry.response)
		}
	}
}

func TestUsageMeter_MemoIsBoundedLeastRecentlyUsed(t *testing.T) {
	w := newUsageWorld(t)
	w.run(t, "analytics", "ork-1-5", "etl", usageNow.Add(-30*time.Minute),
		usageTask{"extract", "main", 1, []usageRecord{{1, 5 * time.Second, 1, 1}}})
	m := w.meter(0)
	m.memo = newRunUsageMemo(3)
	unbounded := measureDay(t, w.meter(0))
	got := measureDay(t, m)
	if m.memo.len() != 3 {
		t.Fatalf("memo holds %d runs, want the bound of 3", m.memo.len())
	}
	// The scan is concurrent, so which run was least recently used depends
	// on scheduling; the rule itself is pinned on the memo directly below.
	direct := newRunUsageMemo(3)
	for _, id := range []string{"a", "b", "c"} {
		direct.put(id, runUsageEntry{orkflow: id})
	}
	direct.get("a")
	direct.put("d", runUsageEntry{orkflow: "d"})
	if _, ok := direct.get("b"); ok {
		t.Fatal("the least recently used entry (b) was not evicted")
	}
	for _, id := range []string{"a", "c", "d"} {
		if _, ok := direct.get(id); !ok {
			t.Fatalf("entry %s was evicted; only the least recently used may be", id)
		}
	}
	if !proto.Equal(withoutComputedAt(got), withoutComputedAt(unbounded)) {
		t.Fatalf("a bounded memo changed the answer:\n%v\n%v", got, unbounded)
	}
	if again := measureDay(t, m); !proto.Equal(withoutComputedAt(again), withoutComputedAt(unbounded)) {
		t.Fatalf("a bounded memo changed a later answer:\n%v\n%v", again, unbounded)
	}
}

func TestUsageMeter_NoArchiveOrNoRunWideReaderIsUnmeasurable(t *testing.T) {
	window, key := lastDay()
	w := newUsageWorld(t)
	noArchive := w.meter(0)
	noArchive.lister = nil
	if _, err := noArchive.Measure(context.Background(), window, nil, key); !errors.Is(err, errUsageNoArchive) || !usageUnmeasurable(err) {
		t.Fatalf("no archive: %v", err)
	}
	noReader := w.meter(0)
	noReader.attempts = nil
	if _, err := noReader.Measure(context.Background(), window, nil, key); !errors.Is(err, errUsageNoAttemptStore) || !usageUnmeasurable(err) {
		t.Fatalf("no run-wide reader: %v", err)
	}
	if !usageUnmeasurable(fmt.Errorf("wrapped: %w", &UsageScanBoundError{})) {
		t.Fatal("a scan bound is unmeasurable")
	}
	if usageUnmeasurable(errors.New("bucket unreachable")) {
		t.Fatal("an IO error is not unmeasurable: asking again can fix it")
	}
	// An attempt store without the run-wide read is unmeasurable too.
	svc := NewService(Config{Attempts: plainAttemptStore{}})
	if svc.usage.attempts != nil {
		t.Fatal("an AttemptStore without RunAttemptReader was accepted as a run-wide reader")
	}
}

type plainAttemptStore struct{}

func (plainAttemptStore) PutAttempt(context.Context, *orkstackv1.TaskAttempt) error { return nil }
func (plainAttemptStore) ListAttempts(context.Context, string, string, artifacts.Item) ([]*orkstackv1.TaskAttempt, error) {
	return nil, nil
}

func runtimeGosched() { runtime.Gosched() }
