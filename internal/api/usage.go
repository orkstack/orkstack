package api

import (
	"container/list"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
	"time"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// Usage is how much of the fleet one namespace RESERVED over one window:
// task-seconds, and cpu- and GiB-seconds as each attempt's reservation times
// how long its Orker held it (internal/engine/usage.go). Two callers ask the
// question — GetUsage and the admission gate for an enforced budget — and
// both must see the SAME measurement, or an operator reading "4% under
// budget" could be refused a second later by a gate that measured something
// else. So the measurement belongs to neither: a UsageMeter owns it, Service
// holds a pointer to it, and its cache is unexported and reachable only
// through Measure, so no other read can reach for it.

const (
	// DefaultUsageCacheTTL is how long one whole usage measurement is reused.
	// Five minutes, against what it bounds: the admission gate measures an
	// enforced namespace's trailing period on submit, and a budget period is
	// at least an hour, so five minutes of age is small against it and is the
	// difference between one measurement per five minutes and one per submit.
	DefaultUsageCacheTTL = 5 * time.Minute
	// UsageCacheTTLVar overrides it. Zero or negative switches the answer
	// cache off (the memo below is not switchable: it cannot change an
	// answer); an unparsable value falls back to the default, the rule
	// ORK_STATS_CACHE_TTL follows.
	UsageCacheTTLVar = "ORK_USAGE_CACHE_TTL"

	// UsageMaxScan bounds the finished runs one measurement reads. It is
	// twice FlakeIndexMaxScan, and deliberately a second constant: a flake
	// read fetches every run's body and records on every call, while a usage
	// read lists each run's records once per measurement and fetches a
	// body and its records once per run per replica (the memo). A usage read
	// past the bound FAILS; it never answers from a subset.
	UsageMaxScan = 10000

	// usageScanConcurrency is the bounded worker pool one measurement runs
	// its per-run reads through. Larger than failureScanConcurrency (8)
	// because the per-run read here is one small listing, not a body plus a
	// listing per task.
	usageScanConcurrency = 16

	// usageMemoMaxRuns bounds the memo, least recently used first. A run's
	// entry is its orkflow, a trimmed TaskRun per dispatched task and one
	// UsageTask per task — about a kilobyte for a fifteen-task run, so the
	// bound is tens of megabytes at most.
	usageMemoMaxRuns = 20000

	// usageComputeTimeout bounds one measurement, which runs on a context
	// detached from the request that started it.
	usageComputeTimeout = 5 * time.Minute

	// usageAnswersMaxEntries bounds the answer cache, statsCacheMaxEntries'
	// rule.
	usageAnswersMaxEntries = 128
)

// UsageCacheTTLFromEnv reads UsageCacheTTLVar, falling back to
// DefaultUsageCacheTTL — the shape of StatsCacheTTLFromEnv.
func UsageCacheTTLFromEnv() time.Duration {
	raw, ok := os.LookupEnv(UsageCacheTTLVar)
	if !ok || raw == "" {
		return DefaultUsageCacheTTL
	}
	parsed, err := time.ParseDuration(raw)
	if err != nil {
		return DefaultUsageCacheTTL
	}
	return parsed
}

var (
	// errUsageNoArchive: no finished run can be listed or read.
	errUsageNoArchive = errors.New("this engine has no run archive configured")
	// errUsageNoAttemptStore: no attempt store that can list a run's records.
	errUsageNoAttemptStore = errors.New("task attempt store is not configured for run-wide reads")
)

// UsageScanBoundError is a measurement refused because its population is
// larger than UsageMaxScan. Nothing past the index listing was read.
type UsageScanBoundError struct {
	Namespace string
	Window    string // the window as asked: its lookback, or "since … until …"
	Runs      int
	Bound     int
}

func (e *UsageScanBoundError) Error() string {
	return fmt.Sprintf("usage of namespace %q over %s would read %d finished runs, more than the %d a usage read is bounded to",
		e.Namespace, e.Window, e.Runs, e.Bound)
}

// usageUnmeasurable reports the measurement failures asking again cannot fix:
// no archive, no run-wide attempt reader, or a population past the bound. It
// is the one predicate GetUsage and the admission gate both use.
func usageUnmeasurable(err error) bool {
	var bound *UsageScanBoundError
	return errors.Is(err, errUsageNoArchive) || errors.Is(err, errUsageNoAttemptStore) || errors.As(err, &bound)
}

// usageKey identifies one measurement: the namespace and window AS SENT
// (statsKey's rule), plus the resolved dimensions joined in order.
type usageKey struct {
	statsKey
	groupBy string
}

// usageKeyFor is the one place a key is built, so the gate's question and
// `orkctl stats usage --since <period>` share one measurement.
func usageKeyFor(
	namespace string,
	since, until *timestamppb.Timestamp,
	lookback *durationpb.Duration,
	groupBy []orkstackv1.FailureGroupBy,
) usageKey {
	return usageKey{
		statsKey: windowKeyFor(artifacts.ResolveNamespace(namespace), since, until, lookback),
		groupBy:  joinGroupBy(groupBy),
	}
}

// label renders the window as it was asked: its lookback when neither bound
// was sent, else its two instants.
func (k usageKey) label(window engine.StatsWindow) string {
	if !k.sinceSet && !k.untilSet {
		if k.lookbackSet {
			return time.Duration(k.lookback).String()
		}
		return DefaultStatsWindow.String()
	}
	return fmt.Sprintf("since %s until %s",
		window.Since.UTC().Format(time.RFC3339), window.Until.UTC().Format(time.RFC3339))
}

type usageEntry struct {
	response *orkstackv1.GetUsageResponse
	expires  time.Time
}

// usageCall is one measurement in flight, shared by every caller of its key.
type usageCall struct {
	done     chan struct{}
	response *orkstackv1.GetUsageResponse
	err      error
}

// runBodyLoader is the one archive read a measurement makes per run.
type runBodyLoader interface {
	LoadRun(ctx context.Context, runID string) (*orkstackv1.Run, error)
}

// UsageMeter measures one namespace's usage over one window, whole, and
// shares the measurement between every caller asking the same question.
type UsageMeter struct {
	lister   FinishedRunIndexLister
	archive  runBodyLoader
	attempts artifacts.RunAttemptReader
	ttl      time.Duration
	now      func() time.Time

	mu       sync.Mutex
	answers  map[usageKey]usageEntry
	inflight map[usageKey]*usageCall
	memo     *runUsageMemo
}

// newUsageMeter builds the meter from what the Service was configured with:
// the archive asserted to the keys-only finished listing, the archive again
// for bodies, and the attempt store asserted to the run-wide reader. A seam
// that is absent stays nil, and a measurement then fails as unmeasurable.
func newUsageMeter(archive RunArchive, attempts artifacts.AttemptStore, ttl time.Duration, now func() time.Time) *UsageMeter {
	m := &UsageMeter{
		ttl:      ttl,
		now:      func() time.Time { return now().UTC() },
		answers:  map[usageKey]usageEntry{},
		inflight: map[usageKey]*usageCall{},
		memo:     newRunUsageMemo(usageMemoMaxRuns),
	}
	if archive != nil {
		m.archive = archive
		if lister, ok := archive.(FinishedRunIndexLister); ok {
			m.lister = lister
		}
	}
	if reader, ok := attempts.(artifacts.RunAttemptReader); ok && attempts != nil {
		m.attempts = reader
	}
	return m
}

// Measure returns one whole measurement of window, grouped by groupBy
// (already resolved and validated), with every budget field unset. key is
// the question as it was asked (usageKeyFor).
//
// One measurement per key at a time: a caller joins the measurement in
// flight for its key or starts one, which runs on a context detached from
// the caller's — a caller that gives up does not abort work the next caller
// will use — and bounded by usageComputeTimeout. A caller whose own context
// ends first is answered Unavailable.
func (m *UsageMeter) Measure(
	ctx context.Context,
	window engine.StatsWindow,
	groupBy []orkstackv1.FailureGroupBy,
	key usageKey,
) (*orkstackv1.GetUsageResponse, error) {
	m.mu.Lock()
	if m.ttl > 0 {
		if entry, ok := m.answers[key]; ok && m.now().Before(entry.expires) {
			m.mu.Unlock()
			return cloneUsage(entry.response), nil
		}
	}
	call, joined := m.inflight[key]
	if !joined {
		call = &usageCall{done: make(chan struct{})}
		m.inflight[key] = call
		groupBy = append([]orkstackv1.FailureGroupBy(nil), groupBy...)
		go m.measure(ctx, call, key, window, groupBy)
	}
	m.mu.Unlock()

	select {
	case <-call.done:
	case <-ctx.Done():
		select {
		case <-call.done:
		default:
			return nil, connect.NewError(connect.CodeUnavailable, fmt.Errorf(
				"usage of namespace %q over %s is still being measured; ask again",
				window.Namespace, key.label(window)))
		}
	}
	if call.err != nil {
		return nil, call.err
	}
	return cloneUsage(call.response), nil
}

// measure runs one measurement and hands it to every waiter. Only a success
// is cached; an error reaches the current waiters and nobody after them.
func (m *UsageMeter) measure(
	parent context.Context,
	call *usageCall,
	key usageKey,
	window engine.StatsWindow,
	groupBy []orkstackv1.FailureGroupBy,
) {
	ctx, cancel := context.WithTimeout(context.WithoutCancel(parent), usageComputeTimeout)
	defer cancel()
	response, err := m.compute(ctx, window, groupBy, key.label(window))

	m.mu.Lock()
	if err == nil && m.ttl > 0 {
		now := m.now()
		for existing, entry := range m.answers {
			if !now.Before(entry.expires) {
				delete(m.answers, existing)
			}
		}
		if len(m.answers) >= usageAnswersMaxEntries {
			clear(m.answers)
		}
		m.answers[key] = usageEntry{response: cloneUsage(response), expires: now.Add(m.ttl)}
	}
	delete(m.inflight, key)
	call.response, call.err = response, err
	m.mu.Unlock()
	close(call.done)
}

// BudgetUsage measures ns's usage over its budget's trailing period — the
// admission gate's question — with no grouping, under the same key an
// `orkctl stats usage --since <period>` request produces, so the gate and
// the operator share one measurement. MeasuredAt is the measurement's
// computed_at, which is the instant the refusal sentence names.
func (m *UsageMeter) BudgetUsage(ctx context.Context, ns *orkstackv1.Namespace) (engine.BudgetUsage, error) {
	period := ns.GetBudget().GetPeriod()
	name := artifacts.ResolveNamespace(ns.GetName())
	until := m.now()
	window := engine.StatsWindow{Namespace: name, Since: until.Add(-period.AsDuration()), Until: until}
	response, err := m.Measure(ctx, window, nil, usageKeyFor(name, nil, nil, period, nil))
	if err != nil {
		return engine.BudgetUsage{}, err
	}
	return engine.BudgetUsage{
		CPUSeconds: response.GetTotal().GetCpuSeconds(),
		GiBSeconds: response.GetTotal().GetGibSeconds(),
		MeasuredAt: response.GetComputedAt().AsTime(),
	}, nil
}

// compute is one measurement: the population's keys, then each run's facts
// through a bounded pool, folded.
func (m *UsageMeter) compute(
	ctx context.Context,
	window engine.StatsWindow,
	groupBy []orkstackv1.FailureGroupBy,
	label string,
) (*orkstackv1.GetUsageResponse, error) {
	if m.lister == nil || m.archive == nil {
		return nil, errUsageNoArchive
	}
	if m.attempts == nil {
		return nil, errUsageNoAttemptStore
	}
	entries, err := m.lister.ListFinishedRunIndex(ctx, engine.ArchiveQuery{
		Namespace: window.Namespace,
		Status:    orkstackv1.Status_STATUS_UNSPECIFIED,
		Since:     window.Since,
		Until:     window.Until,
	})
	switch {
	case errors.Is(err, engine.ErrNoRunArchive):
		return nil, errUsageNoArchive
	case err != nil:
		return nil, fmt.Errorf("list finished runs in namespace %q: %w", window.Namespace, err)
	}
	if len(entries) > UsageMaxScan {
		return nil, &UsageScanBoundError{Namespace: window.Namespace, Window: label, Runs: len(entries), Bound: UsageMaxScan}
	}

	scanned, runs, err := m.scan(ctx, entries)
	if err != nil {
		return nil, err
	}
	total, rows, evidence := engine.FoldUsage(runs, groupBy)
	response := &orkstackv1.GetUsageResponse{
		Total:                total,
		Rows:                 rows,
		GroupBy:              append([]orkstackv1.FailureGroupBy{}, groupBy...),
		RunsInWindow:         int64(len(entries)),
		RunsScanned:          scanned,
		RunsDispatched:       evidence.RunsDispatched,
		RunsEvidenceComplete: evidence.RunsEvidenceComplete,
		RunsEvidenceMissing:  evidence.RunsEvidenceMissing,
		Namespace:            window.Namespace,
		WindowSince:          timestamppb.New(window.Since),
		WindowUntil:          timestamppb.New(window.Until),
		ComputedAt:           timestamppb.New(m.now()),
	}
	if !evidence.CoverageSince.IsZero() {
		response.CoverageSince = timestamppb.New(evidence.CoverageSince)
	}
	return response, nil
}

// scan measures every entry through a pool of usageScanConcurrency and
// returns how many had a body and each one's facts, in index order. An index
// row with no body is skipped (the crash window between the archive's two
// writes); any other failure fails the whole measurement, naming the run.
func (m *UsageMeter) scan(parent context.Context, entries []artifacts.RunIndexEntry) (int64, []engine.UsageRun, error) {
	ctx, stop := context.WithCancel(parent)
	defer stop()
	type result struct {
		run     engine.UsageRun
		present bool
		err     error
	}
	results := make([]result, len(entries))
	var (
		wg    sync.WaitGroup
		slots = make(chan struct{}, usageScanConcurrency)
	)
	for i, entry := range entries {
		if ctx.Err() != nil {
			break
		}
		wg.Add(1)
		slots <- struct{}{}
		go func(i int, entry artifacts.RunIndexEntry) {
			defer wg.Done()
			defer func() { <-slots }()
			run, present, err := m.measureRun(ctx, entry)
			results[i] = result{run: run, present: present, err: err}
			if err != nil {
				stop()
			}
		}(i, entry)
	}
	wg.Wait()
	if err := parent.Err(); err != nil {
		return 0, nil, err
	}
	var casualty error
	for _, result := range results {
		if result.err == nil {
			continue
		}
		if errors.Is(result.err, context.Canceled) {
			if casualty == nil {
				casualty = result.err
			}
			continue
		}
		return 0, nil, result.err
	}
	if casualty != nil {
		return 0, nil, casualty
	}
	var scanned int64
	runs := make([]engine.UsageRun, 0, len(results))
	for _, result := range results {
		if result.present {
			scanned++
			runs = append(runs, result.run)
		}
	}
	return scanned, runs, nil
}

// measureRun is one finished run's facts: its body (from the memo, which only
// ever holds an immutable archived body's facts) and its records (re-listed
// on every measurement, and re-read unless the listing names exactly the
// keys the memo's sums were computed from).
func (m *UsageMeter) measureRun(ctx context.Context, entry artifacts.RunIndexEntry) (engine.UsageRun, bool, error) {
	facts, remembered := m.memo.get(entry.RunID)
	if !remembered {
		run, err := m.archive.LoadRun(ctx, entry.RunID)
		switch {
		case errors.Is(err, artifacts.ErrNotFound):
			return engine.UsageRun{}, false, nil
		case err != nil:
			return engine.UsageRun{}, false, fmt.Errorf("read the archived body of run %q: %w", entry.RunID, err)
		}
		facts = bodyFacts(run)
	}
	refs, err := m.attempts.ListRunAttemptRefs(ctx, entry.RunID)
	if err != nil {
		return engine.UsageRun{}, false, fmt.Errorf("list the attempt records of run %q: %w", entry.RunID, err)
	}
	usage := engine.UsageRun{RunID: entry.RunID, CreatedAt: entry.Time, Orkflow: facts.orkflow}
	if facts.hasUsage && facts.keys == digestRefs(refs) {
		usage.Tasks = append([]engine.UsageTask(nil), facts.usage...)
		m.memo.put(entry.RunID, facts)
		return usage, true, nil
	}

	byTask := map[string][]*orkstackv1.TaskAttempt{}
	read := make([]artifacts.AttemptRef, 0, len(refs))
	for _, ref := range refs {
		record, err := m.attempts.GetAttempt(ctx, ref)
		switch {
		case errors.Is(err, artifacts.ErrNotFound):
			// Reclaimed between the listing and this read: a missing record
			// reads as a missing record.
			continue
		case err != nil:
			return engine.UsageRun{}, false, fmt.Errorf("read attempt %d of task %q in run %q: %w",
				ref.Attempt, ref.TaskID, entry.RunID, err)
		}
		byTask[ref.TaskID] = append(byTask[ref.TaskID], record)
		read = append(read, ref)
	}
	trimmed := &orkstackv1.Run{Id: entry.RunID, OrkflowName: facts.orkflow}
	tasks := make([]engine.UsageTask, 0, len(facts.tasks))
	for _, task := range facts.tasks {
		tasks = append(tasks, engine.UsageOfTask(engine.SampleTask(entry, trimmed, task, byTask[task.GetTaskId()])))
	}
	facts.keys, facts.usage, facts.hasUsage = digestRefs(read), tasks, true
	m.memo.put(entry.RunID, facts)
	usage.Tasks = append([]engine.UsageTask(nil), tasks...)
	return usage, true, nil
}

// bodyFacts keeps what a measurement needs from an archived body: its
// orkflow and its dispatched tasks, trimmed to the fields SampleTask reads.
func bodyFacts(run *orkstackv1.Run) runUsageEntry {
	facts := runUsageEntry{orkflow: run.GetOrkflowName()}
	for _, task := range run.GetTaskRuns() {
		if !engine.Dispatched(task) {
			continue
		}
		facts.tasks = append(facts.tasks, &orkstackv1.TaskRun{
			TaskId: task.GetTaskId(), Pool: task.GetPool(), Attempt: task.GetAttempt(),
		})
	}
	return facts
}

// digestRefs is sha256 over the attempt keys in key order.
func digestRefs(refs []artifacts.AttemptRef) [32]byte {
	var b strings.Builder
	for _, ref := range refs {
		fmt.Fprintf(&b, "%s\x00%s\x00%d\n", ref.RunID, ref.TaskID, ref.Attempt)
	}
	return sha256.Sum256([]byte(b.String()))
}

// runUsageMemo remembers, per finished run, what measuring it costs to
// learn: its orkflow and dispatched tasks, read from its archived body —
// immutable and never deleted (docs/STORAGE.md: "archive bodies are never
// deleted") — and the UsageTasks its attempt records fold to, valid ONLY for
// the exact set of record keys they were read from. It is bounded, least
// recently used first, and guarded by its own mutex.
//
// It is not "a read answered from process memory" (AGENTS.md §7): a body is
// reused only because it cannot change, and a record-derived sum only when
// THIS measurement's listing of the shared store names exactly the keys it
// was computed from — a record reclaimed since, or a late one, changes the
// listing and the run is re-read, on a warm replica exactly as on a cold one.
type runUsageMemo struct {
	mu      sync.Mutex
	max     int
	entries map[string]*list.Element
	order   *list.List // front = most recently used
}

type runUsageEntry struct {
	orkflow  string
	tasks    []*orkstackv1.TaskRun // dispatched only; task_id, pool, attempt copied
	keys     [32]byte              // sha256 over the sorted attempt keys usage was read from
	usage    []engine.UsageTask    // valid only while a fresh listing digests to keys
	hasUsage bool
}

type memoItem struct {
	runID string
	entry runUsageEntry
}

func newRunUsageMemo(max int) *runUsageMemo {
	return &runUsageMemo{max: max, entries: map[string]*list.Element{}, order: list.New()}
}

func (c *runUsageMemo) get(runID string) (runUsageEntry, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	element, ok := c.entries[runID]
	if !ok {
		return runUsageEntry{}, false
	}
	c.order.MoveToFront(element)
	return element.Value.(*memoItem).entry, true
}

func (c *runUsageMemo) put(runID string, entry runUsageEntry) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if element, ok := c.entries[runID]; ok {
		element.Value.(*memoItem).entry = entry
		c.order.MoveToFront(element)
		return
	}
	c.entries[runID] = c.order.PushFront(&memoItem{runID: runID, entry: entry})
	for c.max > 0 && c.order.Len() > c.max {
		oldest := c.order.Back()
		c.order.Remove(oldest)
		delete(c.entries, oldest.Value.(*memoItem).runID)
	}
}

func (c *runUsageMemo) len() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.order.Len()
}

func cloneUsage(response *orkstackv1.GetUsageResponse) *orkstackv1.GetUsageResponse {
	return proto.Clone(response).(*orkstackv1.GetUsageResponse)
}
