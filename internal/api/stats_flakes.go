package api

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// The flake index is the second stats read that opens run BODIES, and it
// opens more of them than the failure breakdown does: every FINISHED run in
// the window, not only the failed ones, because the instability it exposes is
// exactly the kind a SUCCEEDED status hides. It also lists one task's attempt
// records per dispatched task — the one aggregation in the tree that reads
// attempt records ACROSS runs.
//
// The crossing is bounded the same three ways the breakdown's is: by the
// window, by an optional orkflow, and by FlakeIndexMaxScan, past which the
// call FAILS rather than answering from a subset. What docs/STATS.md §1
// protects is preserved: bodies hold references, not bytes, and attempt
// records are bounded (artifacts.AttemptMaxBytes), so the read is
// proportional to runs × dispatched tasks, never to output size. Never clamp,
// paginate or sample it — a partial census reads exactly like a whole one.

const (
	// FlakeIndexDefaultLimit is how many groups a request that names no limit
	// gets, flakiest first.
	FlakeIndexDefaultLimit = 50
	// FlakeIndexMaxLimit caps a named limit. It bounds the answer, never the
	// scan: the totals are always over every group.
	FlakeIndexMaxLimit = 500
	// FlakeIndexMaxScan is the most finished-run bodies one flake index will
	// read. A larger population is refused with the bound and the population
	// in the message. A constant on MostFailingLimit's precedent, not a
	// setting.
	FlakeIndexMaxScan = 5000
)

// DefaultFlakeGroupBy is what an empty group_by means before ORKFLOW is made
// explicit at the front of the key.
var DefaultFlakeGroupBy = []orkstackv1.FailureGroupBy{orkstackv1.FailureGroupBy_FAILURE_GROUP_BY_TASK}

// FinishedRunIndexLister is the archive's keys-only listing of every
// finished index row a query's namespace, orkflow and window match, whatever
// the status (engine.RunArchiver.ListFinishedRunIndex). It is RunIndexLister's
// sibling: that one always answers FAILED rows, which is the breakdown's
// population and not this read's.
type FinishedRunIndexLister interface {
	ListFinishedRunIndex(ctx context.Context, q engine.ArchiveQuery) ([]artifacts.RunIndexEntry, error)
}

// flakeKey identifies one flake-index answer: the namespace and window AS
// SENT (statsKey's rule), and every other field of the question.
type flakeKey struct {
	statsKey
	orkflow string
	groupBy string // the resolved dimensions, joined in request order
	limit   int32  // as sent, 0 kept as 0
}

type flakeEntry struct {
	response *orkstackv1.GetFlakeIndexResponse
	expires  time.Time
}

// GetFlakeIndex groups one namespace's dispatched tasks over one window by
// the dimensions the caller names and reports how often each needed more than
// one attempt.
//
// The order is GetFailureBreakdown's: resolve the namespace, resolve and
// validate the window and every other field, and only then consult the
// cache — so a refused request can never collect a previous caller's groups.
func (h *StatsHandler) GetFlakeIndex(
	ctx context.Context,
	req *connect.Request[orkstackv1.GetFlakeIndexRequest],
) (*connect.Response[orkstackv1.GetFlakeIndexResponse], error) {
	msg := req.Msg
	ns, err := h.svc.namespace(ctx, msg.GetNamespace())
	if err != nil {
		return nil, err
	}
	window, err := h.windowOf(ns.GetName(), msg.GetSince(), msg.GetUntil(), msg.GetLookback())
	if err != nil {
		return nil, err
	}
	groupBy, err := engine.ResolveFlakeGroupBy(msg.GetGroupBy())
	if err != nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, err)
	}
	limit, err := resolveFlakeLimit(msg.GetLimit())
	if err != nil {
		return nil, err
	}
	if h.svc.attempts == nil {
		// Unconditionally: the whole read is about attempt records. An engine
		// that cannot look has not found them missing, and must not answer
		// "every task unavailable". The message is ListTaskAttempts' own.
		return nil, connect.NewError(connect.CodeFailedPrecondition,
			fmt.Errorf("task attempt store is not configured"))
	}

	key := flakeKey{
		statsKey: windowKeyFor(ns.GetName(), msg.GetSince(), msg.GetUntil(), msg.GetLookback()),
		orkflow:  msg.GetOrkflowName(),
		groupBy:  joinGroupBy(groupBy),
		limit:    msg.GetLimit(),
	}
	if cached, ok := h.lookupFlakes(key); ok {
		return connect.NewResponse(cached), nil
	}
	computed, err := h.computeFlakes(ctx, window, msg.GetOrkflowName(), groupBy, limit)
	if err != nil {
		return nil, err
	}
	h.storeFlakes(key, computed)
	return connect.NewResponse(cloneFlakes(computed)), nil
}

// resolveFlakeLimit maps 0 to the default and refuses anything outside
// [0, FlakeIndexMaxLimit].
func resolveFlakeLimit(limit int32) (int, error) {
	switch {
	case limit == 0:
		return FlakeIndexDefaultLimit, nil
	case limit < 0 || limit > FlakeIndexMaxLimit:
		return 0, connect.NewError(connect.CodeInvalidArgument, fmt.Errorf(
			"limit %d is outside 0..%d (0 means %d)", limit, FlakeIndexMaxLimit, FlakeIndexDefaultLimit))
	default:
		return int(limit), nil
	}
}

// computeFlakes reads the population's keys, then every body and one attempt
// listing per dispatched task, and folds the samples into groups.
func (h *StatsHandler) computeFlakes(
	ctx context.Context,
	window engine.StatsWindow,
	orkflow string,
	groupBy []orkstackv1.FailureGroupBy,
	limit int,
) (*orkstackv1.GetFlakeIndexResponse, error) {
	lister, ok := h.svc.archive.(FinishedRunIndexLister)
	if !ok || h.svc.archive == nil {
		return nil, errNoArchive()
	}
	entries, err := lister.ListFinishedRunIndex(ctx, engine.ArchiveQuery{
		Namespace:   window.Namespace,
		OrkflowName: orkflow,
		Status:      orkstackv1.Status_STATUS_UNSPECIFIED,
		Since:       window.Since,
		Until:       window.Until,
	})
	switch {
	case errors.Is(err, engine.ErrNoRunArchive):
		return nil, errNoArchive()
	case err != nil:
		return nil, connect.NewError(connect.CodeUnavailable,
			fmt.Errorf("list finished runs in namespace %q: %w", window.Namespace, err))
	}
	if len(entries) > FlakeIndexMaxScan {
		return nil, connect.NewError(connect.CodeResourceExhausted, fmt.Errorf(
			"flake index for namespace %q would read %d finished runs' bodies and attempt "+
				"records, more than the %d this read is bounded to; narrow the window with "+
				"since/until or name one orkflow",
			window.Namespace, len(entries), FlakeIndexMaxScan))
	}

	scanned, samples, err := h.scanFlakes(ctx, entries)
	if err != nil {
		return nil, err
	}
	groups, totals := engine.GroupFlakes(samples, groupBy, limit)
	return &orkstackv1.GetFlakeIndexResponse{
		Groups:               groups,
		RunsInWindow:         int64(len(entries)),
		RunsScanned:          scanned,
		RunsDispatched:       totals.RunsDispatched,
		RunsEvidenceComplete: totals.RunsEvidenceComplete,
		RunsEvidenceMissing:  totals.RunsEvidenceMissing,
		GroupsTotal:          totals.GroupsTotal,
		BodyKeyed:            totals.BodyKeyed,
		GroupBy:              append([]orkstackv1.FailureGroupBy(nil), totals.GroupBy...),
		Namespace:            window.Namespace,
		WindowSince:          timestamppb.New(window.Since),
		WindowUntil:          timestamppb.New(window.Until),
		ComputedAt:           timestamppb.New(h.now()),
	}, nil
}

// scanFlakes reads one body per entry and one attempt listing per dispatched
// task through a bounded worker pool of failureScanConcurrency, and returns
// how many entries had a body and every dispatched task's sample, in the
// index's own order.
//
// An index row with no body is the crash window between the archive's two
// writes and is skipped (runs_scanned < runs_in_window). Any OTHER error —
// an unreadable body, an attempt listing that errors — fails the whole
// answer, naming the run: "could not read" and "was reclaimed" are different
// facts, and only the second is a number.
func (h *StatsHandler) scanFlakes(
	parent context.Context,
	entries []artifacts.RunIndexEntry,
) (int64, []engine.TaskSample, error) {
	ctx, stop := context.WithCancel(parent)
	defer stop()

	type result struct {
		samples []engine.TaskSample
		present bool
		err     error
	}
	results := make([]result, len(entries))
	var (
		wg    sync.WaitGroup
		slots = make(chan struct{}, failureScanConcurrency)
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
			samples, present, err := h.sampleRun(ctx, entry)
			results[i] = result{samples: samples, present: present, err: err}
			if err != nil {
				stop()
			}
		}(i, entry)
	}
	wg.Wait()

	// The caller going away is its own answer, checked first: every result
	// after it may be a casualty of it rather than a fact.
	if err := parent.Err(); err != nil {
		return 0, nil, connect.NewError(connect.CodeCanceled, err)
	}
	// A real failure is reported in preference to the reads it cancelled, and
	// the first in index order, so the error names the run that caused it.
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
	samples := []engine.TaskSample{}
	for _, result := range results {
		if result.present {
			scanned++
			samples = append(samples, result.samples...)
		}
	}
	return scanned, samples, nil
}

// sampleRun reads one finished run and the attempt records of each task it
// dispatched. A task that was never dispatched is never listed. present is
// false only for an index row with no body.
func (h *StatsHandler) sampleRun(
	ctx context.Context,
	entry artifacts.RunIndexEntry,
) ([]engine.TaskSample, bool, error) {
	run, err := h.svc.archive.LoadRun(ctx, entry.RunID)
	switch {
	case errors.Is(err, artifacts.ErrNotFound):
		return nil, false, nil
	case err != nil:
		return nil, false, connect.NewError(connect.CodeUnavailable,
			fmt.Errorf("read the archived body of run %q: %w", entry.RunID, err))
	}
	var samples []engine.TaskSample
	for _, task := range run.GetTaskRuns() {
		if !engine.Dispatched(task) {
			continue
		}
		records, err := h.svc.attempts.ListAttempts(ctx, entry.RunID, task.GetTaskId())
		if err != nil {
			return nil, false, connect.NewError(connect.CodeUnavailable,
				fmt.Errorf("read attempt records of task %q in run %q: %w", task.GetTaskId(), entry.RunID, err))
		}
		samples = append(samples, engine.SampleTask(entry, run, task, records))
	}
	return samples, true, nil
}

// lookupFlakes returns a cached answer, or reports that there is none.
func (h *StatsHandler) lookupFlakes(key flakeKey) (*orkstackv1.GetFlakeIndexResponse, bool) {
	if h.ttl <= 0 {
		return nil, false
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	entry, ok := h.flakes[key]
	if !ok || !h.now().Before(entry.expires) {
		return nil, false
	}
	return cloneFlakes(entry.response), true
}

// storeFlakes keeps one whole answer, on store's terms: cloned in, expired
// entries swept, the map cleared at statsCacheMaxEntries, never a merge.
func (h *StatsHandler) storeFlakes(key flakeKey, response *orkstackv1.GetFlakeIndexResponse) {
	if h.ttl <= 0 {
		return
	}
	now := h.now()
	h.mu.Lock()
	defer h.mu.Unlock()
	for existing, entry := range h.flakes {
		if !now.Before(entry.expires) {
			delete(h.flakes, existing)
		}
	}
	if len(h.flakes) >= statsCacheMaxEntries {
		clear(h.flakes)
	}
	h.flakes[key] = flakeEntry{response: cloneFlakes(response), expires: now.Add(h.ttl)}
}

func cloneFlakes(response *orkstackv1.GetFlakeIndexResponse) *orkstackv1.GetFlakeIndexResponse {
	return proto.Clone(response).(*orkstackv1.GetFlakeIndexResponse)
}
