package api

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// The failure breakdown is the one stats read that opens run BODIES.
//
// GetStats never does (docs/STATS.md §1): its cost is a function of how many
// runs finished, never of what they hold. The facts that tell one failure from
// another — the task that failed, its code and category, its pool and Orker —
// live in the archived snapshot and not in the index key, and the image and
// phase live only in the attempt records. So this read crosses that line,
// once, in one place, and bounds the crossing three ways instead: by the
// window, by the failed subset of it, and by FailureBreakdownMaxScan, past
// which the call FAILS rather than answering from a subset. A partial census
// reads exactly like a whole one, and is the failure mode this exists to
// prevent — never clamp or paginate it.

const (
	// FailureBreakdownDefaultLimit is how many groups a request that names no
	// limit gets, worst first.
	FailureBreakdownDefaultLimit = 50
	// FailureBreakdownMaxLimit caps a named limit. It bounds the answer, never
	// the scan: the totals are always over every group.
	FailureBreakdownMaxLimit = 500
	// FailureBreakdownMaxScan is the most failed-run bodies one breakdown will
	// read. A window whose failed population is larger is refused, with the
	// bound and the population in the message, so the caller knows what to
	// narrow. It is a constant on MostFailingLimit's precedent, not a setting.
	FailureBreakdownMaxScan = 5000
	// failureScanConcurrency bounds parallel body and attempt reads — the
	// number engine.archiveFetchConcurrency uses for the same shape of work.
	failureScanConcurrency = 8
)

// DefaultFailureGroupBy is the fingerprint an empty group_by resolves to.
var DefaultFailureGroupBy = engine.DefaultFailureGroupBy()

// breakdownKey identifies one breakdown answer: the namespace and window AS
// SENT (statsKey's rule, for statsKey's reasons), and every other field of
// the question. Two requests that differ in any of them are two questions.
type breakdownKey struct {
	statsKey
	orkflow  string
	groupBy  string // the resolved dimensions, joined in request order
	category orkstackv1.FailureCategory
	limit    int32 // as sent, 0 kept as 0
}

type breakdownEntry struct {
	response *orkstackv1.GetFailureBreakdownResponse
	expires  time.Time
}

// GetFailureBreakdown groups one namespace's failed runs over one window by
// the dimensions the caller names.
//
// The order is GetStats': resolve the namespace, resolve and validate the
// window and every other field, and only then consult the cache — so a
// refused request can never collect a previous caller's groups.
func (h *StatsHandler) GetFailureBreakdown(
	ctx context.Context,
	req *connect.Request[orkstackv1.GetFailureBreakdownRequest],
) (*connect.Response[orkstackv1.GetFailureBreakdownResponse], error) {
	msg := req.Msg
	ns, err := h.svc.namespace(ctx, msg.GetNamespace())
	if err != nil {
		return nil, err
	}
	window, err := h.windowOf(ns.GetName(), msg.GetSince(), msg.GetUntil(), msg.GetLookback())
	if err != nil {
		return nil, err
	}
	groupBy, err := resolveFailureGroupBy(msg.GetGroupBy())
	if err != nil {
		return nil, err
	}
	limit, err := resolveFailureLimit(msg.GetLimit())
	if err != nil {
		return nil, err
	}
	if _, known := orkstackv1.FailureCategory_name[int32(msg.GetCategory())]; !known {
		return nil, connect.NewError(connect.CodeInvalidArgument, fmt.Errorf(
			"category %d is not a failure category", msg.GetCategory()))
	}
	attemptRead := engine.NeedsAttemptEvidence(groupBy)
	if attemptRead && h.svc.attempts == nil {
		// Not "every image unavailable": an engine that cannot look has not
		// found the records missing, and the two must not render alike. The
		// message is ListTaskAttempts' own.
		return nil, connect.NewError(connect.CodeFailedPrecondition,
			fmt.Errorf("task attempt store is not configured"))
	}

	key := breakdownKey{
		statsKey: windowKeyFor(ns.GetName(), msg.GetSince(), msg.GetUntil(), msg.GetLookback()),
		orkflow:  msg.GetOrkflowName(),
		groupBy:  joinGroupBy(groupBy),
		category: msg.GetCategory(),
		limit:    msg.GetLimit(),
	}
	if cached, ok := h.lookupBreakdown(key); ok {
		return connect.NewResponse(cached), nil
	}
	computed, err := h.computeBreakdown(ctx, window, msg.GetOrkflowName(), groupBy,
		msg.GetCategory(), limit, attemptRead)
	if err != nil {
		return nil, err
	}
	h.storeBreakdown(key, computed)
	return connect.NewResponse(cloneBreakdown(computed)), nil
}

// resolveFailureGroupBy validates the requested dimensions and resolves an
// empty list to the default. The caller's order is kept: it is echoed and
// decides column order in clients, never the grouping itself.
func resolveFailureGroupBy(requested []orkstackv1.FailureGroupBy) ([]orkstackv1.FailureGroupBy, error) {
	if len(requested) == 0 {
		return engine.DefaultFailureGroupBy(), nil
	}
	seen := make(map[orkstackv1.FailureGroupBy]bool, len(requested))
	out := make([]orkstackv1.FailureGroupBy, 0, len(requested))
	for _, dim := range requested {
		if _, known := orkstackv1.FailureGroupBy_name[int32(dim)]; !known {
			return nil, connect.NewError(connect.CodeInvalidArgument,
				fmt.Errorf("group_by value %d is not a failure dimension", int32(dim)))
		}
		if dim == orkstackv1.FailureGroupBy_FAILURE_GROUP_BY_UNSPECIFIED {
			return nil, connect.NewError(connect.CodeInvalidArgument,
				fmt.Errorf("group_by names %s, which is no dimension", dim))
		}
		if seen[dim] {
			return nil, connect.NewError(connect.CodeInvalidArgument,
				fmt.Errorf("group_by names %s twice", dim))
		}
		seen[dim] = true
		out = append(out, dim)
	}
	return out, nil
}

// resolveFailureLimit maps 0 to the default and refuses anything outside
// [0, FailureBreakdownMaxLimit].
func resolveFailureLimit(limit int32) (int, error) {
	switch {
	case limit == 0:
		return FailureBreakdownDefaultLimit, nil
	case limit < 0 || limit > FailureBreakdownMaxLimit:
		return 0, connect.NewError(connect.CodeInvalidArgument, fmt.Errorf(
			"limit %d is outside 0..%d (0 means %d)",
			limit, FailureBreakdownMaxLimit, FailureBreakdownDefaultLimit))
	default:
		return int(limit), nil
	}
}

func joinGroupBy(groupBy []orkstackv1.FailureGroupBy) string {
	parts := make([]string, len(groupBy))
	for i, dim := range groupBy {
		parts[i] = dim.String()
	}
	return strings.Join(parts, ",")
}

// computeBreakdown reads the population's keys, then its bodies (and, when
// asked, its attempt records), and folds them into groups.
func (h *StatsHandler) computeBreakdown(
	ctx context.Context,
	window engine.StatsWindow,
	orkflow string,
	groupBy []orkstackv1.FailureGroupBy,
	category orkstackv1.FailureCategory,
	limit int,
	attemptRead bool,
) (*orkstackv1.GetFailureBreakdownResponse, error) {
	lister, ok := h.svc.archive.(RunIndexLister)
	if !ok || h.svc.archive == nil {
		return nil, errNoArchive()
	}
	entries, err := lister.ListRunIndex(ctx, engine.ArchiveQuery{
		Namespace:   window.Namespace,
		OrkflowName: orkflow,
		Since:       window.Since,
		Until:       window.Until,
	})
	switch {
	// A lister that exists as a type but holds no store: TallyRuns' case.
	case errors.Is(err, engine.ErrNoRunArchive):
		return nil, errNoArchive()
	case err != nil:
		return nil, connect.NewError(connect.CodeUnavailable,
			fmt.Errorf("list failed runs in namespace %q: %w", window.Namespace, err))
	}
	if len(entries) > FailureBreakdownMaxScan {
		return nil, connect.NewError(connect.CodeResourceExhausted, fmt.Errorf(
			"failure breakdown for namespace %q would read %d failed runs' bodies, more "+
				"than the %d this read is bounded to; narrow the window with since/until or "+
				"name one orkflow",
			window.Namespace, len(entries), FailureBreakdownMaxScan))
	}

	records, err := h.scanFailures(ctx, entries, attemptRead)
	if err != nil {
		return nil, err
	}
	groups, totals := engine.GroupFailures(records, groupBy, category, limit)
	return &orkstackv1.GetFailureBreakdownResponse{
		Groups:                 groups,
		FailedRunsInWindow:     int64(len(entries)),
		FailedRunsScanned:      int64(len(records)),
		FailedRunsGrouped:      totals.Grouped,
		GroupsTotal:            totals.GroupsTotal,
		AttemptEvidenceMissing: totals.AttemptEvidenceMissing,
		AttemptEvidenceRead:    attemptRead,
		GroupBy:                append([]orkstackv1.FailureGroupBy(nil), groupBy...),
		Namespace:              window.Namespace,
		WindowSince:            timestamppb.New(window.Since),
		WindowUntil:            timestamppb.New(window.Until),
		ComputedAt:             timestamppb.New(h.now()),
	}, nil
}

// scanFailures reads one body per entry — and, when attemptRead, the primary
// task's attempt records — through a bounded worker pool, and returns one
// fingerprint per run that had a body, in the index's own order.
//
// An index row with no body is the crash window between the archive's two
// writes, and is skipped as RunArchiver.ListRuns skips it; the response shows
// it as failed_runs_scanned < failed_runs_in_window. Any OTHER error fails the
// whole answer, naming the run: a census with a hole in it is not a census.
func (h *StatsHandler) scanFailures(
	parent context.Context,
	entries []artifacts.RunIndexEntry,
	attemptRead bool,
) ([]engine.FailureRecord, error) {
	// The first failure stops the rest: nothing read after it can be used.
	ctx, stop := context.WithCancel(parent)
	defer stop()

	type result struct {
		record  engine.FailureRecord
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
			record, present, err := h.fingerprint(ctx, entry, attemptRead)
			results[i] = result{record: record, present: present, err: err}
			if err != nil {
				stop()
			}
		}(i, entry)
	}
	wg.Wait()

	// The caller going away is its own answer, and it is checked first: every
	// result after it may be a casualty of it rather than a fact.
	if err := parent.Err(); err != nil {
		return nil, connect.NewError(connect.CodeCanceled, err)
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
		return nil, result.err
	}
	if casualty != nil {
		return nil, casualty
	}

	records := make([]engine.FailureRecord, 0, len(entries))
	for _, result := range results {
		if result.present {
			records = append(records, result.record)
		}
	}
	return records, nil
}

// fingerprint reads one failed run and, when asked, its primary task's
// attempt records. present is false only for an index row with no body.
func (h *StatsHandler) fingerprint(
	ctx context.Context,
	entry artifacts.RunIndexEntry,
	attemptRead bool,
) (engine.FailureRecord, bool, error) {
	run, err := h.svc.archive.LoadRun(ctx, entry.RunID)
	switch {
	case errors.Is(err, artifacts.ErrNotFound):
		return engine.FailureRecord{}, false, nil
	case err != nil:
		return engine.FailureRecord{}, false, connect.NewError(connect.CodeUnavailable,
			fmt.Errorf("read the archived body of failed run %q: %w", entry.RunID, err))
	}
	var attempt *orkstackv1.TaskAttempt
	if attemptRead {
		if task := engine.PrimaryFailedTask(run); task != nil {
			records, err := h.svc.attempts.ListAttempts(ctx, entry.RunID, task.GetTaskId(), artifacts.NoItem)
			if err != nil {
				return engine.FailureRecord{}, false, connect.NewError(connect.CodeUnavailable,
					fmt.Errorf("read attempt records of task %q in failed run %q: %w",
						task.GetTaskId(), entry.RunID, err))
			}
			attempt = engine.FinalAttemptRecord(task, records)
		}
	}
	return engine.FingerprintOf(entry, run, attempt, attemptRead), true, nil
}

// lookupBreakdown returns a cached answer, or reports that there is none.
func (h *StatsHandler) lookupBreakdown(key breakdownKey) (*orkstackv1.GetFailureBreakdownResponse, bool) {
	if h.ttl <= 0 {
		return nil, false
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	entry, ok := h.breakdowns[key]
	if !ok || !h.now().Before(entry.expires) {
		return nil, false
	}
	return cloneBreakdown(entry.response), true
}

// storeBreakdown keeps one whole answer, on store's terms: cloned in, expired
// entries swept, the map cleared at statsCacheMaxEntries, never a merge.
func (h *StatsHandler) storeBreakdown(key breakdownKey, response *orkstackv1.GetFailureBreakdownResponse) {
	if h.ttl <= 0 {
		return
	}
	now := h.now()
	h.mu.Lock()
	defer h.mu.Unlock()
	for existing, entry := range h.breakdowns {
		if !now.Before(entry.expires) {
			delete(h.breakdowns, existing)
		}
	}
	if len(h.breakdowns) >= statsCacheMaxEntries {
		clear(h.breakdowns)
	}
	h.breakdowns[key] = breakdownEntry{response: cloneBreakdown(response), expires: now.Add(h.ttl)}
}

func cloneBreakdown(response *orkstackv1.GetFailureBreakdownResponse) *orkstackv1.GetFailureBreakdownResponse {
	return proto.Clone(response).(*orkstackv1.GetFailureBreakdownResponse)
}
