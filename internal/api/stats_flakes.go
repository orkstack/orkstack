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
// exactly the kind a SUCCEEDED status hides. It also reads the attempt
// records of every dispatched task and fan-out item — the one aggregation in
// the tree that reads attempt records ACROSS runs — with one run-wide listing
// per run where the store offers one.
//
// The crossing is bounded the way the breakdown's is — by the window, by an
// optional orkflow, and by FlakeIndexMaxScan — and once more by
// FlakeIndexMaxRows, the dispatched rows summed from the bodies before any
// attempt read. Past either cap the call FAILS rather than answering from a
// subset. What docs/STATS.md §1
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
	// FlakeIndexMaxRows is the most dispatched rows — tasks, plus each item
	// of a fan-out — whose attempt records one flake index will read. It is
	// summed from the bodies before any attempt record is read, because the
	// run cap alone does not bound a run holding a 1000-item fan-out.
	FlakeIndexMaxRows = 50000
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

// computeFlakes reads the population's keys, then every body, then the
// attempt records of every dispatched row, and folds the samples into groups.
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

	scanned, samples, err := h.scanFlakes(ctx, window.Namespace, entries)
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

// scanFlakes reads the population in two phases, each through a bounded
// worker pool of failureScanConcurrency, and returns how many entries had a
// body and every dispatched task's sample, in the index's own order.
//
// The first phase reads every body and keeps only the dispatched rows (tasks,
// and a fan-out's items). Their sum is the attempt read's size, and it is
// checked against FlakeIndexMaxRows BEFORE any attempt record is read: a run
// cap alone bounds nothing when one run holds a 1000-item fan-out. The second
// phase reads each run's attempt records — with ONE run-wide listing when the
// store can (artifacts.ReadRunAttempts), and one listing per dispatched row
// otherwise.
//
// An index row with no body is the crash window between the archive's two
// writes and is skipped (runs_scanned < runs_in_window). Any OTHER error —
// an unreadable body, an attempt listing that errors — fails the whole
// answer, naming the run: "could not read" and "was reclaimed" are different
// facts, and only the second is a number.
func (h *StatsHandler) scanFlakes(
	ctx context.Context,
	namespace string,
	entries []artifacts.RunIndexEntry,
) (int64, []engine.TaskSample, error) {
	bodies := make([]flakeBody, len(entries))
	err := scanPool(ctx, len(entries), func(ctx context.Context, i int) error {
		body, err := h.flakeBodyOf(ctx, entries[i])
		bodies[i] = body
		return err
	})
	if err != nil {
		return 0, nil, err
	}

	var scanned, rows int64
	for _, body := range bodies {
		if body.present {
			scanned++
			rows += int64(len(body.rows))
		}
	}
	if rows > FlakeIndexMaxRows {
		return 0, nil, connect.NewError(connect.CodeResourceExhausted, fmt.Errorf(
			"flake index for namespace %q would read the attempt records of %d dispatched tasks and "+
				"fan-out items, more than the %d this read is bounded to; narrow the window with "+
				"since/until or name one orkflow",
			namespace, rows, FlakeIndexMaxRows))
	}

	results := make([][]engine.TaskSample, len(entries))
	err = scanPool(ctx, len(entries), func(ctx context.Context, i int) error {
		if !bodies[i].present {
			return nil
		}
		samples, err := h.sampleRun(ctx, entries[i], bodies[i])
		results[i] = samples
		return err
	})
	if err != nil {
		return 0, nil, err
	}
	samples := []engine.TaskSample{}
	for _, result := range results {
		samples = append(samples, result...)
	}
	return scanned, samples, nil
}

// scanPool calls read(ctx, i) for every i in [0, n) through a bounded worker
// pool of failureScanConcurrency. The first error cancels the reads after it.
// The caller going away is reported first, as Canceled; otherwise the first
// real error in index order wins over the reads it cancelled.
func scanPool(parent context.Context, n int, read func(ctx context.Context, i int) error) error {
	ctx, stop := context.WithCancel(parent)
	defer stop()

	errs := make([]error, n)
	var (
		wg    sync.WaitGroup
		slots = make(chan struct{}, failureScanConcurrency)
	)
	for i := 0; i < n; i++ {
		if ctx.Err() != nil {
			break
		}
		wg.Add(1)
		slots <- struct{}{}
		go func(i int) {
			defer wg.Done()
			defer func() { <-slots }()
			if err := read(ctx, i); err != nil {
				errs[i] = err
				stop()
			}
		}(i)
	}
	wg.Wait()

	if err := parent.Err(); err != nil {
		return connect.NewError(connect.CodeCanceled, err)
	}
	var casualty error
	for _, err := range errs {
		switch {
		case err == nil:
		case errors.Is(err, context.Canceled):
			if casualty == nil {
				casualty = err
			}
		default:
			return err
		}
	}
	return casualty
}

// flakeBody is what the flake index keeps of one archived body: its orkflow
// and its dispatched rows, trimmed to the fields SampleTask reads. present is
// false only for an index row with no body.
type flakeBody struct {
	present bool
	orkflow string
	rows    []*orkstackv1.TaskRun
}

// flakeBodyOf reads one finished run's body and keeps its dispatched rows. A
// task that was never dispatched is never listed; a fan-out's own row is no
// sample, and each of its items is one.
func (h *StatsHandler) flakeBodyOf(ctx context.Context, entry artifacts.RunIndexEntry) (flakeBody, error) {
	run, err := h.svc.archive.LoadRun(ctx, entry.RunID)
	switch {
	case errors.Is(err, artifacts.ErrNotFound):
		return flakeBody{}, nil
	case err != nil:
		return flakeBody{}, connect.NewError(connect.CodeUnavailable,
			fmt.Errorf("read the archived body of run %q: %w", entry.RunID, err))
	}
	body := flakeBody{present: true, orkflow: run.GetOrkflowName()}
	for _, task := range run.GetTaskRuns() {
		rows := []*orkstackv1.TaskRun{task}
		if task.GetEach() != nil {
			rows = task.GetItems()
		}
		for _, row := range rows {
			if !engine.Dispatched(row) {
				continue
			}
			body.rows = append(body.rows, &orkstackv1.TaskRun{
				TaskId: row.GetTaskId(), Pool: row.GetPool(), Attempt: row.GetAttempt(), Item: row.Item,
			})
		}
	}
	return body, nil
}

// sampleRun reads the attempt records of each row one run dispatched: with
// one run-wide listing when the store implements artifacts.RunAttemptReader
// (every durable backend does), and one listing per row otherwise.
func (h *StatsHandler) sampleRun(
	ctx context.Context,
	entry artifacts.RunIndexEntry,
	body flakeBody,
) ([]engine.TaskSample, error) {
	run := &orkstackv1.Run{Id: entry.RunID, OrkflowName: body.orkflow}
	samples := make([]engine.TaskSample, 0, len(body.rows))
	if reader, ok := h.svc.attempts.(artifacts.RunAttemptReader); ok && len(body.rows) > 0 {
		grouped, err := artifacts.ReadRunAttempts(ctx, reader, entry.RunID, artifacts.RunAttemptReadConcurrency)
		if err != nil {
			return nil, connect.NewError(connect.CodeUnavailable,
				fmt.Errorf("read attempt records of run %q: %w", entry.RunID, err))
		}
		for _, row := range body.rows {
			scope := artifacts.AttemptScope{TaskID: row.GetTaskId(), Item: artifacts.ItemOf(row.Item)}
			samples = append(samples, engine.SampleTask(entry, run, row, grouped[scope]))
		}
		return samples, nil
	}
	for _, row := range body.rows {
		records, err := h.svc.attempts.ListAttempts(ctx, entry.RunID, row.GetTaskId(), artifacts.ItemOf(row.Item))
		if err != nil {
			return nil, connect.NewError(connect.CodeUnavailable,
				fmt.Errorf("read attempt records of task %q in run %q: %w", row.GetTaskId(), entry.RunID, err))
		}
		samples = append(samples, engine.SampleTask(entry, run, row, records))
	}
	return samples, nil
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
