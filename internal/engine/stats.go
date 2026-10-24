package engine

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"time"

	"go.temporal.io/api/serviceerror"
	workflowpb "go.temporal.io/api/workflow/v1"
	"go.temporal.io/api/workflowservice/v1"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/task"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// A dashboard's numbers come from the two records that already exist, and from
// nothing else. There is no counter, no rollup and no write path: a stat is
// derived when it is asked for, or it is not reported.
//
//	live runs, and how long finished ones took   the cluster's execution index
//	how many runs finished, and how they ended   the run archive's INDEX KEYS
//
// The split is not arbitrary. The cluster holds an execution's timing but only
// while it is inside retention; the archive holds every run that ever finished
// but its index key carries a run's namespace, orkflow, status and time and no
// duration at all. So counts come from the archive, which is complete, and
// percentiles come from the cluster, which is timed — and the answer says
// which window the percentiles actually cover rather than presenting a partial
// one as whole.
//
// Neither source is this process's memory. Two replicas therefore compute the
// same numbers from the same bytes, including a replica that started nothing
// (AGENTS.md §7).

const (
	// statsPageSize is one page of the execution index.
	statsPageSize = 1000

	// maxStatsPages bounds a walk whose namespace the cluster applied itself.
	// Every record it reads already belongs to the namespace that asked, so
	// the bound is about this deployment's own volume.
	maxStatsPages = 100

	// maxStatsScanPages bounds the walk taken when the namespace could NOT be
	// pushed into the query and every other namespace's records have to be
	// read and discarded. It is the same reasoning as ListRuns' own scan
	// bound: a tenancy filter must not be answered by "as far as the budget
	// got", so the walk is generous and running out of it is a failure.
	maxStatsScanPages = 2000

	// statsTimeLayout renders a time bound for the index's query language:
	// UTC, RFC 3339, nanosecond precision.
	statsTimeLayout = "2006-01-02T15:04:05.000000000Z"
)

// StatsWindow is one stats question: a namespace, and a half-open time range.
//
// Since is inclusive and Until exclusive, so consecutive windows tile without
// counting a run twice.
type StatsWindow struct {
	Namespace string
	Since     time.Time
	Until     time.Time
}

// Executions is what the cluster's execution index says about one window.
//
// Running is a fact about NOW rather than about the window: a run that started
// last week and is still going is running, and a dashboard that hid it because
// it began before the window would be hiding the run most worth seeing. Until
// still applies as an upper bound, so a window that ends in the past cannot
// report a run that had not started by then.
//
// Durations holds one sample per run the window CREATED and that has since
// finished, sorted ascending, ready for a percentile. Created, not closed:
// that is the same population the archive counts, and the two halves of one
// answer must be about one set of runs (see stats.proto, GetStatsResponse).
type Executions struct {
	Running   []*orkstackv1.Run
	Durations []time.Duration
}

// ErrNoRunArchive reports a stats question asked of a process with no run
// archive wired. It is not "nothing finished": it is "this process cannot
// tell", and the two need opposite reactions — look at the runs, or configure
// the deployment.
var ErrNoRunArchive = fmt.Errorf("no run archive is configured")

// ExecutionReader is the cluster half of a stats answer.
//
// It is declared beside RunReader and satisfied by the same type on purpose.
// A stats source that could be wired to a different cluster from the run
// reader would answer about runs the reader cannot see; travelling together
// makes that unrepresentable (see Binding).
type ExecutionReader interface {
	Executions(ctx context.Context, w StatsWindow) (Executions, error)
}

// Executions reports the runs executing now and how long the window's own
// finished runs took, both from the cluster's execution index.
//
// Nothing here reads a run's history: a page of the index carries the status,
// the times and the memo this needs, and reading history would cost one call
// per run — the cost the whole service is built to avoid.
func (r *TemporalRunReader) Executions(ctx context.Context, w StatsWindow) (Executions, error) {
	if w.Since.IsZero() || w.Until.IsZero() {
		return Executions{}, fmt.Errorf(
			"engine: stats window needs both bounds (since=%v until=%v)", w.Since, w.Until)
	}
	running, err := r.runningExecutions(ctx, w)
	if err != nil {
		return Executions{}, err
	}
	durations, err := r.windowRunDurations(ctx, w)
	if err != nil {
		return Executions{}, err
	}
	return Executions{Running: running, Durations: durations}, nil
}

// runningExecutions lists the namespace's unfinished runs as shallow
// projections, newest first.
func (r *TemporalRunReader) runningExecutions(
	ctx context.Context,
	w StatsWindow,
) ([]*orkstackv1.Run, error) {
	// Live runs, a chain caught between its two records included: one row
	// per run, its ContinuedAsNew record settled (visibility.go).
	query := fmt.Sprintf("WorkflowType = %q AND %s", RunWorkflowType, liveRunPredicate(r.clock()))
	var records []*workflowpb.WorkflowExecutionInfo
	err := r.walkExecutions(ctx, w, query, func(info *workflowpb.WorkflowExecutionInfo) {
		if inNamespace(r.memoString(info.GetMemo(), MemoNamespace), w.Namespace) {
			records = append(records, info)
		}
	})
	if err != nil {
		return nil, err
	}
	records, _, err = r.resolveChains(ctx, records)
	if err != nil {
		return nil, err
	}
	out := []*orkstackv1.Run{}
	for _, info := range records {
		run := r.listedRun(info)
		if started := run.GetCreatedAt(); started != nil && !started.AsTime().Before(w.Until) {
			// It had not started by the end of the window this answer is
			// about, so it is not part of it.
			continue
		}
		out = append(out, run)
	}
	// Newest first, stably: the index already answers in start-time order, and
	// a stable sort therefore leaves two replicas with the same list. Nothing
	// falls back to comparing run ids — an id carries no order (AGENTS.md
	// §1.5) and inventing one here would be the third place in the tree to do
	// it.
	sort.SliceStable(out, func(i, j int) bool {
		return runStartTime(out[i]).After(runStartTime(out[j]))
	})
	return out, nil
}

// RunningCount is how many runs of namespace are in flight now, from the
// cluster's execution index — the same population runningExecutions lists
// for GetStats.running, asked as a count.
//
// When the cluster indexes a run's namespace it is one CountWorkflow call
// with the namespace as a predicate. Otherwise the cluster cannot count a
// namespace it does not index, and countRunning walks the index filtering on
// the memo instead.
//
// A count that cannot be read is an error, never zero: reporting zero runs in
// flight is exactly the number a cap is built to distrust. It is never cached.
//
// A run that continued as new is ONE run however the index shows it — both
// of its executions Running for a moment, or only the one that just closed
// (ContinuedAsNew) — so the count is of distinct live runs (liveRunCount).
// ceiling is the namespace's max_running: below it at most two CountWorkflow
// calls answer, and at or above it a deduplicated, settled walk decides, so a
// switching run neither refuses a submit at max_running − 1 nor is read as
// gone. A walk, list or describe that fails returns the error.
func (r *TemporalRunReader) RunningCount(ctx context.Context, namespace string, ceiling int64) (int64, error) {
	if !r.namespaceIndexed {
		return r.countRunning(ctx, namespace, ceiling)
	}
	base := fmt.Sprintf("WorkflowType = %q AND %s = %q",
		RunWorkflowType, task.SearchAttrOrkflowNamespace, artifacts.ResolveNamespace(namespace))
	return r.liveRunCount(ctx, base, ceiling)
}

// countRunning is the unindexed half of RunningCount: the live-run query
// runningExecutions issues, walked to the end (the scan bound, failing
// closed) keeping only the records whose memo namespace matches, then
// deduplicated by workflow id and settled, so a chain counts once. No window,
// no sort, no projection — the cluster is asked how many, not which.
func (r *TemporalRunReader) countRunning(ctx context.Context, namespace string, ceiling int64) (int64, error) {
	namespace = artifacts.ResolveNamespace(namespace)
	query := fmt.Sprintf("WorkflowType = %q AND %s", RunWorkflowType, liveRunPredicate(r.clock()))
	return r.walkLiveRuns(ctx, query, func(info *workflowpb.WorkflowExecutionInfo) bool {
		return inNamespace(r.memoString(info.GetMemo(), MemoNamespace), namespace)
	}, ceiling)
}

// windowRunDurations samples how long each run the window CREATED took, for
// the ones that have finished, ascending.
//
// The population is the window's own runs — those whose START falls inside it
// — and not those that happened to CLOSE inside it. That was the defect: the
// archive's index stamps a run by its creation time (RunIndexTime), so
// runs_by_status, success_rate and most_failing count the runs a window
// created, while these percentiles counted the runs it closed. One answer
// carried two populations and said nothing about the difference:
// `orkctl stats --since 15m` over hour-long runs reported no finished runs at
// all beside a p50 of 58 minutes over twelve of them.
//
// Two consequences, both deliberate:
//
//   - A run created inside the window and still going is sampled by nothing
//     and counted as running, which is what runs_by_status says about it too.
//   - A window in the past keeps filling in as its runs end, exactly as its
//     archive counts do. A pinned window is a question about a set of runs,
//     not a photograph of an instant.
//
// The time bound is pushed into the query, so the cluster reads its own index
// by start time instead of handing over every execution it holds. The duration
// is the record's own ExecutionDuration where it has one, and close minus
// start otherwise — the same record either way, never a second call.
//
// A run's creation time and the cluster's StartTime are the two ends of one
// StartRun call, so they differ by the milliseconds that call took: a run
// submitted within that skew of a boundary can be counted on one side of it by
// the archive and sampled on the other by the cluster. Nothing closer exists —
// the cluster does not store the engine's created_at — and the alternative
// (two different populations) is what this replaced.
//
// A run that continued as new is one run with one record per execution. Its
// created_at is the memo the first switch wrote, and its duration is its
// final record's CloseTime minus that created_at. So:
//
//  1. Query A is the window's records that are not a closed generation
//     (ExecutionStatus != "ContinuedAsNew" — this is a duration sample, not
//     a liveness question). Membership is the memo created_at when present,
//     else StartTime; the sample is CloseTime − created_at when the memo is
//     present, else the record's own duration.
//  2. Query B finds the chains created in the window whose final generation
//     started after it (so A never saw them): the ContinuedAsNew records that
//     started in the window, whose memo created_at is in it, not sampled by
//     A. They are settled with one WorkflowId IN (…) listing per 100 ids: a
//     latest record closed and not ContinuedAsNew is sampled; only an id
//     still latest-ContinuedAsNew is described. This is bounded by the chains
//     created in the window, and exact for as long as cluster retention holds
//     generation 0's record.
func (r *TemporalRunReader) windowRunDurations(
	ctx context.Context,
	w StatsWindow,
) ([]time.Duration, error) {
	inWindow := func(at time.Time) bool { return !at.Before(w.Since) && at.Before(w.Until) }
	query := fmt.Sprintf("WorkflowType = %q AND StartTime >= %q AND StartTime < %q AND ExecutionStatus != %q",
		RunWorkflowType, statsTime(w.Since), statsTime(w.Until), executionStatusContinuedAsNew)
	out := []time.Duration{}
	sampled := map[string]bool{}
	err := r.walkExecutions(ctx, w, query, func(info *workflowpb.WorkflowExecutionInfo) {
		if !inNamespace(r.memoString(info.GetMemo(), MemoNamespace), w.Namespace) {
			return
		}
		// The bound is checked here as well as in the query. A cluster that
		// answered a predicate loosely would otherwise widen the population
		// silently, and this is the half of the answer that has no visible
		// length.
		if started := info.GetStartTime(); started == nil ||
			started.AsTime().Before(w.Since) || !started.AsTime().Before(w.Until) {
			return
		}
		created, carried := r.memoTime(info.GetMemo(), MemoCreatedAt)
		if carried && !inWindow(created) {
			// A chain created before the window whose final generation
			// started inside it: the window did not create it.
			return
		}
		sampled[info.GetExecution().GetWorkflowId()] = true
		// Unfinished runs come back from this query too — the window's own
		// runs are a mix — and one has no duration to sample.
		if info.GetCloseTime() == nil || isContinuedAsNew(info) {
			return
		}
		sample := executionDuration(info)
		if carried {
			sample = info.GetCloseTime().AsTime().Sub(created)
		}
		if sample > 0 {
			out = append(out, sample)
		}
	})
	if err != nil {
		return nil, err
	}
	chains, err := r.windowChainDurations(ctx, w, sampled)
	if err != nil {
		return nil, err
	}
	out = append(out, chains...)
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out, nil
}

// windowChainDurations is windowRunDurations' Query B: the chains the window
// created whose final generation started after it.
func (r *TemporalRunReader) windowChainDurations(
	ctx context.Context,
	w StatsWindow,
	sampled map[string]bool,
) ([]time.Duration, error) {
	query := fmt.Sprintf("WorkflowType = %q AND ExecutionStatus = %q AND StartTime >= %q AND StartTime < %q",
		RunWorkflowType, executionStatusContinuedAsNew, statsTime(w.Since), statsTime(w.Until))
	created := map[string]time.Time{}
	var ids []string
	err := r.walkExecutions(ctx, w, query, func(info *workflowpb.WorkflowExecutionInfo) {
		if !inNamespace(r.memoString(info.GetMemo(), MemoNamespace), w.Namespace) {
			return
		}
		id := info.GetExecution().GetWorkflowId()
		at, ok := r.memoTime(info.GetMemo(), MemoCreatedAt)
		if !ok || at.Before(w.Since) || !at.Before(w.Until) || sampled[id] {
			return
		}
		if _, seen := created[id]; !seen {
			ids = append(ids, id)
		}
		created[id] = at
	})
	if err != nil || len(ids) == 0 {
		return nil, err
	}
	latest, err := r.listChainRecords(ctx, ids)
	if err != nil {
		return nil, err
	}
	var out []time.Duration
	for _, id := range ids {
		info, ok := latest[id]
		if !ok || isContinuedAsNew(info) {
			// Still caught mid-switch: ask the chain itself.
			resp, err := r.client.DescribeWorkflowExecution(ctx, id, "")
			if err != nil {
				var notFound *serviceerror.NotFound
				if errors.As(err, &notFound) {
					continue
				}
				return nil, fmt.Errorf("engine: describe run %s for the stats window: %w", id, err)
			}
			info = resp.GetWorkflowExecutionInfo()
		}
		if info.GetCloseTime() == nil || isContinuedAsNew(info) || isOpenExecution(info) {
			continue
		}
		if sample := info.GetCloseTime().AsTime().Sub(created[id]); sample > 0 {
			out = append(out, sample)
		}
	}
	return out, nil
}

// walkExecutions pages one query through the execution index, handing every
// record to visit.
//
// Running out of pages is a FAILURE, not a short answer. A truncated page of a
// list is visibly short; a percentile over a truncated sample looks like an
// answer and is not one, and a count that stopped early reads as an outage
// that did not happen.
func (r *TemporalRunReader) walkExecutions(
	ctx context.Context,
	w StatsWindow,
	query string,
	visit func(*workflowpb.WorkflowExecutionInfo),
) error {
	scoped := false
	if w.Namespace != "" && r.namespaceIndexed {
		query = fmt.Sprintf("%s AND %s = %q", query, task.SearchAttrOrkflowNamespace,
			artifacts.ResolveNamespace(w.Namespace))
		scoped = true
	}
	pages := maxStatsPages
	if w.Namespace != "" && !scoped {
		pages = maxStatsScanPages
	}
	var token []byte
	for page := 0; page < pages; page++ {
		response, err := r.client.ListWorkflow(ctx, &workflowservice.ListWorkflowExecutionsRequest{
			Query:         query,
			PageSize:      statsPageSize,
			NextPageToken: token,
		})
		if err != nil {
			return fmt.Errorf("engine: read the execution index (%s): %w", query, err)
		}
		for _, info := range response.GetExecutions() {
			visit(info)
		}
		token = response.GetNextPageToken()
		if len(token) == 0 {
			return nil
		}
	}
	return fmt.Errorf(
		"engine: stats for namespace %q read %d pages of the execution index without "+
			"reaching the end, so any number computed from it would be a subset "+
			"presented as a total. Narrow the window with since/until%s",
		w.Namespace, pages, unindexedAdvice(scoped, w.Namespace))
}

// unindexedAdvice adds the fix an operator can act on when the namespace could
// not be pushed into the query and every other namespace's records had to be
// read and discarded.
func unindexedAdvice(scoped bool, namespace string) string {
	if scoped || namespace == "" {
		return ""
	}
	return fmt.Sprintf(", or register the %s index (it is provisioned at engine boot) "+
		"so the namespace becomes a query predicate instead of a filter",
		task.SearchAttrOrkflowNamespace)
}

// executionDuration is how long one execution took, from its own record.
func executionDuration(info *workflowpb.WorkflowExecutionInfo) time.Duration {
	if d := info.GetExecutionDuration(); d != nil && d.AsDuration() > 0 {
		return d.AsDuration()
	}
	start, closed := info.GetStartTime(), info.GetCloseTime()
	if start == nil || closed == nil {
		return 0
	}
	return closed.AsTime().Sub(start.AsTime())
}

// inNamespace compares a run's namespace with the one asked for, resolving the
// built-in default on both sides. An empty filter matches everything.
func inNamespace(got, want string) bool {
	if want == "" {
		return true
	}
	return artifacts.ResolveNamespace(got) == artifacts.ResolveNamespace(want)
}

// runStartTime is when a listed run began, for ordering.
func runStartTime(run *orkstackv1.Run) time.Time {
	if ts := run.GetCreatedAt(); ts != nil {
		return ts.AsTime()
	}
	return time.Time{}
}

// statsTime renders a time bound the way the index's query language reads one.
func statsTime(t time.Time) string { return t.UTC().Format(statsTimeLayout) }

// OrkflowTally is one orkflow's record inside a window: how many of its runs
// finished, and how many of those failed.
type OrkflowTally struct {
	Failed int64
	Total  int64
}

// RunTally is what one keys-only scan of the archive index adds up.
//
// Every number in it comes from the KEY of an index object. No body is read,
// which is what makes the cost a function of how many runs finished rather
// than of how big they were.
type RunTally struct {
	// ByStatus counts finished runs by the status the key spells: lowercase
	// and unprefixed ("succeeded", "failed", "canceled").
	ByStatus map[string]int64
	// ByOrkflow counts each orkflow's finished and failed runs.
	ByOrkflow map[string]OrkflowTally
}

// Archive index statuses, as a key spells them.
const (
	StatusKeySucceeded = "succeeded"
	StatusKeyFailed    = "failed"
	StatusKeyCanceled  = "canceled"
	// StatusKeyRunning is not an archive status — nothing finished is running
	// — but it is the key the cluster's half of the answer fills in, and it is
	// named here so both halves spell it the same way.
	StatusKeyRunning = "running"
)

// Finished is how many runs the tally counted, whatever their outcome.
func (t RunTally) Finished() int64 {
	var total int64
	for _, count := range t.ByStatus {
		total += count
	}
	return total
}

// Succeeded is how many of them succeeded.
func (t RunTally) Succeeded() int64 { return t.ByStatus[StatusKeySucceeded] }

// TallyRunIndex folds index entries into a tally. It is pure over its input,
// so the aggregation is testable without a store of any kind.
func TallyRunIndex(entries []artifacts.RunIndexEntry) RunTally {
	tally := RunTally{
		ByStatus:  map[string]int64{},
		ByOrkflow: map[string]OrkflowTally{},
	}
	for _, entry := range entries {
		status := artifacts.NormalizeRunStatus(entry.Status)
		tally.ByStatus[status]++
		per := tally.ByOrkflow[entry.Orkflow]
		per.Total++
		if status == StatusKeyFailed {
			per.Failed++
		}
		tally.ByOrkflow[entry.Orkflow] = per
	}
	return tally
}

// TallyRuns counts the archived runs a query matches, reading INDEX KEYS ONLY.
//
// It is deliberately not ListRuns. That one fetches a body per run on the page
// so it can return whole runs; a tally needs no field a body holds, and
// fetching one would make a dashboard's cost scale with how much output its
// tasks produced. The query's Limit is ignored for the same reason: a total
// that stopped at a page size is not a total.
func (a *RunArchiver) TallyRuns(ctx context.Context, q ArchiveQuery) (RunTally, error) {
	if a == nil {
		// Not an empty tally. "No archive is configured" and "no run finished"
		// are opposite facts that would render identically, and the second is
		// the one a dashboard would believe. ListRuns may answer an absent
		// archive with an empty page — a list of nothing is still a list — but
		// a TOTAL of nothing is a claim, and this one cannot be made.
		return RunTally{}, ErrNoRunArchive
	}
	entries, err := a.archive.ListRuns(ctx, artifacts.RunQuery{
		Namespace: q.Namespace,
		Orkflow:   q.OrkflowName,
		Status:    archiveStatus(q.Status),
		Since:     q.Since,
		Until:     q.Until,
	})
	if err != nil {
		return RunTally{}, err
	}
	return TallyRunIndex(entries), nil
}

// ListRunIndex is the failure breakdown's population: every FAILED index row
// a query's namespace, orkflow and window match, read out of KEYS only.
//
// It is how the breakdown learns WHICH runs failed before it reads any of
// them. Two of the query's fields are deliberately not honoured: Status is
// always FAILED, whatever the caller set, and Limit — whose convention on
// ArchiveQuery is "0 means a page" — is ignored along with Exhaustive,
// because a population that stopped at a page size is not a population. The
// breakdown bounds its cost by refusing an over-large population outright
// (api.FailureBreakdownMaxScan), never by reading part of one.
func (a *RunArchiver) ListRunIndex(ctx context.Context, q ArchiveQuery) ([]artifacts.RunIndexEntry, error) {
	if a == nil {
		// Not an empty population, on TallyRuns' terms: "no archive" and "no
		// run failed" are opposite facts that would render identically.
		return nil, ErrNoRunArchive
	}
	return a.archive.ListRuns(ctx, artifacts.RunQuery{
		Namespace: q.Namespace,
		Orkflow:   q.OrkflowName,
		Status:    archiveStatus(orkstackv1.Status_STATUS_FAILED),
		Since:     q.Since,
		Until:     q.Until,
		Limit:     0,
	})
}

// ListFinishedRunIndex is the flake index's population: every index row a
// query's namespace, orkflow and window match, WHATEVER its finished status —
// succeeded, failed and canceled — read out of KEYS only. It is the same set
// TallyRuns counts, so the flake index's runs_in_window equals GetStats'
// succeeded + failed + canceled for the same window.
//
// It is ListRunIndex's sibling rather than a change to it: ListRunIndex's
// contract is "always FAILED, whatever the caller set", and the failure
// breakdown relies on it. Here Status, Limit and Exhaustive are all ignored,
// for the same reason — a population narrowed or stopped at a page is not a
// population. The flake index bounds its cost by refusing an over-large
// population outright (api.FlakeIndexMaxScan), never by reading part of one.
func (a *RunArchiver) ListFinishedRunIndex(ctx context.Context, q ArchiveQuery) ([]artifacts.RunIndexEntry, error) {
	if a == nil {
		return nil, ErrNoRunArchive
	}
	return a.archive.ListRuns(ctx, artifacts.RunQuery{
		Namespace: q.Namespace,
		Orkflow:   q.OrkflowName,
		Status:    archiveStatus(orkstackv1.Status_STATUS_UNSPECIFIED),
		Since:     q.Since,
		Until:     q.Until,
		Limit:     0,
	})
}

// PercentileDuration returns the p-th percentile of an ASCENDING sample by
// nearest rank: the smallest value at or above p per cent of the samples.
//
// Nearest rank rather than an interpolating variant because every value it
// can return is a duration some run actually took. An interpolated p95 of two
// runs is a number no run ever produced, which is a poor thing to page someone
// about. An empty sample has no percentile and reports zero; callers gate on
// the sample count rather than reading that zero as "instant".
func PercentileDuration(ascending []time.Duration, p int) time.Duration {
	rank := NearestRank(len(ascending), p)
	if rank == 0 {
		return 0
	}
	return ascending[rank-1]
}

// releaseRunsQuery is the run index predicate for runs executing on one
// release (docs/DEPLOY.md §3). RunWorkflow only: the idempotency claim is
// AutoUpgrade and holds no release; a schedule's fire workflow lives exactly
// as long as the run it starts and is not a run.
//
// It names no ExecutionStatus: liveRunCount and the listing add the live-run
// predicate, so a chain caught between its two records — visible only as its
// ContinuedAsNew record — still counts on its release, and a release whose
// count is its drain authority never reads a live chain as gone.
func releaseRunsQuery(indexedVersion string) string {
	return fmt.Sprintf("WorkflowType = %q AND %s = %q",
		RunWorkflowType, task.SearchAttrDeploymentVersion, indexedVersion)
}

// ReleaseRuns counts the runs executing on one release (liveRunCount: at
// most two CountWorkflow calls, plus a settling of chains that switched in
// the last hour) and lists the newest `limit` (one ListWorkflow page of that
// size, deduplicated by workflow id and settled), each through listedRun. The
// index is deployment-wide on purpose: a release carries runs of every
// namespace.
func (r *TemporalRunReader) ReleaseRuns(ctx context.Context, deployment, buildID string, limit int) (int64, []*orkstackv1.Run, error) {
	indexed, err := task.IndexedVersion(deployment, buildID)
	if err != nil {
		return 0, nil, err
	}
	base := releaseRunsQuery(indexed)
	count, err := r.liveRunCount(ctx, base, 0)
	if err != nil {
		return 0, nil, fmt.Errorf("engine: count the runs on release %s: %w", buildID, err)
	}
	runs := []*orkstackv1.Run{}
	if limit > 0 && count > 0 {
		listed, err := r.client.ListWorkflow(ctx, &workflowservice.ListWorkflowExecutionsRequest{
			Query:    base + " AND " + liveRunPredicate(r.clock()),
			PageSize: int32(limit),
		})
		if err != nil {
			return 0, nil, fmt.Errorf("engine: list the runs on release %s: %w", buildID, err)
		}
		records, _, err := r.resolveChains(ctx, listed.GetExecutions())
		if err != nil {
			return 0, nil, fmt.Errorf("engine: list the runs on release %s: %w", buildID, err)
		}
		for _, info := range records {
			runs = append(runs, r.listedRun(info))
		}
		sort.SliceStable(runs, func(i, j int) bool {
			return runStartTime(runs[i]).After(runStartTime(runs[j]))
		})
		if len(runs) > limit {
			runs = runs[:limit]
		}
	}
	return count, runs, nil
}

// RunningRuns counts every executing run, on any release or none — the
// population runningExecutions lists, a chain once (liveRunCount).
func (r *TemporalRunReader) RunningRuns(ctx context.Context) (int64, error) {
	count, err := r.liveRunCount(ctx, fmt.Sprintf("WorkflowType = %q", RunWorkflowType), 0)
	if err != nil {
		return 0, fmt.Errorf("engine: count the runs in flight: %w", err)
	}
	return count, nil
}
