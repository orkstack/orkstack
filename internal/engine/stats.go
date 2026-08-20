package engine

import (
	"context"
	"fmt"
	"sort"
	"time"

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
// (AGENTS.md §1.7).

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

	// executionStatusRunning is how the execution index spells a run that has
	// been accepted and has not finished.
	executionStatusRunning = "Running"

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
// Durations holds one sample per run that FINISHED inside the window, sorted
// ascending, ready for a percentile.
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

// Executions reports the runs executing now and how long the window's finished
// runs took, both from the cluster's execution index.
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
	durations, err := r.finishedDurations(ctx, w)
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
	query := fmt.Sprintf("WorkflowType = %q AND ExecutionStatus = %q",
		RunWorkflowType, executionStatusRunning)
	out := []*orkstackv1.Run{}
	err := r.walkExecutions(ctx, w, query, func(info *workflowpb.WorkflowExecutionInfo) {
		run := r.listedRun(info)
		if !inNamespace(run.GetNamespace(), w.Namespace) {
			return
		}
		if started := run.GetCreatedAt(); started != nil && !started.AsTime().Before(w.Until) {
			// It had not started by the end of the window this answer is
			// about, so it is not part of it.
			return
		}
		out = append(out, run)
	})
	if err != nil {
		return nil, err
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

// finishedDurations samples how long each run that finished inside the window
// took, ascending.
//
// The time bound is pushed into the query, so the cluster reads its own index
// by close time instead of handing over every execution it holds. The
// duration is the record's own ExecutionDuration where it has one, and close
// minus start otherwise — the same record either way, never a second call.
func (r *TemporalRunReader) finishedDurations(
	ctx context.Context,
	w StatsWindow,
) ([]time.Duration, error) {
	query := fmt.Sprintf("WorkflowType = %q AND CloseTime >= %q AND CloseTime < %q",
		RunWorkflowType, statsTime(w.Since), statsTime(w.Until))
	out := []time.Duration{}
	err := r.walkExecutions(ctx, w, query, func(info *workflowpb.WorkflowExecutionInfo) {
		if !inNamespace(r.memoString(info.GetMemo(), MemoNamespace), w.Namespace) {
			return
		}
		if closed := info.GetCloseTime(); closed == nil ||
			closed.AsTime().Before(w.Since) || !closed.AsTime().Before(w.Until) {
			return
		}
		if sample := executionDuration(info); sample > 0 {
			out = append(out, sample)
		}
	})
	if err != nil {
		return nil, err
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
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

// PercentileDuration returns the p-th percentile of an ASCENDING sample by
// nearest rank: the smallest value at or above p per cent of the samples.
//
// Nearest rank rather than an interpolating variant because every value it
// can return is a duration some run actually took. An interpolated p95 of two
// runs is a number no run ever produced, which is a poor thing to page someone
// about. An empty sample has no percentile and reports zero; callers gate on
// the sample count rather than reading that zero as "instant".
func PercentileDuration(ascending []time.Duration, p int) time.Duration {
	n := len(ascending)
	if n == 0 {
		return 0
	}
	if p <= 0 {
		return ascending[0]
	}
	if p >= 100 {
		return ascending[n-1]
	}
	rank := (p*n + 99) / 100 // ceil(p*n/100)
	if rank < 1 {
		rank = 1
	}
	if rank > n {
		rank = n
	}
	return ascending[rank-1]
}
