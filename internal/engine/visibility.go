package engine

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	workflowpb "go.temporal.io/api/workflow/v1"
	"go.temporal.io/api/workflowservice/v1"
)

// A run that has continued as new is a chain of Temporal executions under one
// workflow id, and the cluster's visibility index holds one record per
// execution. It writes generation N's closing record and N+1's starting
// record independently, in an order it does not guarantee, so for the
// indexing lag a chain can show as two Running records, or as ONLY N's
// ContinuedAsNew record. A liveness question that excluded ContinuedAsNew
// would count the second case as no run at all: the deletion gate would pass
// under a live run, admission would under-count, and a release would read as
// drained while a chain still runs on it.
//
// So no liveness question excludes ContinuedAsNew; records are deduplicated
// by workflow id (dedupeChains), a latest ContinuedAsNew record stands for
// its chain as RUNNING, and a question about LIVE runs settles each id whose
// latest record is ContinuedAsNew (resolveChains): one batched listing by id,
// which drops a chain that has since finished without asking it anything,
// and a describe only for a chain still caught mid-switch.

// continuationVisibilityWindow bounds how long after a generation closes its
// ContinuedAsNew record still stands for its chain in a liveness question. It
// must exceed the cluster's lag between indexing that close and indexing the
// successor's start; the server contract test measures the lag
// (TestIntegration_ContinueAsNewServerContract, item j). Reader-side only.
const continuationVisibilityWindow = time.Hour

// resolveChainsBatch is how many workflow ids one by-id listing names.
const resolveChainsBatch = 100

// executionStatusContinuedAsNew is how the execution index spells a closed
// generation of a chain.
var executionStatusContinuedAsNew = enumspb.WORKFLOW_EXECUTION_STATUS_CONTINUED_AS_NEW.String()

// openExecutionsPredicate is every status statusFromExecution maps to RUNNING
// except ContinuedAsNew. A later SDK status (Paused) stays live here, the
// reasoning terminalExecutionStatuses' comment gives.
func openExecutionsPredicate() string {
	return "ExecutionStatus NOT IN (" + quotedExecutionStatuses(terminalExecutionStatuses) +
		", " + strconv.Quote(executionStatusContinuedAsNew) + ")"
}

// recentlyContinuedPredicate is the ContinuedAsNew records closed within the
// switch-visibility window before now.
func recentlyContinuedPredicate(now time.Time) string {
	return fmt.Sprintf("ExecutionStatus = %q AND CloseTime >= %q",
		executionStatusContinuedAsNew, statsTime(now.Add(-continuationVisibilityWindow)))
}

// liveRunPredicate is open executions, plus ContinuedAsNew executions closed
// within the window: a chain caught between its two records is in it either
// way.
func liveRunPredicate(now time.Time) string {
	return "(" + openExecutionsPredicate() + " OR (" + recentlyContinuedPredicate(now) + "))"
}

func isContinuedAsNew(info *workflowpb.WorkflowExecutionInfo) bool {
	return info.GetStatus() == enumspb.WORKFLOW_EXECUTION_STATUS_CONTINUED_AS_NEW
}

// isOpenExecution reports a record of an execution still running (any status
// statusFromExecution maps to RUNNING but ContinuedAsNew).
func isOpenExecution(info *workflowpb.WorkflowExecutionInfo) bool {
	if isContinuedAsNew(info) {
		return false
	}
	for _, status := range terminalExecutionStatuses {
		if info.GetStatus() == status {
			return false
		}
	}
	return true
}

// laterRecord reports whether candidate supersedes current for one workflow
// id: the later StartTime wins; on a tie a non-ContinuedAsNew record wins.
func laterRecord(candidate, current *workflowpb.WorkflowExecutionInfo) bool {
	a, b := candidate.GetStartTime().AsTime(), current.GetStartTime().AsTime()
	if !a.Equal(b) {
		return a.After(b)
	}
	return isContinuedAsNew(current) && !isContinuedAsNew(candidate)
}

// dedupeChains keeps one record per workflow id: the later StartTime, a
// non-ContinuedAsNew record on a tie. Each id keeps the position of its first
// record. Applied over the whole walk, before every trim (completeRunPage).
func dedupeChains(records []*workflowpb.WorkflowExecutionInfo) []*workflowpb.WorkflowExecutionInfo {
	index := make(map[string]int, len(records))
	out := make([]*workflowpb.WorkflowExecutionInfo, 0, len(records))
	for _, info := range records {
		id := info.GetExecution().GetWorkflowId()
		at, seen := index[id]
		if !seen {
			index[id] = len(out)
			out = append(out, info)
			continue
		}
		if laterRecord(info, out[at]) {
			out[at] = info
		}
	}
	return out
}

// chainSettlement is what resolveChains learned about one chain.
type chainSettlement int

const (
	// chainFinished: the chain's latest record is closed and not
	// ContinuedAsNew, or a describe found it closed or gone.
	chainFinished chainSettlement = iota
	// chainLiveIndexed: its successor is now indexed and open.
	chainLiveIndexed
	// chainLiveMidSwitch: still caught between its records; a describe
	// confirmed its current execution is open.
	chainLiveMidSwitch
)

// resolveChains settles every id whose latest record in records is
// ContinuedAsNew:
//
//  1. it lists every record of those ids — WorkflowType = "RunWorkflow" AND
//     WorkflowId IN (…), at most 100 ids per ListWorkflow call, any status —
//     and dedupes again. A latest record that is closed and not
//     ContinuedAsNew is a chain that finished: dropped, no describe. A latest
//     open record is a live chain whose successor is now indexed: kept, as
//     that record.
//  2. only an id whose latest record is STILL ContinuedAsNew — a chain caught
//     between its closing and its successor's starting record — is asked
//     DescribeWorkflowExecution(id, ""): live when the current execution is
//     not closed; dropped when it is closed or NotFound; an error fails the
//     caller.
//
// Cost: ⌈k/100⌉ lists for k such ids, plus one describe per chain still
// mid-switch (bounded by the visibility lag, normally none). On an
// InvalidArgument answer to the IN query (a cluster without IN), it falls back
// for the rest of the process's life to one WorkflowId = "…" list per id. The
// returned records keep the input's order; settled reports each settled id.
func (r *TemporalRunReader) resolveChains(
	ctx context.Context,
	records []*workflowpb.WorkflowExecutionInfo,
) ([]*workflowpb.WorkflowExecutionInfo, map[string]chainSettlement, error) {
	records = dedupeChains(records)
	var pending []string
	for _, info := range records {
		if isContinuedAsNew(info) {
			pending = append(pending, info.GetExecution().GetWorkflowId())
		}
	}
	settled := make(map[string]chainSettlement, len(pending))
	if len(pending) == 0 {
		return records, settled, nil
	}
	latest, err := r.listChainRecords(ctx, pending)
	if err != nil {
		return nil, nil, err
	}
	out := make([]*workflowpb.WorkflowExecutionInfo, 0, len(records))
	for _, info := range records {
		id := info.GetExecution().GetWorkflowId()
		if !isContinuedAsNew(info) {
			out = append(out, info)
			continue
		}
		newest := info
		if found, ok := latest[id]; ok && laterRecord(found, newest) {
			newest = found
		}
		switch {
		case isOpenExecution(newest):
			settled[id] = chainLiveIndexed
			out = append(out, newest)
		case !isContinuedAsNew(newest):
			settled[id] = chainFinished
		default:
			live, err := r.chainStillLive(ctx, id)
			if err != nil {
				return nil, nil, err
			}
			if live {
				settled[id] = chainLiveMidSwitch
				out = append(out, newest)
			} else {
				settled[id] = chainFinished
			}
		}
	}
	return out, settled, nil
}

// listChainRecords lists every record of ids and returns the latest of each.
func (r *TemporalRunReader) listChainRecords(ctx context.Context, ids []string) (map[string]*workflowpb.WorkflowExecutionInfo, error) {
	latest := make(map[string]*workflowpb.WorkflowExecutionInfo, len(ids))
	keep := func(info *workflowpb.WorkflowExecutionInfo) {
		id := info.GetExecution().GetWorkflowId()
		if current, ok := latest[id]; !ok || laterRecord(info, current) {
			latest[id] = info
		}
	}
	for start := 0; start < len(ids); start += resolveChainsBatch {
		batch := ids[start:min(start+resolveChainsBatch, len(ids))]
		if !r.chainINUnsupported.Load() {
			quoted := make([]string, len(batch))
			for i, id := range batch {
				quoted[i] = strconv.Quote(id)
			}
			query := fmt.Sprintf("WorkflowType = %q AND WorkflowId IN (%s)", RunWorkflowType, strings.Join(quoted, ", "))
			err := r.listAll(ctx, query, keep)
			if err == nil {
				continue
			}
			var invalid *serviceerror.InvalidArgument
			if !errors.As(err, &invalid) {
				return nil, err
			}
			r.chainINUnsupported.Store(true)
		}
		for _, id := range batch {
			query := fmt.Sprintf("WorkflowType = %q AND WorkflowId = %q", RunWorkflowType, id)
			if err := r.listAll(ctx, query, keep); err != nil {
				return nil, err
			}
		}
	}
	return latest, nil
}

// chainStillLive describes a chain caught mid-switch: live when its current
// execution is not closed.
func (r *TemporalRunReader) chainStillLive(ctx context.Context, id string) (bool, error) {
	resp, err := r.client.DescribeWorkflowExecution(ctx, id, "")
	if err != nil {
		var notFound *serviceerror.NotFound
		if errors.As(err, &notFound) {
			return false, nil
		}
		return false, fmt.Errorf("engine: describe run %s while it carries on into a fresh history: %w", id, err)
	}
	info := resp.GetWorkflowExecutionInfo()
	return info.GetCloseTime() == nil && !isContinuedAsNew(info) && isOpenExecution(info), nil
}

// listAll pages one visibility query to its end, under the scan bound;
// running out of pages is a failure, never a short answer.
func (r *TemporalRunReader) listAll(ctx context.Context, query string, visit func(*workflowpb.WorkflowExecutionInfo)) error {
	var token []byte
	for page := 0; page < maxRunScanPages; page++ {
		response, err := r.client.ListWorkflow(ctx, &workflowservice.ListWorkflowExecutionsRequest{
			Query:         query,
			PageSize:      DefaultRunPageSize,
			NextPageToken: token,
		})
		if err != nil {
			return fmt.Errorf("engine: list runs from temporal (%s): %w", query, err)
		}
		for _, info := range response.GetExecutions() {
			visit(info)
		}
		token = response.GetNextPageToken()
		if len(token) == 0 {
			return nil
		}
	}
	return fmt.Errorf("engine: list runs from temporal (%s) did not reach the end after %d pages", query, maxRunScanPages)
}

// countAll is one CountWorkflow.
func (r *TemporalRunReader) countAll(ctx context.Context, query string) (int64, error) {
	response, err := r.client.CountWorkflow(ctx, &workflowservice.CountWorkflowExecutionsRequest{Query: query})
	if err != nil {
		return 0, fmt.Errorf("engine: count the execution index (%s): %w", query, err)
	}
	return response.GetCount(), nil
}

// liveRunCount counts distinct live runs under base (a predicate with no
// ExecutionStatus term). It never reads a live chain as zero, and it walks the
// live population only for admission at its ceiling.
//
//  1. open   := CountWorkflow(base AND openExecutionsPredicate())
//  2. recent := CountWorkflow(base AND ExecutionStatus = "ContinuedAsNew" AND CloseTime >= now−W),
//     ASKED AFTER (1).
//  3. recent == 0: return open — unless stopAt > 0 && open >= stopAt (step 6).
//  4. stopAt > 0 && open+recent < stopAt: return open+recent, an upper bound
//     below the ceiling.
//  5. stopAt <= 0 (release and running counts): list the recent ContinuedAsNew
//     records (under the scan bound, failing closed), settle their ids with
//     resolveChains, and return: open == 0, the number of ids settled live
//     (exact for the drain question); open > 0, open plus the ids still
//     caught mid-switch and confirmed live.
//  6. stopAt > 0 at or above the ceiling (admission): walk base AND
//     liveRunPredicate(now) to the end (failing closed), dedupe, settle, and
//     count distinct live ids, stopping as soon as stopAt are found.
//
// Why no step reads a live chain as zero: a chain live while (1) and (2) run
// either has its newest record visible at (1) open, so (1) counts it, or its
// generation N was already indexed ContinuedAsNew without N+1, so (2), asked
// afterwards, counts N's record and step 5's listing finds it — records only
// move forward, and a ContinuedAsNew record stays one.
func (r *TemporalRunReader) liveRunCount(ctx context.Context, base string, stopAt int64) (int64, error) {
	now := r.clock()
	open, err := r.countAll(ctx, base+" AND "+openExecutionsPredicate())
	if err != nil {
		return 0, err
	}
	recentQuery := base + " AND " + recentlyContinuedPredicate(now)
	recent, err := r.countAll(ctx, recentQuery)
	if err != nil {
		return 0, err
	}
	switch {
	case stopAt > 0 && (open >= stopAt || open+recent >= stopAt):
		return r.walkLiveRuns(ctx, base+" AND "+liveRunPredicate(now), nil, stopAt)
	case recent == 0:
		return open, nil
	case stopAt > 0:
		// Step 4: open+recent < stopAt.
		return open + recent, nil
	}
	var records []*workflowpb.WorkflowExecutionInfo
	if err := r.listAll(ctx, recentQuery, func(info *workflowpb.WorkflowExecutionInfo) {
		records = append(records, info)
	}); err != nil {
		return 0, err
	}
	_, settled, err := r.resolveChains(ctx, records)
	if err != nil {
		return 0, err
	}
	var live, midSwitch int64
	for _, outcome := range settled {
		switch outcome {
		case chainLiveIndexed:
			live++
		case chainLiveMidSwitch:
			live++
			midSwitch++
		}
	}
	if open == 0 {
		return live, nil
	}
	return open + midSwitch, nil
}

// walkLiveRuns walks query to the end, keeping records keep accepts (nil
// keeps every one), and counts distinct live runs: deduplicated by workflow
// id, every id whose latest record is ContinuedAsNew settled, stopping as
// soon as stopAt distinct open ids are found when stopAt > 0.
func (r *TemporalRunReader) walkLiveRuns(
	ctx context.Context,
	query string,
	keep func(*workflowpb.WorkflowExecutionInfo) bool,
	stopAt int64,
) (int64, error) {
	latest := map[string]*workflowpb.WorkflowExecutionInfo{}
	var order []string
	openIDs := int64(0)
	var token []byte
	for page := 0; ; page++ {
		if page >= maxRunScanPages {
			return 0, fmt.Errorf("engine: count live runs (%s) did not reach the end after %d pages", query, maxRunScanPages)
		}
		response, err := r.client.ListWorkflow(ctx, &workflowservice.ListWorkflowExecutionsRequest{
			Query:         query,
			PageSize:      statsPageSize,
			NextPageToken: token,
		})
		if err != nil {
			return 0, fmt.Errorf("engine: read the execution index (%s): %w", query, err)
		}
		for _, info := range response.GetExecutions() {
			if keep != nil && !keep(info) {
				continue
			}
			id := info.GetExecution().GetWorkflowId()
			current, seen := latest[id]
			if !seen {
				order = append(order, id)
			}
			if !seen || laterRecord(info, current) {
				wasOpen := seen && isOpenExecution(current)
				latest[id] = info
				switch {
				case isOpenExecution(info) && !wasOpen:
					openIDs++
				case !isOpenExecution(info) && wasOpen:
					openIDs--
				}
			}
		}
		if stopAt > 0 && openIDs >= stopAt {
			return stopAt, nil
		}
		token = response.GetNextPageToken()
		if len(token) == 0 {
			break
		}
	}
	records := make([]*workflowpb.WorkflowExecutionInfo, 0, len(order))
	for _, id := range order {
		records = append(records, latest[id])
	}
	live, _, err := r.resolveChains(ctx, records)
	if err != nil {
		return 0, err
	}
	return int64(len(live)), nil
}

// clock is the reader's wall clock: the switch-visibility window is measured
// from it. It is never workflow code.
func (r *TemporalRunReader) clock() time.Time {
	if r.now != nil {
		return r.now()
	}
	return time.Now()
}
