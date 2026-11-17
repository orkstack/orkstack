package engine

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"go.temporal.io/api/serviceerror"

	enumspb "go.temporal.io/api/enums/v1"
	workflowpb "go.temporal.io/api/workflow/v1"
	"go.temporal.io/api/workflowservice/v1"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/archive"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// The engine's half of the artifact GC (internal/archive.Reclaimer): what
// the cluster's index and a run's archive body say. Nothing here decides.

var (
	_ archive.ReclaimCluster = (*TemporalRunReader)(nil)
	_ archive.RunBodyReader  = (*RunArchiver)(nil)
)

// ReclaimBody reads what the artifact GC needs from a run's archive body:
// who it is, whether it ended and when, and every object it records, with
// its class. A run with no body is an error matching artifacts.ErrNotFound.
func (a *RunArchiver) ReclaimBody(ctx context.Context, runID string) (archive.RunBody, error) {
	run, err := a.LoadRun(ctx, runID)
	if err != nil {
		return archive.RunBody{}, err
	}
	body := archive.RunBody{
		RunID:     run.GetId(),
		Namespace: artifacts.ResolveNamespace(run.GetNamespace()),
		Terminal:  terminalRunStatus(run.GetStatus()),
		Objects:   reclaimObjects(run),
	}
	if at := run.GetFinishedAt(); at.IsValid() && !at.AsTime().IsZero() {
		body.FinishedAt = at.AsTime()
	}
	return body, nil
}

// terminalRunStatus is runstatus.IsTerminal: a PAUSED run is live, so its
// objects are never reclaimed.
func terminalRunStatus(status orkstackv1.Status) bool { return isTerminal(status) }

// reclaimObjects is runObjectIDs with each object's class: the same walk
// over task rows and fan-out items, so the GC's candidates are exactly the
// objects PutRun marked.
func reclaimObjects(run *orkstackv1.Run) []archive.BodyObject {
	var out []archive.BodyObject
	add := func(ref *orkstackv1.ArtifactRef, class string) {
		if id, ok := artifacts.ObjectIDFromURI(ref.GetUri()); ok {
			out = append(out, archive.BodyObject{ID: id, Class: class})
		}
	}
	row := func(taskRun *orkstackv1.TaskRun) {
		for _, ref := range taskRun.GetArtifacts() {
			add(ref, archive.ClassArtifact)
		}
		for _, ref := range taskRun.GetSystemArtifacts() {
			add(ref, archive.ClassSystemArtifact)
		}
		add(taskRun.GetValuesRef(), archive.ClassValues)
	}
	for _, taskRun := range run.GetTaskRuns() {
		row(taskRun)
		for _, item := range taskRun.GetItems() {
			row(item)
		}
	}
	return out
}

// WalkLowWater answers steps 2 and 3 of the artifact GC's low-water mark.
//
// Step 2 walks every live chain — RunWorkflow under liveRunPredicate, paged
// to the end under the scan bound — and keeps every record. A record's
// creation is the earlier of its memo created_at and its StartTime; a record
// of an execution that is not its chain's first, with no created_at, cannot
// be dated and fails the walk closed (no_creation). Chains whose latest
// record is ContinuedAsNew are settled as liveness questions settle them; a
// chain still caught mid-switch fails the walk closed (mid_switch).
//
// Step 3 lists every RunWorkflow closed at or after closedSince that is not
// ContinuedAsNew, drops the ids step 2 found live, and dates each the same
// way.
func (r *TemporalRunReader) WalkLowWater(ctx context.Context, closedSince time.Time) (archive.LowWaterWalk, error) {
	now := r.clock()
	walk := archive.LowWaterWalk{Seen: map[string]struct{}{}}
	var records []*workflowpb.WorkflowExecutionInfo
	liveQuery := fmt.Sprintf("WorkflowType = %q AND %s", RunWorkflowType, liveRunPredicate(now))
	if err := r.pageAll(ctx, liveQuery, func(info *workflowpb.WorkflowExecutionInfo) {
		records = append(records, info)
	}); err != nil {
		return archive.LowWaterWalk{}, err
	}
	for _, info := range records {
		walk.Seen[info.GetExecution().GetWorkflowId()] = struct{}{}
	}
	live, settled, err := r.resolveChains(ctx, records)
	if err != nil {
		return archive.LowWaterWalk{}, err
	}
	for _, outcome := range settled {
		if outcome == chainLiveMidSwitch {
			return archive.LowWaterWalk{Cause: archive.LowWaterMidSwitch}, nil
		}
	}
	walk.Chains = len(live)
	liveIDs := make(map[string]struct{}, len(live))
	for _, info := range live {
		liveIDs[info.GetExecution().GetWorkflowId()] = struct{}{}
	}

	closedQuery := fmt.Sprintf("WorkflowType = %q AND CloseTime >= %q AND ExecutionStatus != %q",
		RunWorkflowType, statsTime(closedSince), executionStatusContinuedAsNew)
	var closed []*workflowpb.WorkflowExecutionInfo
	if err := r.pageAll(ctx, closedQuery, func(info *workflowpb.WorkflowExecutionInfo) {
		id := info.GetExecution().GetWorkflowId()
		walk.Seen[id] = struct{}{}
		if _, isLive := liveIDs[id]; !isLive {
			closed = append(closed, info)
		}
	}); err != nil {
		return archive.LowWaterWalk{}, err
	}

	// A chain that carried on as new while the paged walk ran can sort ahead
	// of its cursor and be missed by it. Its new generation started after
	// the walk began, and carries created_at, so every record started since
	// the switch window opened is read once more and dates the mark too. A
	// run that started during the walk is no older than it, so it lowers
	// nothing it should not. Only a record still running is taken: one that
	// has closed since is the closed listing's, which asks whether it has an
	// archive body, and counting it live too would hold the mark for the
	// whole window after it was archived.
	startedSince := fmt.Sprintf("WorkflowType = %q AND StartTime >= %q",
		RunWorkflowType, statsTime(now.Add(-continuationVisibilityWindow)))
	if err := r.pageAll(ctx, startedSince, func(info *workflowpb.WorkflowExecutionInfo) {
		walk.Seen[info.GetExecution().GetWorkflowId()] = struct{}{}
		if info.GetStatus() == enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING {
			records = append(records, info)
		}
	}); err != nil {
		return archive.LowWaterWalk{}, err
	}

	created, ok, err := r.datedRuns(ctx, append(append([]*workflowpb.WorkflowExecutionInfo(nil), records...), closed...))
	if err != nil {
		return archive.LowWaterWalk{}, err
	}
	if !ok {
		return archive.LowWaterWalk{Cause: archive.LowWaterNoCreation}, nil
	}
	// Every record of the live walk dates the mark, a chain that has since
	// finished included: an earlier mark only keeps more.
	for _, info := range records {
		id := info.GetExecution().GetWorkflowId()
		if at := created[id]; walk.Earliest.IsZero() || at.Before(walk.Earliest) {
			walk.Earliest, walk.EarliestRunID = at, id
		}
	}
	for _, info := range closed {
		id := info.GetExecution().GetWorkflowId()
		walk.Closed = append(walk.Closed, archive.ClosedRun{RunID: id, Created: created[id]})
	}
	return walk, nil
}

// datedRuns dates every run the walk saw by its creation, and ok is false
// when one cannot be dated.
//
// A record carrying created_at (every generation after a run's first switch,
// and the first once it switched) is dated by it. A record with none is asked
// about: every record the cluster holds of that run id is listed (one listing
// per 100 ids), and the run is dated by its one execution's start only when
// the cluster holds exactly one execution of it. Several executions with no
// created_at among them (a manual reset, or a resume that did not carry it)
// cannot be dated: the walk fails closed rather than date the run by a later
// execution's start, which would raise the mark and admit deletes.
//
// The cluster's list answers do not carry first_run_id (measured on Temporal
// server 1.32: TestIntegration_ArtifactGC_ReclaimsOnlyTheUnreferenced), so
// the execution count is what tells a run's first execution from a later one.
func (r *TemporalRunReader) datedRuns(ctx context.Context, infos []*workflowpb.WorkflowExecutionInfo) (map[string]time.Time, bool, error) {
	created := make(map[string]time.Time, len(infos))
	var ask []string
	asked := map[string]bool{}
	earlier := func(id string, at time.Time) {
		if cur, ok := created[id]; !ok || at.Before(cur) {
			created[id] = at
		}
	}
	for _, info := range infos {
		id := info.GetExecution().GetWorkflowId()
		at, hasMemo, present := r.creationMemo(info)
		switch {
		case present && !hasMemo:
			// A created_at that does not decode dates nothing.
			return nil, false, nil
		case hasMemo && at.IsZero():
			return nil, false, nil
		case hasMemo:
			earlier(id, at)
			if start := info.GetStartTime(); start != nil && start.AsTime().Before(at) {
				earlier(id, start.AsTime().UTC())
			}
		case isContinuedAsNew(info):
			// A closed generation carries created_at by construction.
			return nil, false, nil
		case info.GetFirstRunId() != "" && info.GetFirstRunId() != info.GetExecution().GetRunId():
			return nil, false, nil
		default:
			if !asked[id] {
				asked[id] = true
				ask = append(ask, id)
			}
		}
	}
	if len(ask) == 0 {
		return created, true, nil
	}
	executions := make(map[string]map[string]*workflowpb.WorkflowExecutionInfo, len(ask))
	if err := r.listByIDs(ctx, ask, func(info *workflowpb.WorkflowExecutionInfo) {
		id := info.GetExecution().GetWorkflowId()
		if executions[id] == nil {
			executions[id] = map[string]*workflowpb.WorkflowExecutionInfo{}
		}
		executions[id][info.GetExecution().GetRunId()] = info
	}); err != nil {
		return nil, false, err
	}
	for _, info := range infos {
		id := info.GetExecution().GetWorkflowId()
		if !asked[id] {
			continue
		}
		held := executions[id]
		if held == nil {
			held = map[string]*workflowpb.WorkflowExecutionInfo{}
		}
		held[info.GetExecution().GetRunId()] = info
		var dated bool
		for _, rec := range held {
			if at, ok, _ := r.creationMemo(rec); ok && !at.IsZero() {
				earlier(id, at)
				dated = true
			}
		}
		if dated {
			continue
		}
		if len(held) != 1 || info.GetStartTime() == nil || info.GetStartTime().AsTime().IsZero() {
			return nil, false, nil
		}
		earlier(id, info.GetStartTime().AsTime().UTC())
	}
	return created, true, nil
}

// creationMemo reads the earliest of a record's created_at and start stamp.
// present says either key is in the memo at all; ok says every one present
// decodes.
func (r *TemporalRunReader) creationMemo(info *workflowpb.WorkflowExecutionInfo) (at time.Time, ok, present bool) {
	ok = true
	for _, key := range []string{MemoCreatedAt, MemoStartStamp} {
		if _, has := info.GetMemo().GetFields()[key]; !has {
			continue
		}
		present = true
		value, decoded := r.memoTime(info.GetMemo(), key)
		if !decoded {
			ok = false
			continue
		}
		if at.IsZero() || value.Before(at) {
			at = value
		}
	}
	return at, ok && present, present
}

// listByIDs lists every record of ids, any status: one WorkflowId IN query
// per 100 ids, or one per id on a cluster without IN.
func (r *TemporalRunReader) listByIDs(ctx context.Context, ids []string, visit func(*workflowpb.WorkflowExecutionInfo)) error {
	for start := 0; start < len(ids); start += resolveChainsBatch {
		batch := ids[start:min(start+resolveChainsBatch, len(ids))]
		if !r.chainINUnsupported.Load() {
			quoted := make([]string, len(batch))
			for i, id := range batch {
				quoted[i] = strconv.Quote(id)
			}
			err := r.listAll(ctx, fmt.Sprintf("WorkflowType = %q AND WorkflowId IN (%s)", RunWorkflowType, strings.Join(quoted, ", ")), visit)
			if err == nil {
				continue
			}
			var invalid *serviceerror.InvalidArgument
			if !errors.As(err, &invalid) {
				return err
			}
			r.chainINUnsupported.Store(true)
		}
		for _, id := range batch {
			if err := r.listAll(ctx, fmt.Sprintf("WorkflowType = %q AND WorkflowId = %q", RunWorkflowType, id), visit); err != nil {
				return err
			}
		}
	}
	return nil
}

// pageAll pages one visibility query to its end at statsPageSize under the
// scan bound; running out of pages is a failure, never a short answer.
func (r *TemporalRunReader) pageAll(ctx context.Context, query string, visit func(*workflowpb.WorkflowExecutionInfo)) error {
	var token []byte
	for page := 0; page < maxRunScanPages; page++ {
		response, err := r.client.ListWorkflow(ctx, &workflowservice.ListWorkflowExecutionsRequest{
			Query: query, PageSize: statsPageSize, NextPageToken: token,
		})
		if err != nil {
			return fmt.Errorf("engine: read the cluster's run index: %w", err)
		}
		for _, info := range response.GetExecutions() {
			visit(info)
		}
		token = response.GetNextPageToken()
		if len(token) == 0 {
			return nil
		}
	}
	return fmt.Errorf("engine: the cluster's run index did not reach its end after %d pages", maxRunScanPages)
}

// ReclaimRecord reads the artifact GC's record of a run: an error matching
// artifacts.ErrNotFound when the GC has not finished with it, or when the
// archive keeps no such records.
func (a *RunArchiver) ReclaimRecord(ctx context.Context, runID string) (artifacts.ReclaimRecord, error) {
	if a == nil {
		return artifacts.ReclaimRecord{}, fmt.Errorf("engine: reclaim record %q: %w", runID, artifacts.ErrNotFound)
	}
	reader, ok := a.archive.(interface {
		ReclaimRecord(ctx context.Context, runID string) (artifacts.ReclaimRecord, error)
	})
	if !ok {
		return artifacts.ReclaimRecord{}, fmt.Errorf("engine: reclaim record %q: %w", runID, artifacts.ErrNotFound)
	}
	return reader.ReclaimRecord(ctx, runID)
}
