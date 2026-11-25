package engine

import (
	"context"
	"errors"
	"fmt"
	"sync"

	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/converter"

	"github.com/orkstack/orkstack/internal/stablejson"
	"github.com/orkstack/orkstack/internal/task"
)

// GateActivities are the concurrency gate's two local activities (E88
// §5.1.8): reads of the cluster whose answers the gate's history records,
// so the gate decides from recorded bytes. Neither writes anything.
type GateActivities struct {
	client   client.Client
	arrivals ArrivalObserver
}

// NewGateActivities returns the gate's activities over c.
func NewGateActivities(c client.Client) *GateActivities { return &GateActivities{client: c} }

// WithArrivalObserver counts each answered acquire on o; nil counts none.
func (a *GateActivities) WithArrivalObserver(o ArrivalObserver) *GateActivities {
	a.arrivals = o
	return a
}

// RegisterGateActivities registers them on the engine worker, which runs
// them as local activities only.
func RegisterGateActivities(w ActivityRegistrar, a *GateActivities) {
	w.RegisterActivityWithOptions(a.DescribeConcurrencyEntries, activity.RegisterOptions{Name: ActivityDescribeConcurrencyEntries})
	w.RegisterActivityWithOptions(a.ListConcurrencyEntries, activity.RegisterOptions{Name: ActivityListConcurrencyEntries})
	w.RegisterActivityWithOptions(a.AcquireConcurrency, activity.RegisterOptions{Name: ActivityAcquireConcurrency})
}

// gateStuckAttempt is the workflow-task attempt from which a run counts as
// stuck (§5.1.5).
const gateStuckAttempt = 3

// gateListPageSize is how many runs one ListConcurrencyEntries page asks for.
const gateListPageSize = 100

// DescribeConcurrencyEntries describes each run by workflow id, at most
// Parallel at once (§5.1.5). A failure to describe one run is that run's
// finding, never the call's: the gate keeps its entries.
func (a *GateActivities) DescribeConcurrencyEntries(ctx context.Context, in DescribeConcurrencyEntriesInput) ([]gateRunFinding, error) {
	out := make([]gateRunFinding, len(in.Runs))
	sem := make(chan struct{}, max(1, in.Parallel))
	var wg sync.WaitGroup
	for i, run := range in.Runs {
		wg.Add(1)
		go func() {
			defer wg.Done()
			select {
			case sem <- struct{}{}:
			case <-ctx.Done():
				out[i] = gateRunFinding{Run: run, Err: ctx.Err().Error()}
				return
			}
			defer func() { <-sem }()
			out[i] = a.describe(ctx, in.GateID, run)
		}()
	}
	wg.Wait()
	return out, nil
}

func (a *GateActivities) describe(ctx context.Context, gateID, run string) gateRunFinding {
	f := gateRunFinding{Run: run}
	described, err := a.client.DescribeWorkflow(ctx, run, "")
	var missing *serviceerror.NotFound
	switch {
	case errors.As(err, &missing):
		f.NotFound = true
		return f
	case err != nil:
		f.Err = err.Error()
		return f
	case described == nil || described.RawResponse == nil:
		// Never read as a closed run: the entry is kept.
		f.Err = "the cluster described the run with no response"
		return f
	}
	resp := described.RawResponse
	info := resp.GetWorkflowExecutionInfo()
	f.Open = info.GetStatus() == enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING
	var seqErr error
	f.MemoSeq, f.Keys, seqErr = readGateMemo(gateID, info.GetMemo())
	if seqErr != nil && f.Open {
		// An incarnation the gate cannot read is never taken for an old
		// one: the entry is kept, and the error says why.
		f.Err = seqErr.Error()
		return f
	}
	if wft := resp.GetPendingWorkflowTask(); wft != nil && wft.GetAttempt() >= gateStuckAttempt {
		f.StuckSince = wft.GetOriginalScheduledTime().AsTime()
	}
	return f
}

// readGateMemo is a run memo's incarnation and its entries' task indexes
// for gateID. An incarnation present but unreadable is an error: read as 0
// it would make a live holder look like an old incarnation.
func readGateMemo(gateID string, memo *commonpb.Memo) (int64, map[int]bool, error) {
	keys := map[int]bool{}
	var seq int64
	var seqErr error
	for name, payload := range memo.GetFields() {
		if name == gateMemoSeqKey {
			if err := converter.GetDefaultDataConverter().FromPayload(payload, &seq); err != nil {
				seq, seqErr = 0, fmt.Errorf("the run's %s cannot be read: %w", gateMemoSeqKey, err)
			} else if seq < 1 {
				seq, seqErr = 0, fmt.Errorf("the run's %s is %d; it must be at least 1", gateMemoSeqKey, seq)
			}
			continue
		}
		if ix, ok := parseGateMemoKey(gateID, name); ok {
			keys[ix] = true
		}
	}
	return seq, keys, seqErr
}

// ListConcurrencyEntries reads one page of the open runs that hold or queue
// at the gate (§5.1.6), with their recorded entries. An entry the default
// converter did not write as JSON is passed on as it is, and the gate adopts
// it.
func (a *GateActivities) ListConcurrencyEntries(ctx context.Context, in ListConcurrencyEntriesInput) (ListConcurrencyEntriesResult, error) {
	query := fmt.Sprintf(`WorkflowType = %q AND ExecutionStatus = "Running" AND (%s = %q OR %s = %q)`,
		RunWorkflowType, task.SearchAttrOrkflowConcurrencyHeld, in.GateID, task.SearchAttrOrkflowConcurrencyQueued, in.GateID)
	resp, err := a.client.ListWorkflow(ctx, &workflowservice.ListWorkflowExecutionsRequest{
		Query: query, PageSize: gateListPageSize, NextPageToken: in.PageToken,
	})
	if err != nil {
		return ListConcurrencyEntriesResult{}, fmt.Errorf("list the runs at gate %s: %w", in.GateID, err)
	}
	out := ListConcurrencyEntriesResult{NextToken: resp.GetNextPageToken()}
	for _, info := range resp.GetExecutions() {
		rec := gateRunRecord{Run: info.GetExecution().GetWorkflowId(), Entries: map[int]stablejson.RawJSON{}}
		var seqErr error
		rec.MemoSeq, _, seqErr = readGateMemo(in.GateID, info.GetMemo())
		rec.SeqUnreadable = seqErr != nil
		for name, payload := range info.GetMemo().GetFields() {
			if ix, ok := parseGateMemoKey(in.GateID, name); ok {
				rec.Entries[ix] = payload.GetData()
			}
		}
		if len(rec.Entries) > 0 {
			out.Records = append(out.Records, rec)
		}
	}
	return out, nil
}
