package engine

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sync"
	"time"

	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/client"

	"github.com/orkstack/orkstack/internal/task"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// The rebuild states a GateView reports (§5.1.6).
const (
	GateRebuildNone     = gateRebuildNone
	GateRebuildReading  = gateRebuildListing
	GateRebuildSettling = gateRebuildSettling
	GateRebuildFailing  = gateRebuildFailing
)

// GateView is one gate as ConcurrencyService shows it: its key, holders in
// grant order and line in the order it grants, read from its state query.
// Answering is false when the query failed, and NotAnswering says why.
type GateView struct {
	GateID, Namespace, Key string
	Answering              bool
	// NotAnswering is a reader's sentence; Cause is the error behind it,
	// for the operator's log only.
	NotAnswering        string
	Cause               error
	Epoch               int64
	Holders, Line       []GateEntryView
	Rebuild             string
	RebuildFailingSince time.Time
}

// GateEntryView is one holder or waiting entry.
type GateEntryView struct {
	Run                              string
	TaskLevel                        bool
	GrantedAt, ArrivedAt, TimesOutAt time.Time
	Stalled                          bool
	Strategy                         string
	Limit                            int
}

// gateViewOf is a state query's answer as a GateView.
func gateViewOf(gateID string, view GateStateView) GateView {
	g := view.State
	out := GateView{GateID: gateID, Namespace: view.Namespace, Key: view.Key, Answering: true, Epoch: g.Epoch, Rebuild: g.Rebuild.State}
	if g.Rebuild.State == gateRebuildFailing {
		out.RebuildFailingSince = g.Rebuild.Since
	}
	entry := func(e gateEntry) GateEntryView {
		return GateEntryView{Run: e.Run, TaskLevel: e.TaskIx != gateRunLevel, GrantedAt: e.GrantedAt, ArrivedAt: e.ArrivedAt,
			TimesOutAt: e.WaitUntil, Stalled: e.Stalled, Strategy: e.Strategy, Limit: e.Limit}
	}
	holders := slices.Clone(g.Holders)
	slices.SortStableFunc(holders, func(a, b gateEntry) int { return a.GrantedAt.Compare(b.GrantedAt) })
	for _, e := range holders {
		out.Holders = append(out.Holders, entry(e))
	}
	queue := slices.Clone(g.Queue)
	slices.SortStableFunc(queue, func(a, b gateEntry) int {
		switch {
		case a.before(b.ArrivedAt, b.Seq):
			return -1
		case b.before(a.ArrivedAt, a.Seq):
			return 1
		}
		return 0
	})
	for _, e := range queue {
		out.Line = append(out.Line, entry(e))
	}
	return out
}

// gateListParallel bounds the state queries one page of gates makes.
const gateListParallel = 8

// Gates is ListWorkflow over the namespace's running gates, each then
// queried for its state, at most gateListParallel at once.
// One query timeout bounds the whole page: the list and every gate's state.
func (r *TemporalConcurrencyReader) Gates(ctx context.Context, namespace string, pageSize int, token []byte) ([]GateView, []byte, error) {
	pctx, cancel := context.WithTimeout(ctx, r.queryTimeout)
	defer cancel()
	resp, err := r.client.ListWorkflow(pctx, &workflowservice.ListWorkflowExecutionsRequest{
		Query: fmt.Sprintf("WorkflowType = %q AND %s = %q AND ExecutionStatus = %q",
			ConcurrencyGateWorkflowType, task.SearchAttrOrkflowNamespace, artifacts.ResolveNamespace(namespace), "Running"),
		PageSize: int32(pageSize), NextPageToken: token,
	})
	if err != nil {
		return nil, nil, fmt.Errorf("engine: list the namespace's concurrency limits: %w", err)
	}
	executions := resp.GetExecutions()
	views := make([]GateView, len(executions))
	var wg sync.WaitGroup
	sem := make(chan struct{}, gateListParallel)
	for i, e := range executions {
		wg.Add(1)
		go func() {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()
			id := e.GetExecution().GetWorkflowId()
			view, present, err := r.Gate(pctx, id)
			switch {
			case err != nil:
				views[i] = GateView{GateID: id, Namespace: artifacts.ResolveNamespace(namespace), NotAnswering: GateNotAnswering, Cause: err}
			case !present:
				views[i] = GateView{GateID: id, Namespace: artifacts.ResolveNamespace(namespace), NotAnswering: "the limit closed while it was read"}
			default:
				views[i] = view
			}
		}()
	}
	wg.Wait()
	// A caller that left takes no page of not-answering rows for an answer.
	if err := ctx.Err(); err != nil {
		return nil, nil, fmt.Errorf("engine: list the namespace's concurrency limits: %w", err)
	}
	return views, resp.GetNextPageToken(), nil
}

// GateNotAnswering is the sentence of a limit whose state could not be read.
const GateNotAnswering = "the limit did not answer in time"

// Gate queries one gate's state.
func (r *TemporalConcurrencyReader) Gate(ctx context.Context, gateID string) (GateView, bool, error) {
	view, present, err := r.gateState(ctx, gateID)
	if err != nil || !present {
		return GateView{}, false, err
	}
	return gateViewOf(gateID, view), true, nil
}

// gateState queries one gate's whole state; a closed or absent gate has none.
func (r *TemporalConcurrencyReader) gateState(ctx context.Context, gateID string) (GateStateView, bool, error) {
	qctx, cancel := context.WithTimeout(ctx, r.queryTimeout)
	defer cancel()
	// A closed gate is no limit: it is not replayed for an empty answer.
	resp, err := r.client.QueryWorkflowWithOptions(qctx, &client.QueryWorkflowWithOptionsRequest{
		WorkflowID: gateID, QueryType: GateQueryState, QueryRejectCondition: enumspb.QUERY_REJECT_CONDITION_NOT_OPEN})
	var missing *serviceerror.NotFound
	switch {
	case errors.As(err, &missing), err == nil && resp.QueryRejected != nil:
		return GateStateView{}, false, nil
	case err != nil:
		return GateStateView{}, false, fmt.Errorf("engine: ask the concurrency limit %s for its state: %w", gateID, err)
	case resp.QueryResult == nil:
		return GateStateView{}, false, fmt.Errorf("engine: the concurrency limit %s answered nothing", gateID)
	}
	var view GateStateView
	if err := resp.QueryResult.Get(&view); err != nil {
		return GateStateView{}, false, fmt.Errorf("engine: read the concurrency limit %s's state: %w", gateID, err)
	}
	return view, true, nil
}
