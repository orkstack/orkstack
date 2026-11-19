package engine

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"time"

	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/client"

	"github.com/orkstack/orkstack/internal/task"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// The reads submit's key and line checks make (E88 §3.6, §7.1 steps 5-7,
// and the count a max_running refusal names). Each is a lock-free read of
// the cluster, best-effort by a bounded overshoot, as max_running is: the
// gate itself is what holds a limit strictly.

// More submit refusals of a concurrency limit (§7.1): a line or a
// namespace's queued runs at their bound, and a namespace at its bound of
// active keys. Both are ResourceExhausted.
const (
	ConcurrencyQueueFullCode     = "CONCURRENCY_QUEUE_FULL"
	ConcurrencyKeysExhaustedCode = "CONCURRENCY_KEYS_EXHAUSTED"
)

// GateLine is a gate's answer to submit: how many runs and tasks hold its
// key and how many wait. Present is false when no gate runs for the key,
// which is a key that nothing holds or waits for.
type GateLine struct {
	Present bool
	Holders int
	Queued  int

	// What GetRun reads to say where a queued run stands (LiveQueued):
	// the line in order, run-level and task-level entries alike; the runs
	// holding a slot, earliest granted first, and those of them a liveness
	// pass found stalled; and whether the gate is rebuilding its record,
	// and since when a rebuild has been failing (zero when it is not).
	Line                []GateLineEntry
	HolderRuns          []string
	StalledRuns         []string
	Rebuilding          bool
	RebuildFailingSince time.Time
}

// GateLineEntry is one place in a gate's line.
type GateLineEntry struct {
	Run      string
	RunLevel bool // the run's own slot, not one of its tasks'
}

// ConcurrencyAdmissionReader is what submit reads of the cluster for a
// limit. A read that cannot be made is an error, never zero.
type ConcurrencyAdmissionReader interface {
	// ActiveGates counts the namespace's active gates (§3.6).
	ActiveGates(ctx context.Context, namespace string) (int64, error)
	// QueuedRuns counts the namespace's runs waiting in a line (§7.1 step
	// 1 and step 7), composed children included: they carry the attribute
	// too.
	QueuedRuns(ctx context.Context, namespace string) (int64, error)
	// GateLine is one gate's line, by its state query.
	GateLine(ctx context.Context, gateID string) (GateLine, error)
	// OpenGates counts every running gate of the deployment: a key with
	// runs holding or waiting, or lingering empty. A release refuses a
	// target whose engines cannot hold limits while it is above zero.
	OpenGates(ctx context.Context) (int64, error)
}

// TemporalConcurrencyReader reads the cluster for submit's checks.
type TemporalConcurrencyReader struct {
	client       client.Client
	queryTimeout time.Duration
}

// NewTemporalConcurrencyReader reads through c; a state query or a count
// waits at most queryTimeout (the census's bound when zero).
func NewTemporalConcurrencyReader(c client.Client, queryTimeout time.Duration) *TemporalConcurrencyReader {
	if queryTimeout <= 0 {
		queryTimeout = censusQueryTimeout
	}
	return &TemporalConcurrencyReader{client: c, queryTimeout: queryTimeout}
}

// ActiveGates is CountWorkflow over the namespace's active gates.
func (r *TemporalConcurrencyReader) ActiveGates(ctx context.Context, namespace string) (int64, error) {
	return r.count(ctx, activeGatesQuery(namespace))
}

// QueuedRuns is CountWorkflow over the namespace's running runs that wait
// in a line.
func (r *TemporalConcurrencyReader) QueuedRuns(ctx context.Context, namespace string) (int64, error) {
	return r.count(ctx, queuedRunsQuery(namespace))
}

// OpenGates is CountWorkflow over every running gate.
func (r *TemporalConcurrencyReader) OpenGates(ctx context.Context) (int64, error) {
	return r.count(ctx, openGatesQuery())
}

func openGatesQuery() string {
	return fmt.Sprintf("WorkflowType = %q AND ExecutionStatus = %q", ConcurrencyGateWorkflowType, "Running")
}

func activeGatesQuery(namespace string) string {
	return fmt.Sprintf("WorkflowType = %q AND %s = %q AND %s = %q AND ExecutionStatus = %q",
		ConcurrencyGateWorkflowType, task.SearchAttrOrkflowNamespace, artifacts.ResolveNamespace(namespace),
		task.SearchAttrOrkflowGatePhase, gatePhaseActive, "Running")
}

func queuedRunsQuery(namespace string) string {
	return fmt.Sprintf("WorkflowType = %q AND %s = %q AND %s = %q AND ExecutionStatus = %q",
		RunWorkflowType, task.SearchAttrOrkflowNamespace, artifacts.ResolveNamespace(namespace),
		task.SearchAttrOrkflowRunQueued, "true", "Running")
}

func (r *TemporalConcurrencyReader) count(ctx context.Context, query string) (int64, error) {
	// Bounded as the state query is: a slow index refuses the submit
	// unavailable rather than holding it.
	cctx, cancel := context.WithTimeout(ctx, r.queryTimeout)
	defer cancel()
	response, err := r.client.CountWorkflow(cctx, &workflowservice.CountWorkflowExecutionsRequest{Query: query})
	if err != nil {
		return 0, fmt.Errorf("engine: count the cluster's index for a concurrency limit: %w", err)
	}
	return response.GetCount(), nil
}

// GateLine queries the gate's state; a gate id with no execution is no
// line.
func (r *TemporalConcurrencyReader) GateLine(ctx context.Context, gateID string) (GateLine, error) {
	qctx, cancel := context.WithTimeout(ctx, r.queryTimeout)
	defer cancel()
	value, err := r.client.QueryWorkflow(qctx, gateID, "", GateQueryState)
	var missing *serviceerror.NotFound
	switch {
	case errors.As(err, &missing):
		return GateLine{}, nil
	case err != nil:
		return GateLine{}, fmt.Errorf("engine: ask the concurrency limit %s for its line: %w", gateID, err)
	}
	var view GateStateView
	if err := value.Get(&view); err != nil {
		return GateLine{}, fmt.Errorf("engine: read the concurrency limit %s's line: %w", gateID, err)
	}
	return gateLineOf(view.State), nil
}

// gateLineOf reads a gate's state as GateLine.
func gateLineOf(g gateState) GateLine {
	line := GateLine{Present: true, Holders: len(g.Holders), Queued: len(g.Queue)}
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
		line.Line = append(line.Line, GateLineEntry{Run: e.Run, RunLevel: e.TaskIx == gateRunLevel})
	}
	holders := slices.Clone(g.Holders)
	slices.SortStableFunc(holders, func(a, b gateEntry) int { return a.GrantedAt.Compare(b.GrantedAt) })
	for _, e := range holders {
		line.HolderRuns = append(line.HolderRuns, e.Run)
		if e.Stalled {
			line.StalledRuns = append(line.StalledRuns, e.Run)
		}
	}
	switch g.Rebuild.State {
	case gateRebuildListing, gateRebuildSettling:
		line.Rebuilding = true
	case gateRebuildFailing:
		line.Rebuilding = true
		line.RebuildFailingSince = g.Rebuild.Since
	}
	return line
}
