package engine

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservice/v1"

	"github.com/orkstack/orkstack/internal/task"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// FCBelow is an open run below the one a cascade began at.
type FCBelow struct {
	RunID       string `json:"run_id"`
	ParentRunID string `json:"parent_run_id,omitempty"`
	// Orkflow is the name its namespace's readers know it by; a shared one's
	// begins with its owner.
	Orkflow string `json:"orkflow,omitempty"`
	// Depth is how far below; 0 is a run only the index of its root names.
	Depth int `json:"depth,omitempty"`
	// Reached: every run between is open, so its parent's cancel reaches it.
	Reached bool `json:"reached,omitempty"`
}

// FCSubtree is every open run below one, as the cluster's index has them: a
// run not indexed yet is not there.
type FCSubtree struct {
	Open []FCBelow `json:"open,omitempty"`
	// Truncated is the bound that cut the search short: "scan" or "timeout".
	Truncated string `json:"truncated,omitempty"`
	// Excluded are runs that name a run of the tree as theirs and may not be
	// acted on, by why.
	Excluded map[string]string `json:"excluded,omitempty"`
	ByRoot   int               `json:"by_root,omitempty"`
}

// The causes a run of a subtree is left out with.
const (
	FCExcludedNamespace  = "other_namespace"
	FCExcludedUnreadable = "unreadable"
	// FCExcludedUnnamed stands for those whose own id may not be shown.
	FCExcludedUnnamed = "(an id that may not be shown)"
)

// forceCancelTreeDepth is far past any tree's: composition is at most
// MaxComposeDepth deep.
const forceCancelTreeDepth = 4 * MaxComposeDepth

// ErrForceCancelNoIndex is a cluster that does not index what started a run.
var ErrForceCancelNoIndex = errors.New("this deployment's cluster does not index the runs a run started")

// Subtree finds every open run below runID, level by level down from it: the
// runs the cluster indexes as started by one of the level above, the open ones
// and, only to look below them, the closed. Cost follows the subtree, not the
// tree it is in.
func (r *TemporalRunReader) Subtree(ctx context.Context, namespace, runID string, s ForceCancelCascadeSettings) (FCSubtree, error) {
	if !r.originIndexed {
		return FCSubtree{}, ErrForceCancelNoIndex
	}
	ctx, cancel := context.WithTimeout(ctx, s.ListTimeout)
	defer cancel()
	described, err := r.client.DescribeWorkflowExecution(ctx, runID, "")
	var gone *serviceerror.NotFound
	if err != nil && !errors.As(err, &gone) {
		return FCSubtree{}, fmt.Errorf("engine: describe run %s: %w", runID, err)
	}
	// A run gone from the cluster is closed, and may be a root: the index of
	// one that is not names nothing.
	top := r.listedRun(described.GetWorkflowExecutionInfo())
	w := &subtreeWalk{r: r, namespace: artifacts.ResolveNamespace(namespace), s: s, out: FCSubtree{Excluded: map[string]string{}}, seen: map[string]bool{runID: true}}
	level := map[string]bool{runID: err == nil && isOpenExecution(described.GetWorkflowExecutionInfo())}
	for depth := 1; len(level) > 0 && depth <= forceCancelTreeDepth && w.out.Truncated == ""; depth++ {
		if level, err = w.below(ctx, level, depth); err != nil {
			break
		}
	}
	if err == nil && top.GetOrigin().GetRootRunId() == "" && r.rootIndexed && w.out.Truncated == "" {
		_, err = w.scan(ctx, fmt.Sprintf("%s = %s AND ExecutionStatus = \"Running\"", task.SearchAttrOrkflowOriginRootRunId, strconv.Quote(runID)), nil, 0, true)
	}
	if err != nil && ctx.Err() != nil && errors.Is(ctx.Err(), context.DeadlineExceeded) {
		w.out.Truncated, err = "timeout", nil
	}
	return w.out, err
}

// subtreeWalk is one search of a subtree.
type subtreeWalk struct {
	r              *TemporalRunReader
	namespace      string
	s              ForceCancelCascadeSettings
	out            FCSubtree
	seen           map[string]bool
	opened, walked int
}

// below is the runs started by those of level, by whether a cancel of the top
// reaches what they start; the open ones are added to the answer.
func (w *subtreeWalk) below(ctx context.Context, level map[string]bool, depth int) (map[string]bool, error) {
	ids := make([]string, 0, len(level))
	for id := range level {
		ids = append(ids, id)
	}
	next := map[string]bool{}
	for len(ids) > 0 {
		n := min(len(ids), w.s.Batch)
		quoted := make([]string, n)
		for i, id := range ids[:n] {
			quoted[i] = strconv.Quote(id)
		}
		ids = ids[n:]
		started := fmt.Sprintf("%s = \"parent-run\" AND %s IN (%s)", task.SearchAttrOrkflowOriginKind, task.SearchAttrOrkflowOriginId, strings.Join(quoted, ", "))
		// The open ones first: they alone are what is asked for, and a closed
		// one may be an earlier generation of an open one.
		for _, open := range []bool{true, false} {
			status := " AND ExecutionStatus = \"Running\""
			if !open {
				status = " AND ExecutionStatus != \"Running\""
			}
			found, err := w.scan(ctx, started+status, level, depth, open)
			for id, reached := range found {
				next[id] = reached
			}
			if err != nil || w.out.Truncated != "" {
				return next, err
			}
		}
	}
	return next, nil
}

// scan reads what query selects: each run once, the open ones into the answer,
// and all of them as what may have started more.
func (w *subtreeWalk) scan(ctx context.Context, query string, level map[string]bool, depth int, open bool) (map[string]bool, error) {
	found := map[string]bool{}
	var token []byte
	for {
		resp, err := w.r.client.ListWorkflow(ctx, &workflowservice.ListWorkflowExecutionsRequest{PageSize: 1000, NextPageToken: token,
			Query: fmt.Sprintf("WorkflowType = %q AND %s", RunWorkflowType, query)})
		if err != nil {
			return found, fmt.Errorf("engine: list the runs a run started: %w", err)
		}
		for _, info := range resp.GetExecutions() {
			id := info.GetExecution().GetWorkflowId()
			if w.seen[id] {
				continue
			}
			if w.full(open) {
				w.out.Truncated = "scan"
				return found, nil
			}
			w.seen[id] = true
			run := w.r.listedRun(info)
			parent := run.GetOrigin().GetId()
			if checkForceCancelText(forceCancelText{"run_id", id, 256}, forceCancelText{"parent_run_id", parent, 256}, forceCancelText{"orkflow", run.GetOrkflowName(), 200},
				forceCancelText{"owner", run.GetOrkflowNamespace(), 200}) != nil {
				// No run's: whoever started it wrote what would be shown.
				w.out.Excluded[FCExcludedUnnamed] = FCExcludedUnreadable
				continue
			}
			switch {
			case run.GetNamespace() == "" || parent == "":
				w.out.Excluded[id] = FCExcludedUnreadable
				continue
			case artifacts.ResolveNamespace(run.GetNamespace()) != w.namespace:
				w.out.Excluded[id] = FCExcludedNamespace
				continue
			}
			reached := level[parent]
			found[id] = reached && open
			if !open {
				continue
			}
			orkflow := run.GetOrkflowName()
			if run.GetOrkflowNamespace() != "" {
				orkflow = run.GetOrkflowNamespace() + "/" + orkflow
			}
			w.out.Open = append(w.out.Open, FCBelow{RunID: id, ParentRunID: parent, Orkflow: orkflow, Depth: depth, Reached: reached})
			if depth == 0 {
				w.out.ByRoot++
			}
		}
		if token = resp.GetNextPageToken(); len(token) == 0 {
			return found, nil
		}
	}
}

// full reports whether the budget of what is read is spent: the open runs', or
// the closed ones', which are read only to look below them.
func (w *subtreeWalk) full(open bool) bool {
	if open {
		w.opened++
		return w.opened > w.s.Scan
	}
	w.walked++
	return w.walked > w.s.WalkScan
}

// ForceCancelCascadeSettings are engine.runs.force_cancel_cascade_*, pinned in
// the input of a force cancel that cascades.
type ForceCancelCascadeSettings struct {
	// Max is how many runs one cascade force-cancels.
	Max int `json:"max"`
	// Scan and WalkScan bound the open and the closed runs one search reads;
	// Batch, the runs one query names.
	Scan     int `json:"scan"`
	WalkScan int `json:"walk_scan"`
	Batch    int `json:"batch"`
	// StartsPerTask is how many force cancels are started at once.
	StartsPerTask int `json:"starts_per_task"`
	// Poll is how often what is waited for is looked at; ListTimeout bounds
	// one search; SettleSlack, how long past the longest grace a run left to
	// its parent's cancel is waited for.
	Poll        time.Duration `json:"poll"`
	ListTimeout time.Duration `json:"list_timeout"`
	SettleSlack time.Duration `json:"settle_slack"`
}

// DefaultForceCancelCascadeSettings are the settings' defaults.
func DefaultForceCancelCascadeSettings() ForceCancelCascadeSettings {
	return ForceCancelCascadeSettings{Max: 200, Scan: 1000, WalkScan: 2000, Batch: 50, StartsPerTask: 25,
		Poll: 10 * time.Second, ListTimeout: 30 * time.Second, SettleSlack: 30 * time.Second}
}
