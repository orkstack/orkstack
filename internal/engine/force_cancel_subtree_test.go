package engine

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	workflowpb "go.temporal.io/api/workflow/v1"
	"go.temporal.io/api/workflowservice/v1"

	"github.com/orkstack/orkstack/internal/testsupport/must"
)

// treeRun is a run of a tree as the cluster indexes it.
type treeRun struct {
	id, parent, root, namespace, orkflow, owner string
	closed, again                               bool // again: an earlier generation closed beside it
}

// treeClient is a cluster holding runs, answering the two queries a subtree
// search makes.
type treeClient struct {
	runReadClient
	runs    []treeRun
	queries []string
	page    int
	err     error
	gone    bool
	block   bool
}

func (c *treeClient) info(r treeRun, closed bool) *workflowpb.WorkflowExecutionInfo {
	memo := map[string]*commonpb.Payload{}
	for key, value := range map[string]string{MemoNamespace: r.namespace, MemoOrkflowName: r.orkflow, MemoOrkflowOwner: r.owner, MemoOriginID: r.parent, MemoOriginRootRunID: r.root} {
		if value != "" {
			p, err := DataConverter().ToPayload(value)
			if err != nil {
				panic(err)
			}
			memo[key] = p
		}
	}
	if r.parent != "" {
		memo[MemoOriginKind], _ = DataConverter().ToPayload("parent-run")
	}
	info := &workflowpb.WorkflowExecutionInfo{Execution: &commonpb.WorkflowExecution{WorkflowId: r.id}, Status: enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING, Memo: &commonpb.Memo{Fields: memo}}
	if closed {
		info.Status = enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED
	}
	return info
}

func (c *treeClient) DescribeWorkflowExecution(_ context.Context, id, _ string) (*workflowservice.DescribeWorkflowExecutionResponse, error) {
	for _, r := range c.runs {
		if r.id == id && !c.gone {
			return &workflowservice.DescribeWorkflowExecutionResponse{WorkflowExecutionInfo: c.info(r, r.closed)}, nil
		}
	}
	return nil, serviceerror.NewNotFound("none")
}

func (c *treeClient) ListWorkflow(ctx context.Context, in *workflowservice.ListWorkflowExecutionsRequest) (*workflowservice.ListWorkflowExecutionsResponse, error) {
	if c.block {
		<-ctx.Done()
		return nil, ctx.Err()
	}
	q := in.GetQuery()
	c.queries = append(c.queries, q)
	var all []*workflowpb.WorkflowExecutionInfo
	for _, r := range c.runs {
		open := strings.Contains(q, `ExecutionStatus = "Running"`)
		named := strings.Contains(q, "OrkflowOriginId IN") && r.parent != "" && strings.Contains(q, fmt.Sprintf("%q", r.parent)) ||
			strings.Contains(q, "OrkflowOriginRootRunId = ") && r.root != "" && strings.Contains(q, fmt.Sprintf("%q", r.root))
		if named && open != r.closed {
			all = append(all, c.info(r, r.closed))
		}
		if named && !open && r.again {
			all = append(all, c.info(r, true))
		}
	}
	from, page := 0, max(c.page, 1000)
	if len(in.GetNextPageToken()) > 0 {
		fmt.Sscan(string(in.GetNextPageToken()), &from)
	}
	if c.page > 0 {
		page = c.page
	}
	out := &workflowservice.ListWorkflowExecutionsResponse{Executions: all[from:min(len(all), from+page)]}
	if from+page < len(all) {
		out.NextPageToken = fmt.Appendf(nil, "%d", from+page)
	}
	return out, c.err
}

// TestSubtree: every open run below one is found, level by level, through
// closed ones; each is there once, and what may not be acted on is said.
func TestSubtree(t *testing.T) {
	run := func(id, parent string) treeRun {
		return treeRun{id: id, parent: parent, root: "x", namespace: "payments", orkflow: "pay"}
	}
	closed := func(r treeRun) treeRun { r.closed = true; return r }
	settings := DefaultForceCancelCascadeSettings()
	subtree := func(t *testing.T, c *treeClient, s ForceCancelCascadeSettings) FCSubtree {
		t.Helper()
		c.runs = append([]treeRun{{id: "x", namespace: "payments"}}, c.runs...)
		got, err := (&TemporalRunReader{client: c, converter: DataConverter(), originIndexed: true, rootIndexed: true}).Subtree(context.Background(), "payments", "x", s)
		must.OK(t, err)
		return got
	}
	// shape is id/parent/depth of each, "!" where a parent's cancel does not
	// reach it.
	shape := func(s FCSubtree) (out []string) {
		for _, b := range s.Open {
			line := fmt.Sprintf("%s<%s@%d", b.RunID, b.ParentRunID, b.Depth)
			if !b.Reached {
				line += "!"
			}
			out = append(out, line)
		}
		slices.Sort(out)
		return out
	}
	for name, tt := range map[string]struct {
		runs []treeRun
		want []string
	}{
		"nothing below":                 {nil, nil},
		"a chain":                       {[]treeRun{run("a", "x"), run("b", "a"), run("c", "b"), run("d", "c")}, []string{"a<x@1", "b<a@2", "c<b@3", "d<c@4"}},
		"a level":                       {[]treeRun{run("a", "x"), run("b", "x")}, []string{"a<x@1", "b<x@1"}},
		"an orphan below a closed run":  {[]treeRun{closed(run("a", "x")), run("b", "a"), run("c", "b")}, []string{"b<a@2!", "c<b@3!"}},
		"an earlier generation's close": {[]treeRun{{id: "a", parent: "x", root: "x", namespace: "payments", orkflow: "pay", again: true}, run("b", "a")}, []string{"a<x@1", "b<a@2"}},
		"another tree's":                {[]treeRun{{id: "a", parent: "y", root: "y", namespace: "payments", orkflow: "pay"}}, nil},
		"one only its root names":       {[]treeRun{run("b", "gone")}, []string{"b<gone@0!"}},
	} {
		t.Run(name, func(t *testing.T) {
			got := subtree(t, &treeClient{runs: tt.runs}, settings)
			if !reflect.DeepEqual(shape(got), tt.want) || got.Truncated != "" || len(got.Excluded) != 0 {
				t.Fatalf("%v (%+v), want %v", shape(got), got, tt.want)
			}
		})
	}
	t.Run("a run below one that is no root", func(t *testing.T) {
		c := &treeClient{runs: []treeRun{{id: "x", parent: "r", root: "r", namespace: "payments"}, {id: "a", parent: "x", root: "r", namespace: "payments"}, {id: "s", parent: "r", root: "r", namespace: "payments"}}}
		got, err := (&TemporalRunReader{client: c, converter: DataConverter(), originIndexed: true, rootIndexed: true}).Subtree(context.Background(), "payments", "x", settings)
		if err != nil || !reflect.DeepEqual(shape(got), []string{"a<x@1"}) || got.ByRoot != 0 {
			t.Fatalf("%v, %v", shape(got), err)
		}
	})
	t.Run("what may not be acted on", func(t *testing.T) {
		got := subtree(t, &treeClient{runs: []treeRun{{id: "a", parent: "x", namespace: "other"}, {id: "b", parent: "x"}, run("c", "a"), run("d", "x")}}, settings)
		if want := map[string]string{"a": "other_namespace", "b": "unreadable"}; !maps.Equal(got.Excluded, want) || !reflect.DeepEqual(shape(got), []string{"c<a@0!", "d<x@1"}) {
			t.Fatalf("%+v", got)
		}
	})
	t.Run("what would hide text is not named", func(t *testing.T) {
		got := subtree(t, &treeClient{runs: []treeRun{run("a\x1b[0m", "x"), {id: "b", parent: "x", root: "x", namespace: "payments", orkflow: "p\u202e"}, {id: "c", parent: "x", root: "x", namespace: "payments", orkflow: "pay", owner: "b\n"}}}, settings)
		if len(got.Open) != 0 || !maps.Equal(got.Excluded, map[string]string{FCExcludedUnnamed: FCExcludedUnreadable}) {
			t.Fatalf("%+v", got)
		}
	})
	t.Run("a shared Orkflow's name begins with its owner", func(t *testing.T) {
		got := subtree(t, &treeClient{runs: []treeRun{{id: "a", parent: "x", namespace: "payments", orkflow: "pay", owner: "billing"}}}, settings)
		if len(got.Open) != 1 || got.Open[0].Orkflow != "billing/pay" {
			t.Fatalf("%+v", got)
		}
	})
	t.Run("many, in pages and in batches", func(t *testing.T) {
		c := &treeClient{page: 2}
		for i := range 7 {
			c.runs = append(c.runs, run(fmt.Sprintf("a%d", i), "x"), run(fmt.Sprintf("b%d", i), fmt.Sprintf("a%d", i)))
		}
		few := settings
		few.Batch = 3
		if got := subtree(t, c, few); len(got.Open) != 14 || got.Truncated != "" {
			t.Fatalf("%d found, %+v", len(got.Open), got)
		}
		for _, q := range c.queries {
			if i := strings.Index(q, "IN ("); i >= 0 && strings.Count(q[i:strings.Index(q, ")")], ",") > 2 {
				t.Errorf("a query names more than a batch: %s", q)
			}
		}
	})
	t.Run("its bounds", func(t *testing.T) {
		three := []treeRun{run("a", "x"), run("b", "x"), run("c", "x")}
		scan, walk := settings, settings
		scan.Scan, walk.WalkScan = 2, 2
		for name, tt := range map[string]struct {
			runs      []treeRun
			s         ForceCancelCascadeSettings
			open      int
			truncated string
		}{
			"open":             {three, scan, 2, "scan"},
			"open at it":       {three[:2], scan, 2, ""},
			"closed":           {[]treeRun{closed(three[0]), closed(three[1]), closed(three[2]), run("d", "x")}, walk, 1, "scan"},
			"closed, not open": {[]treeRun{closed(three[0]), closed(three[1]), run("d", "x"), run("e", "x"), run("f", "x")}, walk, 3, ""},
		} {
			if got := subtree(t, &treeClient{runs: slices.Clone(tt.runs)}, tt.s); len(got.Open) != tt.open || got.Truncated != tt.truncated {
				t.Errorf("%s: %+v", name, got)
			}
		}
		short := settings
		short.ListTimeout = time.Millisecond
		if got := subtree(t, &treeClient{block: true}, short); got.Truncated != "timeout" {
			t.Errorf("out of time: %+v", got)
		}
	})
	t.Run("a run gone from the cluster still has what it started", func(t *testing.T) {
		got := subtree(t, &treeClient{gone: true, runs: []treeRun{run("a", "x")}}, settings)
		if !reflect.DeepEqual(shape(got), []string{"a<x@1!"}) {
			t.Fatalf("%+v", got)
		}
	})
	t.Run("what cannot be read is an error", func(t *testing.T) {
		c := &treeClient{err: errors.New("down"), runs: []treeRun{{id: "x", namespace: "payments"}}}
		r := &TemporalRunReader{client: c, converter: DataConverter(), originIndexed: true}
		if _, err := r.Subtree(context.Background(), "payments", "x", settings); err == nil {
			t.Error("no error")
		}
		r.originIndexed = false
		if _, err := r.Subtree(context.Background(), "payments", "x", settings); !errors.Is(err, ErrForceCancelNoIndex) {
			t.Errorf("without the index: %v", err)
		}
	})
}
