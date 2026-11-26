package api

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sync"
	"testing"

	"connectrpc.com/connect"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/authn"
	"github.com/orkstack/orkstack/internal/authz"
	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// waitingReader is a cluster that says which runs wait, and whose every gate
// has one holder and one in line.
type waitingReader struct {
	engine.ConcurrencyAdmissionReader
	mu        sync.Mutex
	waiting   []string
	err       error
	described []string
	lines     int
}

func (r *waitingReader) WaitingRuns(_ context.Context, runs []string) ([]string, error) {
	r.described = append(r.described, runs...)
	var out []string
	for _, run := range runs {
		if slices.Contains(r.waiting, run) {
			out = append(out, run)
		}
	}
	return out, r.err
}

func (r *waitingReader) GateLine(context.Context, string) (engine.GateLine, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.lines++
	return engine.GateLine{Present: true, Holders: 1, Queued: 1, HolderRuns: []string{"ork-holder"},
		Line: []engine.GateLineEntry{{Run: "ork-child-0", RunLevel: true}}}, nil
}

// TestGetRun_ShowsWhereAWaitingChildStands: a composed task whose child run
// waits for its own slot says where the child stands, to a caller who may read
// that child; at most api_holders_shown children are asked, whatever the run
// has; and neither a child that cannot be read nor one the caller may not read
// changes the answer.
func TestGetRun_ShowsWhereAWaitingChildStands(t *testing.T) {
	const shown = 3
	child := func(i int) *orkstackv1.TaskRun {
		return &orkstackv1.TaskRun{TaskId: fmt.Sprintf("t%d", i), Status: orkstackv1.Status_STATUS_RUNNING,
			Child: &orkstackv1.TaskChild{RunId: fmt.Sprintf("ork-child-%d", i), ConcurrencyLimited: true}}
	}
	parent := func(rows ...*orkstackv1.TaskRun) *orkstackv1.Run {
		return &orkstackv1.Run{Id: "ork-parent", Namespace: "default", OrkflowName: "outer", Status: orkstackv1.Status_STATUS_RUNNING, TaskRuns: rows}
	}
	get := func(t *testing.T, reader *waitingReader, run *orkstackv1.Run, orkflows ...string) *orkstackv1.Run {
		t.Helper()
		s := newAccessService(t)
		s.concurrencyReader, s.concurrency.APIHoldersShown = reader, shown
		var bindings []*orkstackv1.Binding
		for i, o := range orkflows {
			bindings = append(bindings, &orkstackv1.Binding{Name: fmt.Sprintf("b-%d", i), Role: "reader", Scope: "orkflow:default/" + o, Subjects: []string{"user:corp:alice"}})
		}
		if len(orkflows) > 0 {
			s.access, _ = accessFor(t, authn.ModeEnforce, []*orkstackv1.Role{{Name: "reader", Permissions: []string{string(authz.PermRunRead)}}}, bindings...)
		}
		s.runs.Save(run)
		for i := range 5 {
			s.runs.Save(&orkstackv1.Run{Id: fmt.Sprintf("ork-child-%d", i), Namespace: artifacts.DefaultNamespace, OrkflowName: "inner",
				Status: orkstackv1.Status_STATUS_PENDING, Queued: &orkstackv1.ConcurrencyQueued{Key: "db", Limit: 1}})
		}
		got, err := s.GetRun(asAlice(authn.ModeEnforce), connect.NewRequest(&orkstackv1.GetRunRequest{RunId: "ork-parent"}))
		if err != nil {
			t.Fatal(err)
		}
		return got.Msg.GetRun()
	}
	queuedRows := func(run *orkstackv1.Run) (out []string) {
		for _, row := range run.GetTaskRuns() {
			if row.GetQueued() != nil {
				out = append(out, row.GetTaskId())
			}
		}
		return out
	}

	r := &waitingReader{waiting: []string{"ork-child-0"}}
	got := get(t, r, parent(child(0), child(1)))
	if q := got.GetTaskRuns()[0].GetQueued(); q.GetKey() != "db" || q.GetPosition() != 1 || !q.GetPositionKnown() || !slices.Equal(q.GetHoldersAhead(), []string{"ork-holder"}) {
		t.Errorf("queued %v; want 1st in line at db, behind ork-holder", q)
	}
	if rows := queuedRows(got); !slices.Equal(rows, []string{"t0"}) || r.lines != 1 {
		t.Errorf("%v show a place after %d lines were read; want t0 alone, after one", rows, r.lines)
	}

	r = &waitingReader{waiting: []string{"ork-child-0", "ork-child-1", "ork-child-2", "ork-child-3", "ork-child-4"}}
	got = get(t, r, parent(child(0), child(1), child(2), child(3), child(4)))
	if rows := queuedRows(got); len(r.described) != shown || r.lines != shown || len(rows) != shown {
		t.Errorf("%d children described and %d lines read for %v; want at most %d", len(r.described), r.lines, rows, shown)
	}

	r = &waitingReader{waiting: []string{"ork-child-0"}}
	if rows := queuedRows(get(t, r, parent(child(0)), "outer")); len(rows) != 0 || r.lines != 0 {
		t.Errorf("a caller who may not read the child was shown %v", rows)
	}
	r = &waitingReader{waiting: []string{"ork-child-0"}}
	if rows := queuedRows(get(t, r, parent(child(0)), "outer", "inner")); len(rows) != 1 {
		t.Errorf("a caller who may read the child was shown %v", rows)
	}

	r = &waitingReader{waiting: []string{"ork-gone", "ork-child-1"}, err: errors.New("one could not be described")}
	gone := child(0)
	gone.Child.RunId = "ork-gone"
	if rows := queuedRows(get(t, r, parent(gone, child(1)))); !slices.Equal(rows, []string{"t1"}) {
		t.Errorf("a child that could not be read left %v; want the other one", rows)
	}

	ended, unlimited, unstarted := child(0), child(1), child(2)
	ended.Status, unlimited.Child.ConcurrencyLimited, unstarted.Child.RunId = orkstackv1.Status_STATUS_SUCCEEDED, false, ""
	r = &waitingReader{waiting: []string{"ork-child-0", "ork-child-1"}}
	closed := parent(child(0))
	closed.Status = orkstackv1.Status_STATUS_FAILED
	if a, b := queuedRows(get(t, r, parent(ended, unlimited, unstarted))), queuedRows(get(t, r, closed)); len(a)+len(b)+len(r.described) != 0 {
		t.Errorf("asked of %v and showed %v %v; want nothing", r.described, a, b)
	}
}
