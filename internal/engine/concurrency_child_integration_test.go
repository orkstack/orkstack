//go:build integration

package engine_test

import (
	"context"
	"errors"
	"slices"
	"strings"
	"testing"
	"time"

	"connectrpc.com/connect"
	"go.temporal.io/sdk/temporal"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/internal/testsupport/must"
)

// composeLimited starts runID, an Orkflow with one composed task whose child
// declares a limit of one on key (a template over its inputs), as a submit
// plans it: the child's key unrendered. held, when set, is the key of the
// parent's own limit. It returns the child's run id.
func (c *concRun) composeLimited(runID, key, strategy, held string) string {
	c.t.Helper()
	child := compile(c.t, strings.Replace(concurrencyDocument("conc-inner", "hold"), "concurrency:\n", "inputs:\n  env: {type: string, required: true}\nconcurrency:\n", 1))
	stampDigest(c.t, child)
	with, err := structpbStruct(map[string]any{"env": c.stamp})
	must.OK(c.t, err)
	parent := &orkstackv1.Orkflow{Name: "conc-outer", Version: "1.0.0", Tasks: []*orkstackv1.Task{{Id: "release",
		Uses: "orkflow:conc-inner@1.0.0", With: with,
		Orkflow: &orkstackv1.OrkflowRef{Name: "conc-inner", Version: "1.0.0", DefinitionDigest: child.GetDefinitionDigest()}}}}
	stampDigest(c.t, parent)

	slotted := concurrencyRunPlan(c.canStack, key, 120, time.Minute, nil, nil)
	limit := &engine.ChildConcurrency{Slot: slotted.Concurrency.Slots[0], Plan: *slotted.Concurrency, MaxActiveKeys: 1000}
	limit.Slot.GateID, limit.Slot.Strategy = "", strategy
	limit.Plan.Slots, limit.Plan.Run = nil, nil
	cp, err := engine.NewChildRunPlan(child, c.plan(engine.DefaultHistoryMaxEvents, nil), false, false)
	must.OK(c.t, err)
	cp.RunID, cp.Concurrency = runID+"-c", limit
	plan := c.plan(engine.DefaultHistoryMaxEvents, nil)
	if held != "" {
		plan = concurrencyRunPlan(c.canStack, held, 120, time.Minute, nil, nil)
	}
	plan.Children = map[string]*engine.ChildRunPlan{"release": cp}
	must.OKf(c.t, c.starter.StartRun(c.ctx, runID, parent, nil, plan), "StartRun %s", runID)
	return cp.RunID
}

// TestIntegration_ConcurrencyChildKeepsItsOwnLimit: a composed child asks for
// its own slot on a real gate, under the key its parent renders when it starts
// it. It waits in line inside its composed task, which GetRun on the parent
// shows, and two parents' children share their Orkflow's default key; a child
// whose key its parent holds never starts; a child refused at a full key fails
// its composed task; and an operator's cancel of a parent whose child waits
// ends both and leaves nothing in the line. With ORK_RECORD_GOLDEN_HISTORY=1
// it records the parents (replay_concurrency_child_test.go).
func TestIntegration_ConcurrencyChildKeepsItsOwnLimit(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	c := newConcRun(t, ctx, "conc-child", nil)
	c.starter = c.starter.WithNamespaceIndex(true).WithConcurrencyIndex(true)
	a := newConcAPI(c)
	row := func(id string) (*orkstackv1.Run, *orkstackv1.TaskRun) {
		t.Helper()
		got, err := a.svc.GetRun(ctx, connect.NewRequest(&orkstackv1.GetRunRequest{RunId: id}))
		must.OKf(t, err, "GetRun %s", id)
		rows := got.Msg.GetRun().GetTaskRuns()
		if len(rows) == 0 {
			return got.Msg.GetRun(), nil // it has not started yet
		}
		return got.Msg.GetRun(), rows[0]
	}
	// waits blocks until GetRun on the parent says its child waits first in
	// line, and checks the rest of what it says.
	waits := func(parent, child, key, holder string) {
		t.Helper()
		c.waitFor(parent+" showing its child's place", func() bool {
			_, r := row(parent)
			return r.GetQueued().GetPositionKnown() && len(r.GetQueued().GetHoldersAhead()) > 0
		})
		run, r := row(parent)
		if q := r.GetQueued(); run.GetStatus() != orkstackv1.Status_STATUS_RUNNING || r.GetStatus() != orkstackv1.Status_STATUS_RUNNING ||
			r.GetChild().GetRunId() != child || !r.GetChild().GetConcurrencyLimited() ||
			q.GetKey() != key || q.GetPosition() != 1 || !slices.Equal(q.GetHoldersAhead(), []string{holder}) {
			t.Fatalf("run %v, its composed task %v; want both RUNNING, and %s first in line at %s behind %s", run.GetStatus(), r, child, key, holder)
		}
	}
	record := func(f concurrencyChildFixtureCase, id string) {
		t.Helper()
		h := c.history(id, "")
		closed, _ := closingEvent(h)
		if closed != f.closed {
			t.Fatalf("%s closed %s, want %s", id, closed, f.closed)
		}
		must.OKf(t, replayRecorded(h, id), "%s just recorded does not replay", id)
		writeRecording(t, "concurrency-child", f.path(), h)
	}

	// Its key rendered from its inputs, held by a run of its own.
	db := "db-" + c.stamp
	holder, keyed := c.id("ckh"), c.id("ck")
	c.start(holder, concurrencyDocument("conc-holder", "hold"), concurrencyRunPlan(c.canStack, db, 120, time.Minute, nil, nil))
	c.waitFor(holder+" holding", func() bool { return c.holds.holding(holder) })
	child := c.composeLimited(keyed, "db-{{ .inputs.env }}", engine.ConcurrencyStrategyQueue, "")
	waits(keyed, child, db, holder)
	if c.holds.holding(child) {
		t.Fatalf("%s ran beside the run holding its key", child)
	}
	// A child that may not wait is refused there, and fails its task.
	refused := c.id("cr")
	refusedChild := c.composeLimited(refused, "db-{{ .inputs.env }}", engine.ConcurrencyStrategyRefuse, "")
	if err := c.end(refused); err == nil {
		t.Fatalf("%s succeeded; its child's key is held", refused)
	}
	if _, r := row(refused); r.GetStatus() != orkstackv1.Status_STATUS_FAILED || r.GetChild().GetRunId() != refusedChild || c.holds.holding(refusedChild) {
		t.Fatalf("the composed task is %v; want failed by its refused child", r)
	}
	if run, err := c.reader.GetRun(ctx, refusedChild); err != nil || run.GetError().GetCode() != engine.RunConcurrencyLimitCode {
		t.Fatalf("the refused child ended %v, %v; want %s", run.GetError(), err, engine.RunConcurrencyLimitCode)
	}
	// A cancel of a parent whose child waits ends both, and empties the line.
	canceled := c.id("cc")
	canceledChild := c.composeLimited(canceled, "db-{{ .inputs.env }}", engine.ConcurrencyStrategyQueue, "")
	c.waitGate(c.gate(db), canceledChild+" in line", func(v engine.GateStateView) bool { return slices.Contains(waiting(v), canceledChild) })
	must.OKf(t, c.starter.CancelRun(ctx, canceled, engine.CancelRequest{}), "CancelRun %s", canceled)
	var stopped *temporal.CanceledError
	if err := c.end(canceled); !errors.As(err, &stopped) {
		t.Fatalf("%s ended %v, want canceled", canceled, err)
	}
	c.waitGate(c.gate(db), "the line to hold the first child alone", func(v engine.GateStateView) bool { return slices.Equal(waiting(v), []string{child}) })
	c.holds.let(holder)
	c.mustEnd(holder)
	c.waitFor(child+" running", func() bool { return c.holds.holding(child) })
	if _, r := row(keyed); r.GetQueued() != nil {
		t.Fatalf("a running child is still shown waiting: %v", r.GetQueued())
	}
	c.holds.let(child)
	c.mustEnd(keyed)
	record(concurrencyChildFixtures[0], keyed)

	// Two parents' children share the default key of their Orkflow.
	key := engine.DefaultRunConcurrencyKey("default", "conc-inner")
	first, second := c.id("cu1"), c.id("cu2")
	firstChild := c.composeLimited(first, key, engine.ConcurrencyStrategyQueue, "")
	c.waitFor(firstChild+" running", func() bool { return c.holds.holding(firstChild) })
	secondChild := c.composeLimited(second, key, engine.ConcurrencyStrategyQueue, "")
	waits(second, secondChild, key, firstChild)
	c.holds.let(firstChild)
	c.mustEnd(first)
	c.waitFor(secondChild+" running", func() bool { return c.holds.holding(secondChild) })
	c.holds.let(secondChild)
	c.mustEnd(second)
	record(concurrencyChildFixtures[1], first)
	record(concurrencyChildFixtures[2], second)

	// A key its parent holds: the child never starts, and no line has it.
	conflict := c.id("cx")
	held := "own-" + c.stamp
	conflictChild := c.composeLimited(conflict, "own-{{ .inputs.env }}", engine.ConcurrencyStrategyQueue, held)
	if err := c.end(conflict); err == nil {
		t.Fatalf("%s succeeded; its child asked for its own key", conflict)
	}
	if _, r := row(conflict); r.GetError().GetCode() != engine.TaskConcurrencyKeyConflictCode || r.GetChild().GetRunId() != "" {
		t.Fatalf("the composed task is %v; want %s and no child", r, engine.TaskConcurrencyKeyConflictCode)
	}
	if _, err := describeExecution(c.client, ctx, conflictChild, ""); err == nil {
		t.Fatalf("%s was started", conflictChild)
	}
	record(concurrencyChildFixtures[3], conflict)

	for _, gate := range []string{c.gate(db), c.gate(key), c.gate(held)} {
		c.waitGate(gate, "an empty line", func(v engine.GateStateView) bool { return len(holders(v))+len(waiting(v)) == 0 })
	}
}
