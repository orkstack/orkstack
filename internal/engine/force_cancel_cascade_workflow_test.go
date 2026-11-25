package engine

import (
	"context"
	"fmt"
	"maps"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/workflow"
	"google.golang.org/protobuf/proto"
)

// fcBelow is what is below a force cancel's run, as its cascade sees it.
type fcBelow struct {
	lists    []FCBelowList // each search's answer, the last kept
	derives  map[string][]FCDerived
	children map[string]fcChild
	// open answers, for a run another caller's force cancel is of, how often it
	// is still open; afters, what is open at the end.
	open     map[string]int
	afters   []FCAfter
	opens    *int // how often they are looked at
	failing  map[string]int
	refuses  map[string]bool // steps refused
	stopOn   string          // the run at whose derivation the force cancel is stopped
	afterOf  [][]string
	started  []time.Duration
	startIDs []string
	summary  *ForceCancelCascade
	derived  []string
}

type fcChild struct {
	result ForceCancelResult
	takes  time.Duration
	fails  bool
}

func (b *fcBelow) register(f *fcTarget, env *testsuite.TestWorkflowEnvironment) {
	lock := func() func() { f.mu.Lock(); return f.mu.Unlock }
	reg := func(name string, fn any) { env.RegisterActivityWithOptions(fn, activity.RegisterOptions{Name: name}) }
	fails := func(name string) error {
		for step := range b.refuses {
			if strings.HasPrefix(name, step) {
				return errForceCancelRefused(FCCauseDecisionUnverified)
			}
		}
		if b.failing[name] > 0 {
			b.failing[name]--
			return errFC
		}
		return nil
	}
	reg(ActivityFCBelow, func(context.Context, ForceCancelInput) (FCBelowList, error) {
		defer lock()()
		if err := fails(ActivityFCBelow); err != nil || len(b.lists) == 0 {
			return FCBelowList{}, err
		}
		next := b.lists[0]
		if len(b.lists) > 1 {
			b.lists = b.lists[1:]
		}
		return next, nil
	})
	reg(ActivityFCDerive, func(_ context.Context, in FCBelowStep) (FCDerived, error) {
		defer lock()()
		if b.stopOn == in.RunID {
			f.stop()
		}
		b.derived = append(b.derived, in.RunID)
		if err := fails(ActivityFCDerive + in.RunID); err != nil {
			return FCDerived{}, err
		}
		if d := b.derives[in.RunID]; len(d) > 0 {
			if len(d) > 1 {
				b.derives[in.RunID] = d[1:]
			}
			return d[0], nil
		}
		return FCDerived{WorkflowID: "ork-force-" + in.RunID, RequestID: "fc-1-" + in.RunID, Timeout: time.Hour, Bound: 40 * time.Minute}, nil
	})
	reg(ActivityFCOpen, func(_ context.Context, in FCOpenStep) ([]string, error) {
		defer lock()()
		if b.opens != nil {
			*b.opens++
		}
		var open []string
		for _, id := range in.RunIDs {
			if b.open[id] > 0 {
				b.open[id]--
				open = append(open, id)
			}
		}
		return open, fails(ActivityFCOpen)
	})
	reg(ActivityFCAfter, func(_ context.Context, in FCOpenStep) (FCAfter, error) {
		defer lock()()
		b.afterOf = append(b.afterOf, in.RunIDs)
		if err := fails(ActivityFCAfter); err != nil || len(b.afters) == 0 {
			return FCAfter{}, err
		}
		next := b.afters[0]
		if len(b.afters) > 1 {
			b.afters = b.afters[1:]
		}
		return next, nil
	})
	reg(ActivityFCSummary, func(_ context.Context, in FCSummaryStep) error {
		defer lock()()
		if err := fails(ActivityFCSummary); err != nil {
			return err
		}
		b.summary = &in.Cascade
		return nil
	})
	env.RegisterWorkflowWithOptions(func(ctx workflow.Context, _ ForceCancelInput) (ForceCancelResult, error) {
		info := workflow.GetInfo(ctx)
		id := strings.TrimPrefix(info.WorkflowExecution.ID, "ork-force-")
		f.mu.Lock()
		b.started, b.startIDs = append(b.started, workflow.Now(ctx).Sub(fcEpoch)), append(b.startIDs, id)
		c := b.children[id]
		f.mu.Unlock()
		if c.takes > 0 {
			_ = workflow.Sleep(ctx, c.takes)
		}
		if c.fails {
			return ForceCancelResult{}, errFC
		}
		if c.result.State == "" {
			c.result.State = FCStateForceCanceled
		}
		return c.result, nil
	}, workflow.RegisterOptions{Name: ForceCancelWorkflowType})
}

var fcEpoch = time.Date(2026, 3, 14, 11, 0, 0, 0, time.UTC)

// runCascade runs a force cancel of f that cascades over b.
func runCascade(t *testing.T, f *fcTarget, b *fcBelow, change func(*ForceCancelSettings), tweak ...func(*testsuite.TestWorkflowEnvironment)) (ForceCancelResult, time.Duration) {
	t.Helper()
	f.decision.Cascade = true
	settings := DefaultForceCancelSettings()
	if change != nil {
		change(&settings)
	}
	in := fcInput()
	in.Settings = &settings
	out := runFC(t, f, in, append([]func(*testsuite.TestWorkflowEnvironment){func(e *testsuite.TestWorkflowEnvironment) {
		e.SetStartTime(fcEpoch)
		e.SetWorkflowRunTimeout(24 * time.Hour)
		b.register(f, e)
	}}, tweak...)...)
	if out.Cascade == nil {
		return out, 0
	}
	// Its own end: the environment goes on to that of those it left running.
	return out, out.Cascade.AsOf.Sub(fcEpoch)
}

func below(ids ...string) (out []FCBelow) {
	for i, id := range ids {
		out = append(out, FCBelow{RunID: id, ParentRunID: "ork-r", Orkflow: "pay", Depth: i + 1, Reached: true})
	}
	return out
}

// states is each run below's state, and its cause.
func states(c *ForceCancelCascade) map[string]string {
	out := map[string]string{}
	for _, r := range c.Results {
		out[r.RunID] = r.State
		if r.Cause != "" {
			out[r.RunID] += " " + r.Cause
		}
	}
	return out
}

// TestForceCancelCascade: each open run below has a force cancel of its own,
// and the cascade says what became of every one and what is still open.
func TestForceCancelCascade(t *testing.T) {
	attachedAll := func(rows []FCBelow) map[string][]FCDerived {
		out := map[string][]FCDerived{}
		for _, r := range rows {
			out[r.RunID] = []FCDerived{{Attached: true, RequestID: "fc-9", Bound: 40 * time.Minute}}
		}
		return out
	}
	one := func(s *ForceCancelSettings) { s.Cascade.StartsPerTask = 1 }
	many := func(n int) (out []FCBelow) {
		for i := range n {
			out = append(out, below(fmt.Sprintf("d%03d", i))...)
		}
		return out
	}
	for _, tt := range []struct {
		name   string
		f      func(*fcTarget)
		b      fcBelow
		change func(*ForceCancelSettings)
		state  string
		want   map[string]string
		check  func(*testing.T, ForceCancelResult, *fcBelow, time.Duration)
	}{
		{name: "nothing below", state: FCCascadeComplete, want: map[string]string{}},
		{name: "a tree", b: fcBelow{lists: []FCBelowList{{Rows: below("a", "b", "c")}}, children: map[string]fcChild{
			"a": {result: ForceCancelResult{State: FCStateCanceled}, takes: time.Minute}, "b": {takes: 20 * time.Minute}, "c": {result: ForceCancelResult{State: FCStateEnded}}}},
			state: FCCascadeComplete, want: map[string]string{"a": FCStateCanceled, "b": FCStateForceCanceled, "c": FCStateEnded},
			check: func(t *testing.T, r ForceCancelResult, b *fcBelow, _ time.Duration) {
				for i, at := range b.started {
					if at > time.Second {
						t.Errorf("%s started %s in: none waits for another", b.startIDs[i], at)
					}
				}
				for _, row := range r.Cascade.Results {
					if row.RequestID != "fc-1-"+row.RunID || row.Orkflow != "pay" {
						t.Errorf("row %+v", row)
					}
				}
				if !reflect.DeepEqual(r.Cascade, b.summary) {
					t.Errorf("recorded %+v, answered %+v", b.summary, r.Cascade)
				}
			}},
		{name: "a run that had ended, and what it left open", f: func(f *fcTarget) { f.open, f.closed = false, FCClassified{State: FCStateEnded} },
			b: fcBelow{lists: []FCBelowList{{Rows: below("a")}}}, state: FCCascadeComplete, want: map[string]string{"a": FCStateForceCanceled}},
		{name: "a run that could not be asked", f: func(f *fcTarget) { f.failing[ActivityFCCancel] = 1 << 20 },
			b: fcBelow{lists: []FCBelowList{{Rows: below("a")}}}, state: FCCascadeStopped, want: map[string]string{},
			check: func(t *testing.T, _ ForceCancelResult, b *fcBelow, _ time.Duration) {
				if len(b.derived) != 0 || b.summary == nil || b.summary.State != FCCascadeStopped {
					t.Errorf("derived %v, recorded %+v", b.derived, b.summary)
				}
			}},
		{name: "one that closed before its turn", b: fcBelow{lists: []FCBelowList{{Rows: below("a")}}, derives: map[string][]FCDerived{"a": {{Closed: true}}}},
			state: FCCascadeComplete, want: map[string]string{"a": FCStateAlreadyEnded}},
		{name: "one that is not below", b: fcBelow{lists: []FCBelowList{{Rows: below("a", "b")}}, derives: map[string][]FCDerived{"a": {{Cause: FCCauseNotBelow}}}},
			state: FCCascadePartial, want: map[string]string{"a": "failed not_below", "b": FCStateForceCanceled}},
		{name: "one that could not be decided", b: fcBelow{lists: []FCBelowList{{Rows: below("a", "b")}}, failing: map[string]int{ActivityFCDerive + "a": 1 << 20}},
			state: FCCascadePartial, want: map[string]string{"a": "failed retries_exhausted", "b": FCStateForceCanceled}},
		{name: "one that could not be decided at first", b: fcBelow{lists: []FCBelowList{{Rows: below("a")}}, failing: map[string]int{ActivityFCDerive + "a": 2}},
			state: FCCascadeComplete, want: map[string]string{"a": FCStateForceCanceled}},
		{name: "one whose force cancel answered nothing", b: fcBelow{lists: []FCBelowList{{Rows: below("a")}}, children: map[string]fcChild{"a": {fails: true}}},
			state: FCCascadePartial, want: map[string]string{"a": "failed failed_unexpectedly"}},
		{name: "one whose force cancel failed", b: fcBelow{lists: []FCBelowList{{Rows: below("a")}}, children: map[string]fcChild{"a": {result: ForceCancelResult{State: FCStateFailed, Cause: FCCauseClockSkew}}}},
			state: FCCascadePartial, want: map[string]string{"a": "failed clock_skew"}},
		{name: "one whose id something holds, once", b: fcBelow{lists: []FCBelowList{{Rows: below("held", "a")}},
			derives: map[string][]FCDerived{"a": {{WorkflowID: "ork-force-held", Timeout: time.Hour, Bound: time.Hour}, {WorkflowID: "ork-force-a", RequestID: "fc-1-a", Timeout: time.Hour, Bound: time.Hour}}}},
			change: one, state: FCCascadeComplete, want: map[string]string{"held": FCStateForceCanceled, "a": FCStateForceCanceled}},
		{name: "one whose id something always holds", b: fcBelow{lists: []FCBelowList{{Rows: below("held", "a")}},
			derives: map[string][]FCDerived{"a": {{WorkflowID: "ork-force-held", Timeout: time.Hour, Bound: time.Hour}}}},
			change: one, state: FCCascadePartial, want: map[string]string{"held": FCStateForceCanceled, "a": "failed id_occupied"}},
		{name: "one another caller's is of", b: fcBelow{lists: []FCBelowList{{Rows: below("a")}}, derives: map[string][]FCDerived{"a": {{Attached: true, RequestID: "fc-9", Bound: 40 * time.Minute}}}, open: map[string]int{"a": 3}},
			state: FCCascadeComplete, want: map[string]string{"a": FCStateAlreadyEnded},
			check: func(t *testing.T, r ForceCancelResult, _ *fcBelow, _ time.Duration) {
				if row := r.Cascade.Results[0]; !row.Attached || row.RequestID != "fc-9" {
					t.Errorf("row %+v", row)
				}
			}},
		{name: "one another caller's is of, that does not end", b: fcBelow{lists: []FCBelowList{{Rows: below("a")}}, derives: map[string][]FCDerived{"a": {{Attached: true, RequestID: "fc-9", Bound: 40 * time.Minute}}}, open: map[string]int{"a": 1 << 20}, afters: []FCAfter{{Count: 1, RunIDs: []string{"a"}}}},
			state: FCCascadePartial, want: map[string]string{"a": FCBelowPending},
			check: func(t *testing.T, r ForceCancelResult, b *fcBelow, _ time.Duration) {
				if !slices.Equal(b.afterOf[0], []string{"a"}) || r.Cascade.After.Count != 1 {
					t.Errorf("looked at %v: %+v", b.afterOf, r.Cascade.After)
				}
			}},
		{name: "one that takes longer than it is waited for", b: fcBelow{lists: []FCBelowList{{Rows: below("a")}}, children: map[string]fcChild{"a": {takes: 10 * time.Hour}}},
			state: FCCascadePartial, want: map[string]string{"a": FCBelowPending},
			check: func(t *testing.T, _ ForceCancelResult, _ *fcBelow, took time.Duration) {
				if took > 2*time.Hour {
					t.Errorf("took %s", took)
				}
			}},
		{name: "one found later", b: fcBelow{lists: []FCBelowList{{Rows: below("a")}, {Rows: below("a", "b")}}}, state: FCCascadeComplete, want: map[string]string{"a": FCStateForceCanceled, "b": FCStateForceCanceled},
			check: func(t *testing.T, r ForceCancelResult, _ *fcBelow, _ time.Duration) {
				if !r.Cascade.Rerun {
					t.Error("another run of it is not suggested")
				}
			}},
		{name: "many", b: fcBelow{lists: []FCBelowList{{Rows: many(60)}}}, state: FCCascadeComplete},
		{name: "more than it covers, left to their parents' cancel", b: fcBelow{lists: []FCBelowList{{Rows: below("a"), Reached: 2}}, afters: []FCAfter{{Count: 2}, {Count: 1}, {}}},
			state: FCCascadeComplete, want: map[string]string{"a": FCStateForceCanceled},
			check: func(t *testing.T, r ForceCancelResult, b *fcBelow, _ time.Duration) {
				if r.Cascade.Covered != 2 || len(b.afterOf) != 3 {
					t.Errorf("%+v, looked %d times", r.Cascade, len(b.afterOf))
				}
			}},
		{name: "more than it covers, that stay open", b: fcBelow{lists: []FCBelowList{{Rows: below("a"), Reached: 2}}, afters: []FCAfter{{Count: 2, RunIDs: []string{"x", "y"}}}},
			state: FCCascadePartial, want: map[string]string{"a": FCStateForceCanceled},
			check: func(t *testing.T, _ ForceCancelResult, _ *fcBelow, took time.Duration) {
				if took < 10*time.Minute || took > 15*time.Minute {
					t.Errorf("took %s: they are given the longest grace, and no more", took)
				}
			}},
		{name: "more than it covers, that nothing reaches", b: fcBelow{lists: []FCBelowList{{Rows: below("a"), Unreached: 1}}}, state: FCCascadePartial, want: map[string]string{"a": FCStateForceCanceled}},
		{name: "a later search that finds more than is left room for", b: fcBelow{lists: []FCBelowList{{Rows: below("a", "b")}, {Rows: below("c", "d")}}},
			change: func(s *ForceCancelSettings) { s.Cascade.Max = 3 }, state: FCCascadeComplete, want: map[string]string{"a": FCStateForceCanceled, "b": FCStateForceCanceled, "c": FCStateForceCanceled},
			check: func(t *testing.T, r ForceCancelResult, _ *fcBelow, _ time.Duration) {
				if r.Cascade.Covered != 1 {
					t.Errorf("%+v", r.Cascade)
				}
			}},
		{name: "runs that may not be acted on", b: fcBelow{lists: []FCBelowList{{Excluded: map[string]string{"a": "other_namespace"}}}}, state: FCCascadePartial, want: map[string]string{}},
		{name: "a search cut short that the last one was not", b: fcBelow{lists: []FCBelowList{{Rows: below("a"), Truncated: "scan"}}}, state: FCCascadeComplete, want: map[string]string{"a": FCStateForceCanceled},
			check: func(t *testing.T, r ForceCancelResult, _ *fcBelow, _ time.Duration) {
				if !r.Cascade.Rerun || r.Cascade.Truncated != "scan" {
					t.Errorf("%+v", r.Cascade)
				}
			}},
		{name: "still open after it", b: fcBelow{lists: []FCBelowList{{Rows: below("a")}}, afters: []FCAfter{{Count: 1, RunIDs: []string{"z"}}}}, state: FCCascadePartial, want: map[string]string{"a": FCStateForceCanceled}},
		{name: "not known after it", b: fcBelow{afters: []FCAfter{{Unconfirmed: FCCauseTooManyRuns}}}, state: FCCascadePartial, want: map[string]string{}},
		{name: "what could not be looked for", b: fcBelow{failing: map[string]int{ActivityFCBelow: 1 << 20, ActivityFCAfter: 1 << 20}}, state: FCCascadePartial, want: map[string]string{},
			check: func(t *testing.T, r ForceCancelResult, _ *fcBelow, _ time.Duration) {
				if r.Cascade.After.Unconfirmed != FCCauseCouldNotRead || !r.Cascade.Rerun || r.Cascade.Cause != FCCauseCouldNotRead {
					t.Errorf("%+v", r.Cascade)
				}
			}},
		{name: "steps that are refused", b: fcBelow{lists: []FCBelowList{{Rows: below("a")}}, refuses: map[string]bool{ActivityFCDerive: true, ActivityFCAfter: true}}, state: FCCascadePartial, want: map[string]string{"a": "failed decision_unverified"},
			check: func(t *testing.T, r ForceCancelResult, _ *fcBelow, took time.Duration) {
				if r.Cascade.After.Unconfirmed != FCCauseDecisionUnverified || took > time.Minute {
					t.Errorf("after %s: %+v", took, r.Cascade)
				}
			}},
		{name: "a search that is refused", b: fcBelow{refuses: map[string]bool{ActivityFCBelow: true}}, state: FCCascadeComplete, want: map[string]string{},
			check: func(t *testing.T, r ForceCancelResult, _ *fcBelow, _ time.Duration) {
				if r.Cascade.Cause != FCCauseDecisionUnverified {
					t.Errorf("%+v", r.Cascade)
				}
			}},
		{name: "a record that cannot be written at first", b: fcBelow{failing: map[string]int{ActivityFCSummary: 4}}, state: FCCascadeComplete, want: map[string]string{},
			check: func(t *testing.T, _ ForceCancelResult, b *fcBelow, _ time.Duration) {
				if b.summary == nil {
					t.Error("never written")
				}
			}},
		{name: "a record that never can be", b: fcBelow{failing: map[string]int{ActivityFCSummary: 1 << 20}}, state: FCCascadeComplete, want: map[string]string{},
			check: func(t *testing.T, r ForceCancelResult, _ *fcBelow, _ time.Duration) {
				if !r.Cascade.Unrecorded || !r.Unaudited {
					t.Errorf("%+v", r)
				}
			}},
		{name: "one that needs longer than any force cancel asked for", b: fcBelow{lists: []FCBelowList{{Rows: below("a")}}, children: map[string]fcChild{"a": {takes: 10 * time.Hour}},
			derives: map[string][]FCDerived{"a": {{WorkflowID: "ork-force-a", Timeout: 20 * time.Hour, Bound: 12 * time.Hour}}}},
			state: FCCascadePartial, want: map[string]string{"a": FCBelowPending},
			check: func(t *testing.T, _ ForceCancelResult, _ *fcBelow, took time.Duration) {
				if took > 2*time.Hour {
					t.Errorf("took %s", took)
				}
			}},
		{name: "many that other callers' are of", b: fcBelow{lists: []FCBelowList{{Rows: many(120)}}, derives: attachedAll(many(120)), open: map[string]int{"d005": 4}, opens: new(int)},
			state: FCCascadeComplete,
			check: func(t *testing.T, r ForceCancelResult, b *fcBelow, _ time.Duration) {
				if *b.opens != 5 || len(r.Cascade.Results) != 120 || r.Cascade.Results[5].State != FCStateAlreadyEnded {
					t.Errorf("looked %d times: %+v", *b.opens, r.Cascade.Results[5])
				}
			}},
		{name: "stopped", b: fcBelow{lists: []FCBelowList{{Rows: below("a", "b", "c")}}, children: map[string]fcChild{"a": {takes: 20 * time.Minute}}, stopOn: "b"},
			change: func(s *ForceCancelSettings) { s.Cascade.StartsPerTask = 2 }, state: FCCascadeStopped, want: map[string]string{"a": FCBelowPending, "b": FCStateForceCanceled, "c": "failed stopped"},
			check: func(t *testing.T, r ForceCancelResult, b *fcBelow, took time.Duration) {
				if slices.Contains(b.derived, "c") || took > 10*time.Minute || b.summary == nil || b.summary.State != FCCascadeStopped {
					t.Errorf("derived %v, took %s, recorded %+v", b.derived, took, b.summary)
				}
			}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			f := newFCTarget()
			if tt.f != nil {
				tt.f(f)
			}
			b := tt.b
			out, took := runCascade(t, f, &b, tt.change)
			if out.Cascade == nil || out.Cascade.State != tt.state {
				t.Fatalf("%+v, want it %s", out.Cascade, tt.state)
			}
			if got := states(out.Cascade); tt.want != nil && !maps.Equal(got, tt.want) {
				t.Errorf("%v, want %v", got, tt.want)
			}
			if len(b.started) > 0 && len(b.started) != len(b.startIDs) {
				t.Errorf("started %v", b.startIDs)
			}
			for _, id := range b.startIDs {
				if n := len(slices.DeleteFunc(slices.Clone(b.startIDs), func(o string) bool { return o != id })); n != 1 {
					t.Errorf("%s has %d force cancels", id, n)
				}
			}
			if tt.check != nil {
				tt.check(t, out, &b, took)
			}
		})
	}
	t.Run("one that does not cascade has none", func(t *testing.T) {
		b := &fcBelow{lists: []FCBelowList{{Rows: below("a")}}}
		out := runFC(t, newFCTarget(), fcInput(), func(e *testsuite.TestWorkflowEnvironment) {})
		if out.Cascade != nil || len(b.derived) != 0 {
			t.Errorf("%+v", out.Cascade)
		}
	})
	t.Run("it ends before its own time does", func(t *testing.T) {
		f, b := newFCTarget(), &fcBelow{lists: []FCBelowList{{Rows: below("a"), Reached: 1}}, children: map[string]fcChild{"a": {takes: 10 * time.Hour}}, afters: []FCAfter{{Count: 1}}}
		out, took := runCascade(t, f, b, nil, func(e *testsuite.TestWorkflowEnvironment) {
			e.SetStartWorkflowOptions(client.StartWorkflowOptions{ID: f.decision.WorkflowID(), TaskQueue: "ork-tasks", WorkflowExecutionTimeout: 50 * time.Minute})
		})
		if out.Cascade.State != FCCascadePartial || took > 32*time.Minute || b.summary == nil {
			t.Errorf("after %s: %+v", took, out.Cascade)
		}
	})
	t.Run("a late search waits for what it finds, not for what it gave up on", func(t *testing.T) {
		b := &fcBelow{lists: []FCBelowList{{Rows: below("a")}, {Rows: below("a", "b")}}, children: map[string]fcChild{"a": {takes: 10 * time.Hour}, "b": {takes: time.Minute}}}
		out, took := runCascade(t, newFCTarget(), b, nil)
		if want := map[string]string{"a": FCBelowPending, "b": FCStateForceCanceled}; !maps.Equal(states(out.Cascade), want) || took > 45*time.Minute {
			t.Errorf("after %s: %v", took, states(out.Cascade))
		}
	})
	t.Run("at its most it is a small payload", func(t *testing.T) {
		long := strings.Repeat("o", 200)
		c := ForceCancelCascade{State: FCCascadePartial, Excluded: map[string]string{}, After: &FCAfter{}}
		list := FCBelowList{Excluded: map[string]string{}}
		for i := range forceCancelCascadeMost {
			row := FCBelow{RunID: fmt.Sprintf("ork-%026d", i), ParentRunID: fmt.Sprintf("ork-%026d", i+1), Orkflow: long + "/" + long, Depth: 4, Reached: true}
			list.Rows = append(list.Rows, row)
			c.Results = append(c.Results, FCBelowResult{FCBelow: row, State: FCStateForceCanceled, RequestID: strings.Repeat("0", 69), Attached: true, Cause: FCCauseUnclassified})
			c.After.RunIDs = append(c.After.RunIDs, row.RunID)
		}
		for i := range forceCancelExcludedMost {
			c.Excluded[fmt.Sprintf("ork-%026d", i)] = FCExcludedNamespace
		}
		list.Excluded = c.Excluded
		for _, v := range []any{c, list} {
			p, err := DataConverter().ToPayload(v)
			if err != nil || proto.Size(p) > 256<<10 {
				t.Errorf("%T is %d bytes, %v", v, proto.Size(p), err)
			}
		}
	})
	t.Run("where it stands can be asked", func(t *testing.T) {
		f, b := newFCTarget(), &fcBelow{lists: []FCBelowList{{Rows: below("a", "b")}}, children: map[string]fcChild{"a": {takes: 20 * time.Minute}}}
		var seen ForceCancelCascade
		_, _ = runCascade(t, f, b, nil, func(e *testsuite.TestWorkflowEnvironment) {
			e.RegisterDelayedCallback(func() {
				v, err := e.QueryWorkflow(forceCancelCascadeQuery)
				if err != nil || v.Get(&seen) != nil {
					t.Errorf("query: %v", err)
				}
			}, 10*time.Minute)
		})
		if want := map[string]string{"a": FCBelowWaiting, "b": FCStateForceCanceled}; seen.State != FCCascadeRunning || !maps.Equal(states(&seen), want) {
			t.Errorf("%+v", seen)
		}
	})
}

// TestForceCancelCascade_SettingsAreHeld: what a cascade waits for is bounded
// whatever its input, which nobody signed, says.
func TestForceCancelCascade_SettingsAreHeld(t *testing.T) {
	for _, tt := range []struct{ said, held time.Duration }{{-time.Hour, 0}, {time.Minute, time.Minute}, {1 << 62, forceCancelMaxCeiling}} {
		c := &cascade{f: &forceCancel{in: ForceCancelInput{Settings: &ForceCancelSettings{Ceiling: tt.said, MaxGrace: tt.said}}}}
		if c.ceiling() != tt.held || c.maxGrace() != tt.held {
			t.Errorf("%s: %s and %s, want %s", tt.said, c.ceiling(), c.maxGrace(), tt.held)
		}
	}
}
