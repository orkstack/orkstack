package engine

import (
	"maps"
	"slices"
	"time"

	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"

	"github.com/orkstack/orkstack/internal/task"
)

// forceCancelCascadeQuery answers a cascade as it stands.
const forceCancelCascadeQuery = "force_cancel_cascade"

// forceCancelDeriveTries bounds the ids tried for one run below.
const forceCancelDeriveTries = 3

// forceCancelPollSpread: a long wait is looked at again after this part of it,
// so that it stays a short history.
const forceCancelPollSpread = 20

// forceCancelCascadeReserve is what is kept of a force cancel's own time for its
// cascade's end: its last search, and its record.
const forceCancelCascadeReserve = 20 * time.Minute

// cascade is the runs below of a force cancel that cascades, beside the run's
// own steps. Nothing of it can fail the force cancel, and whatever it cannot
// do it says.
type cascade struct {
	f       *forceCancel
	ctx     workflow.Context // disconnected, as f's
	options workflow.LocalActivityOptions
	s       ForceCancelCascadeSettings
	out     ForceCancelCascade
	rows    map[string]int // a run's place in out.Results
	// until is when those started are waited for no longer.
	until time.Time
	// attached are the runs another caller's force cancel is of, by their
	// place, still looked at.
	attached map[string]int
	// deadline is when it is to be at its end, whatever it waits for.
	deadline time.Time
	// frozen: it has said what became of every run, and changes it no more.
	frozen  bool
	started bool
	done    workflow.Settable
	over    workflow.Future
}

// cascades prepares f's cascade; it begins once the run was asked to cancel.
func (f *forceCancel) cascades(ctx workflow.Context) error {
	c := &cascade{f: f, ctx: f.ctx, s: f.in.Settings.Cascade.bounded(), rows: map[string]int{}, attached: map[string]int{}, out: ForceCancelCascade{State: FCCascadeRunning}}
	timeout := max(f.in.attempt(), c.s.ListTimeout)
	c.options = workflow.LocalActivityOptions{
		StartToCloseTimeout: timeout, ScheduleToCloseTimeout: time.Duration(f.t.StepAttempts)*timeout + time.Minute,
		RetryPolicy: &temporal.RetryPolicy{MaximumAttempts: int32(f.t.StepAttempts)}}
	c.over, c.done = workflow.NewFuture(f.ctx)
	if info := workflow.GetInfo(ctx); info.WorkflowExecutionTimeout > 0 {
		c.deadline = info.WorkflowStartTime.Add(info.WorkflowExecutionTimeout - f.t.CascadeReserve)
	}
	f.below = c
	return workflow.SetQueryHandler(ctx, forceCancelCascadeQuery, func() (ForceCancelCascade, error) { return c.out, nil })
}

// begin starts the cascade, once.
func (c *cascade) begin() {
	if c == nil || c.started {
		return
	}
	c.started = true
	workflow.Go(c.ctx, func(ctx workflow.Context) {
		defer c.done.Set(nil, nil)
		c.run(ctx)
	})
}

// wait is until the cascade is over; nil where there is none, or the force
// cancel stopped before it began.
func (c *cascade) wait(ctx workflow.Context) *ForceCancelCascade {
	if c == nil {
		return nil
	}
	if !c.started {
		// Nothing below was touched, and its record says so.
		c.finish(ctx, true)
	} else {
		_ = c.over.Get(ctx, nil)
	}
	out := c.out
	return &out
}

// stopped: the force cancel was stopped from outside. Those started go on.
func (c *cascade) stopped() bool { return c.f.outside.Err() != nil }

// cut is why the cascade does no more, "" while it goes on.
func (c *cascade) cut() string {
	switch {
	case c.stopped():
		return FCCauseStopped
	case !c.deadline.IsZero() && !c.f.now().Before(c.deadline):
		return FCCauseTookTooLong
	}
	return ""
}

func (c *cascade) step(ctx workflow.Context, name string, arg, out any) error {
	err := workflow.ExecuteLocalActivity(workflow.WithLocalActivityOptions(ctx, c.options), name, arg).Get(ctx, out)
	if err != nil {
		c.f.log().Warn("force cancel: a step of its cascade failed", "step", name, "run_id", c.f.decision.RunID,
			"request_id", c.f.decision.RequestID, "error", err)
	}
	return err
}

// try is a step, tried again for a while; why it was given up on, "" for done.
// A refused one is not tried again, and its refusal is why.
func (c *cascade) try(ctx workflow.Context, name string, arg, out any, gaveUp string) string {
	var err error
	if c.f.until(ctx, c.f.now().Add(c.f.t.Patience), func() error { err = c.step(ctx, name, arg, out); return err }) {
		return ""
	}
	if refused(err) {
		return categoryOf(err).appMessage()
	}
	return gaveUp
}

func (c *cascade) run(ctx workflow.Context) {
	c.cover(ctx, false)
	// Once more: a run may have been started, or indexed, since.
	if c.cut() == "" {
		c.cover(ctx, true)
	}
	stopped := c.stopped()
	c.settle(ctx)
	c.finish(ctx, stopped)
}

// finish is a cascade's outcome, recorded. Nothing changes it after.
func (c *cascade) finish(ctx workflow.Context, stopped bool) {
	c.frozen = true
	c.out.State, c.out.AsOf = c.out.verdict(stopped), c.f.now()
	c.out.Unrecorded = c.try(ctx, ActivityFCSummary, FCSummaryStep{Input: c.f.in, Cascade: c.out}, nil, FCCauseRetriesExhausted) != ""
}

// cover force-cancels what one search finds below and waits for it.
func (c *cascade) cover(ctx workflow.Context, again bool) {
	var found FCBelowList
	c.out.Cause = "" // the newest search's
	if cause := c.try(ctx, ActivityFCBelow, c.f.in, &found, FCCauseCouldNotRead); cause != "" {
		c.out.Rerun, c.out.Cause = true, cause
		return
	}
	var fresh []FCBelow
	for _, b := range found.Rows {
		if _, known := c.rows[b.RunID]; !known {
			fresh = append(fresh, b)
		}
	}
	room := max(0, c.s.Max-len(c.out.Results))
	over := fresh[min(len(fresh), room):]
	fresh = fresh[:min(len(fresh), room)]
	c.out.AsOf, c.out.ByRoot = c.f.now(), max(c.out.ByRoot, found.ByRoot)
	if found.Truncated != "" {
		c.out.Truncated = found.Truncated
	}
	if len(found.Excluded) > 0 {
		c.out.Excluded = found.Excluded
	}
	c.out.Covered, c.out.NotStarted = found.Reached, found.Unreached
	for _, b := range over {
		if b.Reached {
			c.out.Covered++
		} else {
			c.out.NotStarted++
		}
	}
	c.out.Rerun = c.out.Rerun || found.Truncated != "" || again && len(fresh) > 0
	for batch := range slices.Chunk(fresh, c.s.StartsPerTask) {
		if cause := c.cut(); cause != "" {
			for _, b := range batch {
				c.rows[b.RunID] = len(c.out.Results)
				c.out.Results = append(c.out.Results, FCBelowResult{FCBelow: b, State: FCStateFailed, Cause: cause})
			}
			continue
		}
		c.start(ctx, batch)
	}
	c.watch(ctx)
	_, _ = workflow.AwaitWithTimeout(ctx, c.until.Sub(c.f.now()), func() bool { return !c.waiting() || c.stopped() })
	clear(c.attached)
	for i := range c.out.Results {
		if c.out.Results[i].State == FCBelowWaiting || c.out.Results[i].State == FCBelowAttached {
			c.out.Results[i].State = FCBelowPending
		}
	}
}

// start starts a force cancel of each run of batch, all at once: every grace
// runs beside the others, and one that stalls delays none.
func (c *cascade) start(ctx workflow.Context, batch []FCBelow) {
	var wg int
	for _, b := range batch {
		c.rows[b.RunID] = len(c.out.Results)
		c.out.Results = append(c.out.Results, FCBelowResult{FCBelow: b, State: FCBelowWaiting})
		row := c.rows[b.RunID]
		wg++
		workflow.Go(ctx, func(ctx workflow.Context) {
			defer func() { wg-- }()
			c.one(ctx, row)
		})
	}
	_ = workflow.Await(ctx, func() bool { return wg == 0 })
}

// one starts the force cancel of one run below, and leaves it to be waited
// for.
func (c *cascade) one(ctx workflow.Context, row int) {
	set := func(state, cause string) {
		if !c.frozen {
			c.out.Results[row].State, c.out.Results[row].Cause = state, cause
		}
	}
	runID := c.out.Results[row].RunID
	for try := 0; try < c.f.t.DeriveTries; try++ {
		var d FCDerived
		if cause := c.try(ctx, ActivityFCDerive, FCBelowStep{Input: c.f.in, RunID: runID}, &d, FCCauseRetriesExhausted); cause != "" {
			set(FCStateFailed, cause)
			return
		}
		if c.frozen {
			return
		}
		c.out.Results[row].RequestID = d.RequestID
		switch {
		case d.Cause != "":
			set(FCStateFailed, d.Cause)
			return
		case d.Closed:
			set(FCStateAlreadyEnded, "")
			return
		case d.Attached:
			set(FCBelowAttached, "")
			c.out.Results[row].Attached = true
			c.attached[runID] = row
			c.waitFor(d.Bound)
			return
		}
		child := workflow.ExecuteChildWorkflow(workflow.WithChildOptions(ctx, workflow.ChildWorkflowOptions{
			WorkflowID: d.WorkflowID, TaskQueue: task.DefaultQueue, WorkflowExecutionTimeout: d.Timeout,
			WorkflowIDReusePolicy: enumspb.WORKFLOW_ID_REUSE_POLICY_REJECT_DUPLICATE,
			ParentClosePolicy:     enumspb.PARENT_CLOSE_POLICY_ABANDON,
		}), ForceCancelWorkflowType, d.Input)
		err := child.GetChildWorkflowExecution().Get(ctx, nil)
		if categoryOf(err).alreadyStarted {
			// Nobody knows an id before its start is sent, so whatever holds
			// it is nobody's; it is told by its parent, and acts on nothing.
			continue
		}
		if err != nil {
			c.f.log().Error("force cancel: one of a run below could not be started", "run_id", runID, "request_id", d.RequestID, "error", err)
			set(FCStateFailed, FCCauseRetriesExhausted)
			return
		}
		c.follow(ctx, d.Bound, func(ctx workflow.Context) {
			var r ForceCancelResult
			err := child.Get(ctx, &r)
			if err != nil {
				c.f.log().Error("force cancel: one of a run below ended with no result", "run_id", runID, "request_id", d.RequestID, "error", err)
			}
			switch why := categoryOf(err); {
			case why.timeout != nil:
				set(FCStateFailed, FCCauseTimedOut)
			case why.terminated || why.canceled:
				set(FCStateFailed, FCCauseStoppedOutside)
			case err != nil:
				set(FCStateFailed, FCCauseBroke)
			default:
				set(r.State, r.Cause)
			}
		})
		return
	}
	set(FCStateFailed, FCCauseIDOccupied)
}

// ceiling and maxGrace are the input's settings, held to what any may be: they
// are no part of what is signed.
func (c *cascade) ceiling() time.Duration {
	return min(max(c.f.in.Settings.Ceiling, 0), forceCancelMaxCeiling)
}

func (c *cascade) maxGrace() time.Duration {
	return min(max(c.f.in.Settings.MaxGrace, 0), forceCancelMaxCeiling)
}

// waitFor has the cascade wait for bound, and no longer than it can: what
// takes longer than any of this deployment's own force cancels is pending, and
// goes on.
func (c *cascade) waitFor(bound time.Duration) {
	most := c.ceiling() + 2*c.maxGrace() + c.f.t.SettleLeast
	until := c.f.now().Add(min(bound, most))
	if !c.deadline.IsZero() && until.After(c.deadline) {
		until = c.deadline
	}
	if until.After(c.until) {
		c.until = until
	}
}

// follow waits for one run below, for bound at most, beside everything else.
func (c *cascade) follow(ctx workflow.Context, bound time.Duration, wait func(workflow.Context)) {
	c.waitFor(bound)
	workflow.Go(ctx, wait)
}

// waiting reports whether any force cancel below is still waited for.
func (c *cascade) waiting() bool {
	return slices.ContainsFunc(c.out.Results, func(r FCBelowResult) bool { return r.State == FCBelowWaiting || r.State == FCBelowAttached })
}

// watch looks at the runs other callers' force cancels are of, all at once,
// until each closes: less and less often, so a long wait stays a short history.
func (c *cascade) watch(ctx workflow.Context) {
	if len(c.attached) == 0 {
		return
	}
	workflow.Go(ctx, func(ctx workflow.Context) {
		for began := c.f.now(); len(c.attached) > 0 && c.f.now().Before(c.until) && !c.stopped(); {
			if workflow.Sleep(ctx, max(c.s.Poll, c.f.now().Sub(began)/time.Duration(c.f.t.PollSpread))) != nil {
				return
			}
			ids := slices.Sorted(maps.Keys(c.attached))
			var open []string
			if c.step(ctx, ActivityFCOpen, FCOpenStep{Input: c.f.in, RunIDs: ids}, &open) != nil {
				continue
			}
			for _, id := range ids {
				row, waited := c.attached[id]
				if !waited || slices.Contains(open, id) {
					continue
				}
				delete(c.attached, id)
				if !c.frozen {
					c.out.Results[row].State = FCStateAlreadyEnded
				}
			}
		}
	})
}

// settle is what is left open below, once those left to their parents' cancel
// have had their time.
func (c *cascade) settle(ctx workflow.Context) {
	var unsure []string
	for _, r := range c.out.Results {
		if !r.final() {
			unsure = append(unsure, r.RunID)
		}
	}
	began := c.f.now()
	until := began.Add(c.maxGrace() + c.s.SettleSlack)
	for {
		var after FCAfter
		if err := c.step(ctx, ActivityFCAfter, FCOpenStep{Input: c.f.in, RunIDs: unsure}, &after); err != nil {
			after = FCAfter{Unconfirmed: FCCauseCouldNotRead}
			if refused(err) {
				after.Unconfirmed = categoryOf(err).appMessage()
			}
		}
		c.out.After = &after
		if after.Count == 0 && after.Unconfirmed == "" || c.out.Covered == 0 || c.cut() != "" || !c.f.now().Before(until) {
			return
		}
		_ = workflow.Sleep(ctx, max(c.s.Poll, c.f.now().Sub(began)/time.Duration(c.f.t.PollSpread)))
	}
}
