package engine

import (
	"errors"
	"fmt"
	"slices"
	"time"

	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
	"google.golang.org/protobuf/proto"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// ForceCancelWorkflow ends a run that would not end on its own (AGENTS.md §7,
// "Force cancel"; design §22.1.2): it asks the run to cancel, waits its
// grace, and while the run makes progress waits again, up to its ceiling;
// a run that ends on its own in that time is never terminated. A stalled run
// is terminated with a signed record of who decided it, its concurrency
// slots are released at once, and its terminal event is published once.
// Every step but the ending's build and its publication is a local activity
// whose answer this history records, and the workflow always completes with a result, a failed one included.
//
// Its decision says everything it does: each acting step verifies it again
// and takes every value from it, because anyone who can start a workflow on
// the cluster can start this one. Stopping it from outside ends its wait;
// once a termination is sent it finishes what the ended run needs.
//
// It is AutoUpgrade, new in this release: there is no older history of it to
// replay, so it takes no version gate.
func ForceCancelWorkflow(ctx workflow.Context, in ForceCancelInput) (ForceCancelResult, error) {
	f := newForceCancel(ctx, in)
	out, err := f.run(ctx, in)
	if out.Cascade = f.below.wait(f.ctx); out.Cascade != nil && out.Cascade.Unrecorded {
		out.Unaudited = true
	}
	return out, err
}

func (f *forceCancel) run(ctx workflow.Context, in ForceCancelInput) (ForceCancelResult, error) {
	// 0: the decision verifies, and this is the one execution it acts in.
	var verified FCVerified
	if !f.until(ctx, f.now().Add(f.t.SettleLeast), func() error {
		verified = FCVerified{}
		if err := f.step(ActivityFCVerify, in, &verified); err != nil {
			return err
		}
		if verified.Cause == FCCauseKeysUnavailable {
			return errAgain
		}
		return nil
	}) && verified.Cause == "" {
		return ForceCancelResult{State: FCStateFailed, Cause: f.why(ctx)}, nil
	}
	if d := verified.Decision; d != nil && !d.isFor(workflow.GetInfo(ctx)) {
		verified = FCVerified{Cause: FCCauseDecisionUnverified}
	}
	if verified.Decision == nil && verified.Cause == FCCauseKeysUnavailable {
		// Nothing was decided of it, so nobody's decision was refused.
		return ForceCancelResult{State: FCStateFailed, Cause: verified.Cause}, nil
	}
	if verified.Decision == nil {
		if err := f.step(ActivityFCRefuse, verified.Cause, nil); err != nil {
			f.log().Error("force cancel: a refused decision could not be recorded", "cause", verified.Cause, "error", err)
		}
		return ForceCancelResult{State: FCStateFailed, Cause: verified.Cause}, nil
	}
	f.decision = *verified.Decision
	f.out.RequestID = f.decision.RequestID
	if f.decision.Cascade && in.Settings != nil {
		if err := f.cascades(ctx); err != nil {
			return f.out, err
		}
	}
	f.ceiling = f.now().Add(f.decision.Ceiling())
	// 1: the request is recorded. Where nothing is signed it is the only
	// record of who decided, so the run is not ended without it.
	_ = f.audit(forceCancelRequested) // Unaudited says it, and end tries again
	if f.out.Unaudited && in.Unsigned != nil {
		return f.fail(FCCauseAuditUnwritable)
	}
	// 2: a closed run is left as it is.
	var now FCDescribed
	if !f.before(ctx, func() error { return f.step(ActivityFCDescribe, f.at(""), &now) }) {
		return f.gaveUp(ctx)
	}
	if !now.Open {
		f.closed, f.found = now.ExecutionRunID, true
		f.below.begin() // what it started may be open
		return f.finish(f.closed, FCCauseRetriesExhausted)
	}
	// 3: the ordinary cancel.
	ended, cause := f.ask(ctx)
	if cause == "" {
		// Its own cancel first: it starts nothing more below.
		f.below.begin()
	}
	if cause != "" || ended != "" {
		return f.stop(ended, cause)
	}
	for {
		// 4–5: its grace, and whether the run stalled.
		exec, ended, cause := f.wait(ctx)
		if cause != "" || ended != "" {
			return f.stop(ended, cause)
		}
		// 6: what the run holds, read before it is ended.
		f.held = f.held.and(f.gates(exec))
		// 7: terminate, unless it moved since the stall test.
		switches := f.switches
		cause, moved := f.terminate(ctx, exec)
		if moved && f.switches == switches && !f.now().Before(f.ceiling) {
			// It keeps moving: the ceiling ends that, and the record says so.
			f.out.ProgressingAtCeiling = true
			cause, moved = f.terminate(ctx, exec)
		}
		if moved && f.switches != switches {
			// Its newer execution was never asked to cancel.
			f.carried = true
			if ended, cause := f.ask(ctx); cause != "" || ended != "" {
				return f.stop(ended, cause)
			}
		}
		if moved {
			continue // given its grace again
		}
		if cause != "" {
			return f.fail(cause)
		}
		return f.finish(f.closed, "")
	}
}

// isFor reports whether info is the one execution d acts in.
func (d ForceCancelDecision) isFor(info *workflow.Info) bool {
	if d.Of != nil && (info.ParentWorkflowExecution == nil || *info.ParentWorkflowExecution != workflow.Execution{ID: d.Of.WorkflowID, RunID: d.Of.Execution}) {
		return false
	}
	return info.WorkflowExecution.ID == d.WorkflowID()
}

// finish is what an ended run still needs: its close classified, and, when it
// is this force cancel's, its slots released and its ending announced. cause
// is why the force cancel stopped, for a run that turns out not to have ended.
func (f *forceCancel) finish(exec, cause string) (ForceCancelResult, error) {
	f.ended()
	f.out.Execution = exec
	// 9: classify by the verified close.
	closed, ok := f.classify(exec)
	if !ok {
		f.log().Error("force cancel: how the run ended, if it did, could not be told", "run_id", f.decision.RunID,
			"request_id", f.decision.RequestID, "tried", f.tried, "closed", f.closed)
	}
	switch {
	case !ok && f.tried != "" && f.closed == f.tried:
		// It closed, so what it held is free; how it closed, nobody may say.
		f.out.Released = f.release(f.holds(exec))
		return f.end(FCStateFailed, FCCauseUnclassified)
	case !ok && f.tried == "":
		return f.end(FCStateFailed, f.why(f.outside))
	case !ok && cause != "":
		return f.end(FCStateFailed, cause)
	case !ok:
		return f.end(FCStateFailed, FCCauseUnclassified)
	case closed.Request != "" && f.found:
		// Another force cancel ended it before this one began. What that one
		// may have left undone is done, under its request: a release again
		// frees nothing more, and an ending announced again is the same one.
		f.out.EndedBy, f.out.Finishing = closed.Request, true
		f.out.Released = f.release(f.holds(exec))
		return f.end(FCStateAlreadyEnded, f.publish(exec, closed.Request))
	case closed.State != FCStateForceCanceled:
		f.out.EndedBy = closed.Request
		if f.found && closed.State != FCStateFailed {
			return f.end(FCStateAlreadyEnded, closed.Cause)
		}
		return f.end(closed.State, closed.Cause)
	}
	f.out.State, f.out.Terminated = FCStateForceCanceled, true
	// 8: release whatever the run held, at once: what it held before, and
	// what its close shows it took since.
	f.out.Released = f.release(f.holds(exec))
	// 10: the terminal event, once.
	f.out.Cause = f.publish(exec, f.decision.RequestID)
	// 11.
	return f.end(f.out.State, f.out.Cause)
}

const (
	forceCancelRequested = "run.cancel.force.requested"
	forceCancelCompleted = "run.cancel.force.completed"
)

// forceCancel is one force cancel's state.
type forceCancel struct {
	// ctx is disconnected: no step of a force cancel is canceled with it.
	ctx      workflow.Context
	outside  workflow.Context // what stopping the force cancel cancels
	lctx     workflow.Context
	once     workflow.Context // a step tried once
	in       ForceCancelInput
	t        ForceCancelTimings
	decision ForceCancelDecision
	out      ForceCancelResult
	// asked is where the run was last asked to cancel; ceiling, when a run
	// that keeps progressing is ended; carried, that it carried on in a newer
	// execution; extra, that one which did as the ceiling passed had its one
	// grace more.
	asked          FCCanceled
	ceiling        time.Time
	carried, extra bool
	switches       int
	// found: the run had closed before this force cancel began.
	found bool
	// tried is the execution a termination may have been sent to; closed, the
	// one known to have closed; held, what the run was seen to hold.
	tried, closed string
	held          FCGates
	// refusal is why the last step was refused, when it was.
	refusal string
	// patience is when what an ended run still needs is given up on.
	patience time.Time
	// unaudited are the records still to be written.
	unaudited []string
	// below is the cascade, nil where there is none.
	below *cascade
}

func newForceCancel(ctx workflow.Context, in ForceCancelInput) *forceCancel {
	dctx, _ := workflow.NewDisconnectedContext(ctx)
	t := in.timings()
	options := workflow.LocalActivityOptions{
		StartToCloseTimeout: in.attempt(),
		// Without its own bound, every attempt would share one attempt's.
		ScheduleToCloseTimeout: time.Duration(t.StepAttempts)*in.attempt() + time.Minute,
		// The long retry is the workflow's own, a round and a Timer (settle),
		// so one round stays short: an engine that dies repeats little of it.
		RetryPolicy: &temporal.RetryPolicy{MaximumAttempts: int32(t.StepAttempts)},
	}
	f := &forceCancel{ctx: dctx, outside: ctx, in: in, t: t, lctx: workflow.WithLocalActivityOptions(dctx, options)}
	options.RetryPolicy = &temporal.RetryPolicy{MaximumAttempts: 1}
	f.once = workflow.WithLocalActivityOptions(dctx, options)
	return f
}

func (f *forceCancel) step(name string, arg, out any) error { return f.stepOn(f.lctx, name, arg, out) }

func (f *forceCancel) stepOn(ctx workflow.Context, name string, arg, out any) error {
	err := workflow.ExecuteLocalActivity(ctx, name, arg).Get(ctx, out)
	f.refusal = ""
	if refused(err) {
		f.refusal = categoryOf(err).appMessage()
	}
	if err != nil {
		// The result says a cause; what was behind it is only here.
		f.log().Warn("force cancel: a step failed", "step", name, "run_id", f.decision.RunID,
			"request_id", f.decision.RequestID, "error", err)
	}
	return err
}

// at is a step on execution exec.
func (f *forceCancel) at(exec string) FCStep { return FCStep{Input: f.in, ExecutionRunID: exec} }

func (f *forceCancel) now() time.Time { return workflow.Now(f.ctx) }

func (f *forceCancel) log() logger { return workflow.GetLogger(f.ctx) }

// logger is what workflow.GetLogger answers.
type logger interface {
	Warn(msg string, keyvals ...any)
	Error(msg string, keyvals ...any)
}

// until tries until try succeeds or is refused, spacing the tries, up to
// deadline; ctx is what may stop it. A refused decision stays refused.
func (f *forceCancel) until(ctx workflow.Context, deadline time.Time, try func() error) bool {
	for wait := f.t.SettleFirst; ; wait = min(2*wait, f.t.SettleMax) {
		err := try()
		if err == nil {
			return true
		}
		if refused(err) || !f.now().Before(deadline) || workflow.Sleep(ctx, wait) != nil {
			return false
		}
	}
}

// errAgain is a step that answered, and is to be asked again.
var errAgain = errors.New("not yet")

// refused is a step whose decision does not verify, or is not this
// execution's (errForceCancelRefused), or a sealed run's ending
// (sealedEndingRefused).
func refused(err error) bool { return categoryOf(err).appType() == forceCancelRefusedType }

// later is at, or its patience from now if that is later: whatever was
// spent before, a step is not given up on at its first failure.
func (f *forceCancel) later(at time.Time) time.Time {
	if soon := f.now().Add(f.t.Patience); soon.After(at) {
		return soon
	}
	return at
}

// before is a step of a run that is not ended yet: tried up to the ceiling,
// or until the force cancel is stopped.
func (f *forceCancel) before(ctx workflow.Context, try func() error) bool {
	return f.until(ctx, f.later(f.ceiling), try)
}

// gaveUp is a step before the run was ended that never succeeded.
func (f *forceCancel) gaveUp(ctx workflow.Context) (ForceCancelResult, error) {
	return f.fail(f.why(ctx))
}

func (f *forceCancel) why(ctx workflow.Context) string {
	switch {
	case ctx.Err() != nil:
		return FCCauseStopped
	case f.refusal != "":
		return f.refusal
	}
	return FCCauseRetriesExhausted
}

// ended starts the one wait that everything an ended run still needs shares:
// its classification, its ending, its records.
func (f *forceCancel) ended() {
	if f.patience.IsZero() {
		f.patience = f.now().Add(max(f.decision.Ceiling(), f.t.SettleLeast))
	}
}

// after is a step of a run that was ended, or may have been: nothing stops it.
func (f *forceCancel) after(try func() error) bool { return f.until(f.ctx, f.later(f.patience), try) }

// audit writes one record. One that cannot be written is tried again at the
// force cancel's own end, and the result says it is missing.
func (f *forceCancel) audit(kind string) error {
	err := f.step(ActivityFCAudit, FCAuditInput{Input: f.in, Kind: kind, Result: f.out}, nil)
	f.unaudited = slices.DeleteFunc(f.unaudited, func(k string) bool { return k == kind })
	if err != nil {
		f.unaudited = append(f.unaudited, kind)
		f.log().Error("force cancel: a record of it could not be written", "kind", kind,
			"run_id", f.decision.RunID, "request_id", f.decision.RequestID, "error", err)
	}
	f.out.Unaudited = len(f.unaudited) > 0
	return err
}

// end is the force cancel's outcome, recorded.
func (f *forceCancel) end(state, cause string) (ForceCancelResult, error) {
	f.out.State, f.out.Cause = state, cause
	f.unaudited = append(f.unaudited, forceCancelCompleted)
	if f.out.Terminated {
		f.ended()
	} else {
		f.out.ProgressingAtCeiling = false // nothing was ended there
	}
	// Where nothing was ended, its record waits only a little (later).
	f.after(func() error {
		var failed error
		for _, kind := range slices.Clone(f.unaudited) {
			if err := f.audit(kind); err != nil {
				failed = err
			}
		}
		return failed
	})
	return f.out, nil
}

// fail is a force cancel that stops for cause. Where a termination may have
// been sent it looks first whether the run ended: stopping is not a reason to
// leave a run it ended half done.
func (f *forceCancel) fail(cause string) (ForceCancelResult, error) {
	if f.tried != "" {
		return f.finish(f.tried, cause)
	}
	return f.end(FCStateFailed, cause)
}

// stop is a force cancel that ends without a termination: for cause, or
// because the run ended on its own: as ended, or, for forceCancelClassify,
// however its close reads.
func (f *forceCancel) stop(ended, cause string) (ForceCancelResult, error) {
	if cause != "" {
		return f.fail(cause)
	}
	if ended != forceCancelClassify {
		return f.end(ended, "")
	}
	return f.finish(f.closed, FCCauseRetriesExhausted)
}

// forceCancelClassify is "it ended; its close says how".
const forceCancelClassify = "classify"

// ask cancels the run as anyone's cancel does, in the execution it is in now.
// It answers how the run ended, or why the force cancel stops.
func (f *forceCancel) ask(ctx workflow.Context) (ended, cause string) {
	var asked FCCanceled
	if !f.before(ctx, func() error { return f.step(ActivityFCCancel, f.at(""), &asked) }) {
		return "", f.why(ctx)
	}
	f.asked = asked
	switch {
	case asked.Cause != "":
		return "", asked.Cause
	case asked.Closed && (asked.Asked || f.carried || f.tried != ""):
		f.closed = asked.ExecutionRunID
		return forceCancelClassify, ""
	case asked.Closed:
		// Before it was asked: by another force cancel, or by this one's
		// own decision (ForceCancelStarter.direct), or on its own.
		f.closed, f.found = asked.ExecutionRunID, true
		return forceCancelClassify, ""
	}
	return "", ""
}

// wait is the run's grace, and again while it progresses, up to the ceiling.
// It answers the execution to terminate, or how the run ended, or why the
// force cancel stops.
func (f *forceCancel) wait(ctx workflow.Context) (exec, ended, cause string) {
	f.out.ProgressingAtCeiling = false
	for {
		// Past its ceiling a run that keeps carrying on has one grace more, not
		// one in each execution.
		late := !f.now().Before(f.ceiling)
		waited := !late || !f.extra
		if waited {
			f.extra = f.extra || late
			if workflow.Sleep(ctx, f.decision.Grace()) != nil {
				return "", "", FCCauseStopped
			}
		}
		var scanned FCScanned
		window := f.at(f.asked.ExecutionRunID)
		window.L0 = f.asked.L0
		if !f.before(ctx, func() error { return f.step(ActivityFCScan, window, &scanned) }) {
			return "", "", f.why(ctx)
		}
		switch {
		case !scanned.Open:
			f.closed = scanned.ExecutionRunID
			return "", forceCancelClassify, ""
		case scanned.ExecutionRunID != f.asked.ExecutionRunID:
			// It carried on in a new execution, which was never asked to
			// cancel and whose event ids are its own. It is given a grace
			// there, once if its ceiling has passed.
			f.carried = true
			if ended, cause := f.ask(ctx); ended != "" || cause != "" {
				return "", ended, cause
			}
			if f.now().Before(f.ceiling) || !f.extra {
				continue
			}
		case waited && !scanned.Stall.Progressing:
			return f.asked.ExecutionRunID, "", ""
		default:
			f.asked.L0 = scanned.Stall.Newest
		}
		if !f.now().Before(f.ceiling) {
			f.out.ProgressingAtCeiling = true
			return f.asked.ExecutionRunID, "", ""
		}
	}
}

// gates reads what execution exec holds or queues at: one a resume has
// opened since holds its own.
func (f *forceCancel) gates(exec string) FCGates {
	var gates FCGates
	if err := f.step(ActivityFCGates, f.at(exec), &gates); err != nil {
		f.log().Warn("force cancel: what the run holds could not be read", "run_id", f.decision.RunID, "error", err)
		return FCGates{Unreadable: true}
	}
	return gates
}

// holds is what the closed execution exec held: as it was seen to before, and
// as its close says, which is asked until it answers.
func (f *forceCancel) holds(exec string) FCGates {
	var gates FCGates
	if !f.after(func() error { return f.step(ActivityFCGates, f.at(exec), &gates) }) {
		gates.Unreadable = true
	}
	out := f.held.and(gates)
	out.Unreadable = gates.Unreadable
	return out
}

// and is both reads' gates, under the newer seq_base.
func (g FCGates) and(o FCGates) FCGates {
	out := FCGates{Gates: slices.Clone(g.Gates), SeqBase: max(g.SeqBase, o.SeqBase), Unreadable: g.Unreadable || o.Unreadable}
	for _, id := range o.Gates {
		if !slices.Contains(out.Gates, id) {
			out.Gates = append(out.Gates, id)
		}
	}
	return out
}

// terminate sends the termination of exec with its signed record, and again
// up to the ceiling while the run is open and nobody can say it was not sent.
// A lost answer is decided by describing the run again, never by the call's
// own. It answers why the run is not known to be ended; or that it moved,
// since its stall test or to a newer execution, which is asked and waited for
// in its own right, at most TerminateTries times. Stopping the
// force cancel ends it until a termination may have been sent.
func (f *forceCancel) terminate(ctx workflow.Context, exec string) (cause string, moved bool) {
	step := f.at(exec)
	step.L0, step.ProgressingAtCeiling = f.asked.L0, f.out.ProgressingAtCeiling
	deadline := f.later(f.ceiling)
	skewed := false
	for wait := f.t.SettleFirst; ; wait = min(2*wait, f.t.SettleMax) {
		if f.tried == exec {
			ctx = f.ctx
		}
		if ctx.Err() != nil {
			return FCCauseStopped, false
		}
		var sent FCTerminated
		// Once: a try after one whose answer was lost may say it sent nothing.
		err := f.stepOn(f.once, ActivityFCTerminate, step, &sent)
		skewed = err == nil && sent.Cause == FCCauseClockSkew
		switch {
		case refused(err) && f.refusal != "":
			return f.refusal, false
		case skewed: // another engine's clock, or a little later, may agree
		case err == nil && sent.Cause != "":
			return sent.Cause, false
		case err == nil && sent.Progressing:
			return "", true
		case err == nil && sent.Unsent != "":
			f.log().Warn("force cancel: the termination could not be sent", "run_id", f.decision.RunID, "why", sent.Unsent)
		case err != nil || sent.Sent:
			f.out.Terminated, f.tried = true, exec // or may have been
			ctx = f.ctx
		}
		// An engine that died may have sent one this history knows nothing of.
		var now FCDescribed
		if !f.until(ctx, deadline, func() error { return f.step(ActivityFCDescribe, f.at(""), &now) }) {
			return f.why(ctx), false
		}
		switch {
		case !now.Open:
			// By this one, or by one nobody here knows was sent.
			f.closed, f.tried = now.ExecutionRunID, exec
			return "", false
		case now.ExecutionRunID != exec:
			if f.switches++; f.switches == f.t.TerminateTries {
				return FCCauseRunKeptSwitching, false
			}
			return "", true
		}
		if !f.now().Before(deadline) || workflow.Sleep(ctx, wait) != nil {
			break
		}
	}
	if skewed {
		return FCCauseClockSkew, false
	}
	return f.why(ctx), false
}

// classify is how execution exec closed, by its verified close. What cannot
// be told now is asked again, and one still open a little longer; ok is false
// when it never could be.
func (f *forceCancel) classify(exec string) (closed FCClassified, ok bool) {
	ctx := f.outside
	if f.tried != "" {
		ctx = f.ctx // nothing stops what a termination may have left to do
	}
	known, open := false, f.now().Add(f.t.Patience)
	f.until(ctx, f.later(f.patience), func() error {
		var got FCClassified
		if err := f.step(ActivityFCClassify, f.at(exec), &got); err != nil {
			return err
		}
		if got.Open && !f.now().Before(open) {
			return nil
		}
		if got.Open {
			return errAgain
		}
		closed, known = got, true
		if got.Retry {
			return errAgain
		}
		return nil
	})
	return closed, known
}

// release is a force-canceled run's release, as the run makes its own
// (releaseSlots): release_all{run, below its seq_base + 1} to every gate it
// held or queued at. A gate that is gone holds nothing; for any other failure
// the gate's own check pass frees the slot. It reports whether everything
// the run held is known and every gate was told.
func (f *forceCancel) release(gates FCGates) bool {
	futures := make([]workflow.Future, 0, len(gates.Gates))
	for _, gate := range gates.Gates {
		futures = append(futures, workflow.SignalExternalWorkflow(f.ctx, gate, "", GateSignalReleaseAll,
			GateReleaseAll{Run: f.decision.RunID, Below: gates.SeqBase + 1}))
	}
	all := !gates.Unreadable
	for i, future := range futures {
		if err := future.Get(f.ctx, nil); err != nil && !categoryOf(err).unknownRun {
			all = false
			f.log().Warn("force cancel: a concurrency slot could not be released; the limit's check pass frees it",
				"run_id", f.decision.RunID, "gate", gates.Gates[i], "error", err)
		}
	}
	return all
}

// publish builds the run's ending and publishes it through the run's own
// projection, as any run's terminal event: the store, the archive and the
// webhooks see it once. It answers why it is not published.
func (f *forceCancel) publish(exec, request string) string {
	var built FCPublished
	var event *orkstackv1.Event
	if !f.after(func() error {
		built = FCPublished{}
		err := workflow.ExecuteActivity(workflow.WithActivityOptions(f.ctx, f.buildOptions()),
			ActivityFCPublish, f.at(exec)).Get(f.ctx, &built)
		if err == nil {
			if event, err = built.event(); err == nil && !f.mine(built, event, request) {
				err = errAgain
			}
		}
		if err != nil {
			f.log().Warn("force cancel: the run's ending could not be built; it is tried again", "run_id", f.decision.RunID, "error", err)
		}
		return err
	}) {
		return FCCauseEventUnpublished
	}
	key := built.DedupeKey
	if built.Ended {
		f.out.State, f.out.Ends = FCStateEnded, true
	} else {
		key = "force-cancel:" + request
	}
	if len(built.Event) == 0 {
		f.out.TerminalPublished = built.Ended // by the run itself
		return built.Cause
	}
	actx := workflow.WithActivityOptions(f.ctx, f.announceOptions())
	if !f.after(func() error {
		err := workflow.ExecuteActivity(actx, ActivityPublishEvent, event, built.SnapshotURI, key).Get(actx, nil)
		if err != nil {
			f.log().Warn("force cancel: the run's ending could not be published; it is tried again", "run_id", f.decision.RunID, "error", err)
		}
		return err
	}) {
		return FCCauseEventUnpublished
	}
	f.out.TerminalPublished = true
	return ""
}

// mine reports whether an answer is one the ending's build gives for this
// force cancel: it comes from a channel others may take work from. Whoever can
// take it from there holds a credential of the cluster's namespace, which is
// trusted with every event (AGENTS.md §6), and gains nothing by a forged one:
// with a worker of its own that credential schedules PublishEvent itself, with
// any event, snapshot and key, and it can answer a genuine announcement as made
// that was not. So this is no boundary. It holds a wrong answer to this run, its
// own snapshots and its own key, and the readers never ask it: they build the
// ending from the run's close and its signed record.
func (f *forceCancel) mine(built FCPublished, e *orkstackv1.Event, request string) bool {
	switch built.Cause {
	case "", FCCauseEventSequence, FCCauseUnverifiedRecord:
	default:
		return false
	}
	if e == nil {
		return built.Ended || built.Cause != ""
	}
	if e.GetRunId() != f.decision.RunID || e.GetSequence() < 1 || !isTerminalRunEvent(e) || built.Cause != "" ||
		built.SnapshotURI != "" && !artifacts.OverlayBaseURIMatches(built.SnapshotURI, f.decision.Namespace, f.decision.RunID) {
		return false
	}
	if built.Ended {
		// Its own announcement, under its own key (sendEvent's).
		return built.DedupeKey == fmt.Sprintf("%s#%d", f.decision.RunID, e.GetSequence())
	}
	return built.DedupeKey == "" && built.SnapshotURI != "" && e.GetRequestId() == request &&
		e.GetType() == orkstackv1.EventType_EVENT_TYPE_RUN_CANCELED && e.GetStatus() == orkstackv1.Status_STATUS_CANCELED
}

// event is the event an answer carries, nil when it carries none.
func (p FCPublished) event() (*orkstackv1.Event, error) {
	if len(p.Event) == 0 {
		return nil, nil
	}
	e := &orkstackv1.Event{}
	if err := proto.Unmarshal(p.Event, e); err != nil {
		return nil, fmt.Errorf("force cancel: the ending's event is not one: %w", err)
	}
	return e, nil
}

// buildOptions are the ending's build: an ordinary activity on the
// engines' event channel, which reads the whole run and heartbeats. Where no
// engine takes it in time the try has failed and is made again, so the wait
// for one ends as any other does.
func (f *forceCancel) buildOptions() workflow.ActivityOptions {
	return workflow.ActivityOptions{
		TaskQueue:              EventQueue,
		ScheduleToStartTimeout: f.t.PublishStart,
		StartToCloseTimeout:    f.t.PublishTimeout,
		HeartbeatTimeout:       f.t.PublishHeartbeat,
		RetryPolicy:            &temporal.RetryPolicy{MaximumAttempts: 1},
	}
}

// announceOptions are the publication's: a run's own
// (eventActivityOptions), started in bounded time.
func (f *forceCancel) announceOptions() workflow.ActivityOptions {
	options := eventActivityOptions()
	options.ScheduleToStartTimeout = f.t.PublishStart
	return options
}
