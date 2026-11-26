package engine

import (
	"context"
	"encoding/json/jsontext"
	"errors"
	"fmt"
	"maps"
	"slices"
	"time"

	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/eventsource"
	"github.com/orkstack/orkstack/internal/held"
	"github.com/orkstack/orkstack/internal/task"
	"github.com/orkstack/orkstack/pkg/eventsources"
)

// The signals of a source's workflow.
const (
	SignalSourceSync     = "sync"
	SignalSourceReport   = "report"
	SignalSourceRevoked  = "revoked"
	SignalSourceJudged   = "judged"
	SignalSourceReroute  = "reroute"
	SignalSourceHandover = "handover"
)

// The local activities of a source's workflow, which the engine that runs its
// task runs.
const (
	ActivityLoadEventSource  = "LoadEventSource"
	ActivityPurgeEventSource = "PurgeEventSource"
	ActivityPauseEventSource = "PauseEventSource"
	ActivityCheckEventSource = "CheckEventSource"
)

const (
	// maxReportedReason bounds a report's reason in the workflow's state.
	maxReportedReason = 256
	// SourceCarryUnknownType is the failure of a workflow started with a carry
	// of a format this release does not know: it starts nothing.
	SourceCarryUnknownType = "SourceCarryUnknown"
)

// SourceInput starts a source's workflow.
type SourceInput struct {
	Namespace string `json:"namespace"`
	Name      string `json:"name"`
	ID        string `json:"id"`
	// Pool and Instances are the document's its caller read.
	Pool      string `json:"pool,omitempty"`
	Instances int32  `json:"instances,omitempty"`
	// Carry is the last execution's, as the bytes that were there, and a first
	// one's where there is none.
	Carry jsontext.Value `json:"carry"`
	// StartedAfter says that the execution before this one was ended from
	// outside, and how.
	StartedAfter string `json:"startedAfter,omitempty"`
}

// SourceReport is an instance's report, as the engine relays it.
type SourceReport struct {
	Index  int32  `json:"index"`
	Epoch  int64  `json:"epoch"`
	What   string `json:"what"`
	Reason string `json:"reason,omitempty"`
}

// SourceRevoked is a revoke an engine observed for a spec that runs.
type SourceRevoked struct {
	Cause  string `json:"cause"`
	Digest string `json:"digest"`
	Save   string `json:"save"`
}

// SourceJudged is an accepting confirmation of an instance whose source could
// not be judged before.
type SourceJudged struct {
	Index                 int32     `json:"index"`
	ObservedUnreadFirstAt time.Time `json:"observedUnreadFirstAt"`
	JudgedAt              time.Time `json:"judgedAt"`
}

// SourceReroute asks for one activity to be scheduled again.
type SourceReroute struct {
	Index int32 `json:"index"`
	Epoch int64 `json:"epoch"`
}

// SourceRef names a source to a local activity.
type SourceRef struct {
	Namespace string `json:"namespace"`
	Name      string `json:"name"`
	ID        string `json:"id"`
}

// SourceLoadAsk is a load's question. Revision is the service revision of the
// last load that succeeded: a restart after it pins the numbers again.
type SourceLoadAsk struct {
	SourceRef
	Revision int64 `json:"revision"`
	// MissingSaid is when the document's absence was last logged.
	MissingSaid time.Time `json:"missingSaid,omitzero"`
}

// SourcePauseAsk is a pause to write.
type SourcePauseAsk struct {
	SourceRef
	Code    string `json:"code"`
	Refusal string `json:"refusal,omitempty"`
	Detail  string `json:"detail,omitempty"`
	// Revision is the service revision it was decided of.
	Revision int64 `json:"revision"`
	// ResumeSeq is the last resume it was decided after.
	ResumeSeq int64 `json:"resumeSeq"`
}

// SourceCheckAsk is the hold-down's question of what the slots run.
type SourceCheckAsk struct {
	SourceRef
	Specs []eventsource.Spec `json:"specs"`
}

// SourceCheckAnswer is its answer: one of allowed and refused, or neither
// where it was not had.
type SourceCheckAnswer struct {
	Allowed bool   `json:"allowed,omitempty"`
	Refused bool   `json:"refused,omitempty"`
	Detail  string `json:"detail,omitempty"`
}

// SourceWork is what a source's workflow asks of an engine.
type SourceWork interface {
	LoadEventSource(ctx context.Context, ask SourceLoadAsk) (eventsource.Load, error)
	// PurgeEventSource removes what is stored under the id, at most batch
	// objects, and reports whether it left some.
	PurgeEventSource(ctx context.Context, ref SourceRef, batch int) (left bool, err error)
	// PauseEventSource reports whether the source stands paused or marked by a
	// delete, by this or before it. One that is not there, and one that has
	// become another since the pause was decided, is not.
	PauseEventSource(ctx context.Context, ask SourcePauseAsk) (bool, error)
	CheckEventSource(ctx context.Context, ask SourceCheckAsk) (SourceCheckAnswer, error)
}

// EventSourceWorkflow owns one source's instances. It is registered PINNED
// (register.go): it replays only on the release it started on, and moves to
// another by ending there and starting here. It decides nothing itself:
// internal/eventsource does, from what it is told and the workflow's clock.
func EventSourceWorkflow(ctx workflow.Context, in SourceInput) error {
	carry, err := eventsource.DecodeCarry(in.Carry)
	if err != nil {
		why := "the carry this source's workflow was started with is damaged"
		if errors.Is(err, eventsource.ErrCarryFormat) {
			why = "the carry this source's workflow was started with cannot be read; a newer release wrote it"
		}
		return temporal.NewNonRetryableApplicationError(why, SourceCarryUnknownType, nil)
	}
	now := workflow.Now(ctx)
	r := &sourceRun{in: in, m: eventsource.New(&carry, carry.Pinned, now), activities: map[int64]*sourceActivity{},
		source: eventsource.Source{Namespace: in.Namespace, Name: in.Name, Pool: in.Pool, Instances: in.Instances}}
	if in.StartedAfter != "" {
		r.m.Continued = &eventsource.Continued{When: now, How: in.StartedAfter}
	}
	r.do(ctx, r.m.Start())
	for !r.over {
		info := workflow.GetInfo(ctx)
		r.do(ctx, r.m.OnHistory(workflow.Now(ctx), int64(info.GetCurrentHistoryLength()), int64(info.GetCurrentHistorySize())))
		if r.over {
			break
		}
		r.writeMemo(ctx)
		if err := r.wait(ctx); err != nil {
			return err
		}
	}
	r.stopTimer()
	r.writeMemo(ctx)
	if !r.carriesOn {
		return nil
	}
	next := in
	next.Carry, next.StartedAfter, next.Pool, next.Instances = jsontext.Value(r.written[0]), "", r.source.Pool, r.source.Instances
	return workflow.NewContinueAsNewError(ctx, SourceWorkflowType, next)
}

// sourceActivity is one instance's activity.
type sourceActivity struct {
	schedule eventsource.Schedule
	future   workflow.Future
	cancel   workflow.CancelFunc
}

// sourceRun is one execution.
type sourceRun struct {
	in         SourceInput
	m          *eventsource.Machine
	activities map[int64]*sourceActivity
	// Each is the local activity of its kind that is being run.
	load, purge, pause, check workflow.Future

	timer       workflow.Future
	timerAt     time.Time
	cancelTimer workflow.CancelFunc

	source eventsource.Source
	// written are the memo's two blocks as they were last written.
	written   [2]string
	over      bool
	carriesOn bool
}

func (r *sourceRun) ref() SourceRef {
	return SourceRef{Namespace: r.in.Namespace, Name: r.in.Name, ID: r.in.ID}
}

// do carries out what the machine decided, in its order.
func (r *sourceRun) do(ctx workflow.Context, cmds []eventsource.Command) {
	p := r.m.Pinned
	for _, cmd := range cmds {
		switch c := cmd.(type) {
		case eventsource.Schedule:
			r.schedule(ctx, c)
		case eventsource.Cancel:
			if a, ok := r.activities[c.Epoch]; ok {
				a.cancel()
			}
		case eventsource.StartLoad:
			ask := SourceLoadAsk{SourceRef: r.ref(), MissingSaid: c.MissingSaid}
			if r.m.Loaded != nil {
				ask.Revision = r.m.Loaded.Revision
			}
			r.load = r.local(ctx, p.LoadTimeout, nil, ActivityLoadEventSource, ask)
		case eventsource.StartPurge:
			r.purge = r.local(ctx, p.LoadTimeout, nil, ActivityPurgeEventSource, r.ref(), p.PurgeBatch)
		case eventsource.WritePause:
			r.pause = r.local(ctx, p.LoadTimeout, nil, ActivityPauseEventSource, SourcePauseAsk{SourceRef: r.ref(), Code: c.Code, Refusal: c.Refusal, Detail: c.Detail, Revision: c.Revision, ResumeSeq: c.ResumeSeq})
		case eventsource.StartCheck:
			r.check = r.local(ctx, p.CheckTimeout, &temporal.RetryPolicy{InitialInterval: p.CheckBackoff, BackoffCoefficient: 2, MaximumAttempts: int32(p.CheckTries)}, ActivityCheckEventSource, SourceCheckAsk{SourceRef: r.ref(), Specs: c.Specs})
		case eventsource.End:
			r.over = true
		case eventsource.ContinueAsNew:
			r.over, r.carriesOn = true, true
		}
	}
}

// local runs one of the engine's answers as a local activity: the run worker
// is local activities' alone, and what an Orker polls has none of these. With
// no retry it is tried once: the machine tries again, on its own back-off.
func (r *sourceRun) local(ctx workflow.Context, timeout time.Duration, retry *temporal.RetryPolicy, name string, args ...any) workflow.Future {
	if retry == nil {
		retry = &temporal.RetryPolicy{MaximumAttempts: 1}
	}
	return workflow.ExecuteLocalActivity(workflow.WithLocalActivityOptions(ctx, workflow.LocalActivityOptions{StartToCloseTimeout: timeout, RetryPolicy: retry}), name, args...)
}

func (r *sourceRun) schedule(ctx workflow.Context, c eventsource.Schedule) {
	p := r.m.Pinned
	actx, cancel := workflow.WithCancel(ctx)
	actx = workflow.WithActivityOptions(actx, workflow.ActivityOptions{
		ActivityID:          held.ActivityID(c.Index, c.Epoch),
		TaskQueue:           c.Spec.Queue,
		HeartbeatTimeout:    p.Heartbeat,
		StartToCloseTimeout: time.Duration(p.AgeBound)*p.MaxAge + p.StopGrace,
		WaitForCancellation: true,
		RetryPolicy:         &temporal.RetryPolicy{InitialInterval: p.RestartBase, BackoffCoefficient: 2, MaximumInterval: p.RestartMax},
	})
	in := &orkstackv1.HostEventSourceInput{Namespace: r.in.Namespace, Name: r.in.Name, EventSourceId: r.in.ID, Index: c.Index, Instances: c.Instances,
		Epoch: c.Epoch, ServiceDigest: c.Spec.Digest, Trial: c.TrialCode, CompleteAfterHealthy: c.CompleteAfterHealthy,
		ConfirmUnavailableMaxSeconds: int32(p.ConfirmUnavailableMax / time.Second), ProviderRefusedMax: int32(p.ProviderRefusedMax / time.Second)}
	if c.Spec.CPU != "" || c.Spec.Memory != "" {
		in.Resources = &orkstackv1.ResourceRequest{Cpu: c.Spec.CPU, Memory: c.Spec.Memory}
	}
	if c.OnTrial {
		in.ReadyWithinSeconds = int32(p.RollWait / time.Second)
	}
	r.activities[c.Epoch] = &sourceActivity{schedule: c, future: workflow.ExecuteActivity(actx, task.ActivityHostEventSource, in), cancel: cancel}
}

// wait waits for the next thing that happens and tells the machine of it.
func (r *sourceRun) wait(ctx workflow.Context) error {
	if r.wake(ctx) {
		return nil
	}
	var gone bool
	s := workflow.NewSelector(ctx)
	s.AddReceive(ctx.Done(), func(workflow.ReceiveChannel, bool) { gone = true })
	receive := func(name string, take func(c workflow.ReceiveChannel) []eventsource.Command) {
		s.AddReceive(workflow.GetSignalChannel(ctx, name), func(c workflow.ReceiveChannel, _ bool) { r.do(ctx, take(c)) })
	}
	now := func() time.Time { return workflow.Now(ctx) }
	receive(SignalSourceSync, func(c workflow.ReceiveChannel) []eventsource.Command {
		c.Receive(ctx, nil)
		return r.m.OnSync()
	})
	receive(SignalSourceReport, func(c workflow.ReceiveChannel) []eventsource.Command {
		var v SourceReport
		c.Receive(ctx, &v)
		return r.m.OnReport(now(), v.Index, v.Epoch, v.What, eventsources.SafeText(v.Reason, maxReportedReason))
	})
	receive(SignalSourceRevoked, func(c workflow.ReceiveChannel) []eventsource.Command {
		var v SourceRevoked
		c.Receive(ctx, &v)
		r.m.OnRevoked(now(), eventsources.SafeText(v.Cause, maxReportedReason), v.Digest, v.Save)
		return nil
	})
	receive(SignalSourceJudged, func(c workflow.ReceiveChannel) []eventsource.Command {
		var v SourceJudged
		c.Receive(ctx, &v)
		r.m.OnJudged(v.Index, v.ObservedUnreadFirstAt, v.JudgedAt)
		return nil
	})
	receive(SignalSourceReroute, func(c workflow.ReceiveChannel) []eventsource.Command {
		var v SourceReroute
		c.Receive(ctx, &v)
		return r.m.OnReroute(now(), v.Index, v.Epoch)
	})
	receive(SignalSourceHandover, func(c workflow.ReceiveChannel) []eventsource.Command {
		c.Receive(ctx, nil)
		return r.m.OnHandover(now())
	})
	for _, epoch := range slices.Sorted(maps.Keys(r.activities)) {
		a := r.activities[epoch]
		s.AddFuture(a.future, func(f workflow.Future) { r.closed(ctx, a, f) })
	}
	r.answers(ctx, s)
	if r.timer != nil {
		s.AddFuture(r.timer, func(f workflow.Future) {
			r.timer = nil
			if f.Get(ctx, nil) == nil {
				r.do(ctx, r.m.Wake(now()))
			}
		})
	}
	s.Select(ctx)
	if gone {
		// Cancelled from outside, as it could be terminated: it ends at once, its
		// instances stop at their next heartbeat, and the pass starts it again.
		return ctx.Err()
	}
	return nil
}

// answers adds what the engine was asked.
func (r *sourceRun) answers(ctx workflow.Context, s workflow.Selector) {
	add := func(f *workflow.Future, answer func(workflow.Future) []eventsource.Command) {
		if *f == nil {
			return
		}
		s.AddFuture(*f, func(done workflow.Future) {
			*f = nil
			r.do(ctx, answer(done))
		})
	}
	add(&r.load, func(f workflow.Future) []eventsource.Command {
		var l eventsource.Load
		if err := f.Get(ctx, &l); err != nil {
			l = eventsource.Load{Outcome: eventsource.LoadFailed}
		}
		if l.Outcome == eventsource.LoadFound {
			r.source.Pool, r.source.Instances = l.Pool, l.Instances
			if l.Paused || l.Held {
				r.source.Pool, r.source.Instances = "", 0
			}
		} else if l.Outcome == eventsource.LoadDeleting || l.Outcome == eventsource.LoadDeleted {
			r.source.Pool, r.source.Instances = "", 0
		}
		return r.m.OnLoaded(workflow.Now(ctx), l)
	})
	add(&r.purge, func(f workflow.Future) []eventsource.Command {
		var left bool
		err := f.Get(ctx, &left)
		return r.m.OnPurged(workflow.Now(ctx), err == nil && !left, err != nil)
	})
	add(&r.pause, func(f workflow.Future) []eventsource.Command {
		var written bool
		err := f.Get(ctx, &written)
		return r.m.OnPaused(workflow.Now(ctx), err == nil && written)
	})
	add(&r.check, func(f workflow.Future) []eventsource.Command {
		var a SourceCheckAnswer
		if err := f.Get(ctx, &a); err != nil {
			a = SourceCheckAnswer{}
		}
		return r.m.OnChecked(workflow.Now(ctx), a.Allowed, a.Refused, a.Detail)
	})
}

// closed tells the machine of an activity that closed.
func (r *sourceRun) closed(ctx workflow.Context, a *sourceActivity, f workflow.Future) {
	delete(r.activities, a.schedule.Epoch)
	a.cancel()
	var res orkstackv1.HostEventSourceResult
	now := workflow.Now(ctx)
	closing := eventsource.Closing{}
	if err := f.Get(ctx, &res); err == nil {
		judged := eventsource.Classify(&res, a.schedule.OnTrial, now, r.m.Pinned.ProviderRefusedMax)
		closing.Result = &judged
	} else if !categoryOf(err).canceled {
		// What an attempt failed with is the cluster's to retry: this is one it
		// does not (the activity's own bound).
		workflow.GetLogger(ctx).Warn("an event source's instance ended, and said nothing", "index", a.schedule.Index, "epoch", a.schedule.Epoch, "error", err)
	}
	r.do(ctx, r.m.OnClosed(now, a.schedule.Index, a.schedule.Epoch, closing))
}

// wake keeps one Timer for the next thing that is due, and reports that it was
// due already.
func (r *sourceRun) wake(ctx workflow.Context) bool {
	now := workflow.Now(ctx)
	at := r.m.Next(now)
	if !at.IsZero() && !at.After(now) {
		r.do(ctx, r.m.Wake(now))
		if again := r.m.Next(now); again.IsZero() || again.After(now) {
			return true
		}
		// Waking does what is due, so nothing stays so (TestMachine_AnyOrderOfEvents):
		// were it to, it would wait a moment rather than hold the task.
		at = now.Add(r.m.Pinned.RestartBase)
	}
	if r.timer != nil && r.timerAt.Equal(at) {
		return false
	}
	r.stopTimer()
	if at.IsZero() {
		return false
	}
	tctx, cancel := workflow.WithCancel(ctx)
	r.timer, r.timerAt, r.cancelTimer = workflow.NewTimer(tctx, at.Sub(now)), at, cancel
	return false
}

func (r *sourceRun) stopTimer() {
	if r.timer != nil {
		r.cancelTimer()
	}
	r.timer = nil
}

// writeMemo writes the memo where it changed. One that cannot be written is
// this release's fault: the workflow's task fails and is tried again, so what
// runs goes on, and an engine with the fault mended goes on from there.
func (r *sourceRun) writeMemo(ctx workflow.Context) {
	n := r.m.Now(workflow.Now(ctx))
	n.Source = r.source
	written := [2]string{string(mustMemo(r.m.Encode())), string(mustMemo(n.Encode()))}
	if r.written == written {
		return
	}
	r.written = written
	if err := workflow.UpsertMemo(ctx, plainMemo(ctx, map[string]any{sourceMemoCarry: jsontext.Value(written[0]), sourceMemoNow: jsontext.Value(written[1])})); err != nil {
		panic(fmt.Sprintf("engine: an event source's memo: %v", err))
	}
}

func mustMemo(body []byte, err error) []byte {
	if err != nil {
		panic(fmt.Sprintf("engine: an event source's memo: %v", err))
	}
	return body
}
