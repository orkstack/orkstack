package engine

import (
	"context"
	"encoding/json/jsontext"
	"fmt"
	"maps"
	"reflect"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	workflowpb "go.temporal.io/api/workflow/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/interceptor"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/eventsource"
	"github.com/orkstack/orkstack/internal/held"
	"github.com/orkstack/orkstack/internal/task"
)

const sourceQueue = "ork-source-gpu"

func sourcePinned() eventsource.Pinned {
	return eventsource.Pin(7*24*time.Hour, 5*time.Minute, 5*time.Minute)
}

func firstCarry() jsontext.Value {
	body, err := eventsource.FirstCarry(sourcePinned()).Encode()
	if err != nil {
		panic(err)
	}
	return body
}

// sourceWorld is what a source's workflow runs against: what its local
// activities answer, and the hosts its instances are given to.
type sourceWorld struct {
	t   *testing.T
	env *testsuite.TestWorkflowEnvironment
	mu  sync.Mutex

	load   func(SourceLoadAsk) (eventsource.Load, error)
	purge  func() (bool, error)
	check  func(SourceCheckAsk) (SourceCheckAnswer, error)
	loads  []SourceLoadAsk
	pauses []SourcePauseAsk
	purges []int
	checks []SourceCheckAsk

	hosted    map[int64]*orkstackv1.HostEventSourceInput
	order     []int64
	tokens    map[int64][]byte
	infos     map[int64]activity.Info
	cancelled []string
	memos     []map[string]any
}

type sourceMemos struct {
	interceptor.WorkerInterceptorBase
	w *sourceWorld
}

func (r *sourceMemos) InterceptWorkflow(_ workflow.Context, next interceptor.WorkflowInboundInterceptor) interceptor.WorkflowInboundInterceptor {
	return &sourceMemosIn{WorkflowInboundInterceptorBase: interceptor.WorkflowInboundInterceptorBase{Next: next}, w: r.w}
}

type sourceMemosIn struct {
	interceptor.WorkflowInboundInterceptorBase
	w *sourceWorld
}

func (i *sourceMemosIn) Init(out interceptor.WorkflowOutboundInterceptor) error {
	return i.Next.Init(&sourceMemosOut{WorkflowOutboundInterceptorBase: interceptor.WorkflowOutboundInterceptorBase{Next: out}, w: i.w})
}

type sourceMemosOut struct {
	interceptor.WorkflowOutboundInterceptorBase
	w *sourceWorld
}

func (o *sourceMemosOut) UpsertMemo(ctx workflow.Context, memo map[string]any) error {
	o.w.mu.Lock()
	o.w.memos = append(o.w.memos, maps.Clone(memo))
	o.w.mu.Unlock()
	return o.Next.UpsertMemo(ctx, memo)
}

func newSourceWorld(t *testing.T) *sourceWorld {
	t.Helper()
	var suite testsuite.WorkflowTestSuite
	w := &sourceWorld{t: t, env: suite.NewTestWorkflowEnvironment(), hosted: map[int64]*orkstackv1.HostEventSourceInput{}, tokens: map[int64][]byte{}, infos: map[int64]activity.Info{}}
	w.env.SetWorkerOptions(testWorkerOptions(worker.Options{Interceptors: []interceptor.WorkerInterceptor{&sourceMemos{w: w}}}))
	w.env.SetTestTimeout(30 * time.Second)
	w.env.SetStartTime(time.Unix(1_800_000_000, 0))
	RegisterWorkflows(w.env)
	RegisterSourceActivities(w.env, w)
	w.env.RegisterActivityWithOptions(w.host, activity.RegisterOptions{Name: task.ActivityHostEventSource})
	w.env.SetOnActivityCanceledListener(func(info *activity.Info) {
		w.mu.Lock()
		w.cancelled = append(w.cancelled, info.ActivityID)
		w.mu.Unlock()
	})
	w.load = func(SourceLoadAsk) (eventsource.Load, error) { return sourceFound("d1", 1), nil }
	w.purge = func() (bool, error) { return false, nil }
	w.check = func(SourceCheckAsk) (SourceCheckAnswer, error) { return SourceCheckAnswer{Allowed: true}, nil }
	return w
}

func sourceFound(digest string, revision int64) eventsource.Load {
	return eventsource.Load{Outcome: eventsource.LoadFound, Pool: "gpu", Instances: 2, MaxEvents: 51200, MaxBytes: 50 << 20,
		Target: eventsource.Spec{Revision: revision, Digest: digest, AdmissionSave: "s1", Queue: sourceQueue, CPU: "500m", Memory: "64Mi"}}
}

func (w *sourceWorld) LoadEventSource(_ context.Context, ask SourceLoadAsk) (eventsource.Load, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.loads = append(w.loads, ask)
	return w.load(ask)
}

func (w *sourceWorld) PurgeEventSource(_ context.Context, _ SourceRef, batch int) (bool, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.purges = append(w.purges, batch)
	return w.purge()
}

func (w *sourceWorld) PauseEventSource(_ context.Context, ask SourcePauseAsk) (bool, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.pauses = append(w.pauses, ask)
	return true, nil
}

func (w *sourceWorld) CheckEventSource(_ context.Context, ask SourceCheckAsk) (SourceCheckAnswer, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.checks = append(w.checks, ask)
	return w.check(ask)
}

// host takes an instance and keeps it until a test ends it.
func (w *sourceWorld) host(ctx context.Context, in *orkstackv1.HostEventSourceInput) (*orkstackv1.HostEventSourceResult, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.hosted[in.GetEpoch()], w.tokens[in.GetEpoch()], w.infos[in.GetEpoch()] = in, activity.GetInfo(ctx).TaskToken, activity.GetInfo(ctx)
	// Two hosts take what was scheduled in either order.
	w.order = append(w.order, in.GetEpoch())
	slices.Sort(w.order)
	return nil, activity.ErrResultPending
}

// at does something once the workflow has run for d: minutes, since the test's
// clock runs with the host's while an activity does.
func (w *sourceWorld) at(d time.Duration, f func()) { w.env.RegisterDelayedCallback(f, d) }

// ends completes the newest instance of an index.
func (w *sourceWorld) ends(index int32, res *orkstackv1.HostEventSourceResult, err error) {
	in := w.newest(index)
	if err := w.env.CompleteActivity(w.tokens[in.GetEpoch()], res, err); err != nil {
		w.t.Errorf("complete index %d: %v", index, err)
	}
}

// newest is the newest activity of an index.
func (w *sourceWorld) newest(index int32) *orkstackv1.HostEventSourceInput {
	w.mu.Lock()
	defer w.mu.Unlock()
	for _, epoch := range slices.Backward(w.order) {
		if w.hosted[epoch].GetIndex() == index {
			return w.hosted[epoch]
		}
	}
	w.t.Errorf("index %d was never hosted", index)
	return nil
}

func (w *sourceWorld) report(index int32, what string) {
	w.env.SignalWorkflow(SignalSourceReport, SourceReport{Index: index, Epoch: w.newest(index).GetEpoch(), What: what})
}

// finish ends the execution, after d, as a hand-over does.
func (w *sourceWorld) finish(d time.Duration) {
	w.at(d, func() { w.env.SignalWorkflow(SignalSourceHandover, nil) })
}

var sourceIn = SourceInput{Namespace: "team", Name: "orders", ID: "evs-0123456789abcdef", Pool: "gpu", Instances: 2, Carry: firstCarry()}

// run runs the workflow to its end.
func (w *sourceWorld) run(in SourceInput) error {
	w.t.Helper()
	w.env.ExecuteWorkflow(SourceWorkflowType, in)
	if !w.env.IsWorkflowCompleted() {
		w.t.Fatal("the workflow did not end")
	}
	return w.env.GetWorkflowError()
}

// memo is the memo as it was last written.
func (w *sourceWorld) memo() (eventsource.Carry, eventsource.Now) {
	w.t.Helper()
	return w.memoOf(len(w.memos) - 1)
}

func (w *sourceWorld) payloads(i int) map[string]*commonpb.Payload {
	w.t.Helper()
	w.mu.Lock()
	defer w.mu.Unlock()
	out := map[string]*commonpb.Payload{}
	for name, v := range w.memos[i] {
		p, err := DataConverter().ToPayload(v)
		if err != nil {
			w.t.Fatal(err)
		}
		out[name] = p
	}
	return out
}

func (w *sourceWorld) memoOf(i int) (eventsource.Carry, eventsource.Now) {
	w.t.Helper()
	p := w.payloads(i)
	carry, err := eventsource.DecodeCarry(p[sourceMemoCarry].GetData())
	if err != nil {
		w.t.Fatal(err)
	}
	now, err := eventsource.DecodeNow(p[sourceMemoNow].GetData())
	if err != nil {
		w.t.Fatal(err)
	}
	return carry, now
}

// TestEventSourceWorkflow_Runs: a source is loaded, its instances are handed
// to its pool's source hosts as its document says, and its memo says what runs.
func TestEventSourceWorkflow_Runs(t *testing.T) {
	w := newSourceWorld(t)
	w.at(time.Minute, func() { w.report(0, "started"); w.report(1, "started") })
	w.at(2*time.Minute, func() { w.report(0, "ready"); w.report(1, "ready") })
	var during int
	w.at(3*time.Minute, func() { during = len(w.memos) - 1 })
	w.finish(4 * time.Minute)
	if err := w.run(sourceIn); err != nil {
		t.Fatal(err)
	}
	ref := SourceRef{Namespace: "team", Name: "orders", ID: "evs-0123456789abcdef"}
	if want := []SourceLoadAsk{{SourceRef: ref}}; !reflect.DeepEqual(w.loads, want) {
		t.Errorf("loads %+v; want %+v", w.loads, want)
	}
	if len(w.order) != 2 {
		t.Fatalf("%d instances were started", len(w.order))
	}
	for i, epoch := range w.order {
		in, info := w.hosted[epoch], w.infos[epoch]
		if in.GetIndex() != int32(i) || in.GetNamespace() != "team" || in.GetName() != "orders" || in.GetEventSourceId() != ref.ID || in.GetInstances() != 2 ||
			in.GetServiceDigest() != "d1" || in.GetResources().GetCpu() != "500m" || in.GetResources().GetMemory() != "64Mi" || in.GetTrial() != "" ||
			in.GetConfirmUnavailableMaxSeconds() != 3600 || in.GetProviderRefusedMax() != 3600 || !in.GetCompleteAfterHealthy() || in.GetReadyWithinSeconds() != 0 {
			t.Errorf("instance %d was given %v", i, in)
		}
		if info.TaskQueue != sourceQueue || info.ActivityID != held.ActivityID(int32(i), epoch) || info.HeartbeatTimeout != 30*time.Second ||
			info.StartToCloseTimeout != 28*24*time.Hour+5*time.Minute {
			t.Errorf("instance %d's activity is %+v", i, info)
		}
		if want := held.ActivityID(int32(i), epoch); !slices.Contains(w.cancelled, want) {
			t.Errorf("%s was not cancelled by the hand-over: %v", want, w.cancelled)
		}
	}
	carry, now := w.memoOf(during)
	if carry.Good == nil || carry.Good.Digest != "d1" || now.Load != "ok" || now.Wanted != 2 || now.Running != 2 || now.Slots[1].Current.Phase != eventsource.PhaseReady ||
		!reflect.DeepEqual(now.Source, eventsource.Source{Namespace: "team", Name: "orders", Pool: "gpu", Instances: 2}) {
		t.Errorf("carry %+v, now %+v", carry, now)
	}
	if carry, _ := w.memo(); carry.Continued == nil || carry.Continued.How != eventsource.EndedHandover {
		t.Errorf("the last memo's carry says %+v", carry.Continued)
	}

	// What the held check reads of that memo.
	resp := &workflowservice.DescribeWorkflowExecutionResponse{WorkflowExecutionInfo: &workflowpb.WorkflowExecutionInfo{
		Status: enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING, Type: &commonpb.WorkflowType{Name: SourceWorkflowType}, Memo: &commonpb.Memo{Fields: w.payloads(during)}}}
	wf, err := SourceReads{Client: describeAnswer{resp: resp}}.Describe(t.Context(), ref.ID)
	want := held.Memo{Slots: map[int32]held.Slot{0: {Current: held.SlotEpoch{Epoch: w.order[0], Digest: "d1", AdmissionSave: "s1"}},
		1: {Current: held.SlotEpoch{Epoch: w.order[1], Digest: "d1", AdmissionSave: "s1"}}}}
	if err != nil || !wf.Open || !reflect.DeepEqual(wf.Memo, want) {
		t.Errorf("the held check reads %+v, %v; want %+v", wf, err, want)
	}
}

// A load that fails is tried again on the machine's back-off, and a sync does
// not cut it short; one that succeeds is loaded again by a sync.
func TestEventSourceWorkflow_LoadsAgain(t *testing.T) {
	w := newSourceWorld(t)
	w.load = func(SourceLoadAsk) (eventsource.Load, error) {
		if len(w.loads) < 3 {
			return eventsource.Load{}, fmt.Errorf("the store is down")
		}
		return sourceFound("d1", int64(len(w.loads))), nil
	}
	var early, late int
	w.at(time.Minute, func() { w.env.SignalWorkflow(SignalSourceSync, nil) })
	w.at(4*time.Minute, func() {
		early = len(w.loads)
		if _, now := w.memo(); now.Load != "failing" || now.Source.Pool != "gpu" {
			t.Errorf("while the load fails the memo says %+v", now)
		}
	})
	w.at(20*time.Minute, func() { late = len(w.loads); w.env.SignalWorkflow(SignalSourceSync, nil) })
	w.finish(25 * time.Minute)
	pin := sourcePinned()
	pin.LoadBackoff = 5 * time.Minute
	in := sourceIn
	var err error
	if in.Carry, err = eventsource.FirstCarry(pin).Encode(); err != nil {
		t.Fatal(err)
	}
	if err := w.run(in); err != nil {
		t.Fatal(err)
	}
	if early != 1 || late != 3 || len(w.loads) != 4 || w.loads[2].Revision != 0 || w.loads[3].Revision != 3 {
		t.Errorf("loads: %d by 4 minutes, %d by 20, %+v", early, late, w.loads)
	}
}

// A roll replaces an instance beside the one that runs, on trial, and the
// workflow says which epochs a slot has.
func TestEventSourceWorkflow_Rolls(t *testing.T) {
	w := newSourceWorld(t)
	w.at(time.Minute, func() { w.report(0, "ready"); w.report(1, "ready") })
	w.at(2*time.Minute, func() {
		w.load = func(SourceLoadAsk) (eventsource.Load, error) { return sourceFound("d2", 2), nil }
		w.env.SignalWorkflow(SignalSourceSync, nil)
	})
	var first int64
	w.at(3*time.Minute, func() {
		in := w.newest(0)
		first = in.GetEpoch()
		if in.GetServiceDigest() != "d2" || in.GetReadyWithinSeconds() != 300 {
			t.Errorf("the replacement was given %v", in)
		}
		w.report(0, "ready")
	})
	w.finish(5 * time.Minute)
	if err := w.run(sourceIn); err != nil {
		t.Fatal(err)
	}
	if len(w.order) != 4 || first != w.order[2] || w.hosted[w.order[3]].GetIndex() != 1 {
		t.Errorf("started %v", w.order)
	}
	if want := held.ActivityID(0, w.order[0]); len(w.cancelled) == 0 || w.cancelled[0] != want {
		t.Errorf("the first that was cancelled is %v; want %s", w.cancelled, want)
	}
}

// An attempt's end is judged by the workflow's own table and clock: what
// trying again cannot change is paused, in the engine, and nothing is
// started; a reason no row holds only delays.
func TestEventSourceWorkflow_JudgesAnEnd(t *testing.T) {
	for name, tc := range map[string]struct {
		res    *orkstackv1.HostEventSourceResult
		pauses []SourcePauseAsk
		hosted int
	}{
		"unchangeable": {res: &orkstackv1.HostEventSourceResult{End: orkstackv1.InstanceEnd_INSTANCE_END_UNCHANGEABLE, Class: orkstackv1.InstanceEndClass_INSTANCE_END_CLASS_SOURCE,
			Reason: "SERVICE_CONFIG_REJECTED"}, pauses: []SourcePauseAsk{{SourceRef: SourceRef{"team", "orders", "evs-0123456789abcdef"}, Code: "EVENT_SOURCE_INSTANCE_FAILED",
			Refusal: "SERVICE_CONFIG_REJECTED", Revision: 1}}, hosted: 2},
		"unknown": {res: &orkstackv1.HostEventSourceResult{End: orkstackv1.InstanceEnd_INSTANCE_END_UNCHANGEABLE, Class: orkstackv1.InstanceEndClass_INSTANCE_END_CLASS_SOURCE,
			Reason: "SERVICE_OF_A_LATER_RELEASE"}, hosted: 3},
	} {
		t.Run(name, func(t *testing.T) {
			w := newSourceWorld(t)
			w.load = func(SourceLoadAsk) (eventsource.Load, error) {
				l := sourceFound("d1", 1)
				l.Paused = len(w.pauses) > 0
				return l, nil
			}
			w.at(time.Minute, func() { w.ends(0, tc.res, nil) })
			w.finish(5 * time.Minute)
			if err := w.run(sourceIn); err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(w.pauses, tc.pauses) || len(w.order) != tc.hosted {
				t.Errorf("pauses %+v, %d instances", w.pauses, len(w.order))
			}
			_, now := w.memo()
			if tc.pauses == nil && (now.UnknownEnd == nil || now.UnknownEnd.Reason != tc.res.GetReason()) {
				t.Errorf("the memo says %+v", now.UnknownEnd)
			}
			if tc.pauses != nil && (now.Source.Pool != "" || now.Source.Instances != 0) {
				t.Errorf("a paused source asks for %+v", now.Source)
			}
		})
	}
}

// A marked source's instances are stopped, what it kept is removed, batch
// after batch, and its workflow ends.
func TestEventSourceWorkflow_Purges(t *testing.T) {
	w := newSourceWorld(t)
	w.purge = func() (bool, error) {
		switch len(w.purges) {
		case 1:
			return true, nil
		case 2:
			return false, fmt.Errorf("the store is down")
		}
		return false, nil
	}
	w.at(time.Minute, func() {
		w.load = func(SourceLoadAsk) (eventsource.Load, error) {
			return eventsource.Load{Outcome: eventsource.LoadDeleting, MaxEvents: 51200, MaxBytes: 50 << 20}, nil
		}
		w.env.SignalWorkflow(SignalSourceSync, nil)
	})
	if err := w.run(sourceIn); err != nil {
		t.Fatal(err)
	}
	carry, now := w.memo()
	if !slices.Equal(w.purges, []int{1000, 1000, 1000}) || len(w.cancelled) != 2 || carry.Continued.How != eventsource.EndedPurged || now.Source.Pool != "" {
		t.Errorf("purges %v, cancelled %v, carry %+v, now %+v", w.purges, w.cancelled, carry.Continued, now)
	}
	if elapsed := carry.Continued.When.Sub(time.Unix(1_800_000_000, 0)); elapsed < 65*time.Second || elapsed > 2*time.Minute {
		t.Errorf("it ended after %s: a failed purge waits its back-off", elapsed)
	}
}

// Every signal reaches the machine: a revoke is held and checked, judged
// clears what could not be judged, and a reroute is scheduled again.
func TestEventSourceWorkflow_Signals(t *testing.T) {
	w := newSourceWorld(t)
	w.check = func(SourceCheckAsk) (SourceCheckAnswer, error) {
		return SourceCheckAnswer{Refused: true, Detail: "the grant was revoked"}, nil
	}
	w.load = func(SourceLoadAsk) (eventsource.Load, error) {
		l := sourceFound("d1", 1)
		l.Paused = len(w.pauses) > 0
		return l, nil
	}
	var rerouted, firstOf int64
	w.at(time.Minute, func() {
		firstOf = w.newest(1).GetEpoch()
		w.env.SignalWorkflow(SignalSourceReroute, SourceReroute{Index: 1, Epoch: firstOf})
	})
	w.at(2*time.Minute, func() { rerouted = w.newest(1).GetEpoch() })
	var revoked eventsource.Carry
	w.at(3*time.Minute, func() {
		w.env.SignalWorkflow(SignalSourceRevoked, SourceRevoked{Cause: strings.Repeat("é", 400), Digest: "d1", Save: "s1"})
	})
	w.at(4*time.Minute, func() { revoked, _ = w.memo() })
	w.finish(15 * time.Minute)
	if err := w.run(sourceIn); err != nil {
		t.Fatal(err)
	}
	if rerouted == firstOf || !slices.Contains(w.cancelled, held.ActivityID(1, firstOf)) {
		t.Errorf("epoch %d was not rerouted: %v", firstOf, w.cancelled)
	}
	if revoked.Revoke == nil || revoked.Revoke.Save != "s1" || len(revoked.Revoke.Cause) > maxReportedReason+len("…") {
		t.Errorf("the revoke is kept as %+v", revoked.Revoke)
	}
	if len(w.checks) != 1 || len(w.checks[0].Specs) != 1 || w.checks[0].Specs[0].Digest != "d1" || len(w.pauses) != 1 || w.pauses[0].Detail != "the grant was revoked" {
		t.Errorf("checks %+v, pauses %+v", w.checks, w.pauses)
	}
}

// A source whose spec could not be judged goes on from the instant it was
// first seen, and a judgment clears it.
func TestEventSourceWorkflow_Judged(t *testing.T) {
	w := newSourceWorld(t)
	w.at(time.Minute, func() {
		w.ends(0, &orkstackv1.HostEventSourceResult{End: orkstackv1.InstanceEnd_INSTANCE_END_JUDGMENT_UNAVAILABLE,
			Class: orkstackv1.InstanceEndClass_INSTANCE_END_CLASS_HOST, Reason: "IDENTITY_UNAVAILABLE", Unread: "spec"}, nil)
	})
	var unread eventsource.Carry
	w.at(2*time.Minute, func() {
		unread, _ = w.memo()
		w.env.SignalWorkflow(SignalSourceJudged, SourceJudged{Index: 0, ObservedUnreadFirstAt: unread.UnreadFirstAt[0], JudgedAt: w.env.Now()})
	})
	w.finish(5 * time.Minute)
	if err := w.run(sourceIn); err != nil {
		t.Fatal(err)
	}
	if unread.UnreadFirstAt[0].IsZero() {
		t.Fatalf("nothing was kept of what could not be read: %+v", unread)
	}
	if carry, _ := w.memo(); len(carry.UnreadFirstAt) != 0 {
		t.Errorf("a judgment left %+v", carry.UnreadFirstAt)
	}
}

// An execution with a carry starts what it says ran when it cannot load, and
// keeps the numbers it was pinned with.
func TestEventSourceWorkflow_StartsFromItsCarry(t *testing.T) {
	first := newSourceWorld(t)
	first.at(time.Minute, func() { first.report(0, "ready"); first.report(1, "ready") })
	first.finish(3 * time.Minute)
	if err := first.run(sourceIn); err != nil {
		t.Fatal(err)
	}
	w := newSourceWorld(t)
	w.load = func(SourceLoadAsk) (eventsource.Load, error) {
		return eventsource.Load{Outcome: eventsource.LoadMissing}, nil
	}
	in := sourceIn
	in.Carry = first.payloads(len(first.memos) - 1)[sourceMemoCarry].GetData()
	in.StartedAfter = eventsource.EndedAbandoned
	w.at(time.Minute, func() {
		carry, now := w.memo()
		if carry.Continued.How != eventsource.EndedAbandoned || now.StartedAfter != "" || now.Load != "missing" || carry.Pinned.CancelWait != 2*time.Minute {
			t.Errorf("carry %+v, now %+v", carry, now)
		}
	})
	w.finish(5 * time.Minute)
	if err := w.run(in); err != nil {
		t.Fatal(err)
	}
	if len(w.order) != 2 || w.hosted[w.order[0]].GetServiceDigest() != "d1" || w.infos[w.order[0]].TaskQueue != sourceQueue ||
		w.hosted[w.order[0]].GetResources().GetMemory() != "64Mi" || w.order[0] <= slices.Max(first.order) {
		t.Errorf("started %v after %v", w.hosted, first.order)
	}
}

// Past its budget an idle source carries on in a new execution, with its
// carry; one whose carry no release like this one wrote starts nothing.
func TestEventSourceWorkflow_CarriesOn(t *testing.T) {
	w := newSourceWorld(t)
	w.load = func(SourceLoadAsk) (eventsource.Load, error) {
		l := sourceFound("d1", 1)
		l.Paused = true
		return l, nil
	}
	w.env.SetCurrentHistoryLength(2500)
	err := w.run(sourceIn)
	can, ok := err.(*workflow.ContinueAsNewError)
	if !ok {
		if wrapped, is := err.(*temporal.WorkflowExecutionError); is {
			can, ok = wrapped.Unwrap().(*workflow.ContinueAsNewError)
		}
	}
	if !ok || can.WorkflowType.Name != SourceWorkflowType {
		t.Fatalf("it ended with %v", err)
	}
	var next SourceInput
	if err := DataConverter().FromPayloads(can.Input, &next); err != nil {
		t.Fatal(err)
	}
	carry, err := eventsource.DecodeCarry(next.Carry)
	if err != nil || carry.Continued.How != eventsource.EndedIdle || !carry.Loaded.Paused || next.Pool != "" || next.Instances != 0 || next.ID != sourceIn.ID {
		t.Errorf("the next execution is given %+v (%+v, %v)", next, carry, err)
	}

	later := newSourceWorld(t)
	in := sourceIn
	in.Carry = jsontext.Value(`{"format":2}`)
	if err := later.run(in); err == nil || !strings.Contains(err.Error(), "newer release") || len(later.loads) != 0 {
		t.Errorf("a carry of a later format: %v, %d loads", err, len(later.loads))
	}
}

// What the workflow's commands are made with is what its carry pins, never
// this release's own numbers.
func TestEventSourceWorkflow_GoesByWhatIsPinned(t *testing.T) {
	pin := sourcePinned()
	pin.Heartbeat, pin.AgeBound, pin.StopGrace, pin.MaxAge = 45*time.Second, 3, time.Minute, 100*time.Hour
	pin.ConfirmUnavailableMax, pin.ProviderRefusedMax, pin.MemoEvents, pin.MemoBytes = 2*time.Hour, 3*time.Hour, 7, 11
	in := sourceIn
	var err error
	if in.Carry, err = eventsource.FirstCarry(pin).Encode(); err != nil {
		t.Fatal(err)
	}
	w := newSourceWorld(t)
	w.env.SetCurrentHistoryLength(20)
	w.env.SetCurrentHistorySize(40)
	w.finish(5 * time.Minute)
	if err := w.run(in); err != nil {
		t.Fatal(err)
	}
	info, given := w.infos[w.order[0]], w.hosted[w.order[0]]
	if info.HeartbeatTimeout != 45*time.Second || info.StartToCloseTimeout != 300*time.Hour+time.Minute ||
		given.GetConfirmUnavailableMaxSeconds() != 7200 || given.GetProviderRefusedMax() != 10800 {
		t.Errorf("activity %+v, given %v", info, given)
	}
	carry, now := w.memo()
	if now.Events != 14 || now.Bytes != 33 || carry.Pinned != pin {
		t.Errorf("the memo says %d events, %d bytes, pinned %+v", now.Events, now.Bytes, carry.Pinned)
	}
}
