package engine

import (
	"context"
	"errors"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
)

var errFC = errors.New("not now")

// fcTarget is the run a force cancel acts on, as the workflow's activities
// see it: mocked activities over one state, so each case drives the steps it
// is about (AGENTS.md §11.1: workflow tests mock their activities).
type fcTarget struct {
	// publishErr is what the build always answers with, where set, and
	// publishCalls how often it was asked.
	publishErr   error
	publishCalls int

	mu   sync.Mutex
	open bool
	exec string
	// decision is what verifies; verifyCause refuses it.
	decision    ForceCancelDecision
	verifyCause string
	// endsOnCancel ends the run canceled when asked; closedAtCancel answers
	// that it had closed; scans are the stall tests' answers, the last kept.
	endsOnCancel, closedAtCancel bool
	endsInCancel                 bool // it ends on its cancel before Cancel looks
	scans                        []FCScanned
	// scan answers a stall test instead of scans; l0s is where each execution
	// recorded its cancel.
	scan func(FCStep) FCScanned
	l0s  map[string]int64
	// switchOnTerminate moves the run to the next execution this many times
	// instead of closing it.
	switchOnTerminate int
	// failing counts how often each activity still fails.
	failing map[string]int
	// closedLater replaces closed once it has been asked for this often.
	closedLater FCClassified
	askedClosed int
	// closed is what a terminated run classifies as; gates what it holds,
	// gatesAfter what its close shows.
	closed            FCClassified
	gates, gatesAfter FCGates
	terminateCause    string
	unsent            bool
	unsentN           int  // how often a termination cannot be sent
	gone              bool // the run closes on its own just before it
	unsentAfterSwitch bool
	stopInGates       bool
	lateClose         bool // the lost termination landed
	movesAfter        bool // it moved again after its first termination was sent
	verifyUnavailable int
	refusesScan       bool
	refusesTerminate  bool // the decision no longer verifies at the termination
	skews             int  // how often the engine's clock disagrees
	lands             bool // a termination whose answer was lost closes the run, at its next stall test
	describes         int  // how often the run is described
	stopAtDescribe    int  // stops the force cancel at this description, if set
	failDescribeFrom  int  // fails every description from this one on, if set
	stop              func()
	sentBefore        bool // an engine that died had sent the termination
	lost              int  // how many sent terminations fail and change nothing
	terminateCalls    int
	terminateL0       []int64
	// refuses: the decision no longer verifies once the run was ended.
	refuses, refusesAudit     bool
	askedClassify, askedAudit int
	gatesOf, classifiedOf     []string
	moves                     int // how often it still moves between its stall test and its termination
	publish                   FCPublished
	publishEventFails         int

	audits     []string
	results    map[string]ForceCancelResult
	refused    int
	canceled   []string
	scannedL0  []int64
	terminated []string
	released   map[string]GateReleaseAll
	published  []string
}

func newFCTarget() *fcTarget {
	return &fcTarget{open: true, exec: "exec-1",
		decision: ForceCancelDecision{RequestID: "fc-1", Namespace: "payments", RunID: "ork-r", Principal: "alice",
			Reason: "stuck", GraceSeconds: 60, CeilingSeconds: 1800},
		scans:   []FCScanned{{Stall: ForceCancelStall{Newest: 10}, ExecutionRunID: "exec-1", Open: true}},
		closed:  FCClassified{State: FCStateForceCanceled},
		gates:   FCGates{Gates: []string{"ork-conc-aa"}, SeqBase: 4},
		publish: FCPublished{Event: eventBytes(forcedEnd("fc-1")), SnapshotURI: snapOfRun},
		failing: map[string]int{}, results: map[string]ForceCancelResult{}, released: map[string]GateReleaseAll{}}
}

// snapOfRun is where the ended run's snapshot is.
const snapOfRun = "s3://snap/metadata/snapshots/payments/ork-r/9.pb"

func forcedEnd(request string) *orkstackv1.Event {
	return &orkstackv1.Event{RunId: "ork-r", Sequence: 9, RequestId: request, Type: orkstackv1.EventType_EVENT_TYPE_RUN_CANCELED, Status: orkstackv1.Status_STATUS_CANCELED}
}

func eventBytes(e *orkstackv1.Event) []byte {
	p, err := FCPublished{}.with(e)
	if err != nil {
		panic(err)
	}
	return p.Event
}

// answers has the ending's build answer fc-1's, changed.
func (f *fcTarget) answers(change func(*orkstackv1.Event)) {
	e := forcedEnd("fc-1")
	change(e)
	f.publish.Event = eventBytes(e)
}

func (f *fcTarget) fails(name string) error {
	if f.failing[name] > 0 {
		f.failing[name]--
		return errFC
	}
	return nil
}

func (f *fcTarget) register(env *testsuite.TestWorkflowEnvironment) {
	reg := func(name string, fn any) { env.RegisterActivityWithOptions(fn, activity.RegisterOptions{Name: name}) }
	lock := func() func() { f.mu.Lock(); return f.mu.Unlock }
	reg(ActivityFCVerify, func(context.Context, ForceCancelInput) (FCVerified, error) {
		defer lock()()
		if f.verifyUnavailable > 0 {
			f.verifyUnavailable--
			return FCVerified{Cause: FCCauseKeysUnavailable}, nil
		}
		if err := f.fails(ActivityFCVerify); err != nil || f.verifyCause != "" {
			return FCVerified{Cause: f.verifyCause}, err
		}
		return FCVerified{Decision: &f.decision}, nil
	})
	reg(ActivityFCRefuse, func(context.Context, string) error { defer lock()(); f.refused++; return nil })
	reg(ActivityFCAudit, func(_ context.Context, in FCAuditInput) error {
		defer lock()()
		if err := f.fails(ActivityFCAudit + in.Kind); err != nil {
			return err
		}
		if f.refusesAudit && in.Kind == forceCancelCompleted {
			f.askedAudit++
			return errForceCancelRefused(FCCauseDecisionUnverified)
		}
		f.audits = append(f.audits, in.Kind)
		f.results[in.Kind] = in.Result
		return nil
	})
	reg(ActivityFCDescribe, func(context.Context, FCStep) (FCDescribed, error) {
		defer lock()()
		if f.describes++; f.describes == f.stopAtDescribe {
			f.stop()
		}
		if f.failDescribeFrom > 0 && f.describes >= f.failDescribeFrom {
			return FCDescribed{}, errFC
		}
		return FCDescribed{Open: f.open, ExecutionRunID: f.exec}, f.fails(ActivityFCDescribe)
	})
	reg(ActivityFCCancel, func(context.Context, FCStep) (FCCanceled, error) {
		defer lock()()
		if err := f.fails(ActivityFCCancel); err != nil {
			return FCCanceled{}, err
		}
		if f.closedAtCancel {
			f.open = false
			f.closed = FCClassified{State: FCStateEnded}
			return FCCanceled{Closed: true, ExecutionRunID: f.exec}, nil
		}
		f.canceled = append(f.canceled, f.exec)
		if f.endsOnCancel {
			f.open, f.closed = false, FCClassified{State: FCStateCanceled}
		}
		if f.endsInCancel {
			f.open, f.closed = false, FCClassified{State: FCStateCanceled}
			return FCCanceled{Closed: true, Asked: true, ExecutionRunID: f.exec}, nil
		}
		l0 := int64(10 * len(f.canceled))
		if at, ok := f.l0s[f.exec]; ok {
			l0 = at
		}
		return FCCanceled{L0: l0, ExecutionRunID: f.exec}, nil
	})
	reg(ActivityFCScan, func(_ context.Context, in FCStep) (FCScanned, error) {
		defer lock()()
		if f.refusesScan {
			return FCScanned{}, errForceCancelRefused(FCCauseDecisionStale)
		}
		if err := f.fails(ActivityFCScan); err != nil {
			return FCScanned{}, err
		}
		f.scannedL0 = append(f.scannedL0, in.L0)
		if f.lands && f.terminateCalls > 0 {
			f.open = false
		}
		if !f.open {
			return FCScanned{ExecutionRunID: f.exec}, nil
		}
		if f.scan != nil {
			next := f.scan(in)
			f.exec, f.open = next.ExecutionRunID, next.Open
			return next, nil
		}
		next := f.scans[0]
		if len(f.scans) > 1 {
			f.scans = f.scans[1:]
		}
		f.exec, f.open = next.ExecutionRunID, next.Open
		return next, nil
	})
	reads := 0
	reg(ActivityFCGates, func(_ context.Context, in FCStep) (FCGates, error) {
		defer lock()()
		f.gatesOf = append(f.gatesOf, in.ExecutionRunID)
		if f.stopInGates {
			f.stop()
		}
		if err := f.fails(ActivityFCGates); err != nil {
			return FCGates{}, err
		}
		if reads++; reads > 1 && f.gatesAfter.Gates != nil {
			return f.gatesAfter, nil
		}
		return f.gates, nil
	})
	reg(ActivityFCTerminate, func(_ context.Context, in FCStep) (FCTerminated, error) {
		defer lock()()
		f.terminateCalls++
		f.terminateL0 = append(f.terminateL0, in.L0)
		if f.refusesTerminate {
			return FCTerminated{}, errForceCancelRefused(FCCauseDecisionUnverified)
		}
		if f.skews > 0 {
			f.skews--
			return FCTerminated{Cause: FCCauseClockSkew}, nil
		}
		if f.sentBefore {
			f.open = false
		}
		if f.lost > 0 {
			f.lost--
			f.open = f.open && !f.lateClose
			return FCTerminated{}, errFC
		}
		if f.movesAfter && f.terminateCalls == 2 {
			return FCTerminated{Progressing: true}, nil
		}
		if f.moves > 0 && !in.ProgressingAtCeiling {
			f.moves--
			return FCTerminated{Progressing: true}, nil
		}
		if f.unsentAfterSwitch && in.ExecutionRunID != "exec-1" {
			return FCTerminated{Unsent: "no keys"}, nil
		}
		if f.unsentN > 0 {
			f.unsentN--
			return FCTerminated{Unsent: "no keys"}, nil
		}
		if f.unsent {
			return FCTerminated{Unsent: "no keys"}, nil
		}
		if f.gone {
			f.open = false
			return FCTerminated{}, nil
		}
		if f.terminateCause != "" {
			return FCTerminated{Cause: f.terminateCause}, nil
		}
		f.terminated = append(f.terminated, in.ExecutionRunID)
		if f.switchOnTerminate > 0 {
			f.switchOnTerminate--
			f.exec += "'"
			return FCTerminated{Sent: true}, nil
		}
		f.open = false
		return FCTerminated{Sent: true}, f.fails(ActivityFCTerminate)
	})
	reg(ActivityFCClassify, func(_ context.Context, in FCStep) (FCClassified, error) {
		defer lock()()
		f.classifiedOf = append(f.classifiedOf, in.ExecutionRunID)
		if f.askedClassify++; f.refuses {
			return FCClassified{}, errForceCancelRefused(FCCauseDecisionUnverified)
		}
		if err := f.fails(ActivityFCClassify); err != nil {
			return FCClassified{}, err
		}
		if f.open {
			return FCClassified{Open: true}, nil
		}
		if f.askedClosed++; f.askedClosed > 3 && f.closedLater.State != "" {
			return f.closedLater, nil
		}
		return f.closed, nil
	})
	reg(ActivityFCPublish, func(context.Context, FCStep) (FCPublished, error) {
		defer lock()()
		f.publishCalls++
		if f.publishErr != nil {
			return FCPublished{}, f.publishErr
		}
		return f.publish, f.fails(ActivityFCPublish)
	})
	reg(ActivityPublishEvent, func(_ context.Context, e *orkstackv1.Event, uri, key string) error {
		defer lock()()
		if f.publishEventFails > 0 {
			f.publishEventFails--
			return errFC
		}
		f.published = append(f.published, uri+" "+key)
		return nil
	})
	env.OnSignalExternalWorkflow(mock.Anything, mock.Anything, "", GateSignalReleaseAll, mock.Anything).Return(
		func(_, gate, _, _ string, arg any) error {
			defer lock()()
			if err := f.fails("signal"); err != nil {
				return err
			}
			f.released[gate] = arg.(GateReleaseAll)
			return nil
		}).Maybe()
}

func fcInput() ForceCancelInput {
	return ForceCancelInput{Decision: "signed", ScanEvents: 2000, PageSize: 250, RecordTimeout: 10 * time.Second}
}

// runFC runs a force cancel of f under its own workflow id; tweak may change
// the environment first.
func runFC(t *testing.T, f *fcTarget, in ForceCancelInput, tweak ...func(*testsuite.TestWorkflowEnvironment)) ForceCancelResult {
	t.Helper()
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	// A loaded host, not this code, must not fail a workflow task.
	env.SetWorkerOptions(worker.Options{DeadlockDetectionTimeout: time.Minute})
	f.stop = env.CancelWorkflow
	name := ForceCancelWorkflowType
	if in.Settings != nil {
		name = "cascading" // what it starts is the case's own
	}
	env.RegisterWorkflowWithOptions(ForceCancelWorkflow, workflow.RegisterOptions{Name: name})
	env.SetStartWorkflowOptions(client.StartWorkflowOptions{ID: f.decision.WorkflowID(), TaskQueue: "ork-tasks"})
	if in.Settings != nil {
		for _, fn := range tweak {
			fn(env) // it registers a workflow, which nothing mocked may precede
		}
		tweak = nil
	}
	f.register(env)
	for _, fn := range tweak {
		fn(env)
	}
	env.ExecuteWorkflow(name, in)
	if !env.IsWorkflowCompleted() {
		t.Fatal("the force cancel did not complete")
	}
	if err := env.GetWorkflowError(); err != nil {
		t.Fatalf("the force cancel failed instead of answering: %v", err)
	}
	var out ForceCancelResult
	if err := env.GetWorkflowResult(&out); err != nil {
		t.Fatal(err)
	}
	return out
}

func stalled(exec string, newest int64) FCScanned {
	return FCScanned{Stall: ForceCancelStall{Newest: newest}, ExecutionRunID: exec, Open: true}
}

func progressing(exec string, newest int64) FCScanned {
	return FCScanned{Stall: ForceCancelStall{Progressing: true, Newest: newest}, ExecutionRunID: exec, Open: true}
}

// TestForceCancelWorkflow is the step suite of a force cancel without a
// cascade (design §22.1.2): each case is one way the run answers.
func TestForceCancelWorkflow(t *testing.T) {
	done := ForceCancelResult{State: FCStateForceCanceled, RequestID: "fc-1", Terminated: true, Released: true, TerminalPublished: true}
	const requested, completed = "run.cancel.force.requested", "run.cancel.force.completed"
	unpublished := done
	unpublished.TerminalPublished, unpublished.Cause = false, FCCauseEventUnpublished
	tests := []struct {
		name  string
		setup func(*fcTarget)
		in    func(*ForceCancelInput)
		want  ForceCancelResult
		check func(*testing.T, *fcTarget)
	}{
		{"a stalled run is ended after its grace, its slots released and its ending published", nil, nil, done, func(t *testing.T, f *fcTarget) {
			if !slices.Equal(f.terminated, []string{"exec-1"}) || len(f.canceled) != 1 || !slices.Equal(f.scannedL0, []int64{10}) {
				t.Errorf("terminated %v, canceled %v, scanned from %v", f.terminated, f.canceled, f.scannedL0)
			}
			if r := f.released["ork-conc-aa"]; r.Run != "ork-r" || r.Below != 5 {
				t.Errorf("released %+v, want ork-r below its seq_base + 1", f.released)
			}
			if !slices.Equal(f.published, []string{snapOfRun + " force-cancel:fc-1"}) || !slices.Equal(f.audits, []string{requested, completed}) {
				t.Errorf("published %v, audits %v", f.published, f.audits)
			}
		}},
		{"a run that ends on its ordinary cancel is never terminated", func(f *fcTarget) { f.endsOnCancel = true }, nil,
			ForceCancelResult{State: FCStateCanceled, RequestID: "fc-1"}, func(t *testing.T, f *fcTarget) {
				if len(f.terminated)+len(f.released)+len(f.published) != 0 {
					t.Errorf("terminated %v, released %v, published %v", f.terminated, f.released, f.published)
				}
			}},
		{"a run that ended on its cancel at once is said to be canceled", func(f *fcTarget) { f.endsInCancel = true }, nil,
			ForceCancelResult{State: FCStateCanceled, RequestID: "fc-1"}, func(t *testing.T, f *fcTarget) {
				if !slices.Equal(f.classifiedOf, []string{"exec-1"}) {
					t.Errorf("classified %q", f.classifiedOf)
				}
			}},
		{"a decision refused at its stall test is said to be", func(f *fcTarget) { f.refusesScan = true }, nil,
			ForceCancelResult{State: FCStateFailed, Cause: FCCauseDecisionStale, RequestID: "fc-1"}, nil},
		{"an ending is published late, however short the ceiling", func(f *fcTarget) {
			f.decision.CeilingSeconds, f.failing[ActivityFCPublish] = 60, 8
		}, nil, done, nil},
		{"a run that ended on its own just before its termination was not terminated", func(f *fcTarget) {
			f.gone, f.closed = true, FCClassified{State: FCStateEnded}
		}, nil, ForceCancelResult{State: FCStateEnded, RequestID: "fc-1"}, nil},
		{"a stall test that fails for a while is asked again", func(f *fcTarget) { f.failing[ActivityFCScan] = 12 }, nil, done, nil},
		{"a cancel that fails for a while is asked again", func(f *fcTarget) { f.failing[ActivityFCCancel] = 12 }, nil, done, nil},
		{"a description that fails for a while is asked again", func(f *fcTarget) { f.failing[ActivityFCDescribe] = 12 }, nil, done, nil},
		{"a verification that fails for a while is asked again", func(f *fcTarget) { f.failing[ActivityFCVerify] = 12 }, nil, done, nil},
		{"a termination that cannot be sent for a while is sent", func(f *fcTarget) { f.unsentN = 12 }, nil, done, nil},
		{"a decision that no longer verifies is not asked again, and what the run held is released", func(f *fcTarget) {
			f.refuses, f.refusesAudit = true, true
			f.decision.CeilingSeconds = 7200
		}, nil, ForceCancelResult{State: FCStateFailed, Cause: FCCauseUnclassified, RequestID: "fc-1", Terminated: true, Released: true, Unaudited: true}, func(t *testing.T, f *fcTarget) {
			if f.askedClassify != 1 || f.askedAudit != 1 {
				t.Errorf("classify asked %d times, its record %d: what is refused stays refused", f.askedClassify, f.askedAudit)
			}
		}},
		{"a run whose close can never be classified has its slots released", func(f *fcTarget) { f.failing[ActivityFCClassify] = 1 << 30 }, nil,
			ForceCancelResult{State: FCStateFailed, Cause: FCCauseUnclassified, RequestID: "fc-1", Terminated: true, Released: true}, nil},
		{"a run whose end was announced but never published is published as it said", func(f *fcTarget) {
			f.publish = FCPublished{Ended: true, Event: eventBytes(&orkstackv1.Event{RunId: "ork-r", Sequence: 9, Type: orkstackv1.EventType_EVENT_TYPE_RUN_SUCCEEDED}), SnapshotURI: snapOfRun, DedupeKey: "ork-r#9"}
		}, nil, ForceCancelResult{State: FCStateEnded, Ends: true, RequestID: "fc-1", Terminated: true, Released: true, TerminalPublished: true}, func(t *testing.T, f *fcTarget) {
			if !slices.Equal(f.published, []string{snapOfRun + " ork-r#9"}) {
				t.Errorf("published %v", f.published)
			}
		}},
		{"a run that stalls in the execution it moved to past its ceiling is said to have stalled", func(f *fcTarget) {
			f.decision.CeilingSeconds, f.switchOnTerminate = 60, 1
			f.scan = func(in FCStep) FCScanned {
				if in.ExecutionRunID == "exec-1" {
					return progressing(f.exec, 40)
				}
				return stalled(f.exec, 40)
			}
		}, nil, done, nil},
		{"a run that carried on as its ceiling passed is given one grace there", func(f *fcTarget) {
			f.decision.CeilingSeconds = 60
			f.scans = []FCScanned{progressing("exec-2", 40), progressing("exec-2", 50)}
		}, nil, func() ForceCancelResult { r := done; r.ProgressingAtCeiling = true; return r }(), func(t *testing.T, f *fcTarget) {
			if !slices.Equal(f.canceled, []string{"exec-1", "exec-2"}) || len(f.scannedL0) != 2 {
				t.Errorf("canceled %v, scanned %v", f.canceled, f.scannedL0)
			}
		}},
		{"a run that carries on at every window is ended once past its ceiling", func(f *fcTarget) {
			f.decision.CeilingSeconds = 120
			f.scan = func(FCStep) FCScanned { return progressing(f.exec+"'", 40) }
		}, nil, func() ForceCancelResult { r := done; r.ProgressingAtCeiling = true; return r }(), func(t *testing.T, f *fcTarget) {
			if len(f.scannedL0) != 3 {
				t.Errorf("%d windows, want the ceiling's 2 and one more", len(f.scannedL0))
			}
		}},
		{"the slots read after the close are the closed execution's", nil, nil, done, func(t *testing.T, f *fcTarget) {
			if !slices.Equal(f.gatesOf, []string{"exec-1", "exec-1"}) || !slices.Equal(f.classifiedOf, []string{"exec-1"}) {
				t.Errorf("gates of %v, classified %v: a run resumed since holds its own", f.gatesOf, f.classifiedOf)
			}
		}},
		{"a run another force cancel ended and did not finish is finished, under that request", func(f *fcTarget) {
			f.open, f.closed = false, FCClassified{State: FCStateAlreadyEnded, Request: "fc-0"}
			f.publish.Event = eventBytes(forcedEnd("fc-0"))
		}, nil, ForceCancelResult{State: FCStateAlreadyEnded, RequestID: "fc-1", EndedBy: "fc-0", Finishing: true, Released: true, TerminalPublished: true}, func(t *testing.T, f *fcTarget) {
			if len(f.canceled)+len(f.terminated) != 0 || len(f.released) != 1 || !slices.Equal(f.published, []string{snapOfRun + " force-cancel:fc-0"}) || !slices.Equal(f.gatesOf, []string{"exec-1"}) {
				t.Errorf("canceled %v, terminated %v, released %v, published %v, gates of %v", f.canceled, f.terminated, f.released, f.published, f.gatesOf)
			}
		}},
		{"one that announced its own end is finished with its slots", func(f *fcTarget) {
			f.open, f.closed, f.publish = false, FCClassified{State: FCStateAlreadyEnded, Request: "fc-0"}, FCPublished{Ended: true}
		}, nil, ForceCancelResult{State: FCStateAlreadyEnded, Ends: true, RequestID: "fc-1", EndedBy: "fc-0", Finishing: true, Released: true, TerminalPublished: true}, func(t *testing.T, f *fcTarget) {
			if !f.results[completed].succeeded() {
				t.Errorf("recorded as %+v", f.results[completed])
			}
		}},
		{"one whose slots cannot be released is not said to be finished", func(f *fcTarget) {
			f.open, f.closed = false, FCClassified{State: FCStateAlreadyEnded, Request: "fc-0"}
			f.publish.Event, f.failing["signal"] = eventBytes(forcedEnd("fc-0")), 1<<20
		}, nil, ForceCancelResult{State: FCStateAlreadyEnded, RequestID: "fc-1", EndedBy: "fc-0", Finishing: true, TerminalPublished: true}, func(t *testing.T, f *fcTarget) {
			if f.results[completed].succeeded() {
				t.Error("recorded as having succeeded")
			}
		}},
		{"a run ended before that cannot be classified is said to have failed", func(f *fcTarget) {
			f.open, f.failing[ActivityFCClassify] = false, 1<<30
		}, nil, ForceCancelResult{State: FCStateFailed, Cause: FCCauseRetriesExhausted, RequestID: "fc-1"}, nil},
		{"an ending of another request is not taken for the one it finishes", func(f *fcTarget) {
			f.open, f.closed = false, FCClassified{State: FCStateAlreadyEnded, Request: "fc-0"}
		}, nil, ForceCancelResult{State: FCStateAlreadyEnded, Cause: FCCauseEventUnpublished, RequestID: "fc-1", EndedBy: "fc-0", Finishing: true, Released: true}, nil},
		{"its own termination, found when it starts again, is finished", func(f *fcTarget) { f.open = false }, nil, done, nil},
		{"a run ended before whose record's key is not known says so", func(f *fcTarget) {
			f.open, f.closed = false, FCClassified{State: FCStateFailed, Cause: FCCauseUnknownKey, Retry: true}
		}, nil, ForceCancelResult{State: FCStateFailed, Cause: FCCauseUnknownKey, RequestID: "fc-1"}, func(t *testing.T, f *fcTarget) {
			if len(f.released)+len(f.published) != 0 {
				t.Errorf("released %v, published %v", f.released, f.published)
			}
		}},
		{"a run another force cancel ended while this one asked it is left to that one", func(f *fcTarget) {
			f.closed = FCClassified{State: FCStateAlreadyEnded, Request: "fc-0"}
		}, nil, ForceCancelResult{State: FCStateAlreadyEnded, RequestID: "fc-1", EndedBy: "fc-0", Terminated: true}, func(t *testing.T, f *fcTarget) {
			if len(f.released)+len(f.published) != 0 {
				t.Errorf("released %v, published %v", f.released, f.published)
			}
		}},
		{"a run that ended on its own before is left as it is", func(f *fcTarget) { f.open, f.closed = false, FCClassified{State: FCStateEnded} }, nil,
			ForceCancelResult{State: FCStateAlreadyEnded, RequestID: "fc-1"}, nil},
		{"a run that ended canceled before is left as it is", func(f *fcTarget) { f.open, f.closed = false, FCClassified{State: FCStateCanceled} }, nil,
			ForceCancelResult{State: FCStateAlreadyEnded, RequestID: "fc-1"}, func(t *testing.T, f *fcTarget) {
				if len(f.canceled) != 0 || !slices.Contains(f.audits, completed) {
					t.Errorf("canceled %v, audits %v", f.canceled, f.audits)
				}
			}},
		{"a run that closed before its cancel was recorded had already ended", func(f *fcTarget) { f.closedAtCancel = true }, nil,
			ForceCancelResult{State: FCStateAlreadyEnded, RequestID: "fc-1"}, nil},
		{"progress gives the run its grace again, from what was read", func(f *fcTarget) {
			f.scans = []FCScanned{progressing("exec-1", 25), progressing("exec-1", 40), stalled("exec-1", 40)}
		}, nil, done, func(t *testing.T, f *fcTarget) {
			if !slices.Equal(f.terminateL0, []int64{40}) {
				t.Errorf("terminated from %v, want the window its stall test read", f.terminateL0)
			}
			if !slices.Equal(f.scannedL0, []int64{10, 25, 40}) {
				t.Errorf("scanned from %v, want each window to start where the last was read to", f.scannedL0)
			}
		}},
		{"a run still progressing at the ceiling is ended, saying so", func(f *fcTarget) {
			f.scans, f.decision.CeilingSeconds = []FCScanned{progressing("exec-1", 25)}, 300
		}, nil, func() ForceCancelResult { r := done; r.ProgressingAtCeiling = true; return r }(), func(t *testing.T, f *fcTarget) {
			if len(f.scannedL0) != 5 {
				t.Errorf("%d windows of 60 s, want the ceiling of 300 s used in full: 5", len(f.scannedL0))
			}
		}},
		{"a run that carries on in a new execution is asked there too, and judged by its own events", func(f *fcTarget) {
			f.scans = []FCScanned{progressing("exec-2", 5000), stalled("exec-2", 20)}
		}, nil, done, func(t *testing.T, f *fcTarget) {
			if !slices.Equal(f.canceled, []string{"exec-1", "exec-2"}) || !slices.Equal(f.scannedL0, []int64{10, 20}) ||
				!slices.Equal(f.terminated, []string{"exec-2"}) {
				t.Errorf("canceled %v, scanned from %v, terminated %v: want exec-2 canceled, scanned from its own cancel, then ended",
					f.canceled, f.scannedL0, f.terminated)
			}
		}},
		{"a run found ended by its stall test is not terminated", func(f *fcTarget) {
			f.closed = FCClassified{State: FCStateEnded}
			f.scans = []FCScanned{{ExecutionRunID: "exec-1"}}
			f.failing[ActivityFCClassify] = 1
		}, nil, ForceCancelResult{State: FCStateEnded, RequestID: "fc-1"}, func(t *testing.T, f *fcTarget) {
			if len(f.terminated) != 0 {
				t.Errorf("terminated %v", f.terminated)
			}
		}},
		{"a slot taken between the read and the close is released too", func(f *fcTarget) {
			f.gatesAfter = FCGates{Gates: []string{"ork-conc-bb"}, SeqBase: 4}
		}, nil, done, func(t *testing.T, f *fcTarget) {
			if len(f.released) != 2 {
				t.Errorf("released %v, want both reads' gates", f.released)
			}
		}},
		{"slots that could not be read are never said released", func(f *fcTarget) { f.failing[ActivityFCGates] = 1000 }, nil,
			func() ForceCancelResult { r := done; r.Released = false; return r }(), nil},
		{"slots read only in part are released, and not said all released", func(f *fcTarget) { f.gates.Unreadable = true }, nil,
			func() ForceCancelResult { r := done; r.Released = false; return r }(), func(t *testing.T, f *fcTarget) {
				if len(f.released) != 1 {
					t.Errorf("released %v", f.released)
				}
			}},
		{"a gate that cannot be told is not said released", func(f *fcTarget) { f.failing["signal"] = 1000 }, nil,
			func() ForceCancelResult { r := done; r.Released = false; return r }(), nil},
		{"a run that holds nothing has nothing to release", func(f *fcTarget) { f.gates = FCGates{} }, nil, done, nil},
		{"an unpublished ending is said, and everything else done", func(f *fcTarget) { f.failing[ActivityFCPublish] = 100000 }, nil,
			func() ForceCancelResult {
				r := done
				r.TerminalPublished, r.Cause = false, FCCauseEventUnpublished
				return r
			}(), func(t *testing.T, f *fcTarget) {
				if f.results[completed].Cause != FCCauseEventUnpublished {
					t.Errorf("the completed record says %+v", f.results[completed])
				}
			}},
		{"a sealed run's ending is asked for once, said unpublished, and everything else done", func(f *fcTarget) { f.publishErr = sealedEndingRefused("ork-r") }, nil,
			func() ForceCancelResult {
				r := done
				r.TerminalPublished, r.Cause = false, FCCauseEventUnpublished
				return r
			}(), func(t *testing.T, f *fcTarget) {
				if f.publishCalls != 1 || f.results[completed].Cause != FCCauseEventUnpublished {
					t.Errorf("asked %d times; the completed record says %+v", f.publishCalls, f.results[completed])
				}
			}},
		{"an ending that is published late is published", func(f *fcTarget) { f.publishEventFails = 7 }, nil, done, nil},
		{"an ending that has no sequence says so", func(f *fcTarget) { f.publish = FCPublished{Cause: FCCauseEventSequence} }, nil,
			func() ForceCancelResult {
				r := done
				r.TerminalPublished, r.Cause = false, FCCauseEventSequence
				return r
			}(), nil},
		{"someone else's termination is not this force cancel's: nothing is released or published", func(f *fcTarget) {
			f.closed = FCClassified{State: FCStateAlreadyEnded, Cause: FCCauseUnverifiedRecord}
		}, nil, ForceCancelResult{State: FCStateAlreadyEnded, Cause: FCCauseUnverifiedRecord, RequestID: "fc-1", Terminated: true}, func(t *testing.T, f *fcTarget) {
			if len(f.released)+len(f.published) != 0 {
				t.Errorf("released %v, published %v", f.released, f.published)
			}
		}},
		{"a key that stays unknown is waited for until the ceiling, then said", func(f *fcTarget) {
			f.closed = FCClassified{State: FCStateFailed, Cause: FCCauseUnknownKey, Retry: true}
		}, nil, ForceCancelResult{State: FCStateFailed, Cause: FCCauseUnknownKey, RequestID: "fc-1", Terminated: true}, nil},
		{"a key that becomes known while it is waited for makes the run this force cancel's", func(f *fcTarget) {
			f.closed = FCClassified{State: FCStateFailed, Cause: FCCauseUnknownKey, Retry: true}
			f.closedLater = FCClassified{State: FCStateForceCanceled}
		}, nil, done, nil},
		{"a run that moves to a newer execution under its termination is asked there, and given up after three", func(f *fcTarget) {
			f.switchOnTerminate = 99
			f.scan = func(FCStep) FCScanned { return stalled(f.exec, 40) }
		}, nil, ForceCancelResult{State: FCStateFailed, Cause: FCCauseRunKeptSwitching, RequestID: "fc-1", Terminated: true}, func(t *testing.T, f *fcTarget) {
			if !slices.Equal(f.terminated, []string{"exec-1", "exec-1'", "exec-1''"}) || !slices.Equal(f.canceled, []string{"exec-1", "exec-1'", "exec-1''"}) {
				t.Errorf("terminated %v, canceled %v: each execution is asked, waited for and then ended", f.terminated, f.canceled)
			}
		}},
		{"a run that moved between its stall test and its termination gets its grace again", func(f *fcTarget) { f.moves = 1 }, nil, done, func(t *testing.T, f *fcTarget) {
			if len(f.canceled) != 1 || len(f.scannedL0) != 2 || len(f.terminated) != 1 {
				t.Errorf("canceled %v, scanned %v, terminated %v: want one cancel, a second window, then one termination", f.canceled, f.scannedL0, f.terminated)
			}
		}},
		{"a run that always moves between the two is ended at the ceiling, saying so", func(f *fcTarget) { f.moves, f.decision.CeilingSeconds = 1<<30, 300 }, nil,
			func() ForceCancelResult { r := done; r.ProgressingAtCeiling = true; return r }(), func(t *testing.T, f *fcTarget) {
				if len(f.scannedL0) != 5 {
					t.Errorf("%d windows of 60 s, want the ceiling of 300 s: 5", len(f.scannedL0))
				}
			}},
		{"a run that announced its own end before the termination ended as it said", func(f *fcTarget) { f.publish = FCPublished{Ended: true} }, nil,
			ForceCancelResult{State: FCStateEnded, Ends: true, RequestID: "fc-1", Terminated: true, Released: true, TerminalPublished: true}, func(t *testing.T, f *fcTarget) {
				if len(f.published) != 0 || len(f.released) != 1 {
					t.Errorf("published %v, released %v", f.published, f.released)
				}
			}},
		{"a termination that fails before it can be sent is not said to have been", func(f *fcTarget) { f.unsent = true }, nil,
			ForceCancelResult{State: FCStateFailed, Cause: FCCauseRetriesExhausted, RequestID: "fc-1"}, func(t *testing.T, f *fcTarget) {
				if len(f.terminated) != 0 {
					t.Errorf("terminated %v", f.terminated)
				}
			}},
		{"one an engine that died had sent is found, though none could be sent since", func(f *fcTarget) { f.unsent, f.sentBefore = true, true }, nil, done, nil},
		{"a termination whose answer is lost is tried once each time, up to the ceiling, and said to be possible", func(f *fcTarget) { f.lost, f.decision.CeilingSeconds = 1<<30, 600 }, nil,
			ForceCancelResult{State: FCStateFailed, Cause: FCCauseRetriesExhausted, RequestID: "fc-1", Terminated: true}, func(t *testing.T, f *fcTarget) {
				if f.terminateCalls < 5 || f.terminateCalls > 20 {
					t.Errorf("%d calls in 9 minutes", f.terminateCalls)
				}
				if len(f.released) != 0 {
					t.Errorf("released %v of a run that is open", f.released)
				}
			}},
		{"a termination whose answer is lost a few times still ends the run", func(f *fcTarget) { f.lost = 6 }, nil, done, nil},
		{"one whose answer was lost that lands while the run is waited for again is finished", func(f *fcTarget) {
			f.lost, f.lands, f.movesAfter = 1, true, true
		}, nil, done, nil},
		{"a decision refused at the termination is not asked again", func(f *fcTarget) { f.refusesTerminate = true }, nil,
			ForceCancelResult{State: FCStateFailed, Cause: FCCauseDecisionUnverified, RequestID: "fc-1"}, func(t *testing.T, f *fcTarget) {
				if f.terminateCalls != 1 {
					t.Errorf("%d calls", f.terminateCalls)
				}
			}},
		{"an engine whose clock disagrees for a while does not stop it", func(f *fcTarget) { f.skews = 4 }, nil, done, nil},
		{"one whose clock always does is said", func(f *fcTarget) { f.skews, f.decision.CeilingSeconds = 1<<30, 300 }, nil,
			ForceCancelResult{State: FCStateFailed, Cause: FCCauseClockSkew, RequestID: "fc-1"}, nil},
		{"a run that ended on its own after it was asked is classified by the execution that closed", func(f *fcTarget) { f.endsOnCancel = true }, nil,
			ForceCancelResult{State: FCStateCanceled, RequestID: "fc-1"}, func(t *testing.T, f *fcTarget) {
				if !slices.Equal(f.classifiedOf, []string{"exec-1"}) {
					t.Errorf("classified %q", f.classifiedOf)
				}
			}},
		{"slots that could not be read for a while after the close are released", func(f *fcTarget) { f.failing[ActivityFCGates] = 9 }, nil, done, nil},
		{"slots that could not be read before the close, and were after it, are all released", func(f *fcTarget) { f.failing[ActivityFCGates] = 3 }, nil, done, nil},
		{"a run ended at its ceiling by itself was not progressing when it ended", func(f *fcTarget) {
			f.scans, f.decision.CeilingSeconds = []FCScanned{progressing("exec-1", 25)}, 60
			f.gone, f.closed = true, FCClassified{State: FCStateEnded}
		}, nil, ForceCancelResult{State: FCStateEnded, RequestID: "fc-1"}, nil},
		{"keys that cannot be read for five minutes are waited for", func(f *fcTarget) { f.verifyUnavailable = 10 }, nil, done, nil},
		{"a run that switches as its ceiling passes is asked to cancel in its new execution before it is ended there", func(f *fcTarget) {
			f.decision.CeilingSeconds, f.switchOnTerminate = 60, 1
			f.scan = func(FCStep) FCScanned { return progressing(f.exec, 40) }
		}, nil, func() ForceCancelResult { r := done; r.ProgressingAtCeiling = true; return r }(), func(t *testing.T, f *fcTarget) {
			if !slices.Equal(f.terminated, []string{"exec-1", "exec-1'"}) || !slices.Equal(f.canceled, []string{"exec-1", "exec-1'"}) {
				t.Errorf("terminated %v, canceled %v", f.terminated, f.canceled)
			}
		}},
		{"a run that moves under every termination past its ceiling is not given a grace each time", func(f *fcTarget) {
			f.decision.CeilingSeconds, f.switchOnTerminate = 60, 99
			f.scan = func(FCStep) FCScanned {
				if len(f.scannedL0) < 3 {
					return progressing(f.exec, 40)
				}
				return stalled(f.exec, 40) // just asked: nothing to judge it by
			}
		}, nil, ForceCancelResult{State: FCStateFailed, Cause: FCCauseRunKeptSwitching, RequestID: "fc-1", Terminated: true, ProgressingAtCeiling: true}, func(t *testing.T, f *fcTarget) {
			if len(f.scannedL0) != 3 || !slices.Equal(f.canceled, []string{"exec-1", "exec-1'", "exec-1''"}) {
				t.Errorf("%d stall tests, canceled %v: want its own grace's, one more's and one at once, each execution asked", len(f.scannedL0), f.canceled)
			}
		}},
		{"an answer for another run is no ending of this one", func(f *fcTarget) { f.answers(func(e *orkstackv1.Event) { e.RunId = "ork-other" }) }, nil, unpublished, nil},
		{"nor one for another request", func(f *fcTarget) { f.answers(func(e *orkstackv1.Event) { e.RequestId = "fc-9" }) }, nil, unpublished, nil},
		{"nor one that is no ending", func(f *fcTarget) {
			f.answers(func(e *orkstackv1.Event) { e.Type = orkstackv1.EventType_EVENT_TYPE_TASK_LOG })
		}, nil, unpublished, nil},
		{"nor another ending", func(f *fcTarget) {
			f.answers(func(e *orkstackv1.Event) { e.Type = orkstackv1.EventType_EVENT_TYPE_RUN_SUCCEEDED })
		}, nil, unpublished, nil},
		{"nor another status", func(f *fcTarget) {
			f.answers(func(e *orkstackv1.Event) { e.Status = orkstackv1.Status_STATUS_SUCCEEDED })
		}, nil, unpublished, nil},
		{"nor one at no place in its events", func(f *fcTarget) { f.answers(func(e *orkstackv1.Event) { e.Sequence = 0 }) }, nil, unpublished, nil},
		{"nor one that is no event", func(f *fcTarget) { f.publish.Event = []byte{0xff} }, nil, unpublished, nil},
		{"nor one that shows another run", func(f *fcTarget) {
			f.publish.SnapshotURI = "s3://snap/metadata/snapshots/payments/ork-other/9.pb"
		}, nil, unpublished, nil},
		{"nor one that shows no run", func(f *fcTarget) { f.publish.SnapshotURI = "" }, nil, unpublished, nil},
		{"nor one under a key of its own", func(f *fcTarget) { f.publish.DedupeKey = "ork-other#3" }, nil, unpublished, nil},
		{"nor the run's own end under another's key", func(f *fcTarget) { f.publish.Ended, f.publish.DedupeKey = true, "ork-other#3" }, nil, unpublished, nil},
		{"a run that announced its own end and whose slots are not released is not finished", func(f *fcTarget) {
			f.publish, f.failing["signal"] = FCPublished{Ended: true}, 1<<20
		}, nil, ForceCancelResult{State: FCStateEnded, Ends: true, RequestID: "fc-1", Terminated: true, TerminalPublished: true}, func(t *testing.T, f *fcTarget) {
			if f.results[completed].succeeded() || !f.results[completed].lacks() {
				t.Errorf("recorded as %+v", f.results[completed])
			}
		}},
		{"a cause nobody here says is not repeated", func(f *fcTarget) { f.publish = FCPublished{Cause: "<b>anything</b>"} }, nil, unpublished, nil},
		{"a termination refused before it is sent is said", func(f *fcTarget) { f.terminateCause = FCCauseDecisionStale }, nil,
			ForceCancelResult{State: FCStateFailed, Cause: FCCauseDecisionStale, RequestID: "fc-1"}, nil},
		{"a decision that does not verify ends nothing and names nobody", func(f *fcTarget) { f.verifyCause = FCCauseDecisionUnverified }, nil,
			ForceCancelResult{State: FCStateFailed, Cause: FCCauseDecisionUnverified}, func(t *testing.T, f *fcTarget) {
				if len(f.canceled)+len(f.terminated)+len(f.audits) != 0 || f.refused != 1 {
					t.Errorf("canceled %v, terminated %v, audits %v, refused %d", f.canceled, f.terminated, f.audits, f.refused)
				}
			}},
		{"keys that cannot be read blame no one's decision", func(f *fcTarget) { f.verifyCause = FCCauseKeysUnavailable }, nil,
			ForceCancelResult{State: FCStateFailed, Cause: FCCauseKeysUnavailable}, func(t *testing.T, f *fcTarget) {
				if f.refused != 0 {
					t.Errorf("refused %d", f.refused)
				}
			}},
		{"a cluster that cannot be read is not said to be the keys", func(f *fcTarget) { f.failing[ActivityFCVerify] = 1 << 30 }, nil,
			ForceCancelResult{State: FCStateFailed, Cause: FCCauseRetriesExhausted}, func(t *testing.T, f *fcTarget) {
				if f.refused != 0 {
					t.Errorf("refused %d", f.refused)
				}
			}},
		{"a failed outcome the store refuses a few times is still recorded", func(f *fcTarget) {
			f.failing[ActivityFCScan], f.failing[ActivityFCAudit+completed] = 1<<30, 5
		}, nil, ForceCancelResult{State: FCStateFailed, Cause: FCCauseRetriesExhausted, RequestID: "fc-1"}, nil},
		{"an unsigned request that cannot be audited ends nothing", func(f *fcTarget) { f.failing[ActivityFCAudit+requested] = 1000 }, func(in *ForceCancelInput) {
			in.Decision, in.Unsigned = "", &ForceCancelDecision{}
		}, ForceCancelResult{State: FCStateFailed, Cause: FCCauseAuditUnwritable, RequestID: "fc-1", Unaudited: true}, func(t *testing.T, f *fcTarget) {
			if len(f.canceled) != 0 {
				t.Errorf("canceled %v", f.canceled)
			}
		}},
		{"a signed request goes on when it cannot be audited, and says so", func(f *fcTarget) { f.failing[ActivityFCAudit+requested] = 1000 }, nil,
			func() ForceCancelResult { r := done; r.Unaudited = true; return r }(), nil},
		{"an outcome that cannot be audited is said", func(f *fcTarget) { f.failing[ActivityFCAudit+completed] = 100000 }, nil,
			func() ForceCancelResult { r := done; r.Unaudited = true; return r }(), nil},
		{"a step that keeps failing before the termination leaves the run, audited", func(f *fcTarget) { f.failing[ActivityFCScan] = 1 << 30 }, nil,
			ForceCancelResult{State: FCStateFailed, Cause: FCCauseRetriesExhausted, RequestID: "fc-1"}, func(t *testing.T, f *fcTarget) {
				if len(f.terminated) != 0 || f.results[completed].Cause != FCCauseRetriesExhausted {
					t.Errorf("terminated %v, completed %+v", f.terminated, f.results[completed])
				}
			}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f, in := newFCTarget(), fcInput()
			if tt.setup != nil {
				tt.setup(f)
			}
			if tt.in != nil {
				tt.in(&in)
			}
			if got := runFC(t, f, in); got != tt.want {
				t.Errorf("result = %+v\nwant     %+v", got, tt.want)
			}
			if tt.check != nil {
				tt.check(t, f)
			}
		})
	}
}

// TestForceCancelWorkflow_ActsOnlyUnderItsDecisionsOwnID: a genuine decision
// copied into another start does nothing.
func TestForceCancelWorkflow_ActsOnlyUnderItsDecisionsOwnID(t *testing.T) {
	f := newFCTarget()
	got := runFC(t, f, fcInput(), func(env *testsuite.TestWorkflowEnvironment) {
		env.SetStartWorkflowOptions(client.StartWorkflowOptions{ID: "ork-force-copied", TaskQueue: "ork-tasks"})
	})
	if got.State != FCStateFailed || got.Cause != FCCauseDecisionUnverified || len(f.canceled)+len(f.terminated)+len(f.audits) != 0 {
		t.Fatalf("a copied decision: %+v, canceled %v, terminated %v, audits %v", got, f.canceled, f.terminated, f.audits)
	}
}

// TestForceCancelWorkflow_TheTerminationsAnswerIsNotItsOutcome: an answer
// lost on the way back, and a cluster that cannot be read right after, leave a
// run that was ended: it is classified, released, published and audited.
func TestForceCancelWorkflow_TheTerminationsAnswerIsNotItsOutcome(t *testing.T) {
	f := newFCTarget()
	f.failing[ActivityFCTerminate] = 1000
	failed := false
	got := runFC(t, f, fcInput(), func(env *testsuite.TestWorkflowEnvironment) {
		env.SetOnLocalActivityCompletedListener(func(info *activity.Info, _ converter.EncodedValue, _ error) {
			if info.ActivityType.Name == ActivityFCTerminate && !failed {
				failed = true
				f.mu.Lock()
				f.failing[ActivityFCDescribe], f.failing[ActivityFCClassify] = 20, 20
				f.mu.Unlock()
			}
		})
	})
	if got.State != FCStateForceCanceled || !got.Terminated || !got.Released || !got.TerminalPublished || len(f.audits) != 2 {
		t.Fatalf("result %+v, audits %v", got, f.audits)
	}
}

// TestForceCancelWorkflow_StoppedFromOutside: canceled while it waits, it ends
// nothing and says it was stopped; canceled once the run is ended, it still
// releases, publishes and audits.
func TestForceCancelWorkflow_StoppedFromOutside(t *testing.T) {
	t.Run("while it waits", func(t *testing.T) {
		f := newFCTarget()
		got := runFC(t, f, fcInput(), func(env *testsuite.TestWorkflowEnvironment) {
			env.RegisterDelayedCallback(env.CancelWorkflow, 10*time.Second)
		})
		if got.State != FCStateFailed || got.Cause != FCCauseStopped || len(f.terminated) != 0 || len(f.audits) != 2 {
			t.Fatalf("result %+v, terminated %v, audits %v", got, f.terminated, f.audits)
		}
	})
	t.Run("after the termination", func(t *testing.T) {
		f := newFCTarget()
		f.publishEventFails = 7 // its ending is retried well past the cancel
		got := runFC(t, f, fcInput(), func(env *testsuite.TestWorkflowEnvironment) {
			env.RegisterDelayedCallback(env.CancelWorkflow, 62*time.Second)
		})
		if got.State != FCStateForceCanceled || !got.Released || !got.TerminalPublished || len(f.audits) != 2 {
			t.Fatalf("result %+v, audits %v", got, f.audits)
		}
	})
}

// TestForceCancelWorkflow_EndsWhateverFails: with every step an ended run
// needs failing for good, the force cancel still answers, in about one ceiling
// more, shared between them, and says what is missing.
func TestForceCancelWorkflow_EndsWhateverFails(t *testing.T) {
	for name, setup := range map[string]func(*fcTarget){
		"its classification and record": func(f *fcTarget) { f.failing[ActivityFCClassify] = 1 << 30 },
		"its ending and record":         func(f *fcTarget) { f.failing[ActivityFCPublish] = 1 << 30 },
		"its announcement and record":   func(f *fcTarget) { f.publishEventFails = 1 << 30 },
	} {
		t.Run(name, func(t *testing.T) {
			f := newFCTarget()
			f.decision.CeilingSeconds = 7200
			f.failing[ActivityFCAudit+forceCancelCompleted] = 1 << 30
			setup(f)
			var began time.Time
			var env *testsuite.TestWorkflowEnvironment
			got := runFC(t, f, fcInput(), func(e *testsuite.TestWorkflowEnvironment) { env, began = e, e.Now() })
			if took := env.Now().Sub(began); took > 2*time.Hour+15*time.Minute {
				t.Errorf("it took %v for a ceiling of 2h", took)
			}
			if !got.Terminated || !got.Unaudited || got.TerminalPublished || got.Cause == "" {
				t.Errorf("result %+v does not say what is missing", got)
			}
		})
	}
}

// TestForceCancelTimings: what an input says is held to bounds, and what it does
// not say is the default; every start says all of it.
func TestForceCancelTimings(t *testing.T) {
	def := defaultForceCancelTimings()
	mine := def
	mine.Patience, mine.StepAttempts = 3*time.Minute, 5
	spaced, held := def, def
	spaced.SettleFirst, spaced.SettleMax = time.Hour, 30*time.Second
	held.SettleFirst, held.SettleMax = 30*time.Second, 30*time.Second
	for _, tt := range []struct{ in, want ForceCancelTimings }{
		{ForceCancelTimings{}, def},
		{mine, mine},
		{spaced, held},
		{ForceCancelTimings{StepAttempts: 1 << 40, TerminateTries: -1, DeriveTries: 11, PollSpread: 1, SettleFirst: 1, SettleMax: -time.Hour, Patience: 1 << 62}, def},
	} {
		if got := (ForceCancelInput{Timings: tt.in}).timings(); got != tt.want {
			t.Errorf("%+v: %+v, want %+v", tt.in, got, tt.want)
		}
	}
	s := NewForceCancelActivities(ForceCancelDeps{Keys: recordKeys(t), Mode: func() string { return "enforce" }}).starting()
	in, err := s.input(sampleDecision())
	if err != nil {
		t.Fatal(err)
	}
	if in.Timings != def {
		t.Errorf("started with %+v", in.Timings)
	}
}

// TestForceCancelWorkflow_ItsOwnTimings: a force cancel spaces its steps with what
// it was started with, not with this release's.
func TestForceCancelWorkflow_ItsOwnTimings(t *testing.T) {
	took := func(timings ForceCancelTimings) time.Duration {
		f := newFCTarget()
		f.failing[ActivityFCVerify] = 1 << 30
		in := fcInput()
		in.Timings = timings
		var began, ended time.Time
		runFC(t, f, in, func(env *testsuite.TestWorkflowEnvironment) {
			began = env.Now()
			env.SetOnTimerFiredListener(func(string) { ended = env.Now() })
		})
		return ended.Sub(began)
	}
	short := defaultForceCancelTimings()
	short.SettleLeast /= 2
	if whole, half := took(ForceCancelTimings{}), took(short); whole < forceCancelSettleLeast || half >= whole || half < short.SettleLeast {
		t.Errorf("gave up after %s, and with half the time after %s", whole, half)
	}
}

// TestForceCancelOptions: what an engine must take from a channel is waited
// for in bounded time, whether or not one ever polls it.
func TestForceCancelOptions(t *testing.T) {
	f := &forceCancel{t: defaultForceCancelTimings()}
	for name, o := range map[string]workflow.ActivityOptions{"build": f.buildOptions(), "announce": f.announceOptions()} {
		if o.TaskQueue != EventQueue || o.ScheduleToStartTimeout != forceCancelPublishStart || o.StartToCloseTimeout <= 0 {
			t.Errorf("%s options %+v", name, o)
		}
	}
}

// TestForceCancelWorkflow_StoppedWhileItAsksAgain: stopping it from outside
// ends the wait for a step that keeps failing, before anything was ended.
func TestForceCancelWorkflow_StoppedWhileItAsksAgain(t *testing.T) {
	for _, step := range []string{ActivityFCDescribe, ActivityFCCancel, ActivityFCScan} {
		f := newFCTarget()
		f.failing[step] = 1 << 30
		got := runFC(t, f, fcInput(), func(env *testsuite.TestWorkflowEnvironment) {
			env.RegisterDelayedCallback(env.CancelWorkflow, 3*time.Minute)
		})
		if got.State != FCStateFailed || got.Cause != FCCauseStopped || got.Terminated {
			t.Errorf("%s: result %+v, want stopped", step, got)
		}
	}
}

// TestForceCancelWorkflow_StoppedAroundATermination: stopping it from outside
// ends a termination that was not sent, and never one that may have been:
// what the ended run needs is finished, or said to be missing.
func TestForceCancelWorkflow_StoppedAroundATermination(t *testing.T) {
	t.Run("not sent, and the run cannot be described", func(t *testing.T) {
		f := newFCTarget()
		f.unsent, f.failDescribeFrom, f.stopAtDescribe = true, 2, 4
		got := runFC(t, f, fcInput())
		if got.Cause != FCCauseStopped || got.Terminated {
			t.Errorf("result %+v, want stopped", got)
		}
	})
	t.Run("may have been sent, and landed after", func(t *testing.T) {
		f := newFCTarget()
		f.lost, f.stopAtDescribe = 1, 2
		got := runFC(t, f, fcInput())
		if got.State != FCStateForceCanceled || !got.Released || !got.TerminalPublished {
			t.Errorf("result %+v, want it finished", got)
		}
	})
	t.Run("may have been sent, and the run cannot be described", func(t *testing.T) {
		f := newFCTarget()
		f.lost, f.failDescribeFrom, f.stopAtDescribe, f.lateClose = 1, 2, 3, true
		got := runFC(t, f, fcInput())
		if got.State != FCStateForceCanceled || !got.Released || !got.TerminalPublished {
			t.Errorf("result %+v, want it finished by its close", got)
		}
	})
	t.Run("may have been sent, and the run carried on", func(t *testing.T) {
		f := newFCTarget()
		f.switchOnTerminate, f.stopInGates = 1, false
		f.scan = func(in FCStep) FCScanned { return stalled(f.exec, 40) }
		f.unsentAfterSwitch, f.stopAtDescribe = true, 4
		got := runFC(t, f, fcInput())
		if got.Cause != FCCauseStopped {
			t.Errorf("result %+v, want stopped: nothing was sent to the newer execution", got)
		}
	})
	t.Run("sent by an engine that died, and stopped before it is classified", func(t *testing.T) {
		f := newFCTarget()
		f.unsent, f.sentBefore, f.stopAtDescribe, f.failing[ActivityFCClassify] = true, true, 2, 9
		got := runFC(t, f, fcInput())
		if got.State != FCStateForceCanceled || !got.Released || !got.TerminalPublished {
			t.Errorf("result %+v, want it finished", got)
		}
	})
	t.Run("ended on its own, and cannot be classified", func(t *testing.T) {
		f := newFCTarget()
		f.scans = []FCScanned{{ExecutionRunID: "exec-1"}}
		f.failing[ActivityFCClassify] = 1 << 30
		var began time.Time
		var env *testsuite.TestWorkflowEnvironment
		got := runFC(t, f, fcInput(), func(e *testsuite.TestWorkflowEnvironment) {
			env, began = e, e.Now()
			e.RegisterDelayedCallback(e.CancelWorkflow, 5*time.Minute)
		})
		if took := env.Now().Sub(began); took > 10*time.Minute || got.Cause != FCCauseStopped {
			t.Errorf("result %+v after %v: want stopped, with nothing sent", got, took)
		}
	})
	t.Run("stopped before it", func(t *testing.T) {
		f := newFCTarget()
		f.stopInGates = true
		got := runFC(t, f, fcInput())
		if got.Cause != FCCauseStopped || f.terminateCalls != 0 {
			t.Errorf("result %+v after %d terminations, want none", got, f.terminateCalls)
		}
	})
}

// TestForceCancelWorkflow_ARunStillOpenIsNotWaitedForAgain: a termination that
// never landed is looked for a little longer, not for another whole wait.
func TestForceCancelWorkflow_ARunStillOpenIsNotWaitedForAgain(t *testing.T) {
	f := newFCTarget()
	f.lost, f.decision.CeilingSeconds = 1<<30, 600
	var began time.Time
	var env *testsuite.TestWorkflowEnvironment
	got := runFC(t, f, fcInput(), func(e *testsuite.TestWorkflowEnvironment) { env, began = e, e.Now() })
	if took := env.Now().Sub(began); took > 10*time.Minute+forceCancelPatience+2*forceCancelSettleMax || got.Cause != FCCauseRetriesExhausted {
		t.Errorf("result %+v after %v", got, took)
	}
}
