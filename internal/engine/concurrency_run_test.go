package engine

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/workflow"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/payloadcrypt"
	"github.com/orkstack/orkstack/internal/task"
)

const concRunID = "ork-conc-run-test"

// concRunHarness runs one RunWorkflow under a run-level limit, with the
// gate played by answers it is given: the acquire local activity returns
// them in order, and every release the run sends is recorded.
type concRunHarness struct {
	t        *testing.T
	env      *testsuite.TestWorkflowEnvironment
	mu       sync.Mutex
	answers  []AcquireConcurrencyResult
	acquires []AcquireConcurrencyInput
	releases []GateReleaseAll
	events   []*orkstackv1.Event
	last     *orkstackv1.Run
	ran      int
}

func concPlan() RunPlan {
	run := 0
	return RunPlan{Namespace: "team-a", Routes: map[string]string{"main": "ork-tasks"}, Concurrency: &ConcurrencyPlan{
		Slots:     []ConcurrencySlotPlan{{GateID: "ork-conc-test", Key: "deploy", Label: "deploy", Limit: 1, Strategy: gateStrategyQueue, QueueTimeoutSeconds: 3600}},
		Run:       &run,
		SeqBase:   1,
		GateQueue: "ork-tasks", Namespace: "team-a",
		AcquireTimeout: 2 * time.Minute, AcquireBackoffMin: 200 * time.Millisecond, AcquireBackoffMax: 10 * time.Second,
		RequeueInterval: time.Minute, RequeueBackoffFactor: 2, RequeueIntervalMax: 15 * time.Minute,
		PerSlotBytes: 2048, EventHoldersShown: 5, Gate: DefaultGateTunables(),
	}}
}

func concOrkflow() *orkstackv1.Orkflow {
	return &orkstackv1.Orkflow{Name: "migrate", Version: "v1", Namespace: "team-a",
		Tasks: []*orkstackv1.Task{{Id: "apply", Image: &orkstackv1.ImageRef{Reference: "alpine"}}}}
}

func newConcRunHarness(t *testing.T, answers ...AcquireConcurrencyResult) *concRunHarness {
	t.Helper()
	var suite testsuite.WorkflowTestSuite
	h := &concRunHarness{t: t, env: generousDeadlock(suite.NewTestWorkflowEnvironment()), answers: answers}
	h.env.SetTestTimeout(30 * time.Second)
	h.env.RegisterWorkflowWithOptions(RunWorkflow, workflow.RegisterOptions{Name: RunWorkflowType})
	h.env.RegisterWorkflowWithOptions(RunWorkflowRaw, workflow.RegisterOptions{Name: "raw"})
	h.env.RegisterActivityWithOptions(func(_ context.Context, in *orkstackv1.ExecuteTaskInput) (*orkstackv1.ExecuteTaskResult, error) {
		h.mu.Lock()
		h.ran++
		h.mu.Unlock()
		return &orkstackv1.ExecuteTaskResult{TaskId: in.GetTaskId(), ValuesJson: []byte(`{}`)}, nil
	}, activity.RegisterOptions{Name: task.ActivityExecuteTask})
	h.env.RegisterActivityWithOptions(func(_ context.Context, run *orkstackv1.Run, _ int64) (string, error) {
		h.mu.Lock()
		defer h.mu.Unlock()
		h.last = run
		return "memory://x", nil
	}, activity.RegisterOptions{Name: ActivityWriteSnapshot})
	h.env.RegisterActivityWithOptions(func(_ context.Context, e *orkstackv1.Event, _, _ string) error {
		h.mu.Lock()
		defer h.mu.Unlock()
		h.events = append(h.events, e)
		return nil
	}, activity.RegisterOptions{Name: ActivityPublishEvent})
	h.env.RegisterActivityWithOptions(func(_ context.Context, in AcquireConcurrencyInput) (AcquireConcurrencyResult, error) {
		h.mu.Lock()
		defer h.mu.Unlock()
		h.acquires = append(h.acquires, in)
		if len(h.answers) == 0 {
			return AcquireConcurrencyResult{}, errors.New("the test gave no more answers")
		}
		a := h.answers[0]
		h.answers = h.answers[1:]
		return a, nil
	}, activity.RegisterOptions{Name: ActivityAcquireConcurrency})
	h.env.OnSignalExternalWorkflow(mock.Anything, "ork-conc-test", "", GateSignalReleaseAll, mock.Anything).Return(
		func(_, _, _, _ string, arg any) error {
			h.mu.Lock()
			defer h.mu.Unlock()
			h.releases = append(h.releases, payloadcrypt.Unplain(arg).(GateReleaseAll))
			return nil
		})
	h.env.SetStartWorkflowOptions(client.StartWorkflowOptions{ID: concRunID})
	return h
}

func (h *concRunHarness) run(plan RunPlan) { h.runOrkflow(plan, concOrkflow()) }

func (h *concRunHarness) runOrkflow(plan RunPlan, wf *orkstackv1.Orkflow) {
	h.env.ExecuteWorkflow(RunWorkflow, concRunID, wf, &orkstackv1.Payload{}, plan, (*orkstackv1.RunContinuation)(nil))
	if !h.env.IsWorkflowCompleted() {
		h.t.Fatal("the run did not end")
	}
}

func (h *concRunHarness) eventTypes() []orkstackv1.EventType {
	var out []orkstackv1.EventType
	for _, e := range h.events {
		out = append(out, e.GetType())
	}
	return out
}

func granted(seq int64) AcquireConcurrencyResult {
	return AcquireConcurrencyResult{Answer: gateAnswer{Proto: 1, Answer: gateAnswerGranted, Seq: seq, ArrivedAt: time.Unix(1700000000, 0).UTC()}}
}

func queued(seq int64, position int) AcquireConcurrencyResult {
	return AcquireConcurrencyResult{Answer: gateAnswer{Proto: 1, Answer: gateAnswerQueued, Seq: seq, Position: position, HoldersAhead: 1,
		ArrivedAt: time.Unix(1700000000, 0).UTC()}}
}

// A granted run starts, runs, succeeds, and releases its slot below its
// incarnation's next on the way out.
func TestConcurrencyRun_GrantedRunsAndReleases(t *testing.T) {
	h := newConcRunHarness(t, granted(1))
	h.run(concPlan())
	if err := h.env.GetWorkflowError(); err != nil {
		t.Fatalf("the run ended %v", err)
	}
	if h.ran != 1 || len(h.acquires) != 1 {
		t.Fatalf("ran %d tasks after %d acquires; want 1 and 1", h.ran, len(h.acquires))
	}
	if a := h.acquires[0]; a.Request.TaskIx != gateRunLevel || a.Request.SeqBase != 1 || a.Request.Strategy != gateStrategyQueue || a.Start.Key != "deploy" {
		t.Fatalf("the acquire = %+v", a)
	}
	if len(h.releases) != 1 || h.releases[0] != (GateReleaseAll{Run: concRunID, Below: 2}) {
		t.Fatalf("releases %+v, want one release_all below 2", h.releases)
	}
}

// A queued run emits RUN_QUEUED, waits PENDING, and starts on its grant.
func TestConcurrencyRun_QueuedStartsOnItsGrant(t *testing.T) {
	h := newConcRunHarness(t, queued(2, 1))
	h.env.RegisterDelayedCallback(func() {
		if h.ran != 0 {
			t.Error("the queued run started before its grant")
		}
		h.env.SignalWorkflow(ConcurrencyGrantSignal, ConcurrencyGrant{Gate: "ork-conc-test", Run: concRunID, TaskIx: gateRunLevel, SeqBase: 1, Seq: 2})
	}, 30*time.Second)
	h.run(concPlan())
	if err := h.env.GetWorkflowError(); err != nil {
		t.Fatalf("the run ended %v", err)
	}
	types := h.eventTypes()
	if len(types) < 3 || types[0] != orkstackv1.EventType_EVENT_TYPE_RUN_CREATED || types[1] != orkstackv1.EventType_EVENT_TYPE_RUN_QUEUED ||
		types[2] != orkstackv1.EventType_EVENT_TYPE_RUN_STARTED {
		t.Fatalf("events %v; want RUN_CREATED, RUN_QUEUED, RUN_STARTED first", types)
	}
	if h.events[1].GetStatus() != orkstackv1.Status_STATUS_PENDING {
		t.Fatalf("RUN_QUEUED carries %v, want PENDING", h.events[1].GetStatus())
	}
	if h.ran != 1 || len(h.releases) != 1 {
		t.Fatalf("ran %d, released %d; want 1 and 1", h.ran, len(h.releases))
	}
}

// A grant that is not this run's entry (another seq) is ignored.
func TestConcurrencyRun_AForeignGrantIsIgnored(t *testing.T) {
	h := newConcRunHarness(t, queued(2, 1))
	h.env.RegisterDelayedCallback(func() {
		h.env.SignalWorkflow(ConcurrencyGrantSignal, ConcurrencyGrant{Gate: "ork-conc-test", Run: concRunID, TaskIx: gateRunLevel, SeqBase: 1, Seq: 9})
	}, 10*time.Second)
	h.env.RegisterDelayedCallback(func() {
		if h.ran != 0 {
			t.Error("a grant for another seq started the run")
		}
		h.env.SignalWorkflow(ConcurrencyGrantSignal, ConcurrencyGrant{Gate: "ork-conc-test", Run: concRunID, TaskIx: gateRunLevel, SeqBase: 1, Seq: 2})
	}, 20*time.Second)
	h.run(concPlan())
	if err := h.env.GetWorkflowError(); err != nil || h.ran != 1 {
		t.Fatalf("the run ended %v after %d tasks; want success after its own grant", err, h.ran)
	}
}

// Each answer the limit decides ends the run with its own reason, and
// releases.
func TestConcurrencyRun_Endings(t *testing.T) {
	for name, tc := range map[string]struct {
		answer AcquireConcurrencyResult
		code   string
	}{
		"full":        {AcquireConcurrencyResult{Answer: gateAnswer{Proto: 1, Answer: gateAnswerFull}}, RunConcurrencyLimitCode},
		"line full":   {AcquireConcurrencyResult{Answer: gateAnswer{Proto: 1, Answer: gateAnswerFullBound}}, RunConcurrencyFullCode},
		"rebuilding":  {AcquireConcurrencyResult{Answer: gateAnswer{Proto: 1, Answer: gateAnswerUnavailable}}, RunConcurrencyUnavailableCode},
		"no answer":   {AcquireConcurrencyResult{Unavailable: "deadline"}, RunConcurrencyUnavailableCode},
		"fenced":      {AcquireConcurrencyResult{Answer: gateAnswer{Proto: 1, Answer: gateAnswerFenced}}, RunConcurrencyFencedCode},
		"unsupported": {AcquireConcurrencyResult{Unsupported: "protocol"}, RunConcurrencyUnsupportedCode},
	} {
		t.Run(name, func(t *testing.T) {
			h := newConcRunHarness(t, tc.answer)
			h.run(concPlan())
			var app *temporal.ApplicationError
			if err := h.env.GetWorkflowError(); !errors.As(err, &app) || app.Type() != tc.code {
				t.Fatalf("the run ended %v, want %s", err, tc.code)
			}
			if h.ran != 0 || len(h.releases) != 1 {
				t.Fatalf("ran %d, released %d; want 0 and 1", h.ran, len(h.releases))
			}
		})
	}
}

// A queued run whose wait ends times out, and one canceled while it waits
// ends canceled; both release.
func TestConcurrencyRun_QueueTimeoutAndCancel(t *testing.T) {
	plan := concPlan()
	plan.Concurrency.Slots[0].QueueTimeoutSeconds = 30
	h := newConcRunHarness(t, queued(2, 1))
	h.run(plan)
	var app *temporal.ApplicationError
	if err := h.env.GetWorkflowError(); !errors.As(err, &app) || app.Type() != RunQueueTimeoutCode {
		t.Fatalf("the run ended %v, want %s", err, RunQueueTimeoutCode)
	}
	if len(h.releases) != 1 {
		t.Fatalf("released %d times, want 1", len(h.releases))
	}

	c := newConcRunHarness(t, queued(2, 1))
	c.env.RegisterDelayedCallback(c.env.CancelWorkflow, 10*time.Second)
	c.run(concPlan())
	if err := c.env.GetWorkflowError(); !temporal.IsCanceledError(err) {
		t.Fatalf("the run ended %v, want canceled", err)
	}
	if c.ran != 0 || len(c.releases) != 1 {
		t.Fatalf("ran %d, released %d; want 0 and 1", c.ran, len(c.releases))
	}
}

// A waiting run re-asserts its place on its timer, and a re-assert that is
// granted starts it.
func TestConcurrencyRun_ReassertIsGranted(t *testing.T) {
	h := newConcRunHarness(t, queued(2, 1), granted(2))
	h.run(concPlan())
	if err := h.env.GetWorkflowError(); err != nil || h.ran != 1 {
		t.Fatalf("the run ended %v after %d tasks; want success after the re-assert", err, h.ran)
	}
	if len(h.acquires) != 2 || !h.acquires[1].Request.ArrivedAt.Equal(time.Unix(1700000000, 0).UTC()) || h.acquires[1].Request.Seq != 2 ||
		h.acquires[1].UpdateID == h.acquires[0].UpdateID {
		t.Fatalf("acquires %+v; want a re-assert at the place given, as a new attempt", h.acquires)
	}
}

// A run with no limit asks no gate and releases nothing.
func TestConcurrencyRun_NoLimitIsUnchanged(t *testing.T) {
	h := newConcRunHarness(t)
	h.run(RunPlan{Namespace: "team-a", Routes: map[string]string{"main": "ork-tasks"}})
	if err := h.env.GetWorkflowError(); err != nil || len(h.acquires) != 0 || len(h.releases) != 0 || h.ran != 1 {
		t.Fatalf("err %v, %d acquires, %d releases, %d tasks; want a plain run", err, len(h.acquires), len(h.releases), h.ran)
	}
}

// If every re-assert during a wait goes unanswered, the wait's end is the
// limit being unreachable, never a queue timeout.
func TestConcurrencyRun_AnUnansweredWaitEndsUnavailable(t *testing.T) {
	plan := concPlan()
	plan.Concurrency.Slots[0].QueueTimeoutSeconds = 180
	h := newConcRunHarness(t, queued(2, 1), AcquireConcurrencyResult{Unavailable: "down"}, AcquireConcurrencyResult{Unavailable: "down"})
	h.run(plan)
	var app *temporal.ApplicationError
	if err := h.env.GetWorkflowError(); !errors.As(err, &app) || app.Type() != RunConcurrencyUnavailableCode {
		t.Fatalf("the run ended %v, want %s", err, RunConcurrencyUnavailableCode)
	}
	if len(h.acquires) < 2 {
		t.Fatalf("%d acquires; want the wait to have re-asserted", len(h.acquires))
	}
}

// When the gate puts the run's lost entry back under a new seq, the run's
// grant names that seq, and the run takes it.
func TestConcurrencyRun_ARestoredEntrysGrantIsTaken(t *testing.T) {
	h := newConcRunHarness(t, queued(2, 1), queued(5, 1))
	h.env.RegisterDelayedCallback(func() {
		h.env.SignalWorkflow(ConcurrencyGrantSignal, ConcurrencyGrant{Gate: "ork-conc-test", Run: concRunID, TaskIx: gateRunLevel, SeqBase: 1, Seq: 5})
	}, 90*time.Second)
	h.run(concPlan())
	if err := h.env.GetWorkflowError(); err != nil || h.ran != 1 {
		t.Fatalf("the run ended %v after %d tasks; want it started by the grant for its new seq", err, h.ran)
	}
}

// A plan that names a run-level slot it does not have ends the run
// explicitly, never runs it unlimited.
func TestConcurrencyRun_AnInvalidPlanIsRefused(t *testing.T) {
	plan := concPlan()
	bad := 3
	plan.Concurrency.Run = &bad
	h := newConcRunHarness(t)
	h.run(plan)
	var app *temporal.ApplicationError
	if err := h.env.GetWorkflowError(); !errors.As(err, &app) || app.Type() != RunConcurrencyUnsupportedCode || h.ran != 0 {
		t.Fatalf("the run ended %v after %d tasks, want %s and none", err, h.ran, RunConcurrencyUnsupportedCode)
	}
}

// A grant that is not the run's entry does not put its re-assert off.
func TestConcurrencyRun_ForeignGrantsDoNotDelayTheReassert(t *testing.T) {
	h := newConcRunHarness(t, queued(2, 1), granted(2))
	for i := 1; i <= 5; i++ {
		h.env.RegisterDelayedCallback(func() {
			h.env.SignalWorkflow(ConcurrencyGrantSignal, ConcurrencyGrant{Gate: "ork-conc-test", Run: concRunID, TaskIx: gateRunLevel, SeqBase: 1, Seq: 99})
		}, time.Duration(i)*50*time.Second)
	}
	h.env.RegisterDelayedCallback(func() {
		if len(h.acquires) < 2 {
			t.Error("foreign grants every 50s put the 60s re-assert off")
		}
	}, 70*time.Second)
	h.run(concPlan())
	if err := h.env.GetWorkflowError(); err != nil || h.ran != 1 {
		t.Fatalf("the run ended %v after %d tasks", err, h.ran)
	}
}

// A run that ends while it waits in line, or that the gate refuses, never
// held its slot, so it starts no task: a failure handler too would run
// outside the limit the run waited for. It ends with its own reason.
func TestConcurrencyRun_AnEndingInLineStartsNoHandler(t *testing.T) {
	handlerOrkflow := func() *orkstackv1.Orkflow {
		wf := concOrkflow()
		wf.Tasks = append(wf.Tasks, &orkstackv1.Task{Id: "rollback", Needs: []string{"apply"},
			Join: orkstackv1.JoinPolicy_JOIN_POLICY_ANY_FAILED, Image: &orkstackv1.ImageRef{Reference: "alpine"}},
			&orkstackv1.Task{Id: "notify", Needs: []string{"apply"},
				Join: orkstackv1.JoinPolicy_JOIN_POLICY_ALL_DONE, Image: &orkstackv1.ImageRef{Reference: "alpine"}})
		return wf
	}
	timeout := concPlan()
	timeout.Concurrency.Slots[0].QueueTimeoutSeconds = 30
	for name, tc := range map[string]struct {
		plan     RunPlan
		answer   AcquireConcurrencyResult
		code     string
		category orkstackv1.FailureCategory
		// canceled is a run that ends CANCELED with its own code (design
		// §6.1): only a supersede.
		canceled bool
	}{
		"superseded": {concPlan(), AcquireConcurrencyResult{Answer: gateAnswer{Proto: 1, Answer: gateAnswerSuperseded, By: "ork-newer"}}, RunSupersededCode,
			orkstackv1.FailureCategory_FAILURE_CATEGORY_CANCELLED, true},
		"queue timeout": {timeout, queued(2, 1), RunQueueTimeoutCode, orkstackv1.FailureCategory_FAILURE_CATEGORY_TIMED_OUT, false},
		"line full": {concPlan(), AcquireConcurrencyResult{Answer: gateAnswer{Proto: 1, Answer: gateAnswerFullBound}}, RunConcurrencyFullCode,
			orkstackv1.FailureCategory_FAILURE_CATEGORY_UNSPECIFIED, false},
		"limit": {concPlan(), AcquireConcurrencyResult{Answer: gateAnswer{Proto: 1, Answer: gateAnswerFull}}, RunConcurrencyLimitCode,
			orkstackv1.FailureCategory_FAILURE_CATEGORY_UNSPECIFIED, false},
		"fenced": {concPlan(), AcquireConcurrencyResult{Answer: gateAnswer{Proto: 1, Answer: gateAnswerFenced}}, RunConcurrencyFencedCode,
			orkstackv1.FailureCategory_FAILURE_CATEGORY_CONTRACT_VIOLATION, false},
		"unavailable": {concPlan(), AcquireConcurrencyResult{Unavailable: "deadline"}, RunConcurrencyUnavailableCode,
			orkstackv1.FailureCategory_FAILURE_CATEGORY_INFRASTRUCTURE_ERROR, false},
	} {
		t.Run(name, func(t *testing.T) {
			h := newConcRunHarness(t, tc.answer)
			h.runOrkflow(tc.plan, handlerOrkflow())
			wantStatus, wantEvent := orkstackv1.Status_STATUS_FAILED, orkstackv1.EventType_EVENT_TYPE_RUN_FAILED
			if tc.canceled {
				// The execution itself closes canceled, so a list read from
				// the cluster says what the run's own record says.
				wantStatus, wantEvent = orkstackv1.Status_STATUS_CANCELED, orkstackv1.EventType_EVENT_TYPE_RUN_CANCELED
				err := h.env.GetWorkflowError()
				if !temporal.IsCanceledError(err) {
					t.Fatalf("the run ended %v, want canceled", err)
				}
				// Its close names the supersede, for a parent reading it.
				if read := categoryOf(err); !read.superseded || read.supersededSentence != h.last.GetError().GetMessage() {
					t.Fatalf("the close reads superseded=%v %q, want true %q", read.superseded, read.supersededSentence, h.last.GetError().GetMessage())
				}
			} else {
				var app *temporal.ApplicationError
				if err := h.env.GetWorkflowError(); !errors.As(err, &app) || app.Type() != tc.code {
					t.Fatalf("the run ended %v, want %s", err, tc.code)
				}
				// It says it never started, for a schedule's fire to read.
				var detail ConcurrencyRunNotStarted
				if err := app.Details(&detail); err != nil || !detail.NotStarted {
					t.Fatalf("the run's ending carries %+v (%v), want it marked not started", detail, err)
				}
			}
			if got := h.last.GetStatus(); got != wantStatus {
				t.Fatalf("the run ended %s, want %s", got, wantStatus)
			}
			if h.ran != 0 {
				t.Fatalf("%d activities ran; a run that never held its slot starts none", h.ran)
			}
			// The run's own record names the reason, not RUN_FAILED, and no
			// longer says it waits in line.
			if got := h.last.GetError(); got.GetCode() != tc.code || strings.Contains(got.GetMessage(), "type: ") {
				t.Fatalf("the run's error is %s %q, want %s with its own message", got.GetCode(), got.GetMessage(), tc.code)
			}
			if got := h.last.GetError().GetCategory(); got != tc.category {
				t.Fatalf("the run's error category is %s, want %s", got, tc.category)
			}
			if h.last.GetQueued() != nil {
				t.Fatalf("the ended run still reads queued: %v", h.last.GetQueued())
			}
			last := h.events[len(h.events)-1]
			if last.GetType() != wantEvent {
				t.Fatalf("the last event is %s, want %s", last.GetType(), wantEvent)
			}
			if len(h.releases) != 1 {
				t.Fatalf("released %d times, want 1", len(h.releases))
			}
			said := map[string]string{}
			for _, e := range h.events {
				if e.GetType() == orkstackv1.EventType_EVENT_TYPE_TASK_LOG {
					said[e.GetTaskId()] = e.GetMessage()
				}
			}
			for _, tr := range h.last.GetTaskRuns() {
				if tr.GetStatus() != orkstackv1.Status_STATUS_CANCELED || tr.GetStartedAt() != nil ||
					said[tr.GetTaskId()] != "task canceled: "+runEndedBeforeStart {
					t.Fatalf("task %s ended %s (started %v) saying %q, want CANCELED, never started, %q",
						tr.GetTaskId(), tr.GetStatus(), tr.GetStartedAt(), said[tr.GetTaskId()], runEndedBeforeStart)
				}
			}
		})
	}

	// An operator's cancel while the run waits ends it canceled, with no
	// handler started.
	c := newConcRunHarness(t, queued(2, 1))
	c.env.RegisterDelayedCallback(c.env.CancelWorkflow, 10*time.Second)
	c.runOrkflow(concPlan(), handlerOrkflow())
	if err := c.env.GetWorkflowError(); !temporal.IsCanceledError(err) {
		t.Fatalf("the run ended %v, want canceled", err)
	}
	if c.ran != 0 || len(c.releases) != 1 || c.last.GetError().GetCode() != "RUN_CANCELED" || c.last.GetQueued() != nil {
		t.Fatalf("ran %d, released %d, error %s, queued %v; want 0, 1, RUN_CANCELED, none",
			c.ran, len(c.releases), c.last.GetError().GetCode(), c.last.GetQueued())
	}
}

// TestConcurrencyRun_AFirstRunAtANewKeySaysTheLimitIsReadingItsHolders: a
// run that arrives while its key's limit reads which runs hold it (a first
// run at a key, or a record lost) is told so, its place unknown, never
// "position 1 behind 0 running"; once the limit answers, its place is known.
func TestConcurrencyRun_AFirstRunAtANewKeySaysTheLimitIsReadingItsHolders(t *testing.T) {
	first := queued(2, 1)
	first.Answer.Rebuilding, first.Answer.HoldersAhead = true, 0
	h := newConcRunHarness(t, first, queued(2, 1))
	h.env.RegisterDelayedCallback(func() {
		h.env.SignalWorkflow(ConcurrencyGrantSignal, ConcurrencyGrant{Gate: "ork-conc-test", Run: concRunID, TaskIx: gateRunLevel, SeqBase: 1, Seq: 2})
	}, 3*time.Minute)
	h.run(concPlan())
	if err := h.env.GetWorkflowError(); err != nil {
		t.Fatalf("the run ended %v", err)
	}
	const note = "waiting while the limit on key deploy reads which runs hold it (a first run at a key, or after its record was lost)"
	var queuedEvent *orkstackv1.Event
	for _, e := range h.events {
		if e.GetType() == orkstackv1.EventType_EVENT_TYPE_RUN_QUEUED {
			queuedEvent = e
		}
	}
	if queuedEvent == nil || !strings.HasPrefix(queuedEvent.GetMessage(), note) || strings.Contains(queuedEvent.GetMessage(), "behind 0 running") {
		t.Fatalf("RUN_QUEUED %v, want it to say the limit is reading its holders", queuedEvent)
	}
}

// TestSetRebuilding_MarksThePlaceUnknownUntilTheLimitHasReadItsHolders is
// the run's record of the same: its place unknown with the note while the
// limit reads, and known again, the note gone, once it has.
func TestSetRebuilding_MarksThePlaceUnknownUntilTheLimitHasReadItsHolders(t *testing.T) {
	q := &orkstackv1.ConcurrencyQueued{Key: "deploy", Position: 1, PositionKnown: true}
	setRebuilding(q, true)
	if q.GetPositionKnown() || q.GetNoteCode() != QueuedNoteRebuilding ||
		q.GetNote() != "waiting while the limit on key deploy reads which runs hold it (a first run at a key, or after its record was lost)" {
		t.Fatalf("while the limit reads: %v", q)
	}
	q.Position = 2
	setRebuilding(q, false)
	if !q.GetPositionKnown() || q.GetNoteCode() != "" || q.GetNote() != "" || q.GetPosition() != 2 {
		t.Fatalf("once it has read: %v", q)
	}
	q.NoteCode, q.Note = "LINE_FULL", "full"
	setRebuilding(q, false)
	if q.GetNoteCode() != "LINE_FULL" {
		t.Fatalf("another note was cleared: %v", q)
	}
}

// waitEnds are the events of h that end a wait in line, each with its end.
func (h *concRunHarness) waitEnds() map[orkstackv1.EventType]*orkstackv1.ConcurrencyWaitEnd {
	out := map[orkstackv1.EventType]*orkstackv1.ConcurrencyWaitEnd{}
	for _, e := range h.events {
		if e.GetWaitEnd() != nil {
			out[e.GetType()] = e.GetWaitEnd()
		}
	}
	return out
}

// TestConcurrencyRun_TheEventThatEndsAWaitSaysHow: a queued run's
// RUN_STARTED carries how its wait ended (started) and how long it took; a
// run that times out in line carries timed_out on its terminal event, and
// one that is canceled, canceled; a run granted at once carries none.
func TestConcurrencyRun_TheEventThatEndsAWaitSaysHow(t *testing.T) {
	label := CensusLimitLabel(concPlan().Concurrency.Slots[0].Label)
	h := newConcRunHarness(t, queued(2, 1))
	h.env.RegisterDelayedCallback(func() {
		h.env.SignalWorkflow(ConcurrencyGrantSignal, ConcurrencyGrant{Gate: "ork-conc-test", Run: concRunID, TaskIx: gateRunLevel, SeqBase: 1, Seq: 2})
	}, 30*time.Second)
	h.run(concPlan())
	ends := h.waitEnds()
	started := ends[orkstackv1.EventType_EVENT_TYPE_RUN_STARTED]
	if len(ends) != 1 || started.GetOutcome() != orkstackv1.ConcurrencyWaitOutcome_CONCURRENCY_WAIT_OUTCOME_STARTED ||
		started.GetWaited().AsDuration() < 30*time.Second || started.GetLimitLabel() != label || started.GetTaskLevel() ||
		started.GetStrategy() != orkstackv1.ConcurrencyStrategy_CONCURRENCY_STRATEGY_QUEUE {
		t.Fatalf("wait ends %v; want one, on RUN_STARTED: started after 30s, labelled %s", ends, label)
	}

	plan := concPlan()
	plan.Concurrency.Slots[0].QueueTimeoutSeconds = 30
	timeout := newConcRunHarness(t, queued(2, 1))
	timeout.run(plan)
	if end := timeout.waitEnds()[orkstackv1.EventType_EVENT_TYPE_RUN_FAILED]; end.GetOutcome() != orkstackv1.ConcurrencyWaitOutcome_CONCURRENCY_WAIT_OUTCOME_TIMED_OUT {
		t.Fatalf("a run timed out in line: wait ends %v; want timed_out on RUN_FAILED", timeout.waitEnds())
	}

	canceled := newConcRunHarness(t, queued(2, 1))
	canceled.env.RegisterDelayedCallback(canceled.env.CancelWorkflow, 10*time.Second)
	canceled.run(concPlan())
	if end := canceled.waitEnds()[orkstackv1.EventType_EVENT_TYPE_RUN_CANCELED]; end.GetOutcome() != orkstackv1.ConcurrencyWaitOutcome_CONCURRENCY_WAIT_OUTCOME_CANCELED {
		t.Fatalf("a run canceled in line: wait ends %v; want canceled on RUN_CANCELED", canceled.waitEnds())
	}

	granted := newConcRunHarness(t, AcquireConcurrencyResult{Answer: gateAnswer{Proto: 1, Answer: gateAnswerGranted, Seq: 1}})
	granted.run(concPlan())
	if ends := granted.waitEnds(); len(ends) != 0 {
		t.Fatalf("a run granted at once carries wait ends %v; want none", ends)
	}
}
