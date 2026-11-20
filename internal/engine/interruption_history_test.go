package engine_test

import (
	"fmt"
	"sync"
	"testing"
	"time"

	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/sdk/interceptor"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/engine"
)

// The history cost of interruptions, counted in the workflow test
// environment. The environment records no history, so the counter is a
// workflow interceptor that counts the commands the interpreter issues and
// prices each at the events Temporal records for it: an activity is scheduled,
// started and closed (3), a local activity is one marker (1), a timer starts
// and fires or is cancelled (2). Every activity completion and timer firing is
// then taken to be a workflow task of its own (scheduled, started, completed:
// 3), which is the most a sequential run can have, so the figure is an upper
// bound on a real cluster's and a real cluster's own count is the integration
// tier's.

const (
	eventsPerActivity   = 3
	eventsPerLocal      = 1
	eventsPerTimer      = 2
	eventsPerWorkflowTk = 3
)

// eventMeter counts commands and prices them in history events.
type eventMeter struct {
	interceptor.WorkerInterceptorBase
	mu                         sync.Mutex
	activities, locals, timers int64
}

func (m *eventMeter) InterceptWorkflow(_ workflow.Context, next interceptor.WorkflowInboundInterceptor) interceptor.WorkflowInboundInterceptor {
	return &meterInbound{WorkflowInboundInterceptorBase: interceptor.WorkflowInboundInterceptorBase{Next: next}, root: m}
}

type meterInbound struct {
	interceptor.WorkflowInboundInterceptorBase
	root *eventMeter
}

func (i *meterInbound) Init(outbound interceptor.WorkflowOutboundInterceptor) error {
	return i.Next.Init(&meterOutbound{WorkflowOutboundInterceptorBase: interceptor.WorkflowOutboundInterceptorBase{Next: outbound}, root: i.root})
}

type meterOutbound struct {
	interceptor.WorkflowOutboundInterceptorBase
	root *eventMeter
}

func (o *meterOutbound) ExecuteActivity(ctx workflow.Context, activityType string, args ...any) workflow.Future {
	o.root.mu.Lock()
	o.root.activities++
	o.root.mu.Unlock()
	return o.Next.ExecuteActivity(ctx, activityType, args...)
}

func (o *meterOutbound) ExecuteLocalActivity(ctx workflow.Context, activityType string, args ...any) workflow.Future {
	o.root.mu.Lock()
	o.root.locals++
	o.root.mu.Unlock()
	return o.Next.ExecuteLocalActivity(ctx, activityType, args...)
}

func (o *meterOutbound) NewTimer(ctx workflow.Context, d time.Duration) workflow.Future {
	o.root.mu.Lock()
	o.root.timers++
	o.root.mu.Unlock()
	return o.Next.NewTimer(ctx, d)
}

func (o *meterOutbound) Sleep(ctx workflow.Context, d time.Duration) error {
	o.root.mu.Lock()
	o.root.timers++
	o.root.mu.Unlock()
	return o.Next.Sleep(ctx, d)
}

// events is the history the commands counted so far add to a run.
func (m *eventMeter) events() int64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.activities*(eventsPerActivity+eventsPerWorkflowTk) +
		m.locals*eventsPerLocal +
		m.timers*(eventsPerTimer+eventsPerWorkflowTk)
}

// heartbeatLossError is the cluster's heartbeat timeout whose last beat was
// beat, as the activity's failure reaches the workflow.
func heartbeatLossError(beat map[string]any) error {
	return temporal.NewTimeoutError(enumspb.TIMEOUT_TYPE_HEARTBEAT, nil, beat)
}

// lossWhileStoppingError is a heartbeat timeout whose last beat says the Orker
// had begun to stop and the container had started (E92-N2).
func lossWhileStoppingError() error {
	return heartbeatLossError(map[string]any{
		"phase": "run", "orker_id": "orker-a", "started": true, "attempt": 1,
		"stopping_since": time.Date(2026, 11, 16, 9, 0, 0, 0, time.UTC).Format(time.RFC3339Nano),
	})
}

// plainLossError is a heartbeat timeout with no stop behind it.
func plainLossError() error {
	return heartbeatLossError(map[string]any{"phase": "run", "orker_id": "orker-a", "started": true, "attempt": 1})
}

// repeated is n copies of the error mk returns.
func repeated(n int, mk func() error) []error {
	out := make([]error, n)
	for i := range out {
		out[i] = mk()
	}
	return out
}

// meteredRun runs the one-task document with its task scripted to end in
// steps, then succeed, and returns the run's harness and the events the
// interpreter's commands added to its history. When onStep is set it is
// called with the counted total at every snapshot, in the snapshot write's own
// goroutine, as the cluster's history length moves at every workflow task.
func meteredRun(t *testing.T, plan engine.RunPlan, steps []error, onSnapshot func(h *harness, total int64)) (*harness, *eventMeter, error) {
	t.Helper()
	meter := &eventMeter{}
	b := newScript(map[string][]error{"work": steps})
	h := newHarnessCtx(t, &testsuite.WorkflowTestSuite{}, b.run)
	h.env.SetWorkerOptions(testWorkerOptions(worker.Options{Interceptors: []interceptor.WorkerInterceptor{meter}}))
	if onSnapshot != nil {
		h.onSnapshot = func(*orkstackv1.Run, int64) error {
			onSnapshot(h, meter.events())
			return nil
		}
	}
	h.env.ExecuteWorkflow(engine.RunWorkflow, "run-interrupted", compile(t, fmt.Sprintf(oneTaskDocument, "")),
		payload(t, nil), plan, (*orkstackv1.RunContinuation)(nil))
	return h, meter, h.env.GetWorkflowError()
}

// TestMeasure_PerInterruption is P0-7's figure for the workflow half: the
// history events one interruption of each class costs a run, counted in the
// workflow test environment as the difference between a run whose task is
// interrupted n times and one that is not, per interruption, and logged so a
// proof's output records it. The plan pins no reserve of its own for an
// interruption: the guard sees what one consumes in the history it counts, and
// charges each dispatch and each Timer to the failure path it keeps back
// (chargeReserve: PerInFlight, PerAwait). So each class is held to what those
// terms cover for one interruption: the attempt it ended and the dispatch that
// replaces it, each at most one in-flight term, 2·PerInFlight, plus, for a
// class that waits, its Timer's await term, PerAwait. A regression that makes
// an interruption record more than that (a second notice, a snapshot per
// step) fails it here, before the guard's accounting is what finds it.
func TestMeasure_PerInterruption(t *testing.T) {
	reserve := engine.DefaultHistoryReserve
	_, base, err := meteredRun(t, interruptionPlan(nil), nil, nil)
	if err != nil {
		t.Fatalf("baseline run: %v", err)
	}
	baseEvents := base.events()
	t.Logf("a run of one task that is never interrupted: %d events (workflow-task upper bound included)", baseEvents)

	for _, tc := range []struct {
		class    string
		n        int
		steps    func() error
		timer    bool // the class waits on a Timer before its next dispatch
		wantKind func(*orkstackv1.TaskInterruptions) bool
	}{
		{"hand-back (started, no wait)", 5, handBackError, false,
			func(s *orkstackv1.TaskInterruptions) bool { return s.GetHandedBack() == 5 }},
		{"loss (heartbeat timeout, loss wait)", 3, plainLossError, true,
			func(s *orkstackv1.TaskInterruptions) bool { return s.GetLost() == 3 }},
		{"loss while stopping (charged as a hand-back, loss wait kept)", 5, lossWhileStoppingError, true,
			func(s *orkstackv1.TaskInterruptions) bool {
				return s.GetHandedBack() == 5 && s.GetLostWhileStopping() == 5 && s.GetLost() == 0
			}},
		{"never-started stop (bounce, no wait)", 5, stopBounceError, false,
			func(s *orkstackv1.TaskInterruptions) bool { return s.GetNeverStarted() == 5 }},
		{"never-started fault (first at once, then a back-off Timer)", 4, storeFaultError, true,
			func(s *orkstackv1.TaskInterruptions) bool { return s.GetStoreFaults() == 4 }},
	} {
		t.Run(tc.class, func(t *testing.T) {
			h, meter, err := meteredRun(t, interruptionPlan(nil), repeated(tc.n, tc.steps), nil)
			if err != nil {
				t.Fatalf("workflow error: %v", err)
			}
			row := interruptedRow(t, h, "work")
			if row.GetStatus() != orkstackv1.Status_STATUS_SUCCEEDED || !tc.wantKind(row.GetInterruptions()) {
				t.Fatalf("work = %v with %v, want SUCCEEDED having spent exactly the class's own counter %d times",
					row.GetStatus(), row.GetInterruptions(), tc.n)
			}
			total := meter.events() - baseEvents
			per := (total + int64(tc.n) - 1) / int64(tc.n) // rounded up
			bound := 2 * reserve.PerInFlight
			if tc.timer {
				bound += reserve.PerAwait
			}
			t.Logf("%s: %d events for %d interruptions, %d each (rounded up), against the %d its reserve terms cover (2·PerInFlight %d%s)",
				tc.class, total, tc.n, per, bound, 2*reserve.PerInFlight, map[bool]string{true: fmt.Sprintf(" + PerAwait %d", reserve.PerAwait)}[tc.timer])
			if per <= 0 {
				t.Errorf("%s cost %d events: an interruption that adds no history was not counted", tc.class, per)
			}
			if per > bound {
				t.Errorf("%s costs %d events each, above the %d its reserve terms cover", tc.class, per, bound)
			}
		})
	}
}

// guardSample is one history guard check: the history as the guard counts it
// and the reserve it kept back for the run's failure path.
type guardSample struct{ events, reserve int64 }

// lossesWhileStopping runs the one-task document, whose task is lost n times
// while its Orker was stopping and then succeeds, under a history limit of
// maxEvents. The environment's history length follows the counted commands
// at every snapshot, as a cluster's moves at every workflow task, and every
// guard check is sampled.
func lossesWhileStopping(t *testing.T, n int, maxEvents int64) (h *harness, meter *eventMeter, calls int, trace []guardSample, err error) {
	t.Helper()
	var mu sync.Mutex
	restore := engine.SetHistoryGuardProbeForTest(func(_ string, events, reserve int64) {
		mu.Lock()
		defer mu.Unlock()
		trace = append(trace, guardSample{events, reserve})
	})
	t.Cleanup(restore)
	plan := guardedPlan(maxEvents, engine.DefaultHistoryMaxBytes)
	budgets := engine.DefaultInterruptionBudgets
	plan.Interruptions = &budgets
	meter = &eventMeter{}
	b := newScript(map[string][]error{"work": repeated(n, lossWhileStoppingError)})
	h = newHarnessCtx(t, &testsuite.WorkflowTestSuite{}, b.run)
	h.env.SetWorkerOptions(testWorkerOptions(worker.Options{Interceptors: []interceptor.WorkerInterceptor{meter}}))
	// The history a run starts with is a handful of events; every counted
	// command after it is added as it is issued.
	const started = 4
	h.onSnapshot = func(*orkstackv1.Run, int64) error {
		h.env.SetCurrentHistoryLength(int(started + meter.events()))
		return nil
	}
	h.env.ExecuteWorkflow(engine.RunWorkflow, "run-interrupted", compile(t, fmt.Sprintf(oneTaskDocument, "")),
		payload(t, nil), plan, (*orkstackv1.RunContinuation)(nil))
	err = h.env.GetWorkflowError()
	mu.Lock()
	defer mu.Unlock()
	return h, meter, b.count("work"), append([]guardSample(nil), trace...), err
}

// TestHistoryGuard_TenLossesWhileStoppingOnOneTaskStayWithinTheBound is R11-4:
// a loss whose last beat says its Orker was stopping keeps its loss-wait Timer
// but spends the hand-back budget (E92-N2), so the reserve charged for it is
// the loss's, Timer included (PerAwait), for every one of the hand-back
// budget's ten. Ten such losses of one task must (a) each be reserved: the
// guard keeps back Fixed + PerPending + PerAwait while the task waits on its
// Timer, at each of the ten waits; (b) never accumulate: the reserve at the
// tenth equals the first, so it is bounded by one task's, never by how many
// losses came before; and (c) stop the run at its line, explicitly
// RUN_HISTORY_LIMIT and with the history the run would then hold, failure path
// included, within the limit, at exactly the loss where the Timer's reserve
// brings it to the line: a guard that left the Timer out would run on past it.
func TestHistoryGuard_TenLossesWhileStoppingOnOneTaskStayWithinTheBound(t *testing.T) {
	const losses = 10
	r := engine.DefaultHistoryReserve
	waiting := r.Fixed + r.PerPending + r.PerAwait // the one task PENDING, its Timer open

	t.Run("each loss is reserved and none accumulates", func(t *testing.T) {
		h, _, calls, trace, err := lossesWhileStopping(t, losses, engine.DefaultHistoryMaxEvents)
		if err != nil {
			t.Fatalf("workflow error: %v", err)
		}
		if calls != losses+1 {
			t.Fatalf("work ran %d times, want %d: ten losses, then its success", calls, losses+1)
		}
		st := interruptedRow(t, h, "work").GetInterruptions()
		if st.GetHandedBack() != losses || st.GetLostWhileStopping() != losses || st.GetLost() != 0 || st.GetOwnAttemptsUsed() != 0 {
			t.Fatalf("interruptions = %v, want ten hand-backs by losses while stopping, no loss and no own attempt", st)
		}
		var maxReserve int64
		var atWait []int64
		for _, s := range trace {
			maxReserve = max(maxReserve, s.reserve)
			if s.reserve == waiting {
				atWait = append(atWait, s.events)
			}
		}
		if maxReserve != waiting {
			t.Errorf("the largest reserve kept back was %d, want %d (Fixed %d + PerPending %d + PerAwait %d), however many losses came before",
				maxReserve, waiting, r.Fixed, r.PerPending, r.PerAwait)
		}
		// Each wait is seen at one history length, once per loss.
		lengths := map[int64]bool{}
		for _, e := range atWait {
			lengths[e] = true
		}
		if len(lengths) != losses {
			t.Errorf("the Timer's reserve was kept at %d distinct history lengths %v, want one per loss (%d)", len(lengths), atWait, losses)
		}
		for _, s := range trace {
			if s.events+s.reserve >= engine.DefaultHistoryMaxEvents {
				t.Errorf("history %d plus reserve %d reaches the limit %d", s.events, s.reserve, engine.DefaultHistoryMaxEvents)
			}
		}
	})

	t.Run("the line stops the run at the loss that reaches it", func(t *testing.T) {
		const stopAt = 5
		// Where the fifth wait stands, read from a run that has room, and the
		// line set so that history plus the Timer's reserve reaches it there.
		_, _, _, trace, err := lossesWhileStopping(t, losses, engine.DefaultHistoryMaxEvents)
		if err != nil {
			t.Fatalf("the run with room: %v", err)
		}
		var seen []int64
		for _, s := range trace {
			if s.reserve == waiting && (len(seen) == 0 || seen[len(seen)-1] != s.events) {
				seen = append(seen, s.events)
			}
		}
		if len(seen) < stopAt {
			t.Fatalf("saw %d waits, want at least %d", len(seen), stopAt)
		}
		line := seen[stopAt-1] + waiting

		h, meter, calls, _, _ := lossesWhileStopping(t, losses, line)
		requireHistoryLimitError(t, h.env)
		if calls != stopAt {
			t.Errorf("work ran %d times before the guard stopped the run, want %d: the loss whose Timer reserve brings it to the line (%d)", calls, stopAt, line)
		}
		if held := 4 + meter.events(); held > line {
			t.Errorf("the run ended with %d events counted, past the line of %d: the failure path overran its reserve", held, line)
		}
		if row := interruptedRow(t, h, "work"); row.GetStatus() != orkstackv1.Status_STATUS_FAILED || row.GetError().GetCode() != "ORKER_STOPPED" {
			t.Errorf("work = %v %q, want FAILED ORKER_STOPPED, not run again", row.GetStatus(), row.GetError().GetCode())
		}
	})
}
