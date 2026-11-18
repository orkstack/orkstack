package engine

import (
	"fmt"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/interceptor"
	"go.temporal.io/sdk/log"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
)

// deadlockCPUBound is the CPU time one stretch of workflow code may take in a
// test that proves a path within the deadlock detector's budget (AGENTS.md
// §11: the ceiling runs replay "within the deadlock detector's budget"). The
// detector is a 1 s wall clock, which a loaded host moves: a stretch that
// takes 150 ms ran over 1 s while make check's suites ran beside it, and the
// load, not the code, failed the test. So the claim is held on the stretch's
// own CPU time, which a host's load cannot move, at half the detector: the
// other half is room for a loaded host's slower cores (passClock measured a
// quarter more instructions per pass beside 32 CPU burners) and for a
// stretch measured past its yields (deadlockBudget). A race build's
// instrumentation slows workflow code several times over, so there it is
// scaled by raceSlowdown, as the detector is (productionDeadlock).
func deadlockCPUBound() time.Duration {
	if raceBuild {
		return 500 * time.Millisecond * raceSlowdown
	}
	return 500 * time.Millisecond
}

// deadlockBudget holds every workflow coroutine of a test to deadlockCPUBound
// per stretch of workflow code — what the SDK's deadlock detector times,
// from a coroutine's resumption to its next yield — measured on the
// coroutine's own thread clock (threadCPU) in place of the detector's wall
// clock.
//
// A test cannot see the yields themselves: Selector.Select and Future.Get
// are not intercepted, and a wrapped Future cannot be handed to a Selector.
// It sees points around them: a coroutine's start and end, every outbound
// call the workflow makes (commands, versions, the clock, the logger), and
// the top and the Selector of every main-loop pass (readyLoopProbe). Each
// coroutine is locked to its thread when it starts, so the thread's clock is
// the coroutine's alone, read at every point.
//
// Which points a yield lies between is known from the futures the coroutine's
// commands returned: the budget keeps each one still pending, and a future
// that was pending at one point and is ready at the next was made ready by
// the dispatcher, which runs only while every coroutine is blocked — so this
// coroutine yielded between the two. The return of workflow.Sleep is a yield
// too. There, and only there, the stretch is closed at the later point and the
// next one opened at the earlier, so both are measured past the yield. A
// yield the budget cannot place (one no pending future saw end: a signal, a
// future made with workflow.NewFuture) is measured as part of one stretch.
// Nothing else ends a stretch: not the scheduler's preemption, not the
// collector, not a lock. So a stretch measured here is never less than the
// detector's that it covers, and a bound held here holds there
// (TestDeadlockBudget_ControlsItsOwnMeasure).
//
// Where the platform has no thread clock, the budget falls back to the
// production detector's wall clock.
type deadlockBudget struct {
	cpu bool
	// bound is deadlockCPUBound, or a control's own.
	bound time.Duration

	mu      sync.Mutex
	threads map[int]*budgetThread
	// worst is the longest stretch, and the point that closed it.
	worst      time.Duration
	worstAt    string
	stretches  int
	unreadable int
	// passes is how many main-loop passes the test's runs made.
	passes int
}

// budgetThread is one coroutine's thread: where its open stretch started on
// its clock, its clock at the last point, and the futures its commands
// returned that were still pending then.
type budgetThread struct {
	start, last time.Duration
	pending     []workflow.Future
}

// newDeadlockBudget is a budget over every run of the test.
func newDeadlockBudget(t testing.TB) *deadlockBudget {
	t.Helper()
	_, cpu := threadCPU()
	b := &deadlockBudget{cpu: cpu, bound: deadlockCPUBound(), threads: map[int]*budgetThread{}}
	if !b.cpu {
		return b
	}
	onProbe(t, func(p *readyLoopProbe) *func() { return &p.pass }, func() {
		b.mu.Lock()
		b.passes++
		b.mu.Unlock()
		b.point("the top of a main-loop pass", false)
	})
	onProbe(t, func(p *readyLoopProbe) *func() { return &p.selector }, func() { b.point("a main-loop pass's Selector", false) })
	return b
}

// options is options with the budget: a generous detector and the budget's
// interceptor first, or the production detector where there is no thread
// clock.
func (b *deadlockBudget) options(options worker.Options) worker.Options {
	if !b.cpu {
		options.DeadlockDetectionTimeout = productionDeadlock()
		return options
	}
	options.DeadlockDetectionTimeout = testDeadlockDetectionTimeout
	options.Interceptors = append([]interceptor.WorkerInterceptor{&budgetInterceptor{budget: b}}, options.Interceptors...)
	return options
}

// check fails t when a stretch took more than deadlockCPUBound, and says the
// longest.
func (b *deadlockBudget) check(t testing.TB) {
	t.Helper()
	if !b.cpu {
		return
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.stretches == 0 || b.unreadable > 0 {
		t.Fatalf("the deadlock budget measured %d stretches of workflow code, %d points unreadable", b.stretches, b.unreadable)
		return
	}
	if b.worst > b.bound {
		t.Fatalf("a stretch of workflow code closed at %s took %s of thread CPU time, over the %s budget",
			b.worstAt, b.worst, b.bound)
		return
	}
	t.Logf("longest of %d stretches of workflow code (%d main-loop passes): %s of thread CPU time, closed at %s (budget %s)",
		b.stretches, b.passes, b.worst, b.worstAt, b.bound)
}

// begin locks the calling coroutine to its thread and opens its first
// stretch.
func (b *deadlockBudget) begin() {
	runtime.LockOSThread()
	cpu, ok := threadCPU()
	b.mu.Lock()
	defer b.mu.Unlock()
	if !ok {
		b.unreadable++
		return
	}
	b.threads[threadID()] = &budgetThread{start: cpu, last: cpu}
}

// point is a point the calling coroutine reached, named where; yielded says
// the coroutine certainly yielded since its last point. A thread the budget
// did not begin (a goroutine outside workflow code) is not measured.
func (b *deadlockBudget) point(where string, yielded bool) {
	cpu, ok := threadCPU()
	b.mu.Lock()
	defer b.mu.Unlock()
	th := b.threads[threadID()]
	if th == nil {
		return
	}
	if !ok {
		b.unreadable++
		return
	}
	still := th.pending[:0]
	for _, f := range th.pending {
		if f.IsReady() {
			yielded = true
			continue
		}
		still = append(still, f)
	}
	clear(th.pending[len(still):])
	th.pending = still
	if yielded {
		// It yielded since the last point: the stretch ends here at the
		// latest, and the next began there at the earliest.
		b.close(cpu-th.start, where)
		th.start = th.last
	}
	th.last = cpu
}

// pending keeps f, a future the calling coroutine's command returned, while
// it is pending.
func (b *deadlockBudget) pending(f workflow.Future) {
	if f == nil || f.IsReady() {
		return
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if th := b.threads[threadID()]; th != nil {
		th.pending = append(th.pending, f)
	}
}

// end closes the calling coroutine's last stretch. Its thread stays locked:
// the goroutine is ending, and the runtime ends a thread its goroutine left
// locked.
func (b *deadlockBudget) end(where string) {
	b.point(where, false)
	cpu, _ := threadCPU()
	b.mu.Lock()
	defer b.mu.Unlock()
	if th := b.threads[threadID()]; th != nil {
		b.close(cpu-th.start, where)
		delete(b.threads, threadID())
	}
}

// close records a stretch of d, closed at where. b.mu is held.
func (b *deadlockBudget) close(d time.Duration, where string) {
	b.stretches++
	if d > b.worst {
		b.worst, b.worstAt = d, where
	}
}

// onProbe sets one of readyLoopProbeForTest's hooks, named by field, to call
// f after the hook a test already set there, until t ends.
func onProbe(t testing.TB, field func(*readyLoopProbe) *func(), f func()) {
	t.Helper()
	probe := readyLoopProbeForTest
	if probe == nil {
		probe = &readyLoopProbe{}
		readyLoopProbeForTest = probe
		t.Cleanup(func() { readyLoopProbeForTest = nil })
	}
	hook := field(probe)
	previous := *hook
	*hook = func() {
		if previous != nil {
			previous()
		}
		f()
	}
	t.Cleanup(func() { *hook = previous })
}

// passGuard fails a run whose main loop takes more than limit passes over
// the test: the form of "the loop never spins" that a host's load cannot
// decide. A loop that spins comes back to its top without waiting, pass
// after pass, so it reaches any limit at once; a run that waits for its work
// passes a few times per event. It panics in the pass that crosses the limit,
// which ends the workflow task with that panic as the run's error.
func passGuard(t testing.TB, limit int) {
	t.Helper()
	var mu sync.Mutex
	passes := 0
	onProbe(t, func(p *readyLoopProbe) *func() { return &p.pass }, func() {
		mu.Lock()
		passes++
		over := passes > limit
		mu.Unlock()
		if over {
			panic(fmt.Sprintf("the main loop passed more than %d times: it spins", limit))
		}
	})
}

// budgetInterceptor makes every workflow coroutine's start, outbound calls
// and end the budget's points.
type budgetInterceptor struct {
	interceptor.WorkerInterceptorBase
	budget *deadlockBudget
}

func (i *budgetInterceptor) InterceptWorkflow(_ workflow.Context, next interceptor.WorkflowInboundInterceptor) interceptor.WorkflowInboundInterceptor {
	return &budgetInbound{WorkflowInboundInterceptorBase: interceptor.WorkflowInboundInterceptorBase{Next: next}, budget: i.budget}
}

type budgetInbound struct {
	interceptor.WorkflowInboundInterceptorBase
	budget *deadlockBudget
}

func (w *budgetInbound) Init(outbound interceptor.WorkflowOutboundInterceptor) error {
	return w.Next.Init(&budgetOutbound{WorkflowOutboundInterceptorBase: interceptor.WorkflowOutboundInterceptorBase{Next: outbound}, budget: w.budget})
}

func (w *budgetInbound) ExecuteWorkflow(ctx workflow.Context, in *interceptor.ExecuteWorkflowInput) (any, error) {
	w.budget.begin()
	defer w.budget.end("the end of a workflow")
	return w.Next.ExecuteWorkflow(ctx, in)
}

type budgetOutbound struct {
	interceptor.WorkflowOutboundInterceptorBase
	budget *deadlockBudget
}

func (w *budgetOutbound) Go(ctx workflow.Context, name string, f func(ctx workflow.Context)) workflow.Context {
	w.budget.point("workflow.Go", false)
	return w.Next.Go(ctx, name, func(ctx workflow.Context) {
		w.budget.begin()
		defer w.budget.end("the end of a coroutine")
		f(ctx)
	})
}

func (w *budgetOutbound) ExecuteActivity(ctx workflow.Context, activityType string, args ...any) workflow.Future {
	w.budget.point("ExecuteActivity", false)
	f := w.Next.ExecuteActivity(ctx, activityType, args...)
	w.budget.pending(f)
	return f
}

func (w *budgetOutbound) ExecuteLocalActivity(ctx workflow.Context, activityType string, args ...any) workflow.Future {
	w.budget.point("ExecuteLocalActivity", false)
	f := w.Next.ExecuteLocalActivity(ctx, activityType, args...)
	w.budget.pending(f)
	return f
}

func (w *budgetOutbound) ExecuteChildWorkflow(ctx workflow.Context, childWorkflowType string, args ...any) workflow.ChildWorkflowFuture {
	w.budget.point("ExecuteChildWorkflow", false)
	f := w.Next.ExecuteChildWorkflow(ctx, childWorkflowType, args...)
	w.budget.pending(f)
	return f
}

func (w *budgetOutbound) NewTimer(ctx workflow.Context, d time.Duration) workflow.Future {
	w.budget.point("NewTimer", false)
	f := w.Next.NewTimer(ctx, d)
	w.budget.pending(f)
	return f
}

func (w *budgetOutbound) NewTimerWithOptions(ctx workflow.Context, d time.Duration, options workflow.TimerOptions) workflow.Future {
	w.budget.point("NewTimer", false)
	f := w.Next.NewTimerWithOptions(ctx, d, options)
	w.budget.pending(f)
	return f
}

func (w *budgetOutbound) Sleep(ctx workflow.Context, d time.Duration) error {
	w.budget.point("Sleep", false)
	err := w.Next.Sleep(ctx, d)
	// A sleep of any length waits for its timer: a yield.
	w.budget.point("Sleep's return", d > 0)
	return err
}

func (w *budgetOutbound) Await(ctx workflow.Context, condition func() bool) error {
	w.budget.point("Await", false)
	defer w.budget.point("Await's return", false)
	return w.Next.Await(ctx, condition)
}

func (w *budgetOutbound) AwaitWithTimeout(ctx workflow.Context, timeout time.Duration, condition func() bool) (bool, error) {
	w.budget.point("Await", false)
	defer w.budget.point("Await's return", false)
	return w.Next.AwaitWithTimeout(ctx, timeout, condition)
}

func (w *budgetOutbound) SignalExternalWorkflow(ctx workflow.Context, workflowID, runID, signalName string, arg any) workflow.Future {
	w.budget.point("SignalExternalWorkflow", false)
	f := w.Next.SignalExternalWorkflow(ctx, workflowID, runID, signalName, arg)
	w.budget.pending(f)
	return f
}

func (w *budgetOutbound) RequestCancelExternalWorkflow(ctx workflow.Context, workflowID, runID string) workflow.Future {
	w.budget.point("RequestCancelExternalWorkflow", false)
	f := w.Next.RequestCancelExternalWorkflow(ctx, workflowID, runID)
	w.budget.pending(f)
	return f
}

func (w *budgetOutbound) SideEffect(ctx workflow.Context, f func(ctx workflow.Context) any) converter.EncodedValue {
	w.budget.point("SideEffect", false)
	return w.Next.SideEffect(ctx, f)
}

func (w *budgetOutbound) GetVersion(ctx workflow.Context, changeID string, minSupported, maxSupported workflow.Version) workflow.Version {
	w.budget.point("GetVersion", false)
	return w.Next.GetVersion(ctx, changeID, minSupported, maxSupported)
}

func (w *budgetOutbound) Now(ctx workflow.Context) time.Time {
	w.budget.point("workflow.Now", false)
	return w.Next.Now(ctx)
}

func (w *budgetOutbound) GetInfo(ctx workflow.Context) *workflow.Info {
	w.budget.point("workflow.GetInfo", false)
	return w.Next.GetInfo(ctx)
}

func (w *budgetOutbound) GetLogger(ctx workflow.Context) log.Logger {
	w.budget.point("workflow.GetLogger", false)
	return w.Next.GetLogger(ctx)
}

// verdictTB records a check's verdict instead of failing the test.
type verdictTB struct {
	testing.TB
	failed  bool
	message string
}

func (v *verdictTB) Fatalf(format string, args ...any) {
	v.failed, v.message = true, fmt.Sprintf(format, args...)
}

// burn spends d of the calling thread's CPU time.
func burn(d time.Duration) {
	start, _ := threadCPU()
	for {
		if now, _ := threadCPU(); now-start >= d {
			return
		}
	}
}

// TestDeadlockBudget_ControlsItsOwnMeasure: the budget fails a stretch of
// workflow code over its bound, even one full of points where nothing
// yields (the scheduler preempts such a stretch, and the collector parks it,
// and neither ends it); splits a stretch at a yield it can place — a timer's
// future that became ready, a sleep's return — so two stretches each under
// the bound pass; and measures a stretch whose yield it cannot place (a
// signal) as one, never as less: the property every budgeted test leans on.
// The bound is the control's own, a fifth of the real one, so the controls
// stay short.
func TestDeadlockBudget_ControlsItsOwnMeasure(t *testing.T) {
	if _, ok := threadCPU(); !ok {
		t.Skip("no thread clock here: the budget is the production detector's wall clock")
	}
	const bound = 100 * time.Millisecond
	for _, tc := range []struct {
		name     string
		workflow func(workflow.Context) error
		fails    bool
	}{
		{"one stretch over the bound", func(workflow.Context) error {
			burn(bound + bound/2)
			return nil
		}, true},
		{"one stretch over the bound, a point every millisecond", func(ctx workflow.Context) error {
			for i := 0; i < 150; i++ {
				burn(time.Millisecond)
				_ = workflow.Now(ctx)
			}
			return nil
		}, true},
		{"two stretches under it, split by a timer that fired", func(ctx workflow.Context) error {
			// The points either side of the wait are where the stretches are
			// split; without them, both burns would count on both sides.
			timer := workflow.NewTimer(ctx, time.Second)
			burn(bound * 3 / 4)
			_ = workflow.Now(ctx)
			if err := timer.Get(ctx, nil); err != nil {
				return err
			}
			_ = workflow.Now(ctx)
			burn(bound * 3 / 4)
			return nil
		}, false},
		{"two stretches under it, split by a sleep", func(ctx workflow.Context) error {
			burn(bound * 3 / 4)
			if err := workflow.Sleep(ctx, time.Second); err != nil {
				return err
			}
			burn(bound * 3 / 4)
			return nil
		}, false},
		{"two stretches under it, split by a signal the budget cannot place", func(ctx workflow.Context) error {
			burn(bound * 3 / 4)
			workflow.GetSignalChannel(ctx, "go").Receive(ctx, nil)
			burn(bound * 3 / 4)
			return nil
		}, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var suite testsuite.WorkflowTestSuite
			env := suite.NewTestWorkflowEnvironment()
			budget := newDeadlockBudget(t)
			budget.bound = bound
			env.SetWorkerOptions(budget.options(worker.Options{}))
			env.RegisterWorkflowWithOptions(tc.workflow, workflow.RegisterOptions{Name: "control"})
			env.RegisterDelayedCallback(func() { env.SignalWorkflow("go", nil) }, time.Second)
			env.ExecuteWorkflow("control")
			if err := env.GetWorkflowError(); err != nil {
				t.Fatalf("the control failed: %v", err)
			}
			verdict := &verdictTB{TB: t}
			budget.check(verdict)
			if verdict.failed != tc.fails {
				t.Fatalf("the budget failed the control: %v (%s), want %v", verdict.failed, verdict.message, tc.fails)
			}
		})
	}
}

// TestPassGuard_EndsARunPastItsLimit: a run whose main loop passes more often
// than the guard allows ends with the guard's panic, and one within it
// completes.
func TestPassGuard_EndsARunPastItsLimit(t *testing.T) {
	for _, tc := range []struct {
		limit int
		ends  bool
	}{{2, true}, {1000, false}} {
		t.Run(fmt.Sprintf("limit %d", tc.limit), func(t *testing.T) {
			passGuard(t, tc.limit)
			h := newRefusalHarnessOver(t, newMemSnapshotStore())
			plan := refusalRoutedPlan(t)
			h.env.ExecuteWorkflow(RunWorkflowType, refusalWorkflowID, largeDefinition(t, 3, 64), (*orkstackv1.Payload)(nil),
				plan, (*orkstackv1.RunContinuation)(nil))
			err := h.env.GetWorkflowError()
			if spun := err != nil && strings.Contains(err.Error(), "it spins"); spun != tc.ends {
				t.Fatalf("workflow error = %v, want the guard's panic: %v", err, tc.ends)
			}
		})
	}
}
