package engine_test

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/sdk/interceptor"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/internal/task"
)

// pinCreationCapture is a workflow interceptor recording what the replayer
// cannot compare: the options of every session-creation activity the workflow
// schedules (the SDK's ScheduleToStartTimeout of that activity IS
// SessionOptions.CreationTimeout), and every workflow.GetVersion the code
// asks, with the version it was answered. Temporal's replayer matches
// commands by type and id and never compares a timeout, so a replay test
// that must prove a pin creation timeout compares these two against the
// recorded events itself (replay_pin_creation_timeout_test.go).
type pinCreationCapture struct {
	interceptor.WorkerInterceptorBase
	mu       sync.Mutex
	creation []workflow.ActivityOptions
	asked    map[string][]workflow.Version
}

func (c *pinCreationCapture) InterceptWorkflow(_ workflow.Context, next interceptor.WorkflowInboundInterceptor) interceptor.WorkflowInboundInterceptor {
	return &pinCreationInbound{WorkflowInboundInterceptorBase: interceptor.WorkflowInboundInterceptorBase{Next: next}, root: c}
}

type pinCreationInbound struct {
	interceptor.WorkflowInboundInterceptorBase
	root *pinCreationCapture
}

func (i *pinCreationInbound) Init(outbound interceptor.WorkflowOutboundInterceptor) error {
	return i.Next.Init(&pinCreationOutbound{WorkflowOutboundInterceptorBase: interceptor.WorkflowOutboundInterceptorBase{Next: outbound}, root: i.root})
}

type pinCreationOutbound struct {
	interceptor.WorkflowOutboundInterceptorBase
	root *pinCreationCapture
}

func (o *pinCreationOutbound) ExecuteActivity(ctx workflow.Context, activityType string, args ...any) workflow.Future {
	if activityType == sessionCreationActivityName {
		o.root.mu.Lock()
		o.root.creation = append(o.root.creation, workflow.GetActivityOptions(ctx))
		o.root.mu.Unlock()
	}
	return o.Next.ExecuteActivity(ctx, activityType, args...)
}

func (o *pinCreationOutbound) GetVersion(ctx workflow.Context, changeID string, minSupported, maxSupported workflow.Version) workflow.Version {
	v := o.Next.GetVersion(ctx, changeID, minSupported, maxSupported)
	o.root.mu.Lock()
	if o.root.asked == nil {
		o.root.asked = map[string][]workflow.Version{}
	}
	o.root.asked[changeID] = append(o.root.asked[changeID], v)
	o.root.mu.Unlock()
	return v
}

func (c *pinCreationCapture) creationTimeouts() []time.Duration {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]time.Duration, 0, len(c.creation))
	for _, options := range c.creation {
		out = append(out, options.ScheduleToStartTimeout)
	}
	return out
}

func (c *pinCreationCapture) versionsAsked(change string) []workflow.Version {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]workflow.Version(nil), c.asked[change]...)
}

// scheduleToStartTimeout is the error a pool whose every pin host stays full
// ends in once the creation timeout has run out: the creation request was
// never started (a saturated host's creation poller parks and stops polling),
// so the cluster times it out ScheduleToStart. The test environment simulates
// no such timeout for an activity, so the mock returns the error the cluster
// would have delivered.
func scheduleToStartTimeout() error {
	return temporal.NewTimeoutError(enumspb.TIMEOUT_TYPE_SCHEDULE_TO_START, nil)
}

// cordonRefusal is the error a pool whose every pin host is cordoned ends in:
// the Orker's own refusal, turned non-retryable after its attempt bound
// (internal/orker/cordon.go), with the text it carries.
func cordonRefusal() error {
	return temporal.NewNonRetryableApplicationError(
		"orker orker-1 is cordoned (patch window) and opens no new pin; 45 creation attempts were refused, "+
			"so no Orker of this pool could open the pin", task.ReasonOrkerCordoned, nil)
}

// pinnedUnavailableRun runs pinnedDocument against a pool that can never open
// its pin: the creation activity ends in creationErr. It returns what the
// interceptor saw and the message of the failed task.
func pinnedUnavailableRun(t *testing.T, plan engine.RunPlan, creationErr error) (*pinCreationCapture, string) {
	t.Helper()
	var suite testsuite.WorkflowTestSuite
	capture := &pinCreationCapture{}
	h := newPinHarness(t, &suite, func(in *orkstackv1.ExecuteTaskInput) (*orkstackv1.ExecuteTaskResult, error) {
		t.Errorf("task %q was dispatched even though its pin never opened", in.TaskId)
		return succeeded(in.TaskId, nil), nil
	}, nil)
	h.env.SetWorkerOptions(testWorkerOptions(worker.Options{
		EnableSessionWorker: true,
		Interceptors:        []interceptor.WorkerInterceptor{capture},
	}))
	h.env.OnActivity(sessionCreationActivityName, mock.Anything, mock.Anything).Return(creationErr)

	h.env.ExecuteWorkflow(engine.RunWorkflow, "run-pin-creation-timeout",
		compile(t, pinnedDocument), payload(t, nil), plan, (*orkstackv1.RunContinuation)(nil))
	if !h.env.IsWorkflowCompleted() {
		t.Fatal("workflow did not complete")
	}
	stored, ok := h.store.Get("run-pin-creation-timeout")
	if !ok {
		t.Fatal("run read model was not populated")
	}
	prep := taskRunOf(t, stored, "prep")
	if got := prep.GetError().GetCode(); got != "PIN_UNAVAILABLE" {
		t.Fatalf("prep error code = %q, want PIN_UNAVAILABLE", got)
	}
	return capture, prep.GetError().GetMessage()
}

// TestRunWorkflow_APinWaitsForThePlansCreationTimeout is the setting's reach:
// the number a plan carries is how long the pin's creation request waits for
// a free slot, and the PIN_UNAVAILABLE message that ends the wait names it.
// An unset plan keeps the five minutes every build before the field used,
// spelled exactly as the old message spelled it.
func TestRunWorkflow_APinWaitsForThePlansCreationTimeout(t *testing.T) {
	cases := []struct {
		name        string
		seconds     int32
		wantTimeout time.Duration
		wantWithin  string
	}{
		{name: "thirty seconds", seconds: 30, wantTimeout: 30 * time.Second, wantWithin: "within 30s:"},
		{name: "ninety seconds", seconds: 90, wantTimeout: 90 * time.Second, wantWithin: "within 1m30s:"},
		{name: "a day", seconds: 24 * 3600, wantTimeout: 24 * time.Hour, wantWithin: "within 24h0m0s:"},
		{name: "unset keeps five minutes", seconds: 0, wantTimeout: 5 * time.Minute, wantWithin: "within 5m0s:"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			plan := defaultPlan()
			plan.PinCreationTimeoutSeconds = tc.seconds
			capture, message := pinnedUnavailableRun(t, plan, scheduleToStartTimeout())

			got := capture.creationTimeouts()
			if len(got) != 1 || got[0] != tc.wantTimeout {
				t.Errorf("creation requests waited %v, want exactly one of %s", got, tc.wantTimeout)
			}
			if !strings.Contains(message, tc.wantWithin) {
				t.Errorf("PIN_UNAVAILABLE message = %q, want it to say %q", message, tc.wantWithin)
			}
			// Asked once, answered with the build's newest version: a run
			// that starts today takes the gate.
			if asked := capture.versionsAsked(engine.PinCreationTimeoutChange); len(asked) != 1 || asked[0] != 1 {
				t.Errorf("%s asked %v, want exactly one answer of 1", engine.PinCreationTimeoutChange, asked)
			}
		})
	}
}

// TestRunWorkflow_AGateOffPinKeepsTheLegacyFiveMinutes is the DefaultVersion
// branch, taken the way a history recorded before the gate takes it: whatever
// the plan says, the creation request waits the five minutes every build
// before the gate used, and the message says so. It is the same switch the
// recorder uses to write a legacy fixture, and it asks no version.
func TestRunWorkflow_AGateOffPinKeepsTheLegacyFiveMinutes(t *testing.T) {
	defer engine.DisablePinCreationTimeoutGateForTest()()
	plan := defaultPlan()
	plan.PinCreationTimeoutSeconds = 30
	capture, message := pinnedUnavailableRun(t, plan, scheduleToStartTimeout())

	if got := capture.creationTimeouts(); len(got) != 1 || got[0] != 5*time.Minute {
		t.Errorf("creation requests waited %v, want exactly one of 5m0s whatever the plan says", got)
	}
	if !strings.Contains(message, "within 5m0s:") {
		t.Errorf("PIN_UNAVAILABLE message = %q, want it to say within 5m0s", message)
	}
	if asked := capture.versionsAsked(engine.PinCreationTimeoutChange); len(asked) != 0 {
		t.Errorf("%s asked %v with the gate off, want no question", engine.PinCreationTimeoutChange, asked)
	}
}

// TestRunWorkflow_APinsFailureMessageSaysWhatEndedTheWait: under the gate the
// message states the cause the creation ended in, and claims a wait ("within
// <the plan's timeout>") only where the wait is what ended it — nothing polled
// the creation request in time. A pool whose every pin host refused the pin
// because it is cordoned fails after the Orker's own attempt bound, which is
// not the plan's timeout, so the message names the cordon and carries the
// refusal's own text (its reason and its count of refused attempts) instead.
func TestRunWorkflow_APinsFailureMessageSaysWhatEndedTheWait(t *testing.T) {
	plan := defaultPlan()
	plan.PinCreationTimeoutSeconds = 30
	t.Run("no host polled in time", func(t *testing.T) {
		_, message := pinnedUnavailableRun(t, plan, scheduleToStartTimeout())
		if want := `pin "build" could not be opened on pool "main" within 30s: no Orker of the pool had a free pin slot, so this task never reached one: `; !strings.HasPrefix(message, want) {
			t.Errorf("message = %q, want it to start %q", message, want)
		}
	})
	t.Run("every pin host refused it because it is cordoned", func(t *testing.T) {
		_, message := pinnedUnavailableRun(t, plan, cordonRefusal())
		if want := `pin "build" could not be opened on pool "main": every pin host of the pool refused it because it is cordoned, so this task never reached one: `; !strings.HasPrefix(message, want) {
			t.Errorf("message = %q, want it to start %q", message, want)
		}
		for _, carried := range []string{"is cordoned (patch window)", "45 creation attempts were refused"} {
			if !strings.Contains(message, carried) {
				t.Errorf("message = %q, want it to carry the refusal's %q", message, carried)
			}
		}
		if strings.Contains(message, "within") {
			t.Errorf("message = %q claims a wait the cordon refusal did not take", message)
		}
	})
	t.Run("any other cause claims no wait either", func(t *testing.T) {
		_, message := pinnedUnavailableRun(t, plan, temporal.NewNonRetryableApplicationError(
			"too many outstanding sessions", task.CategoryInfrastructureError, nil))
		if want := `pin "build" could not be opened on pool "main": no pin host of the pool opened it, so this task never reached one: `; !strings.HasPrefix(message, want) {
			t.Errorf("message = %q, want it to start %q", message, want)
		}
		if strings.Contains(message, "within") || !strings.Contains(message, "too many outstanding sessions") {
			t.Errorf("message = %q, want the cause and no claimed wait", message)
		}
	})
}

// TestRunWorkflow_AGateOffPinKeepsItsLegacyMessageForEveryCause: a history
// recorded before the gate replays the message its build wrote, whatever the
// cause: the wait it names is always the legacy five minutes.
func TestRunWorkflow_AGateOffPinKeepsItsLegacyMessageForEveryCause(t *testing.T) {
	defer engine.DisablePinCreationTimeoutGateForTest()()
	plan := defaultPlan()
	plan.PinCreationTimeoutSeconds = 30
	for name, creationErr := range map[string]error{
		"timeout": scheduleToStartTimeout(), "cordon": cordonRefusal(),
	} {
		t.Run(name, func(t *testing.T) {
			_, message := pinnedUnavailableRun(t, plan, creationErr)
			if want := `pin "build" could not be opened on pool "main" within 5m0s: no Orker of the pool had a free pin slot, so this task never reached one: `; !strings.HasPrefix(message, want) {
				t.Errorf("message = %q, want the legacy wording %q", message, want)
			}
		})
	}
}

// TestRunWorkflow_TheCreationTimeoutGateIsAskedOnlyWhereAPinOpens: the version
// is asked lazily, where the first pin opens (AGENTS.md §7.2), so a run
// without a pin records no marker and every history of such a run replays as
// it was recorded; and a run that opens several pins asks once and every pin
// takes that decision.
func TestRunWorkflow_TheCreationTimeoutGateIsAskedOnlyWhereAPinOpens(t *testing.T) {
	t.Run("no pin, no question", func(t *testing.T) {
		var suite testsuite.WorkflowTestSuite
		capture := &pinCreationCapture{}
		h := newHarness(t, &suite, func(in *orkstackv1.ExecuteTaskInput) (*orkstackv1.ExecuteTaskResult, error) {
			return succeeded(in.TaskId, map[string]any{"ok": true}), nil
		})
		h.env.SetWorkerOptions(testWorkerOptions(worker.Options{Interceptors: []interceptor.WorkerInterceptor{capture}}))
		plan := defaultPlan()
		plan.PinCreationTimeoutSeconds = 30
		h.env.ExecuteWorkflow(engine.RunWorkflow, "run-no-pin",
			compile(t, linearDocument), payload(t, nil), plan, (*orkstackv1.RunContinuation)(nil))
		if err := h.env.GetWorkflowError(); err != nil {
			t.Fatalf("workflow error: %v", err)
		}
		if asked := capture.versionsAsked(engine.PinCreationTimeoutChange); len(asked) != 0 {
			t.Errorf("%s asked %v in a run that opens no pin, want no question", engine.PinCreationTimeoutChange, asked)
		}
	})

	t.Run("two pins, one question, one number", func(t *testing.T) {
		var suite testsuite.WorkflowTestSuite
		capture := &pinCreationCapture{}
		h := newPinHarness(t, &suite, func(in *orkstackv1.ExecuteTaskInput) (*orkstackv1.ExecuteTaskResult, error) {
			return succeeded(in.TaskId, map[string]any{"ok": true}), nil
		}, nil)
		h.env.SetWorkerOptions(testWorkerOptions(worker.Options{
			EnableSessionWorker: true,
			Interceptors:        []interceptor.WorkerInterceptor{capture},
		}))
		plan := defaultPlan()
		plan.PinCreationTimeoutSeconds = 45
		h.env.ExecuteWorkflow(engine.RunWorkflow, "run-two-pins",
			compile(t, twoPinsDocument), payload(t, nil), plan, (*orkstackv1.RunContinuation)(nil))
		if err := h.env.GetWorkflowError(); err != nil {
			t.Fatalf("workflow error: %v", err)
		}
		if got := capture.creationTimeouts(); len(got) != 2 || got[0] != 45*time.Second || got[1] != 45*time.Second {
			t.Errorf("creation requests waited %v, want two of 45s", got)
		}
		if asked := capture.versionsAsked(engine.PinCreationTimeoutChange); len(asked) != 1 || asked[0] != 1 {
			t.Errorf("%s asked %v, want exactly one answer of 1", engine.PinCreationTimeoutChange, asked)
		}
	})
}

// TestRunWorkflow_ACarriedGenerationKeepsThePinCreationDecision: a run that
// continues as new between two pins asks the version once, in the generation
// where its first pin opened, and carries the decision (VersionDecisions), so
// its second pin, in the next generation, waits the same plan number and the
// next generation never asks again.
func TestRunWorkflow_ACarriedGenerationKeepsThePinCreationDecision(t *testing.T) {
	defer engine.ContinueAtEveryIdleForTest()()
	capture := &pinCreationCapture{}
	plan := defaultPlan()
	plan.PinCreationTimeoutSeconds = 45
	plan.History = &engine.HistoryLimit{MaxEvents: engine.DefaultHistoryMaxEvents,
		MaxBytes: engine.DefaultHistoryMaxBytes, Reserve: engine.DefaultHistoryReserve}
	budget := engine.DefaultContinuationBudget(1, 1<<40, time.Hour)
	plan.Continuation = &budget

	g := runGenerations(t, "run-carried-pins", compile(t, twoPinsDocument), payload(t, nil), plan,
		func(_ context.Context, in *orkstackv1.ExecuteTaskInput) (*orkstackv1.ExecuteTaskResult, error) {
			return succeeded(in.TaskId, map[string]any{"ok": true}), nil
		},
		func(h *harness, _ int) {
			enablePins(h, worker.Options{Interceptors: []interceptor.WorkerInterceptor{capture}})
		})
	if g.err != nil {
		t.Fatalf("the run ended with %v", g.err)
	}
	if len(g.carries) == 0 {
		t.Fatal("the run never continued as new, so no decision crossed a generation")
	}
	if got := g.carries[0].GetVersionDecisions()[engine.PinCreationTimeoutChange]; got != 1 {
		t.Errorf("the first carry holds %s = %d, want the decision 1 (carry %v)",
			engine.PinCreationTimeoutChange, got, g.carries[0].GetVersionDecisions())
	}
	if got := capture.creationTimeouts(); len(got) != 2 || got[0] != 45*time.Second || got[1] != 45*time.Second {
		t.Errorf("creation requests waited %v across the generations, want two of 45s", got)
	}
	if asked := capture.versionsAsked(engine.PinCreationTimeoutChange); len(asked) != 1 {
		t.Errorf("%s asked %v across the generations, want exactly one question: the next generation answers from its carry",
			engine.PinCreationTimeoutChange, asked)
	}
}

// twoPinsDocument opens two pins one after the other, with an unpinned task
// between them: where a run that continues as new has a moment with nothing in
// flight.
const twoPinsDocument = `
apiVersion: orkstack/v1
kind: Orkflow
name: two-pins
pins:
  first: {}
  second: {}
tasks:
  - id: a
    uses: shell
    pin: first
    run_as_orker_user: true
    with: {command: "echo a"}
  - id: gap
    uses: shell
    needs: [a]
    with: {command: "echo gap"}
  - id: b
    uses: shell
    pin: second
    needs: [gap]
    run_as_orker_user: true
    with: {command: "echo b"}
`
