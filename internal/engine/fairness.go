package engine

import (
	"crypto/sha256"
	"encoding/hex"
	"sync/atomic"

	"go.temporal.io/sdk/interceptor"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
)

// Fair dispatch across namespaces (docs/DEPLOY.md "Fair dispatch across
// namespaces") is Temporal's task-queue fairness, keyed by namespace. Every
// task activity of a run carries its namespace's fairness key and weight, both
// decided at submit and pinned in the plan (RunPlan.Fairness), so a namespace
// whose weight changes moves only the runs submitted after it. It is never an
// OrkStack queue: the cluster orders what waits on one channel, and only when
// its operator enabled fairness (matching.enableFairness); without that the
// keys are ignored and dispatch is first in, first out per channel.

// FairnessKeysChange is the workflow.GetVersion change id of the fairness key
// and weight a run's plan carries. Asked lazily, where a run first schedules a
// task activity (priorityFor), and only by a run whose plan carries a
// selection: a history recorded before the gate, or by a build that ignored
// the plan field, replays its DefaultVersion branch, which sets no priority.
const FairnessKeysChange = "fairness-keys"

// maxFairnessKey is Temporal's bound on a fairness key, in bytes.
const maxFairnessKey = 64

// FairnessSelection is one run's fairness key and weight. Weight 0 is the
// cluster's default weight of 1. PinRequests says the run's pin requests
// carry them too (fairness-pin-creation): set at submit by every build since
// the field, so a run submitted before it never keys a pin request, whatever
// build later runs it.
type FairnessSelection struct {
	Key         string  `json:"key"`
	Weight      float32 `json:"weight,omitzero"`
	PinRequests bool    `json:"pin_requests,omitzero"`
}

// priority is the selection as a Temporal priority.
func (f *FairnessSelection) priority() temporal.Priority {
	return temporal.Priority{FairnessKey: f.Key, FairnessWeight: f.Weight}
}

// FairnessKeyFor is a namespace's fairness key: its name when that fits in 64
// bytes, and otherwise a readable prefix of it and a SHA-256 suffix, so two
// long names never share a key and every key still says whose it is.
func FairnessKeyFor(namespace string) string {
	if len(namespace) <= maxFairnessKey {
		return namespace
	}
	sum := sha256.Sum256([]byte(namespace))
	suffix := hex.EncodeToString(sum[:])[:16]
	return namespace[:maxFairnessKey-len(suffix)-1] + "-" + suffix
}

// fairnessFor is the selection SubmitRun pins for a run of ns.
func fairnessFor(ns *orkstackv1.Namespace) *FairnessSelection {
	return &FairnessSelection{Key: FairnessKeyFor(namespaceName(ns)), Weight: float32(ns.GetFairShareWeight()), PinRequests: true}
}

// fairnessKeysGateDisabledForTest is a TEST-ONLY switch, the precedent of
// pinCreationTimeoutGateDisabledForTest: when set, priorityFor takes the
// DefaultVersion branch WITHOUT asking its version, so a recorder can write a
// history in the shape of the builds before the gate. Never set in production
// code.
var fairnessKeysGateDisabledForTest atomic.Bool

// priorityFor is the priority a task activity of this run is scheduled with:
// the plan's fairness key and weight once the gate says so, and none for a run
// whose plan carries no selection or whose history predates the gate.
func (s *runState) priorityFor(ctx workflow.Context) temporal.Priority {
	fairness := s.plan.Fairness
	if fairness == nil || fairnessKeysGateDisabledForTest.Load() ||
		s.version(ctx, FairnessKeysChange, 1) < 1 {
		return temporal.Priority{}
	}
	return fairness.priority()
}

// FairnessPinCreationChange is the workflow.GetVersion change id of the
// fairness key and weight on a pin's creation request. That request waits for
// a free session slot among its pool's pin hosts, beside every other
// namespace's, so it is keyed as the run's tasks are. Asked lazily, where a
// run first opens a pin (openPin), and only by a run whose plan's selection
// says PinRequests: a history recorded before the gate replays its DefaultVersion
// branch, which keys no creation request. A plan built before
// FairnessSelection.PinRequests asks nothing, so a run already in flight when
// the gate shipped never takes it.
const FairnessPinCreationChange = "fairness-pin-creation"

// fairnessPinCreationGateDisabledForTest is the TEST-ONLY switch of
// fairnessKeysGateDisabledForTest for a pin's creation request, so a recorder
// can write a history in the shape of the builds before the gate. Never set
// in production code.
var fairnessPinCreationGateDisabledForTest atomic.Bool

// pinCreationPriorityFor is the priority a pin of this run asks for its
// session with: the plan's fairness key and weight once the gate says so, and
// none for a run whose plan carries no selection, or one from before
// PinRequests, or whose history predates the gate.
func (s *runState) pinCreationPriorityFor(ctx workflow.Context) temporal.Priority {
	fairness := s.plan.Fairness
	if fairness == nil || !fairness.PinRequests || fairnessPinCreationGateDisabledForTest.Load() ||
		s.version(ctx, FairnessPinCreationChange, 1) < 1 {
		return temporal.Priority{}
	}
	return fairness.priority()
}

// sessionCreationActivity is the SDK's own name for a pin's creation request
// (go.temporal.io/sdk internal/session.go). A rename fails
// TestRunWorkflow_APinsCreationRequestCarriesThePlansFairness.
const sessionCreationActivity = "internalSessionCreationActivity"

// pinCreationPriorityKey is the workflow.Context value a pin's coroutine hands
// workflow.CreateSession its priority in.
type pinCreationPriorityKey struct{}

// withPinCreationPriority is ctx carrying the priority its pin's creation
// request is to be scheduled with; a zero priority leaves ctx as it is.
func withPinCreationPriority(ctx workflow.Context, priority temporal.Priority) workflow.Context {
	if priority == (temporal.Priority{}) {
		return ctx
	}
	return workflow.WithValue(ctx, pinCreationPriorityKey{}, priority)
}

// RunWorkerInterceptors is what every worker that runs RunWorkflow is built
// with (cmd/ork): the interceptor that puts a pin's priority on its creation
// request. workflow.CreateSession builds that request's activity options
// itself, with no priority and no way to pass one, so the workflow decides the
// priority (pinCreationPriorityFor) and leaves it on the context it hands
// CreateSession, and this interceptor sets it as the SDK schedules the
// request. It reads only that context value, so it is as deterministic as the
// workflow. A worker built without it opens every pin with no fairness key and
// changes nothing else.
func RunWorkerInterceptors() []interceptor.WorkerInterceptor {
	return []interceptor.WorkerInterceptor{&pinCreationInterceptor{}}
}

type pinCreationInterceptor struct {
	interceptor.WorkerInterceptorBase
}

func (*pinCreationInterceptor) InterceptWorkflow(_ workflow.Context, next interceptor.WorkflowInboundInterceptor) interceptor.WorkflowInboundInterceptor {
	return &pinCreationInbound{WorkflowInboundInterceptorBase: interceptor.WorkflowInboundInterceptorBase{Next: next}}
}

type pinCreationInbound struct {
	interceptor.WorkflowInboundInterceptorBase
}

func (i *pinCreationInbound) Init(outbound interceptor.WorkflowOutboundInterceptor) error {
	return i.Next.Init(&pinCreationOutbound{WorkflowOutboundInterceptorBase: interceptor.WorkflowOutboundInterceptorBase{Next: outbound}})
}

type pinCreationOutbound struct {
	interceptor.WorkflowOutboundInterceptorBase
}

func (o *pinCreationOutbound) ExecuteActivity(ctx workflow.Context, activityType string, args ...any) workflow.Future {
	if activityType == sessionCreationActivity {
		if priority, ok := ctx.Value(pinCreationPriorityKey{}).(temporal.Priority); ok {
			ctx = workflow.WithPriority(ctx, priority)
		}
	}
	return o.Next.ExecuteActivity(ctx, activityType, args...)
}
