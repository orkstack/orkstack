package payloadcrypt

import (
	"context"
	"errors"
	"slices"

	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/interceptor"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
)

// engineKeysCtxKey carries a worker's engine keys into workflow code.
type engineKeysCtxKey struct{}

// WorkflowInterceptor hands keys, the process's engines' payload keys read
// at boot, to the workflow code of every workflow its worker runs, so the
// interpreter can open its run's key (RunKey). Setting a context value issues
// no command; the keys are the same on every replay of one process.
//
// unsealedTypes are the workflow types whose local activities' errors are
// written as they are where the execution carries no key: a run's and a
// fire's, which read them. Any other type's are withheld, as every error
// nobody marked is in a process that seals.
func WorkflowInterceptor(keys *EngineKeys, unsealedTypes ...string) interceptor.WorkerInterceptor {
	return &workerInterceptor{keys: keys, unsealedTypes: unsealedTypes}
}

type workerInterceptor struct {
	interceptor.WorkerInterceptorBase
	keys          *EngineKeys
	unsealedTypes []string
}

func (w *workerInterceptor) InterceptWorkflow(_ workflow.Context, next interceptor.WorkflowInboundInterceptor) interceptor.WorkflowInboundInterceptor {
	return &workflowInbound{WorkflowInboundInterceptorBase: interceptor.WorkflowInboundInterceptorBase{Next: next}, keys: w.keys, unsealedTypes: w.unsealedTypes}
}

type workflowInbound struct {
	interceptor.WorkflowInboundInterceptorBase
	keys          *EngineKeys
	unsealedTypes []string
}

func (w *workflowInbound) ExecuteWorkflow(ctx workflow.Context, in *interceptor.ExecuteWorkflowInput) (any, error) {
	if w.keys == nil {
		return w.Next.ExecuteWorkflow(ctx, in)
	}
	return w.Next.ExecuteWorkflow(workflow.WithValue(ctx, engineKeysCtxKey{}, w.keys), in)
}

func (w *workflowInbound) Init(outbound interceptor.WorkflowOutboundInterceptor) error {
	if w.keys != nil {
		outbound = &workflowOutbound{WorkflowOutboundInterceptorBase: interceptor.WorkflowOutboundInterceptorBase{Next: outbound}, unsealedTypes: w.unsealedTypes}
	}
	return w.Next.Init(outbound)
}

// What a local activity returns is written into its run's history by the SDK:
// its error through the failure converter, its result with the root
// converter, neither of which knows the run. So each must leave its activity
// with its run's key (WithKey, Seal). The activity knows no run's memo: what
// its run's key is opened with travels beside its arguments, in headers the
// SDK keeps in this process and records nowhere.
const (
	headerRunKey      = "ork-payload-key"
	headerRunStamp    = "ork-payload-key-schedule"
	headerRunUnsealed = "ork-payload-unsealed"
	headerPlainResult = "ork-payload-plain-result"
)

// plainResultCtxKey marks a context of PlainLocalResult's.
type plainResultCtxKey struct{}

// PlainLocalResult is ctx whose local activities' results are written in
// plaintext, in a sealed run too: for a run whose key nobody can be given,
// and a result that holds nothing of it. Setting a context value issues no
// command.
func PlainLocalResult(ctx workflow.Context) workflow.Context {
	return workflow.WithValue(ctx, plainResultCtxKey{}, true)
}

type workflowOutbound struct {
	interceptor.WorkflowOutboundInterceptorBase
	unsealedTypes []string
}

func (w *workflowOutbound) ExecuteLocalActivity(ctx workflow.Context, activityType string, args ...any) workflow.Future {
	h := interceptor.WorkflowHeader(ctx)
	if p, ok := workflow.GetInfo(ctx).Memo.GetFields()[MemoField]; ok {
		h[headerRunKey] = p
		if v, ok := workflow.GetTypedSearchAttributes(ctx).GetKeyword(scheduledByIDKey); ok {
			h[headerRunStamp] = &commonpb.Payload{Data: []byte(v)}
		}
		if ctx.Value(plainResultCtxKey{}) != nil {
			h[headerPlainResult] = &commonpb.Payload{}
		}
	} else if slices.Contains(w.unsealedTypes, workflow.GetInfo(ctx).WorkflowType.Name) {
		h[headerRunUnsealed] = &commonpb.Payload{}
	}
	return w.Next.ExecuteLocalActivity(ctx, activityType, args...)
}

// InterceptActivity marks a local activity's error with its run's key, and
// seals its result under it, in a process that holds engines' keys. An activity a worker was dispatched is
// its own boundary's to mark.
func (w *workerInterceptor) InterceptActivity(_ context.Context, next interceptor.ActivityInboundInterceptor) interceptor.ActivityInboundInterceptor {
	if w.keys == nil {
		return next
	}
	return &activityInbound{ActivityInboundInterceptorBase: interceptor.ActivityInboundInterceptorBase{Next: next}, keys: w.keys}
}

type activityInbound struct {
	interceptor.ActivityInboundInterceptorBase
	keys *EngineKeys
}

func (a *activityInbound) ExecuteActivity(ctx context.Context, in *interceptor.ExecuteActivityInput) (any, error) {
	out, err := a.Next.ExecuteActivity(ctx, in)
	info := activity.GetInfo(ctx)
	if !info.IsLocalActivity {
		return out, err
	}
	// No header, or a key that does not open, leaves no scope: the error is
	// withheld, never written in plaintext, and so is a sealed run's result,
	// which then does not convert.
	var scope *Scope
	h := interceptor.Header(ctx)
	p, keyed := h[headerRunKey]
	if keyed {
		memo := &commonpb.Memo{Fields: map[string]*commonpb.Payload{MemoField: p}}
		scope = RunKeyOf(memo, a.keys, info.WorkflowExecution.ID, string(h[headerRunStamp].GetData())).Scope()
	} else if _, ok := h[headerRunUnsealed]; ok {
		scope = Unsealed()
	}
	if err == nil {
		if _, plain := h[headerPlainResult]; out == nil || !keyed || plain {
			return out, nil
		}
		sealed, sealErr := sealLocalResult(scope, out)
		if sealErr != nil {
			// Asking again would do the activity's work again and seal nothing.
			return nil, WithKey(temporal.NewNonRetryableApplicationError(sealErr.Error(), "", nil), scope, "")
		}
		return sealed, nil
	}
	reason := ""
	var app *temporal.ApplicationError
	if errors.As(err, &app) {
		reason = app.Type()
	}
	// The SDK reads a local activity's NextRetryDelay, its benign category and
	// a non-retryable Go type name by type assertion, past which it does not
	// unwrap: an engine's local activity uses none of them.
	return out, WithKey(err, scope, reason)
}

// sealLocalResult is out sealed for scope: the payloads the SDK converted it
// to, or a value it has not converted yet, which its converter seals.
func sealLocalResult(scope *Scope, out any) (any, error) {
	ps, ok := out.(*commonpb.Payloads)
	if !ok {
		return Seal(scope, out), nil
	}
	sealed := &commonpb.Payloads{Payloads: make([]*commonpb.Payload, len(ps.GetPayloads()))}
	for i, p := range ps.GetPayloads() {
		s, err := scope.SealPayload(p)
		if err != nil {
			return nil, err
		}
		sealed.Payloads[i] = s
	}
	return sealed, nil
}

// engineKeysOf is the keys the worker's interceptor handed ctx; nil where
// none did, which holds no key a memo can name.
func engineKeysOf(ctx workflow.Context) *EngineKeys {
	keys, _ := ctx.Value(engineKeysCtxKey{}).(*EngineKeys)
	return keys
}

// ProcessSeals reports whether the worker running ctx's workflow code holds
// engines' payload keys: a process whose failure converter seals, so an
// unsealed run's error must be marked as such (WithKey with Unsealed()).
// Without keys an error is left as it is, for any failure converter.
func ProcessSeals(ctx workflow.Context) bool { return engineKeysOf(ctx) != nil }

// RunKey is the key of the run whose workflow code ctx is, read from the
// execution's own memo (and, for a fire, the cluster's stamp) with the
// worker's engine keys: arithmetic on bytes the history holds, no call. A
// miss (ErrEngineKeyMissing) is the caller's to panic on.
func RunKey(ctx workflow.Context) KeyState {
	info := workflow.GetInfo(ctx)
	stamp := ""
	if _, ok := info.Memo.GetFields()[MemoField]; ok {
		if v, ok := workflow.GetTypedSearchAttributes(ctx).GetKeyword(scheduledByIDKey); ok {
			stamp = v
		}
	}
	return RunKeyOf(info.Memo, engineKeysOf(ctx), info.WorkflowExecution.ID, stamp)
}

// OpenIfSealed is p opened under s where it is sealed, and p itself where it
// is not: a signal of a sealed run may be sealed or plaintext (design §2,
// "What is not refused"). A sealed p that s does not open is ErrUnreadable,
// and so is one in a run that holds no key.
func OpenIfSealed(s *Scope, p *commonpb.Payload) (*commonpb.Payload, error) {
	if !IsSealed(p) {
		return p, nil
	}
	return s.Open(p)
}

// OpenRaw reads raw, a value the SDK handed over unconverted, into ptr: for
// a sealed scope it must be sealed for the scope's context and open under its
// key; for Unsealed() it must not be sealed. dc decodes what is opened. A raw
// value with no payload is an argument not passed: present is false and err
// nil. Anything else that does not read is ErrUnreadable.
func OpenRaw(s *Scope, dc converter.DataConverter, raw converter.RawValue, ptr any) (present bool, err error) {
	p := raw.Payload()
	if p == nil {
		return false, nil
	}
	switch {
	case s.Sealed():
		opened, err := s.Open(p)
		if err != nil {
			return false, err
		}
		p = opened
	case s.unsealed():
		if IsSealed(p) {
			return false, unreadable("a sealed payload in a run that carries no key")
		}
	default:
		return false, unreadable("no key for a run whose key was lost")
	}
	if err := dc.FromPayload(p, ptr); err != nil {
		return false, unreadable("the payload does not decode: %v", err)
	}
	return true, nil
}
