package payloadcrypt

import (
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/interceptor"
	"go.temporal.io/sdk/workflow"
)

// engineKeysCtxKey carries a worker's engine keys into workflow code.
type engineKeysCtxKey struct{}

// WorkflowInterceptor hands keys, the process's engines' payload keys read
// at boot, to the workflow code of every workflow its worker runs, so the
// interpreter can open its run's key (RunKey). Setting a context value issues
// no command; the keys are the same on every replay of one process.
func WorkflowInterceptor(keys *EngineKeys) interceptor.WorkerInterceptor {
	return &workerInterceptor{keys: keys}
}

type workerInterceptor struct {
	interceptor.WorkerInterceptorBase
	keys *EngineKeys
}

func (w *workerInterceptor) InterceptWorkflow(_ workflow.Context, next interceptor.WorkflowInboundInterceptor) interceptor.WorkflowInboundInterceptor {
	return &workflowInbound{WorkflowInboundInterceptorBase: interceptor.WorkflowInboundInterceptorBase{Next: next}, keys: w.keys}
}

type workflowInbound struct {
	interceptor.WorkflowInboundInterceptorBase
	keys *EngineKeys
}

func (w *workflowInbound) ExecuteWorkflow(ctx workflow.Context, in *interceptor.ExecuteWorkflowInput) (any, error) {
	return w.Next.ExecuteWorkflow(workflow.WithValue(ctx, engineKeysCtxKey{}, w.keys), in)
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
