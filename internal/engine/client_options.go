package engine

import (
	"context"

	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/interceptor"
	"go.temporal.io/sdk/workflow"

	"github.com/orkstack/orkstack/internal/payloadcrypt"
)

// ClientOptions is o as an engine dials the cluster with: the one place an
// engine process, or a test standing for one, gets its data converter, its
// failure converter and the interceptor every worker built from the client
// runs with (payload encryption, payload-encryption.md §4.1). keys are the
// process's engines' payload keys, nil where payload encryption is off: then
// every payload and failure is written as the SDK writes it, and the
// converter is only the guard of rule 3. Both converters are replaced whatever
// o sets; everything else of o is kept.
func ClientOptions(o client.Options, keys *payloadcrypt.EngineKeys) client.Options {
	o.DataConverter, o.FailureConverter = DataConverter(), FailureConverter()
	if keys != nil {
		dc := NewDataConverter(keys)
		o.DataConverter, o.FailureConverter = dc, payloadcrypt.NewFailureConverter(dc, true)
	}
	o.Interceptors = append(append([]interceptor.ClientInterceptor(nil), o.Interceptors...), PayloadInterceptor(keys))
	return o
}

// PayloadInterceptor is ClientOptions' interceptor, for a worker that has no
// client of its own (a test environment): it hands workflow code keys,
// marks a local activity's error with its run's key, and marks the arguments
// of plainActivities Plain in a sealed run.
func PayloadInterceptor(keys *payloadcrypt.EngineKeys) interceptor.Interceptor {
	return &payloadInterceptor{keys: payloadcrypt.WorkflowInterceptor(keys, unsealedErrorTypes...)}
}

// unsealedErrorTypes are the workflow types whose local activities' errors
// are written as they are where the execution carries no key: a run's, a
// fire's and a force cancel's, which read them, and the gate's and the
// census's, which hold no run's data (ids, counts and the cluster's own words)
// and which an operator reads when a gate cannot rebuild. A source's workflow
// holds no run's data either, and an operator reads there why its load fails.
// Any other type's are withheld.
var unsealedErrorTypes = []string{RunWorkflowType, ScheduleFireWorkflowType, ConcurrencyGateWorkflowType, ConcurrencyCensusWorkflowType, ForceCancelWorkflowType, SourceWorkflowType}

type payloadInterceptor struct {
	interceptor.InterceptorBase
	keys interceptor.WorkerInterceptor
}

func (p *payloadInterceptor) InterceptWorkflow(ctx workflow.Context, next interceptor.WorkflowInboundInterceptor) interceptor.WorkflowInboundInterceptor {
	return p.keys.InterceptWorkflow(ctx, &plainActivityInbound{
		WorkflowInboundInterceptorBase: interceptor.WorkflowInboundInterceptorBase{Next: next}})
}

func (p *payloadInterceptor) InterceptActivity(ctx context.Context, next interceptor.ActivityInboundInterceptor) interceptor.ActivityInboundInterceptor {
	return p.keys.InterceptActivity(ctx, next)
}
