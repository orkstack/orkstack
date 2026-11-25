package telemetry

import (
	"context"
	"net/http"

	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
)

// TrustInboundTraceVar is telemetry.trust_inbound_trace: whether an API
// request joins the trace its caller sends, with the caller's sampling
// decision and baggage. Off, a request starts a trace of its own, linked to
// the caller's.
const TrustInboundTraceVar = "ORK_TELEMETRY_TRUST_INBOUND_TRACE"

// DefaultTrustInboundTrace is telemetry.trust_inbound_trace's default.
const DefaultTrustInboundTrace = false

// What is kept of a trace state: the W3C limit on members, and the length
// every implementation must carry.
const (
	maxTracestateBytes   = 512
	maxTracestateMembers = 32
)

// boundedPropagator is the W3C propagator over a carrier OrkStack does not
// control: a trace state over its bounds is not read, and baggage only when
// trusted. What it injects is unbounded by it: OrkStack wrote it.
type boundedPropagator struct{ propagation.TextMapPropagator }

// InboundPropagator reads a caller's trace context within bounds, and its
// baggage only when trust says to.
func InboundPropagator(trust bool) propagation.TextMapPropagator {
	if trust {
		return boundedPropagator{propagation.NewCompositeTextMapPropagator(propagation.TraceContext{}, propagation.Baggage{})}
	}
	return boundedPropagator{propagation.TraceContext{}}
}

func (b boundedPropagator) Extract(ctx context.Context, carrier propagation.TextMapCarrier) context.Context {
	return b.TextMapPropagator.Extract(ctx, boundedCarrier{carrier})
}

// boundedCarrier hides a trace state over its bounds.
type boundedCarrier struct{ propagation.TextMapCarrier }

func (c boundedCarrier) Get(key string) string {
	value := c.TextMapCarrier.Get(key)
	if key == "tracestate" {
		return boundedTracestate(value)
	}
	return value
}

// boundedTracestate is state, or empty when it does not parse or is over its
// bounds.
func boundedTracestate(state string) string {
	if len(state) > maxTracestateBytes {
		return ""
	}
	parsed, err := trace.ParseTraceState(state)
	if err != nil || parsed.Len() > maxTracestateMembers {
		return ""
	}
	return state
}

// InboundHTTPOptions are how the API's listener takes part in traces: on p's
// providers, reading a caller's context within bounds, and joining its trace
// only where the deployment trusts it. A deployment that does not trace reads
// no context at all: the no-op tracer would carry a caller's into the run.
func InboundHTTPOptions(p *Providers) []otelhttp.Option {
	trust := p.cfg.TrustInboundTrace
	propagator := InboundPropagator(trust)
	if !p.cfg.TracesEnabled() {
		propagator = propagation.NewCompositeTextMapPropagator()
	}
	return []otelhttp.Option{
		otelhttp.WithTracerProvider(p.Tracer),
		otelhttp.WithMeterProvider(p.Meter),
		otelhttp.WithPropagators(propagator),
		otelhttp.WithPublicEndpointFn(func(*http.Request) bool { return !trust }),
	}
}

// RunTrace is the W3C context of ctx's span, its state within bounds: what a
// run being submitted pins and what a task is given. Empty when ctx has no
// span.
func RunTrace(ctx context.Context) (traceparent, tracestate string) {
	carrier := propagation.MapCarrier{}
	propagation.TraceContext{}.Inject(ctx, carrier)
	return carrier["traceparent"], boundedTracestate(carrier["tracestate"])
}
