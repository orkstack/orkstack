package telemetry

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

// RPCCallDurationMetric is every API call's duration, by service, method and
// Connect code; APIStreamsMetric is the API's open streams, by kind.
const (
	RPCCallDurationMetric = "rpc.server.call.duration"
	APIStreamsMetric      = "ork.api.streams"

	AttrRPCService = "rpc.service"
	AttrRPCMethod  = "rpc.method"
	AttrRPCCode    = "rpc.code"
	AttrStreamKind = "ork.stream.kind"
)

// The kinds of an API stream.
const (
	StreamConnect         = "connect"
	StreamWebSocketEvents = "websocket_events"
	StreamWebSocketLogs   = "websocket_task_logs"
)

// rpcDurationBuckets bound a call in seconds: a read is milliseconds, a
// stream that a client keeps open is minutes.
var rpcDurationBuckets = []float64{0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60, 300}

// RPCMetrics records the API's calls and streams. A nil *RPCMetrics records
// nothing, so a caller with telemetry off holds nil.
type RPCMetrics struct {
	calls   metric.Float64Histogram
	streams metric.Int64UpDownCounter
}

// NewRPCMetrics builds the API's instruments on the default sink's meter
// provider.
func NewRPCMetrics(provider metric.MeterProvider) (*RPCMetrics, error) {
	meter := provider.Meter(ScopeName)
	calls, err := meter.Float64Histogram(RPCCallDurationMetric,
		metric.WithDescription("How long an API call took, by service, method and Connect code; a stream's is its whole life."),
		metric.WithUnit("s"), metric.WithExplicitBucketBoundaries(rpcDurationBuckets...))
	if err != nil {
		return nil, fmt.Errorf("telemetry instruments: %w", err)
	}
	streams, err := meter.Int64UpDownCounter(APIStreamsMetric,
		metric.WithDescription("The API streams open now, by kind."),
		metric.WithUnit("{stream}"))
	if err != nil {
		return nil, fmt.Errorf("telemetry instruments: %w", err)
	}
	return &RPCMetrics{calls: calls, streams: streams}, nil
}

// ObserveCall records one call of procedure ("/pkg.Service/Method"), which
// ended with code ("ok" or a Connect code's name).
func (m *RPCMetrics) ObserveCall(ctx context.Context, procedure, code string, d time.Duration) {
	if m == nil {
		return
	}
	service, method := splitProcedure(procedure)
	m.calls.Record(ctx, d.Seconds(), metric.WithAttributes(
		attribute.String(AttrRPCService, service),
		attribute.String(AttrRPCMethod, method),
		attribute.String(AttrRPCCode, code)))
}

// StreamOpened counts one open stream of kind and returns the function that
// counts it closed; calling that more than once counts it closed once.
func (m *RPCMetrics) StreamOpened(ctx context.Context, kind string) (closed func()) {
	if m == nil {
		return func() {}
	}
	attrs := metric.WithAttributes(attribute.String(AttrStreamKind, kind))
	m.streams.Add(ctx, 1, attrs)
	var once sync.Once
	return func() { once.Do(func() { m.streams.Add(context.WithoutCancel(ctx), -1, attrs) }) }
}

// splitProcedure is a Connect procedure's service and method. A procedure is
// a route the API mounts, never caller text; anything else is "unknown".
func splitProcedure(procedure string) (service, method string) {
	rest, ok := strings.CutPrefix(procedure, "/")
	if ok {
		if service, method, ok = strings.Cut(rest, "/"); ok && service != "" && method != "" && !strings.Contains(method, "/") {
			return service, method
		}
	}
	return "unknown", "unknown"
}
