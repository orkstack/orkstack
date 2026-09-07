package telemetry

import (
	"log/slog"

	"go.opentelemetry.io/otel/attribute"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/contrib/opentelemetry"
	"go.temporal.io/sdk/interceptor"
)

// TemporalMetricsHandler adapts the meter for client.Options.MetricsHandler,
// which turns on the SDK's own instruments — workflow task and activity
// latencies, poll counts, schedule-to-start, failures — at no cost to
// OrkStack code. It returns nil when no metrics exporter is configured, so
// the client options a zero-config deployment dials with are the ones it
// always had.
func TemporalMetricsHandler(providers *Providers, logger *slog.Logger) client.MetricsHandler {
	if providers == nil || !providers.cfg.MetricsEnabled() {
		return nil
	}
	return opentelemetry.NewMetricsHandler(opentelemetry.MetricsHandlerOptions{
		Meter:                providers.Meter.Meter("temporal-sdk-go"),
		UseMonotonicCounters: true,
		OnError: func(err error) {
			if logger != nil {
				logger.Warn("temporal sdk metric refused", "error", err)
			}
		},
	})
}

// TemporalInterceptors returns the tracing interceptor for
// client.Options.Interceptors, or nil when tracing is off. The interceptor is
// replay-aware — it starts no span while a workflow replays — and carries the
// span context in Temporal headers, so an Orker's activity span is a child of
// the engine's workflow span with no OrkStack code in between. Workflow code
// itself never imports OpenTelemetry.
func TemporalInterceptors(providers *Providers) ([]interceptor.ClientInterceptor, error) {
	if providers == nil || !providers.cfg.TracesEnabled() {
		return nil, nil
	}
	tracing, err := opentelemetry.NewTracingInterceptor(opentelemetry.TracerOptions{
		Tracer:            providers.Tracer.Tracer(ScopeName),
		TextMapPropagator: providers.Propagator,
	})
	if err != nil {
		return nil, err
	}
	return []interceptor.ClientInterceptor{tracing}, nil
}

// ResourceAttrs spells the binary's identity as resource attributes, so both
// mains build the same keys.
func ResourceAttrs(version, deployment, buildID, temporalNamespace string) []attribute.KeyValue {
	attrs := []attribute.KeyValue{
		attribute.String("service.version", version),
		attribute.String(AttrDeployment, deployment),
		attribute.String(AttrTemporalNamespace, temporalNamespace),
	}
	if buildID != "" {
		attrs = append(attrs, attribute.String(AttrBuildID, buildID))
	}
	return attrs
}
