package telemetry

import (
	"context"
	"fmt"
	"log/slog"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"github.com/orkstack/orkstack/pkg/artifacts"
)

// StoreRequestsMetric counts the process's calls to the S3-compatible
// bucket, by operation, family of keys and outcome (pkg/artifacts
// RequestObservation). Never a key, a run id or a bucket name.
const StoreRequestsMetric = "ork.store.requests"

// AttrStoreOperation and AttrStoreFamily label the store instruments.
const (
	AttrStoreOperation = "ork.store.operation"
	AttrStoreFamily    = "ork.store.family"
)

// storeDurationBuckets bound a request's duration in seconds: a quick
// metadata read is milliseconds, a large artifact's upload part seconds.
var storeDurationBuckets = []float64{0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30}

func newStoreRequestObserver(p *Providers) (artifacts.RequestObserver, error) {
	meter := p.Meter.Meter(ScopeName)
	requests, err := meter.Int64Counter(StoreRequestsMetric,
		metric.WithDescription("Calls to the object store, by operation, family of keys and outcome; retries are one call."),
		metric.WithUnit("{request}"))
	if err != nil {
		return nil, fmt.Errorf("telemetry instruments: %w", err)
	}
	duration, err := meter.Float64Histogram("ork.store.request.duration",
		metric.WithDescription("How long a call to the object store took, retries included."),
		metric.WithUnit("s"), metric.WithExplicitBucketBoundaries(storeDurationBuckets...))
	if err != nil {
		return nil, fmt.Errorf("telemetry instruments: %w", err)
	}
	sent, err := meter.Int64Counter("ork.store.bytes",
		metric.WithDescription("Bytes a put or upload part declared, or a get returned, by operation and family."),
		metric.WithUnit("By"))
	if err != nil {
		return nil, fmt.Errorf("telemetry instruments: %w", err)
	}
	return func(ctx context.Context, r artifacts.RequestObservation) {
		where := []attribute.KeyValue{attribute.String(AttrStoreOperation, r.Operation), attribute.String(AttrStoreFamily, r.Family)}
		requests.Add(ctx, 1, metric.WithAttributes(append(where, attribute.String(AttrOutcome, r.Outcome))...))
		duration.Record(ctx, r.Duration.Seconds(), metric.WithAttributes(where...))
		if r.Bytes > 0 {
			sent.Add(ctx, r.Bytes, metric.WithAttributes(where...))
		}
	}, nil
}

// ObserveStoreRequests installs the process's object-store observer on p,
// the default sink, when p exports metrics, and reports whether it did. With
// metrics off no request pays for an observer. An instrument that cannot be
// built is logged and leaves the store unobserved; it never stops the process.
func ObserveStoreRequests(p *Providers, logger *slog.Logger) bool {
	if p == nil || !p.Config().MetricsEnabled() {
		return false
	}
	observe, err := newStoreRequestObserver(p)
	if err != nil {
		if logger != nil {
			logger.Warn("telemetry: object store requests are not observed", "error", err)
		}
		return false
	}
	artifacts.SetRequestObserver(observe)
	return true
}
