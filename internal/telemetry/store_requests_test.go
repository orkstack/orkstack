package telemetry

import (
	"context"
	"testing"
	"time"

	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/orkstack/orkstack/internal/testsupport/must"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// TestStoreRequests_RecordEachObservation: one observation moves
// ork.store.requests by operation, family and outcome, ork.store.bytes by its
// bytes, and ork.store.request.duration by its duration.
func TestStoreRequests_RecordEachObservation(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	p := &Providers{Meter: sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader)), cfg: Config{MetricsExporters: []string{ExporterOTLP}}}
	observe, err := newStoreRequestObserver(p)
	must.OK(t, err)
	ctx := context.Background()
	observe(ctx, artifacts.RequestObservation{Operation: "put", Family: "audit", Outcome: artifacts.RequestOK, Bytes: 5, Duration: 20 * time.Millisecond})
	observe(ctx, artifacts.RequestObservation{Operation: "get", Family: "runs", Outcome: artifacts.RequestNotFound, Duration: time.Millisecond})

	var rm metricdata.ResourceMetrics
	must.OK(t, reader.Collect(ctx, &rm))
	requests := map[string]int64{}
	var bytes int64
	var durations uint64
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			switch m.Name {
			case StoreRequestsMetric:
				for _, dp := range m.Data.(metricdata.Sum[int64]).DataPoints {
					op, _ := dp.Attributes.Value(AttrStoreOperation)
					family, _ := dp.Attributes.Value(AttrStoreFamily)
					outcome, _ := dp.Attributes.Value(AttrOutcome)
					requests[op.AsString()+"/"+family.AsString()+"/"+outcome.AsString()] += dp.Value
				}
			case "ork.store.bytes":
				for _, dp := range m.Data.(metricdata.Sum[int64]).DataPoints {
					bytes += dp.Value
				}
			case "ork.store.request.duration":
				for _, dp := range m.Data.(metricdata.Histogram[float64]).DataPoints {
					durations += dp.Count
				}
			}
		}
	}
	if requests["put/audit/ok"] != 1 || requests["get/runs/not_found"] != 1 || len(requests) != 2 {
		t.Fatalf("requests = %v", requests)
	}
	if bytes != 5 || durations != 2 {
		t.Fatalf("bytes = %d, durations = %d; want 5 and 2", bytes, durations)
	}
}

// TestObserveStoreRequests_OnlyWithMetrics: with metrics off nothing is
// installed, so no request pays for an observer.
func TestObserveStoreRequests_OnlyWithMetrics(t *testing.T) {
	t.Cleanup(func() { artifacts.SetRequestObserver(nil) })
	if installed := ObserveStoreRequests(&Providers{Meter: sdkmetric.NewMeterProvider()}, nil); installed {
		t.Fatal("installed an observer with metrics off")
	}
	p := &Providers{Meter: sdkmetric.NewMeterProvider(), cfg: Config{MetricsExporters: []string{ExporterOTLP}}}
	if installed := ObserveStoreRequests(p, nil); !installed {
		t.Fatal("no observer with metrics on")
	}
}
