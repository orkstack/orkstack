package telemetry

import (
	"context"
	"errors"
	"testing"
	"time"

	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/orkstack/orkstack/internal/testsupport/must"
)

func clockSkew(t *testing.T, reader sdkmetric.Reader) (float64, bool) {
	t.Helper()
	var rm metricdata.ResourceMetrics
	must.OK(t, reader.Collect(context.Background(), &rm))
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name == StoreClockSkewMetric {
				points := m.Data.(metricdata.Gauge[float64]).DataPoints
				if len(points) == 1 {
					return points[0].Value, true
				}
			}
		}
	}
	return 0, false
}

// TestStoreClock_ProbeAndSkewGauge: the gauge reports the last good reading
// in seconds, and nothing while the last probe failed, so a stale reading is
// never reported as current. The probe runs at start and once per interval.
func TestStoreClock_ProbeAndSkewGauge(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	p := &Providers{Meter: sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))}
	gauge := NewStoreClockGauge(p, nil)

	gauge.Record(3*time.Second, nil)
	if v, ok := clockSkew(t, reader); !ok || v != 3 {
		t.Fatalf("skew = %v (reported %v); want 3", v, ok)
	}
	gauge.Record(0, errors.New("store unreachable"))
	if v, ok := clockSkew(t, reader); ok {
		t.Fatalf("a failed probe still reports %v", v)
	}
	gauge.Record(-1500*time.Millisecond, nil)
	if v, _ := clockSkew(t, reader); v != -1.5 {
		t.Fatalf("skew = %v; want -1.5", v)
	}

	ctx, cancel := context.WithCancel(context.Background())
	ticks := make(chan time.Time)
	calls := 0
	done := make(chan struct{})
	go func() {
		defer close(done)
		runStoreClockProbe(ctx, ticks, time.Minute, func(context.Context) (time.Duration, error) {
			calls++
			return time.Duration(calls) * time.Second, nil
		}, gauge)
	}()
	ticks <- time.Now()
	ticks <- time.Now() // the second send returns only once the first tick's probe ran
	cancel()
	<-done
	if calls < 2 {
		t.Fatalf("probes = %d; want the start one and one per tick", calls)
	}
	if v, _ := clockSkew(t, reader); v < 2 {
		t.Fatalf("skew = %v; want the latest probe's", v)
	}
}
