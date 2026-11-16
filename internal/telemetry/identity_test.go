package telemetry

import (
	"context"
	"testing"

	sdkmetric "go.opentelemetry.io/otel/sdk/metric"

	"github.com/orkstack/orkstack/internal/testsupport/must"
)

// TestIdentityMetrics_CountStaleRefreshes: each stale refresh adds one to
// ork.identity.keys_copy_stale, and nil Providers record nothing and fail
// nothing.
func TestIdentityMetrics_CountStaleRefreshes(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	t.Cleanup(func() { _ = mp.Shutdown(context.Background()) })
	m, err := NewIdentityMetrics(&Providers{Meter: mp})
	must.OK(t, err)
	m.KeysCopyStale()
	m.KeysCopyStale()
	points := metricPoints(t, reader)["ork.identity.keys_copy_stale"]
	if len(points) != 1 || points[0].value != 2 {
		t.Fatalf("points = %+v, want one of 2", points)
	}
	off, err := NewIdentityMetrics(nil)
	must.OK(t, err)
	off.KeysCopyStale()
}
