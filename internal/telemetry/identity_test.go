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

// TestIdentityMetrics_CountMints: each mint is counted under its outcome, a
// reason outside the closed set as "other", and an engine refusal also in
// ork.identity.engine_refused.
func TestIdentityMetrics_CountMints(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	t.Cleanup(func() { _ = mp.Shutdown(context.Background()) })
	m, err := NewIdentityMetrics(&Providers{Meter: mp})
	must.OK(t, err)
	m.Minted("granted")
	m.Minted("IDENTITY_ENGINE_REFUSED")
	m.Minted("SOMETHING_NEW")
	got := map[string]float64{}
	for _, p := range metricPoints(t, reader)["ork.identity.mints"] {
		reason, _ := p.attrs.Value("reason")
		got[reason.AsString()] += p.value
	}
	if got["granted"] != 1 || got["IDENTITY_ENGINE_REFUSED"] != 1 || got["other"] != 1 || got["SOMETHING_NEW"] != 0 {
		t.Fatalf("mints = %v", got)
	}
	if points := metricPoints(t, reader)["ork.identity.engine_refused"]; len(points) != 1 || points[0].value != 1 {
		t.Fatalf("engine_refused = %+v", points)
	}
	off, err := NewIdentityMetrics(nil)
	must.OK(t, err)
	off.Minted("granted")
}
