package telemetry

import (
	"context"
	"testing"

	sdkmetric "go.opentelemetry.io/otel/sdk/metric"

	"github.com/orkstack/orkstack/internal/testsupport/must"
)

func TestAttemptRecordMetrics_CountWhereALegacyRecordWasMet(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	t.Cleanup(func() { _ = mp.Shutdown(context.Background()) })
	m, err := NewAttemptRecordMetrics(&Providers{Meter: mp}, "orker-a")
	must.OK(t, err)
	m.LegacyRecordsAtClaim(3)
	m.LegacyRecordKept()

	found := map[string]float64{}
	for _, p := range metricPoints(t, reader)["ork.orker.attempt_record_legacy"] {
		if attrOf(p, AttrOrkerID) != "orker-a" {
			t.Fatalf("point without the orker id: %+v", p)
		}
		found[attrOf(p, AttrAttemptRecordFound)] = p.value
	}
	if found["claim"] != 3 || found["record"] != 1 {
		t.Fatalf("counts = %v, want claim 3 and record 1", found)
	}
}
