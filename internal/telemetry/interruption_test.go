package telemetry

import (
	"context"
	"testing"

	sdkmetric "go.opentelemetry.io/otel/sdk/metric"

	"github.com/orkstack/orkstack/internal/testsupport/must"
)

// TestInterruptionMetrics_CountsEmittedAndDeclined: ork.orker.interruption_emission
// counts each typed interruption the Orker emitted and each listed one it
// declined, by outcome, labelled with the Orker (E92 §4.5).
func TestInterruptionMetrics_CountsEmittedAndDeclined(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	t.Cleanup(func() { _ = mp.Shutdown(context.Background()) })
	m, err := NewInterruptionMetrics(&Providers{Meter: mp}, "orker-a")
	must.OK(t, err)
	m.InterruptionEmission(true)
	m.InterruptionEmission(true)
	m.InterruptionEmission(false)
	got := map[string]float64{}
	for _, p := range metricPoints(t, reader)["ork.orker.interruption_emission"] {
		if attrOf(p, AttrOrkerID) != "orker-a" {
			t.Fatalf("point %+v; want orker-a", p)
		}
		got[attrOf(p, AttrInterruptionOutcome)] = p.value
	}
	if got["emitted"] != 2 || got["declined"] != 1 {
		t.Fatalf("interruption_emission by outcome = %v; want emitted 2, declined 1", got)
	}
}

// Off, it is a no-op that never fails.
func TestInterruptionMetrics_DisabledIsANoOp(t *testing.T) {
	m, err := NewInterruptionMetrics(nil, "orker-a")
	must.OK(t, err)
	m.InterruptionEmission(true)
}
