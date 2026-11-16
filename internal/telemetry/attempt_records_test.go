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

// TestAttemptRecordMetrics_CountEachWrite: each attempt's record write is
// counted by pool and outcome, so an attempt left with no record is visible.
func TestAttemptRecordMetrics_CountEachWrite(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	t.Cleanup(func() { _ = mp.Shutdown(context.Background()) })
	m, err := NewAttemptRecordMetrics(&Providers{Meter: mp}, "orker-a")
	must.OK(t, err)
	m.AttemptRecordWritten("gpu", true)
	m.AttemptRecordWritten("gpu", true)
	m.AttemptRecordWritten("gpu", false)
	got := map[string]float64{}
	for _, p := range metricPoints(t, reader)[AttemptRecordsMetric] {
		if attrOf(p, AttrOrkerID) != "orker-a" || attrOf(p, AttrPool) != "gpu" {
			t.Fatalf("point without the orker id or pool: %+v", p)
		}
		got[attrOf(p, AttrOutcome)] = p.value
	}
	if got["written"] != 2 || got["failed"] != 1 {
		t.Fatalf("counts = %v, want written 2 and failed 1", got)
	}
}

// TestOrkerPresence_GaugesAreReported: ork.orker.present is 1 and
// ork.orker.task_log.export_enabled says whether this Orker exports task
// logs by default, each by its Orker id and pool, at every collection.
func TestOrkerPresence_GaugesAreReported(t *testing.T) {
	for _, export := range []bool{true, false} {
		reader := sdkmetric.NewManualReader()
		mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
		WatchOrkerPresence(&Providers{Meter: mp}, "orker-a", "gpu", export, nil)
		want := map[string]float64{OrkerPresentMetric: 1, OrkerTaskLogExportMetric: 0}
		if export {
			want[OrkerTaskLogExportMetric] = 1
		}
		for range 2 {
			points := metricPoints(t, reader)
			for name, v := range want {
				if len(points[name]) != 1 || points[name][0].value != v ||
					attrOf(points[name][0], AttrOrkerID) != "orker-a" || attrOf(points[name][0], AttrPool) != "gpu" {
					t.Fatalf("export %v: %s = %+v; want one point of %v for orker-a in gpu", export, name, points[name], v)
				}
			}
		}
		_ = mp.Shutdown(context.Background())
	}
}
