package telemetry

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	sdkmetric "go.opentelemetry.io/otel/sdk/metric"

	"github.com/orkstack/orkstack/internal/testsupport/must"
)

func TestStoreOutageMetrics_ReportEntryExitAndTheCurrentState(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	t.Cleanup(func() { _ = mp.Shutdown(context.Background()) })
	var active atomic.Bool
	m, err := NewStoreOutageMetrics(&Providers{Meter: mp}, "orker-a", active.Load, nil)
	must.OK(t, err)

	if got := metricPoints(t, reader)["ork.orker.store_outage.active"]; len(got) != 1 || got[0].value != 0 || attrOf(got[0], AttrOrkerID) != "orker-a" {
		t.Fatalf("active before an outage = %+v, want one 0 for orker-a", got)
	}
	active.Store(true)
	m.StoreOutageEntered()
	if got := metricPoints(t, reader)["ork.orker.store_outage.active"]; len(got) != 1 || got[0].value != 1 {
		t.Fatalf("active during an outage = %+v, want 1", got)
	}
	active.Store(false)
	m.StoreOutageLeft(time.Minute)

	transitions := map[string]float64{}
	for _, p := range metricPoints(t, reader)["ork.orker.store_outage.transitions"] {
		transitions[attrOf(p, AttrStoreOutageTransition)] = p.value
	}
	if transitions["entered"] != 1 || transitions["left"] != 1 {
		t.Fatalf("transitions = %v, want one entered and one left", transitions)
	}
}

// Telemetry that is off is a no-op that never reads the state and never fails.
func TestStoreOutageMetrics_DisabledIsANoOp(t *testing.T) {
	m, err := NewStoreOutageMetrics(nil, "orker-a", func() bool {
		t.Error("disabled metrics read the cordon")
		return false
	}, nil)
	must.OK(t, err)
	m.StoreOutageEntered()
	m.StoreOutageLeft(time.Minute)
}
