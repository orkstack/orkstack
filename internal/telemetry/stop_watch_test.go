package telemetry

import (
	"context"
	"testing"

	sdkmetric "go.opentelemetry.io/otel/sdk/metric"

	"github.com/orkstack/orkstack/internal/orker"
	"github.com/orkstack/orkstack/internal/testsupport/must"
)

// TestStopWatchMetrics_CountsEachFailureByReason: ork.orker.stop_watch.failures
// counts each attempt whose container's event stream could not be opened, by
// the step that failed, labelled with the Orker; its reason label holds only
// anchor, pipe, start and exit.
func TestStopWatchMetrics_CountsEachFailureByReason(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	t.Cleanup(func() { _ = mp.Shutdown(context.Background()) })
	m, err := NewStopWatchMetrics(&Providers{Meter: mp}, "orker-a")
	must.OK(t, err)
	m.StopWatchFailure(orker.StopWatchAnchor)
	m.StopWatchFailure(orker.StopWatchAnchor)
	m.StopWatchFailure(orker.StopWatchPipe)
	m.StopWatchFailure(orker.StopWatchStart)
	m.StopWatchFailure(orker.StopWatchExit)
	m.StopWatchFailure(orker.StopWatchExit)
	m.StopWatchFailure("something new")
	got := map[string]float64{}
	for _, p := range metricPoints(t, reader)["ork.orker.stop_watch.failures"] {
		if attrOf(p, AttrOrkerID) != "orker-a" {
			t.Fatalf("point %+v; want orker-a", p)
		}
		got[attrOf(p, AttrStopWatchReason)] = p.value
	}
	want := map[string]float64{"anchor": 2, "pipe": 1, "start": 1, "exit": 2}
	if len(got) != len(want) {
		t.Fatalf("stop_watch.failures by reason = %v; want %v and no other reason", got, want)
	}
	for reason, n := range want {
		if got[reason] != n {
			t.Fatalf("stop_watch.failures by reason = %v; want %v", got, want)
		}
	}
}

// Off, it is a no-op that never fails.
func TestStopWatchMetrics_DisabledIsANoOp(t *testing.T) {
	m, err := NewStopWatchMetrics(nil, "orker-a")
	must.OK(t, err)
	m.StopWatchFailure(orker.StopWatchStart)
}
