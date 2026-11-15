package telemetry

import (
	"context"
	"maps"
	"sync"
	"testing"

	sdkmetric "go.opentelemetry.io/otel/sdk/metric"

	"github.com/orkstack/orkstack/internal/orker"
)

// The Orker's load gauges (docs/DEPLOY.md "Metrics") read the Orker when the
// meter collects, never a stored copy, each labelled with the Orker and its
// pool, so a dashboard sums them by pool.
func TestLoadMetrics_ReadTheOrkerAtCollection(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	t.Cleanup(func() { _ = mp.Shutdown(context.Background()) })
	var mu sync.Mutex
	reading := orker.LoadReading{RunningTasks: 3, OpenPins: 1, MaxConcurrent: 4, Stopping: true}
	WatchOrkerLoad(&Providers{Meter: mp}, "orker-a", "build", func() orker.LoadReading {
		mu.Lock()
		defer mu.Unlock()
		return reading
	}, nil)

	points := metricPoints(t, reader)
	for name, want := range map[string]float64{
		"ork.orker.running_tasks":  3,
		"ork.orker.open_pins":      1,
		"ork.orker.max_concurrent": 4,
	} {
		got := points[name]
		if len(got) != 1 || got[0].value != want || attrOf(got[0], AttrOrkerID) != "orker-a" || attrOf(got[0], AttrPool) != "build" {
			t.Fatalf("%s = %+v; want one %v for orker-a on pool build", name, got, want)
		}
	}
	// One series per cause, 0 or 1, so a cause that does not hold reads 0
	// rather than missing.
	cordoned := cordonedByCause(t, points)
	if want := map[string]float64{"operator": 0, "store_outage": 0, "stopping": 1}; !maps.Equal(cordoned, want) {
		t.Fatalf("ork.orker.cordoned by cause = %v; want %v", cordoned, want)
	}

	// The next collection reads the Orker again.
	mu.Lock()
	reading = orker.LoadReading{MaxConcurrent: 4, Operator: true, StoreOutage: true}
	mu.Unlock()
	points = metricPoints(t, reader)
	if got := points["ork.orker.running_tasks"]; len(got) != 1 || got[0].value != 0 {
		t.Fatalf("running tasks after the task ended = %+v; want 0", got)
	}
	if want := map[string]float64{"operator": 1, "store_outage": 1, "stopping": 0}; !maps.Equal(cordonedByCause(t, points), want) {
		t.Fatalf("ork.orker.cordoned by cause = %v; want %v", cordonedByCause(t, points), want)
	}
}

// Telemetry that is off never reads the Orker and never fails.
func TestLoadMetrics_DisabledIsANoOp(t *testing.T) {
	WatchOrkerLoad(nil, "orker-a", "build", func() orker.LoadReading {
		t.Error("disabled metrics read the Orker")
		return orker.LoadReading{}
	}, nil)
}

func cordonedByCause(t *testing.T, points map[string][]metricPoint) map[string]float64 {
	t.Helper()
	out := map[string]float64{}
	for _, p := range points["ork.orker.cordoned"] {
		if attrOf(p, AttrOrkerID) != "orker-a" || attrOf(p, AttrPool) != "build" {
			t.Fatalf("ork.orker.cordoned point %+v; want orker-a on pool build", p)
		}
		out[attrOf(p, AttrCordonCause)] = p.value
	}
	return out
}
