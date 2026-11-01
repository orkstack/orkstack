package telemetry

import (
	"context"
	"sync"
	"testing"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/orkstack/orkstack/internal/orker"
)

func newCapacityTestMetrics(t *testing.T) (*CapacityMetrics, *sdkmetric.ManualReader) {
	t.Helper()
	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	t.Cleanup(func() { _ = mp.Shutdown(context.Background()) })
	m, err := NewCapacityMetrics(&Providers{Meter: mp}, nil)
	if err != nil {
		t.Fatalf("NewCapacityMetrics: %v", err)
	}
	return m, reader
}

// metricPoint is one data point of a collection: its value (a histogram's
// sum), its count (a histogram's) and its attributes.
type metricPoint struct {
	value float64
	count uint64
	attrs attribute.Set
}

func metricPoints(t *testing.T, reader sdkmetric.Reader) map[string][]metricPoint {
	t.Helper()
	out := map[string][]metricPoint{}
	for _, sm := range collect(t, reader).ScopeMetrics {
		for _, m := range sm.Metrics {
			switch data := m.Data.(type) {
			case metricdata.Gauge[float64]:
				for _, dp := range data.DataPoints {
					out[m.Name] = append(out[m.Name], metricPoint{value: dp.Value, attrs: dp.Attributes})
				}
			case metricdata.Gauge[int64]:
				for _, dp := range data.DataPoints {
					out[m.Name] = append(out[m.Name], metricPoint{value: float64(dp.Value), attrs: dp.Attributes})
				}
			case metricdata.Sum[int64]:
				for _, dp := range data.DataPoints {
					out[m.Name] = append(out[m.Name], metricPoint{value: float64(dp.Value), attrs: dp.Attributes})
				}
			case metricdata.Histogram[float64]:
				for _, dp := range data.DataPoints {
					out[m.Name] = append(out[m.Name], metricPoint{value: dp.Sum, count: dp.Count, attrs: dp.Attributes})
				}
			}
		}
	}
	return out
}

func attrOf(p metricPoint, key string) string {
	v, _ := p.attrs.Value(attribute.Key(key))
	return v.AsString()
}

// The gauges read the ledger when the meter collects, never a stored copy:
// what the Orker admits within, per dimension it bounds, and what stands
// reserved, each labelled with the Orker.
func TestCapacityMetrics_GaugesReadTheLedgerAtCollection(t *testing.T) {
	m, reader := newCapacityTestMetrics(t)
	var mu sync.Mutex
	reading := orker.CapacityReading{CapacityCPU: 4, ReservedCPU: 3, ReservedMemory: 2 << 30}
	m.Watch("orker-a", func() orker.CapacityReading {
		mu.Lock()
		defer mu.Unlock()
		return reading
	})

	points := metricPoints(t, reader)
	for name, want := range map[string]float64{
		"ork.orker.capacity.cpu":             4,
		"ork.orker.capacity.reserved.cpu":    3,
		"ork.orker.capacity.reserved.memory": 2 << 30,
	} {
		got := points[name]
		if len(got) != 1 || got[0].value != want || attrOf(got[0], AttrOrkerID) != "orker-a" {
			t.Errorf("%s = %+v, want one point of %v for orker-a", name, got, want)
		}
	}
	if got := points["ork.orker.capacity.memory"]; len(got) != 0 {
		t.Errorf("ork.orker.capacity.memory = %+v, want no point: the capacity leaves memory unbounded", got)
	}

	mu.Lock()
	reading.ReservedCPU = 1
	mu.Unlock()
	if got := metricPoints(t, reader)["ork.orker.capacity.reserved.cpu"]; len(got) != 1 || got[0].value != 1 {
		t.Fatalf("reserved cpu after the ledger moved = %+v, want 1", got)
	}
}

// A poll's wait is recorded in seconds under what held it, and a refusal is
// counted.
func TestCapacityMetrics_RecordWaitsAndRefusals(t *testing.T) {
	m, reader := newCapacityTestMetrics(t)
	m.Watch("orker-a", func() orker.CapacityReading { return orker.CapacityReading{} })
	m.PollWaited(1500*time.Millisecond, orker.AdmissionBoundCapacity)
	m.PollWaited(500*time.Millisecond, orker.AdmissionBoundSlots)
	m.AdmissionRefused()

	points := metricPoints(t, reader)
	waits := map[string]metricPoint{}
	for _, p := range points["ork.orker.admission.wait.duration"] {
		waits[attrOf(p, AttrAdmissionBound)] = p
		if attrOf(p, AttrOrkerID) != "orker-a" {
			t.Errorf("a wait is labelled orker %q, want orker-a", attrOf(p, AttrOrkerID))
		}
	}
	if p := waits[orker.AdmissionBoundCapacity]; p.count != 1 || p.value != 1.5 {
		t.Errorf("capacity waits = %+v, want one of 1.5 s", p)
	}
	if p := waits[orker.AdmissionBoundSlots]; p.count != 1 || p.value != 0.5 {
		t.Errorf("slot waits = %+v, want one of 0.5 s", p)
	}
	if got := points["ork.orker.admission.refusals"]; len(got) != 1 || got[0].value != 1 {
		t.Errorf("ork.orker.admission.refusals = %+v, want 1", got)
	}
}

// Disabled telemetry is a no-op that never fails the Orker, and never reaches
// the process: nil Providers register no callback, so the ledger is not read
// for them (the gauges read it at collection), not even by a meter provider
// installed process-wide that a careless fallback would find, and nothing
// recorded reaches one.
func TestCapacityMetrics_DisabledProvidersRecordNothing(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	global := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	previous := otel.GetMeterProvider()
	otel.SetMeterProvider(global)
	t.Cleanup(func() {
		otel.SetMeterProvider(previous)
		_ = global.Shutdown(context.Background())
	})

	m, err := NewCapacityMetrics(nil, nil)
	if err != nil {
		t.Fatalf("NewCapacityMetrics(nil): %v", err)
	}
	reads := 0
	m.Watch("orker-a", func() orker.CapacityReading {
		reads++
		return orker.CapacityReading{CapacityCPU: 1}
	})
	m.PollWaited(time.Second, orker.AdmissionBoundCapacity)
	m.AdmissionRefused()

	points := metricPoints(t, reader)
	if reads != 0 {
		t.Errorf("the ledger was read %d times for disabled telemetry, want none: a callback was registered", reads)
	}
	if len(points) != 0 {
		t.Errorf("disabled telemetry reached the process-wide meter provider: %+v", points)
	}
}
