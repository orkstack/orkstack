package telemetry

import (
	"context"
	"log/slog"
	"sync/atomic"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"github.com/orkstack/orkstack/internal/orker"
)

// AttrAdmissionBound labels ork.orker.admission.wait.duration with what held
// a poll that waited: "capacity" (orker.capacity) or "slots" (every
// max_concurrent slot taken). A bounded, two-valued label.
const AttrAdmissionBound = "ork.admission.bound"

// CapacityMetrics is the Orker's capacity admission metrics (docs/DEPLOY.md
// "Declaring an Orker's capacity"), the one implementation of
// orker.CapacityObserver. They are process-level signals, so they go to the
// default sink only, and only an Orker that declares a capacity has any:
//
//   - ork.orker.capacity.cpu and ork.orker.capacity.memory: what it admits
//     tasks within, one gauge per dimension it bounds;
//   - ork.orker.capacity.reserved.cpu and .memory: what stands reserved
//     against it, the figure DescribeOrker reports;
//   - ork.orker.admission.wait.duration: how long a poll for a task waited for
//     room before it was granted, by ork.admission.bound;
//   - ork.orker.admission.refusals: attempts refused
//     RESOURCE_ADMISSION_MISMATCH.
//
// Every series carries ork.orker_id, one per process. The gauges read the
// ledger when the meter collects, under the ledger's own short lock, and are
// never a stored copy (AGENTS.md §7). Recording is best-effort and never
// blocks admission (AGENTS.md §11): an instrument that cannot be built is
// logged and records nothing, and disabled Providers make every call a no-op.
type CapacityMetrics struct {
	meter    metric.Meter
	logger   *slog.Logger
	waits    metric.Float64Histogram
	refusals metric.Int64Counter
	// orker is the Orker's id label, set by Watch before any poll is made.
	orker atomic.Pointer[attribute.Set]
}

var _ orker.CapacityObserver = (*CapacityMetrics)(nil)

// NewCapacityMetrics creates the wait and refusal instruments from p's meter
// provider; Watch adds the gauges. It works, as a no-op, on nil or disabled
// Providers. logger, when set, records what cannot be measured.
func NewCapacityMetrics(p *Providers, logger *slog.Logger) (*CapacityMetrics, error) {
	meter := meterFor(p)
	waits, err := meter.Float64Histogram("ork.orker.admission.wait.duration",
		metric.WithDescription("How long a poll for a task waited for room before it was granted, by what held it."),
		metric.WithUnit("s"))
	if err != nil {
		return nil, err
	}
	refusals, err := meter.Int64Counter("ork.orker.admission.refusals",
		metric.WithDescription("Attempts refused because their request was larger than the reservation they were polled under, with no room for the difference."),
		metric.WithUnit("{attempt}"))
	if err != nil {
		return nil, err
	}
	return &CapacityMetrics{meter: meter, logger: logger, waits: waits, refusals: refusals}, nil
}

// Watch registers the capacity and reserved gauges for orkerID, each read
// from read at collection. A dimension the capacity leaves unbounded (0) has
// no capacity point, and is still reported reserved.
func (m *CapacityMetrics) Watch(orkerID string, read func() orker.CapacityReading) {
	set := attribute.NewSet(attribute.String(AttrOrkerID, orkerID))
	m.orker.Store(&set)
	capacityCPU, err1 := m.meter.Float64ObservableGauge("ork.orker.capacity.cpu",
		metric.WithDescription("The cpu this Orker admits tasks within (orker.capacity.cpu)."),
		metric.WithUnit("{cpu}"))
	capacityMemory, err2 := m.meter.Int64ObservableGauge("ork.orker.capacity.memory",
		metric.WithDescription("The memory this Orker admits tasks within (orker.capacity.memory)."),
		metric.WithUnit("By"))
	reservedCPU, err3 := m.meter.Float64ObservableGauge("ork.orker.capacity.reserved.cpu",
		metric.WithDescription("The cpu reserved against this Orker's capacity: each outstanding poll at the per-task cap, and each task's request."),
		metric.WithUnit("{cpu}"))
	reservedMemory, err4 := m.meter.Int64ObservableGauge("ork.orker.capacity.reserved.memory",
		metric.WithDescription("The memory reserved against this Orker's capacity: each outstanding poll at the per-task cap, and each task's request."),
		metric.WithUnit("By"))
	for _, err := range []error{err1, err2, err3, err4} {
		if err != nil {
			m.warn("telemetry: the Orker's capacity gauges could not be built; capacity is not exported", err)
			return
		}
	}
	_, err := m.meter.RegisterCallback(func(_ context.Context, o metric.Observer) error {
		r := read()
		attrs := metric.WithAttributeSet(set)
		if r.CapacityCPU > 0 {
			o.ObserveFloat64(capacityCPU, r.CapacityCPU, attrs)
		}
		if r.CapacityMemory > 0 {
			o.ObserveInt64(capacityMemory, r.CapacityMemory, attrs)
		}
		o.ObserveFloat64(reservedCPU, r.ReservedCPU, attrs)
		o.ObserveInt64(reservedMemory, r.ReservedMemory, attrs)
		return nil
	}, capacityCPU, capacityMemory, reservedCPU, reservedMemory)
	if err != nil {
		m.warn("telemetry: the Orker's capacity gauges could not be registered; capacity is not exported", err)
	}
}

// PollWaited records one granted poll's wait, in seconds.
func (m *CapacityMetrics) PollWaited(waited time.Duration, bound string) {
	m.waits.Record(context.Background(), waited.Seconds(),
		metric.WithAttributes(append(m.orkerAttrs(), attribute.String(AttrAdmissionBound, bound))...))
}

// AdmissionRefused counts one RESOURCE_ADMISSION_MISMATCH refusal.
func (m *CapacityMetrics) AdmissionRefused() {
	m.refusals.Add(context.Background(), 1, metric.WithAttributes(m.orkerAttrs()...))
}

func (m *CapacityMetrics) orkerAttrs() []attribute.KeyValue {
	if set := m.orker.Load(); set != nil {
		return set.ToSlice()
	}
	return nil
}

func (m *CapacityMetrics) warn(msg string, err error) {
	if m.logger != nil {
		m.logger.Warn(msg, "error", err)
	}
}
