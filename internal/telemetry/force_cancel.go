package telemetry

import (
	"context"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	metricnoop "go.opentelemetry.io/otel/metric/noop"
)

// ForceCancelsMetric counts force cancels by what became of them. It names no
// namespace, so it goes to the operator's sink alone.
const ForceCancelsMetric = "ork.runs.force_canceled"

// AttrCancelOutcome labels it: a closed set of OrkStack's own words.
const AttrCancelOutcome = "ork.cancel.outcome"

// ForceCancelInstruments counts force cancels on the process's default
// providers (engine.ForceCancelCounter).
type ForceCancelInstruments struct {
	runs metric.Int64Counter
}

// NewForceCancelInstruments creates the counter from p's meter provider. It
// works, as a no-op, on disabled or nil Providers.
func NewForceCancelInstruments(p *Providers) (*ForceCancelInstruments, error) {
	var meter metric.Meter
	if p != nil {
		meter = p.Meter.Meter(ScopeName)
	} else {
		meter = metricnoop.NewMeterProvider().Meter(ScopeName)
	}
	runs, err := meter.Int64Counter(ForceCancelsMetric,
		metric.WithDescription("Force cancels that ended, by outcome, and what requests for one found named as one."),
		metric.WithUnit("{run}"))
	if err != nil {
		return nil, err
	}
	return &ForceCancelInstruments{runs: runs}, nil
}

// CountForceCancel counts one. It never blocks and never fails.
func (f *ForceCancelInstruments) CountForceCancel(ctx context.Context, outcome string) {
	if f == nil || f.runs == nil {
		return
	}
	f.runs.Add(ctx, 1, metric.WithAttributes(attribute.String(AttrCancelOutcome, outcome)))
}
