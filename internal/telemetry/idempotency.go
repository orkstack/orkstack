package telemetry

import (
	"context"

	"go.opentelemetry.io/otel/metric"
	metricnoop "go.opentelemetry.io/otel/metric/noop"
)

// IdempotencyLegacyPendingMetric counts keyed submissions answered with a
// pending run that could not be found, under a key claimed by v0.3.0 (or
// answered by a v0.3.0 engine during a rollout): a run that may never start,
// made visible while the rollout is still in hand (docs/DEPLOY.md §5).
const IdempotencyLegacyPendingMetric = "ork.idempotency.legacy_pending"

// IdempotencyInstruments counts what the submit path's idempotency keys need
// an operator to see. It works, as a no-op, on disabled or nil Providers.
type IdempotencyInstruments struct {
	LegacyPending metric.Int64Counter
}

// NewIdempotencyInstruments creates the counter from p's meter provider.
func NewIdempotencyInstruments(p *Providers) (*IdempotencyInstruments, error) {
	var meter metric.Meter
	if p != nil {
		meter = p.Meter.Meter(ScopeName)
	} else {
		meter = metricnoop.NewMeterProvider().Meter(ScopeName)
	}
	pending, err := meter.Int64Counter(IdempotencyLegacyPendingMetric,
		metric.WithDescription("Keyed submissions answered with a pending run that was not found, under a key claimed by an earlier release."),
		metric.WithUnit("{submission}"))
	if err != nil {
		return nil, err
	}
	return &IdempotencyInstruments{LegacyPending: pending}, nil
}

// CountLegacyPending records one such answer. It never blocks and never
// fails: telemetry cannot fail a submission (AGENTS.md §11).
func (i *IdempotencyInstruments) CountLegacyPending(ctx context.Context) {
	if i == nil || i.LegacyPending == nil {
		return
	}
	i.LegacyPending.Add(ctx, 1)
}
