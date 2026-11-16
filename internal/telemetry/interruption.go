package telemetry

import (
	"context"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"github.com/orkstack/orkstack/internal/orker"
)

// AttrInterruptionOutcome labels ork.orker.interruption_emission: "emitted"
// or "declined".
const AttrInterruptionOutcome = "outcome"

// InterruptionMetrics is ork.orker.interruption_emission (E92 §4.5), the one
// implementation of orker.InterruptionObserver: each typed interruption an
// Orker emitted because the attempt's retry policy listed it, and each listed
// one it declined. A process-level signal, so the default sink's, labelled
// with the Orker. Recording fails nothing.
type InterruptionMetrics struct {
	emissions metric.Int64Counter
	orker     attribute.KeyValue
}

var _ orker.InterruptionObserver = (*InterruptionMetrics)(nil)

// NewInterruptionMetrics builds the counter from p's meter provider; a no-op
// on nil or disabled Providers.
func NewInterruptionMetrics(p *Providers, orkerID string) (*InterruptionMetrics, error) {
	emissions, err := meterFor(p).Int64Counter("ork.orker.interruption_emission",
		metric.WithDescription("Typed interruptions this Orker emitted because an attempt's retry policy listed them, "+
			"and listed ones it declined, by outcome."),
		metric.WithUnit("{interruption}"))
	if err != nil {
		return nil, err
	}
	return &InterruptionMetrics{emissions: emissions, orker: attribute.String(AttrOrkerID, orkerID)}, nil
}

// InterruptionEmission counts one decision: emitted, or declined.
func (m *InterruptionMetrics) InterruptionEmission(emitted bool) {
	outcome := "declined"
	if emitted {
		outcome = "emitted"
	}
	m.emissions.Add(context.Background(), 1,
		metric.WithAttributes(m.orker, attribute.String(AttrInterruptionOutcome, outcome)))
}
