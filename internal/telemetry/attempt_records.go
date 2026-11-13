package telemetry

import (
	"context"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"github.com/orkstack/orkstack/internal/orker"
)

// AttrAttemptRecordFound labels ork.orker.attempt_record_legacy with where the
// Orker met an earlier release's record: "claim" (a record with no claim in the
// scope it claimed in) or "record" (one already under the ordinal it claimed).
const AttrAttemptRecordFound = "ork.found"

// AttemptRecordMetrics is ork.orker.attempt_record_legacy, the one
// implementation of orker.AttemptRecordObserver (docs/DEPLOY.md, "Upgrading a
// pool's Orkers"): each time this Orker met an attempt record written by an
// Orker that numbers attempts as Temporal does, which is a pool running two
// releases. Every series carries ork.orker_id. Recording can fail nothing.
type AttemptRecordMetrics struct {
	legacy metric.Int64Counter
	orker  attribute.KeyValue
}

var _ orker.AttemptRecordObserver = (*AttemptRecordMetrics)(nil)

// NewAttemptRecordMetrics builds the counter from p's meter provider. It works,
// as a no-op, on nil or disabled Providers.
func NewAttemptRecordMetrics(p *Providers, orkerID string) (*AttemptRecordMetrics, error) {
	legacy, err := meterFor(p).Int64Counter("ork.orker.attempt_record_legacy",
		metric.WithDescription("Times this Orker met an attempt record written by an Orker of an earlier "+
			"release, which numbers attempts as Temporal does: its pool runs two releases."),
		metric.WithUnit("{record}"))
	if err != nil {
		return nil, err
	}
	return &AttemptRecordMetrics{legacy: legacy, orker: attribute.String(AttrOrkerID, orkerID)}, nil
}

// LegacyRecordsAtClaim counts the scope's records with no claim.
func (m *AttemptRecordMetrics) LegacyRecordsAtClaim(n int) { m.add(int64(n), "claim") }

// LegacyRecordKept counts a record already under this Orker's ordinal.
func (m *AttemptRecordMetrics) LegacyRecordKept() { m.add(1, "record") }

func (m *AttemptRecordMetrics) add(n int64, found string) {
	m.legacy.Add(context.Background(), n,
		metric.WithAttributes(m.orker, attribute.String(AttrAttemptRecordFound, found)))
}
