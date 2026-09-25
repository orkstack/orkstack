package telemetry

import (
	"context"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	metricnoop "go.opentelemetry.io/otel/metric/noop"
)

// AuditRecordsMetric counts control-plane audit writes by action and outcome.
const AuditRecordsMetric = "ork.audit.records"

// AuditInstruments counts audit writes on the process's default providers. It
// satisfies internal/api's AuditCounter seam, which is how internal/api stays
// free of any OpenTelemetry import.
type AuditInstruments struct {
	Records metric.Int64Counter // "ork.audit.records"
}

// NewAuditInstruments creates the audit counter from p's meter provider. It
// works, as a no-op, on disabled or nil Providers.
func NewAuditInstruments(p *Providers) (*AuditInstruments, error) {
	var meter metric.Meter
	if p != nil {
		meter = p.Meter.Meter(ScopeName)
	} else {
		meter = metricnoop.NewMeterProvider().Meter(ScopeName)
	}
	records, err := meter.Int64Counter(AuditRecordsMetric,
		metric.WithDescription("Control-plane audit record writes, by action and outcome (written, or dropped when the audit store refused the write)."),
		metric.WithUnit("{record}"))
	if err != nil {
		return nil, err
	}
	return &AuditInstruments{Records: records}, nil
}

// CountAudit records one attempt to write an audit record. It never blocks
// and never fails: telemetry cannot fail a write RPC (AGENTS.md §11).
func (a *AuditInstruments) CountAudit(ctx context.Context, action, outcome string) {
	if a == nil || a.Records == nil {
		return
	}
	a.Records.Add(ctx, 1, metric.WithAttributes(
		attribute.String("action", action),
		attribute.String("outcome", outcome),
	))
}
