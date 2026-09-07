package telemetry

import (
	"context"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	sdklog "go.opentelemetry.io/otel/sdk/log"
)

// countingLogExporter wraps a sdklog.Exporter to count its Export calls,
// outcome=ok|error, on TaskLogExports. One increment per batch — not per
// record — because a batch is the unit the collector either accepted or
// refused; a partial batch failure is still one refused Export call.
type countingLogExporter struct {
	sdklog.Exporter
	exports metric.Int64Counter
}

func (e *countingLogExporter) Export(ctx context.Context, records []sdklog.Record) error {
	err := e.Exporter.Export(ctx, records)
	outcome := OutcomeOK
	if err != nil {
		outcome = OutcomeError
	}
	e.exports.Add(ctx, 1, metric.WithAttributes(attribute.String(AttrOutcome, outcome)))
	return err
}
