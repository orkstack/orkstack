package telemetry

import (
	"context"
	"log/slog"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"github.com/orkstack/orkstack/internal/archive"
)

// The retention backstop's instrument (docs/DEPLOY.md, "Running without
// Temporal admin rights").
const (
	RetentionBackstopRefusedMetric = "ork.retention.backstop.refused"
	AttrRetentionOutcome           = "outcome"
)

// The closed set of outcome values: covered, the cluster keeps closed runs
// at least as long as the backstop; short, it keeps them for less.
const (
	RetentionOutcomeCovered = "covered"
	RetentionOutcomeShort   = "short"
)

// RetentionObserver counts the cluster refusing the engine the right to set
// its namespace's retention. It implements archive.RetentionObserver; a record
// never blocks or fails the reconcile.
type RetentionObserver struct {
	refused metric.Int64Counter
}

var _ archive.RetentionObserver = (*RetentionObserver)(nil)

// NewRetentionObserver registers the instrument on p, the default sink. An
// instrument that cannot be built is logged, and the observer records nothing.
func NewRetentionObserver(p *Providers, logger *slog.Logger) *RetentionObserver {
	if logger == nil {
		logger = slog.Default()
	}
	refused, err := meterFor(p).Int64Counter(RetentionBackstopRefusedMetric,
		metric.WithDescription("The cluster refusing the engine the right to set its namespace's retention, by outcome: covered (it already keeps runs as long as the backstop) or short (it keeps them for less)."),
		metric.WithUnit("{refusal}"))
	if err != nil {
		logger.Warn("telemetry: the retention backstop instrument could not be built; refusals are only logged", "error", err)
		return &RetentionObserver{}
	}
	return &RetentionObserver{refused: refused}
}

// ObserveBackstopRefused counts one refusal.
func (o *RetentionObserver) ObserveBackstopRefused(ctx context.Context, covered bool) {
	if o == nil || o.refused == nil {
		return
	}
	outcome := RetentionOutcomeShort
	if covered {
		outcome = RetentionOutcomeCovered
	}
	o.refused.Add(ctx, 1, metric.WithAttributes(attribute.String(AttrRetentionOutcome, outcome)))
}
