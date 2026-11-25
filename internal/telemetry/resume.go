package telemetry

import (
	"context"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

// The resume counters (docs/DEPLOY.md §7, "Run and task metrics").
const (
	ResumeAnomaliesMetric = "ork.run.resume.anomalies"
	// AttrResumeAnomalyKind is what the anomaly was.
	AttrResumeAnomalyKind = "kind"
)

// RecordResumeAnomaly counts one anomaly a resumed run met, of kind (for
// example reset_unreadable: a reset run that could not read the reset that
// made it, and failed with no event).
func (o *EngineObserver) RecordResumeAnomaly(ctx context.Context, namespace, kind string) {
	if o == nil {
		return
	}
	inst := o.namespaceInstruments(ctx, namespace)
	if inst == nil {
		return
	}
	inst.ResumeAnomalies.Add(ctx, 1, metric.WithAttributes(
		attribute.String(AttrNamespace, namespace), attribute.String(AttrResumeAnomalyKind, kind)))
}
