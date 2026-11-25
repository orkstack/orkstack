package telemetry

import (
	"bytes"
	"context"
	"testing"

	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

func TestRecordResumeAnomaly_CountsByNamespaceAndKind(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	var buf bytes.Buffer
	o := newTestEngineObserver(t, reader, &buf, false)
	ctx := context.Background()
	o.RecordResumeAnomaly(ctx, "team-a", "reset_unreadable")
	o.RecordResumeAnomaly(ctx, "team-a", "reset_unreadable")
	var nilObserver *EngineObserver
	nilObserver.RecordResumeAnomaly(ctx, "team-a", "reset_unreadable") // no observer, nothing recorded

	var got int64
	for _, sm := range collect(t, reader).ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name != ResumeAnomaliesMetric {
				continue
			}
			for _, dp := range m.Data.(metricdata.Sum[int64]).DataPoints {
				kind, _ := dp.Attributes.Value(AttrResumeAnomalyKind)
				ns, _ := dp.Attributes.Value(AttrNamespace)
				if kind.AsString() == "reset_unreadable" && ns.AsString() == "team-a" {
					got += dp.Value
				}
			}
		}
	}
	if got != 2 {
		t.Fatalf("%s{kind=reset_unreadable} = %d, want 2", ResumeAnomaliesMetric, got)
	}
}
