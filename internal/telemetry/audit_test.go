package telemetry

import (
	"context"
	"testing"

	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

func TestAuditInstruments_CountByActionAndOutcome(t *testing.T) {
	providers, _, reader := newObserverProviders(t)
	inst, err := NewAuditInstruments(providers)
	if err != nil {
		t.Fatalf("NewAuditInstruments: %v", err)
	}
	ctx := context.Background()
	inst.CountAudit(ctx, "namespace.update", "written")
	inst.CountAudit(ctx, "namespace.update", "written")
	inst.CountAudit(ctx, "retention.update", "dropped")

	got := map[string]int64{}
	for _, scope := range collect(t, reader).ScopeMetrics {
		for _, m := range scope.Metrics {
			if m.Name != AuditRecordsMetric {
				continue
			}
			sum, ok := m.Data.(metricdata.Sum[int64])
			if !ok {
				t.Fatalf("%s is %T, want a counter", m.Name, m.Data)
			}
			for _, point := range sum.DataPoints {
				action, _ := point.Attributes.Value("action")
				outcome, _ := point.Attributes.Value("outcome")
				got[action.AsString()+"/"+outcome.AsString()] += point.Value
			}
		}
	}
	if got["namespace.update/written"] != 2 || got["retention.update/dropped"] != 1 || len(got) != 2 {
		t.Fatalf("ork.audit.records = %v, want namespace.update/written=2 and retention.update/dropped=1", got)
	}

	// Nil providers and a nil instrument set are no-ops, never a panic.
	noop, err := NewAuditInstruments(nil)
	if err != nil {
		t.Fatalf("NewAuditInstruments(nil): %v", err)
	}
	noop.CountAudit(ctx, "x", "written")
	var none *AuditInstruments
	none.CountAudit(ctx, "x", "written")
}
