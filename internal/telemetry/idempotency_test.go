package telemetry

import (
	"context"
	"testing"

	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/orkstack/orkstack/internal/testsupport/must"
)

func TestIdempotencyInstruments_CountLegacyPending(t *testing.T) {
	providers, _, reader := newObserverProviders(t)
	inst, err := NewIdempotencyInstruments(providers)
	must.OKf(t, err, "NewIdempotencyInstruments")
	inst.CountLegacyPending(context.Background())
	inst.CountLegacyPending(context.Background())

	var got int64
	for _, scope := range collect(t, reader).ScopeMetrics {
		for _, m := range scope.Metrics {
			if m.Name != IdempotencyLegacyPendingMetric {
				continue
			}
			sum, ok := m.Data.(metricdata.Sum[int64])
			if !ok {
				t.Fatalf("%s is %T, want a counter", m.Name, m.Data)
			}
			for _, point := range sum.DataPoints {
				got += point.Value
			}
		}
	}
	if got != 2 {
		t.Fatalf("%s = %d, want 2", IdempotencyLegacyPendingMetric, got)
	}
	var none *IdempotencyInstruments
	none.CountLegacyPending(context.Background()) // a nil counter is a no-op
}
