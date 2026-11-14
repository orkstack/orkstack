package telemetry

import (
	"context"
	"testing"

	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/orkstack/orkstack/internal/testsupport/must"
)

func TestAuthInstruments_CountAndRecord(t *testing.T) {
	providers, _, reader := newObserverProviders(t)
	inst, err := NewAuthInstruments(providers)
	must.OKf(t, err, "NewAuthInstruments")
	ctx := context.Background()
	inst.CountRequest(ctx, "jwt", "refused")
	inst.CountDecision(ctx, "run.read", "denied")
	inst.CountDecision(ctx, "run.read", "denied")
	inst.CountStreamClosed(ctx, "expired")
	inst.CountBreakGlass(ctx)
	inst.CountModeRecordMissing(ctx)
	inst.SetFleetUnenforced(ctx, 2)

	got := map[string]int64{}
	for _, scope := range collect(t, reader).ScopeMetrics {
		for _, m := range scope.Metrics {
			switch data := m.Data.(type) {
			case metricdata.Sum[int64]:
				for _, p := range data.DataPoints {
					got[m.Name] += p.Value
				}
			case metricdata.Gauge[int64]:
				for _, p := range data.DataPoints {
					got[m.Name] = p.Value
				}
			}
		}
	}
	want := map[string]int64{
		AuthRequestsMetric: 1, AuthDecisionsMetric: 2, AuthStreamsClosedMetric: 1,
		AuthBreakGlassMetric: 1, AuthModeRecordMissingMetric: 1, AuthFleetUnenforcedMetric: 2,
	}
	for name, n := range want {
		if got[name] != n {
			t.Errorf("%s = %d, want %d (all: %v)", name, got[name], n, got)
		}
	}

	noop, err := NewAuthInstruments(nil)
	must.OKf(t, err, "NewAuthInstruments(nil)")
	noop.CountDecision(ctx, "x", "allowed")
	var none *AuthInstruments
	none.CountDecision(ctx, "x", "allowed")
	none.SetFleetUnenforced(ctx, 1)
}

func TestAuthInstruments_PolicyAndSpans(t *testing.T) {
	providers, spans, reader := newObserverProviders(t)
	inst, err := NewAuthInstruments(providers)
	must.OKf(t, err, "NewAuthInstruments")
	ctx := context.Background()
	must.OKf(t, inst.WatchPolicy(func() (int64, float64) { return 42, 7.5 }), "WatchPolicy")
	inst.CountPolicyRefresh(ctx, "failed")
	inst.CountDocumentUnexpected(ctx)

	ints, floats := map[string]int64{}, map[string]float64{}
	for _, scope := range collect(t, reader).ScopeMetrics {
		for _, m := range scope.Metrics {
			switch data := m.Data.(type) {
			case metricdata.Sum[int64]:
				for _, p := range data.DataPoints {
					ints[m.Name] += p.Value
				}
			case metricdata.Gauge[int64]:
				for _, p := range data.DataPoints {
					ints[m.Name] = p.Value
				}
			case metricdata.Gauge[float64]:
				for _, p := range data.DataPoints {
					floats[m.Name] = p.Value
				}
			}
		}
	}
	if ints[AuthPolicyGenerationMetric] != 42 || floats[AuthPolicyAgeMetric] != 7.5 ||
		ints[AuthPolicyRefreshMetric] != 1 || ints[AuthDocumentUnexpectedMetric] != 1 {
		t.Fatalf("ints %v floats %v", ints, floats)
	}

	ctx, span := providers.Tracer.Tracer("t").Start(ctx, "request")
	inst.Annotate(ctx, "user", "jwt", "allowed")
	span.End()
	got := map[string]string{}
	for _, s := range spans.GetSpans() {
		for _, kv := range s.Attributes {
			got[string(kv.Key)] = kv.Value.AsString()
		}
	}
	if got["ork.principal.kind"] != "user" || got["ork.auth.method"] != "jwt" || got["ork.auth.decision"] != "allowed" {
		t.Fatalf("span attributes = %v", got)
	}
	var none *AuthInstruments
	none.Annotate(ctx, "", "", "")
	none.CountPolicyRefresh(ctx, "ok")
}
