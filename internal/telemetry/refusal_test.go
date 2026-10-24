package telemetry

import (
	"bytes"
	"context"
	"log/slog"
	"strings"
	"testing"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"

	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

// refusalPoints is every ork.runs.refused data point the reader holds.
func refusalPoints(t *testing.T, reader sdkmetric.Reader) []metricdata.DataPoint[int64] {
	t.Helper()
	return int64SumPoints(t, reader, "ork.runs.refused")
}

func int64SumPoints(t *testing.T, reader sdkmetric.Reader, name string) []metricdata.DataPoint[int64] {
	t.Helper()
	var out []metricdata.DataPoint[int64]
	for _, sm := range collect(t, reader).ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name != name {
				continue
			}
			sum, ok := m.Data.(metricdata.Sum[int64])
			if !ok {
				t.Fatalf("%s is %T, want a Sum[int64]", name, m.Data)
			}
			out = append(out, sum.DataPoints...)
		}
	}
	return out
}

func pointAttr(dp metricdata.DataPoint[int64], key string) (string, bool) {
	value, ok := dp.Attributes.Value(attribute.Key(key))
	if !ok {
		return "", false
	}
	return value.AsString(), true
}

func TestObserveRefusal_CountsOnTheNamespaceSinkAndNeverTouchesRuns(t *testing.T) {
	for _, highCard := range []bool{false, true} {
		t.Run(map[bool]string{false: "low cardinality", true: "high cardinality"}[highCard], func(t *testing.T) {
			reader := sdkmetric.NewManualReader()
			var buf bytes.Buffer
			observer := newTestEngineObserver(t, reader, &buf, highCard)
			ctx := context.Background()

			observer.ObserveRefusal(ctx, "payments", nil, "reconcile", "NAMESPACE_HELD")
			points := refusalPoints(t, reader)
			if len(points) != 1 || points[0].Value != 1 {
				t.Fatalf("ork.runs.refused points = %+v, want one point of value 1", points)
			}
			if got, _ := pointAttr(points[0], AttrNamespace); got != "payments" {
				t.Errorf("ork.namespace = %q, want payments", got)
			}
			if got, _ := pointAttr(points[0], AttrRefusalReason); got != "namespace_held" {
				t.Errorf("ork.refusal.reason = %q, want namespace_held", got)
			}
			if runs := int64SumPoints(t, reader, "ork.runs"); len(runs) != 0 {
				t.Errorf("ork.runs has %d points; a refusal is not a run", len(runs))
			}

			observer.ObserveRefusal(ctx, "payments", nil, "reconcile", "MAX_RUNNING_REACHED")
			observer.ObserveRefusal(ctx, "payments", nil, "reconcile", "BUDGET_EXCEEDED")
			observer.ObserveRefusal(ctx, "payments", nil, "reconcile", "BUDGET_UNMEASURABLE")
			points = refusalPoints(t, reader)
			if len(points) != 4 {
				t.Fatalf("ork.runs.refused points = %d, want four series", len(points))
			}
			reasons := map[string]int64{}
			for _, dp := range points {
				reason, _ := pointAttr(dp, AttrRefusalReason)
				reasons[reason] = dp.Value
				name, hasName := pointAttr(dp, AttrOrkflowName)
				if hasName != highCard || (highCard && name != "reconcile") {
					t.Errorf("ork.orkflow.name = %q (present %v), want present only under high cardinality",
						name, hasName)
				}
				if _, ok := pointAttr(dp, AttrRunID); ok {
					t.Error("a refusal carries ork.run_id; there is no run")
				}
				if _, ok := pointAttr(dp, AttrOrkflowVersion); ok {
					t.Error("a refusal carries ork.orkflow.version")
				}
			}
			if reasons["namespace_held"] != 1 || reasons["max_running_reached"] != 1 ||
				reasons["budget_exceeded"] != 1 || reasons["budget_unmeasurable"] != 1 {
				t.Errorf("series by reason = %v, want one of each", reasons)
			}
			if buf.Len() != 0 {
				t.Errorf("ObserveRefusal logged %q; the API logs a refusal, once", buf.String())
			}
		})
	}

	var nilObserver *EngineObserver
	nilObserver.ObserveRefusal(context.Background(), "payments", nil, "reconcile", "NAMESPACE_HELD")
}

func TestObserveRefusal_FollowsTheSelectionsSink(t *testing.T) {
	defReader := sdkmetric.NewManualReader()
	defMeter := sdkmetric.NewMeterProvider(sdkmetric.WithReader(defReader))
	t.Cleanup(func() { defMeter.Shutdown(context.Background()) })
	def := &Providers{Meter: defMeter, cfg: Config{MetricsExporters: []string{ExporterOTLP}}}

	bReader := sdkmetric.NewManualReader()
	bMeter := sdkmetric.NewMeterProvider(sdkmetric.WithReader(bReader))
	t.Cleanup(func() { bMeter.Shutdown(context.Background()) })
	b := &Providers{Meter: bMeter, cfg: Config{MetricsExporters: []string{ExporterOTLP}}}

	observer, err := NewEngineObserverForSinks(newSinksForTest(def, map[string]*Providers{"b": b}),
		slog.New(slog.NewTextHandler(&bytes.Buffer{}, nil)))
	if err != nil {
		t.Fatalf("NewEngineObserverForSinks: %v", err)
	}
	observer.ObserveRefusal(context.Background(), "payments",
		&orkstackv1.TelemetrySelection{Sink: "b"}, "reconcile", "NAMESPACE_HELD")

	if points := refusalPoints(t, bReader); len(points) != 1 {
		t.Errorf("sink b recorded %d ork.runs.refused points, want 1", len(points))
	}
	if points := refusalPoints(t, defReader); len(points) != 0 {
		t.Errorf("the default sink recorded %d ork.runs.refused points, want none", len(points))
	}
}

func TestObserveRefusal_SelectionAttrsRideOnlyHighAndOnlyPrefixed(t *testing.T) {
	for _, highCard := range []bool{false, true} {
		t.Run(map[bool]string{false: "low cardinality", true: "high cardinality"}[highCard], func(t *testing.T) {
			reader := sdkmetric.NewManualReader()
			var buf bytes.Buffer
			observer := newTestEngineObserver(t, reader, &buf, highCard)
			observer.ObserveRefusal(context.Background(), "payments",
				&orkstackv1.TelemetrySelection{Attributes: map[string]string{
					"ork.ns.team":    "payments",
					"ork.label.tier": "gold",
				}}, "reconcile", "NAMESPACE_HELD")

			points := refusalPoints(t, reader)
			if len(points) != 1 {
				t.Fatalf("ork.runs.refused points = %d, want 1", len(points))
			}
			dp := points[0]
			team, hasTeam := pointAttr(dp, "ork.ns.team")
			tier, hasTier := pointAttr(dp, "ork.label.tier")
			if hasTeam != highCard || hasTier != highCard {
				t.Errorf("selection attrs present (team %v, tier %v), want %v: they ride only under high cardinality",
					hasTeam, hasTier, highCard)
			}
			if highCard && (team != "payments" || tier != "gold") {
				t.Errorf("ork.ns.team=%q ork.label.tier=%q, want payments and gold", team, tier)
			}
			allowed := map[string]bool{AttrNamespace: true, AttrRefusalReason: true, AttrOrkflowName: true}
			for _, kv := range dp.Attributes.ToSlice() {
				key := string(kv.Key)
				if allowed[key] || strings.HasPrefix(key, "ork.ns.") || strings.HasPrefix(key, "ork.label.") {
					continue
				}
				t.Errorf("ork.runs.refused carries the key %q", key)
			}
			for _, bare := range []string{"team", "tier"} {
				if _, ok := pointAttr(dp, bare); ok {
					t.Errorf("ork.runs.refused carries the bare key %q", bare)
				}
			}
		})
	}
}

func TestNewEngineInstruments_RunsRefusedIsANoOpOnNilProviders(t *testing.T) {
	inst, err := NewEngineInstruments(nil)
	if err != nil {
		t.Fatalf("NewEngineInstruments(nil): %v", err)
	}
	if inst.RunsRefused == nil {
		t.Fatal("RunsRefused is nil on disabled Providers, want a no-op instrument")
	}
	inst.RunsRefused.Add(context.Background(), 1)
}
