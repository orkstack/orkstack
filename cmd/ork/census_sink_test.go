package main

import (
	"context"
	"log/slog"
	"reflect"
	"slices"
	"testing"

	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/internal/telemetry"
)

// TestGateReport_OutcomesAreTheMetricsOwn: every outcome a gate reports is
// one its metric records, and the metric names no other.
func TestGateReport_OutcomesAreTheMetricsOwn(t *testing.T) {
	for name, sets := range map[string][2][]string{
		"reclaimed": {engine.GateReclaimOutcomes(), telemetry.ConcurrencyReclaimOutcomes()},
		"rebuilt":   {engine.GateRebuiltOutcomes(), telemetry.ConcurrencyRebuiltOutcomes()},
		"resets":    {engine.ConcurrencyResetOutcomes(), telemetry.ConcurrencyResetOutcomes()},
	} {
		slices.Sort(sets[0])
		slices.Sort(sets[1])
		if !reflect.DeepEqual(sets[0], sets[1]) {
			t.Errorf("%s: a gate reports %v, the metric records %v", name, sets[0], sets[1])
		}
	}
}

// TestGateReportObserver_CountsEveryPartOfAReport: no part of a report is
// lost between the engine and its metrics.
func TestGateReportObserver_CountsEveryPartOfAReport(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	p := &telemetry.Providers{Meter: sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))}
	m := telemetry.NewConcurrencyMetrics(p, nil, false, slog.New(slog.DiscardHandler))
	gateReportObserver{m}.ObserveGateReport(context.Background(), engine.ConcurrencyGateReport{
		Namespace: "acme", Started: true,
		Reclaimed:    []engine.GateReclaim{{Limit: "a#1", Outcome: "expired", Entries: 2}},
		VerifyErrors: []engine.GateVerifyErrors{{Limit: "a#1", Calls: 3}},
		Rebuilt:      []engine.GateRebuilt{{Outcome: engine.GateRebuiltOK, Times: 4}},
	})
	var rm metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &rm); err != nil {
		t.Fatal(err)
	}
	got := map[string]int64{}
	for _, sm := range rm.ScopeMetrics {
		for _, metric := range sm.Metrics {
			if sum, ok := metric.Data.(metricdata.Sum[int64]); ok {
				for _, pt := range sum.DataPoints {
					got[metric.Name] += pt.Value
				}
			}
		}
	}
	want := map[string]int64{telemetry.ConcurrencyGatesStartedMetric: 1, telemetry.ConcurrencyReclaimedMetric: 2,
		telemetry.ConcurrencyVerifyErrorsMetric: 3, telemetry.ConcurrencyRebuiltMetric: 4}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("counted %v, want %v", got, want)
	}
}
