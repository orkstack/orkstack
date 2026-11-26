package telemetry

import (
	"context"
	"testing"
	"time"
)

// TestConcurrencyMetrics_ReportCountsWhatAGateDecided: what a gate reports is
// counted under the §9.9.1 labels, by its own closed outcomes; a value outside
// one, or a count below one, is not recorded.
func TestConcurrencyMetrics_ReportCountsWhatAGateDecided(t *testing.T) {
	p, reader := areaProviders(t)
	m := NewConcurrencyMetrics(p, nil, true, discardLogger())
	ctx := context.Background()
	const limit = "db:{{ .inputs.env }}#1a2b3c4d"
	m.Report(ctx, ConcurrencyGateReport{
		Namespace: "acme", Started: true,
		Reclaimed: []ConcurrencyReclaim{
			{Limit: limit, Outcome: "closed", Entries: 2}, {Limit: limit, Outcome: "expired", Entries: 1},
			{Limit: limit, Outcome: "evaporated", Entries: 9}, {Limit: limit, Outcome: "stale", Entries: 0}},
		VerifyErrors: []ConcurrencyVerifyErrors{{Limit: limit, Calls: 3}, {Limit: limit, Calls: -1}},
		Rebuilt:      []ConcurrencyRebuild{{Outcome: "failed_attempt", Times: 2}, {Outcome: "ok", Times: 1}, {Outcome: "maybe", Times: 1}},
	})
	m.Report(ctx, ConcurrencyGateReport{Namespace: "acme", Rebuilt: []ConcurrencyRebuild{{Outcome: "skipped", Times: 1}}})
	points := collectGC(t, reader)
	reclaimed := func(outcome string) map[string]string {
		return map[string]string{AttrNamespace: "acme", AttrConcurrencyLimit: limit, AttrConcurrencyOutcome: outcome}
	}
	for _, c := range []struct {
		metric string
		labels map[string]string
		want   float64
	}{
		{ConcurrencyReclaimedMetric, reclaimed("closed"), 2},
		{ConcurrencyReclaimedMetric, reclaimed("expired"), 1},
		{ConcurrencyVerifyErrorsMetric, map[string]string{AttrNamespace: "acme", AttrConcurrencyLimit: limit}, 3},
		{ConcurrencyRebuiltMetric, map[string]string{AttrNamespace: "acme", AttrConcurrencyOutcome: "failed_attempt"}, 2},
		{ConcurrencyRebuiltMetric, map[string]string{AttrNamespace: "acme", AttrConcurrencyOutcome: "ok"}, 1},
		{ConcurrencyRebuiltMetric, map[string]string{AttrNamespace: "acme", AttrConcurrencyOutcome: "skipped"}, 1},
		{ConcurrencyGatesStartedMetric, map[string]string{AttrNamespace: "acme"}, 1},
	} {
		if got := value(t, points, c.metric, c.labels); got != c.want {
			t.Errorf("%s%v = %v, want %v", c.metric, c.labels, got, c.want)
		}
	}
	for metric, series := range map[string]int{ConcurrencyReclaimedMetric: 2, ConcurrencyVerifyErrorsMetric: 1,
		ConcurrencyRebuiltMetric: 3, ConcurrencyGatesStartedMetric: 1} {
		if got := len(points[metric]); got != series {
			t.Errorf("%s has %d series, want %d", metric, got, series)
		}
	}
	var none *ConcurrencyMetrics
	none.Report(ctx, ConcurrencyGateReport{})
}

// TestConcurrencyGauges_CountAPassThatCouldNotRun: each census pass that
// could not run is counted, with no label, and one that ran is not.
func TestConcurrencyGauges_CountAPassThatCouldNotRun(t *testing.T) {
	p, reader := areaProviders(t)
	g := NewConcurrencyGauges(p, time.Minute, time.Second, false, discardLogger())
	g.Record(ConcurrencyCensus{At: time.Now()})
	if n := len(collectGC(t, reader)[ConcurrencyCensusFailuresMetric]); n != 0 {
		t.Fatalf("a pass that ran left %d failure series, want none", n)
	}
	g.Record(ConcurrencyCensus{At: time.Now(), Unavailable: true})
	g.Record(ConcurrencyCensus{At: time.Now(), Unavailable: true})
	if got := value(t, collectGC(t, reader), ConcurrencyCensusFailuresMetric, map[string]string{}); got != 2 {
		t.Fatalf("failed passes = %v, want 2", got)
	}
}

// TestConcurrencyMetrics_ResetCountsItsOutcomes: each outcome under its
// namespace and whether it was forced; one outside the closed set is not
// recorded.
func TestConcurrencyMetrics_ResetCountsItsOutcomes(t *testing.T) {
	p, reader := areaProviders(t)
	m := NewConcurrencyMetrics(p, nil, true, discardLogger())
	ctx := context.Background()
	for _, outcome := range append(ConcurrencyResetOutcomes(), "maybe") {
		m.Reset(ctx, "acme", outcome, false)
	}
	m.Reset(ctx, "acme", "started", true)
	points := collectGC(t, reader)
	for _, outcome := range ConcurrencyResetOutcomes() {
		labels := map[string]string{AttrNamespace: "acme", AttrConcurrencyOutcome: outcome, AttrConcurrencyForced: "false"}
		if got := value(t, points, ConcurrencyResetsMetric, labels); got != 1 {
			t.Errorf("%v = %v, want 1", labels, got)
		}
	}
	if got := len(points[ConcurrencyResetsMetric]); got != len(ConcurrencyResetOutcomes())+1 {
		t.Errorf("%d series, want each outcome's and the forced start's", got)
	}
	var none *ConcurrencyMetrics
	none.Reset(ctx, "acme", "started", false)
}
