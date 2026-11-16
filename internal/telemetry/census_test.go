package telemetry

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"strings"
	"testing"
	"time"

	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/orkstack/orkstack/internal/testsupport/must"
)

// censusReadings reads every census instrument by "metric/census".
func censusReadings(t *testing.T, reader sdkmetric.Reader) map[string]float64 {
	t.Helper()
	var rm metricdata.ResourceMetrics
	must.OK(t, reader.Collect(context.Background(), &rm))
	got := map[string]float64{}
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			switch data := m.Data.(type) {
			case metricdata.Sum[int64]:
				for _, dp := range data.DataPoints {
					name, _ := dp.Attributes.Value(AttrCensus)
					got[m.Name+"/"+name.AsString()] += float64(dp.Value)
				}
			case metricdata.Gauge[int64]:
				for _, dp := range data.DataPoints {
					name, _ := dp.Attributes.Value(AttrCensus)
					got[m.Name+"/"+name.AsString()] = float64(dp.Value)
				}
			}
		}
	}
	return got
}

// TestCensusBudget_RefillsPerMinute: a budget holds a minute's requests,
// refills continuously, and never lends what it does not hold.
func TestCensusBudget_RefillsPerMinute(t *testing.T) {
	now := time.Unix(0, 0)
	b := newCensusBudget(60, func() time.Time { return now })
	if !b.take(60) || b.take(1) {
		t.Fatal("a fresh budget did not hold exactly a minute's 60 requests")
	}
	now = now.Add(10 * time.Second)
	if !b.take(10) || b.take(1) {
		t.Fatal("ten seconds did not refill exactly 10 requests")
	}
	now = now.Add(time.Hour)
	if !b.take(60) || b.take(1) {
		t.Fatal("an idle hour refilled past the minute's capacity")
	}
	if b.take(61) {
		t.Fatal("a pass dearer than the whole budget was admitted")
	}
}

// TestCensus_RespectsBudgetAndRecordsItsPasses: a pass the budget cannot pay
// for is skipped and counted, never run; a pass that runs is charged its
// requests and stamps its last success, and a failed one does not.
func TestCensus_RespectsBudgetAndRecordsItsPasses(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	p := &Providers{Meter: sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))}
	now := time.Unix(1_000_000, 0)
	budget := newCensusBudget(3, func() time.Time { return now })
	m := newCensusMetrics(p, nil)
	ran := 0
	fail := false
	c := census{name: "runs_open", requests: 2, budget: budget, metrics: m, now: func() time.Time { return now },
		pass: func(context.Context) error {
			ran++
			if fail {
				return errors.New("visibility unavailable")
			}
			return nil
		}}
	ctx := context.Background()
	c.once(ctx) // runs: budget 3 → 1
	c.once(ctx) // skipped: 1 < 2
	now = now.Add(20 * time.Second)
	fail = true
	c.once(ctx) // refilled to 2: runs, fails

	got := censusReadings(t, reader)
	if ran != 2 {
		t.Fatalf("passes run = %d; want 2", ran)
	}
	if got[CensusRequestsMetric+"/runs_open"] != 4 || got[CensusSkippedMetric+"/runs_open"] != 1 {
		t.Fatalf("readings = %v; want 4 requests and 1 skip", got)
	}
	if got[CensusLastSuccessMetric+"/runs_open"] != 1_000_000 {
		t.Fatalf("last success = %v; want the first pass's 1000000, not the failed one's", got[CensusLastSuccessMetric+"/runs_open"])
	}
}

// TestRunsOpen_GaugeIsTheLastGoodCount: ork.runs.open reports the count the
// last good pass read, and nothing after a pass that failed.
func TestRunsOpen_GaugeIsTheLastGoodCount(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	p := &Providers{Meter: sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))}
	count := int64(7)
	var countErr error
	pass := newRunsOpenGauge(p, nil, func(context.Context) (int64, error) { return count, countErr })
	ctx := context.Background()
	must.OK(t, pass(ctx))
	read := func() (int64, bool) {
		var rm metricdata.ResourceMetrics
		must.OK(t, reader.Collect(ctx, &rm))
		for _, sm := range rm.ScopeMetrics {
			for _, m := range sm.Metrics {
				if m.Name == RunsOpenMetric {
					if points := m.Data.(metricdata.Gauge[int64]).DataPoints; len(points) == 1 {
						return points[0].Value, true
					}
				}
			}
		}
		return 0, false
	}
	if v, ok := read(); !ok || v != 7 {
		t.Fatalf("ork.runs.open = %d (%v); want 7", v, ok)
	}
	countErr = errors.New("count failed")
	if err := pass(ctx); err == nil {
		t.Fatal("a failed count passed")
	}
	if v, ok := read(); ok {
		t.Fatalf("ork.runs.open still reports %d after a failed count", v)
	}
}

// TestCensus_ChargesWhatAPassMadeAndShowsFailures: a pass that made more
// requests than it was admitted for is charged them, putting the budget in
// debt so later passes skip; a census that never read its source shows
// last_success 0, and each failed pass is counted.
func TestCensus_ChargesWhatAPassMadeAndShowsFailures(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	p := &Providers{Meter: sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))}
	now := time.Unix(1_000_000, 0)
	budget := newCensusBudget(10, func() time.Time { return now })
	m := newCensusMetrics(p, nil)
	var made int64
	c := census{name: "runs_open", requests: 2, budget: budget, metrics: m, now: func() time.Time { return now },
		spent: func() int64 { return made },
		pass: func(context.Context) error {
			made += 25
			return errors.New("visibility unavailable")
		}}
	m.register(c.name)
	ctx := context.Background()
	c.once(ctx) // admitted for 2, made 25: budget 10 → -15
	now = now.Add(time.Minute)
	c.once(ctx) // refilled to -5: skipped

	got := censusReadings(t, reader)
	if got[CensusRequestsMetric+"/runs_open"] != 25 || got[CensusSkippedMetric+"/runs_open"] != 1 {
		t.Fatalf("readings = %v; want 25 requests charged and 1 skip", got)
	}
	if got[CensusFailedMetric+"/runs_open"] != 1 {
		t.Fatalf("failed = %v; want 1", got[CensusFailedMetric+"/runs_open"])
	}
	if v, ok := got[CensusLastSuccessMetric+"/runs_open"]; !ok || v != 0 {
		t.Fatalf("last_success = %v (present %v); want 0 for a census that never read its source", v, ok)
	}
}

// TestCensus_CostPricesEachPass: a census whose price depends on what it read
// last (the policy cache: one read per namespace) is admitted for that price,
// not a fixed one.
func TestCensus_CostPricesEachPass(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	p := &Providers{Meter: sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))}
	now := time.Unix(1_000_000, 0)
	budget := newCensusBudget(6, func() time.Time { return now })
	m := newCensusMetrics(p, nil)
	ran := 0
	c := census{name: "telemetry_policy", budget: budget, metrics: m, now: func() time.Time { return now },
		cost: func() int { return 5 },
		pass: func(context.Context) error { ran++; return nil }}
	ctx := context.Background()
	c.once(ctx) // 6 → 1
	c.once(ctx) // 1 < 5: skipped
	got := censusReadings(t, reader)
	if ran != 1 || got[CensusRequestsMetric+"/telemetry_policy"] != 5 || got[CensusSkippedMetric+"/telemetry_policy"] != 1 {
		t.Fatalf("ran %d, readings %v; want 1 pass charged 5 and 1 skip", ran, got)
	}
}

// TestCensus_APassDearerThanTheBudgetStillRuns: a pass whose price exceeds a
// whole minute's budget (the policy refresh, past as many namespaces as the
// budget holds) is admitted once the budget is full and charged the rest as
// debt, so it runs at the rate the budget pays for, never not at all.
func TestCensus_APassDearerThanTheBudgetStillRuns(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	p := &Providers{Meter: sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))}
	now := time.Unix(1_000_000, 0)
	budget := newCensusBudget(60, func() time.Time { return now })
	m := newCensusMetrics(p, nil)
	var made int64
	ran := 0
	c := census{name: "telemetry_policy", budget: budget, metrics: m, now: func() time.Time { return now },
		cost:  func() int { return 121 },
		spent: func() int64 { return made },
		pass:  func(context.Context) error { ran++; made += 121; return nil }}
	ctx := context.Background()
	c.once(ctx) // full budget 60: runs, charged 121, budget -61
	now = now.Add(time.Minute)
	c.once(ctx) // -1: skipped
	now = now.Add(2 * time.Minute)
	c.once(ctx) // capped back to 60: runs again
	if ran != 2 {
		t.Fatalf("a pass dearer than the budget ran %d times in three minutes; want 2", ran)
	}
	if got := censusReadings(t, reader); got[CensusSkippedMetric+"/telemetry_policy"] != 1 {
		t.Fatalf("readings = %v; want 1 skip while the debt is repaid", got)
	}
}

// TestCensus_ASkipStreakIsLogged: a census its budget skips pass after pass
// is logged at Warn once the streak is long enough, not only counted.
func TestCensus_ASkipStreakIsLogged(t *testing.T) {
	var buf bytes.Buffer
	now := time.Unix(1_000_000, 0)
	budget := newCensusBudget(1, func() time.Time { return now })
	m := newCensusMetrics(&Providers{Meter: sdkmetric.NewMeterProvider()}, slog.New(slog.NewTextHandler(&buf, nil)))
	c := census{name: "runs_open", requests: 2, budget: budget, metrics: m, now: func() time.Time { return now },
		pass: func(context.Context) error { return nil }}
	for range skipWarnAfter {
		c.once(context.Background())
	}
	if n := strings.Count(buf.String(), "level=WARN"); n != 1 || !strings.Contains(buf.String(), "census=runs_open") {
		t.Fatalf("log = %q; want one Warn naming runs_open after %d skips", buf.String(), skipWarnAfter)
	}
}

// TestCensus_APanickingPassFailsThePass: a pass that panics is a failed
// pass, counted, never the end of the engine.
func TestCensus_APanickingPassFailsThePass(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	now := time.Unix(1_000_000, 0)
	m := newCensusMetrics(&Providers{Meter: sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))}, nil)
	c := census{name: "runs_open", requests: 1, budget: newCensusBudget(60, func() time.Time { return now }), metrics: m,
		now: func() time.Time { return now }, pass: func(context.Context) error { panic("boom") }}
	c.once(context.Background())
	if got := censusReadings(t, reader); got[CensusFailedMetric+"/runs_open"] != 1 {
		t.Fatalf("readings = %v; want the panicking pass counted failed", got)
	}
}
