package telemetry

import (
	"testing"
	"time"

	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
)

func concurrencyGauges(t *testing.T, now *time.Time, indexUnavailable bool) (*ConcurrencyGauges, *sdkmetric.ManualReader) {
	t.Helper()
	reader := sdkmetric.NewManualReader()
	g := NewConcurrencyGauges(&Providers{Meter: sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))},
		time.Minute, 5*time.Minute, indexUnavailable, nil)
	g.now = func() time.Time { return *now }
	return g, reader
}

var censusT0 = time.Date(2026, 11, 17, 12, 0, 0, 0, time.UTC)

func fullCensus() ConcurrencyCensus {
	return ConcurrencyCensus{
		At: censusT0, Duration: 2 * time.Second, Gates: 3, Unanswered: 1, Truncated: true,
		Rows: []ConcurrencyRow{{Namespace: "payments", Limit: "deploy#0a1b2c3d", Level: "run",
			SlotsUsed: 2, SlotsLimit: 4, QueueDepth: 1, OldestAge: time.Minute, OldestTimeoutFraction: 0.25}},
		Keys: []ConcurrencyKeyRow{{Namespace: "payments", Limit: "deploy#0a1b2c3d", KeysActive: 2, KeysFull: 1,
			StalledHolders: 1, FenceDegraded: 0}},
		Rebuilding: []ConcurrencyRebuilding{{Namespace: "payments", State: "failing", Gates: 1}},
		QueuedRuns: map[string]int{"payments": 5},
		Partial:    map[string]bool{"payments": true},
	}
}

// A fresh snapshot is reported under its §9.9.3 names and labels.
func TestConcurrencyGauges_ReportAFreshCensus(t *testing.T) {
	now := censusT0.Add(30 * time.Second)
	g, reader := concurrencyGauges(t, &now, false)
	g.Record(fullCensus())
	points := collectGC(t, reader)
	row := map[string]string{AttrNamespace: "payments", AttrConcurrencyLimit: "deploy#0a1b2c3d", AttrConcurrencyLevel: "run"}
	key := map[string]string{AttrNamespace: "payments", AttrConcurrencyLimit: "deploy#0a1b2c3d"}
	ns := map[string]string{AttrNamespace: "payments"}
	for _, tc := range []struct {
		name   string
		labels map[string]string
		want   float64
	}{
		{"ork.concurrency.slots.used", row, 2},
		{"ork.concurrency.slots.limit", row, 4},
		{"ork.concurrency.queue.depth", row, 1},
		{"ork.concurrency.queue.oldest_age", row, 60},
		{"ork.concurrency.queue.oldest_timeout_fraction", row, 0.25},
		{"ork.concurrency.keys.active", key, 2},
		{"ork.concurrency.keys.full", key, 1},
		{"ork.concurrency.stalled_holders", key, 1},
		{"ork.concurrency.fence_degraded", key, 0},
		{"ork.concurrency.rebuilding", map[string]string{AttrNamespace: "payments", AttrConcurrencyState: "failing"}, 1},
		{"ork.concurrency.queued_runs", ns, 5},
		{"ork.concurrency.census.partial", ns, 1},
		{"ork.concurrency.census.gates", map[string]string{}, 3},
		{"ork.concurrency.census.unanswered", map[string]string{}, 1},
		{"ork.concurrency.census.truncated", map[string]string{}, 1},
		{"ork.concurrency.census.unavailable", map[string]string{}, 0},
		{"ork.concurrency.census.duration", map[string]string{}, 2},
		{"ork.concurrency.census.local_age", map[string]string{}, 30},
		{"ork.concurrency.census.interval", map[string]string{}, 60},
		{"ork.concurrency.index_unavailable", map[string]string{}, 0},
	} {
		if got := value(t, points, tc.name, tc.labels); got != tc.want {
			t.Errorf("%s%v = %v, want %v", tc.name, tc.labels, got, tc.want)
		}
	}
}

// A snapshot older than 2 × interval + timeout is not reported as current:
// only its age, the interval and the index flag remain.
func TestConcurrencyGauges_AStaleCensusReportsOnlyItsAge(t *testing.T) {
	now := censusT0.Add(8 * time.Minute)
	g, reader := concurrencyGauges(t, &now, true)
	g.Record(fullCensus())
	points := collectGC(t, reader)
	for _, name := range []string{"ork.concurrency.slots.used", "ork.concurrency.queued_runs", "ork.concurrency.census.gates"} {
		if len(points[name]) != 0 {
			t.Fatalf("%s reported from a stale census: %v", name, points[name])
		}
	}
	if got := value(t, points, "ork.concurrency.census.local_age", map[string]string{}); got != 480 {
		t.Fatalf("local_age = %v, want 480", got)
	}
	if got := value(t, points, "ork.concurrency.index_unavailable", map[string]string{}); got != 1 {
		t.Fatalf("index_unavailable = %v, want 1", got)
	}
}

// A pass that could not run reports the census unavailable and nothing it
// does not know; an unknown queued-runs count is not reported as zero.
func TestConcurrencyGauges_NeverReportAnUnknownAsZero(t *testing.T) {
	now := censusT0.Add(time.Second)
	g, reader := concurrencyGauges(t, &now, false)
	g.Record(ConcurrencyCensus{At: censusT0, Unavailable: true})
	points := collectGC(t, reader)
	if got := value(t, points, "ork.concurrency.census.unavailable", map[string]string{}); got != 1 {
		t.Fatalf("unavailable = %v, want 1", got)
	}
	if len(points["ork.concurrency.census.gates"]) != 0 || len(points["ork.concurrency.queued_runs"]) != 0 {
		t.Fatal("an unavailable census reported gates or queued runs")
	}
	c := fullCensus()
	c.QueuedRuns, c.QueuedRunsUnknown = nil, true
	g.Record(c)
	points = collectGC(t, reader)
	if len(points["ork.concurrency.queued_runs"]) != 0 {
		t.Fatalf("queued runs reported though unknown: %v", points["ork.concurrency.queued_runs"])
	}
}

// Before this replica receives a census, local_age counts from its start:
// a census that never runs shows as an age that grows, not as no series.
func TestConcurrencyGauges_AgeBeforeTheFirstCensus(t *testing.T) {
	now := censusT0
	g, reader := concurrencyGauges(t, &now, false)
	g.started = censusT0
	now = censusT0.Add(10 * time.Minute)
	points := collectGC(t, reader)
	if got := value(t, points, "ork.concurrency.census.local_age", map[string]string{}); got != 600 {
		t.Fatalf("local_age = %v before any census, want 600", got)
	}
}
