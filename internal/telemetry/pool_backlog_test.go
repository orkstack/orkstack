package telemetry

import (
	"context"
	"errors"
	"io"
	"maps"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"

	"github.com/orkstack/orkstack/internal/testsupport/must"
)

var poolBacklogMetrics = []string{
	PoolBacklogTasksMetric, PoolBacklogAgeMetric, PoolTasksAddRateMetric,
	PoolTasksDispatchRateMetric, PoolPollersMetric,
}

// fakePoolReads answers each pool's read from a table a test changes.
type fakePoolReads struct {
	mu    sync.Mutex
	reads map[string]PoolReading
	fails map[string]error
	asked []string
}

func (f *fakePoolReads) read(_ context.Context, pool string) (PoolReading, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.asked = append(f.asked, pool)
	if err := f.fails[pool]; err != nil {
		return PoolReading{}, err
	}
	return f.reads[pool], nil
}

func newPoolBacklogTest(t *testing.T, pools []string, reads *fakePoolReads, now *time.Time) (sdkmetric.Reader, func(context.Context) error) {
	t.Helper()
	reader := sdkmetric.NewManualReader()
	p := &Providers{Meter: sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))}
	pass := newPoolBacklogGauges(p, nil, PoolBacklogSource{Pools: pools, Read: reads.read, Interval: 15 * time.Second},
		func() time.Time { return *now })
	return reader, pass
}

// byPool is one metric's points by their ork.pool, failing on any other label:
// the channel a pool maps to is never exported (AGENTS.md §7.1).
func byPool(t *testing.T, points []metricPoint) map[string]float64 {
	t.Helper()
	out := map[string]float64{}
	for _, p := range points {
		pool, ok := p.attrs.Value(attribute.Key(AttrPool))
		if p.attrs.Len() != 1 || !ok {
			t.Fatalf("point labels = %v; want ork.pool alone", p.attrs.ToSlice())
		}
		out[pool.AsString()] = p.value
	}
	return out
}

// A pool's gauges report what the last read of its channel answered, one
// series per pool labelled with the pool alone, and nothing before the first
// read: not measured is absent, never 0.
func TestPoolBacklog_GaugesReportTheLastReadByPool(t *testing.T) {
	now := time.Unix(1_000_000, 0)
	reads := &fakePoolReads{reads: map[string]PoolReading{
		"main": {Backlog: 7, BacklogAge: 1500 * time.Millisecond, AddRate: 0.5, DispatchRate: 0.25, Pollers: 2},
		"gpu":  {},
	}}
	reader, pass := newPoolBacklogTest(t, []string{"main", "gpu"}, reads, &now)

	points := metricPoints(t, reader)
	for _, name := range poolBacklogMetrics {
		if len(points[name]) != 0 {
			t.Fatalf("%s before any read = %v; want no series", name, points[name])
		}
	}

	must.OK(t, pass(context.Background()))
	points = metricPoints(t, reader)
	for name, want := range map[string]map[string]float64{
		PoolBacklogTasksMetric:      {"main": 7, "gpu": 0},
		PoolBacklogAgeMetric:        {"main": 1.5, "gpu": 0},
		PoolTasksAddRateMetric:      {"main": 0.5, "gpu": 0},
		PoolTasksDispatchRateMetric: {"main": 0.25, "gpu": 0},
		PoolPollersMetric:           {"main": 2, "gpu": 0},
	} {
		if got := byPool(t, points[name]); !maps.Equal(got, want) {
			t.Errorf("%s = %v; want %v", name, got, want)
		}
	}
	slices.Sort(reads.asked)
	if !slices.Equal(reads.asked, []string{"gpu", "main"}) {
		t.Fatalf("pools read = %v; want each once", reads.asked)
	}
}

// A pool whose read fails keeps its last good reading for three intervals and
// is then dropped, while every other pool keeps reporting; the pass names the
// pool it could not read.
func TestPoolBacklog_AFailedReadGoesAbsentAfterThreeIntervals(t *testing.T) {
	now := time.Unix(1_000_000, 0)
	reads := &fakePoolReads{
		reads: map[string]PoolReading{"main": {Backlog: 3}, "gpu": {Backlog: 9}},
		fails: map[string]error{},
	}
	reader, pass := newPoolBacklogTest(t, []string{"main", "gpu"}, reads, &now)
	must.OK(t, pass(context.Background()))

	reads.mu.Lock()
	reads.fails["gpu"] = errors.New("cluster unavailable")
	reads.reads["main"] = PoolReading{Backlog: 4}
	reads.mu.Unlock()
	now = now.Add(15 * time.Second)
	err := pass(context.Background())
	if err == nil || !strings.Contains(err.Error(), `"gpu"`) || !strings.Contains(err.Error(), "cluster unavailable") {
		t.Fatalf("pass error = %v; want it to name pool \"gpu\" and the cause", err)
	}
	now = now.Add(30 * time.Second) // 45 s after gpu's last good read: within 3 × 15 s
	if got := byPool(t, metricPoints(t, reader)[PoolBacklogTasksMetric]); !maps.Equal(got, map[string]float64{"main": 4, "gpu": 9}) {
		t.Fatalf("backlog within three intervals = %v; want main 4 and gpu's last good 9", got)
	}
	now = now.Add(time.Second)
	points := metricPoints(t, reader)
	for _, name := range poolBacklogMetrics {
		got := byPool(t, points[name])
		if _, ok := got["main"]; len(got) != 1 || !ok {
			t.Fatalf("%s past three intervals = %v; want main alone", name, got)
		}
	}
}

// The Prometheus names are the contract dashboards and alerts are written
// against (docs/DEPLOY.md, "A pool's backlog, as metrics").
func TestPoolBacklog_PrometheusNames(t *testing.T) {
	cfg := Config{MetricsExporters: []string{ExporterPrometheus}, AdminAddr: "127.0.0.1:0"}
	p, err := Setup(context.Background(), cfg)
	must.OKf(t, err, "Setup")
	defer p.Shutdown(context.Background())
	reads := &fakePoolReads{reads: map[string]PoolReading{"main": {Backlog: 1, BacklogAge: time.Second, AddRate: 1, DispatchRate: 1, Pollers: 1}}}
	pass := newPoolBacklogGauges(p, nil, PoolBacklogSource{Pools: []string{"main"}, Read: reads.read, Interval: 15 * time.Second}, time.Now)
	must.OK(t, pass(context.Background()))

	srv := httptest.NewServer(NewAdminMux(p, cfg))
	defer srv.Close()
	resp, err := http.Get(srv.URL + MetricsPath)
	must.OKf(t, err, "GET /metrics")
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	must.OK(t, err)
	var got []string
	for _, line := range strings.Split(string(body), "\n") {
		if strings.HasPrefix(line, "ork_pool_") {
			name, labels, _ := strings.Cut(line, "{")
			if !strings.Contains(labels, `ork_pool="main"`) {
				t.Errorf("%s labels = {%s; want ork_pool=\"main\"", name, labels)
			}
			got = append(got, name)
		}
	}
	slices.Sort(got)
	want := []string{
		"ork_pool_backlog_age_seconds", "ork_pool_backlog_tasks", "ork_pool_pollers",
		"ork_pool_tasks_add_rate_per_second", "ork_pool_tasks_dispatch_rate_per_second",
	}
	if !slices.Equal(got, want) {
		t.Fatalf("Prometheus names = %v; want %v", got, want)
	}
}

// A census with no budget, the pool backlog's, runs every pass it is asked
// for and still records its requests and its last success.
func TestCensus_AnUnbudgetedCensusRunsEveryPass(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	p := &Providers{Meter: sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))}
	now := time.Unix(2_000_000, 0)
	ran := 0
	c := census{name: "pool_backlog", requests: 3, metrics: newCensusMetrics(p, nil), now: func() time.Time { return now },
		pass: func(context.Context) error { ran++; return nil }}
	for range 5 {
		c.once(context.Background())
	}
	got := censusReadings(t, reader)
	if ran != 5 || got[CensusRequestsMetric+"/pool_backlog"] != 15 || got[CensusSkippedMetric+"/pool_backlog"] != 0 {
		t.Fatalf("ran %d, readings %v; want 5 passes, 15 requests, no skip", ran, got)
	}
	if got[CensusLastSuccessMetric+"/pool_backlog"] != 2_000_000 {
		t.Fatalf("last success = %v; want 2000000", got[CensusLastSuccessMetric+"/pool_backlog"])
	}
}

// A read that panics fails that pool's read, never the engine: the other
// pools are still read and reported, and the pass names the pool.
func TestPoolBacklog_APanickingReadFailsOnlyItsPool(t *testing.T) {
	now := time.Unix(1_000_000, 0)
	reader := sdkmetric.NewManualReader()
	p := &Providers{Meter: sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))}
	read := func(_ context.Context, pool string) (PoolReading, error) {
		if pool == "gpu" {
			panic("nil response")
		}
		return PoolReading{Backlog: 2}, nil
	}
	pass := newPoolBacklogGauges(p, nil, PoolBacklogSource{Pools: []string{"main", "gpu"}, Read: read, Interval: 15 * time.Second},
		func() time.Time { return now })
	err := pass(context.Background())
	if err == nil || !strings.Contains(err.Error(), `"gpu"`) || !strings.Contains(err.Error(), "panicked") {
		t.Fatalf("pass error = %v; want pool \"gpu\" named as panicking", err)
	}
	if got := byPool(t, metricPoints(t, reader)[PoolBacklogTasksMetric]); !maps.Equal(got, map[string]float64{"main": 2}) {
		t.Fatalf("backlog = %v; want main alone", got)
	}
}
