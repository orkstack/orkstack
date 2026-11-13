package telemetry

import (
	"bytes"
	"context"
	"sort"
	"strings"
	"testing"
	"time"

	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

// series is every data point of the continuation instruments, as
// "name{label=value,…}" → count (a counter's value, a histogram's count).
func continuationSeries(t *testing.T, reader sdkmetric.Reader) map[string]int64 {
	t.Helper()
	out := map[string]int64{}
	for _, sm := range collect(t, reader).ScopeMetrics {
		for _, m := range sm.Metrics {
			if !strings.HasPrefix(m.Name, "ork.run.continuation") {
				continue
			}
			label := func(attrs []string) string { sort.Strings(attrs); return m.Name + "{" + strings.Join(attrs, ",") + "}" }
			switch data := m.Data.(type) {
			case metricdata.Sum[int64]:
				for _, dp := range data.DataPoints {
					var attrs []string
					for _, kv := range dp.Attributes.ToSlice() {
						attrs = append(attrs, string(kv.Key)+"="+kv.Value.String())
					}
					out[label(attrs)] += dp.Value
				}
			case metricdata.Histogram[int64]:
				for _, dp := range data.DataPoints {
					var attrs []string
					for _, kv := range dp.Attributes.ToSlice() {
						attrs = append(attrs, string(kv.Key)+"="+kv.Value.String())
					}
					out[label(attrs)] += int64(dp.Count)
				}
			case metricdata.Histogram[float64]:
				for _, dp := range data.DataPoints {
					var attrs []string
					for _, kv := range dp.Attributes.ToSlice() {
						attrs = append(attrs, string(kv.Key)+"="+kv.Value.String())
					}
					out[label(attrs)] += int64(dp.Count)
				}
			}
		}
	}
	return out
}

// TestEngineObserver_RecordsContinuationInstruments: a manual reader sees
// the five continuation instruments, each with its labels.
func TestEngineObserver_RecordsContinuationInstruments(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	var buf bytes.Buffer
	o := newTestEngineObserver(t, reader, &buf, false)
	ctx := context.Background()
	o.RecordContinuationWrite(ctx, "team-a", "events", "", 1000, 0.1, 0)
	o.RecordContinuationWrite(ctx, "team-a", "snapshot", "", 2000, 0.1, 0)
	o.RecordContinuationWrite(ctx, "team-a", "checkpoint", "written", 3000, 0.2, 12)
	o.RecordContinuationWrite(ctx, "team-a", "checkpoint", "write_failed", 0, 0.2, 0)
	o.RecordContinuationWrite(ctx, "team-a", "checkpoint", "over_cap", 9<<20, 0.2, 0)
	o.RecordContinuationRestore(ctx, "team-a", "", 1, false, 0)
	o.RecordContinuationRestore(ctx, "team-a", "restored", 2, true, 3.5)
	o.RecordContinuationRestore(ctx, "team-a", "RUN_STATE_UNAVAILABLE", 0, false, 0)

	got := continuationSeries(t, reader)
	want := map[string]int64{
		"ork.run.continuations{ork.namespace=team-a,outcome=written}":               1,
		"ork.run.continuations{ork.namespace=team-a,outcome=write_failed}":          1,
		"ork.run.continuations{ork.namespace=team-a,outcome=over_cap}":              1,
		"ork.run.continuations{ork.namespace=team-a,outcome=restored}":              1,
		"ork.run.continuations{ork.namespace=team-a,outcome=RUN_STATE_UNAVAILABLE}": 1,
		"ork.run.continuation.state.bytes{kind=events,ork.namespace=team-a}":        1,
		"ork.run.continuation.state.bytes{kind=snapshot,ork.namespace=team-a}":      1,
		"ork.run.continuation.state.bytes{kind=checkpoint,ork.namespace=team-a}":    2,
		"ork.run.continuation.duration{ork.namespace=team-a,phase=write}":           5,
		"ork.run.continuation.duration{ork.namespace=team-a,phase=restore}":         1,
		"ork.run.continuation.hold.duration{ork.namespace=team-a}":                  1,
		"ork.run.continuation.read.attempts{ork.namespace=team-a}":                  2,
	}
	for series, n := range want {
		if got[series] != n {
			t.Errorf("%s = %d, want %d", series, got[series], n)
		}
	}
	for series := range got {
		if _, ok := want[series]; !ok {
			t.Errorf("unexpected series %s", series)
		}
	}
}

// TestEngineObserver_RecordArchiveSweep: a pass's outcomes are counted, every
// outcome's series exists from the first pass even at zero, and every gauge is
// re-recorded on every pass, so none keeps a stale value: an undated risk reads
// 0 seconds to expiry, and a blind pass sets its gauge and leaves the last
// success where it was. A nil observer records nothing.
func TestEngineObserver_RecordArchiveSweep(t *testing.T) {
	type gauges struct {
		unconfirmed, listFailed, tooLong, lastSuccess int64
		expiry                                        float64
		seen                                          map[string]bool
	}
	read := func(reader sdkmetric.Reader) (map[string]int64, gauges) {
		counts := map[string]int64{}
		g := gauges{seen: map[string]bool{}}
		for _, sm := range collect(t, reader).ScopeMetrics {
			for _, m := range sm.Metrics {
				switch data := m.Data.(type) {
				case metricdata.Sum[int64]:
					if m.Name == "ork.archive.sweep.runs" {
						for _, dp := range data.DataPoints {
							outcome, _ := dp.Attributes.Value(AttrOutcome)
							counts[outcome.AsString()] += dp.Value
							g.seen[outcome.AsString()] = true
						}
					}
				case metricdata.Gauge[int64]:
					switch m.Name {
					case "ork.archive.sweep.unconfirmed":
						g.unconfirmed = data.DataPoints[0].Value
					case "ork.archive.sweep.list_failed":
						g.listFailed = data.DataPoints[0].Value
					case "ork.archive.sweep.interval_too_long":
						g.tooLong = data.DataPoints[0].Value
					case "ork.archive.sweep.last_success":
						g.lastSuccess = data.DataPoints[0].Value
					}
				case metricdata.Gauge[float64]:
					if m.Name == "ork.archive.sweep.earliest_expiry" {
						g.expiry = data.DataPoints[0].Value
					}
				}
			}
		}
		return counts, g
	}
	reader := sdkmetric.NewManualReader()
	var buf bytes.Buffer
	observer := newTestEngineObserver(t, reader, &buf, false)
	ctx := context.Background()
	first := time.Unix(1_800_000_000, 0)

	// A clean pass with nothing to do: every series exists, at zero.
	observer.RecordArchiveSweep(ctx, ArchiveSweepPass{At: first})
	counts, g := read(reader)
	for _, outcome := range []string{"archived", "failed", "expired"} {
		if !g.seen[outcome] || counts[outcome] != 0 {
			t.Fatalf("after a quiet pass the %s series = %v (seen %v), want present at 0", outcome, counts[outcome], g.seen[outcome])
		}
	}
	if g.lastSuccess != first.Unix() || g.listFailed != 0 || g.tooLong != 0 || g.unconfirmed != 0 || g.expiry != 0 {
		t.Fatalf("after a quiet pass gauges = %+v, want last success %d and the rest 0", g, first.Unix())
	}

	// A dated risk.
	observer.RecordArchiveSweep(ctx, ArchiveSweepPass{Archived: 2, Failed: 1, Expired: 1, Unconfirmed: 7,
		SecondsToExpiry: 3600, IntervalTooLong: true, At: first.Add(time.Minute)})
	// A blind pass: the index could not be listed, the risk is undated.
	observer.RecordArchiveSweep(ctx, ArchiveSweepPass{Archived: 1, Unconfirmed: 3, ListFailed: true, At: first.Add(2 * time.Minute)})
	// A pass whose store stopped answering.
	observer.RecordArchiveSweep(ctx, ArchiveSweepPass{Unconfirmed: 3, StoreFailed: true, SecondsToExpiry: 60, At: first.Add(3 * time.Minute)})
	var none *EngineObserver
	none.RecordArchiveSweep(ctx, ArchiveSweepPass{Archived: 9, Unconfirmed: 9, SecondsToExpiry: 9, At: first})

	counts, g = read(reader)
	if counts["archived"] != 3 || counts["failed"] != 1 || counts["expired"] != 1 {
		t.Fatalf("counted %v, want 3 archived, 1 failed, 1 expired", counts)
	}
	if g.unconfirmed != 3 || g.expiry != 60 {
		t.Fatalf("gauges = %+v, want 3 unconfirmed and 60 s to expiry from the last pass", g)
	}
	if g.listFailed != 0 || g.tooLong != 0 {
		t.Fatalf("gauges = %+v, want the last pass's list_failed and interval_too_long (0)", g)
	}
	if want := first.Add(time.Minute).Unix(); g.lastSuccess != want {
		t.Fatalf("last success = %d, want %d: neither blind pass is a success", g.lastSuccess, want)
	}

	// The undated blind pass on its own re-records the expiry as 0.
	reader2 := sdkmetric.NewManualReader()
	o2 := newTestEngineObserver(t, reader2, &buf, false)
	o2.RecordArchiveSweep(ctx, ArchiveSweepPass{Unconfirmed: 2, SecondsToExpiry: 3600, At: first})
	o2.RecordArchiveSweep(ctx, ArchiveSweepPass{Unconfirmed: 2, ListFailed: true, At: first.Add(time.Minute)})
	if _, g := read(reader2); g.expiry != 0 || g.listFailed != 1 || g.lastSuccess != first.Unix() {
		t.Fatalf("after an undated blind pass gauges = %+v, want 0 s to expiry, list_failed 1, last success unchanged", g)
	}
}

// TestEngineObserver_ContinuationRecordsAreNilSafe: a nil observer records
// nothing, through any reader, and never panics.
func TestEngineObserver_ContinuationRecordsAreNilSafe(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	var buf bytes.Buffer
	_ = newTestEngineObserver(t, reader, &buf, false)

	var o *EngineObserver
	o.RecordContinuationWrite(context.Background(), "ns", "checkpoint", "written", 1, 1, 1)
	o.RecordContinuationRestore(context.Background(), "ns", "restored", 1, true, 1)

	if got := continuationSeries(t, reader); len(got) != 0 {
		t.Fatalf("a nil observer recorded %v", got)
	}
}

// TestEngineObserver_RecordArchiveCleanup: each outcome of a cleanup pass is
// counted under its own label, a zero adds nothing, and a nil observer
// records nothing.
func TestEngineObserver_RecordArchiveCleanup(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	var buf bytes.Buffer
	observer := newTestEngineObserver(t, reader, &buf, false)
	ctx := context.Background()
	observer.RecordArchiveCleanup(ctx, 3, 1, 2, 0)
	observer.RecordArchiveCleanup(ctx, 0, 0, 1, 4)
	var none *EngineObserver
	none.RecordArchiveCleanup(ctx, 9, 9, 9, 9)

	counts := map[string]int64{}
	for _, sm := range collect(t, reader).ScopeMetrics {
		for _, m := range sm.Metrics {
			if data, ok := m.Data.(metricdata.Sum[int64]); ok && m.Name == "ork.archive.cleanup.runs" {
				for _, dp := range data.DataPoints {
					outcome, _ := dp.Attributes.Value(AttrOutcome)
					counts[outcome.AsString()] += dp.Value
				}
			}
		}
	}
	want := map[string]int64{"retired": 3, "failed": 1, "not_found_not_due": 3, "not_found_held": 4}
	if len(counts) != len(want) {
		t.Fatalf("counted %v, want %v", counts, want)
	}
	for k, v := range want {
		if counts[k] != v {
			t.Fatalf("counted %v, want %v", counts, want)
		}
	}
}
