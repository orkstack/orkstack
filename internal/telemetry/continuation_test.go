package telemetry

import (
	"bytes"
	"context"
	"sort"
	"strings"
	"testing"

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
						attrs = append(attrs, string(kv.Key)+"="+kv.Value.Emit())
					}
					out[label(attrs)] += dp.Value
				}
			case metricdata.Histogram[int64]:
				for _, dp := range data.DataPoints {
					var attrs []string
					for _, kv := range dp.Attributes.ToSlice() {
						attrs = append(attrs, string(kv.Key)+"="+kv.Value.Emit())
					}
					out[label(attrs)] += int64(dp.Count)
				}
			case metricdata.Histogram[float64]:
				for _, dp := range data.DataPoints {
					var attrs []string
					for _, kv := range dp.Attributes.ToSlice() {
						attrs = append(attrs, string(kv.Key)+"="+kv.Value.Emit())
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

// TestEngineObserver_ContinuationRecordsAreNilSafe: a nil observer records
// nothing and never panics.
func TestEngineObserver_ContinuationRecordsAreNilSafe(t *testing.T) {
	var o *EngineObserver
	o.RecordContinuationWrite(context.Background(), "ns", "checkpoint", "written", 1, 1, 1)
	o.RecordContinuationRestore(context.Background(), "ns", "restored", 1, true, 1)
}
