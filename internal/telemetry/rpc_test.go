package telemetry

import (
	"context"
	"testing"
	"time"

	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/orkstack/orkstack/internal/testsupport/must"
)

// TestRPCMetrics_RecordCallsAndStreams: a call is one duration point by
// service, method and code; a stream moves ork.api.streams up while open and
// back down when closed, once however often it is closed.
func TestRPCMetrics_RecordCallsAndStreams(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	m, err := NewRPCMetrics(sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader)))
	must.OK(t, err)
	ctx := context.Background()
	m.ObserveCall(ctx, "/orkstack.v1.RunService/GetRun", "not_found", 3*time.Millisecond)
	m.ObserveCall(ctx, "not-a-procedure", "ok", time.Millisecond)
	closeA := m.StreamOpened(ctx, StreamWebSocketEvents)
	closeB := m.StreamOpened(ctx, StreamWebSocketEvents)
	closeA()
	closeA()

	var rm metricdata.ResourceMetrics
	must.OK(t, reader.Collect(ctx, &rm))
	calls := map[string]uint64{}
	var open int64
	for _, sm := range rm.ScopeMetrics {
		for _, mm := range sm.Metrics {
			switch mm.Name {
			case RPCCallDurationMetric:
				for _, dp := range mm.Data.(metricdata.Histogram[float64]).DataPoints {
					svc, _ := dp.Attributes.Value(AttrRPCService)
					method, _ := dp.Attributes.Value(AttrRPCMethod)
					code, _ := dp.Attributes.Value(AttrRPCCode)
					calls[svc.AsString()+"/"+method.AsString()+"/"+code.AsString()] += dp.Count
				}
			case APIStreamsMetric:
				for _, dp := range mm.Data.(metricdata.Sum[int64]).DataPoints {
					open += dp.Value
				}
			}
		}
	}
	if calls["orkstack.v1.RunService/GetRun/not_found"] != 1 || calls["unknown/unknown/ok"] != 1 || len(calls) != 2 {
		t.Fatalf("calls = %v", calls)
	}
	if open != 1 {
		t.Fatalf("open streams = %d; want 1", open)
	}
	closeB()
	var nilMetrics *RPCMetrics
	nilMetrics.ObserveCall(ctx, "/a.B/C", "ok", 0)
	nilMetrics.StreamOpened(ctx, StreamConnect)()
}
