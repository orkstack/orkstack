package api_test

import (
	"context"
	"io"
	"log/slog"
	"net/http/httptest"
	"testing"

	"connectrpc.com/connect"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/gen/go/orkstack/v1/orkstackv1connect"
	"github.com/orkstack/orkstack/internal/api"
	"github.com/orkstack/orkstack/internal/telemetry"
	"github.com/orkstack/orkstack/internal/testsupport/must"
)

// TestRPCMetrics_CountEveryCallWithItsCode: a call that succeeds and one a
// handler refuses are each one rpc.server.call.duration point, by service,
// method and Connect code.
func TestRPCMetrics_CountEveryCallWithItsCode(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	metrics, err := telemetry.NewRPCMetrics(sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader)))
	must.OK(t, err)
	svc := api.NewService(api.Config{Logger: slog.New(slog.NewTextHandler(io.Discard, nil))})
	server := httptest.NewServer(api.NewMux(svc, api.MuxOptions{RPCMetrics: metrics}))
	t.Cleanup(server.Close)

	ctx := context.Background()
	system := orkstackv1connect.NewSystemServiceClient(server.Client(), server.URL)
	_, err = system.GetSystemInfo(ctx, connect.NewRequest(&orkstackv1.GetSystemInfoRequest{}))
	must.OK(t, err)
	runs := orkstackv1connect.NewRunServiceClient(server.Client(), server.URL)
	_, err = runs.GetRun(ctx, connect.NewRequest(&orkstackv1.GetRunRequest{}))
	if err == nil {
		t.Fatal("GetRun with no run id succeeded")
	}
	wantCode := connect.CodeOf(err).String()

	var rm metricdata.ResourceMetrics
	must.OK(t, reader.Collect(ctx, &rm))
	calls := map[string]uint64{}
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name != telemetry.RPCCallDurationMetric {
				continue
			}
			for _, dp := range m.Data.(metricdata.Histogram[float64]).DataPoints {
				svc, _ := dp.Attributes.Value(telemetry.AttrRPCService)
				method, _ := dp.Attributes.Value(telemetry.AttrRPCMethod)
				code, _ := dp.Attributes.Value(telemetry.AttrRPCCode)
				calls[svc.AsString()+"/"+method.AsString()+"/"+code.AsString()] += dp.Count
			}
		}
	}
	if calls["orkstack.v1.SystemService/GetSystemInfo/ok"] != 1 || calls["orkstack.v1.RunService/GetRun/"+wantCode] != 1 {
		t.Fatalf("calls = %v; want GetSystemInfo ok and GetRun %s once each", calls, wantCode)
	}
}
