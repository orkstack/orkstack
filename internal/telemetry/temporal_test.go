package telemetry

import (
	"context"
	"testing"
)

func TestTemporalMetricsHandlerPrometheus(t *testing.T) {
	p, err := Setup(context.Background(), Config{
		MetricsExporters: []string{ExporterPrometheus},
		AdminAddr:        "127.0.0.1:0",
	})
	if err != nil {
		t.Fatalf("Setup: %v", err)
	}
	defer p.Shutdown(context.Background())

	handler := TemporalMetricsHandler(p, nil)
	if handler == nil {
		t.Fatal("TemporalMetricsHandler = nil, want a handler")
	}
	// Must not panic; that is the whole contract at this layer.
	handler.Counter("x").Inc(1)
}

func TestTemporalInterceptorsTracing(t *testing.T) {
	p, err := Setup(context.Background(), Config{TracesExporter: ExporterOTLP})
	if err != nil {
		t.Fatalf("Setup: %v", err)
	}
	defer p.Shutdown(context.Background())

	interceptors, err := TemporalInterceptors(p)
	if err != nil {
		t.Fatalf("TemporalInterceptors: %v", err)
	}
	if len(interceptors) != 1 {
		t.Fatalf("TemporalInterceptors = %d interceptors, want 1", len(interceptors))
	}
}
