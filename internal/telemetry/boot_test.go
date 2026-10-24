package telemetry

import (
	"bytes"
	"context"
	"log/slog"
	"reflect"
	"testing"

	"go.temporal.io/sdk/client"
)

// TestBootZeroConfig confirms Boot, given no environment at all, returns a
// Config that reports nothing enabled and a Sinks whose Default() is the
// same no-op Providers SetupSinks alone would give — Boot adds no behavior
// of its own beyond composing ConfigFromEnv and SetupSinks.
func TestBootZeroConfig(t *testing.T) {
	cfg, sinks, err := Boot(ServiceOrker)
	if err != nil {
		t.Fatalf("Boot: %v", err)
	}
	if cfg.Enabled() {
		t.Errorf("Config.Enabled() = true for a bare environment")
	}
	if sinks.Default() == nil {
		t.Fatal("Sinks.Default() = nil")
	}
	if sinks.Default().Enabled() {
		t.Errorf("Sinks.Default().Enabled() = true for a bare environment")
	}
}

// TestClientOptionsZeroConfigIsByteIdentical is ClientOptions' load-bearing
// promise: with telemetry off, the returned client.Options must be exactly
// what both cmd/ork and cmd/orker dialed with before either had a
// MetricsHandler or Interceptors to set — client.Options{HostPort, Namespace,
// Logger}, nothing more.
func TestClientOptionsZeroConfigIsByteIdentical(t *testing.T) {
	p, err := Setup(context.Background(), Config{})
	if err != nil {
		t.Fatalf("Setup: %v", err)
	}
	logger := slog.New(slog.NewTextHandler(&bytes.Buffer{}, nil))

	got, err := ClientOptions("127.0.0.1:7233", "default", p, logger)
	if err != nil {
		t.Fatalf("ClientOptions: %v", err)
	}
	want := client.Options{
		HostPort:  "127.0.0.1:7233",
		Namespace: "default",
		Logger:    logger,
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("ClientOptions on disabled telemetry = %+v, want %+v (byte-identical to the historical literal)", got, want)
	}
}

// TestClientOptionsMetricsAndTracesOnSetsHandlerAndInterceptors is the
// non-zero-config half: with metrics and traces on, MetricsHandler and
// Interceptors are populated instead of left zero.
func TestClientOptionsMetricsAndTracesOnSetsHandlerAndInterceptors(t *testing.T) {
	p, err := Setup(context.Background(), Config{
		MetricsExporters: []string{ExporterPrometheus},
		TracesExporter:   ExporterOTLP,
		AdminAddr:        "127.0.0.1:0",
	})
	if err != nil {
		t.Fatalf("Setup: %v", err)
	}
	defer p.Shutdown(context.Background())

	got, err := ClientOptions("127.0.0.1:7233", "default", p, slog.Default())
	if err != nil {
		t.Fatalf("ClientOptions: %v", err)
	}
	if got.MetricsHandler == nil {
		t.Error("MetricsHandler = nil, want a handler (metrics are on)")
	}
	if len(got.Interceptors) == 0 {
		t.Error("Interceptors = empty, want the tracing interceptor (traces are on)")
	}
}

// TestLogBootSkipsWhenDisabled confirms LogBoot logs nothing at all for a
// Config with nothing enabled — the same silent-by-default promise every
// other entry point in this package makes.
func TestLogBootSkipsWhenDisabled(t *testing.T) {
	var buf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&buf, nil))
	sinks, err := SetupSinks(context.Background(), Config{})
	if err != nil {
		t.Fatalf("SetupSinks: %v", err)
	}
	LogBoot(logger, Config{}, sinks, "")
	if buf.Len() != 0 {
		t.Errorf("LogBoot logged %q for a disabled Config, want nothing", buf.String())
	}
}

// TestLogBootLogsPerSinkStatus confirms LogBoot logs the boot summary plus
// one "telemetry sink ready" line per sink when telemetry is on.
func TestLogBootLogsPerSinkStatus(t *testing.T) {
	table, err := ParseSinks([]byte("sinks:\n  datadog:\n    logs_endpoint: http://127.0.0.1:1\n"))
	if err != nil {
		t.Fatalf("ParseSinks: %v", err)
	}
	if err := table.Resolve(nil, nil); err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	cfg := Config{LogsExporter: ExporterOTLP, Sinks: table}
	sinks, err := SetupSinks(context.Background(), cfg)
	if err != nil {
		t.Fatalf("SetupSinks: %v", err)
	}
	defer sinks.Shutdown(context.Background())

	var buf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&buf, nil))
	LogBoot(logger, cfg, sinks, "127.0.0.1:9090")

	logged := buf.String()
	if !containsAll(logged, "telemetry", "telemetry sink ready", "sink=default", "sink=datadog") {
		t.Errorf("LogBoot output = %q, want a telemetry line plus one per-sink line", logged)
	}
}
