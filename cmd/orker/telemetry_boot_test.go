package main

import (
	"io"
	"log/slog"
	"os"
	"strings"
	"testing"

	"go.temporal.io/sdk/client"

	"github.com/orkstack/orkstack/internal/orker"
	"github.com/orkstack/orkstack/internal/telemetry"
)

// clearTelemetryEnv unsets every variable telemetry.ConfigFromEnv reads, and
// restores whatever the test process had on cleanup. ConfigFromEnv uses
// os.LookupEnv, so an empty-string t.Setenv is not the same as "unset" — a
// test asserting the zero-config path must actually remove the variable.
func clearTelemetryEnv(t *testing.T) {
	t.Helper()
	vars := []string{
		telemetry.LogFormatVar,
		telemetry.LogLevelVar,
		telemetry.AdminAddrVar,
		telemetry.PprofEnabledVar,
		telemetry.HighCardinalityVar,
		telemetry.ServiceNameVar,
		telemetry.TracesExporterVar,
		telemetry.MetricsExporterVar,
		telemetry.LogsExporterVar,
	}
	for _, name := range vars {
		name := name
		old, had := os.LookupEnv(name)
		_ = os.Unsetenv(name)
		t.Cleanup(func() {
			if had {
				_ = os.Setenv(name, old)
			} else {
				_ = os.Unsetenv(name)
			}
		})
	}
}

// TestBootTelemetryZeroConfig holds the non-negotiable invariant: with no
// telemetry env set, this binary's sinks, its Temporal metrics handler, and
// the client options it dials with are exactly what it had before this
// package existed.
func TestBootTelemetryZeroConfig(t *testing.T) {
	clearTelemetryEnv(t)

	tcfg, sinks, err := telemetry.Boot(telemetry.ServiceOrker)
	if err != nil {
		t.Fatalf("telemetry.Boot: %v", err)
	}
	if sinks.Default().Enabled() {
		t.Fatalf("sinks.Default().Enabled() = true, want false with no telemetry env set")
	}
	if h := telemetry.TemporalMetricsHandler(sinks.Default(), nil); h != nil {
		t.Fatalf("TemporalMetricsHandler = %v, want nil", h)
	}

	cfg := config{
		temporalAddress:   client.DefaultHostPort,
		temporalNamespace: "default",
	}
	// A real, non-nil *slog.Logger: client.Options.Logger is the Temporal
	// SDK's own log.Logger interface, and comparing a typed-nil *slog.Logger
	// boxed into that interface against a literal nil would never be equal —
	// that is an artifact of how Go interfaces box nils, not something this
	// test should be asserting about.
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	got, err := telemetry.ClientOptions(cfg.temporalAddress, cfg.temporalNamespace, sinks.Default(), logger)
	if err != nil {
		t.Fatalf("telemetry.ClientOptions: %v", err)
	}
	want := client.Options{
		HostPort:  cfg.temporalAddress,
		Namespace: cfg.temporalNamespace,
		Logger:    logger,
	}
	if got.HostPort != want.HostPort || got.Namespace != want.Namespace || got.Logger != want.Logger {
		t.Fatalf("telemetry.ClientOptions = %+v, want %+v", got, want)
	}
	if got.MetricsHandler != nil {
		t.Fatalf("client.Options.MetricsHandler = %v, want nil", got.MetricsHandler)
	}
	if len(got.Interceptors) != 0 {
		t.Fatalf("client.Options.Interceptors = %v, want none", got.Interceptors)
	}

	// orkerOptions is where the wiring pass adds three telemetry-derived
	// seams (TaskLogs, Phases, TraceContext): with no telemetry env set, all
	// three must stay nil, the same as before this package existed.
	opts := orkerOptions(cfg, tcfg, sinks, nil, orker.LogOptions{}, nil, nil, logger)
	if opts.TaskLogs != nil {
		t.Fatalf("orkerOptions.TaskLogs = %v, want nil with no telemetry env set", opts.TaskLogs)
	}
	if opts.Phases != nil {
		t.Fatalf("orkerOptions.Phases = %v, want nil with no telemetry env set", opts.Phases)
	}
	if opts.TraceContext != nil {
		t.Fatal("orkerOptions.TraceContext = non-nil, want nil with no telemetry env set")
	}
}

// TestOrkerOptionsWithTracingOn asserts the other side of orkerOptions: with
// tracing on, Phases and TraceContext are both wired, and with logs on,
// TaskLogs is too.
func TestOrkerOptionsWithTracingOn(t *testing.T) {
	clearTelemetryEnv(t)
	t.Setenv(telemetry.TracesExporterVar, "otlp")
	t.Setenv(telemetry.LogsExporterVar, "otlp")
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "http://127.0.0.1:1")

	tcfg, sinks, err := telemetry.Boot(telemetry.ServiceOrker)
	if err != nil {
		t.Fatalf("telemetry.Boot: %v", err)
	}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	cfg := config{temporalAddress: client.DefaultHostPort, temporalNamespace: "default"}

	opts := orkerOptions(cfg, tcfg, sinks, nil, orker.LogOptions{}, nil, nil, logger)
	if opts.TaskLogs == nil {
		t.Fatal("orkerOptions.TaskLogs = nil, want non-nil with OTEL_LOGS_EXPORTER=otlp")
	}
	if opts.Phases == nil {
		t.Fatal("orkerOptions.Phases = nil, want non-nil with OTEL_TRACES_EXPORTER=otlp")
	}
	if opts.TraceContext == nil {
		t.Fatal("orkerOptions.TraceContext = nil, want non-nil with OTEL_TRACES_EXPORTER=otlp")
	}
}

// TestBootTelemetryMalformed asserts that a malformed OTEL_* variable is
// refused at boot, naming the variable, rather than silently ignored.
func TestBootTelemetryMalformed(t *testing.T) {
	clearTelemetryEnv(t)
	t.Setenv(telemetry.MetricsExporterVar, "bogus")

	_, _, err := telemetry.Boot(telemetry.ServiceOrker)
	if err == nil {
		t.Fatal("telemetry.Boot: want error for OTEL_METRICS_EXPORTER=bogus, got nil")
	}
	if !strings.Contains(err.Error(), telemetry.MetricsExporterVar) {
		t.Fatalf("telemetry.Boot error %q does not name %s", err, telemetry.MetricsExporterVar)
	}
}

// TestLoadConfig_RefusesAMalformedTaskLogExportFlag holds ORK_ORKER_TASK_LOG_EXPORT
// to the same rule as ORK_ORKER_POOL/ORK_ORKER_QUEUE: a value this process
// cannot read is a boot error naming the variable, never a silent "off".
func TestLoadConfig_RefusesAMalformedTaskLogExportFlag(t *testing.T) {
	t.Setenv(TaskLogExportVar, "notabool")

	cfg := loadConfig()
	if cfg.telemetryErr == nil {
		t.Fatalf("%s=notabool silently fell back to a default", TaskLogExportVar)
	}
	if !strings.Contains(cfg.telemetryErr.Error(), TaskLogExportVar) {
		t.Errorf("boot error %q must name %s", cfg.telemetryErr, TaskLogExportVar)
	}
}
