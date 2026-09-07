package main

import (
	"io"
	"log/slog"
	"net/http"
	"os"
	"strings"
	"testing"

	"go.temporal.io/sdk/client"

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
// the client options it dials with are exactly what it had before
// internal/telemetry existed.
func TestBootTelemetryZeroConfig(t *testing.T) {
	clearTelemetryEnv(t)

	tcfg, sinks, err := telemetry.Boot(telemetry.ServiceEngine)
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
	// A real, non-nil *slog.Logger: client.Options.Logger is the Temporal SDK's
	// own log.Logger interface, and comparing a typed-nil *slog.Logger boxed
	// into that interface against a literal nil would never be equal — that
	// is an artifact of how Go interfaces box nils, not something this test
	// should be asserting about.
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

	// The API handler is the zero-config invariant's other half: with
	// tracing and metrics both off, apiHandler must hand back the exact
	// *http.ServeMux it was given, not a wrapper around one.
	mux := http.NewServeMux()
	if h := apiHandler(tcfg, sinks, mux); h != http.Handler(mux) {
		t.Fatalf("apiHandler with telemetry off = %v, want the same *http.ServeMux", h)
	}
}

// TestAPIHandlerWrapsWithTracingOn asserts the other side of apiHandler: with
// tracing (or metrics) on, the mux is wrapped in otelhttp instrumentation
// rather than served bare.
func TestAPIHandlerWrapsWithTracingOn(t *testing.T) {
	clearTelemetryEnv(t)
	t.Setenv(telemetry.TracesExporterVar, "otlp")
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "http://127.0.0.1:1")

	_, sinks, err := telemetry.Boot(telemetry.ServiceEngine)
	if err != nil {
		t.Fatalf("telemetry.Boot: %v", err)
	}
	tcfg, err := telemetry.ConfigFromEnv(telemetry.ServiceEngine)
	if err != nil {
		t.Fatalf("ConfigFromEnv: %v", err)
	}

	mux := http.NewServeMux()
	h := apiHandler(tcfg, sinks, mux)
	if h == http.Handler(mux) {
		t.Fatal("apiHandler with tracing on = the bare *http.ServeMux, want it wrapped")
	}
}

// TestWireEventObserver_AdminAddrAloneDoesNotWireIt holds the distinction
// between Enabled and SignalsEnabled at the one call site that used to blur
// it: ORK_ADMIN_ADDR alone must open /healthz, /readyz and /metrics without
// wiring the event observer into the projection worker and the API — a
// deployment that only wants liveness probes gets no lifecycle logging it
// never asked for.
func TestWireEventObserver_AdminAddrAloneDoesNotWireIt(t *testing.T) {
	clearTelemetryEnv(t)
	t.Setenv(telemetry.AdminAddrVar, "127.0.0.1:0")

	tcfg, err := telemetry.ConfigFromEnv(telemetry.ServiceEngine)
	if err != nil {
		t.Fatalf("ConfigFromEnv: %v", err)
	}
	if !tcfg.Enabled() {
		t.Fatal("tcfg.Enabled() = false with ORK_ADMIN_ADDR set, want true")
	}
	if wireEventObserver(tcfg) {
		t.Fatal("wireEventObserver = true with only ORK_ADMIN_ADDR set, want false")
	}
}

// TestWireEventObserver_ASignalWiresIt asserts the other side: any signal at
// all — traces, metrics or logs — wires the observer in.
func TestWireEventObserver_ASignalWiresIt(t *testing.T) {
	clearTelemetryEnv(t)
	t.Setenv(telemetry.TracesExporterVar, "otlp")
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "http://127.0.0.1:1")

	tcfg, err := telemetry.ConfigFromEnv(telemetry.ServiceEngine)
	if err != nil {
		t.Fatalf("ConfigFromEnv: %v", err)
	}
	if !wireEventObserver(tcfg) {
		t.Fatal("wireEventObserver = false with OTEL_TRACES_EXPORTER=otlp, want true")
	}
}

// TestBootTelemetryMalformed asserts that a malformed OTEL_* variable is
// refused at boot, naming the variable, rather than silently ignored.
func TestBootTelemetryMalformed(t *testing.T) {
	clearTelemetryEnv(t)
	t.Setenv(telemetry.MetricsExporterVar, "bogus")

	_, _, err := telemetry.Boot(telemetry.ServiceEngine)
	if err == nil {
		t.Fatal("telemetry.Boot: want error for OTEL_METRICS_EXPORTER=bogus, got nil")
	}
	if !strings.Contains(err.Error(), telemetry.MetricsExporterVar) {
		t.Fatalf("telemetry.Boot error %q does not name %s", err, telemetry.MetricsExporterVar)
	}
}
