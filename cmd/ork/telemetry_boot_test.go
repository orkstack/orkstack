package main

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"os"
	"slices"
	"strings"
	"testing"

	"go.temporal.io/sdk/client"

	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/internal/telemetry"

	"github.com/orkstack/orkstack/internal/testsupport/must"
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

// testAddress is an address no SDK default names, so a setting dropped or
// swapped on the way to the dial shows; a loopback IP literal on a port
// nothing listens on, so the canceled dial resolves no name.
const testAddress = "127.0.0.1:1"

// dialedWith is what connectTemporal hands the SDK to dial with, and the
// identity it returns. The context is already canceled, so the dial itself
// fails at once and nothing is contacted: what is read is the options.
func dialedWith(t *testing.T, cfg config, sinks *telemetry.Sinks, logger *slog.Logger) (client.Options, string) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	var dialed client.Options
	told := false
	_, identity, _ := connectTemporal(ctx, cfg, sinks, logger, func(options client.Options) { dialed, told = options, true })
	if !told {
		t.Fatal("connectTemporal dialed without building its options")
	}
	return dialed, identity
}

// requireEngineDial holds what every engine dial carries whatever telemetry
// is set: where it dials and as whom, the engine's capabilities advertised in
// its identity (and that identity the one returned, which the cluster-side
// cancellation records), and the worker's environment never reported.
func requireEngineDial(t *testing.T, got client.Options, identity string, cfg config, logger *slog.Logger) {
	t.Helper()
	if got.HostPort != cfg.temporalAddress || got.Namespace != cfg.temporalNamespace || got.Logger != logger {
		t.Fatalf("dialed %q in namespace %q with logger %v, want %q, %q and the process logger",
			got.HostPort, got.Namespace, got.Logger, cfg.temporalAddress, cfg.temporalNamespace)
	}
	if !slices.Equal(engine.IdentityCapabilities(got.Identity), engine.EngineCapabilities) || !strings.HasPrefix(got.Identity, sdkIdentity()) {
		t.Fatalf("identity %q advertises %v, want the SDK's identity followed by %v",
			got.Identity, engine.IdentityCapabilities(got.Identity), engine.EngineCapabilities)
	}
	if identity != got.Identity {
		t.Fatalf("connectTemporal returned identity %q, but dialed as %q", identity, got.Identity)
	}
	if !got.DisableWorkerEnvironmentInfo {
		t.Fatalf("client.Options.DisableWorkerEnvironmentInfo = false, want true: the worker's environment is never reported")
	}
}

// TestBootTelemetryZeroConfig holds the non-negotiable invariant: with no
// telemetry env set, this binary's sinks, its Temporal metrics handler, and
// the client options it dials with carry no telemetry at all, and those
// options never report the worker's environment to Temporal.
func TestBootTelemetryZeroConfig(t *testing.T) {
	clearTelemetryEnv(t)

	tcfg, sinks, err := telemetry.Boot(telemetry.ServiceEngine)
	must.OKf(t, err, "telemetry.Boot")
	if sinks.Default().Enabled() {
		t.Fatalf("sinks.Default().Enabled() = true, want false with no telemetry env set")
	}
	if h := telemetry.TemporalMetricsHandler(sinks.Default(), nil); h != nil {
		t.Fatalf("TemporalMetricsHandler = %v, want nil", h)
	}

	// Where this binary dials is a setting of its own: values that differ
	// from each other and from the SDK's defaults, so one swapped for another
	// shows. A real, non-nil *slog.Logger: client.Options.Logger is the
	// Temporal SDK's own log.Logger interface, and a typed-nil *slog.Logger
	// boxed into it would never compare equal to a literal nil.
	cfg := config{temporalAddress: testAddress, temporalNamespace: "ork-zero-config"}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	got, identity := dialedWith(t, cfg, sinks, logger)
	requireEngineDial(t, got, identity, cfg, logger)
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
	must.OKf(t, err, "telemetry.Boot")
	tcfg, err := telemetry.ConfigFromEnv(telemetry.ServiceEngine)
	must.OKf(t, err, "ConfigFromEnv")

	mux := http.NewServeMux()
	h := apiHandler(tcfg, sinks, mux)
	if h == http.Handler(mux) {
		t.Fatal("apiHandler with tracing on = the bare *http.ServeMux, want it wrapped")
	}
}

// TestConnectTemporal_DialsWithTheTelemetryItWasGiven: with metrics and traces
// on, the options this binary dials with carry the default sink's metrics
// handler and tracing interceptor, and the dial is otherwise what it is with
// telemetry off.
func TestConnectTemporal_DialsWithTheTelemetryItWasGiven(t *testing.T) {
	clearTelemetryEnv(t)
	t.Setenv(telemetry.MetricsExporterVar, "otlp")
	t.Setenv(telemetry.TracesExporterVar, "otlp")
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "http://127.0.0.1:1")
	_, sinks, err := telemetry.Boot(telemetry.ServiceEngine)
	must.OKf(t, err, "telemetry.Boot")

	cfg := config{temporalAddress: testAddress, temporalNamespace: "ork-telemetry-on"}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	got, identity := dialedWith(t, cfg, sinks, logger)
	requireEngineDial(t, got, identity, cfg, logger)
	if got.MetricsHandler == nil {
		t.Fatal("client.Options.MetricsHandler = nil with OTEL_METRICS_EXPORTER=otlp, want the Temporal client's metrics")
	}
	if len(got.Interceptors) == 0 {
		t.Fatal("client.Options.Interceptors = none with OTEL_TRACES_EXPORTER=otlp, want the tracing interceptor")
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
	must.OKf(t, err, "ConfigFromEnv")
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
	must.OKf(t, err, "ConfigFromEnv")
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
