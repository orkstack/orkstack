package main

import (
	"context"
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

// testAddress is an address no SDK default names, so a setting dropped or
// swapped on the way to the dial shows; a loopback IP literal on a port
// nothing listens on, so the canceled dial resolves no name.
const testAddress = "127.0.0.1:1"

// dialedWith is what connectTemporal hands the SDK to dial with. The context
// is already canceled, so the dial itself fails at once and nothing is
// contacted: what is read is the options.
func dialedWith(t *testing.T, cfg config, sinks *telemetry.Sinks, logger *slog.Logger) client.Options {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	var dialed client.Options
	told := false
	_, _ = connectTemporal(ctx, cfg, testOrkerID, sinks, logger, func(options client.Options) { dialed, told = options, true })
	if !told {
		t.Fatal("connectTemporal dialed without building its options")
	}
	return dialed
}

// testOrkerID is the Orker id the dials above are made for.
const testOrkerID = "orker-dial-test"

// requireOrkerDial holds what every Orker dial carries whatever telemetry is
// set: where it dials, its Orker id as its identity, so what its client sends
// itself (its tasks' heartbeats) names the Orker as its workers' polls do, and
// the worker's environment never reported. An Orker id holds no whitespace
// (task.ValidOrkerID), so it advertises no engine capability.
func requireOrkerDial(t *testing.T, got client.Options, cfg config, logger *slog.Logger) {
	t.Helper()
	if got.HostPort != cfg.temporalAddress || got.Namespace != cfg.temporalNamespace || got.Logger != logger {
		t.Fatalf("dialed %q in namespace %q with logger %v, want %q, %q and the process logger",
			got.HostPort, got.Namespace, got.Logger, cfg.temporalAddress, cfg.temporalNamespace)
	}
	if got.Identity != testOrkerID {
		t.Fatalf("identity = %q, want the Orker id %q", got.Identity, testOrkerID)
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

	// Where this binary dials is a setting of its own: values that differ
	// from each other and from the SDK's defaults, so one swapped for another
	// shows. A real, non-nil *slog.Logger: client.Options.Logger is the
	// Temporal SDK's own log.Logger interface, and a typed-nil *slog.Logger
	// boxed into it would never compare equal to a literal nil.
	cfg := config{temporalAddress: testAddress, temporalNamespace: "ork-zero-config"}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	got := dialedWith(t, cfg, sinks, logger)
	requireOrkerDial(t, got, cfg, logger)
	if got.MetricsHandler != nil {
		t.Fatalf("client.Options.MetricsHandler = %v, want nil", got.MetricsHandler)
	}
	if len(got.Interceptors) != 0 {
		t.Fatalf("client.Options.Interceptors = %v, want none", got.Interceptors)
	}

	// orkerOptions is where the wiring pass adds three telemetry-derived
	// seams (TaskLogs, Phases, TraceContext): with no telemetry env set, all
	// three must stay nil, the same as before this package existed.
	opts := orkerOptions(cfg, tcfg, sinks, nil, orker.LogOptions{}, nil, nil, nil, logger)
	if opts.TaskLogs != nil {
		t.Fatalf("orkerOptions.TaskLogs = %v, want nil with no telemetry env set", opts.TaskLogs)
	}
	if opts.Phases != nil {
		t.Fatalf("orkerOptions.Phases = %v, want nil with no telemetry env set", opts.Phases)
	}
	if opts.TraceContext != nil {
		t.Fatal("orkerOptions.TraceContext = non-nil, want nil with no telemetry env set")
	}
	if opts.CapacityObserver != nil {
		t.Fatalf("orkerOptions.CapacityObserver = %v, want nil with no telemetry env set", opts.CapacityObserver)
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
	_, sinks, err := telemetry.Boot(telemetry.ServiceOrker)
	if err != nil {
		t.Fatalf("telemetry.Boot: %v", err)
	}

	cfg := config{temporalAddress: testAddress, temporalNamespace: "ork-telemetry-on"}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	got := dialedWith(t, cfg, sinks, logger)
	requireOrkerDial(t, got, cfg, logger)
	if got.MetricsHandler == nil {
		t.Fatal("client.Options.MetricsHandler = nil with OTEL_METRICS_EXPORTER=otlp, want the Temporal client's metrics")
	}
	if len(got.Interceptors) == 0 {
		t.Fatal("client.Options.Interceptors = none with OTEL_TRACES_EXPORTER=otlp, want the tracing interceptor")
	}
}

// TestOrkerOptionsWithMetricsOn: the capacity metrics are wired only when
// metrics are exported, and tracing alone does not wire them.
func TestOrkerOptionsWithMetricsOn(t *testing.T) {
	clearTelemetryEnv(t)
	t.Setenv(telemetry.MetricsExporterVar, "otlp")
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "http://127.0.0.1:1")

	tcfg, sinks, err := telemetry.Boot(telemetry.ServiceOrker)
	if err != nil {
		t.Fatalf("telemetry.Boot: %v", err)
	}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	cfg := config{temporalAddress: client.DefaultHostPort, temporalNamespace: "default"}
	if opts := orkerOptions(cfg, tcfg, sinks, nil, orker.LogOptions{}, nil, nil, nil, logger); opts.CapacityObserver == nil {
		t.Fatal("orkerOptions.CapacityObserver = nil, want the capacity metrics with OTEL_METRICS_EXPORTER=otlp")
	}

	clearTelemetryEnv(t)
	t.Setenv(telemetry.TracesExporterVar, "otlp")
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "http://127.0.0.1:1")
	tcfg, sinks, err = telemetry.Boot(telemetry.ServiceOrker)
	if err != nil {
		t.Fatalf("telemetry.Boot: %v", err)
	}
	if opts := orkerOptions(cfg, tcfg, sinks, nil, orker.LogOptions{}, nil, nil, nil, logger); opts.CapacityObserver != nil {
		t.Fatalf("orkerOptions.CapacityObserver = %v, want nil with metrics off", opts.CapacityObserver)
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

	opts := orkerOptions(cfg, tcfg, sinks, nil, orker.LogOptions{}, nil, nil, nil, logger)
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
