// Package telemetry is the one place OrkStack speaks OpenTelemetry.
//
// It exists so that an operator can ship the engine's and an Orker's logs,
// metrics, traces and profiles to whatever they already run — Loki, Tempo,
// Prometheus, Datadog, Pyroscope — with no vendor code in this repository:
// every signal leaves as OTLP or as a standard scrape endpoint, and the
// destination is the standard OTEL_* environment.
//
// Two properties are load-bearing and every function here keeps them:
//
//   - OFF BY DEFAULT. With no OTEL_* or ORK_TELEMETRY_* variable set, Setup
//     returns no-op providers, NewLogger returns exactly the text handler the
//     binaries always had, no listener opens, and nothing is registered with
//     the Temporal client. A deployment that never heard of this package runs
//     as it did before it existed.
//   - BEST EFFORT. An exporter that cannot reach its collector is logged and
//     counted, never returned after boot; a record that cannot be queued is
//     dropped, never waited for. Telemetry cannot fail a task or a run.
//
// Activity images are outside this package's reach on purpose: the Orker
// exports what a container writes to stdout and stderr, and the container
// needs no SDK, agent, sidecar, variable or file to be observed
// (docs/OCI_ACTIVITY_CONTRACT.md).
package telemetry

import (
	"fmt"
	"log/slog"
	"os"
	"sort"
	"strconv"
	"strings"
)

// Service names. They are the default service.name of each binary and the
// instrumentation scope every instrument is registered under.
const (
	ServiceEngine = "ork"
	ServiceOrker  = "orker"
)

// OrkStack's own switches. Everything else is the standard OpenTelemetry
// environment, read by the SDK itself (OTEL_EXPORTER_OTLP_*, OTEL_BSP_*,
// OTEL_BLRP_*, OTEL_METRIC_EXPORT_INTERVAL, OTEL_TRACES_SAMPLER, ...).
const (
	// LogFormatVar selects the process log encoding: "text" (the default, and
	// what both binaries always printed) or "json".
	LogFormatVar = "ORK_LOG_FORMAT"
	// LogLevelVar is the minimum process log level: debug, info, warn, error.
	LogLevelVar = "ORK_LOG_LEVEL"
	// AdminAddrVar is the listener for /healthz, /readyz, /metrics and
	// /debug/pprof. Empty, the default, opens nothing.
	AdminAddrVar = "ORK_ADMIN_ADDR"
	// PprofEnabledVar mounts net/http/pprof on the admin listener. Off by
	// default: an Orker's heap holds resolved secret bytes.
	PprofEnabledVar = "ORK_PPROF_ENABLED"
	// HighCardinalityVar adds run_id, task_id, orkflow and orkflow_version to
	// every ork_* metric that knows them. Off by default because each distinct
	// value is a new time series; an operator who wants per-run metrics and
	// accepts that cost turns it on.
	HighCardinalityVar = "ORK_TELEMETRY_HIGH_CARDINALITY"

	// Standard OpenTelemetry selectors, read here rather than by the SDK
	// because the SDK has no "which exporter" autoconfiguration in Go.
	ServiceNameVar     = "OTEL_SERVICE_NAME"
	TracesExporterVar  = "OTEL_TRACES_EXPORTER"
	MetricsExporterVar = "OTEL_METRICS_EXPORTER"
	LogsExporterVar    = "OTEL_LOGS_EXPORTER"
)

// Exporter names accepted by the OTEL_*_EXPORTER variables.
const (
	ExporterNone       = "none"
	ExporterOTLP       = "otlp"
	ExporterPrometheus = "prometheus"
)

// Log formats accepted by ORK_LOG_FORMAT.
const (
	LogFormatText = "text"
	LogFormatJSON = "json"
)

// DefaultLogLevel is configFrom's fallback for ORK_LOG_LEVEL: the same
// spelling slog.Level.UnmarshalText accepts, so internal/config's Schema
// can bind ORK_LOG_LEVEL's documented default to the one constant this
// package's own default is derived from (see defaultLogLevel below).
const DefaultLogLevel = "info"

// defaultLogLevel is DefaultLogLevel already parsed, computed once so
// configFrom need not re-parse its own constant on every call.
var defaultLogLevel = func() slog.Level {
	var level slog.Level
	if err := level.UnmarshalText([]byte(DefaultLogLevel)); err != nil {
		panic(fmt.Sprintf("telemetry: DefaultLogLevel %q does not parse: %v", DefaultLogLevel, err))
	}
	return level
}()

// Config is everything this package decides from the environment. The zero
// value is "off": no exporters, no listener, text logs at info.
type Config struct {
	// ServiceName is service.name on every signal.
	ServiceName string
	// TracesExporter is "none" or "otlp".
	TracesExporter string
	// MetricsExporters is the set of "prometheus" and "otlp" that were asked
	// for, in the order given, with "none" already removed.
	MetricsExporters []string
	// LogsExporter is "none" or "otlp".
	LogsExporter string
	// AdminAddr is the admin listener, or empty for none.
	AdminAddr string
	// PprofEnabled mounts pprof on the admin listener.
	PprofEnabled bool
	// HighCardinality adds per-run and per-task labels to metrics.
	HighCardinality bool
	// LogFormat is "text" or "json".
	LogFormat string
	// LogLevel is the minimum level the process logger emits.
	LogLevel slog.Level
	// Sinks is every named OTLP destination declared in ORK_TELEMETRY_SINKS,
	// already Resolve()d by ConfigFromEnv. The zero value is an empty table:
	// no named sinks, and the OTEL_* environment remains the default.
	Sinks SinkTable
}

// ConfigFromEnv reads the environment for one binary. service is the default
// service.name — ServiceEngine or ServiceOrker.
//
// A malformed value is an error naming the variable, for the caller to refuse
// boot with: an exporter list this process cannot read is a configuration
// error, and discovering it as "no metrics ever arrived" would be worse than a
// refused start (the same rule cmd/ork applies to ORK_ACTIVITY_CATALOG).
//
// ORK_TELEMETRY_SINKS is parsed here too, and its header sources are resolved
// against the real environment and filesystem before this function returns:
// a sink whose secret cannot be found refuses boot the same way a malformed
// exporter list does, rather than exporting with a blank header later.
func ConfigFromEnv(service string) (Config, error) {
	cfg, err := configFrom(service, os.LookupEnv)
	if err != nil {
		return Config{}, err
	}
	if err := cfg.Sinks.Resolve(os.LookupEnv, os.ReadFile); err != nil {
		return Config{}, joinErrors([]error{err})
	}
	return cfg, nil
}

// configFrom is ConfigFromEnv over an explicit lookup, for tests.
func configFrom(service string, lookup func(string) (string, bool)) (Config, error) {
	get := func(key string) string {
		value, ok := lookup(key)
		if !ok {
			return ""
		}
		return strings.TrimSpace(value)
	}
	cfg := Config{
		ServiceName:    service,
		TracesExporter: ExporterNone,
		LogsExporter:   ExporterNone,
		LogFormat:      LogFormatText,
		LogLevel:       defaultLogLevel,
		AdminAddr:      get(AdminAddrVar),
	}
	if name := get(ServiceNameVar); name != "" {
		cfg.ServiceName = name
	}
	var errs []error

	if raw := get(LogFormatVar); raw != "" {
		switch strings.ToLower(raw) {
		case LogFormatText, LogFormatJSON:
			cfg.LogFormat = strings.ToLower(raw)
		default:
			errs = append(errs, fmt.Errorf("%s=%q must be %q or %q", LogFormatVar, raw, LogFormatText, LogFormatJSON))
		}
	}
	if raw := get(LogLevelVar); raw != "" {
		var level slog.Level
		if err := level.UnmarshalText([]byte(raw)); err != nil {
			errs = append(errs, fmt.Errorf("%s=%q must be debug, info, warn or error", LogLevelVar, raw))
		} else {
			cfg.LogLevel = level
		}
	}
	for _, flag := range []struct {
		key string
		dst *bool
	}{{PprofEnabledVar, &cfg.PprofEnabled}, {HighCardinalityVar, &cfg.HighCardinality}} {
		raw := get(flag.key)
		if raw == "" {
			continue
		}
		parsed, err := strconv.ParseBool(raw)
		if err != nil {
			errs = append(errs, fmt.Errorf("%s=%q must be true or false", flag.key, raw))
			continue
		}
		*flag.dst = parsed
	}

	if raw := get(TracesExporterVar); raw != "" {
		names, err := exporterList(TracesExporterVar, raw, ExporterOTLP)
		if err != nil {
			errs = append(errs, err)
		} else if len(names) > 0 {
			cfg.TracesExporter = ExporterOTLP
		}
	}
	if raw := get(LogsExporterVar); raw != "" {
		names, err := exporterList(LogsExporterVar, raw, ExporterOTLP)
		if err != nil {
			errs = append(errs, err)
		} else if len(names) > 0 {
			cfg.LogsExporter = ExporterOTLP
		}
	}
	if raw := get(MetricsExporterVar); raw != "" {
		names, err := exporterList(MetricsExporterVar, raw, ExporterOTLP, ExporterPrometheus)
		if err != nil {
			errs = append(errs, err)
		} else {
			cfg.MetricsExporters = names
		}
	}
	if cfg.hasMetricsExporter(ExporterPrometheus) && cfg.AdminAddr == "" {
		errs = append(errs, fmt.Errorf("%s includes %q, which is served on the admin listener: set %s",
			MetricsExporterVar, ExporterPrometheus, AdminAddrVar))
	}

	sinks, err := ParseSinks([]byte(get(SinksVar)))
	if err != nil {
		errs = append(errs, err)
	} else {
		cfg.Sinks = sinks
	}

	if len(errs) > 0 {
		return Config{}, joinErrors(errs)
	}
	return cfg, nil
}

// exporterList parses a comma-separated OTEL_*_EXPORTER value against the
// names this package can honor. "none" is accepted and dropped; anything else
// unknown is an error that names the variable and the accepted set.
func exporterList(key, raw string, allowed ...string) ([]string, error) {
	var names []string
	seen := map[string]bool{}
	for _, part := range strings.Split(raw, ",") {
		name := strings.ToLower(strings.TrimSpace(part))
		if name == "" || name == ExporterNone {
			continue
		}
		ok := false
		for _, candidate := range allowed {
			if name == candidate {
				ok = true
				break
			}
		}
		if !ok {
			accepted := append([]string{ExporterNone}, allowed...)
			sort.Strings(accepted)
			return nil, fmt.Errorf("%s=%q: unsupported exporter %q (accepted: %s)",
				key, raw, name, strings.Join(accepted, ", "))
		}
		if !seen[name] {
			seen[name] = true
			names = append(names, name)
		}
	}
	return names, nil
}

func joinErrors(errs []error) error {
	parts := make([]string, 0, len(errs))
	for _, err := range errs {
		parts = append(parts, err.Error())
	}
	return fmt.Errorf("telemetry configuration: %s", strings.Join(parts, "; "))
}

func (c Config) hasMetricsExporter(name string) bool {
	for _, candidate := range c.MetricsExporters {
		if candidate == name {
			return true
		}
	}
	return false
}

// TracesEnabled reports whether spans leave the process.
func (c Config) TracesEnabled() bool { return c.TracesExporter == ExporterOTLP }

// MetricsEnabled reports whether any metrics exporter was asked for.
func (c Config) MetricsEnabled() bool { return len(c.MetricsExporters) > 0 }

// PrometheusEnabled reports whether /metrics is served.
func (c Config) PrometheusEnabled() bool { return c.hasMetricsExporter(ExporterPrometheus) }

// OTLPMetricsEnabled reports whether metrics are pushed over OTLP.
func (c Config) OTLPMetricsEnabled() bool { return c.hasMetricsExporter(ExporterOTLP) }

// LogsEnabled reports whether log records leave the process over OTLP.
func (c Config) LogsEnabled() bool { return c.LogsExporter == ExporterOTLP }

// Enabled reports whether anything at all was switched on. False is the
// zero-config path, and the promise that path makes is that the binaries
// behave as they did before this package existed.
func (c Config) Enabled() bool {
	return c.TracesEnabled() || c.MetricsEnabled() || c.LogsEnabled() || c.AdminAddr != ""
}

// SignalsEnabled reports whether any signal (traces, metrics or logs) leaves
// the process — Enabled minus the admin listener, which serves /healthz and
// /readyz whether or not a single signal is switched on. Providers.Enabled
// documents that it is exactly this.
func (c Config) SignalsEnabled() bool {
	return c.TracesEnabled() || c.MetricsEnabled() || c.LogsEnabled()
}

// boundTo replaces c's per-signal exporter selection with what spec, a named
// sink's own destinations, can actually carry: traces and logs are switched
// on exactly when spec names an endpoint for them, and OTLP metrics the same
// — the global OTEL_*_EXPORTER flags govern only the environment-backed
// default (spec == nil), never a spec-bound sink, so a sink can carry a
// signal the deployment default never turned on, or the reverse. Prometheus
// is left untouched: it is a local scrape registry, not a destination a spec
// names, and the caller (SetupSinks) already decides whether this cfg may
// carry it before calling buildProviders.
func (c Config) boundTo(spec *SinkSpec) Config {
	if spec == nil {
		return c
	}
	out := c
	if _, err := spec.endpointFor("traces", otlpTracesPath); err == nil {
		out.TracesExporter = ExporterOTLP
	} else {
		out.TracesExporter = ExporterNone
	}
	if _, err := spec.endpointFor("logs", otlpLogsPath); err == nil {
		out.LogsExporter = ExporterOTLP
	} else {
		out.LogsExporter = ExporterNone
	}
	metrics := make([]string, 0, len(c.MetricsExporters)+1)
	if c.hasMetricsExporter(ExporterPrometheus) {
		metrics = append(metrics, ExporterPrometheus)
	}
	if _, err := spec.endpointFor("metrics", otlpMetricsPath); err == nil {
		metrics = append(metrics, ExporterOTLP)
	}
	out.MetricsExporters = metrics
	return out
}
