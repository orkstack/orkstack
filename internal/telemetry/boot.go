package telemetry

import (
	"context"
	"log/slog"

	"go.opentelemetry.io/otel/attribute"
	"go.temporal.io/sdk/client"
)

// Boot is cmd/ork's and cmd/orker's shared telemetry bring-up: read the
// environment for service, build every sink SetupSinks names, and hand both
// back for the caller to build its logger from (sinks.Default()) and defer
// sinks.Shutdown on. It exists because both mains carried this exact
// function, verbatim, before this package had one: ConfigFromEnv then
// SetupSinks, nothing else — a caller that wants the default sink alone,
// process attributes and all, uses sinks.Default() on the result rather than
// Setup directly, so that TelemetrySelection routing (Sinks) is always
// available even to a caller that never intends to use a second sink.
func Boot(service string, attrs ...attribute.KeyValue) (Config, *Sinks, error) {
	cfg, err := ConfigFromEnv(service)
	if err != nil {
		return Config{}, nil, err
	}
	sinks, err := SetupSinks(context.Background(), cfg, attrs...)
	if err != nil {
		return Config{}, nil, err
	}
	return cfg, sinks, nil
}

// ClientOptions builds the client.Options a Temporal client dials with. It
// always takes the DEFAULT sink's Providers: the Temporal client is a
// process-level concern, never one a namespace's own sink could redirect.
// With telemetry off, p is the zero-config Providers SetupSinks (via Boot)
// returns for it, TemporalMetricsHandler and TemporalInterceptors both answer
// nil, and the returned options are byte-identical to
// client.Options{HostPort: hostPort, Namespace: namespace, Logger: logger,
// DisableWorkerEnvironmentInfo: true}
// (TestClientOptionsZeroConfigIsByteIdentical holds this).
//
// DisableWorkerEnvironmentInfo is set whatever telemetry says. From SDK 1.49
// a worker's heartbeats otherwise report the Go version, the hosting
// environment the SDK detects (a container, Kubernetes, a cloud platform) and
// the OS platform to the Temporal server. OrkStack sends nothing it was not
// configured to send (AGENTS.md §11: off by default), and no setting asks for
// this. cmd/ork and cmd/orker dial only with these options
// (TestClientOptions_IsHowOrkAndOrkerDialTemporal holds that).
func ClientOptions(hostPort, namespace string, p *Providers, logger *slog.Logger) (client.Options, error) {
	opts := client.Options{
		HostPort:                     hostPort,
		Namespace:                    namespace,
		Logger:                       logger,
		MetricsHandler:               TemporalMetricsHandler(p, logger),
		DisableWorkerEnvironmentInfo: true,
	}
	interceptors, err := TemporalInterceptors(p)
	if err != nil {
		return client.Options{}, err
	}
	if interceptors != nil {
		opts.Interceptors = interceptors
	}
	return opts, nil
}

// LogBoot logs the one "telemetry" boot line both mains print, plus one
// "telemetry sink ready" line per sink (Sinks.Describe) — SetupSinks itself
// never logs, having no logger of its own, so a caller that wants this
// summary calls LogBoot once it has built its process logger. Both lines are
// skipped entirely when cfg.Enabled() is false: a deployment that switched
// nothing on, not even the admin listener, gets exactly the silence it had
// before this package existed.
func LogBoot(logger *slog.Logger, cfg Config, s *Sinks, adminAddr string) {
	if !cfg.Enabled() {
		return
	}
	logger.Info("telemetry",
		"enabled", cfg.SignalsEnabled(),
		"admin_addr", adminAddr,
		"log_format", cfg.LogFormat,
		"metrics", cfg.MetricsExporters,
		"traces", cfg.TracesExporter,
		"logs", cfg.LogsExporter,
		"high_cardinality", cfg.HighCardinality,
		"sinks", s.Names())
	for _, status := range s.Describe() {
		logger.Info("telemetry sink ready",
			"sink", status.Name,
			"logs", status.Logs,
			"traces", status.Traces,
			"metrics", status.Metrics)
	}
}
