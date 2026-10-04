package telemetry

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlplog/otlploghttp"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetrichttp"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	otelprom "go.opentelemetry.io/otel/exporters/prometheus"
	"go.opentelemetry.io/otel/log"
	lognoop "go.opentelemetry.io/otel/log/noop"
	"go.opentelemetry.io/otel/metric"
	metricnoop "go.opentelemetry.io/otel/metric/noop"
	"go.opentelemetry.io/otel/propagation"
	sdklog "go.opentelemetry.io/otel/sdk/log"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
	tracenoop "go.opentelemetry.io/otel/trace/noop"
)

// ScopeName is the instrumentation scope every OrkStack instrument, span and
// log record is registered under.
const ScopeName = "github.com/orkstack/orkstack"

// Attribute keys OrkStack puts on its own signals. They are spelled once here
// so a dashboard written against one binary's output reads the other's.
const (
	AttrDeployment        = "ork.deployment"
	AttrBuildID           = "ork.build_id"
	AttrTemporalNamespace = "ork.temporal_namespace"
	AttrPool              = "ork.pool"
	AttrOrkerID           = "ork.orker_id"
	AttrRunID             = "ork.run_id"
	AttrTaskID            = "ork.task_id"
	AttrAttempt           = "ork.attempt"
	// AttrItem is a fan-out item's 0-based index. Set on task-log records and
	// attempt spans only for an item; never a metric label — a series per item
	// is the unbounded cardinality Cardinality exists to prevent.
	AttrItem           = "ork.item"
	AttrNamespace      = "ork.namespace"
	AttrOrkflowName    = "ork.orkflow.name"
	AttrOrkflowVersion = "ork.orkflow.version"
	AttrOrkflowDigest  = "ork.orkflow.digest"
	AttrStream         = "ork.stream"
)

// Providers is what Setup hands back: one provider per signal, the Prometheus
// registry when /metrics is on, and the readiness flag the admin listener
// reports. Every field is always non-nil — a disabled signal gets the SDK's
// no-op provider — so a caller never branches on configuration to get a
// tracer, a meter or a logger.
type Providers struct {
	Tracer trace.TracerProvider
	Meter  metric.MeterProvider
	Logs   log.LoggerProvider
	// Registry is the Prometheus registry /metrics renders, or nil when
	// Prometheus was not asked for.
	Registry *prometheus.Registry
	// Ready is flipped by the binary once it serves; /readyz reads it.
	Ready *Readiness
	// Propagator is the text-map propagator for cross-process context. It is
	// the SDK default (W3C tracecontext + baggage) and is exposed so the
	// Orker can inject the same shape it received.
	Propagator propagation.TextMapPropagator

	cfg       Config
	shutdowns []func(context.Context) error
	once      sync.Once
	shutdown  error
}

// Readiness is the /readyz answer. It starts false and a binary sets it once
// its workers poll and its server listens.
type Readiness struct {
	ready atomic.Bool
}

// Set records whether the process is ready.
func (r *Readiness) Set(ready bool) {
	if r != nil {
		r.ready.Store(ready)
	}
}

// Ready reports the current answer.
func (r *Readiness) Ready() bool {
	return r != nil && r.ready.Load()
}

// Setup builds the providers cfg asks for. extra are resource attributes the
// binary knows and the environment does not: its version, deployment identity
// and pool. They are placed UNDER the standard environment, so an operator's
// OTEL_RESOURCE_ATTRIBUTES wins a collision.
//
// With nothing enabled it returns no-op providers and touches no global: no
// exporter is built, no goroutine started, no propagator installed. That is
// the zero-config promise, and setupZeroConfigTest holds it.
//
// An exporter that cannot be CONSTRUCTED (a malformed endpoint, an unreadable
// certificate) is returned as an error, because that is configuration. An
// exporter that cannot reach its collector is not: the SDK retries and
// reports through the error handler, and the process runs.
//
// If cfg.Sinks defines a sink named DefaultSink, Setup builds its default
// destination from THAT spec — endpoint, headers and all — instead of from
// the OTEL_* environment. A caller that wants the sink table's other entries
// too, not just the default, uses SetupSinks.
func Setup(ctx context.Context, cfg Config, extra ...attribute.KeyValue) (*Providers, error) {
	if !cfg.TracesEnabled() && !cfg.MetricsEnabled() && !cfg.LogsEnabled() {
		return noopProviders(cfg), nil
	}

	res, err := newResource(ctx, cfg, extra)
	if err != nil {
		return nil, err
	}

	var spec *SinkSpec
	if s, ok := cfg.Sinks.sinks[DefaultSink]; ok {
		spec = &s
	}
	p, err := buildProviders(ctx, cfg, spec, res)
	if err != nil {
		return nil, err
	}
	recordBuildInfo(p.Meter, extra)
	return p, nil
}

// noopProviders is the zero-config answer: every provider is the SDK's own
// no-op implementation, nothing is registered with any global, and no
// goroutine or listener exists.
func noopProviders(cfg Config) *Providers {
	return &Providers{
		Tracer:     tracenoop.NewTracerProvider(),
		Meter:      metricnoop.NewMeterProvider(),
		Logs:       lognoop.NewLoggerProvider(),
		Ready:      &Readiness{},
		Propagator: propagation.NewCompositeTextMapPropagator(propagation.TraceContext{}, propagation.Baggage{}),
		cfg:        cfg,
	}
}

// otlpMetricsPath, otlpTracesPath and otlpLogsPath are the standard OTLP
// http/protobuf paths appended to a sink's generic Endpoint when it names no
// more specific *_endpoint for that signal.
const (
	otlpMetricsPath = "/v1/metrics"
	otlpTracesPath  = "/v1/traces"
	otlpLogsPath    = "/v1/logs"
)

// buildProviders builds one Providers over a shared resource. spec is nil for
// the historical, environment-driven path (exporters read their endpoint,
// headers and TLS settings from OTEL_EXPORTER_OTLP_* the SDK's own
// autoconfiguration understands); a non-nil spec instead names an explicit
// destination — a named sink from ORK_TELEMETRY_SINKS — and every exporter is
// pointed at it instead.
//
// Which signals are built is cfg's decision (TracesEnabled, MetricsEnabled,
// LogsEnabled, OTLPMetricsEnabled) — but for a spec-bound sink, cfg is first
// replaced by cfg.boundTo(spec): a named sink's own destinations decide its
// per-signal enablement, never the global OTEL_*_EXPORTER flags, which govern
// only the environment-backed default (spec == nil). Prometheus is the one
// exception — it is a local scrape registry, not a destination a sink names,
// so it is unaffected by spec (and by boundTo) and is built whenever
// cfg.PrometheusEnabled(); SetupSinks keeps it out of every named sink's cfg
// so only the default Providers ever holds a registry.
//
// A spec with no endpoint at all — none of Endpoint, LogsEndpoint,
// TracesEndpoint, MetricsEndpoint set — is a useless entry and a boot error;
// the caller (Setup or SetupSinks) is left to name the sink. A spec missing
// only SOME signals' endpoints is not an error: that signal is simply off for
// this sink (signalOptions).
func buildProviders(ctx context.Context, cfg Config, spec *SinkSpec, res *resource.Resource) (*Providers, error) {
	if spec != nil {
		if !spec.hasAnyEndpoint() {
			return nil, errors.New("no endpoint set (endpoint, logs_endpoint, traces_endpoint or metrics_endpoint)")
		}
		cfg = cfg.boundTo(spec)
	}
	p := noopProviders(cfg)

	if cfg.MetricsEnabled() {
		var readers []sdkmetric.Option
		if cfg.PrometheusEnabled() {
			registry := prometheus.NewRegistry()
			registry.MustRegister(
				collectors.NewGoCollector(),
				collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
			)
			exporter, err := otelprom.New(otelprom.WithRegisterer(registry))
			if err != nil {
				return nil, fmt.Errorf("prometheus exporter: %w", err)
			}
			p.Registry = registry
			readers = append(readers, sdkmetric.WithReader(exporter))
		}
		if cfg.OTLPMetricsEnabled() {
			opts, ok := signalOptions[otlpmetrichttp.Option](spec, "metrics", otlpMetricsPath,
				otlpmetrichttp.WithEndpointURL, otlpmetrichttp.WithHeaders, otlpmetrichttp.WithInsecure, otlpmetrichttp.WithTimeout)
			if ok {
				exporter, err := otlpmetrichttp.New(ctx, opts...)
				if err != nil {
					return nil, fmt.Errorf("otlp metrics exporter: %w", err)
				}
				readers = append(readers, sdkmetric.WithReader(sdkmetric.NewPeriodicReader(exporter)))
			}
		}
		provider := sdkmetric.NewMeterProvider(append(readers, sdkmetric.WithResource(res))...)
		p.Meter = provider
		p.shutdowns = append(p.shutdowns, provider.Shutdown)
	}

	if cfg.TracesEnabled() {
		opts, _ := signalOptions[otlptracehttp.Option](spec, "traces", otlpTracesPath,
			otlptracehttp.WithEndpointURL, otlptracehttp.WithHeaders, otlptracehttp.WithInsecure, otlptracehttp.WithTimeout)
		exporter, err := otlptracehttp.New(ctx, opts...)
		if err != nil {
			return nil, fmt.Errorf("otlp trace exporter: %w", err)
		}
		provider := sdktrace.NewTracerProvider(
			sdktrace.WithBatcher(exporter),
			sdktrace.WithResource(res),
		)
		p.Tracer = provider
		p.shutdowns = append(p.shutdowns, provider.Shutdown)
	}

	if cfg.LogsEnabled() {
		opts, _ := signalOptions[otlploghttp.Option](spec, "logs", otlpLogsPath,
			otlploghttp.WithEndpointURL, otlploghttp.WithHeaders, otlploghttp.WithInsecure, otlploghttp.WithTimeout)
		exporter, err := otlploghttp.New(ctx, opts...)
		if err != nil {
			return nil, fmt.Errorf("otlp log exporter: %w", err)
		}
		// The exports counter is built from p.Meter, which the metrics block
		// above has already set (to the SDK provider when metrics are on, or
		// the no-op provider otherwise) — so this works whether or not
		// metrics were separately requested.
		exportCounter, err := p.Meter.Meter(ScopeName).Int64Counter("ork.orker.task_log.exports",
			metric.WithDescription("Task log export batches, by outcome."),
			metric.WithUnit("{batch}"))
		if err != nil {
			return nil, fmt.Errorf("telemetry instruments: %w", err)
		}
		counted := &countingLogExporter{Exporter: exporter, exports: exportCounter}
		provider := sdklog.NewLoggerProvider(
			sdklog.WithProcessor(sdklog.NewBatchProcessor(counted)),
			sdklog.WithResource(res),
		)
		p.Logs = provider
		p.shutdowns = append(p.shutdowns, provider.Shutdown)
	}

	return p, nil
}

// signalOptions builds the sink-specific exporter options for one signal.
// spec == nil means "use the environment": no options, exporter construction
// falls through to the SDK's own OTEL_EXPORTER_OTLP_* autoconfiguration, and
// ok is true (the signal is always built in that mode).
//
// spec != nil means a spec-bound sink: ok reports whether this signal has an
// endpoint to build against. A missing endpoint is never an error here, for
// any signal — buildProviders only calls this once cfg.boundTo(spec) has
// already decided the signal is on, which for a spec-bound sink means spec
// DOES have an endpoint for it, so ok=false is in practice unreachable from
// there; it stays a plain bool, not an error, so a future caller that asks
// before deciding gets silence instead of a boot failure (the actual boot
// error — a spec with no endpoint at all — is buildProviders' hasAnyEndpoint
// check, once per sink rather than once per signal).
func signalOptions[Option any](
	spec *SinkSpec, signal, defaultPath string,
	withEndpointURL func(string) Option,
	withHeaders func(map[string]string) Option,
	withInsecure func() Option,
	withTimeout func(time.Duration) Option,
) (opts []Option, ok bool) {
	if spec == nil {
		return nil, true
	}
	endpoint, err := spec.endpointFor(signal, defaultPath)
	if err != nil {
		return nil, false
	}
	opts = append(opts, withEndpointURL(endpoint))
	if headers := mergedHeaders(spec, signal, os.LookupEnv); len(headers) > 0 {
		opts = append(opts, withHeaders(headers))
	}
	if insecureFor(endpoint, spec.Insecure) {
		opts = append(opts, withInsecure())
	}
	if spec.Timeout > 0 {
		opts = append(opts, withTimeout(spec.Timeout))
	}
	return opts, true
}

// newResource describes the process. OrkStack's own attributes go first so
// that WithFromEnv, merged after them, lets OTEL_RESOURCE_ATTRIBUTES and
// OTEL_SERVICE_NAME override — the operator's word over the binary's.
func newResource(ctx context.Context, cfg Config, extra []attribute.KeyValue) (*resource.Resource, error) {
	attrs := append([]attribute.KeyValue{attribute.String("service.name", cfg.ServiceName)}, extra...)
	res, err := resource.New(ctx,
		resource.WithAttributes(attrs...),
		resource.WithTelemetrySDK(),
		resource.WithHost(),
		resource.WithFromEnv(),
	)
	if err != nil && !errors.Is(err, resource.ErrPartialResource) {
		return nil, fmt.Errorf("telemetry resource: %w", err)
	}
	return res, nil
}

// recordBuildInfo publishes ork_build_info = 1 carrying the binary's identity,
// the metric a dashboard joins on to label everything else with a version.
func recordBuildInfo(provider metric.MeterProvider, attrs []attribute.KeyValue) {
	meter := provider.Meter(ScopeName)
	gauge, err := meter.Int64Gauge("ork.build.info",
		metric.WithDescription("Constant 1, labelled with the build identity of this process."))
	if err != nil {
		return
	}
	gauge.Record(context.Background(), 1, metric.WithAttributes(attrs...))
}

// Config reports what this Providers was built from.
func (p *Providers) Config() Config { return p.cfg }

// Cardinality reports the label policy this Providers' own instruments were
// built with (Config.HighCardinality). Phase 3's per-sink routers
// (EngineObserver, OrkerObserver, TaskLogExporter) read this off whichever
// Providers a run or task was pinned to, rather than assuming every sink
// shares the process-wide default — a nil Providers behaves like disabled
// telemetry and reports low cardinality.
func (p *Providers) Cardinality() Cardinality {
	if p == nil {
		return Cardinality{}
	}
	return Cardinality{High: p.cfg.HighCardinality}
}

// Enabled reports whether any signal leaves the process.
func (p *Providers) Enabled() bool {
	return p != nil && (p.cfg.TracesEnabled() || p.cfg.MetricsEnabled() || p.cfg.LogsEnabled())
}

// Shutdown flushes and stops every provider, once. ctx bounds the final
// export so a dead collector cannot hold the process open; the caller passes
// the same shutdown budget it gives its servers.
func (p *Providers) Shutdown(ctx context.Context) error {
	if p == nil {
		return nil
	}
	p.once.Do(func() {
		var errs []error
		for i := len(p.shutdowns) - 1; i >= 0; i-- {
			if err := p.shutdowns[i](ctx); err != nil {
				errs = append(errs, err)
			}
		}
		p.shutdown = errors.Join(errs...)
	})
	return p.shutdown
}

// ShutdownTimeout is the longest Shutdown waits when a caller has no budget
// of its own.
const ShutdownTimeout = 10 * time.Second

// SinkUnknownMetric names the counter For increments when a caller names a
// sink ORK_TELEMETRY_SINKS does not declare.
const SinkUnknownMetric = "ork.telemetry.sink_unknown"

// Sinks is every named sink from ORK_TELEMETRY_SINKS, built once at boot,
// plus the default — the destination a caller gets by asking for "" or an
// unrecognized name. The default is either the OTEL_* environment or, when
// the table defines DefaultSink, that sink's own spec (Setup's rule, applied
// here too).
//
// A named sink shares the default's resource (one process identity across
// every destination) but never opens a listener: /metrics is the deployment's
// one Prometheus scrape endpoint and belongs to the default sink alone; a
// named sink gets OTLP metrics only, and only when it names a metrics
// destination — a sink that names none simply carries no metrics, and neither
// that nor a missing traces or logs endpoint is a boot error any more
// (buildProviders' cfg.boundTo): the only boot error left is a sink with no
// endpoint at all (buildProviders' hasAnyEndpoint check).
type Sinks struct {
	byName  map[string]*Providers
	def     *Providers
	unknown metric.Int64Counter
	logger  *slog.Logger
	// warned dedupes the "sink not declared" Warn For logs: at most one per
	// unknown name, however many times a caller asks for it over the
	// process's life.
	warned sync.Map
}

// SetupSinks builds the default Providers exactly as Setup would, then one
// more Providers per name in cfg.Sinks, all sharing one resource.Resource.
//
// Every named sink is built whenever cfg.Sinks.Len() > 0, regardless of the
// global OTEL_*_EXPORTER flags: naming a sink IS the decision to build it, so
// a deployment with no OTEL_* variable set at all can still have a
// ORK_TELEMETRY_SINKS entry come up carrying whichever signals its own
// endpoints name (cfg.boundTo, applied inside buildProviders). The default
// Providers is the one exception — it stays the environment-backed path,
// governed by cfg's own flags, unless cfg.Sinks itself declares a "default"
// entry (Setup's rule, applied here too).
//
// With nothing enabled and cfg.Sinks empty, this is Setup wearing the Sinks
// shape: Default() is the no-op Providers and For(anything) returns it.
func SetupSinks(ctx context.Context, cfg Config, extra ...attribute.KeyValue) (*Sinks, error) {
	enabled := cfg.TracesEnabled() || cfg.MetricsEnabled() || cfg.LogsEnabled()
	hasSinks := cfg.Sinks.Len() > 0

	if !enabled && !hasSinks {
		return newDisabledSinks(cfg)
	}

	res, err := newResource(ctx, cfg, extra)
	if err != nil {
		return nil, err
	}

	var def *Providers
	if enabled || cfg.Sinks.Has(DefaultSink) {
		var defaultSpec *SinkSpec
		if spec, ok := cfg.Sinks.sinks[DefaultSink]; ok {
			defaultSpec = &spec
		}
		def, err = buildProviders(ctx, cfg, defaultSpec, res)
		if err != nil {
			return nil, fmt.Errorf("sink %q: %w", DefaultSink, err)
		}
		recordBuildInfo(def.Meter, extra)
	} else {
		def = noopProviders(cfg)
	}

	s, err := newSinks(def)
	if err != nil {
		return nil, err
	}

	for _, name := range cfg.Sinks.Names() {
		if name == DefaultSink {
			continue
		}
		spec := cfg.Sinks.sinks[name]
		// namedCfg strips Prometheus and the admin listener: see the type
		// doc. Every other signal's enablement is decided by buildProviders'
		// cfg.boundTo(spec), from the spec's own endpoints, not from cfg.
		namedCfg := cfg
		namedCfg.MetricsExporters = withoutExporter(cfg.MetricsExporters, ExporterPrometheus)
		namedCfg.AdminAddr = ""
		p, err := buildProviders(ctx, namedCfg, &spec, res)
		if err != nil {
			return nil, fmt.Errorf("sink %q: %w", name, err)
		}
		s.byName[name] = p
	}
	return s, nil
}

// newDisabledSinks is SetupSinks' zero-config answer: nothing is globally
// enabled and ORK_TELEMETRY_SINKS names no sink at all, so every destination
// — default and every name a caller could ask for — is the same no-op
// Providers, and nothing here touches a global or starts a goroutine.
func newDisabledSinks(cfg Config) (*Sinks, error) {
	return newSinks(noopProviders(cfg))
}

// newSinks builds the empty Sinks shell around def: no named sinks yet (the
// caller fills s.byName itself when it has any to build), just the
// SinkUnknownMetric counter every For() on an unrecognized name increments.
func newSinks(def *Providers) (*Sinks, error) {
	unknownCounter, err := def.Meter.Meter(ScopeName).Int64Counter(SinkUnknownMetric,
		metric.WithDescription("A caller named a sink ORK_TELEMETRY_SINKS does not declare; the default sink was used instead."),
		metric.WithUnit("{lookup}"))
	if err != nil {
		return nil, fmt.Errorf("telemetry instruments: %w", err)
	}
	return &Sinks{byName: map[string]*Providers{}, def: def, unknown: unknownCounter}, nil
}

// withoutExporter returns names with every occurrence of candidate removed,
// preserving order.
func withoutExporter(names []string, candidate string) []string {
	if len(names) == 0 {
		return names
	}
	out := make([]string, 0, len(names))
	for _, name := range names {
		if name != candidate {
			out = append(out, name)
		}
	}
	return out
}

// sinksOf wraps one Providers as a Sinks whose only member is the default —
// the shape every phase-1 constructor (NewTaskLogExporter, NewEngineObserver,
// NewOrkerObserver) is defined in terms of, so a caller that still builds one
// Providers and hands it to them keeps compiling and behaving exactly as
// before: every sink name, known or not, resolves to p.
func sinksOf(p *Providers) *Sinks {
	return &Sinks{byName: map[string]*Providers{}, def: p}
}

// newSinksForTest builds a Sinks by hand from Providers a test already
// constructed, without going through SetupSinks' environment and
// ORK_TELEMETRY_SINKS parsing. named may be nil. Exported to the package's
// own tests only (unexported, lowercase) — production code always gets a
// Sinks from SetupSinks.
func newSinksForTest(def *Providers, named map[string]*Providers) *Sinks {
	byName := make(map[string]*Providers, len(named))
	for name, p := range named {
		byName[name] = p
	}
	return &Sinks{byName: byName, def: def}
}

// Default is the destination a namespace gets when it names no sink.
func (s *Sinks) Default() *Providers {
	if s == nil {
		return nil
	}
	return s.def
}

// WithLogger attaches logger to s: For then logs one Warn, at most once per
// unknown name, when a caller asks for a sink ORK_TELEMETRY_SINKS never
// declared. Returns s so a caller can chain it onto SetupSinks' result.
// Without a logger (the zero value, or a Sinks never given one), For's only
// signal for that misconfiguration is SinkUnknownMetric.
func (s *Sinks) WithLogger(logger *slog.Logger) *Sinks {
	if s == nil {
		return s
	}
	s.logger = logger
	return s
}

// Has reports whether name is a sink this Sinks actually declares — "" and
// DefaultSink always report true (Default() answers them), an undeclared
// name reports false even though For(name) still falls back to Default()
// rather than failing. A caller that wants to cross-check a stored policy
// against what booted (a namespace's chosen sink, say) uses this instead of
// comparing For's result by identity.
func (s *Sinks) Has(name string) bool {
	if s == nil {
		return false
	}
	if name == "" || name == DefaultSink {
		return true
	}
	_, ok := s.byName[name]
	return ok
}

// For is the destination name asked for: "" or DefaultSink is Default();
// an unrecognized name is also Default(), after incrementing
// SinkUnknownMetric on the default sink's meter with the requested name as
// the "sink" attribute, and — when WithLogger attached one — logging one Warn
// the first time this particular unknown name is seen. A namespace
// misconfiguration should be visible, not a silent fallback, but a namespace
// that keeps naming the same stale sink should not flood the log once per
// task.
func (s *Sinks) For(name string) *Providers {
	if s == nil {
		return nil
	}
	if name == "" || name == DefaultSink {
		return s.def
	}
	if p, ok := s.byName[name]; ok {
		return p
	}
	if s.unknown != nil {
		s.unknown.Add(context.Background(), 1, metric.WithAttributes(attribute.String("sink", name)))
	}
	if s.logger != nil {
		if _, alreadyWarned := s.warned.LoadOrStore(name, struct{}{}); !alreadyWarned {
			s.logger.Warn("telemetry sink not declared; using default", "sink", name)
		}
	}
	return s.def
}

// Names lists every sink this Sinks can serve, sorted, DefaultSink included.
func (s *Sinks) Names() []string {
	if s == nil {
		return nil
	}
	names := make([]string, 0, len(s.byName)+1)
	names = append(names, DefaultSink)
	for name := range s.byName {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

// SinkStatus is one sink's built signal state — what Sinks.Describe reports
// for a caller to log once at boot (boot.go's LogBoot).
type SinkStatus struct {
	Name    string
	Logs    bool
	Traces  bool
	Metrics bool
}

// Describe reports every sink's per-signal state, sorted by name, DefaultSink
// first. SetupSinks itself never logs — it has no logger of its own to log
// through — so this is the shape a caller (LogBoot) turns into one Info line
// per sink at boot, "telemetry sink ready" with logs/traces/metrics booleans.
func (s *Sinks) Describe() []SinkStatus {
	if s == nil {
		return nil
	}
	statuses := make([]SinkStatus, 0, len(s.byName)+1)
	statuses = append(statuses, describeSink(DefaultSink, s.def))
	names := make([]string, 0, len(s.byName))
	for name := range s.byName {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		statuses = append(statuses, describeSink(name, s.byName[name]))
	}
	return statuses
}

func describeSink(name string, p *Providers) SinkStatus {
	cfg := p.Config()
	return SinkStatus{Name: name, Logs: cfg.LogsEnabled(), Traces: cfg.TracesEnabled(), Metrics: cfg.MetricsEnabled()}
}

// Shutdown flushes and stops every sink's providers, default included,
// CONCURRENTLY — a slow or unreachable collector on one sink must not delay
// another's shutdown, and every Providers.Shutdown already bounds itself to
// ctx and is idempotent, so there is nothing here to serialize on. Errors
// from every sink are joined into one.
func (s *Sinks) Shutdown(ctx context.Context) error {
	if s == nil {
		return nil
	}
	all := make([]*Providers, 0, len(s.byName)+1)
	all = append(all, s.def)
	for _, p := range s.byName {
		all = append(all, p)
	}
	errs := make([]error, len(all))
	var wg sync.WaitGroup
	wg.Add(len(all))
	for i, p := range all {
		go func(i int, p *Providers) {
			defer wg.Done()
			errs[i] = p.Shutdown(ctx)
		}(i, p)
	}
	wg.Wait()
	return errors.Join(errs...)
}
