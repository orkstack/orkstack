package telemetry

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"slices"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	otelruntime "go.opentelemetry.io/contrib/instrumentation/runtime"
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
	// AttrCacheOutcome is a cached task's memo decision: hit, miss or
	// bypassed (docs/DSL.md "Caching"). A bounded, three-valued label, set
	// ONLY for a cached task, so a deployment with none keeps exactly its
	// series. The memo key is never an attribute (unbounded).
	AttrCacheOutcome = "ork.cache.outcome"
	// AttrCacheMissReason is a miss's token (MEMO_ABSENT, …), on the Orker's
	// attempt span only.
	AttrCacheMissReason = "ork.cache.miss_reason"
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

// Setup builds the providers cfg asks for, for one destination. The binaries
// boot through SetupSinks (Boot), which also installs the export-error
// handler and warns about identity overrides; Setup does neither, so a
// caller of it relies on the SDK's own handler.
//
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

	res, _, err := newResource(ctx, cfg, extra)
	if err != nil {
		return nil, err
	}

	var spec *SinkSpec
	if s, ok := cfg.Sinks.sinks[DefaultSink]; ok {
		spec = &s
	}
	p, err := buildProviders(ctx, cfg, DefaultSink, spec, res)
	if err != nil {
		return nil, err
	}
	if err := processInstruments(p.cfg, p, extra); err != nil {
		shutdownAfterFailedBoot(p.Shutdown)
		return nil, err
	}
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
func buildProviders(ctx context.Context, cfg Config, name string, spec *SinkSpec, res *resource.Resource) (*Providers, error) {
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
			// The Go runtime's own metrics come from the OpenTelemetry
			// runtime instrumentation on every exporter (processInstruments);
			// the process collector stays, for /metrics only: it has no
			// OpenTelemetry counterpart.
			registry.MustRegister(collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))
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
				readers = append(readers, sdkmetric.WithReader(sdkmetric.NewPeriodicReader(signalMetricExporter{Exporter: exporter, sink: name})))
			}
		}
		provider := sdkmetric.NewMeterProvider(append(append(readers, sdkmetric.WithResource(res)), meterLimitOptions(cfg)...)...)
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
			sdktrace.WithBatcher(signalSpanExporter{SpanExporter: exporter, sink: name}),
			sdktrace.WithResource(res),
			sdktrace.WithIDGenerator(newPinnedIDs()),
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
		counted := &countingLogExporter{Exporter: exporter, exports: exportCounter, sink: name}
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

// newResource describes the process. OrkStack's own attributes go first, so
// that WithFromEnv, merged after them, lets OTEL_RESOURCE_ATTRIBUTES and
// OTEL_SERVICE_NAME override them — the operator's word over the binary's —
// except the process identity (identity.go), merged last, which no
// environment value overrides. conflicts names each identity key the
// environment tried to set to another value, for the caller to warn about.
func newResource(ctx context.Context, cfg Config, extra []attribute.KeyValue) (res *resource.Resource, conflicts []string, err error) {
	identity, rest := identityAttrs(cfg, extra)
	attrs := append([]attribute.KeyValue{attribute.String("service.name", cfg.ServiceName)}, rest...)
	base, err := resource.New(ctx,
		resource.WithAttributes(attrs...),
		resource.WithTelemetrySDK(),
		resource.WithHost(),
		resource.WithFromEnv(),
	)
	if err != nil && !errors.Is(err, resource.ErrPartialResource) {
		return nil, nil, fmt.Errorf("telemetry resource: %w", err)
	}
	// The environment's value for an identity key is dropped whether or not
	// the binary sets that key: an engine has no Orker id, and none from the
	// environment may make it read as an Orker.
	ours := attribute.NewSet(identity...)
	kept := make([]attribute.KeyValue, 0, base.Len())
	for _, kv := range base.Attributes() {
		if !isIdentityKey(kv.Key) {
			kept = append(kept, kv)
			continue
		}
		if v, ok := ours.Value(kv.Key); !ok || v.String() != kv.Value.String() {
			conflicts = append(conflicts, string(kv.Key))
		}
	}
	slices.SortFunc(conflicts, func(a, b string) int {
		return slices.Index(identityKeys, attribute.Key(a)) - slices.Index(identityKeys, attribute.Key(b))
	})
	base = resource.NewWithAttributes(base.SchemaURL(), kept...)
	res, err = resource.Merge(base, resource.NewSchemaless(identity...))
	if err != nil {
		return nil, nil, fmt.Errorf("telemetry resource: %w", err)
	}
	return res, conflicts, nil
}

// processInstruments registers what belongs to the process rather than to a
// sink, on the default destination only: ork.build.info and the Go runtime's
// metrics. A named sink never carries them.
func processInstruments(cfg Config, def *Providers, buildAttrs []attribute.KeyValue) error {
	if err := registerBuildInfo(def.Meter, buildAttrs); err != nil {
		return err
	}
	if !cfg.MetricsEnabled() {
		return nil
	}
	if err := otelruntime.Start(otelruntime.WithMeterProvider(def.Meter)); err != nil {
		return fmt.Errorf("telemetry runtime metrics: %w", err)
	}
	return nil
}

// Config reports what this Providers was built from.
func (p *Providers) Config() Config { return p.cfg }

// Cardinality reports the label policy this Providers' own instruments were
// built with (Config.HighCardinality). The per-sink routers
// (EngineObserver, OrkerObserver, TaskLogExporter) read this off whichever
// Providers a run or task was pinned to, rather than assuming every sink
// shares the process-wide default — a nil Providers behaves like disabled
// telemetry and reports low cardinality.
func (p *Providers) Cardinality() Cardinality {
	if p == nil {
		return Cardinality{}
	}
	return Cardinality{High: p.cfg.HighCardinality, OrkflowLabel: p.cfg.OrkflowLabel}
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
	byName map[string]*Providers
	def    *Providers
	logger *slog.Logger
	// warned dedupes the "sink not declared" Warn ForNamespaced logs: at most one per
	// unknown name, however many times a caller asks for it over the
	// process's life.
	warned sync.Map
	// identityConflicts are the identity keys the environment tried to
	// override (newResource); WithLogger warns about them once.
	identityConflicts []string
	identityWarned    sync.Once
	// exportErrors is the process's otel.ErrorHandler when a signal is on.
	exportErrors *exportErrorHandler
	// anyName is a Sinks with no table (sinksOf): every name is its one
	// destination, because there is no other to choose.
	anyName bool
	// dropWarned dedupes DropRecords' Warn: one per signal and reason.
	dropWarned sync.Map
	// dropped counts records_dropped on the default sink, built at first use.
	dropped     metric.Int64Counter
	droppedOnce sync.Once
}

// RecordsDroppedMetric counts records that named a destination this process
// cannot send them to, by signal and reason, on the default sink. A record
// carrying a namespace leaves only on that namespace's own sink, never on
// another by fallback (OPEN-22), so the default sink carries only this count.
const RecordsDroppedMetric = "ork.telemetry.records_dropped"

// AttrDropReason is why a record was dropped.
const AttrDropReason = "reason"

// DropReasonSinkUnknown is a record whose run selected a sink this process
// does not declare (ORK_TELEMETRY_SINKS differs between processes, or the
// namespace names one no process declares).
const DropReasonSinkUnknown = "sink_unknown"

// DropReasonInstrumentsFailed is a record whose sink's instruments could
// not be built, which the process logs once per sink.
const DropReasonInstrumentsFailed = "instruments_failed"

// DropRecords counts n records of signal dropped for reason, on the default
// sink: a task log line, one attempt's span, one attempt's metrics, or one run
// event's or refusal's metrics is one record. It never blocks and never fails:
// telemetry is best-effort. The count reaches an operator only when the
// default sink exports metrics; ForNamespaced's Warn is the signal otherwise.
func (s *Sinks) DropRecords(ctx context.Context, signal, reason string, n int64) {
	if s == nil || n <= 0 {
		return
	}
	if counter := s.droppedCounter(); counter != nil {
		counter.Add(ctx, n, metric.WithAttributes(
			attribute.String(AttrSignal, signal), attribute.String(AttrDropReason, reason)))
	}
	// With the default sink exporting no metrics the count reaches no one, so
	// the first drop of each signal and reason is logged instead.
	if s.logger != nil && (s.def == nil || !s.def.cfg.MetricsEnabled()) {
		if _, warned := s.dropWarned.LoadOrStore(signal+"/"+reason, struct{}{}); !warned {
			s.logger.Warn("telemetry records dropped; the default sink exports no metrics to count them",
				"signal", signal, "reason", reason)
		}
	}
}

// droppedCounter builds RecordsDroppedMetric once, on the default sink. A
// counter that cannot be built is logged once and drops are then uncounted.
func (s *Sinks) droppedCounter() metric.Int64Counter {
	if s.def == nil || s.def.Meter == nil {
		return nil
	}
	s.droppedOnce.Do(func() {
		counter, err := s.def.Meter.Meter(ScopeName).Int64Counter(RecordsDroppedMetric,
			metric.WithDescription("Records dropped because the destination they named is not one this process can send to, by signal and reason: a task log line, an attempt's span or metrics, a run event's metrics."),
			metric.WithUnit("{record}"))
		if err != nil {
			if s.logger != nil {
				s.logger.Warn("telemetry: cannot count dropped records", "error", err)
			}
			return
		}
		s.dropped = counter
	})
	return s.dropped
}

// ForNamespaced is the destination a namespaced record goes to: "" and
// DefaultSink are the default, a declared sink is itself, and a name this
// process does not declare is refused (nil, false) — the caller drops the
// record and counts it with DropRecords, and never sends it to the default
// sink. The first refusal of each name logs one Warn once WithLogger has
// attached a logger.
func (s *Sinks) ForNamespaced(name string) (*Providers, bool) {
	if s == nil {
		return nil, false
	}
	if name == "" || name == DefaultSink || s.anyName {
		return s.def, true
	}
	if p, ok := s.byName[name]; ok {
		return p, true
	}
	if s.logger != nil {
		if _, alreadyWarned := s.warned.LoadOrStore(name, struct{}{}); !alreadyWarned {
			s.logger.Warn("telemetry sink not declared; records that name it are dropped", "sink", name)
		}
	}
	return nil, false
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
// shape: Default() is the no-op Providers and ForNamespaced refuses every
// name but the default.
func SetupSinks(ctx context.Context, cfg Config, extra ...attribute.KeyValue) (*Sinks, error) {
	enabled := cfg.TracesEnabled() || cfg.MetricsEnabled() || cfg.LogsEnabled()
	hasSinks := cfg.Sinks.Len() > 0

	if !enabled && !hasSinks {
		return newDisabledSinks(cfg)
	}

	res, conflicts, err := newResource(ctx, cfg, extra)
	if err != nil {
		return nil, err
	}

	var def *Providers
	if enabled || cfg.Sinks.Has(DefaultSink) {
		var defaultSpec *SinkSpec
		if spec, ok := cfg.Sinks.sinks[DefaultSink]; ok {
			defaultSpec = &spec
		}
		def, err = buildProviders(ctx, cfg, DefaultSink, defaultSpec, res)
		if err != nil {
			return nil, fmt.Errorf("sink %q: %w", DefaultSink, err)
		}
		if err := processInstruments(def.cfg, def, extra); err != nil {
			shutdownAfterFailedBoot(def.Shutdown)
			return nil, err
		}
	} else {
		def = noopProviders(cfg)
	}

	s := newSinks(def)
	s.identityConflicts = conflicts
	// Export errors are counted on the default destination, whichever sink's
	// exporter failed: the operator reads them there. The handler becomes
	// the process's only once every sink has been built.
	handler, err := newExportErrorHandler(def.Meter)
	if err != nil {
		shutdownAfterFailedBoot(s.Shutdown)
		return nil, err
	}
	s.exportErrors = handler

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
		p, err := buildProviders(ctx, namedCfg, name, &spec, res)
		if err != nil {
			shutdownAfterFailedBoot(s.Shutdown)
			return nil, fmt.Errorf("sink %q: %w", name, err)
		}
		s.byName[name] = p
	}
	installErrorHandler(handler)
	return s, nil
}

// meterLimitOptions is cfg's cardinality limit as meter-provider options:
// none when it is unset, leaving the SDK's own default.
func meterLimitOptions(cfg Config) []sdkmetric.Option {
	if cfg.MetricCardinalityLimit <= 0 {
		return nil
	}
	return []sdkmetric.Option{sdkmetric.WithCardinalityLimit(cfg.MetricCardinalityLimit)}
}

// shutdownAfterFailedBoot stops what a boot that is about to fail had already
// started (batchers, readers), within a bound, so a caller that does not exit
// keeps no exporter running. Its error is the dead boot's, not the caller's.
func shutdownAfterFailedBoot(shutdown func(context.Context) error) {
	ctx, cancel := context.WithTimeout(context.Background(), failedBootShutdownBound)
	defer cancel()
	_ = shutdown(ctx) // the boot error is what the caller reports
}

// failedBootShutdownBound bounds shutdownAfterFailedBoot.
const failedBootShutdownBound = 5 * time.Second

// newDisabledSinks is SetupSinks' zero-config answer: nothing is globally
// enabled and ORK_TELEMETRY_SINKS names no sink at all, so every destination
// — default and every name a caller could ask for — is the same no-op
// Providers, and nothing here touches a global or starts a goroutine.
func newDisabledSinks(cfg Config) (*Sinks, error) {
	return newSinks(noopProviders(cfg)), nil
}

// newSinks builds the empty Sinks shell around def: no named sinks yet (the
// caller fills s.byName itself when it has any to build).
func newSinks(def *Providers) *Sinks {
	return &Sinks{byName: map[string]*Providers{}, def: def}
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
// the shape every single-sink constructor (NewTaskLogExporter, NewEngineObserver,
// NewOrkerObserver) is defined in terms of, so a caller that still builds one
// Providers and hands it to them keeps compiling and behaving exactly as
// before: every sink name, known or not, resolves to p.
func sinksOf(p *Providers) *Sinks {
	return &Sinks{byName: map[string]*Providers{}, def: p, anyName: true}
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

// WithLogger attaches logger to s: ForNamespaced then logs one Warn, at most
// once per unknown name, when a caller asks for a sink ORK_TELEMETRY_SINKS
// never declared; the export-error handler logs through it; and an identity
// override is warned about, once. Returns s so a caller can chain it onto
// SetupSinks' result. Without a logger, a record dropped for an unknown sink
// is still counted (RecordsDroppedMetric).
func (s *Sinks) WithLogger(logger *slog.Logger) *Sinks {
	if s == nil {
		return s
	}
	s.logger = logger
	if s.exportErrors != nil {
		s.exportErrors.setLogger(logger)
	}
	if len(s.identityConflicts) > 0 && logger != nil {
		s.identityWarned.Do(func() {
			logger.Warn("OTEL_RESOURCE_ATTRIBUTES cannot override the process identity; the process's own values are kept",
				"keys", s.identityConflicts)
		})
	}
	return s
}

// Has reports whether name is a sink this Sinks actually declares — "" and
// DefaultSink always report true (Default() answers them), an undeclared
// name reports false. A caller that wants to cross-check a stored policy
// against what booted (a namespace's chosen sink, say) uses this, and logs
// nothing for it, where ForNamespaced would warn.
func (s *Sinks) Has(name string) bool {
	if s == nil {
		return false
	}
	if name == "" || name == DefaultSink || s.anyName {
		return true
	}
	_, ok := s.byName[name]
	return ok
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

// AnyMetrics reports whether any sink of s, the default or a named one,
// exports metrics.
func (s *Sinks) AnyMetrics() bool {
	if s == nil {
		return false
	}
	if s.def != nil && s.def.cfg.MetricsEnabled() {
		return true
	}
	for _, p := range s.byName {
		if p != nil && p.cfg.MetricsEnabled() {
			return true
		}
	}
	return false
}
