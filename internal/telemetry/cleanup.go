package telemetry

import (
	"context"
	"errors"
	"log/slog"
	"slices"
	"sync"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"github.com/orkstack/orkstack/internal/archive"
)

// Retention cleanup's instruments (docs/DEPLOY.md, "Retention cleanup").
const (
	CleanupEnabledMetric          = "ork.cleanup.enabled"
	CleanupIntervalMetric         = "ork.cleanup.interval"
	CleanupRetentionBufferMetric  = "ork.cleanup.retention_buffer"
	CleanupRunsMetric             = "ork.cleanup.runs"
	CleanupObjectsMetric          = "ork.cleanup.objects"
	CleanupBytesMetric            = "ork.cleanup.bytes"
	CleanupPassesMetric           = "ork.cleanup.passes"
	CleanupPassDurationMetric     = "ork.cleanup.pass.duration"
	CleanupLastPassMetric         = "ork.cleanup.last_pass"
	CleanupRetentionLagMetric     = "ork.cleanup.retention_lag"
	CleanupErrorsMetric           = "ork.cleanup.errors"
	DeploymentCleanupRuns         = "ork.deployment.cleanup.runs"
	DeploymentCleanupRetentionLag = "ork.deployment.cleanup.retention_lag"
	AttrCleanupStep               = "ork.cleanup.step"
)

// CleanupSettings are retention cleanup's settings in force on this engine.
// They are exported from boot, whether or not cleanup runs, so an alert can
// tell cleanup that is off from cleanup that is silent.
type CleanupSettings struct {
	Enabled         bool
	Interval        time.Duration
	RetentionBuffer time.Duration
}

// cleanupNamespaced are one sink's namespaced cleanup instruments.
type cleanupNamespaced struct {
	runs    metric.Int64Counter
	objects metric.Int64Counter
	bytes   metric.Int64Counter
	lag     metric.Float64Gauge
}

// cleanupLagKey is one retention-lag series of the last complete walk.
type cleanupLagKey struct {
	sink      *Providers
	namespace string
}

// CleanupObserver turns retention cleanup's passes into the metrics
// docs/DEPLOY.md lists. It implements archive.CleanupObserver. Recording
// never blocks a pass and never fails it: a record that cannot be routed or
// whose labels are outside the contract is dropped, the latter logged once.
type CleanupObserver struct {
	defaults *Providers
	route    GCRouter
	logger   *slog.Logger
	now      func() time.Time

	passes   metric.Int64Counter
	duration metric.Float64Histogram
	errs     metric.Int64Counter
	twinRuns metric.Int64Counter

	namespaced providerCache[*cleanupNamespaced]
	warned     sync.Map

	mu sync.Mutex
	// walked says a walk of the whole index completed; until one has, the
	// gauges that report one report nothing, never a healthy-looking 0.
	walked   bool
	lagKnown bool
	lastWalk time.Time
	maxLag   float64
	// lagSeries holds every series the last walk set, so the next walk sets
	// one it no longer has back to zero.
	lagSeries map[cleanupLagKey]struct{}
}

// NewCleanupObserver registers cleanup's instruments on p, the default sink.
// route is where a namespace's records go, as for the GC; nil sends them all
// to p. It works, as a no-op, on nil or disabled Providers; an instrument
// that cannot be built is logged and the rest still record.
func NewCleanupObserver(p *Providers, route GCRouter, settings CleanupSettings, logger *slog.Logger) *CleanupObserver {
	if logger == nil {
		logger = slog.Default()
	}
	o := &CleanupObserver{defaults: p, route: route, logger: logger, now: time.Now}
	meter := meterFor(p)
	var errs []error
	keep := func(err error) {
		if err != nil {
			errs = append(errs, err)
		}
	}
	var err error
	o.passes, err = meter.Int64Counter(CleanupPassesMetric,
		metric.WithDescription("Retention cleanup passes, by outcome: completed, drain (stopped at a full batch, continued at once) or failed."),
		metric.WithUnit("{pass}"))
	keep(err)
	o.duration, err = meter.Float64Histogram(CleanupPassDurationMetric,
		metric.WithDescription("How long one retention cleanup pass took, by outcome."), metric.WithUnit("s"),
		metric.WithExplicitBucketBoundaries(gcPassDurations...))
	keep(err)
	o.errs, err = meter.Int64Counter(CleanupErrorsMetric,
		metric.WithDescription("Retention cleanup failures, by the step that failed."), metric.WithUnit("{error}"))
	keep(err)
	o.twinRuns, err = meter.Int64Counter(DeploymentCleanupRuns,
		metric.WithDescription("Runs retention cleanup decided on in every namespace, by outcome."), metric.WithUnit("{run}"))
	keep(err)
	keep(o.registerGauges(meter, settings))
	if err := errors.Join(errs...); err != nil {
		logger.Warn("telemetry: some retention cleanup instruments could not be built; the rest still record", "error", err)
	}
	return o
}

// registerGauges registers the settings gauges, reported from boot, and the
// walk gauges, reported once a walk completed.
func (o *CleanupObserver) registerGauges(meter metric.Meter, settings CleanupSettings) error {
	var errs []error
	_, err := meter.Int64ObservableGauge(CleanupEnabledMetric,
		metric.WithDescription("1 when retention cleanup is enabled on this engine, 0 when it is off."), metric.WithUnit("1"),
		metric.WithInt64Callback(func(_ context.Context, ob metric.Int64Observer) error {
			ob.Observe(boolGauge(settings.Enabled))
			return nil
		}))
	errs = append(errs, err)
	for _, s := range []struct {
		name, desc string
		value      time.Duration
	}{
		{CleanupIntervalMetric, "The retention cleanup interval in force on this engine.", settings.Interval},
		{CleanupRetentionBufferMetric, "The retention buffer in force on this engine: how far past the maximum run retention the cluster's own backstop sits.", settings.RetentionBuffer},
	} {
		seconds := s.value.Seconds()
		_, err := meter.Float64ObservableGauge(s.name, metric.WithDescription(s.desc), metric.WithUnit("s"),
			metric.WithFloat64Callback(func(_ context.Context, ob metric.Float64Observer) error {
				ob.Observe(seconds)
				return nil
			}))
		errs = append(errs, err)
	}
	walkGauge := func(name, desc string, read func() (float64, bool)) {
		_, err := meter.Float64ObservableGauge(name, metric.WithDescription(desc), metric.WithUnit("s"),
			metric.WithFloat64Callback(func(_ context.Context, ob metric.Float64Observer) error {
				o.mu.Lock()
				v, ok := read()
				o.mu.Unlock()
				if ok {
					ob.Observe(v)
				}
				return nil
			}))
		errs = append(errs, err)
	}
	walkGauge(CleanupLastPassMetric, "When retention cleanup last completed a walk of the whole run index.",
		func() (float64, bool) { return float64(o.lastWalk.Unix()), o.walked })
	walkGauge(DeploymentCleanupRetentionLag, "How far past its due time the most overdue run the last complete walk that could tell left unretired is, in every namespace; 0 when none is.",
		func() (float64, bool) { return o.maxLag, o.lagKnown })
	return errors.Join(errs...)
}

// ObserveCleanup records one pass. It never blocks on the network: every
// instrument records into the SDK's in-process aggregation.
func (o *CleanupObserver) ObserveCleanup(ctx context.Context, pass archive.CleanupPass) {
	if o == nil {
		return
	}
	if !slices.Contains(archive.CleanupPassOutcomes(), pass.Outcome) {
		o.warnOnce("pass:"+pass.Outcome, "a retention cleanup pass outcome outside the metric contract is not recorded", "outcome", pass.Outcome)
	} else {
		outcome := metric.WithAttributes(attribute.String(AttrOutcome, pass.Outcome))
		if o.passes != nil {
			o.passes.Add(ctx, 1, outcome)
		}
		if o.duration != nil {
			o.duration.Record(ctx, max(pass.Duration.Seconds(), 0), outcome)
		}
	}
	for step, n := range pass.StepErrors {
		if !archive.ValidCleanupStep(step) {
			o.warnOnce("step:"+step, "a retention cleanup step outside the metric contract is not recorded", "step", step)
			continue
		}
		if o.errs != nil && n > 0 {
			o.errs.Add(ctx, int64(n), metric.WithAttributes(attribute.String(AttrCleanupStep, step)))
		}
	}
	routes := map[string]*Providers{}
	for ns, tally := range pass.Namespaces {
		inst, _ := o.namespacedFor(ctx, routes, ns)
		nsAttr := attribute.String(AttrNamespace, ns)
		for outcome, n := range tally.Runs {
			if !archive.ValidCleanupRunOutcome(outcome) {
				o.warnOnce("run:"+outcome, "a retention cleanup run outcome outside the metric contract is not recorded", "outcome", outcome)
				continue
			}
			if n <= 0 {
				continue
			}
			outcomeAttr := attribute.String(AttrOutcome, outcome)
			// The twin counts every namespace, routed or dropped.
			if o.twinRuns != nil {
				o.twinRuns.Add(ctx, int64(n), metric.WithAttributes(outcomeAttr))
			}
			if inst != nil {
				inst.runs.Add(ctx, int64(n), metric.WithAttributes(nsAttr, outcomeAttr))
			}
		}
		if inst != nil && tally.Objects > 0 {
			inst.objects.Add(ctx, int64(tally.Objects), metric.WithAttributes(nsAttr))
		}
		if inst != nil && tally.Bytes > 0 {
			inst.bytes.Add(ctx, tally.Bytes, metric.WithAttributes(nsAttr))
		}
	}
	if pass.WalkComplete {
		o.recordWalk(ctx, routes, pass.RetentionLag, pass.LagUnknown)
	}
}

// recordWalk replaces the retention-lag gauges with the walk's and sets a
// series the walk no longer has back to zero. A walk whose lag is unknown —
// it could not read some run — is no walk for these gauges: last_pass does
// not advance, so it goes stale while the outage lasts, and the deployment
// lag stops reporting rather than repeat an old, healthy-looking value.
func (o *CleanupObserver) recordWalk(ctx context.Context, routes map[string]*Providers, lag map[string]time.Duration, unknown bool) {
	now := o.now()
	if unknown {
		o.mu.Lock()
		o.lagKnown = false
		o.mu.Unlock()
		return
	}
	series := make(map[cleanupLagKey]struct{}, len(lag))
	var maxLag float64
	for ns, d := range lag {
		seconds := max(d.Seconds(), 0)
		maxLag = max(maxLag, seconds)
		inst, sink := o.namespacedFor(ctx, routes, ns)
		if inst == nil {
			continue
		}
		inst.lag.Record(ctx, seconds, metric.WithAttributes(attribute.String(AttrNamespace, ns)))
		series[cleanupLagKey{sink: sink, namespace: ns}] = struct{}{}
	}
	o.mu.Lock()
	stale := o.lagSeries
	o.lagSeries = series
	o.walked, o.lagKnown, o.lastWalk, o.maxLag = true, true, now, maxLag
	o.mu.Unlock()
	for key := range stale {
		if _, kept := series[key]; kept {
			continue
		}
		if inst, err := o.instrumentsFor(key.sink); err == nil {
			inst.lag.Record(ctx, 0, metric.WithAttributes(attribute.String(AttrNamespace, key.namespace)))
		}
	}
}

// namespacedFor is the instruments of the sink a namespace's records go to,
// and that sink; (nil, nil) when the record is dropped. routes memoizes the
// router for one pass.
func (o *CleanupObserver) namespacedFor(ctx context.Context, routes map[string]*Providers, namespace string) (*cleanupNamespaced, *Providers) {
	sink := o.defaults
	if o.route != nil {
		p, asked := routes[namespace]
		if !asked {
			// A canceled caller never costs a record its route.
			if routed, ok := o.route(context.WithoutCancel(ctx), namespace); ok {
				p = routed
			}
			routes[namespace] = p
		}
		if p == nil {
			return nil, nil
		}
		sink = p
	}
	inst, err := o.instrumentsFor(sink)
	if err != nil {
		return nil, nil
	}
	return inst, sink
}

// instrumentsFor is p's namespaced cleanup instruments, built once per sink.
// A sink whose instruments cannot be built is logged once and its records
// dropped.
func (o *CleanupObserver) instrumentsFor(p *Providers) (*cleanupNamespaced, error) {
	inst, err := o.namespaced.getOrBuild(p, func() (*cleanupNamespaced, error) { return newCleanupNamespaced(p) })
	if err != nil {
		if _, warned := o.warned.LoadOrStore(p, struct{}{}); !warned {
			o.logger.Warn("telemetry: a sink's retention cleanup instruments could not be built; its records are dropped", "error", err)
		}
	}
	return inst, err
}

func newCleanupNamespaced(p *Providers) (*cleanupNamespaced, error) {
	meter := meterFor(p)
	var inst cleanupNamespaced
	var errs []error
	var err error
	inst.runs, err = meter.Int64Counter(CleanupRunsMetric,
		metric.WithDescription("Runs retention cleanup decided on, by namespace and outcome: retired, failed, not_archived, still_running or not_found_not_due."),
		metric.WithUnit("{run}"))
	errs = append(errs, err)
	inst.objects, err = meter.Int64Counter(CleanupObjectsMetric,
		metric.WithDescription("Run-scoped objects retention cleanup removed, by namespace."), metric.WithUnit("{object}"))
	errs = append(errs, err)
	inst.bytes, err = meter.Int64Counter(CleanupBytesMetric,
		metric.WithDescription("Bytes of the run-scoped objects retention cleanup removed, by namespace."), metric.WithUnit("By"))
	errs = append(errs, err)
	inst.lag, err = meter.Float64Gauge(CleanupRetentionLagMetric,
		metric.WithDescription("How far past its due time the most overdue run the last complete walk left unretired is, by namespace; 0 when none is."),
		metric.WithUnit("s"))
	errs = append(errs, err)
	if err := errors.Join(errs...); err != nil {
		return nil, err
	}
	return &inst, nil
}

func (o *CleanupObserver) warnOnce(key, msg string, args ...any) {
	if _, warned := o.warned.LoadOrStore(key, struct{}{}); !warned {
		o.logger.Warn("telemetry: "+msg, args...)
	}
}
