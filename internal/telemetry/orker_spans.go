package telemetry

import (
	"context"
	"log/slog"
	"sync"
	"time"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/orker"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
)

// AttrPhase and AttrFailureReason label the Orker's own attempt and phase
// spans, and the ork.orker.* instruments they drive.
const (
	AttrPhase         = "ork.phase"
	AttrFailureReason = "ork.failure.reason"
)

// Outcomes recorded on ork.orker.tasks. Unlike EngineInstruments.Tasks'
// succeeded|failed, an Orker attempt has two more shapes worth telling apart:
// a cancellation is neither a success nor a workload failure, and an adopted
// commit ran no container at all (§5.4).
const (
	OutcomeSucceeded = "succeeded"
	OutcomeFailed    = "failed"
	OutcomeCanceled  = "canceled"
	OutcomeAdopted   = "adopted"
)

// OrkerObserver is the one implementation of orker.PhaseObserver in the
// repository. internal/orker must never import this package — activity code
// stays free of the OpenTelemetry SDK (AGENTS.md §11) — so the seam is a
// plain Go interface on that side, satisfied here structurally.
//
// A task's spans and metrics are a per-run signal (§ package doc): they go to
// whatever sink ExecuteTaskInput.GetTelemetry().GetSink() names, chosen fresh
// in Begin for every attempt. The Orker's own process-level signals — its
// slog output, the Temporal SDK's own metrics, /metrics — never pass through
// this type and stay on the default sink exclusively.
type OrkerObserver struct {
	sinks  *Sinks
	logger *slog.Logger

	cache providerCache[*orkerRoute]
}

// orkerRoute is one sink's cached tracer, propagator, instruments and
// cardinality policy — built at most once per sink, however many attempts
// Begin routes to it.
type orkerRoute struct {
	tracer     trace.Tracer
	propagator propagation.TextMapPropagator
	inst       *OrkerInstruments
	card       Cardinality
}

var _ orker.PhaseObserver = (*OrkerObserver)(nil)

// newOrkerRoute builds one sink's route from p's tracer, meter and
// propagator. A nil p behaves like disabled Providers: newOrkerRoute reuses
// noopProviders (setup.go) for that rather than hand-building the same no-op
// tracer and propagator a second time. inst, when non-nil, is used as-is
// instead of building fresh instruments — the seam NewOrkerObserver uses to
// keep its existing "caller builds instruments once, itself" contract; every
// other caller passes nil and gets instruments built from p (any
// construction error leaves inst nil — a route with nil instruments still
// traces, and every metric call in this file already checks a.inst != nil).
func newOrkerRoute(p *Providers, inst *OrkerInstruments) *orkerRoute {
	if p == nil {
		p = noopProviders(Config{})
	}
	propagator := p.Propagator
	if propagator == nil {
		propagator = propagation.NewCompositeTextMapPropagator(propagation.TraceContext{}, propagation.Baggage{})
	}
	if inst == nil {
		inst, _ = NewOrkerInstruments(p)
	}
	return &orkerRoute{tracer: p.Tracer.Tracer(ScopeName), propagator: propagator, inst: inst, card: p.Cardinality()}
}

// NewOrkerObserverForSinks builds an observer that picks s.For(attempt's
// sink) fresh in every Begin, caching each sink's route the first time it is
// needed. logger records what cannot be a span attribute; nil uses
// slog.Default only where TaskLogExporter and EngineObserver do — this type
// does not itself default logger, since attemptObservation only uses it for
// occasional warnings and a nil *slog.Logger there is simply silent.
func NewOrkerObserverForSinks(s *Sinks, logger *slog.Logger) *OrkerObserver {
	return &OrkerObserver{sinks: s, logger: logger}
}

// NewOrkerObserver is NewOrkerObserverForSinks with one sink: p is both the
// only destination and the source of inst, which is used as p's route
// verbatim rather than rebuilt — preserving the historical contract where
// cmd/orker builds one OrkerInstruments itself and hands it in.
func NewOrkerObserver(p *Providers, inst *OrkerInstruments, logger *slog.Logger) *OrkerObserver {
	o := &OrkerObserver{sinks: sinksOf(p), logger: logger}
	o.cache.set(p, newOrkerRoute(p, inst))
	return o
}

// routeFor returns p's cached route, building it once.
func (o *OrkerObserver) routeFor(p *Providers) *orkerRoute {
	r, _ := o.cache.getOrBuild(p, func() (*orkerRoute, error) {
		r := newOrkerRoute(p, nil)
		if r.inst == nil && o.logger != nil {
			o.logger.Warn("telemetry: failed to build Orker instruments for a sink; metrics disabled for it")
		}
		return r, nil
	})
	return r
}

// Begin starts "ork.task.attempt", whose parent is whatever the Temporal
// tracing interceptor already put on ctx (TemporalInterceptors) — so an
// Orker's attempt span is a child of the engine's workflow span with no
// OrkStack code stitching the two together.
//
// The returned context is what OCIDispatcher.Execute uses for the rest of the
// attempt: every phase span Phase starts is this attempt span's child, and
// OrkerObserver.TraceContext, called on that same context, is what makes the
// span reachable from task.json's optional `trace` key (§3.2).
func (o *OrkerObserver) Begin(
	ctx context.Context, in *orkstackv1.ExecuteTaskInput, attempt int32,
) (context.Context, orker.AttemptObserver) {
	sel := in.GetTelemetry()
	route := o.routeFor(o.sinks.For(sel.GetSink()))

	// The selection's attributes ride the attempt span unconditionally — a
	// span attribute has no per-value time-series cost the way a metric
	// label does — but only ride a metric under route.card.High
	// (attemptObservation.selAttrs, applied in Phase and End).
	selAttrs := selectionAttrs(sel.GetAttributes())
	spanAttrs := []attribute.KeyValue{
		attribute.String(AttrRunID, in.GetRunId()),
		attribute.String(AttrTaskID, in.GetTaskId()),
		attribute.Int64(AttrAttempt, int64(attempt)),
		attribute.String(AttrNamespace, in.GetNamespace()),
		attribute.String(AttrOrkflowName, in.GetOrkflowName()),
		attribute.String(AttrPool, in.GetPool()),
	}
	if each := in.GetEach(); each != nil {
		spanAttrs = append(spanAttrs, attribute.Int64(AttrItem, int64(each.GetIndex())))
	}
	spanAttrs = append(spanAttrs, selAttrs...)

	spanCtx, span := route.tracer.Start(ctx, "ork.task.attempt", trace.WithAttributes(spanAttrs...))
	var metricSelAttrs []attribute.KeyValue
	if route.card.High {
		metricSelAttrs = selAttrs
	}
	return spanCtx, &attemptObservation{
		tracer:      route.tracer,
		inst:        route.inst,
		card:        route.card,
		selAttrs:    metricSelAttrs,
		logger:      o.logger,
		in:          in,
		started:     time.Now(),
		attemptCtx:  spanCtx,
		attemptSpan: span,
	}
}

// TraceContext returns the W3C carrier for ctx — "traceparent" and, when the
// propagator sets one, "tracestate" — or an empty map when ctx carries no
// valid span (the propagator's own Inject is a no-op in that case). Its
// signature matches OCIDispatcher.TraceContext, so cmd/orker can assign it
// directly with no adapter.
//
// Every sink shares the same SDK-default W3C propagator (TraceContext +
// Baggage), so which sink an attempt was routed to makes no difference here;
// this always uses the default sink's route rather than looking one up per
// call.
func (o *OrkerObserver) TraceContext(ctx context.Context) map[string]string {
	carrier := propagation.MapCarrier{}
	route := o.routeFor(o.sinks.Default())
	if route.propagator != nil {
		route.propagator.Inject(ctx, carrier)
	}
	return map[string]string(carrier)
}

// attemptObservation is one attempt's live spans, returned by Begin as an
// orker.AttemptObserver. It is not safe for concurrent Phase/End calls from
// more than one goroutine — the dispatcher calls both from its own single
// sequence — but the mutex guards against Phase racing a concurrent End
// (e.g. a cancellation observed on another goroutine) all the same.
type attemptObservation struct {
	tracer trace.Tracer
	inst   *OrkerInstruments
	card   Cardinality
	// selAttrs is the attempt's telemetry selection attributes, already
	// filtered to only what card.High allows on a metric (nil when card.High
	// is off) — Begin decided this once so Phase and End need no further
	// cardinality check.
	selAttrs []attribute.KeyValue
	logger   *slog.Logger

	in         *orkstackv1.ExecuteTaskInput
	started    time.Time
	attemptCtx context.Context

	mu          sync.Mutex
	attemptSpan trace.Span
	ended       bool
	phaseSpan   trace.Span
	phaseName   string
	phaseStart  time.Time
}

// Phase ends the previous phase span, if any, records its duration, and
// starts a new child span named "ork.task.phase" carrying ork.phase = name.
func (a *attemptObservation) Phase(name string) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.ended {
		return
	}
	now := time.Now()
	a.endPhaseLocked(now)
	_, span := a.tracer.Start(a.attemptCtx, "ork.task.phase",
		trace.WithAttributes(attribute.String(AttrPhase, name)))
	a.phaseSpan = span
	a.phaseName = name
	a.phaseStart = now
}

// endPhaseLocked ends the current phase span and records its duration. The
// caller holds a.mu.
func (a *attemptObservation) endPhaseLocked(now time.Time) {
	if a.phaseSpan == nil {
		return
	}
	a.phaseSpan.End()
	if a.inst != nil && a.inst.TaskPhaseDuration != nil {
		attrs := a.card.TaskAttrs(a.in, attribute.String(AttrPhase, a.phaseName))
		attrs = append(attrs, a.selAttrs...)
		a.inst.TaskPhaseDuration.Record(a.attemptCtx, now.Sub(a.phaseStart).Seconds(),
			metric.WithAttributes(attrs...))
	}
	a.phaseSpan = nil
}

// End ends the last phase span and the attempt span exactly once, sets span
// status Error with the reason when outcome.Reason is non-empty, and records
// the ork.orker.tasks / ork.orker.task.duration / ork.orker.commit_adopted
// instruments.
func (a *attemptObservation) End(outcome orker.AttemptOutcome) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.ended {
		return
	}
	a.ended = true
	now := time.Now()
	a.endPhaseLocked(now)

	label := OutcomeSucceeded
	switch {
	case outcome.Canceled:
		label = OutcomeCanceled
	case outcome.CommitAdopted:
		label = OutcomeAdopted
	case outcome.Reason != "":
		label = OutcomeFailed
	}

	if outcome.Reason != "" {
		a.attemptSpan.SetStatus(codes.Error, outcome.Reason)
		a.attemptSpan.SetAttributes(attribute.String(AttrFailureReason, outcome.Reason))
	}
	a.attemptSpan.SetAttributes(attribute.String(AttrOutcome, label))
	// A cached task's memo decision, and a miss's reason; never the key.
	if cache := CacheOutcomeLabel(outcome.Cache); cache != "" {
		a.attemptSpan.SetAttributes(attribute.String(AttrCacheOutcome, cache))
		if reason := outcome.Cache.GetMissReason(); reason != "" {
			a.attemptSpan.SetAttributes(attribute.String(AttrCacheMissReason, reason))
		}
	}
	a.attemptSpan.End()

	if a.inst == nil {
		return
	}
	attrs := a.card.TaskAttrs(a.in,
		attribute.String(AttrOutcome, label),
		attribute.String(AttrFailureReason, outcome.Reason),
	)
	attrs = append(attrs, a.selAttrs...)
	if a.inst.Tasks != nil {
		a.inst.Tasks.Add(a.attemptCtx, 1, metric.WithAttributes(attrs...))
	}
	durationAttrs := append(a.card.TaskAttrs(a.in), a.selAttrs...)
	if a.inst.TaskDuration != nil {
		a.inst.TaskDuration.Record(a.attemptCtx, now.Sub(a.started).Seconds(),
			metric.WithAttributes(durationAttrs...))
	}
	if outcome.CommitAdopted && a.inst.CommitAdopted != nil {
		a.inst.CommitAdopted.Add(a.attemptCtx, 1, metric.WithAttributes(durationAttrs...))
	}
}
