package telemetry

import (
	"context"
	"errors"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	metricnoop "go.opentelemetry.io/otel/metric/noop"
	"go.opentelemetry.io/otel/trace"
)

// The authentication and authorization metrics (docs/AUTHENTICATION.md,
// docs/ACCESS.md). None carries a token, a cookie or a key, and none a
// principal's id.
const (
	AuthRequestsMetric          = "ork.auth.requests"
	AuthDecisionsMetric         = "ork.auth.decisions"
	AuthStreamsClosedMetric     = "ork.auth.streams_closed"
	AuthBreakGlassMetric        = "ork.auth.break_glass"
	AuthModeRecordMissingMetric = "ork.auth.mode_record_missing"
	AuthFleetUnenforcedMetric   = "ork.auth.fleet_unenforced"
	// The access document's copy on this replica (docs/ACCESS.md, "Every
	// replica's copy"): its revision, how old it is, each refresh's outcome,
	// and each head found written around the API.
	AuthPolicyGenerationMetric   = "ork.auth.policy.generation"
	AuthPolicyAgeMetric          = "ork.auth.policy.age"
	AuthPolicyRefreshMetric      = "ork.auth.policy.refresh"
	AuthDocumentUnexpectedMetric = "ork.auth.document.unexpected"
)

// AuthInstruments counts authentication and authorization on the process's
// default providers. It satisfies internal/api's AuthCounter seam, which is
// how internal/api stays free of any OpenTelemetry import.
type AuthInstruments struct {
	requests      metric.Int64Counter
	decisions     metric.Int64Counter
	streamsClosed metric.Int64Counter
	breakGlass    metric.Int64Counter
	modeMissing   metric.Int64Counter
	fleet         metric.Int64Gauge
	refresh       metric.Int64Counter
	unexpected    metric.Int64Counter
	meter         metric.Meter
}

// NewAuthInstruments creates the instruments from p's meter provider. It
// works, as a no-op, on disabled or nil Providers.
func NewAuthInstruments(p *Providers) (*AuthInstruments, error) {
	var meter metric.Meter
	if p != nil {
		meter = p.Meter.Meter(ScopeName)
	} else {
		meter = metricnoop.NewMeterProvider().Meter(ScopeName)
	}
	counter := func(name, description, unit string) (metric.Int64Counter, error) {
		return meter.Int64Counter(name, metric.WithDescription(description), metric.WithUnit(unit))
	}
	a := AuthInstruments{meter: meter}
	var errs [8]error
	a.requests, errs[0] = counter(AuthRequestsMetric, "Requests that presented a credential, by method and outcome (authenticated, refused, unavailable).", "{request}")
	a.decisions, errs[1] = counter(AuthDecisionsMetric, "Authorization decisions, by permission and decision (allowed, denied, would-deny).", "{decision}")
	a.streamsClosed, errs[2] = counter(AuthStreamsClosedMetric, "Open streams and sockets closed by a re-check, by reason (denied, disabled, unavailable, expired).", "{stream}")
	a.breakGlass, errs[3] = counter(AuthBreakGlassMetric, "Break-glass requests served. Page on any.", "{request}")
	a.modeMissing, errs[4] = counter(AuthModeRecordMissingMetric, "Times a running engine found the mode record it saw gone, and wrote it again.", "{event}")
	a.fleet, errs[5] = meter.Int64Gauge(AuthFleetUnenforcedMetric,
		metric.WithDescription("Engines the last fleet check saw that do not enforce authentication."), metric.WithUnit("{engine}"))
	a.refresh, errs[6] = counter(AuthPolicyRefreshMetric, "Reads of the access document, by outcome (unchanged, changed, failed).", "{read}")
	a.unexpected, errs[7] = counter(AuthDocumentUnexpectedMetric, "Access document heads found written around the API, or gone.", "{event}")
	if err := errors.Join(errs[:]...); err != nil {
		return nil, err
	}
	return &a, nil
}

// CountRequest counts one request that presented a credential.
func (a *AuthInstruments) CountRequest(ctx context.Context, method, outcome string) {
	if a == nil {
		return
	}
	a.requests.Add(ctx, 1, metric.WithAttributes(attribute.String("method", method), attribute.String("outcome", outcome)))
}

// CountDecision counts one authorization decision.
func (a *AuthInstruments) CountDecision(ctx context.Context, permission, decision string) {
	if a == nil {
		return
	}
	a.decisions.Add(ctx, 1, metric.WithAttributes(attribute.String("permission", permission), attribute.String("decision", decision)))
}

// CountStreamClosed counts a stream or socket a re-check closed.
func (a *AuthInstruments) CountStreamClosed(ctx context.Context, reason string) {
	if a == nil {
		return
	}
	a.streamsClosed.Add(ctx, 1, metric.WithAttributes(attribute.String("reason", reason)))
}

// CountBreakGlass counts one break-glass request.
func (a *AuthInstruments) CountBreakGlass(ctx context.Context) {
	if a == nil {
		return
	}
	a.breakGlass.Add(ctx, 1)
}

// CountModeRecordMissing counts a mode record found gone.
func (a *AuthInstruments) CountModeRecordMissing(ctx context.Context) {
	if a == nil {
		return
	}
	a.modeMissing.Add(ctx, 1)
}

// SetFleetUnenforced records the last fleet check's count.
func (a *AuthInstruments) SetFleetUnenforced(ctx context.Context, n int) {
	if a == nil {
		return
	}
	a.fleet.Record(ctx, int64(n))
}

// WatchPolicy reports the access document's copy: its revision and its age
// in seconds, read from state at each collection.
func (a *AuthInstruments) WatchPolicy(state func() (generation int64, ageSeconds float64)) error {
	if a == nil || state == nil {
		return nil
	}
	generation, err := a.meter.Int64ObservableGauge(AuthPolicyGenerationMetric,
		metric.WithDescription("The access document revision this replica decides against."), metric.WithUnit("{revision}"))
	if err != nil {
		return err
	}
	age, err := a.meter.Float64ObservableGauge(AuthPolicyAgeMetric,
		metric.WithDescription("How old this replica's copy of the access document is: past auth.policy_refresh it is served stale."), metric.WithUnit("s"))
	if err != nil {
		return err
	}
	_, err = a.meter.RegisterCallback(func(_ context.Context, o metric.Observer) error {
		g, s := state()
		o.ObserveInt64(generation, g)
		o.ObserveFloat64(age, s)
		return nil
	}, generation, age)
	return err
}

// CountPolicyRefresh counts one read of the access document.
func (a *AuthInstruments) CountPolicyRefresh(ctx context.Context, outcome string) {
	if a == nil {
		return
	}
	a.refresh.Add(ctx, 1, metric.WithAttributes(attribute.String("outcome", outcome)))
}

// CountDocumentUnexpected counts a head written around the API, or gone.
func (a *AuthInstruments) CountDocumentUnexpected(ctx context.Context) {
	if a == nil {
		return
	}
	a.unexpected.Add(ctx, 1)
}

// Annotate sets a request's span attributes: the principal's kind, the
// method and the decision. Never the principal's id.
func (a *AuthInstruments) Annotate(ctx context.Context, kind, method, decision string) {
	if a == nil {
		return
	}
	trace.SpanFromContext(ctx).SetAttributes(
		attribute.String("ork.principal.kind", kind),
		attribute.String("ork.auth.method", method),
		attribute.String("ork.auth.decision", decision))
}
