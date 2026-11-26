package telemetry

import (
	"context"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"github.com/orkstack/orkstack/internal/orker"
)

// The Orker's identity metrics. Each is the one implementation of what
// orker.IdentityObserver hears, so every label is a closed set: none names a
// run, a namespace, a task, an identity or a token.
const (
	// IdentityRefreshMetric counts each refresh of an attempt's identity
	// tokens, after its first ones, by how it ended.
	IdentityRefreshMetric = "ork.identity.refresh"
	// IdentityTokenMarginMetric is how much of its life a token had left when
	// a refresh replaced it.
	IdentityTokenMarginMetric = "ork.identity.token.margin"
	// IdentityClockSkewMetric counts the tokens issued more than 30 s from
	// this Orker's clock.
	IdentityClockSkewMetric = "ork.identity.clock_skew"
	// IdentityEngineRefusedMetric counts, on an Orker, the engine's answers
	// that refused the Orker itself; on an engine, its own refusals
	// (IdentityMetrics).
	IdentityEngineRefusedMetric = "ork.identity.engine_refused"
	// ResultCommitWaitMetric is how long an attempt waited for an engine to
	// record its result.
	ResultCommitWaitMetric = "ork.result.commit.wait"
	// ResultUpstreamUnrecordedMetric counts the attempts that failed because
	// an engine recorded no such result of their upstream.
	ResultUpstreamUnrecordedMetric = "ork.result.upstream_unrecorded"

	// AttrRefusedHow labels ork.identity.engine_refused on an Orker: refused
	// or invalid.
	AttrRefusedHow = "ork.refused"
)

var (
	identityRefreshOutcomes = map[string]bool{orker.IdentityRefreshed: true, orker.IdentityRefreshRetried: true,
		orker.IdentityRefreshEnded: true, orker.IdentityRefreshStopped: true}
	commitWaitOutcomes = map[string]bool{orker.ResultRecorded: true, orker.ResultNotRecorded: true, orker.ResultRefused: true}
	engineRefusals     = map[string]bool{orker.EngineRefusedOrker: true, orker.EngineCouldNotRead: true}

	// A token lives minutes to a day (identity.token_ttl); below zero is one
	// that had expired when it was replaced.
	tokenMarginBuckets = []float64{-60, 0, 30, 60, 120, 300, 600, 1800, 3600, 7200, 21600, 43200}
	// A commit's wait is at most identity.results.commit_wait.
	commitWaitBuckets = []float64{0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60, 120, 300, 600, 1800}
)

// OrkerIdentityMetrics is the Orker's identity metrics (docs/DEPLOY.md,
// "Lifecycle metrics"). Recording can fail nothing.
type OrkerIdentityMetrics struct {
	refresh, skew, refused, upstream metric.Int64Counter
	margin, wait                     metric.Float64Histogram
	orker                            attribute.KeyValue
}

var _ orker.IdentityObserver = (*OrkerIdentityMetrics)(nil)

// NewOrkerIdentityMetrics builds them from p's meter provider. It works, as a
// no-op, on nil or disabled Providers.
func NewOrkerIdentityMetrics(p *Providers, orkerID string) (*OrkerIdentityMetrics, error) {
	meter := meterFor(p)
	m := &OrkerIdentityMetrics{orker: attribute.String(AttrOrkerID, orkerID)}
	for _, c := range []struct {
		into              *metric.Int64Counter
		name, unit, about string
	}{
		{&m.refresh, IdentityRefreshMetric, "{refresh}", "Refreshes of an attempt's identity tokens, after its first ones, by outcome: refreshed, retried, ended (the attempt) or stopped (a best_effort identity was revoked)."},
		{&m.skew, IdentityClockSkewMetric, "{token}", "Identity tokens issued more than 30 s from this Orker's clock."},
		{&m.refused, IdentityEngineRefusedMetric, "{call}", "Engine answers that refused this Orker itself (refused) or could not read what it sent (invalid), to a mint or to a task's result."},
		{&m.upstream, ResultUpstreamUnrecordedMetric, "{attempt}", "Attempts that failed because an engine recorded no such result of their upstream."},
	} {
		var err error
		if *c.into, err = meter.Int64Counter(c.name, metric.WithDescription(c.about), metric.WithUnit(c.unit)); err != nil {
			return nil, err
		}
	}
	var err error
	if m.margin, err = meter.Float64Histogram(IdentityTokenMarginMetric, metric.WithUnit("s"),
		metric.WithDescription("Life left of the identity token nearest its end when a refresh replaced it; below zero for one that had expired."),
		metric.WithExplicitBucketBoundaries(tokenMarginBuckets...)); err != nil {
		return nil, err
	}
	if m.wait, err = meter.Float64Histogram(ResultCommitWaitMetric, metric.WithUnit("s"),
		metric.WithDescription("Time an attempt waited for an engine to record its result, by outcome: recorded, not_recorded or refused."),
		metric.WithExplicitBucketBoundaries(commitWaitBuckets...)); err != nil {
		return nil, err
	}
	return m, nil
}

// Refreshed counts one refresh under an outcome of the closed set.
func (m *OrkerIdentityMetrics) Refreshed(outcome string) {
	m.add(m.refresh, attribute.String(AttrOutcome, within(identityRefreshOutcomes, outcome)))
}

// TokenMargin records the life a replaced token had left.
func (m *OrkerIdentityMetrics) TokenMargin(left time.Duration) {
	m.margin.Record(context.Background(), left.Seconds(), metric.WithAttributes(m.orker))
}

// ClockSkewed counts one token issued far from this Orker's clock.
func (m *OrkerIdentityMetrics) ClockSkewed() { m.add(m.skew) }

// EngineRefused counts one answer that refused this Orker itself.
func (m *OrkerIdentityMetrics) EngineRefused(how string) {
	m.add(m.refused, attribute.String(AttrRefusedHow, within(engineRefusals, how)))
}

// CommitWaited records one attempt's wait for its result to be recorded.
func (m *OrkerIdentityMetrics) CommitWaited(waited time.Duration, outcome string) {
	m.wait.Record(context.Background(), waited.Seconds(), metric.WithAttributes(m.orker, attribute.String(AttrOutcome, within(commitWaitOutcomes, outcome))))
}

// UpstreamUnrecorded counts one attempt that failed for it.
func (m *OrkerIdentityMetrics) UpstreamUnrecorded() { m.add(m.upstream) }

func (m *OrkerIdentityMetrics) add(c metric.Int64Counter, attrs ...attribute.KeyValue) {
	c.Add(context.Background(), 1, metric.WithAttributes(append([]attribute.KeyValue{m.orker}, attrs...)...))
}

// within is value when the closed set holds it, "other" otherwise.
func within(set map[string]bool, value string) string {
	if set[value] {
		return value
	}
	return "other"
}
