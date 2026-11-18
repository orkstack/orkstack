package telemetry

import (
	"context"
	"log/slog"
	"slices"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

// TriggerInvocationsMetric counts HTTP trigger invocations by outcome
// (docs/DEPLOY.md, "Triggers").
const TriggerInvocationsMetric = "ork.trigger.invocations"

// The outcomes of one trigger invocation, a closed set.
const (
	TriggerStarted         = "started"
	TriggerUnauthenticated = "unauthenticated"
	TriggerNotFound        = "not_found"
	TriggerInvalidArgument = "invalid_argument"
	// TriggerRefused: the submission was refused (a failed precondition).
	TriggerRefused = "refused"
	// TriggerUnavailable: the cluster or store did not answer; worth retrying.
	TriggerUnavailable = "unavailable"
	// TriggerConflict: the Idempotency-Key was used for another submission.
	TriggerConflict = "conflict"
	TriggerInternal = "internal"
)

var triggerOutcomes = []string{TriggerStarted, TriggerUnauthenticated, TriggerNotFound, TriggerInvalidArgument,
	TriggerRefused, TriggerUnavailable, TriggerConflict, TriggerInternal}

// TriggerOutcomes is the closed set of trigger outcomes, a fresh copy.
func TriggerOutcomes() []string { return slices.Clone(triggerOutcomes) }

// TriggerMetrics records HTTP trigger invocations. A nil *TriggerMetrics
// records nothing.
type TriggerMetrics struct {
	sinks *areaSinks[metric.Int64Counter]
}

// NewTriggerMetrics builds the trigger instruments: on the namespace's sink
// for an invocation whose trigger was found, on p for one that named none.
func NewTriggerMetrics(p *Providers, route GCRouter, logger *slog.Logger) *TriggerMetrics {
	return &TriggerMetrics{sinks: newAreaSinks(p, route, "trigger", logger, func(p *Providers) (metric.Int64Counter, error) {
		return meterFor(p).Int64Counter(TriggerInvocationsMetric,
			metric.WithDescription("HTTP trigger invocations, by namespace and outcome. One that named no trigger carries no namespace."),
			metric.WithUnit("{invocation}"))
	})}
}

// Invocation counts one invocation. namespace is the found trigger's; ""
// when the request named no trigger, which is counted on the default sink
// without one. An outcome outside the closed set is dropped and logged once.
func (m *TriggerMetrics) Invocation(ctx context.Context, namespace, outcome string) {
	if m == nil {
		return
	}
	if !slices.Contains(triggerOutcomes, outcome) {
		m.sinks.warnOnce("trigger:"+outcome, "a trigger outcome outside the metric contract is not recorded", "outcome", outcome)
		return
	}
	attrs := []attribute.KeyValue{attribute.String(AttrOutcome, outcome)}
	var (
		counter metric.Int64Counter
		ok      bool
	)
	if namespace == "" {
		counter, ok = m.sinks.forDefault()
	} else {
		counter, ok = m.sinks.forNamespace(ctx, namespace)
		attrs = append(attrs, attribute.String(AttrNamespace, namespace))
	}
	if ok {
		counter.Add(ctx, 1, metric.WithAttributes(attrs...))
	}
}
