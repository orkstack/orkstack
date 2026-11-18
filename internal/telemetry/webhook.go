package telemetry

import (
	"context"
	"errors"
	"log/slog"
	"slices"
	"strconv"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"github.com/orkstack/orkstack/pkg/webhooks"
)

// Webhook delivery instruments (docs/DEPLOY.md, "Webhooks").
const (
	WebhookAttemptsMetric         = "ork.webhook.attempts"
	WebhookAttemptDurationMetric  = "ork.webhook.attempt.duration"
	WebhookDeliveriesMetric       = "ork.webhook.deliveries"
	WebhookDeliveryAttemptsMetric = "ork.webhook.delivery.attempts"
	WebhookDeliveryLatencyMetric  = "ork.webhook.delivery.latency"
	WebhookNotQueuedMetric        = "ork.webhook.not_queued"
	// AttrWebhookID is the subscription's id, recorded only with object
	// labels on (telemetry.object_labels).
	AttrWebhookID      = "ork.webhook.id"
	AttrWebhookClass   = "ork.webhook.class"
	AttrWebhookAttempt = "ork.webhook.attempt"
	// AttrNotQueuedReason is why a delivery could not be queued.
	AttrNotQueuedReason = "reason"
)

// What became of one attempt, and of one delivery, a closed set.
const (
	WebhookDelivered    = "delivered"
	WebhookRetry        = "retry"
	WebhookFinalFailure = "final_failure"
)

// The classes of an attempt: the answer's status class, or why none came.
const (
	WebhookClassTransport = "transport"
	WebhookClassNotSent   = "not_sent"
	WebhookClassBlocked   = "blocked"
)

var (
	webhookAttemptOutcomes  = []string{WebhookDelivered, WebhookRetry, WebhookFinalFailure}
	webhookDeliveryOutcomes = []string{WebhookDelivered, WebhookFinalFailure}
	webhookClasses          = []string{"2xx", "3xx", "4xx", "5xx", WebhookClassTransport, WebhookClassNotSent, WebhookClassBlocked}
	webhookDurationBuckets  = []float64{0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30}
	webhookAttemptBuckets   = []float64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}
	webhookLatencyBuckets   = []float64{0.1, 0.5, 1, 5, 15, 60, 300, 900, 3600, 21600}
)

// WebhookClasses is the closed set of attempt classes, a fresh copy.
func WebhookClasses() []string { return slices.Clone(webhookClasses) }

// WebhookClass is the class of one attempt: its status class when the
// receiver answered, blocked for a destination the operator's policy
// refused, not_sent for one that made no request, and transport otherwise.
func WebhookClass(a webhooks.Attempt) string {
	switch a.Code {
	case webhooks.AttemptCodeDestinationBlocked:
		return WebhookClassBlocked
	case webhooks.AttemptCodeBodyLost, webhooks.AttemptCodeStoreUnavailable,
		webhooks.AttemptCodeSubscriptionUnavailable, webhooks.AttemptCodeSubscriptionDeleted,
		webhooks.AttemptCodeUnreported:
		return WebhookClassNotSent
	}
	if a.StatusCode >= 200 && a.StatusCode < 600 {
		return strconv.Itoa(a.StatusCode/100) + "xx"
	}
	return WebhookClassTransport
}

// webhookAttemptLabel is an attempt's number as a bounded label: 1 to 9,
// then 10+.
func webhookAttemptLabel(n int) string {
	switch {
	case n < 1:
		return "1"
	case n >= 10:
		return "10+"
	default:
		return strconv.Itoa(n)
	}
}

type webhookInstruments struct {
	attempts         metric.Int64Counter
	attemptDuration  metric.Float64Histogram
	deliveries       metric.Int64Counter
	deliveryAttempts metric.Float64Histogram
	deliveryLatency  metric.Float64Histogram
	notQueued        metric.Int64Counter
}

// WebhookMetrics records webhook delivery attempts and settled deliveries on
// each namespace's sink. A nil *WebhookMetrics records nothing.
type WebhookMetrics struct {
	sinks        *areaSinks[*webhookInstruments]
	objectLabels bool
}

// NewWebhookMetrics builds the webhook instruments. objectLabels is
// telemetry.object_labels: whether the subscription's id is a label.
func NewWebhookMetrics(p *Providers, route GCRouter, objectLabels bool, logger *slog.Logger) *WebhookMetrics {
	return &WebhookMetrics{objectLabels: objectLabels, sinks: newAreaSinks(p, route, "webhook", logger,
		func(p *Providers) (*webhookInstruments, error) {
			meter := meterFor(p)
			var inst webhookInstruments
			var errs []error
			var err error
			inst.attempts, err = meter.Int64Counter(WebhookAttemptsMetric,
				metric.WithDescription("Webhook delivery attempts, by namespace, class, outcome and attempt number; counted at least once."),
				metric.WithUnit("{attempt}"))
			errs = append(errs, err)
			inst.attemptDuration, err = meter.Float64Histogram(WebhookAttemptDurationMetric,
				metric.WithDescription("How long one webhook delivery attempt's request took, by namespace and class."),
				metric.WithUnit("s"), metric.WithExplicitBucketBoundaries(webhookDurationBuckets...))
			errs = append(errs, err)
			inst.deliveries, err = meter.Int64Counter(WebhookDeliveriesMetric,
				metric.WithDescription("Webhook deliveries settled, by namespace and outcome; counted at least once."),
				metric.WithUnit("{delivery}"))
			errs = append(errs, err)
			inst.deliveryAttempts, err = meter.Float64Histogram(WebhookDeliveryAttemptsMetric,
				metric.WithDescription("Attempts one settled webhook delivery took, by namespace and outcome."),
				metric.WithUnit("{attempt}"), metric.WithExplicitBucketBoundaries(webhookAttemptBuckets...))
			errs = append(errs, err)
			inst.deliveryLatency, err = meter.Float64Histogram(WebhookDeliveryLatencyMetric,
				metric.WithDescription("How long after it was queued a webhook delivery settled, by namespace and outcome."),
				metric.WithUnit("s"), metric.WithExplicitBucketBoundaries(webhookLatencyBuckets...))
			errs = append(errs, err)
			inst.notQueued, err = meter.Int64Counter(WebhookNotQueuedMetric,
				metric.WithDescription("Webhook deliveries that could not be queued, so made no request, by namespace and reason."),
				metric.WithUnit("{delivery}"))
			errs = append(errs, err)
			if err := errors.Join(errs...); err != nil {
				return nil, err
			}
			return &inst, nil
		})}
}

// NotQueued counts one delivery that could not be queued, with one of
// webhooks.NotQueuedReasons.
func (m *WebhookMetrics) NotQueued(ctx context.Context, namespace, reason string) {
	if m == nil {
		return
	}
	if !slices.Contains(webhooks.NotQueuedReasons(), reason) {
		m.sinks.warnOnce("not_queued:"+reason, "a webhook not-queued reason outside the metric contract is recorded as unknown", "reason", reason)
		reason = webhooks.NotQueuedUnknown
	}
	if inst, ok := m.sinks.forNamespace(ctx, namespace); ok {
		inst.notQueued.Add(ctx, 1, metric.WithAttributes(attribute.String(AttrNamespace, namespace), attribute.String(AttrNotQueuedReason, reason)))
	}
}

// Attempt counts one attempt of a delivery to webhookID in namespace, with
// what became of it: delivered, retry, or final_failure.
func (m *WebhookMetrics) Attempt(ctx context.Context, namespace, webhookID string, a webhooks.Attempt, outcome string) {
	if m == nil {
		return
	}
	if !slices.Contains(webhookAttemptOutcomes, outcome) {
		m.sinks.warnOnce("attempt:"+outcome, "a webhook attempt outcome outside the metric contract is not recorded", "outcome", outcome)
		return
	}
	inst, ok := m.sinks.forNamespace(ctx, namespace)
	if !ok {
		return
	}
	class := attribute.String(AttrWebhookClass, WebhookClass(a))
	ns := attribute.String(AttrNamespace, namespace)
	attrs := []attribute.KeyValue{ns, class, attribute.String(AttrOutcome, outcome),
		attribute.String(AttrWebhookAttempt, webhookAttemptLabel(a.Number))}
	if m.objectLabels && webhookID != "" {
		attrs = append(attrs, attribute.String(AttrWebhookID, webhookID))
	}
	inst.attempts.Add(ctx, 1, metric.WithAttributes(attrs...))
	inst.attemptDuration.Record(ctx, max(a.Duration.Seconds(), 0), metric.WithAttributes(ns, class))
}

// Delivery counts one settled delivery: delivered or final_failure, the
// attempts it took, and how long after queuedAt it settled (not recorded
// when queuedAt is zero, a delivery queued by a release before it carried
// one).
func (m *WebhookMetrics) Delivery(ctx context.Context, namespace, webhookID, outcome string, attempts int, queuedAt, settledAt time.Time) {
	if m == nil {
		return
	}
	if !slices.Contains(webhookDeliveryOutcomes, outcome) {
		m.sinks.warnOnce("delivery:"+outcome, "a webhook delivery outcome outside the metric contract is not recorded", "outcome", outcome)
		return
	}
	inst, ok := m.sinks.forNamespace(ctx, namespace)
	if !ok {
		return
	}
	ns := attribute.String(AttrNamespace, namespace)
	out := attribute.String(AttrOutcome, outcome)
	attrs := []attribute.KeyValue{ns, out}
	if m.objectLabels && webhookID != "" {
		attrs = append(attrs, attribute.String(AttrWebhookID, webhookID))
	}
	inst.deliveries.Add(ctx, 1, metric.WithAttributes(attrs...))
	inst.deliveryAttempts.Record(ctx, float64(max(attempts, 0)), metric.WithAttributes(ns, out))
	if !queuedAt.IsZero() && !settledAt.IsZero() {
		inst.deliveryLatency.Record(ctx, max(settledAt.Sub(queuedAt).Seconds(), 0), metric.WithAttributes(ns, out))
	}
}
