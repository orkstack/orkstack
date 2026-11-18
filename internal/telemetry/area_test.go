package telemetry

import (
	"context"
	"io"
	"log/slog"
	"testing"
	"time"

	sdkmetric "go.opentelemetry.io/otel/sdk/metric"

	"github.com/orkstack/orkstack/pkg/webhooks"
)

func areaProviders(t *testing.T) (*Providers, *sdkmetric.ManualReader) {
	t.Helper()
	reader := sdkmetric.NewManualReader()
	return newTestProviders(t, &recordingExporter{}, reader), reader
}

func discardLogger() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

// dropAll is a router that drops every namespace, as the policy cache does
// for one it has not loaded yet.
func dropAll(context.Context, string) (*Providers, bool) { return nil, false }

func TestTriggerMetrics_CountsEachInvocationUnderItsTriggersNamespace(t *testing.T) {
	p, reader := areaProviders(t)
	m := NewTriggerMetrics(p, nil, discardLogger())
	ctx := context.Background()
	m.Invocation(ctx, "acme", TriggerStarted)
	m.Invocation(ctx, "acme", TriggerStarted)
	m.Invocation(ctx, "acme", TriggerUnavailable)
	m.Invocation(ctx, "", TriggerNotFound)
	m.Invocation(ctx, "acme", "exploded")
	points := collectGC(t, reader)
	if got := value(t, points, TriggerInvocationsMetric, map[string]string{AttrNamespace: "acme", AttrOutcome: "started"}); got != 2 {
		t.Fatalf("started = %v", got)
	}
	if got := value(t, points, TriggerInvocationsMetric, map[string]string{AttrNamespace: "acme", AttrOutcome: "unavailable"}); got != 1 {
		t.Fatalf("unavailable = %v", got)
	}
	if got := value(t, points, TriggerInvocationsMetric, map[string]string{AttrOutcome: "not_found"}); got != 1 {
		t.Fatalf("an invocation naming no trigger = %v, want counted with no namespace", got)
	}
	if n := len(points[TriggerInvocationsMetric]); n != 3 {
		t.Fatalf("series = %d, want 3: an outcome outside the set is never recorded", n)
	}
}

func TestTriggerMetrics_ADroppedNamespaceIsNeverSentToTheDefaultSink(t *testing.T) {
	p, reader := areaProviders(t)
	m := NewTriggerMetrics(p, dropAll, discardLogger())
	m.Invocation(context.Background(), "acme", TriggerStarted)
	if points := collectGC(t, reader); len(points[TriggerInvocationsMetric]) != 0 {
		t.Fatalf("a dropped namespace's record reached the default sink: %v", points[TriggerInvocationsMetric])
	}
	var nilMetrics *TriggerMetrics
	nilMetrics.Invocation(context.Background(), "acme", TriggerStarted) // must not panic
}

func TestScheduleMetrics_FiresCarryTheirOutcomeAndARefusalItsReason(t *testing.T) {
	for _, tc := range []struct {
		name         string
		objectLabels bool
	}{{"object labels off", false}, {"object labels on", true}} {
		t.Run(tc.name, func(t *testing.T) {
			p, reader := areaProviders(t)
			m := NewScheduleMetrics(p, nil, tc.objectLabels, discardLogger())
			ctx := context.Background()
			m.Fire(ctx, "acme", "nightly", ScheduleFireStarted, "", 2*time.Second)
			m.Fire(ctx, "acme", "nightly", ScheduleFireRefused, "failed_precondition", time.Second)
			m.Fire(ctx, "acme", "nightly", ScheduleFireRefused, "something new", time.Second)
			m.Fire(ctx, "acme", "nightly", "exploded", "", time.Second)
			points := collectGC(t, reader)

			base := map[string]string{AttrNamespace: "acme"}
			if tc.objectLabels {
				base[AttrSchedule] = "nightly"
			}
			with := func(extra map[string]string) map[string]string {
				out := map[string]string{}
				for k, v := range base {
					out[k] = v
				}
				for k, v := range extra {
					out[k] = v
				}
				return out
			}
			if got := value(t, points, ScheduleFiresMetric, with(map[string]string{AttrOutcome: "started"})); got != 1 {
				t.Fatalf("started = %v", got)
			}
			if got := value(t, points, ScheduleFiresMetric, with(map[string]string{AttrOutcome: "refused", AttrRefusalReason: "failed_precondition"})); got != 1 {
				t.Fatalf("refused = %v", got)
			}
			if got := value(t, points, ScheduleFiresMetric, with(map[string]string{AttrOutcome: "refused", AttrRefusalReason: "internal"})); got != 1 {
				t.Fatalf("a refusal reason outside the set = %v, want recorded as internal", got)
			}
			if got := value(t, points, ScheduleFireLagMetric, base); got != 3 {
				t.Fatalf("lag observations = %v, want one per recorded fire", got)
			}
			for _, p := range points[ScheduleFiresMetric] {
				if _, has := p.labels[AttrSchedule]; has != tc.objectLabels {
					t.Fatalf("schedule label present = %v with object labels %v: %v", has, tc.objectLabels, p)
				}
			}
		})
	}
}

func TestScheduleMetrics_RequestsCountOnlyTheirClosedActions(t *testing.T) {
	p, reader := areaProviders(t)
	m := NewScheduleMetrics(p, nil, false, discardLogger())
	ctx := context.Background()
	for _, action := range ScheduleActions() {
		m.Request(ctx, "acme", action)
	}
	m.Request(ctx, "acme", "teleport")
	points := collectGC(t, reader)
	if got, want := len(points[ScheduleRequestsMetric]), len(ScheduleActions()); got != want {
		t.Fatalf("series = %d, want %d", got, want)
	}
	if got := value(t, points, ScheduleRequestsMetric, map[string]string{AttrNamespace: "acme", AttrAction: "backfill"}); got != 1 {
		t.Fatalf("backfill = %v", got)
	}
}

func TestWebhookClass(t *testing.T) {
	for _, tc := range []struct {
		name string
		in   webhooks.Attempt
		want string
	}{
		{"answered 204", webhooks.Attempt{StatusCode: 204}, "2xx"},
		{"answered 301", webhooks.Attempt{StatusCode: 301}, "3xx"},
		{"answered 429", webhooks.Attempt{StatusCode: 429}, "4xx"},
		{"answered 503", webhooks.Attempt{StatusCode: 503}, "5xx"},
		{"an informational answer", webhooks.Attempt{StatusCode: 101}, "transport"},
		{"connect refused", webhooks.Attempt{Code: webhooks.AttemptCodeConnect}, "transport"},
		{"timed out", webhooks.Attempt{Code: webhooks.AttemptCodeTimeout}, "transport"},
		{"destination blocked", webhooks.Attempt{Code: webhooks.AttemptCodeDestinationBlocked}, "blocked"},
		{"body lost", webhooks.Attempt{Code: webhooks.AttemptCodeBodyLost}, "not_sent"},
		{"subscription deleted", webhooks.Attempt{Code: webhooks.AttemptCodeSubscriptionDeleted}, "not_sent"},
		{"unreported", webhooks.Attempt{Code: webhooks.AttemptCodeUnreported}, "not_sent"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := WebhookClass(tc.in)
			if got != tc.want {
				t.Fatalf("class = %q, want %q", got, tc.want)
			}
			found := false
			for _, c := range WebhookClasses() {
				found = found || c == got
			}
			if !found {
				t.Fatalf("class %q is outside the closed set", got)
			}
		})
	}
}

func TestWebhookMetrics_AttemptsAndDeliveriesCarryBoundedLabels(t *testing.T) {
	p, reader := areaProviders(t)
	m := NewWebhookMetrics(p, nil, false, discardLogger())
	ctx := context.Background()
	m.Attempt(ctx, "acme", "wh-1", webhooks.Attempt{Number: 1, StatusCode: 503, Duration: time.Second}, WebhookRetry)
	m.Attempt(ctx, "acme", "wh-1", webhooks.Attempt{Number: 14, StatusCode: 200, Duration: time.Second}, WebhookDelivered)
	m.Attempt(ctx, "acme", "wh-1", webhooks.Attempt{Number: 2}, "exploded")
	queued := time.Date(2026, 11, 17, 12, 0, 0, 0, time.UTC)
	m.Delivery(ctx, "acme", "wh-1", WebhookDelivered, 14, queued, queued.Add(time.Minute))
	m.Delivery(ctx, "acme", "wh-1", WebhookFinalFailure, 3, time.Time{}, queued)
	points := collectGC(t, reader)

	if got := value(t, points, WebhookAttemptsMetric, map[string]string{AttrNamespace: "acme", AttrWebhookClass: "5xx",
		AttrOutcome: "retry", AttrWebhookAttempt: "1"}); got != 1 {
		t.Fatalf("retry attempt = %v", got)
	}
	if got := value(t, points, WebhookAttemptsMetric, map[string]string{AttrNamespace: "acme", AttrWebhookClass: "2xx",
		AttrOutcome: "delivered", AttrWebhookAttempt: "10+"}); got != 1 {
		t.Fatalf("a fourteenth attempt = %v, want counted as 10+", got)
	}
	if n := len(points[WebhookAttemptsMetric]); n != 2 {
		t.Fatalf("attempt series = %d, want 2: an outcome outside the set is never recorded", n)
	}
	for _, name := range []string{WebhookAttemptsMetric, WebhookDeliveriesMetric} {
		for _, p := range points[name] {
			if _, has := p.labels[AttrWebhookID]; has {
				t.Fatalf("%s carries the webhook id with object labels off: %v", name, p)
			}
		}
	}
	if got := value(t, points, WebhookDeliveriesMetric, map[string]string{AttrNamespace: "acme", AttrOutcome: "final_failure"}); got != 1 {
		t.Fatalf("final failures = %v", got)
	}
	if got := value(t, points, WebhookDeliveryLatencyMetric, map[string]string{AttrNamespace: "acme", AttrOutcome: "delivered"}); got != 1 {
		t.Fatalf("latency observations = %v", got)
	}
	for _, p := range points[WebhookDeliveryLatencyMetric] {
		if p.labels[AttrOutcome] == "final_failure" {
			t.Fatalf("a delivery with no queued time recorded a latency: %v", p)
		}
	}
}

func TestWebhookMetrics_ObjectLabelsAddTheSubscriptionsID(t *testing.T) {
	p, reader := areaProviders(t)
	m := NewWebhookMetrics(p, nil, true, discardLogger())
	m.Delivery(context.Background(), "acme", "wh-1", WebhookDelivered, 1, time.Time{}, time.Time{})
	points := collectGC(t, reader)
	if got := value(t, points, WebhookDeliveriesMetric, map[string]string{AttrNamespace: "acme", AttrOutcome: "delivered", AttrWebhookID: "wh-1"}); got != 1 {
		t.Fatalf("delivered = %v", got)
	}
}

func TestTriggerMetrics_ACanceledCallerStillRoutesItsRecord(t *testing.T) {
	p, reader := areaProviders(t)
	route := func(ctx context.Context, _ string) (*Providers, bool) { return p, ctx.Err() == nil }
	m := NewTriggerMetrics(p, route, discardLogger())
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	m.Invocation(ctx, "acme", TriggerUnavailable)
	if got := value(t, collectGC(t, reader), TriggerInvocationsMetric, map[string]string{AttrNamespace: "acme", AttrOutcome: "unavailable"}); got != 1 {
		t.Fatalf("unavailable = %v, want the record routed", got)
	}
}

func TestWebhookMetrics_NotQueuedCountsItsClosedReason(t *testing.T) {
	p, reader := areaProviders(t)
	m := NewWebhookMetrics(p, nil, false, discardLogger())
	m.NotQueued(context.Background(), "acme", webhooks.NotQueuedNotStarted)
	m.NotQueued(context.Background(), "acme", "something new")
	points := collectGC(t, reader)
	if got := value(t, points, WebhookNotQueuedMetric, map[string]string{AttrNamespace: "acme", AttrNotQueuedReason: "not_started"}); got != 1 {
		t.Fatalf("not started = %v", got)
	}
	if got := value(t, points, WebhookNotQueuedMetric, map[string]string{AttrNamespace: "acme", AttrNotQueuedReason: "unknown"}); got != 1 {
		t.Fatalf("a reason outside the set = %v, want counted as unknown", got)
	}
}
