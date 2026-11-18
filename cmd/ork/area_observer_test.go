package main

import (
	"testing"

	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/internal/telemetry"
)

// TestWebhookObserver_OutcomesAreTheMetricsOwnWords: the adapter passes the
// engine's outcome through unchanged, so each must be the metric's word; one
// that drifted would be dropped as outside the metric contract.
func TestWebhookObserver_OutcomesAreTheMetricsOwnWords(t *testing.T) {
	for engineWord, metricWord := range map[string]string{
		engine.WebhookOutcomeDelivered:    telemetry.WebhookDelivered,
		engine.WebhookOutcomeRetry:        telemetry.WebhookRetry,
		engine.WebhookOutcomeFinalFailure: telemetry.WebhookFinalFailure,
	} {
		if engineWord != metricWord {
			t.Errorf("engine outcome %q is the metric's %q", engineWord, metricWord)
		}
	}
}
