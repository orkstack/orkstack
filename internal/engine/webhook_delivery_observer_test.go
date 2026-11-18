package engine_test

import (
	"context"
	"net/http"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/pkg/webhooks"
)

// recordingWebhookObserver keeps what the delivery activities told it.
type recordingWebhookObserver struct {
	mu         sync.Mutex
	attempts   []toldAttempt
	deliveries []toldDelivery
}

type toldAttempt struct {
	namespace, webhookID, outcome string
	status                        int
}

type toldDelivery struct {
	namespace, webhookID, outcome string
	attempts                      int
	queuedAt, settledAt           time.Time
}

func (r *recordingWebhookObserver) ObserveWebhookAttempt(_ context.Context, namespace, webhookID string, a webhooks.Attempt, outcome string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.attempts = append(r.attempts, toldAttempt{namespace, webhookID, outcome, a.StatusCode})
}

func (r *recordingWebhookObserver) ObserveWebhookDelivery(_ context.Context, namespace, webhookID, outcome string, attempts int, queuedAt, settledAt time.Time) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.deliveries = append(r.deliveries, toldDelivery{namespace, webhookID, outcome, attempts, queuedAt, settledAt})
}

func (r *recordingWebhookObserver) snapshot() ([]toldAttempt, []toldDelivery) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return slices.Clone(r.attempts), slices.Clone(r.deliveries)
}

// TestWebhookDelivery_TellsItsObserverEachAttemptAndTheSettledDelivery: a
// retried attempt is told as retry, the last as what ended the delivery, and
// the delivery once, with how many attempts it took and when it was queued.
func TestWebhookDelivery_TellsItsObserverEachAttemptAndTheSettledDelivery(t *testing.T) {
	for _, tc := range []struct {
		name          string
		statuses      []int
		wantAttempts  []string
		wantDelivered string
	}{
		{name: "delivered after a retry", statuses: []int{http.StatusBadGateway, http.StatusNoContent},
			wantAttempts: []string{engine.WebhookOutcomeRetry, engine.WebhookOutcomeDelivered}, wantDelivered: engine.WebhookOutcomeDelivered},
		{name: "rejected by the receiver", statuses: []int{http.StatusBadRequest},
			wantAttempts: []string{engine.WebhookOutcomeFinalFailure}, wantDelivered: engine.WebhookOutcomeFinalFailure},
		{name: "out of attempts", statuses: []int{http.StatusInternalServerError},
			wantAttempts:  []string{engine.WebhookOutcomeRetry, engine.WebhookOutcomeRetry, engine.WebhookOutcomeRetry, engine.WebhookOutcomeFinalFailure},
			wantDelivered: engine.WebhookOutcomeFinalFailure},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := newDeliveryHarness(t)
			receiver := newDeliveryReceiver(t, tc.statuses...)
			in, _ := h.queue(t, receiver.server.URL)
			h.run(t, in)

			attempts, deliveries := h.told.snapshot()
			var outcomes []string
			for _, a := range attempts {
				if a.namespace != "default" || a.webhookID != in.WebhookID {
					t.Fatalf("attempt told under %q/%q, want default/%s", a.namespace, a.webhookID, in.WebhookID)
				}
				outcomes = append(outcomes, a.outcome)
			}
			if !slices.Equal(outcomes, tc.wantAttempts) {
				t.Fatalf("attempt outcomes = %v, want %v", outcomes, tc.wantAttempts)
			}
			if len(deliveries) != 1 {
				t.Fatalf("deliveries told = %d, want 1", len(deliveries))
			}
			d := deliveries[0]
			if d.outcome != tc.wantDelivered || d.attempts != len(tc.wantAttempts) || d.namespace != "default" ||
				!d.queuedAt.Equal(in.Delivery.QueuedAt) || d.settledAt.IsZero() {
				t.Fatalf("delivery told = %+v, want %s after %d attempts, queued at %v", d, tc.wantDelivered, len(tc.wantAttempts), in.Delivery.QueuedAt)
			}
		})
	}
}

// TestWebhookDelivery_ARecordAlreadyThereIsNotToldAgain: a settle that
// finds its record already written — it wrote it on an earlier try — tells
// the observer nothing, so a delivery is counted once.
func TestWebhookDelivery_ARecordAlreadyThereIsNotToldAgain(t *testing.T) {
	h := newDeliveryHarness(t)
	h.recordExists = true
	receiver := newDeliveryReceiver(t, http.StatusNoContent)
	in, _ := h.queue(t, receiver.server.URL)
	h.env.ExecuteWorkflow(engine.WebhookDeliveryWorkflowType, in)
	if err := h.env.GetWorkflowError(); err != nil {
		t.Fatalf("the delivery workflow failed: %v", err)
	}
	attempts, deliveries := h.told.snapshot()
	if len(attempts) != 1 || len(deliveries) != 0 {
		t.Fatalf("told %d attempts and %d deliveries, want the attempt and no second count of the delivery", len(attempts), len(deliveries))
	}
}
