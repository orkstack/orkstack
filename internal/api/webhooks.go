package api

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"time"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/pkg/webhooks"
)

// Webhook management is served as plain JSON over HTTP rather than as a
// Connect service, because proto is the single source of truth for
// cross-process types (AGENTS.md §1.1) and this milestone does not change it.
// The paths are versioned so a later proto-backed service can take them over
// without breaking a registered subscriber.
const (
	// WebhookCollectionPath registers and lists subscriptions.
	WebhookCollectionPath = "/v1/webhooks"
	// WebhookItemPath addresses one subscription.
	WebhookItemPath = "/v1/webhooks/{id}"
)

// webhookEnvelope is the response body for a single subscription.
type webhookEnvelope struct {
	Webhook webhooks.Webhook `json:"webhook"`
}

// webhookListEnvelope is the response body for a collection.
type webhookListEnvelope struct {
	Webhooks []webhooks.Webhook `json:"webhooks"`
}

// webhookErrorEnvelope is the response body for a failure. It mirrors the
// shape of the Error message the rest of the API uses, so a client renders
// both the same way.
type webhookErrorEnvelope struct {
	Error webhookError `json:"error"`
}

type webhookError struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

// registerWebhookRequest is the POST body. Events are read as plain strings so
// an unknown name is reported as a 400 naming the valid values, rather than
// silently decoding to an event that matches nothing.
type registerWebhookRequest struct {
	URL    string   `json:"url"`
	Events []string `json:"events,omitempty"`
	Secret string   `json:"secret,omitempty"`
}

// NewWebhookHandler serves webhook registration, listing and deletion against
// registry.
func NewWebhookHandler(registry *webhooks.Registry) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST "+WebhookCollectionPath, func(w http.ResponseWriter, r *http.Request) {
		registerWebhook(w, r, registry)
	})
	mux.HandleFunc("GET "+WebhookCollectionPath, func(w http.ResponseWriter, _ *http.Request) {
		writeWebhookJSON(w, http.StatusOK, webhookListEnvelope{Webhooks: registry.List()})
	})
	mux.HandleFunc("GET "+WebhookItemPath, func(w http.ResponseWriter, r *http.Request) {
		hook, ok := registry.Get(r.PathValue("id"))
		if !ok {
			writeWebhookError(w, http.StatusNotFound, "not_found",
				fmt.Sprintf("webhook %q not found", r.PathValue("id")))
			return
		}
		writeWebhookJSON(w, http.StatusOK, webhookEnvelope{Webhook: hook})
	})
	mux.HandleFunc("DELETE "+WebhookItemPath, func(w http.ResponseWriter, r *http.Request) {
		if !registry.Delete(r.PathValue("id")) {
			writeWebhookError(w, http.StatusNotFound, "not_found",
				fmt.Sprintf("webhook %q not found", r.PathValue("id")))
			return
		}
		w.WriteHeader(http.StatusNoContent)
	})
	return mux
}

// registerWebhook validates and stores one subscription. The response is the
// only time the signing secret is disclosed.
func registerWebhook(w http.ResponseWriter, r *http.Request, registry *webhooks.Registry) {
	var body registerWebhookRequest
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20)).Decode(&body); err != nil {
		writeWebhookError(w, http.StatusBadRequest, "invalid_argument",
			fmt.Sprintf("decode request body: %v", err))
		return
	}
	events := make([]webhooks.Event, 0, len(body.Events))
	for _, raw := range body.Events {
		event, err := webhooks.ParseEvent(raw)
		if err != nil {
			writeWebhookError(w, http.StatusBadRequest, "invalid_argument", err.Error())
			return
		}
		events = append(events, event)
	}
	hook, err := registry.Register(webhooks.Registration{
		URL:    body.URL,
		Events: events,
		Secret: body.Secret,
	})
	if err != nil {
		writeWebhookError(w, http.StatusBadRequest, "invalid_argument", err.Error())
		return
	}
	writeWebhookJSON(w, http.StatusCreated, webhookEnvelope{Webhook: hook})
}

func writeWebhookJSON(w http.ResponseWriter, status int, body any) {
	encoded, err := json.Marshal(body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_, _ = w.Write(append(encoded, '\n'))
}

func writeWebhookError(w http.ResponseWriter, status int, code, message string) {
	writeWebhookJSON(w, status, webhookErrorEnvelope{Error: webhookError{Code: code, Message: message}})
}

// WebhookNotifier adapts the engine's run-transition seam to pkg/webhooks. It
// lives here because it is the point where a Run becomes wire JSON, and this
// package owns that dialect (json.go): a subscriber reads "SUCCEEDED" in a
// delivery exactly as it does in every other OrkStack response.
type WebhookNotifier struct {
	notifier *webhooks.Notifier
	logger   *slog.Logger
	now      func() time.Time
}

// Compile-time proof that the adapter satisfies the engine's seam.
var _ engine.RunNotifier = (*WebhookNotifier)(nil)

// NewWebhookNotifier wraps a fan-out notifier. A nil notifier yields a nil
// adapter, which every caller treats as "webhooks are switched off".
func NewWebhookNotifier(notifier *webhooks.Notifier, logger *slog.Logger) *WebhookNotifier {
	if notifier == nil {
		return nil
	}
	if logger == nil {
		logger = slog.New(slog.DiscardHandler)
	}
	return &WebhookNotifier{notifier: notifier, logger: logger, now: time.Now}
}

// NotifyRunTransition queues one delivery per matching subscription. It never
// blocks and never fails the caller: a run's progress does not depend on a
// third party answering its HTTP.
func (n *WebhookNotifier) NotifyRunTransition(
	ctx context.Context,
	event *orkstackv1.Event,
	run *orkstackv1.Run,
) {
	if n == nil || !engine.IsRunTransitionEvent(event) {
		return
	}
	transition, err := webhooks.ParseEvent(event.GetType().String())
	if err != nil {
		n.logger.Error("webhook transition", "run_id", event.GetRunId(), "error", err)
		return
	}
	encoded, err := MarshalJSON(run)
	if err != nil {
		n.logger.Error("webhook encode run", "run_id", event.GetRunId(), "error", err)
		return
	}

	timestamp := n.now().UTC()
	if ts := event.GetTimestamp(); ts.IsValid() {
		timestamp = ts.AsTime().UTC()
	}
	n.notifier.Notify(ctx, webhooks.Delivery{
		Event:       transition,
		RunID:       event.GetRunId(),
		OrkflowName: run.GetWorkflowName(),
		// The event's status is the transition itself, which is what a
		// subscriber filtered on.
		Status:    strings.TrimPrefix(event.GetStatus().String(), "STATUS_"),
		Timestamp: timestamp,
		Run:       encoded,
	})
}
