package api

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"connectrpc.com/connect"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/pkg/artifacts"
	"github.com/orkstack/orkstack/pkg/webhooks"
)

// Webhook management is served as plain JSON over HTTP rather than as a
// Connect service, because proto is the single source of truth for
// cross-process types (AGENTS.md §12) and webhooks have not been added to it.
// The paths are versioned so a later proto-backed service can take them over
// without breaking a registered subscriber.
const (
	// WebhookCollectionPath registers and lists subscriptions.
	WebhookCollectionPath = "/v1/webhooks"
	// WebhookItemPath addresses one subscription.
	WebhookItemPath = "/v1/webhooks/{id}"

	// httpRequestBodyLimit bounds every plain-HTTP request body this package
	// reads: a webhook registration here, and a trigger invocation's input
	// object (triggers.go). It is one number rather than one per adapter, so
	// the surfaces cannot drift apart — and 1 MiB is already far above the
	// 256 KiB an input payload may occupy in a Temporal payload (AGENTS.md
	// §2.2), which is the real limit a large body meets next.
	httpRequestBodyLimit = 1 << 20
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
	// The scope, spelled as webhooks.Webhook spells it, so the envelope a GET
	// returns is what a POST accepts.
	Namespace   string `json:"namespace,omitempty"`
	OrkflowName string `json:"orkflowName,omitempty"`
	TaskID      string `json:"taskId,omitempty"`
}

// NewWebhookHandler serves webhook registration, listing and deletion against
// registry. The adapter is plain HTTP, outside Connect, so the audit
// interceptor cannot see it: its two writes record themselves through
// recorder, with the same actions the Connect WebhookService records and the
// HTTP route as the procedure. A nil recorder records nothing.
func NewWebhookHandler(registry *webhooks.Registry, recorder *AuditRecorder) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST "+WebhookCollectionPath, func(w http.ResponseWriter, r *http.Request) {
		status := &statusWriter{ResponseWriter: w}
		entry := registerWebhook(status, r, registry)
		recordLegacyWebhook(r, recorder, "POST "+WebhookCollectionPath, entry, status.status())
	})
	mux.HandleFunc("GET "+WebhookCollectionPath, func(w http.ResponseWriter, r *http.Request) {
		hooks, err := registry.ListContext(r.Context())
		if err != nil {
			writeWebhookError(w, http.StatusInternalServerError, "storage_error", err.Error())
			return
		}
		writeWebhookJSON(w, http.StatusOK, webhookListEnvelope{Webhooks: hooks})
	})
	mux.HandleFunc("GET "+WebhookItemPath, func(w http.ResponseWriter, r *http.Request) {
		hook, ok, err := registry.GetContext(r.Context(), r.PathValue("id"))
		if err != nil {
			writeWebhookError(w, http.StatusInternalServerError, "storage_error", err.Error())
			return
		}
		if !ok {
			writeWebhookError(w, http.StatusNotFound, "not_found",
				fmt.Sprintf("webhook %q not found", r.PathValue("id")))
			return
		}
		writeWebhookJSON(w, http.StatusOK, webhookEnvelope{Webhook: hook})
	})
	mux.HandleFunc("DELETE "+WebhookItemPath, func(w http.ResponseWriter, r *http.Request) {
		status := &statusWriter{ResponseWriter: w}
		deleteWebhook(status, r, registry)
		recordLegacyWebhook(r, recorder, "DELETE "+WebhookItemPath,
			auditEntry{Action: "webhook.delete", Target: r.PathValue("id")}, status.status())
	})
	return mux
}

func deleteWebhook(w http.ResponseWriter, r *http.Request, registry *webhooks.Registry) {
	ok, err := registry.DeleteContext(r.Context(), r.PathValue("id"))
	if err != nil {
		writeWebhookError(w, http.StatusInternalServerError, "storage_error", err.Error())
		return
	}
	if !ok {
		writeWebhookError(w, http.StatusNotFound, "not_found",
			fmt.Sprintf("webhook %q not found", r.PathValue("id")))
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// registerWebhook validates and stores one subscription. The response is the
// only time the signing secret is disclosed. It returns the audit entry for
// the call, which is built from the URL and the event names only — the secret
// never reaches it.
func registerWebhook(w http.ResponseWriter, r *http.Request, registry *webhooks.Registry) auditEntry {
	entry := auditEntry{Action: "webhook.register", Target: auditNoTarget}
	var body registerWebhookRequest
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, httpRequestBodyLimit)).Decode(&body); err != nil {
		writeWebhookError(w, http.StatusBadRequest, "invalid_argument",
			fmt.Sprintf("decode request body: %v", err))
		return entry
	}
	entry.Detail = webhookAuditDetail(body.URL, body.Events, body.Namespace, body.OrkflowName, body.TaskID)
	events := make([]webhooks.Event, 0, len(body.Events))
	for _, raw := range body.Events {
		event, err := webhooks.ParseEvent(raw)
		if err != nil {
			writeWebhookError(w, http.StatusBadRequest, "invalid_argument", err.Error())
			return entry
		}
		events = append(events, event)
	}
	if _, err := webhooks.ValidateURL(body.URL); err != nil {
		writeWebhookError(w, http.StatusBadRequest, "invalid_argument", err.Error())
		return entry
	}
	if err := webhooks.ValidateScope(body.Namespace, body.OrkflowName, body.TaskID, events); err != nil {
		writeWebhookError(w, http.StatusBadRequest, "invalid_argument", err.Error())
		return entry
	}
	hook, err := registry.RegisterContext(r.Context(), webhooks.Registration{
		URL:         body.URL,
		Events:      events,
		Secret:      body.Secret,
		Namespace:   body.Namespace,
		OrkflowName: body.OrkflowName,
		TaskID:      body.TaskID,
	})
	if err != nil {
		writeWebhookError(w, http.StatusInternalServerError, "storage_error", err.Error())
		return entry
	}
	entry.Target = hook.ID
	writeWebhookJSON(w, http.StatusCreated, webhookEnvelope{Webhook: hook})
	return entry
}

// statusWriter remembers the status a handler wrote, so the adapter can
// record whether its write succeeded without reaching into the handler.
type statusWriter struct {
	http.ResponseWriter
	code int
}

func (w *statusWriter) WriteHeader(code int) {
	if w.code == 0 {
		w.code = code
	}
	w.ResponseWriter.WriteHeader(code)
}

func (w *statusWriter) Write(p []byte) (int, error) {
	if w.code == 0 {
		w.code = http.StatusOK
	}
	return w.ResponseWriter.Write(p)
}

func (w *statusWriter) status() int {
	if w.code == 0 {
		return http.StatusOK
	}
	return w.code
}

// recordLegacyWebhook writes the adapter's audit record after the handler
// answered: the same record shape the interceptor writes, with the HTTP route
// as the procedure and the TCP peer as the source address.
func recordLegacyWebhook(r *http.Request, recorder *AuditRecorder, procedure string, entry auditEntry, status int) {
	if recorder == nil {
		return
	}
	rec := &orkstackv1.AuditRecord{
		Principal:     r.Header.Get(AuditPrincipalHeader),
		SourceAddress: r.RemoteAddr,
		Action:        entry.Action,
		Procedure:     procedure,
		Namespace:     entry.Namespace,
		Target:        entry.Target,
		Detail:        entry.Detail,
		Succeeded:     status >= 200 && status < 300,
	}
	if !rec.Succeeded {
		rec.ErrorCode = legacyWebhookErrorCode(status)
	}
	recorder.Record(r.Context(), rec)
}

// legacyWebhookErrorCode names a refused adapter call in the Connect code
// vocabulary every other record uses: the adapter's statuses are exactly
// these three, and each maps to what the Connect WebhookService returns for
// the same refusal.
func legacyWebhookErrorCode(status int) string {
	switch status {
	case http.StatusBadRequest, http.StatusRequestEntityTooLarge:
		return connect.CodeInvalidArgument.String()
	case http.StatusNotFound:
		return connect.CodeNotFound.String()
	default:
		return connect.CodeInternal.String()
	}
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

// WebhookNotifier adapts the engine's run-event seam to pkg/webhooks. It
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

// NotifyRunEvent queues one delivery per matching subscription, for a run
// transition or a task outcome. It never blocks and never fails the caller: a
// run's progress does not depend on a third party answering its HTTP.
func (n *WebhookNotifier) NotifyRunEvent(
	ctx context.Context,
	event *orkstackv1.Event,
	run *orkstackv1.Run,
) {
	// The engine already gates on this; the same check here is the boundary
	// where the event turns into wire JSON, and it must hold on its own.
	if n == nil || !engine.IsSubscribableEvent(event) {
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
		OrkflowName: run.GetOrkflowName(),
		// The one place an unset namespace becomes "default", so a
		// subscription scoped to "default" matches a run that named none.
		Namespace: artifacts.ResolveNamespace(run.GetNamespace()),
		// The event's status is the transition itself, which is what a
		// subscriber filtered on.
		Status:    strings.TrimPrefix(event.GetStatus().String(), "STATUS_"),
		Timestamp: timestamp,
		TaskID:    event.GetTaskId(),
		Attempt:   taskAttempt(run, event.GetTaskId()),
		Run:       encoded,
	})
}

// taskAttempt is the attempt TaskRun records for taskID: the attempt that
// produced a final outcome, and 0 — omitted from the body — for a task
// Temporal has not numbered yet, a run transition, or a task the run does not
// carry. It is never invented.
func taskAttempt(run *orkstackv1.Run, taskID string) int32 {
	if taskID == "" {
		return 0
	}
	for _, taskRun := range run.GetTaskRuns() {
		if taskRun.GetTaskId() == taskID {
			return taskRun.GetAttempt()
		}
	}
	return 0
}
