package api

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"

	"connectrpc.com/connect"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/dsl"
	"github.com/orkstack/orkstack/internal/engine"
)

// The callback door is plain JSON over HTTP for the reason the trigger invoke
// route is: the caller is an arbitrary external system that must not need a
// Connect client. It resolves ONLY an await declaring `callback: true`, with
// that await's bearer token.
const (
	AwaitCompletePath = "/v1/runs/{run_id}/awaits/{task_id}"
	AwaitRejectPath   = "/v1/runs/{run_id}/awaits/{task_id}/reject"
)

// awaitRejectBodyLimit bounds a rejection envelope: one reason of at most
// awaitReasonMaxBytes, JSON-escaped.
const awaitRejectBodyLimit = 4096

// NewAwaitHandler serves both routes against svc.
func NewAwaitHandler(svc *Service) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST "+AwaitCompletePath, func(w http.ResponseWriter, r *http.Request) {
		resolveAwaitCallback(w, r, svc, false)
	})
	mux.HandleFunc("POST "+AwaitRejectPath, func(w http.ResponseWriter, r *http.Request) {
		resolveAwaitCallback(w, r, svc, true)
	})
	return mux
}

// resolveAwaitCallback authenticates one caller and delivers one resolution.
//
// THE LOAD-BEARING INVARIANT, the trigger door's: the token is the
// capability. It resolves exactly the await it was minted for — the run and
// task in the path are checked against it — and the namespace it acts in is
// the run's own, never the request's. No query, header or body field selects
// anything.
//
// The door queues nothing, retries nothing and keeps no record of what it
// delivered: it answers with the outcome of one signal, and a refused or
// failed request leaves nothing to deliver later.
func resolveAwaitCallback(w http.ResponseWriter, r *http.Request, svc *Service, reject bool) {
	ctx := r.Context()
	// 1. The token is read before any lookup: an unauthorized caller must not
	//    be able to probe a run at all.
	token, ok := bearerToken(r.Header.Get("Authorization"))
	if !ok {
		writeTriggerError(w, http.StatusUnauthorized, "unauthenticated",
			"an Authorization: Bearer <token> header is required")
		return
	}
	runID, taskID := r.PathValue("run_id"), r.PathValue("task_id")
	notFound := fmt.Sprintf("run %q has no callback await %q", runID, taskID)

	// 2. A task id that is not a slug names nothing.
	if err := dsl.ValidSlug("task id", taskID); err != nil || runID == "" {
		writeTriggerError(w, http.StatusNotFound, "not_found", notFound)
		return
	}

	// 3. What the run was started with, from its own start event: the pinned
	//    definition and the plan that holds the token's digest.
	binding, err := svc.locate(ctx, runID)
	if err != nil {
		writeTriggerError(w, http.StatusNotFound, "not_found", notFound)
		return
	}
	if binding.Reader == nil {
		writeTriggerError(w, http.StatusServiceUnavailable, "unavailable", "no run reader configured")
		return
	}
	start, err := binding.Reader.RunStart(ctx, runID)
	if err != nil {
		if errors.Is(err, engine.ErrRunNotFound) {
			writeTriggerError(w, http.StatusNotFound, "not_found", notFound)
			return
		}
		writeTriggerError(w, http.StatusServiceUnavailable, "unavailable", err.Error())
		return
	}

	// 4. A callback await, with a credential on the plan.
	callback := false
	for _, task := range start.Orkflow.GetTasks() {
		if task.GetId() == taskID && task.GetAwait().GetCallback() {
			callback = true
		}
	}
	cred, minted := start.Plan.Awaits[taskID]
	if !callback || !minted {
		writeTriggerError(w, http.StatusNotFound, "not_found", notFound)
		return
	}

	// 5. A wrong token on a real await (401) and an unknown await (404) are
	//    distinguishable, on the trigger door's reasoning: run ids and task
	//    ids are not confidential, tokens are.
	if !verifyAwaitToken(cred, token) {
		writeTriggerError(w, http.StatusUnauthorized, "unauthenticated",
			"the bearer token does not authenticate this await")
		return
	}

	// 6. The body, bounded. A completion's bytes are kept verbatim.
	var (
		values []byte
		reason string
	)
	if reject {
		reason, err = awaitRejectReason(w, r)
		if err != nil {
			writeTriggerError(w, http.StatusBadRequest, "invalid_argument", err.Error())
			return
		}
	} else {
		limit := start.Plan.Limits.Proto().GetValuesMaxBytes()
		values, err = io.ReadAll(http.MaxBytesReader(w, r.Body, limit))
		if err != nil {
			var tooLarge *http.MaxBytesError
			if errors.As(err, &tooLarge) {
				writeTriggerError(w, http.StatusRequestEntityTooLarge, "invalid_argument",
					fmt.Sprintf("values: the values are over the %d-byte limit", limit))
				return
			}
			writeTriggerError(w, http.StatusBadRequest, "invalid_argument",
				fmt.Sprintf("read request body: %v", err))
			return
		}
		if len(bytes.TrimSpace(values)) == 0 {
			values = []byte("{}")
		}
	}

	// 7. The run, from the read model, WITHOUT a namespace check: the
	//    namespace a callback acts in is the run's own.
	run, err := svc.LookupRun(ctx, runID)
	if err != nil {
		if connect.CodeOf(err) == connect.CodeNotFound {
			writeTriggerError(w, http.StatusNotFound, "not_found", notFound)
			return
		}
		status, code := awaitDoorStatus(err)
		writeTriggerError(w, status, code, err.Error())
		return
	}

	// 8. One core, shared with ResolveAwait.
	response, err := svc.resolveAwait(ctx, awaitResolution{
		binding:   binding,
		run:       run,
		start:     start,
		taskID:    taskID,
		reject:    reject,
		reason:    reason,
		values:    values,
		via:       orkstackv1.AwaitResolvedVia_AWAIT_RESOLVED_VIA_CALLBACK,
		principal: boundOriginAssertion(r.Header.Get(AuditPrincipalHeader)),
	})
	if err != nil {
		status, code := awaitDoorStatus(err)
		writeTriggerError(w, status, code, connectMessage(err))
		return
	}

	// 9. 202: durably recorded in the run's history, not yet applied. The
	//    body never carries the run — a token holder learns the coordinates
	//    it already had and the digest of what it sent.
	encoded, err := MarshalJSON(response)
	if err != nil {
		writeTriggerError(w, http.StatusInternalServerError, "internal", err.Error())
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusAccepted)
	_, _ = w.Write(append(encoded, '\n'))
}

// awaitRejectReason reads the rejection envelope: exactly {"reason": "..."}.
func awaitRejectReason(w http.ResponseWriter, r *http.Request) (string, error) {
	raw, err := io.ReadAll(http.MaxBytesReader(w, r.Body, awaitRejectBodyLimit))
	if err != nil {
		return "", fmt.Errorf("read request body: %w", err)
	}
	if !json.Valid(raw) {
		return "", errors.New(`the request body must be one JSON object: {"reason": "..."}`)
	}
	var envelope struct {
		Reason *string `json:"reason"`
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&envelope); err != nil {
		return "", fmt.Errorf(`the request body must be {"reason": "..."}: %v`, err)
	}
	if envelope.Reason == nil {
		return "", errors.New(`reason is required: {"reason": "..."}`)
	}
	return *envelope.Reason, nil
}

// awaitDoorStatus maps resolveAwait's Connect code onto the HTTP status and
// error code the callback door answers with — the sibling of
// triggerSubmitStatus, with FailedPrecondition as 409: the await is not in a
// state that accepts this resolution.
func awaitDoorStatus(err error) (int, string) {
	switch code := connect.CodeOf(err); code {
	case connect.CodeInvalidArgument:
		return http.StatusBadRequest, code.String()
	case connect.CodeNotFound:
		return http.StatusNotFound, code.String()
	case connect.CodeFailedPrecondition:
		return http.StatusConflict, code.String()
	case connect.CodeUnavailable:
		return http.StatusServiceUnavailable, code.String()
	default:
		return http.StatusInternalServerError, "internal"
	}
}

// connectMessage is a Connect error's own message, without the code prefix
// connect.Error.Error() adds: the HTTP envelope carries the code separately.
func connectMessage(err error) string {
	var connectErr *connect.Error
	if errors.As(err, &connectErr) {
		return connectErr.Message()
	}
	return err.Error()
}
