package api

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/types/known/structpb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/pkg/triggers"
)

// The inbound trigger invoke endpoint is served as plain JSON over HTTP rather
// than through Connect, because the caller is an arbitrary external system —
// a CI job, a cron host, a curl one-liner — that must not need a Connect
// client to start a run. The management RPCs are Connect (trigger_service.go);
// this is the one route the outside world posts to.
const (
	// TriggerInvokePath starts one run of the trigger's orkflow.
	TriggerInvokePath = "/v1/triggers/{id}"

	// IdempotencyKeyHeader carries an optional SubmitRun idempotency key. It
	// is passed straight through: the claim is namespace-scoped already
	// (submission.claimKey), so a trigger needs no dedup logic of its own.
	IdempotencyKeyHeader = "Idempotency-Key"

	// bearerPrefix is the only Authorization scheme this route accepts.
	bearerPrefix = "Bearer "
)

// NewTriggerHandler serves the invoke route against svc.
func NewTriggerHandler(svc *Service) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST "+TriggerInvokePath, func(w http.ResponseWriter, r *http.Request) {
		invokeTrigger(w, r, svc)
	})
	return mux
}

// invokeTrigger authenticates one caller and starts one run.
//
// THE LOAD-BEARING INVARIANT: the namespace, orkflow name and version come
// only from the stored record. No path, query, header or body field selects
// them, so a body key named "orkflow_name" is input data like any other key
// and a token can start exactly the orkflow its trigger was created for.
func invokeTrigger(w http.ResponseWriter, r *http.Request, svc *Service) {
	// 1. The Authorization header is read before any lookup: an unauthorized
	//    caller must not be able to probe the store at all.
	token, ok := bearerToken(r.Header.Get("Authorization"))
	if !ok {
		writeTriggerError(w, http.StatusUnauthorized, "unauthenticated",
			"an Authorization: Bearer <token> header is required")
		return
	}

	// 2. The record is resolved from the bare id. It must be read before the
	//    token can be judged, because the salt and digest to compare against
	//    live on it. An id that is not even shaped like one names nothing.
	id := r.PathValue("id")
	if !triggers.ValidID(id) {
		writeTriggerError(w, http.StatusNotFound, "not_found", fmt.Sprintf("trigger %q not found", id))
		return
	}
	record, found, err := svc.Triggers().Get(r.Context(), id)
	if err != nil {
		writeTriggerError(w, http.StatusInternalServerError, "storage_error", err.Error())
		return
	}
	if !found {
		writeTriggerError(w, http.StatusNotFound, "not_found", fmt.Sprintf("trigger %q not found", id))
		return
	}

	// 3. A wrong token on a real id (401) and an unknown id (404) are
	//    deliberately distinguishable: trigger ids are not confidential, only
	//    tokens are, the same way webhook ids are not treated as secrets.
	if !triggers.VerifySecret(record, token) {
		writeTriggerError(w, http.StatusUnauthorized, "unauthenticated",
			"the bearer token does not authenticate this trigger")
		return
	}

	// 4. The body IS the run's input object, not an envelope around it.
	input, err := triggerInput(w, r)
	if err != nil {
		writeTriggerError(w, http.StatusBadRequest, "invalid_argument", err.Error())
		return
	}

	// 5-6. One invoke is one SubmitRun. allow_empty_pool is always false: a
	//      pool with no live Orker fails the invoke loudly rather than
	//      starting a run that would never leave PENDING.
	response, err := svc.SubmitRun(r.Context(), connect.NewRequest(&orkstackv1.SubmitRunRequest{
		OrkflowName:    record.OrkflowName,
		OrkflowVersion: record.OrkflowVersion,
		Namespace:      record.Namespace,
		Input:          input,
		IdempotencyKey: r.Header.Get(IdempotencyKeyHeader),
		AllowEmptyPool: false,
	}))
	if err != nil {
		status, code := triggerSubmitStatus(err)
		writeTriggerError(w, status, code, err.Error())
		return
	}

	// 7. The same envelope `orkctl run submit --json` and the UI already read,
	//    in the short-enum dialect every other JSON surface uses (json.go).
	encoded, err := MarshalJSON(response.Msg)
	if err != nil {
		writeTriggerError(w, http.StatusInternalServerError, "internal", err.Error())
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	_, _ = w.Write(append(encoded, '\n'))
}

// triggerInput reads the request body as the run's input.
//
// The read is bounded by the same limit the webhook adapter bounds its own
// bodies with (httpRequestBodyLimit): one number for every plain-HTTP surface
// on this port. An empty body means no input, and the orkflow's declared
// defaults then apply exactly as they do for any other submission.
func triggerInput(w http.ResponseWriter, r *http.Request) (*orkstackv1.Payload, error) {
	raw, err := io.ReadAll(http.MaxBytesReader(w, r.Body, httpRequestBodyLimit))
	if err != nil {
		return nil, fmt.Errorf("read request body: %w", err)
	}
	body := bytes.TrimSpace(raw)
	if len(body) == 0 {
		return nil, nil
	}
	// The whole body must be ONE JSON value. Checked before decoding because
	// the decoder reads the first value and would silently ignore whatever
	// followed it, accepting `{"a":1}{"b":2}` as `{"a":1}` — a caller sending
	// two objects has a bug and has to hear about it.
	if !json.Valid(body) {
		return nil, fmt.Errorf("request body is not valid JSON")
	}
	// A JSON array, string or number is a well-formed document that is not an
	// input object. Saying so beats protojson's "unexpected token", which
	// describes the parse rather than the contract.
	if body[0] != '{' {
		return nil, fmt.Errorf("request body must be a JSON object holding the run's input")
	}
	data := &structpb.Struct{}
	if err := UnmarshalJSON(body, data); err != nil {
		return nil, fmt.Errorf("decode request body: %w", err)
	}
	return &orkstackv1.Payload{Data: data}, nil
}

// bearerToken reads "Authorization: Bearer <token>". A missing header, another
// scheme, or an empty token are all the same answer: this request presented
// no trigger token.
func bearerToken(header string) (string, bool) {
	if len(header) < len(bearerPrefix) || !strings.EqualFold(header[:len(bearerPrefix)], bearerPrefix) {
		return "", false
	}
	token := strings.TrimSpace(header[len(bearerPrefix):])
	return token, token != ""
}

// triggerSubmitStatus maps SubmitRun's Connect code onto the HTTP status and
// error code this route answers with. The adapter adds no classification of
// its own: every row below is SubmitRun's own refusal carried through.
//
// The 400, 404 and 412 rows are refused by SubmitRun's preparation before a
// run id is minted, so a refused invoke leaves nothing behind — no id burnt,
// no claim taken, no run recorded.
func triggerSubmitStatus(err error) (int, string) {
	switch code := connect.CodeOf(err); code {
	case connect.CodeInvalidArgument:
		return http.StatusBadRequest, code.String()
	case connect.CodeNotFound:
		return http.StatusNotFound, code.String()
	case connect.CodeFailedPrecondition:
		return http.StatusPreconditionFailed, code.String()
	case connect.CodeUnavailable:
		return http.StatusServiceUnavailable, code.String()
	default:
		return http.StatusInternalServerError, "internal"
	}
}

// writeTriggerError answers with the error envelope the webhook adapter
// already uses (webhookErrorEnvelope), so a client renders every plain-HTTP
// failure on this port the same way.
func writeTriggerError(w http.ResponseWriter, status int, code, message string) {
	writeWebhookError(w, status, code, message)
}
