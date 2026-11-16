package api

import (
	"context"
	"errors"
	"net/http"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/proto"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/gen/go/orkstack/v1/orkstackv1connect"
)

// The routes outside Connect are decided by the same rows as the RPCs they
// mirror (docs/ACCESS.md, "Every RPC and route"): the run socket by
// StreamEvents', the task log socket by StreamTaskLogs', and the legacy
// webhook routes by the deployment-scoped rows of the RPCs they stand for.
// Routes the middleware does not authenticate are exemptRoutes.

// exemptPatterns are the mux patterns the middleware serves without
// authenticating: the UI's static files, which hold no data and must load
// for a sign-in, and the two doors whose own bearer token is the
// capability. Every other pattern is authenticated: deny by default.
var exemptPatterns = map[string]string{
	"/":                 "the web UI's static files: the sign-in page must load, and they hold no data",
	TriggerInvokePath:   "a trigger's own bearer token is the capability",
	AwaitCompletePath:   "an await callback's own bearer token is the capability",
	AwaitRejectPath:     "an await callback's own bearer token is the capability",
	IssuerDiscoveryPath: "the task identity issuer's discovery document is public: relying parties read it with no credential",
	IssuerKeysPath:      "the task identity issuer's public keys: relying parties read them with no credential",
	IssuerBundlePath:    "the task identity issuer's SPIFFE bundle is its public keys: relying parties read it with no credential",
}

// routeRows maps every authenticated mux pattern that is not a Connect
// service to the procedure whose row decides it.
var routeRows = map[string]string{
	"/ws/runs/{id}":       orkstackv1connect.RunServiceStreamEventsProcedure,
	TaskLogSocketPath:     orkstackv1connect.RunServiceStreamTaskLogsProcedure,
	WebhookCollectionPath: legacyWebhookRoute,
	WebhookItemPath:       legacyWebhookRoute,
}

// legacyWebhookRoute marks the legacy webhook routes, decided at the
// deployment: their bodies are not read to find a scope.
const legacyWebhookRoute = "legacy-webhooks"

// authorizeRoute decides a plain-HTTP route by the row of procedure, for
// msg. It answers the refusal itself and reports false; in asserted mode it
// decides nothing.
func (s *Service) authorizeRoute(w http.ResponseWriter, r *http.Request, procedure string, msg proto.Message) (*http.Request, *admission, bool) {
	if !s.access.enabled() {
		return r, &admission{}, true
	}
	ctx, adm, err := s.admitCall(r.Context(), procedure, msg, r.RemoteAddr)
	if err != nil {
		status := http.StatusForbidden
		switch connect.CodeOf(err) {
		case connect.CodeNotFound:
			status = http.StatusNotFound
		case connect.CodeUnauthenticated:
			status = http.StatusUnauthorized
		case connect.CodeUnavailable:
			status = http.StatusServiceUnavailable
		case connect.CodeInvalidArgument:
			status = http.StatusBadRequest
		}
		http.Error(w, err.Error(), status)
		return r, adm, false
	}
	return r.WithContext(ctx), adm, true
}

// guardLegacyWebhooks decides the legacy webhook routes at the deployment:
// a read needs webhook.read, a write webhook.write and run.read.
func (s *Service) guardLegacyWebhooks(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		procedure := orkstackv1connect.WebhookServiceListWebhooksProcedure
		var msg proto.Message = &orkstackv1.ListWebhooksRequest{}
		switch r.Method {
		case http.MethodGet, http.MethodHead:
		case http.MethodDelete:
			procedure = orkstackv1connect.WebhookServiceDeleteWebhookProcedure
			msg = &orkstackv1.DeleteWebhookRequest{Id: r.PathValue("id")}
		default:
			procedure = orkstackv1connect.WebhookServiceRegisterWebhookProcedure
			msg = &orkstackv1.RegisterWebhookRequest{}
		}
		r, adm, ok := s.authorizeRoute(w, r, procedure, msg)
		if !ok {
			return
		}
		if adm.filterRows && s.access.enforcing() {
			// The legacy list answers every subscription, unfiltered: only a
			// caller who holds the read at the deployment gets it.
			http.Error(w, "PERMISSION_DENIED: the legacy webhook list needs webhook.read at the deployment; use WebhookService.ListWebhooks", http.StatusForbidden)
			return
		}
		next.ServeHTTP(w, r)
	})
}

// socketWithdrawn reports a socket closed because its caller's access was
// withdrawn while it was open.
func socketWithdrawn(ctx context.Context) bool {
	return errors.Is(context.Cause(ctx), errAccessWithdrawn)
}
