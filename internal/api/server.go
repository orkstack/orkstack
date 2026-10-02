package api

import (
	"net/http"
	"time"

	"connectrpc.com/connect"

	"github.com/orkstack/orkstack/gen/go/orkstack/v1/orkstackv1connect"
)

// DefaultHTTPAddr is the listen address when ORK_HTTP_ADDR is unset.
const DefaultHTTPAddr = ":8080"

// MuxOptions tunes the HTTP surface.
type MuxOptions struct {
	// AllowedOrigins lists Origin header patterns accepted by the WebSocket
	// bridge. Empty means same-origin requests only (coder/websocket default).
	AllowedOrigins []string

	// StatsCacheTTL is how long one stats answer is reused. The zero value
	// takes the documented default (ORK_STATS_CACHE_TTL, else
	// DefaultStatsCacheTTL); a negative value switches the cache off.
	//
	// This is the one place an unset TTL becomes a number, so nothing further
	// down can quietly re-enable an optimisation an operator turned off.
	StatsCacheTTL time.Duration
}

// NewMux mounts the Connect handlers and the WebSocket bridge.
func NewMux(svc *Service, opts MuxOptions) *http.ServeMux {
	mux := http.NewServeMux()
	// Every handler shares one interceptor chain: the audit interceptor sees
	// every unary call, records the audited writes, and passes everything else
	// through untouched (audit.go). HandlerOptions itself is unchanged, so its
	// other callers keep exactly the codec they had.
	handlerOptions := append(HandlerOptions(), connect.WithInterceptors(svc.auditInterceptor()))

	orkflowPath, orkflowHandler := orkstackv1connect.NewOrkflowServiceHandler(svc, handlerOptions...)
	mux.Handle(orkflowPath, orkflowHandler)

	runPath, runHandler := orkstackv1connect.NewRunServiceHandler(svc, handlerOptions...)
	mux.Handle(runPath, runHandler)

	namespacePath, namespaceHandler := orkstackv1connect.NewNamespaceServiceHandler(svc, handlerOptions...)
	mux.Handle(namespacePath, namespaceHandler)

	// ActivityService serves the catalog from engine configuration: an
	// activity is an image and which images exist is engine configuration,
	// so the catalog is served from here rather than gathered from Orkers
	// (§4.2, §6.5).
	activityPath, activityHandler := orkstackv1connect.NewActivityServiceHandler(svc, handlerOptions...)
	mux.Handle(activityPath, activityHandler)

	// System information: what this replica was built and configured as. It
	// is mounted like every other read surface — an operator behind a load
	// balancer asks whichever replica answers, and that is the point of the
	// RPC (see system.go).
	systemPath, systemHandler := orkstackv1connect.NewSystemServiceHandler(svc, handlerOptions...)
	mux.Handle(systemPath, systemHandler)

	// The stats surface is its own handler because it owns a cache. The TTL is
	// resolved HERE — the zero value means "the documented default" and only
	// this line knows that, so a handler built anywhere else takes what it is
	// given and cannot re-enable a switched-off cache.
	statsTTL := opts.StatsCacheTTL
	if statsTTL == 0 {
		statsTTL = StatsCacheTTLFromEnv()
	}
	statsPath, statsHandler := orkstackv1connect.NewStatsServiceHandler(
		NewStatsHandler(svc, statsTTL), handlerOptions...)
	mux.Handle(statsPath, statsHandler)

	webhookPath, webhookHandler := orkstackv1connect.NewWebhookServiceHandler(svc, handlerOptions...)
	mux.Handle(webhookPath, webhookHandler)

	// TriggerService manages inbound run starters — the mirror image of
	// WebhookService: a webhook is outbound and deployment-wide unless it
	// names a scope, a trigger is inbound and bound to one orkflow in one
	// namespace.
	triggerPath, triggerHandler := orkstackv1connect.NewTriggerServiceHandler(svc, handlerOptions...)
	mux.Handle(triggerPath, triggerHandler)

	// ScheduleService manages schedules: the third run starter, bound to one
	// orkflow in one namespace, whose every field lives in a Temporal
	// Schedule (orkflow-schedules).
	schedulePath, scheduleHandler := orkstackv1connect.NewScheduleServiceHandler(svc, handlerOptions...)
	mux.Handle(schedulePath, scheduleHandler)

	// AuditService reads the control-plane audit trail back from the shared
	// store. It is itself a read and is never recorded.
	auditPath, auditHandler := orkstackv1connect.NewAuditServiceHandler(svc, handlerOptions...)
	mux.Handle(auditPath, auditHandler)

	// The legacy webhook adapter is plain HTTP, outside Connect, so the
	// interceptor cannot see it: it records its two writes itself.
	mux.Handle(WebhookCollectionPath, NewWebhookHandler(svc.Webhooks(), svc.audit))
	mux.Handle(WebhookItemPath, NewWebhookHandler(svc.Webhooks(), svc.audit))

	// The invoke route is plain JSON so an arbitrary external caller — a CI
	// job, a cron host, one curl line — starts a run without a Connect client.
	mux.Handle(TriggerInvokePath, NewTriggerHandler(svc))

	// The callback door: the one other route family meant for callers
	// outside the operator's boundary. It resolves exactly the callback await
	// its bearer token was minted for (awaits_http.go).
	awaitHandler := NewAwaitHandler(svc)
	mux.Handle(AwaitCompletePath, awaitHandler)
	mux.Handle(AwaitRejectPath, awaitHandler)

	mux.Handle("/ws/runs/{id}", NewWebSocketBridge(svc, opts.AllowedOrigins))
	mux.Handle(TaskLogSocketPath, NewTaskLogSocket(svc, opts.AllowedOrigins))
	return mux
}

// NewServer returns an HTTP server serving both HTTP/1.1 and unencrypted
// HTTP/2 (h2c), so connect-go, connect-web, and WebSocket clients share a port.
func NewServer(addr string, handler http.Handler) *http.Server {
	if addr == "" {
		addr = DefaultHTTPAddr
	}
	protocols := new(http.Protocols)
	protocols.SetHTTP1(true)
	protocols.SetUnencryptedHTTP2(true)
	return &http.Server{
		Addr:      addr,
		Handler:   handler,
		Protocols: protocols,
	}
}
