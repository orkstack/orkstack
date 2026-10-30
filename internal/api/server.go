package api

import (
	"fmt"
	"net/http"
	"time"

	"connectrpc.com/connect"

	"github.com/orkstack/orkstack/gen/go/orkstack/v1/orkstackv1connect"
	"github.com/orkstack/orkstack/internal/webui"
)

// DefaultHTTPAddr is the listen address when ORK_HTTP_ADDR is unset.
const DefaultHTTPAddr = ":8080"

// DefaultUIEnabled is ORK_UI_ENABLED's default: the engine serves the web UI
// at / on its API listener (MuxOptions.UI, internal/webui).
const DefaultUIEnabled = true

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

	// UI, when set, answers every path no API route takes: the web UI
	// (internal/webui), mounted at / on this same listener so the browser
	// reaches the API and the sockets on the page's own origin. Nil leaves
	// the mux the API alone, where an unknown path is a 404.
	UI http.Handler
}

// NewMux mounts the Connect handlers, the plain-HTTP routes, the WebSocket
// bridge and, when opts.UI is set, the web UI at /.
//
// Every API route must lie under a path webui.Reserved claims for the API,
// so the UI can never take a path the API answers and a mistyped API path is
// a 404 rather than the app's page. A route outside them is a programming
// error that NewMux refuses the way http.ServeMux refuses a conflicting
// pattern: it panics as the route is mounted, in every test that builds a
// mux (TestNewMux_EveryRouteIsReservedFromTheUI).
func NewMux(svc *Service, opts MuxOptions) *http.ServeMux {
	serveMux := http.NewServeMux()
	mux := reservedMux{serveMux: serveMux}
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
	stats := NewStatsHandler(svc, statsTTL)
	statsPath, statsHandler := orkstackv1connect.NewStatsServiceHandler(stats, handlerOptions...)
	mux.Handle(statsPath, statsHandler)

	// CacheService reads and trims the memos cached tasks reuse. It shares
	// the stats handler's window resolver and response cache — no second TTL.
	cachePath, cacheHandler := orkstackv1connect.NewCacheServiceHandler(
		NewCacheHandler(svc, stats), handlerOptions...)
	mux.Handle(cachePath, cacheHandler)

	webhookPath, webhookHandler := orkstackv1connect.NewWebhookServiceHandler(svc, handlerOptions...)
	mux.Handle(webhookPath, webhookHandler)

	// TriggerService manages inbound run starters — the mirror image of
	// WebhookService: a webhook is outbound and deployment-wide unless it
	// names a scope, a trigger is inbound and bound to one orkflow in one
	// namespace.
	triggerPath, triggerHandler := orkstackv1connect.NewTriggerServiceHandler(svc, handlerOptions...)
	mux.Handle(triggerPath, triggerHandler)

	// AutomationService plans and exports a namespace's automation document.
	// It writes nothing: an apply is the client sending the plan's requests
	// to the services above.
	automationPath, automationHandler := orkstackv1connect.NewAutomationServiceHandler(svc, handlerOptions...)
	mux.Handle(automationPath, automationHandler)

	// ScheduleService manages schedules: the third run starter, bound to one
	// orkflow in one namespace, whose every field lives in a Temporal
	// Schedule (docs/ORKFLOW_SCHEDULES.md).
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

	// The UI takes what no route above takes. "/" is less specific than every
	// pattern above, so it never conflicts with one, and the UI answers the
	// reserved paths none of them matched with a 404 itself (webui.Handler).
	if opts.UI != nil {
		serveMux.Handle("/", opts.UI)
	}
	return serveMux
}

// reservedMux mounts API routes, each of which must be reserved from the UI.
// It offers Handle alone, so no route reaches the mux around the check.
type reservedMux struct{ serveMux *http.ServeMux }

func (m reservedMux) Handle(pattern string, handler http.Handler) {
	if !webui.Reserved(pattern) {
		panic(fmt.Sprintf("api: route %q is not reserved from the web UI (internal/webui Reserved)", pattern))
	}
	m.serveMux.Handle(pattern, handler)
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
