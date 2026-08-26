package api

import (
	"net/http"
	"time"

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
	handlerOptions := HandlerOptions()

	orkflowPath, orkflowHandler := orkstackv1connect.NewOrkflowServiceHandler(svc, handlerOptions...)
	mux.Handle(orkflowPath, orkflowHandler)

	runPath, runHandler := orkstackv1connect.NewRunServiceHandler(svc, handlerOptions...)
	mux.Handle(runPath, runHandler)

	namespacePath, namespaceHandler := orkstackv1connect.NewNamespaceServiceHandler(svc, handlerOptions...)
	mux.Handle(namespacePath, namespaceHandler)

	pluginPath, pluginHandler := orkstackv1connect.NewPluginServiceHandler(svc, handlerOptions...)
	mux.Handle(pluginPath, pluginHandler)

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

	mux.Handle(WebhookCollectionPath, NewWebhookHandler(svc.Webhooks()))
	mux.Handle(WebhookItemPath, NewWebhookHandler(svc.Webhooks()))

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
