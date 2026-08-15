package api

import (
	"net/http"

	"github.com/orkstack/orkstack/gen/go/orkstack/v1/orkstackv1connect"
)

// DefaultHTTPAddr is the listen address when ORK_HTTP_ADDR is unset.
const DefaultHTTPAddr = ":8080"

// MuxOptions tunes the HTTP surface.
type MuxOptions struct {
	// AllowedOrigins lists Origin header patterns accepted by the WebSocket
	// bridge. Empty means same-origin requests only (coder/websocket default).
	AllowedOrigins []string
}

// NewMux mounts the Connect handlers and the WebSocket bridge.
func NewMux(svc *Service, opts MuxOptions) *http.ServeMux {
	mux := http.NewServeMux()

	orkflowPath, orkflowHandler := orkstackv1connect.NewOrkflowServiceHandler(svc)
	mux.Handle(orkflowPath, orkflowHandler)

	runPath, runHandler := orkstackv1connect.NewRunServiceHandler(svc)
	mux.Handle(runPath, runHandler)

	pluginPath, pluginHandler := orkstackv1connect.NewPluginServiceHandler(svc)
	mux.Handle(pluginPath, pluginHandler)

	mux.Handle("/ws/runs/{id}", NewWebSocketBridge(svc, opts.AllowedOrigins))
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
