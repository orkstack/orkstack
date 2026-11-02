package telemetry

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"net/http/pprof"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/orkstack/orkstack/internal/hostpolicy"
)

// Paths on the admin listener.
const (
	HealthzPath = "/healthz"
	ReadyzPath  = "/readyz"
	MetricsPath = "/metrics"
	PprofPath   = "/debug/pprof/"
)

// The admin listener's bounds, the API listener's kinds (internal/api
// NewServerWith) at this port's own sizes. Nothing served here needs more
// than a request line, and no handler reads a body into memory.
const (
	// adminReadHeaderTimeout bounds a slow client on the admin port.
	adminReadHeaderTimeout = 5 * time.Second
	// adminIdleTimeout closes a keep-alive connection a scraper or a probe
	// left idle; the next scrape opens another.
	adminIdleTimeout = 2 * time.Minute
	// adminMaxHeaderBytes bounds a request's line and headers, as on the API
	// listener; one over it is 431.
	adminMaxHeaderBytes = 64 << 10
)

// AdminListener names the admin listener's settings for its Host policy
// (internal/hostpolicy): ORK_ADMIN_ADDR, and ORK_ADMIN_ALLOWED_HOSTS
// (telemetry.admin_allowed_hosts). It answers the names every OrkStack
// listener answers, its own address's host and the names that setting
// lists, so a page that rebinds a public name to it cannot read /metrics or
// a profile.
var AdminListener = hostpolicy.Listener{
	Name: "admin listener", AddrVar: AdminAddrVar, AllowedVar: AdminAllowedHostsVar,
	AllowedKey: "telemetry.admin_allowed_hosts",
}

// adminBounds are the limits newAdminServer puts on a client. StartAdmin
// serves within defaultAdminBounds; a test shrinks the two waits to watch a
// connection be closed in a fraction of a second rather than in minutes.
type adminBounds struct {
	readHeaderTimeout, idleTimeout time.Duration
	maxHeaderBytes                 int
}

// defaultAdminBounds are the bounds the admin listener runs with.
var defaultAdminBounds = adminBounds{
	readHeaderTimeout: adminReadHeaderTimeout,
	idleTimeout:       adminIdleTimeout,
	maxHeaderBytes:    adminMaxHeaderBytes,
}

// newAdminServer serves handler within bounds, a bound left zero being the
// listener's default, never unbounded. Like the API listener it sets no
// ReadTimeout and no WriteTimeout: a CPU profile or a trace is written only
// once the seconds it was asked for have passed, and either deadline would
// cut it.
func newAdminServer(handler http.Handler, bounds adminBounds) *http.Server {
	if bounds.readHeaderTimeout == 0 {
		bounds.readHeaderTimeout = defaultAdminBounds.readHeaderTimeout
	}
	if bounds.idleTimeout == 0 {
		bounds.idleTimeout = defaultAdminBounds.idleTimeout
	}
	if bounds.maxHeaderBytes == 0 {
		bounds.maxHeaderBytes = defaultAdminBounds.maxHeaderBytes
	}
	return &http.Server{
		Handler:           handler,
		ReadHeaderTimeout: bounds.readHeaderTimeout,
		IdleTimeout:       bounds.idleTimeout,
		MaxHeaderBytes:    bounds.maxHeaderBytes,
	}
}

// NewAdminMux serves the operational endpoints. It is a SEPARATE listener
// from the API on purpose: /metrics and /debug/pprof describe the process, not
// a tenant's runs, and must never be reachable through whatever exposes the
// API to browsers.
//
//   - /healthz answers 200 as soon as the listener is up: "the process is
//     alive". A restart loop is visible as a flapping healthz.
//   - /readyz answers 503 until the binary flips Ready, then 200: "this
//     replica serves". A load balancer uses it; scripts/stack.sh keeps its
//     real-RPC probe as the authority because "ready" here is the process's
//     own claim.
//   - /metrics is mounted only when Prometheus was asked for.
//   - /debug/pprof/ is mounted only under ORK_PPROF_ENABLED=true.
func NewAdminMux(providers *Providers, cfg Config) *http.ServeMux {
	mux := http.NewServeMux()
	mux.HandleFunc(HealthzPath, func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok\n"))
	})
	mux.HandleFunc(ReadyzPath, func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		if providers == nil || !providers.Ready.Ready() {
			w.WriteHeader(http.StatusServiceUnavailable)
			_, _ = w.Write([]byte("not ready\n"))
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ready\n"))
	})
	if providers != nil && providers.Registry != nil {
		mux.Handle(MetricsPath, promhttp.HandlerFor(providers.Registry, promhttp.HandlerOpts{}))
	}
	if cfg.PprofEnabled {
		mux.HandleFunc(PprofPath, pprof.Index)
		mux.HandleFunc(PprofPath+"cmdline", pprof.Cmdline)
		mux.HandleFunc(PprofPath+"profile", pprof.Profile)
		mux.HandleFunc(PprofPath+"symbol", pprof.Symbol)
		mux.HandleFunc(PprofPath+"trace", pprof.Trace)
	}
	return mux
}

// StartAdmin opens cfg.AdminAddr and serves NewAdminMux on it, returning the
// address it bound and a function that stops the server. With an empty
// address it opens nothing and returns ("", no-op stop, nil), which is the
// zero-config path.
//
// The address is bound synchronously, so an unbindable one is a boot error
// that names it — not a background goroutine's log line after the process
// already reported itself started. The bound address is returned rather than
// only logged because ":0" resolves to an ephemeral port a caller (or a test)
// needs back to reach the listener it just opened.
func StartAdmin(cfg Config, providers *Providers, logger *slog.Logger) (addr string, stop func(context.Context) error, err error) {
	return startAdmin(cfg, providers, logger, defaultAdminBounds)
}

// startAdmin is StartAdmin serving within bounds.
func startAdmin(cfg Config, providers *Providers, logger *slog.Logger, bounds adminBounds) (addr string, stop func(context.Context) error, err error) {
	if logger == nil {
		logger = slog.Default()
	}
	if cfg.AdminAddr == "" {
		return "", func(context.Context) error { return nil }, nil
	}
	hosts, err := hostpolicy.New(AdminListener, cfg.AdminAddr, cfg.AdminAllowedHosts)
	if err != nil {
		return "", nil, err
	}
	listener, err := net.Listen("tcp", cfg.AdminAddr)
	if err != nil {
		return "", nil, fmt.Errorf("%s=%q: %w", AdminAddrVar, cfg.AdminAddr, err)
	}
	server := newAdminServer(hosts.Guard(NewAdminMux(providers, cfg), logger), bounds)
	go func() {
		if err := server.Serve(listener); err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Error("admin listener stopped", "addr", cfg.AdminAddr, "error", err)
		}
	}()
	bound := listener.Addr().String()
	logger.Info("admin listener ready",
		"addr", bound,
		"metrics", cfg.PrometheusEnabled(),
		"pprof", cfg.PprofEnabled)
	return bound, server.Shutdown, nil
}
