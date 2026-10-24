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
)

// Paths on the admin listener.
const (
	HealthzPath = "/healthz"
	ReadyzPath  = "/readyz"
	MetricsPath = "/metrics"
	PprofPath   = "/debug/pprof/"
)

// adminReadHeaderTimeout bounds a slow client on the admin port; nothing
// served here needs more than a request line.
const adminReadHeaderTimeout = 5 * time.Second

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
	if logger == nil {
		logger = slog.Default()
	}
	if cfg.AdminAddr == "" {
		return "", func(context.Context) error { return nil }, nil
	}
	listener, err := net.Listen("tcp", cfg.AdminAddr)
	if err != nil {
		return "", nil, fmt.Errorf("%s=%q: %w", AdminAddrVar, cfg.AdminAddr, err)
	}
	server := &http.Server{
		Handler:           NewAdminMux(providers, cfg),
		ReadHeaderTimeout: adminReadHeaderTimeout,
	}
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
