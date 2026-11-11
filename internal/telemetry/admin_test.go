package telemetry

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/orkstack/orkstack/internal/testsupport/must"
)

func TestAdminMuxHealthz(t *testing.T) {
	mux := NewAdminMux(nil, Config{})
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, HealthzPath, nil))
	if rec.Code != http.StatusOK {
		t.Errorf("GET %s = %d, want 200", HealthzPath, rec.Code)
	}
}

func TestAdminMuxReadyz(t *testing.T) {
	p := &Providers{Ready: &Readiness{}}
	mux := NewAdminMux(p, Config{})

	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, ReadyzPath, nil))
	if rec.Code != http.StatusServiceUnavailable {
		t.Errorf("GET %s before Ready.Set = %d, want 503", ReadyzPath, rec.Code)
	}

	p.Ready.Set(true)
	rec = httptest.NewRecorder()
	mux.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, ReadyzPath, nil))
	if rec.Code != http.StatusOK {
		t.Errorf("GET %s after Ready.Set(true) = %d, want 200", ReadyzPath, rec.Code)
	}
}

func TestAdminMuxMetricsWithoutRegistry(t *testing.T) {
	mux := NewAdminMux(nil, Config{})
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, MetricsPath, nil))
	if rec.Code != http.StatusNotFound {
		t.Errorf("GET %s with nil Registry = %d, want 404", MetricsPath, rec.Code)
	}
}

func TestAdminMuxPprof(t *testing.T) {
	mux := NewAdminMux(nil, Config{})
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, PprofPath, nil))
	if rec.Code != http.StatusNotFound {
		t.Errorf("GET %s with PprofEnabled=false = %d, want 404", PprofPath, rec.Code)
	}

	mux = NewAdminMux(nil, Config{PprofEnabled: true})
	rec = httptest.NewRecorder()
	mux.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, PprofPath, nil))
	if rec.Code != http.StatusOK {
		t.Errorf("GET %s with PprofEnabled=true = %d, want 200", PprofPath, rec.Code)
	}
}

func TestStartAdminNoAddr(t *testing.T) {
	addr, stop, err := StartAdmin(Config{}, nil, slog.Default())
	must.OKf(t, err, "StartAdmin")
	if addr != "" {
		t.Errorf("addr = %q, want empty for no AdminAddr", addr)
	}
	if err := stop(context.Background()); err != nil {
		t.Errorf("no-op stop returned %v", err)
	}
}

// TestStartAdminNilLoggerIsSafe confirms a nil logger — a caller that never
// bothered constructing one, or one that always passes its own possibly-nil
// field straight through — defaults to slog.Default() instead of panicking on
// the Info call inside StartAdmin (and the same nil check covers the
// goroutine's own Error call if the listener stops unexpectedly).
func TestStartAdminNilLoggerIsSafe(t *testing.T) {
	cfg := Config{AdminAddr: "127.0.0.1:0"}
	addr, stop, err := StartAdmin(cfg, nil, nil)
	must.OKf(t, err, "StartAdmin")
	defer stop(context.Background())
	if addr == "" {
		t.Fatal("addr is empty, want the bound address")
	}
}

func TestStartAdminListens(t *testing.T) {
	cfg := Config{AdminAddr: "127.0.0.1:0"}
	addr, stop, err := StartAdmin(cfg, nil, slog.Default())
	must.OKf(t, err, "StartAdmin")
	defer stop(context.Background())
	if addr == "" {
		t.Fatal("addr is empty, want the bound address")
	}

	resp, err := http.Get("http://" + addr + HealthzPath)
	must.OKf(t, err, "GET %s%s", addr, HealthzPath)
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("GET %s%s = %d, want 200", addr, HealthzPath, resp.StatusCode)
	}
}

// The two waits a client could make the admin listener hold, shrunk so a test
// outlasts them in a fraction of a second rather than in minutes. They go
// through startAdmin and newAdminServer, the ones StartAdmin uses, so what is
// proven is that those bounds close a connection, not a copy of them.
const (
	testHeaderTimeout = 150 * time.Millisecond
	testIdleTimeout   = 250 * time.Millisecond
	// closeDeadline is how long a test waits for a bound to close a
	// connection before it says the bound is missing.
	closeDeadline = 5 * time.Second
)

// requireClosedByBound reads r until the server closes conn, and fails when it
// is still open after closeDeadline (nothing bounds it) or closed sooner than
// bound after start (something else closed it). start is taken before what
// starts the server's own timer, so the client's elapsed time is never shorter
// than the server's however the host schedules either. It observes the close;
// it never asserts that nothing happened in a window.
func requireClosedByBound(t *testing.T, conn net.Conn, r io.Reader, start time.Time, bound time.Duration) {
	t.Helper()
	must.OK(t, conn.SetReadDeadline(time.Now().Add(closeDeadline)))
	_, err := io.Copy(io.Discard, r)
	var netErr net.Error
	if errors.As(err, &netErr) && netErr.Timeout() {
		t.Fatalf("the connection was still open %s later: nothing bounds it", closeDeadline)
	}
	if elapsed := time.Since(start); elapsed < bound {
		t.Fatalf("the connection closed after %s, before its %s bound", elapsed, bound)
	}
}

// A client cannot make the admin listener wait for headers that never end or
// hold a keep-alive connection it has left idle: each is closed once its
// bound has passed. (Nothing served here needs more, and the next scrape
// opens another connection.)
func TestStartAdmin_ClosesASlowHeaderAndAnIdleConnection(t *testing.T) {
	addr, stop, err := startAdmin(Config{AdminAddr: "127.0.0.1:0"}, nil, slog.New(slog.DiscardHandler),
		adminBounds{readHeaderTimeout: testHeaderTimeout, idleTimeout: testIdleTimeout, maxHeaderBytes: adminMaxHeaderBytes})
	must.OKf(t, err, "startAdmin")
	defer func() { _ = stop(context.Background()) }()
	dial := func(t *testing.T) net.Conn {
		t.Helper()
		conn, err := net.Dial("tcp", addr)
		must.OKf(t, err, "dial %s", addr)
		t.Cleanup(func() { _ = conn.Close() })
		return conn
	}

	t.Run("a request whose headers never end", func(t *testing.T) {
		start := time.Now()
		conn := dial(t)
		if _, err := fmt.Fprintf(conn, "GET %s HTTP/1.1\r\nHost: %s\r\nX-Slow: ", HealthzPath, addr); err != nil {
			t.Fatal(err)
		}
		requireClosedByBound(t, conn, conn, start, testHeaderTimeout)
	})

	t.Run("a keep-alive connection left idle", func(t *testing.T) {
		conn := dial(t)
		sent := time.Now()
		if _, err := fmt.Fprintf(conn, "GET %s HTTP/1.1\r\nHost: %s\r\n\r\n", HealthzPath, addr); err != nil {
			t.Fatal(err)
		}
		reader := bufio.NewReader(conn)
		response, err := http.ReadResponse(reader, nil)
		must.OKf(t, err, "read response")
		if _, err := io.Copy(io.Discard, response.Body); err != nil {
			t.Fatal(err)
		}
		_ = response.Body.Close()
		if response.StatusCode != http.StatusOK || response.Close {
			t.Fatalf("%s: %s, closing = %t; want 200 on a connection kept alive", HealthzPath, response.Status, response.Close)
		}
		requireClosedByBound(t, conn, reader, sent, testIdleTimeout)
	})
}

// The admin listener runs with every bound set, and with no whole-request or
// write deadline: a CPU profile or a trace is written only once the seconds
// it was asked for have passed, so either deadline would cut one. That a
// deadline is absent cannot be watched without waiting past it, so it is read
// off the server; that the bounds close connections is
// TestStartAdmin_ClosesASlowHeaderAndAnIdleConnection's, and the header-size
// bound TestStartAdmin_RefusesHeadersOverTheBound's.
func TestAdminServer_RunsWithEveryBoundSetAndNoWholeRequestDeadline(t *testing.T) {
	bounds := defaultAdminBounds
	if bounds.readHeaderTimeout <= 0 || bounds.idleTimeout <= 0 || bounds.maxHeaderBytes <= 0 {
		t.Errorf("defaultAdminBounds = %+v, want every bound set (zero is unbounded)", bounds)
	}
	server := newAdminServer(http.NotFoundHandler(), bounds)
	if server.ReadTimeout != 0 || server.WriteTimeout != 0 {
		t.Errorf("ReadTimeout = %s, WriteTimeout = %s; want neither", server.ReadTimeout, server.WriteTimeout)
	}
	// A bound left zero is the default, so no caller's slip unbounds the
	// listener.
	unset := newAdminServer(http.NotFoundHandler(), adminBounds{})
	if unset.ReadHeaderTimeout <= 0 || unset.IdleTimeout <= 0 || unset.MaxHeaderBytes <= 0 {
		t.Errorf("with no bounds given: ReadHeaderTimeout = %s, IdleTimeout = %s, MaxHeaderBytes = %d; want every bound set",
			unset.ReadHeaderTimeout, unset.IdleTimeout, unset.MaxHeaderBytes)
	}
}

func TestStartAdmin_RefusesHeadersOverTheBound(t *testing.T) {
	addr, stop, err := StartAdmin(Config{AdminAddr: "127.0.0.1:0"}, nil, slog.Default())
	must.OKf(t, err, "StartAdmin")
	defer func() { _ = stop(context.Background()) }()
	get := func(header string) int {
		request, err := http.NewRequest(http.MethodGet, "http://"+addr+HealthzPath, nil)
		must.OK(t, err)
		request.Header.Set("X-Pad", header)
		resp, err := http.DefaultClient.Do(request)
		must.OKf(t, err, "GET %s", HealthzPath)
		_ = resp.Body.Close()
		return resp.StatusCode
	}
	if code := get(strings.Repeat("a", 4<<10)); code != http.StatusOK {
		t.Errorf("a 4 KiB header: %d, want 200", code)
	}
	if code := get(strings.Repeat("a", 80<<10)); code != http.StatusRequestHeaderFieldsTooLarge {
		t.Errorf("an 80 KiB header: %d, want 431", code)
	}
}

// The admin listener answers only the Hosts its policy admits — the names
// every OrkStack listener admits, ORK_ADMIN_ADDR's own and those
// ORK_ADMIN_ALLOWED_HOSTS lists — so a DNS-rebinding page cannot read
// /metrics or a profile through it. A refusal is 403 naming the setting.
func TestStartAdmin_AnswersOnlyTheHostsItsPolicyAdmits(t *testing.T) {
	var logged strings.Builder
	addr, stop, err := StartAdmin(Config{AdminAddr: "127.0.0.1:0", AdminAllowedHosts: []string{"host.docker.internal"}},
		nil, slog.New(slog.NewTextHandler(&logged, nil)))
	must.OKf(t, err, "StartAdmin")
	defer func() { _ = stop(context.Background()) }()
	for host, want := range map[string]int{
		addr: http.StatusOK, "localhost:9464": http.StatusOK, "orker:9465": http.StatusOK,
		"host.docker.internal:9464": http.StatusOK, "evil.example": http.StatusForbidden,
		"prometheus.monitoring.svc:9464": http.StatusForbidden,
	} {
		request, err := http.NewRequest(http.MethodGet, "http://"+addr+HealthzPath, nil)
		must.OK(t, err)
		request.Host = host
		resp, err := http.DefaultClient.Do(request)
		must.OKf(t, err, "GET %s under Host %q", HealthzPath, host)
		body, _ := io.ReadAll(resp.Body)
		_ = resp.Body.Close()
		if resp.StatusCode != want {
			t.Errorf("GET %s under Host %q: %d, want %d", HealthzPath, host, resp.StatusCode, want)
		}
		if want == http.StatusForbidden && (!strings.Contains(string(body), AdminAllowedHostsVar) ||
			!strings.Contains(string(body), AdminAddrVar) || !strings.Contains(string(body), "admin listener")) {
			t.Errorf("the refusal of Host %q does not name the admin listener's settings: %q", host, body)
		}
	}
	if !strings.Contains(logged.String(), "listener=\"admin listener\"") {
		t.Errorf("no refusal logged for the admin listener:\n%s", logged.String())
	}
}

// ORK_ADMIN_ALLOWED_HOSTS is a comma-separated list; an entry that is not a
// name or a pattern stops the boot naming it, as the API listener's does.
func TestConfigFrom_ReadsTheAdminListenersAllowedHosts(t *testing.T) {
	cfg, err := configFrom(ServiceOrker, lookupOf(map[string]string{
		AdminAddrVar: "127.0.0.1:9465", AdminAllowedHostsVar: " host.docker.internal, ,*.monitoring.example ",
	}))
	must.OK(t, err)
	if got := strings.Join(cfg.AdminAllowedHosts, ","); got != "host.docker.internal,*.monitoring.example" {
		t.Errorf("AdminAllowedHosts = %q", got)
	}
	for _, entry := range []string{"https://host.docker.internal", "*.com"} {
		_, err := configFrom(ServiceOrker, lookupOf(map[string]string{AdminAddrVar: "127.0.0.1:9465", AdminAllowedHostsVar: entry}))
		if err == nil || !strings.Contains(err.Error(), AdminAllowedHostsVar) {
			t.Errorf("%s=%q: err = %v, want a refusal naming it", AdminAllowedHostsVar, entry, err)
		}
	}
}
