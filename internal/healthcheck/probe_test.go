package healthcheck_test

import (
	"bytes"
	"context"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/orkstack/orkstack/internal/healthcheck"
	"github.com/orkstack/orkstack/internal/telemetry"
)

// admin starts a stand-in for a daemon's admin listener serving handler, and
// returns the address a daemon would have been configured with.
func admin(t *testing.T, handler http.Handler) string {
	t.Helper()
	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)
	return server.Listener.Addr().String()
}

// readyz serves the one path the real admin listener serves readiness on, with
// the status and body given.
func readyz(status int, body string) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc(telemetry.ReadyzPath, func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(status)
		_, _ = w.Write([]byte(body))
	})
	return mux
}

func TestProbe_ReadyIs200(t *testing.T) {
	t.Parallel()
	addr := admin(t, readyz(http.StatusOK, "ready\n"))
	if err := healthcheck.Probe(context.Background(), addr); err != nil {
		t.Fatalf("Probe of a ready listener: %v", err)
	}
}

// TestProbe_AsksForTheReadinessPath: a plain GET of the path the admin listener
// serves readiness on, nothing else.
func TestProbe_AsksForTheReadinessPath(t *testing.T) {
	t.Parallel()
	var method, path atomic.Value
	addr := admin(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		method.Store(r.Method)
		path.Store(r.URL.RequestURI())
		w.WriteHeader(http.StatusOK)
	}))
	if err := healthcheck.Probe(context.Background(), addr); err != nil {
		t.Fatal(err)
	}
	if method.Load() != http.MethodGet || path.Load() != telemetry.ReadyzPath {
		t.Fatalf("probe sent %v %v, want GET %s", method.Load(), path.Load(), telemetry.ReadyzPath)
	}
}

func TestProbe_NotReadyIs503AndSaysSo(t *testing.T) {
	t.Parallel()
	addr := admin(t, readyz(http.StatusServiceUnavailable, "not ready\n"))
	err := healthcheck.Probe(context.Background(), addr)
	if err == nil {
		t.Fatal("Probe of a listener answering 503 reported ready")
	}
	for _, want := range []string{"503", "not ready", telemetry.ReadyzPath} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("reason %q does not mention %q", err, want)
		}
	}
	if strings.Contains(err.Error(), "\n") {
		t.Errorf("reason %q is more than one line", err)
	}
}

// TestProbe_OnlyA200IsReady: a redirect, an empty answer, a missing route and a
// server error are none of them "ready".
func TestProbe_OnlyA200IsReady(t *testing.T) {
	t.Parallel()
	for _, status := range []int{
		http.StatusNoContent, http.StatusMovedPermanently, http.StatusFound, http.StatusBadRequest,
		http.StatusNotFound, http.StatusInternalServerError, http.StatusBadGateway,
	} {
		addr := admin(t, readyz(status, ""))
		err := healthcheck.Probe(context.Background(), addr)
		if err == nil {
			t.Errorf("status %d was reported ready", status)
			continue
		}
		if !strings.Contains(err.Error(), http.StatusText(status)) {
			t.Errorf("status %d: reason %q does not say %q", status, err, http.StatusText(status))
		}
	}
}

// TestProbe_DoesNotFollowARedirect: a 200 that a redirect leads to is not this
// listener's readiness.
func TestProbe_DoesNotFollowARedirect(t *testing.T) {
	t.Parallel()
	mux := http.NewServeMux()
	mux.HandleFunc(telemetry.ReadyzPath, func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, "/elsewhere", http.StatusFound)
	})
	mux.HandleFunc("/elsewhere", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
	if err := healthcheck.Probe(context.Background(), admin(t, mux)); err == nil {
		t.Fatal("a redirect to a 200 was reported ready")
	}
}

func TestProbe_ConnectionRefused(t *testing.T) {
	t.Parallel()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := listener.Addr().String()
	_ = listener.Close()
	err = healthcheck.Probe(context.Background(), addr)
	if err == nil {
		t.Fatal("Probe of a closed port reported ready")
	}
	if !strings.Contains(err.Error(), addr) || !strings.Contains(err.Error(), "refused") {
		t.Errorf("reason %q should name %s and say the connection was refused", err, addr)
	}
	if strings.Contains(err.Error(), "\n") {
		t.Errorf("reason %q is more than one line", err)
	}
}

// TestProbe_AnUnresponsiveListenerFailsRatherThanHangs: a health check that
// waits as long as the listener does is a health check that never answers.
func TestProbe_AnUnresponsiveListenerFailsRatherThanHangs(t *testing.T) {
	t.Parallel()
	release := make(chan struct{})
	addr := admin(t, http.HandlerFunc(func(http.ResponseWriter, *http.Request) { <-release }))
	t.Cleanup(func() { close(release) })
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	started := time.Now()
	err := healthcheck.Probe(ctx, addr)
	if err == nil {
		t.Fatal("Probe of a listener that never answers reported ready")
	}
	if elapsed := time.Since(started); elapsed > 5*time.Second {
		t.Errorf("Probe took %s to give up on a 200ms deadline", elapsed)
	}
	// The caller's deadline ended the wait, not Timeout, and the error says
	// whose it was.
	if msg := err.Error(); strings.Contains(msg, "within "+healthcheck.Timeout.String()) || !strings.Contains(msg, "the caller's deadline") {
		t.Errorf("error %q, want it to name the caller's deadline rather than Timeout", msg)
	}
	if healthcheck.Timeout > 5*time.Second {
		t.Errorf("Timeout is %s: a check outlasting a typical probe interval answers nothing", healthcheck.Timeout)
	}
}

// TestProbe_FollowsReadiness: the same listener, asked twice, answers as its
// own readiness has changed.
func TestProbe_FollowsReadiness(t *testing.T) {
	t.Parallel()
	var ready atomic.Bool
	mux := http.NewServeMux()
	mux.HandleFunc(telemetry.ReadyzPath, func(w http.ResponseWriter, _ *http.Request) {
		if !ready.Load() {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
	})
	addr := admin(t, mux)
	if err := healthcheck.Probe(context.Background(), addr); err == nil {
		t.Fatal("ready before the listener said so")
	}
	ready.Store(true)
	if err := healthcheck.Probe(context.Background(), addr); err != nil {
		t.Fatalf("not ready after the listener said so: %v", err)
	}
	ready.Store(false)
	if err := healthcheck.Probe(context.Background(), addr); err == nil {
		t.Fatal("still ready after the listener went unready")
	}
}

func TestProbe_EmptyAddressNamesTheVariable(t *testing.T) {
	t.Parallel()
	for _, addr := range []string{"", "   "} {
		err := healthcheck.Probe(context.Background(), addr)
		if err == nil {
			t.Fatalf("Probe(%q) reported ready", addr)
		}
		if !strings.Contains(err.Error(), telemetry.AdminAddrVar) || !strings.Contains(err.Error(), "ORK_ADMIN_ADDR") {
			t.Errorf("Probe(%q) reason %q does not name ORK_ADMIN_ADDR", addr, err)
		}
	}
}

// TestReadyURL_ProbesLoopbackForAWildcardListener: a daemon that listens on
// every interface is asked on loopback, because a wildcard address is a place
// to listen and not a place to connect to.
func TestReadyURL_ProbesLoopbackForAWildcardListener(t *testing.T) {
	t.Parallel()
	tests := []struct {
		addr string
		want string
	}{
		{":9464", "http://127.0.0.1:9464/readyz"},
		{"0.0.0.0:9464", "http://127.0.0.1:9464/readyz"},
		{"[::]:9464", "http://[::1]:9464/readyz"},
		{"127.0.0.1:9464", "http://127.0.0.1:9464/readyz"},
		{"[::1]:9464", "http://[::1]:9464/readyz"},
		{"localhost:9465", "http://localhost:9465/readyz"},
		{"10.1.2.3:9464", "http://10.1.2.3:9464/readyz"},
		{"[fe80::1]:9464", "http://[fe80::1]:9464/readyz"},
		{"orker.internal:9465", "http://orker.internal:9465/readyz"},
		{"  :9464  ", "http://127.0.0.1:9464/readyz"},
	}
	for _, tc := range tests {
		got, err := healthcheck.ReadyURL(tc.addr)
		if err != nil {
			t.Errorf("ReadyURL(%q): %v", tc.addr, err)
			continue
		}
		if got != tc.want {
			t.Errorf("ReadyURL(%q) = %q, want %q", tc.addr, got, tc.want)
		}
	}
}

// TestReadyURL_RefusesWhatItCannotAsk: an address with no port, an unreadable
// one, and port 0 (a port chosen at random, which nobody can ask later) each
// fail with a reason naming the variable.
func TestReadyURL_RefusesWhatItCannotAsk(t *testing.T) {
	t.Parallel()
	for _, addr := range []string{
		"9464", "localhost", "[::]", "::1", ":", "host:", ":notaport", ":70000", ":-1",
		":0", "127.0.0.1:0", "http://127.0.0.1:9464", "127.0.0.1:9464/readyz", "a b:9464",
	} {
		got, err := healthcheck.ReadyURL(addr)
		if err == nil {
			t.Errorf("ReadyURL(%q) = %q, want an error", addr, got)
			continue
		}
		if !strings.Contains(err.Error(), "ORK_ADMIN_ADDR") {
			t.Errorf("ReadyURL(%q) reason %q does not name ORK_ADMIN_ADDR", addr, err)
		}
		if strings.Contains(err.Error(), "\n") {
			t.Errorf("ReadyURL(%q) reason is more than one line: %q", addr, err)
		}
	}
}

func runWith(addr string) (code int, stdout, stderr string) {
	var out, errOut bytes.Buffer
	getenv := func(key string) string {
		if key == telemetry.AdminAddrVar {
			return addr
		}
		return ""
	}
	code = healthcheck.Run(context.Background(), "orker", getenv, &out, &errOut)
	return code, out.String(), errOut.String()
}

// TestRun_ExitStatusAndOneLine: 0 with a line on stdout when ready; 1 with one
// line on stderr, prefixed with the process's name, when not.
func TestRun_ExitStatusAndOneLine(t *testing.T) {
	t.Parallel()
	t.Run("ready", func(t *testing.T) {
		t.Parallel()
		code, stdout, stderr := runWith(admin(t, readyz(http.StatusOK, "ready\n")))
		if code != 0 || stderr != "" {
			t.Fatalf("exit %d, stderr %q; want 0 and nothing on stderr", code, stderr)
		}
		if !strings.HasPrefix(stdout, "orker: ready") || strings.Count(stdout, "\n") != 1 {
			t.Errorf("stdout %q, want one line starting \"orker: ready\"", stdout)
		}
	})
	t.Run("not ready", func(t *testing.T) {
		t.Parallel()
		code, stdout, stderr := runWith(admin(t, readyz(http.StatusServiceUnavailable, "not ready\n")))
		if code != 1 || stdout != "" {
			t.Fatalf("exit %d, stdout %q; want 1 and nothing on stdout", code, stdout)
		}
		if !strings.HasPrefix(stderr, "orker: not ready: ") || !strings.Contains(stderr, "503") ||
			strings.Count(stderr, "\n") != 1 {
			t.Errorf("stderr %q, want one line starting \"orker: not ready: \" and naming 503", stderr)
		}
	})
	t.Run("nothing listening", func(t *testing.T) {
		t.Parallel()
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		addr := listener.Addr().String()
		_ = listener.Close()
		code, _, stderr := runWith(addr)
		if code != 1 || !strings.Contains(stderr, "refused") || strings.Count(stderr, "\n") != 1 {
			t.Fatalf("exit %d, stderr %q; want 1 and one line saying refused", code, stderr)
		}
	})
	t.Run("address unset", func(t *testing.T) {
		t.Parallel()
		code, stdout, stderr := runWith("")
		if code != 1 || stdout != "" {
			t.Fatalf("exit %d, stdout %q; want 1 and nothing on stdout", code, stdout)
		}
		if !strings.Contains(stderr, "ORK_ADMIN_ADDR") || strings.Count(stderr, "\n") != 1 {
			t.Errorf("stderr %q, want one line naming ORK_ADMIN_ADDR", stderr)
		}
	})
	t.Run("address is not host:port", func(t *testing.T) {
		t.Parallel()
		code, _, stderr := runWith("not an address")
		if code != 1 || !strings.Contains(stderr, "ORK_ADMIN_ADDR") {
			t.Fatalf("exit %d, stderr %q; want 1 naming ORK_ADMIN_ADDR", code, stderr)
		}
	})
}
