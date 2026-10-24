package telemetry

import (
	"context"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
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
	if err != nil {
		t.Fatalf("StartAdmin: %v", err)
	}
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
	if err != nil {
		t.Fatalf("StartAdmin: %v", err)
	}
	defer stop(context.Background())
	if addr == "" {
		t.Fatal("addr is empty, want the bound address")
	}
}

func TestStartAdminListens(t *testing.T) {
	cfg := Config{AdminAddr: "127.0.0.1:0"}
	addr, stop, err := StartAdmin(cfg, nil, slog.Default())
	if err != nil {
		t.Fatalf("StartAdmin: %v", err)
	}
	defer stop(context.Background())
	if addr == "" {
		t.Fatal("addr is empty, want the bound address")
	}

	resp, err := http.Get("http://" + addr + HealthzPath)
	if err != nil {
		t.Fatalf("GET %s%s: %v", addr, HealthzPath, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("GET %s%s = %d, want 200", addr, HealthzPath, resp.StatusCode)
	}
}
