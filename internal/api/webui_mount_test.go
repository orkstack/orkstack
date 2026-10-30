package api_test

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"testing/fstest"

	"github.com/orkstack/orkstack/internal/api"
	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/internal/webui"
)

const uiPage = "<!doctype html><title>OrkStack</title>"

func uiMux(t *testing.T, ui http.Handler) *httptest.Server {
	t.Helper()
	svc := api.NewService(api.Config{Runs: engine.NewRunStore(), Bus: engine.NewEventBus()})
	server := httptest.NewServer(api.NewMux(svc, api.MuxOptions{UI: ui}))
	t.Cleanup(server.Close)
	return server
}

func fetch(t *testing.T, method, url, contentType, body string) (int, string) {
	t.Helper()
	req, err := http.NewRequest(method, url, strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	if contentType != "" {
		req.Header.Set("Content-Type", contentType)
	}
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()
	b, err := io.ReadAll(res.Body)
	if err != nil {
		t.Fatal(err)
	}
	return res.StatusCode, string(b)
}

// One listener, one origin: the UI answers / and its client routes, and the
// API's own routes still answer as the API — a Connect call, a plain-HTTP
// route, the WebSocket bridge — never as the app.
func TestNewMux_ServesTheUIBesideTheAPI(t *testing.T) {
	server := uiMux(t, webui.Handler(fstest.MapFS{"index.html": {Data: []byte(uiPage)}}))

	for _, path := range []string{"/", "/runs", "/runs/ork-1", "/admin/namespaces"} {
		if code, body := fetch(t, "GET", server.URL+path, "", ""); code != http.StatusOK || body != uiPage {
			t.Errorf("GET %s = %d %q, want the app", path, code, body)
		}
	}
	// A Connect call reaches its service: an unknown run is the service's
	// own not_found, in Connect's JSON error shape, not the app.
	code, body := fetch(t, "POST", server.URL+"/orkstack.v1.RunService/GetRun", "application/json", `{"runId":"ork-none"}`)
	if code != http.StatusNotFound || !strings.Contains(body, `"code":"not_found"`) {
		t.Errorf("GetRun = %d %q, want Connect's not_found", code, body)
	}
	// Paths the API owns but nothing answers are 404 from every method,
	// never the app.
	for _, path := range []string{"/orkstack.v1.NoSuchService/Method", "/v1/nothing", "/ws/nothing", "/healthz", "/metrics"} {
		for _, method := range []string{"GET", "POST"} {
			if code, body := fetch(t, method, server.URL+path, "", ""); code != http.StatusNotFound || strings.Contains(body, uiPage) {
				t.Errorf("%s %s = %d %q, want a 404 that is not the app", method, path, code, body)
			}
		}
	}
	// The bridge still answers its route: a plain GET of a run's socket is
	// the bridge refusing a request that is not an upgrade, not the app.
	if code, body := fetch(t, "GET", server.URL+"/ws/runs/ork-none", "", ""); strings.Contains(body, uiPage) {
		t.Errorf("GET /ws/runs/ork-none = %d answered with the app", code)
	}
}

// With no UI the mux is the API alone, as it always was: / is a 404.
func TestNewMux_WithoutTheUI(t *testing.T) {
	server := uiMux(t, nil)
	if code, _ := fetch(t, "GET", server.URL+"/", "", ""); code != http.StatusNotFound {
		t.Errorf("GET / = %d, want 404 with the UI off", code)
	}
}

// Every route NewMux mounts lies under a path the UI treats as reserved, so
// mounting the UI at / can never take a path the API answers, and a typo in
// an API path is a 404 rather than the app's page. NewMux refuses a route
// outside them; this mounts the whole mux, which would panic on one.
func TestNewMux_EveryRouteIsReservedFromTheUI(t *testing.T) {
	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("NewMux mounts a route the UI does not reserve: %v", r)
		}
	}()
	uiMux(t, webui.Handler(webui.Placeholder()))
	for _, pattern := range []string{
		api.TriggerInvokePath, api.WebhookCollectionPath, api.WebhookItemPath,
		api.AwaitCompletePath, api.AwaitRejectPath, api.TaskLogSocketPath, "/ws/runs/{id}",
	} {
		if !webui.Reserved(pattern) {
			t.Errorf("%s is not reserved from the UI", pattern)
		}
	}
}

// The guard cannot be stepped around: NewMux's mux offers Handle alone, so a
// route mounted any other way does not compile.
func TestNewMux_RefusesARouteTheUIDoesNotReserve(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("mounting /api/x did not panic")
		}
	}()
	api.MountReservedForTest(http.NewServeMux(), "/api/x")
}
