package api_test

import (
	"bytes"
	"context"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/api"
	"github.com/orkstack/orkstack/internal/engine"
)

// The browser is the only client of the WebSocket bridge, and a browser always
// sends an Origin header naming the page it came from. That origin is the UI's,
// never the engine's, so the bridge is cross-origin by construction: the UI is
// served from one port and the engine listens on another.
//
// These tests pin what that means. They exist because a dev stack that did not
// pass ORK_WS_ALLOWED_ORIGINS shipped a UI whose run page never went live and
// whose event log stayed empty, while a page refresh worked — the refresh reads
// through the same-origin Connect proxy, the socket does not.

// wsBridge serves one run, with an event backlog, over the bridge with the
// given allowed origins.
func wsBridge(t *testing.T, allowed []string) (*httptest.Server, string) {
	t.Helper()
	runs := engine.NewRunStore()
	bus := engine.NewEventBus()
	runID := "ork-1-1"
	runs.Save(&orkstackv1.Run{
		Id:             runID,
		OrkflowName:    "hello-ork",
		OrkflowVersion: "0.1.0",
		Status:         orkstackv1.Status_STATUS_SUCCEEDED,
	})
	// A backlog, so a socket that opens has something to deliver. Without it
	// an accepted-but-silent bridge would look identical to a working one.
	for _, event := range []*orkstackv1.Event{
		{RunId: runID, Type: orkstackv1.EventType_EVENT_TYPE_RUN_STARTED, Status: orkstackv1.Status_STATUS_RUNNING, Message: "run started"},
		{RunId: runID, Type: orkstackv1.EventType_EVENT_TYPE_RUN_SUCCEEDED, Status: orkstackv1.Status_STATUS_SUCCEEDED, Message: "run succeeded"},
	} {
		bus.Publish(event, "")
	}
	svc := api.NewService(api.Config{Runs: runs, Bus: bus})
	server := httptest.NewServer(api.NewMux(svc, api.MuxOptions{AllowedOrigins: allowed}))
	t.Cleanup(server.Close)
	return server, runID
}

// handshake performs the upgrade a browser would, and reports the status.
func handshake(t *testing.T, server *httptest.Server, runID, origin string) int {
	t.Helper()
	req, err := http.NewRequest(http.MethodGet, server.URL+"/ws/runs/"+runID+"?from_sequence=0", nil)
	if err != nil {
		t.Fatalf("build request: %v", err)
	}
	req.Header.Set("Connection", "Upgrade")
	req.Header.Set("Upgrade", "websocket")
	req.Header.Set("Sec-WebSocket-Version", "13")
	req.Header.Set("Sec-WebSocket-Key", "dGhlIHNhbXBsZSBub25jZQ==")
	if origin != "" {
		req.Header.Set("Origin", origin)
	}
	resp, err := server.Client().Transport.RoundTrip(req)
	if err != nil {
		t.Fatalf("handshake: %v", err)
	}
	defer resp.Body.Close()
	return resp.StatusCode
}

func TestWebSocketBridge_RefusesAnOriginItWasNotToldToAllow(t *testing.T) {
	server, runID := wsBridge(t, nil)

	// This is the default an unconfigured engine runs with, and it is the right
	// default: an arbitrary page must not read another site's runs.
	if got := handshake(t, server, runID, "http://localhost:5173"); got != http.StatusForbidden {
		t.Fatalf("cross-origin handshake = %d, want %d — an unconfigured engine must not accept a foreign origin",
			got, http.StatusForbidden)
	}
}

func TestWebSocketBridge_AcceptsTheOriginServingTheUI(t *testing.T) {
	const uiOrigin = "http://localhost:5173"
	server, runID := wsBridge(t, []string{"localhost:5173"})

	if got := handshake(t, server, runID, uiOrigin); got != http.StatusSwitchingProtocols {
		t.Fatalf("configured-origin handshake = %d, want %d — the UI's own origin must connect",
			got, http.StatusSwitchingProtocols)
	}
}

func TestWebSocketBridge_DeliversEventsToTheBrowsersOrigin(t *testing.T) {
	const uiOrigin = "http://localhost:5173"
	server, runID := wsBridge(t, []string{"localhost:5173"})

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// The whole point of the handshake succeeding: frames arrive. A test that
	// only asserted 101 would pass against a bridge that accepts and then says
	// nothing, which is indistinguishable from the bug being fixed here.
	conn, _, err := websocket.Dial(ctx, server.URL+"/ws/runs/"+runID+"?from_sequence=0",
		&websocket.DialOptions{HTTPHeader: http.Header{"Origin": []string{uiOrigin}}})
	if err != nil {
		t.Fatalf("dial as the browser: %v", err)
	}
	defer conn.CloseNow()

	typ, payload, err := conn.Read(ctx)
	if err != nil {
		t.Fatalf("read first frame: %v", err)
	}
	if typ != websocket.MessageText {
		t.Fatalf("frame type = %v, want text", typ)
	}
	if len(payload) == 0 {
		t.Fatal("first frame is empty")
	}
}

func TestWebSocketBridge_TellsAForeignOriginNothingAboutWhichRunsExist(t *testing.T) {
	server, runID := wsBridge(t, []string{"localhost:5173"})

	const foreign = "http://evil.example"
	existing := handshake(t, server, runID, foreign)
	missing := handshake(t, server, "ork-does-not-exist", foreign)

	if existing != http.StatusForbidden || missing != http.StatusForbidden {
		t.Fatalf("existing run = %d, missing run = %d; want %d for both — a foreign origin "+
			"that can tell these apart has an oracle for which run ids exist",
			existing, missing, http.StatusForbidden)
	}
}

func TestWebSocketBridge_AnAllowedOriginStillGets404ForAMissingRun(t *testing.T) {
	server, _ := wsBridge(t, []string{"localhost:5173"})

	// This is what `make up` probes: with the origin allowed, a run id that
	// does not exist answers 404, which distinguishes "the engine will talk to
	// my page" from "the engine refuses my origin" (403).
	if got := handshake(t, server, "ork-does-not-exist", "http://localhost:5173"); got != http.StatusNotFound {
		t.Fatalf("missing run from an allowed origin = %d, want %d", got, http.StatusNotFound)
	}
}

// TestWebSocketBridge_RefusalIsLoggedThroughTheInjectedLogger pins the bridge
// to svc.logger rather than the slog package-level logger. Every other
// subsystem logs through the Service's injected *slog.Logger; a refusal
// logged through the package-level default instead would be invisible to
// whatever handler the deployment actually configured (`ork` wires its own
// handler onto Config.Logger, never onto slog.SetDefault).
func TestWebSocketBridge_RefusalIsLoggedThroughTheInjectedLogger(t *testing.T) {
	var injected bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&injected, nil))

	// The package-level default is swapped for its own recorder, so a refusal
	// that (bug) still went through slog.Warn shows up here instead of in
	// the injected logger's buffer, rather than the test just not noticing.
	var fallenThrough bytes.Buffer
	previousDefault := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&fallenThrough, nil)))
	t.Cleanup(func() { slog.SetDefault(previousDefault) })

	runs := engine.NewRunStore()
	bus := engine.NewEventBus()
	runID := "ork-logger-1"
	runs.Save(&orkstackv1.Run{
		Id:             runID,
		OrkflowName:    "hello-ork",
		OrkflowVersion: "0.1.0",
		Status:         orkstackv1.Status_STATUS_SUCCEEDED,
	})
	svc := api.NewService(api.Config{Runs: runs, Bus: bus, Logger: logger})
	server := httptest.NewServer(api.NewMux(svc, api.MuxOptions{AllowedOrigins: []string{"localhost:5173"}}))
	t.Cleanup(server.Close)

	if got := handshake(t, server, runID, "http://evil.example"); got != http.StatusForbidden {
		t.Fatalf("cross-origin handshake = %d, want %d", got, http.StatusForbidden)
	}

	if !strings.Contains(injected.String(), "websocket upgrade refused") {
		t.Fatalf("the Service's injected logger did not record the refusal; got %q", injected.String())
	}
	if strings.Contains(fallenThrough.String(), "websocket upgrade refused") {
		t.Fatalf("the refusal was logged through the package-level default logger instead of "+
			"svc.logger; got %q", fallenThrough.String())
	}
}

func TestWebSocketBridge_AcceptsBothSpellingsOfAnAllowedOrigin(t *testing.T) {
	// ORK_WS_ALLOWED_ORIGINS is written both ways in this repo: the e2e passes
	// a full URL, `make up` passes bare host:port. coder/websocket accepts
	// both, so this check must too — an early check that is stricter than the
	// library's rejects sockets Accept would have allowed, which is a refused
	// upgrade invented by the guard rather than by the policy.
	for _, pattern := range []string{"http://localhost:5173", "localhost:5173"} {
		server, runID := wsBridge(t, []string{pattern})
		if got := handshake(t, server, runID, "http://localhost:5173"); got != http.StatusSwitchingProtocols {
			t.Errorf("pattern %q: handshake = %d, want %d", pattern, got, http.StatusSwitchingProtocols)
		}
	}
}
