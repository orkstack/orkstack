package api_test

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/coder/websocket"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/gen/go/orkstack/v1/orkstackv1connect"
	"github.com/orkstack/orkstack/internal/api"
	"github.com/orkstack/orkstack/internal/engine"
)

func mustHostPolicy(t *testing.T, listen string, allowed ...string) api.HostPolicy {
	t.Helper()
	policy, err := api.NewHostPolicy(listen, allowed)
	if err != nil {
		t.Fatalf("NewHostPolicy(%q, %q): %v", listen, allowed, err)
	}
	return policy
}

// With nothing set, the engine answers the names a DNS-rebinding page cannot
// give: an address, localhost and the names under it, a name with no dot, and
// the name it was told to listen on. Every other name is refused, whatever
// its port or case.
func TestHostPolicy_TheDefaultAdmitsOnlyNamesARebindingPageCannotGive(t *testing.T) {
	for _, tc := range []struct {
		listen string
		hosts  []string
		admit  bool
	}{
		{":8080", []string{
			// Addresses, v4 and v6, with and without a port or a zone.
			"127.0.0.1:8080", "127.0.0.1", "10.0.0.5:30080", "[::1]:8080", "[::1]", "::1", "[fe80::1%25eth0]:8080",
			// localhost and the names under it, any case, a trailing dot.
			"localhost:8080", "LOCALHOST", "localhost.", "localhost:5173", "app.localhost", "A.b.LocalHost.:8080",
			// A name with no dot: a compose service, a Service, a short name, in punycode too.
			"ork:8080", "orkstack-engine", "ORK", "ork_1:8080", "xn--bcher-kva:8080",
			// No Host at all, which no browser sends.
			"",
		}, true},
		{":8080", []string{
			"evil.example", "evil.example:8080", "EVIL.EXAMPLE", "Evil.Example.", "localhost.evil.example",
			"127.0.0.1.nip.io:8080", "2130706433.example", "xn--bcher-kva.example",
			"ork.internal:8080", "ork.orkstack", "ork.orkstack.svc", "ork.orkstack.svc.cluster.local:8080",
			// A dotless name written fully qualified is a top-level name of public DNS.
			"evil.", "evil.:8080",
			// Not a name at all.
			".", ":8080", "evil@localhost", "a:b:c.localhost", "bücher", "evil:.com", "ork:80x", "[::1]:http",
		}, false},
		{"ork.internal:8080", []string{"ork.internal:8080", "ork.internal", "ORK.internal.", "127.0.0.1:8080"}, true},
		{"ork.internal:8080", []string{"other.internal:8080", "evil.ork.internal"}, false},
		{"0.0.0.0:8080", []string{"0.0.0.0:8080", "192.168.1.20:8080"}, true},
		{"0.0.0.0:8080", []string{"evil.example"}, false},
	} {
		policy := mustHostPolicy(t, tc.listen)
		for _, host := range tc.hosts {
			if got := policy.Allows(host); got != tc.admit {
				t.Errorf("listening on %q, Host %q: admitted = %t, want %t", tc.listen, host, got, tc.admit)
			}
		}
	}
}

// ORK_ALLOWED_HOSTS adds to the defaults: a name, every name under a
// *.suffix (never the suffix itself), or * for any Host.
func TestHostPolicy_AllowedHostsAddNamesAndPatterns(t *testing.T) {
	policy := mustHostPolicy(t, ":8080", "orkstack.example.com", " *.Corp.Example ", "", "192.168.0.9", "[::1]",
		"xn--bcher-kva.example")
	for host, want := range map[string]bool{
		"orkstack.example.com":      true,
		"ORKSTACK.EXAMPLE.COM:443":  true,
		"orkstack.example.com.":     true,
		"XN--BCHER-KVA.example:443": true,
		"bücher.example":            false,
		"a.corp.example":            true,
		"b.a.corp.example:8443":     true,
		"corp.example":              false,
		"xcorp.example":             false,
		"orkstack.example.com.evil": false,
		"evil.example":              false,
		"127.0.0.1:8080":            true,
		"localhost":                 true,
		"ork":                       true,
	} {
		if got := policy.Allows(host); got != want {
			t.Errorf("Host %q: admitted = %t, want %t", host, got, want)
		}
	}
	star := mustHostPolicy(t, ":8080", "*")
	for _, host := range []string{"evil.example", "anything.at.all:1", "localhost"} {
		if !star.Allows(host) {
			t.Errorf("with *, Host %q was refused", host)
		}
	}
}

// An entry that is not a name or a pattern is refused at boot, naming it,
// rather than silently admitting or refusing something the operator did not
// mean.
func TestNewHostPolicy_RefusesAnEntryThatIsNotANameOrAPattern(t *testing.T) {
	for _, entry := range []string{
		"http://orkstack.example.com", "orkstack.example.com:8080", "ork*.example.com", "*example.com",
		"a/b", "*.", ".", "..", "a..b", "-a.example", "*.*.example", "ex ample", "é.example",
		// Half-bracketed addresses (FuzzHostPolicy found "::]").
		"::]", "[::1",
	} {
		_, err := api.NewHostPolicy(":8080", []string{entry})
		if err == nil || !strings.Contains(err.Error(), "ORK_ALLOWED_HOSTS") || !strings.Contains(err.Error(), entry) {
			t.Errorf("entry %q: err = %v, want a refusal naming ORK_ALLOWED_HOSTS and the entry", entry, err)
		}
	}
	if _, err := api.NewHostPolicy("ork.internal:8080:9", nil); err == nil || !strings.Contains(err.Error(), "ORK_HTTP_ADDR") {
		t.Errorf("a listen address that is not one: err = %v, want a refusal naming ORK_HTTP_ADDR", err)
	}
}

// A request with a Host the policy refuses is answered 403 before any route
// sees it: as a Connect error naming the Host and the setting on a Connect
// path, as plain text anywhere else.
func TestHostPolicy_GuardRefusesAForeignHostBeforeAnyHandler(t *testing.T) {
	reached := 0
	var logged strings.Builder
	guard := mustHostPolicy(t, ":8080").Guard(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		reached++
		w.WriteHeader(http.StatusNoContent)
	}), slog.New(slog.NewTextHandler(&logged, nil)))

	request := func(method, target, host string) *http.Response {
		req := httptest.NewRequest(method, target, strings.NewReader("{}"))
		req.Host = host
		rec := httptest.NewRecorder()
		guard.ServeHTTP(rec, req)
		return rec.Result()
	}
	for _, host := range []string{"127.0.0.1:8080", "localhost:8080", "ork:8080"} {
		if res := request(http.MethodGet, "/runs", host); res.StatusCode != http.StatusNoContent {
			t.Errorf("Host %q: %d, want the route's 204", host, res.StatusCode)
		}
	}
	admitted := reached

	connectRes := request(http.MethodPost, orkstackv1connect.RunServiceListRunsProcedure, "evil.example:8080")
	var connectErr struct{ Code, Message string }
	if err := json.NewDecoder(connectRes.Body).Decode(&connectErr); err != nil {
		t.Fatalf("decode the Connect error: %v", err)
	}
	if connectRes.StatusCode != http.StatusForbidden || connectRes.Header.Get("Content-Type") != "application/json" ||
		connectErr.Code != "permission_denied" || !strings.Contains(connectErr.Message, `"evil.example:8080"`) ||
		!strings.Contains(connectErr.Message, "ORK_ALLOWED_HOSTS") {
		t.Errorf("a Connect call with a foreign Host: %d %s %+v", connectRes.StatusCode, connectRes.Header.Get("Content-Type"), connectErr)
	}
	for _, target := range []string{"/", "/runs", "/ws/runs/ork-1", "/v1/webhooks"} {
		res := request(http.MethodGet, target, "evil.example")
		text, _ := io.ReadAll(res.Body)
		if res.StatusCode != http.StatusForbidden || !strings.Contains(string(text), "ORK_ALLOWED_HOSTS") {
			t.Errorf("GET %s with a foreign Host: %d %q, want 403 naming ORK_ALLOWED_HOSTS", target, res.StatusCode, text)
		}
		if got := res.Header.Get("Cache-Control"); got != "no-store" {
			t.Errorf("GET %s with a foreign Host: Cache-Control %q, want no-store", target, got)
		}
	}
	if reached != admitted {
		t.Errorf("a refused request reached the route %d times", reached-admitted)
	}
	// Five refusals in a burst are one log line, naming the Host and the
	// setting: a page that loops cannot flood the log.
	if lines := strings.Count(logged.String(), "request refused: Host not allowed"); lines != 1 ||
		!strings.Contains(logged.String(), "evil.example:8080") || !strings.Contains(logged.String(), "ORK_ALLOWED_HOSTS") {
		t.Errorf("five refusals logged %d lines, want one naming the Host and the setting:\n%s", lines, logged.String())
	}
}

// hostTransport sends every request with the Host header set to host, as a
// browser does on a page whose name was made to resolve to the engine.
type hostTransport struct {
	host string
	base http.RoundTripper
}

func (h hostTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	req = req.Clone(req.Context())
	req.Host = h.host
	return h.base.RoundTrip(req)
}

// Over the real listener, a Connect call and a WebSocket under a foreign Host
// are refused, and the same calls under the engine's own address are served.
func TestHostPolicy_TheListenerRefusesAForeignHostOnEveryRoute(t *testing.T) {
	const runID = "run-hosts"
	runs := engine.NewRunStore()
	bus := engine.NewEventBus()
	runs.Save(&orkstackv1.Run{Id: runID, OrkflowName: "hello-ork", Status: orkstackv1.Status_STATUS_RUNNING})
	bus.Publish(&orkstackv1.Event{RunId: runID, Type: orkstackv1.EventType_EVENT_TYPE_RUN_STARTED,
		Status: orkstackv1.Status_STATUS_RUNNING}, "")
	svc := api.NewService(api.Config{Orkflows: engine.NewMemoryOrkflowStore(), Runs: runs, Bus: bus, Starter: &fakeStarter{}})
	policy := mustHostPolicy(t, "127.0.0.1:0")
	server := api.NewServer("127.0.0.1:0", policy.Guard(api.NewMux(svc, api.MuxOptions{}), slog.New(slog.NewTextHandler(io.Discard, nil))))
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(func() { _ = server.Close() })
	base := "http://" + listener.Addr().String()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	for _, tc := range []struct {
		host    string
		refused bool
	}{{"", false}, {"evil.example", true}} {
		transport := &http.Transport{}
		t.Cleanup(transport.CloseIdleConnections)
		var rt http.RoundTripper = transport
		if tc.host != "" {
			rt = hostTransport{host: tc.host, base: transport}
		}
		client := &http.Client{Transport: rt}
		_, err := orkstackv1connect.NewRunServiceClient(client, base, connect.WithProtoJSON()).
			ListRuns(ctx, connect.NewRequest(&orkstackv1.ListRunsRequest{}))
		if !tc.refused && err != nil {
			t.Errorf("ListRuns with the engine's own Host: %v", err)
		}
		if tc.refused && (connect.CodeOf(err) != connect.CodePermissionDenied || !strings.Contains(err.Error(), "ORK_ALLOWED_HOSTS")) {
			t.Errorf("ListRuns with Host %q: %v, want permission_denied naming ORK_ALLOWED_HOSTS", tc.host, err)
		}

		conn, res, err := websocket.Dial(ctx, "ws"+strings.TrimPrefix(base, "http")+"/ws/runs/"+runID,
			&websocket.DialOptions{HTTPClient: client})
		if !tc.refused {
			if err != nil {
				t.Errorf("the run socket with the engine's own Host: %v", err)
				continue
			}
			_ = conn.CloseNow()
			continue
		}
		if err == nil {
			_ = conn.CloseNow()
			t.Errorf("the run socket with Host %q opened", tc.host)
		} else if res == nil || res.StatusCode != http.StatusForbidden {
			t.Errorf("the run socket with Host %q: %v, want 403", tc.host, err)
		}
	}
}
