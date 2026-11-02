package api_test

import (
	"bufio"
	"context"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"testing/fstest"
	"time"

	"connectrpc.com/connect"
	"github.com/coder/websocket"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/gen/go/orkstack/v1/orkstackv1connect"
	"github.com/orkstack/orkstack/internal/api"
	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/internal/hostpolicy"
	"github.com/orkstack/orkstack/internal/webui"
)

func mustHostPolicy(t *testing.T, listen string, allowed ...string) hostpolicy.Policy {
	t.Helper()
	policy, err := api.NewHostPolicy(listen, allowed)
	if err != nil {
		t.Fatalf("NewHostPolicy(%q, %q): %v", listen, allowed, err)
	}
	return policy
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

// rawRequest writes request to the listener at addr as it is, and returns
// the response's status line.
func rawRequest(t *testing.T, addr, request string) string {
	t.Helper()
	conn, err := net.DialTimeout("tcp", addr, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(10 * time.Second))
	if _, err := io.WriteString(conn, request); err != nil {
		t.Fatal(err)
	}
	status, err := bufio.NewReader(conn).ReadString('\n')
	if err != nil {
		t.Fatalf("read the status line: %v", err)
	}
	return strings.TrimSpace(status)
}

// The policy judges the host the server serves the request for, never a
// header a proxy or a client may add: under an absolute-form target that is
// the target's host (RFC 9112 §3.2.2), whatever the Host header says;
// X-Forwarded-Host is never read; and a request with two Host headers is
// refused 400 before the policy sees it.
func TestHostPolicy_TheListenerJudgesTheHostItServes(t *testing.T) {
	svc := api.NewService(api.Config{Orkflows: engine.NewMemoryOrkflowStore(), Runs: engine.NewRunStore(),
		Bus: engine.NewEventBus(), Starter: &fakeStarter{}})
	server := api.NewServer("127.0.0.1:0", mustHostPolicy(t, "127.0.0.1:0").Guard(api.NewMux(svc, api.MuxOptions{}),
		slog.New(slog.NewTextHandler(io.Discard, nil))))
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(func() { _ = server.Close() })
	addr := listener.Addr().String()
	listRuns := func(target string, headers ...string) string {
		return "POST " + target + " HTTP/1.1\r\n" + strings.Join(headers, "\r\n") + "\r\n" +
			"Content-Type: application/json\r\nContent-Length: 2\r\nConnection: close\r\n\r\n{}"
	}
	const procedure = orkstackv1connect.RunServiceListRunsProcedure
	for _, tc := range []struct {
		name, request, want string
	}{
		{"origin-form under the engine's own Host", listRuns(procedure, "Host: "+addr), "HTTP/1.1 200 OK"},
		{"origin-form under a foreign Host", listRuns(procedure, "Host: evil.example"), "HTTP/1.1 403 Forbidden"},
		{"an absolute-form target naming a foreign host, under the engine's own Host",
			listRuns("http://evil.example"+procedure, "Host: "+addr), "HTTP/1.1 403 Forbidden"},
		{"an absolute-form target naming the engine, under a foreign Host",
			listRuns("http://"+addr+procedure, "Host: evil.example"), "HTTP/1.1 200 OK"},
		{"X-Forwarded-Host naming the engine under a foreign Host",
			listRuns(procedure, "Host: evil.example", "X-Forwarded-Host: "+addr), "HTTP/1.1 403 Forbidden"},
		{"X-Forwarded-Host naming a foreign host under the engine's own Host",
			listRuns(procedure, "Host: "+addr, "X-Forwarded-Host: evil.example"), "HTTP/1.1 200 OK"},
		{"two Host headers, the engine's first", listRuns(procedure, "Host: "+addr, "Host: evil.example"),
			"HTTP/1.1 400 Bad Request"},
		{"two Host headers, the foreign one first", listRuns(procedure, "Host: evil.example", "Host: "+addr),
			"HTTP/1.1 400 Bad Request"},
	} {
		if got := rawRequest(t, addr, tc.request); got != tc.want {
			t.Errorf("%s: %q, want %q", tc.name, got, tc.want)
		}
	}
}

// A refusal carries the headers every other error of the listener carries
// (the UI's 404 is the reference): never cached, never sniffed, and under
// the policy that loads nothing and that nothing may frame.
func TestHostPolicy_ARefusalCarriesTheHeadersOfEveryOtherError(t *testing.T) {
	ui := webui.Handler(fstest.MapFS{"index.html": {Data: []byte("<!doctype html>")}})
	refused := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodGet, "/_app/missing.js", nil)
	request.Host = "evil.example"
	mustHostPolicy(t, ":8080").Guard(ui, slog.New(slog.NewTextHandler(io.Discard, nil))).ServeHTTP(refused, request)
	missing := httptest.NewRecorder()
	ui.ServeHTTP(missing, httptest.NewRequest(http.MethodGet, "/_app/missing.js", nil))
	if refused.Code != http.StatusForbidden || missing.Code != http.StatusNotFound {
		t.Fatalf("refused %d, missing %d; want 403 and 404", refused.Code, missing.Code)
	}
	for _, header := range []string{"Cache-Control", "X-Content-Type-Options", "Content-Security-Policy"} {
		got, want := refused.Header().Get(header), missing.Header().Get(header)
		if want == "" || got != want {
			t.Errorf("%s: the refusal sends %q, the UI's 404 %q", header, got, want)
		}
	}
}
