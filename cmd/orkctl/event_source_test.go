package main

import (
	"context"
	"errors"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"connectrpc.com/connect"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/gen/go/orkstack/v1/orkstackv1connect"
	"github.com/orkstack/orkstack/internal/api"
	"github.com/orkstack/orkstack/internal/testsupport/must"
)

// recordingSources answers DeleteEventSource from a script of answers and
// records every request, failing the test on one with no id or another id
// than the first attempt's.
type recordingSources struct {
	orkstackv1connect.UnimplementedEventSourceServiceHandler
	t       *testing.T
	mu      sync.Mutex
	answers []error
	deletes []*orkstackv1.DeleteEventSourceRequest
	creates []*orkstackv1.CreateEventSourceRequest
}

func (r *recordingSources) DeleteEventSource(_ context.Context, req *connect.Request[orkstackv1.DeleteEventSourceRequest]) (*connect.Response[orkstackv1.DeleteEventSourceResponse], error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if req.Msg.GetId() == "" || (len(r.deletes) > 0 && req.Msg.GetId() != r.deletes[0].GetId()) {
		r.t.Errorf("a delete attempt carried id %q; every attempt carries the first one's", req.Msg.GetId())
	}
	r.deletes = append(r.deletes, req.Msg)
	if len(r.answers) == 0 {
		return connect.NewResponse(&orkstackv1.DeleteEventSourceResponse{}), nil
	}
	answer := r.answers[0]
	r.answers = r.answers[1:]
	if answer == nil {
		return connect.NewResponse(&orkstackv1.DeleteEventSourceResponse{}), nil
	}
	return nil, answer
}

func (r *recordingSources) CreateEventSource(_ context.Context, req *connect.Request[orkstackv1.CreateEventSourceRequest]) (*connect.Response[orkstackv1.CreateEventSourceResponse], error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.creates = append(r.creates, req.Msg)
	return connect.NewResponse(&orkstackv1.CreateEventSourceResponse{EventSource: req.Msg.GetEventSource()}), nil
}

func serveSources(t *testing.T, sources *recordingSources) (string, orkstackv1connect.EventSourceServiceClient) {
	t.Helper()
	sources.t = t
	t.Setenv(serverEnvVar, "")
	t.Setenv(namespaceEnvVar, "")
	t.Setenv(configEnvVar, t.TempDir()+"/config.json")
	mux := http.NewServeMux()
	mux.Handle(orkstackv1connect.NewEventSourceServiceHandler(sources))
	server := api.NewServer("127.0.0.1:0", mux)
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	must.OKf(t, err, "listen")
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = server.Shutdown(ctx)
	})
	base := "http://" + listener.Addr().String()
	transport := &http.Transport{}
	transport.Protocols = new(http.Protocols)
	transport.Protocols.SetUnencryptedHTTP2(true)
	return base, orkstackv1connect.NewEventSourceServiceClient(&http.Client{Transport: transport}, base)
}

func TestEventSourceDelete_ClientRule(t *testing.T) {
	const id = "evs-0123456789abcdef"
	mismatch := connect.NewError(connect.CodeFailedPrecondition, errors.New("EVENT_SOURCE_ID_MISMATCH: event source \"orders\" is evs-1111111111111111 now, not "+id))
	unavailable := func() error { return connect.NewError(connect.CodeUnavailable, errors.New("x")) }
	notFound := func() error { return connect.NewError(connect.CodeNotFound, errors.New("gone")) }
	for _, tt := range []struct {
		name     string
		answers  []error
		attempts int
		wantErr  string
	}{
		{"success", nil, 1, ""},
		{"mismatch: refused, nothing more sent", []error{mismatch}, 1, "evs-1111111111111111"},
		{"not found: not finished", []error{notFound()}, 1, "not finished"},
		{"unavailable then success", []error{unavailable(), nil}, 2, ""},
		{"unavailable then not found", []error{unavailable(), notFound()}, 2, "not finished"},
		{"never known: five attempts, then not finished", []error{
			unavailable(), connect.NewError(connect.CodeDeadlineExceeded, errors.New("x")),
			connect.NewError(connect.CodeUnknown, errors.New("x")), connect.NewError(connect.CodeInternal, errors.New("x")),
			unavailable(),
		}, 5, "not finished"},
		{"permission denied: failed at once", []error{connect.NewError(connect.CodePermissionDenied, errors.New("no"))}, 1, "permission"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			sources := &recordingSources{answers: tt.answers}
			_, client := serveSources(t, sources)
			err := deleteEventSourceByID(context.Background(), client, sourceDelete{Namespace: "payments", Name: "orders", ID: id, Pool: "sources", Instances: 3})
			if len(sources.deletes) != tt.attempts {
				t.Fatalf("%d attempts, want %d", len(sources.deletes), tt.attempts)
			}
			if tt.wantErr == "" {
				if err != nil {
					t.Fatalf("delete = %v", err)
				}
				return
			}
			if err == nil || !strings.Contains(strings.ToLower(err.Error()), tt.wantErr) {
				t.Fatalf("delete = %v, want %q", err, tt.wantErr)
			}
			if strings.Contains(err.Error(), "already gone") {
				t.Fatal("a source's delete is never reported already gone")
			}
			if tt.wantErr == "not finished" && (!strings.Contains(err.Error(), "3 instance slot") || !strings.Contains(err.Error(), "sources")) {
				t.Fatalf("a delete that did not finish names the source's pool and instances: %v", err)
			}
		})
	}
}

func TestEventSourceFile_StrictAndNamespace(t *testing.T) {
	dir := t.TempDir()
	const header = "apiVersion: orkstack/v1\nkind: EventSource\n"
	write := func(name, body string) string {
		path := filepath.Join(dir, name)
		must.OKf(t, os.WriteFile(path, []byte(body), 0o600), "write")
		return path
	}
	sources := &recordingSources{}
	server, _ := serveSources(t, sources)

	headerless := write("headerless.yaml", "orkflow: f\nuses: ghcr.io/acme/s:1\n")
	runAgainst(t, server, "event-source", "create", "orders", "--file", headerless).requireExit(t, exitError).requireStderrContains(t, "kind: EventSource")

	merge := write("merge.yaml", header+"uses: &u ghcr.io/acme/s:1\norkflow: f\ndescription: *u\n")
	runAgainst(t, server, "event-source", "create", "orders", "--file", merge).requireExit(t, exitError).requireStderrContains(t, "anchors, aliases and merge keys")

	other := write("other.yaml", header+"namespace: b\norkflow: f\nuses: ghcr.io/acme/s:1\n")
	runAgainst(t, server, "-n", "a", "event-source", "create", "orders", "--file", other).requireExit(t, exitError).requireStderrContains(t, "EVENT_SOURCE_NAMESPACE_MISMATCH")

	named := write("named.yaml", header+"name: invoices\norkflow: f\nuses: ghcr.io/acme/s:1\n")
	runAgainst(t, server, "event-source", "create", "orders", "--file", named).requireExit(t, exitError).requireStderrContains(t, `names event source "invoices"`)

	none := write("none.yaml", header+"orkflow: f\nuses: ghcr.io/acme/s:1\n")
	runAgainst(t, server, "-n", "a", "event-source", "create", "orders", "--file", none).requireExit(t, exitOK)
	runAgainst(t, server, "event-source", "create", "orders", "--file", other).requireExit(t, exitOK)
	runAgainst(t, server, "event-source", "create", "invoices", "--file", named).requireExit(t, exitOK)
	if len(sources.creates) != 3 || sources.creates[0].GetEventSource().GetNamespace() != "a" || sources.creates[1].GetEventSource().GetNamespace() != "b" ||
		sources.creates[2].GetEventSource().GetName() != "invoices" {
		t.Fatalf("creates %v: a document with no namespace takes -n, one that names it acts there, and a name that agrees is kept", sources.creates)
	}
}
