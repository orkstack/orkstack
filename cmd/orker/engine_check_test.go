package main

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"connectrpc.com/connect"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/gen/go/orkstack/v1/orkstackv1connect"
	"github.com/orkstack/orkstack/internal/orker"
)

type acceptingEngine struct {
	orkstackv1connect.UnimplementedAuthServiceHandler
	accept string
}

func (e acceptingEngine) GetSignin(_ context.Context, req *connect.Request[orkstackv1.GetSigninRequest]) (*connect.Response[orkstackv1.GetSigninResponse], error) {
	if req.Header().Get("Authorization") != "Bearer "+e.accept {
		return nil, connect.NewError(connect.CodeUnauthenticated, nil)
	}
	return connect.NewResponse(&orkstackv1.GetSigninResponse{PrincipalId: "service:dev:orker-1"}), nil
}

// TestCheckEngineCredential: an Orker boots only with an orker.engine.* it
// can use: none at all, or a credential the engine accepts.
func TestCheckEngineCredential(t *testing.T) {
	mux := http.NewServeMux()
	mux.Handle(orkstackv1connect.NewAuthServiceHandler(acceptingEngine{accept: "good"}))
	server := httptest.NewUnstartedServer(mux)
	server.Config.Protocols = new(http.Protocols)
	server.Config.Protocols.SetHTTP1(true)
	server.Config.Protocols.SetUnencryptedHTTP2(true)
	server.Start()
	t.Cleanup(server.Close)
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	token := func(v string) string {
		p := filepath.Join(t.TempDir(), "token")
		if err := os.WriteFile(p, []byte(v), 0o600); err != nil {
			t.Fatal(err)
		}
		return p
	}
	ctx := context.Background()
	if err := checkEngineCredential(ctx, orker.EngineSettings{}, logger); err != nil {
		t.Fatalf("no engine: %v", err)
	}
	if err := checkEngineCredential(ctx, orker.EngineSettings{URL: server.URL, Method: orker.EngineAuthTokenFile, TokenFile: token("good")}, logger); err != nil {
		t.Fatalf("an accepted credential: %v", err)
	}
	if err := checkEngineCredential(ctx, orker.EngineSettings{URL: server.URL, Method: orker.EngineAuthTokenFile, TokenFile: token("bad")}, logger); err == nil {
		t.Fatal("a refused credential booted")
	}
	if err := checkEngineCredential(ctx, orker.EngineSettings{URL: server.URL}, logger); err == nil {
		t.Fatal("an engine URL with no method booted")
	}
}
