package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"connectrpc.com/connect"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/gen/go/orkstack/v1/orkstackv1connect"
)

type modeEngine struct {
	orkstackv1connect.UnimplementedSystemServiceHandler
	mode   string
	refuse bool
	mu     sync.Mutex
	asked  int
}

func (m *modeEngine) GetSystemInfo(context.Context, *connect.Request[orkstackv1.GetSystemInfoRequest]) (*connect.Response[orkstackv1.GetSystemInfoResponse], error) {
	m.mu.Lock()
	m.asked++
	m.mu.Unlock()
	if m.refuse {
		return nil, connect.NewError(connect.CodeUnauthenticated, nil)
	}
	return connect.NewResponse(&orkstackv1.GetSystemInfoResponse{AuthMode: m.mode}), nil
}

// TestPrincipal_WarnsOnceWhenTheEngineIgnoresIt: ORK_PRINCIPAL is an
// assertion only an asserted engine records; against one that
// authenticates, orkctl says once that it is not used (authn-authz.md §1).
func TestPrincipal_WarnsOnceWhenTheEngineIgnoresIt(t *testing.T) {
	tests := []struct {
		name   string
		env    string
		engine *modeEngine
		warns  bool
	}{
		{name: "enforce", env: "dsantos", engine: &modeEngine{mode: "enforce"}, warns: true},
		{name: "observe", env: "dsantos", engine: &modeEngine{mode: "observe"}, warns: true},
		{name: "an engine that refuses an unauthenticated caller", env: "dsantos", engine: &modeEngine{refuse: true}, warns: true},
		{name: "asserted", env: "dsantos", engine: &modeEngine{mode: "asserted"}},
		{name: "an engine older than auth modes", env: "dsantos", engine: &modeEngine{}},
		{name: "unset", engine: &modeEngine{mode: "enforce"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mux := http.NewServeMux()
			mux.Handle(orkstackv1connect.NewSystemServiceHandler(tt.engine))
			server := httptest.NewUnstartedServer(mux)
			server.Config.Protocols = new(http.Protocols)
			server.Config.Protocols.SetHTTP1(true)
			server.Config.Protocols.SetUnencryptedHTTP2(true)
			server.Start()
			t.Cleanup(server.Close)
			t.Setenv(serverEnvVar, "")
			t.Setenv(namespaceEnvVar, "")
			t.Setenv(configEnvVar, t.TempDir()+"/config.json")
			t.Setenv(principalEnvVar, tt.env)
			var out, errOut strings.Builder
			Execute([]string{"admin", "info", "--server", server.URL}, &out, &errOut)
			warnings := strings.Count(errOut.String(), "ORK_PRINCIPAL is set")
			if tt.warns && warnings != 1 {
				t.Fatalf("stderr = %q; want the warning exactly once", errOut.String())
			}
			if !tt.warns && warnings != 0 {
				t.Fatalf("stderr = %q; want no warning", errOut.String())
			}
			if tt.env == "" && tt.engine.asked != 1 {
				t.Fatalf("with ORK_PRINCIPAL unset the engine was asked %d times; want only the command's own request", tt.engine.asked)
			}
		})
	}
}
