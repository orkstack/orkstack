package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"connectrpc.com/connect"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/gen/go/orkstack/v1/orkstackv1connect"
)

type signinEngine struct {
	orkstackv1connect.UnimplementedAuthServiceHandler
	resp *orkstackv1.GetSigninResponse
}

func (e signinEngine) GetSignin(context.Context, *connect.Request[orkstackv1.GetSigninRequest]) (*connect.Response[orkstackv1.GetSigninResponse], error) {
	return connect.NewResponse(e.resp), nil
}

func startFakeEngine(t *testing.T, mux *http.ServeMux) string {
	t.Helper()
	server := httptest.NewUnstartedServer(mux)
	server.Config.Protocols = new(http.Protocols)
	server.Config.Protocols.SetHTTP1(true)
	server.Config.Protocols.SetUnencryptedHTTP2(true)
	server.Start()
	t.Cleanup(server.Close)
	t.Setenv(serverEnvVar, "")
	t.Setenv(namespaceEnvVar, "")
	t.Setenv(principalEnvVar, "")
	t.Setenv(tokenFileEnvVar, "")
	t.Setenv(configEnvVar, t.TempDir()+"/config.json")
	return server.URL
}

func TestWhoami(t *testing.T) {
	mux := http.NewServeMux()
	mux.Handle(orkstackv1connect.NewAuthServiceHandler(signinEngine{resp: &orkstackv1.GetSigninResponse{
		PrincipalId: "user:corp:nina", Method: "jwt", Issuer: "corp", Kind: "user", Groups: []string{"eng"},
		Bindings:       []*orkstackv1.ExplainedBinding{{Binding: "payments-admins", Role: "namespace-admin", Scope: "namespace:payments", Subject: "user:corp:nina"}},
		Permissions:    []*orkstackv1.ScopedPermissions{{Scope: "namespace:payments", Permissions: []string{"orkflow.read", "run.read"}}},
		AccessRevision: 7,
	}}))
	url := startFakeEngine(t, mux)
	var out, errOut strings.Builder
	if code := Execute([]string{"whoami", "--server", url}, &out, &errOut); code != exitOK {
		t.Fatalf("whoami = %d: %s", code, errOut.String())
	}
	for _, want := range []string{"user:corp:nina (verified, jwt, issuer corp)", "groups:     eng",
		"payments-admins: namespace-admin at namespace:payments, as user:corp:nina", "at namespace:payments: orkflow.read run.read", "revision 7"} {
		if !strings.Contains(out.String(), want) {
			t.Errorf("whoami printed %q; want it to contain %q", out.String(), want)
		}
	}
	out.Reset()
	if code := Execute([]string{"whoami", "-j", "--server", url}, &out, &errOut); code != exitOK || !strings.Contains(out.String(), `"principalId"`) && !strings.Contains(out.String(), `"principal_id"`) {
		t.Fatalf("whoami -j = %d %q", code, out.String())
	}
	// An asserted engine mounts no AuthService: whoami says what that means.
	asserted := startFakeEngine(t, http.NewServeMux())
	errOut.Reset()
	if code := Execute([]string{"whoami", "--server", asserted}, &out, &errOut); code == exitOK || !strings.Contains(errOut.String(), "asserted") {
		t.Fatalf("whoami against an asserted engine = %d %q", code, errOut.String())
	}
}
