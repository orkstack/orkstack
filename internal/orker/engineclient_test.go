package orker

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"connectrpc.com/connect"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/gen/go/orkstack/v1/orkstackv1connect"
)

func TestEngineSettings_Validate(t *testing.T) {
	dir := t.TempDir()
	token := filepath.Join(dir, "token")
	if err := os.WriteFile(token, []byte("eyJ.x.y\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	ok := []EngineSettings{
		{},
		{URL: "https://ork.example.com", Method: EngineAuthTokenFile, TokenFile: token},
		{URL: "http://127.0.0.1:8080", Method: EngineAuthTokenFile, TokenFile: token},
		{URL: "http://localhost:8080", Method: EngineAuthTokenFile, TokenFile: token},
		{URL: "https://ork.example.com", Method: EngineAuthMTLS, CertFile: "c.pem", KeyFile: "k.pem"},
	}
	for _, s := range ok {
		if err := s.Validate(); err != nil {
			t.Errorf("%+v: %v", s, err)
		}
	}
	bad := map[string]EngineSettings{
		"plain http to a remote host": {URL: "http://ork.example.com", Method: EngineAuthTokenFile, TokenFile: token},
		"a URL with a path":           {URL: "https://ork.example.com/api", Method: EngineAuthTokenFile, TokenFile: token},
		"no method":                   {URL: "https://ork.example.com"},
		"an unknown method":           {URL: "https://ork.example.com", Method: "password"},
		"a token method with no file": {URL: "https://ork.example.com", Method: EngineAuthTokenFile},
		"mtls with no key":            {URL: "https://ork.example.com", Method: EngineAuthMTLS, CertFile: "c.pem"},
		"mtls over plain http":        {URL: "http://127.0.0.1:8080", Method: EngineAuthMTLS, CertFile: "c.pem", KeyFile: "k.pem"},
		"a method with no URL":        {Method: EngineAuthTokenFile, TokenFile: token},
		"a CA file for plain http":    {URL: "http://127.0.0.1:8080", CAFile: "ca.pem", Method: EngineAuthTokenFile, TokenFile: token},
	}
	for name, s := range bad {
		if err := s.Validate(); err == nil {
			t.Errorf("%s was accepted", name)
		}
	}
}

type whoamiEngine struct {
	orkstackv1connect.UnimplementedAuthServiceHandler
	bearer string
}

func (e *whoamiEngine) GetSignin(_ context.Context, req *connect.Request[orkstackv1.GetSigninRequest]) (*connect.Response[orkstackv1.GetSigninResponse], error) {
	e.bearer = req.Header().Get("Authorization")
	if e.bearer != "Bearer eyJ.x.y" {
		return nil, connect.NewError(connect.CodeUnauthenticated, nil)
	}
	return connect.NewResponse(&orkstackv1.GetSigninResponse{PrincipalId: "service:dev:orker-1", Method: "jwt"}), nil
}

func TestEngineClient_PresentsItsCredential(t *testing.T) {
	engine := &whoamiEngine{}
	mux := http.NewServeMux()
	mux.Handle(orkstackv1connect.NewAuthServiceHandler(engine))
	server := httptest.NewUnstartedServer(mux)
	server.Config.Protocols = new(http.Protocols)
	server.Config.Protocols.SetHTTP1(true)
	server.Config.Protocols.SetUnencryptedHTTP2(true)
	server.Start()
	t.Cleanup(server.Close)
	token := filepath.Join(t.TempDir(), "token")
	if err := os.WriteFile(token, []byte("eyJ.x.y\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	c, err := NewEngineClient(EngineSettings{URL: server.URL, Method: EngineAuthTokenFile, TokenFile: token})
	if err != nil {
		t.Fatal(err)
	}
	who, err := c.Whoami(context.Background())
	if err != nil || who.GetPrincipalId() != "service:dev:orker-1" {
		t.Fatalf("Whoami = %v, %v", who, err)
	}
	// A rotated token is read again.
	if err := os.WriteFile(token, []byte("eyJ.other\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Whoami(context.Background()); connect.CodeOf(err) != connect.CodeUnauthenticated || !strings.Contains(engine.bearer, "eyJ.other") {
		t.Fatalf("after rotation = %v, bearer %q; want the new token presented and refused", err, engine.bearer)
	}
	if _, err := NewEngineClient(EngineSettings{}); err == nil {
		t.Fatal("a client with no URL was built")
	}
}

// TestEngineSettings_CredentialFiles: the files that make the Orker's
// credential are its own sources, which no task may name as a secret.
func TestEngineSettings_CredentialFiles(t *testing.T) {
	s := EngineSettings{URL: "https://e", CAFile: "/ca.pem", Method: EngineAuthMTLS, CertFile: "/c.pem", KeyFile: "/k.pem", TokenFile: "/t"}
	got := s.CredentialFiles()
	if strings.Join(got, ",") != "/t,/c.pem,/k.pem" {
		t.Fatalf("CredentialFiles = %v", got)
	}
	if len((EngineSettings{}).CredentialFiles()) != 0 {
		t.Fatal("no settings named credential files")
	}
}
