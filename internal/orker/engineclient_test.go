package orker

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"connectrpc.com/connect"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/gen/go/orkstack/v1/orkstackv1connect"
	"github.com/orkstack/orkstack/internal/jwt"
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
		{URL: "https://ork.example.com", Method: EngineAuthServiceAccount, ServiceAccount: "gpu-1", KeyFile: "k.pem"},
	}
	for _, s := range ok {
		if err := s.Validate(); err != nil {
			t.Errorf("%+v: %v", s, err)
		}
	}
	bad := map[string]EngineSettings{
		"plain http to a remote host":   {URL: "http://ork.example.com", Method: EngineAuthTokenFile, TokenFile: token},
		"a URL with a path":             {URL: "https://ork.example.com/api", Method: EngineAuthTokenFile, TokenFile: token},
		"no method":                     {URL: "https://ork.example.com"},
		"an unknown method":             {URL: "https://ork.example.com", Method: "password"},
		"a token method with no file":   {URL: "https://ork.example.com", Method: EngineAuthTokenFile},
		"mtls with no key":              {URL: "https://ork.example.com", Method: EngineAuthMTLS, CertFile: "c.pem"},
		"a service account with no key": {URL: "https://ork.example.com", Method: EngineAuthServiceAccount, ServiceAccount: "gpu-1"},
		"a key with no service account": {URL: "https://ork.example.com", Method: EngineAuthServiceAccount, KeyFile: "k.pem"},
		"the break-glass account":       {URL: "https://ork.example.com", Method: EngineAuthServiceAccount, ServiceAccount: "break-glass", KeyFile: "k.pem"},
		"a service account with no URL": {ServiceAccount: "gpu-1"},
		"mtls over plain http":          {URL: "http://127.0.0.1:8080", Method: EngineAuthMTLS, CertFile: "c.pem", KeyFile: "k.pem"},
		"a method with no URL":          {Method: EngineAuthTokenFile, TokenFile: token},
		"a CA file for plain http":      {URL: "http://127.0.0.1:8080", CAFile: "ca.pem", Method: EngineAuthTokenFile, TokenFile: token},
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

// TestEngineClient_SignsAsItsServiceAccount: the service_account method
// sends a fresh assertion of its account with every request.
func TestEngineClient_SignsAsItsServiceAccount(t *testing.T) {
	var seen []string
	mux := http.NewServeMux()
	mux.Handle(orkstackv1connect.NewAuthServiceHandler(&signinRecorder{seen: &seen}))
	server := httptest.NewUnstartedServer(mux)
	server.Config.Protocols = new(http.Protocols)
	server.Config.Protocols.SetHTTP1(true)
	server.Config.Protocols.SetUnencryptedHTTP2(true)
	server.Start()
	t.Cleanup(server.Close)
	k, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	der, _ := x509.MarshalPKCS8PrivateKey(k)
	key := filepath.Join(t.TempDir(), "sa.pem")
	if err := os.WriteFile(key, pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: der}), 0o600); err != nil {
		t.Fatal(err)
	}
	c, err := NewEngineClient(EngineSettings{URL: server.URL, Method: EngineAuthServiceAccount, ServiceAccount: "gpu-1", KeyFile: key})
	if err != nil {
		t.Fatal(err)
	}
	for range 2 {
		if _, err := c.Whoami(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	if len(seen) != 2 || seen[0] == seen[1] {
		t.Fatalf("bearers = %q; want two different assertions", seen)
	}
	for _, b := range seen {
		if iss, err := jwt.PeekIssuer(strings.TrimPrefix(b, "Bearer ")); err != nil || iss != "serviceaccount:gpu-1" {
			t.Errorf("issuer = %q, %v", iss, err)
		}
	}
	if _, err := NewEngineClient(EngineSettings{URL: server.URL, Method: EngineAuthServiceAccount, ServiceAccount: "gpu-1",
		KeyFile: filepath.Join(t.TempDir(), "missing")}); err == nil {
		t.Error("a client with no readable key was built")
	}
}

type signinRecorder struct {
	orkstackv1connect.UnimplementedAuthServiceHandler
	seen *[]string
}

func (e *signinRecorder) GetSignin(_ context.Context, req *connect.Request[orkstackv1.GetSigninRequest]) (*connect.Response[orkstackv1.GetSigninResponse], error) {
	*e.seen = append(*e.seen, req.Header().Get("Authorization"))
	return connect.NewResponse(&orkstackv1.GetSigninResponse{PrincipalId: "serviceaccount:gpu-1"}), nil
}

type mintEngine struct {
	orkstackv1connect.UnimplementedIdentityServiceHandler
	bearer string
	got    *orkstackv1.MintAttemptTokensRequest
}

func (e *mintEngine) MintAttemptTokens(_ context.Context, req *connect.Request[orkstackv1.MintAttemptTokensRequest]) (*connect.Response[orkstackv1.MintAttemptTokensResponse], error) {
	e.bearer = req.Header().Get("Authorization")
	e.got = req.Msg
	if req.Msg.GetAttempt() == 9 {
		return nil, connect.NewError(connect.CodeFailedPrecondition, errors.New("IDENTITY_ATTEMPTS_EXHAUSTED"))
	}
	return connect.NewResponse(&orkstackv1.MintAttemptTokensResponse{Tokens: []*orkstackv1.IssuedToken{{Name: "aws", Audience: "sts", Token: "eyJ.t.s", ExpiresInSeconds: 600}}}), nil
}

// TestEngineClient_Mint: the Orker asks the engine for its attempt's tokens
// with its own credential, and the engine's refusal reaches it unchanged.
func TestEngineClient_Mint(t *testing.T) {
	engine := &mintEngine{}
	mux := http.NewServeMux()
	mux.Handle(orkstackv1connect.NewIdentityServiceHandler(engine))
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
	t.Cleanup(c.Close)
	resp, err := c.Mint(context.Background(), &orkstackv1.MintAttemptTokensRequest{Namespace: "ns", RunId: "ork-1", TaskId: "build", Attempt: 1})
	if err != nil || len(resp.GetTokens()) != 1 || resp.GetTokens()[0].GetToken() != "eyJ.t.s" {
		t.Fatalf("Mint = %v, %v", resp, err)
	}
	if engine.bearer != "Bearer eyJ.x.y" || engine.got.GetRunId() != "ork-1" || engine.got.GetTaskId() != "build" {
		t.Fatalf("the engine saw bearer %q, request %v", engine.bearer, engine.got)
	}
	if _, err := c.Mint(context.Background(), &orkstackv1.MintAttemptTokensRequest{Attempt: 9}); connect.CodeOf(err) != connect.CodeFailedPrecondition || !strings.Contains(err.Error(), "IDENTITY_ATTEMPTS_EXHAUSTED") {
		t.Fatalf("a refusal = %v", err)
	}
}

// TestEngineClient_WhoamiDated: the engine's answer's Date header is the
// time it answered at.
func TestEngineClient_WhoamiDated(t *testing.T) {
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
	t.Cleanup(c.Close)
	who, date, err := c.WhoamiDated(context.Background())
	if err != nil || who.GetPrincipalId() == "" {
		t.Fatalf("= %v, %v", who, err)
	}
	if date.IsZero() || time.Since(date).Abs() > time.Minute {
		t.Fatalf("the engine's Date %v is not about now", date)
	}
}

// commitEngine answers CommitResult as an engine's AttemptService does.
type commitEngine struct {
	orkstackv1connect.UnimplementedAttemptServiceHandler
	bearer string
	got    *orkstackv1.CommitResultRequest
}

func (e *commitEngine) CommitResult(_ context.Context, req *connect.Request[orkstackv1.CommitResultRequest]) (*connect.Response[orkstackv1.CommitResultResponse], error) {
	e.bearer, e.got = req.Header().Get("Authorization"), req.Msg
	if req.Msg.GetAttempt() == 9 {
		return nil, connect.NewError(connect.CodeFailedPrecondition, errors.New("COMMIT_IDENTITY_MISMATCH: the result is not of the attempt the request names"))
	}
	return connect.NewResponse(&orkstackv1.CommitResultResponse{Created: true, Commit: &orkstackv1.CommitRef{Uri: "s3://b/metadata/attested/ns/ork-1/build/commit.json"}}), nil
}

// TestEngineClient_CommitResult: the Orker hands the engine its attempt's
// result with its own credential, and the engine's refusal reaches it
// unchanged.
func TestEngineClient_CommitResult(t *testing.T) {
	engine := &commitEngine{}
	mux := http.NewServeMux()
	mux.Handle(orkstackv1connect.NewAttemptServiceHandler(engine))
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
	t.Cleanup(c.Close)
	resp, err := c.CommitResult(context.Background(), &orkstackv1.CommitResultRequest{Namespace: "ns", RunId: "ork-1", TaskId: "build", Attempt: 1})
	if err != nil || !resp.GetCreated() || !strings.Contains(resp.GetCommit().GetUri(), "/metadata/attested/") {
		t.Fatalf("CommitResult = %v, %v", resp, err)
	}
	if engine.bearer != "Bearer eyJ.x.y" || engine.got.GetRunId() != "ork-1" {
		t.Fatalf("the engine saw bearer %q, request %v", engine.bearer, engine.got)
	}
	if _, err := c.CommitResult(context.Background(), &orkstackv1.CommitResultRequest{Attempt: 9}); connect.CodeOf(err) != connect.CodeFailedPrecondition ||
		!strings.Contains(err.Error(), "COMMIT_IDENTITY_MISMATCH") {
		t.Fatalf("a refusal = %v", err)
	}
}
