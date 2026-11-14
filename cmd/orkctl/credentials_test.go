package main

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"connectrpc.com/connect"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/gen/go/orkstack/v1/orkstackv1connect"
)

// recordingEngine answers GetSystemInfo and remembers what each request
// presented.
type recordingEngine struct {
	orkstackv1connect.UnimplementedSystemServiceHandler
	mu     sync.Mutex
	bearer []string
}

func (e *recordingEngine) GetSystemInfo(_ context.Context, req *connect.Request[orkstackv1.GetSystemInfoRequest]) (*connect.Response[orkstackv1.GetSystemInfoResponse], error) {
	e.mu.Lock()
	e.bearer = append(e.bearer, req.Header().Get("Authorization"))
	e.mu.Unlock()
	return connect.NewResponse(&orkstackv1.GetSystemInfoResponse{}), nil
}

func writeFile(t *testing.T, name, body string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), name)
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestCredentials_ATokenFileIsSentAsABearer(t *testing.T) {
	engine := &recordingEngine{}
	mux := http.NewServeMux()
	mux.Handle(orkstackv1connect.NewSystemServiceHandler(engine))
	url := startFakeEngine(t, mux)
	token := writeFile(t, "token", "eyJ.test.token\n")
	var out, errOut strings.Builder
	if code := Execute([]string{"admin", "info", "--server", url, "--token-file", token}, &out, &errOut); code != exitOK {
		t.Fatalf("admin info --token-file = %d: %s", code, errOut.String())
	}
	t.Setenv(tokenFileEnvVar, token)
	if code := Execute([]string{"admin", "info", "--server", url}, &out, &errOut); code != exitOK {
		t.Fatalf("admin info with %s = %d: %s", tokenFileEnvVar, code, errOut.String())
	}
	t.Setenv(tokenFileEnvVar, "")
	if code := Execute([]string{"admin", "info", "--server", url}, &out, &errOut); code != exitOK {
		t.Fatal(errOut.String())
	}
	engine.mu.Lock()
	defer engine.mu.Unlock()
	if got := strings.Join(engine.bearer, "|"); got != "Bearer eyJ.test.token|Bearer eyJ.test.token|" {
		t.Fatalf("Authorization headers = %q; want the file's token twice, then none", got)
	}
	errOut.Reset()
	if code := Execute([]string{"admin", "info", "--server", "http://engine.example.com:8080", "--token-file", token}, &out, &errOut); code == exitOK || !strings.Contains(errOut.String(), "clear") {
		t.Fatalf("a token file for a remote plain-http server = %d %q; want a refusal", code, errOut.String())
	}
	empty := writeFile(t, "empty", "\n")
	errOut.Reset()
	if code := Execute([]string{"admin", "info", "--server", url, "--token-file", empty}, &out, &errOut); code == exitOK || !strings.Contains(errOut.String(), "empty") {
		t.Fatalf("an empty token file = %d %q; want a refusal", code, errOut.String())
	}
}

// testCA is a CA and a leaf it signed, for a TLS server and a client.
func testCertificate(t *testing.T, ca *x509.Certificate, caKey *ecdsa.PrivateKey, isCA bool, name string) (*x509.Certificate, *ecdsa.PrivateKey, []byte, []byte) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{SerialNumber: big.NewInt(time.Now().UnixNano()), Subject: pkix.Name{CommonName: name},
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour), DNSNames: []string{"127.0.0.1"},
		IPAddresses: nil, KeyUsage: x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
		ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth}, BasicConstraintsValid: true, IsCA: isCA}
	parent, signer := tmpl, key
	if ca != nil {
		parent, signer = ca, caKey
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, parent, &key.PublicKey, signer)
	if err != nil {
		t.Fatal(err)
	}
	cert, _ := x509.ParseCertificate(der)
	keyDER, _ := x509.MarshalPKCS8PrivateKey(key)
	return cert, key, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyDER})
}

func TestCredentials_ACAFileAndAClientCertificate(t *testing.T) {
	ca, caKey, caPEM, _ := testCertificate(t, nil, nil, true, "test-ca")
	var clientName string
	var mu sync.Mutex
	mux := http.NewServeMux()
	mux.Handle(orkstackv1connect.NewSystemServiceHandler(&recordingEngine{}))
	server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		if r.TLS != nil && len(r.TLS.PeerCertificates) > 0 {
			clientName = r.TLS.PeerCertificates[0].Subject.CommonName
		}
		mu.Unlock()
		mux.ServeHTTP(w, r)
	}))
	pool := x509.NewCertPool()
	pool.AddCert(ca)
	server.TLS = &tls.Config{ClientAuth: tls.VerifyClientCertIfGiven, ClientCAs: pool}
	server.StartTLS()
	t.Cleanup(server.Close)
	t.Setenv(serverEnvVar, "")
	t.Setenv(namespaceEnvVar, "")
	t.Setenv(principalEnvVar, "")
	t.Setenv(tokenFileEnvVar, "")
	t.Setenv(configEnvVar, t.TempDir()+"/config.json")

	// httptest's own certificate is the server's; trust it by its CA file.
	serverCA := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: server.Certificate().Raw})
	caFile := writeFile(t, "server-ca.pem", string(serverCA))
	var out, errOut strings.Builder
	if code := Execute([]string{"admin", "info", "--server", server.URL}, &out, &errOut); code == exitOK {
		t.Fatal("a server whose certificate no trusted root signed was accepted")
	}
	errOut.Reset()
	if code := Execute([]string{"admin", "info", "--server", server.URL, "--ca-file", caFile}, &out, &errOut); code != exitOK {
		t.Fatalf("admin info --ca-file = %d: %s", code, errOut.String())
	}
	_, _, certPEM, keyPEM := testCertificate(t, ca, caKey, false, "orker-1")
	certFile, keyFile := writeFile(t, "client.pem", string(certPEM)), writeFile(t, "client.key", string(keyPEM))
	if code := Execute([]string{"admin", "info", "--server", server.URL, "--ca-file", caFile, "--client-cert", certFile, "--client-key", keyFile}, &out, &errOut); code != exitOK {
		t.Fatalf("admin info --client-cert = %d: %s", code, errOut.String())
	}
	mu.Lock()
	got := clientName
	mu.Unlock()
	if got != "orker-1" {
		t.Fatalf("the server saw client certificate %q; want orker-1", got)
	}
	_ = caPEM
	for name, args := range map[string][]string{
		"a client certificate with no key":         {"--client-cert", certFile},
		"TLS files for a server that is not https": {"--ca-file", caFile, "--server", "http://127.0.0.1:1"},
	} {
		errOut.Reset()
		full := append([]string{"admin", "info", "--server", server.URL}, args...)
		if code := Execute(full, &out, &errOut); code == exitOK {
			t.Errorf("%s was accepted", name)
		}
	}
}
