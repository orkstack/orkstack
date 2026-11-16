// Command oidctest serves internal/testsupport/oidctest over TLS: the
// development stack's identity provider for sign-in (make up IDENTITY=1).
// It signs in one user, named by its flags, with no page of its own, and
// is never a production issuer.
//
//	oidctest -dir .data/identity/oidc -addr 127.0.0.1:5556 \
//	  -redirect http://localhost:5173/v1/auth/callback/people \
//	  -email dev@example.test -groups dev-admins
//
// On first start it writes a CA and a server certificate for 127.0.0.1 and
// localhost under -dir (ca.pem is what the engine's ca_file and orkctl's
// --issuer-ca-file trust), and the web client's secret (web-client.secret).
package main

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/base64"
	"encoding/json/v2"
	"encoding/pem"
	"errors"
	"flag"
	"fmt"
	"log"
	"math/big"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/orkstack/orkstack/internal/testsupport/oidctest"
)

func main() {
	if err := run(os.Args[1:]); err != nil {
		log.Fatal(err)
	}
}

func run(args []string) error {
	fs := flag.NewFlagSet("oidctest", flag.ContinueOnError)
	dir := fs.String("dir", "", "where the CA, the certificate and the web client's secret are kept")
	addr := fs.String("addr", "127.0.0.1:5556", "the address to serve on")
	redirect := fs.String("redirect", "", "the web client's redirect URI (the engine's /v1/auth/callback/NAME)")
	webClient := fs.String("web-client", "orkstack-web", "the web client's id")
	cliClient := fs.String("cli-client", "orkctl", "the terminal's client id (a public client, loopback redirects)")
	email := fs.String("email", "dev@example.test", "the user it signs in")
	groups := fs.String("groups", "", "the user's groups, comma-separated")
	control := fs.Bool("control", false, "serve POST /_control/user ({email, groups}) to change the user it signs in: for tests only")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *dir == "" || *redirect == "" {
		return errors.New("-dir and -redirect are required")
	}
	if err := os.MkdirAll(*dir, 0o700); err != nil {
		return err
	}
	cert, err := ensureTLS(*dir)
	if err != nil {
		return err
	}
	secret, err := ensureSecret(filepath.Join(*dir, "web-client.secret"))
	if err != nil {
		return err
	}
	var gs []string
	for _, g := range strings.Split(*groups, ",") {
		if g = strings.TrimSpace(g); g != "" {
			gs = append(gs, g)
		}
	}
	p, err := oidctest.New("https://"+*addr, oidctest.Options{
		User: oidctest.User{Subject: *email, Email: *email, Groups: gs},
		Clients: []oidctest.Client{
			{ID: *webClient, Secret: secret, RedirectURIs: []string{*redirect}},
			{ID: *cliClient, RedirectURIs: []string{"http://127.0.0.1/callback"}},
		},
		TokenLifetime:     time.Hour,
		AutoApproveDevice: true,
	})
	if err != nil {
		return err
	}
	ln, err := net.Listen("tcp", *addr)
	if err != nil {
		return err
	}
	log.Printf("oidctest: issuer https://%s signs in %s", *addr, *email)
	var handler http.Handler = p
	if *control {
		mux := http.NewServeMux()
		mux.Handle("/", p)
		mux.HandleFunc("POST /_control/user", func(w http.ResponseWriter, r *http.Request) {
			var u struct {
				Email  string   `json:"email"`
				Groups []string `json:"groups"`
			}
			if err := json.UnmarshalRead(r.Body, &u); err != nil || u.Email == "" {
				http.Error(w, "want {\"email\": ..., \"groups\": [...]}", http.StatusBadRequest)
				return
			}
			p.SetUser(oidctest.User{Subject: u.Email, Email: u.Email, Groups: u.Groups})
			w.WriteHeader(http.StatusNoContent)
		})
		handler = mux
	}
	srv := &http.Server{Handler: handler, ReadHeaderTimeout: 10 * time.Second,
		TLSConfig: &tls.Config{MinVersion: tls.VersionTLS12, Certificates: []tls.Certificate{cert}}}
	return srv.ServeTLS(ln, "", "")
}

// ensureSecret keeps one random secret in path.
func ensureSecret(path string) (string, error) {
	if raw, err := os.ReadFile(path); err == nil {
		return strings.TrimSpace(string(raw)), nil
	}
	b := make([]byte, 24)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	s := base64.RawURLEncoding.EncodeToString(b)
	return s, os.WriteFile(path, []byte(s+"\n"), 0o600)
}

// ensureTLS keeps a CA and a server certificate for 127.0.0.1 and
// localhost under dir, made once.
func ensureTLS(dir string) (tls.Certificate, error) {
	certPath, keyPath := filepath.Join(dir, "server.pem"), filepath.Join(dir, "server-key.pem")
	if pair, err := tls.LoadX509KeyPair(certPath, keyPath); err == nil {
		return pair, nil
	}
	caKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return tls.Certificate{}, err
	}
	now := time.Now()
	caTmpl := &x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "oidctest development CA"},
		NotBefore: now.Add(-time.Hour), NotAfter: now.AddDate(10, 0, 0), IsCA: true, BasicConstraintsValid: true,
		KeyUsage: x509.KeyUsageCertSign}
	caDER, err := x509.CreateCertificate(rand.Reader, caTmpl, caTmpl, &caKey.PublicKey, caKey)
	if err != nil {
		return tls.Certificate{}, err
	}
	ca, _ := x509.ParseCertificate(caDER)
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return tls.Certificate{}, err
	}
	tmpl := &x509.Certificate{SerialNumber: big.NewInt(2), Subject: pkix.Name{CommonName: "oidctest"},
		NotBefore: now.Add(-time.Hour), NotAfter: now.AddDate(10, 0, 0), DNSNames: []string{"localhost"},
		IPAddresses: []net.IP{net.ParseIP("127.0.0.1")}, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		KeyUsage: x509.KeyUsageDigitalSignature}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, ca, &key.PublicKey, caKey)
	if err != nil {
		return tls.Certificate{}, err
	}
	keyDER, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		return tls.Certificate{}, err
	}
	for path, block := range map[string]*pem.Block{
		filepath.Join(dir, "ca.pem"): {Type: "CERTIFICATE", Bytes: caDER},
		certPath:                     {Type: "CERTIFICATE", Bytes: der},
		keyPath:                      {Type: "PRIVATE KEY", Bytes: keyDER},
	} {
		if err := os.WriteFile(path, pem.EncodeToMemory(block), 0o600); err != nil {
			return tls.Certificate{}, fmt.Errorf("write %s: %w", path, err)
		}
	}
	return tls.LoadX509KeyPair(certPath, keyPath)
}
