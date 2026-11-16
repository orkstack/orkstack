package main

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"net/url"
	"os"
	"strings"

	"github.com/orkstack/orkstack/internal/authn"
	"github.com/orkstack/orkstack/internal/jwt"
)

// serviceAccountEnvVar and serviceAccountKeyEnvVar name the service account
// orkctl signs each request as, and the file holding its private key.
const (
	serviceAccountEnvVar    = "ORK_SERVICE_ACCOUNT"
	serviceAccountKeyEnvVar = "ORK_SERVICE_ACCOUNT_KEY"
	breakGlassReasonEnvVar  = "ORK_BREAK_GLASS_REASON"
)

// tokenFileEnvVar names a file holding the bearer token orkctl presents: a
// JWT that something else rotates, which is how CI passes a GitHub or
// Kubernetes token (docs/AUTHENTICATION.md, "From orkctl").
const tokenFileEnvVar = "ORK_TOKEN_FILE"

// cliCredentials are how orkctl authenticates and which roots it trusts.
type cliCredentials struct {
	tokenFile  string
	caFile     string
	clientCert string
	clientKey  string
	// serviceAccount and serviceAccountKey sign one assertion a request
	// (authn-authz.md §3.2); reason is Ork-Reason, which break-glass needs.
	serviceAccount    string
	serviceAccountKey string
	reason            string
	signer            *jwt.Signer
	// tls is the client TLS configuration an https server gets; nil means
	// the system's roots and no client certificate.
	tls *tls.Config
}

// resolveCredentials checks the credential flags and builds the TLS
// configuration they ask for. A client certificate takes precedence over a
// token file: with both, only the certificate is presented.
func (g *globalFlags) resolveCredentials() error {
	c := &g.creds
	if c.tokenFile == "" {
		c.tokenFile = os.Getenv(tokenFileEnvVar)
	}
	if c.tokenFile != "" && !strings.HasPrefix(g.server, "https://") && !loopbackServer(g.server) {
		return fmt.Errorf("a token file is presented only to an https:// server or a loopback http one: "+
			"%s would carry the token in the clear", g.server)
	}
	if (c.clientCert == "") != (c.clientKey == "") {
		return errors.New("--client-cert and --client-key are given together")
	}
	if err := g.resolveServiceAccount(); err != nil {
		return err
	}
	if c.caFile == "" && c.clientCert == "" {
		return nil
	}
	if !strings.HasPrefix(g.server, "https://") {
		return fmt.Errorf("--ca-file and --client-cert are for an https:// server; %s is not one", g.server)
	}
	cfg := &tls.Config{MinVersion: tls.VersionTLS12}
	if c.caFile != "" {
		raw, err := os.ReadFile(c.caFile)
		if err != nil {
			return fmt.Errorf("--ca-file: %w", err)
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(raw) {
			return fmt.Errorf("--ca-file %s holds no PEM certificate", c.caFile)
		}
		cfg.RootCAs = pool
	}
	if c.clientCert != "" {
		pair, err := tls.LoadX509KeyPair(c.clientCert, c.clientKey)
		if err != nil {
			return fmt.Errorf("--client-cert and --client-key: %w", err)
		}
		cfg.Certificates = []tls.Certificate{pair}
	}
	c.tls = cfg
	return nil
}

// resolveServiceAccount reads --service-account and --service-account-key
// (or their environment variables), which are given together. A client
// certificate or a token file takes precedence: with either, no assertion
// is signed (authn-authz.md §9).
func (g *globalFlags) resolveServiceAccount() error {
	c := &g.creds
	if c.serviceAccount == "" {
		c.serviceAccount = os.Getenv(serviceAccountEnvVar)
	}
	if c.serviceAccountKey == "" {
		c.serviceAccountKey = os.Getenv(serviceAccountKeyEnvVar)
	}
	if c.reason == "" {
		c.reason = os.Getenv(breakGlassReasonEnvVar)
	}
	if (c.serviceAccount == "") != (c.serviceAccountKey == "") {
		return errors.New("--service-account and --service-account-key are given together")
	}
	if c.serviceAccount == "" {
		if c.reason != "" {
			return errors.New("--break-glass-reason is for a break-glass key (--service-account " + authn.BreakGlassAccount + ")")
		}
		return nil
	}
	if c.clientCert != "" || c.tokenFile != "" {
		if g.stderr != nil {
			_, _ = fmt.Fprintln(g.stderr, "warning: a client certificate or token file is given, and is sent instead of the service account")
		}
		return nil
	}
	if c.serviceAccount == authn.BreakGlassAccount && c.reason == "" {
		return errors.New("a break-glass request needs --break-glass-reason: why the key is used")
	}
	if c.serviceAccount != authn.BreakGlassAccount && c.reason != "" {
		return errors.New("--break-glass-reason is for a break-glass key (--service-account " + authn.BreakGlassAccount + ")")
	}
	if !strings.HasPrefix(g.server, "https://") && !loopbackServer(g.server) {
		return fmt.Errorf("a service account signs only for an https:// server or a loopback http one: %s", g.server)
	}
	signer, err := authn.ReadSignerFile(c.serviceAccountKey, "--service-account-key")
	if err != nil {
		return err
	}
	c.signer = signer
	return nil
}

// loopbackServer reports an http:// server on this host: the one place a
// bearer may travel without TLS.
func loopbackServer(server string) bool {
	u, err := url.Parse(server)
	if err != nil {
		return false
	}
	return authn.LoopbackHost(u.Hostname())
}
