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

// loopbackServer reports an http:// server on this host: the one place a
// bearer may travel without TLS.
func loopbackServer(server string) bool {
	u, err := url.Parse(server)
	if err != nil {
		return false
	}
	return authn.LoopbackHost(u.Hostname())
}
