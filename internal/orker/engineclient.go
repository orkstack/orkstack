package orker

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"

	"connectrpc.com/connect"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/gen/go/orkstack/v1/orkstackv1connect"
	"github.com/orkstack/orkstack/internal/authn"
)

// The orker.engine.* settings: how this Orker reaches the engine's API as
// itself (docs/DEPLOY.md §1; docs/AUTHENTICATION.md, "An Orker's
// credential"). Empty, the Orker reaches no engine and supports no
// identity.
const (
	EngineURLVar           = "ORK_ORKER_ENGINE_URL"
	EngineCAFileVar        = "ORK_ORKER_ENGINE_CA_FILE"
	EngineAuthMethodVar    = "ORK_ORKER_ENGINE_AUTH_METHOD"
	EngineAuthTokenFileVar = "ORK_ORKER_ENGINE_AUTH_TOKEN_FILE"
	EngineAuthCertFileVar  = "ORK_ORKER_ENGINE_AUTH_CERT_FILE"
	EngineAuthKeyFileVar   = "ORK_ORKER_ENGINE_AUTH_KEY_FILE"
	// EngineAuthServiceAccountVar names the service account the Orker signs
	// each request as, with orker.engine.auth.key_file.
	EngineAuthServiceAccountVar = "ORK_ORKER_ENGINE_AUTH_SERVICE_ACCOUNT"
)

// The methods an Orker authenticates with.
const (
	EngineAuthTokenFile      = "token_file"
	EngineAuthServiceAccount = "service_account"
	EngineAuthMTLS           = "mtls"
)

// EngineSettings are orker.engine.*.
type EngineSettings struct {
	URL       string
	CAFile    string
	Method    string
	TokenFile string
	CertFile  string
	KeyFile   string
	// ServiceAccount is the account the service_account method signs as.
	ServiceAccount string
}

// EngineSettingsFromEnv reads orker.engine.* through get.
func EngineSettingsFromEnv(get func(string) string) EngineSettings {
	trim := func(k string) string { return strings.TrimSpace(get(k)) }
	return EngineSettings{URL: trim(EngineURLVar), CAFile: trim(EngineCAFileVar), Method: trim(EngineAuthMethodVar),
		TokenFile: trim(EngineAuthTokenFileVar), CertFile: trim(EngineAuthCertFileVar), KeyFile: trim(EngineAuthKeyFileVar),
		ServiceAccount: trim(EngineAuthServiceAccountVar)}
}

// Configured reports whether the Orker reaches an engine at all.
func (s EngineSettings) Configured() bool { return s.URL != "" }

// Validate refuses settings an Orker must not boot with: a URL that is
// neither https nor loopback, a method that is not one, or a method without
// its files. Each refusal names its setting.
func (s EngineSettings) Validate() error {
	if s.URL == "" {
		if s.Method != "" || s.TokenFile != "" || s.CertFile != "" || s.KeyFile != "" || s.CAFile != "" || s.ServiceAccount != "" {
			return fmt.Errorf("orker.engine.auth.* is set without orker.engine.url (%s)", EngineURLVar)
		}
		return nil
	}
	u, err := url.Parse(s.URL)
	if err != nil || u.Host == "" || u.User != nil || (u.Path != "" && u.Path != "/") || u.RawQuery != "" || u.Fragment != "" {
		return fmt.Errorf("orker.engine.url (%s) %q is not a scheme and a host alone", EngineURLVar, s.URL)
	}
	switch u.Scheme {
	case "https":
	case "http":
		if !authn.LoopbackHost(u.Hostname()) {
			return fmt.Errorf("orker.engine.url (%s) %q is plain http to a host that is not loopback: a credential would cross the network in the clear", EngineURLVar, s.URL)
		}
		if s.CAFile != "" || s.Method == EngineAuthMTLS {
			return fmt.Errorf("orker.engine.ca_file and the mtls method are for an https engine; %q is not one", s.URL)
		}
	default:
		return fmt.Errorf("orker.engine.url (%s) %q is neither https nor http", EngineURLVar, s.URL)
	}
	switch s.Method {
	case EngineAuthTokenFile:
		if s.TokenFile == "" {
			return fmt.Errorf("orker.engine.auth.method token_file needs orker.engine.auth.token_file (%s)", EngineAuthTokenFileVar)
		}
	case EngineAuthServiceAccount:
		if s.ServiceAccount == "" || s.KeyFile == "" {
			return fmt.Errorf("orker.engine.auth.method service_account needs orker.engine.auth.service_account and key_file (%s, %s)",
				EngineAuthServiceAccountVar, EngineAuthKeyFileVar)
		}
		if s.ServiceAccount == authn.BreakGlassAccount {
			return fmt.Errorf("orker.engine.auth.service_account (%s) is never the break-glass account", EngineAuthServiceAccountVar)
		}
	case EngineAuthMTLS:
		if s.CertFile == "" || s.KeyFile == "" {
			return fmt.Errorf("orker.engine.auth.method mtls needs orker.engine.auth.cert_file and key_file (%s, %s)", EngineAuthCertFileVar, EngineAuthKeyFileVar)
		}
	case "":
		return fmt.Errorf("orker.engine.url is set and orker.engine.auth.method (%s) is not: token_file, service_account or mtls", EngineAuthMethodVar)
	default:
		return fmt.Errorf("orker.engine.auth.method (%s) %q is not token_file, service_account or mtls", EngineAuthMethodVar, s.Method)
	}
	return nil
}

// EngineClient is this Orker's client of the engine's API, presenting its
// own credential on every request.
type EngineClient struct {
	auth      orkstackv1connect.AuthServiceClient
	identity  orkstackv1connect.IdentityServiceClient
	attempts  orkstackv1connect.AttemptServiceClient
	transport *http.Transport
}

// Close releases the client's idle connections to the engine.
func (c *EngineClient) Close() { c.transport.CloseIdleConnections() }

// NewEngineClient builds the client the settings describe. It reads the
// TLS files now and the token file at every request, so a token something
// else rotates is presented as it is then.
func NewEngineClient(s EngineSettings) (*EngineClient, error) {
	if err := s.Validate(); err != nil {
		return nil, err
	}
	if !s.Configured() {
		return nil, errors.New("orker.engine.url is not set: this Orker reaches no engine")
	}
	transport := &http.Transport{Protocols: new(http.Protocols)}
	if strings.HasPrefix(s.URL, "https://") {
		transport.Protocols.SetHTTP1(true)
		transport.Protocols.SetHTTP2(true)
		cfg := &tls.Config{MinVersion: tls.VersionTLS12}
		if s.CAFile != "" {
			raw, err := os.ReadFile(s.CAFile)
			if err != nil {
				return nil, fmt.Errorf("orker.engine.ca_file: %w", err)
			}
			pool := x509.NewCertPool()
			if !pool.AppendCertsFromPEM(raw) {
				return nil, fmt.Errorf("orker.engine.ca_file %s holds no PEM certificate", s.CAFile)
			}
			cfg.RootCAs = pool
		}
		if s.Method == EngineAuthMTLS {
			pair, err := tls.LoadX509KeyPair(s.CertFile, s.KeyFile)
			if err != nil {
				return nil, fmt.Errorf("orker.engine.auth.cert_file and key_file: %w", err)
			}
			cfg.Certificates = []tls.Certificate{pair}
		}
		transport.TLSClientConfig = cfg
	} else {
		transport.Protocols.SetUnencryptedHTTP2(true)
	}
	var opts []connect.ClientOption
	if s.Method == EngineAuthTokenFile {
		opts = append(opts, connect.WithInterceptors(authn.BearerFromFile{Path: s.TokenFile, Label: "orker.engine.auth.token_file"}))
	}
	base := strings.TrimSuffix(s.URL, "/")
	var rt http.RoundTripper = transport
	if s.Method == EngineAuthServiceAccount {
		// The key is read once, at boot: a service account's key is the
		// Orker's own, never rotated under a running process.
		signer, err := authn.ReadSignerFile(s.KeyFile, "orker.engine.auth.key_file")
		if err != nil {
			return nil, err
		}
		// The audience is the engine's URL, which is then the deployment's
		// public URL (auth.public_url): the engine checks both.
		rt = &authn.AssertionTransport{Base: transport, Signer: signer, Account: s.ServiceAccount, Audience: base}
	}
	client := &http.Client{Transport: rt}
	return &EngineClient{
		auth:      orkstackv1connect.NewAuthServiceClient(client, base, opts...),
		identity:  orkstackv1connect.NewIdentityServiceClient(client, base, opts...),
		attempts:  orkstackv1connect.NewAttemptServiceClient(client, base, opts...),
		transport: transport,
	}, nil
}

// Whoami asks the engine which principal this Orker's credential is, and
// what it holds (AuthService.GetSignin): the boot check that the credential
// is accepted.
func (c *EngineClient) Whoami(ctx context.Context) (*orkstackv1.GetSigninResponse, error) {
	resp, err := c.auth.GetSignin(ctx, connect.NewRequest(&orkstackv1.GetSigninRequest{}))
	if err != nil {
		return nil, err
	}
	return resp.Msg, nil
}

// Mint asks the engine for an attempt's identity tokens
// (IdentityService.MintAttemptTokens). The request names no Orker: the
// engine takes it from the credential this client presents. A refusal is
// returned as the engine's connect error, unchanged, for the caller to map.
func (c *EngineClient) Mint(ctx context.Context, req *orkstackv1.MintAttemptTokensRequest) (*orkstackv1.MintAttemptTokensResponse, error) {
	resp, err := c.identity.MintAttemptTokens(ctx, connect.NewRequest(req))
	if err != nil {
		return nil, err
	}
	return resp.Msg, nil
}

// CommitResult asks the engine to record an attempt's result, or with
// preflight set whether it may (AttemptService.CommitResult). As with Mint,
// the engine takes the Orker from the credential, and a refusal is returned
// as the engine's connect error, unchanged, for the caller to map.
func (c *EngineClient) CommitResult(ctx context.Context, req *orkstackv1.CommitResultRequest) (*orkstackv1.CommitResultResponse, error) {
	resp, err := c.attempts.CommitResult(ctx, connect.NewRequest(req))
	if err != nil {
		return nil, err
	}
	return resp.Msg, nil
}

// WhoamiDated is Whoami, with the time the engine answered at from its
// answer's Date header: zero when the header was missing or unreadable.
func (c *EngineClient) WhoamiDated(ctx context.Context) (*orkstackv1.GetSigninResponse, time.Time, error) {
	resp, err := c.auth.GetSignin(ctx, connect.NewRequest(&orkstackv1.GetSigninRequest{}))
	if err != nil {
		return nil, time.Time{}, err
	}
	return resp.Msg, parseEngineDate(resp.Header().Get("Date")), nil
}

// CredentialFiles are the files that make this Orker's credential: its own
// sources, which its secret floor admits as no task's (AGENTS.md §6).
func (s EngineSettings) CredentialFiles() []string {
	var out []string
	for _, p := range []string{s.TokenFile, s.CertFile, s.KeyFile} {
		if p != "" {
			out = append(out, p)
		}
	}
	return out
}
