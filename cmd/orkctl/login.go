package main

import (
	"context"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"encoding/json/v2"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"time"

	"connectrpc.com/connect"
	"github.com/spf13/cobra"
	"golang.org/x/oauth2"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/gen/go/orkstack/v1/orkstackv1connect"
	"github.com/orkstack/orkstack/internal/authn"
)

// credentialsEnvVar overrides where orkctl keeps its logins: for tests, and
// for a user who keeps them somewhere else.
const credentialsEnvVar = "ORK_CREDENTIALS"

// loginTimeout bounds how long `orkctl login` waits for the browser or the
// device's approval.
const loginTimeout = 5 * time.Minute

// refreshAhead is how close to expiry a stored ID token is refreshed before
// it is sent.
const refreshAhead = time.Minute

// storedLogin is one server's login: the issuer's tokens and where to
// refresh and revoke them. Its ID token is sent as a bearer.
type storedLogin struct {
	Issuer             string    `json:"issuer"`
	IssuerURL          string    `json:"issuer_url"`
	ClientID           string    `json:"client_id"`
	IDToken            string    `json:"id_token"`
	RefreshToken       string    `json:"refresh_token,omitzero"`
	ExpiresAt          time.Time `json:"expires_at"`
	TokenEndpoint      string    `json:"token_endpoint"`
	RevocationEndpoint string    `json:"revocation_endpoint,omitzero"`
	IssuerCAFile       string    `json:"issuer_ca_file,omitzero"`
}

type credentialsFile struct {
	Servers map[string]storedLogin `json:"servers"`
}

// credentialsMu serialises this process's reads and writes of the file;
// withCredentials adds a lock beside the file for other processes.
var credentialsMu sync.Mutex

// withCredentials runs fn holding the logins against this process and any
// other orkctl: a read, a refresh and its write happen as one, so two
// processes never redeem one rotating refresh token, and one never writes
// back what another changed meanwhile.
func withCredentials(fn func(f credentialsFile, path string) error) error {
	credentialsMu.Lock()
	defer credentialsMu.Unlock()
	path, err := credentialsPath()
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return err
	}
	lock, err := os.OpenFile(path+".lock", os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return fmt.Errorf("lock %s: %w", path, err)
	}
	defer lock.Close()
	if err := lockFile(lock); err != nil {
		return fmt.Errorf("lock %s: %w", path, err)
	}
	defer func() { _ = unlockFile(lock) }()
	f, path, err := loadCredentials()
	if err != nil {
		return err
	}
	return fn(f, path)
}

func credentialsPath() (string, error) {
	if override := os.Getenv(credentialsEnvVar); override != "" {
		return override, nil
	}
	dir, err := os.UserConfigDir()
	if err != nil {
		return "", fmt.Errorf("locate the user config directory: %w", err)
	}
	return filepath.Join(dir, "orkctl", "credentials.json"), nil
}

// loadCredentials reads the logins. A missing file holds none; a file
// another user can read is refused, as ssh refuses a key it could leak.
func loadCredentials() (credentialsFile, string, error) {
	path, err := credentialsPath()
	if err != nil {
		return credentialsFile{}, "", err
	}
	info, err := os.Stat(path)
	if errors.Is(err, os.ErrNotExist) {
		return credentialsFile{Servers: map[string]storedLogin{}}, path, nil
	}
	if err != nil {
		return credentialsFile{}, path, err
	}
	if runtime.GOOS != "windows" && info.Mode().Perm()&0o077 != 0 {
		return credentialsFile{}, path, fmt.Errorf("%s is readable by others (mode %v): chmod 600 it, or delete it and log in again", path, info.Mode().Perm())
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		return credentialsFile{}, path, err
	}
	var f credentialsFile
	if err := json.Unmarshal(raw, &f); err != nil {
		return credentialsFile{}, path, fmt.Errorf("parse %s: %w", path, err)
	}
	if f.Servers == nil {
		f.Servers = map[string]storedLogin{}
	}
	return f, path, nil
}

// saveCredentials writes the logins 0600, through a file renamed into
// place, so a crash never leaves half a file.
func saveCredentials(f credentialsFile, path string) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return err
	}
	raw, err := json.Marshal(f)
	if err != nil {
		return err
	}
	tmp, err := os.CreateTemp(filepath.Dir(path), ".credentials-*")
	if err != nil {
		return err
	}
	defer os.Remove(tmp.Name())
	if err := tmp.Chmod(0o600); err != nil {
		_ = tmp.Close()
		return err
	}
	if _, err := tmp.Write(append(raw, '\n')); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	return os.Rename(tmp.Name(), path)
}

// serverKey is the server a login belongs to, as --server names it.
func serverKey(server string) string { return strings.TrimSuffix(server, "/") }

// issuerHTTPClient trusts the system's roots, or caFile's, and follows no
// redirect: an issuer's endpoints are read where discovery says.
func issuerHTTPClient(caFile string) (*http.Client, error) {
	cfg := &tls.Config{MinVersion: tls.VersionTLS12}
	if caFile != "" {
		raw, err := os.ReadFile(caFile)
		if err != nil {
			return nil, fmt.Errorf("--issuer-ca-file: %w", err)
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(raw) {
			return nil, fmt.Errorf("--issuer-ca-file %s holds no PEM certificate", caFile)
		}
		cfg.RootCAs = pool
	}
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.TLSClientConfig = cfg
	return &http.Client{Transport: transport, Timeout: 30 * time.Second,
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}, nil
}

// openBrowser opens a URL in the user's browser; tests replace it.
var openBrowser = func(u string) error {
	var cmd *exec.Cmd
	switch runtime.GOOS {
	case "darwin":
		cmd = exec.Command("open", u)
	case "windows":
		cmd = exec.Command("rundll32", "url.dll,FileProtocolHandler", u)
	default:
		cmd = exec.Command("xdg-open", u)
	}
	return cmd.Start()
}

func newLoginCommand(g *globalFlags) *cobra.Command {
	var issuerName, issuerCA string
	var device, noBrowser bool
	cmd := &cobra.Command{
		Use:   "login",
		Short: "Sign in to the server as yourself, with its identity provider",
		Long: "Sign in with one of the server's identity providers: by default in a browser, with a redirect\n" +
			"to this machine; with --device, by entering a code on any device. The tokens are kept in the\n" +
			"user's config directory (orkctl/credentials.json, mode 0600), one login per server, and sent\n" +
			"by every command that is given no other credential.",
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			if !strings.HasPrefix(g.server, "https://") && !loopbackServer(g.server) {
				return fmt.Errorf("login: %s is neither https nor loopback: a login's tokens would cross the network in the clear", g.server)
			}
			if c := g.creds; c.clientCert != "" || c.tokenFile != "" || c.serviceAccount != "" {
				return errors.New("login: a client certificate, token file or service account is given, and it would be sent instead of the login; drop it to log in")
			}
			ctx, cancel := context.WithTimeout(cmd.Context(), loginTimeout)
			defer cancel()
			cfg, err := orkstackv1connect.NewAuthServiceClient(g.httpClient(), g.baseURL()).
				GetAuthConfig(ctx, connect.NewRequest(&orkstackv1.GetAuthConfigRequest{}))
			if err != nil {
				return rpcError("login", err)
			}
			issuer, err := pickIssuer(cfg.Msg.GetIssuers(), issuerName)
			if err != nil {
				return fmt.Errorf("login: %w", err)
			}
			client, err := issuerHTTPClient(issuerCA)
			if err != nil {
				return fmt.Errorf("login: %w", err)
			}
			meta, err := authn.DiscoverProvider(ctx, client, issuer.GetIssuerUrl())
			if err != nil {
				return fmt.Errorf("login: issuer %q: %w", issuer.GetName(), err)
			}
			oc := &oauth2.Config{ClientID: issuer.GetCliClientId(), Scopes: issuer.GetScopes(),
				Endpoint: oauth2.Endpoint{AuthURL: meta.AuthorizationEndpoint, TokenURL: meta.TokenEndpoint,
					DeviceAuthURL: meta.DeviceEndpoint, AuthStyle: oauth2.AuthStyleInParams}}
			octx := context.WithValue(ctx, oauth2.HTTPClient, client)
			var tok *oauth2.Token
			if device {
				if !issuer.GetDeviceFlow() || meta.DeviceEndpoint == "" {
					return fmt.Errorf("login: issuer %q does not offer the device grant; sign in with a browser", issuer.GetName())
				}
				tok, err = deviceLogin(octx, oc, g.stderrOr(cmd))
			} else {
				tok, err = browserLogin(octx, oc, issuer.GetIssuerUrl(), meta.IssParameter, !noBrowser, g.stderrOr(cmd))
			}
			if err != nil {
				return fmt.Errorf("login: %w", err)
			}
			login, err := loginFromToken(tok, issuer, meta, issuerCA)
			if err != nil {
				return fmt.Errorf("login: %w", err)
			}
			err = withCredentials(func(f credentialsFile, path string) error {
				f.Servers[serverKey(g.server)] = login
				return saveCredentials(f, path)
			})
			if err != nil {
				return fmt.Errorf("login: keep the tokens: %w", err)
			}
			if tok.RefreshToken == "" {
				_, _ = fmt.Fprintf(g.stderrOr(cmd), "warning: the identity provider granted no refresh token; this login lasts until %s\n",
					login.ExpiresAt.Local().Format(time.RFC1123))
			}
			who, err := orkstackv1connect.NewAuthServiceClient(g.httpClient(), g.baseURL(),
				connect.WithInterceptors(loginBearer{server: g.server, warn: g.stderrOr(cmd)})).
				GetSignin(ctx, connect.NewRequest(&orkstackv1.GetSigninRequest{}))
			if err != nil {
				// A login the server does not accept is not kept.
				_ = withCredentials(func(f credentialsFile, path string) error {
					delete(f.Servers, serverKey(g.server))
					return saveCredentials(f, path)
				})
				return rpcError("login: signed in at the identity provider, but the server refused the token", err)
			}
			_, err = fmt.Fprintf(cmd.OutOrStdout(), "Signed in to %s as %s\n", g.server, who.Msg.GetPrincipalId())
			return err
		},
	}
	cmd.Flags().StringVar(&issuerName, "issuer", "", "the issuer to sign in with, when the server trusts several")
	cmd.Flags().BoolVar(&device, "device", false, "sign in with a code entered on any device (the device grant)")
	cmd.Flags().BoolVar(&noBrowser, "no-browser", false, "print the sign-in URL instead of opening a browser")
	cmd.Flags().StringVar(&issuerCA, "issuer-ca-file", "", "PEM roots to trust for the identity provider, in place of the system's")
	return cmd
}

func (g *globalFlags) stderrOr(cmd *cobra.Command) io.Writer {
	if g.stderr != nil {
		return g.stderr
	}
	return cmd.ErrOrStderr()
}

func pickIssuer(issuers []*orkstackv1.SigninIssuer, name string) (*orkstackv1.SigninIssuer, error) {
	var names []string
	var chosen *orkstackv1.SigninIssuer
	for _, i := range issuers {
		names = append(names, i.GetName())
		if i.GetName() == name {
			chosen = i
		}
	}
	switch {
	case len(issuers) == 0:
		return nil, errors.New("the server offers no sign-in (no issuer has a login)")
	case name != "" && chosen == nil:
		return nil, fmt.Errorf("the server offers no issuer %q; it offers %s", name, strings.Join(names, ", "))
	case name == "" && len(issuers) > 1:
		return nil, fmt.Errorf("the server offers several issuers (%s): choose one with --issuer", strings.Join(names, ", "))
	case name == "":
		chosen = issuers[0]
	}
	if chosen.GetCliClientId() == "" {
		return nil, fmt.Errorf("issuer %q has no cli_client_id: a terminal cannot sign in with it", chosen.GetName())
	}
	return chosen, nil
}

func randomString() (string, error) {
	b := make([]byte, 32)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(b), nil
}

// browserLogin runs the code flow with PKCE and a loopback redirect
// (RFC 8252 §7.3): it listens on 127.0.0.1 at a free port, sends the
// browser to the issuer, and redeems the code its redirect brings back.
func browserLogin(ctx context.Context, oc *oauth2.Config, issuerURL string, issParam, open bool, out io.Writer) (*oauth2.Token, error) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return nil, fmt.Errorf("listen for the redirect: %w", err)
	}
	defer ln.Close()
	oc.RedirectURL = "http://" + ln.Addr().String() + "/callback"
	state, err1 := randomString()
	nonce, err2 := randomString()
	if err := errors.Join(err1, err2); err != nil {
		return nil, err
	}
	verifier := oauth2.GenerateVerifier()
	// The nonce binds the ID token to this request at the issuer; orkctl does
	// not check it itself, because the engine verifies every token it is
	// sent and a terminal holds no key to verify one with.
	authURL := oc.AuthCodeURL(state, oauth2.S256ChallengeOption(verifier), oauth2.SetAuthURLParam("nonce", nonce))
	type result struct {
		code string
		err  error
		// answered is told the outcome of redeeming the code, which the
		// browser's page then shows.
		answered chan error
	}
	done := make(chan result, 1)
	srv := &http.Server{ReadHeaderTimeout: 10 * time.Second, Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/callback" {
			http.NotFound(w, r)
			return
		}
		q := r.URL.Query()
		if q.Get("state") != state {
			// Not this login's redirect (a prefetch, a stray request): refused,
			// and the login keeps waiting for its own.
			http.Error(w, "not this sign-in's redirect", http.StatusBadRequest)
			return
		}
		var res result
		switch {
		case (q.Get("iss") != "" || issParam) && q.Get("iss") != issuerURL:
			res.err = fmt.Errorf("the redirect's iss %q is not the issuer's", q.Get("iss"))
		case q.Get("error") != "":
			res.err = fmt.Errorf("the issuer answered %s: %s", q.Get("error"), q.Get("error_description"))
		case q.Get("code") == "":
			res.err = errors.New("the redirect carries no code")
		default:
			res.code = q.Get("code")
		}
		res.answered = make(chan error, 1)
		select {
		case done <- res:
		default:
			http.Error(w, "this sign-in was already answered", http.StatusConflict)
			return
		}
		outcome := res.err
		if outcome == nil {
			select {
			case outcome = <-res.answered:
			case <-r.Context().Done():
				return
			}
		}
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		if outcome != nil {
			w.WriteHeader(http.StatusBadRequest)
			_, _ = fmt.Fprintln(w, "Sign-in failed; see the terminal.")
			return
		}
		_, _ = fmt.Fprintln(w, "Signed in. You can close this tab and return to the terminal.")
	})}
	go func() { _ = srv.Serve(ln) }()
	defer srv.Close()
	_, _ = fmt.Fprintf(out, "Sign in at:\n  %s\n", authURL)
	if open {
		if err := openBrowser(authURL); err != nil {
			_, _ = fmt.Fprintf(out, "Could not open a browser (%v): open the URL yourself, or use --device.\n", err)
		}
	}
	select {
	case <-ctx.Done():
		return nil, fmt.Errorf("no sign-in within %s: %w", loginTimeout, ctx.Err())
	case res := <-done:
		if res.err != nil {
			return nil, res.err
		}
		tok, err := oc.Exchange(ctx, res.code, oauth2.VerifierOption(verifier))
		res.answered <- err
		return tok, err
	}
}

// deviceLogin runs the device authorization grant (RFC 8628): it prints
// the verification URL and code and polls at the issuer's interval.
func deviceLogin(ctx context.Context, oc *oauth2.Config, out io.Writer) (*oauth2.Token, error) {
	resp, err := oc.DeviceAuth(ctx)
	if err != nil {
		return nil, fmt.Errorf("start the device grant: %w", err)
	}
	target := resp.VerificationURIComplete
	if target == "" {
		target = resp.VerificationURI
	}
	_, _ = fmt.Fprintf(out, "On any device, open:\n  %s\nand enter the code: %s\n", target, resp.UserCode)
	return oc.DeviceAccessToken(ctx, resp)
}

// loginFromToken is what is kept of a token response: the ID token, which
// is the bearer, its expiry, and the refresh token.
func loginFromToken(tok *oauth2.Token, issuer *orkstackv1.SigninIssuer, meta authn.ProviderMetadata, caFile string) (storedLogin, error) {
	idToken, _ := tok.Extra("id_token").(string)
	if idToken == "" {
		return storedLogin{}, errors.New("the issuer answered no ID token")
	}
	exp, err := tokenExpiry(idToken)
	if err != nil {
		return storedLogin{}, err
	}
	return storedLogin{Issuer: issuer.GetName(), IssuerURL: issuer.GetIssuerUrl(), ClientID: issuer.GetCliClientId(),
		IDToken: idToken, RefreshToken: tok.RefreshToken, ExpiresAt: exp, TokenEndpoint: meta.TokenEndpoint,
		RevocationEndpoint: meta.RevocationEndpoint, IssuerCAFile: caFile}, nil
}

// tokenExpiry reads a JWT's exp without verifying it: the client only needs
// to know when to refresh. The engine verifies every token it is sent.
func tokenExpiry(token string) (time.Time, error) {
	parts := strings.Split(token, ".")
	if len(parts) != 3 {
		return time.Time{}, errors.New("the ID token is not a JWT")
	}
	payload, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		return time.Time{}, errors.New("the ID token's payload is not base64url")
	}
	var claims struct {
		Exp int64 `json:"exp"`
	}
	if err := json.Unmarshal(payload, &claims); err != nil || claims.Exp == 0 {
		return time.Time{}, errors.New("the ID token has no exp")
	}
	return time.Unix(claims.Exp, 0).UTC(), nil
}

// loginBearer is a client interceptor presenting the stored login of the
// server, refreshed first when it is within a minute of expiry. With no
// stored login it presents nothing.
type loginBearer struct {
	server string
	// warn is told what a request cannot report itself.
	warn io.Writer
}

func (l loginBearer) token(ctx context.Context) (token string, err error) {
	err = withCredentials(func(f credentialsFile, path string) error {
		token, err = l.tokenLocked(ctx, f, path)
		return err
	})
	return token, err
}

// errLoginExpired is a stored login past its expiry that cannot be
// refreshed: the request is refused here, saying so, never sent as nobody.
var errLoginExpired = errors.New("the stored login has expired; run `orkctl login` again")

func (l loginBearer) tokenLocked(ctx context.Context, f credentialsFile, path string) (string, error) {
	login, ok := f.Servers[serverKey(l.server)]
	if !ok {
		return "", nil
	}
	if time.Until(login.ExpiresAt) > refreshAhead {
		return login.IDToken, nil
	}
	if login.RefreshToken == "" {
		if time.Now().Before(login.ExpiresAt) {
			return login.IDToken, nil // nothing to refresh with: used to its last second
		}
		return "", errLoginExpired
	}
	client, err := issuerHTTPClient(login.IssuerCAFile)
	if err != nil {
		return "", err
	}
	oc := &oauth2.Config{ClientID: login.ClientID, Endpoint: oauth2.Endpoint{TokenURL: login.TokenEndpoint, AuthStyle: oauth2.AuthStyleInParams}}
	tok, err := oc.TokenSource(context.WithValue(ctx, oauth2.HTTPClient, client),
		&oauth2.Token{RefreshToken: login.RefreshToken, Expiry: time.Now().Add(-time.Minute)}).Token()
	if err != nil {
		var re *oauth2.RetrieveError
		if errors.As(err, &re) && re.ErrorCode == "invalid_grant" {
			return "", fmt.Errorf("the identity provider ended the login to %s: run `orkctl login` again: %w", l.server, err)
		}
		if time.Now().Before(login.ExpiresAt) {
			// The provider did not answer, and the token still holds: use it.
			if l.warn != nil {
				_, _ = fmt.Fprintf(l.warn, "warning: the login to %s could not be refreshed (%v); using it until it expires\n", l.server, err)
			}
			return login.IDToken, nil
		}
		return "", fmt.Errorf("the login to %s could not be refreshed now; try again: %w", l.server, err)
	}
	idToken, _ := tok.Extra("id_token").(string)
	if idToken == "" {
		return "", fmt.Errorf("the issuer refreshed the login to %s without an ID token: run `orkctl login` again", l.server)
	}
	exp, err := tokenExpiry(idToken)
	if err != nil {
		return "", err
	}
	login.IDToken, login.ExpiresAt = idToken, exp
	if tok.RefreshToken != "" {
		login.RefreshToken = tok.RefreshToken
	}
	f.Servers[serverKey(l.server)] = login
	if err := saveCredentials(f, path); err != nil {
		// The token is good for this command; the next one will find the
		// rotated refresh token lost and ask for a new login.
		if l.warn != nil {
			_, _ = fmt.Fprintf(l.warn, "warning: the refreshed login to %s could not be kept (%v); the next command may need `orkctl login`\n", l.server, err)
		}
	}
	return idToken, nil
}

func (l loginBearer) WrapUnary(next connect.UnaryFunc) connect.UnaryFunc {
	return func(ctx context.Context, req connect.AnyRequest) (connect.AnyResponse, error) {
		token, err := l.token(ctx)
		if err != nil {
			return nil, connect.NewError(connect.CodeUnauthenticated, err)
		}
		if token != "" {
			req.Header().Set("Authorization", "Bearer "+token)
		}
		return next(ctx, req)
	}
}

func (l loginBearer) WrapStreamingClient(next connect.StreamingClientFunc) connect.StreamingClientFunc {
	return func(ctx context.Context, spec connect.Spec) connect.StreamingClientConn {
		conn := next(ctx, spec)
		// A stream opens with no request to fail: one whose login cannot be
		// read is opened without it, the engine refuses it, and why is said.
		token, err := l.token(ctx)
		if err != nil && l.warn != nil {
			_, _ = fmt.Fprintf(l.warn, "warning: %v\n", err)
		}
		if token != "" {
			conn.RequestHeader().Set("Authorization", "Bearer "+token)
		}
		return conn
	}
}

func (loginBearer) WrapStreamingHandler(next connect.StreamingHandlerFunc) connect.StreamingHandlerFunc {
	return next
}

func newLogoutCommand(g *globalFlags) *cobra.Command {
	return &cobra.Command{
		Use:   "logout",
		Short: "Forget the login to the server, and revoke its refresh token at the issuer",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			var login storedLogin
			var ok bool
			err := withCredentials(func(f credentialsFile, path string) error {
				if login, ok = f.Servers[serverKey(g.server)]; !ok {
					return nil
				}
				delete(f.Servers, serverKey(g.server))
				return saveCredentials(f, path)
			})
			if err != nil {
				return fmt.Errorf("logout: %w", err)
			}
			if !ok {
				_, err := fmt.Fprintf(cmd.OutOrStdout(), "Not logged in to %s\n", g.server)
				return err
			}
			if login.RefreshToken != "" && login.RevocationEndpoint != "" {
				client, cerr := issuerHTTPClient(login.IssuerCAFile)
				if cerr == nil {
					cerr = authn.RevokeToken(cmd.Context(), client, login.RevocationEndpoint, login.ClientID, "", login.RefreshToken)
				}
				if cerr != nil {
					_, _ = fmt.Fprintf(g.stderrOr(cmd), "warning: the refresh token could not be revoked at %s: %v\n", login.Issuer, cerr)
				}
			}
			_, err = fmt.Fprintf(cmd.OutOrStdout(), "Logged out of %s\n", g.server)
			return err
		},
	}
}
