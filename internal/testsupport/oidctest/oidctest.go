// Package oidctest is a small OpenID Connect provider, in process, for the
// unit tests of sign-in: discovery, a key set, the authorization code flow
// with PKCE (S256 only), refresh tokens that rotate, the device
// authorization grant, revocation and an end-session endpoint. It signs in
// whichever user it is told to, with no page of its own. It is never a
// production issuer, and it is not the development stack's: that is Dex
// (internal/testsupport/dextest), and so is the interop test that proves
// sign-in against a real provider (TestIntegration_SigninWithDex).
//
// It stays for what a real server cannot be made to do on demand: a key set
// that breaks or a key rotated mid-session, a clock skewed against the
// engine's, a refresh or a revocation the issuer refuses
// (SetRefreshFailure), a user removed at the issuer (RemoveUser), and
// discovery that drops what a provider must offer, such as
// code_challenge_methods_supported without S256. A behaviour every real
// provider has belongs in the interop test, not here.
package oidctest

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"encoding/json/v2"
	"fmt"
	"net/http"
	"net/url"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/orkstack/orkstack/internal/jwt"
)

// User is who the provider signs in.
type User struct {
	Subject string
	Email   string
	Groups  []string
}

// Client is one registered client. A public client (the CLI's) has no
// secret; a confidential one (the engine's) must present it.
type Client struct {
	ID           string
	Secret       string
	RedirectURIs []string // exact; a loopback redirect also matches with any port (RFC 8252 §7.3)
}

// Options configure a provider.
type Options struct {
	Clients []Client
	// User is the user an authorization signs in; SetUser changes it.
	User User
	// TokenLifetime is an ID token's life; 5 minutes when zero.
	TokenLifetime time.Duration
	// NoRefreshTokens grants none, as some issuers do.
	NoRefreshTokens bool
	// OmitIssParameter leaves RFC 9207's iss off the authorization response.
	OmitIssParameter bool
	// AutoApproveDevice approves every device authorization for the current
	// user at its first poll, as a person at the verification page would.
	AutoApproveDevice bool
}

type grant struct {
	client    string
	redirect  string
	challenge string
	nonce     string
	user      User
	expires   time.Time
}

type device struct {
	client   string
	userCode string
	approved bool
	user     User
	expires  time.Time
}

// Provider is the provider. It is safe for concurrent use.
type Provider struct {
	issuer      string
	signer      *jwt.Signer
	opts        Options
	mu          sync.Mutex
	now         func() time.Time
	user        User
	codes       map[string]grant
	refresh     map[string]grant // live refresh tokens
	spent       map[string]bool  // rotated-away refresh tokens
	removed     map[string]bool  // subjects removed at the provider
	devices     map[string]*device
	refreshes   int
	revocations int
	// failRefresh, when set, answers every refresh with it.
	failStatus int
	failCode   string
}

// SetRefreshFailure makes every refresh answer status with the OAuth error
// code, as an issuer that rate-limits (429) or no longer knows the client
// (401 invalid_client) does; a zero status answers again.
func (p *Provider) SetRefreshFailure(status int, code string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.failStatus, p.failCode = status, code
}

// New returns a provider whose issuer is issuer, the URL it is served at.
func New(issuer string, opts Options) (*Provider, error) {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, err
	}
	signer, err := jwt.NewSigner(key)
	if err != nil {
		return nil, err
	}
	if opts.TokenLifetime == 0 {
		opts.TokenLifetime = 5 * time.Minute
	}
	return &Provider{issuer: strings.TrimSuffix(issuer, "/"), signer: signer, opts: opts, now: time.Now, user: opts.User,
		codes: map[string]grant{}, refresh: map[string]grant{}, spent: map[string]bool{}, removed: map[string]bool{},
		devices: map[string]*device{}}, nil
}

// Issuer is the provider's issuer URL.
func (p *Provider) Issuer() string { return p.issuer }

// SetNow replaces the provider's clock.
func (p *Provider) SetNow(now func() time.Time) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.now = now
}

// SetUser makes the next authorizations sign in u.
func (p *Provider) SetUser(u User) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.user = u
}

// RemoveUser removes subject at the provider: its refresh tokens are
// refused from now on.
func (p *Provider) RemoveUser(subject string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.removed[subject] = true
}

// Refreshes counts the refresh tokens redeemed.
func (p *Provider) Refreshes() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.refreshes
}

// Revocations counts the revocation requests that revoked a token.
func (p *Provider) Revocations() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.revocations
}

// ApproveDevice approves the device authorization whose user code is code,
// for the current user.
func (p *Provider) ApproveDevice(code string) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, d := range p.devices {
		if d.userCode == code {
			d.approved, d.user = true, p.user
			return true
		}
	}
	return false
}

// IDToken signs an ID token for client with extra claims beside the usual
// ones: how a test makes a token the flow would not.
func (p *Provider) IDToken(client string, u User, nonce string, extra map[string]any) (string, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.idTokenLocked(client, u, nonce, p.now(), extra)
}

func (p *Provider) idTokenLocked(client string, u User, nonce string, now time.Time, extra map[string]any) (string, error) {
	claims := map[string]any{"iss": p.issuer, "sub": u.Subject, "aud": client, "iat": now.Unix(),
		"exp": now.Add(p.opts.TokenLifetime).Unix(), "email": u.Email, "email_verified": u.Email != ""}
	if nonce != "" {
		claims["nonce"] = nonce
	}
	if len(u.Groups) > 0 {
		claims["groups"] = u.Groups
	}
	for k, v := range extra {
		claims[k] = v
	}
	return p.signer.Sign("JWT", claims)
}

// ServeHTTP serves the provider's endpoints.
func (p *Provider) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	switch r.URL.Path {
	case "/.well-known/openid-configuration":
		p.discovery(w)
	case "/jwks":
		p.jwks(w)
	case "/authorize":
		p.authorize(w, r)
	case "/token":
		p.token(w, r)
	case "/device":
		p.deviceAuthorization(w, r)
	case "/revoke":
		p.revoke(w, r)
	case "/end-session":
		_, _ = w.Write([]byte("signed out at the provider\n"))
	default:
		http.NotFound(w, r)
	}
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(status)
	_ = json.MarshalWrite(w, v)
}

func oauthError(w http.ResponseWriter, status int, code string) {
	writeJSON(w, status, map[string]string{"error": code})
}

func (p *Provider) discovery(w http.ResponseWriter) {
	writeJSON(w, http.StatusOK, map[string]any{
		"issuer":                                         p.issuer,
		"authorization_endpoint":                         p.issuer + "/authorize",
		"token_endpoint":                                 p.issuer + "/token",
		"jwks_uri":                                       p.issuer + "/jwks",
		"end_session_endpoint":                           p.issuer + "/end-session",
		"revocation_endpoint":                            p.issuer + "/revoke",
		"device_authorization_endpoint":                  p.issuer + "/device",
		"code_challenge_methods_supported":               []string{"S256"},
		"response_types_supported":                       []string{"code"},
		"authorization_response_iss_parameter_supported": !p.opts.OmitIssParameter,
	})
}

func (p *Provider) jwks(w http.ResponseWriter) {
	jwk, err := p.signer.Public().MarshalJWK(p.signer.KeyID())
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_, _ = fmt.Fprintf(w, `{"keys":[%s]}`, jwk)
}

func (p *Provider) client(id string) (Client, bool) {
	for _, c := range p.opts.Clients {
		if c.ID == id {
			return c, true
		}
	}
	return Client{}, false
}

func redirectAllowed(c Client, redirect string) bool {
	if slices.Contains(c.RedirectURIs, redirect) {
		return true
	}
	u, err := url.Parse(redirect)
	if err != nil || u.Scheme != "http" || (u.Hostname() != "127.0.0.1" && u.Hostname() != "::1") {
		return false
	}
	for _, allowed := range c.RedirectURIs {
		a, err := url.Parse(allowed)
		if err == nil && a.Scheme == "http" && a.Hostname() == u.Hostname() && a.Path == u.Path {
			return true
		}
	}
	return false
}

func random() string {
	b := make([]byte, 24)
	_, _ = rand.Read(b)
	return base64.RawURLEncoding.EncodeToString(b)
}

func (p *Provider) authorize(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	c, ok := p.client(q.Get("client_id"))
	redirect := q.Get("redirect_uri")
	if !ok || !redirectAllowed(c, redirect) {
		http.Error(w, "unknown client or redirect_uri", http.StatusBadRequest)
		return
	}
	back, _ := url.Parse(redirect)
	out := back.Query()
	out.Set("state", q.Get("state"))
	if !p.opts.OmitIssParameter {
		out.Set("iss", p.issuer)
	}
	if q.Get("response_type") != "code" || q.Get("code_challenge_method") != "S256" || q.Get("code_challenge") == "" ||
		!strings.Contains(" "+q.Get("scope")+" ", " openid ") {
		out.Set("error", "invalid_request")
		back.RawQuery = out.Encode()
		http.Redirect(w, r, back.String(), http.StatusFound)
		return
	}
	code := random()
	p.mu.Lock()
	p.codes[code] = grant{client: c.ID, redirect: redirect, challenge: q.Get("code_challenge"), nonce: q.Get("nonce"),
		user: p.user, expires: p.now().Add(time.Minute)}
	p.mu.Unlock()
	out.Set("code", code)
	back.RawQuery = out.Encode()
	http.Redirect(w, r, back.String(), http.StatusFound)
}

// clientAuth authenticates the caller of the token, device and revocation
// endpoints: HTTP Basic, or the form's client_id and client_secret.
func (p *Provider) clientAuth(r *http.Request) (Client, bool) {
	id, secret, basic := r.BasicAuth()
	if basic {
		id, _ = url.QueryUnescape(id)
		secret, _ = url.QueryUnescape(secret)
	} else {
		id, secret = r.PostForm.Get("client_id"), r.PostForm.Get("client_secret")
	}
	c, ok := p.client(id)
	if !ok {
		return Client{}, false
	}
	if c.Secret != "" && subtle.ConstantTimeCompare([]byte(secret), []byte(c.Secret)) != 1 {
		return Client{}, false
	}
	return c, true
}

func (p *Provider) token(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost || r.ParseForm() != nil {
		oauthError(w, http.StatusBadRequest, "invalid_request")
		return
	}
	c, ok := p.clientAuth(r)
	if !ok {
		oauthError(w, http.StatusUnauthorized, "invalid_client")
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	now := p.now()
	var g grant
	switch r.PostForm.Get("grant_type") {
	case "authorization_code":
		code := r.PostForm.Get("code")
		got, ok := p.codes[code]
		delete(p.codes, code)
		sum := sha256.Sum256([]byte(r.PostForm.Get("code_verifier")))
		if !ok || got.client != c.ID || got.redirect != r.PostForm.Get("redirect_uri") || now.After(got.expires) ||
			base64.RawURLEncoding.EncodeToString(sum[:]) != got.challenge {
			oauthError(w, http.StatusBadRequest, "invalid_grant")
			return
		}
		g = got
	case "refresh_token":
		if p.failStatus != 0 {
			oauthError(w, p.failStatus, p.failCode)
			return
		}
		rt := r.PostForm.Get("refresh_token")
		got, ok := p.refresh[rt]
		if !ok || got.client != c.ID || p.removed[got.user.Subject] {
			oauthError(w, http.StatusBadRequest, "invalid_grant")
			return
		}
		delete(p.refresh, rt)
		p.spent[rt] = true
		p.refreshes++
		if p.user.Subject == got.user.Subject {
			got.user = p.user // what the provider now says of them: their groups as of today
		}
		g = got
	case "urn:ietf:params:oauth:grant-type:device_code":
		d, ok := p.devices[r.PostForm.Get("device_code")]
		switch {
		case !ok || d.client != c.ID || now.After(d.expires):
			oauthError(w, http.StatusBadRequest, "expired_token")
			return
		case !d.approved && p.opts.AutoApproveDevice:
			d.approved, d.user = true, p.user
			oauthError(w, http.StatusBadRequest, "authorization_pending")
			return
		case !d.approved:
			oauthError(w, http.StatusBadRequest, "authorization_pending")
			return
		}
		delete(p.devices, r.PostForm.Get("device_code"))
		g = grant{client: c.ID, user: d.user}
	default:
		oauthError(w, http.StatusBadRequest, "unsupported_grant_type")
		return
	}
	idToken, err := p.idTokenLocked(c.ID, g.user, g.nonce, now, nil)
	if err != nil {
		oauthError(w, http.StatusInternalServerError, "server_error")
		return
	}
	out := map[string]any{"access_token": random(), "token_type": "Bearer",
		"expires_in": int(p.opts.TokenLifetime / time.Second), "id_token": idToken}
	if !p.opts.NoRefreshTokens {
		rt := random()
		p.refresh[rt] = grant{client: c.ID, nonce: g.nonce, user: g.user}
		out["refresh_token"] = rt
	}
	writeJSON(w, http.StatusOK, out)
}

func (p *Provider) deviceAuthorization(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost || r.ParseForm() != nil {
		oauthError(w, http.StatusBadRequest, "invalid_request")
		return
	}
	c, ok := p.clientAuth(r)
	if !ok {
		oauthError(w, http.StatusUnauthorized, "invalid_client")
		return
	}
	code, user := random(), strings.ToUpper(random()[:8])
	p.mu.Lock()
	p.devices[code] = &device{client: c.ID, userCode: user, expires: p.now().Add(5 * time.Minute)}
	p.mu.Unlock()
	writeJSON(w, http.StatusOK, map[string]any{"device_code": code, "user_code": user,
		"verification_uri": p.issuer + "/activate", "expires_in": 300, "interval": 1})
}

func (p *Provider) revoke(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost || r.ParseForm() != nil {
		oauthError(w, http.StatusBadRequest, "invalid_request")
		return
	}
	if _, ok := p.clientAuth(r); !ok {
		oauthError(w, http.StatusUnauthorized, "invalid_client")
		return
	}
	p.mu.Lock()
	if _, ok := p.refresh[r.PostForm.Get("token")]; ok {
		delete(p.refresh, r.PostForm.Get("token"))
		p.revocations++
	}
	p.mu.Unlock()
	w.WriteHeader(http.StatusOK)
}
