package authn

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"encoding/hex"
	"encoding/json/v2"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"net/http"
	"net/url"
	"slices"
	"strings"
	"sync"
	"time"

	"golang.org/x/oauth2"

	"github.com/orkstack/orkstack/internal/jwt"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// The cookies and header of sign-in (authn-authz.md §8).
const (
	// SigninCookie holds 32 random bytes; the record is keyed by their
	// SHA-256.
	SigninCookie = "__Host-ork-signin"
	// LoginCookie holds one sign-in in progress, sealed: issuer, state,
	// nonce, PKCE verifier and return path, for ten minutes.
	LoginCookie = "__Host-ork-login"
	// CSRFHeader carries the sign-in's CSRF token on every
	// cookie-authenticated request that is not GET or HEAD.
	CSRFHeader = "Ork-CSRF"
	// LoginPath and CallbackPath are the browser's redirects, by issuer.
	LoginPath    = "/v1/auth/login/{issuer}"
	CallbackPath = "/v1/auth/callback/{issuer}"
)

// The refusals a page reads from a sign-in's answer.
const (
	// ReasonSigninStale: the sign-in is past validated_until; the page calls
	// RefreshSignin.
	ReasonSigninStale = "SIGNIN_STALE"
	// ReasonSigninEnded: the sign-in is gone; the page signs in again.
	ReasonSigninEnded = "SIGNIN_ENDED"
	// ReasonCSRF: a cookie request without its CSRF token, or from another
	// origin.
	ReasonCSRF = "SIGNIN_CSRF"
)

// The defaults of auth.signin.*.
const (
	DefaultSigninMaxAge             = 12 * time.Hour
	DefaultSigninIdleTimeout        = time.Hour
	DefaultSigninRevalidateInterval = 10 * time.Minute
)

const (
	loginLifetime      = 10 * time.Minute
	signinRecordFormat = 1
	// recordCacheTTL is how long a replica answers from a record it read.
	recordCacheTTL = 15 * time.Second
	// maxRecordCache bounds the replica's cache of records.
	maxRecordCache  = 10000
	exchangeTimeout = 15 * time.Second
)

var (
	// ErrSigninEnded is a sign-in that is gone, expired, revoked, or that
	// this engine cannot open.
	ErrSigninEnded = errors.New(ReasonSigninEnded + ": the sign-in has ended; sign in again")
	// ErrSigninUnavailable is a sign-in whose record or document could not
	// be read or written now: it stands, and nothing is ended.
	ErrSigninUnavailable = errors.New("the sign-in could not be checked now; try again")
	// ErrNoRefreshToken is a sign-in its issuer granted no refresh token:
	// it is renewed by signing in again.
	ErrNoRefreshToken = errors.New(ReasonSigninEnded + ": the issuer granted no refresh token; sign in again")
)

// SigninRecords is the store of sign-in records (artifacts.SigninStore).
type SigninRecords interface {
	Create(ctx context.Context, id string, sealed []byte) (string, error)
	Get(ctx context.Context, id string) ([]byte, string, error)
	Replace(ctx context.Context, id string, sealed []byte, etag string) (string, error)
	Delete(ctx context.Context, id string) error
	List(ctx context.Context, startAfter string) ([]string, bool, error)
}

// SigninSettings are auth.signin.*.
type SigninSettings struct {
	MaxAge             time.Duration
	IdleTimeout        time.Duration
	RevalidateInterval time.Duration
}

// SigninEvent is one thing that happened to a sign-in, for the audit trail.
type SigninEvent struct {
	// Action is auth.signin.created, .refreshed, .ended or .refused.
	Action    string
	Principal *Principal
	Issuer    string
	PublicID  string
	// Reason is why a sign-in was refused or ended, for people.
	Reason string
}

// Signins is sign-in for the web UI: the browser's redirects, the cookie
// and its record, refresh and sign-out. It is safe for concurrent use.
type Signins struct {
	Issuers Issuers
	// Keys holds each issuer's key source, for its ID tokens.
	Keys    map[string]jwt.KeySource
	Sealer  *SigninKeys
	Records SigninRecords
	// PublicURL is auth.public_url: the redirect URI's origin and the one
	// Origin a cookie request may carry.
	PublicURL *url.URL
	Settings  SigninSettings
	Leeway    time.Duration
	// Secret reads a login's client_secret source (env:NAME or file:/path).
	Secret func(source string) (string, error)
	// HTTPClient builds an issuer's client; nil trusts its ca_file and
	// follows no redirect.
	HTTPClient func(i *Issuer) (*http.Client, error)
	// Disabled reports a principal under the access document's `disabled`;
	// an error is a document that could not be read, which ends nothing.
	Disabled func(ctx context.Context, id string) (bool, error)
	// OnEvent is told of every sign-in event; it writes the audit record.
	OnEvent func(ctx context.Context, e SigninEvent)
	Logger  *slog.Logger
	Now     func() time.Time

	initOnce  sync.Once
	providers providers
	mu        sync.Mutex
	cache     map[string]cachedRecord
	// forgotSeq is the sequence number at which each id was last forgotten
	// (deleted, revoked or ended): a read that began before it never caches
	// what it read. seq counts forgets.
	forgotSeq map[string]uint64
	seq       uint64
	// refreshing holds this replica's refresh in flight for each id: a
	// second caller waits for the first, so one replica never redeems one
	// rotating refresh token twice.
	refreshing map[string]*refreshCall
}

type refreshCall struct {
	done chan struct{}
	view *SigninView
	err  error
}

type cachedRecord struct {
	rec    *signinRecord
	etag   string
	readAt time.Time
}

// signinRecord is a sign-in record's plaintext, sealed whole.
type signinRecord struct {
	Format         int             `json:"format"`
	PublicID       string          `json:"public_id"`
	Issuer         string          `json:"issuer"`
	Principal      recordPrincipal `json:"principal"`
	Subject        string          `json:"subject"`
	Nonce          string          `json:"nonce"`
	CreatedAt      time.Time       `json:"created_at"`
	ExpiresAt      time.Time       `json:"expires_at"`
	ValidatedUntil time.Time       `json:"validated_until"`
	IdleExpiresAt  time.Time       `json:"idle_expires_at"`
	RefreshToken   string          `json:"refresh_token,omitzero"`
	CSRF           string          `json:"csrf"`
}

type recordPrincipal struct {
	Kind    Kind              `json:"kind"`
	ID      string            `json:"id"`
	Display string            `json:"display"`
	Groups  []string          `json:"groups,omitzero"`
	Claims  map[string]string `json:"claims,omitzero"`
}

// loginState is the login cookie's plaintext.
type loginState struct {
	Issuer   string    `json:"issuer"`
	State    string    `json:"state"`
	Nonce    string    `json:"nonce"`
	Verifier string    `json:"verifier"`
	Return   string    `json:"return"`
	Expires  time.Time `json:"expires"`
}

// SigninView is what a request authenticated by a sign-in carries beside
// its principal.
type SigninView struct {
	PublicID       string
	CSRF           string
	Stale          bool
	CreatedAt      time.Time
	ExpiresAt      time.Time
	ValidatedUntil time.Time
	// IdleExpiresAt is when the sign-in ends unless refreshed: a refresh,
	// not a request, moves it.
	IdleExpiresAt time.Time
	// cookieID is the record's id, for refresh and sign-out.
	cookieID string
}

type signinCtxKey struct{}

// WithSignin marks ctx as authenticated by the sign-in v.
func WithSignin(ctx context.Context, v *SigninView) context.Context {
	return context.WithValue(ctx, signinCtxKey{}, v)
}

// SigninFrom is the sign-in a request was authenticated by, or nil.
func SigninFrom(ctx context.Context) *SigninView {
	v, _ := ctx.Value(signinCtxKey{}).(*SigninView)
	return v
}

func (s *Signins) now() time.Time {
	if s.Now != nil {
		return s.Now()
	}
	return time.Now()
}

func (s *Signins) logger() *slog.Logger {
	if s.Logger != nil {
		return s.Logger
	}
	return slog.Default()
}

func (s *Signins) init() {
	s.initOnce.Do(func() {
		s.providers.newClient = s.HTTPClient
		s.providers.failed = func(i *Issuer, age time.Duration, err error) {
			s.logger().Warn("sign-in: the issuer's discovery could not be read; serving the endpoints read before",
				"issuer", i.Name, "served_since", age.Round(time.Second), "error", err)
		}
		if s.Settings.MaxAge <= 0 {
			s.Settings.MaxAge = DefaultSigninMaxAge
		}
		if s.Settings.IdleTimeout <= 0 {
			s.Settings.IdleTimeout = DefaultSigninIdleTimeout
		}
		if s.Settings.RevalidateInterval <= 0 {
			s.Settings.RevalidateInterval = DefaultSigninRevalidateInterval
		}
		s.cache = map[string]cachedRecord{}
		s.forgotSeq = map[string]uint64{}
		s.refreshing = map[string]*refreshCall{}
	})
}

func (s *Signins) event(ctx context.Context, e SigninEvent) {
	if s.OnEvent != nil {
		s.OnEvent(ctx, e)
	}
}

func randomToken(n int) (string, error) {
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(b), nil
}

// cookieRecordID is a cookie's record id: the hex SHA-256 of the 32 random
// bytes the cookie holds sealed. A cookie no engine sealed names no record,
// and costs no read of the store.
func (s *Signins) cookieRecordID(value string) (string, bool) {
	if len(value) > 256 {
		return "", false
	}
	raw, err := s.Sealer.Open(PurposeCookie, "", []byte(value))
	if err != nil || len(raw) != 32 {
		return "", false
	}
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:]), true
}

// CallbackURL is the redirect URI registered at an issuer for sign-in:
// auth.public_url's origin and /v1/auth/callback/{issuer}.
func (s *Signins) CallbackURL(issuer string) string {
	u := *s.PublicURL
	u.Path = strings.TrimSuffix(u.Path, "/") + "/v1/auth/callback/" + url.PathEscape(issuer)
	u.RawPath, u.RawQuery, u.Fragment = "", "", ""
	return u.String()
}

// oauthConfig is the issuer's client for the code flow. Its secret is read
// at each use, so a rotated one is used as it is then.
func (s *Signins) oauthConfig(i *Issuer, meta ProviderMetadata) (*oauth2.Config, error) {
	secret := ""
	if i.Login.ClientSecret != "" {
		if s.Secret == nil {
			return nil, errors.New("no reader for the client secret")
		}
		v, err := s.Secret(i.Login.ClientSecret)
		if err != nil {
			return nil, fmt.Errorf("issuer %q: login.client_secret: %w", i.Name, err)
		}
		secret = v
	}
	return &oauth2.Config{
		ClientID: i.Login.ClientID, ClientSecret: secret, RedirectURL: s.CallbackURL(i.Name),
		Scopes:   slices.Clone(i.Login.Scopes),
		Endpoint: oauth2.Endpoint{AuthURL: meta.AuthorizationEndpoint, TokenURL: meta.TokenEndpoint},
	}, nil
}

func (s *Signins) loginIssuer(name string) (*Issuer, bool) {
	i, ok := s.Issuers.ByName(name)
	if !ok || i.Login == nil {
		return nil, false
	}
	return i, true
}

func secureCookie(name, value string, maxAge int) *http.Cookie {
	return &http.Cookie{Name: name, Value: value, Path: "/", Secure: true, HttpOnly: true,
		SameSite: http.SameSiteLaxMode, MaxAge: maxAge}
}

func noStore(w http.ResponseWriter) {
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("Referrer-Policy", "no-referrer")
}

// signinFailed sends the browser to the sign-in page with a short reason,
// never the detail, which goes to the log.
func (s *Signins) signinFailed(w http.ResponseWriter, r *http.Request, issuer, reason string, err error) {
	s.logger().Warn("sign-in refused", "issuer", issuer, "reason", reason, "error", err)
	http.SetCookie(w, secureCookie(LoginCookie, "", -1))
	noStore(w)
	http.Redirect(w, r, "/signin?error="+url.QueryEscape(reason), http.StatusFound)
}

// Login starts a sign-in: GET /v1/auth/login/{issuer}?return=/path. It
// seals a fresh state, nonce and PKCE verifier, with the issuer's name and
// the return path, in the login cookie, and redirects to the issuer's
// authorization endpoint with code_challenge_method=S256.
func (s *Signins) Login(w http.ResponseWriter, r *http.Request) {
	s.init()
	noStore(w)
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	i, ok := s.loginIssuer(r.PathValue("issuer"))
	if !ok {
		http.Error(w, "no such sign-in issuer", http.StatusNotFound)
		return
	}
	meta, _, err := s.providers.get(r.Context(), i, s.now())
	if err != nil {
		s.logger().Error("sign-in: the issuer's discovery could not be read", "issuer", i.Name, "error", err)
		http.Error(w, "the identity provider cannot be reached now; try again", http.StatusServiceUnavailable)
		return
	}
	cfg, err := s.oauthConfig(i, meta)
	if err != nil {
		s.logger().Error("sign-in: the issuer's client is misconfigured", "issuer", i.Name, "error", err)
		http.Error(w, "sign-in is misconfigured on this engine", http.StatusServiceUnavailable)
		return
	}
	state, err1 := randomToken(32)
	nonce, err2 := randomToken(32)
	if err := errors.Join(err1, err2); err != nil {
		http.Error(w, "no randomness", http.StatusInternalServerError)
		return
	}
	ls := loginState{Issuer: i.Name, State: state, Nonce: nonce, Verifier: oauth2.GenerateVerifier(),
		Return: SafeReturnPath(r.URL.Query().Get("return")), Expires: s.now().Add(loginLifetime)}
	plain, err := json.Marshal(ls)
	if err != nil {
		http.Error(w, "encode", http.StatusInternalServerError)
		return
	}
	sealed, err := s.Sealer.Seal(PurposeLogin, "", plain)
	if err != nil {
		http.Error(w, "seal", http.StatusInternalServerError)
		return
	}
	http.SetCookie(w, secureCookie(LoginCookie, string(sealed), int(loginLifetime/time.Second)))
	http.Redirect(w, r, cfg.AuthCodeURL(state, oauth2.S256ChallengeOption(ls.Verifier),
		oauth2.SetAuthURLParam("nonce", nonce)), http.StatusFound)
}

// Callback ends a sign-in: GET /v1/auth/callback/{issuer}. It checks state
// against the login cookie, the issuer in the path against the cookie's,
// and the response's iss against the issuer (RFC 9207), so a code from one
// issuer is never redeemed at another; it redeems the code with the
// verifier, verifies the ID token, refuses a disabled principal, writes
// the record, sets the cookie and returns to the return path.
func (s *Signins) Callback(w http.ResponseWriter, r *http.Request) {
	s.init()
	noStore(w)
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	name := r.PathValue("issuer")
	c, err := r.Cookie(LoginCookie)
	if err != nil {
		s.signinFailed(w, r, name, "no_sign_in_in_progress", err)
		return
	}
	plain, err := s.Sealer.Open(PurposeLogin, "", []byte(c.Value))
	var ls loginState
	if err == nil {
		err = json.Unmarshal(plain, &ls)
	}
	if err != nil || s.now().After(ls.Expires) {
		s.signinFailed(w, r, name, "sign_in_expired", err)
		return
	}
	q := r.URL.Query()
	if ls.Issuer != name {
		s.signinFailed(w, r, name, "issuer_mismatch", fmt.Errorf("callback for %q, sign-in started with %q", name, ls.Issuer))
		return
	}
	if subtle.ConstantTimeCompare([]byte(q.Get("state")), []byte(ls.State)) != 1 {
		s.signinFailed(w, r, name, "state_mismatch", nil)
		return
	}
	i, ok := s.loginIssuer(name)
	if !ok {
		s.signinFailed(w, r, name, "unknown_issuer", nil)
		return
	}
	meta, client, err := s.providers.get(r.Context(), i, s.now())
	if err != nil {
		s.signinFailed(w, r, name, "issuer_unavailable", err)
		return
	}
	if iss := q.Get("iss"); iss != "" || meta.IssParameter {
		if iss != i.Issuer {
			s.signinFailed(w, r, name, "issuer_mismatch", fmt.Errorf("the response's iss %q is not issuer %q's", iss, i.Name))
			return
		}
	}
	if e := q.Get("error"); e != "" {
		s.signinFailed(w, r, name, "denied", fmt.Errorf("the issuer answered %s: %s", e, q.Get("error_description")))
		return
	}
	code := q.Get("code")
	if code == "" {
		s.signinFailed(w, r, name, "no_code", nil)
		return
	}
	cfg, err := s.oauthConfig(i, meta)
	if err != nil {
		s.signinFailed(w, r, name, "misconfigured", err)
		return
	}
	ctx, cancel := context.WithTimeout(context.WithValue(r.Context(), oauth2.HTTPClient, client), exchangeTimeout)
	defer cancel()
	tok, err := cfg.Exchange(ctx, code, oauth2.VerifierOption(ls.Verifier))
	if err != nil {
		s.signinFailed(w, r, name, "exchange_failed", err)
		return
	}
	rawID, _ := tok.Extra("id_token").(string)
	if rawID == "" {
		s.signinFailed(w, r, name, "no_id_token", nil)
		return
	}
	claims, err := jwt.VerifyIDToken(ctx, rawID, i.Policy(s.Leeway), s.Keys[i.Name], s.now(), i.Login.ClientID, ls.Nonce)
	if err != nil {
		s.signinFailed(w, r, name, "id_token_refused", err)
		return
	}
	p, dropped, err := PrincipalFromClaims(i, claims, MethodOIDC)
	if err != nil {
		s.signinFailed(w, r, name, "principal_refused", err)
		return
	}
	if dropped {
		s.logger().Warn("sign-in: the groups claim was dropped (over its bounds, or not a list)", "issuer", i.Name, "principal", p.ID)
	}
	if s.Disabled != nil {
		disabled, err := s.Disabled(r.Context(), p.ID)
		if err != nil {
			s.logger().Error("sign-in: whether the principal is disabled could not be read; the sign-in is refused for now",
				"issuer", i.Name, "principal", p.ID, "error", err)
			http.SetCookie(w, secureCookie(LoginCookie, "", -1))
			http.Error(w, "the sign-in could not be checked now; try again", http.StatusServiceUnavailable)
			return
		}
		if disabled {
			s.event(r.Context(), SigninEvent{Action: "auth.signin.refused", Principal: p, Issuer: i.Name, Reason: "the principal is disabled"})
			s.signinFailed(w, r, name, "disabled", fmt.Errorf("principal %q is disabled", p.ID))
			return
		}
	}
	// A sign-in this browser held already ends: one browser, one sign-in.
	if old, err := r.Cookie(SigninCookie); err == nil {
		if oldID, ok := s.cookieRecordID(old.Value); ok {
			switch rec, _, err := s.read(r.Context(), oldID, true); {
			case err == nil:
				if err := s.end(r.Context(), oldID, rec, "replaced by a new sign-in"); err != nil {
					s.logger().Warn("sign-in: the browser's earlier sign-in could not be ended", "error", err)
				}
			case !errors.Is(err, ErrSigninEnded):
				s.logger().Warn("sign-in: the browser's earlier sign-in could not be read to end it", "error", err)
			}
		}
	}
	if tok.RefreshToken == "" {
		s.logger().Warn("sign-in: the issuer granted no refresh token (does the login ask for offline_access?); the sign-in is renewed by signing in again",
			"issuer", i.Name)
	}
	cookie, view, err := s.create(r.Context(), i, p, claims.Subject, ls.Nonce, tok.RefreshToken)
	if err != nil {
		s.logger().Error("sign-in: the record could not be written", "issuer", i.Name, "principal", p.ID, "error", err)
		http.SetCookie(w, secureCookie(LoginCookie, "", -1))
		http.Error(w, "the sign-in could not be recorded now; try again", http.StatusServiceUnavailable)
		return
	}
	p.Signin = view.PublicID
	s.event(r.Context(), SigninEvent{Action: "auth.signin.created", Principal: p, Issuer: i.Name, PublicID: view.PublicID})
	http.SetCookie(w, secureCookie(LoginCookie, "", -1))
	http.SetCookie(w, secureCookie(SigninCookie, cookie, int(s.Settings.MaxAge/time.Second)))
	http.Redirect(w, r, ls.Return, http.StatusFound)
}

func toRecordPrincipal(p *Principal) recordPrincipal {
	return recordPrincipal{Kind: p.Kind, ID: p.ID, Display: p.Display, Groups: slices.Clone(p.Groups), Claims: maps.Clone(p.Claims)}
}

func (s *Signins) seal(id string, rec *signinRecord) ([]byte, error) {
	plain, err := json.Marshal(rec)
	if err != nil {
		return nil, err
	}
	return s.Sealer.Seal(PurposeRecord, artifacts.SigninKey(id), plain)
}

func (s *Signins) create(ctx context.Context, i *Issuer, p *Principal, subject, nonce, refresh string) (string, *SigninView, error) {
	raw := make([]byte, 32)
	if _, err := rand.Read(raw); err != nil {
		return "", nil, err
	}
	sealedCookie, err := s.Sealer.Seal(PurposeCookie, "", raw)
	if err != nil {
		return "", nil, err
	}
	cookie := string(sealedCookie)
	sum := sha256.Sum256(raw)
	id := hex.EncodeToString(sum[:])
	publicID, err1 := randomToken(12)
	csrf, err2 := randomToken(32)
	if err := errors.Join(err1, err2); err != nil {
		return "", nil, err
	}
	now := s.now()
	rec := &signinRecord{Format: signinRecordFormat, PublicID: "si_" + publicID, Issuer: i.Name,
		Principal: toRecordPrincipal(p), Subject: subject, Nonce: nonce, CreatedAt: now,
		ExpiresAt: now.Add(s.Settings.MaxAge), RefreshToken: refresh, CSRF: csrf}
	rec.ValidatedUntil = minTime(now.Add(s.Settings.RevalidateInterval), rec.ExpiresAt)
	rec.IdleExpiresAt = minTime(now.Add(s.Settings.IdleTimeout), rec.ExpiresAt)
	sealed, err := s.seal(id, rec)
	if err != nil {
		return "", nil, err
	}
	etag, err := s.Records.Create(ctx, id, sealed)
	if err != nil {
		return "", nil, err
	}
	s.remember(id, rec, etag)
	return cookie, viewOf(id, rec, now), nil
}

func minTime(a, b time.Time) time.Time {
	if a.Before(b) {
		return a
	}
	return b
}

func viewOf(id string, rec *signinRecord, now time.Time) *SigninView {
	return &SigninView{PublicID: rec.PublicID, CSRF: rec.CSRF, Stale: now.After(rec.ValidatedUntil),
		CreatedAt: rec.CreatedAt, ExpiresAt: rec.ExpiresAt, ValidatedUntil: rec.ValidatedUntil,
		IdleExpiresAt: rec.IdleExpiresAt, cookieID: id}
}

func (s *Signins) remember(id string, rec *signinRecord, etag string) {
	s.mu.Lock()
	began := s.seq
	s.mu.Unlock()
	s.rememberRead(id, rec, etag, began)
}

// readBegins is the sequence number a read compares against when it
// caches what it found.
func (s *Signins) readBegins() uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.seq
}

// rememberRead caches what a read that began at sequence began found,
// unless the id was forgotten since: a sign-in deleted while it was being
// read is never served from this replica's cache.
func (s *Signins) rememberRead(id string, rec *signinRecord, etag string, began uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.forgotSeq[id] > began {
		return
	}
	if len(s.cache) >= maxRecordCache {
		now := s.now()
		for k, v := range s.cache {
			if now.Sub(v.readAt) >= recordCacheTTL {
				delete(s.cache, k)
			}
		}
		if len(s.forgotSeq) >= maxRecordCache {
			// Past the bound the tombstones go: a read in flight at this
			// instant may cache a deleted record for one TTL at most.
			clear(s.forgotSeq)
		}
		if len(s.cache) >= maxRecordCache {
			clear(s.cache)
		}
	}
	s.cache[id] = cachedRecord{rec: rec, etag: etag, readAt: s.now()}
}

func (s *Signins) forget(id string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.cache, id)
	s.seq++
	if len(s.forgotSeq) >= maxRecordCache {
		// Past the bound the tombstones go: a read in flight at this
		// instant may cache a deleted record for one TTL at most.
		clear(s.forgotSeq)
	}
	s.forgotSeq[id] = s.seq
}

// read returns the record id names: from this replica's cache when it is
// younger than recordCacheTTL and fresh is false, else from the store. A
// record that is missing, cannot be opened or has expired is
// ErrSigninEnded; an expired one is deleted.
func (s *Signins) read(ctx context.Context, id string, fresh bool) (*signinRecord, string, error) {
	now := s.now()
	if !fresh {
		s.mu.Lock()
		c, ok := s.cache[id]
		s.mu.Unlock()
		if ok && now.Sub(c.readAt) < recordCacheTTL {
			if now.After(c.rec.ExpiresAt) || now.After(c.rec.IdleExpiresAt) {
				return nil, "", ErrSigninEnded
			}
			return c.rec, c.etag, nil
		}
	}
	began := s.readBegins()
	sealed, etag, err := s.Records.Get(ctx, id)
	if errors.Is(err, artifacts.ErrNotFound) {
		return nil, "", ErrSigninEnded
	}
	if err != nil {
		return nil, "", err
	}
	plain, err := s.Sealer.Open(PurposeRecord, artifacts.SigninKey(id), sealed)
	if err != nil {
		// A record under a key this engine lacks, or one no engine sealed,
		// is not a sign-in: the page signs in again. A key this engine
		// lacks is an operator's to fix, so it is said.
		if errors.Is(err, ErrSigninKeyUnknown) {
			s.logger().Warn("sign-in: a record is sealed under a key auth.signin.key_files does not list; its person must sign in again",
				"keys", s.Sealer.KeyIDs())
		}
		return nil, "", ErrSigninEnded
	}
	var rec signinRecord
	if err := json.Unmarshal(plain, &rec); err != nil || rec.Format != signinRecordFormat {
		s.logger().Warn("sign-in: a record is not one this release reads; its person must sign in again", "format", rec.Format, "error", err)
		return nil, "", ErrSigninEnded
	}
	if now.After(rec.ExpiresAt) || now.After(rec.IdleExpiresAt) {
		if err := s.Records.Delete(ctx, id); err != nil {
			s.logger().Warn("sign-in: an expired record could not be deleted; the lifecycle rule removes it", "error", err)
		}
		s.forget(id)
		return nil, "", ErrSigninEnded
	}
	s.rememberRead(id, &rec, etag, began)
	return &rec, etag, nil
}

// HasCookie reports whether r presents a sign-in cookie.
func HasCookie(r *http.Request) bool {
	_, err := r.Cookie(SigninCookie)
	return err == nil
}

// Authenticate reads r's sign-in cookie. The principal is the record's as
// of its last validation; a record past validated_until is answered with
// its view marked stale, which the middleware refuses except where a stale
// sign-in is served. No cookie is ErrNoCredential.
func (s *Signins) Authenticate(ctx context.Context, r *http.Request) (*Principal, *SigninView, error) {
	s.init()
	c, err := r.Cookie(SigninCookie)
	if err != nil {
		return nil, nil, ErrNoCredential
	}
	id, ok := s.cookieRecordID(c.Value)
	if !ok {
		return nil, nil, &Refusal{Err: ErrSigninEnded}
	}
	rec, _, err := s.read(ctx, id, false)
	if errors.Is(err, ErrSigninEnded) {
		return nil, nil, &Refusal{Err: ErrSigninEnded}
	}
	if err != nil {
		return nil, nil, &Refusal{Unavailable: true, Err: fmt.Errorf("the sign-in could not be read: %w", err)}
	}
	return s.principalOf(rec), viewOf(id, rec, s.now()), nil
}

func (s *Signins) principalOf(rec *signinRecord) *Principal {
	return &Principal{Kind: rec.Principal.Kind, ID: rec.Principal.ID, Display: rec.Principal.Display,
		Issuer: rec.Issuer, Method: MethodOIDC, Groups: slices.Clone(rec.Principal.Groups), Claims: maps.Clone(rec.Principal.Claims),
		Signin: rec.PublicID, Expires: rec.ValidatedUntil}
}

// sameOrigin reports whether an Origin header names public's origin, a
// scheme's default port written or not.
func sameOrigin(origin string, public *url.URL) bool {
	u, err := url.Parse(origin)
	if err != nil || u.Host == "" || u.Path != "" || u.RawQuery != "" || u.User != nil {
		return false
	}
	return strings.EqualFold(u.Scheme, public.Scheme) && strings.EqualFold(hostWithPort(u), hostWithPort(public))
}

func hostWithPort(u *url.URL) string {
	if port := u.Port(); port != "" {
		return u.Host
	}
	switch strings.ToLower(u.Scheme) {
	case "https":
		return u.Host + ":443"
	case "http":
		return u.Host + ":80"
	}
	return u.Host
}

// SigninCheckInterval is how often an open stream asks whether the
// sign-in it was opened under still stands.
const SigninCheckInterval = recordCacheTTL

// Alive reports whether the sign-in v still stands, read from the store:
// ErrSigninEnded once it was signed out, revoked or expired; another error
// when the store could not answer, which ends nothing.
func (s *Signins) Alive(ctx context.Context, v *SigninView) error {
	s.init()
	if v == nil || v.cookieID == "" {
		return ErrSigninEnded
	}
	_, _, err := s.read(ctx, v.cookieID, true)
	return err
}

// CheckCSRF holds a cookie-authenticated request to the sign-in's CSRF
// token and the deployment's origin: a request that is not GET or HEAD
// must carry Ork-CSRF equal to the record's token unless exempt, and any
// request that carries an Origin must carry auth.public_url's.
func (s *Signins) CheckCSRF(r *http.Request, v *SigninView, exempt bool) error {
	if origin := r.Header.Get("Origin"); origin != "" {
		if !sameOrigin(origin, s.PublicURL) {
			return errors.New(ReasonCSRF + ": the request comes from another origin")
		}
	}
	if exempt || r.Method == http.MethodGet || r.Method == http.MethodHead {
		return nil
	}
	got := r.Header.Get(CSRFHeader)
	if got == "" || subtle.ConstantTimeCompare([]byte(got), []byte(v.CSRF)) != 1 {
		return errors.New(ReasonCSRF + ": the request does not carry the sign-in's " + CSRFHeader + " token")
	}
	return nil
}

// Refresh revalidates the sign-in v with its issuer: it redeems the
// record's refresh token, verifies the new ID token, takes its groups, and
// writes the record under its etag. The page calls it one call at a time
// across tabs. An issuer that refuses ends the sign-in; a person removed at
// the issuer loses access within the revalidation interval.
//
// On one replica, a second refresh of a sign-in waits for the one in
// flight and takes its answer. Across replicas the page's lock keeps one
// call at a time; where two replicas still race, the one whose rotated
// token the issuer refuses re-reads the record and takes the other's
// answer instead of ending the sign-in.
func (s *Signins) Refresh(ctx context.Context, v *SigninView) (*SigninView, error) {
	s.init()
	if v == nil || v.cookieID == "" {
		return nil, ErrSigninEnded
	}
	id := v.cookieID
	s.mu.Lock()
	if call, ok := s.refreshing[id]; ok {
		s.mu.Unlock()
		select {
		case <-call.done:
			return call.view, call.err
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	call := &refreshCall{done: make(chan struct{})}
	s.refreshing[id] = call
	s.mu.Unlock()
	defer func() {
		s.mu.Lock()
		delete(s.refreshing, id)
		s.mu.Unlock()
		close(call.done)
	}()
	// The redemption is not cut short by the caller that started it: the
	// callers waiting on it want its answer.
	call.view, call.err = s.refresh(context.WithoutCancel(ctx), id)
	return call.view, call.err
}

// ErrIssuerUnavailable is a refresh the issuer did not answer, or answered
// with an error that is not about the sign-in: the sign-in stands.
var ErrIssuerUnavailable = errors.New("the identity provider could not refresh the sign-in now; try again")

func (s *Signins) refresh(ctx context.Context, id string) (*SigninView, error) {
	ctx, cancel := context.WithTimeout(ctx, 2*exchangeTimeout)
	defer cancel()
	rec, etag, err := s.read(ctx, id, true)
	if err != nil {
		return nil, err
	}
	now := s.now()
	if rec.ValidatedUntil.Sub(now) > s.Settings.RevalidateInterval/2 {
		// Another tab or replica refreshed it a moment ago.
		return viewOf(id, rec, now), nil
	}
	if rec.RefreshToken == "" {
		return nil, ErrNoRefreshToken
	}
	i, ok := s.loginIssuer(rec.Issuer)
	if !ok {
		return nil, s.end(ctx, id, rec, "the issuer no longer offers sign-in")
	}
	meta, client, err := s.providers.get(ctx, i, now)
	if err != nil {
		return nil, err
	}
	cfg, err := s.oauthConfig(i, meta)
	if err != nil {
		return nil, err
	}
	octx, cancel := context.WithTimeout(context.WithValue(ctx, oauth2.HTTPClient, client), exchangeTimeout)
	defer cancel()
	tok, err := cfg.TokenSource(octx, &oauth2.Token{RefreshToken: rec.RefreshToken, Expiry: now.Add(-time.Minute)}).Token()
	if err != nil {
		var re *oauth2.RetrieveError
		switch {
		case errors.As(err, &re) && re.ErrorCode == "invalid_grant":
			// Another replica may have redeemed this token a moment ago and
			// written its answer: take it rather than end a good sign-in.
			again, _, rerr := s.read(ctx, id, true)
			switch {
			case rerr == nil && again.ValidatedUntil.After(rec.ValidatedUntil):
				return viewOf(id, again, s.now()), nil
			case rerr != nil && !errors.Is(rerr, ErrSigninEnded):
				// Whether another replica answered cannot be told: end nothing.
				return nil, fmt.Errorf("%w: %w", ErrSigninUnavailable, rerr)
			}
			return nil, s.end(ctx, id, rec, "the issuer refused the refresh (invalid_grant)")
		case errors.As(err, &re) && re.ErrorCode == "invalid_client":
			s.logger().Error("sign-in: the issuer refused this engine's client; check login.client_id and client_secret",
				"issuer", i.Name, "error", err)
			return nil, fmt.Errorf("%w: %w", ErrIssuerUnavailable, err)
		default:
			s.logger().Warn("sign-in: a refresh was not answered; the sign-in stands", "issuer", i.Name, "error", err)
			return nil, fmt.Errorf("%w: %w", ErrIssuerUnavailable, err)
		}
	}
	p := s.principalOf(rec)
	if rawID, _ := tok.Extra("id_token").(string); rawID != "" {
		keys := s.Keys[i.Name]
		if keys == nil {
			return nil, fmt.Errorf("%w: issuer %q has no key set", ErrSigninUnavailable, i.Name)
		}
		claims, err := jwt.VerifyRefreshedIDToken(octx, rawID, i.Policy(s.Leeway), keys, now,
			i.Login.ClientID, rec.Nonce, rec.Subject)
		if jwt.ReasonOf(err) == jwt.ReasonUnavailable {
			s.logger().Warn("sign-in: the issuer's keys could not be read to verify a refreshed ID token; the sign-in stands",
				"issuer", i.Name, "error", err)
			return nil, fmt.Errorf("%w: %w", ErrIssuerUnavailable, err)
		}
		if err != nil {
			s.logger().Warn("sign-in: a refreshed ID token was refused; the sign-in ends", "issuer", i.Name, "error", err)
			return nil, s.end(ctx, id, rec, "the refreshed ID token was refused")
		}
		fresh, _, err := PrincipalFromClaims(i, claims, MethodOIDC)
		if err != nil || fresh.ID != rec.Principal.ID {
			return nil, s.end(ctx, id, rec, "the refreshed ID token names another principal")
		}
		p = fresh
	} else {
		s.logger().Debug("sign-in: the issuer refreshed without an ID token; the groups stay as they were", "issuer", i.Name)
	}
	if s.Disabled != nil {
		disabled, err := s.Disabled(ctx, p.ID)
		if err != nil {
			return nil, fmt.Errorf("%w: whether the principal is disabled could not be read: %w", ErrSigninUnavailable, err)
		}
		if disabled {
			return nil, s.end(ctx, id, rec, "the principal is disabled")
		}
	}
	next := *rec
	next.Principal = toRecordPrincipal(p)
	if tok.RefreshToken != "" {
		next.RefreshToken = tok.RefreshToken
	}
	next.ValidatedUntil = minTime(now.Add(s.Settings.RevalidateInterval), next.ExpiresAt)
	next.IdleExpiresAt = minTime(now.Add(s.Settings.IdleTimeout), next.ExpiresAt)
	sealed, err := s.seal(id, &next)
	if err != nil {
		return nil, err
	}
	newEtag, err := s.Records.Replace(ctx, id, sealed, etag)
	if errors.Is(err, artifacts.ErrPreconditionFailed) {
		// Another replica wrote it meanwhile: its answer stands.
		s.forget(id)
		again, _, rerr := s.read(ctx, id, true)
		if rerr != nil {
			return nil, rerr
		}
		return viewOf(id, again, now), nil
	}
	if err != nil {
		// The issuer rotated the refresh token and the new one was not kept:
		// the next refresh will be refused and end the sign-in.
		s.logger().Error("sign-in: a refreshed sign-in could not be written; its rotated refresh token is lost",
			"issuer", i.Name, "signin", next.PublicID, "error", err)
		return nil, fmt.Errorf("%w: %w", ErrSigninUnavailable, err)
	}
	s.remember(id, &next, newEtag)
	p.Signin = next.PublicID
	s.event(ctx, SigninEvent{Action: "auth.signin.refreshed", Principal: p, Issuer: i.Name, PublicID: next.PublicID})
	return viewOf(id, &next, now), nil
}

// end deletes a sign-in that has ended and answers ErrSigninEnded; a record
// that could not be deleted still stands, and is answered unavailable.
func (s *Signins) end(ctx context.Context, id string, rec *signinRecord, reason string) error {
	if err := s.Records.Delete(ctx, id); err != nil {
		s.logger().Error("sign-in: a sign-in that ended could not be deleted; it stands until it can be",
			"signin", rec.PublicID, "reason", reason, "error", err)
		return fmt.Errorf("%w: %w", ErrSigninUnavailable, err)
	}
	s.forget(id)
	s.revokeAtIssuer(ctx, rec)
	s.event(ctx, SigninEvent{Action: "auth.signin.ended", Principal: s.principalOf(rec), Issuer: rec.Issuer,
		PublicID: rec.PublicID, Reason: reason})
	return ErrSigninEnded
}

// SignOut ends the sign-in v: it deletes the record, revokes its refresh
// token where the issuer has a revocation endpoint (best effort), and
// answers the issuer's end_session_endpoint, when it has one, for the page
// to visit.
func (s *Signins) SignOut(ctx context.Context, v *SigninView) (endSession string, err error) {
	s.init()
	if v == nil || v.cookieID == "" {
		return "", ErrSigninEnded
	}
	rec, _, err := s.read(ctx, v.cookieID, true)
	if errors.Is(err, ErrSigninEnded) {
		return "", nil
	}
	if err != nil {
		return "", err
	}
	if err := s.Records.Delete(ctx, v.cookieID); err != nil {
		return "", err
	}
	s.forget(v.cookieID)
	s.event(ctx, SigninEvent{Action: "auth.signin.ended", Principal: s.principalOf(rec), Issuer: rec.Issuer,
		PublicID: rec.PublicID, Reason: "signed out"})
	i, ok := s.loginIssuer(rec.Issuer)
	if !ok {
		return "", nil
	}
	s.revokeAtIssuer(ctx, rec)
	meta, _, err := s.providers.get(ctx, i, s.now())
	if err != nil {
		s.logger().Warn("sign-out: the issuer's discovery could not be read; no end-session URL", "issuer", i.Name, "error", err)
		return "", nil
	}
	if meta.EndSessionEndpoint == "" {
		return "", nil
	}
	u, err := url.Parse(meta.EndSessionEndpoint)
	if err != nil {
		return "", nil
	}
	q := u.Query()
	q.Set("client_id", i.Login.ClientID)
	u.RawQuery = q.Encode()
	return u.String(), nil
}

// revokeAtIssuer revokes a record's refresh token where its issuer has a
// revocation endpoint, best effort: the record is gone already, and a
// token the issuer keeps is useless without the sign-in key.
func (s *Signins) revokeAtIssuer(ctx context.Context, rec *signinRecord) {
	if rec == nil || rec.RefreshToken == "" {
		return
	}
	i, ok := s.loginIssuer(rec.Issuer)
	if !ok {
		return
	}
	meta, client, err := s.providers.get(ctx, i, s.now())
	if err == nil && meta.RevocationEndpoint == "" {
		return
	}
	if err == nil {
		var cfg *oauth2.Config
		if cfg, err = s.oauthConfig(i, meta); err == nil {
			err = RevokeToken(ctx, client, meta.RevocationEndpoint, cfg.ClientID, cfg.ClientSecret, rec.RefreshToken)
		}
	}
	if err != nil {
		s.logger().Warn("sign-in: the refresh token could not be revoked at the issuer", "issuer", rec.Issuer, "error", err)
	}
}

// RevokeToken revokes a refresh token at endpoint (RFC 7009), as the
// client clientID, with its secret when it has one.
func RevokeToken(ctx context.Context, client *http.Client, endpoint, clientID, secret, token string) error {
	ctx, cancel := context.WithTimeout(ctx, exchangeTimeout)
	defer cancel()
	form := url.Values{"token": {token}, "token_type_hint": {"refresh_token"}}
	if secret == "" {
		form.Set("client_id", clientID)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, strings.NewReader(form.Encode()))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	if secret != "" {
		req.SetBasicAuth(url.QueryEscape(clientID), url.QueryEscape(secret))
	}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("revocation: %s", resp.Status)
	}
	return nil
}

// SigninListing is one sign-in as AccessService lists it.
type SigninListing struct {
	PublicID       string
	PrincipalID    string
	CreatedAt      time.Time
	ExpiresAt      time.Time
	ValidatedUntil time.Time
	IdleExpiresAt  time.Time
	id             string
}

// List reads every record this engine can open and answers those of
// principal (every principal's when empty). Records are bounded by their
// twelve-hour life; one that cannot be opened is skipped, and said.
func (s *Signins) List(ctx context.Context, principal string) ([]SigninListing, error) {
	s.init()
	var out []SigninListing
	skipped := 0
	defer func() {
		if skipped > 0 {
			s.logger().Warn("sign-in: records this engine cannot open were left out of a listing; one may be a sign-in another engine still honours",
				"skipped", skipped)
		}
	}()
	after := ""
	for {
		ids, more, err := s.Records.List(ctx, after)
		if err != nil {
			return nil, err
		}
		for _, id := range ids {
			rec, _, err := s.read(ctx, id, true)
			if errors.Is(err, ErrSigninEnded) {
				skipped++
				continue
			}
			if err != nil {
				return nil, err
			}
			if principal != "" && rec.Principal.ID != principal {
				continue
			}
			out = append(out, SigninListing{PublicID: rec.PublicID, PrincipalID: rec.Principal.ID, CreatedAt: rec.CreatedAt,
				ExpiresAt: rec.ExpiresAt, ValidatedUntil: rec.ValidatedUntil, IdleExpiresAt: rec.IdleExpiresAt, id: id})
		}
		if !more || len(ids) == 0 {
			return out, nil
		}
		after = ids[len(ids)-1]
	}
}

// Revoke deletes the sign-ins of principal named by publicIDs, or every one
// of its sign-ins when publicIDs is empty, and answers how many it ended.
func (s *Signins) Revoke(ctx context.Context, principal string, publicIDs []string) (int, error) {
	if principal == "" {
		return 0, errors.New("a principal is required")
	}
	listed, err := s.List(ctx, principal)
	if err != nil {
		return 0, err
	}
	n := 0
	for _, l := range listed {
		if len(publicIDs) > 0 && !slices.Contains(publicIDs, l.PublicID) {
			continue
		}
		rec, _, rerr := s.read(ctx, l.id, true)
		if err := s.Records.Delete(ctx, l.id); err != nil {
			return n, err
		}
		s.forget(l.id)
		n++
		if rerr == nil {
			s.revokeAtIssuer(ctx, rec)
		} else {
			s.logger().Warn("sign-in: a revoked record could not be read; its refresh token was not revoked at the issuer", "error", rerr)
		}
		s.event(ctx, SigninEvent{Action: "auth.signin.ended", Principal: &Principal{ID: l.PrincipalID, Method: MethodOIDC},
			PublicID: l.PublicID, Reason: "revoked"})
	}
	return n, nil
}
