package authn

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json/v2"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/orkstack/orkstack/internal/authz"
	"github.com/orkstack/orkstack/internal/jwt"
)

// PrincipalHeader is the request header a deployment's gateway asserts the
// caller's identity in. In asserted mode OrkStack records its value and
// verifies nothing; in observe and enforce mode it is read nowhere. This
// package holds its one reader (Recorded).
const PrincipalHeader = "Ork-Principal"

// ReasonHeader is the header every break-glass request carries: why.
const ReasonHeader = "Ork-Reason"

// The modes, as auth.mode spells them.
const (
	ModeAsserted = "asserted"
	ModeObserve  = "observe"
	ModeEnforce  = "enforce"
)

type modeKey struct{}

// WithMode returns ctx marked as served in mode. Only the middleware marks
// a request; one it never saw is asserted.
func WithMode(ctx context.Context, mode string) context.Context {
	return context.WithValue(ctx, modeKey{}, mode)
}

// ModeFrom is the mode a request was served in: asserted unless the
// middleware marked it.
func ModeFrom(ctx context.Context) string {
	if m, ok := ctx.Value(modeKey{}).(string); ok && m != "" {
		return m
	}
	return ModeAsserted
}

// RecordedPrincipal is who a request is from, as a record carries it.
type RecordedPrincipal struct {
	ID       string
	Verified bool
	Method   string
	Issuer   string
	Signin   string
}

// Recorded is the one reader of who a request is from, for every site that
// records a principal (an audit record, an origin, a cancellation, an
// await's resolution). In asserted mode it is the Ork-Principal header,
// verbatim and unverified, exactly as before; in observe and enforce mode
// it is the verified principal or nobody, and the header is never read.
func Recorded(ctx context.Context, h http.Header) RecordedPrincipal {
	if ModeFrom(ctx) == ModeAsserted {
		if h == nil {
			return RecordedPrincipal{}
		}
		return RecordedPrincipal{ID: h.Get(PrincipalHeader)}
	}
	p := FromContext(ctx)
	if p == nil {
		return RecordedPrincipal{}
	}
	return RecordedPrincipal{ID: p.ID, Verified: true, Method: p.Method, Issuer: p.Issuer, Signin: p.Signin}
}

// DefaultClockSkew is auth.clock_skew's default: the leeway a token's
// expiry, not-before and issued-at are allowed against this engine's clock.
const DefaultClockSkew = time.Minute

// BreakGlass is the one key whose assertions authenticate as
// serviceaccount:break-glass (auth.break_glass_key_file).
type BreakGlass struct {
	Keys jwt.Static
	// Audience is the deployment's public URL (auth.public_url).
	Audience  string
	PublicURL *url.URL
	// Replay is break-glass's own cache, so no account can crowd it out.
	Replay *jwt.ReplayCache
}

// BreakGlassAccount names the break-glass principal: serviceaccount:break-glass.
// It is the account name authz reserves.
const BreakGlassAccount = authz.BreakGlassAccount

// Authenticator turns a request's credential into a principal.
type Authenticator struct {
	Issuers Issuers
	// Keys holds each trusted issuer's key-set cache, by issuer name.
	Keys map[string]jwt.KeySource
	// Leeway is auth.clock_skew.
	Leeway     time.Duration
	BreakGlass *BreakGlass
	// HostAllowed is the listener's host policy, for an assertion's htu.
	HostAllowed func(host string) bool
	Now         func() time.Time
	// ServiceAccounts accepts the access document's service accounts'
	// request-bound assertions; nil accepts none.
	ServiceAccounts *ServiceAccounts

	// BodyTimeout replaces AssertionBodyTimeout when set.
	BodyTimeout time.Duration
	bodyOnce    sync.Once
	bodySlots   chan struct{}
}

// AssertionBodyTimeout bounds how long the body of a request an assertion
// authenticates may take to arrive.
const AssertionBodyTimeout = 10 * time.Second

// MaxBufferedBodies bounds the request bodies the middleware holds at once
// to hash them for an assertion: before an assertion is verified its
// caller is unknown, so the memory it may pin is bounded for everyone.
const MaxBufferedBodies = 64

// bodySlot takes one of the slots; false when every one is taken.
func (a *Authenticator) bodySlot() (release func(), ok bool) {
	a.bodyOnce.Do(func() { a.bodySlots = make(chan struct{}, MaxBufferedBodies) })
	select {
	case a.bodySlots <- struct{}{}:
		return func() { <-a.bodySlots }, true
	default:
		return nil, false
	}
}

// preVerify checks an assertion that needs its body read, before it is:
// every rule but the body's hash and the jti's use. An assertion no account
// signed for this request is refused here and never holds a slot.
func (a *Authenticator) preVerify(ctx context.Context, r *http.Request) error {
	token, err := bearer(r)
	if err != nil {
		return &Refusal{Err: err}
	}
	iss, err := jwt.PeekIssuer(token)
	if err != nil {
		return &Refusal{Err: err}
	}
	name, _ := strings.CutPrefix(iss, jwt.ServiceAccountIssuerPrefix)
	hostAllowed := a.HostAllowed
	if hostAllowed == nil {
		hostAllowed = func(string) bool { return false }
	}
	now := time.Now
	if a.Now != nil {
		now = a.Now
	}
	req := jwt.Request{Method: r.Method, Path: r.URL.EscapedPath(), RawQuery: r.URL.RawQuery, HostAllowed: hostAllowed}
	var policy jwt.AssertionPolicy
	var keys jwt.KeySource
	if name == BreakGlassAccount {
		policy = jwt.AssertionPolicy{Account: name, Audience: a.BreakGlass.Audience, Leeway: a.Leeway}
		keys, req.PublicURL = a.BreakGlass.Keys, a.BreakGlass.PublicURL
	} else {
		account, ok, err := a.ServiceAccounts.Lookup(ctx, name)
		if err != nil {
			return &Refusal{Unavailable: true, Err: fmt.Errorf("service account %q could not be looked up: %w", name, err)}
		}
		if !ok {
			return &Refusal{Err: &jwt.Error{Reason: jwt.ReasonKey, Detail: fmt.Sprintf("service account %q is not in the access document", name)}}
		}
		policy = jwt.AssertionPolicy{Account: name, Audience: a.ServiceAccounts.Audience, Leeway: a.Leeway}
		keys, req.PublicURL = account.Keys, a.ServiceAccounts.PublicURL
	}
	if err := jwt.VerifyRequestSigned(ctx, token, policy, keys, req, now()); err != nil {
		return refusal(err)
	}
	return nil
}

// ServiceAccounts is how the authenticator verifies a service account's
// assertion (authn-authz.md §3.2).
type ServiceAccounts struct {
	// Lookup returns the access document's service account name, from the
	// copy this replica decides by. An error is the engine's: the assertion
	// is answered unavailable, never refused.
	Lookup func(ctx context.Context, name string) (authz.ServiceAccount, bool, error)
	// Audience is the deployment's public URL (auth.public_url).
	Audience  string
	PublicURL *url.URL
	// Replay is the accounts' jti cache, apart from break-glass's.
	Replay *jwt.ReplayCache
}

// ErrNoCredential is a request that presents none.
var ErrNoCredential = errors.New("the request presents no credential")

// Refusal is a presented credential refused: never passed on to the next
// method.
type Refusal struct {
	// Unavailable is true when the refusal is the engine's (an issuer's keys
	// could not be read), not the caller's.
	Unavailable bool
	Err         error
}

func (r *Refusal) Error() string { return r.Err.Error() }
func (r *Refusal) Unwrap() error { return r.Err }

func refusal(err error) *Refusal {
	var jerr *jwt.Error
	return &Refusal{Unavailable: errors.As(err, &jerr) && jerr.Reason == jwt.ReasonUnavailable, Err: err}
}

// Request is what one authentication reads: the request, and the hash of
// its body for an assertion bound to it.
type Request struct {
	HTTP       *http.Request
	BodySHA256 [sha256.Size]byte
}

// bearer is the request's bearer token, or "".
func bearer(r *http.Request) (string, error) {
	h := r.Header.Values("Authorization")
	if len(h) == 0 {
		return "", nil
	}
	if len(h) > 1 {
		return "", errors.New("more than one Authorization header")
	}
	scheme, token, ok := strings.Cut(h[0], " ")
	if !ok || !strings.EqualFold(scheme, "Bearer") || token == "" || strings.ContainsAny(token, " \t") {
		return "", errors.New("the Authorization header is not one Bearer token")
	}
	return token, nil
}

// ErrSigninStaleMessage is the refusal of a request under a sign-in past
// its revalidation: the page calls AuthService.RefreshSignin and retries.
const ErrSigninStaleMessage = ReasonSigninStale + ": the sign-in must be refreshed (AuthService.RefreshSignin)"

// AssertionMaxBody bounds the body of a request a self-signed assertion
// authenticates (a service account's or break-glass's), which is read whole
// to hash it before the assertion is verified (authn-authz.md §3.2).
const AssertionMaxBody = 1 << 20

// BreakGlassMaxBody is AssertionMaxBody, by its earlier name.
const BreakGlassMaxBody = AssertionMaxBody

// needsBody reports whether authenticating r reads its body's hash: only a
// token that claims break-glass where break-glass is configured, or a
// service account where service accounts are accepted. The claim is
// unverified, so nothing else makes the engine buffer a body.
func (a *Authenticator) needsBody(r *http.Request) bool {
	if a == nil || (a.BreakGlass == nil && a.ServiceAccounts == nil) {
		return false
	}
	token, err := bearer(r)
	if err != nil || token == "" {
		return false
	}
	iss, err := jwt.PeekIssuer(token)
	if err != nil || !strings.HasPrefix(iss, jwt.ServiceAccountIssuerPrefix) {
		return false
	}
	if iss == jwt.ServiceAccountIssuerPrefix+BreakGlassAccount {
		return a.BreakGlass != nil
	}
	return a.ServiceAccounts != nil && a.ServiceAccounts.Lookup != nil
}

// Authenticate verifies the request's credential. No credential is
// ErrNoCredential; a credential presented and refused is a *Refusal, and
// never falls through to another method.
func (a *Authenticator) Authenticate(ctx context.Context, req Request) (*Principal, error) {
	token, err := bearer(req.HTTP)
	if err != nil {
		return nil, &Refusal{Err: err}
	}
	if token == "" {
		return nil, ErrNoCredential
	}
	now := time.Now
	if a.Now != nil {
		now = a.Now
	}
	iss, err := jwt.PeekIssuer(token)
	if err != nil {
		return nil, &Refusal{Err: err}
	}
	if iss == jwt.ServiceAccountIssuerPrefix+BreakGlassAccount {
		return a.breakGlass(ctx, token, req, now())
	}
	if account, ok := strings.CutPrefix(iss, jwt.ServiceAccountIssuerPrefix); ok {
		return a.serviceAccount(ctx, account, token, req, now())
	}
	issuer, ok := a.Issuers.ByIssuer(iss)
	if !ok {
		return nil, &Refusal{Err: errors.New("the token's issuer is not a trusted issuer")}
	}
	keys := a.Keys[issuer.Name]
	if keys == nil {
		return nil, &Refusal{Unavailable: true, Err: fmt.Errorf("issuer %q has no key set", issuer.Name)}
	}
	claims, err := jwt.Verify(ctx, token, issuer.Policy(a.Leeway), keys, now())
	if err != nil {
		return nil, refusal(err)
	}
	p, _, err := PrincipalFromClaims(issuer, claims, MethodJWT)
	if err != nil {
		return nil, &Refusal{Err: err}
	}
	p.Expires = claims.Expiry
	return p, nil
}

func (a *Authenticator) breakGlass(ctx context.Context, token string, req Request, now time.Time) (*Principal, error) {
	if a.BreakGlass == nil {
		return nil, &Refusal{Err: errors.New("break-glass is not configured")}
	}
	reason := req.HTTP.Header.Values(ReasonHeader)
	if len(reason) != 1 || reason[0] == "" || len(reason[0]) > 256 || strings.ContainsAny(reason[0], "\r\n") || !printable(reason[0]) {
		return nil, &Refusal{Err: fmt.Errorf("a break-glass request carries one %s header of 1 to 256 bytes on one line", ReasonHeader)}
	}
	hostAllowed := a.HostAllowed
	if hostAllowed == nil {
		hostAllowed = func(string) bool { return false }
	}
	claims, err := jwt.VerifyRequest(ctx, token,
		jwt.AssertionPolicy{Account: BreakGlassAccount, Audience: a.BreakGlass.Audience, Leeway: a.Leeway},
		a.BreakGlass.Keys,
		jwt.Request{Method: req.HTTP.Method, Path: req.HTTP.URL.EscapedPath(), RawQuery: req.HTTP.URL.RawQuery,
			BodySHA256: req.BodySHA256, HostAllowed: hostAllowed, PublicURL: a.BreakGlass.PublicURL},
		a.BreakGlass.Replay, now)
	if err != nil {
		return nil, refusal(err)
	}
	return &Principal{Kind: KindService, ID: ServiceAccountID(BreakGlassAccount), Display: "break-glass",
		Issuer: "config", Method: MethodBreakGlass, Expires: claims.Expiry}, nil
}

func (a *Authenticator) serviceAccount(ctx context.Context, name, token string, req Request, now time.Time) (*Principal, error) {
	sa := a.ServiceAccounts
	if sa == nil || sa.Lookup == nil {
		return nil, &Refusal{Err: errors.New("service-account assertions are not accepted by this engine")}
	}
	account, ok, err := sa.Lookup(ctx, name)
	if err != nil {
		return nil, &Refusal{Unavailable: true, Err: fmt.Errorf("service account %q could not be looked up: %w", name, err)}
	}
	if !ok {
		// Answered as a key the account does not hold, so a caller cannot
		// tell an account that exists from one that does not.
		return nil, &Refusal{Err: &jwt.Error{Reason: jwt.ReasonKey, Detail: fmt.Sprintf("service account %q is not in the access document", name)}}
	}
	hostAllowed := a.HostAllowed
	if hostAllowed == nil {
		hostAllowed = func(string) bool { return false }
	}
	claims, err := jwt.VerifyRequest(ctx, token,
		jwt.AssertionPolicy{Account: name, Audience: sa.Audience, Leeway: a.Leeway},
		account.Keys,
		jwt.Request{Method: req.HTTP.Method, Path: req.HTTP.URL.EscapedPath(), RawQuery: req.HTTP.URL.RawQuery,
			BodySHA256: req.BodySHA256, HostAllowed: hostAllowed, PublicURL: sa.PublicURL},
		sa.Replay, now)
	if err != nil {
		return nil, refusal(err)
	}
	return &Principal{Kind: KindService, ID: ServiceAccountID(name), Display: name, Issuer: "serviceaccount",
		Method: MethodServiceAccount, OrkerID: account.OrkerID, Expires: claims.Expiry}, nil
}

// BreakGlassReason is the reason a break-glass request gave.
func BreakGlassReason(r *http.Request) string { return r.Header.Get(ReasonHeader) }

// MiddlewareOptions configures the HTTP middleware.
type MiddlewareOptions struct {
	// Mode is observe or enforce; asserted installs no middleware.
	Mode string
	// Mux is the mux the middleware wraps: a request is classified by the
	// pattern Mux.Handler says serves it, never by its path.
	Mux *http.ServeMux
	// Exempt reports a pattern the middleware does not authenticate.
	Exempt func(pattern string) bool
	// Authenticator verifies credentials.
	Authenticator *Authenticator
	// OnBreakGlass is called before a break-glass request is served, with
	// the principal and its reason; it writes the audit record. It never
	// stops the request.
	OnBreakGlass func(r *http.Request, p *Principal, reason string)
	// Observe is told of every request that presented a credential: its
	// method ("bearer" when it was refused before one was known) and its
	// outcome, authenticated, refused or unavailable.
	Observe func(r *http.Request, method, outcome string)
	// Signins reads the sign-in cookie of a request that presents no
	// bearer; nil reads none.
	Signins *Signins
	// StaleSigninServed reports a pattern served under a sign-in past its
	// revalidation (the refresh and the sign-out); every other pattern
	// refuses one SIGNIN_STALE.
	StaleSigninServed func(pattern string) bool
	// CSRFExempt reports the one pattern a cookie request may call without
	// Ork-CSRF: the call that hands the token out.
	CSRFExempt func(pattern string) bool
	// Logger is told why a credential could not be checked; nil is
	// slog.Default.
	Logger *slog.Logger
}

// Middleware classifies a request by the pattern the mux matched,
// authenticates it, and puts the principal and the mode in its context. A
// pattern Exempt does not name is authenticated: deny by default. In
// observe mode a request with no credential is still served, with no
// principal; in enforce mode it is refused, unauthenticated. A presented
// credential that is refused is refused in both.
func Middleware(next http.Handler, opts MiddlewareOptions) http.Handler {
	if opts.Authenticator == nil || opts.Mux == nil {
		// Misconfigured: refuse everything rather than serve it unchecked.
		return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			refuse(w, http.StatusServiceUnavailable, "unavailable", "authentication is not configured on this engine")
		})
	}
	var lines LogBound // what a presented credential can make it log
	logger := opts.Logger
	if logger == nil {
		logger = slog.Default()
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx := WithMode(r.Context(), opts.Mode)
		r = r.WithContext(ctx)
		_, pattern := opts.Mux.Handler(r)
		if opts.Exempt != nil && opts.Exempt(pattern) {
			next.ServeHTTP(w, r)
			return
		}
		req := Request{HTTP: r}
		if opts.Authenticator.needsBody(r) {
			const limit = AssertionMaxBody
			if r.ContentLength > limit {
				refuse(w, http.StatusRequestEntityTooLarge, "resource_exhausted", "the request body is over the bound an assertion is checked under")
				return
			}
			if err := opts.Authenticator.preVerify(ctx, r); err != nil {
				var ref *Refusal
				if errors.As(err, &ref) && ref.Unavailable {
					lines.Do("assertion", func(dropped int) {
						logger.Warn("authentication: an assertion could not be checked now; answered unavailable",
							"path", LogText(r.URL.Path, LogTextMax), "error", logError(err), "more_not_logged", dropped)
					})
					refuse(w, http.StatusServiceUnavailable, "unavailable", "the credential could not be checked now; try again")
					return
				}
				if opts.Observe != nil {
					opts.Observe(r, "bearer", "refused")
				}
				refuse(w, http.StatusUnauthorized, "unauthenticated", refusalMessage(err))
				return
			}
			release, ok := opts.Authenticator.bodySlot()
			if !ok {
				refuse(w, http.StatusServiceUnavailable, "unavailable", "too many assertions are being checked now; try again")
				return
			}
			// The body must arrive within AssertionBodyTimeout, so a caller who
			// trickles one cannot hold a slot; a server that cannot set a
			// deadline (a test's recorder) reads without one.
			rc := http.NewResponseController(w)
			timeout := AssertionBodyTimeout
			if opts.Authenticator.BodyTimeout > 0 {
				timeout = opts.Authenticator.BodyTimeout
			}
			_ = rc.SetReadDeadline(time.Now().Add(timeout))
			body, err := io.ReadAll(io.LimitReader(r.Body, limit+1))
			release() // held while the body is read, not while it is served
			_ = r.Body.Close()
			if err != nil {
				// The deadline stays: the connection is closed, never drained.
				w.Header().Set("Connection", "close")
				refuse(w, http.StatusBadRequest, "invalid_argument", "the request body could not be read")
				return
			}
			if int64(len(body)) > limit {
				w.Header().Set("Connection", "close")
				refuse(w, http.StatusRequestEntityTooLarge, "resource_exhausted", "the request body is over the bound an assertion is checked under")
				return
			}
			_ = rc.SetReadDeadline(time.Time{})
			req.BodySHA256 = sha256.Sum256(body)
			r.Body = io.NopCloser(bytes.NewReader(body))
			r.ContentLength = int64(len(body))
		}
		p, err := opts.Authenticator.Authenticate(ctx, req)
		method := "bearer"
		if errors.Is(err, ErrNoCredential) && opts.Signins != nil && HasCookie(r) {
			method = MethodOIDC
			var view *SigninView
			p, view, err = opts.Signins.Authenticate(ctx, r)
			if err == nil {
				if view.Stale && (opts.StaleSigninServed == nil || !opts.StaleSigninServed(pattern)) {
					if opts.Observe != nil {
						opts.Observe(r, MethodOIDC, "refused")
					}
					refuse(w, http.StatusUnauthorized, "unauthenticated", ErrSigninStaleMessage)
					return
				}
				if cerr := opts.Signins.CheckCSRF(r, view, opts.CSRFExempt != nil && opts.CSRFExempt(pattern)); cerr != nil {
					if opts.Observe != nil {
						opts.Observe(r, MethodOIDC, "refused")
					}
					refuse(w, http.StatusForbidden, "permission_denied", cerr.Error())
					return
				}
				ctx = WithSignin(ctx, view)
				r = r.WithContext(ctx)
			}
		}
		switch {
		case errors.Is(err, ErrNoCredential):
			if opts.Mode == ModeEnforce {
				refuse(w, http.StatusUnauthorized, "unauthenticated", "this deployment requires an authenticated caller")
				return
			}
		case err != nil:
			var ref *Refusal
			outcome := "refused"
			if errors.As(err, &ref) && ref.Unavailable {
				outcome = "unavailable"
			}
			if opts.Observe != nil {
				opts.Observe(r, method, outcome)
			}
			if outcome == "unavailable" {
				lines.Do("credential "+method, func(dropped int) {
					logger.Warn("authentication: a credential could not be checked now; answered unavailable",
						"method", method, "path", LogText(r.URL.Path, LogTextMax), "error", logError(err), "more_not_logged", dropped)
				})
				refuse(w, http.StatusServiceUnavailable, "unavailable", "the credential could not be checked now; try again")
				return
			}
			// The caller is told the refusal's class, never the verifier's
			// detail (which key, which claim).
			refuse(w, http.StatusUnauthorized, "unauthenticated", refusalMessage(err))
			return
		default:
			if opts.Observe != nil {
				opts.Observe(r, p.Method, "authenticated")
			}
			if p.Method == MethodBreakGlass && opts.OnBreakGlass != nil {
				opts.OnBreakGlass(r, p, BreakGlassReason(r))
			}
			r = r.WithContext(WithPrincipal(ctx, p))
		}
		next.ServeHTTP(w, r)
	})
}

// refuse answers in the Connect error shape, which a Connect client reads
// as the code and a plain HTTP client as its status.
func refuse(w http.ResponseWriter, status int, code, message string) {
	w.Header().Set("Content-Type", "application/json")
	if status == http.StatusUnauthorized {
		w.Header().Set("WWW-Authenticate", `Bearer realm="orkstack"`)
	}
	w.WriteHeader(status)
	_ = json.MarshalWrite(w, map[string]string{"code": code, "message": message})
}

// refusalMessage names a refused credential's class: a token rule's reason,
// or that the credential is not one this engine accepts.
func refusalMessage(err error) string {
	if errors.Is(err, ErrSigninEnded) || errors.Is(err, ErrNoRefreshToken) {
		return ErrSigninEnded.Error()
	}
	if reason := jwt.ReasonOf(err); reason != "" {
		return "the credential was refused (" + string(reason) + ")"
	}
	return "the credential is not one this engine accepts"
}
