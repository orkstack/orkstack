package vault

import (
	"context"
	"encoding/json/jsontext"
	"encoding/json/v2"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"net/http"
	"slices"
	"sync"
	"time"

	"github.com/orkstack/orkstack/internal/provider"
)

// revokeTries is how often the revoke of a login nobody will hold is tried.
const revokeTries = 3

// revokePause is how long a revoke waits before it is tried again.
const revokePause = 250 * time.Millisecond

// login is one login token of a session. It is never logged; its accessor,
// which cannot be used to act, may be.
type login struct {
	token, accessor string
	renewable       bool
	// first is the life it was issued with; got and ttl are when it was last
	// asked for or extended, as the request was sent, and for how long: what
	// is left of it is never overstated.
	first, ttl time.Duration
	got        time.Time
	// ended says Vault no longer knew the token when it was last asked.
	ended bool
	// verified says the token may look itself up and revoke itself, so a 403
	// to both is its end.
	verified bool
}

func (l *login) left() time.Duration { return max(l.ttl-time.Since(l.got), 0) }

// Login is POST auth/<auth_mount>/login as a role, with the attempt's token.
// The call is bounded by the call timeout and not by the attempt's
// cancellation: one cancelled half way would leave a token nobody holds and
// nobody checked. A login that answers late is closed by whoever gave it up.
// One whose answer never arrives (a timeout, a connection cut) is Vault's to
// end, at the role's own TTL.
func (p *vaultProvider) Login(ctx context.Context, principal string, who provider.Attempt, token provider.TokenSource) (provider.Session, error) {
	if who.Instance && who.MaxLogin <= 0 {
		// Its login could not be held to a bound nobody stated.
		return nil, failed(provider.Refused, opLogin, "instance-login-unbounded", nil)
	}
	s := &session{p: p, principal: principal, token: token, who: who}
	l, err := s.login(ctx)
	if err != nil {
		return nil, err
	}
	s.logins = []*login{l}
	return s, nil
}

type loginRequest struct {
	Role string `json:"role"`
	JWT  string `json:"jwt"`
}

func (s *session) login(ctx context.Context) (*login, error) {
	jwt, _, err := s.token(ctx)
	if err != nil {
		var refused *provider.TokenRefused
		if errors.As(err, &refused) {
			return nil, err
		}
		return nil, failed(provider.Unavailable, opLogin, "no-identity-token", err)
	}
	p := s.p
	callCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), provider.DefaultCallTimeout)
	defer cancel()
	sent := time.Now()
	body, err := p.call(callCtx, opLogin, http.MethodPost, "auth/"+p.authMount+"/login", "", loginRequest{Role: s.principal, JWT: jwt})
	if err != nil {
		if is(err, "404") {
			slog.Error("vault has no such auth mount: check the provider's auth_mount", "provider", p.name, "auth_mount", p.authMount)
		}
		return nil, err
	}
	a, err := readAnswer(opLogin, body)
	if err != nil || a.Auth == nil || a.Auth.ClientToken == "" {
		if l := strayLogin(body); l != nil {
			p.abandon(ctx, l, "its login's answer was malformed")
		}
		return nil, failed(provider.Unavailable, opLogin, "malformed-answer", nil)
	}
	l := &login{token: a.Auth.ClientToken, accessor: a.Auth.Accessor, renewable: a.Auth.Renewable, got: sent}
	ttl, ends := seconds(a.Auth.LeaseDuration)
	switch {
	case a.Auth.TokenType == "batch":
		// It can be neither revoked nor renewed, so nothing issued for the
		// attempt could end with it.
		return nil, failed(provider.Refused, opLogin, "batch-token", nil)
	case a.Auth.NumUses != 0:
		// Its uses run out at a call nobody can foresee.
		p.abandon(ctx, l, "its token has a number of uses")
		return nil, failed(provider.Refused, opLogin, "login-num-uses", nil)
	case !ends:
		p.abandon(ctx, l, "its token never expires")
		return nil, failed(provider.Refused, opLogin, "login-never-expires", nil)
	case ttl > p.maxLoginTTL:
		p.abandon(ctx, l, "its token outlives max_login_ttl")
		return nil, failed(provider.Refused, opLogin, fmt.Sprintf("login-ttl-%s-over-%s", ttl, p.maxLoginTTL), nil)
	}
	l.first, l.ttl = ttl, ttl
	// A time of its own: a login that was slow to answer is not refused for it.
	checkCtx, done := context.WithTimeout(context.WithoutCancel(ctx), provider.DefaultCallTimeout)
	defer done()
	if err := p.mayEnd(checkCtx, l); err != nil {
		p.abandon(ctx, l, "its token may not look itself up or revoke itself, or that could not be asked")
		return nil, err
	}
	if s.who.Instance {
		if err := p.bounded(checkCtx, l, s.who.MaxLogin); err != nil {
			p.abandon(ctx, l, "an event source's instance has no bound on its token, or that could not be asked")
			return nil, err
		}
	}
	l.verified = true
	return l, nil
}

// The paths a login must be allowed, with the capability each needs: without
// them an ended login cannot be told from a refused one, and what it was
// issued cannot be revoked when the attempt ends. Vault's default policy
// allows both.
var selfPaths = map[string]string{"auth/token/lookup-self": "read", "auth/token/revoke-self": "update"}

type capabilitiesRequest struct {
	Paths []string `json:"paths"`
}

// mayEnd refuses a login whose token may not look itself up or revoke itself.
func (p *vaultProvider) mayEnd(ctx context.Context, l *login) error {
	refused := failed(provider.Refused, opLogin, "login-cannot-be-revoked", nil)
	body, err := p.call(ctx, opLogin, http.MethodPost, "sys/capabilities-self", l.token,
		capabilitiesRequest{Paths: slices.Sorted(maps.Keys(selfPaths))})
	if is(err, "403") {
		return refused
	}
	if err != nil {
		return err
	}
	a, err := readAnswer(opLogin, body)
	if err != nil {
		return err
	}
	var granted map[string]jsontext.Value
	if json.Unmarshal(a.Data, &granted) != nil {
		return failed(provider.Unavailable, opLogin, "malformed-answer", nil)
	}
	for path, need := range selfPaths {
		var have []string
		if json.Unmarshal(granted[path], &have) != nil || !slices.Contains(have, need) && !slices.Contains(have, "root") {
			return refused
		}
	}
	return nil
}

// bounded refuses an instance's login that could outlive limit: an instance
// runs for as long as its source does, so its token must have no period and
// an explicit maximum at or below limit (§8, O97).
func (p *vaultProvider) bounded(ctx context.Context, l *login, limit time.Duration) error {
	body, err := p.call(ctx, opLogin, http.MethodGet, "auth/token/lookup-self", l.token, nil)
	if err != nil {
		return err
	}
	a, err := readAnswer(opLogin, body)
	if err != nil {
		return err
	}
	var token struct {
		Period         int64 `json:"period"`
		ExplicitMaxTTL int64 `json:"explicit_max_ttl"`
	}
	if a.Data.Kind() != '{' || json.Unmarshal(a.Data, &token) != nil {
		return failed(provider.Unavailable, opLogin, "malformed-answer", nil)
	}
	if most, ok := seconds(token.ExplicitMaxTTL); !ok || most > limit || token.Period != 0 {
		return failed(provider.Refused, opLogin, "vault-token-unbounded", nil)
	}
	return nil
}

// strayLogin is the login of an answer that cannot be used, when it holds a
// token all the same, read as leniently as JSON can be: to revoke it, never
// to use it.
func strayLogin(body []byte) *login {
	var a struct {
		Auth struct {
			ClientToken string `json:"client_token"`
			Accessor    string `json:"accessor"`
		} `json:"auth"`
	}
	_ = json.Unmarshal(body, &a, jsontext.AllowDuplicateNames(true), jsontext.AllowInvalidUTF8(true))
	if a.Auth.ClientToken == "" {
		return nil
	}
	return &login{token: a.Auth.ClientToken, accessor: a.Auth.Accessor}
}

// abandon revokes a login that is not used. When every try fails its
// accessor is logged, so an operator can revoke it by that.
func (p *vaultProvider) abandon(ctx context.Context, l *login, why string) {
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), provider.CloseTimeout)
	defer cancel()
	_ = p.revoke(ctx, l, why)
}

// revoke revokes a login, trying again while ctx lasts and Vault does not
// answer. A token Vault no longer knows is revoked already. A login it could not revoke is logged by its accessor, so an
// operator can.
func (p *vaultProvider) revoke(ctx context.Context, l *login, why string) error {
	var err error
	for try := range revokeTries {
		if try > 0 {
			select {
			case <-ctx.Done():
			case <-time.After(revokePause):
			}
		}
		_, err = p.call(ctx, opClose, http.MethodPost, "auth/token/revoke-self", l.token, nil)
		if is(err, "403") && l.verified {
			_, lerr := p.call(ctx, opClose, http.MethodGet, "auth/token/lookup-self", l.token, nil)
			if lerr != nil && provider.ClassOf(lerr) == provider.Unavailable {
				err = lerr // whether it lives could not be asked: try again
			}
			if is(lerr, "403") {
				slog.Info("vault no longer knows a login; nothing to revoke", "provider", p.name, "why", why, "accessor", l.accessor)
				return nil
			}
		}
		if err == nil || ctx.Err() != nil || provider.ClassOf(err) != provider.Unavailable {
			break
		}
	}
	if err != nil {
		slog.Error("a vault login could not be revoked; revoke it by its accessor", "provider", p.name, "why", why, "accessor", l.accessor, "error", err)
	}
	return err
}

// session is one attempt's login. Its login token may be replaced when it
// ends before the engine expected; every token it was given is revoked when
// it closes, and not before, since a lease dies with the token that made it.
type session struct {
	p         *vaultProvider
	principal string
	token     provider.TokenSource
	who       provider.Attempt

	// relogin is held while the session logs in again, so two calls that
	// both find the token ended log in once.
	relogin sync.Mutex

	// closing is held by a Close, so a second one waits for the first.
	closing sync.Mutex

	mu     sync.Mutex
	logins []*login // the last is in use
	closed bool
}

func (s *session) current() (*login, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil, failed(provider.Unavailable, opClose, "closed", nil)
	}
	return s.logins[len(s.logins)-1], nil
}

// as makes a call with the session's login. A 403 may be a token that
// expired or was revoked: when Vault no longer knows the token, the session
// logs in once more and repeats the call, and any other 403 is Vault's
// answer.
func (s *session) as(ctx context.Context, op, method, path string, in any) ([]byte, error) {
	l, err := s.current()
	if err != nil {
		return nil, err
	}
	body, err := s.p.call(ctx, op, method, path, l.token, in)
	if !is(err, "403") {
		return body, err
	}
	if _, serr := s.p.call(ctx, op, http.MethodGet, "auth/token/lookup-self", l.token, nil); serr == nil {
		return nil, err
	} else if !is(serr, "403") {
		// Whether the token ended could not be asked: never a refusal.
		return nil, serr
	}
	next, lerr := s.again(ctx, l)
	if lerr != nil {
		return nil, duringCall(op, lerr)
	}
	return s.p.call(ctx, op, method, path, next.token, in)
}

// duringCall is a login that failed inside another call. A refused login is
// not the refusal of the secret or credential that was asked for, so it is
// Unavailable there: the engine's own login meets the refusal and says whose
// it is. A token source's refusal passes through.
func duringCall(op string, err error) error {
	var refused *provider.TokenRefused
	if errors.As(err, &refused) || provider.ClassOf(err) == provider.Unavailable {
		return err
	}
	return failed(provider.Unavailable, op, "login-refused", err)
}

// maxLogins bounds the logins of one session that have not run out: a token
// that keeps ending before its time is a fault to report, not to outrun.
const maxLogins = 16

// maxKept bounds the logins a session keeps to revoke at its close.
const maxKept = 4 * maxLogins

// again logs in again in place of ended, unless another call already did.
func (s *session) again(ctx context.Context, ended *login) (*login, error) {
	s.relogin.Lock()
	defer s.relogin.Unlock()
	l, err := s.current()
	if err != nil || l != ended {
		return l, err
	}
	s.mu.Lock()
	live := 0
	for _, l := range s.logins {
		if l.left() > 0 {
			live++
		}
	}
	s.mu.Unlock()
	if live >= maxLogins {
		return nil, failed(provider.Unavailable, opLogin, "too-many-logins", nil)
	}
	if l, err = s.login(ctx); err != nil {
		return nil, err
	}
	s.mu.Lock()
	closed := s.closed
	if !closed {
		ended.ended = true
		if len(s.logins) >= maxKept {
			// Every one that ran out is tried at the close, up to a bound:
			// Vault may have extended one whose renewal was given up on.
			s.logins = slices.DeleteFunc(s.logins, func(l *login) bool { return l.left() == 0 })
		}
		s.logins = append(s.logins, l)
	}
	s.mu.Unlock()
	if closed {
		s.p.abandon(ctx, l, "the session closed while it logged in")
		return nil, failed(provider.Unavailable, opLogin, "closed", nil)
	}
	return l, nil
}

func (s *session) Secret(ctx context.Context, ref provider.Parsed) (provider.Value, error) {
	d, ok := ref.Detail.(detail)
	if !ok || d.path == "" || ref.Ref.Kind != provider.KindSecret {
		return provider.Value{}, failed(provider.Invalid, opRead, "reference", nil)
	}
	body, err := s.as(ctx, opRead, http.MethodGet, d.path, nil)
	if err != nil {
		return provider.Value{}, err
	}
	return secretValue(body, d)
}

// The files of a credential that is the login itself.
const (
	tokenFile   = "token"
	addressFile = "address"
)

// loginHandle is the handle of a bundle that is the login itself.
type loginHandle struct{ l *login }

func (s *session) Credential(ctx context.Context, ref provider.Parsed) (provider.Bundle, error) {
	d, ok := ref.Detail.(detail)
	if !ok || ref.Ref.Kind != provider.KindCredential {
		return provider.Bundle{}, failed(provider.Invalid, opIssue, "reference", nil)
	}
	if d.path == "" {
		return s.loginCredential(ctx)
	}
	sent := time.Now()
	body, err := s.as(ctx, opIssue, http.MethodGet, d.path, nil)
	if err != nil {
		return provider.Bundle{}, err
	}
	return leaseBundle(body, sent)
}

// loginCredential is the login itself as a bundle. A login with too little
// left to be one is made again; a role whose logins never last that long is
// refused, since nothing Vault would answer later changes it.
func (s *session) loginCredential(ctx context.Context) (provider.Bundle, error) {
	l, err := s.current()
	if err != nil {
		return provider.Bundle{}, err
	}
	if b, ok := s.loginBundle(l); ok {
		return b, nil
	}
	if l.first <= provider.MinBundleLifetime {
		return provider.Bundle{}, failed(provider.Refused, opIssue, fmt.Sprintf("login-ttl-%s-not-over-%s", l.first, provider.MinBundleLifetime), nil)
	}
	if l, err = s.again(ctx, l); err != nil {
		return provider.Bundle{}, duringCall(opIssue, err)
	}
	if b, ok := s.loginBundle(l); ok {
		return b, nil
	}
	return provider.Bundle{}, failed(provider.Unavailable, opIssue, "login-too-short", nil)
}

// loginBundle is l as a bundle; ok is false when too little of it is left:
// a lifetime of zero would say it never ends.
func (s *session) loginBundle(l *login) (provider.Bundle, bool) {
	s.mu.Lock()
	left := l.left()
	s.mu.Unlock()
	return provider.Bundle{
		Files:    map[string][]byte{tokenFile: []byte(l.token), addressFile: []byte(s.p.address)},
		Plain:    map[string]bool{addressFile: true},
		Secrets:  [][]byte{[]byte(l.token)},
		Lifetime: left,
		Handle:   loginHandle{l},
	}, left >= provider.MinBundleLifetime
}

type renewRequest struct {
	LeaseID string `json:"lease_id"`
}

// Renew extends a bundle's lease, or the login a bundle is. What Vault will
// no longer extend, because it is at its maximum, is asked for anew.
func (s *session) Renew(ctx context.Context, b provider.Bundle) (provider.Bundle, error) {
	switch h := b.Handle.(type) {
	case loginHandle:
		l, err := s.current()
		if err != nil {
			return provider.Bundle{}, err
		}
		if l != h.l {
			return provider.Bundle{}, provider.ErrReissue
		}
		if _, err := s.RenewLogin(ctx); err != nil {
			return provider.Bundle{}, err
		}
		b, ok := s.loginBundle(l)
		if !ok {
			return provider.Bundle{}, provider.ErrReissue
		}
		return b, nil
	case lease:
		return s.renewLease(ctx, b, h)
	}
	return provider.Bundle{}, provider.ErrReissue
}

func (s *session) renewLease(ctx context.Context, b provider.Bundle, h lease) (provider.Bundle, error) {
	if !h.renewable {
		return provider.Bundle{}, provider.ErrReissue
	}
	sent := time.Now()
	left := h.life - sent.Sub(h.got)
	body, err := s.as(ctx, opRenew, http.MethodPut, "sys/leases/renew", renewRequest{LeaseID: h.id})
	if notRenewable(err) {
		return provider.Bundle{}, provider.ErrReissue
	}
	if err != nil {
		return provider.Bundle{}, err
	}
	a, err := readAnswer(opRenew, body)
	if err != nil {
		return provider.Bundle{}, err
	}
	life, ok := bundleLife(a.LeaseDuration)
	if !ok || !extended(life, left, h.first) {
		return provider.Bundle{}, provider.ErrReissue
	}
	if a.LeaseID != "" {
		h.id = a.LeaseID
	}
	h.renewable, h.got, h.life = a.Renewable, sent, life
	b.Lifetime, b.Handle = life, h
	return b, nil
}

// notRenewable is Vault's refusal to extend what it was asked to: the lease
// is gone or is not one it extends (400, 404), or the login may not (403).
func notRenewable(err error) bool { return is(err, "400") || is(err, "403") || is(err, "404") }

// renewalRounding is what a renewal must add to count as one. Vault counts a
// life in whole seconds, and ours from when the request was sent, so at a
// maximum its answer can be a moment more than what we hold to be left.
const renewalRounding = 2 * time.Second

// extended reports whether a renewal is worth keeping: it must leave more
// than was left, or the whole of the first life again, as one that follows
// another at once does; and at least half of the first life and the shortest
// life a bundle may have. Vault answers a renewal near the maximum with what
// is left of it, which shrinks to nothing.
func extended(life, left, first time.Duration) bool {
	more := life > left+renewalRounding || life >= first-renewalRounding
	return more && life >= max(first/2, provider.MinBundleLifetime)
}

var _ provider.LoginRenewer = (*session)(nil)

// RenewLogin extends the login token and says how long it now lasts.
// provider.ErrReissue says it is at its maximum, or is not one Vault
// extends: log in again.
func (s *session) RenewLogin(ctx context.Context) (time.Duration, error) {
	l, err := s.current()
	if err != nil {
		return 0, err
	}
	sent := time.Now()
	s.mu.Lock()
	renewable, left := l.renewable, l.left()
	s.mu.Unlock()
	if !renewable {
		return 0, provider.ErrReissue
	}
	body, err := s.p.call(ctx, opRenew, http.MethodPost, "auth/token/renew-self", l.token, nil)
	if notRenewable(err) {
		return 0, provider.ErrReissue
	}
	if err != nil {
		return 0, err
	}
	a, err := readAnswer(opRenew, body)
	if err != nil {
		return 0, err
	}
	if a.Auth == nil {
		return 0, failed(provider.Unavailable, opRenew, "malformed-answer", nil)
	}
	ttl, ok := seconds(a.Auth.LeaseDuration)
	if !ok {
		return 0, provider.ErrReissue
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	// Vault extended it whether or not the extension is used: how long the
	// token lives decides whether it must still be revoked. A renewal that was
	// given up on may answer after a later one, whose answer stands.
	if sent.Before(l.got) {
		if l.ended || s.closed {
			return 0, provider.ErrReissue
		}
		return l.left(), nil
	}
	l.ttl, l.got, l.renewable = ttl, sent, a.Auth.Renewable
	if l.ended || s.closed || ttl > s.p.maxLoginTTL || !extended(ttl, left, l.first) {
		return 0, provider.ErrReissue
	}
	return ttl, nil
}

// Lifetime is what is left of the login in use, at least a moment: zero
// would say that it never ends.
func (s *session) Lifetime() time.Duration {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return time.Nanosecond
	}
	return max(s.logins[len(s.logins)-1].left(), time.Nanosecond)
}

// Close revokes every login of the session, and with each the leases it
// made. One that could not be revoked is kept, so
// a second Close tries it again.
func (s *session) Close(ctx context.Context) error {
	s.closing.Lock()
	defer s.closing.Unlock()
	s.mu.Lock()
	logins := s.logins
	s.closed, s.logins = true, nil
	s.mu.Unlock()
	errs := make([]error, len(logins))
	var wg sync.WaitGroup
	for i, l := range logins {
		wg.Go(func() { errs[i] = s.p.revoke(ctx, l, "its session closed") })
	}
	wg.Wait()
	var first error
	s.mu.Lock()
	defer s.mu.Unlock()
	for i, err := range errs {
		if err != nil {
			s.logins = append(s.logins, logins[i])
			if first == nil {
				first = err
			}
		}
	}
	return first
}
