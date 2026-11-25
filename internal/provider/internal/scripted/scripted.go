// Package scripted is a provider whose every answer a test chooses. It exists
// for the fault cases a real provider cannot produce on demand and in order:
// a refusal on the second refresh and not the first, an outage that ends
// before or after a bundle's expiry, an answer that breaks the bundle rules,
// a renewal that stops at a chosen count. The refresh engine's tests and the
// self-test of providertest.Conformance use it; every built-in provider type
// is tested against a real component instead (LocalStack or Moto for aws, a
// Vault dev server for vault). It is imported by tests only.
//
// Weighed and not used: a Vault dev server and LocalStack answer as their
// policies say, so they cannot refuse the second refresh and not the first,
// hang one call while answering the next, or hand back a bundle that breaks
// the rules; toxiproxy cuts or slows a connection but cannot choose an
// answer's class. Each is the right stand-in for its own provider type's
// test, and none can drive the engine's fault table case by case.
package scripted

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/orkstack/orkstack/internal/provider"
)

// Type is the "scripted" provider type, for documents in tests.
type Type struct{}

func (Type) Name() string { return "scripted" }

// New reads the keys a scripted declaration takes: audience (required) and
// region (optional, shown by Describe).
func (Type) New(name string, c *provider.Config) (provider.Provider, error) {
	audience, ok := c.String("audience")
	if !ok {
		c.Errorf("audience", "is required")
	}
	region, _ := c.String("region")
	p := New(name, audience)
	p.region = region
	return p, nil
}

// Provider is one scripted provider.
type Provider struct {
	name, audience, region string

	mu       sync.Mutex
	admit    map[string]bool
	secrets  map[string][]byte
	lifetime time.Duration
	renewals int
	script   map[string][]error
	down     bool
	issued   int
	tokens   []string
	revoked  map[string]bool
	open     int
	files    []string
	hangs    map[string]<-chan struct{}
	// sessionLife is the login lifetime each new session reports.
	sessionLife time.Duration
	// loginRenewals and renewedLife are SetLoginRenewals'.
	loginRenewals int
	renewedLife   time.Duration
	panicClose    bool
}

// SetSessionLifetime makes each new login report d as its lifetime.
func (p *Provider) SetSessionLifetime(d time.Duration) {
	p.mu.Lock()
	p.sessionLife = d
	p.mu.Unlock()
}

// RevokedSerial reports whether the session that issued the bundle of this
// serial ("serial-0001") closed.
func (p *Provider) RevokedSerial(serial string) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.revoked[serial]
}

// New builds a scripted provider that admits no principal until Admit.
func New(name, audience string) *Provider {
	return &Provider{
		name: name, audience: audience,
		admit:   map[string]bool{},
		secrets: map[string][]byte{},
		script:  map[string][]error{},
		revoked: map[string]bool{},
		hangs:   map[string]<-chan struct{}{},
	}
}

// Hang makes the next call of op block, holding nothing, until release is
// closed. A hung "credential" then answers Unavailable; a hung "login"
// answers as it would have, late.
func (p *Provider) Hang(op string, release <-chan struct{}) {
	p.mu.Lock()
	p.hangs[op] = release
	p.mu.Unlock()
}

// waitHang waits out a Hang of op, if one is set, and then lets the call
// answer as it would have: a call that answers late. mu is not held.
func (p *Provider) waitHang(op string) {
	p.mu.Lock()
	release := p.hangs[op]
	delete(p.hangs, op)
	p.mu.Unlock()
	if release != nil {
		<-release
	}
}

// hung waits out a Hang of op, if one is set. mu is not held.
func (p *Provider) hung(op string) error {
	p.mu.Lock()
	release := p.hangs[op]
	delete(p.hangs, op)
	p.mu.Unlock()
	if release == nil {
		return nil
	}
	<-release
	return Unavailable(op)
}

// Admit lets principal log in.
func (p *Provider) Admit(principal string) { p.mu.Lock(); p.admit[principal] = true; p.mu.Unlock() }

// SetSecret stores a value at locator.
func (p *Provider) SetSecret(locator string, value []byte) {
	p.mu.Lock()
	p.secrets[locator] = value
	p.mu.Unlock()
}

// SetLifetime sets the lifetime of each bundle issued from now on, and how
// many renewals extend one before ErrReissue.
func (p *Provider) SetLifetime(life time.Duration, renewals int) {
	p.mu.Lock()
	p.lifetime, p.renewals = life, renewals
	p.mu.Unlock()
}

// SetFiles makes the next bundles hold these file names. "username" is plain.
func (p *Provider) SetFiles(names ...string) { p.mu.Lock(); p.files = names; p.mu.Unlock() }

// Script queues answers for op ("login", "secret", "credential", "renew"):
// each call takes the next; a nil entry answers normally.
func (p *Provider) Script(op string, answers ...error) {
	p.mu.Lock()
	p.script[op] = append(p.script[op], answers...)
	p.mu.Unlock()
}

// Down makes every call answer Unavailable until Up.
func (p *Provider) Down() { p.mu.Lock(); p.down = true; p.mu.Unlock() }

// Up ends an outage.
func (p *Provider) Up() { p.mu.Lock(); p.down = false; p.mu.Unlock() }

// Tokens are the identity tokens presented, in order.
func (p *Provider) Tokens() []string { p.mu.Lock(); defer p.mu.Unlock(); return slices.Clone(p.tokens) }

// Issued is how many bundles Credential issued.
func (p *Provider) Issued() int { p.mu.Lock(); defer p.mu.Unlock(); return p.issued }

// Open is how many sessions are logged in and not closed.
func (p *Provider) Open() int { p.mu.Lock(); defer p.mu.Unlock(); return p.open }

// Revoked reports whether the bundle's issuing session closed.
func (p *Provider) Revoked(b provider.Bundle) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	h, _ := b.Handle.(*handle)
	return h != nil && p.revoked[h.serial]
}

// Panic is a scripted answer that makes the call panic, as a provider
// handed a reply it did not expect might.
func Panic() error { return errPanic }

var errPanic = errors.New("scripted panic")

// Refused is the error a refusal answers with.
func Refused(op string) error {
	return &provider.Error{Class: provider.Refused, Op: op, Code: "AccessDenied"}
}

// Unavailable is the error an outage answers with.
func Unavailable(op string) error {
	return &provider.Error{Class: provider.Unavailable, Op: op, Code: "ServiceUnavailable"}
}

func (p *Provider) Name() string     { return p.name }
func (p *Provider) Type() string     { return "scripted" }
func (p *Provider) Audience() string { return p.audience }

func (p *Provider) LoginScheme() string { return "login" }

func (p *Provider) Describe() provider.Description {
	d := provider.Description{Type: "scripted", Audience: p.audience, Fields: map[string]string{}}
	if p.region != "" {
		d.Fields["region"] = p.region
	}
	return d
}

// Parse accepts any valid locator for a secret, and for a credential either
// no locator (the login itself) or one under "creds/".
func (p *Provider) Parse(ref provider.Ref) (provider.Parsed, error) {
	switch ref.Kind {
	case provider.KindSecret:
		if err := provider.ValidLocator(ref.Locator); err != nil {
			return provider.Parsed{}, err
		}
		return provider.Parsed{Ref: ref}, nil
	case provider.KindCredential:
		if ref.Locator == "" {
			return provider.Parsed{Ref: ref}, nil
		}
		if err := provider.ValidLocator(ref.Locator); err != nil {
			return provider.Parsed{}, err
		}
		if !strings.HasPrefix(ref.Locator, "creds/") || strings.Contains(ref.Locator, "#") {
			return provider.Parsed{}, errors.New("a credential's path is creds/<role>")
		}
		return provider.Parsed{Ref: ref}, nil
	}
	return provider.Parsed{}, fmt.Errorf("unknown kind %s", ref.Kind)
}

// answer is the scripted answer for op. mu is held.
func (p *Provider) answer(op string) error {
	if p.down {
		return Unavailable(op)
	}
	queue := p.script[op]
	if len(queue) == 0 {
		return nil
	}
	p.script[op] = queue[1:]
	if queue[0] == errPanic {
		p.mu.Unlock() // the caller's deferred Unlock runs as the panic unwinds
		defer p.mu.Lock()
		panic("scripted panic")
	}
	return queue[0]
}

// Login asks the token source, then decides by principal.
func (p *Provider) Login(ctx context.Context, principal string, _ provider.Attempt, token provider.TokenSource) (provider.Session, error) {
	tok, _, err := token(ctx)
	p.waitHang("login")
	if err != nil {
		var refused *provider.TokenRefused
		if errors.As(err, &refused) {
			return nil, err
		}
		return nil, &provider.Error{Class: provider.Unavailable, Op: "login", Code: "TokenUnavailable", Err: err}
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	p.tokens = append(p.tokens, tok)
	if err := p.answer("login"); err != nil {
		return nil, err
	}
	if !p.admit[principal] {
		return nil, Refused("login")
	}
	p.open++
	s := &session{p: p, life: p.sessionLife}
	if p.loginRenewals > 0 {
		return &renewingSession{session: s}, nil
	}
	return s, nil
}

// SetLoginRenewals has each later login renewable n times, to life each time;
// after that it answers ErrReissue.
func (p *Provider) SetLoginRenewals(n int, life time.Duration) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.loginRenewals, p.renewedLife = n, life
}

// renewingSession is a login that can be extended, as a Vault token's can.
type renewingSession struct {
	*session
	renewed int
}

// RenewLogin extends the login, unless "renew-login" is scripted otherwise.
func (s *renewingSession) RenewLogin(context.Context) (time.Duration, error) {
	s.p.waitHang("renew-login")
	s.p.mu.Lock()
	defer s.p.mu.Unlock()
	if err := s.p.answer("renew-login"); err != nil {
		return 0, err
	}
	if s.renewed == s.p.loginRenewals {
		return 0, provider.ErrReissue
	}
	s.renewed++
	return s.p.renewedLife, nil
}

type session struct {
	p       *Provider
	life    time.Duration
	serials []string
	closed  bool
}

type handle struct {
	serial  string
	renewed int
}

func (s *session) Secret(ctx context.Context, ref provider.Parsed) (provider.Value, error) {
	p := s.p
	p.mu.Lock()
	defer p.mu.Unlock()
	if err := p.answer("secret"); err != nil {
		return provider.Value{}, err
	}
	path, _, _ := strings.Cut(ref.Ref.Locator, "#")
	v, ok := p.secrets[path]
	if !ok {
		return provider.Value{}, &provider.Error{Class: provider.NotFound, Op: "secret", Code: "NotFound"}
	}
	return provider.Value{Bytes: slices.Clone(v)}, nil
}

func (s *session) Credential(ctx context.Context, ref provider.Parsed) (provider.Bundle, error) {
	p := s.p
	if err := p.hung("credential"); err != nil {
		return provider.Bundle{}, err
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if err := p.answer("credential"); err != nil {
		return provider.Bundle{}, err
	}
	p.issued++
	serial := fmt.Sprintf("serial-%04d", p.issued)
	s.serials = append(s.serials, serial)
	return p.bundle(serial, 0), nil
}

// bundle is one answer. mu is held.
func (p *Provider) bundle(serial string, renewed int) provider.Bundle {
	files := map[string][]byte{}
	plain := map[string]bool{}
	names := p.files
	if names == nil {
		names = []string{"password", "username"}
	}
	for _, name := range names {
		if name == "username" {
			files[name] = []byte("user-" + serial)
			plain[name] = true
			continue
		}
		files[name] = []byte(fmt.Sprintf("%s-%s-secret-%d", name, serial, renewed))
	}
	return provider.Bundle{Files: files, Plain: plain, Lifetime: p.lifetime, Handle: &handle{serial: serial, renewed: renewed}}
}

func (s *session) Renew(ctx context.Context, b provider.Bundle) (provider.Bundle, error) {
	p := s.p
	p.mu.Lock()
	defer p.mu.Unlock()
	if err := p.answer("renew"); err != nil {
		return provider.Bundle{}, err
	}
	h, _ := b.Handle.(*handle)
	if h == nil || h.renewed >= p.renewals {
		return provider.Bundle{}, provider.ErrReissue
	}
	return p.bundle(h.serial, h.renewed+1), nil
}

func (s *session) Lifetime() time.Duration { return s.life }

// PanicOnClose makes every session's Close panic.
func (p *Provider) PanicOnClose() { p.mu.Lock(); p.panicClose = true; p.mu.Unlock() }

func (s *session) Close(ctx context.Context) error {
	p := s.p
	p.mu.Lock()
	if p.panicClose {
		p.mu.Unlock()
		panic("scripted close panic")
	}
	defer p.mu.Unlock()
	if s.closed {
		return nil
	}
	s.closed = true
	p.open--
	for _, serial := range s.serials {
		p.revoked[serial] = true
	}
	return nil
}
