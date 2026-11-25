package provider

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"regexp"
	"slices"
	"sync"
	"time"

	"github.com/orkstack/orkstack/internal/task"
	"github.com/orkstack/orkstack/pkg/logstream"
)

// RefreshPolicy is a credential's `refresh:`.
type RefreshPolicy int

const (
	// RefreshRequired stops the container when the credential expires
	// unrefreshed. It is the default.
	RefreshRequired RefreshPolicy = iota
	// RefreshBestEffort leaves the files in place at expiry and writes one
	// line to the task's log; the loop keeps trying.
	RefreshBestEffort
)

func (p RefreshPolicy) String() string {
	if p == RefreshBestEffort {
		return "best_effort"
	}
	return "required"
}

// The engine's defaults that are not pacing.
const (
	// DefaultMaskGrace is orker.log.mask_grace: how long a replaced bundle's
	// values stay masked past its own expiry, because a process may hold and
	// print it until then (secrets-and-masking.md §3.3).
	DefaultMaskGrace = 5 * time.Minute
	// HoldLimit is how long a bundle the provider gave is kept, and its write
	// retried, after a local write fault, before it is dropped and the next
	// try starts from a fresh identity token (providers.md §6).
	HoldLimit = 60 * time.Second
	// WatchdogInterval is the watchdog's ticker.
	WatchdogInterval = 5 * time.Second
)

// Clock is the engine's two clocks (workload-identity.md §6). Boot keeps
// counting while the host is suspended, as CLOCK_BOOTTIME does; Wall is the
// wall time a bundle's `expiration` is written in and checked against. A
// lifetime is counted on both from the moment its request was sent, and what
// is on disk expires at the earlier.
type Clock interface {
	Boot() time.Duration
	Wall() time.Time
}

// Masker is the attempt's log masker (pkg/logstream.Masker). A holder holds
// the forms of the values it added until it is dropped; a form leaves with
// its last holder.
type Masker interface {
	Add(holder string, value []byte) (bool, error)
	Drop(holder string)
}

// ResultGuard is a Masker that also keeps a value out of a task's result,
// under a holder it has masked it under, until that is dropped. It is
// narrower than the mask: only what is certainly secret is guarded
// (secrets-and-masking.md, the values.json check).
type ResultGuard interface {
	Guard(holder string, value []byte)
}

// guard keeps value out of the task's result, where the masker can.
func (e *Engine) guard(holder string, value []byte) {
	if g, ok := e.cfg.Masker.(ResultGuard); ok && len(value) > 0 {
		g.Guard(holder, value)
	}
}

// Grant is one grant an attempt uses: where it logs in, and as whom.
type Grant struct {
	Provider  Provider
	Principal string
	// Token mints the attempt's identity token for Provider.Audience(). It
	// is the only credential the provider is given.
	Token TokenSource
}

// CredentialSpec is one entry of a task's credentials:, parsed.
type CredentialSpec struct {
	Name   string
	Grant  string
	Ref    Parsed
	Policy RefreshPolicy
}

// TokenRefused is what a TokenSource returns when the engine refused to mint
// (IDENTITY_REVOKED, a digest that differs): a refusal with its own reason,
// believed at once and never retried. Any other error of a TokenSource is
// "could not be asked".
type TokenRefused struct {
	Reason string
	Err    error
}

func (e *TokenRefused) Error() string { return "identity token refused: " + e.Reason }

// reasonShape is a reason code: what a TokenRefused must carry to be believed.
var reasonShape = regexp.MustCompile(`^[A-Z][A-Z0-9_]{2,63}$`)

// tokenRefusal is err's TokenRefused, when it carries a reason of a reason's
// shape. One that does not is "could not be asked", never a reason of its
// own: a reason is the engine's word, and it is stored in history.
func tokenRefusal(err error) (*TokenRefused, bool) {
	var refused *TokenRefused
	if !errors.As(err, &refused) || !reasonShape.MatchString(refused.Reason) {
		return nil, false
	}
	return refused, true
}
func (e *TokenRefused) Unwrap() error { return e.Err }

// Failure is how an attempt ends because of a provider. Detail names the
// credential, secret or grant, the provider and the provider's error code;
// never a value, a token, a principal or a provider's message.
type Failure struct {
	Outcome
	Detail string
}

func (f *Failure) Error() string { return f.Reason + ": " + f.Detail }

// Hooks are how the engine reaches the Orker.
type Hooks struct {
	// TaskLine writes one "[ork]" line to the task's log.
	TaskLine func(line string)
	// Stop stops the container now, through the attempt's own cancellation,
	// and ends the attempt with f. It is called at most once.
	Stop func(f Failure)
	// Logger is the Orker's log.
	Logger *slog.Logger
	// Refreshed counts one refresh's outcome (ork.credential.refresh).
	Refreshed func(provider, outcome string)
	// Closed counts one session's close (ork.provider.close).
	Closed func(provider string, err error)
}

// EngineConfig is one attempt's engine.
type EngineConfig struct {
	Attempt     Attempt
	Grants      map[string]Grant
	Credentials []CredentialSpec
	// Root is <attempt root>/credentials. Each credential gets a directory
	// under it.
	Root   string
	Masker Masker
	Clock  Clock
	// Limiters bound the Orker's calls to each provider, by name. They are
	// the Orker's, shared by every attempt; a provider with none is not
	// bounded here.
	Limiters    map[string]*Limiter
	RetryMin    time.Duration
	CallTimeout time.Duration
	MaskGrace   time.Duration
	// InstanceMaxTTL is identity.token.instance_max_ttl: for an event
	// source's instance, the oldest a login or a renewed bundle may be
	// before the engine logs in again from a fresh token (providers.md §6).
	InstanceMaxTTL time.Duration
	// Unit is a uniform draw in [0, 1); nil uses math/rand/v2.
	Unit func() float64
	// Sleep waits d or until ctx is done; nil uses a real timer. It is how
	// setup waits between tries, and how Run waits between ticks.
	Sleep func(ctx context.Context, d time.Duration) error
	Hooks Hooks
}

// Engine is one attempt's logins and credentials: setup, then a refresh loop
// and a watchdog beside it. Its methods are safe for concurrent use; a
// provider call is never made while its lock is held, so a hung call cannot
// delay the watchdog.
type Engine struct {
	cfg EngineConfig

	mu        sync.Mutex
	grants    map[string]*grantState
	creds     []*credState
	releases  []release
	stopped   bool
	closed    bool
	holderSeq int
	// lines and counts are written under mu and emitted by flush once it is
	// released: a hook may block on the task's log sink, and the watchdog
	// must never wait on one.
	lines []string
	// said are the lines taskLineOnce wrote.
	said   map[string]bool
	counts [][2]string
}

type grantState struct {
	name     string
	grant    Grant
	session  Session
	gen      int
	sentBoot time.Duration
	life     time.Duration
	// madeBoot is when the login was made; sentBoot moves when it is renewed.
	madeBoot time.Duration
	due      time.Duration
	failures int
	refusal  RefusalConfirm
	old      []oldSession
	broken   bool
	// ends is when the last bundle this login issued expires, and forever
	// that one does not.
	ends    time.Duration
	forever bool
	// readSecret is a login a secret was read through: the task's only one.
	readSecret bool
}

type oldSession struct {
	session    Session
	gen        int
	ends       time.Duration
	forever    bool
	readSecret bool
}

type credState struct {
	spec    CredentialSpec
	dir     *Dir
	placed  *bundleState
	held    *bundleState
	heldAt  time.Duration
	due     time.Duration
	retries int
	refusal RefusalConfirm
	// done: the loop asks the provider for this credential no more.
	done bool
	// cause is the last reason a refresh did not land, for the message at
	// expiry; stop is a refusal's own outcome.
	cause         string
	stop          *Outcome
	retrying      bool
	expiredLogged bool
}

type bundleState struct {
	bundle   Bundle
	holder   string
	gen      int
	sentBoot time.Duration
	sentWall time.Time
	// madeBoot is when the chain of renewals this bundle belongs to began:
	// a Credential call starts one, a Renew continues it.
	madeBoot time.Duration
}

type release struct {
	holder string
	at     time.Duration
}

// NewEngine builds an attempt's engine. It does no I/O.
func NewEngine(cfg EngineConfig) (*Engine, error) {
	if cfg.Clock == nil {
		return nil, errors.New("provider engine: no clock")
	}
	if cfg.Masker == nil {
		return nil, errors.New("provider engine: no masker")
	}
	if cfg.RetryMin <= 0 {
		cfg.RetryMin = DefaultRetryMin
	}
	if cfg.CallTimeout <= 0 {
		cfg.CallTimeout = DefaultCallTimeout
	}
	if cfg.MaskGrace <= 0 {
		cfg.MaskGrace = DefaultMaskGrace
	}
	if cfg.Unit == nil {
		cfg.Unit = rand.Float64
	}
	if cfg.Sleep == nil {
		cfg.Sleep = sleep
	}
	if cfg.Hooks.Logger == nil {
		cfg.Hooks.Logger = slog.New(slog.DiscardHandler)
	}
	if cfg.Attempt.Instance && cfg.InstanceMaxTTL > 0 {
		cfg.Attempt.MaxLogin = cfg.InstanceMaxTTL
	}
	e := &Engine{cfg: cfg, grants: map[string]*grantState{}}
	for name, g := range cfg.Grants {
		if g.Provider == nil || g.Token == nil {
			return nil, fmt.Errorf("provider engine: grant %q has no provider or token source", name)
		}
		e.grants[name] = &grantState{name: name, grant: g}
	}
	seen := map[string]bool{}
	for _, spec := range cfg.Credentials {
		if seen[spec.Name] {
			return nil, fmt.Errorf("provider engine: credential %q is declared twice", spec.Name)
		}
		seen[spec.Name] = true
		if err := ValidCredentialName(spec.Name); err != nil {
			return nil, fmt.Errorf("provider engine: %w", err)
		}
		if _, ok := e.grants[spec.Grant]; !ok {
			return nil, fmt.Errorf("provider engine: credential %q names grant %q, which the attempt does not hold", spec.Name, spec.Grant)
		}
		spec.Ref.Ref.Name = spec.Name
		e.creds = append(e.creds, &credState{spec: spec})
	}
	return e, nil
}

// Open logs in once per grant, concurrently, within ctx, which carries the
// setup budget. An Unavailable answer is retried inside the budget by the one
// retry rule; a refusal is believed after RefusalAnswers answers over
// RefusalWindow. A cancellation is returned as ctx's error; every other
// failure is a *Failure.
func (e *Engine) Open(ctx context.Context) error {
	names := make([]string, 0, len(e.grants))
	for name := range e.grants {
		names = append(names, name)
	}
	slices.Sort(names)
	// The first grant that fails ends the others: the attempt fails anyway,
	// and a grant still retrying an outage would hold it to the full budget.
	grantsCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	var (
		wg    sync.WaitGroup
		once  sync.Once
		first error
	)
	for _, name := range names {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := e.openGrant(grantsCtx, e.grants[name]); err != nil && grantsCtx.Err() == nil || isFailure(err) {
				once.Do(func() { first = err; cancel() })
			}
		}()
	}
	wg.Wait()
	if err := ctx.Err(); err != nil && !errors.Is(err, context.DeadlineExceeded) {
		return err
	}
	if first != nil {
		return first
	}
	if err := ctx.Err(); err != nil {
		return &Failure{Outcome: Classify(Unavailable, AtSetup, CallLogin), Detail: "the setup budget ran out"}
	}
	return nil
}

func isFailure(err error) bool {
	var f *Failure
	return errors.As(err, &f)
}

func (e *Engine) openGrant(ctx context.Context, g *grantState) error {
	p := g.grant.Provider
	var confirm RefusalConfirm
	for attempt := 1; ; attempt++ {
		sent := e.cfg.Clock.Boot()
		session, err := e.login(ctx, g)
		if err == nil {
			e.mu.Lock()
			if e.closed {
				e.mu.Unlock()
				e.closeSession(p.Name(), session)
				return context.Canceled
			}
			g.session, g.gen, g.sentBoot, g.madeBoot = session, 1, sent, sent
			g.life = boundLifetime(safeLifetime(session))
			g.due = e.loginDue(g)
			e.mu.Unlock()
			return nil
		}
		if end := e.setupEnded(ctx, err, CallLogin, fmt.Sprintf("grant %s, provider %s", g.name, p.Name())); end != nil {
			return end
		}
		if refused, ok := tokenRefusal(err); ok {
			return e.tokenFailure(refused, "grant "+g.name)
		}
		wait, failure := e.setupAnswer(&confirm, err, CallLogin, attempt, fmt.Sprintf("grant %s, provider %s", g.name, p.Name()))
		if failure != nil {
			return failure
		}
		if serr := e.cfg.Sleep(ctx, wait); serr != nil {
			if end := e.setupEnded(ctx, err, CallLogin, fmt.Sprintf("grant %s, provider %s", g.name, p.Name())); end != nil {
				return end
			}
			return serr
		}
	}
}

// setupEnded is how setup ends when ctx is done: a cancellation is ctx's own
// error, and the attempt ends Cancelled; the setup budget running out ends it
// with the last answer's reason, PROVIDER_UNAVAILABLE for a provider that did
// not answer (workload-identity.md §7). It is nil while ctx is live.
func (e *Engine) setupEnded(ctx context.Context, last error, call Call, what string) error {
	if ctx.Err() == nil {
		return nil
	}
	if !errors.Is(ctx.Err(), context.DeadlineExceeded) {
		return ctx.Err()
	}
	// A refusal that ends the attempt was already believed and returned; one
	// still waiting to be believed is not a refusal yet, so the budget's end
	// is the provider's unavailability whatever the last answer was.
	detail := what + ": the setup budget ran out"
	if last != nil {
		detail += " (" + describe(last) + ")"
	}
	return &Failure{Outcome: Classify(Unavailable, AtSetup, call), Detail: detail}
}

// setupAnswer decides what a failed call at setup does: wait and ask again,
// or end the attempt.
func (e *Engine) setupAnswer(confirm *RefusalConfirm, err error, call Call, attempt int, what string) (time.Duration, *Failure) {
	class := ClassOf(err)
	if class == Unavailable {
		confirm.Observe(nil, time.Time{})
		return RetryDelay(attempt, e.cfg.RetryMin, e.cfg.Unit()), nil
	}
	// Only a refusal waits to be believed; a missing secret or a request
	// the provider calls invalid is final at once.
	if class != Refused || confirm.Observe(err, time.Time{}.Add(e.cfg.Clock.Boot())) {
		outcome := Classify(class, AtSetup, call)
		if class == Refused && e.cfg.Attempt.Instance {
			outcome = instanceOutcome(outcome)
		}
		return 0, &Failure{Outcome: outcome, Detail: what + ": " + describe(err)}
	}
	return confirm.NextTry(), nil
}

// instanceOutcome is a refusal, the provider's or the engine's, for an event
// source's instance: an Orker cannot tell a login revoked at the provider
// from a throttled one, so it is an InfrastructureError, retried, and never a
// pause (providers.md §6). It is applied to refusals only: a missing secret
// or an invalid request is the same for an instance as for a task.
func instanceOutcome(o Outcome) Outcome {
	o.Category, o.Retryable = task.CategoryInfrastructureError, true
	return o
}

func (e *Engine) tokenFailure(refused *TokenRefused, what string) *Failure {
	o := Outcome{Reason: refused.Reason, Category: task.CategoryContractViolation}
	if e.cfg.Attempt.Instance {
		o = instanceOutcome(o)
	}
	return &Failure{Outcome: o, Detail: what + ": the identity token was refused"}
}

func (e *Engine) login(ctx context.Context, g *grantState) (Session, error) {
	provider := g.grant.Provider.Name()
	return within(ctx, e, provider, func(callCtx context.Context) (Session, error) {
		return g.grant.Provider.Login(callCtx, g.grant.Principal, e.cfg.Attempt, g.grant.Token)
	}, func(late Session) {
		// A login that answered after it was given up on is closed at once:
		// nobody holds it, and its token would otherwise live to its end.
		e.closeSession(provider, late)
	})
}

// within makes one provider call under the provider's limiter and the call
// timeout, and gives it up at the timeout whether or not the provider
// honours its context: a provider that ignores ctx cannot wedge the loop.
// The limiter's slot is held until the call really returns, so an abandoned
// call still counts against the bound. late, when not nil, receives a result
// that arrived after the call was given up on.
func within[T any](ctx context.Context, e *Engine, provider string, call func(context.Context) (T, error), late func(T)) (T, error) {
	var zero T
	release, err := e.acquire(ctx, provider)
	if err != nil {
		return zero, err
	}
	callCtx, cancel := context.WithTimeout(ctx, e.cfg.CallTimeout)
	type result struct {
		v   T
		err error
	}
	done := make(chan result, 1)
	var mu sync.Mutex
	abandoned := false
	go func() {
		defer release()
		var r result
		func() {
			// A provider that panics (a malformed reply it did not expect)
			// fails its call; it never takes the Orker down.
			defer func() {
				if p := recover(); p != nil {
					r = result{err: &Error{Class: Unavailable, Op: "call", Code: "Panic"}}
				}
			}()
			r.v, r.err = call(callCtx)
		}()
		// The check and the send are one step under mu, so a result is
		// either received by the caller or handed to late, never lost.
		mu.Lock()
		if abandoned {
			mu.Unlock()
			if r.err == nil && late != nil {
				late(r.v)
			}
			return
		}
		done <- r
		mu.Unlock()
	}()
	select {
	case r := <-done:
		cancel()
		return r.v, r.err
	case <-callCtx.Done():
		mu.Lock()
		select {
		case r := <-done:
			mu.Unlock()
			cancel()
			return r.v, r.err
		default:
			abandoned = true
		}
		mu.Unlock()
		cancel()
		if err := ctx.Err(); err != nil {
			return zero, err
		}
		return zero, &Error{Class: Unavailable, Op: "call", Code: "Timeout", Err: context.DeadlineExceeded}
	}
}

func (e *Engine) acquire(ctx context.Context, provider string) (func(), error) {
	l := e.cfg.Limiters[provider]
	if l == nil {
		return func() {}, nil
	}
	return l.Acquire(ctx)
}

// MaxLifetime bounds a lifetime a provider reports, for a login or a bundle:
// a larger one is read as this. It keeps every sum of instants finite, and no
// credential OrkStack obtains is meant to live longer.
const MaxLifetime = 31 * 24 * time.Hour

// MaxTaskLogin is the longest a task's login is kept since it was made,
// however often it is renewed (the user's decision S1-D5 of 2026-11-24): a
// renewal asks the engine nothing, so a grant taken away is seen within a
// day. The task runs on: its login is replaced, and its credentials issued
// again, as at any refresh.
const MaxTaskLogin = 24 * time.Hour

// maxOldLogins bounds the replaced logins of one grant a task keeps open for
// what they issued: a login that cannot be renewed is replaced every third of
// its life, while a bundle may last a month. Past it the oldest that issued
// nothing still in place is closed.
const maxOldLogins = 32

// loginCap is the longest a login is kept since it was made: an event
// source's instance's is its setting's.
func (e *Engine) loginCap() time.Duration {
	if e.cfg.Attempt.Instance && e.cfg.InstanceMaxTTL > 0 {
		return boundLifetime(e.cfg.InstanceMaxTTL)
	}
	return MaxTaskLogin
}

// safeLifetime is s.Lifetime(), or zero when the provider panics.
func safeLifetime(s Session) (d time.Duration) {
	defer func() {
		if recover() != nil {
			d = 0
		}
	}()
	return s.Lifetime()
}

func boundLifetime(d time.Duration) time.Duration {
	if d > MaxLifetime {
		return MaxLifetime
	}
	if d < 0 {
		return 0
	}
	return d
}

func (e *Engine) loginDue(g *grantState) time.Duration {
	after, ok := RefreshAfter(g.life, e.cfg.Unit())
	// From when the login was made, however often it was renewed since.
	if left := g.madeBoot + e.loginCap() - g.sentBoot; !ok || after > left {
		after = left
	}
	return g.sentBoot + after
}

// Secret reads one provider secret through its grant's session, within ctx,
// which carries the setup budget, and registers its value with the masker
// under its own holder before returning it. The caller writes it as every
// secret is written.
func (e *Engine) Secret(ctx context.Context, name, grant string, ref Parsed) (Value, error) {
	g, ok := e.grants[grant]
	if !ok || g.session == nil {
		return Value{}, fmt.Errorf("provider engine: secret %q names grant %q, which is not open", name, grant)
	}
	what := fmt.Sprintf("secret %q, provider %s", printable(name), g.grant.Provider.Name())
	var confirm RefusalConfirm
	for attempt := 1; ; attempt++ {
		v, err := e.readSecret(ctx, g, ref)
		if err == nil {
			for _, value := range append([][]byte{v.Bytes}, v.Also...) {
				if len(value) > MaxValueBytes {
					return Value{}, &Failure{Outcome: OutcomeTooLarge, Detail: fmt.Sprintf("%s: a value is %d bytes; at most %d", what, len(value), MaxValueBytes)}
				}
			}
			// What a secret's login issued with it lives as long as the task:
			// a secret is read once and never refreshed.
			e.mu.Lock()
			g.readSecret = true
			e.mu.Unlock()
			holder := "secret/" + name
			for i, value := range append([][]byte{v.Bytes}, v.Also...) {
				masked, err := e.cfg.Masker.Add(holder, value)
				if err != nil {
					e.cfg.Masker.Drop(holder)
					return Value{}, &Failure{Outcome: OutcomeMaskCapacity, Detail: what + ": " + maskDetail(err)}
				}
				if i == 0 {
					e.guard(holder, value)
				}
				if !masked && i == 0 && e.cfg.Hooks.TaskLine != nil {
					e.cfg.Hooks.TaskLine(fmt.Sprintf("[ork] secret %q is shorter than %d bytes and is not masked", printable(name), logstream.MaskMinFormBytes))
				}
			}
			return v, nil
		}
		if end := e.setupEnded(ctx, err, CallSecret, what); end != nil {
			return Value{}, end
		}
		if refused, ok := tokenRefusal(err); ok {
			return Value{}, e.tokenFailure(refused, what)
		}
		wait, failure := e.setupAnswer(&confirm, err, CallSecret, attempt, what)
		if failure != nil {
			return Value{}, failure
		}
		if serr := e.cfg.Sleep(ctx, wait); serr != nil {
			if end := e.setupEnded(ctx, err, CallSecret, what); end != nil {
				return Value{}, end
			}
			return Value{}, serr
		}
	}
}

func (e *Engine) readSecret(ctx context.Context, g *grantState, ref Parsed) (Value, error) {
	session := g.session
	return within(ctx, e, g.grant.Provider.Name(), func(callCtx context.Context) (Value, error) {
		return session.Secret(callCtx, ref)
	}, nil)
}

// describe is all a message may say of a provider call's error: a
// provider's *Error prints only its op, class and code. Anything else is not
// shown at all, because a transport or SDK error carries a URL, a role name
// or a response body, and a failure message is stored in history.
func describe(err error) string {
	var pe *Error
	if errors.As(err, &pe) {
		return pe.Error()
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return "the call timed out"
	}
	return "the call failed, unclassified"
}

func maskDetail(err error) string {
	var capacity *logstream.MaskCapacityError
	if errors.As(err, &capacity) {
		return "the masker's bound would be exceeded"
	}
	return "the masker would not take the value"
}

// Issue puts every credential's first bundle in place, within ctx, which
// carries the setup budget. A write fault is WORKDIR_FAILED's, so it is
// returned as the plain error; the caller classifies it as it does any
// other file it cannot write.
func (e *Engine) Issue(ctx context.Context) error {
	for _, c := range e.creds {
		if err := e.issueOne(ctx, c); err != nil {
			return err
		}
	}
	return nil
}

func (e *Engine) issueOne(ctx context.Context, c *credState) error {
	g := e.grants[c.spec.Grant]
	what := fmt.Sprintf("credential %s, provider %s", c.spec.Name, g.grant.Provider.Name())
	dir, err := NewDir(e.cfg.Root, c.spec.Name)
	if err != nil {
		return err
	}
	c.dir = dir
	var confirm RefusalConfirm
	for attempt := 1; ; attempt++ {
		sentBoot, sentWall := e.cfg.Clock.Boot(), e.cfg.Clock.Wall()
		e.mu.Lock()
		session, gen := g.session, g.gen
		e.mu.Unlock()
		b, err := e.credential(ctx, g.grant.Provider.Name(), session, c.spec.Ref)
		if err == nil {
			if verr := ValidBundle(b); verr != nil {
				return &Failure{Outcome: OutcomeContractViolation, Detail: what + ": the provider's answer cannot be put in place: " + verr.Error()}
			}
			bs := &bundleState{bundle: b, gen: gen, sentBoot: sentBoot, sentWall: sentWall, madeBoot: sentBoot}
			if merr := e.hold(bs, c.spec.Name); merr != nil {
				return &Failure{Outcome: OutcomeMaskCapacity, Detail: what + ": " + maskDetail(merr)}
			}
			if perr := c.dir.Put(b, expiresWall(bs)); perr != nil && !isCleanup(perr) {
				e.cfg.Masker.Drop(bs.holder)
				return perr
			}
			e.mu.Lock()
			c.placed = bs
			e.issuedLocked(g, bs)
			c.due = e.bundleDue(bs)
			e.mu.Unlock()
			return nil
		}
		if end := e.setupEnded(ctx, err, CallCredential, what); end != nil {
			return end
		}
		if refused, ok := tokenRefusal(err); ok {
			return e.tokenFailure(refused, what)
		}
		wait, failure := e.setupAnswer(&confirm, err, CallCredential, attempt, what)
		if failure != nil {
			return failure
		}
		if serr := e.cfg.Sleep(ctx, wait); serr != nil {
			if end := e.setupEnded(ctx, err, CallCredential, what); end != nil {
				return end
			}
			return serr
		}
	}
}

// credential and renew give a late answer up: a bundle never put in place is
// dropped, not revoked, as every dropped bundle is (providers.md §6).
func (e *Engine) credential(ctx context.Context, provider string, s Session, ref Parsed) (Bundle, error) {
	return within(ctx, e, provider, func(callCtx context.Context) (Bundle, error) {
		return s.Credential(callCtx, ref)
	}, nil)
}

func (e *Engine) renew(ctx context.Context, provider string, s Session, b Bundle) (Bundle, error) {
	return within(ctx, e, provider, func(callCtx context.Context) (Bundle, error) {
		return s.Renew(callCtx, b)
	}, nil)
}

// hold registers every sensitive file of bs with the masker under a holder of
// its own, and the values in one that is JSON or an INI file, since a process
// prints a key more often than the file it read it from. On an error nothing
// of it stays held.
func (e *Engine) hold(bs *bundleState, name string) error {
	e.mu.Lock()
	e.holderSeq++
	bs.holder = fmt.Sprintf("credential/%s/%d", name, e.holderSeq)
	e.mu.Unlock()
	for _, file := range sortedNames(bs.bundle.Files) {
		if bs.bundle.Plain[file] {
			continue
		}
		masked, err := e.cfg.Masker.Add(bs.holder, bs.bundle.Files[file])
		if err != nil {
			e.cfg.Masker.Drop(bs.holder)
			return err
		}
		if !masked && len(bs.bundle.Files[file]) > 0 {
			e.mu.Lock()
			e.taskLineOnce(fmt.Sprintf("[ork] credential %s: file %s is shorter than %d bytes and is not masked", name, file, logstream.MaskMinFormBytes))
			e.mu.Unlock()
		}
		values, whole := fileValues(bs.bundle.Files[file])
		short := false
		for _, value := range values {
			masked, err := e.cfg.Masker.Add(bs.holder, value)
			if err != nil {
				e.cfg.Masker.Drop(bs.holder)
				return err
			}
			short = short || !masked
		}
		e.mu.Lock()
		if short {
			e.taskLineOnce(fmt.Sprintf("[ork] credential %s: file %s holds a value shorter than %d bytes, which is not masked by itself", name, file, logstream.MaskMinFormBytes))
		}
		if !whole {
			e.taskLineOnce(fmt.Sprintf("[ork] credential %s: file %s holds more than %d values; the first %d are masked by themselves", name, file, maxFileValues, maxFileValues))
		}
		e.mu.Unlock()
	}
	// What is certainly secret is masked by itself too, wherever in a file it
	// is, and is kept out of the task's result.
	for _, value := range bs.bundle.Secrets {
		masked, err := e.cfg.Masker.Add(bs.holder, value)
		if err != nil {
			e.cfg.Masker.Drop(bs.holder)
			return err
		}
		if !masked {
			e.mu.Lock()
			e.taskLineOnce(fmt.Sprintf("[ork] credential %s: a secret value is shorter than %d bytes; it is not masked by itself, nor looked for in values.json", name, logstream.MaskMinFormBytes))
			e.mu.Unlock()
		}
		e.guard(bs.holder, value)
	}
	return nil
}

func isCleanup(err error) bool {
	var c *CleanupError
	return errors.As(err, &c)
}

func expiresWall(bs *bundleState) time.Time {
	if bs.bundle.Lifetime <= 0 {
		return time.Time{}
	}
	return bs.sentWall.Add(bs.bundle.Lifetime)
}

func (e *Engine) bundleDue(bs *bundleState) time.Duration {
	after, ok := RefreshAfter(bs.bundle.Lifetime, e.cfg.Unit())
	if !ok {
		return -1
	}
	return bs.sentBoot + after
}

// expired reports whether what is on disk has expired by either clock.
func (e *Engine) expired(bs *bundleState) bool {
	if bs == nil || bs.bundle.Lifetime <= 0 {
		return false
	}
	if e.cfg.Clock.Boot() >= bs.sentBoot+bs.bundle.Lifetime {
		return true
	}
	return !e.cfg.Clock.Wall().Before(bs.sentWall.Add(bs.bundle.Lifetime))
}

// Tick does everything due now: masker holders whose bundles expired past
// the grace are dropped, logins due are replaced, and credentials due are
// refreshed. It returns how long until something is due next, or a negative
// duration when nothing ever is. Provider calls are bounded by ctx and by the
// call timeout.
func (e *Engine) Tick(ctx context.Context) time.Duration {
	now := e.cfg.Clock.Boot()
	e.dropReleases(now)
	for _, name := range e.grantNames() {
		g := e.grants[name]
		e.mu.Lock()
		due := g.due >= 0 && now >= g.due && !g.broken && !e.stopped
		e.mu.Unlock()
		if due {
			e.relogin(ctx, g)
		}
	}
	for _, c := range e.creds {
		e.mu.Lock()
		due := !c.done && c.due >= 0 && now >= c.due && c.dir != nil && !e.stopped
		e.mu.Unlock()
		if due {
			e.refresh(ctx, c)
		}
	}
	e.closeRetired()
	e.flush()
	return e.nextDue()
}

func (e *Engine) grantNames() []string {
	names := make([]string, 0, len(e.grants))
	for name := range e.grants {
		names = append(names, name)
	}
	slices.Sort(names)
	return names
}

func (e *Engine) dropReleases(now time.Duration) {
	e.mu.Lock()
	var due []string
	kept := e.releases[:0]
	for _, r := range e.releases {
		if now >= r.at {
			due = append(due, r.holder)
		} else {
			kept = append(kept, r)
		}
	}
	e.releases = kept
	e.mu.Unlock()
	for _, holder := range due {
		e.cfg.Masker.Drop(holder)
	}
}

func (e *Engine) nextDue() time.Duration {
	e.mu.Lock()
	defer e.mu.Unlock()
	now := e.cfg.Clock.Boot()
	next := time.Duration(-1)
	consider := func(at time.Duration) {
		if at < 0 {
			return
		}
		d := at - now
		if d < 0 {
			d = 0
		}
		if next < 0 || d < next {
			next = d
		}
	}
	if e.stopped {
		return -1
	}
	for _, g := range e.grants {
		if !g.broken {
			consider(g.due)
		}
		for _, old := range g.old {
			if !old.forever && old.ends > now {
				consider(old.ends)
			}
		}
	}
	for _, c := range e.creds {
		if !c.done {
			consider(c.due)
		}
	}
	for _, r := range e.releases {
		consider(r.at)
	}
	return next
}

// renewLogin extends g's login where its session can, and says whether the
// tick is done with it: it was extended, or could not be asked and is asked
// again. False is the caller's to log in anew: a session that does not renew,
// one at its maximum, one that has ended, or a refusal, which only a fresh
// login, with a fresh token, can tell from a grant that is gone.
func (e *Engine) renewLogin(ctx context.Context, g *grantState) bool {
	sent := e.cfg.Clock.Boot()
	e.mu.Lock()
	renewer, ok := g.session.(LoginRenewer)
	left := g.sentBoot + g.life - sent
	end := min(g.sentBoot+g.life, g.madeBoot+e.loginCap())
	capped := sent >= g.madeBoot+e.loginCap()
	e.mu.Unlock()
	if !ok || left <= 0 || capped {
		return false
	}
	life, err := within(ctx, e, g.grant.Provider.Name(), renewer.RenewLogin, nil)
	e.mu.Lock()
	defer e.mu.Unlock()
	switch {
	case ctx.Err() != nil || e.closed:
		return true
	case errors.Is(err, ErrReissue):
		return false
	case err != nil && ClassOf(err) == Unavailable && !panicked(err):
		// Never past the login's own end: there it is logged in anew.
		g.failures++
		g.refusal = RefusalConfirm{}
		g.due = min(e.cfg.Clock.Boot()+RetryDelay(g.failures, e.cfg.RetryMin, e.cfg.Unit()), end)
		e.cfg.Hooks.Logger.Warn("a provider login could not be renewed; retrying", "grant", g.name, "provider", g.grant.Provider.Name(), "error", describe(err))
		return true
	case err != nil:
		e.cfg.Hooks.Logger.Warn("a provider login was not renewed; logging in again", "grant", g.name, "provider", g.grant.Provider.Name(), "error", describe(err))
		return false
	case life < MinBundleLifetime || life <= left:
		return false
	}
	g.sentBoot, g.life = sent, boundLifetime(life)
	g.failures, g.refusal = 0, RefusalConfirm{}
	g.due = e.loginDue(g)
	return true
}

// panicked reports a call that panicked, which asking again would not change.
func panicked(err error) bool {
	var pe *Error
	return errors.As(err, &pe) && pe.Code == "Panic"
}

func (e *Engine) relogin(ctx context.Context, g *grantState) {
	if e.renewLogin(ctx, g) {
		return
	}
	sent := e.cfg.Clock.Boot()
	session, err := e.login(ctx, g)
	e.mu.Lock()
	if err == nil && (e.closed || ctx.Err() != nil) {
		// Close or the loop's end came first: nobody would ever close this
		// login, so it is closed now.
		e.mu.Unlock()
		e.closeSession(g.grant.Provider.Name(), session)
		return
	}
	if ctx.Err() != nil {
		e.mu.Unlock()
		return
	}
	defer e.mu.Unlock()
	if err == nil {
		if g.session != nil {
			g.old = append(g.old, oldSession{session: g.session, gen: g.gen, ends: g.ends, forever: g.forever, readSecret: g.readSecret})
		}
		g.ends, g.forever, g.readSecret = 0, false, false
		g.session, g.sentBoot, g.madeBoot, g.life = session, sent, sent, boundLifetime(safeLifetime(session))
		g.gen++
		g.failures, g.refusal = 0, RefusalConfirm{}
		g.due = e.loginDue(g)
		// Every credential of the grant is issued again from the new login
		// before the old one is closed, since a Vault lease dies with the
		// token that made it.
		for _, c := range e.creds {
			if c.spec.Grant == g.name && !c.done {
				c.due = sent
			}
		}
		return
	}
	if refused, ok := tokenRefusal(err); ok {
		g.broken = true
		e.failGrantLocked(g, tokenOutcome(refused, e.cfg.Attempt.Instance), "the identity token was refused")
		return
	}
	if ClassOf(err) == Unavailable {
		g.refusal = RefusalConfirm{} // any other answer starts a refusal again
		g.failures++
		g.due = e.cfg.Clock.Boot() + RetryDelay(g.failures, e.cfg.RetryMin, e.cfg.Unit())
		e.cfg.Hooks.Logger.Warn("provider login failed; retrying", "grant", g.name, "provider", g.grant.Provider.Name(), "error", describe(err))
		return
	}
	if ClassOf(err) == Refused && !g.refusal.Observe(err, time.Time{}.Add(e.cfg.Clock.Boot())) {
		g.due = e.cfg.Clock.Boot() + g.refusal.NextTry()
		return
	}
	g.broken = true
	outcome := Classify(ClassOf(err), AtRefresh, CallLogin)
	if ClassOf(err) == Refused && e.cfg.Attempt.Instance {
		outcome = instanceOutcome(outcome)
	}
	e.failGrantLocked(g, outcome, describe(err))
}

func tokenOutcome(refused *TokenRefused, instance bool) Outcome {
	o := Outcome{Reason: refused.Reason, Category: task.CategoryContractViolation}
	if instance {
		o = instanceOutcome(o)
	}
	return o
}

// failGrantLocked is a refusal that ends every credential of g: each is asked
// no more, and the first required one stops the container now.
func (e *Engine) failGrantLocked(g *grantState, outcome Outcome, detail string) {
	for _, c := range e.creds {
		if c.spec.Grant != g.name || c.done {
			continue
		}
		e.refusedLocked(c, outcome, fmt.Sprintf("credential %s, provider %s: %s", c.spec.Name, g.grant.Provider.Name(), detail))
	}
}

func (e *Engine) refresh(ctx context.Context, c *credState) {
	g := e.grants[c.spec.Grant]
	provider := g.grant.Provider.Name()
	what := fmt.Sprintf("credential %s, provider %s", c.spec.Name, provider)

	e.mu.Lock()
	held, heldAt := c.held, c.heldAt
	e.mu.Unlock()
	if held != nil {
		now := e.cfg.Clock.Boot()
		left := held.sentBoot + held.bundle.Lifetime - now
		if now-heldAt <= HoldLimit && (held.bundle.Lifetime <= 0 || left >= held.bundle.Lifetime/3) {
			e.place(c, held, what)
			return
		}
		// Dropped: never put in place. Its files may still have been read
		// from their version directory while it was held, so its values stay
		// masked until it expires plus the grace, as a replaced bundle's do.
		e.mu.Lock()
		if held.bundle.Lifetime > 0 {
			e.releases = append(e.releases, release{holder: held.holder, at: held.sentBoot + held.bundle.Lifetime + e.cfg.MaskGrace})
		}
		c.held = nil
		e.mu.Unlock()
	}

	e.mu.Lock()
	session, gen, placed := g.session, g.gen, c.placed
	closed := e.closed
	now := e.cfg.Clock.Boot()
	over := now >= g.madeBoot+e.loginCap() || (g.life > 0 && now >= g.sentBoot+g.life)
	e.mu.Unlock()
	if closed || session == nil {
		return
	}
	if over {
		// Its login is at its end and could not be replaced yet: nothing is
		// asked through it, so nothing outlives it that the new one's answer
		// would not have let live.
		e.refreshFailed(c, &Error{Class: Unavailable, Op: "login", Code: "LoginEnded"}, what)
		return
	}
	sentBoot, sentWall := e.cfg.Clock.Boot(), e.cfg.Clock.Wall()
	madeBoot := sentBoot
	var b Bundle
	var err error
	renewable := placed != nil && placed.gen == gen
	if renewable && e.cfg.Attempt.Instance && e.cfg.InstanceMaxTTL > 0 && sentBoot-placed.madeBoot >= e.cfg.InstanceMaxTTL {
		renewable = false
	}
	if renewable {
		b, err = e.renew(ctx, provider, session, placed.bundle)
		if err == nil {
			madeBoot = placed.madeBoot
		}
	}
	if !renewable || errors.Is(err, ErrReissue) {
		b, err = e.credential(ctx, provider, session, c.spec.Ref)
		madeBoot = sentBoot
	}
	if ctx.Err() != nil {
		return
	}
	if err != nil {
		e.refreshFailed(c, err, what)
		return
	}
	if verr := ValidBundle(b); verr != nil {
		e.mu.Lock()
		e.unusableLocked(c, "the provider's answer cannot be put in place: "+verr.Error())
		e.count(provider, "unusable")
		e.mu.Unlock()
		return
	}
	bs := &bundleState{bundle: b, gen: gen, sentBoot: sentBoot, sentWall: sentWall, madeBoot: madeBoot}
	if merr := e.hold(bs, c.spec.Name); merr != nil {
		e.mu.Lock()
		e.unusableLocked(c, maskDetail(merr))
		e.count(provider, "mask_refused")
		e.mu.Unlock()
		return
	}
	e.place(c, bs, what)
}

// place writes a held bundle and makes it current. A local write fault keeps
// the bundle, with its holder, for the next try; it is never asked of the
// provider again while held.
func (e *Engine) place(c *credState, bs *bundleState, what string) {
	provider := e.grants[c.spec.Grant].grant.Provider.Name()
	err := c.dir.Put(bs.bundle, expiresWall(bs))
	e.mu.Lock()
	defer e.mu.Unlock()
	if err != nil && !isCleanup(err) {
		if c.held == nil {
			c.held, c.heldAt = bs, e.cfg.Clock.Boot()
		}
		c.retries++
		c.cause = "the credential's files could not be written"
		c.due = e.cfg.Clock.Boot() + RetryDelay(c.retries, e.cfg.RetryMin, e.cfg.Unit())
		e.cfg.Hooks.Logger.Warn("credential write failed; retrying with the bundle held", "credential", c.spec.Name, "error", err.Error())
		e.count(provider, "write_failed")
		return
	}
	if err != nil {
		e.cfg.Hooks.Logger.Warn("credential directory cleanup failed", "credential", c.spec.Name, "error", err.Error())
	}
	// A replaced bundle's values stay masked until it expires plus the
	// grace; one that never expires stays masked for the attempt, since a
	// process may hold and print it as long as it runs.
	if previous := c.placed; previous != nil && previous.bundle.Lifetime > 0 {
		at := previous.sentBoot + previous.bundle.Lifetime + e.cfg.MaskGrace
		e.releases = append(e.releases, release{holder: previous.holder, at: at})
	}
	c.placed, c.held = bs, nil
	e.issuedLocked(e.grants[c.spec.Grant], bs)
	c.retries, c.refusal, c.cause = 0, RefusalConfirm{}, ""
	c.due = e.bundleDue(bs)
	c.expiredLogged = false
	if c.retrying {
		c.retrying = false
		e.taskLine(fmt.Sprintf("[ork] %s: refreshed again", what))
	}
	e.count(provider, "ok")
}

func (e *Engine) refreshFailed(c *credState, err error, what string) {
	provider := e.grants[c.spec.Grant].grant.Provider.Name()
	e.mu.Lock()
	defer e.mu.Unlock()
	if refused, ok := tokenRefusal(err); ok {
		e.refusedLocked(c, tokenOutcome(refused, e.cfg.Attempt.Instance), what+": the identity token was refused")
		e.count(provider, "refused")
		return
	}
	class := ClassOf(err)
	if class == Unavailable {
		c.refusal = RefusalConfirm{} // any other answer starts a refusal again
		c.retries++
		c.cause = "the provider did not answer: " + describe(err)
		c.due = e.cfg.Clock.Boot() + RetryDelay(c.retries, e.cfg.RetryMin, e.cfg.Unit())
		if !c.retrying {
			c.retrying = true
			e.taskLine(fmt.Sprintf("[ork] %s: refresh failed (%s); retrying until it expires", what, describe(err)))
		}
		e.count(provider, "unavailable")
		return
	}
	if class == Refused && !c.refusal.Observe(err, time.Time{}.Add(e.cfg.Clock.Boot())) {
		c.due = e.cfg.Clock.Boot() + c.refusal.NextTry()
		return
	}
	outcome := Classify(class, AtRefresh, CallCredential)
	if class == Refused && e.cfg.Attempt.Instance {
		outcome = instanceOutcome(outcome)
	}
	e.refusedLocked(c, outcome, what+": "+describe(err))
	e.count(provider, "refused")
}

// refusedLocked is the "Refused" row: the loop stops asking; a required
// credential stops the container now with the refusal's own reason, a
// best_effort one writes a line.
func (e *Engine) refusedLocked(c *credState, outcome Outcome, detail string) {
	c.done = true
	c.stop = &outcome
	c.cause = detail
	if c.spec.Policy == RefreshRequired {
		e.stopLocked(Failure{Outcome: outcome, Detail: detail})
		return
	}
	e.taskLine(fmt.Sprintf("[ork] %s; it is refreshed no more", detail))
}

// unusableLocked is the "Unusable" row, and a masker that will not take the
// new values: what came back cannot be put in place and would not be a
// minute later, so the loop asks for this credential no more. The task keeps
// what it holds; the watchdog decides at expiry.
func (e *Engine) unusableLocked(c *credState, cause string) {
	c.done = true
	c.cause = cause
	e.taskLine(fmt.Sprintf("[ork] credential %s: %s; it is refreshed no more", c.spec.Name, cause))
}

func (e *Engine) stopLocked(f Failure) {
	if e.stopped {
		return
	}
	e.stopped = true
	if e.cfg.Hooks.Stop != nil {
		go e.cfg.Hooks.Stop(f)
	}
}

// taskLine queues one line for the task's log. mu is held.
func (e *Engine) taskLine(line string) { e.lines = append(e.lines, line) }

// taskLineOnce is taskLine for what every refresh would say again.
func (e *Engine) taskLineOnce(line string) {
	if e.said[line] {
		return
	}
	if e.said == nil {
		e.said = map[string]bool{}
	}
	e.said[line] = true
	e.taskLine(line)
}

// count queues one refresh outcome for the metric. mu is held.
func (e *Engine) count(provider, outcome string) {
	e.counts = append(e.counts, [2]string{provider, outcome})
}

// flush emits what was queued under mu. mu is not held.
func (e *Engine) flush() {
	e.mu.Lock()
	lines, counts := e.lines, e.counts
	e.lines, e.counts = nil, nil
	e.mu.Unlock()
	if e.cfg.Hooks.TaskLine != nil {
		for _, line := range lines {
			e.cfg.Hooks.TaskLine(line)
		}
	}
	if e.cfg.Hooks.Refreshed != nil {
		for _, c := range counts {
			e.cfg.Hooks.Refreshed(c[0], c[1])
		}
	}
}

// closeRetired closes the logins a grant replaced once every credential of
// the grant is in place from a newer one, or is asked no more.
// issuedLocked notes a bundle put in place, against the login it is of.
func (e *Engine) issuedLocked(g *grantState, bs *bundleState) {
	ends, forever := bs.sentBoot+bs.bundle.Lifetime, bs.bundle.Lifetime <= 0
	if bs.gen == g.gen {
		g.ends, g.forever = max(g.ends, ends), g.forever || forever
		return
	}
	for i := range g.old {
		if g.old[i].gen == bs.gen {
			g.old[i].ends, g.old[i].forever = max(g.old[i].ends, ends), g.old[i].forever || forever
		}
	}
}

func (e *Engine) closeRetired() {
	now := e.cfg.Clock.Boot()
	e.mu.Lock()
	var retire []struct {
		provider string
		session  Session
	}
	for _, g := range e.grants {
		kept := g.old[:0]
		over := len(g.old) - maxOldLogins
		for _, old := range g.old {
			// A login stays open while a bundle it issued is on disk and
			// unexpired: a Vault lease dies with the token that made it, and
			// a credential refreshed no more is still the task's to read.
			// What it issued is never revoked early from a task either: a
			// process may still hold a file it read before the swap. An
			// instance's is, once the new one works.
			free := e.cfg.Attempt.Instance || (!old.readSecret && ((!old.forever && now >= old.ends) || over > 0))
			for _, c := range e.creds {
				if c.spec.Grant != g.name {
					continue
				}
				if (c.placed != nil && c.placed.gen <= old.gen && !e.expired(c.placed)) || (c.held != nil && c.held.gen <= old.gen) {
					free = false
				}
			}
			if free {
				over--
				retire = append(retire, struct {
					provider string
					session  Session
				}{g.grant.Provider.Name(), old.session})
			} else {
				kept = append(kept, old)
			}
		}
		g.old = kept
	}
	e.mu.Unlock()
	for _, r := range retire {
		e.closeSession(r.provider, r.session)
	}
}

func (e *Engine) closeSession(provider string, s Session) {
	ctx, cancel := context.WithTimeout(context.Background(), CloseTimeout)
	defer cancel()
	err := func() (err error) {
		// A provider that panics in Close fails its close; it never takes
		// the Orker or the refresh loop down.
		defer func() {
			if p := recover(); p != nil {
				err = &Error{Class: Unavailable, Op: "close", Code: "Panic"}
			}
		}()
		return s.Close(ctx)
	}()
	if err != nil {
		e.cfg.Hooks.Logger.Warn("provider session close failed", "provider", provider, "error", describe(err))
	}
	if e.cfg.Hooks.Closed != nil {
		e.cfg.Hooks.Closed(provider, err)
	}
}

// Check is the watchdog's one look: it compares the expiry of what is on
// disk with both clocks. A required credential that expired stops the
// container, with the refusal's own reason when a refusal is why and
// CREDENTIAL_REFRESH_FAILED otherwise, naming the cause; a best_effort one
// writes one line. A bundle never put in place is never looked at.
func (e *Engine) Check() {
	e.check()
	e.flush()
}

func (e *Engine) check() {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.stopped {
		return
	}
	for _, c := range e.creds {
		if !e.expired(c.placed) {
			continue
		}
		g := e.grants[c.spec.Grant]
		what := fmt.Sprintf("credential %s, provider %s", c.spec.Name, g.grant.Provider.Name())
		cause := c.cause
		if cause == "" {
			cause = "it was not refreshed in time"
		}
		if c.spec.Policy == RefreshRequired {
			outcome := OutcomeRefreshFailed
			if c.stop != nil {
				outcome = *c.stop
			}
			e.stopLocked(Failure{Outcome: outcome, Detail: what + " expired: " + cause})
			return
		}
		if !c.expiredLogged {
			c.expiredLogged = true
			e.taskLine(fmt.Sprintf("[ork] %s expired (%s); its files stay, and it is refreshed again when it can be", what, cause))
		}
	}
}

// Run is the refresh loop: Tick, then wait until something is due, until ctx
// is done. The watchdog runs beside it on its own ticker, so a call that
// hangs in Tick cannot delay a stop. after is the loop's sleep; nil uses a
// real timer.
func (e *Engine) Run(ctx context.Context) {
	watchdog := time.NewTicker(WatchdogInterval)
	defer watchdog.Stop()
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case <-watchdog.C:
				e.Check()
			}
		}
	}()
	for {
		next := e.Tick(ctx)
		if next < 0 {
			<-ctx.Done()
			return
		}
		if next < time.Second {
			next = time.Second
		}
		if e.cfg.Sleep(ctx, next) != nil {
			return
		}
	}
}

// State is one credential's state for the attempt's heartbeat: never a
// value, a path or a token.
type State struct {
	Name        string
	LastRefresh time.Time
	Expires     time.Time
	// Condition is "fresh", "retrying" or "expired".
	Condition string
}

// States are the attempt's credentials, in declaration order.
func (e *Engine) States() []State {
	e.mu.Lock()
	defer e.mu.Unlock()
	out := make([]State, 0, len(e.creds))
	for _, c := range e.creds {
		s := State{Name: c.spec.Name, Condition: "fresh"}
		if c.placed != nil {
			s.LastRefresh = c.placed.sentWall
			s.Expires = expiresWall(c.placed)
		}
		switch {
		case e.expired(c.placed):
			s.Condition = "expired"
		case c.retrying || c.held != nil:
			s.Condition = "retrying"
		}
		out = append(out, s)
	}
	return out
}

// Close ends every login, the replaced ones included, each on a context
// detached from the attempt's cancellation and bounded at CloseTimeout. A
// close that fails is logged and counted, never ignored. The credentials'
// directories go with the attempt's own.
func (e *Engine) Close() {
	e.mu.Lock()
	if e.closed {
		e.mu.Unlock()
		return
	}
	e.closed = true
	e.stopped = true
	type open struct {
		provider string
		session  Session
	}
	var sessions []open
	for _, name := range e.grantNames() {
		g := e.grants[name]
		for _, old := range g.old {
			sessions = append(sessions, open{g.grant.Provider.Name(), old.session})
		}
		if g.session != nil {
			sessions = append(sessions, open{g.grant.Provider.Name(), g.session})
		}
		g.old, g.session = nil, nil
	}
	e.mu.Unlock()
	var wg sync.WaitGroup
	for _, s := range sessions {
		wg.Add(1)
		go func() {
			defer wg.Done()
			e.closeSession(s.provider, s.session)
		}()
	}
	wg.Wait()
}

func sleep(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		return ctx.Err()
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}
