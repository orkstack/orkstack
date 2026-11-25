package orker

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"io/fs"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/dsl"
	"github.com/orkstack/orkstack/internal/identity"
	"github.com/orkstack/orkstack/internal/provider"
)

// credentialsMountPath is where a task's credentials are in its container.
const credentialsMountPath = "/orkstack/credentials"

// SetProviders gives the dispatcher the providers this Orker declares
// (ORK_PROVIDERS), and the bounds its attempts share: at most maxConcurrent
// calls at once to one provider, each at most callTimeout.
func (d *OCIDispatcher) SetProviders(set provider.Set, maxConcurrent int, callTimeout time.Duration) {
	d.providers, d.providerCallTimeout = set, callTimeout
	d.providerLimiters = make(map[string]*provider.Limiter, set.Len())
	for _, name := range set.Names() {
		d.providerLimiters[name] = provider.NewLimiter(maxConcurrent)
	}
}

// describeProviders is what DescribeOrker says this Orker declares, in name
// order: the name and the type, nothing else of the declaration.
func (d *OCIDispatcher) describeProviders() []*orkstackv1.ProviderInfo {
	var out []*orkstackv1.ProviderInfo
	for _, name := range d.providers.Names() {
		p, _ := d.providers.ByName(name)
		out = append(out, &orkstackv1.ProviderInfo{Name: name, Type: p.Type()})
	}
	return out
}

// providerUse is what a task asks of providers, each reference accepted by
// its provider and by this Orker's floor.
type providerUse struct {
	grants  map[string]provider.Grant
	asks    map[string]*orkstackv1.IdentityGrant
	creds   []provider.CredentialSpec
	secrets []providerSecret
}

type providerSecret struct {
	name, grant string
	unmasked    bool
	ref         provider.Parsed
}

// providerUseOf judges every credential and granted secret of in: its grant
// is one the run was admitted with, of a provider this Orker declares for
// the audience it was admitted for; the provider accepts the reference; and
// this Orker's ORK_ORKER_SECRET_ALLOW admits its policy string. It reads no
// value and asks nobody. Nil is a task that uses no provider.
func (d *OCIDispatcher) providerUseOf(in *orkstackv1.ExecuteTaskInput) (*providerUse, *Failure) {
	use := &providerUse{grants: map[string]provider.Grant{}, asks: map[string]*orkstackv1.IdentityGrant{}}
	for _, g := range in.GetIdentity().GetGrants() {
		use.asks[g.GetName()] = g
	}
	for _, c := range in.GetIdentity().GetCredentials() {
		what := fmt.Sprintf("credential %q", c.GetName())
		if strings.Contains(c.GetPath(), "#") {
			return nil, Fail(provider.ReasonProviderRequestInvalid, "%s: its path may not hold a field (#)", what)
		}
		ref := provider.Ref{Kind: provider.KindCredential, Locator: c.GetPath(), Params: c.GetParams()}
		parsed, failure := d.admitReference(use, what, c.GetGrant(), "", ref)
		if failure != nil {
			return nil, failure
		}
		policy := provider.RefreshRequired
		if c.GetRefresh() == orkstackv1.RefreshPolicy_REFRESH_POLICY_BEST_EFFORT {
			policy = provider.RefreshBestEffort
		}
		use.creds = append(use.creds, provider.CredentialSpec{Name: c.GetName(), Grant: c.GetGrant(), Ref: parsed, Policy: policy})
	}
	for _, s := range in.GetSecrets() {
		if !provider.ReadByGrant(s.GetSource(), s.GetGrant()) {
			continue
		}
		what := fmt.Sprintf("secret %q", s.GetName())
		scheme, locator, _ := provider.SplitSource(s.GetSource())
		ref := provider.Ref{Kind: provider.KindSecret, Locator: locator}
		if s.GetCiphertext() != "" {
			raw, err := base64.StdEncoding.Strict().DecodeString(s.GetCiphertext())
			if err != nil {
				return nil, Fail(provider.ReasonProviderRequestInvalid, "%s: its ciphertext is not standard base64", what)
			}
			ref.Ciphertext = raw
		}
		parsed, failure := d.admitReference(use, what, s.GetGrant(), scheme, ref)
		if failure != nil {
			return nil, failure
		}
		use.secrets = append(use.secrets, providerSecret{name: s.GetName(), grant: s.GetGrant(), unmasked: s.GetUnmasked(), ref: parsed})
	}
	if len(use.creds) == 0 && len(use.secrets) == 0 {
		return nil, nil
	}
	return use, nil
}

// admitReference is one reference through grant; source is the provider a
// secret's source names, "" for a credential. A failure's message is stored
// in history, and a login's policy string holds its principal: what a check
// says goes to the cause, which never crosses, and never to the message.
func (d *OCIDispatcher) admitReference(use *providerUse, what, grant, source string, ref provider.Ref) (provider.Parsed, *Failure) {
	g, ok := use.asks[grant]
	if !ok {
		return provider.Parsed{}, Fail(identity.ReasonNotAdmitted, "%s: the run was not admitted with grant %q", what, grant)
	}
	if source != "" && source != g.GetProvider() {
		return provider.Parsed{}, Fail(identity.ReasonNotAdmitted, "%s: its source is provider %q's, and grant %q is for provider %q", what, source, grant, g.GetProvider())
	}
	p, ok := d.providers.ByName(g.GetProvider())
	if !ok {
		return provider.Parsed{}, Fail(provider.ReasonProviderNotConfigured, "%s: this Orker declares no provider %q (ORK_PROVIDERS)", what, g.GetProvider())
	}
	if p.Audience() != g.GetAudience() {
		return provider.Parsed{}, Fail(provider.ReasonProviderNotConfigured,
			"%s: this Orker declares provider %q for audience %q, and the run was admitted for %q", what, g.GetProvider(), p.Audience(), g.GetAudience())
	}
	parsed, err := p.Parse(ref)
	if err != nil {
		return provider.Parsed{}, FailCause(provider.ReasonProviderRequestInvalid, err, "%s: provider %q does not accept it", what, g.GetProvider())
	}
	policy, err := provider.Policy(p, ref, g.GetPrincipal())
	if err != nil {
		return provider.Parsed{}, FailCause(provider.ReasonProviderRequestInvalid, err, "%s: it has no policy string", what)
	}
	if len(d.Policy.SecretAllow) > 0 {
		if err := dsl.CheckSecretSource(d.Policy.SecretAllow, policy); err != nil {
			return provider.Parsed{}, FailCause(ReasonSecretNotPermitted, err,
				"%s, through grant %q at provider %q, is outside this orker's %s; it is refused before anything runs", what, grant, g.GetProvider(), SecretAllowVar)
		}
	}
	use.grants[grant] = provider.Grant{Provider: p, Principal: g.GetPrincipal()}
	return parsed, nil
}

// providerAttempt is one attempt's logins and credentials: the engine that
// keeps them, and the directory bound at /orkstack/credentials, "" for a task
// that only reads secrets.
type providerAttempt struct {
	engine *provider.Engine
	dir    string
	// remove gives back the attempt's own directory in the identity storage,
	// when it is this attempt's to give back.
	base    string
	ownBase bool

	stopped  atomic.Pointer[Failure]
	onStop   atomic.Pointer[func(*Failure)]
	toldOnce sync.Once

	cancel   context.CancelFunc
	done     chan struct{}
	stopOnce sync.Once
}

// setupProviders logs in once for each grant the task uses, writes its
// granted secrets into the input tree, and its credentials under the
// attempt's own directory: step 6a′, after the identities and inside the
// same setup budget, which ends at deadline. A failure leaves no login and
// no file.
func (d *OCIDispatcher) setupProviders(ctx context.Context, deadline time.Time, in *orkstackv1.ExecuteTaskInput, attempt int32, use *providerUse,
	ids *identityAttempt, ws *Workspace, staged *stagedInputs) (*providerAttempt, *Failure) {
	if d.identityMint == nil || d.identityConfig == nil {
		return nil, Fail(ReasonIdentityNotConfigured, "task %q logs in to a provider and this Orker reaches no engine (orker.engine.url)", in.GetTaskId())
	}
	pa := &providerAttempt{}
	fail := func(f *Failure) (*providerAttempt, *Failure) {
		if err := pa.close(); err != nil {
			d.logger().Warn("an attempt's credentials directory could not be removed after its setup failed",
				"run", in.GetRunId(), "task", in.GetTaskId(), "path", pa.dir, "error", err)
		}
		return nil, f
	}
	if len(use.creds) > 0 {
		if ids != nil {
			pa.base = ids.base
		} else {
			base, own, f := d.attemptIdentityBase(in, ws.Root)
			if f != nil {
				return nil, f
			}
			pa.base, pa.ownBase = base, own
		}
		pa.dir = filepath.Join(pa.base, "credentials")
		if err := os.Mkdir(pa.dir, 0o700); err != nil {
			return fail(FailCause(ReasonWorkdirFailed, err, "task %q: the credentials directory could not be made: %s", in.GetTaskId(), errorKind(err)))
		}
	}
	tokens := newGrantTokens(d, in, attempt, map[string]*orkstackv1.IdentityGrant{})
	grants := make(map[string]provider.Grant, len(use.grants))
	for name, g := range use.grants {
		tokens.asks[name] = use.asks[name]
		g.Token = tokens.source(name)
		grants[name] = g
	}
	logger := d.logger().With("run", in.GetRunId(), "task", in.GetTaskId())
	mask := newProviderMasker(staged.mask, use.secrets)
	engine, err := provider.NewEngine(provider.EngineConfig{
		Attempt: provider.Attempt{Namespace: in.GetNamespace(), RunID: in.GetRunId(), TaskID: in.GetTaskId(), Attempt: int(attempt)},
		Grants:  grants, Credentials: use.creds, Root: pa.dir, Masker: mask, Clock: providerClock{},
		Limiters: d.providerLimiters, RetryMin: d.identityConfig.RetryMin, CallTimeout: d.providerCallTimeout, MaskGrace: d.identityConfig.MaskGrace,
		Hooks: provider.Hooks{TaskLine: mask.note, Stop: pa.stop, Logger: logger},
	})
	if err != nil {
		logger.Error("an attempt's logins could not be prepared", "error", err)
		return fail(providerFailure(ctx, in, err))
	}
	pa.engine = engine
	budget, cancel := context.WithDeadline(ctx, deadline)
	defer cancel()
	if err := engine.Open(budget); err != nil {
		return fail(providerFailure(ctx, in, err))
	}
	for _, s := range use.secrets {
		v, err := engine.Secret(budget, s.name, s.grant, s.ref)
		if err != nil {
			return fail(providerFailure(ctx, in, err))
		}
		if f := writeSecret(ws, staged, s.name, v.Bytes); f != nil {
			return fail(f)
		}
	}
	if err := engine.Issue(budget); err != nil {
		return fail(providerFailure(ctx, in, err))
	}
	if pa.dir != "" {
		if err := os.Chmod(pa.dir, 0o555); err != nil {
			return fail(FailCause(ReasonWorkdirFailed, err, "task %q: the credentials directory could not be sealed: %s", in.GetTaskId(), errorKind(err)))
		}
	}
	tokens.running.Store(true)
	tokens.clear()
	return pa, nil
}

// providerFailure is how setup ends: stopped with the attempt, with the
// provider engine's own reason, or with a file that could not be written.
func providerFailure(ctx context.Context, in *orkstackv1.ExecuteTaskInput, err error) *Failure {
	if ctx.Err() != nil {
		return beforeClaimStopped(context.Cause(ctx))
	}
	var f *provider.Failure
	if errors.As(err, &f) {
		return Fail(f.Reason, "task %q: %s", in.GetTaskId(), f.Detail)
	}
	var path *fs.PathError
	var link *os.LinkError
	if errors.As(err, &path) || errors.As(err, &link) {
		return FailCause(ReasonWorkdirFailed, err, "task %q: a credential could not be put in place: %s", in.GetTaskId(), errorKind(err))
	}
	return FailCause(ReasonProviderSetup, err, "task %q: its logins could not be prepared; the Orker's log says why", in.GetTaskId())
}

// maxMintTokens is the most tokens one mint may ask for (identity.MaxMintTokens).
const maxMintTokens = identity.MaxMintTokens

// grantTokenAge is how long a token minted beside another grant's waits for
// its own login before a fresh one is minted instead.
const grantTokenAge = 30 * time.Second

// grantTokens mints an attempt's tokens for its logins: the engine grants an
// attempt four mints a minute, so at setup one mint answers every grant that
// has no unused token. Each token is handed out once.
type grantTokens struct {
	d       *OCIDispatcher
	in      *orkstackv1.ExecuteTaskInput
	attempt int32
	asks    map[string]*orkstackv1.IdentityGrant
	// running says the container has been set up: a clock apart from the
	// engine's ends a setup, and at a refresh is asked again, as for an
	// identity.
	running atomic.Bool

	// turn is held by the one login that reads or mints: a login whose context
	// ends stops waiting for it.
	turn   chan struct{}
	unused map[string]grantToken
}

func newGrantTokens(d *OCIDispatcher, in *orkstackv1.ExecuteTaskInput, attempt int32, asks map[string]*orkstackv1.IdentityGrant) *grantTokens {
	return &grantTokens{d: d, in: in, attempt: attempt, asks: asks, turn: make(chan struct{}, 1), unused: map[string]grantToken{}}
}

// clear forgets every token nobody used, once the attempt has logged in.
func (t *grantTokens) clear() {
	t.turn <- struct{}{}
	defer func() { <-t.turn }()
	clear(t.unused)
}

type grantToken struct {
	token string
	life  time.Duration
	at    time.Duration // boot clock, when its mint was sent
}

// source is the token source of a login through grant name.
func (t *grantTokens) source(name string) provider.TokenSource {
	return func(ctx context.Context) (string, time.Duration, error) {
		select {
		case t.turn <- struct{}{}:
			defer func() { <-t.turn }()
		case <-ctx.Done():
			return "", 0, ctx.Err()
		}
		if tok, ok := t.take(name); ok {
			return tok.token, tok.life, nil
		}
		names := t.wanted(name)
		err := t.mint(ctx, names)
		var refused *provider.TokenRefused
		if len(names) > 1 && errors.As(err, &refused) {
			// One grant's refusal is not another's: this one is asked alone.
			err = t.mint(ctx, []string{name})
		}
		if err != nil {
			return "", 0, err
		}
		tok, ok := t.take(name)
		if !ok {
			return "", 0, fmt.Errorf("the token of grant %s took too long to be minted", name)
		}
		return tok.token, tok.life, nil
	}
}

// take hands name's unused token out, when it has one young enough, with the
// life it has left.
func (t *grantTokens) take(name string) (grantToken, bool) {
	tok, ok := t.unused[name]
	delete(t.unused, name)
	age := bootClock() - tok.at
	if !ok || age > grantTokenAge || age >= tok.life {
		return grantToken{}, false
	}
	tok.life -= age
	return tok, true
}

// wanted is name and, at setup, when every grant logs in at once, the others
// with no unused token, in order, as many as one mint may ask for. A refresh's
// times are spread, and it asks for what it needs alone.
func (t *grantTokens) wanted(name string) []string {
	out := []string{name}
	if t.running.Load() {
		return out
	}
	for _, other := range slices.Sorted(maps.Keys(t.asks)) {
		if len(out) == maxMintTokens {
			break
		}
		if _, held := t.unused[other]; other != name && !held {
			out = append(out, other)
		}
	}
	return out
}

// mint asks for a token for each of names. An answer that ends an attempt is
// the engine's refusal, with its reason; any other is "could not be asked",
// which the provider engine asks again.
func (t *grantTokens) mint(ctx context.Context, names []string) error {
	req, f := identityRequest(t.in, t.attempt)
	if f != nil {
		return &provider.TokenRefused{Reason: f.Reason, Err: f}
	}
	req.Tokens = nil
	want := make(map[string]string, len(names)) // token name to grant
	for _, name := range names {
		req.Tokens = append(req.Tokens, &orkstackv1.TokenRequest{Name: "grant:" + name, Audience: t.asks[name].GetAudience()})
		want["grant:"+name] = name
	}
	sent := bootClock()
	resp, err := t.d.identityMint(ctx, req)
	if err != nil {
		if ans := mapMintAnswer(err, t.d.BuildID); ans.Then != mintAskAgain {
			return &provider.TokenRefused{Reason: ans.Reason, Err: err}
		}
		return err
	}
	got := make(map[string]grantToken, len(names))
	for _, tok := range resp.GetTokens() {
		name, ok := want[tok.GetName()]
		if _, twice := got[name]; !ok || twice || tok.GetAudience() != t.asks[name].GetAudience() || tok.GetToken() == "" ||
			tok.GetExpiresInSeconds() <= 0 || !tok.GetIssuedAt().IsValid() {
			return errors.New("the engine's answer is not one token with a life and an issue time for each login asked for")
		}
		if skew := time.Since(tok.GetIssuedAt().AsTime()).Abs(); skew > t.d.identityConfig.MaxClockSkew {
			err := fmt.Errorf("the engine issued %s %s from this Orker's clock", tok.GetName(), skew)
			if t.running.Load() {
				return err
			}
			return &provider.TokenRefused{Reason: ReasonIdentityClockSkew, Err: err}
		}
		got[name] = grantToken{token: tok.GetToken(), life: time.Duration(tok.GetExpiresInSeconds()) * time.Second, at: sent}
	}
	if len(got) != len(names) {
		return errors.New("the engine's answer has no token for a login asked for")
	}
	maps.Copy(t.unused, got)
	return nil
}

// start runs the refresh loop beside the container until close, and calls
// onStop once with why the attempt must stop, if it must.
func (pa *providerAttempt) start(ctx context.Context, onStop func(*Failure)) {
	pa.onStop.Store(&onStop)
	pa.tell()
	ctx, pa.cancel = context.WithCancel(ctx)
	pa.done = make(chan struct{})
	go func() {
		defer close(pa.done)
		pa.engine.Run(ctx)
	}()
}

// stop is the provider engine's stop of the container: a required credential
// or login that cannot be kept.
func (pa *providerAttempt) stop(f provider.Failure) {
	failure := Fail(f.Reason, "%s", f.Detail)
	if !pa.stopped.CompareAndSwap(nil, failure) {
		return
	}
	pa.tell()
}

// tell calls onStop once, when there is one and a stop for it.
func (pa *providerAttempt) tell() {
	onStop, failure := pa.onStop.Load(), pa.stopped.Load()
	if onStop != nil && failure != nil {
		pa.toldOnce.Do(func() { (*onStop)(failure) })
	}
}

// halt ends the refresh loop and waits for it, and says why it stopped the
// attempt, if it did.
func (pa *providerAttempt) halt() *Failure {
	pa.stopOnce.Do(func() {
		if pa.cancel != nil {
			pa.cancel()
			<-pa.done
		}
	})
	return pa.stopped.Load()
}

// close ends every login and removes the credentials.
func (pa *providerAttempt) close() error {
	pa.halt()
	if pa.engine != nil {
		pa.engine.Close()
	}
	target := pa.dir
	if pa.ownBase {
		target = pa.base
	}
	if target == "" {
		return nil
	}
	if err := removeSealedTree(target); err != nil && !errors.Is(err, fs.ErrNotExist) {
		return err
	}
	return nil
}

// credentialFiles is the file names of one credential, as task.json lists
// them.
func (pa *providerAttempt) credentialFiles(name string) ([]string, error) {
	entries, err := os.ReadDir(filepath.Join(pa.dir, name))
	if err != nil {
		return nil, err
	}
	var out []string
	for _, e := range entries {
		if n := e.Name(); n != "current" && !strings.HasPrefix(n, ".") {
			out = append(out, n)
		}
	}
	return out, nil
}

// credentialsOf is the task's credentials as task.json names them.
func credentialsOf(pa *providerAttempt, in *orkstackv1.ExecuteTaskInput) ([]taskJSONCredential, *Failure) {
	if pa == nil || pa.dir == "" {
		return nil, nil
	}
	grants := map[string]string{}
	for _, g := range in.GetIdentity().GetGrants() {
		grants[g.GetName()] = g.GetProvider()
	}
	var out []taskJSONCredential
	for _, c := range in.GetIdentity().GetCredentials() {
		files, err := pa.credentialFiles(c.GetName())
		if err != nil {
			return nil, FailCause(ReasonWorkdirFailed, err, "credential %q's directory could not be read: %s", c.GetName(), errorKind(err))
		}
		out = append(out, taskJSONCredential{Name: c.GetName(), Provider: grants[c.GetGrant()],
			Path: credentialsMountPath + "/" + c.GetName(), Files: files})
	}
	return out, nil
}

// providerClock is the Orker's two clocks, as the provider engine reads them.
type providerClock struct{}

func (providerClock) Boot() time.Duration { return bootClock() }
func (providerClock) Wall() time.Time     { return time.Now() }

// providerMasker is the attempt's mask as the provider engine is given it: a
// secret whose author said it is not one to hide is neither masked nor
// looked for, and what a pool does not mask is said once, not as a value
// too short to mask.
type providerMasker struct {
	mask     *attemptMask
	unmasked map[string]bool

	mu   sync.Mutex
	said map[string]bool
}

func newProviderMasker(mask *attemptMask, secrets []providerSecret) *providerMasker {
	m := &providerMasker{mask: mask, unmasked: map[string]bool{}, said: map[string]bool{}}
	for _, s := range secrets {
		if s.unmasked {
			m.unmasked["secret/"+s.name] = true
		}
	}
	return m
}

func (m *providerMasker) note(line string) {
	if m.mask != nil && m.mask.note != nil {
		m.mask.note(line)
	}
}

func (m *providerMasker) Add(holder string, value []byte) (bool, error) {
	if m.unmasked[holder] {
		return true, nil
	}
	if m.mask == nil {
		// Every attempt has a mask (executeAttempt); a path without one
		// would run the container unmasked and unchecked.
		return false, errors.New("this attempt has no masker")
	}
	// What is kept out of a result is Guard's, and narrower.
	masked, err := m.mask.maskValue(holder, value)
	if err != nil || masked || m.mask.logs {
		return masked, err
	}
	what, _, _ := strings.Cut(strings.Replace(holder, "/", " ", 1), "/")
	m.mu.Lock()
	first := !m.said[what]
	m.said[what] = true
	m.mu.Unlock()
	if first {
		m.note(fmt.Sprintf("[ork] %s is not masked in logs: orker.log.mask_secrets is off on this pool", what))
	}
	return true, nil
}

// Guard keeps a value that is certainly secret out of the task's result.
func (m *providerMasker) Guard(holder string, value []byte) {
	if !m.unmasked[holder] {
		m.mask.Guard(holder, value)
	}
}

func (m *providerMasker) Drop(holder string) { m.mask.Drop(holder) }

// ReasonProviderSetup is an attempt whose provider engine could not be made:
// a fault of this Orker.
const ReasonProviderSetup = "PROVIDER_SETUP_FAILED"

var (
	_ provider.Masker      = (*providerMasker)(nil)
	_ provider.ResultGuard = (*providerMasker)(nil)
	_ provider.Clock       = providerClock{}
)
