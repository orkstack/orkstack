package orker

import (
	"context"
	"fmt"
	"log/slog"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/identity"
	"github.com/orkstack/orkstack/internal/task"
)

// ReasonIdentityClockSkew is a mint whose issued_at is further from this
// Orker's wall clock than orker.identity.max_clock_skew: a token whose
// window the relying party would place elsewhere is not one to run on. An
// InfrastructureError, retryable under the task's own policy; the attempt
// does not ask again.
const ReasonIdentityClockSkew = "IDENTITY_CLOCK_SKEW"

// identitySkewNoted is how far apart the two clocks may be before the
// Orker logs it and counts ork.identity.clock_skew.
const identitySkewNoted = 30 * time.Second

// identityMintCallTimeout bounds one mint call, so an engine that accepts a
// request and never answers costs one bounded wait.
const identityMintCallTimeout = 30 * time.Second

// ReasonIdentityRefreshFailed is an attempt stopped because a required
// identity's token expired before a refresh replaced it, for no refusal: the
// engine could not be asked, or could not answer, for that long. Its
// container was stopped through the attempt's own cancellation. Retryable.
const ReasonIdentityRefreshFailed = "IDENTITY_REFRESH_FAILED"

// identityClock is the refresher's two clocks and its one way to wait. Boot
// keeps running while the host is suspended (CLOCK_BOOTTIME on Linux), which
// Go's monotonic clock does not.
type identityClock interface {
	Now() time.Time
	Boot() time.Duration
	Sleep(ctx context.Context, d time.Duration) error
}

// identityMasker is what the refresher needs of the attempt's log masker.
type identityMasker interface {
	Add(holder string, value []byte) (bool, error)
	Drop(holder string)
}

// heldIdentity is what is on disk for one identity.
type heldIdentity struct {
	gen      int
	holder   string
	sentBoot time.Duration
	issuedAt time.Time
	life     time.Duration
}

// identityRefresher gets an attempt's identity tokens and keeps them fresh.
type identityRefresher struct {
	files       map[string]*identityFiles
	mint        func(ctx context.Context) (*orkstackv1.MintAttemptTokensResponse, error)
	masker      identityMasker
	clock       identityClock
	draw        func() float64
	retryMin    time.Duration
	setupBudget time.Duration
	maxSkew     time.Duration
	orkerBuild  string
	// onSkew, when set, is told of a skew above identitySkewNoted that is
	// within the bound.
	onSkew func(time.Duration)
	// onClock, when set, is told of each answer's clock check: whether the
	// engine's issued_at agreed with this Orker's clock (IdentitySupport
	// counts the skewed ones).
	onClock func(agreed bool)
	// required says, by identity, whether its refresh is required (the
	// default): one that expires unrefreshed stops the attempt.
	required map[string]bool
	// maskGrace is how long a replaced value stays masked after it has
	// expired, since a process may hold and print it until then.
	maskGrace time.Duration
	// onExpired, when set, is told once of a best_effort identity whose
	// token expired unrefreshed, for the task's log.
	onExpired func(name string)
	// onRevoked, when set, is told once that a best_effort identity was
	// revoked and is no longer refreshed, for the task's log.
	onRevoked func(reason string)

	// failures counts the refreshes that failed since the last that did not.
	failures int
	// lastFailure is what the last failed refresh met, for the failure the
	// watchdog reports when nothing replaced the token.
	lastFailure string
	// logger, when set, hears every failed refresh and every cleanup that
	// went wrong after a swap.
	logger *slog.Logger
	// done is set when nothing is refreshed any more: a best_effort
	// identity was revoked.
	done bool

	// mu guards held: the watchdog reads an expiry under the lock a
	// refresh writes it under.
	mu     sync.Mutex
	held   map[string]heldIdentity
	issued map[string]*orkstackv1.AttemptIdentity
	// live is what the heartbeat reads, published after each change under
	// mu, so a beat never waits on a refresh's file I/O.
	live     atomic.Pointer[identityLiveSnapshot]
	drops    []identityDrop
	expiredN map[string]bool
}

// identityDrop is a replaced value that stays masked until at.
type identityDrop struct {
	holder string
	at     time.Time
}

// Setup gets the attempt's first tokens: asked again by the one retry rule
// while the answer says so and the setup budget lasts, never asked again for
// an answer that ends the attempt. Nil is every declared identity on disk.
func (r *identityRefresher) Setup(ctx context.Context) *Failure {
	start := r.clock.Boot()
	for n := 1; ; n++ {
		sent := r.clock.Boot()
		resp, err := r.mintWithin(ctx, r.setupBudget-(r.clock.Boot()-start))
		if ctx.Err() != nil {
			return beforeClaimStopped(context.Cause(ctx))
		}
		var again mintAnswer
		if err != nil {
			ans := mapMintAnswer(err, r.orkerBuild)
			if ans.Then != mintAskAgain {
				return Fail(ans.Reason, "%s", ans.Message)
			}
			again = ans
		} else {
			f, retry := r.apply(sent, resp)
			if !retry {
				return f
			}
			again = mintAnswer{Reason: f.Reason, Message: f.Message}
		}
		d := identityRetryDelay(n, r.retryMin, r.draw())
		if r.clock.Boot()-start+d > r.setupBudget {
			return Fail(again.Reason, "the attempt's identity tokens were not issued within its setup budget of %s: %s", r.setupBudget, again.Message)
		}
		r.failures = n
		r.logFailure("the attempt's identity tokens were not issued; asking again", again, d)
		if err := r.clock.Sleep(ctx, d); err != nil {
			return beforeClaimStopped(context.Cause(ctx))
		}
	}
}

// apply puts a mint's answer on disk: every declared identity, each masked
// before it is written. retry says the answer was not one to use and asking
// again may help; otherwise the failure, if any, ends the attempt.
func (r *identityRefresher) apply(sent time.Duration, resp *orkstackv1.MintAttemptTokensResponse) (*Failure, bool) {
	byName := make(map[string]*orkstackv1.IssuedToken, len(resp.GetTokens()))
	for _, tok := range resp.GetTokens() {
		if _, declared := r.files[tok.GetName()]; !declared {
			return Fail(identity.ReasonUnavailable, "the engine answered a token for identity %q, which the task does not declare", tok.GetName()), true
		}
		if _, twice := byName[tok.GetName()]; twice || tok.GetToken() == "" || tok.GetExpiresInSeconds() <= 0 || !tok.GetIssuedAt().IsValid() {
			return Fail(identity.ReasonUnavailable, "the engine's answer for identity %q is not one token with a life and an issue time", tok.GetName()), true
		}
		byName[tok.GetName()] = tok
	}
	for name := range r.files {
		if byName[name] == nil {
			return Fail(identity.ReasonUnavailable, "the engine's answer has no token for identity %q", name), true
		}
	}
	now := r.clock.Now()
	for name, tok := range byName {
		skew := now.Sub(tok.GetIssuedAt().AsTime()).Abs()
		if skew > r.maxSkew {
			if r.onClock != nil {
				r.onClock(false)
			}
			return Fail(ReasonIdentityClockSkew, "the engine issued identity %q at %s, %s from this Orker's clock (%s), beyond orker.identity.max_clock_skew (%s)",
				name, tok.GetIssuedAt().AsTime().UTC().Format(time.RFC3339), skew, now.UTC().Format(time.RFC3339), r.maxSkew), false
		}
		if skew > identitySkewNoted && r.onSkew != nil {
			r.onSkew(skew)
		}
	}
	if r.onClock != nil {
		r.onClock(true)
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.held == nil {
		r.held = make(map[string]heldIdentity, len(r.files))
	}
	for name, tok := range byName {
		prev := r.held[name]
		next := heldIdentity{gen: prev.gen + 1, sentBoot: sent, issuedAt: tok.GetIssuedAt().AsTime(),
			life: time.Duration(tok.GetExpiresInSeconds()) * time.Second}
		next.holder = fmt.Sprintf("identity:%s:%d", name, next.gen)
		if _, err := r.masker.Add(next.holder, []byte(tok.GetToken())); err != nil {
			return FailCause(ReasonMaskCapacityExceeded, err, "identity %q's token cannot be masked in the task's logs: %s", name, errorKind(err)), false
		}
		cleanup, err := r.files[name].Swap([]byte(tok.GetToken()), next.issuedAt.Add(next.life))
		if err != nil {
			r.masker.Drop(next.holder)
			return FailCause(ReasonWorkdirFailed, err, "identity %q's token could not be written to the attempt's identity directory: %s", name, errorKind(err)), false
		}
		if cleanup != nil && r.logger != nil {
			r.logger.Warn("an identity refresh was written, but its old files could not all be removed", "identity", name, "error", cleanup)
		}
		if prev.gen > 0 {
			r.drops = append(r.drops, identityDrop{holder: prev.holder, at: prev.issuedAt.Add(prev.life + r.maskGrace)})
		}
		r.held[name] = next
		r.noteIssued(name, tok, next.issuedAt.Add(next.life))
	}
	r.publishLive()
	return nil, false
}

// refreshStep refreshes every token once and says how long to wait before
// the next step, or why the attempt must stop. A refresh that could not ask,
// a write that failed among them, waits by the one retry rule; what is on
// disk stays until the watchdog finds it expired.
func (r *identityRefresher) refreshStep(ctx context.Context) (time.Duration, *Failure) {
	sent := r.clock.Boot()
	resp, err := r.mintWithin(ctx, identityMintCallTimeout)
	if ctx.Err() != nil {
		return 0, nil
	}
	if err != nil {
		ans := mapMintAnswer(err, r.orkerBuild)
		switch ans.Then {
		case mintEnd:
			return 0, Fail(ans.Reason, "%s", ans.Message)
		case mintStop:
			if r.anyRequired() {
				return 0, Fail(ans.Reason, "%s", ans.Message)
			}
			r.done = true
			if r.logger != nil {
				r.logger.Warn("a best_effort identity was revoked and is no longer refreshed", "reason", ans.Reason, "message", ans.Message)
			}
			if r.onRevoked != nil {
				r.onRevoked(ans.Reason)
			}
			return 0, nil
		}
		return r.retryAfter(ans), nil
	}
	f, retry := r.apply(sent, resp)
	switch {
	case f == nil:
	// A refresh that could not be used, a write that failed and a clock
	// that disagrees are failed refreshes: what is on disk stays until it
	// expires, and the watchdog decides then.
	case retry || f.Reason == ReasonWorkdirFailed || f.Reason == ReasonIdentityClockSkew:
		return r.retryAfter(mintAnswer{Reason: f.Reason, Message: f.Message}), nil
	default:
		return 0, f
	}
	r.failures = 0
	r.mu.Lock()
	r.lastFailure = ""
	r.publishLive()
	r.mu.Unlock()
	return max(r.nextDue()-r.clock.Boot(), 0), nil
}

// retryAfter counts a failed refresh, keeps what it met and logs it, and
// says how long to wait by the one retry rule.
func (r *identityRefresher) retryAfter(ans mintAnswer) time.Duration {
	r.failures++
	r.mu.Lock()
	r.lastFailure = ans.Reason + ": " + ans.Message
	r.publishLive()
	r.mu.Unlock()
	d := identityRetryDelay(r.failures, r.retryMin, r.draw())
	r.logFailure("an identity refresh failed; asking again", ans, d)
	return d
}

// logFailure logs one failed ask, at error when the engine refused this
// Orker itself (ork.identity.engine_refused), at warning otherwise.
func (r *identityRefresher) logFailure(msg string, ans mintAnswer, next time.Duration) {
	if r.logger == nil {
		return
	}
	attrs := []any{"reason", ans.Reason, "message", ans.Message, "failures", r.failures, "next", next}
	if ans.Count != "" {
		r.logger.Error(msg+": the engine refused this Orker", append(attrs, "count", ans.Count)...)
		return
	}
	r.logger.Warn(msg, attrs...)
}

// mintWithin asks once, giving up after within (at most the call timeout),
// so a hung engine holds neither a concurrency slot nor the setup budget.
func (r *identityRefresher) mintWithin(ctx context.Context, within time.Duration) (*orkstackv1.MintAttemptTokensResponse, error) {
	within = min(max(within, time.Second), identityMintCallTimeout)
	callCtx, cancel := context.WithTimeout(ctx, within)
	defer cancel()
	return r.mint(callCtx)
}

func (r *identityRefresher) anyRequired() bool {
	for name := range r.files {
		if r.isRequired(name) {
			return true
		}
	}
	return false
}

// isRequired: an identity with no entry is required, the default.
func (r *identityRefresher) isRequired(name string) bool {
	required, ok := r.required[name]
	return !ok || required
}

// nextDue is the earliest refresh point of what is on disk, on the boot
// clock.
func (r *identityRefresher) nextDue() time.Duration {
	r.mu.Lock()
	defer r.mu.Unlock()
	var due time.Duration
	first := true
	for _, h := range r.held {
		at := h.sentBoot + identityRefreshAfter(h.life, r.draw())
		if first || at < due {
			due, first = at, false
		}
	}
	return due
}

// watchdog checks what is on disk against both clocks. A required identity
// that expired unrefreshed stops the attempt; a best_effort one is told of
// once and the attempt runs on.
func (r *identityRefresher) watchdog() *Failure {
	r.mu.Lock()
	defer r.mu.Unlock()
	now, boot := r.clock.Now(), r.clock.Boot()
	for name, h := range r.held {
		if !identityExpired(boot-h.sentBoot, now, h.issuedAt, h.life) {
			continue
		}
		if r.isRequired(name) {
			why := r.lastFailure
			if why == "" {
				why = "no refresh was answered"
			}
			return Fail(ReasonIdentityRefreshFailed, "identity %q's token expired at %s and no refresh replaced it (last: %s)",
				name, h.issuedAt.Add(h.life).UTC().Format(time.RFC3339), why)
		}
		if r.expiredN == nil {
			r.expiredN = map[string]bool{}
		}
		if !r.expiredN[name] {
			r.expiredN[name] = true
			if r.onExpired != nil {
				r.onExpired(name)
			}
		}
	}
	return nil
}

// dropDue drops from the masker every replaced value that has expired
// plus the grace.
func (r *identityRefresher) dropDue() {
	r.mu.Lock()
	defer r.mu.Unlock()
	now := r.clock.Now()
	kept := r.drops[:0]
	for _, d := range r.drops {
		if now.Before(d.at) {
			kept = append(kept, d)
			continue
		}
		r.masker.Drop(d.holder)
	}
	r.drops = kept
}

// identityWatchdogTick is how often the watchdog reads both clocks. A Go
// timer stops with the host; a ticker catches up on its first tick after a
// pause.
const identityWatchdogTick = 5 * time.Second

// Run keeps the attempt's tokens fresh from the first refresh point until
// ctx ends or the attempt must stop, and returns why it must stop, or nil
// when ctx ended first. The watchdog runs beside the refresh loop on a
// ticker of its own, so a mint that hangs cannot delay it; the refresh loop
// is ended with it.
func (r *identityRefresher) Run(ctx context.Context) *Failure {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	stopped := make(chan *Failure, 2)
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		for r.clock.Sleep(ctx, identityWatchdogTick) == nil {
			r.dropDue()
			if f := r.watchdog(); f != nil {
				stopped <- f
				cancel()
				return
			}
		}
	}()
	go func() {
		defer wg.Done()
		wait := max(r.nextDue()-r.clock.Boot(), 0)
		for !r.done && r.clock.Sleep(ctx, wait) == nil {
			var f *Failure
			wait, f = r.refreshStep(ctx)
			if f != nil && ctx.Err() == nil {
				stopped <- f
				cancel()
				return
			}
		}
	}()
	wg.Wait()
	select {
	case f := <-stopped:
		return f
	default:
		return nil
	}
}

// liveIdentities is what the heartbeat says of each identity, in name
// order: when what is on disk was issued, when it expires, and whether it is
// fresh, retrying (a refresh failed since) or expired. Never a value.
func (r *identityRefresher) liveIdentities() []task.HeartbeatIdentity {
	snap := r.live.Load()
	if snap == nil {
		return nil
	}
	now, boot := r.clock.Now(), r.clock.Boot()
	names := make([]string, 0, len(snap.held))
	for name := range snap.held {
		names = append(names, name)
	}
	sort.Strings(names)
	if len(names) > task.HeartbeatIdentitiesMax {
		names = names[:task.HeartbeatIdentitiesMax]
	}
	out := make([]task.HeartbeatIdentity, 0, len(names))
	for _, name := range names {
		h := snap.held[name]
		state := task.HeartbeatIdentityFresh
		switch {
		case identityExpired(boot-h.sentBoot, now, h.issuedAt, h.life):
			state = task.HeartbeatIdentityExpired
		case snap.retrying:
			state = task.HeartbeatIdentityRetrying
		}
		out = append(out, task.HeartbeatIdentity{Name: name, Kind: "identity", State: state,
			LastRefresh: h.issuedAt.UTC().Format(time.RFC3339), ExpiresAt: h.issuedAt.Add(h.life).UTC().Format(time.RFC3339)})
	}
	return out
}

// noteIssued counts one token put on disk for the attempt's record. The
// caller holds mu.
func (r *identityRefresher) noteIssued(name string, tok *orkstackv1.IssuedToken, expires time.Time) {
	if r.issued == nil {
		r.issued = map[string]*orkstackv1.AttemptIdentity{}
	}
	rec := r.issued[name]
	if rec == nil {
		rec = &orkstackv1.AttemptIdentity{Name: name, FirstJti: tok.GetJti()}
		r.issued[name] = rec
	}
	rec.Audience = tok.GetAudience()
	rec.TokensIssued++
	rec.LastJti, rec.LastKid = tok.GetJti(), tok.GetKid()
	rec.LastExpiresAt = timestamppb.New(expires)
}

// attemptIdentities is what the attempt's record says of its identities,
// in name order: copies, so a record built while a refresh lands is whole.
func (r *identityRefresher) attemptIdentities() []*orkstackv1.AttemptIdentity {
	r.mu.Lock()
	defer r.mu.Unlock()
	names := make([]string, 0, len(r.issued))
	for name := range r.issued {
		names = append(names, name)
	}
	sort.Strings(names)
	out := make([]*orkstackv1.AttemptIdentity, 0, len(names))
	for _, name := range names {
		out = append(out, proto.Clone(r.issued[name]).(*orkstackv1.AttemptIdentity))
	}
	return out
}

// identityLiveSnapshot is the state liveIdentities reads without mu.
type identityLiveSnapshot struct {
	held     map[string]heldIdentity
	retrying bool
}

// publishLive copies what the heartbeat reads. The caller holds mu.
func (r *identityRefresher) publishLive() {
	held := make(map[string]heldIdentity, len(r.held))
	for k, v := range r.held {
		held[k] = v
	}
	r.live.Store(&identityLiveSnapshot{held: held, retrying: r.lastFailure != ""})
}
