package orker

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"slices"
	"sync"
	"time"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/task"
)

// IdentitySupportInterval is how often an Orker checks, with the engine,
// that it may serve task identity (docs/TASK_IDENTITY.md, "Which Orkers
// serve it"); identitySupportAsk bounds one question, and an answer older
// than identitySupportStale grants nothing, so a check that stops
// answering ends support.
const (
	IdentitySupportInterval = 5 * time.Minute
	// identitySupportUnfitInterval is how often an Orker unfit for its
	// clock asks, so that it advertises again soon after its clock agrees.
	identitySupportUnfitInterval = 2 * time.Minute
	// identityClockSkewsUnfit is how many skewed mints in a row make an
	// Orker unfit for its clock.
	identityClockSkewsUnfit = 3
	// identityDateResolution is the Date header's: whole seconds.
	identityDateResolution = time.Second
	identitySupportAsk     = 30 * time.Second
	identitySupportStale   = 2 * IdentitySupportInterval
)

// identityPermissions are the permissions an Orker's own calls for an
// attempt that holds an identity need at its pool: the mint, and the
// recording of the result (workload-identity §3.4).
var identityPermissions = []string{"identity.token.mint", "task.result.commit"}

// judgeIdentitySupport says whether an Orker supports task identity, and if
// not why (providers.md §10): it is something checked, never configured.
// The engine's last answer must name this Orker's own id and grant it, at
// its pool (or more widely), every permission an Orker's calls need, and its
// storage credential must be confirmed scoped. The clock check is the
// caller's: an Orker unfit for its clock serves none (IdentitySupport).
func judgeIdentitySupport(who *orkstackv1.GetSigninResponse, orkerID, pool string, scope orkstackv1.StoreScope) (bool, string) {
	if pool == "" {
		pool = task.DefaultPool
	}
	switch {
	case who == nil:
		return false, "the engine has not answered who this Orker is"
	case who.GetOrkerId() == "":
		return false, "this Orker's credential names no Orker"
	case who.GetOrkerId() != orkerID:
		return false, fmt.Sprintf("this Orker's credential names Orker %q, not %q", who.GetOrkerId(), orkerID)
	}
	for _, perm := range identityPermissions {
		if !holdsAt(who, perm, pool) {
			return false, fmt.Sprintf("this Orker's credential does not hold %s at pool %q", perm, pool)
		}
	}
	if scope != orkstackv1.StoreScope_STORE_SCOPE_SCOPED {
		return false, "this Orker's storage credential is not confirmed scoped"
	}
	return true, ""
}

// holdsAt reports a permission held at pool, or at the deployment.
func holdsAt(who *orkstackv1.GetSigninResponse, perm, pool string) bool {
	for _, sp := range who.GetPermissions() {
		if (sp.GetScope() == "pool:"+pool || sp.GetScope() == "deployment") && slices.Contains(sp.GetPermissions(), perm) {
			return true
		}
	}
	return false
}

// Whoamier is the engine's answer of who this Orker's credential is.
type Whoamier interface {
	Whoami(ctx context.Context) (*orkstackv1.GetSigninResponse, error)
}

// IdentitySupport is the loop that keeps an Orker's verdict, and the
// verdict DescribeOrker reports.
type IdentitySupport struct {
	engine  Whoamier
	orkerID string
	pool    string
	scope   func() orkstackv1.StoreScope
	logger  *slog.Logger
	onLost  func()
	now     func() time.Time
	// ask bounds one question of the engine (identitySupportAsk).
	ask time.Duration

	mu        sync.Mutex
	supported bool
	last      *orkstackv1.GetSigninResponse
	lastAt    time.Time
	// skews counts the mints in a row whose issued_at was beyond maxSkew of
	// this Orker's clock; at identityClockSkewsUnfit the Orker is unfit for
	// its clock until an engine answer's Date agrees with it.
	skews    int
	clockOff bool
	maxSkew  time.Duration
	// dateSaid is what the last check found of the engine\'s Date while
	// unfit for the clock, for the reason Report gives.
	dateSaid string
}

// datedWhoamier is a Whoamier that also says the time the engine answered
// at, from its answer's HTTP Date header: zero when the header was missing
// or could not be read.
type datedWhoamier interface {
	WhoamiDated(ctx context.Context) (*orkstackv1.GetSigninResponse, time.Time, error)
}

// SetMaxClockSkew is orker.identity.max_clock_skew, which the Date header's
// recheck holds the engine's clock to.
func (s *IdentitySupport) SetMaxClockSkew(d time.Duration) {
	s.mu.Lock()
	s.maxSkew = d
	s.mu.Unlock()
}

// ClockSkewed counts a mint whose issued_at was beyond the bound; the third
// in a row makes this Orker unfit for its clock.
func (s *IdentitySupport) ClockSkewed() {
	if s == nil {
		return
	}
	s.mu.Lock()
	s.skews++
	unfit := s.skews >= identityClockSkewsUnfit && !s.clockOff
	if unfit {
		s.clockOff = true
	}
	s.mu.Unlock()
	if unfit {
		s.Report()
	}
}

// ClockAgreed is a mint whose issued_at agreed with this Orker's clock: the
// count starts again.
func (s *IdentitySupport) ClockAgreed() {
	if s == nil {
		return
	}
	s.mu.Lock()
	s.skews = 0
	s.mu.Unlock()
}

// interval is how long the check waits: shorter while unfit for its clock.
func (s *IdentitySupport) interval() time.Duration {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.clockOff {
		return identitySupportUnfitInterval
	}
	return IdentitySupportInterval
}

// parseEngineDate reads an HTTP Date header; zero when it is absent or not
// HTTP time, which is no evidence about the engine's clock.
func parseEngineDate(header string) time.Time {
	if header == "" {
		return time.Time{}
	}
	t, err := http.ParseTime(header)
	if err != nil {
		return time.Time{}
	}
	return t.UTC()
}

// NewIdentitySupport builds the check for an Orker of pool, whose storage
// scope verdict scope reads; onLost is called each time support is lost.
func NewIdentitySupport(engine Whoamier, orkerID, pool string, scope func() orkstackv1.StoreScope, logger *slog.Logger, onLost func()) *IdentitySupport {
	if logger == nil {
		logger = slog.Default()
	}
	return &IdentitySupport{engine: engine, orkerID: orkerID, pool: pool, scope: scope, logger: logger, onLost: onLost, now: time.Now,
		ask: identitySupportAsk, maxSkew: DefaultIdentityMaxClockSkew}
}

// Step asks the engine once, within identitySupportAsk, and judges. An
// engine that does not answer, or refuses the credential, leaves no answer:
// support ends, since an Orker the engine cannot vouch for mints nothing.
func (s *IdentitySupport) Step(ctx context.Context) {
	sent := s.now()
	askCtx, cancel := context.WithTimeout(ctx, s.ask)
	var (
		who  *orkstackv1.GetSigninResponse
		date time.Time
		err  error
	)
	if dated, ok := s.engine.(datedWhoamier); ok {
		who, date, err = dated.WhoamiDated(askCtx)
	} else {
		who, err = s.engine.Whoami(askCtx)
	}
	cancel()
	if err != nil {
		if ctx.Err() != nil {
			return // stopping: the verdict moves nothing
		}
		s.logger.Warn("the engine did not answer this Orker's identity check", "error", err)
		who = nil
	}
	received := s.now()
	// The engine answered somewhere between sending and receiving: its Date
	// is held to the midpoint, so a slow answer adds no skew of its own.
	mid := sent.Add(received.Sub(sent) / 2)
	s.mu.Lock()
	s.last, s.lastAt = who, received
	// An Orker unfit for its clock checks it against the engine's: an
	// answer whose Date is within the bound, plus the header's resolution,
	// restores it. A missing or unreadable Date is no evidence.
	wasOff := s.clockOff
	switch {
	case !s.clockOff || who == nil:
	case date.IsZero():
		s.dateSaid = "the engine's answer carried no readable Date header"
	case mid.Sub(date).Abs() <= s.maxSkew+identityDateResolution:
		s.clockOff, s.skews, s.dateSaid = false, 0, ""
	default:
		s.dateSaid = fmt.Sprintf("the engine's Date said %s, %s from this Orker's clock", date.Format(time.RFC3339), mid.Sub(date).Abs().Round(time.Second))
	}
	off, said := s.clockOff, s.dateSaid
	s.mu.Unlock()
	switch {
	case wasOff && !off:
		s.logger.Info("this Orker's clock agrees with the engine's again", "engine_date", date)
	case off && who != nil:
		s.logger.Warn("this Orker is still unfit for its clock and serves no task identity", "why", said)
	}
	s.Report()
}

// Run checks at once and then every IdentitySupportInterval until ctx ends.
func (s *IdentitySupport) Run(ctx context.Context) {
	for {
		s.Step(ctx)
		select {
		case <-ctx.Done():
			return
		case <-time.After(s.interval()):
		}
	}
}

// Report is whether this Orker serves task identity now, and why not. The
// storage verdict is read now, so a verdict that lapses ends support at
// once rather than at the next check; an answer past identitySupportStale
// grants nothing. Losing support is logged at error, gaining it at info.
func (s *IdentitySupport) Report() (bool, string) {
	scope := s.scope()
	s.mu.Lock()
	who := s.last
	if who != nil && s.now().Sub(s.lastAt) > identitySupportStale {
		who = nil
	}
	supported, reason := judgeIdentitySupport(who, s.orkerID, s.pool, scope)
	if supported && s.clockOff {
		supported, reason = false, fmt.Sprintf("%d mints in a row were issued at a time beyond orker.identity.max_clock_skew of this Orker's clock", identityClockSkewsUnfit)
		if s.dateSaid != "" {
			reason += "; " + s.dateSaid
		}
	}
	lost, gained := s.supported && !supported, !s.supported && supported
	s.supported = supported
	s.mu.Unlock()
	// Logged and told outside the lock, so an onLost may ask Report.
	switch {
	case lost:
		s.logger.Error("this Orker no longer serves task identity", "reason", reason)
		if s.onLost != nil {
			s.onLost()
		}
	case gained:
		s.logger.Info("this Orker serves task identity")
	}
	return supported, reason
}
