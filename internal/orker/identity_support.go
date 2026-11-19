package orker

import (
	"context"
	"fmt"
	"log/slog"
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
	identitySupportAsk      = 30 * time.Second
	identitySupportStale    = 2 * IdentitySupportInterval
)

// identityPermissions are the permissions an Orker's own calls for an
// attempt that holds an identity need at its pool: the mint, and the
// recording of the result (workload-identity §3.4).
var identityPermissions = []string{"identity.token.mint", "task.result.commit"}

// judgeIdentitySupport says whether an Orker supports task identity, and if
// not why (providers.md §10): it is something checked, never configured.
// The engine's last answer must name this Orker's own id and grant it, at
// its pool (or more widely), every permission an Orker's calls need, and its
// storage credential must be confirmed scoped. The clock check of
// workload-identity §10 is not part of the verdict in this build.
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
}

// NewIdentitySupport builds the check for an Orker of pool, whose storage
// scope verdict scope reads; onLost is called each time support is lost.
func NewIdentitySupport(engine Whoamier, orkerID, pool string, scope func() orkstackv1.StoreScope, logger *slog.Logger, onLost func()) *IdentitySupport {
	if logger == nil {
		logger = slog.Default()
	}
	return &IdentitySupport{engine: engine, orkerID: orkerID, pool: pool, scope: scope, logger: logger, onLost: onLost, now: time.Now, ask: identitySupportAsk}
}

// Step asks the engine once, within identitySupportAsk, and judges. An
// engine that does not answer, or refuses the credential, leaves no answer:
// support ends, since an Orker the engine cannot vouch for mints nothing.
func (s *IdentitySupport) Step(ctx context.Context) {
	askCtx, cancel := context.WithTimeout(ctx, s.ask)
	who, err := s.engine.Whoami(askCtx)
	cancel()
	if err != nil {
		if ctx.Err() != nil {
			return // stopping: the verdict moves nothing
		}
		s.logger.Warn("the engine did not answer this Orker's identity check", "error", err)
		who = nil
	}
	s.mu.Lock()
	s.last, s.lastAt = who, s.now()
	s.mu.Unlock()
	s.Report()
}

// Run checks at once and then every IdentitySupportInterval until ctx ends.
func (s *IdentitySupport) Run(ctx context.Context) {
	for {
		s.Step(ctx)
		select {
		case <-ctx.Done():
			return
		case <-time.After(IdentitySupportInterval):
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
