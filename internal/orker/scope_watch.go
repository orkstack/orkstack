package orker

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// The scope watch keeps this Orker's verdict on its own storage credential
// (docs/DEPLOY.md, "An Orker's own storage credential"): none, scoped or
// unscoped. It runs a round of the scope check at boot and every
// orker.store_scope_recheck, and a minute after an unclear one. Only a round
// that ends scoped or unscoped sets the verdict; an unclear round never moves
// it, in either direction. A scoped verdict whose last confirmation is older
// than two re-check intervals lapses to none. The verdict is reported on
// DescribeOrker and logged at error at every round while it is not scoped. It
// never cordons the Orker and never ends a task: like a cordon, it is
// process-local, never stored and gone with the process.

const (
	// StoreScopeRecheckVar is orker.store_scope_recheck.
	StoreScopeRecheckVar = "ORK_ORKER_STORE_SCOPE_RECHECK"
	// DefaultStoreScopeRecheck is its default.
	DefaultStoreScopeRecheck = time.Hour
	// MinStoreScopeRecheck and MaxStoreScopeRecheck bound it.
	MinStoreScopeRecheck = time.Second
	MaxStoreScopeRecheck = 7 * 24 * time.Hour

	// scopeRetryUnclear is how soon a round follows an unclear one.
	scopeRetryUnclear = time.Minute
	// One round is bounded by the re-check interval, held between these.
	minScopeRoundTime = 30 * time.Second
	maxScopeRoundTime = 5 * time.Minute
)

// ScopeCheck runs one round of the scope check.
type ScopeCheck func(ctx context.Context) artifacts.ScopeRound

// ScopeWatchConfig is the orker.store_scope_recheck setting.
type ScopeWatchConfig struct {
	Recheck time.Duration
}

// DefaultScopeWatchConfig is the setting an Orker that sets none runs with.
func DefaultScopeWatchConfig() ScopeWatchConfig {
	return ScopeWatchConfig{Recheck: DefaultStoreScopeRecheck}
}

// Validate refuses a re-check interval out of range, naming the variable.
func (c ScopeWatchConfig) Validate() error {
	if c.Recheck < MinStoreScopeRecheck || c.Recheck > MaxStoreScopeRecheck {
		return fmt.Errorf("%s=%s must be between %s and %s",
			StoreScopeRecheckVar, c.Recheck, MinStoreScopeRecheck, MaxStoreScopeRecheck)
	}
	return nil
}

type scopeVerdict int

const (
	verdictNone scopeVerdict = iota
	verdictScoped
	verdictUnscoped
)

// ScopeReport is what DescribeOrker says of the verdict.
type ScopeReport struct {
	Scope       orkstackv1.StoreScope
	Detail      string
	ConfirmedAt *timestamppb.Timestamp
	Results     orkstackv1.ResultsScope
}

// ScopeWatch is the loop and the verdict it keeps.
type ScopeWatch struct {
	check  ScopeCheck
	cfg    ScopeWatchConfig
	now    func() time.Time
	logger *slog.Logger

	mu          sync.Mutex
	rounds      int
	verdict     scopeVerdict
	confirmedAt time.Time
	last        artifacts.ScopeRound
	// decided is the round that set the verdict: its detail stays the
	// verdict's reason through later unclear rounds.
	decided artifacts.ScopeRound
	results artifacts.ResultsReach
}

// NewScopeWatch builds a watch over check. A nil logger logs nothing.
func NewScopeWatch(check ScopeCheck, cfg ScopeWatchConfig, logger *slog.Logger) *ScopeWatch {
	if logger == nil {
		logger = slog.New(slog.DiscardHandler)
	}
	return &ScopeWatch{check: check, cfg: cfg, now: time.Now, logger: logger}
}

// WatchStoreScope builds this Orker's scope watch over check, logging to the
// Orker's log, and makes DescribeOrker report its verdict. The caller runs it.
func (a *Activities) WatchStoreScope(check ScopeCheck, cfg ScopeWatchConfig) *ScopeWatch {
	w := NewScopeWatch(check, cfg, a.dispatcher.logger())
	a.scope.Store(w)
	return w
}

// Run runs a round at once and then on its cadence until ctx is done. A
// round cut short because the process is stopping moves nothing.
func (w *ScopeWatch) Run(ctx context.Context) {
	for {
		w.Step(ctx)
		timer := time.NewTimer(w.next())
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
		}
	}
}

// Step runs one round, bounded by the re-check interval and five minutes.
func (w *ScopeWatch) Step(ctx context.Context) {
	roundCtx, cancel := context.WithTimeout(ctx, min(max(w.cfg.Recheck, minScopeRoundTime), maxScopeRoundTime))
	defer cancel()
	round := w.runCheck(roundCtx)
	if ctx.Err() != nil {
		return
	}
	w.observe(round)
}

// runCheck runs one round; a check that panics is an unclear round, never
// the Orker's end, since the watch decides nothing.
func (w *ScopeWatch) runCheck(ctx context.Context) (round artifacts.ScopeRound) {
	defer func() {
		if r := recover(); r != nil {
			round = artifacts.ScopeRound{Outcome: artifacts.ScopeUnclear,
				Detail: artifacts.BoundText(fmt.Sprintf("the check panicked: %v", r), artifacts.MaxScopeDetail)}
		}
	}()
	return w.check(ctx)
}

// next is how long until the next round: a minute after an unclear one (or
// the re-check interval, when that is shorter), else the re-check interval.
func (w *ScopeWatch) next() time.Duration {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.last.Outcome == artifacts.ScopeUnclear {
		return min(scopeRetryUnclear, w.cfg.Recheck)
	}
	return w.cfg.Recheck
}

// observe applies one round to the verdict and logs it.
func (w *ScopeWatch) observe(round artifacts.ScopeRound) {
	w.mu.Lock()
	now := w.now()
	w.rounds++
	w.last = round
	switch round.Outcome {
	case artifacts.ScopeScoped:
		w.verdict, w.confirmedAt, w.decided = verdictScoped, now, round
	case artifacts.ScopeUnscoped:
		w.verdict, w.decided = verdictUnscoped, round
	}
	if round.Results != artifacts.ResultsUnknown {
		w.results = round.Results
	}
	report := w.reportLocked(now)
	w.mu.Unlock()

	attrs := []any{"verdict", scopeWord(report.Scope), "round", round.Outcome.String()}
	if round.Detail != "" {
		attrs = append(attrs, "detail", round.Detail)
	}
	if round.IgnoredCondition {
		attrs = append(attrs, "store_ignored_condition", true)
	}
	switch {
	case report.Scope != orkstackv1.StoreScope_STORE_SCOPE_SCOPED:
		w.logger.Error("this Orker's storage credential is not confirmed scoped to the Orker's own prefixes: "+
			"it may reach the control plane's objects (docs/DEPLOY.md, \"An Orker's own storage credential\")", attrs...)
	case round.Outcome == artifacts.ScopeUnclear:
		w.logger.Warn("the storage scope check could not tell; the verdict stands", attrs...)
	default:
		w.logger.Info("the storage scope check ended scoped", attrs...)
	}
}

// Report is the verdict as DescribeOrker states it: unspecified before any
// round, unknown while no round has ended scoped or unscoped or a scoped
// verdict has lapsed.
func (w *ScopeWatch) Report() ScopeReport {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.reportLocked(w.now())
}

func (w *ScopeWatch) reportLocked(now time.Time) ScopeReport {
	if w.rounds == 0 {
		// The watch runs but its first round has not ended: no verdict yet,
		// which an older build's unset field must not be mistaken for.
		return ScopeReport{Scope: orkstackv1.StoreScope_STORE_SCOPE_UNKNOWN, Detail: "the first round has not ended"}
	}
	// The detail is the verdict's reason, then what the last round found
	// when it could not tell.
	detail := w.decided.Detail
	if w.last.Outcome == artifacts.ScopeUnclear && w.last.Detail != "" {
		if detail != "" {
			detail += "; the last round could not tell: "
		}
		detail += w.last.Detail
	}
	r := ScopeReport{Detail: artifacts.BoundText(detail, artifacts.MaxScopeDetail), Results: resultsScope(w.results)}
	if !w.confirmedAt.IsZero() {
		r.ConfirmedAt = timestamppb.New(w.confirmedAt)
	}
	switch w.verdict {
	case verdictScoped:
		r.Scope = orkstackv1.StoreScope_STORE_SCOPE_SCOPED
		if now.Sub(w.confirmedAt) > 2*w.cfg.Recheck {
			r.Scope = orkstackv1.StoreScope_STORE_SCOPE_UNKNOWN
		}
	case verdictUnscoped:
		r.Scope = orkstackv1.StoreScope_STORE_SCOPE_UNSCOPED
	default:
		r.Scope = orkstackv1.StoreScope_STORE_SCOPE_UNKNOWN
	}
	return r
}

func resultsScope(r artifacts.ResultsReach) orkstackv1.ResultsScope {
	switch r {
	case artifacts.ResultsEveryNamespace:
		return orkstackv1.ResultsScope_RESULTS_SCOPE_EVERY_NAMESPACE
	case artifacts.ResultsByNamespace:
		return orkstackv1.ResultsScope_RESULTS_SCOPE_BY_NAMESPACE
	}
	return orkstackv1.ResultsScope_RESULTS_SCOPE_UNSPECIFIED
}

func scopeWord(s orkstackv1.StoreScope) string {
	switch s {
	case orkstackv1.StoreScope_STORE_SCOPE_SCOPED:
		return "scoped"
	case orkstackv1.StoreScope_STORE_SCOPE_UNSCOPED:
		return "unscoped"
	}
	return "none"
}
