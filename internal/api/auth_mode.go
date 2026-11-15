package api

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"sync"
	"time"

	"github.com/orkstack/orkstack/pkg/artifacts"
)

// The mode record (docs/AUTHENTICATION.md, "The mode record"): the highest
// authentication mode the deployment runs. An engine does not start in a
// lower mode than it; the mode comes down by a command, never by one
// replica's configuration.

// ModeRecordWatchInterval is how often a running engine reads the record
// again.
const ModeRecordWatchInterval = time.Minute

// claimGrace is the grace beside an idempotency key's life that every
// window over a claim includes.
const claimGrace = 5 * time.Minute

// modeSwapAttempts bounds the retries of a lost compare-and-swap.
const modeSwapAttempts = 5

// ErrModeRecordHigher is a record that names a higher mode than an engine's
// own: it refuses to start.
var ErrModeRecordHigher = errors.New("the deployment runs a higher authentication mode")

// ModeGuard is one engine's side of the mode record.
type ModeGuard struct {
	Store *artifacts.ModeStore
	// Mode is this engine's configured auth.mode.
	Mode string
	// ClaimTTL is engine.idempotency_ttl; lowering to asserted waits it and
	// claimGrace from the instant the deployment last left enforce.
	ClaimTTL time.Duration
	Now      func() time.Time
	Logger   *slog.Logger
	// Host names this engine in the record's updated_by.
	Host string
	// Missing counts a record this process saw and then found gone.
	Missing func(ctx context.Context)
	// PeerClaimTTL reads the longest engine.idempotency_ttl the engines on
	// the presence channel advertise (engine.LargestClaimTTL). Lowering to
	// asserted waits the longer of it and ClaimTTL, because a claim made by
	// any engine lives that long. Nil reads none: only ClaimTTL counts.
	PeerClaimTTL func(ctx context.Context) (time.Duration, error)

	mu      sync.Mutex
	peerTTL time.Duration // the longest PeerClaimTTL this process has read
	seen    bool
	record  artifacts.ModeRecord
	etag    string
	problem string // what the Access page's banner says, empty when nothing
	missing bool   // a record this process saw went missing
}

func (g *ModeGuard) now() time.Time {
	if g.Now != nil {
		return g.Now().UTC()
	}
	return time.Now().UTC()
}

func (g *ModeGuard) logger() *slog.Logger {
	if g.Logger != nil {
		return g.Logger
	}
	return slog.Default()
}

func (g *ModeGuard) by() string {
	host := g.Host
	if host == "" {
		host, _ = os.Hostname()
	}
	return "engine " + host
}

// raised is the record after this engine raises it to its own mode.
func (g *ModeGuard) raised(prev artifacts.ModeRecord, had bool) artifacts.ModeRecord {
	now := g.now()
	next := artifacts.ModeRecord{Mode: g.Mode, RaisedAt: now, UpdatedAt: now, UpdatedBy: g.by()}
	if had {
		next.LeftEnforceAt = prev.LeftEnforceAt
		if prev.Mode == g.Mode {
			next.RaisedAt = prev.RaisedAt
		}
	}
	if g.Mode == artifacts.AuthModeEnforce {
		// Entering enforce again starts the wait for asserted again.
		next.LeftEnforceAt = time.Time{}
	}
	return next
}

// Boot applies the record's rules at an engine's start: a record naming its
// own mode or a lower one is raised to it; a higher one refuses; none is
// written in observe or enforce and logged in asserted; one that does not
// decode refuses, naming the object. A store that cannot be read is an
// error: the engine does not serve until it can.
func (g *ModeGuard) Boot(ctx context.Context) error {
	for attempt := 0; attempt < modeSwapAttempts; attempt++ {
		rec, etag, err := g.Store.Get(ctx)
		switch {
		case errors.Is(err, artifacts.ErrNotFound):
			if g.Mode == artifacts.AuthModeAsserted {
				g.logger().Info("no mode record: this deployment has not turned authentication on", "record", g.Store.Location())
				return nil
			}
			etag, err := g.Store.Put(ctx, g.raised(artifacts.ModeRecord{}, false), artifacts.PutConditions{IfAbsent: true})
			if errors.Is(err, artifacts.ErrPreconditionFailed) {
				continue // another engine wrote it first: apply the rules to what it holds
			}
			if err != nil {
				return fmt.Errorf("write the mode record %s: %w", g.Store.Location(), err)
			}
			g.adopt(g.raised(artifacts.ModeRecord{}, false), etag)
			return nil
		case errors.Is(err, artifacts.ErrModeRecordUnreadable):
			return fmt.Errorf("the mode record %s cannot be read (%w); fix it with `ork --set-mode-record MODE --reason TEXT --force`", g.Store.Location(), err)
		case err != nil:
			return fmt.Errorf("read the mode record %s: %w", g.Store.Location(), err)
		}
		own, recorded := artifacts.AuthModeRank(g.Mode), artifacts.AuthModeRank(rec.Mode)
		if recorded > own {
			return fmt.Errorf("%w: the mode record %s names %q and this engine's auth.mode is %q; start it in %q, or lower the deployment with `orkctl admin auth lower`",
				ErrModeRecordHigher, g.Store.Location(), rec.Mode, g.Mode, rec.Mode)
		}
		if recorded == own {
			g.adopt(rec, etag)
			return nil
		}
		next := g.raised(rec, true)
		newETag, err := g.Store.Put(ctx, next, artifacts.PutConditions{IfMatch: etag})
		if errors.Is(err, artifacts.ErrPreconditionFailed) {
			continue
		}
		if err != nil {
			return fmt.Errorf("raise the mode record %s to %q: %w", g.Store.Location(), g.Mode, err)
		}
		g.logger().Info("mode record raised", "from", rec.Mode, "to", g.Mode, "record", g.Store.Location())
		g.adopt(next, newETag)
		return nil
	}
	return fmt.Errorf("the mode record %s kept changing while this engine raised it; try again", g.Store.Location())
}

func (g *ModeGuard) adopt(rec artifacts.ModeRecord, etag string) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.seen, g.record, g.etag, g.problem = true, rec, etag, ""
}

// Check reads the record again, as a running engine does each minute: a
// higher mode is logged and shown, never a stop; a record this process saw
// and that is now gone is logged, counted, shown and written again at the
// engine's own mode; one that does not decode is logged and shown.
func (g *ModeGuard) Check(ctx context.Context) {
	rec, etag, err := g.Store.Get(ctx)
	switch {
	case errors.Is(err, artifacts.ErrNotFound):
		g.mu.Lock()
		seen := g.seen
		g.mu.Unlock()
		if !seen || g.Mode == artifacts.AuthModeAsserted {
			return
		}
		g.logger().Error("the mode record this engine saw is gone; writing it again at this engine's mode",
			"record", g.Store.Location(), "mode", g.Mode)
		if g.Missing != nil {
			g.Missing(ctx)
		}
		g.setProblem("the mode record was missing; this engine wrote it again at " + g.Mode)
		g.mu.Lock()
		g.missing = true
		g.mu.Unlock()
		next := g.raised(artifacts.ModeRecord{}, false)
		etag, err := g.Store.Put(ctx, next, artifacts.PutConditions{IfAbsent: true})
		switch {
		case err == nil:
			g.mu.Lock()
			g.record, g.etag = next, etag
			g.mu.Unlock()
		case errors.Is(err, artifacts.ErrPreconditionFailed):
			// Another engine wrote it first: read what it holds next minute.
			g.logger().Info("the mode record was written again by another engine", "record", g.Store.Location())
		default:
			g.logger().Error("the mode record could not be written again; it stays missing until a write succeeds",
				"record", g.Store.Location(), "mode", g.Mode, "error", err)
			g.setProblem("the mode record is missing and could not be written again: " + err.Error())
		}
	case errors.Is(err, artifacts.ErrModeRecordUnreadable):
		g.logger().Error("the mode record cannot be read; serving on the mode this engine booted with", "record", g.Store.Location(), "error", err)
		g.setProblem("the mode record cannot be read: " + err.Error())
	case err != nil:
		// The store's own alarms report an outage.
	default:
		g.mu.Lock()
		g.seen, g.record, g.etag = true, rec, etag
		g.mu.Unlock()
		if artifacts.AuthModeRank(rec.Mode) > artifacts.AuthModeRank(g.Mode) {
			g.logger().Error("the mode record names a higher mode than this engine runs; restart it in that mode",
				"record", g.Store.Location(), "recorded", rec.Mode, "mode", g.Mode)
			g.setProblem(fmt.Sprintf("the deployment runs %s and this engine %s", rec.Mode, g.Mode))
			return
		}
		if artifacts.AuthModeRank(rec.Mode) < artifacts.AuthModeRank(g.Mode) {
			g.logger().Error("the mode record names a lower mode than this engine runs; it was lowered: restart this engine in that mode",
				"record", g.Store.Location(), "recorded", rec.Mode, "mode", g.Mode, "updated_by", rec.UpdatedBy)
			g.setProblem(fmt.Sprintf("the mode record was lowered to %s and this engine runs %s", rec.Mode, g.Mode))
			return
		}
		g.setProblem("")
	}
}

func (g *ModeGuard) setProblem(p string) {
	g.mu.Lock()
	g.problem = p
	g.mu.Unlock()
}

// Watch runs Check and RefreshPeers every ModeRecordWatchInterval until ctx
// ends.
func (g *ModeGuard) Watch(ctx context.Context) {
	t := time.NewTicker(ModeRecordWatchInterval)
	defer t.Stop()
	g.RefreshPeers(ctx)
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			g.Check(ctx)
			g.RefreshPeers(ctx)
		}
	}
}

// ModeStatus is what the Access page reads of the record.
type ModeStatus struct {
	Recorded     string
	ETag         string
	Missing      bool // a record this engine saw went missing
	Problem      string
	AssertedSafe time.Time // zero when lowering to asserted waits for nothing
}

// Status reports the record as this engine last read it.
func (g *ModeGuard) Status() ModeStatus {
	g.mu.Lock()
	defer g.mu.Unlock()
	s := ModeStatus{Recorded: g.record.Mode, ETag: g.etag, Problem: g.problem, Missing: g.missing}
	s.AssertedSafe = g.assertedSafeLocked()
	return s
}

func (g *ModeGuard) assertedSafeLocked() time.Time {
	life := g.claimLife(g.peerTTL)
	switch g.record.Mode {
	case artifacts.AuthModeEnforce:
		return g.now().Add(life)
	case "", artifacts.AuthModeAsserted:
		return time.Time{}
	}
	if g.record.LeftEnforceAt.IsZero() {
		return time.Time{}
	}
	safe := g.record.LeftEnforceAt.Add(life)
	if !safe.After(g.now()) {
		return time.Time{}
	}
	return safe
}

// longestClaim is the longer of this engine's claim life and peer, the
// longest a peer advertises.
func (g *ModeGuard) longestClaim(peer time.Duration) time.Duration {
	ttl := g.ClaimTTL
	if ttl <= 0 {
		ttl = 24 * time.Hour
	}
	return max(ttl, peer)
}

// claimLife is one claim's life and its grace.
func (g *ModeGuard) claimLife(peer time.Duration) time.Duration {
	return g.longestClaim(peer) + claimGrace
}

// RefreshPeers reads PeerClaimTTL again for the Access page's "asserted
// safe" (Status). The longest life this process has read is kept, so an
// engine that has left, whose claims outlive its last poll, still counts
// while this process runs. A read that fails is logged and changes nothing;
// Lower reads it afresh in any case.
func (g *ModeGuard) RefreshPeers(ctx context.Context) {
	if g.PeerClaimTTL == nil {
		return
	}
	ttl, err := g.PeerClaimTTL(ctx)
	if err != nil {
		g.logger().Warn("mode record: the engines could not be listed; asserted's wait keeps the claim life last read",
			"error", err)
		return
	}
	g.mu.Lock()
	g.peerTTL = max(g.peerTTL, ttl)
	g.mu.Unlock()
}

// LowerRefusal is a lowering the record's rules refuse; Code says how.
type LowerRefusal struct {
	Precondition bool // failed_precondition rather than invalid_argument
	Err          error
}

func (e *LowerRefusal) Error() string { return e.Err.Error() }
func (e *LowerRefusal) Unwrap() error { return e.Err }

// Lower rewrites the record downward, under the etag the caller read
// (LowerAuthMode). enforce to observe is never refused; to asserted is
// refused until the deployment has been out of enforce for one idempotency
// key's life, unless acceptDuplicates. It returns the mode it replaced.
func (g *ModeGuard) Lower(ctx context.Context, target, etag string, acceptDuplicates bool, by, reason string) (string, error) {
	rec, current, err := g.Store.Get(ctx)
	if errors.Is(err, artifacts.ErrNotFound) {
		return "", &LowerRefusal{Precondition: true, Err: errors.New("there is no mode record: the deployment never turned authentication on")}
	}
	if err != nil {
		return "", &LowerRefusal{Precondition: true, Err: fmt.Errorf("the mode record cannot be read, so it is not lowered: %w", err)}
	}
	if etag == "" || etag != current {
		return "", &LowerRefusal{Precondition: true, Err: errors.New("the mode record changed since it was read; read it again (record_etag)")}
	}
	if artifacts.AuthModeRank(g.Mode) < artifacts.AuthModeRank(rec.Mode) {
		return "", &LowerRefusal{Precondition: true, Err: fmt.Errorf(
			"this engine runs %q, below the record's %q: lower the deployment through an engine that runs its mode", g.Mode, rec.Mode)}
	}
	to, from := artifacts.AuthModeRank(target), artifacts.AuthModeRank(rec.Mode)
	if to == 0 || target == artifacts.AuthModeEnforce {
		return "", &LowerRefusal{Err: fmt.Errorf("mode %q is not observe or asserted", target)}
	}
	if to >= from {
		return "", &LowerRefusal{Err: fmt.Errorf("the record names %q; LowerAuthMode only lowers, and %q is not below it", rec.Mode, target)}
	}
	now := g.now()
	next := rec
	next.Mode, next.UpdatedAt, next.UpdatedBy, next.Reason = target, now, by, reason
	if rec.Mode == artifacts.AuthModeEnforce {
		next.LeftEnforceAt = now
	}
	if target == artifacts.AuthModeAsserted && !acceptDuplicates {
		if left := next.LeftEnforceAt; !left.IsZero() {
			var peer time.Duration
			if g.PeerClaimTTL != nil {
				if peer, err = g.PeerClaimTTL(ctx); err != nil {
					return "", &LowerRefusal{Precondition: true, Err: fmt.Errorf(
						"the engines could not be listed, so how long their idempotency keys live is not known and asserted is not written; try again (accept_duplicate_retries skips the wait): %w", err)}
				}
				g.mu.Lock()
				g.peerTTL = max(g.peerTTL, peer)
				peer = g.peerTTL
				g.mu.Unlock()
			}
			if safe := left.Add(g.claimLife(peer)); safe.After(now) {
				return "", &LowerRefusal{Precondition: true, Err: fmt.Errorf(
					"lower to observe first; asserted is safe from %s, one idempotency key's life (the longest engine.idempotency_ttl the engines advertise, %s, and five minutes; this engine's log names the engine that advertises it) after the deployment last left enforce (accept_duplicate_retries skips the wait)",
					safe.Format(time.RFC3339), g.longestClaim(peer))}
			}
		}
	}
	newETag, err := g.Store.Put(ctx, next, artifacts.PutConditions{IfMatch: current})
	if errors.Is(err, artifacts.ErrPreconditionFailed) {
		return "", &LowerRefusal{Precondition: true, Err: errors.New("the mode record changed while it was lowered; read it again")}
	}
	if err != nil {
		return "", fmt.Errorf("write the mode record: %w", err)
	}
	g.mu.Lock()
	g.record, g.etag = next, newETag
	g.mu.Unlock()
	return rec.Mode, nil
}
