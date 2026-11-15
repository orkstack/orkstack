package api

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/engine"
)

// FleetCheckInterval is how often an enforcing engine looks for an engine
// that does not enforce.
const FleetCheckInterval = time.Minute

// FleetReport is the fleet check of an enforcing engine: each minute it
// reads the engines' presence channel, where every engine of this release
// says its mode whatever it serves, and the engine's workflow channel, where
// an engine of an earlier release still shows whether it advertises
// authn-enforce. It logs at error, counts and shows on the Access page every
// engine that does not enforce, and logs at error an authenticating peer
// whose auth.admins or auth.orkers differs from this engine's. It refuses
// nothing: a poller stays listed for minutes after its process is gone, so a
// refusal would turn every deploy into an outage (docs/AUTHENTICATION.md,
// "Every engine enforces").
type FleetReport struct {
	// Presence lists the engines' presence channel.
	Presence engine.EnginePresenceReader
	// Checker lists the workflow channel's pollers that lack authn-enforce.
	Checker engine.CapabilityChecker
	// Self is this engine's own description, whose digests a peer's are
	// compared with.
	Self   engine.EngineDescription
	Logger *slog.Logger
	Now    func() time.Time
	// Unenforced is told how many engines do not enforce, at each check.
	Unenforced func(ctx context.Context, n int)

	mu        sync.Mutex
	engines   []*orkstackv1.EngineSeen
	checkedAt time.Time // the last check that read the cluster; zero before one
}

// Check runs one fleet check. A check that cannot read either list keeps the
// last report: an outage never reads as a fleet that enforces.
func (f *FleetReport) Check(ctx context.Context) {
	if f == nil || (f.Checker == nil && f.Presence == nil) {
		return
	}
	now := time.Now
	if f.Now != nil {
		now = f.Now
	}
	logger := f.Logger
	if logger == nil {
		logger = slog.Default()
	}
	var presence []engine.EnginePresence
	if f.Presence != nil {
		var err error
		if presence, err = f.Presence.ObserveEngines(ctx); err != nil {
			logger.Warn("fleet check: the engines could not be listed", "channel", "presence", "error", err)
			return
		}
	}
	var missing []string
	if f.Checker != nil {
		var err error
		if missing, err = f.Checker.MissingCapability(ctx, engine.CapabilityAuthnEnforce, engine.ScopeWorkflow); err != nil {
			logger.Warn("fleet check: the engines could not be listed", "channel", "workflow", "error", err)
			return
		}
	}
	checked := now()
	at := timestamppb.New(checked)
	var seen []*orkstackv1.EngineSeen
	var ids []string
	unenforcedListed := map[string]bool{}
	var adminsDiffer, orkersDiffer []string
	for _, e := range presence {
		d := e.Description
		row := &orkstackv1.EngineSeen{Id: e.Identity, BuildId: d.BuildID, Mode: d.Mode, LastSeen: at}
		if !e.LastSeen.IsZero() {
			row.LastSeen = timestamppb.New(e.LastSeen)
		}
		if row.Mode == "" {
			row.Mode = "not stated"
		}
		if d.Mode == "observe" || d.Mode == "enforce" { // an asserted engine's lists decide nothing
			if differs(f.Self.AdminsDigest, d.AdminsDigest) {
				adminsDiffer = append(adminsDiffer, e.Identity)
				row.SettingsDiffer = append(row.SettingsDiffer, "auth.admins")
			}
			if differs(f.Self.OrkersDigest, d.OrkersDigest) {
				orkersDiffer = append(orkersDiffer, e.Identity)
				row.SettingsDiffer = append(row.SettingsDiffer, "auth.orkers")
			}
		}
		if d.Mode != "enforce" {
			unenforcedListed[e.Identity] = true
			ids = append(ids, e.Identity)
		}
		if d.Mode != "enforce" || len(row.SettingsDiffer) > 0 {
			seen = append(seen, row)
		}
	}
	for _, identity := range missing {
		if identity != "" && unenforcedListed[identity] {
			continue // the presence channel already listed it as not enforcing
		}
		if identity == "" {
			identity = "(an engine that sent no identity)"
		}
		// Here too when the presence channel said enforce: the two disagree,
		// and the workflow channel's poller is the one deciding runs.
		seen = append(seen, &orkstackv1.EngineSeen{Id: identity, Mode: "not enforce", LastSeen: at})
		ids = append(ids, identity)
	}
	if len(ids) > 0 {
		logger.Error("fleet check: engines of this deployment do not enforce authentication; set auth.mode: enforce on every replica",
			"engines", ids)
	}
	if len(adminsDiffer) > 0 {
		logger.Error("fleet check: engines of this deployment hold another auth.admins; set the same list on every replica",
			"engines", adminsDiffer)
	}
	if len(orkersDiffer) > 0 {
		logger.Error("fleet check: engines of this deployment hold another auth.orkers; set the same list on every replica",
			"engines", orkersDiffer)
	}
	if f.Unenforced != nil {
		f.Unenforced(ctx, len(ids))
	}
	f.mu.Lock()
	f.engines, f.checkedAt = seen, checked
	f.mu.Unlock()
}

// CheckedAt is when the last check that read the cluster ran; Engines is
// what it saw. Zero before one has: the Access page then shows the report
// has not been made, never an empty one.
func (f *FleetReport) CheckedAt() time.Time {
	if f == nil {
		return time.Time{}
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.checkedAt
}

// differs reports two digests that are both said and are not equal.
func differs(own, peer string) bool {
	return own != "" && peer != "" && own != peer
}

// Run checks every FleetCheckInterval until ctx ends.
func (f *FleetReport) Run(ctx context.Context) {
	t := time.NewTicker(FleetCheckInterval)
	defer t.Stop()
	for {
		f.Check(ctx)
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
	}
}

// Engines are the engines the last check saw that do not enforce or hold
// another auth.admins or auth.orkers (EngineSeen.settings_differ).
func (f *FleetReport) Engines() []*orkstackv1.EngineSeen {
	if f == nil {
		return nil
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]*orkstackv1.EngineSeen(nil), f.engines...)
}
