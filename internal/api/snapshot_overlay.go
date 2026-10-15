package api

import (
	"context"
	"sync"
	"time"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/engine"
)

// Snapshot overlays (docs/DEPLOY.md §1, engine.snapshot_overlays): with the
// flag on, a run tree that could use overlays gets the policy on its
// top-level plan, which startChild copies into each child's start plan.
// Which run of the tree then writes overlays is decided in workflow code,
// from that run's own declared rows (engine.DeclaredRows).

// overlaysOffWarnEvery is how often one process logs that the fleet keeps
// new runs off overlays: at most once a minute, so a busy submit path does
// not repeat it per run.
const overlaysOffWarnEvery = time.Minute

// warnLimiter lets a Warn through at most once per interval.
type warnLimiter struct {
	mu   sync.Mutex
	last time.Time
}

// allow reports whether a Warn may be logged now, and if so starts the next
// interval.
func (l *warnLimiter) allow(now time.Time, every time.Duration) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	if !l.last.IsZero() && now.Sub(l.last) < every {
		return false
	}
	l.last = now
	return true
}

// maxTreeRows is the largest declared row count — tasks plus every fan-out's
// max_items — of any run in the planned tree: the top-level definition and
// every child planChildren planned, grandchildren included.
func maxTreeRows(planned *orkstackv1.Orkflow, plan *engine.RunPlan) (int64, error) {
	largest := engine.DeclaredRows(planned)
	err := engine.ForEachChild(planned, plan, func(_ string, _ *engine.ChildRunPlan, def *orkstackv1.Orkflow) error {
		largest = max(largest, engine.DeclaredRows(def))
		return nil
	})
	return largest, err
}

// pinSnapshotOverlay sets plan.SnapshotOverlay to the pinned default policy
// when the deployment enables overlays, some run of the tree declares more
// than engine.DefaultOverlayMinRows rows, and no engine process that could
// decide the run — nor any process polling the projection channel, whatever
// its build — lacks the capability (engine.ScopeWorkflowAndEvents): an
// older process would refuse an overlay's reference and lose that publish.
//
// It runs after planChildren, so every child's definition is known: a tree
// none of whose runs could use an overlay asks the fleet nothing, which is
// exact — no run of it would write one with or without the policy — and is
// what keeps every small submit free of the two describes. It never refuses:
// a fleet that cannot read overlays, or that cannot be read, leaves the
// policy nil, so the run writes full snapshots as it always has, and the
// engine logs why, at most once a minute.
func (s *Service) pinSnapshotOverlay(ctx context.Context, planned *orkstackv1.Orkflow, plan *engine.RunPlan) {
	if !s.snapshotOverlays {
		return
	}
	rows, err := maxTreeRows(planned, plan)
	if err != nil || rows <= engine.DefaultOverlayMinRows {
		return
	}
	if s.capabilities == nil {
		s.warnOverlaysOff("this engine is not wired to a cluster, so it cannot check the engine processes",
			"orkflow", planned.GetName(), "version", planned.GetVersion())
		return
	}
	lacking, err := s.capabilities.MissingCapability(ctx, engine.CapabilitySnapshotOverlay, engine.ScopeWorkflowAndEvents)
	if err != nil {
		s.warnOverlaysOff("the cluster's list of engine processes could not be read",
			"orkflow", planned.GetName(), "version", planned.GetVersion(), "error", err)
		return
	}
	if len(lacking) > 0 {
		// The identities carry host names (pid@host), an operator surface
		// only: this log line, never a client.
		s.warnOverlaysOff("engine processes run a release that cannot read snapshot overlays",
			"missing_capability", engine.CapabilitySnapshotOverlay, "identities", lacking,
			"orkflow", planned.GetName(), "version", planned.GetVersion())
		return
	}
	policy := engine.DefaultOverlayPolicy()
	plan.SnapshotOverlay = &policy
}

// warnOverlaysOff logs why a run tree that could use overlays writes full
// snapshots, at most once a minute per process.
func (s *Service) warnOverlaysOff(why string, keyvals ...any) {
	if !s.overlaysOff.allow(time.Now(), overlaysOffWarnEvery) {
		return
	}
	s.logger.Warn("new runs write full snapshots, not overlays: "+why,
		append([]any{"snapshot_overlays", true}, keyvals...)...)
}
