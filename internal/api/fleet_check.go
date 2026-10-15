package api

import (
	"context"
	"fmt"

	"connectrpc.com/connect"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/dsl"
	"github.com/orkstack/orkstack/internal/engine"
)

// The fleet check (docs/DEPLOY.md §5, "Engine capabilities and the fleet
// check"): a run that needs an engine capability is refused at submit while
// an `ork` process that could decide it lacks that capability. Such a process
// would compute the run's commands differently, and the first process that
// has it would then fail the run's replay — a stuck run (AGENTS.md §11).
//
// The check reads the cluster's own poller list, once, on the submit that
// needs it; nothing is cached and nothing is registered. It fails closed: a
// list that cannot be read refuses the submit Unavailable.

// itemsDeclaration is the run of a run tree that declares the most fan-out
// items: the top-level definition, or a child it composes.
type itemsDeclaration struct {
	wf    *orkstackv1.Orkflow
	total int32
}

// largestItemsDeclaration walks the planned tree — the top-level definition
// and every child planChildren planned — and returns the run declaring the
// most fan-out items. A child is a run of its own, with its own history, so
// the largest single declaration is what a capability is needed for.
func largestItemsDeclaration(planned *orkstackv1.Orkflow, plan *engine.RunPlan) (itemsDeclaration, error) {
	largest := itemsDeclaration{wf: planned, total: dsl.EachItemsDeclared(planned)}
	err := engine.ForEachChild(planned, plan, func(_ string, _ *engine.ChildRunPlan, def *orkstackv1.Orkflow) error {
		if total := dsl.EachItemsDeclared(def); total > largest.total {
			largest = itemsDeclaration{wf: def, total: total}
		}
		return nil
	})
	return largest, err
}

// fleetCheck refuses a run tree whose runs need a capability some engine
// process that could decide them lacks. It runs after planChildren, so every
// child definition is known, and before anything is minted, so a refusal
// leaves nothing behind. A tree that needs no capability issues no read.
func (s *Service) fleetCheck(ctx context.Context, planned *orkstackv1.Orkflow, plan *engine.RunPlan) error {
	largest, err := largestItemsDeclaration(planned, plan)
	if err != nil {
		return connect.NewError(connect.CodeInternal, fmt.Errorf("read the planned child runs: %w", err))
	}
	if largest.total <= engine.LegacyEachMaxItems {
		return nil
	}
	what := fmt.Sprintf("orkflow %s@%s declares %d fan-out items",
		largest.wf.GetName(), largest.wf.GetVersion(), largest.total)
	if s.capabilities == nil {
		return connect.NewError(connect.CodeUnavailable, fmt.Errorf(
			"%s; this engine is not wired to a cluster, so it cannot check that every engine process can run "+
				"more than %d", what, engine.LegacyEachMaxItems))
	}
	lacking, err := s.capabilities.MissingCapability(ctx, engine.CapabilityEachItems10k, engine.ScopeWorkflow)
	if err != nil {
		s.logger.Warn("fleet check could not read the engine processes",
			"missing_capability", engine.CapabilityEachItems10k, "orkflow", largest.wf.GetName(),
			"version", largest.wf.GetVersion(), "error", err)
		return connect.NewError(connect.CodeUnavailable, fmt.Errorf(
			"%s; the cluster's list of engine processes could not be read, so whether every engine "+
				"process can run more than %d is unknown; try again", what, engine.LegacyEachMaxItems))
	}
	if len(lacking) == 0 {
		return nil
	}
	// The identities carry host names (pid@host), an operator surface only:
	// the log names them, the refusal counts them.
	s.logger.Warn("submit refused: engine processes run a release without a capability the run needs",
		"missing_capability", engine.CapabilityEachItems10k, "identities", lacking,
		"orkflow", largest.wf.GetName(), "version", largest.wf.GetVersion(), "items", largest.total)
	return connect.NewError(connect.CodeFailedPrecondition, fmt.Errorf(
		"%s; %s a release that cannot run more than %d; finish the rollout first",
		what, processCount(len(lacking)), engine.LegacyEachMaxItems))
}

// processCount is "1 engine process runs" or "N engine processes run".
func processCount(n int) string {
	if n == 1 {
		return "1 engine process runs"
	}
	return fmt.Sprintf("%d engine processes run", n)
}
