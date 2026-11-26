package api

import (
	"context"
	"fmt"
	"slices"

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

// timeoutDeclaration is the first run of a run tree whose definition
// declares a run timeout: the top-level definition, or a child it composes,
// depth first; nil when none does.
func timeoutDeclaration(planned *orkstackv1.Orkflow, plan *engine.RunPlan) (*orkstackv1.Orkflow, error) {
	if planned.GetTimeoutSeconds() > 0 {
		return planned, nil
	}
	var found *orkstackv1.Orkflow
	err := engine.ForEachChild(planned, plan, func(_ string, _ *engine.ChildRunPlan, def *orkstackv1.Orkflow) error {
		if found == nil && def.GetTimeoutSeconds() > 0 {
			found = def
		}
		return nil
	})
	return found, err
}

// linesDeclaration is the first run of the tree whose definition declares a
// fan-out over an artifact's lines, or nil.
func linesDeclaration(planned *orkstackv1.Orkflow, plan *engine.RunPlan) (*orkstackv1.Orkflow, error) {
	declares := func(wf *orkstackv1.Orkflow) bool {
		return slices.ContainsFunc(wf.GetTasks(), func(t *orkstackv1.Task) bool { return dsl.IsEachLines(t.GetEach()) })
	}
	if declares(planned) {
		return planned, nil
	}
	var found *orkstackv1.Orkflow
	err := engine.ForEachChild(planned, plan, func(_ string, _ *engine.ChildRunPlan, def *orkstackv1.Orkflow) error {
		if found == nil && declares(def) {
			found = def
		}
		return nil
	})
	return found, err
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
	if largest.total > engine.LegacyEachMaxItems {
		if err := s.requireCapability(ctx, capabilityNeed{
			capability: engine.CapabilityEachItems10k,
			wf:         largest.wf,
			what: fmt.Sprintf("orkflow %s@%s declares %d fan-out items",
				largest.wf.GetName(), largest.wf.GetVersion(), largest.total),
			ability: fmt.Sprintf("run more than %d", engine.LegacyEachMaxItems),
			attrs:   []any{"items", largest.total},
		}); err != nil {
			return err
		}
	}
	lines, err := linesDeclaration(planned, plan)
	if err != nil {
		return connect.NewError(connect.CodeInternal, fmt.Errorf("read the planned child runs: %w", err))
	}
	if lines != nil {
		if err := s.requireCapability(ctx, capabilityNeed{
			capability: engine.CapabilityEachLines,
			wf:         lines,
			what:       fmt.Sprintf("orkflow %s@%s declares a fan-out over an artifact's lines", lines.GetName(), lines.GetVersion()),
			ability:    "expand a fan-out over an artifact's lines",
		}); err != nil {
			return err
		}
	}
	timed, err := timeoutDeclaration(planned, plan)
	if err != nil {
		return connect.NewError(connect.CodeInternal, fmt.Errorf("read the planned child runs: %w", err))
	}
	if timed == nil {
		return nil
	}
	return s.requireCapability(ctx, capabilityNeed{
		capability: engine.CapabilityRunTimeout,
		wf:         timed,
		what:       fmt.Sprintf("orkflow %s@%s declares a run timeout", timed.GetName(), timed.GetVersion()),
		ability:    "enforce a run timeout",
	})
}

// capabilityNeed is one capability a run tree needs: which run of it needs
// it (wf), what it declares that needs it (what), and what a process lacking
// it cannot do (ability), each said in the refusal.
type capabilityNeed struct {
	capability string
	wf         *orkstackv1.Orkflow
	what       string
	ability    string
	attrs      []any // extra attributes of the refusal's log line
}

// requireCapability refuses unless every engine process that could decide a
// run started now advertises need.capability. It fails closed: an engine not
// wired to a cluster, and a list that cannot be read, refuse Unavailable.
func (s *Service) requireCapability(ctx context.Context, need capabilityNeed) error {
	if s.capabilities == nil {
		return connect.NewError(connect.CodeUnavailable, fmt.Errorf(
			"%s; this engine is not wired to a cluster, so it cannot check that every engine process can %s",
			need.what, need.ability))
	}
	lacking, err := s.capabilities.MissingCapability(ctx, need.capability, engine.ScopeWorkflow)
	if err != nil {
		s.logger.Warn("fleet check could not read the engine processes",
			"missing_capability", need.capability, "orkflow", need.wf.GetName(),
			"version", need.wf.GetVersion(), "error", err)
		return connect.NewError(connect.CodeUnavailable, outage(fmt.Errorf(
			"%s; the cluster's list of engine processes could not be read, so whether every engine "+
				"process can %s is unknown; try again", need.what, need.ability)))
	}
	if len(lacking) == 0 {
		return nil
	}
	// The identities carry host names (pid@host), an operator surface only:
	// the log names them, the refusal counts them.
	attrs := append([]any{"missing_capability", need.capability, "identities", lacking,
		"orkflow", need.wf.GetName(), "version", need.wf.GetVersion()}, need.attrs...)
	s.logger.Warn("submit refused: engine processes run a release without a capability the run needs", attrs...)
	return connect.NewError(connect.CodeFailedPrecondition, refused(refusalWait, waitRollout, fmt.Errorf(
		"%s; %s a release that cannot %s; finish the rollout first",
		need.what, processCount(len(lacking)), need.ability)))
}

// processCount is "1 engine process runs" or "N engine processes run".
func processCount(n int) string {
	if n == 1 {
		return "1 engine process runs"
	}
	return fmt.Sprintf("%d engine processes run", n)
}
