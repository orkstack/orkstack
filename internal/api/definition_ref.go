package api

import (
	"context"
	"errors"
	"fmt"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/proto"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/internal/task"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// A definition passed by reference (docs/DEPLOY.md §1,
// engine.definition_by_reference). A top-level run's planned definition is
// one of its start payloads, which AGENTS.md §2.2 bounds at 256 KiB. With
// the flag on, one larger than that travels by reference: prepare encodes it
// (engine.EncodeDefinitionObject), the plan names it without a URI
// (engine.RunPlan.Definition), the start carries a stub
// (engine.DefinitionStub), and the object is written under the run's own
// prefix once the run id is final, immediately before each start that uses
// it. A child never needs one: its definition travels in its parent's plan,
// which engine.MaxPlanBytes bounds.

// definitionCarryShare is the share of a generation's copy bound
// (ContinuationBudget.CheckpointMaxBytes) a definition passed by reference
// may take, compressed: a quarter, so a run at the bound still has three
// quarters of every generation's copy for its saved state. Read at submit
// and apply only; the workflow checks the shared bound it pinned.
const definitionCarryShare = 4

// DefinitionObjectWriter stores a run's definition object create-if-absent
// under its content-addressed key (artifacts.RunSnapshotStore's
// WriteContinuation, kind "definition", generation 0).
type DefinitionObjectWriter interface {
	WriteContinuation(ctx context.Context, namespace, runID string, generation int64, kind string, data []byte) (string, error)
}

// definitionAtStart decides how a top-level run's planned definition
// travels. Inline, the start definition is planned and data is nil — always
// when the flag is off, and whenever the definition fits a start payload.
// By reference, it checks the fleet (every engine process that could decide
// the run must read a definition this large), encodes the object, bounds
// it at a quarter of the copy a generation may take, names it on the plan,
// and returns the stub with the object's bytes, which startPreparedRun (or
// the fire's preparation) writes once the run id is final.
func (s *Service) definitionAtStart(
	ctx context.Context,
	planned *orkstackv1.Orkflow,
	plan *engine.RunPlan,
	input *orkstackv1.Payload,
) (*orkstackv1.Orkflow, []byte, error) {
	if !s.definitionByReference || plan.Continuation == nil {
		return planned, nil, nil
	}
	size, err := task.PayloadSize(planned)
	if err != nil {
		return nil, nil, connect.NewError(connect.CodeInternal, fmt.Errorf("encode the orkflow definition: %w", err))
	}
	if size <= int64(task.MaxActivityInputBytes) {
		return planned, nil, nil
	}
	if err := s.definitionFleetCheck(ctx, planned, input); err != nil {
		return nil, nil, err
	}
	data, ref, err := engine.EncodeDefinitionObject(planned)
	if err != nil {
		return nil, nil, connect.NewError(connect.CodeInternal, fmt.Errorf("encode the orkflow definition: %w", err))
	}
	if limit := plan.Continuation.CheckpointMaxBytes / definitionCarryShare; ref.SizeBytes > limit {
		return nil, nil, connect.NewError(connect.CodeFailedPrecondition, fmt.Errorf(
			"orkflow %s@%s's planned definition is %s compressed, over the %s a run may carry forward; "+
				"split it into composed Orkflows", planned.GetName(), planned.GetVersion(),
			mebibyteText(ref.SizeBytes), mebibyteText(limit)))
	}
	plan.Definition = &ref
	return engine.DefinitionStub(planned), data, nil
}

// definitionFleetCheck refuses a definition that must travel by reference
// while an engine process that could decide the run runs a release without
// CapabilityDefinitionRef: that process would find a start definition with
// no tasks. The refusal is today's 256 KiB one with a sentence counting the
// processes, never naming them; the identities go to the engine's log. An
// unreadable fleet refuses UNAVAILABLE: fail closed.
func (s *Service) definitionFleetCheck(ctx context.Context, planned *orkstackv1.Orkflow, input *orkstackv1.Payload) error {
	if s.capabilities == nil {
		return connect.NewError(connect.CodeUnavailable, fmt.Errorf(
			"orkflow %s@%s's planned definition is over the %d KiB a run's start may carry; this engine is not "+
				"wired to a cluster, so it cannot check that every engine process can read it by reference",
			planned.GetName(), planned.GetVersion(), task.MaxActivityInputBytes/1024))
	}
	lacking, err := s.capabilities.MissingCapability(ctx, engine.CapabilityDefinitionRef, engine.ScopeWorkflow)
	if err != nil {
		s.logger.Warn("fleet check could not read the engine processes",
			"missing_capability", engine.CapabilityDefinitionRef, "orkflow", planned.GetName(),
			"version", planned.GetVersion(), "error", err)
		return connect.NewError(connect.CodeUnavailable, fmt.Errorf(
			"orkflow %s@%s's planned definition is over the %d KiB a run's start may carry; the cluster's list of "+
				"engine processes could not be read, so whether every engine process can read it by reference "+
				"is unknown; try again", planned.GetName(), planned.GetVersion(), task.MaxActivityInputBytes/1024))
	}
	if len(lacking) == 0 {
		return nil
	}
	s.logger.Warn("submit refused: engine processes run a release without a capability the run needs",
		"missing_capability", engine.CapabilityDefinitionRef, "identities", lacking,
		"orkflow", planned.GetName(), "version", planned.GetVersion())
	refusal := checkRunStartPayloads(planned, input)
	var ce *connect.Error
	if !errors.As(refusal, &ce) {
		return refusal
	}
	return connect.NewError(ce.Code(), fmt.Errorf("%s; %s a release that cannot yet read a definition this large",
		ce.Message(), processCount(len(lacking))))
}

// checkDefinitionCarriable is apply's half of the bound: with the flag on, a
// definition too large for a start payload must also fit the quarter of a
// generation's copy it would travel in, measured on the applied definition
// as a run would start it (no source). Refused INVALID_ARGUMENT at tasks,
// before anything is stored.
func (s *Service) checkDefinitionCarriable(wf *orkstackv1.Orkflow) error {
	if !s.definitionByReference || s.continuation == nil {
		return nil
	}
	started := proto.Clone(wf).(*orkstackv1.Orkflow)
	started.SourceYaml = ""
	size, err := task.PayloadSize(started)
	if err != nil {
		return connect.NewError(connect.CodeInternal, fmt.Errorf("encode the orkflow definition: %w", err))
	}
	if size <= int64(task.MaxActivityInputBytes) {
		return nil
	}
	_, ref, err := engine.EncodeDefinitionObject(started)
	if err != nil {
		return connect.NewError(connect.CodeInternal, fmt.Errorf("encode the orkflow definition: %w", err))
	}
	if limit := s.continuation.CheckpointMaxBytes / definitionCarryShare; ref.SizeBytes > limit {
		return connect.NewError(connect.CodeInvalidArgument, &validationFieldError{Field: "tasks", Err: fmt.Errorf(
			"orkflow %s@%s's definition is %s compressed, over the %s a run may carry forward; "+
				"split it into composed Orkflows", wf.GetName(), wf.GetVersion(),
			mebibyteText(ref.SizeBytes), mebibyteText(limit))})
	}
	return nil
}

// writeDefinitionObject writes a submission's definition object under runID,
// create-if-absent and content-addressed, so a retried or reissued write
// converges; a no-op for a definition that travels inline. Called once the
// run id is final, immediately before the start that uses it.
func (s *Service) writeDefinitionObject(ctx context.Context, runID string, sub submission) error {
	if sub.definitionObject == nil {
		return nil
	}
	if s.definitionObjects == nil {
		return errors.New("store the run's definition: no object store is configured")
	}
	namespace := sub.wf.GetNamespace()
	if _, err := s.definitionObjects.WriteContinuation(ctx, namespace, runID, 0,
		artifacts.ContinuationKindDefinition, sub.definitionObject); err != nil {
		// A store error quotes the object's URI or path, whose last segment
		// names its generation and kind; that segment becomes
		// "<definition>", as on every read (engine.withoutObjectName).
		if key, keyErr := artifacts.RunContinuationKey(namespace, runID, 0, artifacts.ContinuationKindDefinition,
			artifacts.ContinuationSHA256(sub.definitionObject)); keyErr == nil {
			err = engine.WithoutDefinitionObjectName(err, key)
		}
		return fmt.Errorf("store the run's definition: %w", err)
	}
	// R3's check (docs/DEPLOY.md §4): an operator rolling back below this
	// release looks for these lines.
	s.logger.Info("run starts with its definition passed by reference",
		"run_id", runID, "definition_by_reference", true, "definition_bytes", len(sub.definitionObject))
	return nil
}

// mebibyteText is n bytes in MiB, one decimal.
func mebibyteText(n int64) string {
	return fmt.Sprintf("%.1f MiB", float64(n)/(1<<20))
}
