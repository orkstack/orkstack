package engine

import (
	"compress/gzip"
	"errors"
	"fmt"
	"strings"
	"time"

	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// A definition passed by reference (docs/DEPLOY.md §1,
// engine.definition_by_reference). A run's planned definition is one of its
// start payloads, bound at 256 KiB (AGENTS.md §2.2). One too large for that
// is written, before the start that uses it, as an immutable object under
// the run's own snapshot prefix — artifacts.RunContinuationKey(ns, run, 0,
// "definition", sha256), derived from the run id wherever it is read — and
// the start carries a stub naming it (DefinitionStub) with the plan's
// DefinitionRef. Every generation copies the object into its own history,
// in the bootstrap's bounded chunks, before it decides anything
// (loadDefinition): the AGENTS.md §2.2 saved-state exception, sharing the
// checkpoint's one cap per generation. The stub is what RunStart returns and
// what a switch hands on; only the await doors resolve the reference
// (TemporalRunReader.RunDefinition).

// DefinitionByReferenceChange is the workflow.GetVersion change id of the
// definition load. It is asked once, in generation 0, before anything else,
// and only for a plan that names a definition by reference; a carried
// generation holds the decision in its carry and never asks it again. A
// history recorded by a build that ignored the plan field replays its
// DefaultVersion branch: the stub has no tasks, and the run ends
// INVALID_ORKFLOW as that build recorded.
const DefinitionByReferenceChange = "definition-by-reference"

// The run's own codes for a definition passed by reference that could not
// be used. Each is a non-retryable ApplicationError type and the run's
// Error.code; the reader (closedExecution) and a composed parent
// (childFailure) map them.
const (
	// RunDefinitionUnavailableCode: the definition could not be read from
	// object storage before the restore timeout. InfrastructureError.
	RunDefinitionUnavailableCode = "RUN_DEFINITION_UNAVAILABLE"
	// RunDefinitionCorruptCode: the definition failed verification and was
	// not used. ContractViolation.
	RunDefinitionCorruptCode = "RUN_DEFINITION_CORRUPT"
)

// isRunDefinitionCode reports whether code is one of the RUN_DEFINITION_*
// codes.
func isRunDefinitionCode(code string) bool {
	return code == RunDefinitionUnavailableCode || code == RunDefinitionCorruptCode
}

// runDefinitionCategory is the failure category of each RUN_DEFINITION_*
// code.
func runDefinitionCategory(code string) orkstackv1.FailureCategory {
	if code == RunDefinitionUnavailableCode {
		return orkstackv1.FailureCategory_FAILURE_CATEGORY_INFRASTRUCTURE_ERROR
	}
	return orkstackv1.FailureCategory_FAILURE_CATEGORY_CONTRACT_VIOLATION
}

// corruptDefinitionMessage is RUN_DEFINITION_CORRUPT's sentence, in run
// vocabulary.
func corruptDefinitionMessage(what string) string {
	return fmt.Sprintf("its definition failed verification (%s) and was not used; the run has stopped", what)
}

// unavailableDefinitionMessage is RUN_DEFINITION_UNAVAILABLE's sentence.
func unavailableDefinitionMessage(timeout time.Duration, last error) string {
	cause := "none"
	if last != nil {
		cause = errorText(last)
	}
	return fmt.Sprintf("its definition could not be read from object storage for %s (last error: %s); "+
		"the run has stopped", timeout, cause)
}

// DefinitionStub is what a run whose definition travels by reference starts
// with: the definition's name, version, namespace and digest, and nothing
// else. RunStart returns it; a switch hands it on unchanged.
func DefinitionStub(wf *orkstackv1.Orkflow) *orkstackv1.Orkflow {
	return &orkstackv1.Orkflow{
		Name:             wf.GetName(),
		Version:          wf.GetVersion(),
		Namespace:        wf.GetNamespace(),
		DefinitionDigest: wf.GetDefinitionDigest(),
	}
}

// definitionParts splits a planned definition into the messages its object
// stores, in order: part 0 is the Orkflow with its tasks cleared, then
// Orkflow{tasks: [...]} parts of at most checkpointPartRawBytes encoded each
// (a larger task goes alone). Concatenated Orkflow encodings merge — repeated
// tasks append, and the scalars, maps and pins appear only in part 0 — so a
// reader can unmarshal the whole object once, and a generation decodes it
// part by part.
func definitionParts(planned *orkstackv1.Orkflow) []*orkstackv1.Orkflow {
	header := proto.Clone(planned).(*orkstackv1.Orkflow)
	header.Tasks = nil
	parts := []*orkstackv1.Orkflow{header}
	var current *orkstackv1.Orkflow
	size := 0
	for _, t := range planned.GetTasks() {
		taskSize := proto.Size(t) + 6 // tag and length prefix
		if current == nil || (size > 0 && size+taskSize > checkpointPartRawBytes) {
			current = &orkstackv1.Orkflow{}
			parts = append(parts, current)
			size = 0
		}
		current.Tasks = append(current.Tasks, t)
		size += taskSize
	}
	return parts
}

// EncodeDefinitionObject encodes a planned definition as the object a run
// whose definition travels by reference reads: concatenated gzip members
// (BestCompression), each one deterministic Orkflow encoding of
// definitionParts, with every member's and the whole object's SHA-256.
// Deterministic, so the same definition gives the same bytes and key, and a
// retried write converges create-if-absent.
func EncodeDefinitionObject(planned *orkstackv1.Orkflow) ([]byte, DefinitionRef, error) {
	if planned == nil || len(planned.GetTasks()) == 0 {
		return nil, DefinitionRef{}, errors.New("engine: encode definition object: the definition has no tasks")
	}
	var (
		data []byte
		ref  DefinitionRef
	)
	for i, part := range definitionParts(planned) {
		plain, err := proto.MarshalOptions{Deterministic: true}.Marshal(part)
		if err != nil {
			return nil, DefinitionRef{}, fmt.Errorf("engine: encode the definition, part %d: %w", i, err)
		}
		member, err := gzipBytes(plain, gzip.BestCompression)
		if err != nil {
			return nil, DefinitionRef{}, fmt.Errorf("engine: compress the definition, part %d: %w", i, err)
		}
		ref.Parts = append(ref.Parts, DefinitionPartRef{
			Offset:    int64(len(data)),
			SizeBytes: int64(len(member)),
			RawBytes:  int64(len(plain)),
			SHA256:    artifacts.ContinuationSHA256(member),
		})
		ref.RawBytes += int64(len(plain))
		data = append(data, member...)
	}
	ref.SizeBytes = int64(len(data))
	ref.SHA256 = artifacts.ContinuationSHA256(data)
	return data, ref, nil
}

// object is the reference as the bootstrap's reader takes one: no URI (the
// key is derived from the run id).
func (r *DefinitionRef) object() *orkstackv1.ContinuationObject {
	obj := &orkstackv1.ContinuationObject{Sha256: r.SHA256, SizeBytes: r.SizeBytes, RawBytes: r.RawBytes}
	for _, part := range r.Parts {
		obj.Parts = append(obj.Parts, &orkstackv1.ContinuationPart{
			Offset: part.Offset, SizeBytes: part.SizeBytes, RawBytes: part.RawBytes, Sha256: part.SHA256,
		})
	}
	return obj
}

// check is the structural check every reader of a definition reference
// makes before reading anything: a positive size within capBytes, a 64-hex
// SHA-256 for the whole and each part, parts contiguous from offset 0 that
// cover the object, and raw sizes that sum to the whole. It returns what
// failed, or "".
func (r *DefinitionRef) check(capBytes int64) string {
	switch {
	case r == nil:
		return "the run's plan names no definition"
	case r.SizeBytes <= 0 || r.RawBytes <= 0:
		return "its reference records no size"
	case r.SizeBytes > capBytes:
		return fmt.Sprintf("it is %d bytes, over the %d the run carries forward", r.SizeBytes, capBytes)
	case !isHexSHA256(r.SHA256):
		return "its reference names no valid SHA-256"
	case len(r.Parts) == 0:
		return "its reference names no parts"
	}
	var offset, raw int64
	for i, part := range r.Parts {
		if part.Offset != offset || part.SizeBytes <= 0 || part.RawBytes <= 0 || !isHexSHA256(part.SHA256) {
			return fmt.Sprintf("part %d is not contiguous from offset 0", i)
		}
		offset += part.SizeBytes
		raw += part.RawBytes
	}
	if offset != r.SizeBytes {
		return fmt.Sprintf("its parts cover %d of %d bytes", offset, r.SizeBytes)
	}
	if raw != r.RawBytes {
		return "its parts decode to another size than its reference records"
	}
	return ""
}

func isHexSHA256(s string) bool {
	if len(s) != 64 {
		return false
	}
	return strings.Trim(s, "0123456789abcdef") == ""
}

// mergeDefinitionPart folds one decoded part into the definition being
// assembled: part 0 is the header and must carry no task; every later part
// carries tasks and nothing else.
func mergeDefinitionPart(into **orkstackv1.Orkflow, index int, part *orkstackv1.Orkflow) error {
	if index == 0 {
		if len(part.GetTasks()) != 0 {
			return invalidDefinition("part 0 holds tasks; only later parts may")
		}
		*into = part
		return nil
	}
	if len(part.GetTasks()) == 0 {
		return invalidDefinition("part %d holds no task", index)
	}
	rest := proto.Clone(part).(*orkstackv1.Orkflow)
	rest.Tasks = nil
	if proto.Size(rest) != 0 {
		return invalidDefinition("part %d holds more than tasks", index)
	}
	(*into).Tasks = append((*into).Tasks, part.GetTasks()...)
	return nil
}

func invalidDefinition(format string, args ...any) error {
	return invalidContinuation(subjectDefinition, format, args...)
}

// definitionIdentity checks that a loaded definition is the one the stub
// names: name, version, namespace and digest. It returns what differs, or
// "".
func definitionIdentity(loaded, stub *orkstackv1.Orkflow) string {
	switch {
	case loaded.GetName() != stub.GetName() || loaded.GetVersion() != stub.GetVersion():
		return "it names another orkflow or version than the run started"
	case loaded.GetNamespace() != stub.GetNamespace():
		return "it names another namespace than the run started in"
	case loaded.GetDefinitionDigest() != stub.GetDefinitionDigest():
		return "its digest is not the one the run started with"
	case len(loaded.GetTasks()) == 0:
		return "it declares no tasks"
	}
	return ""
}

// DecodeDefinitionObject verifies and decodes a whole stored definition
// object against its reference — its size, the whole object's SHA-256, each
// part's place, SHA-256 and decoded size — and checks that it is the
// definition stub names. Every failure wraps ErrContinuationObjectInvalid
// and never names the object. Not workflow code: the await doors' read
// (RunDefinition).
func DecodeDefinitionObject(data []byte, ref *DefinitionRef, stub *orkstackv1.Orkflow) (*orkstackv1.Orkflow, error) {
	if what := ref.check(int64(len(data))); what != "" {
		return nil, invalidDefinition("%s", what)
	}
	if err := verifyContinuationBytes(data, ref.object(), subjectDefinition); err != nil {
		return nil, err
	}
	var loaded *orkstackv1.Orkflow
	for i, part := range ref.Parts {
		member := data[part.Offset : part.Offset+part.SizeBytes]
		if artifacts.ContinuationSHA256(member) != part.SHA256 {
			return nil, invalidDefinition("part %d SHA-256 mismatch", i)
		}
		plain, err := gunzipBounded(member, part.RawBytes)
		if err != nil {
			return nil, invalidDefinition("part %d: %v", i, err)
		}
		message := &orkstackv1.Orkflow{}
		if err := proto.Unmarshal(plain, message); err != nil {
			return nil, invalidDefinition("part %d: %v", i, err)
		}
		if err := mergeDefinitionPart(&loaded, i, message); err != nil {
			return nil, err
		}
	}
	if what := definitionIdentity(loaded, stub); what != "" {
		return nil, invalidDefinition("%s", what)
	}
	return loaded, nil
}

// definitionDecision is the definition-by-reference decision of this
// generation: asked once in generation 0, before anything else, and read
// from the carry in every later one, never asked again (AGENTS.md §7.2). A
// carry without it cannot have come from a generation that loaded the
// definition, so it is reported as absent.
func definitionDecision(ctx workflow.Context, carry *orkstackv1.RunContinuation) (workflow.Version, bool) {
	if carry != nil {
		v, ok := carry.GetVersionDecisions()[DefinitionByReferenceChange]
		return workflow.Version(v), ok
	}
	if definitionByReferenceForcedForTest {
		return 1, true
	}
	return workflow.GetVersion(ctx, DefinitionByReferenceChange, workflow.DefaultVersion, 1), true
}

// definitionByReferenceForcedForTest is a TEST-ONLY switch, the precedent of
// continueAsNewForcedForTest: generation 0 takes version 1 WITHOUT asking
// workflow.GetVersion, so a negative replay test proves the committed
// fixtures replay only because of the gate. Never set in production code.
var definitionByReferenceForcedForTest = false

// loadDefinition copies the run's definition object into this generation's
// history with the bootstrap's own reader (readBootstrapObject): one local
// activity per chunk, retried by Temporal's local-activity retry policy
// until the bootstrap's deadline, ChunksPerTask per workflow task with a
// yield, every part verified as it completes, then the whole object's
// SHA-256 and the stub's identity. Workflow code never sees a URI: the read
// names the object by its key, and the store resolves it.
//
// It checks the plan's budget and the reference first, so a malformed plan
// ends the run RUN_DEFINITION_CORRUPT and never panics the workflow task.
// It returns the loaded definition, or restoreCanceled, or a
// RUN_DEFINITION_* code and its sentence. capBytes is what this generation
// may copy: the plan's checkpoint bound, shared with the checkpoint it
// reads next.
func (s *runState) loadDefinition(ctx workflow.Context, stub *orkstackv1.Orkflow) (*orkstackv1.Orkflow, string, string) {
	budget := s.plan.Continuation
	if !budget.valid() {
		return nil, RunDefinitionCorruptCode, corruptDefinitionMessage("the run's plan names no valid budget to read it with")
	}
	ref := s.plan.Definition
	if what := ref.check(budget.CheckpointMaxBytes); what != "" {
		return nil, RunDefinitionCorruptCode, corruptDefinitionMessage(what)
	}
	var limit HistoryLimit
	if l := s.historyLimit(); l != nil {
		limit = *l
	}
	var loaded *orkstackv1.Orkflow
	code, detail := s.readBootstrapObject(ctx, *budget, limit, bootstrapObject{
		loc: ContinuationLocator{
			Namespace: stub.GetNamespace(), RunID: s.runID, Generation: 0,
			Kind: artifacts.ContinuationKindDefinition, SHA256: ref.SHA256,
		},
		ref: ref.object(),
		decode: func(index int, plain []byte) error {
			part := &orkstackv1.Orkflow{}
			if err := proto.Unmarshal(plain, part); err != nil {
				return invalidDefinition("part %d: %v", index, err)
			}
			return mergeDefinitionPart(&loaded, index, part)
		},
	})
	switch code {
	case "":
	case restoreCanceled:
		return nil, restoreCanceled, "run canceled"
	case RunStateCorruptCode:
		return nil, RunDefinitionCorruptCode, corruptDefinitionMessage(detail)
	default:
		return nil, RunDefinitionUnavailableCode, unavailableDefinitionMessage(bootstrapTimeout(*budget), s.restoreErr)
	}
	if what := definitionIdentity(loaded, stub); what != "" {
		return nil, RunDefinitionCorruptCode, corruptDefinitionMessage(what)
	}
	return loaded, "", ""
}

// endBeforeStart ends generation 0 of a run whose definition could not be
// used, explicitly, on a disconnected context, so an operator's cancel that
// arrived during the load cannot stop the ending itself: the run, built from
// the stub and the plan alone (its id, namespace, name, version, digest and
// origin; created at its start), publishes RUN_CREATED at sequence 1 —
// every stream begins there, as a start that failed before Temporal begins
// its own (recordFailedStart) — and then RUN_FAILED (or RUN_CANCELED) at
// sequence 2, each with its snapshot, so the run has a stream and an archive
// body. It returns the ApplicationError, or the cancel.
func (s *runState) endBeforeStart(ctx workflow.Context, code, message string) (*orkstackv1.Run, error) {
	canceled := ctx.Err() != nil || code == restoreCanceled
	ectx, cancel := workflow.NewDisconnectedContext(ctx)
	defer cancel()
	if err := s.emit(ectx, orkstackv1.EventType_EVENT_TYPE_RUN_CREATED, "", orkstackv1.Status_STATUS_PENDING,
		fmt.Sprintf("run created for orkflow %s@%s", s.wf.GetName(), s.wf.GetVersion())); err != nil {
		// Object storage is down: the reader synthesizes the run's end from
		// the execution's own close, mapped to the same code (closedExecution).
		workflow.GetLogger(ctx).Error("write the first snapshot of a run whose definition could not be used failed",
			"run_id", s.runID, "code", code, "error", err)
	}
	s.finished = timestamppb.New(workflow.Now(ectx))
	eventType := orkstackv1.EventType_EVENT_TYPE_RUN_FAILED
	s.status = orkstackv1.Status_STATUS_FAILED
	s.runError = &orkstackv1.Error{Code: code, Message: message, Category: runDefinitionCategory(code)}
	if canceled {
		eventType = orkstackv1.EventType_EVENT_TYPE_RUN_CANCELED
		s.status = orkstackv1.Status_STATUS_CANCELED
		s.runError = &orkstackv1.Error{Code: "RUN_CANCELED", Message: "run canceled",
			Category: orkstackv1.FailureCategory_FAILURE_CATEGORY_CANCELLED}
	}
	if err := s.emit(ectx, eventType, "", s.status, s.runError.GetMessage()); err != nil {
		workflow.GetLogger(ctx).Error("write the terminal snapshot of a run whose definition could not be used failed",
			"run_id", s.runID, "code", s.runError.GetCode(), "error", err)
	}
	if canceled {
		return s.snapshot(), temporal.NewCanceledError()
	}
	return s.snapshot(), temporal.NewNonRetryableApplicationError(message, code, nil)
}

// definitionBytes is what this run's definition passed by reference takes
// of each generation's copy: its stored size, or 0 for a run whose
// definition travelled inline. The checkpoint shares the cap with it.
func (p RunPlan) definitionBytes() int64 {
	if p.Definition == nil {
		return 0
	}
	return p.Definition.SizeBytes
}

// loadDefinitionArgument resolves RunWorkflow's definition argument. For a
// plan with no definition by reference — every plan before it existed — it
// does nothing and returns no loader. Otherwise it decides the gate
// (definitionDecision); on DefaultVersion the stub stays the definition, so
// a history recorded by a build that ignored the plan field replays its
// INVALID_ORKFLOW. On version 1 it loads the definition in a loader state
// built from the stub, and returns that state for the run's own to adopt
// its history count and bootstrap clock. A load that fails ends the run
// here (end true): generation 0 through endBeforeStart, a carried one
// through endUnrestorable, from its switch snapshot.
func loadDefinitionArgument(
	ctx workflow.Context,
	runID string,
	stub *orkstackv1.Orkflow,
	input *orkstackv1.Payload,
	plan RunPlan,
	carry *orkstackv1.RunContinuation,
) (loader *runState, loaded *orkstackv1.Orkflow, ended *orkstackv1.Run, end bool, err error) {
	if plan.Definition == nil {
		return nil, nil, nil, false, nil
	}
	v, decided := definitionDecision(ctx, carry)
	if decided && v < 1 {
		return nil, nil, nil, false, nil
	}
	loader = newRunState(ctx, runID, stub, input, plan)
	loader.versions = map[string]workflow.Version{DefinitionByReferenceChange: v}
	if carry != nil {
		loader.generation = carry.GetGeneration()
	}
	if !decided {
		run, err := loader.endUnrestorable(ctx, carry, RunDefinitionCorruptCode,
			corruptDefinitionMessage("the run's saved state does not record how its definition is read"))
		return nil, nil, run, true, err
	}
	// A carried generation copies its definition and its checkpoint under
	// one cap: refused before either is read.
	if budget := plan.Continuation; carry != nil && budget.valid() {
		if what := copyOverCap(*budget, plan, carry); what != "" {
			run, err := loader.endUnrestorable(ctx, carry, RunStateCorruptCode, corruptStateMessage(what))
			return nil, nil, run, true, err
		}
	}
	def, code, message := loader.loadDefinition(ctx, stub)
	switch {
	case code == "":
		return loader, def, nil, false, nil
	case carry != nil && code == restoreCanceled:
		run, err := loader.endUnrestorable(ctx, carry, "RUN_CANCELED", "run canceled")
		return nil, nil, run, true, err
	case carry != nil:
		run, err := loader.endUnrestorable(ctx, carry, code, message)
		return nil, nil, run, true, err
	default:
		run, err := loader.endBeforeStart(ctx, code, message)
		return nil, nil, run, true, err
	}
}

// adoptDefinitionLoad carries what the definition's load did over to the
// run's own state: the commands it counted in this workflow task, the
// bootstrap's clock and batch, which the checkpoint read after it shares,
// and — in generation 0 — the gate's decision, so the first switch carries
// it (a carried generation's restore reads every decision from its carry).
func (s *runState) adoptDefinitionLoad(loader *runState, carry *orkstackv1.RunContinuation) {
	s.hist = loader.hist
	s.bootstrapStarted, s.bootstrapDeadline, s.bootstrapBatch =
		loader.bootstrapStarted, loader.bootstrapDeadline, loader.bootstrapBatch
	if carry == nil {
		if s.versions == nil {
			s.versions = map[string]workflow.Version{}
		}
		s.versions[DefinitionByReferenceChange] = loader.versions[DefinitionByReferenceChange]
	}
}
