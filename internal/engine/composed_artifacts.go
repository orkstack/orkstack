package engine

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sort"
	"strings"
	"time"

	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/dsl"
	"github.com/orkstack/orkstack/internal/stablejson"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// Composed runs pass artifacts (docs/DSL.md, "Passing artifacts to a child
// run"). A composed task's artifacts: bindings are resolved, when it starts
// its child, from the results this run recorded; the objects are stat'd and
// claimed for the parent (docs/STORAGE.md, "Reclaiming space"), and the
// child's plan carries references to them, never the bytes. The child claims
// them again under its own id before its first dispatch, and a task of the
// child whose definition names one (Task.input_artifacts) is handed it as
// the reserved upstream _inputs.

const (
	// ActivityStatAndClaimChildInputs is the local activity a composed task
	// runs before it starts a child that receives artifacts: it stats every
	// object and, only when all are there, claims them for the parent.
	ActivityStatAndClaimChildInputs = "StatAndClaimChildInputs"
	// ActivityClaimReturnedArtifacts is the local activity a composed task
	// runs when its child succeeded with artifact outputs: it claims them
	// for the parent before the parent records them as the task's own.
	ActivityClaimReturnedArtifacts = "ClaimReturnedArtifacts"
	// ActivityClaimReceivedArtifacts is the local activity a child that
	// received artifacts runs before its first dispatch: it claims them for
	// itself.
	ActivityClaimReceivedArtifacts = "ClaimReceivedArtifacts"

	// ChildStartTooLargeCode: the references a composed task passes would
	// take its child's start past what the submit reserved for them
	// (ContractViolation); no child is started.
	ChildStartTooLargeCode = "CHILD_START_TOO_LARGE"

	// Source metadata keys on a passed ArtifactRef: audit data naming the
	// run, task and name that recorded the object, never an input to any
	// decision.
	ArtifactSourceRunKey  = "source_run_id"
	ArtifactSourceTaskKey = "source_task_id"
	ArtifactSourceNameKey = "source_name"
)

// resolveChildArtifacts resolves a composed task's bindings, without IO, from
// what this run recorded: a producer's committed result, a failed direct
// need's recorded artifacts for a failure handler, or one of the run's own
// received artifacts for _inputs. Each reference is renamed to the input it
// binds and carries its source. A binding that resolves to nothing is the
// refusal's sentence.
func (s *runState) resolveChildArtifacts(id string, bindings []*orkstackv1.ChildArtifact) ([]NamedArtifactPlan, string) {
	out := make([]NamedArtifactPlan, 0, len(bindings))
	for _, b := range bindings {
		label := b.GetTaskId() + "/" + b.GetName()
		if b.GetTaskId() == dsl.InputsProducer {
			received, ok := s.receivedArtifact(b.GetName())
			if !ok {
				return nil, fmt.Sprintf("task %q: artifacts.%s: this run received no artifact %q",
					id, b.GetInput(), b.GetName())
			}
			received.Name = b.GetInput()
			received.Artifact.Name = b.GetInput()
			out = append(out, received)
			continue
		}
		var recorded []*orkstackv1.ArtifactRef
		if result, ok := s.results[b.GetTaskId()]; ok {
			recorded = result.GetArtifacts()
		} else if isFailureHandler(s.tasks[id]) && s.directlyNeeds(id, b.GetTaskId()) {
			recorded = failedNeedRef(s.taskRuns[b.GetTaskId()]).GetArtifacts()
		}
		var ref *orkstackv1.ArtifactRef
		for _, candidate := range recorded {
			if candidate.GetName() == b.GetName() {
				ref = candidate
				break
			}
		}
		if ref == nil {
			return nil, fmt.Sprintf("task %q: artifacts.%s: task %q recorded no artifact %q, so %s cannot be passed",
				id, b.GetInput(), b.GetTaskId(), b.GetName(), label)
		}
		artifact := ArtifactRefPlanOf(ref)
		artifact.Name = b.GetInput()
		if artifact.Metadata == nil {
			artifact.Metadata = map[string]string{}
		}
		artifact.Metadata[ArtifactSourceRunKey] = s.runID
		artifact.Metadata[ArtifactSourceTaskKey] = b.GetTaskId()
		artifact.Metadata[ArtifactSourceNameKey] = b.GetName()
		out = append(out, NamedArtifactPlan{Name: b.GetInput(), RunID: s.runID, TaskID: b.GetTaskId(), Artifact: artifact})
	}
	return out, ""
}

// receivedArtifact is the artifact this run received under name.
func (s *runState) receivedArtifact(name string) (NamedArtifactPlan, bool) {
	for _, received := range s.plan.InputArtifacts {
		if received.Name == name {
			copied := received
			copied.Artifact = ArtifactRefPlanOf(received.Artifact.Proto())
			return copied, true
		}
	}
	return NamedArtifactPlan{}, false
}

// childArtifactsBytes is the stable JSON size of what a composed task adds
// to its child's start: deterministic, so the same on every replay.
func childArtifactsBytes(received []NamedArtifactPlan) (int, error) {
	encoded, err := stablejson.Marshal(received)
	if err != nil {
		return 0, err
	}
	return len(encoded), nil
}

// passChildArtifacts is a composed task's half of passing artifacts, between
// its child's input render and its start: resolve, bound, stat and claim.
// It returns the references the child's plan carries, or the error that
// failed the task; no child is started on an error.
func (s *runState) passChildArtifacts(ctx workflow.Context, id string) ([]NamedArtifactPlan, error) {
	bindings := s.tasks[id].GetChildArtifacts()
	if len(bindings) == 0 {
		return nil, nil
	}
	received, missing := s.resolveChildArtifacts(id, bindings)
	if missing != "" {
		return nil, s.failComposed(ctx, id, ChildInputArtifactMissingCode,
			orkstackv1.FailureCategory_FAILURE_CATEGORY_CONTRACT_VIOLATION, missing, "")
	}
	claimCtx, policy, err := s.claimContext(ctx)
	if err != nil {
		return nil, s.failComposed(ctx, id, InvalidCompositionCode,
			orkstackv1.FailureCategory_FAILURE_CATEGORY_CONTRACT_VIOLATION,
			fmt.Sprintf("task %q: %v; no child run was started", id, err), "")
	}
	size, err := childArtifactsBytes(received)
	if err != nil {
		return nil, s.failComposed(ctx, id, ChildStartFailedCode,
			orkstackv1.FailureCategory_FAILURE_CATEGORY_INFRASTRUCTURE_ERROR,
			fmt.Sprintf("task %q: its child's artifacts could not be encoded: %v", id, err), "")
	}
	if reserved := len(bindings) * int(policy.ReserveBytesPerBind); size > reserved {
		return nil, s.failComposed(ctx, id, ChildStartTooLargeCode,
			orkstackv1.FailureCategory_FAILURE_CATEGORY_CONTRACT_VIOLATION,
			fmt.Sprintf("task %q: the %d artifact references it passes its child take %d bytes, more than the %d "+
				"its run reserved for them (%d per binding)", id, len(bindings), size, reserved,
				policy.ReserveBytesPerBind), "")
	}
	// Recorded on the row BEFORE the claim: a claim that writes some markers
	// and then fails, or a run ended between the claim and its next record,
	// still leaves the parent's archive listing every object it may have
	// claimed, so its retirement releases them.
	child := s.taskRuns[id].GetChild()
	if child == nil {
		return nil, s.failComposed(ctx, id, InvalidCompositionCode,
			orkstackv1.FailureCategory_FAILURE_CATEGORY_CONTRACT_VIOLATION,
			fmt.Sprintf("task %q has no child record to list what it passes; no child run was started", id), "")
	}
	child.InputArtifacts = namedArtifactsProto(received)
	req := ArtifactClaimRequest{RunID: s.runID, Artifacts: received}
	err = workflow.ExecuteLocalActivity(claimCtx, ActivityStatAndClaimChildInputs, req).Get(ctx, nil)
	if err != nil {
		node := categoryOf(err)
		if node.canceled || ctx.Err() != nil {
			return nil, s.canceledBeforeChildStart(ctx, id, err)
		}
		if node.appType() == ChildInputArtifactMissingCode {
			return nil, s.failComposed(ctx, id, ChildInputArtifactMissingCode,
				orkstackv1.FailureCategory_FAILURE_CATEGORY_CONTRACT_VIOLATION,
				fmt.Sprintf("task %q: %s; no child run was started", id, node.appMessage()), "")
		}
		return nil, s.failComposed(ctx, id, ChildInputUnavailableCode,
			orkstackv1.FailureCategory_FAILURE_CATEGORY_INFRASTRUCTURE_ERROR,
			fmt.Sprintf("task %q: the artifacts it passes its child could not be checked and claimed in object "+
				"storage: %v; no child run was started", id, err), "")
	}
	return received, nil
}

// claimReceived is a child's own claim of what it received, before its
// first dispatch, in its first generation only: markers persist across a
// continuation. It first checks that every artifact input a task names, in a
// template or as the source a fan-out reads the lines of, was received, so no
// task is dispatched without a file its definition names.
// nil when the run received nothing and no task names anything.
func (s *runState) claimReceived(ctx workflow.Context) error {
	for _, task := range s.wf.GetTasks() {
		for _, name := range namedInputs(task) {
			if _, ok := s.receivedArtifact(name); !ok {
				return runLevelError(ChildInputArtifactMissingCode, fmt.Sprintf(
					"task %q names artifact input %q, which this run did not receive; no task was started",
					task.GetId(), name))
			}
		}
	}
	if len(s.plan.InputArtifacts) == 0 {
		return nil
	}
	req := ArtifactClaimRequest{RunID: s.runID, Artifacts: s.plan.InputArtifacts}
	claimCtx, _, err := s.claimContext(ctx)
	if err != nil {
		return runLevelError(InvalidCompositionCode, err.Error())
	}
	err = workflow.ExecuteLocalActivity(claimCtx, ActivityClaimReceivedArtifacts, req).Get(ctx, nil)
	if err == nil {
		return nil
	}
	node := categoryOf(err)
	switch {
	case node.canceled || ctx.Err() != nil:
		return err // a cancel ends the run canceled, never as a claim failure
	case node.appType() == ChildInputArtifactMissingCode:
		return runLevelError(ChildInputArtifactMissingCode, fmt.Sprintf(
			"an artifact this run received names no object of this deployment's store: %s", node.appMessage()))
	}
	return runLevelError(ChildInputUnavailableCode, fmt.Sprintf(
		"the %d artifacts this run received could not be claimed in object storage before its first task: %v",
		len(s.plan.InputArtifacts), err))
}

// namedInputs are the artifact inputs a task's definition names: the ones its
// templates read, and the one its fan-out reads the lines of.
func namedInputs(task *orkstackv1.Task) []string {
	names := task.GetInputArtifacts()
	if art := task.GetEach().GetSource().GetArtifact(); art.GetProducer() == dsl.InputsProducer {
		names = append(slices.Clone(names), art.GetName())
	}
	return names
}

// claimContext is the local-activity context of a claim, bounded by the
// plan's pinned claim policy. A plan that passes artifacts and pins none was
// built by no submit of this release: it is refused, not given a default.
func (s *runState) claimContext(ctx workflow.Context) (workflow.Context, ArtifactClaimPolicy, error) {
	policy := s.plan.ArtifactClaims
	if policy == nil || policy.TimeoutSeconds <= 0 || policy.MaxAttempts <= 0 || policy.ReserveBytesPerBind <= 0 {
		return nil, ArtifactClaimPolicy{}, fmt.Errorf("this run's plan passes artifacts but pins no claim policy; submit it again")
	}
	// Each attempt has the policy's timeout, and all of them together the
	// timeout times the attempts: a slow attempt does not spend the others.
	attempt := time.Duration(policy.TimeoutSeconds) * time.Second
	return workflow.WithLocalActivityOptions(ctx, workflow.LocalActivityOptions{
		StartToCloseTimeout:    attempt,
		ScheduleToCloseTimeout: attempt * time.Duration(policy.MaxAttempts),
		RetryPolicy: &temporal.RetryPolicy{
			MaximumAttempts:        policy.MaxAttempts,
			NonRetryableErrorTypes: []string{ChildInputArtifactMissingCode},
		},
	}), *policy, nil
}

// inputsUpstream is the reserved upstream _inputs a task is handed: exactly
// the received artifacts its definition names (Task.input_artifacts), under
// their input names, or nil.
func inputsUpstream(task *orkstackv1.Task, received []NamedArtifactPlan) *orkstackv1.UpstreamRef {
	names := task.GetInputArtifacts()
	if len(names) == 0 {
		return nil
	}
	ref := &orkstackv1.UpstreamRef{TaskId: dsl.InputsProducer}
	for _, name := range names {
		for _, r := range received {
			if r.Name == name {
				ref.Artifacts = append(ref.Artifacts, r.Artifact.Proto())
			}
		}
	}
	return ref
}

// resolveOutputArtifacts resolves the artifacts a succeeded run returns,
// without IO, from what it recorded: a producer's committed result, or one of
// the run's own received artifacts for _inputs. Each is renamed to its output
// and carries its source. A producer that recorded no such artifact fails the
// run OUTPUTS_ARTIFACT_MISSING.
func (s *runState) resolveOutputArtifacts() ([]*orkstackv1.NamedArtifact, *orkstackv1.Error) {
	returned := s.wf.GetOutputArtifacts()
	if len(returned) == 0 {
		return nil, nil
	}
	names := make([]string, 0, len(returned))
	for name := range returned {
		names = append(names, name)
	}
	sort.Strings(names)
	out := make([]*orkstackv1.NamedArtifact, 0, len(names))
	for _, name := range names {
		spec := returned[name]
		missing := func(why string) *orkstackv1.Error {
			return &orkstackv1.Error{
				Code:     OutputsArtifactMissingCode,
				Category: orkstackv1.FailureCategory_FAILURE_CATEGORY_CONTRACT_VIOLATION,
				Message: fmt.Sprintf("the run's tasks finished but its declared outputs could not be produced: "+
					"outputs.%s: %s", name, why),
				Details: "outputs." + name,
			}
		}
		if spec.GetTaskId() == dsl.InputsProducer {
			received, ok := s.receivedArtifact(spec.GetName())
			if !ok {
				return nil, missing(fmt.Sprintf("this run received no artifact %q", spec.GetName()))
			}
			received.Name = name
			received.Artifact.Name = name
			out = append(out, received.Proto())
			continue
		}
		var ref *orkstackv1.ArtifactRef
		for _, candidate := range s.results[spec.GetTaskId()].GetArtifacts() {
			if candidate.GetName() == spec.GetName() {
				ref = candidate
				break
			}
		}
		if ref == nil {
			return nil, missing(fmt.Sprintf("task %q recorded no artifact %q", spec.GetTaskId(), spec.GetName()))
		}
		artifact := ArtifactRefPlanOf(ref)
		artifact.Name = name
		if artifact.Metadata == nil {
			artifact.Metadata = map[string]string{}
		}
		// The source is the run that recorded it: a returned artifact of a
		// composed producer already names its own.
		if _, sourced := artifact.Metadata[ArtifactSourceRunKey]; !sourced {
			artifact.Metadata[ArtifactSourceRunKey] = s.runID
			artifact.Metadata[ArtifactSourceTaskKey] = spec.GetTaskId()
			artifact.Metadata[ArtifactSourceNameKey] = spec.GetName()
		}
		out = append(out, NamedArtifactPlan{Name: name, RunID: s.runID, TaskID: spec.GetTaskId(), Artifact: artifact}.Proto())
	}
	return out, nil
}

// receiveChildArtifacts is a composed task's half of a child's returned
// artifacts, once the child succeeded: claim every object for this run,
// then return them as the task's own artifacts, by output name. The error is
// the one that failed the task; nothing is recorded on an error.
func (s *runState) receiveChildArtifacts(ctx workflow.Context, id string, child *orkstackv1.Run) ([]*orkstackv1.ArtifactRef, error) {
	returned := child.GetOutputArtifacts()
	if len(returned) == 0 {
		return nil, nil
	}
	plans := make([]NamedArtifactPlan, 0, len(returned))
	refs := make([]*orkstackv1.ArtifactRef, 0, len(returned))
	for _, r := range returned {
		artifact := ArtifactRefPlanOf(r.GetArtifact())
		artifact.Name = r.GetName()
		plans = append(plans, NamedArtifactPlan{Name: r.GetName(), RunID: r.GetRunId(), TaskID: r.GetTaskId(), Artifact: artifact})
		refs = append(refs, artifact.Proto())
	}
	claimCtx, _, err := s.claimContext(ctx)
	if err != nil {
		return nil, s.failComposed(ctx, id, InvalidCompositionCode,
			orkstackv1.FailureCategory_FAILURE_CATEGORY_CONTRACT_VIOLATION,
			fmt.Sprintf("task %q: %v", id, err), "")
	}
	// Listed on the row before the claim, as passChildArtifacts does: a
	// claim that writes some markers and then fails still leaves this run's
	// archive listing every object it may have claimed.
	s.taskRuns[id].Artifacts = refs
	req := ArtifactClaimRequest{RunID: s.runID, Artifacts: plans}
	if err := workflow.ExecuteLocalActivity(claimCtx, ActivityClaimReturnedArtifacts, req).Get(ctx, nil); err != nil {
		node := categoryOf(err)
		if node.canceled || ctx.Err() != nil {
			return nil, err
		}
		cause := err.Error()
		if node.isApplication() {
			cause = node.appMessage()
		}
		return nil, s.failComposed(ctx, id, ChildOutputsUnclaimedCode,
			orkstackv1.FailureCategory_FAILURE_CATEGORY_INFRASTRUCTURE_ERROR,
			fmt.Sprintf("child run %s succeeded, but the %d artifacts it returned could not be claimed in "+
				"object storage for task %q: %s", child.GetId(), len(returned), id, cause), "")
	}
	return refs, nil
}

// ---------------------------------------------------------------- activities

// ArtifactStater is the slice of artifacts.Store the claims stat with.
type ArtifactStater interface {
	Stat(ctx context.Context, uri string) (artifacts.ArtifactRef, error)
}

// ObjectClaimer writes a run's markers on objects before its archive does:
// artifacts.BlobRunArchive.ClaimObjects.
type ObjectClaimer interface {
	ClaimObjects(ctx context.Context, runID string, objectIDs []string) error
}

// ArtifactClaimRequest is one claim: the run that claims, and what.
type ArtifactClaimRequest struct {
	RunID     string              `json:"run_id"`
	Artifacts []NamedArtifactPlan `json:"artifacts"`
}

// ArtifactClaimActivities owns the two claim local activities. Both need a
// claimer: a claim that cannot be written is never skipped.
type ArtifactClaimActivities struct {
	stat  ArtifactStater
	claim ObjectClaimer
}

// NewArtifactClaimActivities wires the claims to the store objects are
// stat'd in and the archive markers are written to.
func NewArtifactClaimActivities(stat ArtifactStater, claim ObjectClaimer) *ArtifactClaimActivities {
	return &ArtifactClaimActivities{stat: stat, claim: claim}
}

// StatAndClaimChildInputs stats every object a composed task passes and,
// only when every one is there, claims them for the parent. An object that
// is gone is a non-retryable ChildInputArtifactMissingCode naming it; any
// other failure is returned, for the local activity's retry policy.
func (a *ArtifactClaimActivities) StatAndClaimChildInputs(ctx context.Context, req ArtifactClaimRequest) error {
	var gone []string
	for _, received := range req.Artifacts {
		uri := received.Artifact.URI
		if _, err := a.stat.Stat(ctx, uri); err != nil {
			if errors.Is(err, artifacts.ErrNotFound) {
				gone = append(gone, fmt.Sprintf("%s (%s)", received.Name, uri))
				continue
			}
			return fmt.Errorf("stat %s (%s): %w", received.Name, uri, err)
		}
	}
	if len(gone) > 0 {
		return temporal.NewNonRetryableApplicationError(
			fmt.Sprintf("the artifacts %s are no longer in object storage", strings.Join(gone, ", ")),
			ChildInputArtifactMissingCode, nil)
	}
	return a.claimFor(ctx, req)
}

// ClaimReturnedArtifacts claims what a child returned, for its parent.
func (a *ArtifactClaimActivities) ClaimReturnedArtifacts(ctx context.Context, req ArtifactClaimRequest) error {
	return a.claimFor(ctx, req)
}

// ClaimReceivedArtifacts claims what a child received, for the child.
func (a *ArtifactClaimActivities) ClaimReceivedArtifacts(ctx context.Context, req ArtifactClaimRequest) error {
	return a.claimFor(ctx, req)
}

func (a *ArtifactClaimActivities) claimFor(ctx context.Context, req ArtifactClaimRequest) error {
	if a.claim == nil {
		return fmt.Errorf("no run archive is wired to write claim markers")
	}
	ids := make([]string, 0, len(req.Artifacts))
	for _, received := range req.Artifacts {
		id, ok := artifacts.ObjectIDFromURI(received.Artifact.URI)
		if !ok {
			return temporal.NewNonRetryableApplicationError(fmt.Sprintf(
				"%s (%s) names no object of this deployment's store", received.Name, received.Artifact.URI),
				ChildInputArtifactMissingCode, nil)
		}
		ids = append(ids, id)
	}
	sort.Strings(ids)
	return a.claim.ClaimObjects(ctx, req.RunID, ids)
}

// outputsDeclared is dsl.OutputsDeclared, the one reading of whether an
// Orkflow returns anything.
func outputsDeclared(wf *orkstackv1.Orkflow) bool { return dsl.OutputsDeclared(wf) }
