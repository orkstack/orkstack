package engine

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"

	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/dsl"
	"github.com/orkstack/orkstack/internal/task"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// Composed Orkflows (docs/DSL.md "Composition"): a task whose `uses:` names
// another Orkflow starts a CHILD RUN — Temporal's own ExecuteChildWorkflow of
// this same RunWorkflow interpreter, over the child's compiled definition —
// and waits for it. Everything the child needs was decided at the parent's
// submit and rides RunPlan.Children; workflow code renders the child's input,
// copies the run-tree limits onto the child's start plan, starts it, and reads
// its declared outputs.

const (
	// Reason codes on a composed task's TaskRun.error.
	ChildInputRenderFailedCode = "CHILD_INPUT_RENDER_FAILED" // ContractViolation
	ChildInputTooLargeCode     = "CHILD_INPUT_TOO_LARGE"     // ContractViolation
	ChildInputUnavailableCode  = "CHILD_INPUT_UNAVAILABLE"   // InfrastructureError
	ChildStartFailedCode       = "CHILD_START_FAILED"        // InfrastructureError
	ChildRunFailedCode         = "CHILD_RUN_FAILED"          // the child's own category
	ChildRunCanceledCode       = "CHILD_RUN_CANCELED"        // Cancelled; status CANCELED
	ChildRunTerminatedCode     = "CHILD_RUN_TERMINATED"      // InfrastructureError
	ChildValuesUnstoredCode    = "CHILD_VALUES_UNSTORED"     // InfrastructureError
	InvalidCompositionCode     = "INVALID_COMPOSITION"       // ContractViolation; hand-built only

	// Reason codes on Run.error when declared outputs cannot be produced.
	OutputsRenderFailedCode = "OUTPUTS_RENDER_FAILED" // ContractViolation
	OutputsTooLargeCode     = "OUTPUTS_TOO_LARGE"     // ContractViolation
	OutputsUnavailableCode  = "OUTPUTS_UNAVAILABLE"   // InfrastructureError

	// Local activities on the run worker. Internal: no user-facing surface
	// names them.
	ActivityRenderComposedValues = "RenderComposedValues"
	ActivityStoreComposedValues  = "StoreComposedValues"

	// The snapshot write's budget (workflow.go): 10 s per attempt, 3
	// attempts, 60 s end to end.
	composeAttemptTimeout = 10 * time.Second
	composeTimeout        = 60 * time.Second

	// MaxComposeDepth is the ceiling ORK_COMPOSE_MAX_DEPTH may lower and never
	// raise.
	MaxComposeDepth = 4

	// composeInlineValuesLimit is the size at or under which a composed
	// task's stored values also travel inline on its result, the bound an
	// Orker's values.json inlining uses.
	composeInlineValuesLimit = 4096

	// renderPrefixChildInput and renderPrefixOutputs are the two code
	// prefixes a render request carries.
	renderPrefixChildInput = "CHILD_INPUT"
	renderPrefixOutputs    = "OUTPUTS"
)

// MaxPlanBytes bounds one run's encoded plan: AGENTS.md §2.2's 256 KiB hard
// payload maximum, the constant task.MaxActivityInputBytes already spells.
// The plan is one payload of RunWorkflow's start and, for a child, of its
// parent's child start.
const MaxPlanBytes = task.MaxActivityInputBytes

// MaxRunIDLength is the longest id RunIDGenerator.NewRunID can issue:
// len("ork-") + 19 (int64 nanoseconds) + len("-") + 20 (uint64) = 44.
const MaxRunIDLength = 44

// RunOriginKindParentRun is the plan-side spelling of a child run's origin
// kind: the enum's proto name, as RunOrigin.Kind holds it.
var RunOriginKindParentRun = orkstackv1.RunOriginKind_RUN_ORIGIN_KIND_PARENT_RUN.String()

// ChildRunPlan is one child run, planned at the parent's submit. Plain Go for
// the reason RunPlan is; the definition travels as protojson bytes because
// encoding/json cannot round-trip google.protobuf.Struct.
type ChildRunPlan struct {
	// RunID is minted by the API's RunIDGenerator after every refusal passed.
	// Never derived from the parent's id, never parsed (AGENTS.md §7).
	RunID string `json:"run_id"`
	// Orkflow is the child's PLANNED definition (pools resolved by
	// PoolResolver.Plan) as compact protojson, with source_yaml cleared: the
	// source is not what runs, and the stored version keeps it.
	Orkflow json.RawMessage `json:"orkflow"`
	// Plan is the child's own plan — Routes, CommitRoot, Limits,
	// TransferBudgetSeconds, Telemetry (from the CHILD's labels), ImagePolicy
	// (the parent's selection: one for the tree), Awaits and Children. Origin
	// is NOT set here: the parent's run id is not minted when this is built,
	// so the parent's workflow code sets it when it starts the child.
	Plan RunPlan `json:"plan"`
	// The engine's boot answers (TemporalStarter.IndexAnswers) at submit, so
	// the child is stamped exactly as a top-level run is
	// (RunStartSearchAttributes).
	NamespaceIndexed bool `json:"namespace_indexed,omitempty"`
	OriginIndexed    bool `json:"origin_indexed,omitempty"`
}

// NewChildRunPlan builds one from a planned definition: clones it, clears
// source_yaml, marshals with protojson and compacts the bytes (protojson's
// output is deliberately unstable in its whitespace; compacted, the number of
// bytes a plan carries does not vary by build). RunID is left empty for the
// minting step.
func NewChildRunPlan(planned *orkstackv1.Orkflow, plan RunPlan, namespaceIndexed, originIndexed bool) (*ChildRunPlan, error) {
	if planned == nil {
		return nil, errors.New("child run plan: no definition")
	}
	clone := proto.Clone(planned).(*orkstackv1.Orkflow)
	clone.SourceYaml = ""
	raw, err := protojson.Marshal(clone)
	if err != nil {
		return nil, fmt.Errorf("child run plan: encode %s@%s: %w", planned.GetName(), planned.GetVersion(), err)
	}
	var compact bytes.Buffer
	if err := json.Compact(&compact, raw); err != nil {
		return nil, fmt.Errorf("child run plan: compact %s@%s: %w", planned.GetName(), planned.GetVersion(), err)
	}
	return &ChildRunPlan{
		Orkflow:          json.RawMessage(compact.Bytes()),
		Plan:             plan,
		NamespaceIndexed: namespaceIndexed,
		OriginIndexed:    originIndexed,
	}, nil
}

// Definition decodes Orkflow (protojson, DiscardUnknown). Pure; safe in
// workflow code.
func (c *ChildRunPlan) Definition() (*orkstackv1.Orkflow, error) {
	if c == nil || len(c.Orkflow) == 0 {
		return nil, errors.New("the child run plan carries no definition")
	}
	wf := &orkstackv1.Orkflow{}
	if err := (protojson.UnmarshalOptions{DiscardUnknown: true}).Unmarshal(c.Orkflow, wf); err != nil {
		return nil, fmt.Errorf("decode the planned child definition: %w", err)
	}
	return wf, nil
}

// ForEachChild visits every ChildRunPlan of plan's tree, depth first, in the
// declaration order of the definitions that compose them (order lists the
// composing task ids of plan's own definition). Children of a child are
// visited in their own definition's order, decoded from the child plan.
func ForEachChild(wf *orkstackv1.Orkflow, plan *RunPlan, visit func(taskID string, child *ChildRunPlan, def *orkstackv1.Orkflow) error) error {
	for _, t := range wf.GetTasks() {
		if t.GetOrkflow() == nil {
			continue
		}
		child := plan.Children[t.GetId()]
		if child == nil {
			continue
		}
		def, err := child.Definition()
		if err != nil {
			return err
		}
		if err := visit(t.GetId(), child, def); err != nil {
			return err
		}
		if err := ForEachChild(def, &child.Plan, visit); err != nil {
			return err
		}
	}
	return nil
}

// composeStartWithoutRender is a TEST-ONLY switch, the precedent of
// awaitDeadlineAfterEmit: TestComposedHistory_CatchesAChildStartWithoutItsRender
// flips it to prove the composed fixture can see a child started without its
// recorded input render. Never set in production code.
var composeStartWithoutRender = false

// childState is one composed task's life in this run; a pure function of the
// workflow input and recorded history, like pinState.
type childState struct {
	plan *ChildRunPlan       // s.plan.Children[id]; nil only for a hand-built plan
	wf   *orkstackv1.Orkflow // plan.Definition(), decoded once in newRunState
	ref  *orkstackv1.OrkflowRef
	err  error // why plan or wf is unusable → INVALID_COMPOSITION
}

// label is "name@version" as the parent pinned it.
func (c *childState) label() string { return dsl.ComposedLabel(c.ref) }

// newChildState decodes one composed task's planned child. err is set when
// the plan has no entry, its definition does not decode, or the decoded
// definition is not the one the task pinned.
func newChildState(plan *ChildRunPlan, t *orkstackv1.Task) *childState {
	c := &childState{plan: plan, ref: t.GetOrkflow()}
	if plan == nil {
		c.err = fmt.Errorf("no child run was planned for task %q (orkflow %s)", t.GetId(), c.label())
		return c
	}
	if plan.RunID == "" {
		c.err = fmt.Errorf("the child run planned for task %q (orkflow %s) has no run id", t.GetId(), c.label())
		return c
	}
	wf, err := plan.Definition()
	if err != nil {
		c.err = fmt.Errorf("task %q (orkflow %s): %w", t.GetId(), c.label(), err)
		return c
	}
	if wf.GetDefinitionDigest() != t.GetOrkflow().GetDefinitionDigest() {
		c.err = fmt.Errorf("task %q pins orkflow %s as %s, but the planned child definition is %s",
			t.GetId(), c.label(), t.GetOrkflow().GetDefinitionDigest(), wf.GetDefinitionDigest())
		return c
	}
	c.wf = wf
	return c
}

// childWorkflowOptions is the one place a child's start options are decided.
// Pure, so it is unit-tested directly and the replay payload test compares
// what it produced against the recorded command.
func childWorkflowOptions(parentTaskQueue string, c *ChildRunPlan,
	childWf *orkstackv1.Orkflow, childPlan RunPlan) workflow.ChildWorkflowOptions {
	options := workflow.ChildWorkflowOptions{
		WorkflowID: c.RunID,
		// The run channel the parent itself runs on (ork-tasks), read from
		// the parent's own start — never ORK_ORKER_QUEUE, which moves task
		// execution only (docs/DEPLOY.md §2).
		TaskQueue:             parentTaskQueue,
		WorkflowIDReusePolicy: enumspb.WORKFLOW_ID_REUSE_POLICY_REJECT_DUPLICATE,
		// A parent that closes WITHOUT running its own workflow code — a
		// terminate from outside OrkStack — asks its running children to
		// cancel, so their containers get SIGTERM and they end CANCELED
		// through their own code. Never TERMINATE (no workflow code would run
		// for the child either) and never ABANDON (a child would outlive the
		// run that accepted it, its outputs read by nobody).
		ParentClosePolicy: enumspb.PARENT_CLOSE_POLICY_REQUEST_CANCEL,
		// A cancelled parent waits until the child has actually ended, so
		// the parent's terminal snapshot never precedes its child's.
		WaitForCancellation: true,
		Memo:                RunStartMemo(childWf, childPlan),
		// No RetryPolicy (a retried child would reuse the run id), no
		// execution or run timeout (it would terminate the child without its
		// workflow code), no cron.
	}
	if attrs := RunStartSearchAttributes(childWf, childPlan, c.NamespaceIndexed, c.OriginIndexed); len(attrs) > 0 {
		options.TypedSearchAttributes = temporal.NewSearchAttributes(attrs...)
	}
	return options
}

// ChildWorkflowOptionsForTest exposes childWorkflowOptions to the package's
// external tests (TestChildWorkflowOptions, the composed replay payload
// test). Pure.
func ChildWorkflowOptionsForTest(parentTaskQueue string, c *ChildRunPlan, childWf *orkstackv1.Orkflow, childPlan RunPlan) workflow.ChildWorkflowOptions {
	return childWorkflowOptions(parentTaskQueue, c, childWf, childPlan)
}

// composeLocalContext is the local-activity options both compose activities
// run with: the snapshot write's budget.
func composeLocalContext(ctx workflow.Context) workflow.Context {
	return workflow.WithLocalActivityOptions(ctx, workflow.LocalActivityOptions{
		StartToCloseTimeout:    composeAttemptTimeout,
		ScheduleToCloseTimeout: composeTimeout,
		RetryPolicy:            &temporal.RetryPolicy{MaximumAttempts: 3},
	})
}

// renderRequest builds one render: templates as JSON, the run input, and
// every upstream the templates read whose values are addressable.
func (s *runState) renderRequest(label, prefix string, templates any, readable map[string]bool) (ComposeRenderRequest, error) {
	encodedTemplates, err := json.Marshal(templates)
	if err != nil {
		return ComposeRenderRequest{}, fmt.Errorf("encode templates: %w", err)
	}
	encodedInputs, err := json.Marshal(s.inputs)
	if err != nil {
		return ComposeRenderRequest{}, fmt.Errorf("encode run input: %w", err)
	}
	ids, err := dsl.ReferencedUpstreamIn(templates)
	if err != nil {
		return ComposeRenderRequest{}, err
	}
	req := ComposeRenderRequest{
		Label:      label,
		CodePrefix: prefix,
		Templates:  encodedTemplates,
		Inputs:     encodedInputs,
		MaxBytes:   s.plan.Limits.Proto().GetValuesMaxBytes(),
	}
	for _, id := range ids {
		if readable != nil && !readable[id] {
			continue
		}
		ref := s.results[id].GetValuesRef()
		if ref == nil || ref.GetUri() == "" {
			continue
		}
		req.Upstream = append(req.Upstream, ComposeUpstream{TaskID: id, URI: ref.GetUri(), SHA256: ref.GetSha256()})
	}
	return req, nil
}

// renderFailure reads a failed render's reason code and category: the
// activity's ActivityFailureDetail when it carries one, else <prefix>_UNAVAILABLE
// (the store unreachable past the budget, a timeout).
func renderFailure(prefix string, err error) (code string, category orkstackv1.FailureCategory, message string) {
	var applicationErr *temporal.ApplicationError
	var detail *orkstackv1.ActivityFailureDetail
	if errors.As(err, &applicationErr) && applicationErr.Details(&detail) == nil && detail.GetReasonCode() != "" {
		category = detail.GetCategory()
		if category == orkstackv1.FailureCategory_FAILURE_CATEGORY_UNSPECIFIED {
			category = categoryEnumFor(applicationErr.Type())
		}
		return detail.GetReasonCode(), category, applicationErr.Message()
	}
	message = err.Error()
	if errors.As(err, &applicationErr) {
		message = applicationErr.Message()
	}
	return prefix + "_UNAVAILABLE", orkstackv1.FailureCategory_FAILURE_CATEGORY_INFRASTRUCTURE_ERROR, message
}

// categoryTypeFor is categoryEnumFor's inverse: the ApplicationError type a
// category is raised with, so a grandparent reads it back by the same table.
func categoryTypeFor(category orkstackv1.FailureCategory) string {
	switch category {
	case orkstackv1.FailureCategory_FAILURE_CATEGORY_ACTIVITY_FAILED:
		return task.CategoryActivityFailed
	case orkstackv1.FailureCategory_FAILURE_CATEGORY_INFRASTRUCTURE_ERROR:
		return task.CategoryInfrastructureError
	case orkstackv1.FailureCategory_FAILURE_CATEGORY_CONTRACT_VIOLATION:
		return task.CategoryContractViolation
	case orkstackv1.FailureCategory_FAILURE_CATEGORY_TIMED_OUT:
		return task.CategoryTimedOut
	case orkstackv1.FailureCategory_FAILURE_CATEGORY_CANCELLED:
		return task.CategoryCancelled
	default:
		return ""
	}
}

// failComposed records a composed task's own failure before or instead of a
// child: FAILED (or CANCELED for the CANCELLED category), the error, a
// TASK_FAILED event; the returned error is non-retryable and carries the
// category's type so fail() and a grandparent read it.
func (s *runState) failComposed(ctx workflow.Context, id, code string, category orkstackv1.FailureCategory, message, details string) error {
	taskRun := s.taskRuns[id]
	taskRun.Status = orkstackv1.Status_STATUS_FAILED
	taskRun.FinishedAt = timestamppb.New(workflow.Now(ctx))
	if details == "" {
		details = fmt.Sprintf("task %q", id)
	}
	taskRun.Error = &orkstackv1.Error{
		Code:      code,
		Message:   message,
		Details:   details,
		Category:  category,
		Retryable: false,
	}
	if err := s.emit(ctx, orkstackv1.EventType_EVENT_TYPE_TASK_FAILED, id,
		taskRun.GetStatus(), message); err != nil {
		workflow.GetLogger(ctx).Error("emit task failure failed",
			"run_id", s.runID, "task_id", id, "error", err)
	}
	return temporal.NewNonRetryableApplicationError(message, categoryTypeFor(category), nil)
}

// childStartPlan is the plan a child run starts with: the planned one, with
// exactly its origin set and the run-tree limits copied here. The limits
// travel once per tree, on the top-level plan; a recorded parent's plan has
// none, so its child starts are byte-identical, and a grandchild inherits
// through its parent's copy. Never on the nested plan the parent carries:
// that would cost every composed task its bytes inside the 256 KiB plan.
func (s *runState) childStartPlan(planned *ChildRunPlan) RunPlan {
	childPlan := planned.Plan
	childPlan.Origin = &RunOrigin{Kind: RunOriginKindParentRun, ID: s.runID}
	childPlan.History = s.plan.History
	childPlan.Continuation = s.plan.Continuation
	childPlan.InFlight = s.plan.InFlight
	childPlan.SnapshotOverlay = s.plan.SnapshotOverlay
	childPlan.PinCreationTimeoutSeconds = s.plan.PinCreationTimeoutSeconds
	// A child runs in its parent's namespace, so its fairness is the
	// parent's, copied here rather than carried by every nested plan.
	childPlan.Fairness = s.plan.Fairness
	return childPlan
}

// startChild requests one composed task's child run and returns its result
// future once the start is confirmed. It is called from the main loop for a
// ready task classify decided to run.
func (s *runState) startChild(ctx, runCtx workflow.Context, id string) (workflow.Future, error) {
	c := s.children[id]
	if c.err != nil {
		return nil, s.failComposed(ctx, id, InvalidCompositionCode,
			orkstackv1.FailureCategory_FAILURE_CATEGORY_CONTRACT_VIOLATION,
			fmt.Sprintf("task %q cannot start its child run: %v", id, c.err), "")
	}
	t := s.tasks[id]
	taskRun := s.taskRuns[id]

	// 2. Requested.
	taskRun.Status = orkstackv1.Status_STATUS_RUNNING
	taskRun.ScheduledAt = timestamppb.New(workflow.Now(ctx))
	if err := s.emit(ctx, orkstackv1.EventType_EVENT_TYPE_TASK_SCHEDULED, id,
		orkstackv1.Status_STATUS_RUNNING, "child run requested for orkflow "+c.label()); err != nil {
		return nil, err
	}

	// 3. The child's input, rendered by one local activity whose result is
	// recorded as a marker: a replay reads the marker and never re-renders.
	needs := s.closureOf(id)
	closure := make(map[string]bool, len(needs))
	for _, need := range needs {
		closure[need] = true
	}
	var with any = map[string]any{}
	if t.GetWith() != nil {
		with = t.GetWith().AsMap()
	}
	req, err := s.renderRequest(fmt.Sprintf("task %q: with", id), renderPrefixChildInput, with, closure)
	if err != nil {
		return nil, s.failComposed(ctx, id, ChildInputRenderFailedCode,
			orkstackv1.FailureCategory_FAILURE_CATEGORY_CONTRACT_VIOLATION,
			fmt.Sprintf("task %q: the child run's input could not be rendered: %v", id, err), "")
	}
	var rendered json.RawMessage
	if composeStartWithoutRender {
		// TEST-ONLY: the raw with:, never rendered and never recorded.
		rendered = req.Templates
	} else {
		localCtx := composeLocalContext(ctx)
		if err := workflow.ExecuteLocalActivity(localCtx, ActivityRenderComposedValues, req).Get(localCtx, &rendered); err != nil {
			code, category, message := renderFailure(renderPrefixChildInput, err)
			return nil, s.failComposed(ctx, id, code, category,
				fmt.Sprintf("task %q: the child run's input could not be rendered: %s", id, message), "")
		}
	}
	childInput, err := payloadOfJSON(rendered)
	if err != nil {
		return nil, s.failComposed(ctx, id, ChildInputRenderFailedCode,
			orkstackv1.FailureCategory_FAILURE_CATEGORY_CONTRACT_VIOLATION,
			fmt.Sprintf("task %q: the child run's input could not be rendered: %v", id, err), "")
	}

	// 4. The child's plan: the planned one, with exactly its origin set and
	// the run-tree limits copied (childStartPlan).
	childPlan := s.childStartPlan(c.plan)

	// 5. Start it, and wait for the start to be confirmed.
	options := childWorkflowOptions(workflow.GetInfo(ctx).TaskQueueName, c.plan, c.wf, childPlan)
	if limit := s.historyLimit(); limit != nil {
		s.noteCommand(ctx, 1, limit.Reserve.ChildStartBytes)
		s.chargeTaskDispatch()
	}
	future := workflow.ExecuteChildWorkflow(workflow.WithChildOptions(runCtx, options),
		RunWorkflowType, c.plan.RunID, c.wf, childInput, childPlan)
	if err := future.GetChildWorkflowExecution().Get(ctx, nil); err != nil {
		// 6. Refused. child.run_id stays empty: a refused id names no run.
		if temporal.IsCanceledError(err) {
			message := fmt.Sprintf("run ended before child run %s started", c.plan.RunID)
			taskRun.Status = orkstackv1.Status_STATUS_CANCELED
			taskRun.FinishedAt = timestamppb.New(workflow.Now(ctx))
			taskRun.Error = &orkstackv1.Error{
				Code:     ChildRunCanceledCode,
				Message:  message,
				Details:  fmt.Sprintf("task %q", id),
				Category: orkstackv1.FailureCategory_FAILURE_CATEGORY_CANCELLED,
			}
			if emitErr := s.emit(ctx, orkstackv1.EventType_EVENT_TYPE_TASK_FAILED, id,
				orkstackv1.Status_STATUS_CANCELED, message); emitErr != nil {
				workflow.GetLogger(ctx).Error("emit task failure failed",
					"run_id", s.runID, "task_id", id, "error", emitErr)
			}
			return nil, &childCanceledError{message: message, cause: err}
		}
		return nil, s.failComposed(ctx, id, ChildStartFailedCode,
			orkstackv1.FailureCategory_FAILURE_CATEGORY_INFRASTRUCTURE_ERROR,
			fmt.Sprintf("child run %s of orkflow %s could not be started: %v", c.plan.RunID, c.label(), err), "")
	}

	// 7. Started.
	taskRun.StartedAt = timestamppb.New(workflow.Now(ctx))
	taskRun.Child.RunId = c.plan.RunID
	if err := s.emit(ctx, orkstackv1.EventType_EVENT_TYPE_TASK_STARTED, id,
		orkstackv1.Status_STATUS_RUNNING, fmt.Sprintf("child run %s started", c.plan.RunID)); err != nil {
		return nil, err
	}
	return future, nil
}

// payloadOfJSON decodes a rendered JSON object (UseNumber, so an integer
// keeps its digits until structpb stores it) into a Payload.
func payloadOfJSON(raw []byte) (*orkstackv1.Payload, error) {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	var object map[string]any
	if err := decoder.Decode(&object); err != nil {
		return nil, fmt.Errorf("the rendered input is not a JSON object: %w", err)
	}
	if len(object) == 0 {
		return &orkstackv1.Payload{}, nil
	}
	data, err := structpb.NewStruct(object)
	if err != nil {
		return nil, fmt.Errorf("encode the rendered input: %w", err)
	}
	return &orkstackv1.Payload{Data: data}, nil
}

// childCanceledError is a composed task's cancellation as fail() reads it:
// Error() is the sentence naming the child, Unwrap() the cancellation, so
// temporal.IsCanceledError holds and the run records RUN_CANCELED.
type childCanceledError struct {
	message string
	cause   error
}

func (e *childCanceledError) Error() string { return e.message }
func (e *childCanceledError) Unwrap() error { return e.cause }

// canonicalValuesJSON renders a composed task's values: keys sorted, no HTML
// escaping, no trailing newline — the bytes stored as its values.json.
func canonicalValuesJSON(values map[string]any) ([]byte, error) {
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(values); err != nil {
		return nil, err
	}
	return bytes.TrimSuffix(buf.Bytes(), []byte("\n")), nil
}

// collectChild records a composed task's outcome from its child's result.
func (s *runState) collectChild(ctx workflow.Context, id string, future workflow.Future) error {
	c := s.children[id]
	taskRun := s.taskRuns[id]
	result := &orkstackv1.Run{}
	if err := future.Get(ctx, result); err != nil {
		return s.recordChildFailure(ctx, id, err)
	}

	// The child's declared outputs, or {}: the leaf aggregation never
	// crosses a composition boundary.
	values := map[string]any{}
	if len(c.wf.GetOutputs()) > 0 {
		values = payloadMap(result.GetOutput())
	}
	raw, err := canonicalValuesJSON(values)
	if err != nil {
		return s.failComposed(ctx, id, ChildValuesUnstoredCode,
			orkstackv1.FailureCategory_FAILURE_CATEGORY_INFRASTRUCTURE_ERROR,
			fmt.Sprintf("child run %s succeeded, but its outputs could not be stored as task %q's values: %v",
				c.plan.RunID, id, err), "")
	}
	localCtx := composeLocalContext(ctx)
	var ref *orkstackv1.ArtifactRef
	if err := workflow.ExecuteLocalActivity(localCtx, ActivityStoreComposedValues, ComposeStoreRequest{
		Orkflow: s.wf.GetName(),
		RunID:   s.runID,
		TaskID:  id,
		Values:  raw,
	}).Get(localCtx, &ref); err != nil || ref == nil {
		if err == nil {
			err = errors.New("the store returned no reference")
		}
		var applicationErr *temporal.ApplicationError
		cause := err.Error()
		if errors.As(err, &applicationErr) {
			cause = applicationErr.Message()
		}
		return s.failComposed(ctx, id, ChildValuesUnstoredCode,
			orkstackv1.FailureCategory_FAILURE_CATEGORY_INFRASTRUCTURE_ERROR,
			fmt.Sprintf("child run %s succeeded, but its outputs could not be stored as task %q's values: %s",
				c.plan.RunID, id, cause), "")
	}
	output, err := payloadOf(values)
	if err != nil {
		return s.failComposed(ctx, id, ChildValuesUnstoredCode,
			orkstackv1.FailureCategory_FAILURE_CATEGORY_INFRASTRUCTURE_ERROR,
			fmt.Sprintf("child run %s succeeded, but its outputs could not be recorded as task %q's values: %v",
				c.plan.RunID, id, err), "")
	}
	taskRun.Status = orkstackv1.Status_STATUS_SUCCEEDED
	taskRun.FinishedAt = timestamppb.New(workflow.Now(ctx))
	taskRun.Output = output
	taskRun.ValuesRef = ref
	inlined := len(raw) <= composeInlineValuesLimit
	stored := &orkstackv1.ExecuteTaskResult{TaskId: id, ValuesRef: ref, ValuesInlined: inlined}
	if inlined {
		stored.ValuesJson = raw
	}
	s.results[id] = stored
	s.outputs[id] = values
	return s.emit(ctx, orkstackv1.EventType_EVENT_TYPE_TASK_SUCCEEDED, id,
		orkstackv1.Status_STATUS_SUCCEEDED, fmt.Sprintf("child run %s succeeded", c.plan.RunID))
}

// childFailure maps a child run's failure to the composed task's status, code
// and category (docs/DSL.md "Composition").
func childFailure(err error) (status orkstackv1.Status, code string, category orkstackv1.FailureCategory) {
	var terminated *temporal.TerminatedError
	var applicationErr *temporal.ApplicationError
	var timeoutErr *temporal.TimeoutError
	switch {
	case temporal.IsCanceledError(err):
		return orkstackv1.Status_STATUS_CANCELED, ChildRunCanceledCode, orkstackv1.FailureCategory_FAILURE_CATEGORY_CANCELLED
	case errors.As(err, &terminated):
		return orkstackv1.Status_STATUS_FAILED, ChildRunTerminatedCode, orkstackv1.FailureCategory_FAILURE_CATEGORY_INFRASTRUCTURE_ERROR
	}
	if errors.As(err, &applicationErr) {
		if category := categoryEnumFor(applicationErr.Type()); category != orkstackv1.FailureCategory_FAILURE_CATEGORY_UNSPECIFIED {
			return orkstackv1.Status_STATUS_FAILED, ChildRunFailedCode, category
		}
		switch applicationErr.Type() {
		case RunHistoryLimitCode:
			// The child stopped before its history limit: a fact about the
			// platform, not the workload.
			return orkstackv1.Status_STATUS_FAILED, ChildRunFailedCode, orkstackv1.FailureCategory_FAILURE_CATEGORY_INFRASTRUCTURE_ERROR
		case RunStateUnavailableCode, RunDefinitionUnavailableCode:
			// The child's saved state, or its definition, could not be read
			// in time: the platform's, like the history limit. (A child's
			// definition travels in its parent's plan, so only a hand-built
			// plan reaches the second.)
			return orkstackv1.Status_STATUS_FAILED, ChildRunFailedCode, orkstackv1.FailureCategory_FAILURE_CATEGORY_INFRASTRUCTURE_ERROR
		case RunStateCorruptCode, RunStateUnsupportedCode, RunDefinitionCorruptCode:
			return orkstackv1.Status_STATUS_FAILED, ChildRunFailedCode, orkstackv1.FailureCategory_FAILURE_CATEGORY_CONTRACT_VIOLATION
		case AwaitTimedOutCode:
			return orkstackv1.Status_STATUS_FAILED, ChildRunFailedCode, orkstackv1.FailureCategory_FAILURE_CATEGORY_TIMED_OUT
		case AwaitRejectedCode:
			return orkstackv1.Status_STATUS_FAILED, ChildRunFailedCode, orkstackv1.FailureCategory_FAILURE_CATEGORY_ACTIVITY_FAILED
		}
	}
	if errors.As(err, &timeoutErr) {
		if timeoutErr.TimeoutType() == enumspb.TIMEOUT_TYPE_HEARTBEAT {
			return orkstackv1.Status_STATUS_FAILED, ChildRunFailedCode, orkstackv1.FailureCategory_FAILURE_CATEGORY_INFRASTRUCTURE_ERROR
		}
		return orkstackv1.Status_STATUS_FAILED, ChildRunFailedCode, orkstackv1.FailureCategory_FAILURE_CATEGORY_TIMED_OUT
	}
	return orkstackv1.Status_STATUS_FAILED, ChildRunFailedCode, orkstackv1.FailureCategory_FAILURE_CATEGORY_UNSPECIFIED
}

// childInnerMessage is the child's own words for its failure: the first
// ApplicationError's Message() in the chain, else the chain's text below the
// SDK's child-workflow wrapper — never that wrapper, which names Temporal
// concepts on a user-facing field.
func childInnerMessage(err error) string {
	var applicationErr *temporal.ApplicationError
	if errors.As(err, &applicationErr) && applicationErr.Message() != "" {
		return applicationErr.Message()
	}
	var childErr *temporal.ChildWorkflowExecutionError
	if errors.As(err, &childErr) {
		if inner := errors.Unwrap(childErr); inner != nil {
			return inner.Error()
		}
		return "the child run failed"
	}
	return err.Error()
}

// childReasonCode is the reason code an ActivityFailureDetail in the chain
// carries, or "".
func childReasonCode(err error) string {
	var applicationErr *temporal.ApplicationError
	if !errors.As(err, &applicationErr) {
		return ""
	}
	var detail *orkstackv1.ActivityFailureDetail
	if applicationErr.Details(&detail) == nil {
		return detail.GetReasonCode()
	}
	return ""
}

// recordChildFailure maps a child's terminal error onto its composed task by
// the failure table and returns the error the run fails with.
func (s *runState) recordChildFailure(ctx workflow.Context, id string, cause error) error {
	c := s.children[id]
	taskRun := s.taskRuns[id]
	status, code, category := childFailure(cause)
	who := fmt.Sprintf("child run %s (%s)", c.plan.RunID, c.label())
	var message string
	switch code {
	case ChildRunCanceledCode:
		message = who + " was canceled"
	case ChildRunTerminatedCode:
		message = who + " was terminated outside OrkStack"
	default:
		message = who + " failed: " + childInnerMessage(cause)
	}
	details := fmt.Sprintf("child run %s; open it with orkctl run get %s", c.plan.RunID, c.plan.RunID)
	if reason := childReasonCode(cause); reason != "" {
		details += "; reason " + reason
	}
	taskRun.Status = status
	taskRun.FinishedAt = timestamppb.New(workflow.Now(ctx))
	taskRun.Error = &orkstackv1.Error{
		Code:      code,
		Message:   message,
		Details:   details,
		Category:  category,
		Retryable: false,
	}
	if err := s.emit(ctx, orkstackv1.EventType_EVENT_TYPE_TASK_FAILED, id, status, message); err != nil {
		workflow.GetLogger(ctx).Error("emit task failure failed",
			"run_id", s.runID, "task_id", id, "error", err)
	}
	if status == orkstackv1.Status_STATUS_CANCELED {
		return &childCanceledError{message: message, cause: cause}
	}
	return temporal.NewNonRetryableApplicationError(message, categoryTypeFor(category), nil)
}

// renderOutputs renders the Orkflow's declared outputs once, at success. A
// failure is returned with its code and category.
func (s *runState) renderOutputs(ctx workflow.Context) (*orkstackv1.Payload, *orkstackv1.Error) {
	outputs := s.wf.GetOutputs()
	fail := func(code string, category orkstackv1.FailureCategory, cause string) *orkstackv1.Error {
		failure := &orkstackv1.Error{
			Code:     code,
			Category: category,
			Message:  "the run's tasks finished but its declared outputs could not be produced: " + cause,
		}
		if strings.HasPrefix(cause, "outputs.") {
			if field, _, ok := strings.Cut(cause, ":"); ok {
				failure.Details = field
			}
		}
		return failure
	}
	req, err := s.renderRequest("outputs", renderPrefixOutputs, outputs, nil)
	if err != nil {
		return nil, fail(OutputsRenderFailedCode, orkstackv1.FailureCategory_FAILURE_CATEGORY_CONTRACT_VIOLATION, err.Error())
	}
	localCtx := composeLocalContext(ctx)
	var rendered json.RawMessage
	if err := workflow.ExecuteLocalActivity(localCtx, ActivityRenderComposedValues, req).Get(localCtx, &rendered); err != nil {
		code, category, message := renderFailure(renderPrefixOutputs, err)
		return nil, fail(code, category, message)
	}
	output, err := payloadOfJSON(rendered)
	if err != nil {
		return nil, fail(OutputsRenderFailedCode, orkstackv1.FailureCategory_FAILURE_CATEGORY_CONTRACT_VIOLATION, err.Error())
	}
	return output, nil
}

// failOutputs ends a run whose tasks all finished but whose declared outputs
// could not be produced: FAILED, never SUCCEEDED.
func (s *runState) failOutputs(ctx workflow.Context, failure *orkstackv1.Error) (*orkstackv1.Run, error) {
	s.status = orkstackv1.Status_STATUS_FAILED
	s.finished = timestamppb.New(workflow.Now(ctx))
	s.runError = failure
	if err := s.emit(ctx, orkstackv1.EventType_EVENT_TYPE_RUN_FAILED, "",
		orkstackv1.Status_STATUS_FAILED, failure.GetMessage()); err != nil {
		workflow.GetLogger(ctx).Error("emit terminal run event failed",
			"run_id", s.runID, "event", "RUN_FAILED", "error", err)
	}
	return s.result(), temporal.NewNonRetryableApplicationError(failure.GetMessage(),
		categoryTypeFor(failure.GetCategory()), nil)
}

// isChildRun reports whether this run was started by a parent's composed
// task.
func (s *runState) isChildRun() bool {
	return s.plan.Origin != nil && s.plan.Origin.Kind == RunOriginKindParentRun
}

// result is the workflow's own return value, and it is O(1) by construction:
// a top-level run returns its coordinates, status and timestamps only
// (compactRunResult), and a child run the compact result its parent reads
// (compactChildResult). Nothing reads a top-level result — the reader
// rebuilds a run from its events and snapshots, and a schedule fire waits on
// the future with a nil target — so returning every task row only put the
// run's completion at risk: past AGENTS.md §2.2's 256 KiB at about 170
// tasks, and refused outright at Temporal's blob limit, after RUN_SUCCEEDED
// had already been published.
func (s *runState) result() *orkstackv1.Run {
	if s.isChildRun() {
		child := compactChildResult(s.snapshot())
		// The parent reads Output only for declared outputs (collectChild); an
		// undeclared leaf aggregation is unbounded and read by nobody.
		if len(s.wf.GetOutputs()) == 0 {
			child.Output = nil
		}
		return child
	}
	return compactRunResult(s.snapshot())
}

// compactRunResult is what a top-level run returns: its coordinates, status,
// origin and timestamps, with no Input, Output, TaskRuns or Error. Every one
// of those is on the run's snapshots, which is where every reader takes them
// from. Pure.
func compactRunResult(run *orkstackv1.Run) *orkstackv1.Run {
	return &orkstackv1.Run{
		Id:                      run.GetId(),
		Namespace:               run.GetNamespace(),
		OrkflowName:             run.GetOrkflowName(),
		OrkflowVersion:          run.GetOrkflowVersion(),
		OrkflowDefinitionDigest: run.GetOrkflowDefinitionDigest(),
		Status:                  run.GetStatus(),
		Origin:                  run.GetOrigin(),
		CreatedAt:               run.GetCreatedAt(),
		StartedAt:               run.GetStartedAt(),
		FinishedAt:              run.GetFinishedAt(),
	}
}

// compactChildResult is what a child run returns: only the coordinates, the
// status, the output and the origin — what the parent reads, bounded by
// values_max_bytes, rather than every task row. Nothing else reads a workflow
// result: the reader rebuilds a run from its events and snapshots.
func compactChildResult(run *orkstackv1.Run) *orkstackv1.Run {
	return &orkstackv1.Run{
		Id:                      run.GetId(),
		Namespace:               run.GetNamespace(),
		OrkflowName:             run.GetOrkflowName(),
		OrkflowVersion:          run.GetOrkflowVersion(),
		OrkflowDefinitionDigest: run.GetOrkflowDefinitionDigest(),
		Status:                  run.GetStatus(),
		Output:                  run.GetOutput(),
		Origin:                  run.GetOrigin(),
	}
}

// ---------------------------------------------------------------- activities

// ComposeStore is the slice of artifacts.Store the two activities need.
type ComposeStore interface {
	Get(ctx context.Context, uri string) (io.ReadCloser, error)
	Put(ctx context.Context, name string, r io.Reader, opts artifacts.PutOptions) (artifacts.ArtifactRef, error)
}

// ComposeActivities owns the two local activities a composed task and
// declared outputs use, over the store the run's values.json objects live in.
type ComposeActivities struct{ store ComposeStore }

// NewComposeActivities builds them over store.
func NewComposeActivities(store ComposeStore) *ComposeActivities {
	return &ComposeActivities{store: store}
}

// ComposeRenderRequest is plain JSON (encoding/json), never a proto message
// nesting a Struct — the reason RunPlan is plain Go.
type ComposeRenderRequest struct {
	Label      string            `json:"label"`
	CodePrefix string            `json:"code_prefix"` // "CHILD_INPUT" | "OUTPUTS"
	Templates  json.RawMessage   `json:"templates"`
	Inputs     json.RawMessage   `json:"inputs"`
	Upstream   []ComposeUpstream `json:"upstream"`
	MaxBytes   int64             `json:"max_bytes"`
}

// ComposeUpstream is one upstream values.json a render reads.
type ComposeUpstream struct {
	TaskID string `json:"task_id"`
	URI    string `json:"uri"`
	SHA256 string `json:"sha256"`
}

// composeActivityError is a non-retryable refusal with the reason-code detail
// the workflow reads.
func composeActivityError(code string, category orkstackv1.FailureCategory, message string) error {
	return temporal.NewNonRetryableApplicationError(message, categoryTypeFor(category), nil,
		&orkstackv1.ActivityFailureDetail{ReasonCode: code, Category: category})
}

// decodeJSONNumbers decodes one JSON value with UseNumber.
func decodeJSONNumbers(raw []byte) (any, error) {
	if len(bytes.TrimSpace(raw)) == 0 {
		return nil, nil
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	var value any
	if err := decoder.Decode(&value); err != nil {
		return nil, err
	}
	return value, nil
}

// RenderComposedValues reads each upstream values.json (at most MaxBytes+1
// bytes; bytes whose sha256 is not the recorded one are refused — the store
// contradicts the commit, non-retryable <prefix>_UNAVAILABLE), decodes with
// json.Decoder.UseNumber, and returns dsl.RenderValues(...) encoded as a JSON
// object. A render error is NonRetryable(msg, ContractViolation,
// ActivityFailureDetail{ReasonCode: <prefix>_RENDER_FAILED}); a result over
// MaxBytes is <prefix>_TOO_LARGE likewise; a read error is returned as-is and
// retried by the local activity's policy.
func (a *ComposeActivities) RenderComposedValues(ctx context.Context, req ComposeRenderRequest) (json.RawMessage, error) {
	prefix := req.CodePrefix
	if prefix == "" {
		prefix = renderPrefixChildInput
	}
	unavailable := func(message string) error {
		return composeActivityError(prefix+"_UNAVAILABLE",
			orkstackv1.FailureCategory_FAILURE_CATEGORY_INFRASTRUCTURE_ERROR, message)
	}
	maxBytes := req.MaxBytes
	if maxBytes <= 0 {
		maxBytes = DefaultValuesMaxBytes
	}
	outputs := make(map[string]map[string]any, len(req.Upstream))
	for _, up := range req.Upstream {
		if a == nil || a.store == nil {
			return nil, unavailable(fmt.Sprintf("%s: this engine has no artifact store to read task %q's values from",
				req.Label, up.TaskID))
		}
		reader, err := a.store.Get(ctx, up.URI)
		if err != nil {
			return nil, fmt.Errorf("%s: read task %q's values.json at %s: %w", req.Label, up.TaskID, up.URI, err)
		}
		raw, err := io.ReadAll(io.LimitReader(reader, maxBytes+1))
		_ = reader.Close()
		if err != nil {
			return nil, fmt.Errorf("%s: read task %q's values.json at %s: %w", req.Label, up.TaskID, up.URI, err)
		}
		if int64(len(raw)) > maxBytes {
			return nil, unavailable(fmt.Sprintf("%s: task %q's values.json at %s is over the %d-byte values limit",
				req.Label, up.TaskID, up.URI, maxBytes))
		}
		sum := sha256.Sum256(raw)
		if up.SHA256 != "" && hex.EncodeToString(sum[:]) != up.SHA256 {
			return nil, unavailable(fmt.Sprintf("%s: task %q's values.json at %s hashes to %s but its record says %s",
				req.Label, up.TaskID, up.URI, hex.EncodeToString(sum[:]), up.SHA256))
		}
		value, err := decodeJSONNumbers(raw)
		if err != nil {
			return nil, unavailable(fmt.Sprintf("%s: task %q's values.json is not JSON: %v", req.Label, up.TaskID, err))
		}
		object, ok := value.(map[string]any)
		if !ok && value != nil {
			return nil, unavailable(fmt.Sprintf("%s: task %q's values.json is not a JSON object", req.Label, up.TaskID))
		}
		if object == nil {
			object = map[string]any{}
		}
		outputs[up.TaskID] = object
	}
	templates, err := decodeJSONNumbers(req.Templates)
	if err != nil {
		return nil, composeActivityError(prefix+"_RENDER_FAILED",
			orkstackv1.FailureCategory_FAILURE_CATEGORY_CONTRACT_VIOLATION,
			fmt.Sprintf("%s: the templates are not JSON: %v", req.Label, err))
	}
	if templates == nil {
		templates = map[string]any{}
	}
	inputValue, err := decodeJSONNumbers(req.Inputs)
	if err != nil {
		return nil, composeActivityError(prefix+"_RENDER_FAILED",
			orkstackv1.FailureCategory_FAILURE_CATEGORY_CONTRACT_VIOLATION,
			fmt.Sprintf("%s: the run input is not JSON: %v", req.Label, err))
	}
	inputs, _ := inputValue.(map[string]any)
	rendered, err := dsl.RenderValues(req.Label, templates, inputs, outputs)
	if err != nil {
		return nil, composeActivityError(prefix+"_RENDER_FAILED",
			orkstackv1.FailureCategory_FAILURE_CATEGORY_CONTRACT_VIOLATION, err.Error())
	}
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(rendered); err != nil {
		return nil, composeActivityError(prefix+"_RENDER_FAILED",
			orkstackv1.FailureCategory_FAILURE_CATEGORY_CONTRACT_VIOLATION,
			fmt.Sprintf("%s: encode the rendered values: %v", req.Label, err))
	}
	out := bytes.TrimSuffix(buf.Bytes(), []byte("\n"))
	if int64(len(out)) > maxBytes {
		return nil, composeActivityError(prefix+"_TOO_LARGE",
			orkstackv1.FailureCategory_FAILURE_CATEGORY_CONTRACT_VIOLATION,
			fmt.Sprintf("%s: the rendered values are %d bytes, over the %d-byte values limit",
				req.Label, len(out), maxBytes))
	}
	return json.RawMessage(out), nil
}

// ComposeStoreRequest is one composed task's values, to be stored.
type ComposeStoreRequest struct {
	Orkflow string          `json:"orkflow"`
	RunID   string          `json:"run_id"`
	TaskID  string          `json:"task_id"`
	Values  json.RawMessage `json:"values"`
}

// StoreComposedValues writes Values as the task's values.json:
// store.Put(ctx, "values.json", …, PutOptions{ContentType: "application/json",
// Metadata: Provenance{Orkflow, TaskID, RunID}.Metadata({"artifact":
// "values.json"})}) — the arguments an Orker's values.json and an awaited
// task's resolution use, so the object lands in the content-addressed family
// a container's values.json does. A returned sha256 that differs from the
// bytes' own is an error.
func (a *ComposeActivities) StoreComposedValues(ctx context.Context, req ComposeStoreRequest) (*orkstackv1.ArtifactRef, error) {
	if a == nil || a.store == nil {
		return nil, composeActivityError(ChildValuesUnstoredCode,
			orkstackv1.FailureCategory_FAILURE_CATEGORY_INFRASTRUCTURE_ERROR,
			"this engine has no artifact store to write the values to")
	}
	raw := []byte(req.Values)
	meta := artifacts.Provenance{Orkflow: req.Orkflow, TaskID: req.TaskID, RunID: req.RunID}.
		Metadata(map[string]string{"artifact": "values.json"})
	ref, err := a.store.Put(ctx, "values.json", bytes.NewReader(raw), artifacts.PutOptions{
		ContentType: "application/json",
		Metadata:    meta,
	})
	if err != nil {
		return nil, fmt.Errorf("store task %q's values.json: %w", req.TaskID, err)
	}
	sum := sha256.Sum256(raw)
	if want := hex.EncodeToString(sum[:]); ref.SHA256 != want {
		return nil, fmt.Errorf("the store recorded task %q's values.json as sha256 %s, but its bytes hash to %s",
			req.TaskID, ref.SHA256, want)
	}
	return &orkstackv1.ArtifactRef{
		Name:        ref.Name,
		Uri:         ref.URI,
		ContentType: ref.ContentType,
		SizeBytes:   ref.SizeBytes,
		Sha256:      ref.SHA256,
	}, nil
}
