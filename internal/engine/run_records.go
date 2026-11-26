package engine

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"maps"
	"slices"
	"strings"
	"time"

	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/dsl"
	"github.com/orkstack/orkstack/internal/identity"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// What the interpreter does for a run whose results the engine records
// (docs/TASK_IDENTITY.md): the values it takes of a child, and the outputs a
// child ends with, are what a record the engine made says.

// Reason codes of what is not on record, or not as the run holds it.
const (
	ChildValuesNotRecordedCode = identity.ReasonChildValuesNotRecorded // InfrastructureError
	OutputsNotRecordedCode     = identity.ReasonOutputsNotRecorded     // InfrastructureError
)

// errCanceledAtSuccess is a run canceled while it recorded its outputs.
var errCanceledAtSuccess = errors.New("the run was canceled while its outputs were recorded")

// recorded reports whether the engine records this run's results and values.
func (s *runState) recorded() bool {
	return s.plan.Identity != nil && s.plan.Identity.ValueRecords > 0
}

// planCarriesRecords reports that the plan of a run whose results the engine
// records carries every number it decides by.
func planCarriesRecords(plan RunPlan) bool {
	id := plan.Identity
	b := id.RecordBudget
	return id.ResolutionsPerPass > 0 && id.NonesPerAwait > 0 && b != nil && b.AttemptSeconds > 0 && b.TotalSeconds > 0 && b.Attempts > 0 &&
		b.OutputsAttemptSeconds > 0 && b.OutputsAttempts > 0 && b.DeadlineRereadSeconds > 0 && b.ReadEvents > 0 && b.RetrySeconds > 0 &&
		b.TotalSeconds >= b.AttemptSeconds && b.TotalSeconds >= b.OutputsAttemptSeconds &&
		b.MissConfirmMS > 0 && b.ReaskInitialMS > 0 && b.ReaskMaxMS >= b.ReaskInitialMS && b.AttemptMarginMS >= 0
}

// recordContext is the options of the activities that make a record, from the
// run's own budget.
func (s *runState) recordContext(ctx workflow.Context) workflow.Context {
	b := s.plan.Identity.RecordBudget
	return workflow.WithLocalActivityOptions(ctx, workflow.LocalActivityOptions{
		StartToCloseTimeout:    time.Duration(b.OutputsAttemptSeconds) * time.Second,
		ScheduleToCloseTimeout: time.Duration(b.TotalSeconds) * time.Second,
		RetryPolicy: &temporal.RetryPolicy{MaximumAttempts: b.OutputsAttempts,
			InitialInterval: time.Duration(b.RetrySeconds) * time.Second, BackoffCoefficient: max(b.RetryCoefficient, 1)},
	})
}

// recordComposed records composed task id's values, whose canonical bytes are
// raw, from its child's record; the error is the one that failed the task, or
// a cancellation.
func (s *runState) recordComposed(ctx workflow.Context, id string, raw []byte, child *orkstackv1.Run) (*orkstackv1.ArtifactRef, error) {
	c := s.children[id]
	fail := func(code, message string) error {
		return s.failComposed(ctx, id, code, orkstackv1.FailureCategory_FAILURE_CATEGORY_INFRASTRUCTURE_ERROR, message, "")
	}
	unstored := func(cause string) error {
		return fail(ChildValuesUnstoredCode, fmt.Sprintf("child run %s succeeded, but its outputs could not be recorded as task %q's values: %s",
			c.plan.RunID, id, cause))
	}
	input, err := identity.StartInput(s.inputArg)
	if err != nil {
		return nil, unstored(err.Error())
	}
	var admission string
	if c.plan.Plan.Identity != nil {
		admission = c.plan.Plan.Identity.AdmissionSHA256
	}
	sum := sha256.Sum256(raw)
	var got RecordedValuesResult
	recordCtx := s.recordContext(ctx)
	err = s.getLocal(recordCtx, workflow.ExecuteLocalActivity(recordCtx, ActivityRecordComposed, RecordComposedRequest{
		Namespace: artifacts.ResolveNamespace(s.wf.GetNamespace()), RunID: s.runID, AdmissionSHA256: s.plan.Identity.AdmissionSHA256, TaskID: id, Input: input,
		ChildAdmissionSHA256: admission, ValuesSHA256: hex.EncodeToString(sum[:]), Spacings: s.plan.Identity.RecordBudget.Spacings(),
	}), &got)
	if err != nil {
		failure := categoryOf(err)
		switch {
		case ctx.Err() != nil:
			return nil, err
		case unreadableLocal(err):
			return nil, s.failComposed(ctx, id, PayloadUnreadableCode, orkstackv1.FailureCategory_FAILURE_CATEGORY_CONTRACT_VIOLATION,
				fmt.Sprintf("child run %s succeeded, but task %q's values on record could not be read from this run's history", c.plan.RunID, id), "")
		case failure.reasonCode() != "":
			return nil, fail(failure.reasonCode(), fmt.Sprintf("task %q: %s", id, failure.appMessage()))
		case failure.isApplication():
			return nil, unstored(failure.appMessage())
		}
		return nil, unstored(err.Error())
	}
	s.answered()
	if why := differentArtifacts(got.Artifacts, child.GetOutputArtifacts()); why != "" {
		return nil, fail(ChildValuesNotRecordedCode, fmt.Sprintf("child run %s returned other artifacts than its outputs on record: %s", c.plan.RunID, why))
	}
	s.taskRuns[id].Record = &orkstackv1.RecordRef{Sha256: got.RecordSHA256, Kind: orkstackv1.RecordedValuesKind_RECORDED_VALUES_KIND_COMPOSED}
	return got.ValuesRef.Proto(), nil
}

// recordOutputs renders this child run's outputs from its tasks' records, and
// records them: what it returns, and the artifacts the record names.
func (s *runState) recordOutputs(ctx workflow.Context, returned []*orkstackv1.NamedArtifact) (*orkstackv1.Payload, *orkstackv1.Error, error) {
	refuse := func(code, message string) *orkstackv1.Error {
		return s.outputsFailure(code, orkstackv1.FailureCategory_FAILURE_CATEGORY_INFRASTRUCTURE_ERROR, message)
	}
	input, err := identity.StartInput(s.inputArg)
	if err != nil {
		return nil, refuse(OutputsUnavailableCode, err.Error()), nil
	}
	read, err := dsl.ReferencedUpstreamIn(s.wf.GetOutputs())
	if err != nil {
		return nil, s.outputsFailure(OutputsRenderFailedCode, orkstackv1.FailureCategory_FAILURE_CATEGORY_CONTRACT_VIOLATION, err.Error()), nil
	}
	names := make(map[string]bool, len(read))
	for _, id := range read {
		names[id] = true
	}
	for _, a := range s.wf.GetOutputArtifacts() {
		names[a.GetTaskId()] = true
	}
	req := RecordOutputsRequest{Namespace: artifacts.ResolveNamespace(s.wf.GetNamespace()), RunID: s.runID, AdmissionSHA256: s.plan.Identity.AdmissionSHA256,
		Input: input, MaxBytes: s.plan.Limits.Proto().GetValuesMaxBytes(), Spacings: s.plan.Identity.RecordBudget.Spacings()}
	for _, id := range slices.Sorted(maps.Keys(names)) {
		if ref := s.results[id].GetValuesRef(); ref.GetSha256() != "" && s.taskRuns[id].GetStatus() == orkstackv1.Status_STATUS_SUCCEEDED {
			req.Offered = append(req.Offered, ComposeUpstream{TaskID: id, URI: ref.GetUri(), SHA256: ref.GetSha256()})
		}
	}
	var got RecordedValuesResult
	recordCtx := s.recordContext(ctx)
	if err := s.getLocal(recordCtx, workflow.ExecuteLocalActivity(recordCtx, ActivityRecordOutputs, req), &got); err != nil {
		if ctx.Err() != nil {
			return nil, nil, errCanceledAtSuccess
		}
		code, category, message := renderFailure(renderPrefixOutputs, err)
		if code == OutputsUnavailableCode {
			// Whichever step of the record went unanswered, or ran out of
			// time, the reason says that the record was not made.
			class := unansweredClass(err)
			message = fmt.Sprintf("their record could not be made (%s): %s", class, strings.TrimPrefix(message, class+": "))
		}
		return nil, s.outputsFailure(code, category, message), nil
	}
	s.answered()
	if why := differentArtifacts(got.Artifacts, returned); why != "" {
		return nil, refuse(OutputsNotRecordedCode, "the artifacts the run returns are not the ones on record: "+why), nil
	}
	output, err := payloadOfJSON(got.Values)
	if err != nil {
		return nil, s.outputsFailure(OutputsRenderFailedCode, orkstackv1.FailureCategory_FAILURE_CATEGORY_CONTRACT_VIOLATION, err.Error()), nil
	}
	return output, nil, nil
}

// differentArtifacts says how the artifacts a run returns differ from the ones
// on record, by name and content, or "".
func differentArtifacts(recorded []ArtifactRefPlan, returned []*orkstackv1.NamedArtifact) string {
	on := make(map[string]string, len(recorded))
	for _, a := range recorded {
		on[a.Name] = a.SHA256
	}
	for _, a := range returned {
		if sha, ok := on[a.GetName()]; !ok || sha != a.GetArtifact().GetSha256() {
			return fmt.Sprintf("artifact %q", a.GetName())
		}
		delete(on, a.GetName())
	}
	for _, name := range slices.Sorted(maps.Keys(on)) {
		return fmt.Sprintf("artifact %q is on record and not returned", name)
	}
	return ""
}

// nameTaskFailure gives the failure of a run whose results the engine records,
// which failed at task, that task's own code and category: a run that could
// not read whether anyone answered, and one that nobody answered, differ by
// more than a sentence.
func (s *runState) nameTaskFailure(failure *orkstackv1.Error, task string) {
	own := s.taskRuns[task].GetError()
	if !s.recorded() || own.GetCode() == "" || failure.GetCode() != "RUN_FAILED" {
		return
	}
	failure.TaskCode = own.GetCode()
	failure.Category = own.GetCategory()
	failure.Details = fmt.Sprintf("task %q; %s", task, own.GetCode())
}

// childTaskCode is the code of the task a child run failed at, where its parent
// is a run whose results the engine records.
func (s *runState) childTaskCode(cause error) string {
	if !s.recorded() {
		return ""
	}
	if code := childReasonCode(cause); code != "" {
		return code
	}
	if code := categoryOf(cause).appType(); categoryEnumFor(code) == orkstackv1.FailureCategory_FAILURE_CATEGORY_UNSPECIFIED {
		return code
	}
	return ""
}

// recordURI is the engine's record of awaited or composed task id, for a
// dependent to stage from; "" for any other task or run.
func (s *runState) recordURI(id string) string {
	kind := artifacts.RecordResolution
	switch {
	case !s.recorded():
		return ""
	case s.children[id] != nil:
		kind = artifacts.RecordComposed
	case s.awaits[id] == nil:
		return ""
	}
	if s.plan.CommitRoot == "" {
		return "" // as commitFor's
	}
	key, err := artifacts.RecordKey(kind, artifacts.ResolveNamespace(s.wf.GetNamespace()), s.runID, id, 0)
	if err != nil {
		return "" // no such task: the Orker refuses a ref with no record
	}
	return strings.TrimSuffix(s.plan.CommitRoot, "/") + "/" + key
}
