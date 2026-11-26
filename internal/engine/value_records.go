package engine

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"maps"
	"slices"

	"go.temporal.io/sdk/temporal"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/identity"
	"github.com/orkstack/orkstack/internal/stablejson"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// The two local activities that record a child run's outputs and a composed
// task's values, of a run whose results the engine records
// (docs/TASK_IDENTITY.md).

// Local activities on the run worker. Internal: no user-facing surface names
// them.
const (
	ActivityRecordOutputs  = "RecordOutputs"
	ActivityRecordComposed = "RecordComposedValues"
)

// RecordStoreUnansweredType is the type of a failure in which nothing was
// answered: retried within the activity's budget.
const RecordStoreUnansweredType = "RECORD_STORE_UNANSWERED"

// ValuesRecorder is identity.ValuesRecorder.
type ValuesRecorder interface {
	RecordOutputs(ctx context.Context, q identity.OutputsAsk) (identity.RecordedOutcome, error)
	RecordComposed(ctx context.Context, q identity.ComposedAsk) (identity.RecordedOutcome, error)
}

// ValueRecordActivities owns the two activities. A nil recorder, an engine
// that issues no task identity, records nothing and says so.
type ValueRecordActivities struct{ rec ValuesRecorder }

// NewValueRecordActivities builds them over rec.
func NewValueRecordActivities(rec ValuesRecorder) *ValueRecordActivities {
	return &ValueRecordActivities{rec: rec}
}

// RecordOutputsRequest is a child run that succeeded.
type RecordOutputsRequest struct {
	Namespace       string `json:"namespace"`
	RunID           string `json:"run_id"`
	AdmissionSHA256 string `json:"admission_sha256"`
	RootScope       uint64 `json:"root_scope,omitzero"`
	// Input is the run's start input (identity.StartInput).
	Input stablejson.RawJSON `json:"input"`
	// Offered is each task that succeeded and the values the run holds of it.
	Offered  []ComposeUpstream `json:"offered"`
	MaxBytes int64             `json:"max_bytes"`
	// Spacings are the waits of one read, the run's own; nil is the reader's.
	Spacings *artifacts.RecordSpacings `json:"spacings,omitempty"`
}

// RecordComposedRequest is a composed task whose child run succeeded.
type RecordComposedRequest struct {
	Namespace            string             `json:"namespace"`
	RunID                string             `json:"run_id"`
	AdmissionSHA256      string             `json:"admission_sha256"`
	TaskID               string             `json:"task_id"`
	ResumeScope          uint64             `json:"resume_scope,omitzero"`
	RootScope            uint64             `json:"root_scope,omitzero"`
	Input                stablejson.RawJSON `json:"input"`
	ChildAdmissionSHA256 string             `json:"child_admission_sha256"`
	// ValuesSHA256 is of the values the child returned (canonicalValuesJSON).
	ValuesSHA256 string                    `json:"values_sha256"`
	Spacings     *artifacts.RecordSpacings `json:"spacings,omitempty"`
}

// RecordedValuesResult is the record made, or found as it would have been
// made. Values are the bytes of a run's outputs.
type RecordedValuesResult struct {
	RecordSHA256 string             `json:"record_sha256"`
	ValuesRef    ArtifactRefPlan    `json:"values_ref"`
	Values       stablejson.RawJSON `json:"values,omitempty"`
	Artifacts    []ArtifactRefPlan  `json:"artifacts,omitempty"`
}

// RecordOutputs records a child run's outputs.
func (a *ValueRecordActivities) RecordOutputs(ctx context.Context, req RecordOutputsRequest) (RecordedValuesResult, error) {
	if a == nil || a.rec == nil {
		return RecordedValuesResult{}, noRecorder()
	}
	offered := make(map[string]*orkstackv1.ArtifactRef, len(req.Offered))
	for _, up := range req.Offered {
		offered[up.TaskID] = &orkstackv1.ArtifactRef{Uri: up.URI, Sha256: up.SHA256}
	}
	got, err := a.rec.RecordOutputs(ctx, identity.OutputsAsk{Namespace: req.Namespace, RunID: req.RunID, AdmissionSHA256: req.AdmissionSHA256,
		Scopes: identity.Scopes{Root: req.RootScope}, Input: req.Input, Offered: offered, MaxBytes: req.MaxBytes, Spacing: req.Spacings})
	return recordedResult(ctx, got, err)
}

// RecordComposed records a composed task's values.
func (a *ValueRecordActivities) RecordComposed(ctx context.Context, req RecordComposedRequest) (RecordedValuesResult, error) {
	if a == nil || a.rec == nil {
		return RecordedValuesResult{}, noRecorder()
	}
	got, err := a.rec.RecordComposed(ctx, identity.ComposedAsk{Namespace: req.Namespace, RunID: req.RunID, AdmissionSHA256: req.AdmissionSHA256,
		TaskID: req.TaskID, Scopes: identity.Scopes{Resume: req.ResumeScope, Root: req.RootScope}, Input: req.Input,
		ChildAdmissionSHA256: req.ChildAdmissionSHA256, ValuesSHA256: req.ValuesSHA256, Spacing: req.Spacings})
	return recordedResult(ctx, got, err)
}

func noRecorder() error {
	return recordUnanswered(artifacts.UnansweredUnavailable, errors.New("this engine records no values; another replica may"))
}

func recordedResult(ctx context.Context, got identity.RecordedOutcome, err error) (RecordedValuesResult, error) {
	if err != nil {
		return RecordedValuesResult{}, recordFailure(ctx, err)
	}
	out := RecordedValuesResult{RecordSHA256: got.RecordSHA256, ValuesRef: ArtifactRefPlanOf(got.ValuesRef), Values: got.Values}
	for _, ref := range got.Artifacts {
		out.Artifacts = append(out.Artifacts, ArtifactRefPlanOf(ref))
	}
	return out, nil
}

// recordFailure is the activities' typed boundary: a refusal of what was read
// carries its reason and is final, a render's refusal is as it is, and
// anything else was not answered, by its class.
func recordFailure(ctx context.Context, err error) error {
	if errors.Is(ctx.Err(), context.Canceled) {
		return temporal.NewCanceledError() // the run's own
	}
	var refusal *identity.ValuesRefusal
	if errors.As(err, &refusal) {
		return composeActivityError(refusal.Reason, orkstackv1.FailureCategory_FAILURE_CATEGORY_INFRASTRUCTURE_ERROR, refusal.Message)
	}
	if failure := categoryOf(err); failure.isApplication() {
		return failure.app
	}
	return recordUnanswered(artifacts.UnansweredClass(err), err)
}

func recordUnanswered(class string, err error) error {
	return temporal.NewApplicationErrorWithOptions(fmt.Sprintf("%s: %v", class, err), RecordStoreUnansweredType,
		temporal.ApplicationErrorOptions{Cause: err})
}

// RenderOutputs renders a run's declared outputs as RenderComposedValues does,
// and answers them as a parent canonicalises them.
func (a *ComposeActivities) RenderOutputs(ctx context.Context, r identity.OutputsRender) ([]byte, error) {
	failed := func(err error) error {
		return composeActivityError(OutputsRenderFailedCode, orkstackv1.FailureCategory_FAILURE_CATEGORY_CONTRACT_VIOLATION,
			fmt.Sprintf("outputs: %v", err))
	}
	input, err := payloadOfJSON(r.Input)
	if err != nil {
		return nil, failed(err)
	}
	inputs, err := stablejson.Marshal(payloadMap(ApplyInputDefaults(r.Definition, input)))
	if err != nil {
		return nil, failed(err)
	}
	templates, err := stablejson.Marshal(r.Definition.GetOutputs())
	if err != nil {
		return nil, failed(err)
	}
	req := ComposeRenderRequest{Label: "outputs", CodePrefix: renderPrefixOutputs, Templates: templates, Inputs: inputs, MaxBytes: r.MaxBytes}
	for _, id := range slices.Sorted(maps.Keys(r.Upstream)) {
		ref := r.Upstream[id]
		req.Upstream = append(req.Upstream, ComposeUpstream{TaskID: id, URI: ref.GetUri(), SHA256: ref.GetSha256()})
	}
	rendered, err := a.RenderComposedValues(ctx, req)
	if err != nil {
		return nil, err
	}
	output, err := payloadOfJSON(rendered)
	if err != nil {
		return nil, failed(err)
	}
	raw, err := canonicalValuesJSON(payloadMap(output))
	if err != nil {
		return nil, failed(err)
	}
	return raw, nil
}

// StoreOutputs stores a run's outputs as a composed task's values are, with
// the child run they are of for their provenance.
func (a *ComposeActivities) StoreOutputs(ctx context.Context, adm artifacts.Admission, raw []byte) (*orkstackv1.ArtifactRef, error) {
	if a == nil || a.store == nil {
		return nil, errors.New("this engine has no artifact store to write the outputs to")
	}
	return a.StoreComposedValues(ctx, ComposeStoreRequest{Orkflow: adm.Orkflow, RunID: adm.RunID, TaskID: adm.Starter.TaskID, Values: bytes.Clone(raw)})
}
