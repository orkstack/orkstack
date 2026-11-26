package engine

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/stablejson"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// The one read of an await's record (docs/TASK_IDENTITY.md, "Resolutions the
// engine records"): a local activity that answers with a value whenever the
// store answered, and with an error only when it did not.

// ActivityReadResolution is that local activity. Internal: no user-facing
// surface names it.
const ActivityReadResolution = "ReadResolutionRecord"

// The outcomes of a read the store answered.
const (
	ResolutionFound       = "FOUND"       // stored in time
	ResolutionLate        = "LATE"        // stored after the deadline
	ResolutionNone        = "NONE"        // said twice
	ResolutionInvalid     = "INVALID"     // there, and not this await's, or not readable
	ResolutionUnsupported = "UNSUPPORTED" // of a format this build does not read
)

// ResolutionReadRequest names the await whose record is read.
type ResolutionReadRequest struct {
	Namespace       string `json:"namespace"`
	RunID           string `json:"run_id"`
	TaskID          string `json:"task_id"`
	ResumeScope     uint64 `json:"resume_scope,omitzero"`
	AdmissionSHA256 string `json:"admission_sha256"`
	// Deadline is the await's: a record stored after it settles nothing.
	Deadline time.Time                 `json:"deadline"`
	Spacings *artifacts.RecordSpacings `json:"spacings,omitempty"`
}

// ResolutionRead is what the store answered. Everything but Outcome, Object,
// Why and Version is of a resolution found in time.
type ResolutionRead struct {
	Outcome           string             `json:"outcome"`
	RecordSHA256      string             `json:"record_sha256,omitzero"`
	Rejected          bool               `json:"rejected,omitzero"`
	Reason            string             `json:"reason,omitzero"`
	Values            *ArtifactRefPlan   `json:"values,omitempty"`
	ValuesJSON        stablejson.RawJSON `json:"values_json,omitempty"`
	Via               string             `json:"via,omitzero"`
	Principal         string             `json:"principal,omitzero"`
	PrincipalVerified bool               `json:"principal_verified,omitzero"`
	// Object and Why say what an invalid or unsupported answer is about.
	Object  string `json:"object,omitzero"`
	Why     string `json:"why,omitzero"`
	Version int64  `json:"version,omitzero"`
}

// via is the door a resolution came through.
func (r ResolutionRead) via() orkstackv1.AwaitResolvedVia {
	return orkstackv1.AwaitResolvedVia(orkstackv1.AwaitResolvedVia_value[r.Via])
}

// ResolutionReader reads an await's record: artifacts.RecordStore.
type ResolutionReader interface {
	ReadRecordedValues(ctx context.Context, class artifacts.SlotClass, spacing *artifacts.RecordSpacings, key string, want artifacts.RecordWant) (artifacts.RecordRead[*artifacts.RecordedFound], error)
}

// ResolutionReadActivities owns the read. A nil reader, an engine that keeps
// no identity records, answers nothing and says so.
type ResolutionReadActivities struct{ records ResolutionReader }

// NewResolutionReadActivities builds it over records.
func NewResolutionReadActivities(records ResolutionReader) *ResolutionReadActivities {
	return &ResolutionReadActivities{records: records}
}

// ReadResolution reads one await's record.
func (a *ResolutionReadActivities) ReadResolution(ctx context.Context, req ResolutionReadRequest) (ResolutionRead, error) {
	if a == nil || a.records == nil {
		return ResolutionRead{}, recordUnanswered(artifacts.UnansweredUnavailable, errors.New("this engine reads no records; another replica may"))
	}
	key, err := artifacts.RecordKey(artifacts.RecordResolution, req.Namespace, req.RunID, req.TaskID, req.ResumeScope)
	if err != nil {
		return ResolutionRead{Outcome: ResolutionInvalid, Object: "record", Why: err.Error()}, nil
	}
	read, err := a.records.ReadRecordedValues(ctx, artifacts.SlotRun, req.Spacings, key, artifacts.RecordWant{
		Kind: orkstackv1.RecordedValuesKind_RECORDED_VALUES_KIND_RESOLUTION, Namespace: req.Namespace, RunID: req.RunID, TaskID: req.TaskID,
		Scope: req.ResumeScope, AdmissionSHA256: req.AdmissionSHA256})
	if err != nil {
		return ResolutionRead{}, recordFailure(ctx, err)
	}
	switch read.Answer {
	case artifacts.RecordNone:
		return ResolutionRead{Outcome: ResolutionNone}, nil
	case artifacts.RecordUnsupported:
		return ResolutionRead{Outcome: ResolutionUnsupported, Object: read.Object, Version: read.Version}, nil
	case artifacts.RecordFound:
	default:
		return ResolutionRead{Outcome: ResolutionInvalid, Object: read.Object, Why: boundedWhy(read.Reason)}, nil
	}
	adopted, inTime := read.Value.InTime(req.Deadline, time.Time{})
	if !inTime {
		return ResolutionRead{Outcome: ResolutionLate}, nil
	}
	rec, detail := adopted.Record(), read.Value.Detail()
	out := ResolutionRead{Outcome: ResolutionFound, RecordSHA256: read.Value.RecordSHA256(), Rejected: rec.GetRejected(),
		Reason: detail.GetReason(), Via: detail.GetVia().String(), Principal: detail.GetPrincipal(), PrincipalVerified: detail.GetPrincipalVerified()}
	if v := rec.GetValues(); v != nil {
		ref := ArtifactRefPlanOf(v)
		out.Values, out.ValuesJSON = &ref, adopted.Inline()
	}
	return out, nil
}

// maxResolutionWhy bounds what a run's history and message hold of why a
// record cannot be used.
const maxResolutionWhy = 512

func boundedWhy(why string) string {
	if len(why) <= maxResolutionWhy {
		return why
	}
	return strings.ToValidUTF8(why[:maxResolutionWhy], "") + "…"
}

// resolutionReadContext is the options of one read, from the run's own budget.
func (s *runState) resolutionReadContext(ctx workflow.Context) workflow.Context {
	b := s.plan.Identity.RecordBudget
	return workflow.WithLocalActivityOptions(ctx, workflow.LocalActivityOptions{
		StartToCloseTimeout:    time.Duration(b.AttemptSeconds) * time.Second,
		ScheduleToCloseTimeout: time.Duration(b.TotalSeconds) * time.Second,
		RetryPolicy: &temporal.RetryPolicy{MaximumAttempts: b.Attempts,
			InitialInterval: time.Duration(b.RetrySeconds) * time.Second, BackoffCoefficient: max(b.RetryCoefficient, 1)},
	})
}

// resolutionAnswer is one read as the run takes it: what the store answered,
// or the class of what did not, or the run's cancel.
type resolutionAnswer struct {
	ResolutionRead
	unanswered string // its class; "" for an answer
	err        error  // why, with unanswered; the cancellation, with canceled
	canceled   bool
}

// resolutionRequest names await id's record: under the resume that opened it.
func (s *runState) resolutionRequest(id string) ResolutionReadRequest {
	return ResolutionReadRequest{
		Namespace: artifacts.ResolveNamespace(s.wf.GetNamespace()), RunID: s.runID, TaskID: id, ResumeScope: s.resumeScope(id),
		AdmissionSHA256: s.plan.Identity.AdmissionSHA256, Deadline: s.awaits[id].deadlineAt, Spacings: s.plan.Identity.RecordBudget.Spacings(),
	}
}

// readResolution reads await id's record. It changes nothing of the run.
func (s *runState) readResolution(ctx workflow.Context, id string) resolutionAnswer {
	var got ResolutionRead
	readCtx := s.resolutionReadContext(ctx)
	err := s.getLocal(readCtx, workflow.ExecuteLocalActivity(readCtx, ActivityReadResolution, s.resolutionRequest(id)), &got)
	switch {
	case err != nil && ctx.Err() != nil:
		return resolutionAnswer{canceled: true, err: temporal.NewCanceledError()}
	case err != nil:
		return resolutionAnswer{unanswered: unansweredClass(err), err: err}
	}
	switch got.Outcome {
	case ResolutionFound, ResolutionLate, ResolutionNone, ResolutionInvalid, ResolutionUnsupported:
		return resolutionAnswer{ResolutionRead: got}
	}
	return resolutionAnswer{unanswered: classInternal, err: fmt.Errorf("the read answered %q", got.Outcome)}
}

// classInternal is a read that failed by the engine's own fault.
const classInternal = "internal"

// unansweredClass is the class of a read's failure as its activity gave it.
func unansweredClass(err error) string {
	failure := categoryOf(err)
	if failure.isApplication() && failure.appType() == RecordStoreUnansweredType {
		if class, _, ok := strings.Cut(failure.appMessage(), ": "); ok {
			return class
		}
	}
	switch {
	case failure.timedOut():
		return artifacts.UnansweredTimeout
	case failure.canceled:
		return artifacts.UnansweredUnavailable // the engine's own stop, not the run's cancel
	}
	return classInternal
}
