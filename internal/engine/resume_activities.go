package engine

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/pkg/artifacts"
	"go.temporal.io/sdk/temporal"

	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/workflowservice/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// historyReader is the one call LoadResumePlan makes of the cluster: a read
// of its own execution's history (workflowservice.WorkflowServiceClient).
type historyReader interface {
	GetWorkflowExecutionHistory(ctx context.Context, in *workflowservice.GetWorkflowExecutionHistoryRequest, opts ...grpc.CallOption) (*workflowservice.GetWorkflowExecutionHistoryResponse, error)
}

// resetHistoryPageSize is how many events one page of the read asks for.
const resetHistoryPageSize = 1000

// readResetFacts reads the history of the execution (workflowID, runID)
// forward from its first event until it meets the reset that made it
// (findResetEvent), and answers false when the history holds none: a run that
// was never reset, which the caller has already ruled out. The read is
// bounded by ctx, which the local activity's ScheduleToClose bounds (§30.3,
// third review), and by the history's own length, never by a page cap.
func readResetFacts(ctx context.Context, history historyReader, namespace, workflowID, runID string) (ResetFacts, bool, error) {
	var token []byte
	for {
		page, err := history.GetWorkflowExecutionHistory(ctx, &workflowservice.GetWorkflowExecutionHistoryRequest{
			Namespace:              namespace,
			Execution:              &commonpb.WorkflowExecution{WorkflowId: workflowID, RunId: runID},
			MaximumPageSize:        resetHistoryPageSize,
			NextPageToken:          token,
			HistoryEventFilterType: enumspb.HISTORY_EVENT_FILTER_TYPE_ALL_EVENT,
		})
		if err != nil {
			switch status.Code(err) {
			case codes.NotFound, codes.PermissionDenied, codes.InvalidArgument, codes.Unauthenticated:
				// The cluster answered, and no retry changes that answer.
				return ResetFacts{}, false, temporal.NewNonRetryableApplicationError(
					fmt.Sprintf("read the history of run %s: %v", workflowID, err), ReasonResumePlanUnreadable, err)
			}
			return ResetFacts{}, false, fmt.Errorf("read the history of run %s: %w", workflowID, err)
		}
		if facts, ok := findResetEvent(page.GetHistory().GetEvents(), runID); ok {
			return facts, true, nil
		}
		token = page.GetNextPageToken()
		if len(token) == 0 {
			return ResetFacts{}, false, nil
		}
	}
}

// The two local activities a reset run runs at R (E85 §30.3), in order:
// ReadResetFacts finds the reset that made the run in its history, the
// workflow reads the plan key in its reason, and LoadResumePlan loads and
// verifies the plan's record. They are two so that every ending after the
// first knows the key, and so the sequence above the base's events.
const (
	ActivityReadResetFacts = "ReadResetFacts"
	ActivityLoadResumePlan = "LoadResumePlan"
)

// ResumePlanLoadTimeout bounds LoadResumePlan for a run whose plan pins no
// restore timeout: a constant AGENTS.md §2.1 fixes, not a setting.
const ResumePlanLoadTimeout = 5 * time.Minute

// The endings of a plan that cannot be run (§30.3). Both leave the run as
// its base ended it, FAILED and resumable, with nothing run again.
const (
	ReasonResumePlanInvalid    = "RESUME_PLAN_INVALID"
	ReasonResumePlanUnreadable = "RESUME_PLAN_UNREADABLE"
)

// ReadResetFactsInput names the reset run: its run id (the workflow id) and
// the execution that is asking.
type ReadResetFactsInput struct {
	Namespace   string `json:"namespace"`
	RunID       string `json:"run_id"`
	ExecutionID string `json:"execution_id"`
}

// ResumeObserver is told what a resumed run met that should not happen
// (ork.run.resume.anomalies), and the tries each read of its kept results
// took once it answered or failed for good
// (ork.run.resume.readback.attempts; docs/DEPLOY.md §7).
type ResumeObserver interface {
	ObserveResumeAnomaly(ctx context.Context, namespace, kind string)
	ObserveReadBackCall(ctx context.Context, namespace string, attempts int)
}

// resumeAnomalyResetUnreadable: a reset run could not read the reset that
// made it, so it fails with no event (R4-2).
const resumeAnomalyResetUnreadable = "reset_unreadable"

// WithObserver sets the observer the activities report to.
func (a *ResumeActivities) WithObserver(o ResumeObserver) *ResumeActivities {
	a.observer = o
	return a
}

// LoadResumePlanInput is the plan to load: the run, and the reset that names
// the plan.
type LoadResumePlanInput struct {
	Namespace string     `json:"namespace"`
	RunID     string     `json:"run_id"`
	Reset     ResetFacts `json:"reset"`
}

// resumePlanSource is what LoadResumePlan reads besides its history: the
// plan's record, and the identity records' newest resume of the run.
type resumePlanSource interface {
	ReadResumePlan(ctx context.Context, namespace, runID string, scope uint64, n int) (artifacts.RecordRead[*orkstackv1.ResumePlanRecord], string, error)
	NewestResume(ctx context.Context, namespace, runID string) (uint64, artifacts.RecordRead[artifacts.ResumeRecord], error)
}

// WithHistory gives the activities the cluster's history reads, in the
// deployment's Temporal namespace, and the identity records.
func (a *ResumeActivities) WithHistory(history historyReader, temporalNamespace string, records resumePlanSource) *ResumeActivities {
	a.history, a.temporalNamespace, a.records = history, temporalNamespace, records
	return a
}

// LoadResumePlan answers the plan a reset run carries out (§30.3): its
// verified record, or a record with no block when the reset carries no plan
// key, which is a cluster reset made by hand and no resume. A plan key whose
// record is absent, does not hash to the key's digest, names another base,
// another plan or another reset point, or is not the newest resume of the
// run, is a final RESUME_PLAN_INVALID. A store or a cluster that does not
// answer is retryable, until the call's ScheduleToClose; the workflow then
// ends the run RESUME_PLAN_UNREADABLE.
func (a *ResumeActivities) LoadResumePlan(ctx context.Context, in LoadResumePlanInput) (*orkstackv1.ResumePlanRecord, error) {
	if a.records == nil {
		return nil, temporal.NewNonRetryableApplicationError("this engine has no record store for a resume plan", ReasonResumePlanUnreadable, nil)
	}
	facts := in.Reset
	key, isResume, err := planKeyOf(in.RunID, facts.Reason)
	if err != nil {
		return nil, err
	}
	if !isResume {
		return &orkstackv1.ResumePlanRecord{}, nil
	}
	read, digest, err := a.records.ReadResumePlan(ctx, in.Namespace, in.RunID, key.Scope, key.N)
	if err != nil {
		return nil, unansweredPlanRead(err)
	}
	newest, link, err := a.records.NewestResume(ctx, in.Namespace, in.RunID)
	if err != nil {
		return nil, unansweredPlanRead(err)
	}
	if why := verifyResumePlan(key, facts, read, digest, newest, link); why != "" {
		return nil, temporal.NewNonRetryableApplicationError(
			fmt.Sprintf("run %s: the resume plan %s cannot be run: %s", in.RunID, key, why), ReasonResumePlanInvalid, nil)
	}
	return read.Value, nil
}

// ReadResetFacts answers the reset that made run (in.RunID, in.ExecutionID),
// from its own history (§30.3, step 1). A history that holds none is final:
// the caller asks only for a run that is a reset run.
func (a *ResumeActivities) ReadResetFacts(ctx context.Context, in ReadResetFactsInput) (ResetFacts, error) {
	if a.history == nil {
		return ResetFacts{}, temporal.NewNonRetryableApplicationError("this engine has no history reader for a resume plan", ReasonResumePlanUnreadable, nil)
	}
	facts, found, err := readResetFacts(ctx, a.history, a.temporalNamespace, in.RunID, in.ExecutionID)
	if err == nil && !found {
		err = temporal.NewNonRetryableApplicationError(
			fmt.Sprintf("run %s: its history does not say how it was resumed", in.RunID), ReasonResumePlanUnreadable, nil)
	}
	if err != nil {
		// Counted when no further try can follow: a final answer, or the
		// call's own deadline. A retried try is not an anomaly yet.
		if categoryOf(err).nonRetryable() || errors.Is(ctx.Err(), context.DeadlineExceeded) {
			if a.observer != nil {
				a.observer.ObserveResumeAnomaly(ctx, in.Namespace, resumeAnomalyResetUnreadable)
			}
		}
		return ResetFacts{}, err
	}
	return facts, nil
}

// planKeyOf reads a reset's reason: a plan key, or no resume at all. Only a
// reason that is no plan key of any version is a reset made by hand; one
// that claims to be a plan key and does not parse is a plan this build
// cannot run, a final RESUME_PLAN_INVALID, never a resume silently dropped.
func planKeyOf(runID, reason string) (ResumePlanKey, bool, error) {
	if key, ok := ParseResumePlanKey(reason); ok {
		return key, true, nil
	}
	if strings.HasPrefix(reason, resumeKeyFamily) {
		return ResumePlanKey{}, false, temporal.NewNonRetryableApplicationError(
			fmt.Sprintf("run %s was resumed with plan key %q, which this build does not read", runID, reason),
			ReasonResumePlanInvalid, nil)
	}
	return ResumePlanKey{}, false, nil
}

func unansweredPlanRead(err error) error {
	var un *artifacts.RecordStoreUnanswered
	if errors.As(err, &un) || errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
		return fmt.Errorf("read the resume plan: %w", err)
	}
	return temporal.NewNonRetryableApplicationError(fmt.Sprintf("read the resume plan: %v", err), ReasonResumePlanInvalid, err)
}

// verifyResumePlan is §30.3 step 3 and §30.9 MEDIUM-3: why the record read
// for key cannot be the plan the reset (facts) carries, or "" when it is.
func verifyResumePlan(key ResumePlanKey, facts ResetFacts, read artifacts.RecordRead[*orkstackv1.ResumePlanRecord], digest string,
	newest uint64, link artifacts.RecordRead[artifacts.ResumeRecord]) string {
	switch read.Answer {
	case artifacts.RecordFound:
	case artifacts.RecordNone:
		return "it has no record"
	case artifacts.RecordUnsupported:
		return fmt.Sprintf("its record is of format %d, which this build does not read", read.Version)
	default:
		return "its record " + read.Reason
	}
	rec := read.Value
	switch {
	case digest != key.PlanDigest:
		return fmt.Sprintf("its record hashes to %s, not %s", digest, key.PlanDigest)
	case rec.GetBaseExecutionRunId() != facts.BaseRunID:
		return fmt.Sprintf("its record is of the ending of %q, and the run was resumed from that of %q", rec.GetBaseExecutionRunId(), facts.BaseRunID)
	case rec.GetRScheduledEventId() != facts.ScheduledEventID || rec.GetRStartedEventId() != facts.StartedEventID:
		return fmt.Sprintf("its record starts from point %d/%d of the run, and the run was resumed from %d/%d",
			rec.GetRScheduledEventId(), rec.GetRStartedEventId(), facts.ScheduledEventID, facts.StartedEventID)
	case rec.GetBlock().GetScope() != key.Scope:
		return fmt.Sprintf("its block is of scope %d, not %d", rec.GetBlock().GetScope(), key.Scope)
	case rec.GetPlanDigest() != ResumePlanDigestOf(rec):
		return "its plan is not the one it was confirmed as"
	}
	switch {
	case link.Answer != artifacts.RecordFound:
		return fmt.Sprintf("the run's newest resume cannot be read (%v %s)", link.Answer, link.Reason)
	case newest != key.Scope || link.Value.N != key.N:
		return fmt.Sprintf("the run's newest resume is plan %d.%03d", newest, link.Value.N)
	}
	return ""
}
