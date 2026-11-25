package engine

import (
	"context"
	"errors"
	"fmt"
	"time"

	commonpb "go.temporal.io/api/common/v1"
	deploymentpb "go.temporal.io/api/deployment/v1"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/api/serviceerror"
	workflowpb "go.temporal.io/api/workflow/v1"
	"go.temporal.io/api/workflowservice/v1"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
)

// The resume seam (resume design §10.2, §30.4): what ResumeRun asks of the
// cluster about a run's base execution, and the one reset it makes. A run id
// is the Temporal workflow id; an execution is one Temporal run of it.

// ErrResumeExecutionNotFound: the cluster holds no such execution (its
// history is gone, or it never existed).
var ErrResumeExecutionNotFound = errors.New("the cluster holds no such execution")

// ErrResetOutcomeUnknown: the reset was sent and no answer says whether the
// cluster applied it. It is never sent again: the base's ResetRunId says,
// once it can be read, whether it was made (spike T21, T24).
var ErrResetOutcomeUnknown = errors.New("the reset was sent and whether it was made is not known")

// ResumeExecution is what a resume reads of one execution.
type ResumeExecution struct {
	ExecutionRunID string
	Status         enumspb.WorkflowExecutionStatus
	CloseTime      time.Time
	HistoryLength  int64
	// ResetRunID names the run the latest reset of this execution started;
	// empty when it was never reset.
	ResetRunID string
	// DeploymentName and BuildID are the Worker Deployment Version it ran
	// on; empty for an unversioned execution.
	DeploymentName string
	BuildID        string
}

// ResumeReset is the one reset a resume makes.
type ResumeReset struct {
	RunID              string
	BaseExecutionRunID string
	// WorkflowTaskFinishEventID is R's WorkflowTaskCompleted event id.
	WorkflowTaskFinishEventID int64
	// Reason is the plan key (ResumePlanKey.String()), at most 100 bytes.
	Reason string
	// Deadline is the plan's expires_at: the reset is never applied after it.
	Deadline time.Time
	// DeploymentName and BuildID pin the reset run to the base's build;
	// empty for an unversioned base.
	DeploymentName string
	BuildID        string
}

// ResumeCluster is the resume seam. TemporalStarter implements it.
type ResumeCluster interface {
	// DescribeResumeExecution describes one execution of runID, or its
	// current one when executionRunID is empty.
	DescribeResumeExecution(ctx context.Context, runID, executionRunID string) (ResumeExecution, error)
	// ResumeExecutionHistory is one execution's whole history, in order.
	ResumeExecutionHistory(ctx context.Context, runID, executionRunID string) ([]*historypb.HistoryEvent, error)
	// ResetKeyOf is the reason a reset run was started under, read from its
	// RESET_WORKFLOW workflow-task failure; found is false for a run no
	// reset started.
	ResetKeyOf(ctx context.Context, runID, executionRunID string) (key string, found bool, err error)
	// ResetForResume sends the reset exactly once and answers the new
	// execution's id, or ErrResetOutcomeUnknown when the answer was lost.
	ResetForResume(ctx context.Context, reset ResumeReset) (string, error)
}

var _ ResumeCluster = (*TemporalStarter)(nil)

// DescribeResumeExecution implements ResumeCluster.
func (s *TemporalStarter) DescribeResumeExecution(ctx context.Context, runID, executionRunID string) (ResumeExecution, error) {
	resp, err := s.client.WorkflowService().DescribeWorkflowExecution(ctx, &workflowservice.DescribeWorkflowExecutionRequest{
		Namespace: s.clusterNamespace,
		Execution: &commonpb.WorkflowExecution{WorkflowId: runID, RunId: executionRunID},
	})
	if err != nil {
		var notFound *serviceerror.NotFound
		if errors.As(err, &notFound) {
			return ResumeExecution{}, fmt.Errorf("describe run %s execution %q: %w", runID, executionRunID, ErrResumeExecutionNotFound)
		}
		return ResumeExecution{}, fmt.Errorf("describe run %s execution %q: %w", runID, executionRunID, err)
	}
	info := resp.GetWorkflowExecutionInfo()
	out := ResumeExecution{
		ExecutionRunID: info.GetExecution().GetRunId(),
		Status:         info.GetStatus(),
		HistoryLength:  info.GetHistoryLength(),
		ResetRunID:     resp.GetWorkflowExtendedInfo().GetResetRunId(),
	}
	if info.GetCloseTime() != nil {
		out.CloseTime = info.GetCloseTime().AsTime()
	}
	if v := info.GetVersioningInfo().GetDeploymentVersion(); v != nil {
		out.DeploymentName, out.BuildID = v.GetDeploymentName(), v.GetBuildId()
	}
	return out, nil
}

// ResumeExecutionHistory implements ResumeCluster.
func (s *TemporalStarter) ResumeExecutionHistory(ctx context.Context, runID, executionRunID string) ([]*historypb.HistoryEvent, error) {
	var events []*historypb.HistoryEvent
	err := s.pageHistory(ctx, runID, executionRunID, func(page []*historypb.HistoryEvent) bool {
		events = append(events, page...)
		return true
	})
	return events, err
}

// ResetKeyOf implements ResumeCluster. An execution no reset made names itself
// as its original, in its first event. For another it reads forward only until
// it finds the reset that made it, which follows the copied prefix.
func (s *TemporalStarter) ResetKeyOf(ctx context.Context, runID, executionRunID string) (string, bool, error) {
	var key string
	found := false
	err := s.pageHistory(ctx, runID, executionRunID, func(page []*historypb.HistoryEvent) bool {
		if len(page) > 0 {
			if started := page[0].GetWorkflowExecutionStartedEventAttributes(); started != nil && started.GetOriginalExecutionRunId() == executionRunID {
				return false
			}
		}
		if facts, ok := findResetEvent(page, executionRunID); ok {
			key, found = facts.Reason, true
		}
		return !found
	})
	return key, found, err
}

// pageHistory reads one execution's history page by page, until each runs
// out or more says stop.
func (s *TemporalStarter) pageHistory(ctx context.Context, runID, executionRunID string, more func([]*historypb.HistoryEvent) bool) error {
	var token []byte
	for {
		resp, err := s.client.WorkflowService().GetWorkflowExecutionHistory(ctx, &workflowservice.GetWorkflowExecutionHistoryRequest{
			Namespace:     s.clusterNamespace,
			Execution:     &commonpb.WorkflowExecution{WorkflowId: runID, RunId: executionRunID},
			NextPageToken: token,
		})
		if err != nil {
			var notFound *serviceerror.NotFound
			if errors.As(err, &notFound) {
				return fmt.Errorf("read run %s execution %q: %w", runID, executionRunID, ErrResumeExecutionNotFound)
			}
			return fmt.Errorf("read run %s execution %q: %w", runID, executionRunID, err)
		}
		if !more(resp.GetHistory().GetEvents()) || len(resp.GetNextPageToken()) == 0 {
			return nil
		}
		token = resp.GetNextPageToken()
	}
}

// resetReapplyExcluded is every kind of event a reset can reapply from the
// base's later history: none is, because the plan is what the reset run
// does (resume design §30.4). A cancel request is not one: the server
// reapplies none, and its exclusion is a deprecated option it never
// implemented.
var resetReapplyExcluded = []enumspb.ResetReapplyExcludeType{
	enumspb.RESET_REAPPLY_EXCLUDE_TYPE_SIGNAL,
	enumspb.RESET_REAPPLY_EXCLUDE_TYPE_UPDATE,
	enumspb.RESET_REAPPLY_EXCLUDE_TYPE_NEXUS,
}

// ResetForResume implements ResumeCluster. It calls the service directly,
// once, under reset.Deadline and no retry: the SDK client's own reset
// retries a lost answer and so resets the run again (spike T24), and a
// RequestId does not dedupe on released servers (spike T3).
func (s *TemporalStarter) ResetForResume(ctx context.Context, reset ResumeReset) (string, error) {
	req := &workflowservice.ResetWorkflowExecutionRequest{
		Namespace:                 s.clusterNamespace,
		WorkflowExecution:         &commonpb.WorkflowExecution{WorkflowId: reset.RunID, RunId: reset.BaseExecutionRunID},
		Reason:                    reset.Reason,
		WorkflowTaskFinishEventId: reset.WorkflowTaskFinishEventID,
		RequestId:                 newActivityID("resume-reset"),
		ResetReapplyExcludeTypes:  resetReapplyExcluded,
		Identity:                  s.clusterIdentity,
	}
	if reset.BuildID != "" {
		req.PostResetOperations = []*workflowpb.PostResetOperation{{
			Variant: &workflowpb.PostResetOperation_UpdateWorkflowOptions_{UpdateWorkflowOptions: &workflowpb.PostResetOperation_UpdateWorkflowOptions{
				WorkflowExecutionOptions: &workflowpb.WorkflowExecutionOptions{VersioningOverride: &workflowpb.VersioningOverride{
					Override: &workflowpb.VersioningOverride_Pinned{Pinned: &workflowpb.VersioningOverride_PinnedOverride{
						Behavior: workflowpb.VersioningOverride_PINNED_OVERRIDE_BEHAVIOR_PINNED,
						Version:  &deploymentpb.WorkerDeploymentVersion{DeploymentName: reset.DeploymentName, BuildId: reset.BuildID},
					}},
				}},
				UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"versioning_override"}},
			}},
		}}
	}
	// Round drops the monotonic reading, which stands still while a host is
	// suspended: what is sent to the cluster counts to the recorded instant.
	ctx, cancel := context.WithDeadline(ctx, reset.Deadline.Round(0))
	defer cancel()
	resp, err := s.client.WorkflowService().ResetWorkflowExecution(ctx, req)
	if err == nil {
		return resp.GetRunId(), nil
	}
	if notFound := (*serviceerror.NotFound)(nil); errors.As(err, &notFound) {
		return "", fmt.Errorf("reset run %s: %w: %w", reset.RunID, ErrResumeExecutionNotFound, err)
	}
	if resetDidNotHappen(err) {
		return "", fmt.Errorf("reset run %s: %w", reset.RunID, err)
	}
	return "", fmt.Errorf("reset run %s: %w: %w", reset.RunID, ErrResetOutcomeUnknown, err)
}

// resetDidNotHappen says the cluster refused the reset before applying it;
// every other error, a lost or cancelled answer among them, leaves it
// unknown.
func resetDidNotHappen(err error) bool {
	var (
		invalid      *serviceerror.InvalidArgument
		precondition *serviceerror.FailedPrecondition
		denied       *serviceerror.PermissionDenied
	)
	return errors.As(err, &invalid) || errors.As(err, &precondition) || errors.As(err, &denied)
}
