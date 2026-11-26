package engine

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"time"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	commonpb "go.temporal.io/api/common/v1"
	deploymentpb "go.temporal.io/api/deployment/v1"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/api/serviceerror"
	workflowpb "go.temporal.io/api/workflow/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/converter"
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
	// StartTime is when the execution started.
	StartTime     time.Time
	HistoryLength int64
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
	// ResetKeyOf is the reason of the reset an execution is of, read from its
	// RESET_WORKFLOW workflow-task failure, or from that of the execution it
	// carried on from; found is false for a run no reset started, and where
	// the cluster no longer holds the one that was.
	ResetKeyOf(ctx context.Context, runID, executionRunID string) (key string, found bool, err error)
	// ResetForResume sends the reset exactly once and answers the new
	// execution's id, or ErrResetOutcomeUnknown when the answer was lost.
	ResetForResume(ctx context.Context, reset ResumeReset) (string, error)
	// ExecutionPayloads is what one execution's history is read with, given
	// its start: the run's own payloads, sealed or not (PayloadsOf).
	// payloadcrypt.ErrKeyless and ErrUnreadable say it never can be.
	ExecutionPayloads(ctx context.Context, runID string, start *historypb.HistoryEvent) (converter.DataConverter, error)
}

// ExecutionPayloads implements ResumeCluster.
func (s *TemporalStarter) ExecutionPayloads(ctx context.Context, runID string, start *historypb.HistoryEvent) (converter.DataConverter, error) {
	if s.payloads == nil {
		return DataConverter(), nil
	}
	return s.payloads(ctx, runID, start)
}

// WithPayloads has what the resume seam reads of a run's history read with
// the run's own payloads (TemporalRunReader.PayloadsOf).
func (s *TemporalStarter) WithPayloads(payloads func(context.Context, string, *historypb.HistoryEvent) (converter.DataConverter, error)) *TemporalStarter {
	s.payloads = payloads
	return s
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
	if info.GetStartTime() != nil {
		out.StartTime = info.GetStartTime().AsTime()
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
	key, carry, err := s.ownResetKey(ctx, runID, executionRunID)
	if _, resume := ParseResumePlanKey(key); err != nil || resume || carry.GetResume() == nil {
		return key, key != "", err
	}
	// It carried on from a resume's: the history that published the resume's
	// first event is the one its reset made.
	first := int64(carry.GetResume().GetSeqBase())
	for _, segment := range carry.GetSegments() {
		if first < segment.GetFirstSequence() || first > segment.GetLastSequence() {
			continue
		}
		earlier, _, err := s.ownResetKey(ctx, runID, segment.GetExecutionId())
		if err != nil && !errors.Is(err, ErrResumeExecutionNotFound) {
			return "", false, err
		}
		if _, resume := ParseResumePlanKey(earlier); resume {
			return earlier, true, nil
		}
	}
	return key, key != "", nil
}

// ownResetKey is the reason of the reset that made one execution, or of the
// newest resume's reset in the history it copied, when its own was made by
// hand: such an execution stays of that resume (startOf says the same). It
// answers the saved state the execution started from too.
func (s *TemporalStarter) ownResetKey(ctx context.Context, runID, executionRunID string) (key string, carry *orkstackv1.RunContinuation, err error) {
	var (
		decodeErr error
		newest    ResumePlanKey
	)
	err = s.pageHistory(ctx, runID, executionRunID, func(page []*historypb.HistoryEvent) bool {
		for _, event := range page {
			if started := event.GetWorkflowExecutionStartedEventAttributes(); started != nil {
				dc, payloadsErr := s.ExecutionPayloads(ctx, runID, event)
				if payloadsErr != nil {
					decodeErr = payloadsErr
					return false
				}
				if carry, decodeErr = startCarry(dc, started); decodeErr != nil {
					return false
				}
				if started.GetOriginalExecutionRunId() == executionRunID {
					return false
				}
			}
			failed := event.GetWorkflowTaskFailedEventAttributes()
			if failed.GetCause() != enumspb.WORKFLOW_TASK_FAILED_CAUSE_RESET_WORKFLOW {
				continue
			}
			reason := failed.GetFailure().GetMessage()
			if plan, ok := ParseResumePlanKey(reason); ok && plan.Scope > newest.Scope {
				newest, key = plan, reason
			}
			if failed.GetNewRunId() == executionRunID {
				key = cmp.Or(key, reason)
				return false
			}
		}
		return true
	})
	return key, carry, cmp.Or(err, decodeErr)
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
