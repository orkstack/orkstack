package engine_test

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
	"time"

	commonpb "go.temporal.io/api/common/v1"
	deploymentpb "go.temporal.io/api/deployment/v1"
	enumspb "go.temporal.io/api/enums/v1"
	failurepb "go.temporal.io/api/failure/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/api/serviceerror"
	workflowpb "go.temporal.io/api/workflow/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/client"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/orkstack/orkstack/internal/engine"
)

// resumeClient is a cluster for the resume seam: it answers Describe,
// GetWorkflowExecutionHistory and ResetWorkflowExecution from what a test
// states, and records every reset it was asked for.
type resumeClient struct {
	client.Client
	service *resumeService
}

func (c *resumeClient) WorkflowService() workflowservice.WorkflowServiceClient { return c.service }

type resumeService struct {
	workflowservice.WorkflowServiceClient
	describe map[string]*workflowservice.DescribeWorkflowExecutionResponse
	history  map[string][]*historypb.HistoryEvent
	// pages counts the history pages each execution was asked for.
	pages map[string]int
	// resetErr is what each reset answers after it is applied: the response
	// lost, as when the cluster applied the reset and the answer never came.
	resetErr       error
	resetRunID     string
	resets         []*workflowservice.ResetWorkflowExecutionRequest
	resetDeadlines []time.Time
}

func (s *resumeService) DescribeWorkflowExecution(_ context.Context, in *workflowservice.DescribeWorkflowExecutionRequest, _ ...grpc.CallOption) (*workflowservice.DescribeWorkflowExecutionResponse, error) {
	resp, ok := s.describe[in.GetExecution().GetRunId()]
	if !ok {
		return nil, serviceerror.NewNotFound("no such execution")
	}
	return resp, nil
}

func (s *resumeService) GetWorkflowExecutionHistory(_ context.Context, in *workflowservice.GetWorkflowExecutionHistoryRequest, _ ...grpc.CallOption) (*workflowservice.GetWorkflowExecutionHistoryResponse, error) {
	events, ok := s.history[in.GetExecution().GetRunId()]
	if !ok {
		return nil, serviceerror.NewNotFound("no such execution")
	}
	if s.pages == nil {
		s.pages = map[string]int{}
	}
	s.pages[in.GetExecution().GetRunId()]++
	// Two pages, so a reader that stops after one is caught.
	half := len(events) / 2
	if len(in.GetNextPageToken()) == 0 {
		return &workflowservice.GetWorkflowExecutionHistoryResponse{
			History: &historypb.History{Events: events[:half]}, NextPageToken: []byte("2"),
		}, nil
	}
	return &workflowservice.GetWorkflowExecutionHistoryResponse{History: &historypb.History{Events: events[half:]}}, nil
}

func (s *resumeService) ResetWorkflowExecution(ctx context.Context, in *workflowservice.ResetWorkflowExecutionRequest, _ ...grpc.CallOption) (*workflowservice.ResetWorkflowExecutionResponse, error) {
	s.resets = append(s.resets, in)
	deadline, _ := ctx.Deadline()
	s.resetDeadlines = append(s.resetDeadlines, deadline)
	if s.resetErr != nil {
		return nil, s.resetErr
	}
	return &workflowservice.ResetWorkflowExecutionResponse{RunId: s.resetRunID}, nil
}

func resumeStarter(svc *resumeService) *engine.TemporalStarter {
	return engine.NewTemporalStarter(&resumeClient{service: svc}, "").WithClusterIdentity("ork-main", "engine-a")
}

func describeResp(execID string, status enumspb.WorkflowExecutionStatus, resetRunID, buildID string) *workflowservice.DescribeWorkflowExecutionResponse {
	resp := &workflowservice.DescribeWorkflowExecutionResponse{
		WorkflowExecutionInfo: &workflowpb.WorkflowExecutionInfo{
			Execution:     &commonpb.WorkflowExecution{WorkflowId: "ork-run", RunId: execID},
			Status:        status,
			CloseTime:     timestamppb.New(time.Date(2026, 11, 20, 12, 0, 0, 0, time.UTC)),
			HistoryLength: 42,
		},
		WorkflowExtendedInfo: &workflowpb.WorkflowExecutionExtendedInfo{ResetRunId: resetRunID},
	}
	if buildID != "" {
		resp.WorkflowExecutionInfo.VersioningInfo = &workflowpb.WorkflowExecutionVersioningInfo{
			DeploymentVersion: &deploymentpb.WorkerDeploymentVersion{DeploymentName: "ork", BuildId: buildID},
		}
	}
	return resp
}

// TestDescribeResumeExecution: a resume reads, of the execution it names (or
// the run's current one), its id, status, close time, history length, the
// run its reset started, and the build it ran on.
func TestDescribeResumeExecution(t *testing.T) {
	svc := &resumeService{describe: map[string]*workflowservice.DescribeWorkflowExecutionResponse{
		"exec-1": describeResp("exec-1", enumspb.WORKFLOW_EXECUTION_STATUS_FAILED, "exec-2", "build-7"),
		"":       describeResp("exec-2", enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING, "", ""),
	}}
	got, err := resumeStarter(svc).DescribeResumeExecution(context.Background(), "ork-run", "exec-1")
	if err != nil {
		t.Fatal(err)
	}
	want := engine.ResumeExecution{ExecutionRunID: "exec-1", Status: enumspb.WORKFLOW_EXECUTION_STATUS_FAILED,
		CloseTime: time.Date(2026, 11, 20, 12, 0, 0, 0, time.UTC), HistoryLength: 42, ResetRunID: "exec-2",
		DeploymentName: "ork", BuildID: "build-7"}
	if got != want {
		t.Fatalf("describe = %+v, want %+v", got, want)
	}
	current, err := resumeStarter(svc).DescribeResumeExecution(context.Background(), "ork-run", "")
	if err != nil || current.ExecutionRunID != "exec-2" || current.ResetRunID != "" {
		t.Fatalf("current = %+v, %v", current, err)
	}
	if _, err := resumeStarter(svc).DescribeResumeExecution(context.Background(), "ork-run", "exec-gone"); !errors.Is(err, engine.ErrResumeExecutionNotFound) {
		t.Fatalf("a missing execution = %v, want ErrResumeExecutionNotFound", err)
	}
}

// TestResumeExecutionHistory: the whole history of one execution, every page.
func TestResumeExecutionHistory(t *testing.T) {
	var events []*historypb.HistoryEvent
	for i := int64(1); i <= 5; i++ {
		events = append(events, &historypb.HistoryEvent{EventId: i})
	}
	svc := &resumeService{history: map[string][]*historypb.HistoryEvent{"exec-1": events}}
	got, err := resumeStarter(svc).ResumeExecutionHistory(context.Background(), "ork-run", "exec-1")
	if err != nil || len(got) != 5 || got[4].GetEventId() != 5 {
		t.Fatalf("history = %d events, %v; want all 5 across both pages", len(got), err)
	}
}

// TestResetKeyOf: the key a reset run was started under is the reason of the
// reset that made it: the workflow-task failure that names it as the new run.
// A run reset again copied its base's history, whose own reset event names
// the base; a run no reset started has none.
func TestResetKeyOf(t *testing.T) {
	reset := func(id int64, newRun, reason string) *historypb.HistoryEvent {
		return &historypb.HistoryEvent{EventId: id, EventType: enumspb.EVENT_TYPE_WORKFLOW_TASK_FAILED,
			Attributes: &historypb.HistoryEvent_WorkflowTaskFailedEventAttributes{WorkflowTaskFailedEventAttributes: &historypb.WorkflowTaskFailedEventAttributes{
				Cause: enumspb.WORKFLOW_TASK_FAILED_CAUSE_RESET_WORKFLOW, NewRunId: newRun, Failure: &failurepb.Failure{Message: reason},
			}}}
	}
	svc := &resumeService{history: map[string][]*historypb.HistoryEvent{
		"reset-run":   {{EventId: 1}, {EventId: 10}, {EventId: 11}, reset(12, "reset-run", "first"), {EventId: 13}},
		"reset-again": {{EventId: 1}, {EventId: 10}, {EventId: 11}, reset(12, "reset-run", "first"), {EventId: 13}, reset(14, "reset-again", "second"), {EventId: 15}},
		"plain-run":   {{EventId: 1}, {EventId: 2}},
	}}
	started := func(original string) *historypb.HistoryEvent {
		return &historypb.HistoryEvent{EventId: 1, EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED,
			Attributes: &historypb.HistoryEvent_WorkflowExecutionStartedEventAttributes{WorkflowExecutionStartedEventAttributes: &historypb.WorkflowExecutionStartedEventAttributes{OriginalExecutionRunId: original}}}
	}
	// A run no reset started names itself as its original: that one event
	// answers, however long its history is.
	svc.history["own-run"] = []*historypb.HistoryEvent{started("own-run"), {EventId: 2}, {EventId: 3}, {EventId: 4}}
	if _, found, err := resumeStarter(svc).ResetKeyOf(context.Background(), "ork-run", "own-run"); err != nil || found || svc.pages["own-run"] != 1 {
		t.Errorf("a run no reset started = found %v, %v after %d pages; want none after one", found, err, svc.pages["own-run"])
	}
	svc.history["reset-run"][0] = started("base-run")
	for run, want := range map[string]string{"reset-run": "first", "reset-again": "second", "plain-run": ""} {
		key, found, err := resumeStarter(svc).ResetKeyOf(context.Background(), "ork-run", run)
		if err != nil || found != (want != "") || key != want {
			t.Errorf("%s: key = %q, %v, %v; want %q", run, key, found, err, want)
		}
	}
}

// TestResetForResume_SentOnce: the reset is sent exactly once, under the
// deadline the caller gives and no retry of its own, even when the cluster
// applied it and the answer was lost; that answer is the ambiguous outcome,
// never a second reset. The request names R's completion, the plan key as
// its reason, excludes every kind of reapply, and pins the base's build.
func TestResetForResume_SentOnce(t *testing.T) {
	for _, lost := range []error{serviceerror.NewUnavailable("connection reset"), context.DeadlineExceeded, errors.New("transport")} {
		svc := &resumeService{resetErr: lost}
		_, err := resumeStarter(svc).ResetForResume(context.Background(), engine.ResumeReset{
			RunID: "ork-run", BaseExecutionRunID: "exec-1", WorkflowTaskFinishEventID: 10,
			Reason: "ork-resume:v1:00000000000000000041.000:abc", Deadline: time.Now().Add(time.Minute),
			DeploymentName: "ork", BuildID: "build-7",
		})
		if !errors.Is(err, engine.ErrResetOutcomeUnknown) {
			t.Fatalf("a lost answer (%v) = %v, want ErrResetOutcomeUnknown", lost, err)
		}
		if len(svc.resets) != 1 {
			t.Fatalf("a lost answer (%v) sent %d resets, want exactly 1", lost, len(svc.resets))
		}
	}
	svc := &resumeService{resetRunID: "exec-2"}
	runID, err := resumeStarter(svc).ResetForResume(context.Background(), engine.ResumeReset{
		RunID: "ork-run", BaseExecutionRunID: "exec-1", WorkflowTaskFinishEventID: 10,
		Reason: "ork-resume:v1:00000000000000000041.000:abc", Deadline: time.Now().Add(time.Minute),
		DeploymentName: "ork", BuildID: "build-7",
	})
	if err != nil || runID != "exec-2" {
		t.Fatalf("reset = %q, %v", runID, err)
	}
	req := svc.resets[0]
	if req.GetNamespace() != "ork-main" || req.GetWorkflowExecution().GetWorkflowId() != "ork-run" ||
		req.GetWorkflowExecution().GetRunId() != "exec-1" || req.GetWorkflowTaskFinishEventId() != 10 ||
		req.GetReason() != "ork-resume:v1:00000000000000000041.000:abc" {
		t.Fatalf("reset request = %v", req)
	}
	if got := req.GetResetReapplyExcludeTypes(); len(got) != 3 {
		t.Fatalf("exclude types = %v, want SIGNAL, UPDATE and NEXUS", got)
	}
	ops := req.GetPostResetOperations()
	if len(ops) != 1 {
		t.Fatalf("post-reset operations = %v, want the build pin", ops)
	}
	pin := ops[0].GetUpdateWorkflowOptions()
	version := pin.GetWorkflowExecutionOptions().GetVersioningOverride().GetPinned().GetVersion()
	if version.GetBuildId() != "build-7" || version.GetDeploymentName() != "ork" ||
		strings.Join(pin.GetUpdateMask().GetPaths(), ",") != "versioning_override" {
		t.Fatalf("build pin = %v", pin)
	}
	// An unversioned base is reset with no pin.
	svc = &resumeService{resetRunID: "exec-2"}
	if _, err := resumeStarter(svc).ResetForResume(context.Background(), engine.ResumeReset{
		RunID: "ork-run", BaseExecutionRunID: "exec-1", WorkflowTaskFinishEventID: 10, Reason: "k", Deadline: time.Now().Add(time.Minute),
	}); err != nil || len(svc.resets[0].GetPostResetOperations()) != 0 {
		t.Fatalf("an unversioned base = %v, %v", svc.resets[0].GetPostResetOperations(), err)
	}
	// A refusal that says the reset did not happen is no ambiguity.
	svc = &resumeService{resetErr: serviceerror.NewInvalidArgument("bad event id")}
	if _, err := resumeStarter(svc).ResetForResume(context.Background(), engine.ResumeReset{
		RunID: "ork-run", BaseExecutionRunID: "exec-1", WorkflowTaskFinishEventID: 10, Reason: "k", Deadline: time.Now().Add(time.Minute),
	}); err == nil || errors.Is(err, engine.ErrResetOutcomeUnknown) {
		t.Fatalf("an invalid-argument refusal = %v, want a plain error", err)
	}
}

// TestResetForResume_DeadlineIsTheRecordedInstant: the deadline the request
// carries to the cluster is the recorded one to the nanosecond, on the wall
// clock it was recorded by. A deadline keeping its monotonic reading would
// stand still while the engine's host is suspended and be sent as unexpired
// long after the instant every other engine compares with.
func TestResetForResume_DeadlineIsTheRecordedInstant(t *testing.T) {
	deadline := time.Now().Add(time.Minute)
	svc := &resumeService{resetRunID: "exec-2"}
	if _, err := resumeStarter(svc).ResetForResume(context.Background(), engine.ResumeReset{
		RunID: "ork-run", BaseExecutionRunID: "exec-1", WorkflowTaskFinishEventID: 10, Reason: "k", Deadline: deadline,
	}); err != nil {
		t.Fatalf("reset: %v", err)
	}
	// == compares the monotonic reading too: the sent one has none.
	if got := svc.resetDeadlines[0]; got != deadline.Round(0) {
		t.Fatalf("the cluster was sent deadline %v, want %v", got, deadline.Round(0))
	}
}

// TestNoRetryingResetOutsideResumeCluster: the SDK client's
// ResetWorkflowExecution retries a lost answer and so resets a run again
// (spike T24); nothing but the resume seam resets a run, and that seam calls
// the service directly, once.
func TestNoRetryingResetOutsideResumeCluster(t *testing.T) {
	root := filepath.Join("..", "..")
	call := regexp.MustCompile(`\.ResetWorkflowExecution\(`)
	direct := regexp.MustCompile(`WorkflowService\(\)\.ResetWorkflowExecution\(`)
	err := filepath.WalkDir(root, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() && (d.Name() == "gen" || d.Name() == "node_modules" || strings.HasPrefix(d.Name(), ".")) && path != root {
			return filepath.SkipDir
		}
		if d.IsDir() || !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		src, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		for _, line := range strings.Split(string(src), "\n") {
			if !call.MatchString(line) {
				continue
			}
			if !direct.MatchString(line) || filepath.Base(path) != "resume_cluster.go" {
				t.Errorf("%s resets a run outside the resume seam: %s", path, strings.TrimSpace(line))
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}
