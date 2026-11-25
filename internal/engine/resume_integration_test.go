//go:build integration

package engine_test

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/worker"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/durationpb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/internal/testsupport/must"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// A failed run resumed by a real reset of its closed execution (E85 PR4a):
// the reset's reason names a plan record; the reset run finds its reset in
// its own history, loads and verifies the record, keeps build from its
// commit without running it, and runs deploy again under the resume's key.
func TestIntegration_AResumedRunKeepsWhatSucceededAndRunsTheRestAgain(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	commits := artifacts.NewMemoryCommitStore()
	var mu sync.Mutex
	var dispatched []string
	behavior := func(actx context.Context, s *canStack, in *orkstackv1.ExecuteTaskInput) (*orkstackv1.ExecuteTaskResult, error) {
		mu.Lock()
		dispatched = append(dispatched, in.GetIdempotencyKey())
		mu.Unlock()
		if in.GetTaskId() == "deploy" && !strings.Contains(in.GetIdempotencyKey(), "@r") {
			return nil, temporal.NewNonRetryableApplicationError("deploy failed", "ActivityFailed", nil)
		}
		commit := &orkstackv1.ActivityCommit{
			SchemaVersion: artifacts.CommitSchemaVersion, Namespace: artifacts.ResolveNamespace(in.GetNamespace()),
			RunId: in.GetRunId(), TaskId: in.GetTaskId(), OrkflowDefinitionDigest: in.GetOrkflowDefinitionDigest(),
			IdempotencyKey: in.GetIdempotencyKey(), Image: in.GetImage(), Attempt: 1, OrkerId: "orker-it",
		}
		record, err := commits.Create(actx, in.GetCommitUri(), commit)
		if err != nil {
			return nil, err
		}
		result := succeeded(in.GetTaskId(), map[string]any{"ok": true})
		result.Commit = &orkstackv1.CommitRef{Uri: in.GetCommitUri(), Sha256: record.SHA256}
		return result, nil
	}
	records, err := artifacts.NewRecordStore(ctx, s3Config())
	must.OKf(t, err, "record store")
	var s *canStack
	s = startCANStack(t, ctx, "resume", false, behavior, func(w worker.Worker) {
		acts := engine.NewResumeActivities(commits, nil).WithHistory(lazyHistory{&s}, lazyNamespace(&s), records)
		w.RegisterActivityWithOptions(acts.ReadResetFacts, activity.RegisterOptions{Name: engine.ActivityReadResetFacts})
		w.RegisterActivityWithOptions(acts.LoadResumePlan, activity.RegisterOptions{Name: engine.ActivityLoadResumePlan})
		w.RegisterActivityWithOptions(acts.AdoptRecorded, activity.RegisterOptions{Name: engine.ActivityAdoptRecorded})
	})

	// The base: build succeeds, deploy fails.
	runID := fmt.Sprintf("ork-resume-it-%d", time.Now().UnixNano())
	plan := s.plan(10_000, nil)
	must.OKf(t, s.starter.StartRun(ctx, runID, compile(t, resumeTwoTaskDocument), nil, plan), "StartRun")
	base := s.client.GetWorkflow(ctx, runID, "")
	if err := base.Get(ctx, nil); err == nil {
		t.Fatal("the base run succeeded; deploy was meant to fail")
	}
	baseExecution := base.GetRunID()
	recorded := s.history(runID, baseExecution)
	scanned, err := engine.ScanResumeBase(recorded.GetEvents(), converter.GetDefaultDataConverter())
	must.OKf(t, err, "scan the base")
	var terminal int64
	for _, e := range s.bus.Events(runID, 0) {
		terminal = max(terminal, e.GetSequence())
	}
	if terminal < 3 {
		t.Fatalf("the base published up to sequence %d", terminal)
	}

	// The plan ResumeRun would record, and the identity record that makes it
	// the newest resume.
	scope := uint64(terminal + 1)
	record := &orkstackv1.ResumePlanRecord{
		Format: artifacts.ResumePlanFormat, Namespace: "default", RunId: runID, Scope: scope, N: 0,
		BaseExecutionRunId: baseExecution,
		RScheduledEventId:  scanned.ResetPoint.ScheduledEventID, RStartedEventId: scanned.ResetPoint.StartedEventID,
		BaseTerminalSequence: terminal, Principal: "ops@example.com", Reason: "integration",
		Block: &orkstackv1.ResumeBlock{
			SeqBase: scope, Scope: scope, Action: orkstackv1.ResumeAction_RESUME_ACTION_RESUME,
			Rerun:        []*orkstackv1.ResumeUnitRange{{TaskId: "deploy", FirstItem: -1}},
			Dispositions: []*orkstackv1.ResumeUnitDisposition{{TaskId: "build", Item: -1, Disposition: orkstackv1.ResumeDisposition_RESUME_DISPOSITION_ADOPT, EncodedSize: 1024}},
			Readback: &orkstackv1.ResumeReadback{
				Deadline: durationpb.New(2 * time.Minute), Budget: 128 << 10, MaxEntries: 128, Concurrency: 4,
				GetTimeout: durationpb.New(10 * time.Second), GetTries: 3, CallTimeout: durationpb.New(30 * time.Second),
				RetryInitial: durationpb.New(time.Second), RetryCoefficient: 2, CallsPerTask: 4,
			},
		},
	}
	digest, err := records.WriteResumePlan(ctx, record)
	must.OKf(t, err, "write the plan")
	must.OKf(t, records.WriteResume(ctx, artifacts.ResumeRecord{Namespace: "default", RunID: runID, Scope: scope, N: 0, Tasks: []string{"deploy"}}),
		"write the identity record")

	// The one reset, with the plan key as its reason.
	key := engine.ResumePlanKey{Scope: scope, N: 0, PlanDigest: digest}
	reset, err := s.client.WorkflowService().ResetWorkflowExecution(ctx, &workflowservice.ResetWorkflowExecutionRequest{
		Namespace:                 s.namespace,
		WorkflowExecution:         &commonpb.WorkflowExecution{WorkflowId: runID, RunId: baseExecution},
		Reason:                    key.String(),
		WorkflowTaskFinishEventId: scanned.ResetPoint.CompletedEventID,
		RequestId:                 uuid.NewString(),
		// Nothing the base received after R is applied again.
		ResetReapplyExcludeTypes: []enumspb.ResetReapplyExcludeType{
			enumspb.RESET_REAPPLY_EXCLUDE_TYPE_SIGNAL,
			enumspb.RESET_REAPPLY_EXCLUDE_TYPE_UPDATE,
			enumspb.RESET_REAPPLY_EXCLUDE_TYPE_NEXUS,
		},
	})
	must.OKf(t, err, "reset the base to R")
	if err := s.client.GetWorkflow(ctx, runID, reset.GetRunId()).Get(ctx, nil); err != nil {
		t.Fatalf("the resumed run failed: %v", err)
	}

	mu.Lock()
	defer mu.Unlock()
	var builds, reruns int
	for _, k := range dispatched {
		switch {
		case strings.HasSuffix(k, "/build"):
			builds++
		case strings.HasSuffix(k, fmt.Sprintf("/deploy@r%d", scope)):
			reruns++
		}
	}
	if builds != 1 || reruns != 1 {
		t.Fatalf("dispatched %v: want build once (the base only) and deploy once under scope %d", dispatched, scope)
	}
	var resumed bool
	for _, e := range s.bus.Events(runID, 0) {
		if e.GetType() == orkstackv1.EventType_EVENT_TYPE_RUN_RESUMED {
			resumed = e.GetSequence() == int64(scope) && e.GetResumed().GetPrincipal() == "ops@example.com"
		}
	}
	if !resumed {
		t.Fatalf("no RUN_RESUMED at %d", scope)
	}
}

// lazyHistory reads through the stack's client once the stack exists: the
// run worker's activities are registered while the stack is being built.
type lazyHistory struct{ s **canStack }

func (l lazyHistory) GetWorkflowExecutionHistory(ctx context.Context, in *workflowservice.GetWorkflowExecutionHistoryRequest,
	opts ...grpc.CallOption,
) (*workflowservice.GetWorkflowExecutionHistoryResponse, error) {
	in.Namespace = (*l.s).namespace
	return (*l.s).client.WorkflowService().GetWorkflowExecutionHistory(ctx, in, opts...)
}

func lazyNamespace(**canStack) string { return "" }
