package engine_test

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
	"google.golang.org/protobuf/proto"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/internal/task"
)

func TestImagePolicySelection_ProtoIsNilSafe(t *testing.T) {
	var none *engine.ImagePolicySelection
	if none.Proto() != nil {
		t.Fatal("a nil selection must render a nil message")
	}
	if engine.ImagePolicySelectionFromProto(nil) != nil {
		t.Fatal("a nil message must read back as a nil selection")
	}
	sel := &orkstackv1.ImagePolicySelection{
		RequireDigest: true, AllowedRegistries: []string{"ghcr.io/acme/"},
		RequireSignedBy: "cosign:release", SigningKeyPem: task.FixedSigningKeyPEM,
	}
	if back := engine.ImagePolicySelectionFromProto(sel).Proto(); !proto.Equal(back, sel) {
		t.Fatalf("round trip = %v, want %v", back, sel)
	}
	// A plan with none encodes exactly as before the field existed.
	raw, err := json.Marshal(engine.RunPlan{Namespace: "default"})
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(raw), "image_policy") {
		t.Fatalf("a plan with no policy encodes image_policy: %s", raw)
	}
}

// policyPlanSelection is the selection TestRunWorkflow_ReplaysAPolicyCarryingPlan
// pins: every field set, so nothing can be dropped unnoticed.
func policyPlanSelection() *engine.ImagePolicySelection {
	return &engine.ImagePolicySelection{
		RequireDigest:     true,
		AllowedRegistries: []string{"localhost:5001/orkstack/"},
		RequireSignedBy:   "cosign:release",
		SigningKeyPEM:     task.FixedSigningKeyPEM,
	}
}

// withPolicy rewrites the committed golden history IN MEMORY as the history
// the same run would have recorded had it been submitted under an image
// policy: the plan argument carries the selection, and every ExecuteTask
// input carries it as image_policy. The fixture on disk is not touched.
func withPolicy(t *testing.T, recorded *historypb.History, sel *engine.ImagePolicySelection) *historypb.History {
	t.Helper()
	history := proto.Clone(recorded).(*historypb.History)
	dc := converter.GetDefaultDataConverter()
	for _, event := range history.GetEvents() {
		if attrs := event.GetWorkflowExecutionStartedEventAttributes(); attrs != nil {
			var (
				runID string
				wf    *orkstackv1.Orkflow
				input *orkstackv1.Payload
				plan  engine.RunPlan
			)
			if err := dc.FromPayloads(attrs.GetInput(), &runID, &wf, &input, &plan); err != nil {
				t.Fatalf("decode the recorded workflow input: %v", err)
			}
			plan.ImagePolicy = sel
			encoded, err := dc.ToPayloads(runID, wf, input, plan)
			if err != nil {
				t.Fatal(err)
			}
			attrs.Input = encoded
		}
		if attrs := event.GetActivityTaskScheduledEventAttributes(); attrs != nil &&
			attrs.GetActivityType().GetName() == task.ActivityExecuteTask {
			var in *orkstackv1.ExecuteTaskInput
			if err := dc.FromPayloads(attrs.GetInput(), &in); err != nil {
				t.Fatalf("decode a recorded ExecuteTask input: %v", err)
			}
			in.ImagePolicy = sel.Proto()
			encoded, err := dc.ToPayloads(in)
			if err != nil {
				t.Fatal(err)
			}
			attrs.Input = encoded
		}
	}
	return history
}

// TestRunWorkflow_ReplaysAPolicyCarryingPlan: a plan carrying an image policy
// selection (require_digest, one registry, a signer and its PEM) reaches every
// ExecuteTaskInput byte-equal to the plan's, and the history such a run
// records replays against current code. The negative: the committed golden
// fixture was recorded with no policy, and it is read here unmodified — the
// plain replay (TestRunWorkflow_ReplaysCommittedV1History) and payload guard
// (TestGoldenHistory_ActivityPayloadsMatchWhatCurrentCodeSends) keep passing
// over it without a re-recording.
func TestRunWorkflow_ReplaysAPolicyCarryingPlan(t *testing.T) {
	raw, err := os.ReadFile(goldenHistory)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(raw), "imagePolicy") || strings.Contains(string(raw), "image_policy") {
		t.Fatal("the committed golden fixture carries an image policy; it was recorded with none and must not be re-recorded")
	}
	recorded := loadGoldenHistory(t)
	sel := policyPlanSelection()
	history := withPolicy(t, recorded, sel)

	// 1. The policy-carrying history replays: the selection is data on the
	// plan and changes no command.
	replayer := worker.NewWorkflowReplayer()
	replayer.RegisterWorkflowWithOptions(engine.RunWorkflow, workflow.RegisterOptions{Name: engine.RunWorkflowType})
	if err := replayer.ReplayWorkflowHistory(nil, history); err != nil {
		t.Fatalf("replaying a policy-carrying history failed: %v", err)
	}

	// 2. Re-executed from that history's own input, every ExecuteTaskInput
	// carries the plan's selection, byte for byte, and nothing else changed.
	runID, wf, runInput, plan := goldenWorkflowArgs(t, history)
	if plan.ImagePolicy == nil {
		t.Fatal("the rewritten plan lost its selection")
	}
	recordedTaskInputs, _, recordedResults, _ := goldenActivityPayloads(t, history)
	var (
		mu   sync.Mutex
		sent []*orkstackv1.ExecuteTaskInput
	)
	suite := &testsuite.WorkflowTestSuite{}
	env := generousDeadlock(suite.NewTestWorkflowEnvironment())
	env.SetTestTimeout(60 * time.Second)
	env.RegisterWorkflowWithOptions(engine.RunWorkflow, workflow.RegisterOptions{Name: engine.RunWorkflowType})
	env.RegisterActivityWithOptions(
		func(_ context.Context, in *orkstackv1.ExecuteTaskInput) (*orkstackv1.ExecuteTaskResult, error) {
			mu.Lock()
			defer mu.Unlock()
			sent = append(sent, in)
			result, ok := recordedResults[in.TaskId]
			if !ok {
				return nil, fmt.Errorf("task %q was never executed by the recorded run", in.TaskId)
			}
			return result, nil
		},
		activity.RegisterOptions{Name: task.ActivityExecuteTask},
	)
	env.RegisterActivityWithOptions(
		func(_ context.Context, run *orkstackv1.Run, sequence int64) (string, error) {
			return fmt.Sprintf("memory://snapshots/%s/%d.pb", run.GetId(), sequence), nil
		},
		activity.RegisterOptions{Name: engine.ActivityWriteSnapshot},
	)
	env.RegisterActivityWithOptions(
		func(context.Context, *orkstackv1.Event, string, string) error { return nil },
		activity.RegisterOptions{Name: engine.ActivityPublishEvent},
	)
	env.ExecuteWorkflow(engine.RunWorkflow, runID, wf, runInput, plan, (*orkstackv1.RunContinuation)(nil))
	if !env.IsWorkflowCompleted() || env.GetWorkflowError() != nil {
		t.Fatalf("re-execution did not complete: %v", env.GetWorkflowError())
	}
	if len(sent) == 0 {
		t.Fatal("no task was dispatched")
	}
	want := sel.Proto()
	wantBytes, _ := proto.Marshal(want)
	for _, in := range sent {
		gotBytes, _ := proto.Marshal(in.GetImagePolicy())
		if !proto.Equal(in.GetImagePolicy(), want) || string(gotBytes) != string(wantBytes) {
			t.Errorf("task %q image_policy = %v, want the plan's %v", in.GetTaskId(), in.GetImagePolicy(), want)
		}
	}
	sentInputs := map[string]map[string]any{}
	for _, in := range sent {
		sentInputs[in.TaskId] = canonicalPayload(t, in)
	}
	compareTaskInputs(t, recordedTaskInputs, sentInputs)
}
