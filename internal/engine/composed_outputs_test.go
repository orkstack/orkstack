package engine_test

import (
	"errors"
	"slices"
	"strings"
	"testing"

	"go.temporal.io/sdk/workflow"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/internal/testsupport/must"
)

const returningChildDocument = `
apiVersion: orkstack/v1
kind: Orkflow
name: train
version: "1.0.0"
outputs:
  model:
    artifact: fit/model.bin
tasks:
  - id: fit
    uses: shell
    with: { op: exec, command: ["fit"] }
`

const consumingParentDocument = `
apiVersion: orkstack/v1
kind: Orkflow
name: release-model
version: "4.1.0"
tasks:
  - id: train
    uses: orkflow:train@1.0.0
  - id: publish
    uses: shell
    needs: [train]
    with: { op: exec, command: ["publish", '{{ artifact "train/model" }}'] }
`

// returningFixture is a parent whose composed task's child returns model.
func returningFixture(t *testing.T) (*orkstackv1.Orkflow, engine.RunPlan) {
	t.Helper()
	parent := compile(t, consumingParentDocument)
	for _, task := range parent.GetTasks() {
		if task.GetOrkflow() != nil {
			task.Orkflow.DefinitionDigest = childDigest
		}
	}
	child := compile(t, returningChildDocument)
	child.DefinitionDigest = childDigest
	childPlan := defaultPlan()
	childPlan.CommitRoot = "mem://commits"
	cp, err := engine.NewChildRunPlan(child, childPlan, true, true)
	must.OK(t, err)
	cp.RunID = "ork-child"
	plan := defaultPlan()
	plan.CommitRoot = "mem://commits"
	plan.Children = map[string]*engine.ChildRunPlan{"train": cp}
	claims := engine.DefaultArtifactClaims
	plan.ArtifactClaims = &claims
	return parent, plan
}

func childReturning(uri string) func(workflow.Context, childStart) (*orkstackv1.Run, error) {
	return func(_ workflow.Context, start childStart) (*orkstackv1.Run, error) {
		return &orkstackv1.Run{Id: start.runID, Status: orkstackv1.Status_STATUS_SUCCEEDED,
			OutputArtifacts: []*orkstackv1.NamedArtifact{{Name: "model", RunId: start.runID, TaskId: "fit",
				Artifact: &orkstackv1.ArtifactRef{Name: "model", Uri: uri, Sha256: "cd",
					Metadata: map[string]string{engine.ArtifactSourceRunKey: start.runID}}}}}, nil
	}
}

func TestRunWorkflow_ChildArtifactOutputsBecomeComposedArtifacts(t *testing.T) {
	uri := storedDataset(t)
	rec := &artifactClaimLog{}
	h := newComposeHarness(t, func(hh *composeHarness, in *orkstackv1.ExecuteTaskInput) (*orkstackv1.ExecuteTaskResult, error) {
		rec.note("task " + in.GetTaskId())
		return defaultBehavior(hh, in)
	})
	registerClaims(h, rec)
	parent, plan := returningFixture(t)
	h.mockChild(childReturning(uri))
	h.run("ork-parent", parent, payload(t, nil), plan)
	must.OKf(t, h.env.GetWorkflowError(), "workflow error")

	// The parent claims what its child returned before any task uses it.
	if events, want := rec.events(), []string{"claim ork-parent", "task publish"}; !slices.Equal(events, want) {
		t.Fatalf("order = %v, want %v", events, want)
	}
	succeeded := taskRunOfRun(h.snapshotAt(t, orkstackv1.EventType_EVENT_TYPE_TASK_SUCCEEDED, "train"), "train")
	if got := succeeded.GetArtifacts(); len(got) != 1 || got[0].GetName() != "model" || got[0].GetUri() != uri {
		t.Fatalf("train's artifacts = %v, want the child's model, the same object", got)
	}
	var fromTrain *orkstackv1.UpstreamRef
	for _, ref := range h.inputFor("publish").GetUpstream() {
		if ref.GetTaskId() == "train" {
			fromTrain = ref
		}
	}
	if fromTrain == nil || len(fromTrain.GetArtifacts()) != 1 || fromTrain.GetArtifacts()[0].GetName() != "model" {
		t.Fatalf("publish's upstream train = %v, want its model artifact", fromTrain)
	}
}

func TestRunWorkflow_ReturnedArtifactsUnclaimedFailsTheTask(t *testing.T) {
	uri := storedDataset(t)
	rec := &artifactClaimLog{claimErr: errors.New("bucket read-only")}
	h := newComposeHarness(t, func(hh *composeHarness, in *orkstackv1.ExecuteTaskInput) (*orkstackv1.ExecuteTaskResult, error) {
		t.Errorf("task %s ran with unclaimed artifacts", in.GetTaskId())
		return defaultBehavior(hh, in)
	})
	registerClaims(h, rec)
	parent, plan := returningFixture(t)
	h.mockChild(childReturning(uri))
	h.run("ork-parent", parent, payload(t, nil), plan)
	if h.env.GetWorkflowError() == nil {
		t.Fatal("the run succeeded")
	}
	failed := taskRunOfRun(h.snapshotAt(t, orkstackv1.EventType_EVENT_TYPE_TASK_FAILED, "train"), "train")
	if failed.GetError().GetCode() != engine.ChildOutputsUnclaimedCode ||
		failed.GetError().GetCategory() != orkstackv1.FailureCategory_FAILURE_CATEGORY_INFRASTRUCTURE_ERROR ||
		!strings.Contains(failed.GetError().GetMessage(), "bucket read-only") {
		t.Fatalf("train error = %v", failed.GetError())
	}
	// The row lists what it may have claimed, so the archive releases it.
	if got := failed.GetArtifacts(); len(got) != 1 || got[0].GetUri() != uri {
		t.Fatalf("the failed row lists %v, want the object it tried to claim", got)
	}
}

func TestRunWorkflow_OutputArtifactsOfASucceededRun(t *testing.T) {
	uri := storedDataset(t)
	h := newComposeHarness(t, func(hh *composeHarness, in *orkstackv1.ExecuteTaskInput) (*orkstackv1.ExecuteTaskResult, error) {
		result, err := defaultBehavior(hh, in)
		result.Artifacts = []*orkstackv1.ArtifactRef{{Name: "model.bin", Uri: uri, Sha256: "cd"}}
		return result, err
	})
	h.run("ork-child", compile(t, returningChildDocument), payload(t, nil), defaultPlan())
	must.OKf(t, h.env.GetWorkflowError(), "workflow error")
	final := h.snapshotAt(t, orkstackv1.EventType_EVENT_TYPE_RUN_SUCCEEDED, "")
	got := final.GetOutputArtifacts()
	if len(got) != 1 || got[0].GetName() != "model" || got[0].GetArtifact().GetUri() != uri ||
		got[0].GetArtifact().GetName() != "model" || got[0].GetTaskId() != "fit" ||
		got[0].GetArtifact().GetMetadata()[engine.ArtifactSourceNameKey] != "model.bin" {
		t.Fatalf("Run.output_artifacts = %v", got)
	}
}

func TestRunWorkflow_OutputArtifactMissingFailsRun(t *testing.T) {
	h := newComposeHarness(t, defaultBehavior)
	h.run("ork-child", compile(t, returningChildDocument), payload(t, nil), defaultPlan())
	if h.env.GetWorkflowError() == nil {
		t.Fatal("the run succeeded without the artifact it returns")
	}
	final := h.snapshotAt(t, orkstackv1.EventType_EVENT_TYPE_RUN_FAILED, "")
	if final.GetError().GetCode() != engine.OutputsArtifactMissingCode ||
		!strings.Contains(final.GetError().GetMessage(), `outputs.model: task "fit" recorded no artifact "model.bin"`) {
		t.Fatalf("Run.error = %v", final.GetError())
	}
}
