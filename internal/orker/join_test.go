package orker

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"google.golang.org/protobuf/types/known/structpb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// TestWorkspace_TaskJSONCarriesJoinOnlyForAFailureHandler is §3.2's additive
// rule for `join`: present with its documented shape when the input carries a
// JoinContext, absent as a key otherwise, and schemaVersion stays 1.
func TestWorkspace_TaskJSONCarriesJoinOnlyForAFailureHandler(t *testing.T) {
	base := func() (*orkstackv1.ExecuteTaskInput, TaskContext) {
		return &orkstackv1.ExecuteTaskInput{
			RunId: "ork-1", OrkflowName: "o", OrkflowVersion: "1", TaskId: "rollback",
			Limits: testLimits(),
		}, TaskContext{
			Attempt: 1, Pool: "main", Namespace: "default", Network: NetworkNone,
			Image: &orkstackv1.ImageRef{Reference: "shell", Pinned: "x@sha256:y", IndexDigest: "sha256:y"},
		}
	}
	read := func(t *testing.T, ws *Workspace) map[string]any {
		t.Helper()
		raw, err := os.ReadFile(filepath.Join(ws.Input, "task.json"))
		if err != nil {
			t.Fatalf("read task.json: %v", err)
		}
		var doc map[string]any
		if err := json.Unmarshal(raw, &doc); err != nil {
			t.Fatalf("task.json is not valid JSON: %v\n%s", err, raw)
		}
		return doc
	}

	t.Run("an ordinary task has no join key", func(t *testing.T) {
		ws := newTestWorkspace(t)
		in, tc := base()
		if err := ws.WriteTaskJSON(in, tc); err != nil {
			t.Fatalf("WriteTaskJSON: %v", err)
		}
		doc := read(t, ws)
		if _, present := doc["join"]; present {
			t.Fatalf("join is present for a task that is not a failure handler: %v", doc["join"])
		}
		if doc["schemaVersion"] != float64(1) {
			t.Fatalf("schemaVersion = %v", doc["schemaVersion"])
		}
	})

	t.Run("a failure handler carries its needs' outcomes", func(t *testing.T) {
		ws := newTestWorkspace(t)
		in, tc := base()
		in.Join = &orkstackv1.JoinContext{
			Policy: orkstackv1.JoinPolicy_JOIN_POLICY_ANY_FAILED,
			Needs: []*orkstackv1.NeedOutcome{
				{TaskId: "deploy", Status: orkstackv1.Status_STATUS_FAILED, Error: &orkstackv1.Error{
					Code: "TASK_FAILED", Message: "container exited with status 1", Details: `task "deploy"`,
					Category: orkstackv1.FailureCategory_FAILURE_CATEGORY_ACTIVITY_FAILED,
				}},
				{TaskId: "smoke", Status: orkstackv1.Status_STATUS_CANCELED, Error: &orkstackv1.Error{
					Code: "TASK_CANCELED", Message: "canceled",
					Category: orkstackv1.FailureCategory_FAILURE_CATEGORY_CANCELLED,
				}},
				{TaskId: "verify", Status: orkstackv1.Status_STATUS_SUCCEEDED},
				{TaskId: "canary", Status: orkstackv1.Status_STATUS_SKIPPED, Error: &orkstackv1.Error{Code: "X", Message: "m"}},
			},
		}
		if err := ws.WriteTaskJSON(in, tc); err != nil {
			t.Fatalf("WriteTaskJSON: %v", err)
		}
		doc := read(t, ws)
		want := map[string]any{
			"policy": "any_failed",
			"needs": []any{
				map[string]any{"taskId": "deploy", "status": "FAILED", "error": map[string]any{
					"code": "TASK_FAILED", "category": "ActivityFailed",
					"message": "container exited with status 1", "details": `task "deploy"`,
				}},
				map[string]any{"taskId": "smoke", "status": "CANCELED", "error": map[string]any{
					"code": "TASK_CANCELED", "category": "Cancelled", "message": "canceled",
				}},
				map[string]any{"taskId": "verify", "status": "SUCCEEDED"},
				map[string]any{"taskId": "canary", "status": "SKIPPED", "error": map[string]any{
					"code": "X", "message": "m",
				}},
			},
		}
		if !reflect.DeepEqual(doc["join"], any(want)) {
			got, _ := json.Marshal(doc["join"])
			t.Fatalf("join = %s", got)
		}
		if doc["schemaVersion"] != float64(1) {
			t.Fatalf("schemaVersion = %v", doc["schemaVersion"])
		}
	})
}

// TestStageInputs_AFailedNeedsArtifactsStageWithoutValues: a failed need's
// UpstreamRef carries artifacts and no values; staging puts the files under
// input/artifacts/<need>/, lists them in inputs.artifacts, writes no
// inputs.upstream entry, and the artifact helper resolves them.
func TestStageInputs_AFailedNeedsArtifactsStageWithoutValues(t *testing.T) {
	h := newCacheHarness(t)
	log := h.put(t, "deploy.log", []byte("deploying\ndeploy failed\n"))
	in := cachedTaskInput(t, "ork-join")
	in.Cache = nil
	in.TaskId = "rollback"
	with, _ := structpb.NewStruct(map[string]any{"log": `{{ artifact "deploy/deploy.log" }}`})
	in.With = with
	in.Upstream = []*orkstackv1.UpstreamRef{{TaskId: "deploy", Artifacts: []*orkstackv1.ArtifactRef{log}}}
	ws, err := NewWorkspace(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	staged, failure := h.dispatcher.stageInputs(context.Background(), in, ws, false)
	if failure != nil {
		t.Fatal(failure)
	}
	b, err := os.ReadFile(filepath.Join(ws.Input, "artifacts", "deploy", "deploy.log"))
	if err != nil || string(b) != "deploying\ndeploy failed\n" {
		t.Fatalf("staged deploy.log = %q, %v", b, err)
	}
	if _, err := os.Stat(filepath.Join(ws.Input, "upstream", "deploy", "values.json")); !os.IsNotExist(err) {
		t.Fatalf("a values.json was staged for a failed need: %v", err)
	}
	if len(staged.upstream) != 0 {
		t.Fatalf("inputs.upstream = %+v, want none for a failed need", staged.upstream)
	}
	if len(staged.artifacts) != 1 || staged.artifacts[0].Producer != "deploy" || staged.artifacts[0].Name != "deploy.log" {
		t.Fatalf("inputs.artifacts = %+v", staged.artifacts)
	}
	rendered, rf := h.dispatcher.render(context.Background(), in, staged, nil)
	if rf != nil {
		t.Fatal(rf)
	}
	if rendered["log"] != staged.artifacts[0].Path || rendered["log"] != ContainerInputArtifacts+"/deploy/deploy.log" {
		t.Fatalf("artifact helper = %v, want %s", rendered["log"], staged.artifacts[0].Path)
	}
}

func TestDescribeOrker_ReportsJoinSupported(t *testing.T) {
	store, err := artifacts.NewLocalStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	activities := NewActivitiesWithOptions(store, Options{Runtime: &reapRecorder{}, WorkRoot: t.TempDir()})
	result, err := activities.DescribeOrker(context.Background())
	if err != nil {
		t.Fatalf("DescribeOrker: %v", err)
	}
	if !result.GetJoinSupported() {
		t.Fatal("this build reads ExecuteTaskInput.join and must say so")
	}
}
