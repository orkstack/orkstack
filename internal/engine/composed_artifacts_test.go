package engine_test

import (
	"context"
	"errors"
	"slices"
	"strings"
	"sync"
	"testing"

	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/workflow"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/internal/testsupport/must"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

const artifactParentDocument = `
apiVersion: orkstack/v1
kind: Orkflow
name: release-model
version: "4.1.0"
tasks:
  - id: build
    uses: shell
    with: { op: eval, command: ["true"] }
  - id: train
    uses: orkflow:train@1.0.0
    needs: [build]
    artifacts:
      dataset: build/dataset.tar.gz
`

const artifactChildDocument = `
apiVersion: orkstack/v1
kind: Orkflow
name: train
version: "1.0.0"
inputs:
  dataset: { type: artifact, required: true }
tasks:
  - id: fit
    uses: shell
    with: { op: exec, command: ["fit", '{{ artifact "_inputs/dataset" }}'] }
  - id: report
    uses: shell
    needs: [fit]
    with: { op: exec, command: ["report"] }
`

// storedDataset puts dataset.tar.gz in a local store of the test's own and
// returns its URI: a canonical object key, so its object id resolves as a
// real one does.
func storedDataset(t *testing.T) string {
	t.Helper()
	store, err := artifacts.NewLocalStore(t.TempDir())
	must.OK(t, err)
	ref, err := store.Put(context.Background(), "dataset.tar.gz", strings.NewReader("dataset"), artifacts.PutOptions{})
	must.OK(t, err)
	return ref.URI
}

// artifactClaimLog is the store and the archive the claim activities stat and
// write markers through, recording every call in order with the child starts
// and the task dispatches, so a test can assert what came first. statErr and
// claimErr, when set, fail every stat or claim: faults a real store cannot
// produce on demand.
type artifactClaimLog struct {
	mu       sync.Mutex
	order    []string
	statErr  error
	claimErr error
}

func (r *artifactClaimLog) note(s string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.order = append(r.order, s)
}

func (r *artifactClaimLog) events() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string(nil), r.order...)
}

func (r *artifactClaimLog) Stat(_ context.Context, uri string) (artifacts.ArtifactRef, error) {
	r.note("stat " + uri)
	if r.statErr != nil {
		return artifacts.ArtifactRef{}, r.statErr
	}
	return artifacts.ArtifactRef{URI: uri}, nil
}

func (r *artifactClaimLog) ClaimObjects(_ context.Context, runID string, _ []string) error {
	r.note("claim " + runID)
	return r.claimErr
}

func registerClaims(h *composeHarness, rec *artifactClaimLog) {
	claims := engine.NewArtifactClaimActivities(rec, rec)
	h.env.RegisterActivityWithOptions(claims.StatAndClaimChildInputs,
		activity.RegisterOptions{Name: engine.ActivityStatAndClaimChildInputs})
	h.env.RegisterActivityWithOptions(claims.ClaimReceivedArtifacts,
		activity.RegisterOptions{Name: engine.ActivityClaimReceivedArtifacts})
	h.env.RegisterActivityWithOptions(claims.ClaimReturnedArtifacts,
		activity.RegisterOptions{Name: engine.ActivityClaimReturnedArtifacts})
}

// artifactFixture is the parent and its plan: build records dataset.tar.gz,
// and train passes it to its child's dataset input.
func artifactFixture(t *testing.T) (*orkstackv1.Orkflow, engine.RunPlan) {
	t.Helper()
	parent := compile(t, artifactParentDocument)
	for _, task := range parent.GetTasks() {
		if task.GetOrkflow() != nil {
			task.Orkflow.DefinitionDigest = childDigest
		}
	}
	child := compile(t, artifactChildDocument)
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

func buildRecords(name, uri string) func(*composeHarness, *orkstackv1.ExecuteTaskInput) (*orkstackv1.ExecuteTaskResult, error) {
	return func(h *composeHarness, in *orkstackv1.ExecuteTaskInput) (*orkstackv1.ExecuteTaskResult, error) {
		result := h.committed(in, map[string]any{"value": "ok"})
		if in.GetTaskId() == "build" {
			result.Artifacts = []*orkstackv1.ArtifactRef{{Name: name, Uri: uri, Sha256: "ab", SizeBytes: 7}}
		}
		return result, nil
	}
}

func TestRunWorkflow_ComposedTaskPassesArtifactRefs(t *testing.T) {
	datasetURI := storedDataset(t)
	rec := &artifactClaimLog{}
	h := newComposeHarness(t, buildRecords("dataset.tar.gz", datasetURI))
	registerClaims(h, rec)
	parent, plan := artifactFixture(t)
	h.mockChild(func(_ workflow.Context, start childStart) (*orkstackv1.Run, error) {
		rec.note("child " + start.runID)
		return &orkstackv1.Run{Status: orkstackv1.Status_STATUS_SUCCEEDED}, nil
	})
	h.run("ork-parent", parent, payload(t, nil), plan)
	must.OKf(t, h.env.GetWorkflowError(), "workflow error")

	starts := h.startedChildren()
	if len(starts) != 1 {
		t.Fatalf("%d child starts, want 1", len(starts))
	}
	got := starts[0].plan.InputArtifacts
	if len(got) != 1 {
		t.Fatalf("child plan input_artifacts = %+v, want one", got)
	}
	g := got[0]
	if g.Name != "dataset" || g.RunID != "ork-parent" || g.TaskID != "build" ||
		g.Artifact.URI != datasetURI || g.Artifact.Name != "dataset" || g.Artifact.SHA256 != "ab" ||
		g.Artifact.Metadata[engine.ArtifactSourceRunKey] != "ork-parent" ||
		g.Artifact.Metadata[engine.ArtifactSourceTaskKey] != "build" ||
		g.Artifact.Metadata[engine.ArtifactSourceNameKey] != "dataset.tar.gz" {
		t.Fatalf("passed %+v: want dataset, the same object, renamed, with its source", g)
	}

	// Stat, then claim for the PARENT, then the start: never a start before
	// the claim, never a claim before every stat.
	if events, want := rec.events(), []string{"stat " + datasetURI, "claim ork-parent", "child ork-child"}; !slices.Equal(events, want) {
		t.Fatalf("order = %v, want %v", events, want)
	}
	started := taskRunOfRun(h.snapshotAt(t, orkstackv1.EventType_EVENT_TYPE_TASK_STARTED, "train"), "train")
	passed := started.GetChild().GetInputArtifacts()
	if len(passed) != 1 || passed[0].GetName() != "dataset" || passed[0].GetArtifact().GetUri() != datasetURI {
		t.Fatalf("the composed row records %v, want the passed dataset", passed)
	}
}

func TestRunWorkflow_ArtifactNotRecordedStartsNoChild(t *testing.T) {
	datasetURI := storedDataset(t)
	rec := &artifactClaimLog{}
	h := newComposeHarness(t, buildRecords("other.tar", datasetURI))
	registerClaims(h, rec)
	parent, plan := artifactFixture(t)
	h.mockChild(func(workflow.Context, childStart) (*orkstackv1.Run, error) {
		t.Error("a child was started for an artifact nobody recorded")
		return &orkstackv1.Run{Status: orkstackv1.Status_STATUS_SUCCEEDED}, nil
	})
	h.run("ork-parent", parent, payload(t, nil), plan)
	requireComposedFailure(t, h, engine.ChildInputArtifactMissingCode,
		`task "build" recorded no artifact "dataset.tar.gz", so build/dataset.tar.gz cannot be passed`)
	if events := rec.events(); len(events) != 0 {
		t.Fatalf("a binding that resolved to nothing was stat'd or claimed: %v", events)
	}
}

func TestRunWorkflow_StatNotFoundVsStoreError(t *testing.T) {
	datasetURI := storedDataset(t)
	for _, tc := range []struct {
		name, code, want string
		statErr          error
	}{
		{"gone", engine.ChildInputArtifactMissingCode, "are no longer in object storage", artifacts.ErrNotFound},
		{"store down", engine.ChildInputUnavailableCode, "could not be checked and claimed in object storage", errors.New("connection refused")},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rec := &artifactClaimLog{statErr: tc.statErr}
			h := newComposeHarness(t, buildRecords("dataset.tar.gz", datasetURI))
			registerClaims(h, rec)
			parent, plan := artifactFixture(t)
			h.mockChild(func(workflow.Context, childStart) (*orkstackv1.Run, error) {
				t.Error("a child was started although its artifact could not be stat'd")
				return &orkstackv1.Run{Status: orkstackv1.Status_STATUS_SUCCEEDED}, nil
			})
			h.run("ork-parent", parent, payload(t, nil), plan)
			requireComposedFailure(t, h, tc.code, tc.want)
			for _, e := range rec.events() {
				if strings.HasPrefix(e, "claim") {
					t.Fatalf("a failed stat still claimed: %v", rec.events())
				}
			}
		})
	}
}

func TestRunWorkflow_ClaimFailureStartsNoChild(t *testing.T) {
	datasetURI := storedDataset(t)
	rec := &artifactClaimLog{claimErr: errors.New("throttled")}
	h := newComposeHarness(t, buildRecords("dataset.tar.gz", datasetURI))
	registerClaims(h, rec)
	parent, plan := artifactFixture(t)
	h.mockChild(func(workflow.Context, childStart) (*orkstackv1.Run, error) {
		t.Error("a child was started although its parent could not claim what it passes")
		return &orkstackv1.Run{Status: orkstackv1.Status_STATUS_SUCCEEDED}, nil
	})
	h.run("ork-parent", parent, payload(t, nil), plan)
	requireComposedFailure(t, h, engine.ChildInputUnavailableCode, "throttled")
}

// requireComposedFailure: the run failed, train failed with code and a
// message containing want, and no child started.
func requireComposedFailure(t *testing.T, h *composeHarness, code, want string) {
	t.Helper()
	if h.env.GetWorkflowError() == nil {
		t.Fatal("the run succeeded")
	}
	failed := taskRunOfRun(h.snapshotAt(t, orkstackv1.EventType_EVENT_TYPE_TASK_FAILED, "train"), "train")
	if failed.GetError().GetCode() != code || !strings.Contains(failed.GetError().GetMessage(), want) {
		t.Fatalf("train error = %v, want %s containing %q", failed.GetError(), code, want)
	}
	if len(h.startedChildren()) != 0 {
		t.Fatal("a child was started")
	}
}

// receivedPlan is the child's plan as its parent starts it: one received
// dataset.
func receivedPlan(uri string) engine.RunPlan {
	plan := defaultPlan()
	plan.CommitRoot = "mem://commits"
	claims := engine.DefaultArtifactClaims
	plan.ArtifactClaims = &claims
	plan.Origin = &engine.RunOrigin{Kind: engine.RunOriginKindParentRun, ID: "ork-parent"}
	plan.InputArtifacts = []engine.NamedArtifactPlan{{Name: "dataset", RunID: "ork-parent", TaskID: "build",
		Artifact: engine.ArtifactRefPlan{Name: "dataset", URI: uri, SHA256: "ab"}}}
	return plan
}

func TestRunWorkflow_OnlyNamingTasksReceiveInputs(t *testing.T) {
	datasetURI := storedDataset(t)
	rec := &artifactClaimLog{}
	h := newComposeHarness(t, func(hh *composeHarness, in *orkstackv1.ExecuteTaskInput) (*orkstackv1.ExecuteTaskResult, error) {
		rec.note("task " + in.GetTaskId())
		return defaultBehavior(hh, in)
	})
	registerClaims(h, rec)
	h.run("ork-child", compile(t, artifactChildDocument), payload(t, nil), receivedPlan(datasetURI))
	must.OKf(t, h.env.GetWorkflowError(), "workflow error")

	// The child claims what it received, for itself, before its first task.
	if events := rec.events(); len(events) < 2 || events[0] != "claim ork-child" || events[1] != "task fit" {
		t.Fatalf("order = %v, want the child's own claim before its first dispatch", events)
	}
	var inputs *orkstackv1.UpstreamRef
	for _, ref := range h.inputFor("fit").GetUpstream() {
		if ref.GetTaskId() == "_inputs" {
			inputs = ref
		}
	}
	if inputs == nil || len(inputs.GetArtifacts()) != 1 || inputs.GetArtifacts()[0].GetName() != "dataset" ||
		inputs.GetArtifacts()[0].GetUri() != datasetURI || inputs.GetCommitUri() != "" {
		t.Fatalf("fit's _inputs = %v, want the dataset, artifacts only", inputs)
	}
	for _, ref := range h.inputFor("report").GetUpstream() {
		if ref.GetTaskId() == "_inputs" {
			t.Fatalf("report names no _inputs and was handed %v", ref)
		}
	}
	started := h.snapshotAt(t, orkstackv1.EventType_EVENT_TYPE_RUN_STARTED, "")
	if got := started.GetInputArtifacts(); len(got) != 1 || got[0].GetName() != "dataset" || got[0].GetRunId() != "ork-parent" {
		t.Fatalf("Run.input_artifacts = %v", got)
	}
}

func TestRunWorkflow_ChildClaimFailureStartsNoTask(t *testing.T) {
	datasetURI := storedDataset(t)
	rec := &artifactClaimLog{claimErr: errors.New("bucket read-only")}
	h := newComposeHarness(t, func(hh *composeHarness, in *orkstackv1.ExecuteTaskInput) (*orkstackv1.ExecuteTaskResult, error) {
		t.Error("a task ran although the run could not claim what it received")
		return defaultBehavior(hh, in)
	})
	registerClaims(h, rec)
	h.run("ork-child", compile(t, artifactChildDocument), payload(t, nil), receivedPlan(datasetURI))
	if h.env.GetWorkflowError() == nil {
		t.Fatal("the run succeeded")
	}
	final := h.snapshotAt(t, orkstackv1.EventType_EVENT_TYPE_RUN_FAILED, "")
	if final.GetError().GetCode() != engine.ChildInputUnavailableCode ||
		final.GetError().GetCategory() != orkstackv1.FailureCategory_FAILURE_CATEGORY_INFRASTRUCTURE_ERROR ||
		!strings.Contains(final.GetError().GetMessage(), "bucket read-only") {
		t.Fatalf("Run.error = %v, want CHILD_INPUT_UNAVAILABLE naming the claim's failure", final.GetError())
	}
}

func TestRunWorkflow_ChildStartTooLarge(t *testing.T) {
	datasetURI := storedDataset(t)
	rec := &artifactClaimLog{}
	h := newComposeHarness(t, func(h *composeHarness, in *orkstackv1.ExecuteTaskInput) (*orkstackv1.ExecuteTaskResult, error) {
		result := h.committed(in, map[string]any{"value": "ok"})
		if in.GetTaskId() == "build" {
			result.Artifacts = []*orkstackv1.ArtifactRef{{Name: "dataset.tar.gz", Uri: datasetURI,
				Metadata: map[string]string{"note": strings.Repeat("x", int(engine.DefaultArtifactClaims.ReserveBytesPerBind))}}}
		}
		return result, nil
	})
	registerClaims(h, rec)
	parent, plan := artifactFixture(t)
	h.mockChild(func(workflow.Context, childStart) (*orkstackv1.Run, error) {
		t.Error("a child was started past its reserved bytes")
		return &orkstackv1.Run{Status: orkstackv1.Status_STATUS_SUCCEEDED}, nil
	})
	h.run("ork-parent", parent, payload(t, nil), plan)
	requireComposedFailure(t, h, engine.ChildStartTooLargeCode, "more than the 1024 its run reserved for them")
	if events := rec.events(); len(events) != 0 {
		t.Fatalf("an oversized start was stat'd or claimed: %v", events)
	}
}

// A plan that passes artifacts and pins no claim policy was built by no
// submit: the composed task fails explicitly, with nothing stat'd or claimed.
func TestRunWorkflow_NoClaimPolicyStartsNoChild(t *testing.T) {
	datasetURI := storedDataset(t)
	rec := &artifactClaimLog{}
	h := newComposeHarness(t, buildRecords("dataset.tar.gz", datasetURI))
	registerClaims(h, rec)
	parent, plan := artifactFixture(t)
	plan.ArtifactClaims = nil
	h.mockChild(func(workflow.Context, childStart) (*orkstackv1.Run, error) {
		t.Error("a child was started with no claim policy")
		return &orkstackv1.Run{Status: orkstackv1.Status_STATUS_SUCCEEDED}, nil
	})
	h.run("ork-parent", parent, payload(t, nil), plan)
	requireComposedFailure(t, h, engine.InvalidCompositionCode, "pins no claim policy")
	if events := rec.events(); len(events) != 0 {
		t.Fatalf("stat'd or claimed with no policy: %v", events)
	}
}

// A task that names an artifact input the run did not receive is never
// dispatched without the file: the run ends before any task.
func TestRunWorkflow_NamedInputNotReceivedStartsNoTask(t *testing.T) {
	rec := &artifactClaimLog{}
	h := newComposeHarness(t, func(hh *composeHarness, in *orkstackv1.ExecuteTaskInput) (*orkstackv1.ExecuteTaskResult, error) {
		t.Error("a task ran without the artifact input it names")
		return defaultBehavior(hh, in)
	})
	registerClaims(h, rec)
	plan := receivedPlan(storedDataset(t))
	plan.InputArtifacts = nil
	h.run("ork-child", compile(t, artifactChildDocument), payload(t, nil), plan)
	if h.env.GetWorkflowError() == nil {
		t.Fatal("the run succeeded")
	}
	final := h.snapshotAt(t, orkstackv1.EventType_EVENT_TYPE_RUN_FAILED, "")
	if final.GetError().GetCode() != engine.ChildInputArtifactMissingCode ||
		!strings.Contains(final.GetError().GetMessage(), `task "fit" names artifact input "dataset", which this run did not receive`) {
		t.Fatalf("Run.error = %v", final.GetError())
	}
}
