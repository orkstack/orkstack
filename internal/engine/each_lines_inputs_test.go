package engine_test

import (
	"context"
	"errors"
	"strings"
	"testing"

	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/workflow"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/internal/task"
	"github.com/orkstack/orkstack/internal/testsupport/must"
)

// A fan-out over the lines of an artifact this run received (_inputs), and of
// one a composed task's child returned (fanout-artifacts design §5.7): the
// same index and the same items, from a source the run's own claims hold.

const linesInputsDocument = `
apiVersion: orkstack/v1
kind: Orkflow
name: lines-inputs
inputs:
  dataset: { type: artifact, required: true }
tasks:
  - id: convert
    uses: shell
    each: {over_artifact: _inputs/dataset, format: jsonl, max_items: 50}
    with: {op: exec}
`

// registerComposeIndex gives a compose harness the line indexer: three lines,
// recording what it was asked and, in rec's order, when.
func registerComposeIndex(h *composeHarness, rec *artifactClaimLog, asked *[]*orkstackv1.IndexEachLinesInput) {
	h.env.RegisterActivityWithOptions(func(_ context.Context, in *orkstackv1.IndexEachLinesInput) (*orkstackv1.IndexEachLinesResult, error) {
		rec.note("index " + in.GetTaskId())
		*asked = append(*asked, in)
		return &orkstackv1.IndexEachLinesResult{Count: 3, TotalLines: 3, Index: linesIndex}, nil
	}, activity.RegisterOptions{Name: task.ActivityIndexEachLines})
}

// sourced gives a received reference what a real one carries: a digest, and
// the source keys that name who recorded it, as long as they may be.
func sourced(plan engine.RunPlan) engine.RunPlan {
	a := &plan.InputArtifacts[0].Artifact
	a.SHA256, a.SizeBytes = strings.Repeat("ab", 32), 1234
	a.Metadata = map[string]string{
		engine.ArtifactSourceRunKey:  "ork-parent",
		engine.ArtifactSourceTaskKey: strings.Repeat("b", 200),
		engine.ArtifactSourceNameKey: strings.Repeat("c", 255),
	}
	plan.EachLines = engine.DefaultEachLinesPolicy()
	return plan
}

func requireItemsOver(t *testing.T, calls []*orkstackv1.ExecuteTaskInput, id, name, uri string) {
	t.Helper()
	var items int
	for _, in := range calls {
		if in.GetTaskId() != id {
			continue
		}
		items++
		src := in.GetEach().GetLines().GetSource()
		if src.GetName() != name || src.GetUri() != uri || src.GetSha256() == "" || len(src.GetMetadata()) != 0 {
			t.Fatalf("item %d's source = %v; want %s at %s, and no source metadata in every item's input",
				in.GetEach().GetIndex(), src, name, uri)
		}
		for _, up := range in.GetUpstream() {
			for _, a := range up.GetArtifacts() {
				if a.GetUri() == uri {
					t.Fatalf("item %d is staged the source itself: %v", in.GetEach().GetIndex(), up)
				}
			}
		}
	}
	if items != 3 {
		t.Fatalf("%d items of %s; want 3", items, id)
	}
}

// TestEachLines_OverAnArtifactInput: the run claims what it received before
// the index reads it; the index and every item read the received object, and
// no item is staged it.
func TestEachLines_OverAnArtifactInput(t *testing.T) {
	uri := storedDataset(t)
	rec := &artifactClaimLog{}
	h := newComposeHarness(t, func(hh *composeHarness, in *orkstackv1.ExecuteTaskInput) (*orkstackv1.ExecuteTaskResult, error) {
		return itemResult(in), nil
	})
	registerClaims(h, rec)
	var asked []*orkstackv1.IndexEachLinesInput
	registerComposeIndex(h, rec, &asked)
	h.run("ork-child", compile(t, linesInputsDocument), payload(t, nil), sourced(receivedPlan(uri)))
	must.OKf(t, h.env.GetWorkflowError(), "workflow error")
	if events := rec.events(); len(events) < 2 || events[0] != "claim ork-child" || events[1] != "index convert" {
		t.Fatalf("order = %v; want the run's own claim before its source is read", events)
	}
	if len(asked) != 1 || asked[0].GetProducer() != "_inputs" || asked[0].GetName() != "dataset" || asked[0].GetSource().GetUri() != uri {
		t.Fatalf("indexed %v; want _inputs/dataset once", asked)
	}
	requireItemsOver(t, h.calls, "convert", "dataset", uri)
}

// TestEachLines_ArtifactInputNotReceivedStartsNoTask: a fan-out over an
// artifact input its run did not receive starts nothing, as a task that names
// one in a template does.
func TestEachLines_ArtifactInputNotReceivedStartsNoTask(t *testing.T) {
	rec := &artifactClaimLog{}
	h := newComposeHarness(t, func(hh *composeHarness, in *orkstackv1.ExecuteTaskInput) (*orkstackv1.ExecuteTaskResult, error) {
		t.Error("an item ran without the source it reads")
		return itemResult(in), nil
	})
	registerClaims(h, rec)
	var asked []*orkstackv1.IndexEachLinesInput
	registerComposeIndex(h, rec, &asked)
	plan := sourced(receivedPlan(storedDataset(t)))
	plan.InputArtifacts = nil
	h.run("ork-child", compile(t, linesInputsDocument), payload(t, nil), plan)
	if h.env.GetWorkflowError() == nil {
		t.Fatal("the run succeeded")
	}
	final := h.snapshotAt(t, orkstackv1.EventType_EVENT_TYPE_RUN_FAILED, "")
	if final.GetError().GetCode() != engine.ChildInputArtifactMissingCode || len(asked) != 0 ||
		!strings.Contains(final.GetError().GetMessage(), `task "convert" names artifact input "dataset", which this run did not receive`) {
		t.Fatalf("Run.error = %v after %d indexes", final.GetError(), len(asked))
	}
}

// TestEachLines_OverAComposedTasksArtifact: the artifact a child returned is
// claimed for this run before its lines are indexed, and its producer, which
// has no Orker of its own here, is never asked for an index.
func TestEachLines_OverAComposedTasksArtifact(t *testing.T) {
	uri := storedDataset(t)
	rec := &artifactClaimLog{}
	h := newComposeHarness(t, func(hh *composeHarness, in *orkstackv1.ExecuteTaskInput) (*orkstackv1.ExecuteTaskResult, error) {
		if in.GetEach() != nil {
			return itemResult(in), nil
		}
		return defaultBehavior(hh, in)
	})
	registerClaims(h, rec)
	var asked []*orkstackv1.IndexEachLinesInput
	registerComposeIndex(h, rec, &asked)
	parent, plan := linesOverComposed(t)
	h.mockChild(childReturning(uri))
	h.run("ork-parent", parent, payload(t, nil), plan)
	must.OKf(t, h.env.GetWorkflowError(), "workflow error")
	if events := rec.events(); len(events) < 2 || events[0] != "claim ork-parent" || events[1] != "index publish" {
		t.Fatalf("order = %v; want the claim of what the child returned before its lines are read", events)
	}
	requireItemsOver(t, h.calls, "publish", "model", uri)
}

// linesOverComposed is returningFixture with publish a fan-out over the lines
// of the artifact the composed task train returned.
func linesOverComposed(t *testing.T) (*orkstackv1.Orkflow, engine.RunPlan) {
	t.Helper()
	parent, plan := returningFixture(t)
	for _, tk := range parent.GetTasks() {
		if tk.GetId() == "publish" {
			tk.With, tk.InputArtifacts = nil, nil
			tk.Each = &orkstackv1.EachSpec{OverArtifact: "train/model", Format: orkstackv1.EachLineFormat_EACH_LINE_FORMAT_JSONL,
				As: "item", MaxItems: 50,
				Source: &orkstackv1.EachSource{Artifact: &orkstackv1.EachArtifactSource{Producer: "train", Name: "model"}}}
		}
	}
	plan.EachLines = engine.DefaultEachLinesPolicy()
	return parent, plan
}

// TestEachLines_APassedSourceThatIsNotThereIsExplicit: a child that returned
// no such artifact, one that returned a reference with no digest, and a
// received artifact with none each fail the fan-out with a reason before
// anything is indexed; a child that failed fails the run with its own.
func TestEachLines_APassedSourceThatIsNotThereIsExplicit(t *testing.T) {
	uri := storedDataset(t)
	succeeded := func(artifacts ...*orkstackv1.NamedArtifact) func(workflow.Context, childStart) (*orkstackv1.Run, error) {
		return func(_ workflow.Context, start childStart) (*orkstackv1.Run, error) {
			return &orkstackv1.Run{Id: start.runID, Status: orkstackv1.Status_STATUS_SUCCEEDED, OutputArtifacts: artifacts}, nil
		}
	}
	over := func(t *testing.T, child func(workflow.Context, childStart) (*orkstackv1.Run, error)) (*composeHarness, int) {
		t.Helper()
		rec := &artifactClaimLog{}
		h := newComposeHarness(t, func(hh *composeHarness, in *orkstackv1.ExecuteTaskInput) (*orkstackv1.ExecuteTaskResult, error) {
			t.Errorf("task %s item %d ran without its source", in.GetTaskId(), in.GetEach().GetIndex())
			return itemResult(in), nil
		})
		registerClaims(h, rec)
		var asked []*orkstackv1.IndexEachLinesInput
		registerComposeIndex(h, rec, &asked)
		parent, plan := linesOverComposed(t)
		h.mockChild(child)
		h.run("ork-parent", parent, payload(t, nil), plan)
		if h.env.GetWorkflowError() == nil {
			t.Fatal("the run succeeded without its fan-out's source")
		}
		return h, len(asked)
	}
	for _, tc := range []struct {
		name, code string
		child      func(workflow.Context, childStart) (*orkstackv1.Run, error)
	}{
		{"the child returned nothing", engine.EachOverMissingCode, succeeded()},
		{"the child returned no digest", engine.EachExpansionFailedCode, succeeded(&orkstackv1.NamedArtifact{Name: "model",
			RunId: "ork-child", TaskId: "fit", Artifact: &orkstackv1.ArtifactRef{Name: "model", Uri: uri}})},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h, asked := over(t, tc.child)
			failed := taskRunOfRun(h.snapshotAt(t, orkstackv1.EventType_EVENT_TYPE_TASK_FAILED, "publish"), "publish")
			if failed.GetError().GetCode() != tc.code || asked != 0 {
				t.Fatalf("publish = %v after %d indexes; want %s and none", failed.GetError(), asked, tc.code)
			}
		})
	}
	t.Run("the child failed", func(t *testing.T) {
		h, asked := over(t, func(workflow.Context, childStart) (*orkstackv1.Run, error) {
			return nil, errors.New("the child failed")
		})
		train := taskRunOfRun(h.snapshotAt(t, orkstackv1.EventType_EVENT_TYPE_TASK_FAILED, "train"), "train")
		publish := taskRunOfRun(h.snapshotAt(t, orkstackv1.EventType_EVENT_TYPE_RUN_FAILED, "train"), "publish")
		if train.GetStatus() != orkstackv1.Status_STATUS_FAILED || asked != 0 || publish.GetEach().GetExpanded() ||
			publish.GetStatus() == orkstackv1.Status_STATUS_RUNNING || publish.GetStatus() == orkstackv1.Status_STATUS_SUCCEEDED {
			t.Fatalf("train %v, publish %v after %d indexes", train.GetError(), publish, asked)
		}
	})
	t.Run("the received artifact has no digest", func(t *testing.T) {
		rec := &artifactClaimLog{}
		h := newComposeHarness(t, func(hh *composeHarness, in *orkstackv1.ExecuteTaskInput) (*orkstackv1.ExecuteTaskResult, error) {
			t.Error("an item ran over a source with no digest")
			return itemResult(in), nil
		})
		registerClaims(h, rec)
		var asked []*orkstackv1.IndexEachLinesInput
		registerComposeIndex(h, rec, &asked)
		plan := sourced(receivedPlan(uri))
		plan.InputArtifacts[0].Artifact.SHA256 = ""
		h.run("ork-child", compile(t, linesInputsDocument), payload(t, nil), plan)
		failed := taskRunOfRun(h.snapshotAt(t, orkstackv1.EventType_EVENT_TYPE_TASK_FAILED, "convert"), "convert")
		if failed.GetError().GetCode() != engine.EachExpansionFailedCode || len(asked) != 0 {
			t.Fatalf("convert = %v after %d indexes", failed.GetError(), len(asked))
		}
	})
}

// TestEachLines_OverAnArtifactInputAcrossASwitch: a run that carries on
// between the task before its fan-out and the fan-out reads the artifact it
// received in the generation that expands it, claimed once, in the first.
func TestEachLines_OverAnArtifactInputAcrossASwitch(t *testing.T) {
	doc := strings.Replace(linesInputsDocument, "tasks:\n", "tasks:\n  - id: before\n    uses: shell\n    with: {op: exec}\n", 1)
	doc = strings.Replace(doc, "    each: {over", "    needs: [before]\n    each: {over", 1)
	uri := storedDataset(t)
	rec := &artifactClaimLog{}
	plan := sourced(receivedPlan(uri))
	held := holdPlan()
	plan.History, plan.Continuation = held.History, held.Continuation
	indexedIn := -1
	g := runGenerations(t, "ork-child", compile(t, doc), payload(t, nil), plan,
		func(ctx context.Context, in *orkstackv1.ExecuteTaskInput) (*orkstackv1.ExecuteTaskResult, error) {
			if in.GetEach() != nil {
				return itemResult(in), nil
			}
			return okBehavior(ctx, in)
		}, func(h *harness, generation int) {
			claims := engine.NewArtifactClaimActivities(rec, rec)
			h.env.RegisterActivityWithOptions(claims.ClaimReceivedArtifacts,
				activity.RegisterOptions{Name: engine.ActivityClaimReceivedArtifacts})
			registerIndex(h, func(_ context.Context, in *orkstackv1.IndexEachLinesInput) (*orkstackv1.IndexEachLinesResult, error) {
				indexedIn = generation
				return &orkstackv1.IndexEachLinesResult{Count: 3, TotalLines: 3, Index: linesIndex}, nil
			})
			if generation == 0 {
				raiseHistoryWhen(h, 650, taskStatusIs("before", orkstackv1.Status_STATUS_SUCCEEDED))
			}
		})
	generationsOK(t, g, 2)
	if indexedIn < 1 {
		t.Fatalf("the source was indexed in generation %d; want one after the switch", indexedIn)
	}
	if events := rec.events(); len(events) != 1 || events[0] != "claim ork-child" {
		t.Fatalf("claims = %v; want one, in the first generation", events)
	}
	var calls []*orkstackv1.ExecuteTaskInput
	for _, h := range g.harnesses {
		calls = append(calls, h.invocations()...)
	}
	requireItemsOver(t, calls, "convert", "dataset", uri)
}
