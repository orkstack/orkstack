package engine_test

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"strings"
	"sync"
	"testing"

	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/internal/identity"
	"github.com/orkstack/orkstack/internal/testsupport/must"
)

// recordHarness is a compose harness whose two record activities are the real
// ones over an engine's recorder the test controls.
type recordHarness struct {
	*composeHarness
	mu       sync.Mutex
	outputs  []identity.OutputsAsk
	composed []identity.ComposedAsk
	onOutput func(identity.OutputsAsk) (identity.RecordedOutcome, error)
	onChild  func(identity.ComposedAsk) (identity.RecordedOutcome, error)
}

func (h *recordHarness) RecordOutputs(_ context.Context, q identity.OutputsAsk) (identity.RecordedOutcome, error) {
	h.mu.Lock()
	h.outputs = append(h.outputs, q)
	h.mu.Unlock()
	return h.onOutput(q)
}

func (h *recordHarness) RecordComposed(_ context.Context, q identity.ComposedAsk) (identity.RecordedOutcome, error) {
	h.mu.Lock()
	h.composed = append(h.composed, q)
	h.mu.Unlock()
	return h.onChild(q)
}

const recordedOutputs = `{"digest":"sha256:abc","url":"https://r/abc"}`

func newRecordHarness(t *testing.T) *recordHarness {
	t.Helper()
	h := &recordHarness{composeHarness: newComposeHarness(t, defaultBehavior)}
	h.onOutput = func(identity.OutputsAsk) (identity.RecordedOutcome, error) {
		return identity.RecordedOutcome{RecordSHA256: "r", Values: []byte(recordedOutputs), ValuesRef: h.values.put("mem://outputs", []byte(recordedOutputs))}, nil
	}
	h.onChild = func(identity.ComposedAsk) (identity.RecordedOutcome, error) {
		return identity.RecordedOutcome{RecordSHA256: "c", ValuesRef: h.values.put("mem://recorded", []byte(recordedOutputs))}, nil
	}
	act := engine.NewValueRecordActivities(h)
	h.env.RegisterActivityWithOptions(act.RecordOutputs, activity.RegisterOptions{Name: engine.ActivityRecordOutputs})
	h.env.RegisterActivityWithOptions(act.RecordComposed, activity.RegisterOptions{Name: engine.ActivityRecordComposed})
	return h
}

// recordedFixture is the composed fixture as a tree whose results the engine
// records.
func recordedFixture(t *testing.T) (*orkstackv1.Orkflow, engine.RunPlan) {
	t.Helper()
	parent, plan, _ := composedFixture(t, composedChildDocument)
	budget := engine.DefaultRecordBudget
	plan.Identity = &engine.IdentityPlan{AdmissionSHA256: strings.Repeat("a", 64), ValueRecords: 1, RecordBudget: &budget,
		ResolutionsPerPass: engine.DefaultResolutionsPerPass, NonesPerAwait: engine.DefaultNonesPerAwait}
	plan.Children["release"].Plan.Identity = &engine.IdentityPlan{AdmissionSHA256: strings.Repeat("b", 64), ValueRecords: 1, RecordBudget: &budget,
		ResolutionsPerPass: engine.DefaultResolutionsPerPass, NonesPerAwait: engine.DefaultNonesPerAwait}
	return parent, plan
}

func (h *recordHarness) failedWith(t *testing.T, code string) *orkstackv1.Run {
	t.Helper()
	run := h.snapshotAt(t, orkstackv1.EventType_EVENT_TYPE_TASK_FAILED, "release")
	tr := taskRunOfRun(run, "release")
	if tr.GetError().GetCode() != code || tr.GetError().GetCategory() != orkstackv1.FailureCategory_FAILURE_CATEGORY_INFRASTRUCTURE_ERROR {
		t.Fatalf("release failed %v, want %s", tr.GetError(), code)
	}
	return run
}

// TestRecordedRun_AChildsOutputsAndItsComposedTask: the child asks the engine
// to record what it ends with, the parent what it takes, and each holds what
// the engine answered: nothing is rendered or stored by the runs themselves.
func TestRecordedRun_AChildsOutputsAndItsComposedTask(t *testing.T) {
	h := newRecordHarness(t)
	parent, plan := recordedFixture(t)
	h.realChild()
	h.run("ork-parent", parent, payload(t, map[string]any{"image": "i"}), plan)
	must.OK(t, h.env.GetWorkflowError())
	if len(h.outputs) != 1 || len(h.composed) != 1 || len(h.stores) != 0 {
		t.Fatalf("%d outputs, %d composed, %d stored", len(h.outputs), len(h.composed), len(h.stores))
	}
	for _, r := range h.renders {
		if r.CodePrefix == "OUTPUTS" {
			t.Fatal("the child rendered its own outputs")
		}
	}
	out := h.outputs[0]
	child := plan.Children["release"].RunID
	if out.RunID != child || out.Namespace != "default" || out.AdmissionSHA256 != strings.Repeat("b", 64) || len(out.Offered) != 1 ||
		out.Offered["sign"].GetSha256() == "" || out.MaxBytes == 0 || out.Spacing == nil || string(out.Input) != `{"image":"img@build","note":"fixed"}` {
		t.Fatalf("%+v input %s", out, out.Input)
	}
	in := h.composed[0]
	if in.RunID != "ork-parent" || in.TaskID != "release" || in.AdmissionSHA256 != strings.Repeat("a", 64) ||
		in.ChildAdmissionSHA256 != strings.Repeat("b", 64) || in.ValuesSHA256 != hexSHA256(recordedOutputs) || string(in.Input) != `{"image":"i"}` {
		t.Fatalf("%+v input %s", in, in.Input)
	}
	for _, up := range h.inputFor("announce").GetUpstream() {
		want := ""
		if up.GetTaskId() == "release" {
			want = "mem://commits/metadata/attested/default/ork-parent/release/composed.json"
			if up.GetValues().GetUri() != "mem://recorded" {
				t.Fatalf("announce is given %v", up)
			}
		}
		if up.GetRecordUri() != want {
			t.Fatalf("announce is given %v, want the record %q", up, want)
		}
	}
}

// TestRecordedRun_TheComposedTaskFails: by what the engine found, by its
// reason, and by a step that was not answered, in its own words.
func TestRecordedRun_TheComposedTaskFails(t *testing.T) {
	child := func(workflow.Context, childStart) (*orkstackv1.Run, error) {
		return childOutputs(t, map[string]any{"digest": "sha256:abc", "url": "https://r/abc"}), nil
	}
	t.Run("not on record", func(t *testing.T) {
		h := newRecordHarness(t)
		parent, plan := recordedFixture(t)
		h.mockChild(child)
		h.onChild = func(identity.ComposedAsk) (identity.RecordedOutcome, error) {
			return identity.RecordedOutcome{}, &identity.ValuesRefusal{Reason: identity.ReasonChildValuesNotRecorded, Message: "child run c ended, and no engine recorded the outputs it returned"}
		}
		h.run("ork-parent", parent, nil, plan)
		run := h.failedWith(t, engine.ChildValuesNotRecordedCode)
		if !strings.Contains(taskRunOfRun(run, "release").GetError().GetMessage(), "no engine recorded") || len(h.composed) != 1 {
			t.Fatalf("%v, asked %d times", taskRunOfRun(run, "release").GetError(), len(h.composed))
		}
	})
	t.Run("unanswered", func(t *testing.T) {
		h := newRecordHarness(t)
		parent, plan := recordedFixture(t)
		h.mockChild(child)
		h.onChild = func(identity.ComposedAsk) (identity.RecordedOutcome, error) {
			return identity.RecordedOutcome{}, errors.New("frontend down")
		}
		h.run("ork-parent", parent, nil, plan)
		run := h.failedWith(t, engine.ChildValuesUnstoredCode)
		if !strings.Contains(taskRunOfRun(run, "release").GetError().GetMessage(), "unavailable: frontend down") || len(h.composed) != 2 {
			t.Fatalf("%v, asked %d times", taskRunOfRun(run, "release").GetError(), len(h.composed))
		}
	})
	t.Run("what was recorded does not open", func(t *testing.T) {
		h := newRecordHarness(t)
		parent, plan := recordedFixture(t)
		h.mockChild(child)
		h.onChild = func(identity.ComposedAsk) (identity.RecordedOutcome, error) {
			return identity.RecordedOutcome{}, temporal.NewNonRetryableApplicationError("a result recorded in this run's history could not be read", "ContractViolation", nil,
				&orkstackv1.ActivityFailureDetail{ReasonCode: engine.PayloadUnreadableCode, Category: orkstackv1.FailureCategory_FAILURE_CATEGORY_CONTRACT_VIOLATION})
		}
		h.run("ork-parent", parent, nil, plan)
		tr := taskRunOfRun(h.snapshotAt(t, orkstackv1.EventType_EVENT_TYPE_TASK_FAILED, "release"), "release")
		if tr.GetError().GetCode() != engine.PayloadUnreadableCode || tr.GetError().GetCategory() != orkstackv1.FailureCategory_FAILURE_CATEGORY_CONTRACT_VIOLATION || len(h.composed) != 1 {
			t.Fatalf("%v, asked %d times", tr.GetError(), len(h.composed))
		}
	})
	t.Run("the child returned an artifact that is not on record", func(t *testing.T) {
		h := newRecordHarness(t)
		parent, plan := recordedFixture(t)
		h.mockChild(func(workflow.Context, childStart) (*orkstackv1.Run, error) {
			run := childOutputs(t, map[string]any{"digest": "d", "url": "u"})
			run.OutputArtifacts = []*orkstackv1.NamedArtifact{{Name: "bundle", Artifact: &orkstackv1.ArtifactRef{Name: "bundle", Sha256: strings.Repeat("9", 64)}}}
			return run, nil
		})
		h.run("ork-parent", parent, nil, plan)
		h.failedWith(t, engine.ChildValuesNotRecordedCode)
	})
}

// TestRecordedRun_TheChildsEndFails: FAILED with the reason, never SUCCEEDED.
func TestRecordedRun_TheChildsEndFails(t *testing.T) {
	for _, tc := range []struct {
		name, code, says string
		err              error
		category         orkstackv1.FailureCategory
	}{
		{"a task is not on record", engine.OutputsNotRecordedCode, "task gate", &identity.ValuesRefusal{Reason: identity.ReasonOutputsNotRecorded, Message: "task gate"}, orkstackv1.FailureCategory_FAILURE_CATEGORY_INFRASTRUCTURE_ERROR},
		{"unanswered", engine.OutputsUnavailableCode, "their record could not be made (unavailable): frontend down", errors.New("frontend down"), orkstackv1.FailureCategory_FAILURE_CATEGORY_INFRASTRUCTURE_ERROR},
		{"the render", engine.OutputsRenderFailedCode, "outputs.digest: no key", temporal.NewNonRetryableApplicationError("outputs.digest: no key", "ContractViolation", nil,
			&orkstackv1.ActivityFailureDetail{ReasonCode: "OUTPUTS_RENDER_FAILED", Category: orkstackv1.FailureCategory_FAILURE_CATEGORY_CONTRACT_VIOLATION}), orkstackv1.FailureCategory_FAILURE_CATEGORY_CONTRACT_VIOLATION},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := newRecordHarness(t)
			parent, plan := recordedFixture(t)
			h.realChild()
			h.onOutput = func(identity.OutputsAsk) (identity.RecordedOutcome, error) { return identity.RecordedOutcome{}, tc.err }
			h.run("ork-parent", parent, nil, plan)
			var run *orkstackv1.Run
			for _, e := range h.bus.Events(plan.Children["release"].RunID, 0) {
				if e.GetType() == orkstackv1.EventType_EVENT_TYPE_RUN_FAILED {
					run, _ = h.store.Get(plan.Children["release"].RunID)
				}
			}
			if run.GetError().GetCode() != tc.code || run.GetError().GetCategory() != tc.category || !strings.Contains(run.GetError().GetMessage(), tc.says) || len(h.composed) != 0 {
				t.Fatalf("%v, %d composed", run.GetError(), len(h.composed))
			}
			if tr := taskRunOfRun(h.snapshotAt(t, orkstackv1.EventType_EVENT_TYPE_TASK_FAILED, "release"), "release"); tr.GetError().GetCode() != engine.ChildRunFailedCode || tr.GetError().GetCategory() != tc.category || !strings.Contains(tr.GetError().GetDetails(), "child_code="+tc.code) {
				t.Fatalf("release: %v", tr.GetError())
			}
		})
	}
}

// TestRecordedRun_AChildThatFailedAtAnAwait tells its parent which way.
func TestRecordedRun_AChildThatFailedAtAnAwait(t *testing.T) {
	h := newRecordHarness(t)
	parent, plan := recordedFixture(t)
	h.mockChild(func(workflow.Context, childStart) (*orkstackv1.Run, error) {
		return nil, temporal.NewNonRetryableApplicationError("its record could not be read", engine.ResolutionRecordUnavailableCode, nil)
	})
	h.run("ork-parent", parent, nil, plan)
	tr := taskRunOfRun(h.snapshotAt(t, orkstackv1.EventType_EVENT_TYPE_TASK_FAILED, "release"), "release")
	if tr.GetError().GetCode() != engine.ChildRunFailedCode || tr.GetError().GetCategory() != orkstackv1.FailureCategory_FAILURE_CATEGORY_INFRASTRUCTURE_ERROR ||
		!strings.Contains(tr.GetError().GetDetails(), "child_code="+engine.ResolutionRecordUnavailableCode) {
		t.Fatalf("%v", tr.GetError())
	}
}

// TestRecordedRun_AChildThatDeclaresNoOutputs records them all the same, and
// shows its end tasks' values as it always did.
func TestRecordedRun_AChildThatDeclaresNoOutputs(t *testing.T) {
	h := newRecordHarness(t)
	parent, plan := recordedFixture(t)
	doc := composedChildDocument[:strings.Index(composedChildDocument, "outputs:")] + composedChildDocument[strings.Index(composedChildDocument, "tasks:"):]
	child := compile(t, doc)
	child.DefinitionDigest = childDigest
	cp, err := engine.NewChildRunPlan(child, plan.Children["release"].Plan, true, true)
	must.OK(t, err)
	cp.RunID = plan.Children["release"].RunID
	plan.Children["release"] = cp
	parent.Tasks = parent.Tasks[:2] // announce reads an output
	h.onOutput = func(identity.OutputsAsk) (identity.RecordedOutcome, error) {
		return identity.RecordedOutcome{Values: []byte("{}"), ValuesRef: h.values.put("mem://outputs", []byte("{}"))}, nil
	}
	h.realChild()
	h.run("ork-parent", parent, nil, plan)
	must.OK(t, h.env.GetWorkflowError())
	if len(h.outputs) != 1 || len(h.outputs[0].Offered) != 0 || h.composed[0].ValuesSHA256 != hexSHA256("{}") {
		t.Fatalf("%+v %+v", h.outputs, h.composed)
	}
	run, ok := h.store.Get(cp.RunID)
	if !ok || len(run.GetOutput().GetData().GetFields()) != 1 {
		t.Fatalf("%v", run.GetOutput())
	}
}

func hexSHA256(s string) string {
	sum := sha256.Sum256([]byte(s))
	return hex.EncodeToString(sum[:])
}
