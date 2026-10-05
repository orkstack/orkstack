package dsl

import (
	"errors"
	"strings"
	"testing"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/structpb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
)

// handBuiltComposed is a composed task that never met the YAML decoder, as a
// request arriving at internal/api would carry it.
func handBuiltComposed() *orkstackv1.Orkflow {
	return &orkstackv1.Orkflow{
		Name: "hand-built",
		Tasks: []*orkstackv1.Task{
			{Id: "build", Uses: "shell"},
			{
				Id:      "release",
				Uses:    "orkflow:release-block@2.1.0",
				Needs:   []string{"build"},
				Orkflow: &orkstackv1.OrkflowRef{Name: "release-block", Version: "2.1.0"},
			},
		},
	}
}

func TestValidate_HandBuiltComposedTaskMustAgreeWithUses(t *testing.T) {
	cases := []struct {
		name  string
		set   func(*orkstackv1.Orkflow)
		field string
		want  string
	}{
		{"orkflow disagrees with uses", func(wf *orkstackv1.Orkflow) {
			wf.Tasks[1].Orkflow.Version = "2.0.0"
		}, "tasks[1].orkflow", "does not match uses"},
		{"uses names an orkflow but no ref", func(wf *orkstackv1.Orkflow) {
			wf.Tasks[1].Orkflow = nil
		}, "tasks[1].orkflow", "carries no compiled orkflow reference"},
		{"orkflow under an image uses", func(wf *orkstackv1.Orkflow) {
			wf.Tasks[0].Orkflow = &orkstackv1.OrkflowRef{Name: "x", Version: "1"}
		}, "tasks[0].orkflow", "does not name an orkflow"},
		{"image on a composed task", func(wf *orkstackv1.Orkflow) {
			wf.Tasks[1].Image = &orkstackv1.ImageRef{Reference: "shell"}
		}, "tasks[1].image", "a composed task runs no image"},
		{"pool", func(wf *orkstackv1.Orkflow) { wf.Tasks[1].Pool = "gpu" }, "tasks[1].pool", `"pool" does not apply`},
		{"retry", func(wf *orkstackv1.Orkflow) {
			wf.Tasks[1].Retry = &orkstackv1.RetryPolicy{MaximumAttempts: 2}
		}, "tasks[1].retry", `"retry" does not apply`},
		{"each", func(wf *orkstackv1.Orkflow) {
			wf.Tasks[1].Each = &orkstackv1.EachSpec{Over: "{{ .inputs.x }}", As: "item", MaxItems: 1}
		}, "tasks[1].each", `"each" does not apply`},
		{"await", func(wf *orkstackv1.Orkflow) {
			wf.Tasks[1].Await = &orkstackv1.AwaitSpec{TimeoutSeconds: 60}
		}, "tasks[1].uses", "uses: and await: are exclusive"},
		{"timeout", func(wf *orkstackv1.Orkflow) { wf.Tasks[1].TimeoutSeconds = 300 }, "tasks[1].timeout_seconds", `"timeout_seconds" does not apply`},
		{"command", func(wf *orkstackv1.Orkflow) { wf.Tasks[1].Command = []string{"x"} }, "tasks[1].command", `"command" does not apply`},
		{"run_as_orker_user", func(wf *orkstackv1.Orkflow) { wf.Tasks[1].RunAsOrkerUser = true }, "tasks[1].run_as_orker_user", "run_as_orker_user"},
		{"version missing", func(wf *orkstackv1.Orkflow) { wf.Tasks[1].Uses = "orkflow:release-block" }, "tasks[1].uses", "needs a version"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			wf := handBuiltComposed()
			tc.set(wf)
			err := validate(wf, Options{})
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("error = %v, want %q", err, tc.want)
			}
			var verr *ValidationError
			if !errors.As(err, &verr) || verr.Field != tc.field {
				t.Fatalf("error not anchored at %s: %#v", tc.field, err)
			}
		})
	}
	t.Run("a clean hand-built composed task validates", func(t *testing.T) {
		if err := validate(handBuiltComposed(), Options{}); err != nil {
			t.Fatalf("refused: %v", err)
		}
	})
}

func composedGolden(t *testing.T) *orkstackv1.Orkflow {
	wf := proto.Clone(goldenOrkflow(t)).(*orkstackv1.Orkflow)
	wf.Tasks = append(wf.Tasks, &orkstackv1.Task{
		Id:      "release",
		Uses:    "orkflow:release-block@2.1.0",
		Orkflow: &orkstackv1.OrkflowRef{Name: "release-block", Version: "2.1.0", DefinitionDigest: "sha256:aaa"},
	})
	return wf
}

func TestCanonicalOrkflow_OutputsAndOrkflowEmittedOnlyWhenSet(t *testing.T) {
	plain := goldenOrkflow(t)
	canonical, err := CanonicalOrkflow(plain)
	if err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{`"outputs"`, `"orkflow"`} {
		if strings.Contains(string(canonical), key) {
			t.Fatalf("a definition with neither emitted %s: %s", key, canonical)
		}
	}
	if got, _ := DefinitionDigest(plain); got != GoldenDefinitionDigest {
		t.Fatalf("the golden digest moved: %s", got)
	}
	if got, _ := DefinitionDigest(pinnedOrkflow(t)); got != GoldenPinnedDefinitionDigest {
		t.Fatalf("the golden pinned digest moved: %s", got)
	}

	withOutputs := proto.Clone(plain).(*orkstackv1.Orkflow)
	withOutputs.Outputs = map[string]string{"digest": "{{ .tasks.fetch.output.value }}"}
	got, _ := DefinitionDigest(withOutputs)
	if got == GoldenDefinitionDigest {
		t.Fatal("adding outputs did not change the digest")
	}
	canonical, _ = CanonicalOrkflow(withOutputs)
	if !strings.Contains(string(canonical), `"outputs":{"digest":"{{ .tasks.fetch.output.value }}"}`) {
		t.Fatalf("canonical outputs missing: %s", canonical)
	}
	empty := proto.Clone(plain).(*orkstackv1.Orkflow)
	empty.Outputs = map[string]string{}
	if got, _ := DefinitionDigest(empty); got != GoldenDefinitionDigest {
		t.Fatal("an empty outputs map moved the digest")
	}

	composed := composedGolden(t)
	before, err := DefinitionDigest(composed)
	if err != nil {
		t.Fatal(err)
	}
	canonical, _ = CanonicalOrkflow(composed)
	if !strings.Contains(string(canonical), `"orkflow":{"definitionDigest":"sha256:aaa","name":"release-block","version":"2.1.0"}`) {
		t.Fatalf("canonical orkflow missing: %s", canonical)
	}
	moved := proto.Clone(composed).(*orkstackv1.Orkflow)
	moved.Tasks[len(moved.Tasks)-1].Orkflow.DefinitionDigest = "sha256:bbb"
	if after, _ := DefinitionDigest(moved); after == before {
		t.Fatal("changing the child's definition_digest did not change the parent's digest")
	}
}

func TestDerivedVersion_CoversTheChildDigest(t *testing.T) {
	source := []byte("name: x\n")
	composed := composedGolden(t)
	a, err := DerivedVersion(source, composed)
	if err != nil {
		t.Fatal(err)
	}
	moved := proto.Clone(composed).(*orkstackv1.Orkflow)
	moved.Tasks[len(moved.Tasks)-1].Orkflow.DefinitionDigest = "sha256:bbb"
	b, _ := DerivedVersion(source, moved)
	if a == b {
		t.Fatal("the derived version did not cover the child digest")
	}
}

func TestDerivedVersion_UnchangedWithoutComposedTasks(t *testing.T) {
	wf := goldenOrkflow(t)
	images, err := canonicalImages(wf)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(images), "orkflow") {
		t.Fatalf("canonicalImages of a document with no composed task names orkflow: %s", images)
	}
	// The pre-composition rendering, spelled out: one {image, taskId} object
	// per task and nothing else.
	for _, task := range wf.GetTasks() {
		if !strings.Contains(string(images), `"taskId":"`+task.GetId()+`"}`) {
			t.Fatalf("entry for %s is not the two-key shape: %s", task.GetId(), images)
		}
	}
}

func TestValidate_EngineTemplateCollectorIsAuthoritative(t *testing.T) {
	with, _ := structpb.NewStruct(map[string]any{"x": `{{ index .tasks .inputs.t "output" "v" }}`})
	wf := handBuiltComposed()
	wf.Inputs, _ = structpb.NewStruct(map[string]any{"t": map[string]any{"type": "string"}})
	wf.Tasks[1].With = with
	err := validate(wf, Options{})
	if err == nil || !strings.Contains(err.Error(), "names every task it reads literally") {
		t.Fatalf("error = %v", err)
	}
}
