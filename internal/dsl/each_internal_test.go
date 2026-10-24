package dsl

import (
	"errors"
	"testing"

	"google.golang.org/protobuf/types/known/structpb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
)

func eachDocInternal(task string) []byte {
	return []byte(`
apiVersion: orkstack/v1
kind: Orkflow
name: fan
tasks:
  - id: list
    uses: shell
    with: {op: exec}
` + task)
}

func internalFieldOf(err error) string {
	var verr *ValidationError
	if errors.As(err, &verr) {
		return verr.Field
	}
	return ""
}

func TestValidate_HandBuiltEachSourceMustMatchOver(t *testing.T) {
	wf, err := Compile(eachDocInternal(`
  - id: convert
    uses: shell
    needs: [list]
    each: {over: "{{ .tasks.list.output.parts }}", max_items: 3}
    with: {op: exec}
`))
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	if err := validate(wf, Options{}); err != nil {
		t.Fatalf("the compiled message fails Validate: %v", err)
	}
	wf.Tasks[1].Each.Source = &orkstackv1.EachSource{TaskId: "list", Path: []string{"other"}}
	err = validate(wf, Options{})
	if err == nil || internalFieldOf(err) != "tasks[1].each.source" {
		t.Fatalf("a source that disagrees with over = %v (field %q)", err, internalFieldOf(err))
	}
	wf.Tasks[1].Each.Source = nil
	if err := validate(wf, Options{}); err == nil || internalFieldOf(err) != "tasks[1].each.source" {
		t.Fatalf("a missing source = %v", err)
	}
}

// The compiled-message half of rule 15's each refusal: a hand-built request.
func TestValidate_HandBuiltAwaitRefusesEach(t *testing.T) {
	schema, _ := structpb.NewStruct(map[string]any{"parts": map[string]any{"type": "array"}})
	wf := &orkstackv1.Orkflow{Name: "fan", Inputs: schema, Tasks: []*orkstackv1.Task{{
		Id: "gate", Await: &orkstackv1.AwaitSpec{TimeoutSeconds: 60},
		Each: &orkstackv1.EachSpec{Over: "{{ .inputs.parts }}", As: "item", MaxItems: 3,
			Source: &orkstackv1.EachSource{Input: "parts"}},
	}}}
	if err := validate(wf, Options{}); err == nil || internalFieldOf(err) != "tasks[0].each" {
		t.Fatalf("each on an await (message) = %v", err)
	}
}

// A hand-built definition is refused at rule 23 even when the YAML front end
// never saw it.
func TestValidate_HandBuiltFanOutRefusesAPin(t *testing.T) {
	schema, _ := structpb.NewStruct(map[string]any{"parts": map[string]any{"type": "array"}})
	wf := &orkstackv1.Orkflow{Name: "fan", Inputs: schema, Pins: []*orkstackv1.Pin{{Name: "b"}}, Tasks: []*orkstackv1.Task{{
		Id: "convert", Uses: "shell", Pin: "b",
		Each: &orkstackv1.EachSpec{Over: "{{ .inputs.parts }}", As: "item", MaxItems: 3,
			Source: &orkstackv1.EachSource{Input: "parts"}},
	}}}
	if err := validate(wf, Options{}); err == nil || internalFieldOf(err) != "tasks[0].pin" {
		t.Fatalf("a hand-built pinned fan-out = %v", err)
	}
}
