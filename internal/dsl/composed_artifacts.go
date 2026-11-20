package dsl

import (
	"fmt"
	"regexp"
	"sort"
	"strings"

	"go.yaml.in/yaml/v4"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
)

// Artifact inputs and their bindings (docs/DSL.md, "Passing artifacts to a
// child run"; rules 55 and 56). An input of `type: artifact` is a file a
// composing task hands a child run: the composed task binds it with
// `artifacts:` to an artifact a task of the parent recorded, the engine
// resolves the binding when it starts the child and passes a reference,
// never the bytes, and a task of the child reads it as
// {{ artifact "_inputs/<name>" }}.

// InputsProducer is the reserved producer of a run's artifact inputs:
// {{ artifact "_inputs/<name>" }} reads one, and an `artifacts:` binding of
// "_inputs/<name>" forwards one to a child. No task id can collide with it:
// a task id is a slug and begins with a letter.
const InputsProducer = "_inputs"

// InputTypeArtifact is the `type:` of an artifact input.
const InputTypeArtifact = "artifact"

// MaxArtifactInputs bounds the artifact inputs one Orkflow declares (rule 55).
const MaxArtifactInputs = 16

// maxArtifactInputBytes bounds one artifact input's name, so the staged path
// /orkstack/input/artifacts/_inputs/<name> is the name as written.
const maxArtifactInputBytes = 64

// artifactInputPattern is rule 55's name rule: the name is staged as a file
// name as written, so it is one the Orker never rewrites.
var artifactInputPattern = regexp.MustCompile(`^[a-z][a-z0-9_]*$`)

// ArtifactInputs lists, sorted, the inputs of wf whose type is artifact.
func ArtifactInputs(wf *orkstackv1.Orkflow) []string {
	var out []string
	for name, decl := range wf.GetInputs().GetFields() {
		if ReadInput(decl).Type == InputTypeArtifact {
			out = append(out, name)
		}
	}
	sort.Strings(out)
	return out
}

// DeclaresArtifactInputs reports whether wf declares an artifact input.
func DeclaresArtifactInputs(wf *orkstackv1.Orkflow) bool {
	for _, decl := range wf.GetInputs().GetFields() {
		if ReadInput(decl).Type == InputTypeArtifact {
			return true
		}
	}
	return false
}

// ParseArtifactBinding splits one `artifacts:` value, a literal
// "<task>/<name>", at its first "/". A template, a missing half or a NUL is
// refused: the binding is resolved by the engine from a recorded result, so
// it must say exactly which one.
func ParseArtifactBinding(value string) (taskID, name string, err error) {
	if strings.Contains(value, "{{") {
		return "", "", fmt.Errorf("a binding is a literal \"<task>/<name>\", never a template")
	}
	taskID, name, found := strings.Cut(value, "/")
	if !found || !validBindingHalves(taskID, name) {
		return "", "", fmt.Errorf("%q must be \"<task>/<name>\"", value)
	}
	return taskID, name, nil
}

func validBindingHalves(taskID, name string) bool {
	return taskID != "" && name != "" && !strings.ContainsRune(taskID+name, 0)
}

// compileChildArtifacts compiles a composed task's `artifacts:` map into its
// bindings, sorted by input so one document always compiles to one message.
func compileChildArtifacts(taskIndex int, td taskDocument) ([]*orkstackv1.ChildArtifact, error) {
	if len(td.Artifacts) == 0 {
		return nil, nil
	}
	inputs := make([]string, 0, len(td.Artifacts))
	for input := range td.Artifacts {
		inputs = append(inputs, input)
	}
	sort.Strings(inputs)
	out := make([]*orkstackv1.ChildArtifact, 0, len(inputs))
	for _, input := range inputs {
		taskID, name, err := ParseArtifactBinding(td.Artifacts[input])
		if err != nil {
			return nil, validationError(fmt.Sprintf("tasks[%d].artifacts.%s", taskIndex, input),
				fmt.Errorf("task %q: artifacts.%s: %w", td.ID, input, err))
		}
		out = append(out, &orkstackv1.ChildArtifact{Input: input, TaskId: taskID, Name: name})
	}
	return out, nil
}

// onlyComposedBinds is the refusal of `artifacts:` on a task that starts no
// child run.
func onlyComposedBinds(taskIndex int, id string) error {
	return validationError(fmt.Sprintf("tasks[%d].artifacts", taskIndex), fmt.Errorf(
		"task %q: artifacts: binds a child Orkflow's artifact inputs, so only a composed task "+
			"(uses: orkflow:…) takes it", id))
}

// deliverySet is the sorted set of artifact inputs task's with: names
// literally, {{ artifact "_inputs/<name>" }}: exactly the inputs the engine
// stages for it. A template that does not parse names nothing here; rule 8
// refuses it.
func deliverySet(task *orkstackv1.Task) []string {
	if task.GetOrkflow() != nil {
		return nil
	}
	names := map[string]struct{}{}
	c := &templateChecker{refs: map[string]struct{}{}, inputNames: names}
	_ = c.checkValue("with", task.GetWith().AsMap())
	if len(names) == 0 {
		return nil
	}
	out := make([]string, 0, len(names))
	for name := range names {
		out = append(out, name)
	}
	sort.Strings(out)
	return out
}

// validateArtifactInputs is rule 55: every artifact input is required, has
// no default, and has a name the Orker stages as written; at most
// MaxArtifactInputs of them.
func validateArtifactInputs(wf *orkstackv1.Orkflow) (map[string]struct{}, error) {
	names := ArtifactInputs(wf)
	if len(names) > MaxArtifactInputs {
		return nil, validationError("inputs", fmt.Errorf(
			"orkflow %q declares %d artifact inputs; at most %d", wf.GetName(), len(names), MaxArtifactInputs))
	}
	set := make(map[string]struct{}, len(names))
	for _, name := range names {
		field := "inputs." + name
		if err := checkArtifactInputName(field, name); err != nil {
			return nil, err
		}
		decl := ReadInput(wf.GetInputs().GetFields()[name])
		if decl.Default != nil {
			return nil, validationError(field, fmt.Errorf(
				"%s: an artifact input takes no default: the composing task binds it", field))
		}
		if !decl.Required {
			return nil, validationError(field, fmt.Errorf(
				"%s: an artifact input is required: write required: true (a composing task must bind it)", field))
		}
		set[name] = struct{}{}
	}
	return set, nil
}

func checkArtifactInputName(field, name string) error {
	if len(name) > maxArtifactInputBytes || !artifactInputPattern.MatchString(name) {
		return validationError(field, fmt.Errorf(
			"%s: an artifact input's name is %s, at most %d bytes: it is staged as a file name as written",
			field, artifactInputPattern, maxArtifactInputBytes))
	}
	return nil
}

// declaredList renders the declared artifact inputs for a refusal.
func declaredList(artifactInputs map[string]struct{}) string {
	if len(artifactInputs) == 0 {
		return "none"
	}
	names := make([]string, 0, len(artifactInputs))
	for name := range artifactInputs {
		names = append(names, name)
	}
	sort.Strings(names)
	return strings.Join(names, ", ")
}

// validateArtifactBindings is rule 56 and the compiled half of rule 55's
// delivery set: a composed task's bindings are sorted by input and name a
// producer in its needs closure that records artifacts, or one of this
// Orkflow's artifact inputs; no other task binds; and every task's
// input_artifacts is exactly the set its with: names literally.
func validateArtifactBindings(wf *orkstackv1.Orkflow, byID map[string]*orkstackv1.Task,
	closures map[string]map[string]struct{}, artifactInputs map[string]struct{}) error {
	for taskIndex, task := range wf.GetTasks() {
		id := task.GetId()
		// Rule 55, compiled: the delivery set is the literal reads.
		if want, got := deliverySet(task), task.GetInputArtifacts(); !equalStrings(want, got) {
			return validationError(fmt.Sprintf("tasks[%d].input_artifacts", taskIndex), fmt.Errorf(
				"task %q: input_artifacts %s is not the set its templates name literally (%s)",
				id, bracketed(got), bracketed(want)))
		}
		// Rule 56: bindings only on a composed task, sorted, each naming a
		// producer in the needs closure that records artifacts, or _inputs.
		bindings := task.GetChildArtifacts()
		if len(bindings) == 0 {
			continue
		}
		if task.GetOrkflow() == nil {
			return onlyComposedBinds(taskIndex, id)
		}
		for i, b := range bindings {
			field := fmt.Sprintf("tasks[%d].artifacts.%s", taskIndex, b.GetInput())
			at := func(format string, args ...any) error {
				return validationError(field, fmt.Errorf("task %q: artifacts.%s: "+format,
					append([]any{id, b.GetInput()}, args...)...))
			}
			if i > 0 && bindings[i-1].GetInput() >= b.GetInput() {
				return validationError(fmt.Sprintf("tasks[%d].child_artifacts", taskIndex), fmt.Errorf(
					"task %q: child_artifacts are sorted by input, each input once (%q after %q)",
					id, b.GetInput(), bindings[i-1].GetInput()))
			}
			if len(b.GetInput()) > maxArtifactInputBytes || !artifactInputPattern.MatchString(b.GetInput()) {
				return at("an artifact input's name is %s, at most %d bytes", artifactInputPattern, maxArtifactInputBytes)
			}
			producer, name := b.GetTaskId(), b.GetName()
			if !validBindingHalves(producer, name) {
				return at("%q must be \"<task>/<name>\"", producer+"/"+name)
			}
			if producer == InputsProducer {
				if _, ok := artifactInputs[name]; !ok {
					return at("%q names no artifact input of this Orkflow (declared: %s)",
						producer+"/"+name, declaredList(artifactInputs))
				}
				continue
			}
			source, exists := byID[producer]
			if !exists {
				return at("%q names unknown task %q", producer+"/"+name, producer)
			}
			if _, ok := closures[id][producer]; !ok {
				return at("task %q is not in the needs closure of %q; add it to needs:", producer, id)
			}
			switch {
			case source.GetAwait() != nil:
				return at("%q is an awaited task and produces no artifacts", producer)
			case source.GetSleep() != nil:
				return at("%q is a sleeping task and produces no artifacts", producer)
			case source.GetEach() != nil:
				return at("%q is a fan-out; a binding names one artifact of one task, and a fan-out's "+
					"artifacts belong to its items", producer)
			}
		}
	}
	return nil
}

func equalStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func bracketed(names []string) string {
	return "[" + strings.Join(names, " ") + "]"
}

// MaxArtifactOutputs bounds the artifact outputs one Orkflow declares
// (rule 57).
const MaxArtifactOutputs = 16

// OutputsDeclared reports whether wf declares a return contract: a value
// output or an artifact output. Every reader that asks "does this Orkflow
// return anything" asks this, never the length of one map.
func OutputsDeclared(wf *orkstackv1.Orkflow) bool {
	return len(wf.GetOutputs()) > 0 || len(wf.GetOutputArtifacts()) > 0
}

// compileOutputs decodes outputs: strictly: a scalar is a template (a value
// with no template action is a constant), a mapping is exactly
// {artifact: "<task>/<name>"}; anything else is refused, naming the output.
func compileOutputs(nodes map[string]yaml.Node) (map[string]string, map[string]*orkstackv1.OutputArtifact, error) {
	if len(nodes) == 0 {
		return nil, nil, nil
	}
	var values map[string]string
	var returned map[string]*orkstackv1.OutputArtifact
	names := make([]string, 0, len(nodes))
	for name := range nodes {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		node := nodes[name]
		field := "outputs." + name
		switch node.Kind {
		case yaml.ScalarNode:
			if node.Tag != "!!str" {
				return nil, nil, validationError(field, fmt.Errorf(
					"line %d: %s: an output is a template string or {artifact: <task>/<name>}, got %s",
					node.Line, field, DescribeNode(&node)))
			}
			if values == nil {
				values = map[string]string{}
			}
			values[name] = node.Value
		case yaml.MappingNode:
			taskID, artifact, err := decodeOutputArtifact(field, &node)
			if err != nil {
				return nil, nil, err
			}
			if returned == nil {
				returned = map[string]*orkstackv1.OutputArtifact{}
			}
			returned[name] = &orkstackv1.OutputArtifact{TaskId: taskID, Name: artifact}
		default:
			return nil, nil, validationError(field, fmt.Errorf(
				"line %d: %s: an output is a template string or {artifact: <task>/<name>}, got %s",
				node.Line, field, DescribeNode(&node)))
		}
	}
	return values, returned, nil
}

func decodeOutputArtifact(field string, node *yaml.Node) (string, string, error) {
	var value *yaml.Node
	for i := 0; i+1 < len(node.Content); i += 2 {
		key, v := node.Content[i], node.Content[i+1]
		if key.Value != "artifact" {
			return "", "", validationError(field, fmt.Errorf(
				"line %d: %s: an output is a template string or {artifact: <task>/<name>}; %q is not a key of one",
				key.Line, field, key.Value))
		}
		value = v
	}
	if value == nil {
		return "", "", validationError(field, fmt.Errorf(
			"line %d: %s: an output is a template string or {artifact: <task>/<name>}", node.Line, field))
	}
	if value.Kind != yaml.ScalarNode || value.Tag != "!!str" {
		return "", "", validationError(field, fmt.Errorf(
			"line %d: %s.artifact must be a string \"<task>/<name>\", got %s", value.Line, field, DescribeNode(value)))
	}
	taskID, name, err := ParseArtifactBinding(value.Value)
	if err != nil {
		return "", "", validationError(field, fmt.Errorf("%s: %w", field, err))
	}
	return taskID, name, nil
}

// validateOutputArtifacts is rule 57: at most MaxArtifactOutputs artifact
// outputs, each named by rule 29's key rule and naming an artifact of a task
// that records artifacts — not an awaited, sleeping or fan-out task, and not
// one rule 41 says is always skipped when the run succeeds — or one of this
// Orkflow's artifact inputs. A composed task's child must return the
// artifact; internal/api checks that, against the child.
func validateOutputArtifacts(wf *orkstackv1.Orkflow, byID map[string]*orkstackv1.Task,
	artifactInputs map[string]struct{}) error {
	returned := wf.GetOutputArtifacts()
	if len(returned) == 0 {
		return nil
	}
	if len(returned) > MaxArtifactOutputs {
		return validationError("outputs", fmt.Errorf(
			"orkflow %q declares %d artifact outputs; at most %d", wf.GetName(), len(returned), MaxArtifactOutputs))
	}
	names := make([]string, 0, len(returned))
	for name := range returned {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		if err := checkOutputName(name); err != nil {
			return err
		}
		field := "outputs." + name
		if _, both := wf.GetOutputs()[name]; both {
			return validationError(field, fmt.Errorf("%s is declared both as a value and as an artifact", field))
		}
		out := returned[name]
		producer, artifact := out.GetTaskId(), out.GetName()
		at := func(format string, args ...any) error {
			return validationError(field, fmt.Errorf("%s: "+format, append([]any{field}, args...)...))
		}
		if !validBindingHalves(producer, artifact) {
			return at("%q must be \"<task>/<name>\"", producer+"/"+artifact)
		}
		if producer == InputsProducer {
			if _, ok := artifactInputs[artifact]; !ok {
				return at("%q names no artifact input of this Orkflow (declared: %s)",
					producer+"/"+artifact, declaredList(artifactInputs))
			}
			continue
		}
		source, ok := byID[producer]
		if !ok {
			return at("%q names unknown task %q", producer+"/"+artifact, producer)
		}
		switch {
		case source.GetAwait() != nil:
			return at("%q is an awaited task and produces no artifacts", producer)
		case source.GetSleep() != nil:
			return at("%q is a sleeping task and produces no artifacts", producer)
		case source.GetEach() != nil:
			return at("%q is a fan-out; an output names one artifact of one task, and a fan-out's "+
				"artifacts belong to its items", producer)
		case source.GetJoin() == orkstackv1.JoinPolicy_JOIN_POLICY_ANY_FAILED:
			// Rule 41 for artifact outputs.
			return at("task %q has join: any_failed, so it is always skipped when the run succeeds", producer)
		}
	}
	return nil
}
