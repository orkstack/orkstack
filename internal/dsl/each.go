package dsl

import (
	"fmt"
	"regexp"
	"strings"
	"text/template/parse"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/structpb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
)

// MaxEachItems bounds a whole Orkflow: the sum of max_items over every each:
// task it declares (docs/DSL.md rule 21). It is the ceiling
// ORK_EACH_MAX_ITEMS may lower and never raise; engine/each.go derives the
// per-item history bounds from the declared sum (spec §5.1).
const MaxEachItems = 1000

// DefaultEachAs is the name .each binds an item to when `as:` is absent.
const DefaultEachAs = "item"

// maxEachAsBytes bounds `each.as` (rule 20).
const maxEachAsBytes = 64

// eachAsPattern is rule 20's shape: a Go template field name, so no "-".
var eachAsPattern = regexp.MustCompile(`^[a-z][a-z0-9_]*$`)

// eachDocument is one fan-out's declaration. KnownFields(true) applies, so a
// misspelled key inside `each:` is refused.
type eachDocument struct {
	Over string `yaml:"over"`
	As   string `yaml:"as"`
	// MaxItems is required: a pointer so an absent key is refused by name
	// rather than read as 0.
	MaxItems    *int32 `yaml:"max_items"`
	MaxParallel *int32 `yaml:"max_parallel"`
}

// compileEach builds a task's EachSpec, or nil when the document declares no
// `each:`. `as` is normalised here, so a document that writes `as: item` and
// one that omits `as` compile to one message and one digest; a source that
// does not parse is left nil and rule 19 reports why.
func compileEach(taskIndex int, td taskDocument) (*orkstackv1.EachSpec, error) {
	if td.Each == nil {
		return nil, nil
	}
	if td.Each.MaxItems == nil {
		return nil, validationError(fmt.Sprintf("tasks[%d].each.max_items", taskIndex), fmt.Errorf(
			"task %q: each.max_items is required — a fan-out with no bound could expand into an "+
				"unbounded run history, and the author is the only party who knows the real bound", td.ID))
	}
	as := td.Each.As
	if as == "" {
		as = DefaultEachAs
	}
	var maxParallel int32
	if td.Each.MaxParallel != nil {
		maxParallel = *td.Each.MaxParallel
	}
	spec := &orkstackv1.EachSpec{
		Over:        td.Each.Over,
		As:          as,
		MaxItems:    *td.Each.MaxItems,
		MaxParallel: maxParallel,
	}
	if source, err := ParseEachOver(td.Each.Over); err == nil {
		spec.Source = source
	}
	return spec, nil
}

// eachOverShapes is the accepted grammar, quoted in every refusal.
const eachOverShapes = `{{ .inputs.NAME }}, {{ .inputs.NAME.KEY… }}, {{ .tasks.ID.output.KEY… }}, ` +
	`{{ index .inputs "NAME" "KEY" … }} or {{ index .tasks "ID" "output" "KEY" … }}`

// ParseEachOver compiles one `each.over` string into its source. It accepts
// exactly one template action, after trimming surrounding whitespace, in one
// of four shapes, and nothing else:
//
//	{{ .inputs.NAME }}                    {{ .inputs.NAME.K1.K2 }}
//	{{ .tasks.ID.output.K1 }}             {{ .tasks.ID.output.K1.K2 }}
//	{{ index .inputs "NAME" "K1" … }}     {{ index .tasks "ID" "output" "K1" … }}
//
// (trim markers {{- and -}} are accepted). In the index form every argument
// after the root is a string literal; a task id or key containing "-" can only
// be written that way, which is text/template's rule, not this one's. Refused:
// text around the action, two actions, a pipeline, a function other than
// index, a variable, a non-literal index argument, a .tasks reference that
// does not continue with "output" and at least one key, a root other than
// .inputs/.tasks, and an empty string. The error names the shape that was
// seen and shows the accepted ones. It never executes a template.
func ParseEachOver(over string) (*orkstackv1.EachSource, error) {
	refuse := func(seen string) error {
		return fmt.Errorf("each.over %q is not one reference to an array: %s; accepted: %s",
			over, seen, eachOverShapes)
	}
	text := strings.TrimSpace(over)
	if text == "" {
		return nil, refuse("it is empty")
	}
	tmpl, err := Parse("each.over", text)
	if err != nil {
		return nil, refuse("it does not parse (" + err.Error() + ")")
	}
	if len(tmpl.Templates()) > 1 {
		return nil, refuse("it defines a template")
	}
	if tmpl.Tree == nil || tmpl.Tree.Root == nil {
		return nil, refuse("it holds no action")
	}
	var action *parse.ActionNode
	for _, node := range tmpl.Tree.Root.Nodes {
		switch typed := node.(type) {
		case *parse.TextNode:
			if strings.TrimSpace(string(typed.Text)) != "" {
				return nil, refuse("it has text around the action")
			}
		case *parse.ActionNode:
			if action != nil {
				return nil, refuse("it holds two actions")
			}
			action = typed
		default:
			return nil, refuse("it holds a control structure, not one action")
		}
	}
	if action == nil {
		return nil, refuse("it is text, not a template action")
	}
	pipe := action.Pipe
	if pipe == nil || len(pipe.Decl) > 0 {
		return nil, refuse("it declares a variable")
	}
	if len(pipe.Cmds) != 1 {
		return nil, refuse("it is a pipeline")
	}
	args := pipe.Cmds[0].Args
	if len(args) == 0 {
		return nil, refuse("it holds no reference")
	}
	switch first := args[0].(type) {
	case *parse.FieldNode:
		if len(args) != 1 {
			return nil, refuse("a field is followed by arguments")
		}
		return eachSourceOf(first.Ident, refuse)
	case *parse.IdentifierNode:
		if first.Ident != "index" {
			return nil, refuse(fmt.Sprintf("it calls %q, and the only function allowed is index", first.Ident))
		}
		if len(args) < 2 {
			return nil, refuse("index has no root")
		}
		base, ok := args[1].(*parse.FieldNode)
		if !ok || len(base.Ident) != 1 {
			return nil, refuse("index's first argument is not .inputs or .tasks")
		}
		ident := []string{base.Ident[0]}
		for _, arg := range args[2:] {
			str, ok := arg.(*parse.StringNode)
			if !ok {
				return nil, refuse("an index argument is not a string literal")
			}
			ident = append(ident, str.Text)
		}
		return eachSourceOf(ident, refuse)
	case *parse.VariableNode:
		return nil, refuse("it reads a variable")
	case *parse.DotNode:
		return nil, refuse("it reads the whole context")
	default:
		return nil, refuse("it is not a field reference")
	}
}

// eachSourceOf turns a root-first key list into its source.
func eachSourceOf(ident []string, refuse func(string) error) (*orkstackv1.EachSource, error) {
	switch ident[0] {
	case "inputs":
		if len(ident) < 2 || ident[1] == "" {
			return nil, refuse(".inputs names no input")
		}
		return &orkstackv1.EachSource{Input: ident[1], Path: nonNilPath(ident[2:])}, nil
	case "tasks":
		if len(ident) < 2 || ident[1] == "" {
			return nil, refuse(".tasks names no task")
		}
		if len(ident) < 3 || ident[2] != "output" {
			return nil, refuse(fmt.Sprintf(".tasks.%s does not continue with .output", ident[1]))
		}
		if len(ident) < 4 {
			return nil, refuse(fmt.Sprintf(".tasks.%s.output names no key — a values.json is an object; name the key that holds the array", ident[1]))
		}
		return &orkstackv1.EachSource{TaskId: ident[1], Path: ident[3:]}, nil
	default:
		return nil, refuse(fmt.Sprintf("its root is .%s; a fan-out reads .inputs or .tasks", ident[0]))
	}
}

// nonNilPath returns nil for an empty path, so a compiled source and a
// hand-built one compare equal under proto.Equal however the slice was made.
func nonNilPath(path []string) []string {
	if len(path) == 0 {
		return nil
	}
	return append([]string(nil), path...)
}

// EachItemsDeclared is the sum of max_items over wf's each: tasks: the most
// items one run of wf can expand. Pure; 0 for an Orkflow with no fan-out.
func EachItemsDeclared(wf *orkstackv1.Orkflow) int32 {
	var sum int64
	for _, task := range wf.GetTasks() {
		if e := task.GetEach(); e != nil && e.GetMaxItems() > 0 {
			sum += int64(e.GetMaxItems())
		}
	}
	if sum > 1<<31-1 {
		return 1<<31 - 1
	}
	return int32(sum)
}

// EachContributors renders "convert 1000, render 200": every fan-out and its
// declared max_items, in declaration order, for the rule-21 sentences.
func EachContributors(wf *orkstackv1.Orkflow) string {
	parts := make([]string, 0, len(wf.GetTasks()))
	for _, task := range wf.GetTasks() {
		if e := task.GetEach(); e != nil {
			parts = append(parts, fmt.Sprintf("%s %d", task.GetId(), e.GetMaxItems()))
		}
	}
	return strings.Join(parts, ", ")
}

// EachSumAnchor returns the field of the first fan-out whose running sum of
// max_items passes limit ("tasks[i].each.max_items"), or "" when none does.
func EachSumAnchor(wf *orkstackv1.Orkflow, limit int32) string {
	var sum int64
	for i, task := range wf.GetTasks() {
		if e := task.GetEach(); e != nil {
			sum += int64(e.GetMaxItems())
			if sum > int64(limit) {
				return fmt.Sprintf("tasks[%d].each.max_items", i)
			}
		}
	}
	return ""
}

// validateEach enforces rules 19–23, and rule 24's two non-template halves
// (a fan-out's source and its needs may not name a fan-out), on the compiled
// message, so a hand-built request meets them.
func validateEach(wf *orkstackv1.Orkflow, byID map[string]*orkstackv1.Task, closures map[string]map[string]struct{}) error {
	for taskIndex, task := range wf.GetTasks() {
		spec := task.GetEach()
		if spec == nil {
			continue
		}
		id := task.GetId()
		at := func(field string) string { return fmt.Sprintf("tasks[%d].%s", taskIndex, field) }

		// Rule 23: a fan-out is not pinned.
		if task.GetPin() != "" {
			return validationError(at("pin"), fmt.Errorf(
				"task %q: a fan-out cannot join a pin in this version — its items would share one "+
					"/orkstack/shared concurrently", id))
		}

		// Rule 24 (needs): a fan-out may not need another fan-out.
		for needIndex, need := range task.GetNeeds() {
			if byID[need].GetEach() != nil {
				return validationError(at(fmt.Sprintf("needs[%d]", needIndex)), fmt.Errorf(
					"task %q: needs fan-out %q — a fan-out may not depend on another fan-out: every "+
						"item would stage every item of %q for a dependency nothing may read", id, need, need))
			}
		}

		// Rule 19: `over` is one reference to an array.
		parsed, err := ParseEachOver(spec.GetOver())
		if err != nil {
			return validationError(at("each.over"), fmt.Errorf("task %q: %w", id, err))
		}
		if !proto.Equal(spec.GetSource(), parsed) {
			return validationError(at("each.source"), fmt.Errorf(
				"task %q: each.source does not match each.over %q — the source is compiled from over "+
					"and must agree with it", id, spec.GetOver()))
		}
		if producer := parsed.GetTaskId(); producer != "" {
			if _, exists := byID[producer]; !exists {
				return validationError(at("each.over"), fmt.Errorf(
					"task %q: each.over: template references unknown task %q", id, producer))
			}
			if byID[producer].GetEach() != nil {
				return validationError(at("each.over"), fmt.Errorf(
					"task %q: each.over: %q is a fan-out; a fan-out cannot fan out over another "+
						"fan-out's items (no nesting)", id, producer))
			}
			// Rule 30: a fan-out does not read a composed task.
			if IsComposedTask(byID[producer]) {
				return validationError(at("each.over"), fmt.Errorf(
					"task %q: each.over: %q is a composed task; its values are the child's declared "+
						"outputs, which are strings and never an array", id, producer))
			}
			if _, ok := closures[id][producer]; !ok {
				return validationError(at("each.over"), fmt.Errorf(
					"task %q: each.over: template references task %q which is not in its needs closure",
					id, producer))
			}
		} else if err := checkEachInput(wf, id, parsed); err != nil {
			return validationError(at("each.over"), err)
		}

		// Rule 20: `as` is a template field name.
		as := spec.GetAs()
		switch {
		case as == "":
			return validationError(at("each.as"), fmt.Errorf(
				"task %q: each.as is empty — the compiler writes %q when the document names none",
				id, DefaultEachAs))
		case len(as) > maxEachAsBytes:
			return validationError(at("each.as"), fmt.Errorf(
				"task %q: each.as of %d bytes exceeds the %d-byte limit", id, len(as), maxEachAsBytes))
		case !eachAsPattern.MatchString(as):
			return validationError(at("each.as"), fmt.Errorf(
				"task %q: each.as %q must match %s — it is a template field name, so .each.%s must parse",
				id, as, eachAsPattern, as))
		case as == "index" || as == "count":
			return validationError(at("each.as"), fmt.Errorf(
				"task %q: each.as %q is reserved — .each.index and .each.count are the item's position", id, as))
		}

		// Rule 21 (per task): 1 <= max_items <= MaxEachItems.
		if n := spec.GetMaxItems(); n < 1 || n > MaxEachItems {
			return validationError(at("each.max_items"), fmt.Errorf(
				"task %q: each.max_items %d is out of range (1 … %d)", id, n, MaxEachItems))
		}

		// Rule 22: max_parallel is not negative and not above max_items.
		if p := spec.GetMaxParallel(); p < 0 || p > spec.GetMaxItems() {
			return validationError(at("each.max_parallel"), fmt.Errorf(
				"task %q: each.max_parallel %d must be between 0 (no bound of its own) and max_items %d",
				id, p, spec.GetMaxItems()))
		}
	}

	// Rule 21 (per Orkflow): the declared total stays inside one history budget.
	if total := EachItemsDeclared(wf); total > MaxEachItems {
		return validationError(EachSumAnchor(wf, MaxEachItems), fmt.Errorf(
			"orkflow declares %d items across its fan-outs (%s); at most %d — the items of one run "+
				"share one history budget", total, EachContributors(wf), MaxEachItems))
	}
	return nil
}

// checkEachInput is rule 19 for an input source: the input is declared, and
// its declared type is array when the path is empty and object when it is not.
func checkEachInput(wf *orkstackv1.Orkflow, taskID string, source *orkstackv1.EachSource) error {
	name := source.GetInput()
	decl, ok := wf.GetInputs().GetFields()[name]
	if !ok {
		return fmt.Errorf("task %q: each.over: template references undeclared input %q", taskID, name)
	}
	declared := "with no type"
	if fields, isStruct := decl.GetKind().(*structpb.Value_StructValue); isStruct {
		if typ, isString := fields.StructValue.GetFields()["type"].GetKind().(*structpb.Value_StringValue); isString {
			declared = typ.StringValue
		}
	}
	want := "array"
	if len(source.GetPath()) > 0 {
		want = "object"
	}
	if declared == want {
		return nil
	}
	if declared == "array" {
		return fmt.Errorf("task %q: each.over: input %q is declared array, which has no key %q to follow — "+
			"name the input itself", taskID, name, source.GetPath()[0])
	}
	if !strings.HasPrefix(declared, "with") {
		declared = "type: " + declared
	}
	return fmt.Errorf("task %q: each.over: input %q is declared %s; a fan-out needs an array — declare it "+
		"type: array, or name the key that holds one", taskID, name, declared)
}
