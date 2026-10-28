package dsl

import (
	"bytes"
	"errors"
	"text/template"
	"text/template/parse"
)

// NoValueMarker is what text/template prints for a value that is present and
// nil. Rendering it would silently corrupt a command or a URL, so a nil value
// that reaches the output is an error. The same ten characters are also what a
// user's data, or an author's literal text, may legitimately spell, however
// they came together, and those render as themselves.
const NoValueMarker = "<no value>"

// nilCheck is the name the guard gives the function it appends to every
// action that prints. An author cannot write it: the template is parsed
// before the function exists.
const nilCheck = "ork_print_nil_check"

// Stage names where a TemplateError arose.
const (
	StageParse  = "parse"
	StageRender = "render"
)

// ErrNoValue is the render-stage error for a template that printed a nil
// value.
var ErrNoValue = errors.New("template resolved to " + NoValueMarker)

// TemplateError is a template that did not render. Stage says whether it did
// not parse or did not execute; Err is the cause, ErrNoValue for a nil value.
type TemplateError struct {
	Stage string
	Err   error
}

func (e *TemplateError) Error() string { return e.Stage + ": " + e.Err.Error() }
func (e *TemplateError) Unwrap() error { return e.Err }

// ExecuteChecked parses text with the caller's options and functions, runs it
// against data and returns what it printed, refusing a nil value that would
// print as NoValueMarker.
//
// A nil value is found where it is printed, not in the text it printed. The
// marker in the output can come from a nil value, from a string in data or in
// the template that spells it, or from pieces that only spell it side by side,
// and the output cannot say which. So every action that prints gets one more
// stage on its pipeline, the way html/template adds its escapers, that sees
// the value about to be printed and notes whether it is nil. The template
// then runs once, on the real data, and its output is returned exactly as
// text/template wrote it.
func ExecuteChecked(text string, data map[string]any, parse func(text string) (*template.Template, error)) (string, error) {
	tmpl, err := parse(text)
	if err != nil {
		return "", &TemplateError{Stage: StageParse, Err: err}
	}
	printedNil := false
	tmpl.Funcs(template.FuncMap{nilCheck: func(value any) any {
		if value == nil {
			printedNil = true
		}
		return value
	}})
	guardPrints(tmpl)
	var buf bytes.Buffer
	if err := tmpl.Execute(&buf, data); err != nil {
		return "", &TemplateError{Stage: StageRender, Err: err}
	}
	if printedNil {
		return "", &TemplateError{Stage: StageRender, Err: ErrNoValue}
	}
	return buf.String(), nil
}

// guardPrints appends the nil check to every pipeline of tmpl, and of the
// templates defined beside it, that is printed. A pipeline that declares a
// variable, or tests a branch, or is handed to a template, prints nothing.
func guardPrints(tmpl *template.Template) {
	seen := map[*parse.Tree]bool{}
	for _, t := range tmpl.Templates() {
		if t.Tree != nil && !seen[t.Tree] {
			seen[t.Tree] = true
			guardList(t.Tree.Root)
		}
	}
}

func guardList(list *parse.ListNode) {
	if list == nil {
		return
	}
	for _, node := range list.Nodes {
		switch typed := node.(type) {
		case *parse.ActionNode:
			if pipe := typed.Pipe; len(pipe.Decl) == 0 {
				stage := parse.NewIdentifier(nilCheck).SetPos(pipe.Pos)
				pipe.Cmds = append(pipe.Cmds, &parse.CommandNode{
					NodeType: parse.NodeCommand, Pos: pipe.Pos, Args: []parse.Node{stage},
				})
			}
		case *parse.IfNode:
			guardList(typed.List)
			guardList(typed.ElseList)
		case *parse.RangeNode:
			guardList(typed.List)
			guardList(typed.ElseList)
		case *parse.WithNode:
			guardList(typed.List)
			guardList(typed.ElseList)
		}
	}
}
