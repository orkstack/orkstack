package dsl

import (
	"fmt"
	"sort"
	"strings"
	"text/template"
	"text/template/parse"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
)

// TemplateFuncs are the helper stubs the DSL exposes to templates: `artifact`,
// `env`, and `sh` (docs/DSL.md, "Templating"). They exist so a document
// parses; the Orker supplies the real implementations at render time
// (internal/orker/render.go).
func TemplateFuncs() template.FuncMap {
	return template.FuncMap{
		"artifact": func(string) string { return "" },
		"env":      func(string) string { return "" },
		"sh":       func(any) string { return "" },
	}
}

// Parse parses a DSL template expression with the helper stubs registered.
func Parse(name, text string) (*template.Template, error) {
	return template.New(name).Option("missingkey=zero").Funcs(TemplateFuncs()).Parse(text)
}

// templateChecker validates rule 8 for one task.
//
// A checker built by ReferencedUpstream has refs set: it then COLLECTS the
// task ids read through .tasks instead of refusing anything, walking the same
// tree with the same spellings, so the checker and the collector cannot
// disagree about what counts as a reference.
type templateChecker struct {
	taskID   string
	inputs   map[string]struct{}
	tasks    map[string]*orkstackv1.Task
	upstream map[string]struct{}
	refs     map[string]struct{}
}

// ReferencedUpstream lists the task ids a template reads through .tasks —
// the field spelling checkField accepts (.tasks.<id>…) and the index
// spelling checkIndex accepts (index .tasks "<id>" …) — sorted and deduped.
// A parse error is returned as is; a dynamic index argument is not a
// reference and is ignored. It is advisory: callers use it to SAY which
// upstream outputs a condition read, never to decide anything.
func ReferencedUpstream(text string) ([]string, error) {
	if !strings.Contains(text, "{{") {
		return nil, nil
	}
	tmpl, err := Parse("reference", text)
	if err != nil {
		return nil, err
	}
	c := &templateChecker{refs: map[string]struct{}{}}
	if err := c.walkTemplate("", tmpl); err != nil {
		return nil, err
	}
	out := make([]string, 0, len(c.refs))
	for id := range c.refs {
		out = append(out, id)
	}
	sort.Strings(out)
	return out, nil
}

// refuse returns err while checking and nil while collecting: a collector
// reports what a template reads and never judges it.
func (c *templateChecker) refuse(err error) error {
	if c.refs != nil {
		return nil
	}
	return err
}

// checkValue walks every string inside a decoded `with` value.
func (c *templateChecker) checkValue(path string, value any) error {
	switch typed := value.(type) {
	case string:
		return c.checkString(path, typed)
	case map[string]any:
		keys := make([]string, 0, len(typed))
		for k := range typed {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		for _, k := range keys {
			if err := c.checkValue(path+"."+k, typed[k]); err != nil {
				return err
			}
		}
	case []any:
		for i, item := range typed {
			if err := c.checkValue(fmt.Sprintf("%s[%d]", path, i), item); err != nil {
				return err
			}
		}
	}
	return nil
}

func (c *templateChecker) checkString(path, text string) error {
	if !strings.Contains(text, "{{") {
		return nil
	}
	tmpl, err := Parse(c.taskID+"/"+path, text)
	if err != nil {
		return fmt.Errorf("task %q: %s: template parse error: %w", c.taskID, path, err)
	}
	return c.walkTemplate(path, tmpl)
}

// walkTemplate walks the main template and every template it defines.
func (c *templateChecker) walkTemplate(path string, tmpl *template.Template) error {
	associated := tmpl.Templates()
	sort.Slice(associated, func(i, j int) bool {
		return associated[i].Name() < associated[j].Name()
	})
	for _, candidate := range associated {
		if candidate.Tree == nil || candidate.Tree.Root == nil {
			continue
		}
		// A named template receives caller-selected dot. Its relative fields
		// cannot be treated as root DSL context without inventing a type rule;
		// `$` references remain root and are checked regardless of rebound.
		rebound := candidate.Name() != tmpl.Name()
		if err := c.walk(path, candidate.Tree.Root, rebound); err != nil {
			return err
		}
	}
	return nil
}

// walk inspects the parse tree. rebound is true inside `with`/`range` bodies,
// where `.` no longer refers to the root context; references there are left to
// the Orker rather than rejected.
func (c *templateChecker) walk(path string, node parse.Node, rebound bool) error {
	switch typed := node.(type) {
	case nil:
		return nil
	case *parse.ListNode:
		if typed == nil {
			return nil
		}
		for _, child := range typed.Nodes {
			if err := c.walk(path, child, rebound); err != nil {
				return err
			}
		}
	case *parse.ActionNode:
		return c.walk(path, typed.Pipe, rebound)
	case *parse.PipeNode:
		if typed == nil {
			return nil
		}
		for _, cmd := range typed.Cmds {
			if err := c.walk(path, cmd, rebound); err != nil {
				return err
			}
		}
	case *parse.CommandNode:
		handled, err := c.checkCommand(path, typed, rebound)
		if err != nil {
			return err
		}
		if handled {
			return nil
		}
		for _, arg := range typed.Args {
			if err := c.walk(path, arg, rebound); err != nil {
				return err
			}
		}
	case *parse.FieldNode:
		if rebound {
			return nil
		}
		return c.checkField(path, typed.Ident)
	case *parse.VariableNode:
		// `$` is the immutable root context even when dot is rebound by with
		// or range. Ordinary named locals stay permissive until declaration
		// tracking is part of the DSL contract.
		if len(typed.Ident) > 1 && typed.Ident[0] == "$" {
			return c.checkField(path, typed.Ident[1:])
		}
		return nil
	case *parse.ChainNode:
		return c.walk(path, typed.Node, rebound)
	case *parse.IfNode:
		return c.walkBranch(path, &typed.BranchNode, rebound, rebound, rebound)
	case *parse.WithNode:
		return c.walkBranch(path, &typed.BranchNode, rebound,
			withBodyRebound(typed.Pipe, rebound), rebound)
	case *parse.RangeNode:
		return c.walkBranch(path, &typed.BranchNode, rebound, true, rebound)
	case *parse.TemplateNode:
		return c.walk(path, typed.Pipe, rebound)
	}
	return nil
}

func (c *templateChecker) walkBranch(
	path string,
	branch *parse.BranchNode,
	pipeRebound bool,
	listRebound bool,
	elseRebound bool,
) error {
	if err := c.walk(path, branch.Pipe, pipeRebound); err != nil {
		return err
	}
	if err := c.walk(path, branch.List, listRebound); err != nil {
		return err
	}
	return c.walk(path, branch.ElseList, elseRebound)
}

// withBodyRebound reports whether a with body changes dot away from the root
// context. `with .` preserves the incoming dot and `with $` restores root;
// other pipelines produce a caller-selected relative value.
func withBodyRebound(pipe *parse.PipeNode, rebound bool) bool {
	if pipe == nil || len(pipe.Cmds) != 1 || len(pipe.Cmds[0].Args) != 1 {
		return true
	}
	switch arg := pipe.Cmds[0].Args[0].(type) {
	case *parse.DotNode:
		return rebound
	case *parse.VariableNode:
		if len(arg.Ident) == 1 && arg.Ident[0] == "$" {
			return false
		}
	}
	return true
}

func (c *templateChecker) checkField(path string, ident []string) error {
	if len(ident) == 0 {
		return nil
	}
	switch ident[0] {
	case "inputs":
		if len(ident) < 2 {
			return c.refuse(fmt.Errorf("task %q: %s: .inputs must name a declared input", c.taskID, path))
		}
		return c.checkInput(path, ident[1])
	case "tasks":
		if len(ident) < 2 {
			return c.refuse(fmt.Errorf("task %q: %s: .tasks must name an upstream task", c.taskID, path))
		}
		if c.refs != nil {
			c.refs[ident[1]] = struct{}{}
			return nil
		}
		return c.checkUpstream(path, ident[1])
	default:
		return c.refuse(fmt.Errorf("task %q: %s: unknown template root %q", c.taskID, path, ident[0]))
	}
}

func (c *templateChecker) checkInput(path, name string) error {
	if _, ok := c.inputs[name]; !ok {
		return c.refuse(fmt.Errorf("task %q: %s: template references undeclared input %q",
			c.taskID, path, name))
	}
	return nil
}

func (c *templateChecker) checkUpstream(path, taskID string) error {
	if c.refs != nil {
		return nil
	}
	if _, exists := c.tasks[taskID]; !exists {
		return fmt.Errorf("task %q: %s: template references unknown task %q", c.taskID, path, taskID)
	}
	if _, ok := c.upstream[taskID]; !ok {
		return fmt.Errorf("task %q: %s: template references task %q which is not in its needs closure",
			c.taskID, path, taskID)
	}
	return nil
}

// checkCommand validates helper calls and references made through index. The
// handled result tells the walker that checkCommand already traversed the
// command's arguments (needed to avoid rejecting the bare `.inputs`/`.tasks`
// base of a valid index expression).
func (c *templateChecker) checkCommand(path string, cmd *parse.CommandNode, rebound bool) (bool, error) {
	if len(cmd.Args) == 0 {
		return false, nil
	}
	ident, ok := cmd.Args[0].(*parse.IdentifierNode)
	if !ok {
		return false, nil
	}
	switch ident.Ident {
	case "index":
		return true, c.checkIndex(path, cmd, rebound)
	case "artifact":
		if len(cmd.Args) < 2 {
			return false, c.refuse(fmt.Errorf("task %q: %s: artifact helper requires a \"<task>/<name>\" argument", c.taskID, path))
		}
		str, ok := cmd.Args[1].(*parse.StringNode)
		if !ok {
			return false, nil // dynamic reference: the Orker resolves it at render time
		}
		producer, name, found := strings.Cut(str.Text, "/")
		if !found || producer == "" || name == "" {
			return false, c.refuse(fmt.Errorf("task %q: %s: artifact reference %q must be \"<task>/<name>\"",
				c.taskID, path, str.Text))
		}
		if err := c.checkUpstream(path, producer); err != nil {
			return false, err
		}
		// Rule 18: an awaited task produces no artifacts — only values.
		if c.refs == nil && c.tasks[producer].GetAwait() != nil {
			return false, c.refuse(fmt.Errorf("task %q: %s: %q is an awaited task and produces no artifacts; "+
				"read its values with .tasks.%s.output", c.taskID, path, producer, producer))
		}
		return false, nil
	case "env":
		if len(cmd.Args) < 2 {
			return false, c.refuse(fmt.Errorf("task %q: %s: env helper requires a variable name", c.taskID, path))
		}
	}
	return false, nil
}

func (c *templateChecker) checkIndex(path string, cmd *parse.CommandNode, rebound bool) error {
	if len(cmd.Args) < 2 {
		return nil
	}

	root := ""
	keyArg := 2
	rootContainer := false
	switch base := cmd.Args[1].(type) {
	case *parse.FieldNode:
		if !rebound && len(base.Ident) == 1 {
			root = base.Ident[0]
		}
	case *parse.VariableNode:
		switch {
		case len(base.Ident) == 2 && base.Ident[0] == "$":
			root = base.Ident[1]
		case len(base.Ident) == 1 && base.Ident[0] == "$":
			rootContainer = true
		}
	case *parse.DotNode:
		rootContainer = !rebound
	}

	if rootContainer {
		if len(cmd.Args) < 3 {
			return nil
		}
		rootKey, literal := cmd.Args[2].(*parse.StringNode)
		if !literal {
			return c.walkIndexArgs(path, cmd.Args[2:], rebound)
		}
		root = rootKey.Text
		keyArg = 3
		if root != "inputs" && root != "tasks" {
			return c.refuse(fmt.Errorf("task %q: %s: unknown template root %q", c.taskID, path, root))
		}
	}

	if root != "inputs" && root != "tasks" {
		return c.walkIndexArgs(path, cmd.Args[1:], rebound)
	}
	if len(cmd.Args) <= keyArg {
		if root == "inputs" {
			return c.refuse(fmt.Errorf("task %q: %s: index inputs must name a declared input", c.taskID, path))
		}
		return c.refuse(fmt.Errorf("task %q: %s: index tasks must name an upstream task", c.taskID, path))
	}
	if key, literal := cmd.Args[keyArg].(*parse.StringNode); literal {
		if root == "inputs" {
			if err := c.checkInput(path, key.Text); err != nil {
				return err
			}
		} else if c.refs != nil {
			c.refs[key.Text] = struct{}{}
		} else if err := c.checkUpstream(path, key.Text); err != nil {
			return err
		}
	}
	return c.walkIndexArgs(path, cmd.Args[keyArg:], rebound)
}

// walkIndexArgs preserves permissive dynamic indexes while still checking any
// concrete root references nested inside their key expressions.
func (c *templateChecker) walkIndexArgs(path string, args []parse.Node, rebound bool) error {
	for _, arg := range args {
		if _, literal := arg.(*parse.StringNode); literal {
			continue
		}
		if err := c.walk(path, arg, rebound); err != nil {
			return err
		}
	}
	return nil
}
