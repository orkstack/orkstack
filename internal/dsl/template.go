package dsl

import (
	"fmt"
	"sort"
	"strings"
	"text/template"
	"text/template/parse"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
)

// TemplateFuncs are the helper stubs the DSL exposes to templates. They exist
// so a document parses; the Orker supplies the real implementations at render
// time (see internal/task package doc).
func TemplateFuncs() template.FuncMap {
	return template.FuncMap{
		"artifact": func(string) string { return "" },
		"env":      func(string) string { return "" },
	}
}

// Parse parses a DSL template expression with the helper stubs registered.
func Parse(name, text string) (*template.Template, error) {
	return template.New(name).Option("missingkey=zero").Funcs(TemplateFuncs()).Parse(text)
}

// templateChecker validates rule 6 for one task.
type templateChecker struct {
	taskID   string
	inputs   map[string]struct{}
	tasks    map[string]*orkstackv1.Task
	upstream map[string]struct{}
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
	if tmpl.Tree == nil || tmpl.Tree.Root == nil {
		return nil
	}
	return c.walk(path, tmpl.Tree.Root, false)
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
		if err := c.checkCommand(path, typed); err != nil {
			return err
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
	case *parse.IfNode:
		return c.walkBranch(path, &typed.BranchNode, rebound, rebound)
	case *parse.WithNode:
		return c.walkBranch(path, &typed.BranchNode, rebound, true)
	case *parse.RangeNode:
		return c.walkBranch(path, &typed.BranchNode, rebound, true)
	case *parse.TemplateNode:
		return c.walk(path, typed.Pipe, rebound)
	}
	return nil
}

func (c *templateChecker) walkBranch(path string, branch *parse.BranchNode, rebound, bodyRebound bool) error {
	if err := c.walk(path, branch.Pipe, rebound); err != nil {
		return err
	}
	if err := c.walk(path, branch.List, bodyRebound); err != nil {
		return err
	}
	return c.walk(path, branch.ElseList, bodyRebound)
}

func (c *templateChecker) checkField(path string, ident []string) error {
	if len(ident) == 0 {
		return nil
	}
	switch ident[0] {
	case "inputs":
		if len(ident) < 2 {
			return nil
		}
		if _, ok := c.inputs[ident[1]]; !ok {
			return fmt.Errorf("task %q: %s: template references undeclared input %q",
				c.taskID, path, ident[1])
		}
	case "tasks":
		if len(ident) < 2 {
			return nil
		}
		return c.checkUpstream(path, ident[1])
	}
	return nil
}

func (c *templateChecker) checkUpstream(path, taskID string) error {
	if _, exists := c.tasks[taskID]; !exists {
		return fmt.Errorf("task %q: %s: template references unknown task %q", c.taskID, path, taskID)
	}
	if _, ok := c.upstream[taskID]; !ok {
		return fmt.Errorf("task %q: %s: template references task %q which is not in its needs closure",
			c.taskID, path, taskID)
	}
	return nil
}

// checkCommand validates the `artifact` and `env` helpers.
func (c *templateChecker) checkCommand(path string, cmd *parse.CommandNode) error {
	if len(cmd.Args) == 0 {
		return nil
	}
	ident, ok := cmd.Args[0].(*parse.IdentifierNode)
	if !ok {
		return nil
	}
	switch ident.Ident {
	case "artifact":
		if len(cmd.Args) < 2 {
			return fmt.Errorf("task %q: %s: artifact helper requires a \"<task>/<name>\" argument", c.taskID, path)
		}
		str, ok := cmd.Args[1].(*parse.StringNode)
		if !ok {
			return nil // dynamic reference: the Orker resolves it at render time
		}
		producer, name, found := strings.Cut(str.Text, "/")
		if !found || producer == "" || name == "" {
			return fmt.Errorf("task %q: %s: artifact reference %q must be \"<task>/<name>\"",
				c.taskID, path, str.Text)
		}
		return c.checkUpstream(path, producer)
	case "env":
		if len(cmd.Args) < 2 {
			return fmt.Errorf("task %q: %s: env helper requires a variable name", c.taskID, path)
		}
	}
	return nil
}
