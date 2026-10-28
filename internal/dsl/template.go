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
	// anchor is the checked task's field path ("tasks[3]"); a fan-out refusal
	// (rules 24 and 25) is anchored at anchor + "." + the template's path.
	anchor string
	// each is the item name .each.<each> binds while checking a fan-out's
	// `with:` (rule 25); fanOut is whether the checked task declares `each:`.
	// Both are unset while checking a condition.
	each   string
	fanOut bool
	// engine is set while checking a template the ENGINE renders — a
	// composed task's with: and an Orkflow's outputs: (rule 28). It refuses
	// the three Orker helpers, a .tasks read of anything but output, .each,
	// and a .tasks index whose task argument is not a string literal.
	// engineWhat names what the rendered template becomes, for the sentences.
	engine     bool
	engineWhat string
	// outputRef, when set, is called for every literal read of
	// .tasks.<id>.output.<key> (OutputReferences), in either spelling.
	outputRef func(taskID, key string)
	// reads, when set (collect mode, with refs: record and never refuse),
	// receives every read of a task (TaskReads, W1). guards is the stack of
	// task ids guarded at the current point of the walk: the body of an
	// {{ if index .tasks "<id>" }} or {{ with index .tasks "<id>" }}. With
	// reads nil and guards empty the checker behaves exactly as without them.
	reads  *[]TaskRead
	guards []string
}

// recordRead appends one task read when collecting reads.
func (c *templateChecker) recordRead(path, taskID string, kind TaskReadKind, name string) {
	if c.reads == nil {
		return
	}
	guarded := false
	for _, g := range c.guards {
		if g == taskID {
			guarded = true
			break
		}
	}
	*c.reads = append(*c.reads, TaskRead{TaskID: taskID, Kind: kind, Path: path, Guarded: guarded, Name: name})
}

// guardOf is the task id an {{ if }} or {{ with }} pipeline guards: non-empty
// exactly when the pipeline is one command `index .tasks "<id>"` (.tasks not
// rebound) or `index $.tasks "<id>"`, with no declaration. index of a missing
// key is nil, so the body runs only when the task is present. No other
// pipeline is recognised as a guard.
func guardOf(pipe *parse.PipeNode, rebound bool) string {
	if pipe == nil || len(pipe.Decl) > 0 || len(pipe.Cmds) != 1 {
		return ""
	}
	args := pipe.Cmds[0].Args
	if len(args) != 3 {
		return ""
	}
	if ident, ok := args[0].(*parse.IdentifierNode); !ok || ident.Ident != "index" {
		return ""
	}
	switch base := args[1].(type) {
	case *parse.FieldNode:
		if rebound || len(base.Ident) != 1 || base.Ident[0] != "tasks" {
			return ""
		}
	case *parse.VariableNode:
		if len(base.Ident) != 2 || base.Ident[0] != "$" || base.Ident[1] != "tasks" {
			return ""
		}
	default:
		return ""
	}
	id, ok := args[2].(*parse.StringNode)
	if !ok {
		return ""
	}
	return id.Text
}

// engineHelperRefusal is rule 28's sentence for one Orker helper.
func (c *templateChecker) engineHelperRefusal(path, helper string) error {
	var what string
	switch helper {
	case "artifact":
		what = "the artifact helper resolves on an Orker"
	case "env":
		what = "the env helper reads an Orker's allowlisted environment"
	default:
		what = "the sh helper quotes for a container's shell on an Orker"
	}
	return c.refuseAt(path, fmt.Errorf("task %q: %s: %s; %s", c.taskID, path, what, c.engineWhat))
}

// eachKeysSentence names the three legal keys of .each for one item name.
func eachKeysSentence(as string) string {
	return fmt.Sprintf(".each.%s, .each.index and .each.count", as)
}

// refuseAt anchors a fan-out refusal at the template's field.
func (c *templateChecker) refuseAt(path string, err error) error {
	if c.refs != nil {
		return nil
	}
	if c.anchor == "" {
		return err
	}
	return validationError(c.anchor+"."+path, err)
}

// checkEachRoot is rule 25 for one reference to `.each`; key is the first
// key below .each, or "" when the reference names none.
func (c *templateChecker) checkEachRoot(path, key string) error {
	if c.refs != nil {
		return nil
	}
	if c.engine {
		return c.refuseAt(path, fmt.Errorf("task %q: %s: .each is not available here — only a "+
			"fan-out's with: has items, and %s", c.taskID, path, c.engineWhat))
	}
	if strings.HasPrefix(path, "condition") {
		return c.refuseAt(path, fmt.Errorf("task %q: %s: .each is not available in a condition — a "+
			"condition is evaluated once, before the items exist", c.taskID, path))
	}
	if !c.fanOut {
		return c.refuseAt(path, fmt.Errorf("task %q: %s: .each is available only in the with: of a task "+
			"that declares each: (a fan-out)", c.taskID, path))
	}
	if key == c.each || key == "index" || key == "count" {
		return nil
	}
	if key == "" {
		return c.refuseAt(path, fmt.Errorf("task %q: %s: .each must name a key: %s",
			c.taskID, path, eachKeysSentence(c.each)))
	}
	return c.refuseAt(path, fmt.Errorf("task %q: %s: .each.%s is not a key of .each; the keys are %s",
		c.taskID, path, key, eachKeysSentence(c.each)))
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
		return c.walkBranch(path, &typed.BranchNode, rebound, rebound, rebound, c.guardFor(typed.Pipe, rebound))
	case *parse.WithNode:
		return c.walkBranch(path, &typed.BranchNode, rebound,
			withBodyRebound(typed.Pipe, rebound), rebound, c.guardFor(typed.Pipe, rebound))
	case *parse.RangeNode:
		return c.walkBranch(path, &typed.BranchNode, rebound, true, rebound, "")
	case *parse.TemplateNode:
		return c.walk(path, typed.Pipe, rebound)
	}
	return nil
}

// guardFor is guardOf while reads are collected, and "" otherwise, so a
// checker that collects no reads walks exactly as it always did.
func (c *templateChecker) guardFor(pipe *parse.PipeNode, rebound bool) string {
	if c.reads == nil {
		return ""
	}
	return guardOf(pipe, rebound)
}

// walkBranch walks an if/with/range. guard, when non-empty, is pushed onto
// the guard stack around the body (branch.List) only — never the pipeline or
// the else.
func (c *templateChecker) walkBranch(
	path string,
	branch *parse.BranchNode,
	pipeRebound bool,
	listRebound bool,
	elseRebound bool,
	guard string,
) error {
	if err := c.walk(path, branch.Pipe, pipeRebound); err != nil {
		return err
	}
	if guard != "" {
		c.guards = append(c.guards, guard)
	}
	err := c.walk(path, branch.List, listRebound)
	if guard != "" {
		c.guards = c.guards[:len(c.guards)-1]
	}
	if err != nil {
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
		if len(ident) >= 4 && ident[2] == "output" && c.outputRef != nil {
			c.outputRef(ident[1], ident[3])
		}
		c.recordRead(path, ident[1], TaskReadField, "")
		if c.refs != nil {
			c.refs[ident[1]] = struct{}{}
			return nil
		}
		if err := c.checkUpstream(path, ident[1]); err != nil {
			return err
		}
		if c.engine && len(ident) >= 3 && ident[2] != "output" {
			return c.refuseAt(path, fmt.Errorf("task %q: %s: .tasks.%s.%s is not a value — an "+
				"engine-rendered template reads .tasks.<id>.output only, and %s",
				c.taskID, path, ident[1], ident[2], c.engineWhat))
		}
		return nil
	case "each":
		key := ""
		if len(ident) > 1 {
			key = ident[1]
		}
		return c.checkEachRoot(path, key)
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
	// Rule 24: a fan-out's items are files, never template values.
	if c.tasks[taskID].GetEach() != nil {
		return c.refuseAt(path, fmt.Errorf("task %q: %s: %q is a fan-out; its items are read as files "+
			"under /orkstack/input/upstream/%s/<item>/ and /orkstack/input/artifacts/%s/<item>/",
			c.taskID, path, taskID, taskID, taskID))
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
	if c.engine && c.refs == nil {
		switch ident.Ident {
		case "artifact", "env", "sh":
			return false, c.engineHelperRefusal(path, ident.Ident)
		}
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
		c.recordRead(path, producer, TaskReadArtifact, name)
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
			if c.engine {
				return c.dynamicIndexRefusal(path)
			}
			return c.walkIndexArgs(path, cmd.Args[2:], rebound)
		}
		root = rootKey.Text
		keyArg = 3
		if root != "inputs" && root != "tasks" && root != "each" {
			return c.refuse(fmt.Errorf("task %q: %s: unknown template root %q", c.taskID, path, root))
		}
	}

	if root == "each" {
		// Rule 25, index spelling: index .each "part" ….
		if len(cmd.Args) <= keyArg {
			if err := c.checkEachRoot(path, ""); err != nil {
				return err
			}
			return nil
		}
		if key, literal := cmd.Args[keyArg].(*parse.StringNode); literal {
			if err := c.checkEachRoot(path, key.Text); err != nil {
				return err
			}
		} else if err := c.checkEachRoot(path, c.each); err != nil {
			// A dynamic key is legal only where .each is.
			return err
		}
		return c.walkIndexArgs(path, cmd.Args[keyArg:], rebound)
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
	key, literal := cmd.Args[keyArg].(*parse.StringNode)
	if literal {
		if root == "inputs" {
			if err := c.checkInput(path, key.Text); err != nil {
				return err
			}
		} else {
			if err := c.checkTaskIndex(path, key.Text, cmd.Args[keyArg+1:]); err != nil {
				return err
			}
		}
	} else if root == "tasks" && c.engine {
		return c.dynamicIndexRefusal(path)
	}
	return c.walkIndexArgs(path, cmd.Args[keyArg:], rebound)
}

// checkTaskIndex is `index .tasks "<id>" rest…` with a literal id: the
// reference is recorded or checked, and in an engine-rendered template the
// key after the id must be the literal "output" (rule 28).
func (c *templateChecker) checkTaskIndex(path, taskID string, rest []parse.Node) error {
	if c.outputRef != nil && len(rest) >= 2 {
		sub, subLiteral := rest[0].(*parse.StringNode)
		key, keyLiteral := rest[1].(*parse.StringNode)
		if subLiteral && keyLiteral && sub.Text == "output" {
			c.outputRef(taskID, key.Text)
		}
	}
	if len(rest) > 0 {
		c.recordRead(path, taskID, TaskReadIndex, "")
	}
	if c.refs != nil {
		c.refs[taskID] = struct{}{}
		return nil
	}
	if err := c.checkUpstream(path, taskID); err != nil {
		return err
	}
	if c.engine && len(rest) > 0 {
		sub, subLiteral := rest[0].(*parse.StringNode)
		if !subLiteral {
			return c.dynamicIndexRefusal(path)
		}
		if sub.Text != "output" {
			return c.refuseAt(path, fmt.Errorf("task %q: %s: .tasks.%s.%s is not a value — an "+
				"engine-rendered template reads .tasks.<id>.output only, and %s",
				c.taskID, path, taskID, sub.Text, c.engineWhat))
		}
	}
	return nil
}

// dynamicIndexRefusal is rule 28's sentence for a .tasks index whose task
// argument is not a string literal. It is returned while COLLECTING too:
// ReferencedUpstreamIn is authoritative, so a read it cannot name is an
// error, never a skip.
func (c *templateChecker) dynamicIndexRefusal(path string) error {
	sentence := "an engine-rendered template names every task it reads literally, so the " +
		"engine reads exactly those values"
	err := fmt.Errorf("task %q: %s: %s", c.taskID, path, sentence)
	if c.taskID == "" {
		err = fmt.Errorf("%s: %s", path, sentence)
	}
	if c.refs != nil {
		return err
	}
	return c.refuseAt(path, err)
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
