package dsl

import (
	"fmt"
	"slices"
	"strconv"
	"strings"
	"text/template/parse"
	"time"

	"go.yaml.in/yaml/v4"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
)

// Concurrency limits, run-level and task-level (docs/DSL.md "Concurrency
// limits"; E88 §3): a key's slots, shared by every declaration in a namespace that renders
// it. These bounds hold whatever the deployment; the deployment's own
// (engine.concurrency.max_entries, min_queue_timeout, max_queue_timeout,
// max_limits_per_orkflow, max_run_slot_entries) are held at validate, create
// and plan by the API, which has them.
const (
	// ConcurrencyKeyTemplateMaxBytes is the longest key template a limit may
	// declare.
	ConcurrencyKeyTemplateMaxBytes = 512
	// ConcurrencyKeyMaxBytes is the longest rendered authored key.
	ConcurrencyKeyMaxBytes = 128
	// concurrencyReservedPrefix starts every default key, and no authored
	// one.
	concurrencyReservedPrefix = "ork:"
	// ConcurrencyKeyInvalidCode refuses a key that breaks §3.3's rule, or a
	// template that does not render: at validate for a literal key, at
	// submit for a rendered one.
	ConcurrencyKeyInvalidCode = "CONCURRENCY_KEY_INVALID"
)

// concurrencyDocument is a `concurrency:` block as written. The strict
// decode refuses a key it does not name.
type concurrencyDocument struct {
	Key   *string `yaml:"key"`
	Limit *int32  `yaml:"limit"`
	// OnLimit is the strategy's name; absent is queue.
	OnLimit string `yaml:"on_limit"`
	// QueueTimeout is a Go duration of whole seconds. A yaml.Node, as the
	// run's timeout: is, so a bare number is refused by name.
	QueueTimeout yaml.Node `yaml:"queue_timeout"`
	MaxQueued    *int32    `yaml:"max_queued"`
}

// taskConcurrencyDocument is a task's `concurrency:` block as written: a
// run's without max_queued, which a submit checks and a task never meets.
type taskConcurrencyDocument struct {
	Key          *string   `yaml:"key"`
	Limit        *int32    `yaml:"limit"`
	OnLimit      string    `yaml:"on_limit"`
	QueueTimeout yaml.Node `yaml:"queue_timeout"`
}

func (d *taskConcurrencyDocument) limit() *concurrencyDocument {
	if d == nil {
		return nil
	}
	return &concurrencyDocument{Key: d.Key, Limit: d.Limit, OnLimit: d.OnLimit, QueueTimeout: d.QueueTimeout}
}

// concurrencyStrategies are on_limit's closed set, by name.
var concurrencyStrategies = map[string]orkstackv1.ConcurrencyStrategy{
	"queue":         orkstackv1.ConcurrencyStrategy_CONCURRENCY_STRATEGY_QUEUE,
	"refuse":        orkstackv1.ConcurrencyStrategy_CONCURRENCY_STRATEGY_REFUSE,
	"skip":          orkstackv1.ConcurrencyStrategy_CONCURRENCY_STRATEGY_SKIP,
	"cancel_oldest": orkstackv1.ConcurrencyStrategy_CONCURRENCY_STRATEGY_CANCEL_OLDEST,
	"replace":       orkstackv1.ConcurrencyStrategy_CONCURRENCY_STRATEGY_REPLACE,
}

// concurrencyStrategyWords are the strategies this release decides, as on_limit
// writes them; ValidateConcurrency refuses any other.
var concurrencyStrategyWords = []string{"queue", "refuse", "skip"}

// concurrencyStrategyName is a strategy's DSL name, "" for none.
func concurrencyStrategyName(s orkstackv1.ConcurrencyStrategy) string {
	for name, v := range concurrencyStrategies {
		if v == s {
			return name
		}
	}
	return ""
}

// limitSite is where a limit is declared, as its refusals name it: the field,
// and what comes before "concurrency." in the sentence.
type limitSite struct{ field, prefix string }

// runLimitSite is the Orkflow's own limit.
var runLimitSite = limitSite{field: "concurrency"}

// taskLimitSite is one task's limit.
func taskLimitSite(taskIndex int, taskID string) limitSite {
	return limitSite{field: fmt.Sprintf("tasks[%d].concurrency", taskIndex), prefix: fmt.Sprintf("task %q: ", taskID)}
}

func (s limitSite) refuse(format string, args ...any) error {
	return validationError(s.field, fmt.Errorf(s.prefix+format, args...))
}

// compileConcurrency reads a `concurrency:` block: absent is no limit.
func compileConcurrency(doc *concurrencyDocument, at limitSite) (*orkstackv1.ConcurrencySpec, error) {
	if doc == nil {
		return nil, nil
	}
	spec := &orkstackv1.ConcurrencySpec{OnLimit: orkstackv1.ConcurrencyStrategy_CONCURRENCY_STRATEGY_QUEUE}
	if doc.Limit != nil {
		spec.Limit = *doc.Limit
	}
	if doc.Key != nil {
		if *doc.Key == "" {
			return nil, at.refuse("concurrency.key is empty; omit it for its own key")
		}
		spec.Key = *doc.Key
	}
	if doc.OnLimit != "" {
		strategy, ok := concurrencyStrategies[doc.OnLimit]
		if !ok {
			return nil, at.refuse("concurrency.on_limit: %q is not one of queue, refuse, skip, cancel_oldest, replace", doc.OnLimit)
		}
		spec.OnLimit = strategy
	}
	if doc.MaxQueued != nil {
		spec.MaxQueued = *doc.MaxQueued
	}
	seconds, err := compileQueueTimeout(&doc.QueueTimeout, at)
	if err != nil {
		return nil, err
	}
	spec.QueueTimeoutSeconds = seconds
	if err := validateLimit(spec, at); err != nil {
		return nil, err
	}
	return spec, nil
}

// compileQueueTimeout reads `queue_timeout:`: absent is 0, the deployment's
// default; otherwise a Go duration of whole seconds above zero, a string
// scalar only.
func compileQueueTimeout(node *yaml.Node, at limitSite) (int64, error) {
	if node.Kind == 0 {
		return 0, nil
	}
	if err := structural(node, at.field+".queue_timeout"); err != nil {
		return 0, authoredError(err)
	}
	fail := func(format string, args ...any) (int64, error) {
		return 0, validationError(at.field, fmt.Errorf("line %d: %s%w", node.Line, at.prefix, fmt.Errorf(format, args...)))
	}
	if node.Kind != yaml.ScalarNode || node.Tag != "!!str" {
		return fail("concurrency.queue_timeout: %s is not a duration such as 30m or 2h", DescribeNode(node))
	}
	d, err := time.ParseDuration(node.Value)
	if err != nil {
		return fail("concurrency.queue_timeout: %q is not a duration such as 30m or 2h", node.Value)
	}
	if d%time.Second != 0 {
		return fail("concurrency.queue_timeout: %s must be a whole number of seconds", node.Value)
	}
	if d <= 0 {
		return fail("concurrency.queue_timeout: %s must be above zero; omit it for the deployment's default", node.Value)
	}
	return int64(d / time.Second), nil
}

// ValidateConcurrency checks a compiled run-level limit against the bounds
// that hold whatever the deployment. A nil spec is no limit.
func ValidateConcurrency(spec *orkstackv1.ConcurrencySpec) error {
	return validateLimit(spec, runLimitSite)
}

// validateTaskConcurrency checks a task's limit against the same bounds and
// its own: only a task that runs its own container takes a slot, it always
// waits for one, and never for the key its own run holds.
func validateTaskConcurrency(taskIndex int, wf *orkstackv1.Orkflow, task *orkstackv1.Task) error {
	spec := task.GetConcurrency()
	if spec == nil {
		return nil
	}
	at := taskLimitSite(taskIndex, task.GetId())
	if err := validateLimit(spec, at); err != nil {
		return err
	}
	run := wf.GetConcurrency()
	switch {
	case spec.GetOnLimit() != orkstackv1.ConcurrencyStrategy_CONCURRENCY_STRATEGY_QUEUE:
		return at.refuse("concurrency.on_limit: %s is a run's; a task always waits in line (queue)", concurrencyStrategyName(spec.GetOnLimit()))
	case spec.GetMaxQueued() != 0:
		return at.refuse("concurrency.max_queued is a run's, checked when it is submitted; a task waits whatever its line holds")
	case task.GetEach() != nil:
		return at.refuse("concurrency: is not for a fan-out; bound its items with each.max_parallel")
	case task.GetPin() != "":
		return at.refuse("concurrency: is not for a pin's member: it would hold its pin's Orker while it waits")
	case IsFailureHandler(task):
		return at.refuse("concurrency: is not for a failure handler, which starts in a failing run and never waits for a slot")
	case run != nil && run.GetKey() != "" && run.GetKey() == spec.GetKey():
		return at.refuse("concurrency.key %s is the run's own: the task would wait for a slot its own run holds", strconv.Quote(spec.GetKey()))
	}
	return nil
}

// validateLimit holds one limit to the bounds every one of them has.
func validateLimit(spec *orkstackv1.ConcurrencySpec, at limitSite) error {
	if spec == nil {
		return nil
	}
	switch {
	case spec.GetLimit() < 1:
		return at.refuse("concurrency.limit is %d; it must be at least 1", spec.GetLimit())
	case spec.GetQueueTimeoutSeconds() < 0:
		return at.refuse("concurrency.queue_timeout is %ds; it must be above zero", spec.GetQueueTimeoutSeconds())
	case spec.GetMaxQueued() < 0:
		return at.refuse("concurrency.max_queued is %d; it must be 0 (no bound of its own) or above", spec.GetMaxQueued())
	}
	switch name := concurrencyStrategyName(spec.GetOnLimit()); {
	case slices.Contains(concurrencyStrategyWords, name):
	case spec.GetOnLimit() == orkstackv1.ConcurrencyStrategy_CONCURRENCY_STRATEGY_UNSPECIFIED:
		return at.refuse("concurrency.on_limit is unset")
	default:
		// The closed set is parsed; a strategy whose deciding gate has not
		// landed is refused, never read as queue (E88 P3, D6).
		return at.refuse("concurrency.on_limit: %s is not available yet; this release supports queue, refuse and skip", name)
	}
	key := spec.GetKey()
	if key == "" {
		return nil
	}
	if len(key) > ConcurrencyKeyTemplateMaxBytes {
		return at.refuse("concurrency.key is %d bytes; a key template is at most %d bytes", len(key), ConcurrencyKeyTemplateMaxBytes)
	}
	check := func(k string) error { return CheckConcurrencyKey(k, KeyAuthored) } // a literal key is its own rendering
	if strings.Contains(key, "{{") {
		check = checkConcurrencyKeyTemplate
	}
	if err := check(key); err != nil {
		return at.refuse("concurrency.key: %w", err)
	}
	return nil
}

// concurrencyKeyRoots are the only run metadata a key template may read
// (§3.2), beside the run's inputs: what is the same on every retry of one
// submit. The run id, timestamps, origin.reference and origin.principal are
// excluded.
var concurrencyKeyRoots = []string{"run.orkflow", "run.version", "run.origin.kind", "run.origin.id"}

// checkConcurrencyKeyTemplate parses a key template and refuses anything
// but text and plain field reads of its roots: no function, pipeline,
// variable, condition or loop (§3.2; no expression language, AGENTS.md
// §13).
func checkConcurrencyKeyTemplate(text string) error {
	// Functions are not checked at parse, so a call is refused below by
	// what it is rather than as an unknown name.
	tree := parse.New("concurrency.key")
	tree.Mode = parse.SkipFuncCheck
	trees := map[string]*parse.Tree{}
	if _, err := tree.Parse(text, "{{", "}}", trees); err != nil {
		return fmt.Errorf("template parse error: %w", err)
	}
	if len(trees) != 1 || tree.Root == nil {
		return fmt.Errorf("a key template reads only a field, such as {{ .inputs.env }}")
	}
	for _, node := range tree.Root.Nodes {
		switch n := node.(type) {
		case *parse.TextNode:
		case *parse.ActionNode:
			if err := checkConcurrencyKeyAction(n); err != nil {
				return err
			}
		default:
			return fmt.Errorf("%s: a key template reads only a field, such as {{ .inputs.env }}", node.String())
		}
	}
	return nil
}

func checkConcurrencyKeyAction(n *parse.ActionNode) error {
	pipe := n.Pipe
	if pipe == nil || len(pipe.Decl) > 0 || len(pipe.Cmds) != 1 || len(pipe.Cmds[0].Args) != 1 {
		return fmt.Errorf("%s: a key template reads only a field, such as {{ .inputs.env }}", n.String())
	}
	field, ok := pipe.Cmds[0].Args[0].(*parse.FieldNode)
	if !ok {
		return fmt.Errorf("%s: a key template reads only a field, such as {{ .inputs.env }}", n.String())
	}
	path := strings.Join(field.Ident, ".")
	if len(field.Ident) >= 2 && field.Ident[0] == "inputs" {
		return nil
	}
	for _, root := range concurrencyKeyRoots {
		if path == root {
			return nil
		}
	}
	return fmt.Errorf("%s: a key template may read .inputs.<name>, .run.orkflow, .run.version, .run.origin.kind or .run.origin.id; .%s is not one",
		n.String(), path)
}

// ConcurrencyKeyMaker is who made a key, which decides the rule it is held to.
type ConcurrencyKeyMaker int

const (
	// KeyAuthored is a key an author wrote, or that a template an author wrote
	// rendered to: it may not be one of OrkStack's own.
	KeyAuthored ConcurrencyKeyMaker = iota + 1
	// KeyPlanned is a key OrkStack planned, stored, or is asked to read back or
	// act on: an authored one, or its own, which an Orkflow or a task that
	// declares no key has.
	KeyPlanned
)

// CheckConcurrencyKey is the one rule for a concurrency key, refused
// CONCURRENCY_KEY_INVALID. Every caller says who made the key
// (TestConcurrencyKeys_EveryCallerSaysWhoMadeTheKey lists them).
func CheckConcurrencyKey(key string, madeBy ConcurrencyKeyMaker) error {
	switch madeBy {
	case KeyAuthored:
		return checkAuthoredKey(key)
	case KeyPlanned:
		return checkPlannedKey(key)
	}
	return fmt.Errorf("%s: the key's maker is not known", ConcurrencyKeyInvalidCode)
}

// checkAuthoredKey is §3.3's rule for a rendered authored key: 1 to 128
// bytes of [A-Za-z0-9._:/@+=-], starting with a letter or digit, and not
// starting with ork:. A refusal is CONCURRENCY_KEY_INVALID, naming up to 64
// bytes of the key and the offset of its first bad byte.
func checkAuthoredKey(key string) error {
	shown := key
	if len(shown) > 64 {
		shown = shown[:64] + "…"
	}
	invalid := func(format string, args ...any) error {
		return fmt.Errorf("%s: the key %s %s", ConcurrencyKeyInvalidCode, strconv.Quote(shown), fmt.Sprintf(format, args...))
	}
	switch {
	case key == "":
		return invalid("is empty")
	case len(key) > ConcurrencyKeyMaxBytes:
		return invalid("is %d bytes; a key is at most %d", len(key), ConcurrencyKeyMaxBytes)
	case strings.HasPrefix(key, concurrencyReservedPrefix):
		return invalid("starts with %s, which only OrkStack's own keys use", concurrencyReservedPrefix)
	}
	for i := 0; i < len(key); i++ {
		c := key[i]
		alnum := c >= 'A' && c <= 'Z' || c >= 'a' && c <= 'z' || c >= '0' && c <= '9'
		if i == 0 && !alnum {
			return invalid("has %q at offset 0; a key starts with a letter or a digit", c)
		}
		if !alnum && !strings.ContainsRune("._:/@+=-", rune(c)) {
			return invalid("has %q at offset %d; a key holds only letters, digits and . _ : / @ + = -", c, i)
		}
	}
	return nil
}

// checkPlannedKey is the rule for a key that is read back or acted on rather
// than authored: an authored key's, or one of OrkStack's own, which
// an Orkflow or a task that declares no key has (ork:orkflow:<namespace>/<name>,
// ork:task:<namespace>/<orkflow>:<task>), each part a name. Any other is
// refused CONCURRENCY_KEY_INVALID.
func checkPlannedKey(key string) error {
	own, reserved := strings.CutPrefix(key, concurrencyReservedPrefix)
	if !reserved {
		return checkAuthoredKey(key)
	}
	var names []string
	if rest, ok := strings.CutPrefix(own, "orkflow:"); ok {
		names = strings.SplitN(rest, "/", 2)
	} else if rest, ok := strings.CutPrefix(own, "task:"); ok {
		if owner, rest, ok := strings.Cut(rest, "/"); ok {
			if orkflow, id, ok := strings.Cut(rest, ":"); ok {
				names = []string{owner, orkflow, id}
			}
		}
	}
	if len(names) < 2 || slices.ContainsFunc(names, func(n string) bool { return checkSlug("name", n) != nil }) {
		shown := key
		if len(shown) > 64 {
			shown = shown[:64] + "…"
		}
		return fmt.Errorf("%s: the key %s starts with %s and is not one OrkStack makes", ConcurrencyKeyInvalidCode, strconv.Quote(shown), concurrencyReservedPrefix)
	}
	return nil
}
