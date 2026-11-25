package dsl

import (
	"fmt"
	"strconv"
	"strings"
	"text/template/parse"
	"time"

	"go.yaml.in/yaml/v4"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
)

// The run-level concurrency limit (docs/DSL.md "Concurrency limits"; E88
// §3): a key's slots, shared by every declaration in a namespace that renders
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

// concurrencyStrategies are on_limit's closed set, by name.
var concurrencyStrategies = map[string]orkstackv1.ConcurrencyStrategy{
	"queue":         orkstackv1.ConcurrencyStrategy_CONCURRENCY_STRATEGY_QUEUE,
	"refuse":        orkstackv1.ConcurrencyStrategy_CONCURRENCY_STRATEGY_REFUSE,
	"skip":          orkstackv1.ConcurrencyStrategy_CONCURRENCY_STRATEGY_SKIP,
	"cancel_oldest": orkstackv1.ConcurrencyStrategy_CONCURRENCY_STRATEGY_CANCEL_OLDEST,
	"replace":       orkstackv1.ConcurrencyStrategy_CONCURRENCY_STRATEGY_REPLACE,
}

// concurrencyStrategyName is a strategy's DSL name, "" for none.
func concurrencyStrategyName(s orkstackv1.ConcurrencyStrategy) string {
	for name, v := range concurrencyStrategies {
		if v == s {
			return name
		}
	}
	return ""
}

// compileConcurrency reads the document's `concurrency:` block: absent is
// no limit.
func compileConcurrency(doc *concurrencyDocument) (*orkstackv1.ConcurrencySpec, error) {
	if doc == nil {
		return nil, nil
	}
	spec := &orkstackv1.ConcurrencySpec{OnLimit: orkstackv1.ConcurrencyStrategy_CONCURRENCY_STRATEGY_QUEUE}
	if doc.Limit != nil {
		spec.Limit = *doc.Limit
	}
	if doc.Key != nil {
		if *doc.Key == "" {
			return nil, validationError("concurrency", fmt.Errorf("concurrency.key is empty; omit it for the Orkflow's own key"))
		}
		spec.Key = *doc.Key
	}
	if doc.OnLimit != "" {
		strategy, ok := concurrencyStrategies[doc.OnLimit]
		if !ok {
			return nil, validationError("concurrency", fmt.Errorf("concurrency.on_limit: %q is not one of queue, refuse, skip, cancel_oldest, replace", doc.OnLimit))
		}
		spec.OnLimit = strategy
	}
	if doc.MaxQueued != nil {
		spec.MaxQueued = *doc.MaxQueued
	}
	seconds, err := compileQueueTimeout(&doc.QueueTimeout)
	if err != nil {
		return nil, err
	}
	spec.QueueTimeoutSeconds = seconds
	if err := ValidateConcurrency(spec); err != nil {
		return nil, err
	}
	return spec, nil
}

// compileQueueTimeout reads `queue_timeout:`: absent is 0, the deployment's
// default; otherwise a Go duration of whole seconds above zero, a string
// scalar only.
func compileQueueTimeout(node *yaml.Node) (int64, error) {
	if node.Kind == 0 {
		return 0, nil
	}
	if err := structural(node, "concurrency.queue_timeout"); err != nil {
		return 0, authoredError(err)
	}
	fail := func(err error) (int64, error) {
		return 0, validationError("concurrency", fmt.Errorf("line %d: %w", node.Line, err))
	}
	if node.Kind != yaml.ScalarNode || node.Tag != "!!str" {
		return fail(fmt.Errorf("concurrency.queue_timeout: %s is not a duration such as 30m or 2h", DescribeNode(node)))
	}
	d, err := time.ParseDuration(node.Value)
	if err != nil {
		return fail(fmt.Errorf("concurrency.queue_timeout: %q is not a duration such as 30m or 2h", node.Value))
	}
	if d%time.Second != 0 {
		return fail(fmt.Errorf("concurrency.queue_timeout: %s must be a whole number of seconds", node.Value))
	}
	if d <= 0 {
		return fail(fmt.Errorf("concurrency.queue_timeout: %s must be above zero; omit it for the deployment's default", node.Value))
	}
	return int64(d / time.Second), nil
}

// ValidateConcurrency checks a compiled limit against the bounds that hold
// whatever the deployment: create and plan run it again over a stored
// definition, as validate does at compile. A nil spec is no limit.
func ValidateConcurrency(spec *orkstackv1.ConcurrencySpec) error {
	if spec == nil {
		return nil
	}
	fail := func(format string, args ...any) error {
		return validationError("concurrency", fmt.Errorf(format, args...))
	}
	switch {
	case spec.GetLimit() < 1:
		return fail("concurrency.limit is %d; it must be at least 1", spec.GetLimit())
	case spec.GetQueueTimeoutSeconds() < 0:
		return fail("concurrency.queue_timeout is %ds; it must be above zero", spec.GetQueueTimeoutSeconds())
	case spec.GetMaxQueued() < 0:
		return fail("concurrency.max_queued is %d; it must be 0 (no bound of its own) or above", spec.GetMaxQueued())
	}
	switch spec.GetOnLimit() {
	case orkstackv1.ConcurrencyStrategy_CONCURRENCY_STRATEGY_QUEUE,
		orkstackv1.ConcurrencyStrategy_CONCURRENCY_STRATEGY_REFUSE,
		orkstackv1.ConcurrencyStrategy_CONCURRENCY_STRATEGY_SKIP:
	case orkstackv1.ConcurrencyStrategy_CONCURRENCY_STRATEGY_UNSPECIFIED:
		return fail("concurrency.on_limit is unset")
	default:
		// The closed set is parsed; a strategy whose deciding gate has not
		// landed is refused, never read as queue (E88 P3, D6).
		return fail("concurrency.on_limit: %s is not available yet; this release supports queue, refuse and skip", concurrencyStrategyName(spec.GetOnLimit()))
	}
	key := spec.GetKey()
	if key == "" {
		return nil
	}
	if len(key) > ConcurrencyKeyTemplateMaxBytes {
		return fail("concurrency.key is %d bytes; a key template is at most %d bytes", len(key), ConcurrencyKeyTemplateMaxBytes)
	}
	if !strings.Contains(key, "{{") {
		// A literal key is its own rendering, held to §3.3 now.
		if err := CheckConcurrencyKey(key); err != nil {
			return validationError("concurrency", fmt.Errorf("concurrency.key: %w", err))
		}
		return nil
	}
	if err := checkConcurrencyKeyTemplate(key); err != nil {
		return validationError("concurrency", fmt.Errorf("concurrency.key: %w", err))
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

// CheckConcurrencyKey is §3.3's rule for a rendered authored key: 1 to 128
// bytes of [A-Za-z0-9._:/@+=-], starting with a letter or digit, and not
// starting with ork:. A refusal is CONCURRENCY_KEY_INVALID, naming up to 64
// bytes of the key and the offset of its first bad byte.
func CheckConcurrencyKey(key string) error {
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
