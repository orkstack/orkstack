package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"

	"connectrpc.com/connect"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/dsl"
	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// Composed Orkflows (spec composed-orkflows §4, §5): the apply-time
// resolution of `uses: orkflow:<name>@<version>` (rules 31 and 32), and the
// submit-time planning of every child run a submission can start.

// composeMaxDepthOf defaults ORK_COMPOSE_MAX_DEPTH: zero or anything above the
// built-in ceiling means the ceiling.
func composeMaxDepthOf(configured int32) int {
	if configured <= 0 || configured > engine.MaxComposeDepth {
		return engine.MaxComposeDepth
	}
	return int(configured)
}

// composeNode is one child in the tree a definition composes.
type composeNode struct {
	TaskID   string                 // the composing task, in its parent
	Ref      *orkstackv1.OrkflowRef // as the parent pinned it
	Orkflow  *orkstackv1.Orkflow    // the stored version it resolved to
	Path     []string               // "name@version" from the root, this node last
	Children []*composeNode         // declaration order
}

// composeProblem is one node that did not resolve: which root task it hangs
// off, its path, and the sentence §4.3 gives its kind.
type composeProblem struct {
	TaskIndex int      // the ROOT composing task's index, where apply anchors
	Path      []string // "name@version" from the root
	Kind      string   // "missing" | "digest" | "cycle" | "depth"
	Message   string
}

// composeKey is a reference's "name@version".
func composeKey(name, version string) string { return name + "@" + version }

// pathNames is a path of "name@version" entries as names only.
func pathNames(path []string) []string {
	out := make([]string, 0, len(path))
	for _, entry := range path {
		name, _, _ := strings.Cut(entry, "@")
		out = append(out, name)
	}
	return out
}

// checkComposeNode is the four checks every node of a tree is held to, in this
// order — depth (depth+1 > maxDepth), cycle (name@version already on path),
// existence (stored is nil: the store answered NotFound), pinned digest
// (stored.definition_digest != ref.definition_digest, skipped only for a root
// ref being stamped at apply). Pure. loadComposeTree (apply) and
// planChildren (submit) both call it, so the two doors cannot disagree about
// what a tree may be. path is the composing definition's own path.
func checkComposeNode(ref *orkstackv1.OrkflowRef, stored *orkstackv1.Orkflow, path []string, depth, maxDepth int, stamping bool) *composeProblem {
	key := composeKey(ref.GetName(), ref.GetVersion())
	full := append(append([]string(nil), path...), key)
	if depth+1 > maxDepth {
		return &composeProblem{Path: full, Kind: "depth", Message: fmt.Sprintf(
			"composition is %d levels deep (%s); this deployment allows %d (ORK_COMPOSE_MAX_DEPTH)",
			depth+1, strings.Join(pathNames(full), " → "), maxDepth)}
	}
	for _, seen := range path {
		if seen == key {
			return &composeProblem{Path: full, Kind: "cycle", Message: fmt.Sprintf(
				"composition cycle: %s", strings.Join(full, " → "))}
		}
	}
	if stored == nil {
		return &composeProblem{Path: full, Kind: "missing", Message: fmt.Sprintf(
			"orkflow %q has no version %q", ref.GetName(), ref.GetVersion())}
	}
	if !stamping && stored.GetDefinitionDigest() != ref.GetDefinitionDigest() {
		parent := "this definition"
		if len(path) > 0 {
			parent = path[len(path)-1]
		}
		return &composeProblem{Path: full, Kind: "digest", Message: fmt.Sprintf(
			"%s pins %s as %s, but the stored %s is %s — it was deleted and re-applied with other "+
				"content; apply %s as a new version to re-pin it",
			parent, key, ref.GetDefinitionDigest(), key, stored.GetDefinitionDigest(),
			pathNames([]string{parent})[0])}
	}
	return nil
}

// composeReader memoizes one call's reads: one read per distinct
// name@version.
type composeReader struct {
	s     *Service
	ns    string
	cache map[string]*orkstackv1.Orkflow
	seen  map[string]bool
}

// get reads one version; nil, nil is NotFound.
func (r *composeReader) get(ctx context.Context, name, version string) (*orkstackv1.Orkflow, error) {
	key := composeKey(name, version)
	if r.seen[key] {
		return r.cache[key], nil
	}
	stored, err := r.s.orkflows.Get(ctx, r.ns, name, version)
	if err != nil && !errors.Is(err, artifacts.ErrNotFound) {
		return nil, err
	}
	if err != nil {
		stored = nil
	}
	r.seen[key] = true
	r.cache[key] = stored
	return stored, nil
}

// loadComposeTree reads, for every composed task of wf in declaration order,
// the stored version it names — one read per distinct name@version per call
// (memoized) — and recurses into it. Every node is held to checkComposeNode;
// a node that fails it is reported, not descended into, and the walk
// continues with its siblings, so apply names every problem at once. err is a
// store outage only. With stampRoot, a root-level ref with an empty digest is
// stamped from the stored document.
func (s *Service) loadComposeTree(ctx context.Context, ns string, wf *orkstackv1.Orkflow, stampRoot bool) ([]*composeNode, []composeProblem, error) {
	reader := &composeReader{s: s, ns: ns, cache: map[string]*orkstackv1.Orkflow{}, seen: map[string]bool{}}
	root := []string{composeKey(wf.GetName(), wf.GetVersion())}
	var problems []composeProblem
	var walk func(def *orkstackv1.Orkflow, path []string, depth, rootIndex int, rootTask string) ([]*composeNode, error)
	walk = func(def *orkstackv1.Orkflow, path []string, depth, rootIndex int, rootTask string) ([]*composeNode, error) {
		var nodes []*composeNode
		for i, t := range def.GetTasks() {
			ref := t.GetOrkflow()
			if ref == nil {
				continue
			}
			index, task := rootIndex, rootTask
			if depth == 0 {
				index, task = i, t.GetId()
			}
			stored, err := reader.get(ctx, ref.GetName(), ref.GetVersion())
			if err != nil {
				return nil, err
			}
			stamping := stampRoot && depth == 0 && ref.GetDefinitionDigest() == ""
			if problem := checkComposeNode(ref, stored, path, depth, s.composeMaxDepth, stamping); problem != nil {
				problem.TaskIndex = index
				if problem.Kind == "missing" {
					problem.Message = s.missingChildSentence(ctx, ns, ref)
				}
				if depth > 0 {
					problem.Message = fmt.Sprintf("%s: %s",
						strings.Join(append([]string{task}, problem.Path[1:]...), " → "), problem.Message)
				}
				problems = append(problems, *problem)
				continue
			}
			if stamping {
				ref.DefinitionDigest = stored.GetDefinitionDigest()
			}
			node := &composeNode{
				TaskID:  t.GetId(),
				Ref:     ref,
				Orkflow: stored,
				Path:    append(append([]string(nil), path...), composeKey(ref.GetName(), ref.GetVersion())),
			}
			children, err := walk(stored, node.Path, depth+1, index, task)
			if err != nil {
				return nil, err
			}
			node.Children = children
			nodes = append(nodes, node)
		}
		return nodes, nil
	}
	nodes, err := walk(wf, root, 0, 0, "")
	if err != nil {
		return nil, nil, err
	}
	return nodes, problems, nil
}

// missingChildSentence is rule 31 item 1: the versions that exist, newest
// first, with the current one marked — or that no such orkflow exists. A
// failed catalog read keeps the bare sentence.
func (s *Service) missingChildSentence(ctx context.Context, ns string, ref *orkstackv1.OrkflowRef) string {
	versions, err := s.orkflows.Versions(ctx, ns, ref.GetName())
	if err != nil {
		return fmt.Sprintf("orkflow %q has no version %q in namespace %q", ref.GetName(), ref.GetVersion(), ns)
	}
	if len(versions.All) == 0 {
		return fmt.Sprintf("no orkflow %q exists in namespace %q", ref.GetName(), ns)
	}
	return fmt.Sprintf("orkflow %q has no version %q in namespace %q (versions: %s)",
		ref.GetName(), ref.GetVersion(), ns, versionList(versions))
}

// versionList renders "2.1.0 (current), 2.0.3, 1.9.0": newest first, the
// version an empty one resolves to marked.
func versionList(versions artifacts.OrkflowVersions) string {
	resolved := versions.Resolved()
	parts := make([]string, 0, len(versions.All))
	for _, wf := range versions.All {
		entry := wf.GetVersion()
		if entry == resolved {
			entry += " (current)"
		}
		parts = append(parts, entry)
	}
	return strings.Join(parts, ", ")
}

// composeVersionRefusal is the compiler's ComposeVersionRequiredError with
// the stored versions appended (spec §4.5).
type composeVersionRefusal struct {
	missing *dsl.ComposeVersionRequiredError
	suffix  string
}

func (e *composeVersionRefusal) Error() string { return e.missing.Error() + e.suffix }
func (e *composeVersionRefusal) Unwrap() error { return e.missing }

// enrichVersionRequired answers a compile error that is
// *dsl.ComposeVersionRequiredError by naming the versions the namespace
// holds. Any error on that best-effort read leaves err as it was.
func (s *Service) enrichVersionRequired(ctx context.Context, requestedNamespace string, err error) error {
	var missing *dsl.ComposeVersionRequiredError
	if !errors.As(err, &missing) {
		return err
	}
	selected, nsErr := applyNamespace(requestedNamespace, missing.Namespace)
	if nsErr != nil {
		return err
	}
	ns, nsErr := s.namespace(ctx, selected)
	if nsErr != nil {
		return err
	}
	versions, vErr := s.orkflows.Versions(ctx, ns.GetName(), missing.Name)
	if vErr != nil {
		return err
	}
	suffix := fmt.Sprintf("; no orkflow %q exists in namespace %q", missing.Name, ns.GetName())
	if len(versions.All) > 0 {
		suffix = fmt.Sprintf("; versions in namespace %q: %s", ns.GetName(), versionList(versions))
	}
	return &dsl.ValidationError{
		Field: fmt.Sprintf("tasks[%d].uses", missing.TaskIndex),
		Err:   &composeVersionRefusal{missing: missing, suffix: suffix},
	}
}

func composeIssue(field, message string, severity orkstackv1.ValidationSeverity) *orkstackv1.ValidationIssue {
	return &orkstackv1.ValidationIssue{Field: field, Message: message, Severity: severity}
}

// resolveComposition resolves every composed task of wf in ns, stamps each
// Task.orkflow.definition_digest, loads the tree the document composes, and
// checks rules 31 and 32. It returns the issues in document order; err is a
// dependency outage only (the orkflow store unreachable → Unavailable). A
// document with no composed task returns (nil, nil) without touching the
// store.
func (s *Service) resolveComposition(ctx context.Context, ns *orkstackv1.Namespace, wf *orkstackv1.Orkflow) ([]*orkstackv1.ValidationIssue, error) {
	composes := false
	for _, t := range wf.GetTasks() {
		if t.GetOrkflow() != nil {
			composes = true
			break
		}
	}
	if !composes {
		return nil, nil
	}
	nodes, problems, err := s.loadComposeTree(ctx, ns.GetName(), wf, true)
	if err != nil {
		return nil, orkflowStoreError(err, "read the orkflows %s@%s composes in namespace %q",
			wf.GetName(), wf.GetVersion(), ns.GetName())
	}
	const (
		errorSeverity   = orkstackv1.ValidationSeverity_VALIDATION_SEVERITY_ERROR
		warningSeverity = orkstackv1.ValidationSeverity_VALIDATION_SEVERITY_WARNING
	)
	usesField := func(i int) string { return fmt.Sprintf("tasks[%d].uses", i) }
	taskIndex := map[string]int{}
	for i, t := range wf.GetTasks() {
		taskIndex[t.GetId()] = i
	}

	// Rule 31.
	var rule31 []*orkstackv1.ValidationIssue
	for _, p := range problems {
		rule31 = append(rule31, composeIssue(usesField(p.TaskIndex), p.Message, errorSeverity))
	}
	type sized struct {
		label string
		bytes int
	}
	var sizes []sized
	total := 0
	var visit func(rootIndex int, node *composeNode, chain []string)
	visit = func(rootIndex int, node *composeNode, chain []string) {
		label := dsl.ComposedLabel(node.Ref)
		prefix := fmt.Sprintf("child orkflow %s (task %s): ", label, node.TaskID)
		// Item 5: the namespace allows every descendant's pools and secrets.
		if err := s.pools.CheckAllowedPools(ns, node.Orkflow); err != nil {
			rule31 = append(rule31, composeIssue(usesField(rootIndex), prefix+err.Error(), errorSeverity))
		}
		if err := engine.CheckAllowedSecrets(ns, node.Orkflow); err != nil {
			rule31 = append(rule31, composeIssue(usesField(rootIndex), prefix+err.Error(), errorSeverity))
		}
		// Item 6: the definition as the plan will carry it.
		if cp, err := engine.NewChildRunPlan(node.Orkflow, engine.RunPlan{}, false, false); err == nil {
			chainHere := append(append([]string(nil), chain...), label)
			sizes = append(sizes, sized{label: strings.Join(chainHere, " → "), bytes: len(cp.Orkflow)})
			total += len(cp.Orkflow)
		}
		// The one WARNING: callback awaits are minted and disclosed to no one.
		var callbacks []string
		for _, t := range node.Orkflow.GetTasks() {
			if t.GetAwait().GetCallback() {
				callbacks = append(callbacks, t.GetId())
			}
		}
		if len(callbacks) > 0 {
			rule31 = append(rule31, composeIssue(usesField(rootIndex), fmt.Sprintf(
				"child orkflow %s (task %s) declares callback awaits (%s); as a child run its callback "+
					"tokens are minted and disclosed to no one: resolve them with orkctl run resolve "+
					"<child-run> --task %s or on the child run's page",
				label, node.TaskID, strings.Join(callbacks, ", "), callbacks[0]), warningSeverity))
		}
		for _, child := range node.Children {
			visit(rootIndex, child, append(append([]string(nil), chain...), label))
		}
	}
	for _, node := range nodes {
		visit(taskIndex[node.TaskID], node, []string{node.TaskID})
	}
	if total > engine.MaxPlanBytes && len(nodes) > 0 {
		parts := make([]string, 0, len(sizes))
		for _, sz := range sizes {
			parts = append(parts, fmt.Sprintf("%s %d KiB", sz.label, kib(sz.bytes)))
		}
		rule31 = append(rule31, composeIssue(usesField(taskIndex[nodes[0].TaskID]), fmt.Sprintf(
			"the Orkflows this document composes total %d KiB (%s); a run carries every one of them in "+
				"its plan, which may be at most %d KiB — compose fewer or smaller Orkflows",
			kib(total), strings.Join(parts, ", "), engine.MaxPlanBytes/1024), errorSeverity))
	}

	// Rule 32: the child's interface, for the document's own composed tasks.
	var rule32 []*orkstackv1.ValidationIssue
	children := map[string]*composeNode{}
	for _, node := range nodes {
		children[node.TaskID] = node
		i := taskIndex[node.TaskID]
		rule32 = append(rule32, childInputIssues(i, wf.GetTasks()[i], node)...)
	}
	refs, err := dsl.OutputReferences(wf)
	if err == nil {
		for _, ref := range refs {
			node := children[ref.TaskID]
			if node == nil {
				continue
			}
			outputs := node.Orkflow.GetOutputs()
			label := dsl.ComposedLabel(node.Ref)
			reading := fmt.Sprintf(".tasks.%s.output.%s", ref.TaskID, ref.Key)
			if len(outputs) == 0 {
				rule32 = append(rule32, composeIssue(ref.Field, fmt.Sprintf(
					"%s: %s declares no outputs, so task %q has no values to read", reading, label, ref.TaskID), errorSeverity))
				continue
			}
			if _, ok := outputs[ref.Key]; !ok {
				rule32 = append(rule32, composeIssue(ref.Field, fmt.Sprintf(
					"%s: %s declares no output %q (outputs: %s)", reading, label, ref.Key,
					strings.Join(sortedStringKeys(outputs), ", ")), errorSeverity))
			}
		}
	}
	return append(rule31, rule32...), nil
}

func kib(n int) int { return (n + 1023) / 1024 }

func sortedStringKeys(m map[string]string) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

// childInputIssues is rule 32's with: half for one composed task: required
// inputs supplied (ERROR), undeclared keys (WARNING), and literal types
// (WARNING) — the severities activity-interface-manifests gives a with:
// against an image's interface.
func childInputIssues(taskIndex int, t *orkstackv1.Task, node *composeNode) []*orkstackv1.ValidationIssue {
	label := dsl.ComposedLabel(node.Ref)
	declared := node.Orkflow.GetInputs().GetFields()
	with := map[string]any{}
	if t.GetWith() != nil {
		with = t.GetWith().AsMap()
	}
	field := func(k string) string { return fmt.Sprintf("tasks[%d].with.%s", taskIndex, k) }
	names := make([]string, 0, len(declared))
	for name := range declared {
		names = append(names, name)
	}
	sort.Strings(names)
	var issues []*orkstackv1.ValidationIssue
	for _, name := range names {
		// dsl.ReadInput is the one reading of "required": required: true and
		// no default — the reading the interface comparison applies too.
		if !dsl.ReadInput(declared[name]).Required {
			continue
		}
		if value, ok := with[name]; !ok || value == nil {
			issues = append(issues, composeIssue(field(name), fmt.Sprintf(
				"with.%s is required by %s and is not supplied", name, label),
				orkstackv1.ValidationSeverity_VALIDATION_SEVERITY_ERROR))
		}
	}
	keys := make([]string, 0, len(with))
	for k := range with {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	inputs := "none"
	if len(names) > 0 {
		inputs = strings.Join(names, ", ")
	}
	for _, k := range keys {
		if _, ok := declared[k]; !ok {
			issues = append(issues, composeIssue(field(k), fmt.Sprintf(
				"with.%s is not an input of %s (inputs: %s); the child run will carry it, and no "+
					"template of the child can read it", k, label, inputs),
				orkstackv1.ValidationSeverity_VALIDATION_SEVERITY_WARNING))
		}
	}
	for _, k := range keys {
		decl, ok := declared[k]
		if !ok {
			continue
		}
		want := decl.GetStructValue().GetFields()["type"].GetStringValue()
		value := with[k]
		if want == "" || value == nil {
			continue
		}
		if s, isString := value.(string); isString && strings.Contains(s, "{{") {
			continue // a templated string is never typed: it renders to a string
		}
		if got := literalJSONType(value); got != want {
			issues = append(issues, composeIssue(field(k), fmt.Sprintf(
				"with.%s is %s, but %s declares input %s as %s", k, withArticleOf(got), label, k, want),
				orkstackv1.ValidationSeverity_VALIDATION_SEVERITY_WARNING))
		}
	}
	return issues
}

// literalJSONType names a decoded Struct value's JSON type in the inputs
// vocabulary.
func literalJSONType(value any) string {
	switch value.(type) {
	case string:
		return "string"
	case float64, json.Number:
		return "number"
	case bool:
		return "boolean"
	case map[string]any:
		return "object"
	case []any:
		return "array"
	default:
		return "null"
	}
}

func withArticleOf(typ string) string {
	switch typ {
	case "object", "array":
		return "an " + typ
	}
	return "a " + typ
}

// ---------------------------------------------------------------- submit

// composeRequest is what one submission's tree is planned under: one
// namespace, one pool resolver, one allow_empty_pool, one image-policy
// selection and one pair of boot answers, for every node.
type composeRequest struct {
	ns               *orkstackv1.Namespace
	resolver         *engine.PoolResolver
	allowEmpty       bool
	sel              *orkstackv1.ImagePolicySelection
	namespaceIndexed bool
	originIndexed    bool
}

// indexAnswers is the starter's boot answers when it has them (the
// TemporalStarter), and (false, false) for a starter that does not — a test
// double stamps nothing.
func indexAnswers(starter engine.RunStarter) (namespaceIndexed, originIndexed bool) {
	if answers, ok := starter.(interface{ IndexAnswers() (bool, bool) }); ok {
		return answers.IndexAnswers()
	}
	return false, false
}

// childRefusal prefixes a refusal with the child it concerns, keeping the
// Connect code.
func childRefusal(err error, ref *orkstackv1.OrkflowRef, taskID string) error {
	prefix := fmt.Sprintf("child orkflow %s (task %q): ", dsl.ComposedLabel(ref), taskID)
	var connectErr *connect.Error
	if errors.As(err, &connectErr) {
		return connect.NewError(connectErr.Code(), errors.New(prefix+connectErr.Message()))
	}
	return connect.NewError(connect.CodeFailedPrecondition, errors.New(prefix+err.Error()))
}

// planChildren builds plan.Children for every composed task of parent, in
// declaration order, and recursively for theirs. It refuses and mints
// nothing.
func (s *Service) planChildren(ctx context.Context, r composeRequest, parent *orkstackv1.Orkflow, plan *engine.RunPlan, depth int, path []string) error {
	if len(path) == 0 {
		path = []string{composeKey(parent.GetName(), parent.GetVersion())}
	}
	ns := r.ns.GetName()
	for _, t := range parent.GetTasks() {
		ref := t.GetOrkflow()
		if ref == nil {
			continue
		}
		name, version := ref.GetName(), ref.GetVersion()
		// 1. The stored child.
		child, err := s.orkflows.Get(ctx, ns, name, version)
		if err != nil && !errors.Is(err, artifacts.ErrNotFound) {
			return orkflowStoreError(err, "child orkflow %s@%s of task %q in namespace %q", name, version, t.GetId(), ns)
		}
		if err != nil {
			child = nil
		}
		// 2. The tree's shape, by the same rule apply holds it to.
		if problem := checkComposeNode(ref, child, path, depth, s.composeMaxDepth, false); problem != nil {
			message := problem.Message
			switch problem.Kind {
			case "depth":
				message = fmt.Sprintf("composition is %d levels deep (%s); this deployment now allows %d "+
					"(ORK_COMPOSE_MAX_DEPTH was lowered after %s was applied)",
					depth+1, strings.Join(pathNames(problem.Path), " → "), s.composeMaxDepth, path[0])
			case "missing":
				message = fmt.Sprintf("child orkflow %s@%s of task %q no longer exists in namespace %q; apply "+
					"it again, or submit a version of %s that composes an existing one",
					name, version, t.GetId(), ns, pathNames(path[len(path)-1:])[0])
			}
			return connect.NewError(connect.CodeFailedPrecondition, errors.New(message))
		}
		// 3. The ordinary pool plan over the child: secret policy, the
		// allow-list, the liveness probe, pin hosts, fan-out support and the
		// child's own telemetry selection.
		childPlanned, childPlan, err := r.resolver.Plan(ctx, r.ns, child, r.allowEmpty)
		if err != nil {
			return childRefusal(poolPlanError(err), ref, t.GetId())
		}
		// 4. The image policy: one selection for the whole tree.
		if err := s.checkImagePolicy(ctx, r.ns, r.sel, childPlanned); err != nil {
			return childRefusal(submitPolicyError(err), ref, t.GetId())
		}
		childPlan.ImagePolicy = engine.ImagePolicySelectionFromProto(r.sel)
		// 5. The fan-out cap, re-checked per child.
		if err := s.checkEachCapAtSubmit(child); err != nil {
			return childRefusal(err, ref, t.GetId())
		}
		// 6. The durable activity configuration.
		if err := s.pinDurabilityPlan(&childPlan); err != nil {
			return connect.NewError(connect.CodeInternal,
				fmt.Errorf("child orkflow %s@%s (task %q): resolve durable activity configuration: %w",
					name, version, t.GetId(), err))
		}
		// 7. Its own children.
		childPath := append(append([]string(nil), path...), composeKey(name, version))
		if err := s.planChildren(ctx, r, childPlanned, &childPlan, depth+1, childPath); err != nil {
			return err
		}
		// 8. The plan entry.
		cp, err := engine.NewChildRunPlan(childPlanned, childPlan, r.namespaceIndexed, r.originIndexed)
		if err != nil {
			return connect.NewError(connect.CodeInternal, err)
		}
		if plan.Children == nil {
			plan.Children = map[string]*engine.ChildRunPlan{}
		}
		plan.Children[t.GetId()] = cp
	}
	return nil
}

// mintChildren discloses nothing and refuses once: it mints every child's
// callback awaits (mintAwaitCallbacks on the child's planned definition and
// plan; plaintexts discarded), then encodes the plan with every RunID set to
// a MaxRunIDLength placeholder and refuses when it exceeds
// engine.MaxPlanBytes, then assigns RunID = s.newRunID() to every
// ChildRunPlan in the tree, depth first, declaration order. A plan with no
// children is untouched.
func (s *Service) mintChildren(planned *orkstackv1.Orkflow, plan *engine.RunPlan) error {
	if len(plan.Children) == 0 {
		return nil
	}
	placeholder := strings.Repeat("0", engine.MaxRunIDLength)
	var all []*engine.ChildRunPlan
	err := engine.ForEachChild(planned, plan, func(_ string, child *engine.ChildRunPlan, def *orkstackv1.Orkflow) error {
		// A child's callback tokens: the digests ride its plan, the
		// plaintexts are discarded here — disclosed to no one.
		if _, err := mintAwaitCallbacks(def, &child.Plan); err != nil {
			return err
		}
		child.RunID = placeholder
		all = append(all, child)
		return nil
	})
	if err != nil {
		return connect.NewError(connect.CodeInternal, fmt.Errorf("prepare the child runs: %w", err))
	}
	encoded, err := json.Marshal(plan)
	if err != nil {
		return connect.NewError(connect.CodeInternal, fmt.Errorf("encode the run's plan: %w", err))
	}
	if len(encoded) > engine.MaxPlanBytes {
		return connect.NewError(connect.CodeFailedPrecondition, fmt.Errorf(
			"the run's plan — this definition and the %d child runs it composes — encodes to %d KiB, "+
				"over the %d KiB a run may carry; compose fewer or smaller Orkflows",
			len(all), kib(len(encoded)), engine.MaxPlanBytes/1024))
	}
	for _, child := range all {
		child.RunID = s.newRunID()
	}
	return nil
}

// composedTaskLogRefusal is StreamTaskLogs' answer for a composed task: it
// writes no logs of its own, and a follower would wait on chunks nobody
// writes.
func composedTaskLogRefusal(runID string, taskRun *orkstackv1.TaskRun) error {
	child := taskRun.GetChild()
	if child == nil {
		return nil
	}
	label := child.GetOrkflowName() + "@" + child.GetOrkflowVersion()
	if child.GetRunId() == "" {
		return connect.NewError(connect.CodeFailedPrecondition, fmt.Errorf(
			"task %q of run %q runs orkflow %s as a child run and writes no logs of its own; its child run has not started",
			taskRun.GetTaskId(), runID, label))
	}
	return connect.NewError(connect.CodeFailedPrecondition, fmt.Errorf(
		"task %q of run %q runs orkflow %s as child run %s and writes no logs of its own; read the child's: orkctl run logs %s",
		taskRun.GetTaskId(), runID, label, child.GetRunId(), child.GetRunId()))
}
