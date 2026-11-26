package api

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"

	"connectrpc.com/connect"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/authn"
	"github.com/orkstack/orkstack/internal/authz"
	"github.com/orkstack/orkstack/internal/dsl"
	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/internal/stablejson"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// Composed Orkflows (docs/DSL.md "Composition"): the apply-time
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
	Owner    string                 // the namespace that owns the child, resolved
	Orkflow  *orkstackv1.Orkflow    // the stored version it resolved to
	Path     []string               // "name@version" from the root, this node last
	Children []*composeNode         // declaration order
}

// composeProblem is one node that did not resolve: which root task it hangs
// off, its path, and the fixed sentence for its kind.
type composeProblem struct {
	TaskIndex int      // the ROOT composing task's index, where apply anchors
	Path      []string // "name@version" from the root
	Kind      string   // "missing" | "digest" | "cycle" | "depth" | "own-namespace" | "not-shared"
	Message   string
}

// composeKey is a reference's "name@version".
func composeKey(name, version string) string {
	// A document being prepared has no version yet when it pins none, and nothing
	// can compose a version that does not exist, so its name alone cannot clash.
	if version == "" {
		return name
	}
	return name + "@" + version
}

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
// order — depth (depth+1 > maxDepth), cycle (key already on path; treeKey),
// existence (stored is nil: the store answered NotFound), pinned digest
// (stored.definition_digest != ref.definition_digest, skipped only for a root
// ref being stamped at apply). Pure. loadComposeTree (apply) and
// planChildren (submit) both call it, so the two doors cannot disagree about
// what a tree may be. path is the composing definition's own path.
func checkComposeNode(ref *orkstackv1.OrkflowRef, key string, stored *orkstackv1.Orkflow, path []string, depth, maxDepth int, stamping bool) *composeProblem {
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
			"%s pins %s as %s, but the stored %s is %s — it was deleted and created again with other "+
				"content; create %s as a new version to re-pin it",
			parent, key, ref.GetDefinitionDigest(), key, stored.GetDefinitionDigest(),
			pathNames([]string{parent})[0])}
	}
	return nil
}

// composeReader memoizes one call's reads: one read per distinct
// namespace/name@version.
type composeReader struct {
	s     *Service
	cache map[string]*orkstackv1.Orkflow
	seen  map[string]bool
}

// get reads one version of owner's; nil, nil is NotFound.
func (r *composeReader) get(ctx context.Context, owner, name, version string) (*orkstackv1.Orkflow, error) {
	key := owner + "/" + composeKey(name, version)
	if r.seen[key] {
		return r.cache[key], nil
	}
	stored, err := r.s.orkflows.Get(ctx, owner, name, version)
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
	reader := &composeReader{s: s, cache: map[string]*orkstackv1.Orkflow{}, seen: map[string]bool{}}
	rootNS := artifacts.ResolveNamespace(ns)
	root := []string{composeKey(wf.GetName(), wf.GetVersion())}
	var problems []composeProblem
	var walk func(def *orkstackv1.Orkflow, path []string, depth, rootIndex int, rootTask string) ([]*composeNode, error)
	walk = func(def *orkstackv1.Orkflow, path []string, depth, rootIndex int, rootTask string) ([]*composeNode, error) {
		var nodes []*composeNode
		composer := rootNS
		if depth > 0 {
			composer = engine.DefinitionOwner(def)
		}
		for i, t := range def.GetTasks() {
			ref := t.GetOrkflow()
			if ref == nil {
				continue
			}
			index, task := rootIndex, rootTask
			if depth == 0 {
				index, task = i, t.GetId()
			}
			owner := refOwner(composer, ref)
			key := treeKey(rootNS, owner, ref.GetName(), ref.GetVersion())
			report := func(problem *composeProblem) {
				problem.TaskIndex = index
				if depth > 0 {
					problem.Message = fmt.Sprintf("%s: %s",
						strings.Join(append([]string{task}, problem.Path[1:]...), " → "), problem.Message)
				}
				problems = append(problems, *problem)
			}
			if err := ownNamespaceRef(composer, ref); err != nil {
				report(&composeProblem{Path: append(append([]string(nil), path...), key), Kind: "own-namespace", Message: err.Error()})
				continue
			}
			stored, err := reader.get(ctx, owner, ref.GetName(), ref.GetVersion())
			if err != nil {
				return nil, err
			}
			stamping := stampRoot && depth == 0 && ref.GetDefinitionDigest() == ""
			if problem := checkComposeNode(ref, key, stored, path, depth, s.composeMaxDepth, stamping); problem != nil {
				problem.TaskIndex = index
				if problem.Kind == "missing" {
					problem.Message = s.missingChildSentence(ctx, owner, ref)
					if foreign, opaque := s.opaqueForeignRef(ctx, composer, def.GetName(), owner, ref); opaque {
						problem.Kind, problem.Message = "not-shared", foreign
					}
				}
				if depth > 0 {
					problem.Message = fmt.Sprintf("%s: %s",
						strings.Join(append([]string{task}, problem.Path[1:]...), " → "), problem.Message)
				}
				problems = append(problems, *problem)
				continue
			}
			// Sharing: another namespace's Orkflow is composed only when the
			// pinned version's shared_with admits the Orkflow writing uses:.
			if owner != composer {
				if _, err := authorizeChild(composer, def.GetName(), owner, ref, stored); err != nil {
					message := err.Error()
					if foreign, opaque := s.opaqueForeignRef(ctx, composer, def.GetName(), owner, ref); opaque {
						message = foreign
					}
					report(&composeProblem{Path: append(append([]string(nil), path...), key), Kind: "not-shared", Message: message})
					continue
				}
			}
			if stamping {
				ref.DefinitionDigest = stored.GetDefinitionDigest()
			}
			node := &composeNode{
				TaskID:  t.GetId(),
				Ref:     ref,
				Owner:   owner,
				Orkflow: stored,
				Path:    append(append([]string(nil), path...), key),
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

// opaqueForeignRef is the one sentence a caller who may not read another
// namespace's Orkflow gets for it, whether it is missing, lacks that version
// or is not shared: a composition must not be a way to list another
// namespace's Orkflows or their versions. opaque is false where the caller
// may read it, or in asserted mode, and the precise sentence stands.
func (s *Service) opaqueForeignRef(ctx context.Context, composer, composerName, owner string, ref *orkstackv1.OrkflowRef) (string, bool) {
	if !s.access.enabled() || owner == composer || s.mayListVersions(ctx, owner, ref.GetName()) {
		return "", false
	}
	return fmt.Sprintf("%s: orkflow %q is not shared with %q, or no such Orkflow or version exists in a namespace you may read",
		authz.ReasonNotShared, owner+"/"+ref.GetName()+"@"+ref.GetVersion(), composer+"/"+composerName), true
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
// the stored versions appended.
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
	if !s.mayListVersions(ctx, ns.GetName(), missing.Name) {
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

// mayListVersions reports whether a refusal may name the versions of
// namespace/name: always in asserted mode; with authentication on, only in
// the namespace the request was decided in, or where the caller may read
// that Orkflow. A refusal must not be a way to list another namespace.
func (s *Service) mayListVersions(ctx context.Context, namespace, name string) bool {
	if !s.access.enabled() {
		return true
	}
	if d := decisionFrom(ctx); d != nil {
		if scope, err := authz.ParseScope(d.Scope); err == nil && scope.Namespace != "" && scope.Namespace == namespace {
			return true
		}
	}
	policy, _ := s.access.policy(ctx)
	// It decides only what a refusal's message names: one whose ids cannot
	// be read names nothing, which is the refusal it would otherwise make.
	may, err := s.mayWithIDs(ctx, authz.Request{Action: authz.Action(authz.PermOrkflowRead),
		Subject: subjectOf(authn.FromContext(ctx)), Resource: authz.Resource{Namespace: namespace, Orkflow: name}, Policy: policy})
	return err == nil && may
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
	// Rule 48 against the namespace the document is applied into.
	var shareIssues []*orkstackv1.ValidationIssue
	if issue := sharedWithIssue(ns, wf); issue != nil {
		shareIssues = append(shareIssues, issue)
	}
	composes := false
	for _, t := range wf.GetTasks() {
		if t.GetOrkflow() != nil {
			composes = true
			break
		}
	}
	if !composes {
		return shareIssues, nil
	}
	nodes, problems, err := s.loadComposeTree(ctx, ns.GetName(), wf, true)
	if err != nil {
		return nil, orkflowStoreError(err, "read the orkflows %s composes in namespace %q",
			engine.OrkflowRefText(wf), ns.GetName())
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
		if err := engine.CheckForeignSecrets(ns, node.Owner, node.Orkflow); err != nil {
			rule31 = append(rule31, composeIssue(usesField(rootIndex), prefix+err.Error(), errorSeverity))
		}
		if err := s.checkChildIdentity(ctx, ns, node.Orkflow, node.Owner); err != nil {
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
	}
	for _, node := range nodes {
		i := taskIndex[node.TaskID]
		rule32 = append(rule32, childInputIssues(i, wf.GetTasks()[i], node)...)
		rule32 = append(rule32, childArtifactIssues(i, wf.GetTasks()[i], node, children)...)
	}
	rule32 = append(rule32, eachSourceIssues(wf, children)...)
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
			if _, isArtifact := node.Orkflow.GetOutputArtifacts()[ref.Key]; isArtifact {
				rule32 = append(rule32, composeIssue(ref.Field, fmt.Sprintf(
					"%s: %s returns %q as an artifact, not a value; read it as {{ artifact \"%s/%s\" }}",
					reading, label, ref.Key, ref.TaskID, ref.Key), errorSeverity))
				continue
			}
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
	return append(append(shareIssues, rule31...), rule32...), nil
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
		decl := dsl.ReadInput(declared[name])
		if !decl.Required || decl.Type == dsl.InputTypeArtifact {
			continue // an artifact input is bound under artifacts: (childArtifactIssues)
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
		if decl, ok := declared[k]; ok && dsl.ReadInput(decl).Type == dsl.InputTypeArtifact {
			issues = append(issues, composeIssue(field(k), fmt.Sprintf(
				"with.%s is an artifact input of %s; bind it under artifacts:, never with:", k, label),
				orkstackv1.ValidationSeverity_VALIDATION_SEVERITY_ERROR))
			continue
		}
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
		if want == "" || want == dsl.InputTypeArtifact || value == nil {
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

// childArtifactIssues is rule 32's artifacts: half for one composed task:
// every artifact input of the child is bound, every binding names one, and a
// binding to a composed task of this document names an artifact its child
// returns (all ERRORs: an unbound or wrong binding fails the child's start).
func childArtifactIssues(taskIndex int, t *orkstackv1.Task, node *composeNode,
	children map[string]*composeNode) []*orkstackv1.ValidationIssue {
	label := dsl.ComposedLabel(node.Ref)
	declared := node.Orkflow.GetInputs().GetFields()
	field := func(k string) string { return fmt.Sprintf("tasks[%d].artifacts.%s", taskIndex, k) }
	issue := func(k, msg string) *orkstackv1.ValidationIssue {
		return composeIssue(field(k), msg, orkstackv1.ValidationSeverity_VALIDATION_SEVERITY_ERROR)
	}
	bound := map[string]*orkstackv1.ChildArtifact{}
	for _, b := range t.GetChildArtifacts() {
		bound[b.GetInput()] = b
	}
	artifactInputs := dsl.ArtifactInputs(node.Orkflow)
	var issues []*orkstackv1.ValidationIssue
	for _, name := range artifactInputs {
		if _, ok := bound[name]; !ok {
			issues = append(issues, issue(name, fmt.Sprintf(
				"artifacts.%s is required by %s and is not bound; bind it to an artifact: %s: <task>/<name>",
				name, label, name)))
		}
	}
	inputs := "none"
	if len(artifactInputs) > 0 {
		inputs = strings.Join(artifactInputs, ", ")
	}
	for _, b := range t.GetChildArtifacts() {
		name := b.GetInput()
		decl, ok := declared[name]
		if !ok {
			issues = append(issues, issue(name, fmt.Sprintf(
				"artifacts.%s is not an input of %s (artifact inputs: %s)", name, label, inputs)))
			continue
		}
		if typ := dsl.ReadInput(decl).Type; typ != dsl.InputTypeArtifact {
			if typ == "" {
				typ = "untyped"
			}
			issues = append(issues, issue(name, fmt.Sprintf(
				"artifacts.%s binds an artifact, but %s declares input %s as %s; pass it in with:",
				name, label, name, typ)))
			continue
		}
		producer, ok := children[b.GetTaskId()]
		if !ok {
			continue
		}
		if why := returnsNoArtifact(producer, b.GetName()); why != "" {
			issues = append(issues, issue(name, fmt.Sprintf("artifacts.%s: %s", name, why)))
		}
	}
	return issues
}

// returnsNoArtifact is why a composed task's child returns no artifact name,
// or "" when it does.
func returnsNoArtifact(producer *composeNode, name string) string {
	returned := producer.Orkflow.GetOutputArtifacts()
	if _, ok := returned[name]; ok {
		return ""
	}
	names := make([]string, 0, len(returned))
	for k := range returned {
		names = append(names, k)
	}
	sort.Strings(names)
	list := "none"
	if len(names) > 0 {
		list = strings.Join(names, ", ")
	}
	return fmt.Sprintf("%s returns no artifact %q (artifact outputs: %s)", dsl.ComposedLabel(producer.Ref), name, list)
}

// eachSourceIssues is rule 32 for a fan-out over the lines of a composed
// task's artifact: its child must return that artifact, or the fan-out could
// only ever fail EACH_OVER_MISSING.
func eachSourceIssues(wf *orkstackv1.Orkflow, children map[string]*composeNode) []*orkstackv1.ValidationIssue {
	var issues []*orkstackv1.ValidationIssue
	for i, t := range wf.GetTasks() {
		art := t.GetEach().GetSource().GetArtifact()
		producer, ok := children[art.GetProducer()]
		if art == nil || !ok {
			continue
		}
		if why := returnsNoArtifact(producer, art.GetName()); why != "" {
			issues = append(issues, composeIssue(fmt.Sprintf("tasks[%d].each.over_artifact", i),
				"each.over_artifact: "+why, orkstackv1.ValidationSeverity_VALIDATION_SEVERITY_ERROR))
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
	case float64, stablejson.Number:
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
	// noCache is SubmitRunRequest.no_cache: one for the whole tree.
	noCache bool
	// holdsIdentity is a run whose tree holds an identity: every child's
	// pools are asked whether they serve task identity.
	holdsIdentity bool
	// rootOrigin is the kind of start of the tree's root, which a grant
	// that names the starts it serves is judged on in every run of it.
	rootOrigin string
}

// rootIndexAnswer is the boot answer a plan pins for its child starts (false
// for a starter that does not say, which stamps nothing and so is always safe).
func rootIndexAnswer(starter engine.RunStarter) bool {
	if answer, ok := starter.(interface{ RootIndexAnswer() bool }); ok {
		return answer.RootIndexAnswer()
	}
	return false
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
// Connect code and how it is classified, and nothing else of its chain: a
// field path of the child's is none of the parent's.
func childRefusal(err error, ref *orkstackv1.OrkflowRef, taskID string) error {
	prefix := fmt.Sprintf("child orkflow %s (task %q): ", dsl.ComposedLabel(ref), taskID)
	var connectErr *connect.Error
	if errors.As(err, &connectErr) {
		return connect.NewError(connectErr.Code(), &refusalMark{verdict: classifyRefusal(err), err: errors.New(prefix + connectErr.Message())})
	}
	return connect.NewError(connect.CodeFailedPrecondition, &refusalMark{verdict: classifyRefusal(err), err: errors.New(prefix + err.Error())})
}

// planChildren builds plan.Children for every composed task of parent, in
// declaration order, and recursively for theirs. It refuses and mints
// nothing.
func (s *Service) planChildren(ctx context.Context, r composeRequest, parent *orkstackv1.Orkflow, plan *engine.RunPlan, depth int, path []string) error {
	if len(path) == 0 {
		path = []string{composeKey(parent.GetName(), parent.GetVersion())}
	}
	root := artifacts.ResolveNamespace(r.ns.GetName())
	composer := engine.DefinitionOwner(parent)
	for _, t := range parent.GetTasks() {
		ref := t.GetOrkflow()
		if ref == nil {
			continue
		}
		name, version := ref.GetName(), ref.GetVersion()
		owner := refOwner(composer, ref)
		if err := ownNamespaceRef(composer, ref); err != nil {
			return childRefusal(refused(refusalPause, "COMPOSED_ORKFLOW_UNUSABLE", err), ref, t.GetId())
		}
		// 1. The stored child, read from the namespace that owns it.
		child, err := s.orkflows.Get(ctx, owner, name, version)
		if err != nil && !errors.Is(err, artifacts.ErrNotFound) {
			return orkflowStoreError(err, "child orkflow %s@%s of task %q in namespace %q", name, version, t.GetId(), owner)
		}
		if err != nil {
			child = nil
		}
		key := treeKey(root, owner, name, version)
		// 2. The tree's shape, by the same rule apply holds it to.
		if problem := checkComposeNode(ref, key, child, path, depth, s.composeMaxDepth, false); problem != nil {
			message := problem.Message
			switch problem.Kind {
			case "depth":
				message = fmt.Sprintf("composition is %d levels deep (%s); this deployment now allows %d "+
					"(ORK_COMPOSE_MAX_DEPTH was lowered after %s was applied)",
					depth+1, strings.Join(pathNames(problem.Path), " → "), s.composeMaxDepth, path[0])
			case "missing":
				message = fmt.Sprintf("child orkflow %s@%s of task %q no longer exists in namespace %q; apply "+
					"it again, or submit a version of %s that composes an existing one",
					name, version, t.GetId(), owner, pathNames(path[len(path)-1:])[0])
			}
			return connect.NewError(connect.CodeFailedPrecondition, refused(refusalPause, "COMPOSED_ORKFLOW_UNUSABLE", errors.New(message)))
		}
		// 2b. Sharing, against the pinned version's own shared_with. The
		// planned copy of another namespace's Orkflow runs in this run's
		// namespace, under its rules, and names its owner.
		var decision authz.Decision
		if owner != composer {
			if decision, err = authorizeChild(composer, parent.GetName(), owner, ref, child); err != nil {
				return childRefusal(err, ref, t.GetId())
			}
		}
		child = plannedInRunNamespace(child, r.ns.GetName(), owner)
		if err := foreignSecretsRefusal(r.ns, owner, child); err != nil {
			return childRefusal(err, ref, t.GetId())
		}
		needs, err := s.checkIdentityAtSubmit(ctx, r.ns, child, owner, r.rootOrigin)
		if err != nil {
			return childRefusal(err, ref, t.GetId())
		}
		// 3. The ordinary pool plan over the child: secret policy, the
		// allow-list, the liveness probe, pin hosts, fan-out support and the
		// child's own telemetry selection.
		childPlanned, childPlan, err := r.resolver.Plan(ctx, r.ns, child, r.allowEmpty)
		if err != nil {
			return childRefusal(poolPlanError(err), ref, t.GetId())
		}
		if r.holdsIdentity {
			if err := r.resolver.CheckIdentitySupport(ctx, r.ns, childPlanned, needs); err != nil {
				return childRefusal(poolPlanError(err), ref, t.GetId())
			}
		}
		if owner != composer {
			childPlan.Shared = &decision
		}
		// Fairness is a run-tree value: the child inherits the parent's at
		// start (childStartPlan), so the nested plan carries none of its
		// bytes inside the 256 KiB plan.
		childPlan.Fairness = nil
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
		s.pinCommitWait(&childPlan, r.holdsIdentity)
		// 6b. The child's caching plan, under the parent's no_cache.
		childPlan.Cache = s.cachePlanFor(childPlanned, r.noCache)
		childPlan.RunTimeout = s.runTimeoutPlanFor(childPlanned)
		childPlan.EachLines = eachLinesPolicyFor(childPlanned)
		childPlan.Pause = s.pausePlanFor()
		// 7. Its own children.
		childPath := append(append([]string(nil), path...), key)
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
	// Every artifacts: binding of the tree reserves room for the reference a
	// composed task adds to its child's start (ArtifactClaimPolicy).
	bindings := countChildArtifacts(planned)
	returns := false
	err := engine.ForEachChild(planned, plan, func(_ string, child *engine.ChildRunPlan, def *orkstackv1.Orkflow) error {
		bindings += countChildArtifacts(def)
		returns = returns || len(def.GetOutputArtifacts()) > 0
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
	// A tree that passes or returns artifacts pins how its claims are tried,
	// once, on the top-level plan; every child's start copies it
	// (childStartPlan).
	if bindings > 0 || returns {
		claims := engine.DefaultArtifactClaims
		plan.ArtifactClaims = &claims
	}
	encoded, err := stablejson.Marshal(plan)
	if err != nil {
		return connect.NewError(connect.CodeInternal, fmt.Errorf("encode the run's plan: %w", err))
	}
	// What a child's start adds to its planned bytes: its origin, and the
	// references its artifacts: bindings pass. Its trace is no larger than the
	// top-level plan's, which is in the measured bytes (a context's parts have
	// fixed lengths, and the state is copied).
	references := 0
	if plan.ArtifactClaims != nil {
		references = bindings * int(plan.ArtifactClaims.ReserveBytesPerBind)
	}
	reserved := engine.ChildOriginMaxBytes + references
	if len(encoded)+reserved > engine.MaxPlanBytes {
		what := fmt.Sprintf(", and a child's start reserves %d KiB for its origin,", engine.ChildOriginMaxBytes/1024)
		if references > 0 {
			what = fmt.Sprintf(", a child's start reserves %d KiB for its origin, and its %d artifact bindings "+
				"reserve %d KiB for the references they pass,", engine.ChildOriginMaxBytes/1024, bindings, kib(references))
		}
		return connect.NewError(connect.CodeFailedPrecondition, refused(refusalPause, "PLAN_TOO_LARGE", fmt.Errorf(
			"the run's plan — this definition and the %d child runs it composes — encodes to %d KiB%s "+
				"over the %d KiB a run may carry; compose fewer or smaller Orkflows",
			len(all), kib(len(encoded)), what, engine.MaxPlanBytes/1024)))
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

// countChildArtifacts is the number of artifacts: bindings of wf's composed
// tasks.
func countChildArtifacts(wf *orkstackv1.Orkflow) int {
	n := 0
	for _, t := range wf.GetTasks() {
		n += len(t.GetChildArtifacts())
	}
	return n
}
