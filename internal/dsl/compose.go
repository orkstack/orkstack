package dsl

import (
	"errors"
	"fmt"
	"regexp"
	"sort"
	"strings"

	"google.golang.org/protobuf/types/known/structpb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
)

// OrkflowUsesPrefix marks a composed task's `uses:`. A `uses:` that begins
// with it is ALWAYS a composition reference and never an image: a registry
// host literally named "orkflow" is written with its domain
// ("orkflow.example:5000/app"), which does not begin with "orkflow:".
const OrkflowUsesPrefix = "orkflow:"

// orkflowVersionPattern is the rule pkg/artifacts holds a stored version to
// (validOrkflowComponent, cleanComponentPattern in s3_manifest.go), spelled
// here rather than imported — this package imports nothing from
// pkg/artifacts, and MaxSlug sets the precedent: two spellings held together
// by measurement (TestOrkflowVersionRule_RegistryAndCompilerAgree), not a
// shared constant.
var orkflowVersionPattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]*$`)

// orkflowUsesShape is the one accepted spelling, quoted in every refusal.
const orkflowUsesShape = "orkflow:<name>@<version>"

// ComposeVersionRequiredError is `uses: orkflow:<name>` with no version.
// Namespace is the document's own `namespace:` (empty = default), carried so
// internal/api can name the versions that exist; the compiler is pure and
// cannot.
type ComposeVersionRequiredError struct {
	TaskIndex int
	TaskID    string
	Name      string
	Namespace string
}

func (e *ComposeVersionRequiredError) Error() string {
	sentence := fmt.Sprintf("uses: orkflow:%s needs a version — a composed task pins one immutable "+
		"version and never follows a pointer; list them with orkctl orkflow versions %s", e.Name, e.Name)
	if e.TaskID == "" {
		return sentence
	}
	return fmt.Sprintf("task %q: %s", e.TaskID, sentence)
}

// ParseOrkflowUses reports whether uses is a composition reference and, when
// it is, the name and version it pins. The only accepted shape is
//
//	orkflow:<name>@<version>
//
// with <name> a slug (checkSlug, rule 2's) and <version> a stored-version
// string: non-empty, at most MaxSlug bytes, ^[A-Za-z0-9][A-Za-z0-9._-]*$ —
// the rule pkg/artifacts holds a version to. A template action is refused
// before anything else (ResolveUses rule 1's sentence). A missing
// "@<version>" is *ComposeVersionRequiredError. ok is false, with a nil
// error, for a `uses:` that does not begin with OrkflowUsesPrefix.
func ParseOrkflowUses(uses string) (name, version string, ok bool, err error) {
	if !strings.HasPrefix(uses, OrkflowUsesPrefix) {
		return "", "", false, nil
	}
	refuse := func(why string) error {
		return fmt.Errorf("uses: %q is not a composition reference: %s; the only accepted shape is %s",
			uses, why, orkflowUsesShape)
	}
	if templateAction.MatchString(uses) {
		return "", "", true, fmt.Errorf("uses: %q cannot be a template. The engine resolves `uses:` to "+
			"an immutable definition when the orkflow is applied, and a value that depends on run "+
			"input is not knowable then. Compose one pinned version (%s)", uses, orkflowUsesShape)
	}
	rest := strings.TrimPrefix(uses, OrkflowUsesPrefix)
	name, version, found := strings.Cut(rest, "@")
	if name == "" {
		return "", "", true, refuse("it names no orkflow")
	}
	if err := checkSlug("orkflow name", name); err != nil {
		return "", "", true, refuse(err.Error())
	}
	if !found {
		return "", "", true, &ComposeVersionRequiredError{Name: name}
	}
	switch {
	case version == "":
		return "", "", true, &ComposeVersionRequiredError{Name: name}
	case len(version) > MaxSlug:
		return "", "", true, refuse(fmt.Sprintf("its version of %d bytes exceeds the %d-byte limit",
			len(version), MaxSlug))
	case !orkflowVersionPattern.MatchString(version):
		return "", "", true, refuse(fmt.Sprintf("its version %q must match %s", version, orkflowVersionPattern))
	}
	return name, version, true, nil
}

// IsComposedTask reports whether t is a composed task: its `uses:` names an
// Orkflow, or (in a hand-built message) its compiled reference is set.
func IsComposedTask(t *orkstackv1.Task) bool {
	return t.GetOrkflow() != nil || strings.HasPrefix(t.GetUses(), OrkflowUsesPrefix)
}

// ComposedLabel renders a reference as "name@version".
func ComposedLabel(ref *orkstackv1.OrkflowRef) string {
	return ref.GetName() + "@" + ref.GetVersion()
}

// composeFieldRefusal is rule 27's sentence for one key.
func composeFieldRefusal(taskID, key, name, version string) error {
	return fmt.Errorf("task %q: a composed task takes only id, needs, condition, join, uses and with; "+
		"%q does not apply (it runs orkflow %s@%s as a child run, whose own tasks declare where "+
		"and how they run)", taskID, key, name, version)
}

// composeExecutionKeys lists, in the order they are checked, the YAML keys a
// composed task may not carry, each with whether the author wrote it. It is
// the YAML-layer half of rule 27: the compiled message is checked again by
// validate, which is what a hand-built request meets.
func composeExecutionKeys(td taskDocument) []struct {
	key     string
	written bool
} {
	return []struct {
		key     string
		written bool
	}{
		{"command", td.Command != nil},
		{"env", td.Env != nil},
		{"secrets", td.Secrets != nil},
		{"resources", td.Resources != nil},
		{"network", td.Network != ""},
		{"pool", td.Pool != ""},
		{"pin", td.Pin != ""},
		{"retry", td.Retry != nil},
		{"timeout_seconds", td.TimeoutSeconds != nil},
		{"cancellation_grace_period_seconds", td.CancellationGracePeriodSeconds != nil},
		{"run_as_orker_user", td.RunAsOrkerUser},
		{"each", td.Each != nil},
		{"await", td.Await != nil},
		{"cache", td.Cache != nil},
	}
}

// compileComposedTask builds the Task for a document whose `uses:` names an
// Orkflow, after refusing every execution key the author wrote. The
// reference's definition_digest stays empty: internal/api stamps it at apply,
// exactly as it stamps Task.image.
func compileComposedTask(taskIndex int, td taskDocument, namespace string) (*orkstackv1.Task, error) {
	name, version, _, err := ParseOrkflowUses(td.Uses)
	if err != nil {
		if missing, ok := err.(*ComposeVersionRequiredError); ok {
			missing.TaskIndex = taskIndex
			missing.TaskID = td.ID
			missing.Namespace = namespace
			return nil, validationError(fmt.Sprintf("tasks[%d].uses", taskIndex), missing)
		}
		return nil, validationError(fmt.Sprintf("tasks[%d].uses", taskIndex), fmt.Errorf("task %q: %w", td.ID, err))
	}
	for _, field := range composeExecutionKeys(td) {
		if field.written {
			return nil, validationError(fmt.Sprintf("tasks[%d].%s", taskIndex, field.key),
				composeFieldRefusal(td.ID, field.key, name, version))
		}
	}
	join, err := compileJoin(taskIndex, td)
	if err != nil {
		return nil, err
	}
	task := &orkstackv1.Task{
		Id:        td.ID,
		Uses:      td.Uses,
		Needs:     td.Needs,
		Condition: td.Condition,
		Join:      join,
		// 0, not DefaultTimeoutSeconds: a composed task has no timeout of its
		// own — its child's tasks carry theirs (rule 27).
		TimeoutSeconds: 0,
		Orkflow:        &orkstackv1.OrkflowRef{Name: name, Version: version},
	}
	if td.With != nil {
		with, err := structpb.NewStruct(normalizeMap(td.With))
		if err != nil {
			return nil, fmt.Errorf("task %q: with: %w", td.ID, err)
		}
		task.With = with
	}
	return task, nil
}

// validateComposedTask enforces rules 26 and 27 on one compiled task whose
// `uses:` begins with OrkflowUsesPrefix.
func validateComposedTask(taskIndex int, task *orkstackv1.Task) error {
	id := task.GetId()
	at := func(field string) string { return fmt.Sprintf("tasks[%d].%s", taskIndex, field) }
	// Rule 26: the reference parses, and the compiled ref agrees with it.
	name, version, _, err := ParseOrkflowUses(task.GetUses())
	if err != nil {
		if missing, ok := err.(*ComposeVersionRequiredError); ok {
			missing.TaskIndex = taskIndex
			missing.TaskID = id
			return validationError(at("uses"), missing)
		}
		return validationError(at("uses"), fmt.Errorf("task %q: %w", id, err))
	}
	ref := task.GetOrkflow()
	if ref == nil {
		return validationError(at("orkflow"), fmt.Errorf(
			"task %q: uses: %s names an orkflow, but the task carries no compiled orkflow reference",
			id, task.GetUses()))
	}
	if ref.GetName() != name || ref.GetVersion() != version {
		return validationError(at("orkflow"), fmt.Errorf(
			"task %q: orkflow %s@%s does not match uses: %s — the reference is compiled from uses: "+
				"and must agree with it", id, ref.GetName(), ref.GetVersion(), task.GetUses()))
	}
	if task.GetImage() != nil {
		return validationError(at("image"), fmt.Errorf("task %q: a composed task runs no image", id))
	}
	// Rule 27: nothing but id, needs, condition, uses, with and orkflow.
	checks := []struct {
		field string
		set   bool
	}{
		{"command", len(task.GetCommand()) > 0},
		{"env", len(task.GetEnv()) > 0},
		{"secrets", len(task.GetSecrets()) > 0},
		{"resources", task.GetResources() != nil},
		{"network", task.GetNetwork() != orkstackv1.NetworkMode_NETWORK_MODE_UNSPECIFIED},
		{"pool", task.GetPool() != ""},
		{"pin", task.GetPin() != ""},
		{"retry", task.GetRetry() != nil},
		{"timeout_seconds", task.GetTimeoutSeconds() != 0},
		{"cancellation_grace_period_seconds", task.GetCancellationGracePeriodSeconds() != 0},
		{"run_as_orker_user", task.GetRunAsOrkerUser()},
		{"each", task.GetEach() != nil},
		{"await", task.GetAwait() != nil},
		{"cache", task.GetCache() != nil},
	}
	for _, c := range checks {
		if c.set {
			return validationError(at(c.field), composeFieldRefusal(id, c.field, name, version))
		}
	}
	return nil
}

// outputNamePattern is rule 29's key rule: a template field name, so
// .tasks.<task>.output.<name> parses.
var outputNamePattern = regexp.MustCompile(`^[a-z][a-z0-9_]*$`)

// maxOutputNameBytes bounds one output name (rule 29).
const maxOutputNameBytes = 128

// checkOutputName is rule 29's key half.
func checkOutputName(key string) error {
	if len(key) > maxOutputNameBytes || !outputNamePattern.MatchString(key) {
		return validationError("outputs."+key, fmt.Errorf(
			"outputs.%s: an output name is a template field name — .tasks.<task>.output.<name> must "+
				"parse (%s, at most %d bytes)", key, outputNamePattern, maxOutputNameBytes))
	}
	return nil
}

// validateOutputs is rule 29 over wf.outputs, in sorted key order.
func validateOutputs(wf *orkstackv1.Orkflow, inputs map[string]struct{}, byID map[string]*orkstackv1.Task) error {
	outputs := wf.GetOutputs()
	if len(outputs) == 0 {
		return nil
	}
	upstream := make(map[string]struct{}, len(byID))
	for id, t := range byID {
		if t.GetEach() == nil {
			upstream[id] = struct{}{}
		}
	}
	keys := make([]string, 0, len(outputs))
	for k := range outputs {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, key := range keys {
		if err := checkOutputName(key); err != nil {
			return err
		}
		checker := &templateChecker{
			taskID:     "outputs",
			inputs:     inputs,
			tasks:      byID,
			upstream:   upstream,
			engine:     true,
			engineWhat: "an Orkflow's outputs: are rendered by the engine when the run succeeds, and carry values, not files",
		}
		if err := checker.checkString("outputs."+key, outputs[key]); err != nil {
			var anchored *ValidationError
			if errors.As(err, &anchored) {
				return err
			}
			return validationError("outputs."+key, err)
		}
	}
	return nil
}
