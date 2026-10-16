package dsl

import (
	"errors"
	"fmt"
	"regexp"
	"slices"
	"sort"
	"strings"

	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/types/known/structpb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
)

// Activity interface manifests.
//
// An image MAY declare its `with:` contract in one OCI image label. The
// engine reads it during the apply-time resolution it already performs
// (registry_resolver.go, Inspect) and checks every task's `with:` against it
// (CheckOrkflowInterfaces) — in internal/api, after resolution, never in
// Compile, which stays pure. An image without the label is an ordinary
// activity: nothing here is required of it, and nothing here is stored.

// InterfaceLabel is the OCI image label that carries an ActivityInterface.
const InterfaceLabel = "org.orkstack.activity.interface"

// Bounds. A label is config-blob content an untrusted image supplies, so
// every dimension is bounded before it is parsed or rendered.
const (
	MaxInterfaceLabelBytes = 64 << 10 // 64 KiB of label text
	MaxInterfaceOps        = 64       // entries of ops
	MaxInterfaceFields     = 256      // entries of any one fields/values map
	MaxInterfaceKeyBytes   = 128      // one with: key
)

// interfaceTypes is the complete type vocabulary, the one docs/DSL.md
// "Inputs" already has. A field's type is one of these words, or several
// joined by "|" for a key that accepts any of them (fieldTypes).
var interfaceTypes = map[string]struct{}{
	"string": {}, "number": {}, "boolean": {}, "object": {}, "array": {},
}

// interfaceKeyPattern bounds a with: key. It is looser than a slug because
// first-party images already read camelCase keys (timeoutSeconds,
// captureBodyAs) and a third-party image may read dotted ones.
var interfaceKeyPattern = regexp.MustCompile(`^[A-Za-z0-9_][A-Za-z0-9_.-]{0,127}$`)

// opNamePattern is docs/ACTIVITY_IMAGES.md rule 2: lowercase verbs.
var opNamePattern = regexp.MustCompile(`^[a-z][a-z0-9_-]{0,63}$`)

// opKey is the with: key the op convention selects an operation with.
const opKey = "op"

// ParseInterfaceLabel turns the label's text into a validated
// ActivityInterface. The error is the human-readable reason the manifest
// was rejected and becomes ActivityInfo.interface_error verbatim. Pure.
//
// A field's type is one of "string", "number", "boolean", "object", "array",
// or several joined by "|" (string|object|array) for a key that accepts any
// of them.
func ParseInterfaceLabel(raw string) (*orkstackv1.ActivityInterface, error) {
	if len(raw) > MaxInterfaceLabelBytes {
		return nil, fmt.Errorf("label is %d bytes, limit is %d", len(raw), MaxInterfaceLabelBytes)
	}
	iface := &orkstackv1.ActivityInterface{}
	// DiscardUnknown false: a word outside the vocabulary is refused, never
	// dropped — the same strict decoding the DSL applies to its documents.
	if err := (protojson.UnmarshalOptions{DiscardUnknown: false}).Unmarshal([]byte(raw), iface); err != nil {
		return nil, fmt.Errorf("label is not the JSON encoding of an ActivityInterface: %v", err)
	}
	if iface.GetSchemaVersion() != 1 {
		return nil, fmt.Errorf("schemaVersion %d is not supported (this engine reads 1)", iface.GetSchemaVersion())
	}
	if n := len(iface.GetOps()); n > MaxInterfaceOps {
		return nil, fmt.Errorf("declares %d ops, limit is %d", n, MaxInterfaceOps)
	}
	opNames := mapKeys(iface.GetOps())
	for _, op := range opNames {
		if !opNamePattern.MatchString(op) {
			return nil, fmt.Errorf("op %q is not a lowercase op name (%s)", op, opNamePattern.String())
		}
	}
	if err := checkFieldMap("fields", iface.GetFields()); err != nil {
		return nil, err
	}
	if err := checkFieldMap("values", iface.GetValues()); err != nil {
		return nil, err
	}
	for _, op := range opNames {
		if err := checkFieldMap("ops."+op+".fields", iface.GetOps()[op].GetFields()); err != nil {
			return nil, err
		}
	}
	if _, declared := iface.GetFields()[opKey]; declared && len(opNames) > 0 {
		return nil, errors.New("fields.op is declared explicitly but ops is non-empty; op is implied by ops")
	}
	if def := iface.GetDefaultOp(); def != "" {
		if len(opNames) == 0 {
			return nil, fmt.Errorf("defaultOp %q is set but ops is empty", def)
		}
		if _, ok := iface.GetOps()[def]; !ok {
			return nil, fmt.Errorf("defaultOp %q is not one of the declared ops (%s)", def, strings.Join(opNames, ", "))
		}
	}
	for _, op := range opNames {
		for _, key := range mapKeys(iface.GetOps()[op].GetFields()) {
			if _, shared := iface.GetFields()[key]; shared {
				return nil, fmt.Errorf("key %q is declared both in fields and under op %q; declare it once", key, op)
			}
		}
	}
	return iface, nil
}

// checkFieldMap validates one fields/values map; where names it in messages.
func checkFieldMap(where string, fields map[string]*orkstackv1.FieldSchema) error {
	if n := len(fields); n > MaxInterfaceFields {
		return fmt.Errorf("%s declares %d keys, limit is %d", where, n, MaxInterfaceFields)
	}
	for _, key := range mapKeys(fields) {
		if !interfaceKeyPattern.MatchString(key) {
			return fmt.Errorf("%s: key %q is not a valid with: key", where, key)
		}
		f := fields[key]
		seen := map[string]bool{}
		for _, word := range fieldTypes(f) {
			if _, ok := interfaceTypes[word]; !ok {
				return fmt.Errorf("%s.%s: type %q is not one of string, number, boolean, object, array, or several of them joined by |",
					where, key, f.GetType())
			}
			if seen[word] {
				return fmt.Errorf("%s.%s: type %q names %q twice", where, key, f.GetType(), word)
			}
			seen[word] = true
		}
		def := f.GetDefault()
		if f.GetRequired() && def != nil {
			return fmt.Errorf("%s.%s: required and default contradict; a defaulted key is never required", where, key)
		}
		if def == nil {
			continue
		}
		if _, isNull := def.GetKind().(*structpb.Value_NullValue); isNull {
			return fmt.Errorf("%s.%s: null is not a default; omit the field", where, key)
		}
		if got := valueTypeOf(def); !slices.Contains(fieldTypes(f), got) {
			return fmt.Errorf("%s.%s: default is a %s but type is %s", where, key, got, f.GetType())
		}
	}
	return nil
}

// fieldTypes is a field's type split into its words: one, or several joined
// by "|".
func fieldTypes(f *orkstackv1.FieldSchema) []string {
	return strings.Split(f.GetType(), "|")
}

// valueTypeOf is jsonTypeOf for a structpb.Value.
func valueTypeOf(v *structpb.Value) string {
	switch v.GetKind().(type) {
	case *structpb.Value_StringValue:
		return "string"
	case *structpb.Value_NumberValue:
		return "number"
	case *structpb.Value_BoolValue:
		return "boolean"
	case *structpb.Value_StructValue:
		return "object"
	case *structpb.Value_ListValue:
		return "array"
	default:
		return "null"
	}
}

// jsonTypeOf maps a structpb AsMap value to the five vocabulary words, or
// "null".
func jsonTypeOf(v any) string {
	switch v.(type) {
	case string:
		return "string"
	case float64:
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

func mapKeys[V any](m map[string]V) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

// isTemplated reports a string value the Orker renders at run time.
func isTemplated(s string) bool { return strings.Contains(s, "{{") }

// CheckOrkflowInterfaces walks wf.tasks in document order and checks each
// task's with: against the interface its resolved image declares. A task
// whose uses: has no entry in resolutions (no resolver, or a later
// proposal's `uses: orkflow:` task) produces nothing. Pure.
func CheckOrkflowInterfaces(wf *orkstackv1.Orkflow, resolutions Resolutions) []*orkstackv1.ValidationIssue {
	var issues []*orkstackv1.ValidationIssue
	for i, task := range wf.GetTasks() {
		res := resolutions[task.GetUses()]
		switch {
		case res == nil:
		case res.InterfaceError != "":
			issues = append(issues, &orkstackv1.ValidationIssue{
				Field: fmt.Sprintf("tasks[%d].uses", i),
				Message: fmt.Sprintf("%s carries an %s label that could not be read (%s); with: was not checked against it",
					task.GetImage().GetPinned(), InterfaceLabel, res.InterfaceError),
				Severity: orkstackv1.ValidationSeverity_VALIDATION_SEVERITY_WARNING,
			})
		case res.Interface == nil:
		default:
			issues = append(issues, CheckWith(i, task, res.Interface)...)
		}
	}
	return issues
}

// CheckWith checks ONE task's with: against ONE interface, anchoring every
// issue at tasks[taskIndex].with.<key>. Pure; deterministic order.
//
// A missing required key and an op the image does not declare are ERRORs:
// the image would refuse them at run time. An undeclared key and a literal of
// the wrong JSON type are WARNINGs, never errors — the image may accept more
// than it declares, and the DSL must never be stricter than the ABI.
func CheckWith(taskIndex int, task *orkstackv1.Task, iface *orkstackv1.ActivityInterface) []*orkstackv1.ValidationIssue {
	with := task.GetWith().AsMap()
	if with == nil {
		with = map[string]any{}
	}
	prefix := fmt.Sprintf("tasks[%d].with", taskIndex)
	pinned := task.GetImage().GetPinned()
	hasOps := len(iface.GetOps()) > 0
	opNames := mapKeys(iface.GetOps())
	declaredOps := strings.Join(opNames, ", ")

	var opIssues, required, unknown, typed []*orkstackv1.ValidationIssue
	issue := func(sev orkstackv1.ValidationSeverity, key, message string) *orkstackv1.ValidationIssue {
		return &orkstackv1.ValidationIssue{Field: prefix + "." + key, Message: message, Severity: sev}
	}
	errorAt := func(key, message string) *orkstackv1.ValidationIssue {
		return issue(orkstackv1.ValidationSeverity_VALIDATION_SEVERITY_ERROR, key, message)
	}
	warnAt := func(key, message string) *orkstackv1.ValidationIssue {
		return issue(orkstackv1.ValidationSeverity_VALIDATION_SEVERITY_WARNING, key, message)
	}

	// Step 1: op selection.
	op, opKnown := "", true
	if hasOps {
		value, present := with[opKey]
		text, isString := value.(string)
		switch {
		case !present && iface.GetDefaultOp() == "":
			opKnown = false
			opIssues = append(opIssues, errorAt(opKey, fmt.Sprintf(
				"with.op is required: %s selects its operation with op: and declares no default (declared ops: %s)",
				pinned, declaredOps)))
		case !present:
			op = iface.GetDefaultOp()
		case !isString:
			opKnown = false
			opIssues = append(opIssues, errorAt(opKey, fmt.Sprintf(
				"with.op must be a string naming one of %s's operations (declared ops: %s)", pinned, declaredOps)))
		case isTemplated(text):
			// The op is chosen at run time.
			opKnown = false
		default:
			if _, ok := iface.GetOps()[text]; !ok {
				opKnown = false
				opIssues = append(opIssues, errorAt(opKey, fmt.Sprintf(
					"with.op %q is not an operation of %s (declared ops: %s)", text, pinned, declaredOps)))
			} else {
				op = text
			}
		}
	}

	// Step 2: the declared set F, and U for the unknown-key check when the
	// op is not known — a key an unchosen op reads is not "undeclared".
	declared := map[string]*orkstackv1.FieldSchema{}
	for k, f := range iface.GetFields() {
		declared[k] = f
	}
	if opKnown && op != "" {
		for k, f := range iface.GetOps()[op].GetFields() {
			declared[k] = f
		}
	}
	if hasOps {
		declared[opKey] = &orkstackv1.FieldSchema{Type: "string"}
	}
	known := declared
	if hasOps && !opKnown {
		known = map[string]*orkstackv1.FieldSchema{}
		for k, f := range declared {
			known[k] = f
		}
		for _, name := range opNames {
			for k, f := range iface.GetOps()[name].GetFields() {
				known[k] = f
			}
		}
	}
	suffix := ""
	if hasOps && opKnown {
		suffix = fmt.Sprintf(" for op %q", op)
	}

	// Step 3: required keys. A JSON null is absent: the first-party images
	// read `.key // empty`.
	for _, k := range mapKeys(declared) {
		if !declared[k].GetRequired() {
			continue
		}
		if v, present := with[k]; !present || v == nil {
			required = append(required, errorAt(k, fmt.Sprintf(
				"with.%s is required by the interface of %s%s and is not supplied", k, pinned, suffix)))
		}
	}

	// Step 4: undeclared keys.
	knownList := strings.Join(mapKeys(known), ", ")
	for _, k := range mapKeys(with) {
		if _, ok := known[k]; ok {
			continue
		}
		unknown = append(unknown, warnAt(k, fmt.Sprintf(
			"with.%s is not declared by the interface of %s%s (declared: %s); the image may still accept it — "+
				"check for a typo or a key renamed in a newer image", k, pinned, suffix, knownList)))
	}

	// Step 5: literal types. A templated string is never typed: the Orker
	// renders every template to a string. `op` is step 1's to report.
	for _, k := range mapKeys(with) {
		f, ok := declared[k]
		if !ok || (hasOps && k == opKey) {
			continue
		}
		v := with[k]
		if v == nil {
			continue
		}
		if s, isString := v.(string); isString && isTemplated(s) {
			continue
		}
		if got := jsonTypeOf(v); !slices.Contains(fieldTypes(f), got) {
			typed = append(typed, warnAt(k, fmt.Sprintf(
				"with.%s is declared %s by the interface of %s; this document supplies a %s — "+
					"the image may coerce it, or refuse it at run time", k, f.GetType(), pinned, got)))
		}
	}

	out := make([]*orkstackv1.ValidationIssue, 0, len(opIssues)+len(required)+len(unknown)+len(typed))
	out = append(out, opIssues...)
	out = append(out, required...)
	out = append(out, unknown...)
	return append(out, typed...)
}
