package dsl

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"

	"google.golang.org/protobuf/types/known/structpb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
)

// The canonical form of an Orkflow, and the two digests derived from it (§4.4).
//
// WHY THIS IS NOT proto.MarshalOptions{Deterministic: true}. The upstream doc
// comment disqualifies that option for exactly this use, verbatim: "It is not
// guaranteed to remain stable over time… Users who need canonical
// serialization (e.g., persistent storage in a canonical form, fingerprinting,
// etc.) must define their own canonicalization specification and implement
// their own serializer rather than relying on this API." A definition digest
// is stamped into an immutable document and compared across engine releases,
// which is that use precisely. So the serializer is OrkStack's own.
//
// THE SPECIFICATION, from §4.4:
//
//	JSON, UTF-8, no insignificant whitespace, object keys sorted by their
//	UTF-8 byte sequence, integers rendered without exponent or fraction, and
//	ONLY the fields enumerated below, emitted whether or not they are zero.
//
// The enumeration is a whitelist and that is the point: adding a proto field
// cannot silently change an existing digest, because a field this file does
// not name is not serialized. A field that SHOULD change the digest has to be
// added here on purpose, and the golden vector beside this file fails when one
// is.

// canonicalFieldOrder is not a variable anywhere in this file. Keys are sorted
// by byte sequence at emit time, so the order the code writes them in cannot
// affect the output — which removes a whole class of "I reordered the struct
// and the digest moved" bug.

// CanonicalInputJSON renders a run input (or any Struct payload) for a
// human-readable, deterministic line diff: the §4.4 canonical value tree —
// keys sorted by byte sequence, integers without fraction or exponent, no
// HTML escaping — indented two spaces, one key per line, no trailing newline.
// A nil struct renders as "{}". It exists so DiffRuns can hand dsl.Unified
// two texts in which the same value always spells the same way, and a
// reordered or 5.0-vs-5 input never shows as a change.
func CanonicalInputJSON(s *structpb.Struct) (string, error) {
	// canonicalStruct returns (nil, nil) for a nil *Struct, which json would
	// render as `null`; an absent input is the empty object.
	if s == nil {
		return "{}", nil
	}
	tree, err := canonicalStruct(s)
	if err != nil {
		return "", err
	}
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	enc.SetIndent("", "  ")
	if err := enc.Encode(tree); err != nil {
		return "", err
	}
	return strings.TrimSuffix(buf.String(), "\n"), nil
}

// CanonicalInputLine renders a run input (or any Struct payload) as ONE line
// of compact canonical JSON for a human-readable header: the §4.4 canonical
// value tree — keys sorted by byte sequence, integers without fraction or
// exponent, no HTML escaping — with no whitespace and no trailing newline.
// A nil struct renders as "{}". It is the single-line sibling of
// CanonicalInputJSON, which indents one key per line for a line diff; the
// two are not interchangeable.
func CanonicalInputLine(s *structpb.Struct) (string, error) {
	// canonicalStruct returns (nil, nil) for a nil *Struct, which canonicalJSON
	// would render as `null`; an absent input is the empty object.
	if s == nil {
		return "{}", nil
	}
	tree, err := canonicalStruct(s)
	if err != nil {
		return "", err
	}
	out, err := canonicalJSON(tree)
	if err != nil {
		return "", err
	}
	return string(out), nil
}

// CanonicalOrkflow renders wf in the canonical form of §4.4.
func CanonicalOrkflow(wf *orkstackv1.Orkflow) ([]byte, error) {
	tasks := make([]any, 0, len(wf.GetTasks()))
	for _, t := range wf.GetTasks() {
		task, err := canonicalTask(t)
		if err != nil {
			return nil, fmt.Errorf("task %q: %w", t.GetId(), err)
		}
		tasks = append(tasks, task)
	}
	inputs, err := canonicalStruct(wf.GetInputs())
	if err != nil {
		return nil, fmt.Errorf("inputs: %w", err)
	}
	// NOT source_yaml and NOT created_at (§4.4). The source is not part of
	// what runs, and the stamp is per-apply — including either would make a
	// re-apply of an unchanged document mint a different digest.
	out := map[string]any{
		"namespace":   wf.GetNamespace(),
		"name":        wf.GetName(),
		"version":     wf.GetVersion(),
		"description": wf.GetDescription(),
		"labels":      canonicalStringMap(wf.GetLabels()),
		"inputs":      inputs,
		"tasks":       tasks,
	}
	// pins is emitted ONLY when non-empty — see canonicalPins for why this one
	// key breaks the "whether or not zero" rule the rest of this file follows.
	if pins := canonicalPins(wf.GetPins()); pins != nil {
		out["pins"] = pins
	}
	// outputs is emitted ONLY when non-empty — the canonicalPins exception,
	// for the same reason: an Orkflow that declares none keeps its digest.
	if len(wf.GetOutputs()) > 0 {
		out["outputs"] = canonicalStringMap(wf.GetOutputs())
	}
	return canonicalJSON(out)
}

// canonicalPins renders Orkflow.pins, sorted by name, and returns nil when
// there are none.
//
// THE DOCUMENTED EXCEPTION TO "EMITTED WHETHER OR NOT ZERO". Every other key in
// this file appears in the canonical form regardless of its value, which is
// what makes the whitelist auditable. `pins` — and `pin` on the task, below —
// do not: an orkflow that declares no pin gets no `pins` key at all, and a task
// that names no pin gets no `pin` key at all.
//
// The reason is that the digest is STAMPED INTO the document it describes.
// Emitting `"pins": []` and `"pin": ""` unconditionally would move the digest
// of EVERY definition already stored, and the move is not cosmetic:
// sameStoredOrkflow (pkg/artifacts/orkflow_store.go) decides "this apply is a
// repeat of an unchanged document" with proto.Equal after clearing only
// created_at. A re-apply of an unchanged, source-hash-versioned document would
// then carry a moved definition_digest, compare unequal, and be refused as an
// attempt to rewrite an immutable version — the author would get
// `already_exists` for a document they did not change. Making the two new keys
// conditional keeps every unpinned definition's digest exactly where it is,
// which the golden vector in canonical_test.go proves, and still hashes the
// fields for every definition that actually uses them.
//
// docs/OCI_ACTIVITY_CONTRACT.md §4.4 records this exception to "emitted
// whether or not zero".
func canonicalPins(pins []*orkstackv1.Pin) []any {
	if len(pins) == 0 {
		return nil
	}
	sorted := make([]*orkstackv1.Pin, len(pins))
	copy(sorted, pins)
	sort.SliceStable(sorted, func(i, j int) bool { return sorted[i].GetName() < sorted[j].GetName() })
	out := make([]any, 0, len(sorted))
	for _, p := range sorted {
		out = append(out, map[string]any{
			"name":           p.GetName(),
			"timeoutSeconds": int64(p.GetTimeoutSeconds()),
		})
	}
	return out
}

// canonicalTask renders the enumerated Task fields of §4.4.
func canonicalTask(t *orkstackv1.Task) (map[string]any, error) {
	with, err := canonicalStruct(t.GetWith())
	if err != nil {
		return nil, fmt.Errorf("with: %w", err)
	}
	secrets := make([]any, 0, len(t.GetSecrets()))
	for _, s := range t.GetSecrets() {
		secrets = append(secrets, map[string]any{
			"name":   s.GetName(),
			"source": s.GetSource(),
		})
	}
	var retry any
	if r := t.GetRetry(); r != nil {
		retry = map[string]any{
			"maximumAttempts":        int64(r.GetMaximumAttempts()),
			"initialIntervalSeconds": int64(r.GetInitialIntervalSeconds()),
			"backoffCoefficient":     canonicalNumber(r.GetBackoffCoefficient()),
			"maximumIntervalSeconds": int64(r.GetMaximumIntervalSeconds()),
		}
	}
	var resources any
	if r := t.GetResources(); r != nil {
		resources = map[string]any{"cpu": r.GetCpu(), "memory": r.GetMemory()}
	}
	out := map[string]any{
		"id":                             t.GetId(),
		"uses":                           t.GetUses(),
		"image":                          canonicalImageRef(t.GetImage()),
		"with":                           with,
		"needs":                          canonicalStrings(t.GetNeeds()),
		"condition":                      t.GetCondition(),
		"timeoutSeconds":                 int64(t.GetTimeoutSeconds()),
		"retry":                          retry,
		"pool":                           t.GetPool(),
		"command":                        canonicalStrings(t.GetCommand()),
		"env":                            canonicalStringMap(t.GetEnv()),
		"secrets":                        secrets,
		"resources":                      resources,
		"network":                        int64(t.GetNetwork()),
		"cancellationGracePeriodSeconds": int64(t.GetCancellationGracePeriodSeconds()),
		"runAsOrkerUser":                 t.GetRunAsOrkerUser(),
	}
	// pin is emitted ONLY when set — the same exception, for the same reason,
	// as `pins` on the orkflow (canonicalPins).
	if t.GetPin() != "" {
		out["pin"] = t.GetPin()
	}
	// join is emitted ONLY when it is not UNSPECIFIED (all_succeeded) — the
	// same exception, for the same reason, as `pin`: a definition with no
	// join keeps its digest.
	if t.GetJoin() != orkstackv1.JoinPolicy_JOIN_POLICY_UNSPECIFIED {
		out["join"] = int64(t.GetJoin())
	}
	// await is emitted ONLY when set — the same exception, for the same
	// reason, as `pin`: a document with no await keeps the digest it had.
	if a := t.GetAwait(); a != nil {
		schema, err := canonicalStruct(a.GetSchema())
		if err != nil {
			return nil, fmt.Errorf("await.schema: %w", err)
		}
		out["await"] = map[string]any{
			"timeoutSeconds": int64(a.GetTimeoutSeconds()),
			"callback":       a.GetCallback(),
			"schema":         schema,
		}
	}
	// orkflow is emitted ONLY when set — the same exception, for the same
	// reason, as `pin`. The digest is included: it is what runs, stamped at
	// apply exactly as `image` is, so the parent's digest pins the child's
	// and, through the child's own canonical form, every descendant's.
	if o := canonicalOrkflowRef(t.GetOrkflow()); o != nil {
		out["orkflow"] = o
	}
	// each is emitted ONLY when set — the same exception, for the same
	// reason, as `pin`. source is included: it is what runs, compiled at
	// apply exactly as `image` is, so a build that compiled `over` differently
	// would produce a different definition and must produce a different digest.
	if e := t.GetEach(); e != nil {
		src := e.GetSource()
		out["each"] = map[string]any{
			"over":        e.GetOver(),
			"as":          e.GetAs(),
			"maxItems":    int64(e.GetMaxItems()),
			"maxParallel": int64(e.GetMaxParallel()),
			"source": map[string]any{
				"input":  src.GetInput(),
				"taskId": src.GetTaskId(),
				"path":   canonicalStrings(src.GetPath()),
			},
		}
	}
	// cache is emitted ONLY when set — the same exception, for the same
	// reason, as `pin`. It is part of the definition: a version that consults
	// memos is not the version that does not. It is NOT part of any memo key
	// (docs/DSL.md "What the key covers"): changing a TTL invalidates nothing.
	if c := t.GetCache(); c != nil {
		out["cache"] = map[string]any{
			"ttlSeconds": int64(c.GetTtlSeconds()),
			"scope":      int64(c.GetScope()),
		}
	}
	return out, nil
}

// canonicalImageRef renders the seven enumerated ImageRef fields of §4.4.
//
// platform_digest, platform and delivery are deliberately ABSENT. They are
// per-execution facts an Orker records after a pull — a definition has none of
// them, and hashing a field that is always empty in the thing being hashed is
// how a digest acquires a dependency on a future value. resolved_at does not
// exist at all (§2.2 reserves it): a per-apply timestamp inside Task.image
// would make every re-apply of an unchanged document unequal, and
// pkg/artifacts.sameStoredOrkflow would refuse it with ErrVersionExists.
//
// platforms is emitted in the INDEX'S OWN ORDER, not sorted (§4.3 step 4).
// The digest hashes it, so the order is part of the contract; index order is
// what the registry returned and needs no second rule to agree on.
func canonicalImageRef(image *orkstackv1.ImageRef) any {
	if image == nil {
		return nil
	}
	return map[string]any{
		"reference":   image.GetReference(),
		"registry":    image.GetRegistry(),
		"repository":  image.GetRepository(),
		"indexDigest": image.GetIndexDigest(),
		"pinned":      image.GetPinned(),
		"mediaType":   image.GetMediaType(),
		"platforms":   canonicalStrings(image.GetPlatforms()),
	}
}

// DefinitionDigest is the canonical digest of what a version runs (§4.4).
//
//	orkflow_definition_digest = "sha256:" + hex(sha256(canonicalOrkflow(wf)))
func DefinitionDigest(wf *orkstackv1.Orkflow) (string, error) {
	canonical, err := CanonicalOrkflow(wf)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(canonical)
	return "sha256:" + hex.EncodeToString(sum[:]), nil
}

// canonicalImages is the canonical JSON of [{taskId, image}] in TASK
// DECLARATION ORDER (§4.4). Declaration order, not sorted: the tasks are a
// sequence in the document, and reordering them is an edit.
func canonicalImages(wf *orkstackv1.Orkflow) ([]byte, error) {
	images := make([]any, 0, len(wf.GetTasks()))
	for _, t := range wf.GetTasks() {
		entry := map[string]any{
			"taskId": t.GetId(),
			"image":  canonicalImageRef(t.GetImage()),
		}
		// Only for a composed task, so a document with none derives the
		// version it always did, and one derived version still names one
		// definition when a child version was deleted and re-applied with
		// other content.
		if o := canonicalOrkflowRef(t.GetOrkflow()); o != nil {
			entry["orkflow"] = o
		}
		images = append(images, entry)
	}
	return canonicalJSON(images)
}

// canonicalOrkflowRef renders a composed task's reference, or nil when the
// task composes nothing.
func canonicalOrkflowRef(o *orkstackv1.OrkflowRef) map[string]any {
	if o == nil {
		return nil
	}
	return map[string]any{
		"name":             o.GetName(),
		"version":          o.GetVersion(),
		"definitionDigest": o.GetDefinitionDigest(),
	}
}

// derivedVersionSeparator is the 0x1F unit separator of §4.4. It cannot occur
// in canonical JSON (JSON escapes control characters) and is vanishingly
// unlikely in YAML source, so the concatenation is unambiguous.
const derivedVersionSeparator = 0x1F

// DerivedVersion is the version of a document that pins none (§4.4):
//
//	hex(sha256(sourceYAML ‖ 0x1F ‖ canonicalImages(wf)))
//
// It is assigned AFTER image resolution, and this is the whole point. Compile
// stamps a provisional sourceHash, and the applier replaces it here once the
// images are resolved: a version minted before resolution would name the SAME
// string for a DIFFERENT definition whenever a tag moved, and
// pkg/artifacts.orkflow_store would then refuse the apply as an attempt to
// rewrite an immutable version. The author would get a refusal instead of a
// new version.
//
// This does NOT re-open the question hash.go settles — "two notions of 'the
// same document' in one system is one too many". The source bytes are hashed
// in FULL here, so a comment-only edit mints a new version. The resolution is
// not a second notion of the document but a second input to what runs.
func DerivedVersion(sourceYAML []byte, wf *orkstackv1.Orkflow) (string, error) {
	images, err := canonicalImages(wf)
	if err != nil {
		return "", err
	}
	h := sha256.New()
	h.Write(sourceYAML)
	h.Write([]byte{derivedVersionSeparator})
	h.Write(images)
	return hex.EncodeToString(h.Sum(nil)), nil
}

// canonicalJSON marshals v with no insignificant whitespace and object keys
// sorted by UTF-8 byte sequence.
//
// encoding/json already does both for map[string]any: it emits no whitespace,
// and it sorts map keys with a plain string comparison, which in Go IS a
// byte-sequence comparison. What it does NOT do safely is numbers, so every
// number reaching this function has already been converted to an int64 or a
// json.Number by canonicalNumber. HTML escaping is turned off because it would
// rewrite `<`, `>` and `&` inside a `with:` string into \u sequences, making
// the digest depend on an encoder default rather than on the document.
func canonicalJSON(v any) ([]byte, error) {
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(v); err != nil {
		return nil, err
	}
	// Encode appends a newline; the canonical form has no trailing byte.
	return bytes.TrimSuffix(buf.Bytes(), []byte("\n")), nil
}

// canonicalNumber renders a float64 without exponent or fraction where it is
// an integer, and without an exponent at all otherwise (§4.4).
//
// This is the one place Go's default number formatting cannot be trusted.
// encoding/json renders a float64 with %g-like rules, so 1e21 marshals as
// "1e+21" and 5.0 marshals as "5" — the first violates "no exponent" outright,
// and the pair means the SAME numeric value can print two ways depending on
// magnitude. structpb holds every JSON number as a float64, so `with: {port:
// 8080}` arrives here as 8080.0 and must render as 8080.
func canonicalNumber(f float64) any {
	if math.IsNaN(f) || math.IsInf(f, 0) {
		// Not representable in JSON at all. Render the IEEE bits so the
		// digest is still a function of the value rather than failing to be
		// one; a document cannot actually carry these — YAML .nan/.inf do not
		// survive structpb.NewStruct.
		return json.Number(strconv.FormatUint(math.Float64bits(f), 10))
	}
	if f == math.Trunc(f) && math.Abs(f) < 1<<53 {
		return int64(f)
	}
	// 'f' format never uses an exponent; -1 precision is the shortest
	// round-trippable rendering, so the value survives a decode unchanged.
	return json.Number(strconv.FormatFloat(f, 'f', -1, 64))
}

// canonicalStruct converts a structpb.Struct to the plain Go shape
// canonicalJSON can render deterministically.
func canonicalStruct(s *structpb.Struct) (any, error) {
	if s == nil {
		return nil, nil
	}
	return canonicalValue(structpb.NewStructValue(s))
}

func canonicalValue(v *structpb.Value) (any, error) {
	switch kind := v.GetKind().(type) {
	case *structpb.Value_NullValue, nil:
		return nil, nil
	case *structpb.Value_NumberValue:
		return canonicalNumber(kind.NumberValue), nil
	case *structpb.Value_StringValue:
		return kind.StringValue, nil
	case *structpb.Value_BoolValue:
		return kind.BoolValue, nil
	case *structpb.Value_StructValue:
		out := make(map[string]any, len(kind.StructValue.GetFields()))
		for key, field := range kind.StructValue.GetFields() {
			converted, err := canonicalValue(field)
			if err != nil {
				return nil, err
			}
			out[key] = converted
		}
		return out, nil
	case *structpb.Value_ListValue:
		values := kind.ListValue.GetValues()
		out := make([]any, 0, len(values))
		for _, item := range values {
			converted, err := canonicalValue(item)
			if err != nil {
				return nil, err
			}
			out = append(out, converted)
		}
		return out, nil
	default:
		return nil, fmt.Errorf("unrepresentable value %T", kind)
	}
}

// canonicalStrings renders a repeated string field, preserving order. A nil
// slice and an empty slice both render as [], because §4.4 emits an
// enumerated field "whether or not it is zero" and JSON has no way to say
// "absent" for a value that is present in the schema.
func canonicalStrings(in []string) []any {
	out := make([]any, 0, len(in))
	for _, s := range in {
		out = append(out, s)
	}
	return out
}

// canonicalStringMap renders a map field. The keys are sorted by byte sequence
// at emit time; this exists only to give encoding/json a map[string]any it
// will sort, and to make nil and empty render identically.
func canonicalStringMap(in map[string]string) map[string]any {
	out := make(map[string]any, len(in))
	keys := make([]string, 0, len(in))
	for k := range in {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		out[k] = in[k]
	}
	return out
}
