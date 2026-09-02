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
	return canonicalJSON(map[string]any{
		"namespace":   wf.GetNamespace(),
		"name":        wf.GetName(),
		"version":     wf.GetVersion(),
		"description": wf.GetDescription(),
		"labels":      canonicalStringMap(wf.GetLabels()),
		"inputs":      inputs,
		"tasks":       tasks,
	})
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
	return map[string]any{
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
	}, nil
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
		images = append(images, map[string]any{
			"taskId": t.GetId(),
			"image":  canonicalImageRef(t.GetImage()),
		})
	}
	return canonicalJSON(images)
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
