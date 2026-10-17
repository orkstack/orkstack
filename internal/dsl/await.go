package dsl

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"slices"
	"sort"
	"strings"
	"unicode/utf8"

	"google.golang.org/protobuf/types/known/structpb"
	"gopkg.in/yaml.v3"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
)

// MaxAwaitTimeoutSeconds bounds await.timeout_seconds: 30 days. An awaiting
// run is a live execution pinned to the release that started it (docs/DEPLOY.md
// §2, "Why RunWorkflow is Pinned"), so the deadline is also the longest an
// await can keep that release's workers required. A longer business process
// is two runs.
const MaxAwaitTimeoutSeconds = 30 * 24 * 60 * 60 // 2592000

// awaitDocument is one awaited task's declaration. KnownFields(true) on the
// decoder applies to it, so a misspelled key inside `await:` is refused.
type awaitDocument struct {
	// TimeoutSeconds is required. A pointer so an absent key is refused by
	// name rather than read as 0.
	TimeoutSeconds *int32 `yaml:"timeout_seconds"`
	// Callback asks SubmitRun to mint a one-time callback token for this
	// await and disclose it in the submission response.
	Callback bool `yaml:"callback"`
	// Schema declares the values a completion must carry, in the vocabulary
	// top-level `inputs:` uses. Optional.
	// A data position: a yaml.Node value, like document.Inputs.
	Schema yaml.Node `yaml:"schema"`
}

// awaitExecutionKeys lists, in the order they are checked, the YAML keys an
// awaited task may not carry, each with whether the author wrote it. It is the
// YAML-layer half of rule 15: the compiled message is checked again by
// validate, which is what a hand-built request meets.
func awaitExecutionKeys(td taskDocument) []struct {
	key     string
	written bool
} {
	return []struct {
		key     string
		written bool
	}{
		{"uses", td.Uses != ""},
		{"with", present(td.With)},
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
		{"cache", td.Cache != nil},
	}
}

// awaitFieldRefusal is rule 15's sentence for one key.
func awaitFieldRefusal(taskID, key string) error {
	reason := "it runs no container"
	switch key {
	case "each":
		reason = "an await is one resolution, not N"
	case "cache":
		reason = "its value comes from whoever resolves it, not from a computation"
	}
	msg := fmt.Sprintf("task %q: an awaited task takes only id, needs, condition, join and await; "+
		"%q does not apply (%s)", taskID, key, reason)
	if key == "timeout_seconds" {
		msg += " — the deadline is await.timeout_seconds"
	}
	return fmt.Errorf("%s", msg)
}

// awaitUsesRefusal is rule 6's sentence for a task that declares both.
func awaitUsesRefusal(taskID string) error {
	return fmt.Errorf("task %q: uses: and await: are exclusive — an awaited task runs no image", taskID)
}

// compileAwaitTask builds the Task for a document that declares `await:`,
// after refusing every execution key the author wrote.
func compileAwaitTask(taskIndex int, td taskDocument) (*orkstackv1.Task, error) {
	for _, field := range awaitExecutionKeys(td) {
		if field.written && field.key == "uses" {
			return nil, validationError(fmt.Sprintf("tasks[%d].uses", taskIndex), awaitUsesRefusal(td.ID))
		}
		if field.written {
			return nil, validationError(fmt.Sprintf("tasks[%d].%s", taskIndex, field.key),
				awaitFieldRefusal(td.ID, field.key))
		}
	}
	if td.Await.TimeoutSeconds == nil {
		return nil, validationError(fmt.Sprintf("tasks[%d].await.timeout_seconds", taskIndex),
			fmt.Errorf("task %q: await.timeout_seconds is required — an await with no deadline could wait forever", td.ID))
	}
	spec := &orkstackv1.AwaitSpec{
		TimeoutSeconds: *td.Await.TimeoutSeconds,
		Callback:       td.Await.Callback,
	}
	schema, err := freeForm("await.schema", fmt.Sprintf("tasks[%d].await.schema", taskIndex), &td.Await.Schema)
	if err != nil {
		return nil, err
	}
	spec.Schema = schema
	join, err := compileJoin(taskIndex, td)
	if err != nil {
		return nil, err
	}
	return &orkstackv1.Task{
		Id:        td.ID,
		Needs:     td.Needs,
		Condition: td.Condition,
		Join:      join,
		// 0, not DefaultTimeoutSeconds: an await's deadline is its own.
		TimeoutSeconds: 0,
		Await:          spec,
	}, nil
}

// validateAwaitTask enforces rules 15, 16 and 17 on one compiled awaited
// task. Rule 6's exclusivity of uses: and await: is checked by the caller,
// before this.
func validateAwaitTask(taskIndex int, task *orkstackv1.Task) error {
	id := task.GetId()
	at := func(field string) string { return fmt.Sprintf("tasks[%d].%s", taskIndex, field) }
	// Rule 15: nothing but id, needs, condition, join and await.
	checks := []struct {
		field string
		set   bool
	}{
		{"with", task.GetWith() != nil},
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
		{"image", task.GetImage() != nil},
		{"each", task.GetEach() != nil},
		{"cache", task.GetCache() != nil},
	}
	for _, c := range checks {
		if c.set {
			return validationError(at(c.field), awaitFieldRefusal(id, c.field))
		}
	}
	// Rule 16: a bounded deadline.
	spec := task.GetAwait()
	if t := spec.GetTimeoutSeconds(); t < 1 || t > MaxAwaitTimeoutSeconds {
		return validationError(at("await.timeout_seconds"), fmt.Errorf(
			"task %q: await.timeout_seconds %d is out of range (1 s … 30 days)", id, t))
	}
	// Rule 17: the schema is the inputs vocabulary, with no default.
	fields := spec.GetSchema().GetFields()
	names := make([]string, 0, len(fields))
	for name := range fields {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		field := at("await.schema." + name)
		entry, ok := fields[name].GetKind().(*structpb.Value_StructValue)
		if !ok {
			return validationError(field, fmt.Errorf(
				"task %q: await.schema.%s must be a mapping with a `type` (one of %s)",
				id, name, strings.Join(typeWords, ", ")))
		}
		decl := entry.StructValue.GetFields()
		keys := make([]string, 0, len(decl))
		for key := range decl {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		for _, key := range keys {
			switch key {
			case "type", "required":
			case "default":
				return validationError(field, fmt.Errorf(
					"task %q: await.schema.%s: an await's schema declares no default — a "+
						"resolution's values.json is stored as the caller's exact bytes, and a "+
						"default would make the stored object differ from what was sent", id, name))
			default:
				return validationError(field, fmt.Errorf(
					"task %q: await.schema.%s: unknown key %q — an entry declares only `type` and `required`",
					id, name, key))
			}
		}
		typ, ok := decl["type"].GetKind().(*structpb.Value_StringValue)
		if !ok || !slices.Contains(typeWords, typ.StringValue) {
			return validationError(field, fmt.Errorf(
				"task %q: await.schema.%s: `type` is required and must be one of %s",
				id, name, strings.Join(typeWords, ", ")))
		}
		if required, present := decl["required"]; present {
			if _, ok := required.GetKind().(*structpb.Value_BoolValue); !ok {
				return validationError(field, fmt.Errorf(
					"task %q: await.schema.%s: `required` must be true or false", id, name))
			}
		}
	}
	return nil
}

// AwaitValuesError is a resolution body refused against its await. Field is
// a path the caller can act on — "" for the body as a whole, "values.<key>"
// for one key — and Message says what to change.
type AwaitValuesError struct {
	Field   string
	Message string
}

func (e *AwaitValuesError) Error() string {
	if e.Field == "" {
		return "values: " + e.Message
	}
	return e.Field + ": " + e.Message
}

// ValidateAwaitValues checks one completion body before anything is stored
// or delivered; the doors call it and the workflow never does. The rules,
// in order, first failure wins:
//
//	len(raw) > maxBytes                → "", "the values are N bytes, over the M-byte limit"
//	!utf8.Valid(raw)                   → "", "the values are not valid UTF-8"
//	!json.Valid(raw)                   → "", "the values are not one valid JSON value"
//	top level is not an object         → "", "the values must be a JSON object, not <kind>"
//	for each schema entry, sorted by name:
//	  required and absent              → "values.<k>", "is required by the await's schema"
//	  present and null                 → "values.<k>", "must be a <type>, not null"
//	  present and of another JSON type → "values.<k>", "must be a <type>, not <kind>"
//
// Decoding uses json.Decoder.UseNumber, so a number is type-checked without
// being rounded; raw itself is never re-serialized. Keys the schema does not
// declare are accepted — the schema says what downstream relies on, not
// everything a caller may send. A nil spec or empty schema checks only the
// first four rules.
func ValidateAwaitValues(spec *orkstackv1.AwaitSpec, raw []byte, maxBytes int64) error {
	if maxBytes > 0 && int64(len(raw)) > maxBytes {
		return &AwaitValuesError{Message: fmt.Sprintf("the values are %d bytes, over the %d-byte limit", len(raw), maxBytes)}
	}
	if !utf8.Valid(raw) {
		return &AwaitValuesError{Message: "the values are not valid UTF-8"}
	}
	if !json.Valid(raw) {
		return &AwaitValuesError{Message: "the values are not one valid JSON value"}
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	var top any
	if err := decoder.Decode(&top); err != nil {
		return &AwaitValuesError{Message: "the values are not one valid JSON value"}
	}
	if _, err := decoder.Token(); err != io.EOF {
		return &AwaitValuesError{Message: "the values are not one valid JSON value"}
	}
	object, ok := top.(map[string]any)
	if !ok {
		return &AwaitValuesError{Message: "the values must be a JSON object, not " + jsonValueKind(top)}
	}
	fields := spec.GetSchema().GetFields()
	names := make([]string, 0, len(fields))
	for name := range fields {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		decl := fields[name].GetStructValue().GetFields()
		want := decl["type"].GetStringValue()
		required := decl["required"].GetBoolValue()
		value, present := object[name]
		if !present {
			if required {
				return &AwaitValuesError{Field: "values." + name, Message: "is required by the await's schema"}
			}
			continue
		}
		if !jsonValueIs(value, want) {
			return &AwaitValuesError{Field: "values." + name,
				Message: fmt.Sprintf("must be %s, not %s", withArticle(want), jsonValueKind(value))}
		}
	}
	return nil
}

func jsonValueIs(value any, want string) bool {
	switch want {
	case "string":
		_, ok := value.(string)
		return ok
	case "number":
		_, ok := value.(json.Number)
		return ok
	case "boolean":
		_, ok := value.(bool)
		return ok
	case "object":
		_, ok := value.(map[string]any)
		return ok
	case "array":
		_, ok := value.([]any)
		return ok
	}
	return false
}

// jsonValueKind names a decoded JSON value's kind the way the Orker's
// values.json check names it.
func jsonValueKind(value any) string {
	switch value.(type) {
	case nil:
		return "null"
	case bool:
		return "a boolean"
	case json.Number, float64:
		return "a number"
	case string:
		return "a string"
	case []any:
		return "an array"
	case map[string]any:
		return "an object"
	default:
		return "an unexpected value"
	}
}

func withArticle(typ string) string {
	switch typ {
	case "object", "array":
		return "an " + typ
	}
	return "a " + typ
}
