package dsl

import (
	"fmt"
	"os"
	"regexp"
	"sort"

	"google.golang.org/protobuf/types/known/structpb"
	"gopkg.in/yaml.v3"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
)

// DefaultTimeoutSeconds is applied when a task omits timeout_seconds.
const DefaultTimeoutSeconds = 300

// slugPattern is the identifier rule for workflow names and task ids
// (docs/DSL.md validation rules 1 and 2).
var slugPattern = regexp.MustCompile(`^[a-z][a-z0-9-]*$`)

// KnownPlugins is the default set of plugin names accepted by rule 5. There is
// no runtime plugin registry in M2, so the compiler validates against the
// documented builtins (docs/PLUGINS.md).
var KnownPlugins = []string{"file", "http", "shell"}

// Options tunes compilation.
type Options struct {
	// KnownPlugins overrides the accepted `plugin:` names. Empty means
	// KnownPlugins.
	KnownPlugins []string
}

// document is the YAML shape of a *.ork.yaml file.
type document struct {
	Name        string            `yaml:"name"`
	Version     string            `yaml:"version"`
	Description string            `yaml:"description"`
	Labels      map[string]string `yaml:"labels"`
	Inputs      map[string]any    `yaml:"inputs"`
	Tasks       []taskDocument    `yaml:"tasks"`
}

type taskDocument struct {
	ID             string         `yaml:"id"`
	Plugin         string         `yaml:"plugin"`
	Needs          []string       `yaml:"needs"`
	Condition      string         `yaml:"condition"`
	TimeoutSeconds *int32         `yaml:"timeout_seconds"`
	Retry          *retryDocument `yaml:"retry"`
	Queue          string         `yaml:"queue"`
	With           map[string]any `yaml:"with"`
}

type retryDocument struct {
	MaximumAttempts        int32   `yaml:"maximum_attempts"`
	InitialIntervalSeconds int32   `yaml:"initial_interval_seconds"`
	BackoffCoefficient     float64 `yaml:"backoff_coefficient"`
	MaximumIntervalSeconds int32   `yaml:"maximum_interval_seconds"`
}

// CompileFile reads path and compiles it into an orkstack.v1.Orkflow.
func CompileFile(path string) (*orkstackv1.Orkflow, error) {
	source, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("dsl: read %s: %w", path, err)
	}
	wf, err := Compile(source)
	if err != nil {
		return nil, fmt.Errorf("dsl: %s: %w", path, err)
	}
	return wf, nil
}

// Compile parses, validates, and compiles a DSL document. The returned
// Orkflow carries the original source bytes in SourceYaml; when the document
// does not pin `version`, Version is a stable content hash of the document.
func Compile(source []byte) (*orkstackv1.Orkflow, error) {
	return CompileWithOptions(source, Options{})
}

// CompileWithOptions is Compile with explicit options.
func CompileWithOptions(source []byte, opts Options) (*orkstackv1.Orkflow, error) {
	var doc document
	if err := yaml.Unmarshal(source, &doc); err != nil {
		return nil, fmt.Errorf("parse yaml: %w", err)
	}

	wf := &orkstackv1.Orkflow{
		Name:        doc.Name,
		Version:     doc.Version,
		Description: doc.Description,
		Labels:      doc.Labels,
		SourceYaml:  string(source),
	}

	if doc.Inputs != nil {
		inputs, err := structpb.NewStruct(normalizeMap(doc.Inputs))
		if err != nil {
			return nil, fmt.Errorf("inputs: %w", err)
		}
		wf.Inputs = inputs
	}

	for _, td := range doc.Tasks {
		task := &orkstackv1.Task{
			Id:             td.ID,
			Plugin:         td.Plugin,
			Needs:          td.Needs,
			Condition:      td.Condition,
			TimeoutSeconds: DefaultTimeoutSeconds,
			Queue:          td.Queue,
		}
		if td.TimeoutSeconds != nil {
			task.TimeoutSeconds = *td.TimeoutSeconds
		}
		if td.Retry != nil {
			task.Retry = &orkstackv1.RetryPolicy{
				MaximumAttempts:        td.Retry.MaximumAttempts,
				InitialIntervalSeconds: td.Retry.InitialIntervalSeconds,
				BackoffCoefficient:     td.Retry.BackoffCoefficient,
				MaximumIntervalSeconds: td.Retry.MaximumIntervalSeconds,
			}
		}
		if td.With != nil {
			with, err := structpb.NewStruct(normalizeMap(td.With))
			if err != nil {
				return nil, fmt.Errorf("task %q: with: %w", td.ID, err)
			}
			task.With = with
		}
		wf.Tasks = append(wf.Tasks, task)
	}

	if err := validate(wf, opts); err != nil {
		return nil, err
	}

	if wf.GetVersion() == "" {
		version, err := contentHash(source)
		if err != nil {
			return nil, err
		}
		wf.Version = version
	}
	return wf, nil
}

// normalizeMap converts a YAML-decoded value into the JSON-compatible shape
// structpb requires (map keys become strings, ints stay numeric).
func normalizeMap(in map[string]any) map[string]any {
	out := make(map[string]any, len(in))
	for k, v := range in {
		out[k] = normalizeValue(v)
	}
	return out
}

func normalizeValue(v any) any {
	switch typed := v.(type) {
	case map[string]any:
		return normalizeMap(typed)
	case map[any]any:
		// YAML permits non-string keys, while JSON/Struct do not. Distinct YAML
		// keys can stringify to the same value (for example null and "<nil>").
		// Sort by both the normalized key and a typed identity before applying
		// the unavoidable last-value-wins conversion so the result, and thus an
		// unpinned content hash, is stable across map iteration order.
		type entry struct {
			key      string
			identity string
			value    any
		}
		entries := make([]entry, 0, len(typed))
		for key, value := range typed {
			entries = append(entries, entry{
				key:      fmt.Sprint(key),
				identity: fmt.Sprintf("%T:%#v", key, key),
				value:    value,
			})
		}
		sort.Slice(entries, func(i, j int) bool {
			if entries[i].key != entries[j].key {
				return entries[i].key < entries[j].key
			}
			return entries[i].identity < entries[j].identity
		})
		out := make(map[string]any, len(entries))
		for _, item := range entries {
			out[item.key] = normalizeValue(item.value)
		}
		return out
	case []any:
		out := make([]any, len(typed))
		for i, val := range typed {
			out[i] = normalizeValue(val)
		}
		return out
	case int:
		return float64(typed)
	case int32:
		return float64(typed)
	case int64:
		return float64(typed)
	case uint64:
		return float64(typed)
	default:
		return v
	}
}
