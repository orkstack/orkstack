package dsl_test

import (
	"fmt"
	"sort"

	"gopkg.in/yaml.v3"
)

// The previous decode of a data position, kept verbatim for
// TestStrictJSON_AgreesWithThePreviousDecodeOrRefuses: yaml.v3 into a
// map[string]any, as the old `With map[string]any` field decoded (yaml.v3's
// duplicate-key check included), then the deleted normalizeMap.
func legacyDecode(source string) (map[string]any, error) {
	var in map[string]any
	if err := yaml.Unmarshal([]byte(source), &in); err != nil {
		return nil, err
	}
	if in == nil {
		return nil, fmt.Errorf("not a mapping")
	}
	return normalizeMap(in), nil
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
		// the unavoidable last-value-wins conversion, so one document always
		// compiles to one message. A version does not depend on this — it is
		// the source's hash — but the STORE compares the compiled document, so
		// a compilation that varied with map iteration order would make
		// re-applying an unchanged file a different document under a stored
		// version (pkg/artifacts.SameStoredOrkflow).
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
