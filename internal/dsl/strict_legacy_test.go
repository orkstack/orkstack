package dsl_test

import (
	"fmt"
	"sort"
	"strconv"
	"strings"

	"go.yaml.in/yaml/v4"
)

// The previous decode of a data position, kept for
// TestStrictJSON_AgreesWithThePreviousDecodeOrRefuses: yaml.v3 into a
// map[string]any, as the old `With map[string]any` field decoded (yaml.v3's
// duplicate-key check included), then the deleted normalizeMap. It decodes
// with go-yaml v4 and undoes the two ways v4 reads a plain scalar differently
// that the strict mapping accepts (asV3). A third, an integer past 64 bits
// written with + (+18446744073709551615, a float to yaml.v3 and an integer to
// v4), is out of range for the strict mapping either way.
func legacyDecode(source string) (map[string]any, error) {
	var root yaml.Node
	if err := yaml.Unmarshal([]byte(source), &root); err != nil {
		return nil, err
	}
	asV3(&root)
	var in map[string]any
	if err := root.Decode(&in); err != nil {
		return nil, err
	}
	if in == nil {
		return nil, fmt.Errorf("not a mapping")
	}
	return normalizeMap(in), nil
}

// asV3 gives back yaml.v3's reading of the two things go-yaml v4 reads
// differently. A text v4 reads as a float that yaml.v3's integer rule
// (strconv.ParseInt in base 0, underscores removed) reads as an integer: the
// zeros written with a sign, -0 among them, which v4 reads as the float -0
// and yaml.v3 read as the integer 0. And the non-specific tag (! x), which
// yaml.v3 dropped, resolving the node as if it were untagged, and v4 keeps,
// reading a scalar so tagged as a string.
func asV3(node *yaml.Node) {
	if node.Tag == "!" {
		node.Tag = ""
	}
	if node.Kind == yaml.ScalarNode && node.Style == 0 {
		if n, err := strconv.ParseInt(strings.ReplaceAll(node.Value, "_", ""), 0, 64); err == nil {
			var v any
			plain := yaml.Node{Kind: yaml.ScalarNode, Tag: node.Tag, Value: node.Value}
			err := plain.Decode(&v)
			if _, float := v.(float64); err == nil && float {
				node.Tag, node.Value = "!!int", strconv.FormatInt(n, 10)
			}
		}
	}
	for _, child := range node.Content {
		asV3(child)
	}
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
