package automation

import (
	"fmt"
	"sort"
	"strings"

	"gopkg.in/yaml.v3"
)

// The helpers below read and build yaml.v3 nodes. The parser walks nodes
// rather than decoding into structs because three distinctions it must draw
// are invisible to a struct decode: an absent section versus {} versus null,
// an absent settings key versus its zero value, and a scalar input versus a
// mapping one.

// stringScalar reads a scalar written as a string. A number or a boolean is
// refused: a description of 12 is a typo, not a label.
func stringScalar(node *yaml.Node) (string, error) {
	if node.Kind != yaml.ScalarNode || node.Tag != "!!str" {
		return "", fmt.Errorf("must be a string, got %s", describeNode(node))
	}
	return node.Value, nil
}

// versionScalar reads a version: a string, or a number YAML would otherwise
// read as one ("version: 2" means the version named "2").
func versionScalar(node *yaml.Node) (string, error) {
	if node.Kind == yaml.ScalarNode {
		switch node.Tag {
		case "!!str", "!!int", "!!float":
			return node.Value, nil
		}
	}
	return "", fmt.Errorf("must be a version string, got %s", describeNode(node))
}

// stringList reads a sequence of strings.
func stringList(node *yaml.Node) ([]string, error) {
	if node.Kind != yaml.SequenceNode {
		return nil, fmt.Errorf("must be a list of strings, got %s", describeNode(node))
	}
	out := make([]string, 0, len(node.Content))
	for i, item := range node.Content {
		value, err := stringScalar(item)
		if err != nil {
			return nil, fmt.Errorf("item %d %w", i, err)
		}
		out = append(out, value)
	}
	if len(out) == 0 {
		return nil, nil
	}
	return out, nil
}

// stringMap reads a mapping of string keys to string values.
func stringMap(node *yaml.Node) (map[string]string, error) {
	if node.Kind != yaml.MappingNode {
		return nil, fmt.Errorf("must be a mapping of strings, got %s", describeNode(node))
	}
	out := map[string]string{}
	for i := 0; i+1 < len(node.Content); i += 2 {
		key, value := node.Content[i], node.Content[i+1]
		if _, dup := out[key.Value]; dup {
			return nil, fmt.Errorf("key %q is written twice", key.Value)
		}
		text, err := stringScalar(value)
		if err != nil {
			return nil, fmt.Errorf("value of %q %w", key.Value, err)
		}
		out[key.Value] = text
	}
	if len(out) == 0 {
		return nil, nil
	}
	return out, nil
}

// describeNode names what a node is, for a type error.
func describeNode(node *yaml.Node) string {
	switch node.Kind {
	case yaml.MappingNode:
		return "a mapping"
	case yaml.SequenceNode:
		return "a list"
	case yaml.ScalarNode:
		switch node.Tag {
		case "!!null":
			return "null"
		case "!!int", "!!float":
			return "the number " + node.Value
		case "!!bool":
			return "the boolean " + node.Value
		}
		return fmt.Sprintf("%q", node.Value)
	case yaml.AliasNode:
		return "an alias"
	}
	return "an unsupported node"
}

func isNull(node *yaml.Node) bool {
	return node.Kind == yaml.ScalarNode && node.Tag == "!!null"
}

// strNode is a string scalar the encoder quotes only when it must.
func strNode(value string) *yaml.Node {
	return &yaml.Node{Kind: yaml.ScalarNode, Tag: "!!str", Value: value}
}

// plainNode is a scalar written as given: a key, a slug, a duration.
func plainNode(value string) *yaml.Node {
	return &yaml.Node{Kind: yaml.ScalarNode, Tag: "!!str", Value: value}
}

// listNode is a flow sequence of strings: [] when empty.
func listNode(values []string) *yaml.Node {
	out := &yaml.Node{Kind: yaml.SequenceNode, Tag: "!!seq", Style: yaml.FlowStyle}
	for _, value := range values {
		out.Content = append(out.Content, strNode(value))
	}
	return out
}

// mapNode is a block mapping of strings sorted by key: {} when empty.
func mapNode(values map[string]string) *yaml.Node {
	out := &yaml.Node{Kind: yaml.MappingNode, Tag: "!!map"}
	if len(values) == 0 {
		out.Style = yaml.FlowStyle
		return out
	}
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		out.Content = append(out.Content, plainNode(key), strNode(values[key]))
	}
	return out
}

func joinKeys(keys []string) string { return strings.Join(keys, ", ") }

func sortStrings(values []string) { sort.Strings(values) }
