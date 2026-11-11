package dsl

import (
	"fmt"
	"reflect"

	"go.yaml.in/yaml/v4"
)

// MergeTagError is a merge the YAML decoder would read where none can stand:
// an explicit !!merge tag, on a key or a value, or a << written as a value
// that is not a string. go-yaml reads either as a merge directive, and a
// merge directive that is not a mapping key means nothing, so the decoder
// would drop the value without a word.
type MergeTagError struct {
	Path   string
	Line   int
	Reason string
}

func (e *MergeTagError) Error() string {
	return fmt.Sprintf("line %d: %s: %s", e.Line, e.Path, e.Reason)
}

const (
	mergeTagReason   = "the tag !!merge is not allowed; write the key or value without it, and a merge as <<: *anchor"
	mergeValueReason = "<< is a merge key, not a value; write the value here"
)

// RefuseMergeTags reads source against the type it decodes into, doc, and
// returns the first merge it holds where none can stand (MergeTagError), or
// nil. A data position (a yaml.Node field) is left to the strict mapping,
// which refuses every merge in its own words; a plain <<: key merges as it
// always did; a << for a string is the text <<. A source that does not parse
// is the decoder's to refuse.
func RefuseMergeTags(source []byte, doc any) error {
	return refuseMergeTags(source, doc)
}

// mergeTagRefusal is RefuseMergeTags as a document's ValidationError, at the
// merge's path.
func mergeTagRefusal(source []byte, doc any) error {
	err := refuseMergeTags(source, doc)
	if refused, ok := err.(*MergeTagError); ok {
		return validationError(refused.Path, refused)
	}
	return err
}

func refuseMergeTags(source []byte, doc any) error {
	var root yaml.Node
	if yaml.Unmarshal(source, &root) != nil {
		return nil
	}
	w := mergeTagWalk{walked: map[walkedNode]bool{}, fields: map[reflect.Type]map[string]reflect.Type{}}
	return w.walk(&root, reflect.TypeOf(doc), "")
}

// mergeTagWalk walks a node tree against the Go type it decodes into, each
// node once per type, as unknownKeyWalk does.
type mergeTagWalk struct {
	walked map[walkedNode]bool
	fields map[reflect.Type]map[string]reflect.Type
}

// explicitMerge reports a node written with the !!merge tag.
func explicitMerge(node *yaml.Node) bool {
	return node.Tag == "!!merge" && node.Style&yaml.TaggedStyle != 0
}

// plainMerge reports a plain << scalar, which go-yaml reads as a merge.
func plainMerge(node *yaml.Node) bool {
	return node.Kind == yaml.ScalarNode && node.Tag == "!!merge" && node.Style&yaml.TaggedStyle == 0
}

func (w *mergeTagWalk) walk(node *yaml.Node, t reflect.Type, path string) error {
	for node != nil && (node.Kind == yaml.DocumentNode || node.Kind == yaml.AliasNode) {
		if node.Kind == yaml.AliasNode {
			node = node.Alias
			continue
		}
		if len(node.Content) == 0 {
			return nil
		}
		node = node.Content[0]
	}
	if node == nil || t == nil || t == yamlNodeType {
		return nil
	}
	for t.Kind() == reflect.Pointer {
		t = t.Elem()
		if t == yamlNodeType {
			return nil
		}
	}
	if explicitMerge(node) {
		return &MergeTagError{Path: path, Line: node.Line, Reason: mergeTagReason}
	}
	at := walkedNode{node: node, t: t}
	if w.walked[at] {
		return nil
	}
	w.walked[at] = true
	switch node.Kind {
	case yaml.ScalarNode:
		if plainMerge(node) && t.Kind() != reflect.String {
			return &MergeTagError{Path: path, Line: node.Line, Reason: mergeValueReason}
		}
	case yaml.MappingNode:
		if t.Kind() != reflect.Struct && t.Kind() != reflect.Map {
			return nil
		}
		for i := 0; i+1 < len(node.Content); i += 2 {
			key, value := node.Content[i], node.Content[i+1]
			if key.Kind == yaml.AliasNode && key.Alias != nil {
				key = key.Alias
			}
			if explicitMerge(key) {
				return &MergeTagError{Path: joinPath(path, key.Value), Line: key.Line, Reason: mergeTagReason}
			}
			if plainMerge(key) {
				// The merged mappings deliver their keys here.
				if err := w.mergeSources(value, t, path); err != nil {
					return err
				}
				continue
			}
			var child reflect.Type
			if t.Kind() == reflect.Map {
				child = t.Elem()
			} else if field, declared := w.structFields(t)[key.Value]; declared {
				child = field
			} else {
				continue // the strict decode refuses it by name
			}
			if err := w.walk(value, child, joinPath(path, key.Value)); err != nil {
				return err
			}
		}
	case yaml.SequenceNode:
		if t.Kind() != reflect.Slice && t.Kind() != reflect.Array {
			return nil
		}
		for i, item := range node.Content {
			if err := w.walk(item, t.Elem(), fmt.Sprintf("%s[%d]", path, i)); err != nil {
				return err
			}
		}
	}
	return nil
}

// mergeSources walks what a <<: key merges, a mapping or a list of them,
// against the type the keys land in.
func (w *mergeTagWalk) mergeSources(value *yaml.Node, t reflect.Type, path string) error {
	if value.Kind == yaml.AliasNode && value.Alias != nil {
		value = value.Alias
	}
	if value.Kind != yaml.SequenceNode {
		return w.walk(value, t, path)
	}
	for _, item := range value.Content {
		if err := w.walk(item, t, path); err != nil {
			return err
		}
	}
	return nil
}

func (w *mergeTagWalk) structFields(t reflect.Type) map[string]reflect.Type {
	if fields, ok := w.fields[t]; ok {
		return fields
	}
	fields := yamlFields(t)
	w.fields[t] = fields
	return fields
}
