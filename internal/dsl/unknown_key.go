package dsl

import (
	"errors"
	"fmt"
	"reflect"
	"sort"
	"strings"

	"gopkg.in/yaml.v3"
)

// strictDecodeError words a strict decode's refusal for the author. yaml.v3
// refuses a key the schema does not define as "field X not found in type
// dsl.taskDocument", naming the Go type the key would have decoded into
// (AGENTS.md §7: nothing internal on a user surface). So the source is read
// again, walked against the same struct, and the first key the decoder
// refused is refused in the author's words — the key, the path it stands at
// and the document's kind — at the key's field path. Only a key the decoder
// itself refused, at the same line, is named, so the walk can never report a
// key yaml.v3 accepted or hide its real refusal; any other refusal is
// returned as the decoder worded it.
//
// doc is the struct the strict decode decoded into; kind names the document
// in the sentence ("Orkflow", "activity interface").
func strictDecodeError(source []byte, doc reflect.Type, kind string, err error) error {
	var typeErr *yaml.TypeError
	if errors.As(err, &typeErr) {
		var root yaml.Node
		refused := func(key string, line int) bool {
			want := fmt.Sprintf("line %d: field %s not found in type ", line, key)
			for _, message := range typeErr.Errors {
				if strings.HasPrefix(message, want) {
					return true
				}
			}
			return false
		}
		if yaml.Unmarshal(source, &root) == nil {
			if found := firstUnknownKey(&root, doc, "", refused); found != nil {
				where := "at the top level"
				if found.parent != "" {
					where = "in " + found.parent
				}
				return validationError(joinPath(found.parent, found.key),
					fmt.Errorf("line %d: unknown key %q %s of the %s; allowed: %s",
						found.line, found.key, where, kind, strings.Join(found.allowed, ", ")))
			}
		}
	}
	return fmt.Errorf("parse yaml: %w", err)
}

// unknownKey is one key a document struct does not declare: the key, the
// path of the mapping it was written in, its line, and the keys that mapping
// takes.
type unknownKey struct {
	key     string
	parent  string
	line    int
	allowed []string
}

// firstUnknownKey walks node against t in document order and returns the
// first mapping key t's yaml tags do not declare and refused confirms the
// decoder refused at its line, or nil. A data position (a yaml.Node field) is
// authored data, judged by the strict mapping instead; a node of the wrong
// shape, a merge key and a key that is not a plain scalar are left to the
// decoder's own refusal.
func firstUnknownKey(node *yaml.Node, t reflect.Type, path string, refused func(key string, line int) bool) *unknownKey {
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
	if node == nil || t == yamlNodeType {
		return nil
	}
	for t.Kind() == reflect.Pointer {
		t = t.Elem()
	}
	switch t.Kind() {
	case reflect.Struct:
		if node.Kind != yaml.MappingNode {
			return nil
		}
		fields := yamlFields(t)
		for i := 0; i+1 < len(node.Content); i += 2 {
			key, ok := plainKey(node.Content[i])
			if !ok {
				continue
			}
			field, declared := fields[key]
			if !declared {
				if !refused(key, node.Content[i].Line) {
					continue
				}
				allowed := make([]string, 0, len(fields))
				for name := range fields {
					allowed = append(allowed, name)
				}
				sort.Strings(allowed)
				return &unknownKey{key: key, parent: path, line: node.Content[i].Line, allowed: allowed}
			}
			if found := firstUnknownKey(node.Content[i+1], field, joinPath(path, key), refused); found != nil {
				return found
			}
		}
	case reflect.Map:
		if node.Kind != yaml.MappingNode {
			return nil
		}
		for i := 0; i+1 < len(node.Content); i += 2 {
			key, ok := plainKey(node.Content[i])
			if !ok {
				continue
			}
			if found := firstUnknownKey(node.Content[i+1], t.Elem(), joinPath(path, key), refused); found != nil {
				return found
			}
		}
	case reflect.Slice:
		if node.Kind != yaml.SequenceNode {
			return nil
		}
		for i, item := range node.Content {
			if found := firstUnknownKey(item, t.Elem(), fmt.Sprintf("%s[%d]", path, i), refused); found != nil {
				return found
			}
		}
	}
	return nil
}

// plainKey is a mapping key as yaml.v3 reads it — an alias resolved to the
// scalar it names — or false for a merge key or a key that is not a scalar,
// which the walk leaves to the decoder.
func plainKey(node *yaml.Node) (string, bool) {
	if node.Kind == yaml.AliasNode {
		node = node.Alias
	}
	if node == nil || node.Kind != yaml.ScalarNode || node.Tag == "!!merge" {
		return "", false
	}
	return node.Value, true
}

// yamlFields maps each key a document struct declares to its field's type,
// as yaml.v3 reads the struct: the tag's name, or the lowercased field name
// when the tag gives none; "-" and unexported fields take no key.
func yamlFields(t reflect.Type) map[string]reflect.Type {
	fields := make(map[string]reflect.Type, t.NumField())
	for i := 0; i < t.NumField(); i++ {
		field := t.Field(i)
		if !field.IsExported() {
			continue
		}
		name := strings.Split(field.Tag.Get("yaml"), ",")[0]
		switch name {
		case "-":
			continue
		case "":
			name = strings.ToLower(field.Name)
		}
		fields[name] = field.Type
	}
	return fields
}
