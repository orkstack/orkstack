package dsl

import (
	"errors"
	"fmt"
	"reflect"
	"regexp"
	"sort"
	"strconv"
	"strings"

	"gopkg.in/yaml.v3"
)

// unknownFieldRefusal is yaml.v3's sentence for a key a struct does not
// declare. The key may hold any character, a line break or " not found in
// type " included, so it is everything between "field " and the last word,
// which is the Go type.
var unknownFieldRefusal = regexp.MustCompile(`(?s)^line (\d+): field (.*) not found in type \S+$`)

// moreUnknownKeysNamed is how many of the other unknown keys the sentence
// names after the first: a document of another shape can hold thousands,
// and the answer is read by a person.
const moreUnknownKeysNamed = 10

// strictDecodeError words a strict decode's refusal for the author. yaml.v3
// refuses a key the schema does not define as "field X not found in type
// dsl.taskDocument", naming the Go type the key would have decoded into
// (AGENTS.md §7: nothing internal on a user surface). Every key it refused
// that way is reworded from the decoder's own line and key. The first, in
// line order, has the sentence: at the path it stands at, with the keys that
// path takes, when the source walked against the same struct finds the key
// there at that line, and by its line alone when the walk does not (a key a
// merge delivered). The rest are counted after it, and the first
// moreUnknownKeysNamed of them named with their lines, so one answer shows
// how many keys are unknown. When no key was refused, the decoder's refusal is returned as
// it worded it; a refusal of another kind beside refused keys (a value of
// the wrong type) is the next decode's answer, once the keys are fixed.
//
// doc is the struct the strict decode decoded into; kind names the document
// in the sentence ("Orkflow", "activity interface").
func strictDecodeError(source []byte, doc reflect.Type, kind string, err error) error {
	var typeErr *yaml.TypeError
	if !errors.As(err, &typeErr) {
		return fmt.Errorf("parse yaml: %w", err)
	}
	refusals := refusedKeys(typeErr)
	if len(refusals) == 0 {
		return fmt.Errorf("parse yaml: %w", err)
	}
	refused := make(map[refusedKey]bool, len(refusals))
	for _, r := range refusals {
		refused[r] = true
	}
	located := map[refusedKey]unknownKey{}
	var root yaml.Node
	if yaml.Unmarshal(source, &root) == nil {
		newUnknownKeyWalk(func(found unknownKey) {
			at := refusedKey{line: found.line, key: found.key}
			if _, seen := located[at]; refused[at] && !seen {
				located[at] = found
			}
		}).walk(&root, doc, "")
	}
	more := moreUnknownKeys(refusals[1:])
	first := refusals[0]
	found, ok := located[first]
	if !ok {
		return validationError("", fmt.Errorf("line %d: unknown key %q in the %s%s", first.line, first.key, kind, more))
	}
	where := "at the top level"
	if found.parent != "" {
		where = "in " + found.parent
	}
	return validationError(joinPath(found.parent, found.key),
		fmt.Errorf("line %d: unknown key %q %s of the %s%s; allowed: %s",
			found.line, found.key, where, kind, more, strings.Join(found.allowed, ", ")))
}

// moreUnknownKeys words the unknown keys after the first: how many, and the
// first moreUnknownKeysNamed of them with their lines.
func moreUnknownKeys(rest []refusedKey) string {
	if len(rest) == 0 {
		return ""
	}
	noun, which := "key", ""
	if len(rest) > 1 {
		noun = "keys"
	}
	named := rest
	if len(named) > moreUnknownKeysNamed {
		named, which = named[:moreUnknownKeysNamed], fmt.Sprintf(", the first %d", moreUnknownKeysNamed)
	}
	names := make([]string, len(named))
	for i, r := range named {
		names[i] = fmt.Sprintf("%q at line %d", r.key, r.line)
	}
	return fmt.Sprintf(" (and %d more unknown %s%s: %s)", len(rest), noun, which, strings.Join(names, ", "))
}

// refusedKey is one key the decoder refused as undeclared, at its line.
type refusedKey struct {
	line int
	key  string
}

// refusedKeys reads the keys the decoder refused as undeclared out of its
// refusal, in line order and each once: a key under an anchor is refused
// again at every alias that repeats it. The decoder names a key by its line
// and name alone, so two keys of one name on one line are one key here.
func refusedKeys(typeErr *yaml.TypeError) []refusedKey {
	var out []refusedKey
	seen := map[refusedKey]bool{}
	for _, message := range typeErr.Errors {
		m := unknownFieldRefusal.FindStringSubmatch(message)
		if m == nil {
			continue
		}
		line, err := strconv.Atoi(m[1])
		if err != nil {
			continue
		}
		r := refusedKey{line: line, key: m[2]}
		if !seen[r] {
			seen[r] = true
			out = append(out, r)
		}
	}
	sort.SliceStable(out, func(i, j int) bool { return out[i].line < out[j].line })
	return out
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

// unknownKeyWalk walks a document against its struct in document order and
// hands report every mapping key the struct's yaml tags do not declare. A
// data position (a yaml.Node field) is authored data, judged by the strict
// mapping instead; a node of the wrong shape, a merge key and a key that is
// not a plain scalar are left to the decoder's own refusal.
//
// Each node is walked once per type: every alias of an anchored node would
// report the same keys at the same lines, and only the first report of each
// is kept, so walking it again could only cost — once per alias, which a
// document of many aliases of a large anchor makes quadratic.
type unknownKeyWalk struct {
	report func(unknownKey)
	walked map[walkedNode]bool
	fields map[reflect.Type]structKeys
	visits int // nodes walked; the cost test reads it
}

// walkedNode is one node walked against one type.
type walkedNode struct {
	node *yaml.Node
	t    reflect.Type
}

// structKeys is what a document struct declares: each key's field type, and
// the keys sorted.
type structKeys struct {
	types   map[string]reflect.Type
	allowed []string
}

func newUnknownKeyWalk(report func(unknownKey)) *unknownKeyWalk {
	return &unknownKeyWalk{
		report: report,
		walked: map[walkedNode]bool{},
		fields: map[reflect.Type]structKeys{},
	}
}

func (w *unknownKeyWalk) walk(node *yaml.Node, t reflect.Type, path string) {
	for node != nil && (node.Kind == yaml.DocumentNode || node.Kind == yaml.AliasNode) {
		if node.Kind == yaml.AliasNode {
			node = node.Alias
			continue
		}
		if len(node.Content) == 0 {
			return
		}
		node = node.Content[0]
	}
	if node == nil || t == yamlNodeType {
		return
	}
	for t.Kind() == reflect.Pointer {
		t = t.Elem()
	}
	at := walkedNode{node: node, t: t}
	if w.walked[at] {
		return
	}
	w.walked[at] = true
	w.visits++
	switch t.Kind() {
	case reflect.Struct:
		if node.Kind != yaml.MappingNode {
			return
		}
		keys := w.structKeys(t)
		for i := 0; i+1 < len(node.Content); i += 2 {
			key, ok := plainKey(node.Content[i])
			if !ok {
				continue
			}
			field, declared := keys.types[key]
			if !declared {
				w.report(unknownKey{key: key, parent: path, line: node.Content[i].Line, allowed: keys.allowed})
				continue
			}
			w.walk(node.Content[i+1], field, joinPath(path, key))
		}
	case reflect.Map:
		if node.Kind != yaml.MappingNode {
			return
		}
		for i := 0; i+1 < len(node.Content); i += 2 {
			key, ok := plainKey(node.Content[i])
			if !ok {
				continue
			}
			w.walk(node.Content[i+1], t.Elem(), joinPath(path, key))
		}
	case reflect.Slice:
		if node.Kind != yaml.SequenceNode {
			return
		}
		for i, item := range node.Content {
			w.walk(item, t.Elem(), fmt.Sprintf("%s[%d]", path, i))
		}
	}
}

// structKeys reads t's declared keys once per walk.
func (w *unknownKeyWalk) structKeys(t reflect.Type) structKeys {
	if keys, ok := w.fields[t]; ok {
		return keys
	}
	types := yamlFields(t)
	allowed := make([]string, 0, len(types))
	for name := range types {
		allowed = append(allowed, name)
	}
	sort.Strings(allowed)
	keys := structKeys{types: types, allowed: allowed}
	w.fields[t] = keys
	return keys
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
