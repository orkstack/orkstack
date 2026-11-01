package dsl

import (
	"fmt"
	"reflect"
	"strings"
	"testing"
	"unicode"

	"gopkg.in/yaml.v3"
)

// TestUnknownKeyWalk_WalksEachNodeOnce bounds the walk's cost by what it
// does, not by a clock: a document of many aliases of a large anchor is
// walked node by node once, never once per alias. The document is one the
// decoder refuses for an unknown key, so it is exactly what Compile walks.
func TestUnknownKeyWalk_WalksEachNodeOnce(t *testing.T) {
	t.Parallel()
	const n = 2000
	var b strings.Builder
	b.WriteString("apiVersion: orkstack/v1\nkind: Orkflow\nname: aliases\nbogus: 1\n")
	b.WriteString("inputs:\n  s: &s {name: a, source: \"env:X\"}\n  t: &t\n    id: x\n    id: y\n    secrets:\n")
	for range n {
		b.WriteString("      - *s\n")
	}
	b.WriteString("tasks:\n")
	for range n {
		b.WriteString("  - *t\n")
	}
	var root yaml.Node
	if err := yaml.Unmarshal([]byte(b.String()), &root); err != nil {
		t.Fatalf("parse: %v", err)
	}
	// Every node of the tree once, an alias's target only where it is written.
	nodes := 0
	var count func(*yaml.Node)
	count = func(node *yaml.Node) {
		nodes++
		for _, child := range node.Content {
			count(child)
		}
	}
	count(&root)

	var reported []string
	w := newUnknownKeyWalk(func(found unknownKey) {
		reported = append(reported, fmt.Sprintf("%d %s", found.line, joinPath(found.parent, found.key)))
	})
	w.walk(&root, reflect.TypeOf(document{}), "")
	if w.visits > nodes {
		t.Fatalf("the walk visited %d nodes of a document of %d: an anchor is walked again at each "+
			"of its %d aliases", w.visits, nodes, n)
	}
	if len(reported) == 0 || reported[0] != "4 bogus" {
		t.Fatalf("the walk reported %q, want the unknown key bogus at line 4 first", reported)
	}
}

// TestUnknownKey_DocumentTypesHaveOneWordNames: the decoder names the struct a
// key was not found in by its Go type, and strictDecodeError reads that name
// as the refusal's last word. A struct type with no name, or with a space in
// its name, would leave the decoder's sentence unread and on a user surface,
// so every struct an Orkflow or an activity interface decodes into has one.
func TestUnknownKey_DocumentTypesHaveOneWordNames(t *testing.T) {
	t.Parallel()
	seen := map[reflect.Type]bool{}
	var check func(reflect.Type, string)
	check = func(typ reflect.Type, path string) {
		for typ.Kind() == reflect.Pointer || typ.Kind() == reflect.Slice || typ.Kind() == reflect.Map {
			typ = typ.Elem()
		}
		if typ.Kind() != reflect.Struct || typ == yamlNodeType || seen[typ] {
			return
		}
		seen[typ] = true
		if typ.Name() == "" || strings.IndexFunc(typ.String(), unicode.IsSpace) >= 0 {
			t.Errorf("%s decodes into %q, which the decoder cannot name in one word", path, typ.String())
		}
		for name, field := range yamlFields(typ) {
			check(field, joinPath(path, name))
		}
	}
	check(reflect.TypeOf(document{}), "Orkflow")
	check(reflect.TypeOf(interfaceDocument{}), "activity interface")
	if len(seen) < 3 {
		t.Fatalf("only %d struct types were reached; the walk did not descend", len(seen))
	}
}
