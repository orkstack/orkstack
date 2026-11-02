package dsl

import (
	"reflect"
	"strings"
	"testing"
	"unicode"
)

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
