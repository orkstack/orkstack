package schemas_test

import (
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"testing"

	"github.com/orkstack/orkstack/internal/dsl"
	"github.com/orkstack/orkstack/internal/schemas"
)

const root = "../.."

// TestSchemas_AreGenerated: the committed schemas are byte for byte what the
// generator writes.
func TestSchemas_AreGenerated(t *testing.T) {
	files, err := schemas.Files(root)
	if err != nil {
		t.Fatal(err)
	}
	committed, err := filepath.Glob(filepath.Join(root, schemas.Dir, "*.json"))
	if err != nil {
		t.Fatal(err)
	}
	if len(committed) != len(files) {
		t.Errorf("%s holds %d files, the generator writes %d", schemas.Dir, len(committed), len(files))
	}
	for path, want := range files {
		got, err := os.ReadFile(filepath.Join(root, path))
		if err != nil || string(got) != string(want) {
			t.Errorf("%s is stale (%v): run go run ./internal/schemas/cmd/genschemas", path, err)
		}
	}
}

func trees(t *testing.T) map[string]map[string]any {
	t.Helper()
	trees, err := schemas.Trees(root)
	if err != nil {
		t.Fatal(err)
	}
	// Round-trip through JSON, so the tests read what the files hold.
	out := map[string]map[string]any{}
	for name, tree := range trees {
		encoded, err := schemas.Encode(tree)
		if err != nil {
			t.Fatal(err)
		}
		var decoded map[string]any
		if err := json.Unmarshal(encoded, &decoded); err != nil {
			t.Fatal(err)
		}
		out[name] = decoded
	}
	return out
}

func definitions(tree map[string]any) map[string]any { return tree["definitions"].(map[string]any) }

func TestSchemas_EveryFirstPartyImageHasAWithSchema(t *testing.T) {
	defs := definitions(trees(t)[schemas.OrkflowFile])
	names := dsl.BuiltinActivityCatalog().Names()
	if len(names) != 6 {
		t.Fatalf("the builtin catalog has %d names, want six", len(names))
	}
	for _, name := range names {
		if _, ok := defs["orkflow.with."+name]; !ok {
			t.Errorf("no orkflow.with.%s", name)
		}
	}
	var withs []string
	for name := range defs {
		if strings.HasPrefix(name, "orkflow.with.") {
			withs = append(withs, strings.TrimPrefix(name, "orkflow.with."))
		}
	}
	sort.Strings(withs)
	if !reflect.DeepEqual(withs, names) {
		t.Errorf("with schemas %v, want exactly the catalog %v", withs, names)
	}
}

// typesOf reads a field schema's declared types back: {"type": t} or
// {"anyOf": [{"type": t}, template]}.
func typesOf(t *testing.T, field map[string]any) []string {
	t.Helper()
	if anyOf, ok := field["anyOf"].([]any); ok {
		field = anyOf[0].(map[string]any)
	}
	switch typ := field["type"].(type) {
	case string:
		return []string{typ}
	case []any:
		var out []string
		for _, word := range typ {
			out = append(out, word.(string))
		}
		return out
	}
	t.Fatalf("field %v has no type", field)
	return nil
}

func stringsOf(list any) []string {
	var out []string
	for _, item := range list.([]any) {
		out = append(out, item.(string))
	}
	sort.Strings(out)
	return out
}

// TestSchemas_FirstPartyWithIsTheLabel: each orkflow.with.<name> holds
// exactly the label's fields, types, defaults, required keys, ops and
// default op — what CheckWith judges a with: by.
func TestSchemas_FirstPartyWithIsTheLabel(t *testing.T) {
	interfaces, err := schemas.FirstPartyInterfaces(root)
	if err != nil {
		t.Fatal(err)
	}
	defs := definitions(trees(t)[schemas.OrkflowFile])
	for name, iface := range interfaces {
		def := defs["orkflow.with."+name].(map[string]any)
		properties := def["properties"].(map[string]any)
		var wantKeys, wantRequired []string
		for key, field := range iface.GetFields() {
			wantKeys = append(wantKeys, key)
			got := properties[key].(map[string]any)
			if want := strings.Split(field.GetType(), "|"); !reflect.DeepEqual(typesOf(t, got), want) {
				t.Errorf("%s.%s: types %v, want %v", name, key, typesOf(t, got), want)
			}
			if field.GetDefault() != nil && !reflect.DeepEqual(got["default"], field.GetDefault().AsInterface()) {
				t.Errorf("%s.%s: default %v, want %v", name, key, got["default"], field.GetDefault().AsInterface())
			}
			if field.GetRequired() {
				wantRequired = append(wantRequired, key)
			}
		}
		if len(iface.GetOps()) > 0 {
			wantKeys = append(wantKeys, "op")
			op := properties["op"].(map[string]any)
			var ops []string
			for op := range iface.GetOps() {
				ops = append(ops, op)
			}
			sort.Strings(ops)
			if got := stringsOf(op["anyOf"].([]any)[0].(map[string]any)["enum"]); !reflect.DeepEqual(got, ops) {
				t.Errorf("%s: ops %v, want %v", name, got, ops)
			}
			if def := iface.GetDefaultOp(); def == "" {
				wantRequired = append(wantRequired, "op")
			} else if op["default"] != def {
				t.Errorf("%s: default op %v, want %s", name, op["default"], def)
			}
			// Each op's own fields, under its if/then.
			seen := map[string]bool{}
			for _, entry := range def["allOf"].([]any) {
				clause := entry.(map[string]any)
				cond := clause["if"].(map[string]any)
				then := clause["then"].(map[string]any)
				opName := ""
				if props, ok := cond["properties"].(map[string]any); ok {
					opName = props["op"].(map[string]any)["const"].(string)
				} else {
					opName = iface.GetDefaultOp()
				}
				seen[opName] = true
				fields := iface.GetOps()[opName].GetFields()
				thenProps, _ := then["properties"].(map[string]any)
				if len(thenProps) != len(fields) {
					t.Errorf("%s op %s: %d keys, want %d", name, opName, len(thenProps), len(fields))
				}
				var required []string
				for key, field := range fields {
					got, ok := thenProps[key].(map[string]any)
					if !ok {
						t.Errorf("%s op %s: no key %s", name, opName, key)
						continue
					}
					if want := strings.Split(field.GetType(), "|"); !reflect.DeepEqual(typesOf(t, got), want) {
						t.Errorf("%s op %s.%s: types %v, want %v", name, opName, key, typesOf(t, got), want)
					}
					if field.GetRequired() {
						required = append(required, key)
					}
				}
				sort.Strings(required)
				var gotRequired []string
				if list, ok := then["required"]; ok {
					gotRequired = stringsOf(list)
				}
				if !reflect.DeepEqual(gotRequired, required) {
					t.Errorf("%s op %s: required %v, want %v", name, opName, gotRequired, required)
				}
			}
			for op := range iface.GetOps() {
				if !seen[op] {
					t.Errorf("%s: op %s has no if/then", name, op)
				}
			}
		}
		var gotKeys []string
		for key := range properties {
			gotKeys = append(gotKeys, key)
		}
		sort.Strings(gotKeys)
		sort.Strings(wantKeys)
		if !reflect.DeepEqual(gotKeys, wantKeys) {
			t.Errorf("%s: keys %v, want %v", name, gotKeys, wantKeys)
		}
		sort.Strings(wantRequired)
		var gotRequired []string
		if list, ok := def["required"]; ok {
			gotRequired = stringsOf(list)
		}
		if !reflect.DeepEqual(gotRequired, wantRequired) {
			t.Errorf("%s: required %v, want %v", name, gotRequired, wantRequired)
		}
	}
}

// refs collects every $ref under v.
func refs(v any, out *[]string) {
	switch typed := v.(type) {
	case map[string]any:
		for k, child := range typed {
			if k == "$ref" {
				*out = append(*out, child.(string))
				continue
			}
			refs(child, out)
		}
	case []any:
		for _, child := range typed {
			refs(child, out)
		}
	}
}

func TestSchemas_DispatcherHoldsBothKinds(t *testing.T) {
	all := trees(t)
	dispatcher := all[schemas.DispatcherFile]
	allOf := dispatcher["allOf"].([]any)
	kinds := map[string]string{}
	for _, entry := range allOf {
		clause := entry.(map[string]any)
		kind := clause["if"].(map[string]any)["properties"].(map[string]any)["kind"].(map[string]any)["const"].(string)
		kinds[kind] = clause["then"].(map[string]any)["$ref"].(string)
	}
	want := map[string]string{dsl.KindOrkflow: "#/definitions/orkflow", dsl.KindAutomation: "#/definitions/automation"}
	if !reflect.DeepEqual(kinds, want) {
		t.Fatalf("the dispatcher selects %v, want %v", kinds, want)
	}
	for name, tree := range all {
		var found []string
		refs(tree, &found)
		defs := definitions(tree)
		for _, ref := range found {
			if _, ok := defs[strings.TrimPrefix(ref, "#/definitions/")]; !ok {
				t.Errorf("%s: %s resolves to nothing", name, ref)
			}
		}
	}
}
