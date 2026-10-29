package schemas_test

import (
	"encoding/json"
	"fmt"
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
// generator writes, in docs/schemas/ and in the UI's generated copy, which the
// editor is built from (ui/src/lib/yaml/schemas.ts).
func TestSchemas_AreGenerated(t *testing.T) {
	files, err := schemas.Files(root)
	if err != nil {
		t.Fatal(err)
	}
	for dir, want := range map[string]int{schemas.Dir: 4, schemas.UIDir: len(schemas.UIFiles)} {
		committed, err := filepath.Glob(filepath.Join(root, dir, "*.json"))
		if err != nil {
			t.Fatal(err)
		}
		if len(committed) != want {
			t.Errorf("%s holds %d files, the generator writes %d", dir, len(committed), want)
		}
	}
	for path, want := range files {
		got, err := os.ReadFile(filepath.Join(root, path))
		if err != nil || string(got) != string(want) {
			t.Errorf("%s is stale (%v): run go run ./internal/schemas/cmd/genschemas", path, err)
		}
	}
}

// TestSchemas_UICopyIsTheReference: the UI's copy of each kind's schema is the
// docs/schemas/ file itself, byte for byte, never a second rendering of it.
func TestSchemas_UICopyIsTheReference(t *testing.T) {
	files, err := schemas.Files(root)
	if err != nil {
		t.Fatal(err)
	}
	if len(schemas.UIFiles) != 2 {
		t.Fatalf("UIFiles = %v, want the two kinds' schemas", schemas.UIFiles)
	}
	for _, name := range schemas.UIFiles {
		ui, ok := files[schemas.UIDir+"/"+name]
		if !ok {
			t.Fatalf("the generator writes no %s/%s", schemas.UIDir, name)
		}
		if string(ui) != string(files[schemas.Dir+"/"+name]) {
			t.Errorf("%s/%s differs from %s/%s", schemas.UIDir, name, schemas.Dir, name)
		}
	}
}

// TestInterfaces_LabelsAreGenerated: every Dockerfile built beside an
// interface.ork.yaml carries, byte for byte, the LABEL line the generator
// packs from it — the first-party images and the examples alike.
func TestInterfaces_LabelsAreGenerated(t *testing.T) {
	files, err := schemas.Dockerfiles(root)
	if err != nil {
		t.Fatal(err)
	}
	documents, err := filepath.Glob(filepath.Join(root, "*", "*", "interface.ork.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	if len(files) != len(documents) || len(files) != 8 {
		t.Errorf("the generator writes %d Dockerfiles, the tree holds %d interface documents; want eight", len(files), len(documents))
	}
	for path, want := range files {
		got, err := os.ReadFile(filepath.Join(root, path))
		if err != nil || string(got) != string(want) {
			t.Errorf("%s is stale (%v): run go run ./internal/schemas/cmd/genschemas", path, err)
		}
	}
	// A Dockerfile that carries an interface LABEL carries one generated
	// from a document: a hand-written label here has no source to drift from.
	for _, parent := range []string{"images", "examples"} {
		dockerfiles, err := filepath.Glob(filepath.Join(root, parent, "*", "Dockerfile"))
		if err != nil {
			t.Fatal(err)
		}
		for _, dockerfile := range dockerfiles {
			raw, err := os.ReadFile(dockerfile)
			if err != nil {
				t.Fatal(err)
			}
			rel, _ := filepath.Rel(root, dockerfile)
			if _, generated := files[filepath.ToSlash(rel)]; strings.Contains(string(raw), "LABEL "+dsl.InterfaceLabel) && !generated {
				t.Errorf("%s carries an interface LABEL with no %s beside it", rel, schemas.InterfaceDocument)
			}
		}
	}
}

// interfaceDocumentPacking builds an interface document whose packed label
// is exactly size bytes: keys are added while the label stays under size,
// then the first keys' names grow by the bytes still missing.
func interfaceDocumentPacking(t *testing.T, size int) string {
	t.Helper()
	build := func(keys int, pad []int) string {
		var doc strings.Builder
		doc.WriteString("apiVersion: orkstack/v1\nkind: ActivityInterface\nops:\n")
		for i := 0; i < keys; i++ {
			if i%250 == 0 {
				fmt.Fprintf(&doc, "  op%d:\n    fields:\n", i/250)
			}
			extra := 0
			if i < len(pad) {
				extra = pad[i]
			}
			fmt.Fprintf(&doc, "      k%04d%s:\n        type: string|number|boolean|object|array\n", i, strings.Repeat("x", extra))
		}
		return doc.String()
	}
	packed := func(source string) int {
		iface, err := dsl.ParseInterfaceDocument([]byte(source))
		if err != nil {
			t.Fatal(err)
		}
		label, err := dsl.PackInterfaceLabel(iface)
		if err != nil {
			return 1 << 30
		}
		return len(label)
	}
	low, high := 1, 2000 // the most keys whose label stays under size
	for low < high {
		mid := (low + high + 1) / 2
		if packed(build(mid, nil)) <= size {
			low = mid
		} else {
			high = mid - 1
		}
	}
	missing := size - packed(build(low, nil))
	var pad []int
	for missing > 0 {
		step := min(missing, 100)
		pad = append(pad, step)
		missing -= step
	}
	source := build(low, pad)
	if got := packed(source); got != size {
		t.Fatalf("the document packs to %d bytes, want %d", got, size)
	}
	return source
}

// TestInterfaces_TheGeneratorRefusesALineBuildKitCannotRead: a label near
// the packer's bound fits a --label argument but not a Dockerfile line,
// which BuildKit reads up to 65535 bytes.
func TestInterfaces_TheGeneratorRefusesALineBuildKitCannotRead(t *testing.T) {
	overhead := len("LABEL " + dsl.InterfaceLabel + "=''")
	for _, tc := range []struct {
		name  string
		label int
		err   string
	}{
		{"the longest line BuildKit reads", schemas.MaxDockerfileLine - overhead, ""},
		{"one byte longer", schemas.MaxDockerfileLine - overhead + 1, "BuildKit refuses a Dockerfile line over 65535"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			image := filepath.Join(dir, "images", "activity-x")
			if err := os.MkdirAll(image, 0o755); err != nil {
				t.Fatal(err)
			}
			files := map[string]string{
				"interface.ork.yaml": interfaceDocumentPacking(t, tc.label),
				"Dockerfile":         "FROM scratch\nLABEL " + dsl.InterfaceLabel + "='{}'\n",
			}
			for name, content := range files {
				if err := os.WriteFile(filepath.Join(image, name), []byte(content), 0o644); err != nil {
					t.Fatal(err)
				}
			}
			out, err := schemas.Dockerfiles(dir)
			if tc.err == "" {
				if err != nil {
					t.Fatal(err)
				}
				if line := strings.Split(string(out["images/activity-x/Dockerfile"]), "\n")[1]; len(line) != schemas.MaxDockerfileLine {
					t.Fatalf("the LABEL line is %d bytes, want %d", len(line), schemas.MaxDockerfileLine)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), tc.err) {
				t.Fatalf("Dockerfiles = %v; want an error containing %q", err, tc.err)
			}
		})
	}
}

// TestInterfaces_TheGeneratorReplacesOnlyTheLabelLine: a Dockerfile's other
// lines are its author's, and a Dockerfile with no interface LABEL line, or
// two, is refused rather than guessed at.
func TestInterfaces_TheGeneratorReplacesOnlyTheLabelLine(t *testing.T) {
	const document = "apiVersion: orkstack/v1\nkind: ActivityInterface\nfields:\n  a:\n    type: string\n"
	const label = `LABEL ` + dsl.InterfaceLabel + `='{"schemaVersion":1,"fields":{"a":{"type":"string"}}}'`
	for _, tc := range []struct {
		name, dockerfile, want, err string
	}{
		{"stale", "FROM scratch\n# kept\nLABEL " + dsl.InterfaceLabel + "='{}'\nENTRYPOINT [\"/x\"]\n",
			"FROM scratch\n# kept\n" + label + "\nENTRYPOINT [\"/x\"]\n", ""},
		{"current", "FROM scratch\n" + label + "\n", "FROM scratch\n" + label + "\n", ""},
		{"none", "FROM scratch\n", "", "carries 0 interface LABEL lines, want exactly one"},
		{"two", "FROM scratch\n" + label + "\n" + label + "\n", "", "carries 2 interface LABEL lines, want exactly one"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			image := filepath.Join(dir, "images", "activity-x")
			if err := os.MkdirAll(image, 0o755); err != nil {
				t.Fatal(err)
			}
			for name, content := range map[string]string{"interface.ork.yaml": document, "Dockerfile": tc.dockerfile} {
				if err := os.WriteFile(filepath.Join(image, name), []byte(content), 0o644); err != nil {
					t.Fatal(err)
				}
			}
			files, err := schemas.Dockerfiles(dir)
			if tc.err != "" {
				if err == nil || !strings.Contains(err.Error(), tc.err) {
					t.Fatalf("Dockerfiles = %v; want an error containing %q", err, tc.err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if got := string(files["images/activity-x/Dockerfile"]); got != tc.want {
				t.Fatalf("Dockerfile\n%s\nwant\n%s", got, tc.want)
			}
		})
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

func TestSchemas_DispatcherHoldsEveryKind(t *testing.T) {
	all := trees(t)
	dispatcher := all[schemas.DispatcherFile]
	allOf := dispatcher["allOf"].([]any)
	kinds := map[string]string{}
	for _, entry := range allOf {
		clause := entry.(map[string]any)
		kind := clause["if"].(map[string]any)["properties"].(map[string]any)["kind"].(map[string]any)["const"].(string)
		kinds[kind] = clause["then"].(map[string]any)["$ref"].(string)
	}
	want := map[string]string{
		dsl.KindOrkflow:           "#/definitions/orkflow",
		dsl.KindAutomation:        "#/definitions/automation",
		dsl.KindActivityInterface: "#/definitions/activityinterface",
	}
	if got := stringsOf(dispatcher["properties"].(map[string]any)["kind"].(map[string]any)["enum"]); !reflect.DeepEqual(got,
		[]string{dsl.KindActivityInterface, dsl.KindAutomation, dsl.KindOrkflow}) {
		t.Errorf("the dispatcher's kind enum is %v", got)
	}
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
