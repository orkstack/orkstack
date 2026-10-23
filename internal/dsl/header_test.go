package dsl_test

import (
	"errors"
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"

	"github.com/orkstack/orkstack/internal/dsl"
)

const okBody = "name: x\ntasks:\n  - id: a\n    uses: shell\n    with: {op: exec, command: [\"true\"]}\n"

// headerError compiles source and returns its refusal's field and text.
func headerError(t *testing.T, source string) (string, string) {
	t.Helper()
	_, err := dsl.Compile([]byte(source))
	if err == nil {
		t.Fatalf("Compile accepted:\n%s", source)
	}
	field := ""
	var verr *dsl.ValidationError
	if errors.As(err, &verr) {
		field = verr.Field
	}
	return field, err.Error()
}

const h1 = `the document has no apiVersion or kind: an Orkflow begins with the two lines "apiVersion: orkstack/v1" and "kind: Orkflow" — add them at the top, after any leading comments (docs/DSL.md, "The document header")`

func TestCompile_RequiresTheHeader(t *testing.T) {
	for name, source := range map[string]string{
		"an empty document":       "",
		"a comment-only document": "# nothing here\n",
		"a body without a header": okBody,
		"a commented body":        "# OrkStack orkflow DSL\n" + okBody,
	} {
		field, message := headerError(t, source)
		if field != "apiVersion" || message != h1 {
			t.Errorf("%s: field %q, message %q\nwant field apiVersion, %q", name, field, message, h1)
		}
	}
}

func TestCompile_RefusesAnUnsupportedAPIVersion(t *testing.T) {
	for _, tc := range []struct{ value, want string }{
		{"orkstack/v2", `apiVersion "orkstack/v2" is not supported: this engine reads apiVersion: orkstack/v1`},
		{"v1", `apiVersion "v1" is not supported: this engine reads apiVersion: orkstack/v1`},
		{"{a: b}", `apiVersion must be the string orkstack/v1, got a mapping`},
		{"[orkstack/v1]", `apiVersion must be the string orkstack/v1, got a list`},
		{"1", `apiVersion must be the string orkstack/v1, got the number 1`},
		{"true", `apiVersion must be the string orkstack/v1, got the boolean true`},
		{"~", `apiVersion must be the string orkstack/v1, got null`},
	} {
		field, message := headerError(t, "apiVersion: "+tc.value+"\nkind: Orkflow\n"+okBody)
		if field != "apiVersion" || message != tc.want {
			t.Errorf("apiVersion: %s → field %q, %q; want apiVersion, %q", tc.value, field, message, tc.want)
		}
	}
}

func TestCompile_RefusesAnotherKind(t *testing.T) {
	for _, tc := range []struct{ source, field, want string }{
		{"apiVersion: orkstack/v1\nkind: Automation\n" + okBody, "kind",
			`kind: Automation is an automation document, not an Orkflow: apply it with orkctl apply or Admin → Automation (docs/AUTOMATION.md); an Orkflow says kind: Orkflow`},
		{"apiVersion: orkstack/v1\nkind: Workflow\n" + okBody, "kind",
			`kind "Workflow" is not a kind OrkStack reads: an Orkflow says kind: Orkflow, an automation document kind: Automation`},
		{"apiVersion: orkstack/v1\nkind: orkflow\n" + okBody, "kind",
			`kind "orkflow" is not a kind OrkStack reads: an Orkflow says kind: Orkflow, an automation document kind: Automation`},
		{"apiVersion: orkstack/v1\nkind: [Orkflow]\n" + okBody, "kind", `kind must be the string Orkflow, got a list`},
		{"apiVersion: orkstack/v1\nkind: 1\n" + okBody, "kind", `kind must be the string Orkflow, got the number 1`},
		{"kind: Orkflow\n" + okBody, "apiVersion",
			`apiVersion is required: add the line "apiVersion: orkstack/v1" above "kind: Orkflow"`},
		{"apiVersion: orkstack/v1\n" + okBody, "kind",
			`kind is required: add the line "kind: Orkflow" below "apiVersion: orkstack/v1"`},
		{"apiVersion: orkstack/v2\nkind: Workflow\n" + okBody, "apiVersion",
			`apiVersion "orkstack/v2" is not supported: this engine reads apiVersion: orkstack/v1`},
	} {
		field, message := headerError(t, tc.source)
		if field != tc.field || message != tc.want {
			t.Errorf("%q → field %q, %q; want %s, %q", tc.source, field, message, tc.field, tc.want)
		}
	}
	// H2: the root is not a mapping.
	for value, got := range map[string]string{"[a]": "a list", `"x"`: `"x"`, "1": "the number 1", "~": "null"} {
		field, message := headerError(t, value+"\n")
		want := `an Orkflow is a YAML mapping that begins with "apiVersion: orkstack/v1" and "kind: Orkflow", got ` + got
		if field != "" || message != want {
			t.Errorf("%s → field %q, %q; want no field, %q", value, field, message, want)
		}
	}
}

// TestCompile_ChecksTheHeaderBeforeTheBody: an automation document is named
// for what it is, not refused key by key ("field settings not found").
func TestCompile_ChecksTheHeaderBeforeTheBody(t *testing.T) {
	raw, err := os.ReadFile("../../examples/automation/examples.ork.yaml")
	if err != nil {
		t.Fatal(err)
	}
	field, message := headerError(t, string(raw))
	if field != "kind" || !strings.HasPrefix(message, "kind: Automation is an automation document, not an Orkflow") {
		t.Fatalf("field %q, %q; want kind, the automation sentence", field, message)
	}
}

// TestCompile_AcceptsTheHeaderAnywhereAtTheTopLevel: key order is not
// semantic, so the compiler accepts the two keys anywhere, quoted or not.
func TestCompile_AcceptsTheHeaderAnywhereAtTheTopLevel(t *testing.T) {
	for name, source := range map[string]string{
		"after name:": "name: x\napiVersion: orkstack/v1\nkind: Orkflow\n" + strings.TrimPrefix(okBody, "name: x\n"),
		"quoted":      "apiVersion: \"orkstack/v1\"\nkind: 'Orkflow'\n" + okBody,
		"reordered":   "kind: Orkflow\napiVersion: orkstack/v1\n" + okBody,
		"last":        okBody + "apiVersion: orkstack/v1\nkind: Orkflow\n",
		"tagged":      "apiVersion: !!str orkstack/v1\nkind: Orkflow\n" + okBody,
	} {
		if _, err := dsl.Compile([]byte(source)); err != nil {
			t.Errorf("%s: %v", name, err)
		}
	}
}

func TestDescribeNode_IsAutomationsVocabulary(t *testing.T) {
	for source, want := range map[string]string{
		"{a: b}": "a mapping",
		"[a]":    "a list",
		"~":      "null",
		"1":      "the number 1",
		"1.5":    "the number 1.5",
		"true":   "the boolean true",
		"x":      `"x"`,
		`"1"`:    `"1"`,
	} {
		var doc yaml.Node
		if err := yaml.Unmarshal([]byte("v: "+source+"\n"), &doc); err != nil {
			t.Fatal(err)
		}
		if got := dsl.DescribeNode(doc.Content[0].Content[1]); got != want {
			t.Errorf("DescribeNode(%s) = %q, want %q", source, got, want)
		}
	}
	var doc yaml.Node
	if err := yaml.Unmarshal([]byte("a: &x 1\nb: *x\n"), &doc); err != nil {
		t.Fatal(err)
	}
	if got := dsl.DescribeNode(doc.Content[0].Content[3]); got != "an alias" {
		t.Errorf("DescribeNode(alias) = %q", got)
	}
}

// TestAPIVersion_IsSpelledOnce: the value every authored document names is
// spelled in one place in the Go tree, internal/dsl/header.go.
func TestAPIVersion_IsSpelledOnce(t *testing.T) {
	var found []string
	for _, root := range []string{"../../cmd", "../../internal", "../../pkg"} {
		err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if d.IsDir() || !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
				return nil
			}
			file, err := parser.ParseFile(token.NewFileSet(), path, nil, 0)
			if err != nil {
				return err
			}
			ast.Inspect(file, func(n ast.Node) bool {
				if lit, ok := n.(*ast.BasicLit); ok && lit.Kind == token.STRING {
					if value, err := strconv.Unquote(lit.Value); err == nil && value == "orkstack/v1" {
						found = append(found, filepath.ToSlash(path))
					}
				}
				return true
			})
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	if len(found) != 1 || found[0] != "../../internal/dsl/header.go" {
		t.Fatalf(`"orkstack/v1" is spelled in %q, want exactly once in internal/dsl/header.go`, found)
	}
}

// TestCompile_SchemaVectorsCompile: the two headered vectors the UI test
// validates are documents this compiler accepts. A vector the compiler
// refuses would be a false example on the UI test's list.
func TestCompile_SchemaVectorsCompile(t *testing.T) {
	paths, err := filepath.Glob("testdata/schema-vectors/*.ork.yaml")
	if err != nil || len(paths) != 2 {
		t.Fatalf("glob schema vectors: %v (%d found)", err, len(paths))
	}
	for _, path := range paths {
		raw, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := dsl.Compile(raw); err != nil {
			t.Errorf("%s: %v", path, err)
		}
	}
}
