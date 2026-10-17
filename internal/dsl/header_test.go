package dsl_test

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"testing"

	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"gopkg.in/yaml.v3"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/dsl"
)

const preHeaderDir = "testdata/pre-header"

// preHeaderSource is how a pre-header vector is compiled since the header is
// required: with the two lines added where OrkStack shows them (AddHeader).
func preHeaderSource(t *testing.T, raw []byte) []byte {
	t.Helper()
	added, ok := dsl.AddHeader(string(raw))
	if !ok {
		t.Fatalf("AddHeader refused a pre-header vector:\n%s", raw)
	}
	return []byte(added)
}

// TestCompile_PreHeaderVectorsKeepTheirDigests compiles documents written
// before the header existed and holds each to the digest, version and
// canonical form recorded at the base compiler, and to the compiled message
// recorded then (source_yaml aside). The vectors are pre-header documents by
// design and are never given a header in the repository.
// Regenerate with: go test ./internal/dsl -run '^TestCompile_PreHeaderVectorsKeepTheirDigests$' -update
func TestCompile_PreHeaderVectorsKeepTheirDigests(t *testing.T) {
	paths, err := filepath.Glob(filepath.Join(preHeaderDir, "*.yaml"))
	if err != nil || len(paths) == 0 {
		t.Fatalf("glob vectors: %v (%d found)", err, len(paths))
	}
	sort.Strings(paths)
	var got strings.Builder
	for _, path := range paths {
		raw, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		wf, err := dsl.Compile(preHeaderSource(t, raw))
		if err != nil {
			t.Fatalf("%s: %v", path, err)
		}
		digest, err := dsl.DefinitionDigest(wf)
		if err != nil {
			t.Fatal(err)
		}
		canonical, err := dsl.CanonicalOrkflow(wf)
		if err != nil {
			t.Fatal(err)
		}
		sum := sha256.Sum256(canonical)
		fmt.Fprintf(&got, "%s %s %s %s\n", filepath.Base(path), digest, wf.GetVersion(), hex.EncodeToString(sum[:]))

		bare := proto.Clone(wf).(*orkstackv1.Orkflow)
		bare.SourceYaml = ""
		compiledPath := strings.TrimSuffix(path, ".yaml") + ".compiled.json"
		if *updateCompat {
			out, err := protojson.MarshalOptions{Multiline: true}.Marshal(bare)
			if err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(compiledPath, append(out, '\n'), 0o644); err != nil {
				t.Fatal(err)
			}
		}
		recorded, err := os.ReadFile(compiledPath)
		if err != nil {
			t.Fatalf("read %s: %v (record it with -update)", compiledPath, err)
		}
		var want orkstackv1.Orkflow
		if err := protojson.Unmarshal(recorded, &want); err != nil {
			t.Fatalf("%s: %v", compiledPath, err)
		}
		if !proto.Equal(&want, bare) {
			t.Errorf("%s: the compiled message moved:\n got %v\nwant %v", path, bare, &want)
		}
	}
	golden := filepath.Join(preHeaderDir, "digests.golden")
	if *updateCompat {
		if err := os.WriteFile(golden, []byte(got.String()), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	want, err := os.ReadFile(golden)
	if err != nil {
		t.Fatalf("read %s: %v (record it with -update)", golden, err)
	}
	if got.String() != string(want) {
		t.Fatalf("pre-header digests moved:\n got:\n%s\nwant:\n%s", got.String(), want)
	}
}

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

const bom = "\xEF\xBB\xBF"

func TestAddHeader_PlacesTheHeaderAfterLeadingComments(t *testing.T) {
	H := dsl.OrkflowHeader
	crlf := strings.ReplaceAll(H, "\n", "\r\n")
	for _, tc := range []struct {
		name, in, want string
		ok             bool
	}{
		{"no comments", okBody, H + okBody, true},
		{"comments", "# a\n# b\n" + okBody, "# a\n# b\n" + H + okBody, true},
		{"blank lines and comments", "\n# a\n\n" + okBody, "\n# a\n\n" + H + okBody, true},
		{"document start", "---\n" + okBody, "---\n" + H + okBody, true},
		{"document start with a comment", "--- # doc\n" + okBody, "--- # doc\n" + H + okBody, true},
		{"%YAML 1.1 then ---", "# c\n%YAML 1.1\n---\n" + okBody, "# c\n%YAML 1.1\n---\n" + H + okBody, true},
		{"%YAML 1.2 is refused by yaml.v3", "%YAML 1.2\n---\n" + okBody, "%YAML 1.2\n---\n" + okBody, false},
		{"a flow root", "{name: x, tasks: []}\n", "{name: x, tasks: []}\n", false},
		{"apiVersion present", "apiVersion: orkstack/v1\n" + okBody, "apiVersion: orkstack/v1\n" + okBody, false},
		{"kind present", okBody + "kind: Orkflow\n", okBody + "kind: Orkflow\n", false},
		{"first key not at column 1", "  name: x\n  tasks: []\n", "  name: x\n  tasks: []\n", false},
		{"CRLF", "# a\r\n" + strings.ReplaceAll(okBody, "\n", "\r\n"), "# a\r\n" + crlf + strings.ReplaceAll(okBody, "\n", "\r\n"), true},
		{"not YAML", "name: [\n", "name: [\n", false},
		{"empty", "", "", false},
		{"invalid UTF-8", "name: \xff\ntasks: []\n", "name: \xff\ntasks: []\n", false},
		{"a byte order mark", bom + okBody, bom + H + okBody, true},
		{"a byte order mark and comments", bom + "# c\n" + okBody, bom + "# c\n" + H + okBody, true},
	} {
		got, ok := dsl.AddHeader(tc.in)
		if got != tc.want || ok != tc.ok {
			t.Errorf("%s: AddHeader = %q, %v; want %q, %v", tc.name, got, ok, tc.want, tc.ok)
		}
	}
}

func topLevelKeys(t *testing.T, source string) []string {
	t.Helper()
	var doc yaml.Node
	if err := yaml.Unmarshal([]byte(source), &doc); err != nil {
		t.Fatal(err)
	}
	var keys []string
	for i := 0; i < len(doc.Content[0].Content); i += 2 {
		keys = append(keys, doc.Content[0].Content[i].Value)
	}
	return keys
}

func TestAddHeader_KeepsAByteOrderMarkInFront(t *testing.T) {
	for _, in := range []string{bom + okBody, bom + "# c\n" + okBody} {
		added, ok := dsl.AddHeader(in)
		if !ok {
			t.Fatalf("AddHeader refused %q", in)
		}
		keys := topLevelKeys(t, added)
		if want := []string{"apiVersion", "kind", "name", "tasks"}; strings.Join(keys, ",") != strings.Join(want, ",") {
			t.Errorf("keys = %q, want %q", keys, want)
		}
		for _, key := range keys {
			if strings.Contains(key, bom) {
				t.Errorf("key %q holds the byte order mark", key)
			}
		}
		if _, err := dsl.Compile([]byte(added)); err != nil {
			t.Errorf("Compile(%q): %v", added, err)
		}
		if got := string(dsl.VersionedSource([]byte(added))); got != in {
			t.Errorf("VersionedSource(AddHeader(x)) = %q, want %q", got, in)
		}
		rest := strings.TrimPrefix(in, bom)
		if got := string(dsl.VersionedSource([]byte(bom + dsl.OrkflowHeader + rest))); got != in {
			t.Errorf("VersionedSource(BOM + header + rest) = %q, want %q", got, in)
		}
	}
	if _, ok := dsl.AddHeader("\xff\xfe" + okBody); ok {
		t.Error("AddHeader gave a header to a source that is not valid UTF-8")
	}
}

func TestVersionedSource_FactorsOutTheHeaderAmongLeadingLines(t *testing.T) {
	const legacy = "# one\n# two\n" + okBody
	H := dsl.OrkflowHeader
	for _, newline := range []string{"\n", "\r\n"} {
		conv := func(s string) string { return strings.ReplaceAll(s, "\n", newline) }
		for name, placed := range map[string]string{
			"above":   H + legacy,
			"between": "# one\n" + H + "# two\n" + okBody,
			"below":   "# one\n# two\n" + H + okBody,
		} {
			if got := string(dsl.VersionedSource([]byte(conv(placed)))); got != conv(legacy) {
				t.Errorf("%s (%q): VersionedSource = %q, want %q", name, newline, got, conv(legacy))
			}
			if dsl.HeaderOutOfForm(conv(placed)) {
				t.Errorf("%s (%q): HeaderOutOfForm is true for a header in form", name, newline)
			}
		}
	}
	for name, placed := range map[string]string{
		"after name:":            "# one\n# two\nname: x\n" + H + strings.TrimPrefix(okBody, "name: x\n"),
		"quoted":                 "# one\n# two\napiVersion: \"orkstack/v1\"\nkind: Orkflow\n" + okBody,
		"reordered":              "# one\n# two\nkind: Orkflow\napiVersion: orkstack/v1\n" + okBody,
		"a trailing comment":     "# one\n# two\napiVersion: orkstack/v1\nkind: Orkflow # x\n" + okBody,
		"a comment between them": "# one\n# two\napiVersion: orkstack/v1\n# between\nkind: Orkflow\n" + okBody,
	} {
		if got := string(dsl.VersionedSource([]byte(placed))); got != placed {
			t.Errorf("%s: VersionedSource removed something: %q", name, got)
		}
		if !dsl.HeaderOutOfForm(placed) {
			t.Errorf("%s: HeaderOutOfForm is false", name)
		}
	}
	if dsl.HeaderOutOfForm(legacy) {
		t.Error("HeaderOutOfForm is true for a source with no header")
	}
}

func TestEmittedSource_IsIdempotent(t *testing.T) {
	for _, s := range headerSeeds(t) {
		once := dsl.EmittedSource(s)
		if twice := dsl.EmittedSource(once); twice != once {
			t.Errorf("EmittedSource is not idempotent on %q: %q then %q", s, once, twice)
		}
		if strings.Contains(s, "apiVersion") && dsl.HeaderOutOfForm(s) && once != s {
			t.Errorf("EmittedSource changed a source that declares the header: %q → %q", s, once)
		}
	}
	if got := dsl.EmittedSource(""); got != "" {
		t.Errorf("EmittedSource(\"\") = %q", got)
	}
	if got := dsl.EmittedSource("name@1.0.0"); got != "name@1.0.0" {
		t.Errorf("EmittedSource changed a source that is not a block mapping: %q", got)
	}
}

func TestVersionExistsAdvice_NamesTheHeaderOnlyWhenItIsOutOfForm(t *testing.T) {
	const today = "a version is immutable, so publish a new version"
	for _, s := range []string{okBody, "# c\n" + dsl.OrkflowHeader + okBody, dsl.OrkflowHeader + "# c\n" + okBody} {
		if got := dsl.VersionExistsAdvice(s); got != today {
			t.Errorf("VersionExistsAdvice(%q) = %q, want today's words", s, got)
		}
	}
	const header = `its header is not the two lines "apiVersion: orkstack/v1" and "kind: Orkflow" written first (after any leading comments) — the one form a version leaves out; if the header is all you changed, write it that way; otherwise a version is immutable, so publish a new version`
	for _, s := range []string{
		"name: x\n" + dsl.OrkflowHeader + "tasks: []\n",
		"apiVersion: 'orkstack/v1'\nkind: Orkflow\n" + okBody,
		"kind: Orkflow\napiVersion: orkstack/v1\n" + okBody,
		"apiVersion: orkstack/v1\nkind: Orkflow # x\n" + okBody,
		"apiVersion: orkstack/v1\n# c\nkind: Orkflow\n" + okBody,
	} {
		if got := dsl.VersionExistsAdvice(s); got != header {
			t.Errorf("VersionExistsAdvice(%q) = %q, want the header sentence", s, got)
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

// headerSeeds is the seed corpus of the header properties: every shipped
// example as it is stored before the header (its header removed) — in LF and
// CRLF, with and without a byte order mark — the AddHeader table's inputs, a
// directive document and a byte-order-marked source with its header on line 1.
func headerSeeds(t testing.TB) []string {
	paths, err := filepath.Glob("../../examples/*.ork.yaml")
	if err != nil || len(paths) == 0 {
		t.Fatalf("glob examples: %v", err)
	}
	var seeds []string
	for _, path := range paths {
		raw, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		legacy := string(dsl.VersionedSource(raw))
		crlf := strings.ReplaceAll(legacy, "\n", "\r\n")
		seeds = append(seeds, string(raw), legacy, crlf, bom+legacy, bom+crlf)
	}
	return append(seeds, okBody, "# a\n# b\n"+okBody, "---\n"+okBody, "# c\n%YAML 1.1\n---\n"+okBody,
		"{name: x, tasks: []}\n", "  name: x\n", "name: [\n", "", bom+dsl.OrkflowHeader+"# c\n"+okBody,
		"name: x\n"+dsl.OrkflowHeader+"tasks: []\n")
}

func FuzzHeader_AddThenVersionedIsTheIdentity(f *testing.F) {
	for _, s := range headerSeeds(f) {
		f.Add(s)
	}
	f.Fuzz(func(t *testing.T, s string) {
		added, ok := dsl.AddHeader(s)
		if !ok {
			if added != s {
				t.Fatalf("AddHeader changed a source it refused: %q → %q", s, added)
			}
			return
		}
		if got := string(dsl.VersionedSource([]byte(added))); got != s {
			t.Fatalf("VersionedSource(AddHeader(%q)) = %q", s, got)
		}
	})
}

func FuzzEmittedSource_KeepsTheIdentity(f *testing.F) {
	for _, s := range headerSeeds(f) {
		f.Add(s)
	}
	f.Fuzz(func(t *testing.T, s string) {
		shown := dsl.EmittedSource(s)
		if got, want := dsl.VersionedSource([]byte(shown)), dsl.VersionedSource([]byte(s)); string(got) != string(want) {
			t.Fatalf("VersionedSource(EmittedSource(%q)) = %q, want %q", s, got, want)
		}
		if again := dsl.EmittedSource(shown); again != shown {
			t.Fatalf("EmittedSource is not idempotent on %q", s)
		}
	})
}
