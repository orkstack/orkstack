package dsl_test

import (
	"strconv"
	"strings"
	"testing"

	"github.com/orkstack/orkstack/internal/dsl"
)

const singleOrkflow = "apiVersion: orkstack/v1\nkind: Orkflow\nname: a\ntasks:\n  - id: t\n    await:\n      timeout_seconds: 60\n"

// secondDocumentCases are files that hold a second YAML document, and the line
// it begins on. singleOrkflow is 7 lines.
var secondDocumentCases = []struct {
	name   string
	source string
	line   int
}{
	{"a second Orkflow", singleOrkflow + "---\n" + singleOrkflow, 8},
	{"a second document that does not parse", singleOrkflow + "---\nname: [unclosed\n", 8},
	{"a second document with bad indentation", singleOrkflow + "---\na: b: c\n  - d\n", 8},
	{"a second document with a tab", singleOrkflow + "---\n\tname: a\n", 8},
	{"an empty second document", singleOrkflow + "---\n", 8},
	{"a second document after a leading marker", "---\n" + singleOrkflow + "---\nname: b\n", 9},
	{"a second document after comments", "# one\n---\n# two\n" + singleOrkflow + "\n# three\n---\nname: b\n", 13},
	{"a marker with a comment", singleOrkflow + "--- # the next one\nname: b\n", 8},
	{"content on the marker's line", singleOrkflow + "--- {name: b}\n", 8},
	{"a document after an end marker, with no start marker", singleOrkflow + "...\nname: b\n", 9},
	{"a start marker after an end marker", singleOrkflow + "...\n---\nname: b\n", 9},
	{"CRLF line ends", strings.ReplaceAll(singleOrkflow, "\n", "\r\n") + "---\r\nname: b\r\n", 8},
}

// singleDocumentCases hold one document, whatever else they hold.
var singleDocumentCases = []struct {
	name   string
	source string
}{
	{"plain", singleOrkflow},
	{"a leading marker", "---\n" + singleOrkflow},
	{"comments and directives first", "# one\n%YAML 1.1\n---\n" + singleOrkflow},
	{"an end marker", singleOrkflow + "...\n"},
	{"an end marker and a comment", singleOrkflow + "...\n# done\n"},
	{"a trailing comment", singleOrkflow + "# ---\n"},
	{"marker-like text in a block scalar", "apiVersion: orkstack/v1\nkind: Orkflow\nname: a\ndescription: |\n  ---\n  x\n  ...\ntasks:\n  - id: t\n    await:\n      timeout_seconds: 60\n"},
	{"marker-like text in a quoted string", "apiVersion: orkstack/v1\nkind: Orkflow\nname: a\ndescription: \"--- x\"\ntasks:\n  - id: t\n    await:\n      timeout_seconds: 60\n"},
	{"three dashes that are no marker", "apiVersion: orkstack/v1\nkind: Orkflow\nname: a\nlabels:\n  ---x: y\ntasks:\n  - id: t\n    await:\n      timeout_seconds: 60\n"},
}

func TestSecondDocumentLine(t *testing.T) {
	for _, tc := range secondDocumentCases {
		t.Run(tc.name, func(t *testing.T) {
			line, found := dsl.SecondDocumentLine([]byte(tc.source))
			if !found || line != tc.line {
				t.Fatalf("SecondDocumentLine = %d, %v, want line %d", line, found, tc.line)
			}
		})
	}
	for _, tc := range singleDocumentCases {
		t.Run(tc.name, func(t *testing.T) {
			if line, found := dsl.SecondDocumentLine([]byte(tc.source)); found {
				t.Fatalf("SecondDocumentLine found a second document on line %d of\n%s", line, tc.source)
			}
		})
	}
	// A file whose first document does not parse has no second document to
	// report: the parser's own refusal names that.
	for _, source := range []string{"", "# only a comment\n", "name: [unclosed\n"} {
		if line, found := dsl.SecondDocumentLine([]byte(source)); found {
			t.Fatalf("SecondDocumentLine(%q) = line %d", source, line)
		}
	}
}

// TestCompile_RefusesASecondDocument: an author who concatenates two Orkflows,
// or leaves anything after a marker, is told, on the line it begins; nothing
// after the first document is ignored in silence.
func TestCompile_RefusesASecondDocument(t *testing.T) {
	for _, tc := range secondDocumentCases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := dsl.Compile([]byte(tc.source))
			requireSecondDocumentRefusal(t, err, "one Orkflow per file", tc.line)
		})
	}
	for _, tc := range singleDocumentCases {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := dsl.Compile([]byte(tc.source)); err != nil && strings.Contains(err.Error(), "second YAML document") {
				t.Fatalf("Compile refused a single document: %v", err)
			}
		})
	}
}

const singleInterface = "apiVersion: orkstack/v1\nkind: ActivityInterface\nfields:\n  src:\n    type: string\n"

func TestParseActivityDocument_RefusesASecondDocument(t *testing.T) {
	for _, tc := range []struct {
		name, rest string
	}{
		{"a second interface", "---\n" + singleInterface},
		{"a second document that does not parse", "---\nfields: [unclosed\n"},
		{"an empty second document", "---\n"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := dsl.ParseActivityDocument([]byte(singleInterface + tc.rest))
			requireSecondDocumentRefusal(t, err, "one activity interface per file", 6)
		})
	}
}

func requireSecondDocumentRefusal(t *testing.T, err error, what string, line int) {
	t.Helper()
	if err == nil {
		t.Fatal("the file was accepted")
	}
	want := "line " + strconv.Itoa(line) + ": " + what + ": a second YAML document begins here"
	if !strings.Contains(err.Error(), want) {
		t.Fatalf("refusal = %q, want it to hold %q", err, want)
	}
}

// TestParseCatalog_RefusesASecondDocument: an activity after the first document
// would be one a deployment believes it pinned.
func TestParseCatalog_RefusesASecondDocument(t *testing.T) {
	const first = "activities:\n  shell: ghcr.io/x/y:1\n"
	for name, rest := range map[string]string{
		"a second document":                     "---\nactivities:\n  file: ghcr.io/x/z:1\n",
		"a second document that does not parse": "---\nactivities: [\n",
		"a tab the first read trips on":         "---\n\tactivities: {}\n",
		"an empty second document":              "---\n",
	} {
		t.Run(name, func(t *testing.T) {
			_, err := dsl.ParseCatalog([]byte(first + rest))
			want := "ORK_ACTIVITY_CATALOG: line 3: a second YAML document begins here"
			if err == nil || !strings.Contains(err.Error(), want) {
				t.Fatalf("ParseCatalog = %v, want a refusal holding %q", err, want)
			}
		})
	}
	if catalog, err := dsl.ParseCatalog([]byte("---\n" + first + "...\n")); err != nil || catalog["shell"] == "" {
		t.Fatalf("ParseCatalog of one document = %v, %v", catalog, err)
	}
}
