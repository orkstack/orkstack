package dsl_test

import (
	"errors"
	"strings"
	"testing"

	"go.yaml.in/yaml/v4"

	"github.com/orkstack/orkstack/internal/dsl"
)

const mergeTagHead = "apiVersion: orkstack/v1\nkind: Orkflow\nname: m\n"

// TestCompile_RefusesAMergeTagWhereTheDecoderWouldDropIt: an explicit
// !!merge tag, on a key or a value, and a << written as a value that is not a
// string are refused at their path and line, never dropped (go-yaml reads
// them as a merge, which for a value means nothing at all).
func TestCompile_RefusesAMergeTagWhereTheDecoderWouldDropIt(t *testing.T) {
	for _, tc := range []struct {
		name, body, field, sentence string
	}{
		{"a tagged integer value", "tasks:\n  - id: a\n    uses: shell\n    timeout_seconds: !!merge 3\n",
			"tasks[0].timeout_seconds", "line 7: tasks[0].timeout_seconds: the tag !!merge is not allowed"},
		{"a tagged string value", "description: !!merge x\ntasks:\n  - id: a\n    uses: shell\n",
			"description", "line 4: description: the tag !!merge is not allowed"},
		{"a << for an integer", "tasks:\n  - id: a\n    uses: shell\n    timeout_seconds: <<\n",
			"tasks[0].timeout_seconds", "line 7: tasks[0].timeout_seconds: << is a merge key, not a value"},
		{"a << for a list", "tasks:\n  - id: a\n    uses: shell\n    needs: <<\n",
			"tasks[0].needs", "line 7: tasks[0].needs: << is a merge key, not a value"},
		{"a tagged key", "tasks:\n  - id: a\n    uses: shell\n    !!merge needs: []\n",
			"tasks[0].needs", "line 7: tasks[0].needs: the tag !!merge is not allowed"},
		{"a tagged value a merge delivers", "pins:\n  p: &p {timeout_seconds: !!merge 3}\n  q:\n    <<: *p\n" +
			"tasks:\n  - id: a\n    uses: shell\n    pin: p\n  - id: b\n    uses: shell\n    pin: q\n",
			"pins.p.timeout_seconds", "line 5: pins.p.timeout_seconds: the tag !!merge is not allowed"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := dsl.Compile([]byte(mergeTagHead + tc.body))
			if err == nil {
				t.Fatal("Compile accepted it")
			}
			var verr *dsl.ValidationError
			if !errors.As(err, &verr) || verr.Field != tc.field {
				t.Errorf("error %v is not at %s", err, tc.field)
			}
			if !strings.Contains(err.Error(), tc.sentence) {
				t.Errorf("error %q does not say %q", err, tc.sentence)
			}
		})
	}
}

// TestCompile_KeepsThePlainMergeKeyAndTheStrictMappingsWords: a plain <<:
// still merges in a typed position, and in a data position the strict
// mapping's own refusal still answers.
func TestCompile_KeepsThePlainMergeKeyAndTheStrictMappingsWords(t *testing.T) {
	wf, err := dsl.Compile([]byte(mergeTagHead + "pins:\n  p: &p {timeout_seconds: 30}\n  q:\n    <<: *p\n" +
		"tasks:\n  - id: a\n    uses: shell\n    pin: p\n  - id: b\n    uses: shell\n    pin: q\n"))
	if err != nil {
		t.Fatalf("a plain <<: merge: %v", err)
	}
	for _, pin := range wf.GetPins() {
		if pin.GetName() == "q" && pin.GetTimeoutSeconds() != 30 {
			t.Errorf("pin q = %v, want the merged timeout 30", pin)
		}
	}
	// A << for a string is the text <<, as it always was.
	wf, err = dsl.Compile([]byte(mergeTagHead + "description: <<\ntasks:\n  - id: a\n    uses: shell\n"))
	if err != nil || wf.GetDescription() != "<<" {
		t.Errorf("description: << = %q, %v; want the text <<", wf.GetDescription(), err)
	}
	_, err = dsl.Compile([]byte(mergeTagHead + "tasks:\n  - id: a\n    uses: shell\n    with: {a: <<}\n"))
	if err == nil || !strings.Contains(err.Error(), "a merge key (<<) is not allowed in authored data; write the keys out") {
		t.Errorf("a << in with: = %v, want the strict mapping's refusal", err)
	}
	_, err = dsl.Compile([]byte(mergeTagHead + "tasks:\n  - id: a\n    uses: shell\n    with: {a: !!merge x}\n"))
	if err == nil || !strings.Contains(err.Error(), "the tag !!merge is not allowed in authored data") {
		t.Errorf("a tagged !!merge in with: = %v, want the strict mapping's refusal", err)
	}
}

// TestParsers_RefuseAMergeTagTheDecoderWouldDrop: the activity interface and
// the activity catalog refuse it the same way.
func TestParsers_RefuseAMergeTagTheDecoderWouldDrop(t *testing.T) {
	const iface = "apiVersion: orkstack/v1\nkind: ActivityInterface\n"
	if _, err := dsl.ParseInterfaceDocument([]byte(iface + "fields:\n  a:\n    type: !!merge string\n")); err == nil ||
		!strings.Contains(err.Error(), "line 5: fields.a.type: the tag !!merge is not allowed") {
		t.Errorf("interface = %v", err)
	}
	if _, err := dsl.ParseCatalog([]byte("activities:\n  shell: !!merge ghcr.io/acme/shell:1\n")); err == nil ||
		!strings.Contains(err.Error(), "ORK_ACTIVITY_CATALOG: line 2: activities.shell: the tag !!merge is not allowed") {
		t.Errorf("catalog = %v", err)
	}
}

// TestCompile_ReadsTheNonSpecificTagAsAString: `! orkstack/v1` is the string
// orkstack/v1, as YAML says.
func TestCompile_ReadsTheNonSpecificTagAsAString(t *testing.T) {
	if _, err := dsl.Compile([]byte("apiVersion: ! orkstack/v1\nkind: ! Orkflow\nname: m\ntasks:\n  - id: a\n    uses: shell\n")); err != nil {
		t.Errorf("a header tagged !: %v", err)
	}
}

// mergeTagShape is a document type of every shape the walk descends: a
// struct, a pointer, a list, a map, a string, an integer and a data position.
type mergeTagShape struct {
	S     string                   `yaml:"s"`
	N     int                      `yaml:"n"`
	P     *int32                   `yaml:"p"`
	L     []mergeTagShape          `yaml:"l"`
	M     map[string]mergeTagShape `yaml:"m"`
	Data  yaml.Node                `yaml:"data"`
	Names []string                 `yaml:"names"`
}

// FuzzRefuseMergeTags_NamesAMergeOnItsLine: RefuseMergeTags never panics, and
// a refusal is a MergeTagError whose line is a line of the source that holds
// the merge it names (the tag or the <<), with one of its two reasons.
func FuzzRefuseMergeTags_NamesAMergeOnItsLine(f *testing.F) {
	for _, seed := range []string{
		"", "s: x\n", "n: !!merge 3\n", "n: <<\n", "s: <<\n", "!!merge s: x\n", "l:\n  - n: <<\n",
		"m:\n  a: {p: !!merge 1}\n", "a: &a {n: !!merge 1}\nm:\n  b:\n    <<: *a\n", "m:\n  <<: [{a: {n: <<}}]\n",
		"data: {k: <<}\n", "data: !!merge x\n", "names: [<<]\n", "names: <<\n", "x: [\n", "*a\n", "l: [*x]\n",
	} {
		f.Add([]byte(seed))
	}
	f.Fuzz(func(t *testing.T, source []byte) {
		err := dsl.RefuseMergeTags(source, mergeTagShape{})
		if err == nil {
			return
		}
		var refused *dsl.MergeTagError
		if !errors.As(err, &refused) {
			t.Fatalf("RefuseMergeTags(%q) = %T %v", source, err, err)
		}
		if !strings.Contains(refused.Reason, "!!merge") && !strings.Contains(refused.Reason, "<<") {
			t.Fatalf("RefuseMergeTags(%q) gives the reason %q", source, refused.Reason)
		}
		lines := strings.Split(strings.NewReplacer("\r\n", "\n", "\r", "\n").Replace(string(source)), "\n")
		if refused.Line < 1 || refused.Line > len(lines) {
			t.Fatalf("RefuseMergeTags(%q) names line %d of %d", source, refused.Line, len(lines))
		}
		if text := lines[refused.Line-1]; !strings.Contains(text, "!!merge") && !strings.Contains(text, "<<") &&
			!strings.Contains(text, "!<tag:yaml.org,2002:merge>") {
			t.Fatalf("RefuseMergeTags(%q) names line %d, %q, which holds no merge", source, refused.Line, text)
		}
	})
}
