package dsl_test

import (
	"encoding/json/v2"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"go.yaml.in/yaml/v4"

	"github.com/orkstack/orkstack/internal/dsl"
	"github.com/orkstack/orkstack/internal/stablejson"
)

// strictGolden is the strict mapping's answer for every document of
// strictGoldenSources, recorded when OrkStack read YAML with yaml.v3. A change
// of YAML library must leave every answer as it was: the same JSON, or the
// same refusal at the same path and line for the same reason.
const strictGolden = "testdata/strict_mapping.golden.json"

type strictAnswer struct {
	Source string `json:"source"`
	JSON   string `json:"json,omitempty"`
	Path   string `json:"path,omitempty"`
	Line   int    `json:"line,omitzero"`
	Reason string `json:"reason,omitempty"`
	// Unread is a source the YAML parser refuses before the strict mapping
	// sees it.
	Unread bool `json:"unread,omitzero"`
}

// strictGoldenSources is every document the agreement test and its fuzz
// target seed: each seed as a value, as a key and in a flow and a block
// collection, each fuzz seed in all five of its shapes, and the structural
// refusals.
func strictGoldenSources() []string {
	var out []string
	for _, text := range agreementSeeds {
		out = append(out, "v: "+text+"\n")
		if text != "" {
			out = append(out, text+": v\n")
		}
		out = append(out, "l: ["+text+"]\nm:\n  k: "+text+"\n")
	}
	for i, a := range agreementSeeds {
		b := agreementSeeds[(i+7)%len(agreementSeeds)]
		out = append(out,
			"v: "+a+"\nw: "+b+"\n",
			a+": "+b+"\n",
			"l:\n  - "+a+"\n  - "+b+"\n",
			"m:\n  "+a+": 1\n  "+b+": 2\n",
			"n:\n  k:\n    "+a+": "+b+"\n",
		)
	}
	return append(out,
		"a: 1\na: 2\n",
		"1: a\n\"1\": b\n",
		"a: &x 1\nb: *x\n",
		"base: &b {k: 1}\nother:\n  <<: *b\n  j: 2\n",
		"a: !!str 1\n",
		"a: !custom x\n",
		"a: &base {k: 1}\n",
		"<<: {k: 1}\n",
		"a: <<\n",
		"a: '-0'\nb: \"0755\"\nc: |\n  yes\n",
		"? [a]\n: b\n",
		"~: x\n",
		"v: !\n",
		"v: ! x\n",
		"v: ! 1\n",
		"v: ! yes\n",
		"v: ! -0\n",
		"! k: v\n",
		"! 1: v\n",
		"v: ! {a: 1}\n",
		"v: ! [1]\n",
		"l:\n  - 0\n  - !\n",
	)
}

func strictAnswerOf(t *testing.T, source string) strictAnswer {
	t.Helper()
	answer := strictAnswer{Source: source}
	var doc yaml.Node
	if err := yaml.Unmarshal([]byte(source), &doc); err != nil || len(doc.Content) == 0 || doc.Content[0].Kind != yaml.MappingNode {
		answer.Unread = true
		return answer
	}
	got, err := dsl.AuthoredJSON(doc.Content[0], "with")
	if err != nil {
		var refused *dsl.AuthoredDataError
		if !errors.As(err, &refused) {
			t.Fatalf("%q: the strict mapping refused it with %T: %v", source, err, err)
		}
		answer.Path, answer.Line, answer.Reason = refused.Path, refused.Line, refused.Reason
		return answer
	}
	encoded, err := stablejson.Marshal(got)
	if err != nil {
		t.Fatalf("%q: %v", source, err)
	}
	answer.JSON = string(encoded)
	return answer
}

// TestStrictJSON_AnswersAsRecorded holds the strict mapping to the answers it
// gave under yaml.v3, document by document.
func TestStrictJSON_AnswersAsRecorded(t *testing.T) {
	sources := strictGoldenSources()
	answers := make([]strictAnswer, len(sources))
	for i, source := range sources {
		answers[i] = strictAnswerOf(t, source)
	}
	if *updateCompat {
		encoded, err := stablejson.MarshalIndent(answers, "  ")
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.FromSlash(strictGolden), append(encoded, '\n'), 0o644); err != nil {
			t.Fatal(err)
		}
		return
	}
	raw, err := os.ReadFile(filepath.FromSlash(strictGolden))
	if err != nil {
		t.Fatal(err)
	}
	var recorded []strictAnswer
	if err := json.Unmarshal(raw, &recorded); err != nil {
		t.Fatalf("%s: %v", strictGolden, err)
	}
	if len(recorded) != len(answers) {
		t.Fatalf("%s holds %d answers for %d documents", strictGolden, len(recorded), len(answers))
	}
	for i, want := range recorded {
		got := answers[i]
		if want.Unread && !got.Unread && got.Source == want.Source {
			// go-yaml v4 reads a block entry with an empty key (": x") as a
			// null key, where yaml.v3 refused it as a syntax error. Either
			// way the document is refused, never given a value.
			if got.JSON != "" {
				t.Errorf("%q: yaml.v3 could not read it, and the strict mapping now accepts it as %s", want.Source, got.JSON)
			}
			continue
		}
		if got.Source != want.Source || got.JSON != want.JSON || got.Path != want.Path ||
			got.Line != want.Line || got.Reason != want.Reason || got.Unread != want.Unread {
			t.Errorf("%q:\n got %+v\nwant %+v", want.Source, got, want)
		}
	}
}
