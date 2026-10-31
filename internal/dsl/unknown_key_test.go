package dsl_test

import (
	"errors"
	"strings"
	"testing"

	"github.com/orkstack/orkstack/internal/dsl"
)

// TestUnknownKey_NamesTheKeyWhereItStandsAndTheDocument: a key the schema
// does not define is refused in the author's words — the key, the path it
// stands at and the kind of document — at the key's field path and line.
// Never in the decoder's words, which name the Go type the key would have
// decoded into (AGENTS.md §7: nothing internal on a user surface).
func TestUnknownKey_NamesTheKeyWhereItStandsAndTheDocument(t *testing.T) {
	t.Parallel()
	compile := func(source []byte) error {
		_, err := dsl.Compile(source)
		return err
	}
	parseInterface := func(source []byte) error {
		_, err := dsl.ParseInterfaceDocument(source)
		return err
	}
	const orkflow = "apiVersion: orkstack/v1\nkind: Orkflow\nname: typo\n"
	const iface = "apiVersion: orkstack/v1\nkind: ActivityInterface\n"
	tests := []struct {
		name      string
		parse     func([]byte) error
		source    string
		wantField string
		// wantPrefix is the whole sentence up to the allowed keys, and
		// wantAllowed one key the list must offer.
		wantPrefix  string
		wantAllowed string
	}{
		{
			name:  "a task key",
			parse: compile,
			source: orkflow + "tasks:\n  - id: a\n    uses: shell\n    with: {command: \"echo a\"}\n" +
				"  - id: b\n    uses: shell\n    need: [a]\n    with: {command: \"echo b\"}\n",
			wantField:   "tasks[1].need",
			wantPrefix:  `line 10: unknown key "need" in tasks[1] of the Orkflow; allowed: `,
			wantAllowed: "needs",
		},
		{
			name:        "a top-level key",
			parse:       compile,
			source:      orkflow + "tsks:\n  - id: a\n    uses: shell\n",
			wantField:   "tsks",
			wantPrefix:  `line 4: unknown key "tsks" at the top level of the Orkflow; allowed: `,
			wantAllowed: "tasks",
		},
		{
			name:  "a key of a pin",
			parse: compile,
			source: orkflow + "pins:\n  build:\n    timeout_second: 60\n" +
				"tasks:\n  - id: a\n    uses: shell\n    pin: build\n    with: {command: \"echo 1\"}\n",
			wantField:   "pins.build.timeout_second",
			wantPrefix:  `line 6: unknown key "timeout_second" in pins.build of the Orkflow; allowed: `,
			wantAllowed: "timeout_seconds",
		},
		{
			name:  "a key of a task's retry",
			parse: compile,
			source: orkflow + "tasks:\n  - id: a\n    uses: shell\n    with: {command: \"echo 1\"}\n" +
				"    retry:\n      max_attempts: 3\n",
			wantField:   "tasks[0].retry.max_attempts",
			wantPrefix:  `line 9: unknown key "max_attempts" in tasks[0].retry of the Orkflow; allowed: `,
			wantAllowed: "maximum_attempts",
		},
		{
			name:  "a key of a secret",
			parse: compile,
			source: orkflow + "tasks:\n  - id: a\n    uses: shell\n    with: {command: \"echo 1\"}\n" +
				"    secrets:\n      - name: db\n        sorce: env:DB\n",
			wantField:   "tasks[0].secrets[0].sorce",
			wantPrefix:  `line 10: unknown key "sorce" in tasks[0].secrets[0] of the Orkflow; allowed: `,
			wantAllowed: "source",
		},
		{
			name:        "every unknown key, the first in the sentence and the rest after it",
			parse:       compile,
			source:      orkflow + "tsks: 1\n\"a\\nb\": 2\npinz: 3\n",
			wantField:   "tsks",
			wantPrefix:  `line 4: unknown key "tsks" at the top level of the Orkflow (and 2 more unknown keys: "a\nb" at line 5, "pinz" at line 6); allowed: `,
			wantAllowed: "tasks",
		},
		{
			name:  "a second unknown key, on a later line",
			parse: compile,
			source: orkflow + "tasks:\n  - id: a\n    uses: shell\n    with: {command: \"echo a\"}\n" +
				"    retry:\n      max_attempts: 3\n    timeout_second: 60\n",
			wantField:   "tasks[0].retry.max_attempts",
			wantPrefix:  `line 9: unknown key "max_attempts" in tasks[0].retry of the Orkflow (and 1 more unknown key: "timeout_second" at line 10); allowed: `,
			wantAllowed: "maximum_attempts",
		},
		{
			name:  "more than ten unknown keys, the first ten of the rest named",
			parse: compile,
			source: orkflow + "k1: 1\nk2: 1\nk3: 1\nk4: 1\nk5: 1\nk6: 1\nk7: 1\nk8: 1\nk9: 1\nk10: 1\n" +
				"k11: 1\nk12: 1\nk13: 1\n",
			wantField: "k1",
			wantPrefix: `line 4: unknown key "k1" at the top level of the Orkflow (and 12 more unknown keys, the first 10: ` +
				`"k2" at line 5, "k3" at line 6, "k4" at line 7, "k5" at line 8, "k6" at line 9, "k7" at line 10, ` +
				`"k8" at line 11, "k9" at line 12, "k10" at line 13, "k11" at line 14); allowed: `,
			wantAllowed: "tasks",
		},
		{
			name:        "a top-level key of an activity interface",
			parse:       parseInterface,
			source:      iface + "field:\n  a:\n    type: string\n",
			wantField:   "field",
			wantPrefix:  `line 3: unknown key "field" at the top level of the activity interface; allowed: `,
			wantAllowed: "fields",
		},
		{
			name:        "a snake_case key of an activity interface",
			parse:       parseInterface,
			source:      iface + "default_op: get\nops:\n  get: {}\n",
			wantField:   "default_op",
			wantPrefix:  `line 3: unknown key "default_op" at the top level of the activity interface; allowed: `,
			wantAllowed: "defaultOp",
		},
		{
			name:        "a key of an interface field",
			parse:       parseInterface,
			source:      iface + "fields:\n  a:\n    type: string\n    pattern: x\n",
			wantField:   "fields.a.pattern",
			wantPrefix:  `line 6: unknown key "pattern" in fields.a of the activity interface; allowed: `,
			wantAllowed: "required",
		},
		{
			name:        "a key of an op",
			parse:       parseInterface,
			source:      iface + "ops:\n  get:\n    feilds:\n      a:\n        type: string\n",
			wantField:   "ops.get.feilds",
			wantPrefix:  `line 5: unknown key "feilds" in ops.get of the activity interface; allowed: `,
			wantAllowed: "fields",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			err := tc.parse([]byte(tc.source))
			if err == nil {
				t.Fatal("a document with an unknown key was accepted")
			}
			message := err.Error()
			if !strings.HasPrefix(message, tc.wantPrefix) {
				t.Fatalf("error = %q\nwant it to begin %q", message, tc.wantPrefix)
			}
			allowed := strings.Split(strings.TrimPrefix(message, tc.wantPrefix), ", ")
			if !contains(allowed, tc.wantAllowed) {
				t.Errorf("the allowed keys %v do not offer %q", allowed, tc.wantAllowed)
			}
			for _, internal := range []string{"not found in type", "dsl.", "Document", "unmarshal"} {
				if strings.Contains(message, internal) {
					t.Errorf("error %q names the decoder's internals (%q)", message, internal)
				}
			}
			var validation *dsl.ValidationError
			if !errors.As(err, &validation) {
				t.Fatalf("error %T is not a *dsl.ValidationError, so no editor can place it", err)
			}
			if validation.Field != tc.wantField {
				t.Errorf("field = %q, want %q", validation.Field, tc.wantField)
			}
		})
	}
}

func contains(values []string, want string) bool {
	for _, v := range values {
		if v == want {
			return true
		}
	}
	return false
}

// TestUnknownKey_NamesOnlyAKeyTheDecoderRefused: a key is named only as the
// decoder refused it — its line and its key, never the Go type it would have
// decoded into. A key written as an alias is named as the name it resolves
// to, at the alias's line, and a value of the wrong type beside it is left to
// the next decode; a null key the decoder ignores is never named, and the
// decoder's own refusal stands; a key a merge delivered, which no path of the
// document holds, is named by its line alone, and first when its line is,
// though the decoder refuses it last; a key under an anchor, refused again at
// each alias of it, is named once, at the path where it is written.
func TestUnknownKey_NamesOnlyAKeyTheDecoderRefused(t *testing.T) {
	t.Parallel()
	const orkflow = "apiVersion: orkstack/v1\nkind: Orkflow\nname: typo\n"
	tests := []struct {
		name      string
		source    string
		wantField string
		// want is the whole refusal when wantAllowed is empty, and the
		// sentence up to the allowed keys when it is not.
		want        string
		wantAllowed string
		absent      string // what the refusal must not say
	}{
		{
			name: "an alias used as a key, resolving to an unknown name",
			source: orkflow + "tasks:\n  - id: a\n    uses: shell\n    with: {command: &k bogus}\n" +
				"  - *k : b\n    uses: shell\n    timeout_seconds: abc\n",
			wantField:   "tasks[1].bogus",
			want:        `line 8: unknown key "bogus" in tasks[1] of the Orkflow; allowed: `,
			wantAllowed: "needs",
			absent:      "abc",
		},
		{
			name:   "a null key yaml.v3 ignores, beside a value of the wrong type",
			source: orkflow + "tasks:\n  - id: a\n    uses: shell\n    ~: x\n    timeout_seconds: abc\n",
			want:   "parse yaml: yaml: unmarshal errors:\n  line 8: cannot unmarshal !!str `abc` into int32",
		},
		{
			name: "an unknown key delivered by a merge",
			source: orkflow + "tasks:\n  - id: a\n    uses: shell\n    with: &w {id: b, uses: shell, bogus: 1}\n" +
				"  - <<: *w\n",
			want: `line 7: unknown key "bogus" in the Orkflow`,
		},
		{
			name: "a merged key on an earlier line than a key written in place",
			source: orkflow + "tasks:\n  - id: a\n    with: &w {id: b, bogus: 1}\n    wrong1: 1\n" +
				"  - <<: *w\n",
			want: `line 6: unknown key "bogus" in the Orkflow (and 1 more unknown key: "wrong1" at line 7)`,
		},
		{
			name:      "a key under an anchor, repeated by an alias",
			source:    orkflow + "pins:\n  p: &p {timeout_second: 1}\n  q: *p\n",
			wantField: "pins.p.timeout_second",
			want:      `line 5: unknown key "timeout_second" in pins.p of the Orkflow; allowed: timeout_seconds`,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			_, err := dsl.Compile([]byte(tc.source))
			if err == nil {
				t.Fatal("the document compiled")
			}
			message := err.Error()
			if tc.wantAllowed == "" && message != tc.want {
				t.Fatalf("error = %q\nwant %q", message, tc.want)
			}
			if tc.wantAllowed != "" {
				rest, ok := strings.CutPrefix(message, tc.want)
				if !ok {
					t.Fatalf("error = %q\nwant it to begin %q", message, tc.want)
				}
				if !contains(strings.Split(rest, ", "), tc.wantAllowed) {
					t.Errorf("the allowed keys %q do not offer %q", rest, tc.wantAllowed)
				}
			}
			if tc.absent != "" && strings.Contains(message, tc.absent) {
				t.Errorf("error %q says %q", message, tc.absent)
			}
			if strings.Contains(message, "unknown key") {
				for _, internal := range []string{"not found in type", "dsl.", "Document"} {
					if strings.Contains(message, internal) {
						t.Errorf("error %q names the decoder's internals (%q)", message, internal)
					}
				}
			}
			var validation *dsl.ValidationError
			if placed, named := errors.As(err, &validation), strings.Contains(message, "unknown key"); placed != named {
				t.Fatalf("error %q is a ValidationError: %v; names a key: %v. "+
					"A refusal is placed exactly when it names a key", message, placed, named)
			}
			if validation != nil && validation.Field != tc.wantField {
				t.Errorf("field = %q, want %q", validation.Field, tc.wantField)
			}
		})
	}
}
