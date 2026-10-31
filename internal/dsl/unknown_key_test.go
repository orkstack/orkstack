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

// TestUnknownKey_NamesOnlyAKeyTheDecoderRefused: the walk names a key only
// when the decoder refused that key at that line. A key written as an alias,
// a null key the decoder ignores, or a key delivered by a merge is never
// reported under a name the decoder did not refuse, and never hides the
// decoder's real refusal.
func TestUnknownKey_NamesOnlyAKeyTheDecoderRefused(t *testing.T) {
	t.Parallel()
	const orkflow = "apiVersion: orkstack/v1\nkind: Orkflow\nname: typo\n"
	tests := []struct {
		name     string
		source   string
		mustHave string // what the refusal must still say
	}{
		{
			name: "an alias used as a key, beside a value of the wrong type",
			source: orkflow + "tasks:\n  - &k id: a\n    uses: shell\n    with: {command: \"echo a\"}\n" +
				"  - *k : b\n    uses: shell\n    timeout_seconds: abc\n",
			mustHave: "abc",
		},
		{
			name:     "a null key yaml.v3 ignores, beside a value of the wrong type",
			source:   orkflow + "tasks:\n  - id: a\n    uses: shell\n    ~: x\n    timeout_seconds: abc\n",
			mustHave: "abc",
		},
		{
			name: "an unknown key delivered by a merge",
			source: orkflow + "tasks:\n  - id: a\n    uses: shell\n    with: &w {id: b, uses: shell, bogus: 1}\n" +
				"  - <<: *w\n",
			mustHave: "bogus",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			_, err := dsl.Compile([]byte(tc.source))
			if err == nil {
				t.Fatal("the document compiled")
			}
			if strings.Contains(err.Error(), "unknown key") {
				t.Fatalf("error %q names a key the decoder did not refuse", err)
			}
			if !strings.Contains(err.Error(), tc.mustHave) {
				t.Fatalf("error %q hides the decoder's refusal of %q", err, tc.mustHave)
			}
		})
	}
}
