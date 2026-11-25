package provider_test

import (
	"strings"
	"testing"

	"github.com/orkstack/orkstack/internal/provider"
	"github.com/orkstack/orkstack/internal/provider/internal/scripted"
	"go.yaml.in/yaml/v4"
)

var testTypes = map[string]provider.Type{"scripted": scripted.Type{}}

const validDocument = `providers:
  sts:
    type: scripted
    audience: sts.amazonaws.com
    region: eu-west-1
  kv:
    type: scripted
    audience: vault.internal
`

func TestParse_DeclaresEachProviderByName(t *testing.T) {
	set, err := provider.ParseWith([]byte(validDocument), testTypes)
	if err != nil {
		t.Fatal(err)
	}
	if got := strings.Join(set.Names(), ","); got != "kv,sts" {
		t.Fatalf("Names() = %q", got)
	}
	p, ok := set.ByName("sts")
	if !ok || p.Audience() != "sts.amazonaws.com" || p.Type() != "scripted" || p.Describe().Fields["region"] != "eu-west-1" {
		t.Fatalf("sts = %+v", p)
	}
}

func TestParse_EmptyDeclaresNone(t *testing.T) {
	for _, doc := range []string{"", "  \n", "providers: {}\n"} {
		set, err := provider.ParseWith([]byte(doc), testTypes)
		if err != nil || set.Len() != 0 {
			t.Errorf("ParseWith(%q) = %d providers, %v", doc, set.Len(), err)
		}
	}
}

func TestParseWith_ATypeNotInTheSetIsRefusedByName(t *testing.T) {
	if _, err := provider.ParseWith([]byte(validDocument), nil); err == nil || !strings.Contains(err.Error(), `type "scripted" is not one this build has (none)`) {
		t.Fatalf("ParseWith an empty set = %v; want the type refused by name", err)
	}
}

// documentRefusals are the refusals of the document, and the fuzz target's
// seeds beside the valid one.
var documentRefusals = []struct {
	name, doc, want string
}{
	{"unknown key", "providers:\n  sts:\n    type: scripted\n    audience: a\n    regoin: eu\n", `key "regoin" is not one this provider's type reads`},
	{"a key that would hold a secret", "providers:\n  sts:\n    type: scripted\n    audience: a\n    client_secret: x\n", "holds no secret"},
	{"a token key", "providers:\n  sts:\n    type: scripted\n    audience: a\n    token_file: /x\n", "holds no secret"},
	{"a reserved name", "providers:\n  env:\n    type: scripted\n    audience: a\n", "is taken"},
	{"a name with a hyphen", "providers:\n  my-vault:\n    type: scripted\n    audience: a\n", "must match"},
	{"no type", "providers:\n  sts:\n    audience: a\n", "type is required"},
	{"an unknown type", "providers:\n  sts:\n    type: gcp\n    audience: a\n", `type "gcp" is not one this build has`},
	{"a number for a string", "providers:\n  sts:\n    type: scripted\n    audience: 123\n", "must be a string"},
	{"a boolean for a string", "providers:\n  sts:\n    type: scripted\n    audience: true\n", "must be a string"},
	{"a missing required key", "providers:\n  sts:\n    type: scripted\n", "audience: is required"},
	{"an anchor", "providers:\n  sts:\n    type: scripted\n    audience: &a x\n", "anchors and aliases"},
	{"an explicit tag", "providers:\n  sts:\n    type: scripted\n    audience: !!str x\n", "explicit tag"},
	{"a custom tag", "providers:\n  sts:\n    type: scripted\n    audience: !secret x\n", "is refused"},
	{"a merge key", "providers:\n  base: &b\n    type: scripted\n    audience: a\n  sts:\n    <<: *b\n", "anchors and aliases"},
	{"an anchor on a declaration", "providers:\n  sts: &a\n    type: scripted\n    audience: a\n", "anchors and aliases"},
	{"a tag on a declaration", "providers:\n  sts: !foo\n    type: scripted\n    audience: a\n", "is refused"},
	{"an anchor on a key", "providers:\n  sts:\n    type: scripted\n    &k audience: a\n", "anchors and aliases"},
	{"an inline merge key", "providers:\n  <<: {a: {type: scripted, audience: x}}\n", "merge keys are refused"},
	{"a merge key in a declaration", "providers:\n  sts:\n    <<: {type: scripted}\n    audience: a\n", "merge keys are refused"},
	{"a bidirectional control", "providers:\n  sts:\n    type: scripted\n    audience: \"a\u202eb\"\n", "formatting character"},
	{"a C1 control", "providers:\n  sts:\n    type: scripted\n    audience: \"a\\u0085b\"\n", "formatting character"},
	{"an anchor on providers", "providers: &p\n  sts:\n    type: scripted\n    audience: a\n", "anchors and aliases"},
	{"a second document", "providers: {}\n---\nproviders: {}\n", "second YAML document"},
	{"an unknown top-level key", "provider:\n  sts:\n    type: scripted\n", "field provider not found"},
	{"a declaration that is not a mapping", "providers:\n  sts: scripted\n", "mapping"},
	{"a duplicate key", "providers:\n  sts:\n    type: scripted\n    audience: a\n    audience: b\n", "declared twice"},
}

func TestParse_Refusals(t *testing.T) {
	for _, tc := range documentRefusals {
		_, err := provider.ParseWith([]byte(tc.doc), testTypes)
		if err == nil {
			t.Errorf("%s: accepted", tc.name)
			continue
		}
		if !strings.Contains(err.Error(), tc.want) {
			t.Errorf("%s: %v; want it to say %q", tc.name, err, tc.want)
		}
	}
}

func TestParse_AtMostSixteenProviders(t *testing.T) {
	var b strings.Builder
	b.WriteString("providers:\n")
	for i := range provider.MaxProviders + 1 {
		b.WriteString("  p" + string(rune('a'+i)) + ":\n    type: scripted\n    audience: a\n")
	}
	if _, err := provider.ParseWith([]byte(b.String()), testTypes); err == nil || !strings.Contains(err.Error(), "at most 16") {
		t.Fatalf("17 providers = %v", err)
	}
}

// FuzzProvidersDocument: a document the parser accepts declares only valid
// names, and no key of any declaration holds a word a secret would be named
// by.
func FuzzProvidersDocument(f *testing.F) {
	f.Add(validDocument)
	for _, tc := range documentRefusals {
		f.Add(tc.doc)
	}
	f.Fuzz(func(t *testing.T, doc string) {
		set, err := provider.ParseWith([]byte(doc), testTypes)
		if err != nil {
			return
		}
		for _, name := range set.Names() {
			if provider.ValidName(name) != nil {
				t.Fatalf("accepted the name %q", name)
			}
			p, _ := set.ByName(name)
			if p.Audience() == "" {
				t.Fatalf("accepted %q with no audience", name)
			}
		}
		if set.Len() == 0 {
			// Whitespace YAML itself refuses ("\f") declares none, as an
			// empty setting does.
			return
		}
		var decoded struct {
			Providers map[string]map[string]any `yaml:"providers"`
		}
		if err := yaml.Unmarshal([]byte(doc), &decoded); err != nil {
			t.Fatalf("accepted a document yaml cannot decode: %v", err)
		}
		for name, decl := range decoded.Providers {
			for key := range decl {
				for _, word := range []string{"secret", "password", "token", "private", "credential"} {
					if strings.Contains(strings.ToLower(key), word) {
						t.Fatalf("accepted %s's key %q", name, key)
					}
				}
			}
		}
	})
}

// intType reads one integer, to hold Config.Int to the strict reading.
type intType struct{}

func (intType) Name() string { return "scripted" }
func (intType) New(name string, c *provider.Config) (provider.Provider, error) {
	c.Int("seconds", 0, 1<<20)
	return scripted.New(name, "aud"), nil
}

func TestConfig_IntIsPlainDecimalOnly(t *testing.T) {
	types := map[string]provider.Type{"scripted": intType{}}
	for _, tc := range []struct {
		value string
		ok    bool
	}{
		{"900", true}, {"0", true},
		{"0755", false}, {"010", false}, {"+5", false}, {"1_000", false}, {"0x10", false}, {"-1", false}, {"9.5", false}, {"\"900\"", false},
	} {
		doc := "providers:\n  p:\n    type: scripted\n    seconds: " + tc.value + "\n"
		_, err := provider.ParseWith([]byte(doc), types)
		if (err == nil) != tc.ok {
			t.Errorf("seconds: %s = %v; want ok=%v", tc.value, err, tc.ok)
		}
	}
}
