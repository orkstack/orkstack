package config

import (
	"context"
	"testing"

	"github.com/orkstack/orkstack/internal/provider"
	"github.com/orkstack/orkstack/internal/testsupport/must"
)

// declaredType is a provider type that only declares: it reads an audience
// and builds nothing that can log in. No built-in type exists until P16, and
// this test is about the document's round trip, not about a provider.
type declaredType struct{}

func (declaredType) Name() string { return "declared" }
func (declaredType) New(name string, c *provider.Config) (provider.Provider, error) {
	audience, _ := c.String("audience")
	return declared{name: name, audience: audience}, nil
}

type declared struct{ name, audience string }

func (d declared) Name() string                              { return d.name }
func (declared) Type() string                                { return "declared" }
func (declared) LoginScheme() string                         { return "login" }
func (d declared) Audience() string                          { return d.audience }
func (declared) Describe() provider.Description              { return provider.Description{} }
func (declared) Parse(provider.Ref) (provider.Parsed, error) { return provider.Parsed{}, nil }
func (declared) Login(context.Context, string, provider.Attempt, provider.TokenSource) (provider.Session, error) {
	return nil, nil
}

// TestLoad_ProvidersIsOneDocumentBothBinariesRead: the providers block of a
// configuration file reaches ork and orker as the same ORK_PROVIDERS text,
// wrapped in one `providers:` key, which provider.ParseWith decodes as it is.
func TestLoad_ProvidersIsOneDocumentBothBinariesRead(t *testing.T) {
	path := writeTemp(t, "orkstack.yaml", `providers:
  aws:
    type: declared
    audience: sts.amazonaws.com
  vault:
    type: declared
    audience: vault.internal
`)
	var texts []string
	for _, binary := range []Binary{BinaryOrk, BinaryOrker} {
		doc, err := Load(path, binary)
		must.OKf(t, err, "Load")
		text := doc.values[provider.Var].value
		texts = append(texts, text)
		set, err := provider.ParseWith([]byte(text), map[string]provider.Type{"declared": declaredType{}})
		must.OKf(t, err, "provider.ParseWith(re-serialized providers)")
		if got := set.Names(); len(got) != 2 || got[0] != "aws" || got[1] != "vault" {
			t.Errorf("%v: providers = %v", binary, got)
		}
	}
	if texts[0] != texts[1] {
		t.Errorf("ork and orker read different providers documents:\n%s\n%s", texts[0], texts[1])
	}
}
