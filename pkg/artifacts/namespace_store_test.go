package artifacts

import (
	"errors"
	"testing"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
)

func TestNamespaceKey_ResolvesTheBuiltInDefault(t *testing.T) {
	for _, name := range []string{"", DefaultNamespace} {
		got, err := NamespaceKey(name)
		if err != nil {
			t.Fatalf("NamespaceKey(%q): %v", name, err)
		}
		if got != NamespacePrefix+"/default.json" {
			t.Errorf("NamespaceKey(%q) = %q, want the default key", name, got)
		}
	}
}

func TestNamespaceKey_RejectsUnsafeNames(t *testing.T) {
	for _, name := range []string{"..", "../billing", "billing/team", "billing team", string(rune(10))} {
		_, err := NamespaceKey(name)
		if !errors.Is(err, ErrInvalidNamespace) {
			t.Errorf("NamespaceKey(%q) error = %v, want ErrInvalidNamespace", name, err)
		}
	}

}
func TestParseNamespaceKey_RoundTripsAndSkipsForeignKeys(t *testing.T) {
	for _, want := range []string{"default", "payments", "team-42"} {
		key, err := NamespaceKey(want)
		if err != nil {
			t.Fatalf("NamespaceKey(%q): %v", want, err)
		}
		got, ok := ParseNamespaceKey(key)
		if !ok || got != want {
			t.Errorf("ParseNamespaceKey(%q) = %q, %v; want %q, true", key, got, ok, want)
		}
	}
	for _, foreign := range []string{
		"metadata/namespaces/payments.txt",
		"metadata/namespaces/payments/extra.json",
		"metadata/orkflows/payments/deploy/1.0.0.json",
		"metadata/namespaces/../secret.json",
	} {
		if got, ok := ParseNamespaceKey(foreign); ok {
			t.Errorf("ParseNamespaceKey(%q) = %q, true; want false", foreign, got)
		}
	}
}

func TestDefaultNamespaceDocument_IsAlwaysAvailableWithoutAWrite(t *testing.T) {
	ns := DefaultNamespaceDocument()
	if ns.GetName() != DefaultNamespace {
		t.Fatalf("default namespace name = %q, want %q", ns.GetName(), DefaultNamespace)
	}
	if ns.GetDescription() == "" {
		t.Fatal("default namespace has no description")
	}
}

func TestValidNamespacePools_UsesMainWhenDefaultIsUnset(t *testing.T) {
	if err := validNamespacePools(&orkstackv1.Namespace{
		Name:         "payments",
		AllowedPools: []string{"main", "gpu"},
	}); err != nil {
		t.Fatalf("validNamespacePools: %v", err)
	}
	for _, ns := range []*orkstackv1.Namespace{
		{Name: "payments", DefaultPool: "gpu", AllowedPools: []string{"main"}},
		{Name: "payments", DefaultPool: "bad pool"},
		{Name: "payments", AllowedPools: []string{"GPU"}},
	} {
		if err := validNamespacePools(ns); !errors.Is(err, ErrInvalidNamespace) {
			t.Errorf("validNamespacePools(%v) = %v, want ErrInvalidNamespace", ns, err)
		}
	}
}
