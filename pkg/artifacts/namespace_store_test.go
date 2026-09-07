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

// TestValidNamespaceTelemetry_EnforcesOnlyTheDocumentOnlyRules is the store
// half of the retention/pools split: shape and reserved prefixes are checked
// here, sink existence is not — this package has no ORK_TELEMETRY_SINKS
// table to check it against, only the engine that parsed one does
// (internal/api.validNamespaceTelemetry). A sink name of any shape at all,
// even one no deployment would ever declare, must pass here.
func TestValidNamespaceTelemetry_EnforcesOnlyTheDocumentOnlyRules(t *testing.T) {
	for _, ns := range []*orkstackv1.Namespace{
		{Name: "payments"},
		{Name: "payments", Telemetry: &orkstackv1.TelemetryPolicy{Sink: "no-such-sink-anywhere"}},
		{Name: "payments", Telemetry: &orkstackv1.TelemetryPolicy{
			Attributes: map[string]string{"team": "money"},
		}},
	} {
		if err := validNamespaceTelemetry(ns); err != nil {
			t.Errorf("validNamespaceTelemetry(%v) = %v, want nil (sink existence is the API's job)", ns, err)
		}
	}
}

// TestValidNamespaceTelemetry_RefusesAReservedAttributePrefix.
func TestValidNamespaceTelemetry_RefusesAReservedAttributePrefix(t *testing.T) {
	for _, prefix := range []string{"ork.", "service.", "host."} {
		ns := &orkstackv1.Namespace{
			Name: "payments",
			Telemetry: &orkstackv1.TelemetryPolicy{
				Attributes: map[string]string{prefix + "x": "1"},
			},
		}
		if err := validNamespaceTelemetry(ns); !errors.Is(err, ErrInvalidNamespace) {
			t.Errorf("validNamespaceTelemetry(prefix %q) = %v, want ErrInvalidNamespace", prefix, err)
		}
	}
}

// TestValidNamespaceTelemetry_RefusesAMalformedAttribute delegates key/value
// shape to internal/dsl.ValidateAttributeMap; this pins that the store
// actually calls it rather than silently accepting anything.
func TestValidNamespaceTelemetry_RefusesAMalformedAttribute(t *testing.T) {
	ns := &orkstackv1.Namespace{
		Name: "payments",
		Telemetry: &orkstackv1.TelemetryPolicy{
			Attributes: map[string]string{"Not-Lowercase": "1"},
		},
	}
	if err := validNamespaceTelemetry(ns); !errors.Is(err, ErrInvalidNamespace) {
		t.Errorf("validNamespaceTelemetry(bad key) = %v, want ErrInvalidNamespace", err)
	}
}

// TestValidNamespaceTelemetry_RefusesAControlCharacterInAValue: an attribute
// value is rendered verbatim by `orkctl namespace get` and the UI's
// namespace page, so a control character in it (an embedded newline, an
// escape sequence) is refused at the same document-only boundary as a
// malformed key.
func TestValidNamespaceTelemetry_RefusesAControlCharacterInAValue(t *testing.T) {
	ns := &orkstackv1.Namespace{
		Name: "payments",
		Telemetry: &orkstackv1.TelemetryPolicy{
			Attributes: map[string]string{"team": "payments\ndrop table"},
		},
	}
	if err := validNamespaceTelemetry(ns); !errors.Is(err, ErrInvalidNamespace) {
		t.Errorf("validNamespaceTelemetry(control char value) = %v, want ErrInvalidNamespace", err)
	}
}
