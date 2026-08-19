package artifacts

import (
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/orkstack/orkstack/internal/dsl"
)

// A namespace exists to be NAMED: by a `namespace:` in a YAML document, which
// is the only way an orkflow enters one. A name this registry accepts and no
// document can reference is a namespace that can be created, written to, and
// then never used — the same "stored value nothing can honour" defect the
// registry's pool fields had (pool_name_test.go), one concept over.
//
// So the two rules must be one rule. They cannot be one piece of CODE: pkg/
// may not import internal/, or no external importer of pkg/artifacts could
// build. They are one rule by measurement instead, here, in a test — which no
// external importer builds either. The import above is the whole point of the
// file: it is the only place pkg/artifacts names internal/dsl, and it exists
// to make drift a test failure rather than an unreachable stored namespace.

// namespaceCorpus is spelled out rather than generated, so a reader can see
// exactly what is claimed. The second group is what review measured slipping
// through cleanComponentPattern (`^[A-Za-z0-9][A-Za-z0-9._-]*$`), which is the
// rule for a stored VERSION string and was never the rule for a name.
var namespaceCorpus = []string{
	// nameable in a document, and storable
	"default", "payments", "pay-ments", "a", "team-42", "a--b", "x1",
	// stored by the old clean-component rule; no document can name any of them
	"Payments", "pay.ments", "1payments", "a_b",
	// refused by both, and the reason a name is validated and not sanitized
	"-a", "..", "../billing", "billing/team", "billing team", "PAY", "_a",
}

// TestNamespaceName_RegistryAndCompilerAgree is the pin: for every name, the
// registry and the compiler answer the same way.
func TestNamespaceName_RegistryAndCompilerAgree(t *testing.T) {
	for _, name := range namespaceCorpus {
		stored := validNamespaceName(name) == nil
		nameable := compilerAcceptsNamespace(t, name)
		switch {
		case stored && !nameable:
			t.Errorf("the registry stores namespace %q, no document can name it: a namespace that "+
				"can be created and written to and then never referenced in YAML", name)
		case nameable && !stored:
			t.Errorf("a document can name namespace %q, the registry refuses to store it: its "+
				"orkflows would live in a namespace nothing can register or list", name)
		}
	}
}

// TestNamespaceName_RejectsTheCaseReviewMeasured names the four values
// explicitly, at the public boundary a caller uses, so a future loosening of
// the rule fails on the exact regression rather than on an abstraction. Each
// of these was stored and keyed before, and refused by every document that
// could have named it.
func TestNamespaceName_RejectsTheCaseReviewMeasured(t *testing.T) {
	for _, name := range []string{"Payments", "pay.ments", "1payments", "a_b"} {
		if key, err := NamespaceKey(name); !errors.Is(err, ErrInvalidNamespace) {
			t.Errorf("NamespaceKey(%q) = %q, %v; want ErrInvalidNamespace", name, key, err)
		}
		// The same name as an orkflow's namespace: one concept, one rule, so
		// a definition cannot be written under a namespace the registry
		// refuses to hold.
		key, err := OrkflowKey(name, "deploy", "0.1.0")
		if !errors.Is(err, ErrInvalidNamespace) || !errors.Is(err, ErrInvalidOrkflow) {
			t.Errorf("OrkflowKey(%q, …) = %q, %v; want an error matching both ErrInvalidNamespace "+
				"and ErrInvalidOrkflow", name, key, err)
		}
	}
}

// TestNamespaceName_AnAbsentNamespaceIsTheOneNameOutsideTheCorpus covers the
// value the corpus cannot: empty. In a document it means "unset", which is
// legal and means the built-in default — so the registry must resolve it to
// that same default rather than refuse it as a name.
func TestNamespaceName_AnAbsentNamespaceIsTheOneNameOutsideTheCorpus(t *testing.T) {
	wf, err := dsl.Compile([]byte("name: probe\ntasks:\n  - id: only\n    plugin: shell\n"))
	if err != nil {
		t.Fatalf("a document naming no namespace must compile: %v", err)
	}
	if wf.GetNamespace() != "" {
		t.Fatalf("an absent namespace compiled to %q, want empty — the resolution belongs to the "+
			"store, where a name becomes a key", wf.GetNamespace())
	}
	key, err := NamespaceKey(wf.GetNamespace())
	if err != nil {
		t.Fatalf("NamespaceKey(%q): %v", wf.GetNamespace(), err)
	}
	if want := NamespacePrefix + "/" + DefaultNamespace + namespaceSuffix; key != want {
		t.Errorf("NamespaceKey(\"\") = %q, want %q", key, want)
	}
}

// compilerAcceptsNamespace answers whether a DSL document can name this
// namespace, by compiling one. It measures the compiler rather than a copy of
// its regexp, so the pin cannot drift with the rule it is pinning.
func compilerAcceptsNamespace(t *testing.T, name string) bool {
	t.Helper()
	document := fmt.Sprintf(
		"namespace: %q\nname: probe\ntasks:\n  - id: only\n    plugin: shell\n", name)
	wf, err := dsl.Compile([]byte(document))
	switch {
	case err == nil:
		if got := wf.GetNamespace(); got != name {
			t.Fatalf("a probe document for namespace %q compiled to %q; the probe is not measuring "+
				"what it claims", name, got)
		}
		return true
	case strings.Contains(err.Error(), "namespace"):
		return false
	default:
		// Anything else means the probe document is wrong, not the name — a
		// silent source of false agreement.
		t.Fatalf("the probe document for namespace %q failed for an unrelated reason: %v", name, err)
		return false
	}
}
