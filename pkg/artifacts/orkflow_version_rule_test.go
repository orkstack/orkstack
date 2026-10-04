package artifacts

import (
	"strings"
	"testing"

	"github.com/orkstack/orkstack/internal/dsl"
)

// A composed task pins `uses: orkflow:<name>@<version>`, and the version it
// names is a stored version string: the compiler and the registry must agree
// on which strings those are, or a document could pin a version nothing can
// store (or refuse one that exists). They are two spellings held together by
// measurement, the precedent TestNamespaceName_RegistryAndCompilerAgree sets.
func TestOrkflowVersionRule_RegistryAndCompilerAgree(t *testing.T) {
	corpus := []string{
		"2.1.0", "1", "v1.2.3", "1.0.0-rc.1", "a_b", "A.B", strings.Repeat("ab", 32),
		"-1", ".hidden", "_x", "1/2", "a@b", "a b", "a:b", "ä", "1+build",
		strings.Repeat("v", maxOrkflowComponent),
		strings.Repeat("v", maxOrkflowComponent+1),
	}
	for _, version := range corpus {
		_, storeErr := OrkflowKey("default", "release-block", version)
		_, _, ok, parseErr := dsl.ParseOrkflowUses("orkflow:release-block@" + version)
		if !ok {
			t.Fatalf("ParseOrkflowUses did not take %q as a composition", version)
		}
		stored, pinnable := storeErr == nil, parseErr == nil
		if stored != pinnable {
			t.Errorf("version %q: the registry says stored=%v (%v), the compiler says pinnable=%v (%v)",
				version, stored, storeErr, pinnable, parseErr)
		}
	}
}
