package dist_test

import (
	"bytes"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/orkstack/orkstack/internal/dist"
)

// identityGateTag is the build tag that opens task identity for a test's own
// binaries (internal/dsl/identity_testgate.go).
const identityGateTag = "ork_test_identity"

// TestNoBuildOpensTheIdentityGate: a release's own build tags, and a build
// with none, leave the file the tag guards out; and no other tracked file
// names the tag, so no script, image or stack passes it.
func TestNoBuildOpensTheIdentityGate(t *testing.T) {
	root, err := filepath.Abs("../..")
	if err != nil {
		t.Fatal(err)
	}
	const gate = "identity_testgate.go"
	files := func(tags string) string {
		t.Helper()
		cmd := exec.Command("go", "list", "-tags="+tags, "-f", "{{.GoFiles}}", "./internal/dsl")
		cmd.Dir = root
		cmd.Env = append(cmd.Environ(), "GOFLAGS=")
		out, err := cmd.Output()
		if err != nil {
			t.Fatalf("go list -tags=%s: %v", tags, err)
		}
		return string(out)
	}
	if !strings.Contains(files(identityGateTag), gate) {
		t.Fatalf("%s is in no build, so this test holds nothing", gate)
	}
	for _, tags := range []string{"", dist.BuildTags, "integration"} {
		if strings.Contains(files(tags), gate) {
			t.Errorf("a build with tags %q opens the gate", tags)
		}
	}
	cmd := exec.Command("git", "grep", "-l", "-F", identityGateTag)
	cmd.Dir = root
	out, err := cmd.Output()
	if err != nil {
		t.Fatalf("git grep: %v", err)
	}
	var found []string
	for line := range bytes.Lines(out) {
		found = append(found, strings.TrimSpace(string(line)))
	}
	slices.Sort(found)
	want := []string{"internal/dist/identity_gate_test.go", "internal/dsl/identity_testgate.go", "internal/testsupport/testbin/testbin.go"}
	if !slices.Equal(found, want) {
		t.Fatalf("%s is named by %v; want only %v", identityGateTag, found, want)
	}
}
