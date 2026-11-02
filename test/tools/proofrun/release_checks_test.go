package main

import (
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

// The release's refusals are shell commands in release.yml and
// scripts/quickstart-smoke.sh: a jq over the signature bundles the registry
// holds, a jq over the signing config, and a grep of the notes for each
// image's digest. Their text is held by the lints of release_workflow_test.go;
// these tests take the command out of the file the release runs and run it
// over samples it must accept and samples it must refuse, so a check that
// stopped refusing is seen whatever its text.

// extractCheck returns the first capture group of re in the file at rel, or
// fails the test: a check that cannot be found is a check the release lacks.
func extractCheck(t *testing.T, rel string, re *regexp.Regexp) string {
	t.Helper()
	m := re.FindStringSubmatch(readRepoFileForPins(t, rel))
	if m == nil {
		t.Fatalf("%s holds no command matching %s", rel, re)
	}
	return m[1]
}

// runJQCheck runs `jq -es PROGRAM` over input and says whether it accepts it.
func runJQCheck(t *testing.T, program, input string) bool {
	t.Helper()
	jq, err := exec.LookPath("jq")
	if err != nil {
		t.Skip("jq is not installed")
	}
	cmd := exec.Command(jq, "-es", program)
	cmd.Stdin = strings.NewReader(input)
	err = cmd.Run()
	if err == nil {
		return true
	}
	if ee, ok := err.(*exec.ExitError); ok && ee.ExitCode() == 1 {
		return false // jq -e: the program's last output was false or null
	}
	t.Fatalf("jq %q: %v", program, err)
	return false
}

const (
	bundleType  = `"mediaType":"application/vnd.dev.sigstore.bundle.v0.3+json"`
	bundleNoLog = `{` + bundleType + `,"verificationMaterial":{"publicKey":{"hint":"k"}}}`
)

// The bundles `cosign download signature` prints for an image are signatures
// made with no transparency log: at least one, each a v0.3 bundle that
// records no tlog entry. Publish reads them back from the registry after it
// signs, and the smoke reads its own registry's the same way.
func TestRelease_TheBundleReadBackAcceptsOnlyBundlesWithNoTransparencyLogEntry(t *testing.T) {
	checks := map[string]string{
		"release.yml publish": extractCheck(t, ".github/workflows/release.yml",
			regexp.MustCompile(`cos download signature "\$ref" \| jq -es '([^']+)'`)),
		"quickstart-smoke.sh": extractCheck(t, "scripts/quickstart-smoke.sh",
			regexp.MustCompile(`cos download signature --allow-insecure-registry "\$ref" [^|]*\| jq -es '([^']+)'`)),
	}
	for _, c := range []struct {
		name   string
		input  string
		accept bool
	}{
		{"one bundle with no tlog entries field", bundleNoLog, true},
		{"one bundle with an empty tlog entry list", `{` + bundleType + `,"verificationMaterial":{"tlogEntries":[]}}`, true},
		{"two such bundles", bundleNoLog + "\n" + bundleNoLog, true},
		{"no signature at all", "", false},
		{"a bundle that records a tlog entry", `{` + bundleType + `,"verificationMaterial":{"tlogEntries":[{"logIndex":"1"}]}}`, false},
		{"a good bundle and one with an entry", bundleNoLog + "\n" + `{` + bundleType + `,"verificationMaterial":{"tlogEntries":[{"logIndex":"1"}]}}`, false},
		{"an older bundle format", `{"mediaType":"application/vnd.dev.sigstore.bundle+json;version=0.1","verificationMaterial":{}}`, false},
		{"a legacy signature with no bundle", `{"payload":"e30=","signature":"MEUC"}`, false},
		{"a bundle with no media type", `{"verificationMaterial":{}}`, false},
	} {
		for name, program := range checks {
			if got := runJQCheck(t, program, c.input); got != c.accept {
				t.Errorf("%s: %s: accepted = %v, want %v", name, c.name, got, c.accept)
			}
		}
	}
}

// Publish refuses to sign with a signing config that names a service (a
// transparency log, a timestamp authority): the one it accepts is the config
// that names none, and the committed deploy/cosign-signing-config.json is that
// one. The workflow's own command runs over the committed file and over
// configs that name a service.
func TestRelease_ThePublishRefusesASigningConfigThatNamesAService(t *testing.T) {
	program := extractCheck(t, ".github/workflows/release.yml",
		regexp.MustCompile(`jq -es '([^']+)' meta/cosign-signing-config\.json`))
	committed := readRepoFileForPins(t, "deploy/cosign-signing-config.json")
	const typ = `"mediaType":"application/vnd.dev.sigstore.signingconfig.v0.2+json"`
	for _, c := range []struct {
		name, config string
		accept       bool
	}{
		{"the committed config", committed, true},
		{"the same config with another key order and layout", "{\n \"tsaConfig\": {},\n \"rekorTlogConfig\": {},\n " + typ + "\n}\n", true},
		{"a transparency log", `{` + typ + `,"rekorTlogConfig":{"url":"https://rekor.example"},"tsaConfig":{}}`, false},
		{"a timestamp authority", `{` + typ + `,"rekorTlogConfig":{},"tsaConfig":{"url":"https://tsa.example"}}`, false},
		{"a certificate authority", `{` + typ + `,"rekorTlogConfig":{},"tsaConfig":{},"caUrl":"https://fulcio.example"}`, false},
		{"another media type", `{"mediaType":"application/vnd.dev.sigstore.signingconfig.v0.1+json","rekorTlogConfig":{},"tsaConfig":{}}`, false},
		{"two configs", committed + committed, false},
		{"an empty file", "", false},
	} {
		if got := runJQCheck(t, program, c.config); got != c.accept {
			t.Errorf("%s: accepted = %v, want %v", c.name, got, c.accept)
		}
	}
}

// Publish refuses release notes that do not name an image it is about to
// push by its digest, as a whole line: the workflow's own grep runs over notes
// that do and notes that do not.
func TestRelease_ThePublishRefusesNotesThatMissAnImagesDigest(t *testing.T) {
	bash, err := exec.LookPath("bash")
	if err != nil {
		t.Skip("bash is not installed")
	}
	grep := extractCheck(t, ".github/workflows/release.yml",
		regexp.MustCompile(`(grep -qxF "cosign verify[^"]*" meta/notes\.md)`))
	const (
		name   = "ork"
		digest = "sha256:abc123"
		line   = "cosign verify --key cosign.pub --insecure-ignore-tlog=true ghcr.io/orkstack/ork@sha256:abc123"
	)
	for _, c := range []struct {
		name, notes string
		accept      bool
	}{
		{"the line among others", "# v1\n\n" + line + "\n\nmore\n", true},
		{"the line last, with no newline", "# v1\n" + line, true},
		{"no line for the image", "# v1\n", false},
		{"another image's digest", strings.ReplaceAll(line, "ork@", "orker@"), false},
		{"another digest of the image", strings.ReplaceAll(line, "abc123", "def456"), false},
		{"a longer digest the image's is a prefix of", line + "9", false},
		{"the line inside a longer line", "run: " + line + " now", false},
		{"the line with another key", strings.ReplaceAll(line, "cosign.pub", "other.pub"), false},
	} {
		dir := t.TempDir()
		if err := os.MkdirAll(filepath.Join(dir, "meta"), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(dir, "meta", "notes.md"), []byte(c.notes), 0o644); err != nil {
			t.Fatal(err)
		}
		cmd := exec.Command(bash, "-c", "name="+name+" digest="+digest+"\n"+grep)
		cmd.Dir = dir
		err := cmd.Run()
		if got := err == nil; got != c.accept {
			t.Errorf("%s: accepted = %v (%v), want %v", c.name, got, err, c.accept)
		}
	}
}
