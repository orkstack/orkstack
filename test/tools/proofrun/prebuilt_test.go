package main

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// prebuiltDir writes a manifest of tree for the given binaries (name ->
// listing), as `proofrun compile` would, with fake scripts for binaries.
func prebuiltDir(t *testing.T, tree string, pkgs map[string][]string) string {
	t.Helper()
	dir := t.TempDir()
	m := compiledManifest{Format: compiledFormat, Tree: tree}
	for importPath, names := range pkgs {
		name := testBinaryName(importPath)
		bin := shippedScript(t, dir, name, names...)
		sha, err := fileSHA256(bin)
		if err != nil {
			t.Fatal(err)
		}
		m.Packages = append(m.Packages, compiledPackage{ImportPath: importPath, Binary: name, BinarySHA256: sha, ListingSHA256: sha256String(strings.Join(names, "\n"))})
	}
	if err := writeJSON(filepath.Join(dir, compiledManifestName), m); err != nil {
		t.Fatal(err)
	}
	return dir
}

func TestPrebuiltBinaries_AdoptsAMatchingManifestAndRefusesEveryMismatch(t *testing.T) {
	fake := []pkgInfo{{ImportPath: fakeSuite, Dir: filepath.Join(mustRepoRoot(), fakeDir)}}
	stubTree(t, "this-tree")

	// No manifest: nothing adopted, and no error — the planner compiles.
	if bins, err := prebuiltBinaries(t.TempDir(), fake); bins != nil || err != nil {
		t.Fatalf("a directory with no manifest: %v, %v; want nil, nil", bins, err)
	}

	// The positive control: this tree, these packages, these bytes.
	dir := prebuiltDir(t, "this-tree", map[string][]string{fakeSuite: fakeListing})
	bins, err := prebuiltBinaries(dir, fake)
	if err != nil || bins[fakeSuite] != filepath.Join(dir, testBinaryName(fakeSuite)) {
		t.Fatalf("a matching manifest: %v, %v", bins, err)
	}

	for _, c := range []struct {
		name string
		dir  func() string
		want string
	}{
		{"another tree", func() string {
			return prebuiltDir(t, "another-tree", map[string][]string{fakeSuite: fakeListing})
		}, "compiled from tree another-tree, and this checkout is this-tree"},
		{"a package too many", func() string {
			return prebuiltDir(t, "this-tree", map[string][]string{fakeSuite: fakeListing, modulePath + "/internal/other": {"TestX"}})
		}, "extra " + modulePath + "/internal/other"},
		{"a package missing", func() string {
			return prebuiltDir(t, "this-tree", map[string][]string{})
		}, "missing " + fakeSuite},
		{"a binary changed after the manifest", func() string {
			d := prebuiltDir(t, "this-tree", map[string][]string{fakeSuite: fakeListing})
			shippedScript(t, d, testBinaryName(fakeSuite), append(append([]string{}, fakeListing...), "TestAdded")...)
			return d
		}, "hashes to"},
		{"an unknown format", func() string {
			d := t.TempDir()
			if err := writeJSON(filepath.Join(d, compiledManifestName), compiledManifest{Format: 99, Tree: "this-tree"}); err != nil {
				t.Fatal(err)
			}
			return d
		}, "format 99"},
	} {
		bins, err := prebuiltBinaries(c.dir(), fake)
		if bins != nil || err == nil || !strings.Contains(err.Error(), c.want) {
			t.Errorf("%s: %v, %v; want an error naming %q", c.name, bins, err, c.want)
		}
	}
}

// The listing is checked too: a binary whose bytes match the manifest and
// whose -test.list output does not (a manifest edited by hand) is refused.
func TestPrebuiltBinaries_RefusesAListingThatDiffers(t *testing.T) {
	stubTree(t, "this-tree")
	dir := prebuiltDir(t, "this-tree", map[string][]string{fakeSuite: fakeListing})
	var m compiledManifest
	if err := readJSON(filepath.Join(dir, compiledManifestName), &m); err != nil {
		t.Fatal(err)
	}
	m.Packages[0].ListingSHA256 = sha256String("TestSomethingElse")
	if err := writeJSON(filepath.Join(dir, compiledManifestName), m); err != nil {
		t.Fatal(err)
	}
	if bins, err := prebuiltBinaries(dir, []pkgInfo{{ImportPath: fakeSuite, Dir: filepath.Join(mustRepoRoot(), fakeDir)}}); bins != nil || err == nil || !strings.Contains(err.Error(), "lists tests hashing to") {
		t.Errorf("a listing that differs from the manifest's: %v, %v", bins, err)
	}
}

// End to end: `proofrun compile` builds the fake suite, and `proofrun
// integration -bin` adopts exactly those bytes — the plan records the
// compile's SHA-256 — instead of compiling again. A planner given a
// directory with no manifest compiles into it, as before.
func TestCompile_ThenIntegrationAdoptsItsBinaries(t *testing.T) {
	if testing.Short() {
		t.Skip("compiles a test binary")
	}
	var log bytes.Buffer
	prev := stdout
	stdout = &log
	t.Cleanup(func() { stdout = prev })
	bins := t.TempDir()
	if code := cmdCompile([]string{"-out", bins, "./testdata/fakesuite"}); code != 0 {
		t.Fatalf("compile exit %d:\n%s", code, log.String())
	}
	var m compiledManifest
	if err := readJSON(filepath.Join(bins, compiledManifestName), &m); err != nil {
		t.Fatal(err)
	}
	if len(m.Packages) != 1 || m.Packages[0].ImportPath != fakeSuite || m.Tree == "" {
		t.Fatalf("manifest = %+v", m)
	}
	out := t.TempDir()
	log.Reset()
	if code := cmdIntegration([]string{"-out", out, "-bin", bins, "-plan-only", "-jobs", "1", "-costs", "", "-allow", "", "-quarantine", "",
		"-nightly", "", "-host-speed", "", "./testdata/fakesuite"}); code != 0 {
		t.Fatalf("plan-only exit %d:\n%s", code, log.String())
	}
	if !strings.Contains(log.String(), "adopted 1 integration test binaries") || strings.Contains(log.String(), "compiled "+fakeSuite) {
		t.Errorf("the planner did not adopt the compiled binary:\n%s", log.String())
	}
	p, err := readPlan(filepath.Join(out, "integration-plan.json"))
	if err != nil {
		t.Fatal(err)
	}
	if len(p.Packages) != 1 || p.Packages[0].BinarySHA256 != m.Packages[0].BinarySHA256 || p.Packages[0].ListingSHA256 != m.Packages[0].ListingSHA256 {
		t.Errorf("plan packages %+v, want the compile's binary %s", p.Packages, m.Packages[0].BinarySHA256)
	}

	// The positive control for the fallback: no manifest, so it compiles.
	fresh := t.TempDir()
	log.Reset()
	if code := cmdIntegration([]string{"-out", t.TempDir(), "-bin", fresh, "-plan-only", "-jobs", "1", "-costs", "", "-allow", "", "-quarantine", "",
		"-nightly", "", "-host-speed", "", "./testdata/fakesuite"}); code != 0 {
		t.Fatalf("plan-only exit %d:\n%s", code, log.String())
	}
	if !strings.Contains(log.String(), "compiled "+fakeSuite) || strings.Contains(log.String(), "adopted") {
		t.Errorf("with no manifest the planner did not compile:\n%s", log.String())
	}
	if _, err := os.Stat(filepath.Join(fresh, compiledManifestName)); !os.IsNotExist(err) {
		t.Errorf("the planner's own compile wrote a manifest (%v): only proofrun compile writes one", err)
	}
}
