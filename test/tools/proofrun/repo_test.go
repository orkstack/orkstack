package main

import (
	"path/filepath"
	"strings"
	"testing"
)

// The repository's own suites must stay selectable: every binary a test
// builds goes through testbin.Path with a literal package, so a feature proof
// knows which packages each suite really exercises.
func TestRepositoryBinaryEdgesAreResolvable(t *testing.T) {
	if testing.Short() {
		t.Skip("lists the whole module")
	}
	root, err := filepath.Abs(filepath.Join("..", "..", ".."))
	if err != nil {
		t.Fatal(err)
	}
	d, unresolved, err := loadDeps(root)
	if err != nil {
		t.Fatal(err)
	}
	if len(unresolved) > 0 {
		t.Fatalf("builds proofrun cannot resolve:\n  %s", strings.Join(unresolved, "\n  "))
	}
	var bins []string
	for _, b := range d.bins[pOrkctl] {
		bins = append(bins, relDir(b.Package))
	}
	got := strings.Join(dedupe(bins), " ")
	for _, want := range []string{"cmd/ork", "cmd/orkctl", "cmd/orker", "test/tools/cosign-sign"} {
		if !strings.Contains(got, want) {
			t.Errorf("cmd/orkctl's suite builds %q; it lacks %s", got, want)
		}
	}
	// cmd/orker's own packages reach cmd/orkctl's suite only through the
	// binary it runs.
	edge := false
	for p := range d.binDeps[pOrkerB] {
		if !d.test[cfgIntegration][pOrkctl][p] {
			if ok, how := d.dependsOn(cfgIntegration, pOrkctl, p); ok && strings.Contains(how, "runs cmd/orker") {
				edge = true
				break
			}
		}
	}
	if !edge {
		t.Error("no package reaches cmd/orkctl's suite only through cmd/orker; the binary edges are not applied")
	}
}
