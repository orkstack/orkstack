package main

import (
	"fmt"
	"os"
	"path/filepath"
	"regexp"
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

// Stop only what you started (AGENTS.md §11.1): a name or pattern kill — even
// one anchored to this worktree, like `pkill -f "$REPO/bin/orker"` — is the
// advice an operator copies onto a host where other worktrees' stacks and
// other agents' proofs run. No script, Makefile or document may carry one;
// ui/test/e2eProcessHygiene.test.ts holds the Playwright suite to the same.
func TestRepositoryNeverKillsByNameOrPattern(t *testing.T) {
	root, err := filepath.Abs(filepath.Join("..", "..", ".."))
	if err != nil {
		t.Fatal(err)
	}
	patternKill := regexp.MustCompile(`\b(pkill|killall|pgrep)\b|xargs\s+kill`)
	var files []string
	for _, glob := range []string{
		"scripts/*.sh", "scripts/github/*", "images/*/*.sh", "Makefile", "images/Makefile",
		"*.md", "docs/*.md", "deploy/*.md", "deploy/*/*.md",
		".github/workflows/*.yml", ".github/actions/*/action.yml", ".github/*.md",
	} {
		m, err := filepath.Glob(filepath.Join(root, glob))
		if err != nil {
			t.Fatal(err)
		}
		files = append(files, m...)
	}
	if len(files) < 10 {
		t.Fatalf("found only %d files to check under %s; the globs no longer match the repository", len(files), root)
	}
	var offenders []string
	for _, f := range files {
		b, err := os.ReadFile(f)
		if err != nil {
			t.Fatal(err)
		}
		for i, line := range strings.Split(string(b), "\n") {
			if patternKill.MatchString(line) {
				rel, _ := filepath.Rel(root, f)
				offenders = append(offenders, fmt.Sprintf("%s:%d: %s", rel, i+1, strings.TrimSpace(line)))
			}
		}
	}
	if len(offenders) > 0 {
		t.Errorf("kills by name or pattern:\n  %s", strings.Join(offenders, "\n  "))
	}
}
