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

// The identity a verdict judges reaches part.json only through the flags the
// scripts pass `proofrun record`: a script that stopped passing -temporal,
// -image-tag-own or -gates-override would make a shared Temporal, an outside
// tag or a PROOF_GATES run look like a clean one. Each flag the scripts pass
// must also be one record defines.
func TestRepositoryProofScripts_PassRecordItsIdentityFlags(t *testing.T) {
	root := mustRepoRoot()
	recordFlags := subcommandFlags(t, "record")
	flagRE := regexp.MustCompile(`(?:^|[\s(])-([a-z][a-z0-9-]*)`)
	for _, c := range []struct {
		file, start, end string
		want             []string
	}{
		{"scripts/proof.sh", "record_args=(", "README=", []string{`-temporal "$PROOF_TEMPORAL"`, "-image-tag-own", `-gates-override "$PROOF_GATES"`, "-clean-start=", "-tree-start", "-base-commit"}},
		{"scripts/proof-part.sh", "part_record() {", "\n}\n", []string{"-temporal own", "-image-tag-own", "-clean-start=", "-tree-start", "-base-commit", "-part", "-attempt"}},
	} {
		b, err := os.ReadFile(filepath.Join(root, c.file))
		if err != nil {
			t.Fatal(err)
		}
		text := string(b)
		i := strings.Index(text, c.start)
		if i < 0 {
			t.Errorf("%s: no %q", c.file, c.start)
			continue
		}
		j := strings.Index(text[i:], c.end)
		if j < 0 {
			t.Errorf("%s: no end of the record arguments", c.file)
			continue
		}
		block := text[i : i+j]
		for _, w := range c.want {
			if !strings.Contains(block, w) {
				t.Errorf("%s's record arguments lack %s", c.file, w)
			}
		}
		for _, m := range flagRE.FindAllStringSubmatch(block, -1) {
			// One-letter words are the shell's own tests ([ -f … ], [ -n … ]).
			if len(m[1]) > 1 && !recordFlags[m[1]] {
				t.Errorf("%s passes record -%s, which record does not define", c.file, m[1])
			}
		}
	}
}
