package main

import (
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

// Static analysis and the vulnerability scan (AGENTS.md §11.1).
//
// staticcheck is part of `make lint`, so `make check`, `make merge-check` and
// the lint gate of every proof run it, with its default checks, twice: as
// built and with the integration tag the integration suites are vetted with.
// govulncheck is not: it reads the vulnerability database over the network, and
// a network failure must never fail a proof, so `make vuln` is a job of ci.yml
// and of nightly.yml and appears in no proof gate. Each rule here returns its
// problems, so a companion test shows that it bites on a weakened copy.

// makeRecipe is the first line of one Makefile target and the tab-indented
// lines under it.
func makeRecipe(mk, target string) (string, bool) {
	re := regexp.MustCompile(`(?m)^` + regexp.QuoteMeta(target) + `:[^\n]*\n(?:\t[^\n]*\n?)*`)
	m := re.FindString(mk)
	return m, m != ""
}

// lintProblems holds the Makefile to the rules above.
func lintProblems(mk string) []string {
	var p []string
	lint, ok := makeRecipe(mk, "lint")
	if !ok {
		return []string{"the Makefile has no lint target"}
	}
	for _, want := range []string{"go vet ./...", "go tool staticcheck ./...", "go tool staticcheck -tags=integration ./..."} {
		if !strings.Contains(lint, want) {
			p = append(p, fmt.Sprintf("make lint does not run %q", want))
		}
	}
	// A pinned tool is run through `go tool`, never from a PATH install or a
	// floating `go run pkg@latest`.
	if strings.Contains(lint, "@latest") || strings.Contains(lint, "go run honnef.co") {
		p = append(p, "make lint runs staticcheck other than through the go.mod tool directive")
	}
	if strings.Contains(lint, "staticcheck.conf") || strings.Contains(lint, "-checks") {
		p = append(p, "make lint changes staticcheck's check set; it runs the default checks")
	}
	vuln, ok := makeRecipe(mk, "vuln")
	if !ok || !strings.Contains(vuln, "go tool govulncheck ./...") {
		p = append(p, "make vuln does not run `go tool govulncheck ./...`")
	}
	// Nothing a proof or the fast path runs may reach the network database.
	for _, target := range []string{"lint", "check", "merge-check", "test", "build"} {
		recipe, ok := makeRecipe(mk, target)
		if ok && (strings.Contains(recipe, "vuln") || strings.Contains(recipe, "govulncheck")) {
			p = append(p, fmt.Sprintf("make %s runs the vulnerability scan, which needs the network", target))
		}
	}
	return p
}

func readRepoFile(t *testing.T, rel string) string {
	t.Helper()
	b, err := os.ReadFile(filepath.Join("..", "..", "..", filepath.FromSlash(rel)))
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

func TestMakefile_LintRunsStaticcheckAndNothingOnTheNetwork(t *testing.T) {
	mk := readRepoFile(t, "Makefile")
	if p := lintProblems(mk); len(p) > 0 {
		t.Fatalf("Makefile:\n  %s", strings.Join(p, "\n  "))
	}
	for _, c := range []struct{ name, from, to string }{
		{"the build-tagged run dropped", "\tgo tool staticcheck -tags=integration ./...\n", ""},
		{"the plain run dropped", "\tgo tool staticcheck ./...\n", ""},
		{"an unpinned staticcheck", "\tgo tool staticcheck ./...\n", "\tgo run honnef.co/go/tools/cmd/staticcheck@latest ./...\n"},
		{"a narrowed check set", "\tgo tool staticcheck ./...\n", "\tgo tool staticcheck -checks=SA4006 ./...\n"},
		{"the vulnerability scan inside lint", "\tgo tool staticcheck ./...\n", "\tgo tool staticcheck ./...\n\tgo tool govulncheck ./...\n"},
		{"the vulnerability scan dropped", "\tgo tool govulncheck ./...\n", "\ttrue\n"},
		{"merge-check running the scan", "merge-check: build lint test ", "merge-check: build lint test vuln "},
	} {
		if !strings.Contains(mk, c.from) {
			t.Fatalf("the mutation %q finds nothing to change in the Makefile", c.name)
		}
		// A target's prerequisites ride its first line, which its recipe starts with.
		if len(lintProblems(strings.Replace(mk, c.from, c.to, 1))) == 0 {
			t.Errorf("the rule accepts the weakening %q", c.name)
		}
	}
}

// vulnJobProblems: the vulnerability scan is a job of its own that runs
// `make vuln`, with nothing that lets an advisory pass.
func vulnJobProblems(file string) func(*wfFile) []string {
	return func(w *wfFile) []string {
		job, ok := w.Jobs["vuln"]
		if !ok {
			return []string{file + " has no vuln job"}
		}
		var p []string
		if job.ContinueOnError != nil {
			p = append(p, file+": jobs.vuln continues on error, so an advisory would not fail the run")
		}
		ran := false
		for i, s := range job.Steps {
			if s.ContinueOnError != nil {
				p = append(p, fmt.Sprintf("%s: jobs.vuln %s continues on error", file, s.label(i)))
			}
			if strings.TrimSpace(s.Run) == "make vuln" {
				ran = true
				if s.If != "" {
					p = append(p, fmt.Sprintf("%s: jobs.vuln %s is conditional, so the scan can be skipped", file, s.label(i)))
				}
			}
		}
		if !ran {
			p = append(p, file+": jobs.vuln does not run `make vuln`")
		}
		return p
	}
}

func TestWorkflows_TheVulnerabilityScanRunsInCIAndNightly(t *testing.T) {
	for _, file := range []string{"ci.yml", "nightly.yml"} {
		expectEachMutationRefused(t, file, vulnJobProblems(file), []mutation{
			{"no vuln job", func(w *wfFile) { delete(w.Jobs, "vuln") }},
			{"a job that continues on error", func(w *wfFile) { w.Jobs["vuln"].ContinueOnError = true }},
			{"a step that continues on error", func(w *wfFile) {
				for i := range w.Jobs["vuln"].Steps {
					if w.Jobs["vuln"].Steps[i].Run != "" {
						w.Jobs["vuln"].Steps[i].ContinueOnError = true
					}
				}
			}},
			{"a skippable scan step", func(w *wfFile) {
				for i := range w.Jobs["vuln"].Steps {
					if w.Jobs["vuln"].Steps[i].Run != "" {
						w.Jobs["vuln"].Steps[i].If = "false"
					}
				}
			}},
			{"a job that runs something else", func(w *wfFile) {
				for i := range w.Jobs["vuln"].Steps {
					if w.Jobs["vuln"].Steps[i].Run != "" {
						w.Jobs["vuln"].Steps[i].Run = "make lint"
					}
				}
			}},
		})
	}
}

// proofScanProblems: no proof gate reads the vulnerability database.
func proofScanProblems(files map[string]string) []string {
	var p []string
	for name, text := range files {
		for _, banned := range []string{"govulncheck", "make vuln"} {
			if strings.Contains(text, banned) {
				p = append(p, fmt.Sprintf("%s mentions %q: a network failure would fail a proof", name, banned))
			}
		}
	}
	return p
}

func TestProofs_NeverRunTheVulnerabilityScan(t *testing.T) {
	files := map[string]string{
		"proof.yml": workflowFile(t, "proof.yml"),
	}
	scripts, err := filepath.Glob(filepath.Join("..", "..", "..", "scripts", "*.sh"))
	if err != nil || len(scripts) == 0 {
		t.Fatalf("scripts/*.sh: %v (%d files)", err, len(scripts))
	}
	for _, s := range scripts {
		b, err := os.ReadFile(s)
		if err != nil {
			t.Fatal(err)
		}
		files["scripts/"+filepath.Base(s)] = string(b)
	}
	if p := proofScanProblems(files); len(p) > 0 {
		t.Fatalf("\n  %s", strings.Join(p, "\n  "))
	}
	files["scripts/proof.sh"] += "\nstep vuln vuln.log make vuln\n"
	if len(proofScanProblems(files)) == 0 {
		t.Error("the rule accepts a proof gate that runs `make vuln`")
	}
}
