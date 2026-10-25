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

// proofScript is scripts/proof.sh's text.
func proofScript(t *testing.T) string {
	t.Helper()
	b, err := os.ReadFile(filepath.Join(mustRepoRoot(), "scripts", "proof.sh"))
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

// at is the index of the first line of text that starts with prefix (after
// its tabs), or -1: the script's own order, not a mention in a comment.
func at(text, prefix string) int {
	pos := 0
	for _, line := range strings.SplitAfter(text, "\n") {
		if strings.HasPrefix(strings.TrimLeft(line, "\t"), prefix) {
			return pos
		}
		pos += len(line)
	}
	return -1
}

// A failed full proof can never become eligible, so the script finds a
// failure in the fast gates first: lint, build, unit, vet, and the two UI
// gates run one after another, alone, before the Temporal, the images and the
// binaries, and before any gate that takes hours. The UI gates both read
// ui/.svelte-kit, which is generated once before either.
func TestRepositoryProofScript_RunsTheFastGatesBeforeAnythingLong(t *testing.T) {
	text := proofScript(t)
	fast := []string{"step lint ", "step build ", "step unit ", "step vet-integration ", "step ui-check ", "step ui-unit "}
	long := []string{"step temporal ", "step images ", "step binaries ", "run_bg activity-fixture ", "run_bg integration ", "step ui-e2e ", "step observability "}
	last := -1
	for _, g := range fast {
		i := at(text, g)
		if i < 0 {
			t.Fatalf("proof.sh has no line starting %q", g)
		}
		if i < last {
			t.Errorf("%q comes before an earlier fast gate: the fast gates run in the order lint, build, unit, vet-integration, ui-check, ui-unit", g)
		}
		last = i
	}
	for _, g := range long {
		i := at(text, g)
		if i < 0 {
			// run_bg gates sit in an indented block; at() strips tabs, so a
			// miss is a rename.
			t.Fatalf("proof.sh has no line starting %q", g)
		}
		if i < last {
			t.Errorf("%q comes before the fast gates have finished", g)
		}
	}
	sync := at(text, "in_ui ./node_modules/.bin/svelte-kit sync")
	if sync < 0 {
		t.Fatal("proof.sh does not run svelte-kit sync")
	}
	for _, g := range []string{"step ui-check ", "step ui-unit "} {
		if sync > at(text, g) {
			t.Errorf("svelte-kit sync runs after %q: both UI gates need ui/.svelte-kit first", g)
		}
	}
	if n := strings.Count(text, "svelte-kit sync >"); n != 1 {
		t.Errorf("svelte-kit sync is generated %d times, want once", n)
	}
	// The only gates that run beside each other are the two that start
	// together; nothing else is started in the background.
	if n := strings.Count(text, "\trun_bg "); n != 2 {
		t.Errorf("%d run_bg call sites, want 2 (activity-fixture and integration)", n)
	}
}

// Fail fast (PROOF_FAIL_FAST): on by default, switched off by 0, validated,
// handed to proofrun integration, and recorded for the gates it stops or never
// starts — which proofrun merge must know how to judge.
func TestRepositoryProofScript_FailsFast(t *testing.T) {
	text := proofScript(t)
	for _, want := range []string{
		`PROOF_FAIL_FAST="${PROOF_FAIL_FAST:-1}"`,
		`case "$PROOF_FAIL_FAST" in 0 | 1) ;;`,
		`if [ "$PROOF_FAIL_FAST" = 1 ]; then integ_args+=(-fail-fast); fi`,
		`{ [ "$PROOF_FAIL_FAST" = 1 ] && [ -z "$FF_TRIGGER" ]; } || return 0`,
		"stop_running_gates",
		`not-started`,
		`merge_args+=(-stopped)`,
	} {
		if !strings.Contains(text, want) {
			t.Errorf("proof.sh lacks %q", want)
		}
	}
	integration := subcommandFlags(t, "integration")
	merge := subcommandFlags(t, "merge")
	if !integration["fail-fast"] {
		t.Error("proofrun integration defines no -fail-fast")
	}
	if !merge["stopped"] {
		t.Error("proofrun merge defines no -stopped")
	}
	// Playwright and observability start only while nothing has failed, and
	// every gate step goes through step, which checks that.
	i := at(text, "step() {")
	if i < 0 || !strings.Contains(text[i:i+300], `[ -z "$FF_TRIGGER" ] || return 0`) {
		t.Error("step does not refuse to start a gate once the proof has stopped")
	}
	for _, g := range []string{"lint", "build", "unit", "vet-integration", "ui-check", "ui-unit", "ui-e2e", "observability", "temporal", "images", "binaries"} {
		if !strings.Contains(text, "step "+g+" ") {
			t.Errorf("gate %s is not run through step: a stopped proof would still start it", g)
		}
	}
	// A stopped gate is ended through the pid its own run recorded.
	if !strings.Contains(text, `kill -TERM "$pid"`) || !strings.Contains(text, `printf '%s\n' "$cpid" >"$DIR/running.$gate"`) {
		t.Error("proof.sh no longer stops a gate through the command pid it recorded")
	}
}

// Playwright's --project takes every word after it as a project name, so a
// spec file passed after it is refused as an unknown project and the
// invocation runs nothing. Every split UI invocation of the gate passes its
// spec files before --project, and passes --pass-with-no-tests, because a
// selection or a shard may hold no file of one of the projects.
func TestRepositoryPlaywrightProjectInvocationsPassFilesFirst(t *testing.T) {
	root := mustRepoRoot()
	for file, files := range map[string]string{
		"scripts/proof.sh":      `"${SPEC_ARGS[@]}"`,
		"scripts/proof-part.sh": `"${rel[@]}"`,
	} {
		src, err := os.ReadFile(filepath.Join(root, file))
		if err != nil {
			t.Fatal(err)
		}
		seen := 0
		for n, line := range strings.Split(string(src), "\n") {
			project := strings.Index(line, "--project ")
			if project < 0 || strings.HasPrefix(strings.TrimSpace(line), "#") {
				continue
			}
			seen++
			at := strings.Index(line, files)
			switch {
			case at < 0:
				t.Errorf("%s:%d: a --project invocation without %s: %s", file, n+1, files, strings.TrimSpace(line))
			case at > project:
				t.Errorf("%s:%d: %s after --project, which takes it as project names: %s", file, n+1, files, strings.TrimSpace(line))
			}
			if !strings.Contains(line, "--pass-with-no-tests") {
				t.Errorf("%s:%d: a --project invocation without --pass-with-no-tests: %s", file, n+1, strings.TrimSpace(line))
			}
		}
		if seen != 2 {
			t.Errorf("%s: %d --project invocations, want the parallel one and the solo one", file, seen)
		}
	}
}
