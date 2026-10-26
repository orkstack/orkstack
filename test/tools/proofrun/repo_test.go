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
// failure in the fast gates first: lint, build, unit, vet, the two UI gates
// and the site gate run one after another, alone, before the Temporal, the
// images and the binaries, and before any gate that takes hours. The UI gates
// both read ui/.svelte-kit, which is generated once before either. The setup —
// Temporal, images, binaries and the integration test binaries — then runs as
// one concurrent group, collected before any test starts.
func TestRepositoryProofScript_RunsTheFastGatesBeforeAnythingLong(t *testing.T) {
	text := proofScript(t)
	fast := []string{"step lint ", "step build ", "step unit ", "step vet-integration ", "step ui-check ", "step ui-unit ", "step site "}
	long := []string{"step_bg temporal ", "step_bg images ", "step_bg binaries ", "step_bg integration-compile ", "run_bg activity-fixture ", "run_bg integration ", "step ui-e2e ", "step observability "}
	last := -1
	for _, g := range fast {
		i := at(text, g)
		if i < 0 {
			t.Fatalf("proof.sh has no line starting %q", g)
		}
		if i < last {
			t.Errorf("%q comes before an earlier fast gate: the fast gates run in the order lint, build, unit, vet-integration, ui-check, ui-unit, site", g)
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
	// The gates that run beside each other: the setup group's members
	// (through step_bg, which calls run_bg once) and then integration with
	// the activity fixture; nothing else is started in the background.
	if n := strings.Count(text, "\trun_bg "); n != 3 {
		t.Errorf("%d run_bg call sites, want 3 (step_bg, activity-fixture and integration)", n)
	}
	if n := strings.Count(text, "\tstep_bg "); n != 4 {
		t.Errorf("%d step_bg call sites, want 4 (temporal, images, binaries, integration-compile)", n)
	}
	// The setup group is collected, and temporal.env read, before the stack
	// environment is built and before integration or the fixture starts.
	group := at(text, "step_bg integration-compile ")
	collect := at(text[group:], "wait_bg\n")
	env := at(text[group:], ". \"$DIR/temporal.env\"")
	stack := at(text[group:], "STACK_ENV=(")
	first := at(text[group:], "run_bg activity-fixture ")
	if collect < 0 || env < collect || stack < env || first < stack {
		t.Errorf("the setup group is not collected (wait_bg), its temporal.env read, and STACK_ENV built before the first long gate starts")
	}
	// What proofrun compile builds is what proofrun integration adopts.
	for _, want := range []string{`compile_args=(-out "$DIR/bin/test")`, `integ_args=(-out "$DIR" -bin "$DIR/bin/test"`} {
		if !strings.Contains(text, want) {
			t.Errorf("proof.sh lacks %q", want)
		}
	}
	if !subcommandFlags(t, "integration")["bin"] || !subcommandFlags(t, "compile")["out"] {
		t.Error("proofrun integration -bin or proofrun compile -out is gone")
	}
}

// The site gate is one of the fast gates: the full and release tiers list it,
// the nightly tier resets the list and never adds it, the feature tier reads it
// from the selection, and the proof's gate list (PROOF_GATES, the README's
// "not run" lines, the stopped gates) knows it. The scripts' behaviour is
// proved by sitegate_test.go; this holds the text those paths read.
func TestRepositoryProofScript_ListsTheSiteGate(t *testing.T) {
	text := proofScript(t)
	allGates := regexp.MustCompile(`(?m)^ALL_GATES="([^"]*)"`).FindStringSubmatch(text)
	if allGates == nil || !contains(strings.Fields(allGates[1]), "site") {
		t.Errorf("ALL_GATES = %v: it lacks site", allGates)
	}
	full := regexp.MustCompile(`(?m)^\t+for g in ([a-z0-9 -]+); do RUN\[\$g\]=1; done$`).FindAllStringSubmatch(text, -1)
	var tiered []string
	for _, m := range full {
		if strings.Contains(m[1], "ui-unit") {
			tiered = strings.Fields(m[1])
		}
	}
	if !contains(tiered, "site") {
		t.Errorf("the full and release tiers run %v: no site gate", tiered)
	}
	if i, after := indexOf(tiered, "site"), indexOf(tiered, "ui-unit")+1; i != after {
		t.Errorf("the tiers list site at %d, and the slot right after ui-unit is %d", i, after)
	}
	nightly := text[strings.Index(text, `if [ "$TIER" = nightly ]; then`):]
	nightly = nightly[:strings.Index(nightly, "\n\tfi\n")]
	if !strings.Contains(nightly, "RUN=()") || strings.Contains(nightly, "site") {
		t.Errorf("the nightly tier's block is\n%s\nit resets the gates and adds the integration gate alone", nightly)
	}
	if !strings.Contains(text, "integration | ui_check | ui_unit | site | ui_e2e | activity_fixture | observability)") {
		t.Error("the feature tier does not read the selection's site=yes")
	}
	if !strings.Contains(text, "in_site() {") || !strings.Contains(text, "step site site.log in_site bash -c 'bun run check && bun run build'") {
		t.Error("the site gate is not `bun run check && bun run build` in site/, logged to site.log")
	}
}

func indexOf(list []string, want string) int {
	for i, s := range list {
		if s == want {
			return i
		}
	}
	return -1
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
	for _, g := range []string{"lint", "build", "unit", "vet-integration", "ui-check", "ui-unit", "site", "ui-e2e", "observability"} {
		if !strings.Contains(text, "step "+g+" ") {
			t.Errorf("gate %s is not run through step: a stopped proof would still start it", g)
		}
	}
	for _, g := range []string{"temporal", "images", "binaries", "integration-compile"} {
		if !strings.Contains(text, "\tstep_bg "+g+" ") {
			t.Errorf("setup member %s is not run through step_bg: a stopped proof would still start it", g)
		}
	}
	if i := at(text, "step_bg() {"); i < 0 || !strings.Contains(text[i:i+200], `[ -z "$FF_TRIGGER" ] || return 0`) {
		t.Error("step_bg does not refuse to start a gate once the proof has stopped")
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
		"scripts/proof.sh":      `"${run_specs[@]}"`, // the selection's files, or the UI plan's when it leaves one out
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

// The setup's links and image builds are parallel: one `go build` writes all
// nine stack binaries into bin/ (in a local proof and a CI part alike), and
// the images Makefile builds, pushes and exports its images IMAGE_JOBS at a
// time, each image a target of its own.
func TestRepositoryProofScript_BuildsTheSetupInParallel(t *testing.T) {
	text := proofScript(t)
	part, err := os.ReadFile(filepath.Join(mustRepoRoot(), "scripts", "proof-part.sh"))
	if err != nil {
		t.Fatal(err)
	}
	// STACK_BINARIES is defined before proof.sh sources proof-part.sh.
	def, src := at(text, "STACK_BINARIES=("), at(text, `. "$REPO/scripts/proof-part.sh"`)
	if def < 0 || src < 0 || def > src {
		t.Error("STACK_BINARIES is not defined before a CI part sources proof-part.sh")
	}
	for name, body := range map[string]string{"proof.sh": text, "proof-part.sh": string(part)} {
		if !strings.Contains(body, `go build -o "$DIR/bin/" "${STACK_BINARIES[@]}"`) {
			t.Errorf("%s does not link the stack binaries with one go build into bin/", name)
		}
		if regexp.MustCompile(`go build -o "\$DIR/bin/\$\(basename`).MatchString(body) {
			t.Errorf("%s still links the stack binaries one by one", name)
		}
	}
	// The same nine the Playwright suite's global setup builds when missing.
	setup, err := os.ReadFile(filepath.Join(mustRepoRoot(), "ui", "e2e", "global-setup.ts"))
	if err != nil {
		t.Fatal(err)
	}
	line := text[def : def+strings.Index(text[def:], "\n")]
	for _, m := range regexp.MustCompile(`'?([a-z-]+)'?: '(\./[a-z/-]+)'`).FindAllStringSubmatch(string(setup), -1) {
		if !strings.Contains(line, " "+m[2]+" ") && !strings.Contains(line, " "+m[2]+")") && !strings.Contains(line, "("+m[2]+" ") {
			t.Errorf("ui/e2e/global-setup.ts builds %s from %s, which STACK_BINARIES lacks", m[1], m[2])
		}
	}
	mk, err := os.ReadFile(filepath.Join(mustRepoRoot(), "images", "Makefile"))
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{
		"IMAGE_JOBS ?= 6",
		"@$(MAKE) -j$(IMAGE_JOBS) -Otarget $(addprefix image-,$(ACTIVITIES))",
		"@$(MAKE) -j$(IMAGE_JOBS) -Otarget $(addprefix push-,$(ACTIVITIES))",
		"$(addprefix push-,$(ACTIVITIES)): push-%: image-%",
		"@$(MAKE) -j$(IMAGE_JOBS) -Otarget $(addprefix oci-,$(ACTIVITIES))",
	} {
		if !strings.Contains(string(mk), want) {
			t.Errorf("images/Makefile lacks %q", want)
		}
	}
}

// The UI pre-build: started beside integration only when Playwright will run,
// waiting for integration-heavy-done (never beside a heavy test), building
// through the same ui/e2e/build-ui.ts the suite's global setup calls, and
// collected before the Playwright gate starts, so the two never build at
// once. It is never a gate: a failure leaves the gate to build as before.
func TestRepositoryProofScript_PrebuildsTheUIAfterTheHeavyJobs(t *testing.T) {
	text := proofScript(t)
	fn := at(text, "ui_prebuild() {")
	if fn < 0 {
		t.Fatal("proof.sh has no ui_prebuild")
	}
	body := text[fn : fn+strings.Index(text[fn:], "\n}\n")]
	wait := strings.Index(body, `while [ ! -e "$DIR/integration-heavy-done" ]`)
	build := strings.Index(body, "bun e2e/build-ui.ts")
	if wait < 0 || build < 0 || build < wait {
		t.Error("ui_prebuild does not wait for integration-heavy-done before it builds")
	}
	if strings.Contains(body, ">>\"$GATES_TSV\"") || strings.Contains(body, "step ") || strings.Contains(body, "run_bg ") {
		t.Error("ui_prebuild records a gate: a pre-build is a head start, never evidence")
	}
	start := at(text, "ui_prebuild >\"$DIR/ui-prebuild.log\" 2>&1 &")
	integ := at(text, "run_bg integration ")
	collect := at(text, `wait "$UI_PREBUILD"`)
	e2e := at(text, "step ui-e2e ")
	if start < 0 || start < integ || collect < start || e2e < collect {
		t.Error("the pre-build is not started after integration and collected before the Playwright gate")
	}
	if !strings.Contains(text, `if [ -s "$DIR/ui-e2e-list.json" ]; then`) {
		t.Error("the Playwright gate does not reuse the pre-build's listing")
	}
	setup, err := os.ReadFile(filepath.Join(mustRepoRoot(), "ui", "e2e", "global-setup.ts"))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(setup), "ensureUIBuild(uiDir)") || strings.Contains(string(setup), "execFileSync('bun', ['run', 'build']") {
		t.Error("ui/e2e/global-setup.ts does not build through build-ui.ts's ensureUIBuild: the pre-build and the gate would decide differently")
	}
}
