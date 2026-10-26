package main

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"sort"
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

// A runner's setup can stall where the tree is fine: GitHub Actions run
// 40346476240 (2026-10-26) lost `activity-fixture (json)` to an artifact
// download that made no progress for its 3-minute limit, and `ui-e2e (3)` to
// `playwright install --with-deps chromium` apt-installing at 50 KB/s. Both
// fixes are pinned here: every fetch of a run's own artifact, and the browser
// image's pull, go through .github/actions/proof-fetch — one retry with a
// longer limit, a bounded total, every attempt in the setup log the part
// carries into part.json and the merger into proof.json — and never a job
// re-run; and the ui-e2e parts drive the pinned browser container, with the
// host's Chromium installed only in the shard whose files drive one.
func TestRepositoryCIParts_FetchWithOneBoundedRetry(t *testing.T) {
	root := mustRepoRoot()
	action, err := os.ReadFile(filepath.Join(root, ".github", "actions", "proof-fetch", "action.yml"))
	if err != nil {
		t.Fatal(err)
	}
	text := string(action)
	// Two attempts, the second with twice the limit; both bounded by
	// `timeout`, which kills what makes no progress.
	if !strings.Contains(text, "for limit in 180 360; do") {
		t.Error("proof-fetch does not make exactly two attempts of 180 s then 360 s")
	}
	if strings.Count(text, `timeout -k 10 "$limit" `) != 3 {
		t.Errorf("proof-fetch bounds %d of its three fetch commands (artifact, pattern, image) with timeout", strings.Count(text, `timeout -k 10 "$limit" `))
	}
	if strings.Contains(text, "gh run rerun") || strings.Contains(text, "continue-on-error") {
		t.Error("proof-fetch re-runs a job or continues on error; a setup failure needs a new run of the same tree")
	}
	// The lines the merger reads back (merge.go setupAttemptRE).
	for _, line := range []string{
		`line="fetch $what: attempt $n ok in ${took}s (limit ${limit}s)"`,
		`line="fetch $what: attempt $n timed out after ${took}s (limit ${limit}s)"`,
		`line="fetch $what: attempt $n failed, exit $code after ${took}s (limit ${limit}s)"`,
	} {
		if !strings.Contains(text, line) {
			t.Errorf("proof-fetch does not write %s", line)
		}
		sample := strings.NewReplacer("$what", "artifact plan-a1", "$n", "2", "${took}", "14", "${limit}", "360", "$code", "1").
			Replace(strings.TrimSuffix(strings.TrimPrefix(line, `line="`), `"`))
		if !setupAttemptRE.MatchString(sample) {
			t.Errorf("the merger's setupAttemptRE does not match the action's line %q", sample)
		}
	}
	if !strings.Contains(text, `echo "$line" | tee -a "$LOG"`) {
		t.Error("proof-fetch does not append each attempt to the setup log")
	}

	w := loadWorkflow(t, "proof.yml")
	for _, name := range w.jobNames() {
		job := w.Jobs[name]
		fetches, part := 0, -1
		for i, s := range job.Steps {
			if strings.HasPrefix(s.Uses, "actions/download-artifact@") {
				t.Errorf("jobs.%s %s downloads with actions/download-artifact, one unbounded attempt; use %s", name, s.label(i), proofFetchAction)
			}
			if strings.Contains(s.Run, "scripts/proof.sh --part") {
				part = i
			}
			if s.Uses != proofFetchAction {
				continue
			}
			fetches++
			// 180 s + 360 s = 9 minutes; the step's limit leaves the second
			// attempt its full time and still fails a stalled runner inside
			// ten, which keeps the job's own limit small.
			if n, _, _ := timeoutMinutes(s.Timeout); n != 10 {
				t.Errorf("jobs.%s %s has timeout-minutes %v, want 10 (two attempts of 3 and 6 minutes)", name, s.label(i), s.Timeout)
			}
			if withString(s, "image") == "" && withString(s, "token") != "${{ github.token }}" {
				t.Errorf("jobs.%s %s fetches an artifact without the run's token", name, s.label(i))
			}
			if part >= 0 {
				t.Errorf("jobs.%s %s fetches after the part step; its log would miss the part", name, s.label(i))
			}
		}
		if fetches == 0 {
			continue
		}
		if job.Permissions["actions"] != "read" {
			t.Errorf("jobs.%s fetches artifacts with `gh run download` but has no actions: read", name)
		}
		if part >= 0 && !strings.Contains(job.Steps[part].Run, `--setup-log "$RUNNER_TEMP/ci/setup.log"`) {
			t.Errorf("jobs.%s's part step does not pass --setup-log, so the fetch attempts never reach its part.json", name)
		}
	}

	// The part copies the log in and records it; the merger lists a retry.
	part, err := os.ReadFile(filepath.Join(root, "scripts", "proof-part.sh"))
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{`cp "$SETUP_LOG" "$DIR/setup.log"`, `args+=(-setup-log "$DIR/setup.log")`} {
		if !strings.Contains(string(part), want) {
			t.Errorf("scripts/proof-part.sh lacks %s", want)
		}
	}
	if !strings.Contains(proofScript(t), `--setup-log) [ $# -ge 2 ] || invalid`) {
		t.Error("scripts/proof.sh does not accept --setup-log")
	}
}

// hostChromiumSpecs are the spec files that drive the HOST's Chromium, read
// from the suite itself: the project ui/playwright.config.ts leaves without a
// connection to the container (its testMatch), and every spec that launches
// a browser of its own (network-change.spec.ts's positive control).
func hostChromiumSpecs(t *testing.T) []string {
	t.Helper()
	root := mustRepoRoot()
	cfg, err := os.ReadFile(filepath.Join(root, "ui", "playwright.config.ts"))
	if err != nil {
		t.Fatal(err)
	}
	files, err := filepath.Glob(filepath.Join(root, "ui", "e2e", "*.spec.ts"))
	if err != nil {
		t.Fatal(err)
	}
	var specs []string
	// A project whose `use` does not spread isolatedBrowser unconditionally
	// drives the host: every spec file its testMatch (a JavaScript regular
	// expression Go's syntax also reads) matches is one of them.
	for _, m := range regexp.MustCompile(`(?s)\{\s*name: '([a-z-]+)',(.*?)testMatch: /([^/\n]+)/`).FindAllStringSubmatch(string(cfg), -1) {
		if strings.Contains(m[2], "use: { browserName: 'chromium', ...isolatedBrowser }") {
			continue
		}
		match, err := regexp.Compile(m[3])
		if err != nil {
			t.Fatalf("project %s: testMatch %q: %v", m[1], m[3], err)
		}
		for _, f := range files {
			if match.MatchString(filepath.Base(f)) {
				specs = append(specs, strings.TrimSuffix(filepath.Base(f), ".spec.ts"))
			}
		}
	}
	for _, f := range files {
		b, err := os.ReadFile(f)
		if err != nil {
			t.Fatal(err)
		}
		if strings.Contains(string(b), "chromium.launch(") {
			specs = append(specs, strings.TrimSuffix(filepath.Base(f), ".spec.ts"))
		}
	}
	sort.Strings(specs)
	if len(specs) == 0 {
		t.Fatal("no spec drives the host's Chromium; the rule below would be vacuous")
	}
	return specs
}

func TestRepositoryCIParts_DriveChromiumInTheContainer(t *testing.T) {
	root := mustRepoRoot()
	specs := hostChromiumSpecs(t)
	if want := []string{"network-change", "run-page-ceiling", "run-page-reduced-ceiling"}; strings.Join(specs, ",") != strings.Join(want, ",") {
		t.Fatalf("the specs driving the host's Chromium are %v, want %v: update the rule in proof.yml and proof-part.sh with this test", specs, want)
	}
	alternation := "(" + strings.Join(specs, "|") + ")"

	w := loadWorkflow(t, "proof.yml")
	job := w.Jobs["ui-e2e"]
	if job == nil {
		t.Fatal("proof.yml has no ui-e2e job")
	}
	image, decide, install, pull, part := -1, -1, -1, -1, -1
	for i, s := range job.Steps {
		switch {
		case s.ID == "browser-image":
			image = i
			if !strings.Contains(s.Run, "ui/e2e/browser-image.txt") {
				t.Errorf("jobs.ui-e2e %s does not read the pinned browser image from ui/e2e/browser-image.txt", s.label(i))
			}
		case s.Uses == proofFetchAction && withString(s, "image") != "":
			pull = i
			if withString(s, "image") != "${{ steps.browser-image.outputs.image }}" {
				t.Errorf("jobs.ui-e2e %s pulls %q, not the reference browser-image.txt names", s.label(i), withString(s, "image"))
			}
		case s.ID == "host-chromium":
			decide = i
			if !strings.Contains(s.Run, `"$RUNNER_TEMP/ci/plan/ui-plan.json"`) || !strings.Contains(s.Run, `select(.index == $k)`) {
				t.Errorf("jobs.ui-e2e %s does not decide from this shard's files in the UI plan", s.label(i))
			}
			if !strings.Contains(s.Run, `select(test("`+alternation+`\\.spec\\.ts$"))`) {
				t.Errorf("jobs.ui-e2e %s does not name exactly the specs that drive the host's Chromium, %s", s.label(i), alternation)
			}
			if envString(s, "SHARD") != "${{ matrix.shard }}" {
				t.Errorf("jobs.ui-e2e %s does not take the shard through env", s.label(i))
			}
		case strings.Contains(s.Run, "playwright install"):
			install = i
			if normalizeIf(s.If) != "steps.host-chromium.outputs.needed == 'true'" {
				t.Errorf("jobs.ui-e2e %s installs the host's Chromium under %q; want only when the shard needs it", s.label(i), s.If)
			}
			if !strings.Contains(s.Run, "--with-deps") {
				t.Errorf("jobs.ui-e2e %s installs Chromium without its system libraries", s.label(i))
			}
		case strings.Contains(s.Run, "scripts/proof.sh --part"):
			part = i
			if strings.Contains(s.Run, "ORK_E2E_BROWSER") || envString(s, "ORK_E2E_BROWSER") != "" {
				t.Errorf("jobs.ui-e2e %s chooses the browser; the suite's global setup drives the container", s.label(i))
			}
		}
	}
	for what, i := range map[string]int{"browser image": image, "its pull": pull, "the host-chromium decision": decide, "the host install": install, "the UI shard": part} {
		if i < 0 {
			t.Errorf("jobs.ui-e2e has no step for %s", what)
		}
	}
	if !(image < pull && pull < decide && decide < install && install < part) {
		t.Errorf("jobs.ui-e2e orders its steps image %d, pull %d, decide %d, install %d, part %d; want that order", image, pull, decide, install, part)
	}
	if decide >= 0 {
		planFetched := false
		for _, s := range job.Steps[:decide] {
			if fetchedArtifact(s) == "${{ needs.plan.outputs.plan_artifact }}" {
				planFetched = true
			}
		}
		if !planFetched {
			t.Error("jobs.ui-e2e decides on the UI plan before fetching it")
		}
	}

	// The part applies the same rule as a prerequisite, so a missing host
	// Chromium is refused explicitly and only where a spec drives one.
	src, err := os.ReadFile(filepath.Join(root, "scripts", "proof-part.sh"))
	if err != nil {
		t.Fatal(err)
	}
	ps := string(src)
	if !strings.Contains(ps, `grep -qE '`+alternation+`\.spec\.ts$'`) {
		t.Errorf("scripts/proof-part.sh does not name exactly the specs that drive the host's Chromium, %s", alternation)
	}
	if !strings.Contains(ps, `part_ui_shard_drives_host_chromium "${PART#ui-e2e:}"; then`) ||
		!strings.Contains(ps, `part_need "Playwright's Chromium on this host`) {
		t.Error("scripts/proof-part.sh does not require the host's Chromium for the shard that drives it")
	}
	if strings.Contains(ps, "ORK_E2E_BROWSER=host") {
		t.Error("scripts/proof-part.sh drives the host's Chromium for the whole suite")
	}
	setup, err := os.ReadFile(filepath.Join(root, "ui", "e2e", "global-setup.ts"))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(setup), "startBrowserServer(") {
		t.Error("ui/e2e/global-setup.ts no longer starts the browser server the parts rely on")
	}
}

// scripts/proof-collect.sh assembles the merged proof's copy of the parts:
// every part's records; the events and per-job logs only of a part that did
// not pass (a gate not `pass`, or no part.json); never a part's bin/,
// layouts/ or artifacts/; and GitHub's job records. With the binaries out, a
// green run's merged proof was still 921 MB of passing parts' events and
// logs (run 40388322907), against a five-minute upload limit.
func TestRepositoryProofCollect_KeepsRecordsAndFailingPartsDetails(t *testing.T) {
	root := mustRepoRoot()
	dir := t.TempDir()
	parts, jobs, out := filepath.Join(dir, "parts"), filepath.Join(dir, "jobs"), filepath.Join(dir, "proof")
	write := func(rel, text string) {
		t.Helper()
		p := filepath.Join(dir, rel)
		if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(p, []byte(text), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	passed := `{"part":"integration:1","gates":[{"gate":"temporal","result":"pass"},{"gate":"integration","result":"pass"}]}`
	failed := `{"part":"integration:2","gates":[{"gate":"temporal","result":"pass"},{"gate":"integration","result":"fail"}]}`
	for _, f := range []string{
		// A passing shard: records stay, the rest goes.
		"parts/part-integration-1-a1/part.json:" + passed,
		"parts/part-integration-1-a1/gates.tsv:integration\tpass\t9\tintegration.log\tproofrun integration",
		"parts/part-integration-1-a1/integration-tests.json:{}",
		"parts/part-integration-1-a1/integration.log:progress",
		"parts/part-integration-1-a1/setup.log:fetch artifact plan-a1: attempt 1 ok in 6s (limit 180s)",
		"parts/part-integration-1-a1/integration.events.jsonl:{}",
		"parts/part-integration-1-a1/integration/engine-001.log:big",
		"parts/part-integration-1-a1/bin/ork:binary",
		"parts/part-integration-1-a1/artifacts/run/a:artifact",
		// A failing shard: its events and per-job logs come too.
		"parts/part-integration-2-a1/part.json:" + failed,
		"parts/part-integration-2-a1/integration-tests.json:{}",
		"parts/part-integration-2-a1/integration.events.jsonl:{}",
		"parts/part-integration-2-a1/integration/engine-002.log:failure output",
		"parts/part-integration-2-a1/bin/ork:binary",
		// A shard whose process died: no part.json, so everything it left.
		"parts/part-integration-3-a1/integration.events.jsonl:{}",
		"parts/part-integration-3-a1/bin/ork:binary",
		// The fast part passed; the plan's bin/ and the build's layouts never travel.
		"parts/part-fast-a1/part.json:" + `{"part":"fast","gates":[{"gate":"unit","result":"pass"}]}`,
		"parts/part-fast-a1/unit-tests.json:{}",
		"parts/part-fast-a1/lint.log:ok",
		"parts/part-fast-a1/unit.log:verbose",
		"parts/part-fast-a1/unit.events.jsonl:{}",
		"parts/plan-a1/part.json:" + `{"part":"plan","gates":[]}`,
		"parts/plan-a1/integration-plan.json:{}",
		"parts/plan-a1/bin/test/m.test:binary",
		"parts/part-build-a1/part.json:" + `{"part":"build","gates":[{"gate":"images","result":"pass"}]}`,
		"parts/part-build-a1/layouts/activity-shell/index.json:{}",
		"jobs/attempt-1.json:{}",
	} {
		rel, text, _ := strings.Cut(f, ":")
		write(rel, text)
	}
	cmd := exec.Command("bash", filepath.Join(root, "scripts", "proof-collect.sh"), parts, jobs, out)
	outText, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("proof-collect.sh: %v\n%s", err, outText)
	}
	var got []string
	if err := filepath.WalkDir(out, func(p string, d os.DirEntry, err error) error {
		if err == nil && !d.IsDir() {
			rel, _ := filepath.Rel(out, p)
			got = append(got, filepath.ToSlash(rel))
		}
		return err
	}); err != nil {
		t.Fatal(err)
	}
	sort.Strings(got)
	want := []string{
		"jobs/attempt-1.json",
		"parts/part-build-a1/part.json",
		"parts/part-fast-a1/lint.log",
		"parts/part-fast-a1/part.json",
		"parts/part-fast-a1/unit-tests.json",
		"parts/part-integration-1-a1/gates.tsv",
		"parts/part-integration-1-a1/integration-tests.json",
		"parts/part-integration-1-a1/integration.log",
		"parts/part-integration-1-a1/part.json",
		"parts/part-integration-1-a1/setup.log",
		"parts/part-integration-2-a1/integration-tests.json",
		"parts/part-integration-2-a1/integration.events.jsonl",
		"parts/part-integration-2-a1/integration/engine-002.log",
		"parts/part-integration-2-a1/part.json",
		"parts/part-integration-3-a1/integration.events.jsonl",
		"parts/plan-a1/integration-plan.json",
		"parts/plan-a1/part.json",
	}
	if strings.Join(got, "\n") != strings.Join(want, "\n") {
		t.Errorf("the merged proof holds\n  %s\nwant\n  %s\n%s", strings.Join(got, "\n  "), strings.Join(want, "\n  "), outText)
	}
	for _, line := range []string{
		"collect part-integration-1-a1: passed; records only",
		"collect part-integration-2-a1: did not pass, or left no part.json; records, events and logs",
		"collect part-integration-3-a1: did not pass, or left no part.json; records, events and logs",
	} {
		if !strings.Contains(string(outText), line) {
			t.Errorf("the script did not say %q:\n%s", line, outText)
		}
	}
	// Without the parts, nothing is written and the exit says why.
	if err := exec.Command("bash", filepath.Join(root, "scripts", "proof-collect.sh"), filepath.Join(dir, "absent"), jobs, filepath.Join(dir, "p2")).Run(); err == nil {
		t.Error("an absent parts directory was accepted")
	}

	// The fast part trims its own unit log and events once its unit gate
	// passed, so its artifact matches docs/PROOFS.md §7's row.
	part, err := os.ReadFile(filepath.Join(root, "scripts", "proof-part.sh"))
	if err != nil {
		t.Fatal(err)
	}
	ps := string(part)
	trim := at(ps, `if grep -q $'^unit\tpass\t' "$DIR/gates.tsv"; then`)
	unit := at(ps, "part_run unit unit.log ")
	if trim < 0 || !strings.Contains(ps, `rm -f "$DIR/unit.log" "$DIR/unit.events.jsonl"`) {
		t.Error("scripts/proof-part.sh does not leave the unit log and events out of a part whose unit gate passed")
	}
	if unit < 0 || trim < unit {
		t.Error("scripts/proof-part.sh trims the unit log before the unit gate has run")
	}
}

// The runner-health floor (AGENTS.md §11.2): every CI part measures its
// runner FIRST, before any gate, through `proofrun runner-health` against
// scripts/proof-runner-floors.tsv, into runner-health.json beside its
// records; the stack parts stop below the floor, and exactly the parts the
// merger's floorBinds names; the record carries the file; the local proof
// measures for information into setup.log; and the CI summary names a
// degraded runner.
func TestRepositoryCIParts_MeasureTheRunnerFirst(t *testing.T) {
	root := mustRepoRoot()
	part, err := os.ReadFile(filepath.Join(root, "scripts", "proof-part.sh"))
	if err != nil {
		t.Fatal(err)
	}
	text := string(part)
	main := text[strings.Index(text, "part_main() {"):]
	health, firstGate := at(main, "if ! part_runner_health; then"), at(main, "part_run ")
	if health < 0 {
		t.Fatal("scripts/proof-part.sh's part_main does not run part_runner_health")
	}
	if firstGate < 0 || health > firstGate {
		t.Error("scripts/proof-part.sh runs a gate before the runner-health step")
	}
	// The part's own work begins where it decides whether observability is
	// due; the first `case "$PART"` above it only validates the name.
	if work := at(main, "OBSERVABILITY_REQUIRED=0"); work < 0 || health > work {
		t.Error("scripts/proof-part.sh measures the runner after the part's work begins")
	}
	for _, want := range []string{
		`"$PR" runner-health -class "$class" -floors scripts/proof-runner-floors.tsv -out "$DIR/runner-health.json"`,
		`[ -f "$DIR/runner-health.json" ] && args+=(-runner-health "$DIR/runner-health.json")`,
		`printf 'github-hosted/%s/%scpu\n' "$ImageOS" "$(nproc)"`,
		"\t\tpart_record 1 || true\n",
	} {
		if !strings.Contains(text, want) {
			t.Errorf("scripts/proof-part.sh lacks %s", want)
		}
	}
	// The parts the floor stops are exactly the ones the merger holds to it.
	binds := regexp.MustCompile(`part_floor_binds\(\) \{\n\tcase "\$PART" in\n\t([^\n]+)\) return 0 ;;`).FindStringSubmatch(text)
	if binds == nil {
		t.Fatal("scripts/proof-part.sh has no part_floor_binds case")
	}
	for part, pattern := range map[string]string{"integration:3": "integration:*", "ui-e2e:1": "ui-e2e:*", "activity-fixture:git": "activity-fixture:*", "observability": "observability"} {
		if !floorBinds(part) {
			t.Errorf("floorBinds(%s) is false, and scripts/proof-part.sh stops it", part)
		}
		if !strings.Contains(binds[1], pattern) {
			t.Errorf("scripts/proof-part.sh's part_floor_binds lacks %s, which floorBinds holds to the floor", pattern)
		}
	}
	for _, part := range []string{"plan", "build", "fast"} {
		if floorBinds(part) || strings.Contains(binds[1], part) {
			t.Errorf("the floor binds the %s part, which measures for information only", part)
		}
	}
	// The real floors table parses, so the step never exits 2 on it.
	if _, err := readRunnerFloors(filepath.Join(root, runnerFloorsFile)); err != nil {
		t.Errorf("the repository's floors table does not parse: %v", err)
	}
	// The local proof measures before its first gate, into setup.log, and
	// records it; the setup group appends to that log rather than replacing
	// the measurement.
	script := proofScript(t)
	measure, lint := at(script, `"$PR" runner-health -class local`), at(script, "step lint ")
	if measure < 0 || lint < 0 || measure > lint {
		t.Error("scripts/proof.sh does not measure the runner before its first gate")
	}
	for _, want := range []string{
		`-floors scripts/proof-runner-floors.tsv -out "$DIR/runner-health.json" >"$DIR/setup.log" 2>&1`,
		`done >>"$DIR/setup.log"`,
		`if [ -f "$DIR/runner-health.json" ]; then record_args+=(-runner-health "$DIR/runner-health.json"); fi`,
	} {
		if !strings.Contains(script, want) {
			t.Errorf("scripts/proof.sh lacks %s", want)
		}
	}
	if !subcommandFlags(t, "record")["runner-health"] || !subcommandFlags(t, "merge")["runner-floors"] {
		t.Error("record -runner-health or merge -runner-floors is missing")
	}
	wf := workflowFile(t, "proof.yml")
	if !strings.Contains(wf, `.degraded_runners[]?`) {
		t.Error(".github/workflows/proof.yml's summary does not name a degraded runner")
	}
}
