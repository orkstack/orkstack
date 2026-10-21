package main

import (
	"flag"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
)

// flakeRun is one test's outcome in one proof.
type flakeRun struct {
	proof   string
	tree    string
	base    string
	suite   string
	pkg     string
	test    string
	outcome string
	when    int64
}

// flakeRate is one test's row in flakes.tsv. Flips counts the trees on which
// the test both passed and failed; Suspect counts failures the change could
// not explain (not counting a failure already counted as a flip).
type flakeRate struct {
	Suite    string
	Package  string
	Test     string
	Runs     int
	Fails    int
	Flips    int
	Suspect  int
	flakes   int
	LastTree string
	LastPrf  string
}

func (f flakeRate) rate() float64 {
	if f.Runs == 0 {
		return 0
	}
	return float64(f.flakes) / float64(f.Runs)
}

func cmdFlakes(args []string) int {
	fs := flag.NewFlagSet("flakes", flag.ContinueOnError)
	from := fs.String("from", "", "proof directories or globs, comma-separated")
	repo := fs.String("repo", "", "the repository to resolve a proof's change against (enables the suspect-failure signal)")
	window := fs.Int("window", 50, "the newest this many proofs of each test")
	out := fs.String("out", "", "write flakes.tsv here (default stdout)")
	md := fs.String("md", "", "write the markdown summary here")
	minRuns := fs.Int("min-runs", 1, "omit tests with fewer runs")
	if err := fs.Parse(args); err != nil || *from == "" || *window < 1 || fs.NArg() > 0 {
		fmt.Fprintln(os.Stderr, "proofrun flakes -from 'proof/*' [-repo DIR] [-window 50] [-out FILE] [-md FILE]")
		return 2
	}
	runs, err := readFlakeRuns(*from)
	if err != nil {
		fmt.Fprintln(os.Stderr, "proofrun flakes:", err)
		return 2
	}
	if *repo != "" {
		markSuspects(runs, *repo)
	}
	rows := flakeRates(runs, *window, *minRuns)
	report := flakeReport(rows)
	if *out != "" {
		if err := os.WriteFile(*out, []byte(report.tsv), 0o644); err != nil {
			fmt.Fprintln(os.Stderr, "proofrun flakes:", err)
			return 1
		}
	} else {
		fmt.Fprint(os.Stdout, report.tsv)
	}
	if *md != "" {
		if err := os.WriteFile(*md, []byte(report.markdown), 0o644); err != nil {
			fmt.Fprintln(os.Stderr, "proofrun flakes:", err)
			return 1
		}
	}
	return 0
}

// readFlakeRuns reads every proof a pattern names. A directory without a
// proof.json is not a proof and is skipped; the ledgers it does not carry are
// skipped too, so a feature proof with no integration phase is still read for
// its unit suites.
func readFlakeRuns(from string) ([]flakeRun, error) {
	var dirs []string
	for _, pattern := range strings.Split(from, ",") {
		pattern = strings.TrimSpace(pattern)
		if pattern == "" {
			continue
		}
		matches, err := filepath.Glob(pattern)
		if err != nil {
			return nil, err
		}
		if len(matches) == 0 {
			if st, err := os.Stat(pattern); err == nil && st.IsDir() {
				matches = []string{pattern}
			}
		}
		dirs = append(dirs, matches...)
	}
	sort.Strings(dirs)
	seen := map[string]bool{}
	var runs []flakeRun
	for _, dir := range dirs {
		if seen[dir] {
			continue
		}
		seen[dir] = true
		var p proofRecord
		if err := readJSON(filepath.Join(dir, "proof.json"), &p); err != nil {
			continue
		}
		var when int64
		if st, err := os.Stat(filepath.Join(dir, "proof.json")); err == nil {
			when = st.ModTime().Unix()
		}
		var unit, integration ledger
		if readJSON(filepath.Join(dir, "unit-tests.json"), &unit) == nil {
			runs = appendLedgerRuns(runs, dir, p, "unit", unit, when)
		}
		if readJSON(filepath.Join(dir, "integration-tests.json"), &integration) == nil {
			runs = appendLedgerRuns(runs, dir, p, "integration", integration, when)
		}
		var ui uiLedger
		if readJSON(filepath.Join(dir, "ui-e2e-tests.json"), &ui) == nil {
			for _, t := range ui.Tests {
				runs = append(runs, flakeRun{
					proof: dir, tree: p.Tree, base: p.BaseCommit, suite: "ui-e2e",
					pkg: t.File, test: t.Title, outcome: t.Outcome, when: when,
				})
			}
		}
	}
	return runs, nil
}

func appendLedgerRuns(runs []flakeRun, dir string, p proofRecord, suite string, l ledger, when int64) []flakeRun {
	for _, t := range l.Tests {
		if t.Test == "" {
			continue
		}
		runs = append(runs, flakeRun{
			proof: dir, tree: p.Tree, base: p.BaseCommit, suite: suite,
			pkg: t.Package, test: t.Test, outcome: t.Outcome, when: when,
		})
	}
	return runs
}

func isFlakeOutcome(outcome string) bool {
	return outcome == outcomeFail || outcome == outcomeQuarantinedFail || outcome == "suspect-fail"
}

// flakeRates keeps each test's newest window runs and counts its flips and
// suspect failures. A flip is a fail and a pass on one tree; a suspect
// failure is a failure whose proof's change could not reach the test and whose
// base's tree passed it (markSuspects marks it "suspect-fail").
func flakeRates(runs []flakeRun, window, minRuns int) []flakeRate {
	byTest := map[[3]string][]flakeRun{}
	for _, r := range runs {
		byTest[[3]string{r.suite, r.pkg, r.test}] = append(byTest[[3]string{r.suite, r.pkg, r.test}], r)
	}
	var out []flakeRate
	for key, rs := range byTest {
		sort.Slice(rs, func(i, j int) bool {
			if rs[i].when != rs[j].when {
				return rs[i].when < rs[j].when
			}
			return rs[i].proof < rs[j].proof
		})
		if len(rs) > window {
			rs = rs[len(rs)-window:]
		}
		trees := map[string]map[string]bool{}
		for _, r := range rs {
			if trees[r.tree] == nil {
				trees[r.tree] = map[string]bool{}
			}
			trees[r.tree][r.outcome] = true
		}
		row := flakeRate{Suite: key[0], Package: key[1], Test: key[2], Runs: len(rs)}
		var failRuns []flakeRun
		for _, r := range rs {
			if isFlakeOutcome(r.outcome) {
				row.Fails++
				failRuns = append(failRuns, r)
			}
		}
		flipTrees := map[string]bool{}
		for tree, outcomes := range trees {
			if outcomes[outcomePass] && (outcomes[outcomeFail] || outcomes[outcomeQuarantinedFail] || outcomes["suspect-fail"]) {
				flipTrees[tree] = true
			}
		}
		row.Flips = len(flipTrees)
		for tree := range flipTrees {
			row.LastTree = tree
			for _, r := range rs {
				if r.tree == tree {
					row.LastPrf = r.proof
				}
			}
		}
		for _, r := range failRuns {
			if flipTrees[r.tree] {
				row.flakes++
				continue
			}
			if r.outcome == "suspect-fail" {
				row.Suspect++
				row.flakes++
			}
		}
		if row.Runs < minRuns {
			continue
		}
		out = append(out, row)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].rate() != out[j].rate() {
			return out[i].rate() > out[j].rate()
		}
		if out[i].Fails != out[j].Fails {
			return out[i].Fails > out[j].Fails
		}
		return out[i].Package+"."+out[i].Test < out[j].Package+"."+out[j].Test
	})
	return out
}

// markSuspects marks a failure suspect when the proof's change set could not
// reach the test's package and the test passed on the proof's base tree in
// another proof. A proof whose base commit the repository does not have is
// skipped, never guessed at.
func markSuspects(runs []flakeRun, repo string) {
	markSuspectsWith(runs,
		func(base, commit string) (map[string]bool, string, error) { return changedDirs(repo, base, commit) },
		func(pkg string) (map[string]bool, error) { return testDeps(repo, pkg) })
}

// markSuspectsWith is markSuspects over a change reader (the directories a
// proof's change touched, relative to the repository, and its base's tree)
// and a dependency reader (the repository-relative directories a test
// package's binary imports), so a test can drive it.
func markSuspectsWith(runs []flakeRun, changedOf func(base, commit string) (map[string]bool, string, error),
	depsOf func(pkg string) (map[string]bool, error),
) {
	passedOnTree := map[string]bool{}
	for _, r := range runs {
		if r.outcome == outcomePass {
			passedOnTree[r.tree+"/"+r.pkg+"/"+r.test] = true
		}
	}
	byProof := map[string][]int{}
	for i, r := range runs {
		byProof[r.proof] = append(byProof[r.proof], i)
	}
	depsCache := map[string]map[string]bool{}
	for proof, indexes := range byProof {
		var p proofRecord
		if err := readJSON(filepath.Join(proof, "proof.json"), &p); err != nil || p.BaseCommit == "" || p.Commit == "" {
			continue
		}
		changed, baseTree, err := changedOf(p.BaseCommit, p.Commit)
		if err != nil {
			continue
		}
		for _, i := range indexes {
			if !isFlakeOutcome(runs[i].outcome) {
				continue
			}
			if !passedOnTree[baseTree+"/"+runs[i].pkg+"/"+runs[i].test] {
				continue
			}
			deps, ok := depsCache[runs[i].pkg]
			if !ok {
				deps, _ = depsOf(runs[i].pkg)
				depsCache[runs[i].pkg] = deps
			}
			if len(deps) == 0 {
				continue
			}
			reached := false
			for dir := range changed {
				if deps[dir] {
					reached = true
					break
				}
			}
			if !reached {
				runs[i].outcome = "suspect-fail"
			}
		}
	}
}

// changedDirs maps the directories of a proof's changed files, and returns
// the tree its base commit names.
func changedDirs(repo, base, commit string) (map[string]bool, string, error) {
	out, err := exec.Command("git", "-C", repo, "diff", "--name-only", base, commit).Output()
	if err != nil {
		return nil, "", err
	}
	dirs := map[string]bool{}
	for _, line := range strings.Split(string(out), "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		dir := filepath.Dir(line)
		if dir == "." {
			dir = ""
		}
		dirs[dir] = true
	}
	tree, err := exec.Command("git", "-C", repo, "rev-parse", base+"^{tree}").Output()
	if err != nil {
		return nil, "", err
	}
	return dirs, strings.TrimSpace(string(tree)), nil
}

// testDeps is the test package and the directories its test binary imports,
// relative to repo as changedDirs names them (the repository's root is "");
// a directory outside the repository (the standard library, a module) is left
// out, because no change of the repository can touch it.
func testDeps(repo, pkg string) (map[string]bool, error) {
	cmd := exec.Command("go", "list", "-deps", "-test", "-f", "{{.Dir}}", pkg)
	cmd.Dir = repo
	out, err := cmd.Output()
	if err != nil {
		// A test-only package path ("file › title" for a UI ledger) is not a
		// Go package; it has no dependency edge by this rule.
		return nil, err
	}
	root, err := filepath.Abs(repo)
	if err != nil {
		return nil, err
	}
	if top, err := exec.Command("git", "-C", root, "rev-parse", "--show-toplevel").Output(); err == nil {
		root = strings.TrimSpace(string(top))
	}
	return repoRelativeDirs(root, string(out)), nil
}

// repoRelativeDirs turns go list's absolute directories into the
// repository-relative form a diff names, dropping any outside root.
func repoRelativeDirs(root, list string) map[string]bool {
	dirs := map[string]bool{}
	for _, line := range strings.Split(list, "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		rel, err := filepath.Rel(root, line)
		if err != nil || rel == ".." || strings.HasPrefix(rel, "../") {
			continue
		}
		if rel == "." {
			rel = ""
		}
		dirs[rel] = true
	}
	return dirs
}

type flakeReportText struct {
	tsv      string
	markdown string
}

func flakeReport(rows []flakeRate) flakeReportText {
	var b strings.Builder
	b.WriteString("suite\tpackage\ttest\truns\tfails\tflips\tsuspect\trate\tlast_flip_tree\tlast_flip_proof\n")
	for _, r := range rows {
		fmt.Fprintf(&b, "%s\t%s\t%s\t%d\t%d\t%d\t%d\t%.3f\t%s\t%s\n",
			r.Suite, r.Package, r.Test, r.Runs, r.Fails, r.Flips, r.Suspect, r.rate(), r.LastTree, r.LastPrf)
	}
	var md strings.Builder
	md.WriteString("# Flake rates\n\n")
	md.WriteString("Derived from proof ledgers (`unit-tests.json`, `integration-tests.json`, `ui-e2e-tests.json`). ")
	md.WriteString("A flip is a fail and a pass on one tree; a suspect failure is one whose change could not reach the test. ")
	md.WriteString("A rate is the failing runs a flip or a suspect failure explains, over the newest window's runs.\n\n")
	md.WriteString("| Suite | Test | Runs | Fails | Flips | Suspect | Rate |\n|---|---|---|---|---|---|---|\n")
	n := len(rows)
	if n > 25 {
		n = 25
	}
	for _, r := range rows[:n] {
		fmt.Fprintf(&md, "| %s | `%s` | %d | %d | %d | %d | %.1f%% |\n",
			r.Suite, r.Test, r.Runs, r.Fails, r.Flips, r.Suspect, r.rate()*100)
	}
	return flakeReportText{tsv: b.String(), markdown: md.String()}
}

// parseFlakesTSV reads flakes.tsv, so a procedure or a test can act on a
// report without re-reading every ledger.
func parseFlakesTSV(data string) ([]flakeRate, error) {
	var out []flakeRate
	for i, line := range strings.Split(strings.TrimSpace(data), "\n") {
		if i == 0 || strings.TrimSpace(line) == "" {
			continue
		}
		f := strings.Split(line, "\t")
		if len(f) != 10 {
			return nil, fmt.Errorf("flakes.tsv line %d: %d columns", i+1, len(f))
		}
		ints := make([]int, 4)
		for j := range ints {
			v, err := strconv.Atoi(f[3+j])
			if err != nil {
				return nil, fmt.Errorf("flakes.tsv line %d: %w", i+1, err)
			}
			ints[j] = v
		}
		out = append(out, flakeRate{Suite: f[0], Package: f[1], Test: f[2],
			Runs: ints[0], Fails: ints[1], Flips: ints[2], Suspect: ints[3], LastTree: f[8], LastPrf: f[9]})
	}
	return out, nil
}
