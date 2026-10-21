package main

import (
	"os"
	"path/filepath"
	"testing"
)

func fr(proof, tree, suite, pkg, test, outcome string, when int64) flakeRun {
	return flakeRun{proof: proof, tree: tree, suite: suite, pkg: pkg, test: test, outcome: outcome, when: when}
}

func TestFlakeRates_AFlipIsAFailAndAPassOnOneTree(t *testing.T) {
	runs := []flakeRun{
		fr("p1", "tree-a", "unit", "p", "TestX", outcomePass, 1),
		fr("p2", "tree-a", "unit", "p", "TestX", outcomeFail, 2),
		fr("p3", "tree-b", "unit", "p", "TestX", outcomePass, 3),
	}
	rows := flakeRates(runs, 50, 1)
	if len(rows) != 1 {
		t.Fatalf("rows = %d", len(rows))
	}
	r := rows[0]
	if r.Runs != 3 || r.Fails != 1 || r.Flips != 1 || r.rate() != 1.0/3.0 {
		t.Errorf("row = %+v rate %.3f", r, r.rate())
	}
	if r.LastTree != "tree-a" || r.LastPrf != "p2" {
		t.Errorf("last flip = %q %q", r.LastTree, r.LastPrf)
	}
}

func TestFlakeRates_AWindowKeepsTheNewestProofs(t *testing.T) {
	var runs []flakeRun
	for i := 0; i < 5; i++ {
		outcome := outcomePass
		if i == 0 {
			outcome = outcomeFail
		}
		runs = append(runs, fr("p", "tree", "unit", "p", "TestX", outcome, int64(i)))
	}
	rows := flakeRates(runs, 2, 1)
	if rows[0].Runs != 2 || rows[0].Fails != 0 || rows[0].Flips != 0 {
		t.Errorf("window row = %+v", rows[0])
	}
}

func TestFlakeRates_ASuspectFailureIsCountedOnlyWhenUnreachable(t *testing.T) {
	runs := []flakeRun{
		fr("p1", "tree-a", "unit", "p", "TestX", outcomePass, 1),
		fr("p2", "tree-b", "unit", "p", "TestX", "suspect-fail", 2),
		fr("p3", "tree-b", "unit", "p", "TestX", outcomeFail, 3),
	}
	rows := flakeRates(runs, 50, 1)
	r := rows[0]
	if r.Fails != 2 || r.Flips != 0 || r.Suspect != 1 || r.rate() != 1.0/3.0 {
		t.Errorf("row = %+v rate %.3f", r, r.rate())
	}
}

func TestReadFlakeRuns_ReadsTheThreeLedgers(t *testing.T) {
	dir := t.TempDir()
	write := func(name, body string) {
		if err := os.WriteFile(filepath.Join(dir, name), []byte(body), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	write("proof.json", `{"tier":"full","tree":"tree-a","commit":"c1","base_commit":"c0"}`)
	write("unit-tests.json", `{"suite":"unit","tests":[{"package":"p","test":"TestU","outcome":"pass"}],"problems":[]}`)
	write("integration-tests.json", `{"suite":"integration","tests":[{"package":"p","test":"TestI","outcome":"fail"}],"problems":["p.TestI failed"]}`)
	write("ui-e2e-tests.json", `{"suite":"ui-e2e","tests":[{"file":"a.spec.ts","title":"works","outcome":"pass"}]}`)
	runs, err := readFlakeRuns(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(runs) != 3 {
		t.Fatalf("runs = %+v", runs)
	}
	seen := map[string]bool{}
	for _, r := range runs {
		seen[r.test] = true
		if r.tree != "tree-a" || r.base != "c0" {
			t.Errorf("run = %+v", r)
		}
	}
	for _, want := range []string{"TestU", "TestI", "works"} {
		if !seen[want] {
			t.Errorf("%s not read", want)
		}
	}
}

func TestParseFlakesTSV_RoundTripsTheReport(t *testing.T) {
	rows := []flakeRate{{Suite: "unit", Package: "p", Test: "TestX", Runs: 4, Fails: 2, Flips: 1, Suspect: 1,
		flakes: 2, LastTree: "tree", LastPrf: "proof"}}
	report := flakeReport(rows)
	got, err := parseFlakesTSV(report.tsv)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0].Runs != 4 || got[0].Fails != 2 || got[0].Flips != 1 || got[0].Suspect != 1 {
		t.Errorf("got = %+v", got)
	}
}

func TestFlakes_FailingOnEveryRunIsABreakNotAFlake(t *testing.T) {
	var runs []flakeRun
	for i, tree := range []string{"tree-a", "tree-a", "tree-b", "tree-c"} {
		runs = append(runs, fr("p"+itoa(i), tree, "integration", "p", "TestBroken", outcomeFail, int64(i)))
	}
	rows := flakeRates(runs, 50, 1)
	if len(rows) != 1 {
		t.Fatalf("rows = %+v", rows)
	}
	r := rows[0]
	if r.Fails != 4 || r.Flips != 0 || r.Suspect != 0 || r.rate() != 0 {
		t.Errorf("a test that fails on every run counted as a flake: %+v rate %.2f", r, r.rate())
	}
}

// suspectFixture lays out two proofs of one test: p1 passed on the base's
// tree, and p2, a change on top of it, failed. It returns the runs.
func suspectFixture(t *testing.T) []flakeRun {
	t.Helper()
	dir := t.TempDir()
	p1, p2 := filepath.Join(dir, "p1"), filepath.Join(dir, "p2")
	for d, r := range map[string]proofRecord{
		p1: {Tree: "base-tree", Commit: "base", BaseCommit: "older"},
		p2: {Tree: "change-tree", Commit: "change", BaseCommit: "base"},
	} {
		if err := os.MkdirAll(d, 0o755); err != nil {
			t.Fatal(err)
		}
		if err := writeJSON(filepath.Join(d, "proof.json"), r); err != nil {
			t.Fatal(err)
		}
	}
	return []flakeRun{
		fr(p1, "base-tree", "integration", "example.com/m/internal/engine", "TestX", outcomePass, 1),
		fr(p2, "change-tree", "integration", "example.com/m/internal/engine", "TestX", outcomeFail, 2),
	}
}

func suspectReaders(changed ...string) (func(string, string) (map[string]bool, string, error), func(string) (map[string]bool, error)) {
	trees := map[string]string{"older": "older-tree", "base": "base-tree"}
	return func(base, commit string) (map[string]bool, string, error) {
			dirs := map[string]bool{}
			for _, c := range changed {
				dirs[c] = true
			}
			return dirs, trees[base], nil
		}, func(pkg string) (map[string]bool, error) {
			// The engine's test binary imports the engine and the DSL.
			return map[string]bool{"internal/engine": true, "internal/dsl": true}, nil
		}
}

func TestFlakes_AFailureTheChangeCouldNotReachIsSuspect(t *testing.T) {
	runs := suspectFixture(t)
	changedOf, depsOf := suspectReaders("docs", "ui/src/routes")
	markSuspectsWith(runs, changedOf, depsOf)
	if runs[1].outcome != "suspect-fail" {
		t.Fatalf("a failure no changed directory reaches is %q, want suspect-fail", runs[1].outcome)
	}
	if r := flakeRates(runs, 50, 1)[0]; r.Suspect != 1 || r.rate() != 0.5 {
		t.Errorf("row = %+v", r)
	}
}

func TestFlakes_AFailureTheChangeCouldReachIsNotSuspect(t *testing.T) {
	runs := suspectFixture(t)
	changedOf, depsOf := suspectReaders("internal/dsl")
	markSuspectsWith(runs, changedOf, depsOf)
	if runs[1].outcome != outcomeFail {
		t.Fatalf("a failure the change reaches is %q, want fail", runs[1].outcome)
	}
}

// testDeps names directories the way a diff does, relative to the
// repository: before, it returned go list's absolute paths, so no change ever
// "reached" a test and every failure on a passing base was suspect.
func TestFlakes_TestDepsAreRepositoryRelative(t *testing.T) {
	if got := repoRelativeDirs("/repo", "/repo/internal/engine\n/repo\n/usr/lib/go/src/fmt\n/repository-sibling/x\n"); len(got) != 2 ||
		!got["internal/engine"] || !got[""] {
		t.Errorf("repoRelativeDirs = %v", got)
	}
	if testing.Short() {
		t.Skip("runs go list")
	}
	deps, err := testDeps(mustRepoRoot(), modulePath+"/test/tools/proofrun")
	if err != nil {
		t.Fatal(err)
	}
	if !deps["test/tools/proofrun"] {
		t.Errorf("testDeps lacks the package's own directory, repository-relative: %v", deps)
	}
	for d := range deps {
		if filepath.IsAbs(d) {
			t.Errorf("testDeps returned an absolute directory %s", d)
		}
	}
}
