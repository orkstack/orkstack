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
