package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

const cPkg = "example.com/c"

// costSourceDir writes a proof directory costs reads: its proof.json, its
// integration ledger, and its plan (format 2 listing listed, or a format-1
// bare array when listed is nil and format1 is set).
func costSourceDir(t *testing.T, tier string, measured map[string]float64, listed []string, format1 bool) string {
	t.Helper()
	dir := t.TempDir()
	var tests []entry
	for name, s := range measured {
		tests = append(tests, entry{Package: cPkg, Test: name, Outcome: outcomePass, Seconds: s})
	}
	for _, w := range []struct {
		name string
		v    any
	}{
		{"proof.json", proofRecord{Tier: tier, Name: "src-" + tier, Commit: "0123456789abcdef"}},
		{"integration-tests.json", ledger{Suite: "integration", Tests: tests}},
	} {
		if err := writeJSON(filepath.Join(dir, w.name), w.v); err != nil {
			t.Fatal(err)
		}
	}
	switch {
	case format1:
		if err := os.WriteFile(filepath.Join(dir, "integration-plan.json"), []byte(`[{"id":"c-001","package":"`+cPkg+`","tests":["TestA"]}]`), 0o644); err != nil {
			t.Fatal(err)
		}
	case listed != nil:
		p := integrationPlan{Format: planFormat, Packages: []planPackage{{ImportPath: cPkg, Listed: listed}}}
		if err := writeJSON(filepath.Join(dir, "integration-plan.json"), p); err != nil {
			t.Fatal(err)
		}
	}
	return dir
}

// refreshCosts runs costs over sources with committed as the cost ledger's
// current text, and returns the rows it wrote and its whole text.
func refreshCosts(t *testing.T, committed string, sources ...string) (map[string]string, string) {
	t.Helper()
	quiet(t)
	out := t.TempDir()
	costsFile := filepath.Join(out, "costs.tsv")
	if err := os.WriteFile(costsFile, []byte(committed), 0o644); err != nil {
		t.Fatal(err)
	}
	args := []string{"-costs-out", costsFile, "-slow-out", filepath.Join(out, "slow.txt"), "-ui-costs-out", filepath.Join(out, "ui.tsv")}
	for _, s := range sources {
		args = append(args, "-from", s)
	}
	prev := os.Stdout
	devnull, _ := os.Open(os.DevNull)
	os.Stdout = devnull
	code := cmdCosts(args)
	os.Stdout = prev
	if code != 0 {
		t.Fatalf("costs exit %d", code)
	}
	b, _ := os.ReadFile(costsFile)
	rows := map[string]string{}
	for _, line := range strings.Split(string(b), "\n") {
		f := strings.Split(line, "\t")
		if len(f) == 3 && !strings.HasPrefix(line, "#") {
			rows[f[1]] = f[2]
		}
	}
	return rows, string(b)
}

func committedCosts(rows ...string) string {
	var b strings.Builder
	b.WriteString("# source: proof/older\n")
	for i := 0; i+1 < len(rows); i += 2 {
		b.WriteString(cPkg + "\t" + rows[i] + "\t" + rows[i+1] + "\n")
	}
	return b.String()
}

func TestCosts_ANightlyOnlySourceKeepsEveryOtherRow(t *testing.T) {
	// A nightly proof runs only TestSoak, and its plan lists the package's
	// every test: only TestSoak's row changes.
	nightly := costSourceDir(t, tierNightly, map[string]float64{"TestSoak": 250}, []string{"TestA", "TestB", "TestSoak"}, false)
	rows, _ := refreshCosts(t, committedCosts("TestA", "10.00", "TestB", "20.00", "TestSoak", "300.00"), nightly)
	want := map[string]string{"TestA": "10.00", "TestB": "20.00", "TestSoak": "250.00"}
	if len(rows) != len(want) {
		t.Fatalf("rows = %v, want %v", rows, want)
	}
	for k, v := range want {
		if rows[k] != v {
			t.Errorf("%s = %q, want %q", k, rows[k], v)
		}
	}
}

func TestCosts_OnlyATestNoSourcePlanListsIsDropped(t *testing.T) {
	full := costSourceDir(t, tierFull, map[string]float64{"TestA": 12}, []string{"TestA", "TestB"}, false)
	rows, _ := refreshCosts(t, committedCosts("TestA", "10.00", "TestB", "20.00", "TestGone", "5.00"), full)
	if rows["TestA"] != "12.00" || rows["TestB"] != "20.00" {
		t.Errorf("rows = %v: a measured row is the newest, a listed unmeasured row is kept", rows)
	}
	if _, ok := rows["TestGone"]; ok {
		t.Errorf("a test no source's plan lists kept its row: %v", rows)
	}
	// The newest source that measured a test wins.
	newer := costSourceDir(t, tierNightly, map[string]float64{"TestA": 7}, []string{"TestA", "TestB"}, false)
	if rows, _ := refreshCosts(t, committedCosts(), full, newer); rows["TestA"] != "7.00" {
		t.Errorf("TestA = %q, want the newer source's 7.00", rows["TestA"])
	}
}

func TestCosts_AFormat1PlanFallsBackToItsLedgersEntries(t *testing.T) {
	old := costSourceDir(t, tierFull, map[string]float64{"TestA": 3}, nil, true)
	rows, text := refreshCosts(t, committedCosts("TestA", "10.00", "TestB", "20.00"), old)
	if rows["TestA"] != "3.00" {
		t.Errorf("TestA = %q, want 3.00", rows["TestA"])
	}
	// "Listed" is the ledger's entries: TestB has none, so it is dropped.
	if _, ok := rows["TestB"]; ok {
		t.Errorf("rows = %v", rows)
	}
	if !strings.Contains(text, "# note: proof/src-full (commit 0123456789ab) has no format-2 plan: the tests it lists are its ledger's entries") {
		t.Errorf("the costs file does not note the fallback:\n%s", text)
	}
}
