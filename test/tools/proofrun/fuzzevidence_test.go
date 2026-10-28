package main

import (
	"path/filepath"
	"strings"
	"testing"
)

const fuzzSuite = modulePath + "/test/tools/proofrun/testdata/fuzzsuite"

// A Fuzz target the evidence selector names must be one the proof can run and
// record: go test runs its seeds, f.Add and testdata/fuzz alike, as an
// ordinary test, so the unit ledger lists it once beside the TestXxx
// functions and the integration runner's anchored -test.run runs it by name.
// testdata/fuzzsuite holds a target whose seeds pass and one whose corpus
// file fails.

func TestUnitRunner_ListsAndRunsEachFuzzTargetOnce(t *testing.T) {
	if testing.Short() {
		t.Skip("runs go test")
	}
	quiet(t)
	out := t.TempDir()
	if code := cmdUnit([]string{"-out", out, "-allow", "", "-quarantine", "", "./testdata/fuzzsuite"}); code != 1 {
		t.Fatalf("exit %d, want 1: FuzzSeedFails has a failing corpus file", code)
	}
	var l ledger
	if err := readJSON(filepath.Join(out, "unit-tests.json"), &l); err != nil {
		t.Fatal(err)
	}
	counts := map[string]int{}
	by := map[string]entry{}
	for _, e := range l.Tests {
		counts[e.Test]++
		by[e.Test] = e
	}
	for _, name := range []string{"TestPlain", "FuzzSeedPasses", "FuzzSeedFails"} {
		if counts[name] != 1 {
			t.Errorf("%s is in the unit ledger %d times, want once: %+v", name, counts[name], l.Tests)
		}
	}
	if l.Summary.Expected != 3 {
		t.Errorf("expected = %d, want 3 (the listing holds each Fuzz target once)", l.Summary.Expected)
	}
	if by["FuzzSeedPasses"].Outcome != outcomePass || by["FuzzSeedFails"].Outcome != outcomeFail {
		t.Errorf("outcomes: FuzzSeedPasses %q, FuzzSeedFails %q", by["FuzzSeedPasses"].Outcome, by["FuzzSeedFails"].Outcome)
	}
	if !strings.Contains(strings.Join(l.Problems, "\n"), "FuzzSeedFails") {
		t.Errorf("problems = %v, want the failing fuzz target named", l.Problems)
	}
}

func TestIntegrationRunner_RunsANamedFuzzTargetBySeed(t *testing.T) {
	if testing.Short() {
		t.Skip("compiles a test binary")
	}
	quiet(t)
	out := t.TempDir()
	sel := selection{Integration: map[string]*pkgSelection{
		fuzzSuite: {Evidence: map[string]string{"FuzzSeedPasses": "named", "FuzzSeedFails": "named"}},
	}}
	selFile := filepath.Join(out, "selection.json")
	if err := writeJSON(selFile, sel); err != nil {
		t.Fatal(err)
	}
	if code := cmdIntegration([]string{"-out", out, "-jobs", "1", "-costs", "", "-allow", "", "-select", selFile}); code != 1 {
		t.Fatalf("exit %d, want 1", code)
	}
	var l ledger
	if err := readJSON(filepath.Join(out, "integration-tests.json"), &l); err != nil {
		t.Fatal(err)
	}
	by := map[string]entry{}
	for _, e := range l.Tests {
		by[e.Test] = e
	}
	if by["FuzzSeedPasses"].Outcome != outcomePass {
		t.Errorf("FuzzSeedPasses = %+v, want a pass", by["FuzzSeedPasses"])
	}
	if by["FuzzSeedFails"].Outcome != outcomeFail {
		t.Errorf("FuzzSeedFails = %+v, want a fail: -test.run must run the corpus file's seed", by["FuzzSeedFails"])
	}
	if _, ran := by["TestPlain"]; ran {
		t.Error("TestPlain ran: the selection named two Fuzz targets")
	}
}
