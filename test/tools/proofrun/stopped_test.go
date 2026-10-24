package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// stoppedGates is what scripts/proof.sh records when the integration gate
// failed fast: the failed gate, the one it stopped while it ran, and the
// gates it never started.
func stoppedGates() []gateRecord {
	var g []gateRecord
	for _, name := range fullGates {
		switch name {
		case "integration":
			g = append(g, gateRecord{Gate: name, Result: "fail", Seconds: 90, Log: "integration.log"})
		case "activity-fixture":
			g = append(g, gateRecord{Gate: name, Result: gateStopped, Seconds: 80, Log: "activity-fixture.log", Command: "stopped: integration failed"})
		case "ui-e2e":
			g = append(g, gateRecord{Gate: name, Result: gateNotStarted, Log: "-", Command: "not started: integration failed"})
		default:
			g = append(g, gateRecord{Gate: name, Result: "pass"})
		}
	}
	return g
}

// A proof that stopped at its first failure is a failing proof that says which
// gate failed and which gates it stopped or never started, with the reason.
func TestMerge_AStoppedProofIsAFailureThatNamesEveryStoppedGate(t *testing.T) {
	f := newMergeFixture(t, tierFull, false)
	f.writeLocalWith(func(r *partRecord) { r.Gates = stoppedGates() },
		ledger{Suite: "integration", Problems: []string{"m.TestA failed (job m-001)"},
			Tests: []entry{{Package: mPkg, Test: "TestA", Outcome: outcomeFail}, {Package: mPkg, Test: "TestB", Outcome: outcomeNone, Reason: "not run: the run was stopped before it started (fail-fast: m.TestA failed)"}}})
	r, code := f.merge()
	if code != 1 || r.Result != "fail" || r.MergeEligible {
		t.Fatalf("exit %d, result %s, eligible %v: a stopped proof must fail", code, r.Result, r.MergeEligible)
	}
	reasons := strings.Join(r.Ineligible, "\n")
	for _, want := range []string{"gate integration failed", "gate activity-fixture was stopped: stopped: integration failed", "gate ui-e2e was not started: not started: integration failed"} {
		if !strings.Contains(reasons, want) {
			t.Errorf("reasons lack %q:\n%s", want, reasons)
		}
	}
	readme, _ := os.ReadFile(filepath.Join(f.out, "README.md"))
	for _, want := range []string{"## Stopped early", "failed: **integration**", "stopped: **activity-fixture** — stopped: integration failed", "not started: **ui-e2e** — not started: integration failed", "Result: **FAIL**"} {
		if !strings.Contains(string(readme), want) {
			t.Errorf("README lacks %q:\n%s", want, readme)
		}
	}
	got := map[string]string{}
	for _, g := range r.Gates {
		got[g.Gate] = g.Result
	}
	if got["activity-fixture"] != gateStopped || got["ui-e2e"] != gateNotStarted || got["integration"] != "fail" {
		t.Errorf("gate results = %v", got)
	}
}

// A gate that was stopped or never started is never read as a pass, even when
// every other gate and every ledger is clean.
func TestMerge_AStoppedGateAloneFailsTheProof(t *testing.T) {
	for _, result := range []string{gateStopped, gateNotStarted} {
		t.Run(result, func(t *testing.T) {
			f := newMergeFixture(t, tierFull, false)
			f.writeLocalWith(func(r *partRecord) {
				r.Gates = fullGateRecords()
				r.Gates[len(r.Gates)-1] = gateRecord{Gate: "ui-e2e", Result: result, Command: result + ": unit failed"}
			}, ledger{Suite: "integration", Tests: []entry{passEntry("TestA"), passEntry("TestB")}})
			r, code := f.merge()
			if code != 1 || r.Result != "fail" || r.MergeEligible {
				t.Fatalf("exit %d, result %s, eligible %v", code, r.Result, r.MergeEligible)
			}
		})
	}
}

// A proof that stopped before `proofrun integration` planned anything has no
// plan to merge against. With -stopped the merger still writes the failing
// proof.json and README from the local part; without it a missing plan is
// still an invalid invocation.
func TestMerge_AProofStoppedBeforeItsPlanStillGetsAFailingVerdict(t *testing.T) {
	f := newMergeFixture(t, tierFull, false)
	var gates []gateRecord
	for _, name := range fullGates {
		switch name {
		case "lint", "build":
			gates = append(gates, gateRecord{Gate: name, Result: "pass"})
		case "unit":
			gates = append(gates, gateRecord{Gate: name, Result: "fail", Log: "unit.log"})
		default:
			gates = append(gates, gateRecord{Gate: name, Result: gateNotStarted, Log: "-", Command: "not started: unit failed"})
		}
	}
	rec := f.basePart("local")
	rec.Gates = gates
	f.writePart(rec, nil)
	quiet(t)
	missing := filepath.Join(f.dir, "no-plan.json")

	if code := cmdMerge([]string{"-plan", missing, "-parts", f.parts, "-out", f.out, "-name", "t"}); code != 2 {
		t.Fatalf("a missing plan without -stopped: exit %d, want 2", code)
	}
	if code := cmdMerge([]string{"-plan", missing, "-stopped", "-parts", f.parts, "-out", f.out, "-name", "t"}); code != 1 {
		t.Fatalf("exit %d, want 1", code)
	}
	var r proofRecord
	if err := readJSON(filepath.Join(f.out, "proof.json"), &r); err != nil {
		t.Fatal(err)
	}
	if r.Result != "fail" || r.MergeEligible || r.Tier != tierFull || r.Tree != "tree1" {
		t.Fatalf("record = result %s eligible %v tier %s tree %s", r.Result, r.MergeEligible, r.Tier, r.Tree)
	}
	reasons := strings.Join(r.Ineligible, "\n")
	for _, want := range []string{"gate unit failed", "gate integration was not started: not started: unit failed", "no test of the integration or UI suites ran"} {
		if !strings.Contains(reasons, want) {
			t.Errorf("reasons lack %q:\n%s", want, reasons)
		}
	}
	readme, _ := os.ReadFile(filepath.Join(f.out, "README.md"))
	if !strings.Contains(string(readme), "failed: **unit**") || !strings.HasSuffix(string(readme), "Result: **FAIL**\n") {
		t.Errorf("README:\n%s", readme)
	}

	// With no local part at all it is still a failure, never an empty pass.
	empty := filepath.Join(f.dir, "empty-parts")
	out := filepath.Join(f.dir, "out2")
	if code := cmdMerge([]string{"-plan", missing, "-stopped", "-parts", empty, "-out", out, "-name", "t"}); code != 1 {
		t.Fatalf("no parts: exit %d, want 1", code)
	}
	if err := readJSON(filepath.Join(out, "proof.json"), &r); err != nil || r.Result != "fail" || r.MergeEligible {
		t.Errorf("no parts: %+v, %v", r, err)
	}
}

// Gates the proof never started have no ledger, and the record does not add a
// "ledger is incomplete" reason for them: the gate itself is the reason.
func TestRecord_ANotStartedGateIsRecordedWithItsReasonAndNeedsNoLedger(t *testing.T) {
	dir := t.TempDir()
	tsv := "unit\tpass\t3\tunit.log\tproofrun unit\n" +
		"integration\tnot-started\t0\t-\tnot started: lint failed\n" +
		"ui-e2e\tnot-started\t0\t-\tnot started: lint failed\n"
	if err := os.WriteFile(filepath.Join(dir, "gates.tsv"), []byte(tsv), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := writeJSON(filepath.Join(dir, "unit-tests.json"), unitLedger()); err != nil {
		t.Fatal(err)
	}
	quiet(t)
	prev := os.Stdout
	devnull, _ := os.Open(os.DevNull)
	os.Stdout = devnull
	code := cmdRecord([]string{"-dir", dir, "-tier", "feature", "-tree-start", "t", "-tree-end", "t", "-clean-start", "-clean-end"})
	os.Stdout = prev
	if code != 1 {
		t.Fatalf("exit %d, want 1: a gate that did not start is not a pass", code)
	}
	var r proofRecord
	if err := readJSON(filepath.Join(dir, "proof.json"), &r); err != nil {
		t.Fatal(err)
	}
	reasons := strings.Join(r.Ineligible, "\n")
	if r.Result != "fail" || strings.Contains(reasons, "ledger is incomplete") {
		t.Errorf("result %s, reasons:\n%s", r.Result, reasons)
	}
	for _, g := range r.Gates {
		if g.Gate == "integration" && (g.Result != gateNotStarted || g.Command != "not started: lint failed") {
			t.Errorf("integration gate = %+v", g)
		}
	}
}
