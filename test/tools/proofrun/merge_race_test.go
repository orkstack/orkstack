package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// The race gate joins the full and release tiers' required gates, and the
// race repeat and fuzz lane join the nightly tier's (AGENTS.md §11.2). The
// merger holds each to its gate and its ledger, wherever the proof ran it:
// the local part carries every gate, a CI proof one part per lane.

var integrationLedger = ledger{Suite: "integration", Tests: []entry{passEntry("TestA"), passEntry("TestB")}}

// shortSkip is a race ledger's entry for a test the -short run skipped.
func shortSkip(test string) entry {
	return entry{Package: mPkg, Test: test, Outcome: outcomeSkip, Reason: shortSkipReason}
}

func TestPartsExpected_NameTheRaceAndNightlyLanes(t *testing.T) {
	full := partsExpected(tierFull, false, 2, 1, false)
	if !containsString(full, "race") || indexOf(full, "race") != indexOf(full, "fast")+1 {
		t.Errorf("a CI full proof expects %v: a race part right after fast", full)
	}
	release := partsExpected(tierRelease, false, 2, 1, true)
	if !containsString(release, "race") {
		t.Errorf("a CI release proof expects %v: the pre-release gate is the full tier", release)
	}
	nightly := partsExpected(tierNightly, false, 2, 1, false)
	for _, want := range []string{suiteRaceRepeat, suiteFuzz} {
		if !containsString(nightly, want) {
			t.Errorf("a CI nightly proof expects %v: no %s part", nightly, want)
		}
	}
	if containsString(nightly, "race") || containsString(full, suiteRaceRepeat) || containsString(full, suiteFuzz) {
		t.Errorf("the lanes belong to one tier each: full %v, nightly %v", full, nightly)
	}
	if got := partsExpected(tierNightly, true, 2, 1, false); len(got) != 1 || got[0] != "local" {
		t.Errorf("a local nightly proof is one part: %v", got)
	}
}

func TestMerge_AFullProofIsHeldToTheRaceGate(t *testing.T) {
	for _, c := range []struct {
		name   string
		gates  func() []gateRecord
		reason string
	}{
		{"missing", func() []gateRecord { return withoutGate(fullGateRecords(), suiteRace) }, "gate race did not run"},
		{"failed", func() []gateRecord { g := fullGateRecords(); setGate(g, suiteRace, "fail"); return g }, "gate race failed"},
		{"not started", func() []gateRecord {
			g := fullGateRecords()
			for i := range g {
				if g[i].Gate == suiteRace {
					g[i] = gateRecord{Gate: suiteRace, Result: gateNotStarted, Log: "-", Command: "not started: unit failed"}
				}
			}
			return g
		}, "gate race was not started: not started: unit failed"},
	} {
		t.Run("local "+c.name, func(t *testing.T) {
			f := newMergeFixture(t, tierFull, false)
			f.writeLocalWith(func(r *partRecord) { r.Gates = c.gates() }, integrationLedger)
			r, code := f.merge()
			if code != 1 || r.Result != "fail" || r.MergeEligible || !strings.Contains(joined(r), c.reason) {
				t.Errorf("code %d, result %s, eligible %v: %v, want a reason containing %q", code, r.Result, r.MergeEligible, r.Ineligible, c.reason)
			}
		})
	}
	t.Run("CI failed", func(t *testing.T) {
		f := newMergeFixture(t, tierFull, true)
		f.writeCI()
		f.writeRace([]gateRecord{{Gate: suiteRace, Result: "fail"}}, raceLedger())
		f.writeShard(1, []entry{passEntry("TestA")}, nil, 1)
		f.writeShard(2, []entry{passEntry("TestB")}, nil, 1)
		r, code := f.merge()
		if code != 1 || r.MergeEligible || !strings.Contains(joined(r), "gate race failed") {
			t.Errorf("code %d, eligible %v: %v", code, r.MergeEligible, r.Ineligible)
		}
	})
	t.Run("CI missing part", func(t *testing.T) {
		f := newMergeFixture(t, tierFull, true)
		f.writeCI()
		if err := os.RemoveAll(filepath.Join(f.parts, "race-a1")); err != nil {
			t.Fatal(err)
		}
		f.writeShard(1, []entry{passEntry("TestA")}, nil, 1)
		f.writeShard(2, []entry{passEntry("TestB")}, nil, 1)
		r, code := f.merge()
		if code != 1 || !strings.Contains(joined(r), "part race left no result") {
			t.Errorf("code %d: %v", code, r.Ineligible)
		}
	})
}

// A CI proof and a local proof of the same tests agree, race gate included.
func TestMerge_TheRaceLedgerIsMergedAndRecorded(t *testing.T) {
	for _, ci := range []bool{false, true} {
		name := map[bool]string{false: "local", true: "CI"}[ci]
		t.Run(name, func(t *testing.T) {
			f := newMergeFixture(t, tierFull, ci)
			if ci {
				f.writeCI()
				f.writeShard(1, []entry{passEntry("TestA")}, nil, 1)
				f.writeShard(2, []entry{passEntry("TestB")}, nil, 1)
			} else {
				f.writeLocal(integrationLedger)
			}
			r, code := f.merge()
			if code != 0 || !r.MergeEligible {
				t.Fatalf("code %d, eligible %v: %v", code, r.MergeEligible, r.Ineligible)
			}
			ledgers, _ := r.Ledgers.(map[string]any)
			if _, ok := ledgers[suiteRace]; !ok {
				t.Errorf("proof.json ledgers = %v: no race entry", ledgers)
			}
			var l ledger
			if err := readJSON(filepath.Join(f.out, "race-tests.json"), &l); err != nil || l.Suite != suiteRace {
				t.Errorf("the merged proof keeps race-tests.json: %v %+v", err, l)
			}
		})
	}
}

func TestMerge_ARaceLedgerWithProblemsFailsTheProof(t *testing.T) {
	for _, ci := range []bool{false, true} {
		name := map[bool]string{false: "local", true: "CI"}[ci]
		t.Run(name, func(t *testing.T) {
			bad := raceLedger()
			bad.Tests[0].Outcome = outcomeFail
			bad.Problems = []string{mPkg + ".TestUnit reported a data race (1 report): the detector's output is in the gate's log"}
			f := newMergeFixture(t, tierFull, ci)
			if ci {
				f.writeCI()
				f.writeRace(raceGateRecords(), bad)
				f.writeShard(1, []entry{passEntry("TestA")}, nil, 1)
				f.writeShard(2, []entry{passEntry("TestB")}, nil, 1)
			} else {
				f.writeLocalWith(func(r *partRecord) {}, integrationLedger)
				rec := f.basePart("local")
				_ = rec
				// Overwrite the local part's race ledger with the bad one.
				if err := writeJSON(filepath.Join(f.parts, "local-a1", "race-tests.json"), bad); err != nil {
					t.Fatal(err)
				}
			}
			r, code := f.merge()
			if code != 1 || r.Result != "fail" || r.MergeEligible {
				t.Fatalf("code %d, result %s, eligible %v: a race ledger with problems must fail the proof", code, r.Result, r.MergeEligible)
			}
			if !strings.Contains(joined(r), "the race ledger is incomplete or has problems") {
				t.Errorf("ineligible = %v", r.Ineligible)
			}
		})
	}
}

// A test the race lane skipped under -short was run by the unit gate, or by
// nothing: the merger takes the unit ledger's word for it, and accepts no
// other reason.
func TestMerge_AShortSkipIsHeldToTheUnitGatesPass(t *testing.T) {
	unit := func(outcome string) ledger {
		l := unitLedger()
		l.Tests = append(l.Tests, entry{Package: mPkg, Test: "TestScale", Outcome: outcome, Seconds: 30})
		return l
	}
	race := func() ledger {
		l := raceLedger()
		l.Tests = append(l.Tests, shortSkip("TestScale"))
		return l
	}
	for _, c := range []struct {
		name    string
		unit    ledger
		race    ledger
		problem string
	}{
		{"the unit gate passed it", unit(outcomePass), race(), ""},
		{"the unit gate did not", unit(outcomeFail), race(), mPkg + ".TestScale was skipped under -short, and the unit ledger has outcome \"fail\" for it"},
		{"the unit ledger lacks it", unitLedger(), race(), mPkg + ".TestScale was skipped under -short, and the unit ledger does not list it"},
	} {
		t.Run(c.name, func(t *testing.T) {
			f := newMergeFixture(t, tierFull, false)
			f.writeLocal(integrationLedger)
			dir := filepath.Join(f.parts, "local-a1")
			for name, l := range map[string]ledger{"unit-tests.json": c.unit, "race-tests.json": c.race} {
				if err := writeJSON(filepath.Join(dir, name), l); err != nil {
					t.Fatal(err)
				}
			}
			r, code := f.merge()
			if c.problem == "" {
				if code != 0 || !r.MergeEligible {
					t.Fatalf("code %d, eligible %v: %v", code, r.MergeEligible, r.Ineligible)
				}
				ledgers, _ := r.Ledgers.(map[string]any)
				raceSummary, _ := ledgers[suiteRace].(map[string]any)
				if raceSummary["summary"] == nil {
					t.Errorf("race ledger = %v", ledgers[suiteRace])
				}
				return
			}
			if code != 1 || r.MergeEligible || !strings.Contains(joined(r), c.problem) {
				t.Errorf("code %d, eligible %v: %v, want a reason containing %q", code, r.MergeEligible, r.Ineligible, c.problem)
			}
		})
	}
}

// The race ledger must cover the unit ledger: a race run narrowed to fewer
// packages, or one whose listing lost a test, is not the gate.
func TestMerge_TheRaceLedgerListsEveryTestTheUnitLedgerDoes(t *testing.T) {
	f := newMergeFixture(t, tierFull, false)
	f.writeLocal(integrationLedger)
	dir := filepath.Join(f.parts, "local-a1")
	unit := unitLedger()
	unit.Tests = append(unit.Tests, entry{Package: mPkg, Test: "TestOnlyUnit", Outcome: outcomePass, Seconds: 1})
	if err := writeJSON(filepath.Join(dir, "unit-tests.json"), unit); err != nil {
		t.Fatal(err)
	}
	r, code := f.merge()
	if code != 1 || r.MergeEligible || !strings.Contains(joined(r), mPkg+".TestOnlyUnit is in the unit ledger and not in the race ledger") {
		t.Errorf("code %d, eligible %v: %v", code, r.MergeEligible, r.Ineligible)
	}
}

// No quarantine excuses a race failure, whatever a hand-written ledger says.
func TestMerge_ARaceFailureIsNeverQuarantined(t *testing.T) {
	f := newMergeFixture(t, tierFull, false)
	f.writeLocal(integrationLedger)
	bad := raceLedger()
	bad.Tests[0].Outcome = outcomeQuarantinedFail
	bad.Tests[0].Quarantine = "someone, review by 2026-11-01: flaky"
	if err := writeJSON(filepath.Join(f.parts, "local-a1", "race-tests.json"), bad); err != nil {
		t.Fatal(err)
	}
	r, code := f.merge()
	if code != 1 || r.MergeEligible || !strings.Contains(joined(r), "quarantined-fail in the race ledger") {
		t.Errorf("code %d, eligible %v: %v", code, r.MergeEligible, r.Ineligible)
	}
}

// A race skip that names an allowed-skip entry stands on that entry, like any
// other: the list the plan hashed is the judge, and a unit entry covers it.
func TestMerge_ARaceAllowedSkipNeedsTheListsEntry(t *testing.T) {
	allowed := entry{Package: mPkg, Test: "TestRecorder", Outcome: outcomeSkip, AllowedSkip: "opt-in recorder"}
	race := raceLedger()
	race.Tests = append(race.Tests, allowed)
	unit := unitLedger()
	unit.Tests = append(unit.Tests, allowed)
	for _, c := range []struct {
		name  string
		list  string
		clean bool
	}{
		{"a unit entry covers it", "unit\t" + mPkg + "\tTestRecorder\topt-in recorder\n", true},
		{"an any entry covers it", "any\t" + mPkg + "\tTestRecorder\topt-in recorder\n", true},
		{"no entry", "", false},
		{"an entry with another reason", "unit\t" + mPkg + "\tTestRecorder\tsomething else\n", false},
	} {
		t.Run(c.name, func(t *testing.T) {
			f := newMergeFixture(t, tierFull, false)
			f.pinAllowlist(c.list)
			f.writeLocal(integrationLedger)
			dir := filepath.Join(f.parts, "local-a1")
			for name, l := range map[string]ledger{"unit-tests.json": unit, "race-tests.json": race} {
				if err := writeJSON(filepath.Join(dir, name), l); err != nil {
					t.Fatal(err)
				}
			}
			r, code := f.merge()
			if c.clean && (code != 0 || !r.MergeEligible) {
				t.Errorf("code %d: %v", code, r.Ineligible)
			}
			if !c.clean && (code != 1 || r.MergeEligible || !strings.Contains(joined(r), "TestRecorder skipped, and the allowed-skips list the plan read has no entry for it with that reason")) {
				t.Errorf("code %d, eligible %v: %v, want the unexplained skip refused", code, r.MergeEligible, r.Ineligible)
			}
		})
	}
}

// ---- the nightly tier's lanes -------------------------------------------------

func (f *mergeFixture) writeNightlyLocal(gates []gateRecord, files map[string]any) {
	f.t.Helper()
	rec := f.basePart("local")
	rec.Gates = gates
	rec.Images = map[string]string{"activity-shell": "sha256:aaa"}
	all := map[string]any{"integration-tests.json": integrationLedger}
	for k, v := range files {
		all[k] = v
	}
	if integrationLedger.Binaries == nil {
		l := integrationLedger
		l.Binaries = mBinaries()
		all["integration-tests.json"] = l
	}
	f.writePart(rec, all)
}

func TestMerge_ANightlyProofIsHeldToItsTwoLanes(t *testing.T) {
	lanes := map[string]any{"race-repeat-tests.json": raceRepeatLedger(), "fuzz-tests.json": fuzzLedger()}
	t.Run("both pass", func(t *testing.T) {
		f := newMergeFixture(t, tierNightly, false)
		f.writeNightlyLocal(nightlyGateRecords(), lanes)
		r, code := f.merge()
		if code != 0 || r.Result != "pass" || !r.EligibleButForTier {
			t.Fatalf("code %d, result %s, eligible but for tier %v: %v", code, r.Result, r.EligibleButForTier, r.Ineligible)
		}
		ledgers, _ := r.Ledgers.(map[string]any)
		for _, suite := range []string{suiteRaceRepeat, suiteFuzz} {
			if _, ok := ledgers[suite]; !ok {
				t.Errorf("proof.json ledgers = %v: no %s entry", ledgers, suite)
			}
			var l ledger
			if err := readJSON(filepath.Join(f.out, suite+"-tests.json"), &l); err != nil || l.Suite != suite {
				t.Errorf("the merged proof keeps %s-tests.json: %v", suite, err)
			}
		}
	})
	for _, gate := range []string{suiteRaceRepeat, suiteFuzz} {
		t.Run("missing "+gate, func(t *testing.T) {
			f := newMergeFixture(t, tierNightly, false)
			f.writeNightlyLocal(withoutGate(nightlyGateRecords(), gate), lanes)
			r, code := f.merge()
			if code != 1 || r.Result == "pass" || !strings.Contains(joined(r), "gate "+gate+" did not run") {
				t.Errorf("code %d, result %s: %v", code, r.Result, r.Ineligible)
			}
		})
		t.Run("failed "+gate, func(t *testing.T) {
			f := newMergeFixture(t, tierNightly, false)
			g := nightlyGateRecords()
			setGate(g, gate, "fail")
			f.writeNightlyLocal(g, lanes)
			r, code := f.merge()
			if code != 1 || r.Result == "pass" || !strings.Contains(joined(r), "gate "+gate+" failed") {
				t.Errorf("code %d, result %s: %v", code, r.Result, r.Ineligible)
			}
		})
	}
	t.Run("a fuzz ledger with a crasher", func(t *testing.T) {
		bad := fuzzLedger()
		bad.Tests[0].Outcome = outcomeFail
		bad.Tests[0].Reason = "crasher kept at crashers/x"
		bad.Problems = []string{mPkg + ".FuzzUnit failed: crasher kept at crashers/x"}
		f := newMergeFixture(t, tierNightly, false)
		f.writeNightlyLocal(nightlyGateRecords(), map[string]any{"race-repeat-tests.json": raceRepeatLedger(), "fuzz-tests.json": bad})
		r, code := f.merge()
		if code != 1 || r.Result == "pass" || !strings.Contains(joined(r), "the fuzz ledger is incomplete or has problems") {
			t.Errorf("code %d, result %s: %v", code, r.Result, r.Ineligible)
		}
	})
	t.Run("a gate with no ledger", func(t *testing.T) {
		f := newMergeFixture(t, tierNightly, false)
		f.writeNightlyLocal(nightlyGateRecords(), map[string]any{"race-repeat-tests.json": raceRepeatLedger()})
		r, code := f.merge()
		if code != 1 || r.Result == "pass" || !strings.Contains(joined(r), "left no fuzz ledger") {
			t.Errorf("code %d, result %s: %v", code, r.Result, r.Ineligible)
		}
	})
}

// writeNightlyLanes writes the two CI parts a nightly proof adds: the race
// repeat and the fuzz lane, passing.
func (f *mergeFixture) writeNightlyLanes() {
	f.t.Helper()
	f.writeLane(suiteRaceRepeat, []gateRecord{{Gate: suiteRaceRepeat, Result: "pass"}}, raceRepeatLedger())
	f.writeLane(suiteFuzz, []gateRecord{{Gate: suiteFuzz, Result: "pass"}}, fuzzLedger())
}

// writeLane writes one lane's CI part with the gates and ledger it recorded.
func (f *mergeFixture) writeLane(suite string, gates []gateRecord, l ledger) {
	f.t.Helper()
	part := f.basePart(suite)
	part.Gates = gates
	f.writePart(part, map[string]any{suite + "-tests.json": l})
}

func TestMerge_ACINightlyProofRunsItsTwoLanes(t *testing.T) {
	write := func(f *mergeFixture, raceGates, fuzzGates []gateRecord, fuzz ledger) {
		f.writePart(f.basePart("plan"), nil)
		build := f.basePart("build")
		build.Gates = []gateRecord{{Gate: "images", Result: "pass"}}
		build.Images = map[string]string{"activity-shell": "sha256:aaa"}
		f.writePart(build, nil)
		f.writeShard(1, []entry{passEntry("TestA")}, nil, 1)
		f.writeShard(2, []entry{passEntry("TestB")}, nil, 1)
		f.writeLane(suiteRaceRepeat, raceGates, raceRepeatLedger())
		f.writeLane(suiteFuzz, fuzzGates, fuzz)
	}
	pass := func(g string) []gateRecord { return []gateRecord{{Gate: g, Result: "pass"}} }
	t.Run("passes", func(t *testing.T) {
		f := newMergeFixture(t, tierNightly, true)
		write(f, pass(suiteRaceRepeat), pass(suiteFuzz), fuzzLedger())
		r, code := f.merge()
		if code != 0 || r.Result != "pass" || !r.EligibleButForTier {
			t.Fatalf("code %d, result %s: %v", code, r.Result, r.Ineligible)
		}
		if !containsString(r.Parts, suiteRaceRepeat) || !containsString(r.Parts, suiteFuzz) {
			t.Errorf("parts = %v", r.Parts)
		}
	})
	t.Run("a crasher fails it", func(t *testing.T) {
		f := newMergeFixture(t, tierNightly, true)
		write(f, pass(suiteRaceRepeat), []gateRecord{{Gate: suiteFuzz, Result: "fail"}}, fuzzLedger())
		r, code := f.merge()
		if code != 1 || r.Result == "pass" || !strings.Contains(joined(r), "gate fuzz failed") {
			t.Errorf("code %d, result %s: %v", code, r.Result, r.Ineligible)
		}
	})
	t.Run("a missing lane fails it", func(t *testing.T) {
		f := newMergeFixture(t, tierNightly, true)
		write(f, pass(suiteRaceRepeat), pass(suiteFuzz), fuzzLedger())
		if err := os.RemoveAll(filepath.Join(f.parts, "fuzz-a1")); err != nil {
			t.Fatal(err)
		}
		r, code := f.merge()
		if code != 1 || r.Result == "pass" || !strings.Contains(joined(r), "part fuzz left no result") {
			t.Errorf("code %d, result %s: %v", code, r.Result, r.Ineligible)
		}
	})
}

func TestJobNames_MapToTheLaneParts(t *testing.T) {
	for name, want := range map[string]string{
		"race":                      "race",
		"race-repeat":               "race-repeat",
		"fuzz":                      "fuzz",
		"nightly / race-repeat":     "race-repeat",
		"nightly / fuzz":            "fuzz",
		"gate / race":               "race",
		"integration (3)":           "integration:3",
		"nightly / integration (1)": "integration:1",
	} {
		if got := partFromJobName(name); got != want {
			t.Errorf("partFromJobName(%q) = %q, want %q", name, got, want)
		}
	}
}

func TestRecord_ALaneGateWithoutItsLedgerFailsTheRecord(t *testing.T) {
	for _, suite := range []string{suiteRace, suiteRaceRepeat, suiteFuzz} {
		t.Run(suite, func(t *testing.T) {
			dir := t.TempDir()
			if err := os.WriteFile(filepath.Join(dir, "gates.tsv"), []byte(suite+"\tpass\t3\t"+suite+".log\tproofrun\n"), 0o644); err != nil {
				t.Fatal(err)
			}
			prev := os.Stdout
			devnull, _ := os.Open(os.DevNull)
			os.Stdout = devnull
			code := cmdRecord([]string{"-dir", dir, "-tier", "feature", "-tree-start", "t", "-tree-end", "t", "-clean-start", "-clean-end"})
			os.Stdout = prev
			if code != 1 {
				t.Fatalf("exit %d, want 1", code)
			}
			var r proofRecord
			if err := readJSON(filepath.Join(dir, "proof.json"), &r); err != nil {
				t.Fatal(err)
			}
			if r.Result != "fail" || !strings.Contains(strings.Join(r.Ineligible, "\n"), "the "+suite+" ledger is incomplete") {
				t.Errorf("record = %+v", r)
			}
		})
	}
}

// A feature proof's record reads the lane ledgers it finds, as it reads the
// unit one: a ledger with problems fails it.
func TestRecord_AFeatureProofReadsTheRaceLedger(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "gates.tsv"), []byte("race\tpass\t3\trace.log\tproofrun\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	bad := raceLedger()
	bad.Problems = []string{mPkg + ".TestUnit reported a data race (1 report): the detector's output is in the gate's log"}
	if err := writeJSON(filepath.Join(dir, "race-tests.json"), bad); err != nil {
		t.Fatal(err)
	}
	prev := os.Stdout
	devnull, _ := os.Open(os.DevNull)
	os.Stdout = devnull
	code := cmdRecord([]string{"-dir", dir, "-tier", "feature", "-tree-start", "t", "-tree-end", "t", "-clean-start", "-clean-end"})
	os.Stdout = prev
	if code != 1 {
		t.Fatalf("exit %d, want 1", code)
	}
	var r proofRecord
	if err := readJSON(filepath.Join(dir, "proof.json"), &r); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(strings.Join(r.Ineligible, "\n"), "the race ledger is incomplete or has problems") {
		t.Errorf("ineligible = %v", r.Ineligible)
	}
}
