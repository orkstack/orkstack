package main

import (
	"path/filepath"
	"strings"
	"testing"
)

// The 4,000-task ceiling spec and its merge-gate twin, as a plan lists them.
var (
	ceilingTest = uiPlanTest{File: "run-page-ceiling.spec.ts", Title: "at 4,000", Project: "chromium-solo-run-page-ceiling"}
	reducedTest = uiPlanTest{File: "run-page-reduced-ceiling.spec.ts", Title: "at 1,000", Project: "chromium-solo-run-page-ceiling"}
)

// uiDeferral gives the fixture a UI plan that read the nightly spec lists in
// mode: the tree's list carries the ceiling row, and the base's copy beside
// the plan carries it when baseHasRow. With defer set the plan leaves the
// ceiling spec out, as ui-plan does when both lists carry it.
func uiDeferral(f *mergeFixture, mode string, baseHasRow, deferIt bool) {
	f.t.Helper()
	tree := f.writeFile("nightly-specs.txt", specRow+"\n")
	baseText := ""
	if baseHasRow {
		baseText = specRow + "\n"
	}
	base := f.writeFile("nightly-specs-base.txt", baseText)
	treeSHA, _ := fileSHA256(tree)
	baseSHA, _ := fileSHA256(base)
	p := uiPlan{Format: 1}
	p.Tests = []uiPlanTest{{File: "a.spec.ts", Title: "t", Project: "chromium"}, ceilingTest, reducedTest}
	p.Files = []uiPlanFile{{File: "a.spec.ts", Tests: 1, Shard: 1}, {File: reducedTest.File, Tests: 1, Shard: 1}}
	p.Shards = []uiShard{{Index: 1, Files: []string{"a.spec.ts", reducedTest.File}}}
	if !deferIt {
		p.Files = append(p.Files, uiPlanFile{File: ceilingTest.File, Tests: 1, Shard: 1})
		p.Shards[0].Files = append(p.Shards[0].Files, ceilingTest.File)
	} else {
		p.Deferred = []uiDeferredSpec{{File: ceilingTest.File, Entry: specRow}}
	}
	p.Nightly = &uiPlanNightly{Mode: mode, File: tree, SHA256: treeSHA, Entries: []string{"ui/e2e/run-page-ceiling.spec.ts"}}
	if mode == nightlyDefer {
		p.Nightly.BaseFile, p.Nightly.BaseSHA256 = base, baseSHA
		if baseHasRow {
			p.Nightly.BaseEntries = []string{"ui/e2e/run-page-ceiling.spec.ts"}
		}
	}
	f.uiPlan = p
	f.save()
}

func uiEntryOf(t uiPlanTest, outcome, detail string) uiEntry {
	return uiEntry{File: t.File, Title: t.Title, Project: t.Project, Outcome: outcome, Detail: detail, Seconds: 10}
}

// writeLocalUI writes the local part with the given UI ledger.
func (f *mergeFixture) writeLocalUI(ui uiLedger, gates []gateRecord) {
	f.t.Helper()
	rec := f.basePart("local")
	rec.Gates = gates
	rec.Images = map[string]string{"activity-shell": "sha256:aaa"}
	f.writePart(rec, map[string]any{
		"integration-tests.json": ledger{Suite: "integration", Tests: []entry{passEntry("TestA"), passEntry("TestB")}, Binaries: mBinaries()},
		"unit-tests.json":        unitLedger(),
		"ui-e2e-tests.json":      ui,
	})
}

func deferredUILedger() uiLedger {
	return uiLedger{
		Suite: "ui-e2e", Listed: 3, Pass: 2, Deferred: 1, GuardRetryCeiling: 3, Problems: []string{},
		Tests: []uiEntry{
			uiEntryOf(uiPlanTest{File: "a.spec.ts", Title: "t", Project: "chromium"}, outcomePass, ""),
			uiEntryOf(ceilingTest, "deferred", specRow),
			uiEntryOf(reducedTest, outcomePass, ""),
		},
		Files: []uiFileEntry{{File: "a.spec.ts", Shard: 1, Tests: 1, Seconds: 10, WallSeconds: 10}, {File: reducedTest.File, Shard: 1, Tests: 1, Seconds: 10, WallSeconds: 10}},
	}
}

func TestMerge_AFullProofRecordsTheUIPlansDeferredSpecs(t *testing.T) {
	f := newMergeFixture(t, tierFull, false)
	uiDeferral(f, nightlyDefer, true, true)
	f.writeLocalUI(deferredUILedger(), fullGateRecords())
	r, code := f.merge()
	if code != 0 || r.Result != "pass" || !r.MergeEligible {
		t.Fatalf("code %d, result %s eligible %v: %v", code, r.Result, r.MergeEligible, r.Ineligible)
	}
	var ui uiLedger
	if err := readJSON(filepath.Join(f.out, "ui-e2e-tests.json"), &ui); err != nil {
		t.Fatal(err)
	}
	n := 0
	for _, e := range ui.Tests {
		if e.File == ceilingTest.File {
			n++
			if e.Outcome != "deferred" || e.Detail != specRow {
				t.Errorf("the ceiling spec's entry = %+v", e)
			}
		}
	}
	if n != 1 || ui.Deferred != 1 || ui.Listed != 3 || len(ui.Problems) != 0 {
		t.Errorf("ceiling entries %d, deferred %d of %d, problems %v", n, ui.Deferred, ui.Listed, ui.Problems)
	}
}

func TestMerge_AUIDeferralIsCheckedAgainstBothLists(t *testing.T) {
	cases := []struct {
		name                string
		tier, mode          string
		baseHasRow, deferIt bool
		ui                  uiLedger
		want                string
	}{
		{
			name: "a row the change adds is run", tier: tierFull, mode: nightlyDefer, baseHasRow: false, deferIt: true,
			ui:   deferredUILedger(),
			want: "run-page-ceiling.spec.ts was deferred, but the base's nightly spec list does not carry it",
		},
		{
			name: "a row both lists carry is deferred", tier: tierFull, mode: nightlyDefer, baseHasRow: true, deferIt: false,
			ui: func() uiLedger {
				l := deferredUILedger()
				l.Tests[1] = uiEntryOf(ceilingTest, outcomePass, "")
				l.Pass, l.Deferred = 3, 0
				return l
			}(),
			want: "ui/e2e/run-page-ceiling.spec.ts is in both nightly spec lists and was not deferred",
		},
		{
			name: "a deferred spec that ran", tier: tierFull, mode: nightlyDefer, baseHasRow: true, deferIt: true,
			ui: func() uiLedger {
				l := deferredUILedger()
				l.Tests[1] = uiEntryOf(ceilingTest, outcomePass, "")
				return l
			}(),
			want: "local reported run-page-ceiling.spec.ts › at 4,000 [chromium-solo-run-page-ceiling], which the UI plan defers to the nightly tier",
		},
		{
			name: "only the full tier defers", tier: tierRelease, mode: nightlyInclude, baseHasRow: true, deferIt: true,
			ui:   deferredUILedger(),
			want: "run-page-ceiling.spec.ts was deferred, and the UI plan's nightly mode is include: only defer leaves a spec out",
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			f := newMergeFixture(t, c.tier, false)
			uiDeferral(f, c.mode, c.baseHasRow, c.deferIt)
			gates := fullGateRecords()
			if c.tier == tierRelease {
				gates = append(gates, gateRecord{Gate: "observability", Result: "pass"})
			}
			f.writeLocalUI(c.ui, gates)
			r, _ := f.merge()
			if r.MergeEligible || r.EligibleButForTier {
				t.Errorf("the deferral was accepted: %v", r.Ineligible)
			}
			if got := joined(r); !strings.Contains(got, c.want) {
				t.Errorf("ineligible lacks %q:\n%s", c.want, got)
			}
		})
	}
}

// The nightly tier runs the listed specs: its UI plan holds them alone, and
// the proof needs the ui-e2e gate beside the integration gate.
func TestMerge_ANightlyProofRunsTheListedSpecs(t *testing.T) {
	nightly := func(gates []gateRecord) proofRecord {
		f := newMergeFixture(t, tierNightly, false)
		uiDeferral(f, nightlyOnly, true, false)
		f.uiPlan.Tests = []uiPlanTest{ceilingTest}
		f.uiPlan.Files = []uiPlanFile{{File: ceilingTest.File, Tests: 1, Shard: 1}}
		f.uiPlan.Shards = []uiShard{{Index: 1, Files: []string{ceilingTest.File}}}
		f.save()
		f.writeLocalUI(uiLedger{
			Suite: "ui-e2e", Listed: 1, Pass: 1, GuardRetryCeiling: 3, Problems: []string{},
			Tests: []uiEntry{uiEntryOf(ceilingTest, outcomePass, "")},
			Files: []uiFileEntry{{File: ceilingTest.File, Shard: 1, Tests: 1, Seconds: 10, WallSeconds: 10}},
		}, gates)
		r, _ := f.merge("-ui-plan", f.uiPlanPath, "-nightly-specs", f.uiPlan.Nightly.File)
		return r
	}
	r := nightly([]gateRecord{{Gate: "integration", Result: "pass"}, {Gate: "ui-e2e", Result: "pass"}})
	if r.Result != "pass" || !r.EligibleButForTier {
		t.Fatalf("a nightly proof that ran its specs: result %s, eligible but for tier %v: %v", r.Result, r.EligibleButForTier, r.Ineligible)
	}
	r = nightly([]gateRecord{{Gate: "integration", Result: "pass"}})
	if r.Result == "pass" || !strings.Contains(joined(r), "ui-e2e") {
		t.Errorf("a nightly proof without its Playwright gate passed: %s, %v", r.Result, r.Ineligible)
	}
}

// nightlyUIPlan gives the fixture a nightly UI plan of the listed spec alone.
func nightlyUIPlan(f *mergeFixture) {
	f.t.Helper()
	uiDeferral(f, nightlyOnly, true, false)
	f.uiPlan.Tests = []uiPlanTest{ceilingTest}
	f.uiPlan.Files = []uiPlanFile{{File: ceilingTest.File, Tests: 1, Shard: 1}}
	f.uiPlan.Shards = []uiShard{{Index: 1, Files: []string{ceilingTest.File}}}
	f.save()
}

func ceilingLedger() uiLedger {
	return uiLedger{
		Suite: "ui-e2e", Listed: 1, Pass: 1, GuardRetryCeiling: 3, Problems: []string{},
		Tests: []uiEntry{uiEntryOf(ceilingTest, outcomePass, "")},
		Files: []uiFileEntry{{File: ceilingTest.File, Shard: 1, Tests: 1, Seconds: 10, WallSeconds: 10}},
	}
}

// A CI nightly proof runs its listed specs as ui-e2e parts: the plan expects
// them, and the merge takes their ledgers.
func TestMerge_ACINightlyProofRunsItsUIParts(t *testing.T) {
	f := newMergeFixture(t, tierNightly, true)
	f.plan.PartsExpected = partsExpected(tierNightly, false, 2, nightlyUIShards(1, make([]nightlySpecEntry, 1)), false)
	nightlyUIPlan(f)
	f.writePart(f.basePart("plan"), nil)
	build := f.basePart("build")
	build.Gates = []gateRecord{{Gate: "images", Result: "pass"}}
	build.Images = map[string]string{"activity-shell": "sha256:aaa"}
	f.writePart(build, nil)
	f.writeShard(1, []entry{passEntry("TestA")}, nil, 1)
	f.writeShard(2, []entry{passEntry("TestB")}, nil, 1)
	ui := f.basePart("ui-e2e:1")
	ui.Gates = []gateRecord{{Gate: "ui-e2e", Result: "pass"}}
	ui.Images = map[string]string{"activity-shell": "sha256:aaa"}
	f.writePart(ui, map[string]any{"ui-e2e-tests.json": ceilingLedger()})
	r, _ := f.merge("-ui-plan", f.uiPlanPath, "-nightly-specs", f.uiPlan.Nightly.File)
	if r.Result != "pass" || !r.EligibleButForTier {
		t.Fatalf("a CI nightly proof that ran its UI part: result %s, eligible but for tier %v: %v", r.Result, r.EligibleButForTier, r.Ineligible)
	}
}

// A nightly proof is held to the tree's nightly spec list itself, never only
// to the UI plan it was given: with a row listed it needs a UI plan read from
// that list, whose files are exactly the rows', and its ui-e2e gate.
func TestMerge_ANightlyProofIsHeldToTheTreesSpecList(t *testing.T) {
	gates := []gateRecord{{Gate: "integration", Result: "pass"}, {Gate: "ui-e2e", Result: "pass"}}
	cases := []struct {
		name   string
		mutate func(f *mergeFixture) []string // the merge's extra arguments
		want   string
	}{
		{"no UI plan", func(f *mergeFixture) []string {
			return []string{"-nightly-specs", f.uiPlan.Nightly.File}
		}, "the nightly spec list names 1 spec file(s), and the proof was given no UI plan"},
		{"a UI plan of other files", func(f *mergeFixture) []string {
			f.uiPlan.Files, f.uiPlan.Shards, f.uiPlan.Tests = nil, []uiShard{}, nil
			f.save()
			return []string{"-ui-plan", f.uiPlanPath, "-nightly-specs", f.uiPlan.Nightly.File}
		}, "the nightly UI plan's files [] are not the nightly spec list's [run-page-ceiling.spec.ts]"},
		{"a UI plan read from another list", func(f *mergeFixture) []string {
			f.uiPlan.Nightly.SHA256 = emptySHA
			f.save()
			return []string{"-ui-plan", f.uiPlanPath, "-nightly-specs", f.uiPlan.Nightly.File}
		}, "is not the one the UI plan read"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			f := newMergeFixture(t, tierNightly, false)
			nightlyUIPlan(f)
			args := c.mutate(f)
			f.writeLocalUI(ceilingLedger(), gates)
			r, _ := f.merge(args...)
			if r.Result == "pass" {
				t.Errorf("the nightly proof passed")
			}
			if got := joined(r); !strings.Contains(got, c.want) {
				t.Errorf("ineligible lacks %q:\n%s", c.want, got)
			}
		})
	}
}

// The nightly tier's CI graph has a UI part per shard of its UI plan, which
// holds the listed spec files alone: at most the requested shards, at most
// one per row, and none when the list is empty.
func TestPlan_ANightlyCIProofExpectsItsUIParts(t *testing.T) {
	if p := partsExpected(tierNightly, false, 2, 1, false); !containsString(p, "ui-e2e:1") || containsString(p, "fast") || containsString(p, "activity-fixture:shell") {
		t.Errorf("nightly parts = %v", p)
	}
	if p := partsExpected(tierNightly, false, 2, 0, false); containsString(p, "ui-e2e:1") {
		t.Errorf("a nightly proof with no listed spec expects a UI part: %v", p)
	}
	rows, _ := parseNightlySpecs(strings.NewReader(specRow+"\n"), "list")
	for _, c := range []struct{ requested, rows, want int }{{1, 1, 1}, {5, 1, 1}, {1, 0, 0}, {0, 1, 0}} {
		if got := nightlyUIShards(c.requested, rows[:c.rows]); got != c.want {
			t.Errorf("nightlyUIShards(%d, %d rows) = %d, want %d", c.requested, c.rows, got, c.want)
		}
	}
}
