package main

import (
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"
)

func joined(r proofRecord) string { return strings.Join(r.Ineligible, "\n") }

// The merger re-derives every UI entry's verdict: the shard's problem lines
// are not trusted to be complete (review round 3).
func TestMerge_AUIShardEntryWithoutAProblemLineStillFails(t *testing.T) {
	stray := uiShardLedger(outcomePass, "", nil)
	stray.Tests = append(stray.Tests, uiEntry{File: "z.spec.ts", Title: "z", Project: "chromium", Outcome: outcomePass})
	cases := []struct {
		name  string
		shard uiLedger
		want  string
	}{
		{"a skip with no problem", uiShardLedger(outcomeSkip, "", nil), "the shard's ledger names no problem for it"},
		{"a failure with no problem", uiShardLedger(outcomeFail, "Error: boom", nil), "the shard's ledger names no problem for it"},
		{"no outcome with no problem", uiShardLedger(outcomeNone, "", nil), "the shard's ledger names no problem for it"},
		{"an outcome no ledger writes", uiShardLedger("flaky", "", nil), "the shard's ledger names no problem for it"},
		{"an entry the UI plan does not list", stray, "which the UI plan does not list"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			f := newMergeFixture(t, tierFull, true)
			r, code := mergeUIShard(t, f, 16, "pass", c.shard)
			if code != 1 || r.Result != "fail" || r.MergeEligible || !strings.Contains(joined(r), c.want) {
				t.Errorf("code %d result %s eligible %v: %v", code, r.Result, r.MergeEligible, r.Ineligible)
			}
		})
	}
	// A problem line naming the entry keeps it a (carried) problem, not a
	// second merger line.
	f := newMergeFixture(t, tierFull, true)
	r, _ := mergeUIShard(t, f, 16, "fail", uiShardLedger(outcomeSkip, "", []string{"a.spec.ts › t [chromium] skipped; a skip is never a pass"}))
	if strings.Contains(joined(r), "names no problem") || !strings.Contains(joined(r), "skipped; a skip is never a pass") {
		t.Errorf("a named skip: %v", r.Ineligible)
	}
}

// An excusal the merger derives itself does not need the shard to have
// written the problem line.
func TestMerge_ADerivedExcusalNeedsNoShardProblemLine(t *testing.T) {
	f := newMergeFixture(t, tierFull, true)
	f.anchorHostSpeed(hostRow, hostRow)
	r, code := mergeUIShard(t, f, 2, "fail", uiShardLedger(outcomeFail, timingDetail, nil))
	if code != 0 || !r.MergeEligible || len(r.KnownGaps) != 1 {
		t.Fatalf("code %d eligible %v gaps %v: %v", code, r.MergeEligible, r.KnownGaps, r.Ineligible)
	}
}

func TestMerge_AnIntegrationEntryNeedsAPassOrAnExplainedSkip(t *testing.T) {
	bare := func(outcome string) entry { e := passEntry("TestB"); e.Outcome = outcome; return e }
	claimed := bare(outcomeSkip)
	claimed.AllowedSkip = "opt-in recorder"
	wrongReason := claimed
	wrongReason.AllowedSkip = "another reason"
	cases := []struct {
		name  string
		allow string
		entry entry
		want  string
	}{
		{"a failure with no problem line", "", bare(outcomeFail), "has outcome \"fail\", which is not a pass"},
		{"no outcome and no problem line", "", bare(outcomeNone), "has outcome \"none\", which is not a pass"},
		{"an outcome no ledger writes", "", bare("flaky"), "which no ledger writes"},
		{"a bare skip with no problem line", "", bare(outcomeSkip), "has no entry for it with that reason"},
		{"an allowed skip the list does not carry", "", claimed, "has no entry for it with that reason"},
		{"an allowed skip with another reason", "integration\t" + mPkg + "\tTestB\topt-in recorder\n", wrongReason, "has no entry for it with that reason"},
		{"an allowed skip for another suite", "unit\t" + mPkg + "\tTestB\topt-in recorder\n", claimed, "has no entry for it with that reason"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			f := newMergeFixture(t, tierFull, false)
			if c.allow != "" {
				f.pinAllowlist(c.allow)
			}
			f.writeLocal(ledger{Suite: "integration", Tests: []entry{passEntry("TestA"), c.entry}})
			r, code := f.merge()
			if code != 1 || r.MergeEligible || !strings.Contains(joined(r), c.want) {
				t.Errorf("code %d eligible %v: %v", code, r.MergeEligible, r.Ineligible)
			}
		})
	}
	// "any" covers the integration suite too.
	f := newMergeFixture(t, tierFull, false)
	f.pinAllowlist("any\t" + mPkg + "\tTestB\topt-in recorder\n")
	f.writeLocal(ledger{Suite: "integration", Tests: []entry{passEntry("TestA"), claimed}})
	if r, code := f.merge(); code != 0 || !r.MergeEligible {
		t.Errorf("an any-suite allowance: code %d %v", code, r.Ineligible)
	}
}

func TestMerge_AUnitEntryWithoutAProblemLineStillFails(t *testing.T) {
	f := newMergeFixture(t, tierFull, true)
	f.writeCI()
	f.writeShard(1, []entry{passEntry("TestA")}, nil, 1)
	f.writeShard(2, []entry{passEntry("TestB")}, nil, 1)
	fast := f.basePart("fast")
	fast.Gates = []gateRecord{{Gate: "unit", Result: "pass"}}
	f.writePart(fast, map[string]any{"unit-tests.json": ledger{Suite: "unit", Problems: []string{},
		Tests: []entry{{Package: mPkg, Test: "TestUnit", Outcome: outcomeFail}}}})
	r, code := f.merge()
	if code != 1 || !strings.Contains(joined(r), "has outcome \"fail\"") {
		t.Errorf("code %d: %v", code, r.Ineligible)
	}
}

func TestMerge_ThePlanMustBeTheWholeSuiteOfItsTier(t *testing.T) {
	cases := []struct {
		name   string
		mutate func(f *mergeFixture)
		want   string
	}{
		{"an incomplete plan", func(f *mergeFixture) { f.plan.Complete = false }, "the plan is not complete"},
		{"full with include", func(f *mergeFixture) { f.plan.Nightly.Mode = nightlyInclude }, "that tier runs mode defer"},
		{"full with only", func(f *mergeFixture) { f.plan.Nightly.Mode = nightlyOnly }, "that tier runs mode defer"},
		{"another tier's parts", func(f *mergeFixture) { f.plan.PartsExpected = f.plan.PartsExpected[:len(f.plan.PartsExpected)-1] }, "the plan expects parts"},
		{"a tree-less plan", func(f *mergeFixture) { f.plan.Tree = "" }, "the plan records no tree"},
		{"an id-less plan", func(f *mergeFixture) { f.plan.ProofID = "" }, "the plan records no proof id"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			f := newMergeFixture(t, tierFull, true)
			c.mutate(f)
			f.save()
			f.writeCI()
			f.writeShard(1, []entry{passEntry("TestA")}, nil, 1)
			f.writeShard(2, []entry{passEntry("TestB")}, nil, 1)
			r, code := f.merge()
			if r.MergeEligible || code == 0 && !strings.Contains(joined(r), c.want) || !strings.Contains(joined(r), c.want) {
				t.Errorf("code %d eligible %v: %v", code, r.MergeEligible, r.Ineligible)
			}
		})
	}
	for tier, mode := range map[string]string{tierRelease: nightlyDefer, tierNightly: nightlyInclude} {
		f := newMergeFixture(t, tier, true)
		f.plan.Nightly.Mode = mode
		f.save()
		r, _ := f.merge()
		if !strings.Contains(joined(r), "that tier runs mode") {
			t.Errorf("tier %s with mode %s: %v", tier, mode, r.Ineligible)
		}
	}
}

func TestMerge_APartTheTierDoesNotExpectIsFlagged(t *testing.T) {
	for _, extra := range []string{"observability", "ui-e2e:2", "integration:3", "activity-fixture:bogus", "local"} {
		t.Run(extra, func(t *testing.T) {
			f := newMergeFixture(t, tierFull, true)
			f.writeCI()
			f.writeShard(1, []entry{passEntry("TestA")}, nil, 1)
			f.writeShard(2, []entry{passEntry("TestB")}, nil, 1)
			rec := f.basePart(extra)
			rec.Gates = []gateRecord{{Gate: "ui-e2e", Result: "fail"}}
			f.writePart(rec, nil)
			r, code := f.merge()
			if code != 1 || !strings.Contains(joined(r), "part "+extra+" is not one the plan expects") {
				t.Errorf("code %d: %v", code, r.Ineligible)
			}
			for _, g := range r.Gates {
				if g.Gate == "ui-e2e" && g.Result != "pass" {
					t.Errorf("an unexpected part's gate counted: %+v", g)
				}
			}
		})
	}
}

func TestMerge_ACIProofWithoutAUIPlanFails(t *testing.T) {
	f := newMergeFixture(t, tierFull, true)
	f.writeCI()
	f.writeShard(1, []entry{passEntry("TestA")}, nil, 1)
	f.writeShard(2, []entry{passEntry("TestB")}, nil, 1)
	quiet(t)
	code := cmdMerge([]string{"-plan", f.planPath, "-parts", f.parts, "-out", f.out, "-name", "t", "-host-speed", f.hostSpeed})
	var r proofRecord
	if err := readJSON(filepath.Join(f.out, "proof.json"), &r); err != nil {
		t.Fatal(err)
	}
	if code != 1 || !strings.Contains(joined(r), "was given no UI plan") {
		t.Errorf("code %d: %v", code, r.Ineligible)
	}
}

func TestMerge_IdentityFieldsAbsentOnBothSidesDoNotMatch(t *testing.T) {
	cases := []struct {
		name   string
		mutate func(r *partRecord)
		want   string
	}{
		{"no plan hash", func(r *partRecord) { r.PlanSHA256 = "" }, "recorded no plan hash"},
		{"no images", func(r *partRecord) { r.Images = nil }, "recorded no activity image digests"},
		{"no image tag", func(r *partRecord) { r.ImageTag = "" }, "activity images were tag"},
		{"another UI plan", func(r *partRecord) { r.UIPlanSHA256 = "other" }, "it read UI plan"},
		{"no UI plan hash", func(r *partRecord) { r.UIPlanSHA256 = "" }, "it read UI plan"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			f := newMergeFixture(t, tierFull, true)
			f.writeCI()
			f.writeShard(1, []entry{passEntry("TestA")}, nil, 1)
			f.writeShard(2, []entry{passEntry("TestB")}, nil, 1)
			ui := f.basePart("ui-e2e:1")
			ui.Gates = []gateRecord{{Gate: "ui-e2e", Result: "pass"}}
			c.mutate(&ui)
			f.writePart(ui, map[string]any{"ui-e2e-tests.json": uiLedgerFixture()})
			r, _ := f.merge()
			if r.MergeEligible || !strings.Contains(joined(r), c.want) {
				t.Errorf("eligible %v: %v", r.MergeEligible, r.Ineligible)
			}
		})
	}
	// A build part that recorded no digests leaves nothing to compare with.
	f := newMergeFixture(t, tierFull, true)
	f.writeCI()
	f.writeShard(1, []entry{passEntry("TestA")}, nil, 1)
	f.writeShard(2, []entry{passEntry("TestB")}, nil, 1)
	build := f.basePart("build")
	build.Gates = []gateRecord{{Gate: "images", Result: "pass"}}
	build.Images = nil
	f.writePart(build, nil)
	if r, _ := f.merge(); r.MergeEligible || !strings.Contains(joined(r), "the build part recorded no activity image digests") {
		t.Errorf("a build without digests: %v", r.Ineligible)
	}
}

func TestPartFromJobName_StripsAReusableWorkflowPrefix(t *testing.T) {
	for in, want := range map[string]string{
		"integration (3)":                 "integration:3",
		"gate / integration (3)":          "integration:3",
		"nightly / integration (3)":       "integration:3",
		"gate / ui-e2e (1)":               "ui-e2e:1",
		"gate / activity-fixture (shell)": "activity-fixture:shell",
		"gate / fast":                     "fast",
		"gate / observability":            "observability",
		"gate / proof":                    "",
	} {
		if got := partFromJobName(in); got != want {
			t.Errorf("partFromJobName(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestMerge_ACorruptPartJSONOrUnreadableEventsIsAResult(t *testing.T) {
	for name, setup := range map[string]func(dir string){
		"a corrupt part.json": func(dir string) {
			_ = os.WriteFile(filepath.Join(dir, "part.json"), []byte("{not json"), 0o644)
		},
		"unreadable events": func(dir string) {
			// A directory where the events file belongs opens and then fails to read.
			_ = os.MkdirAll(filepath.Join(dir, "integration.events.jsonl"), 0o755)
		},
	} {
		t.Run(name, func(t *testing.T) {
			f := newMergeFixture(t, tierFull, true)
			f.writeCI()
			f.writeShard(1, []entry{passEntry("TestA")}, nil, 1)
			// Attempt 1 of shard 2 is unreadable; attempt 2 ran again.
			dir := filepath.Join(f.parts, "integration-2-a1")
			if err := os.MkdirAll(dir, 0o755); err != nil {
				t.Fatal(err)
			}
			setup(dir)
			f.writeShard(2, []entry{passEntry("TestB")}, nil, 2)
			f.writeJobs(1, githubJob{Part: "integration:2", Conclusion: "failure", RunnerLost: true})
			f.writeJobs(2, githubJob{Part: "integration:2", Conclusion: "success"})
			r, _ := f.merge()
			if r.MergeEligible || !strings.Contains(joined(r), "after attempt 1 recorded its result") {
				t.Errorf("the unreadable attempt was not a result: %v", r.Ineligible)
			}
		})
	}
}

func TestReadParts_RefusesTwoDirectoriesForOneAttempt(t *testing.T) {
	f := newMergeFixture(t, tierFull, true)
	f.writeShard(1, []entry{passEntry("TestA")}, nil, 1)
	rec := f.basePart("integration:1")
	if err := os.MkdirAll(filepath.Join(f.parts, "copy-of-it"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := writeJSON(filepath.Join(f.parts, "copy-of-it", "part.json"), rec); err != nil {
		t.Fatal(err)
	}
	if _, _, err := readParts(f.parts); err == nil || !strings.Contains(err.Error(), "both claim part integration:1 attempt 1") {
		t.Errorf("err = %v", err)
	}
	quiet(t)
	if code := cmdMerge([]string{"-plan", f.planPath, "-parts", f.parts, "-out", f.out}); code != 2 {
		t.Errorf("merge exit %d, want 2", code)
	}
}

func TestMerge_AWriteErrorIsANonZeroExit(t *testing.T) {
	f := newMergeFixture(t, tierFull, false)
	f.writeLocal(ledger{Suite: "integration", Tests: []entry{passEntry("TestA"), passEntry("TestB")}})
	// The README's path is a directory: it cannot be written.
	if err := os.MkdirAll(filepath.Join(f.out, "README.md"), 0o755); err != nil {
		t.Fatal(err)
	}
	quiet(t)
	code := cmdMerge([]string{"-plan", f.planPath, "-parts", f.parts, "-out", f.out, "-name", "t", "-ui-plan", f.uiPlanPath, "-host-speed", f.hostSpeed})
	if code != 2 {
		t.Errorf("exit %d, want 2", code)
	}
	if err := os.RemoveAll(filepath.Join(f.out, "README.md")); err != nil {
		t.Fatal(err)
	}
	if err := os.RemoveAll(filepath.Join(f.out, "integration-tests.json")); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(f.out, "integration-tests.json"), 0o755); err != nil {
		t.Fatal(err)
	}
	if code := cmdMerge([]string{"-plan", f.planPath, "-parts", f.parts, "-out", f.out, "-name", "t", "-ui-plan", f.uiPlanPath, "-host-speed", f.hostSpeed}); code != 2 {
		t.Errorf("a ledger that cannot be written: exit %d, want 2", code)
	}
}

func TestMerge_AnUnreadablePlanHashIsAnError(t *testing.T) {
	quiet(t)
	f := newMergeFixture(t, tierFull, false)
	f.writeLocal(ledger{Suite: "integration", Tests: []entry{passEntry("TestA"), passEntry("TestB")}})
	if code := cmdMerge([]string{"-plan", f.planPath, "-ui-plan", filepath.Join(f.dir, "absent.json"), "-parts", f.parts, "-out", f.out}); code != 2 {
		t.Errorf("exit %d, want 2", code)
	}
}

// The reasons and the gate logs come out in one order, whatever order the
// map of parts is walked in.
func TestMerge_TheOutputIsDeterministic(t *testing.T) {
	var first proofRecord
	for i := 0; i < 8; i++ {
		f := newMergeFixture(t, tierFull, true)
		f.writeCI()
		f.writeShard(1, []entry{passEntry("TestA")}, nil, 1)
		f.writeShard(2, []entry{passEntry("TestB")}, nil, 1)
		for _, part := range []string{"fast", "build", "integration:1", "integration:2", "ui-e2e:1", "activity-fixture:shell"} {
			var rec partRecord
			dir := filepath.Join(f.parts, strings.NewReplacer(":", "-").Replace(part)+"-a1")
			if err := readJSON(filepath.Join(dir, "part.json"), &rec); err != nil {
				t.Fatal(err)
			}
			rec.Tree, rec.TreeEnd = "other", "other"
			rec.Gates = append(rec.Gates, gateRecord{Gate: "extra", Result: "pass", Log: part + ".log"})
			if err := writeJSON(filepath.Join(dir, "part.json"), rec); err != nil {
				t.Fatal(err)
			}
		}
		r, _ := f.merge()
		if i == 0 {
			first = r
			continue
		}
		if !reflect.DeepEqual(first.Ineligible, r.Ineligible) || !reflect.DeepEqual(first.Gates, r.Gates) {
			t.Fatalf("run %d differs:\n%v\n%v", i, first.Ineligible, r.Ineligible)
		}
	}
	if !strings.HasPrefix(first.Ineligible[0], "part activity-fixture:shell") {
		t.Errorf("reasons are not in part order: %v", first.Ineligible[:2])
	}
}

func TestVerify_AProofWhoseQuarantineCheckIsOlderThanItsArtifactIsRefused(t *testing.T) {
	today := time.Date(2026, 10, 21, 0, 0, 0, 0, time.UTC)
	for name, c := range map[string]struct {
		on   string
		want bool
	}{
		"today":                {"2026-10-21", false},
		"at the retention":     {"2026-09-21", false},
		"one day past it":      {"2026-09-20", true},
		"months old":           {"2026-05-01", true},
		"not a date":           {"soon", true},
		"a proof with no date": {"", false},
	} {
		r := proofRecord{Quarantine: &quarantineRecord{CheckedOn: c.on}}
		if got := quarantineAgeProblem(r, today) != ""; got != c.want {
			t.Errorf("%s: refused = %v, want %v", name, got, c.want)
		}
	}
	old := proofRecord{Tier: "full", Result: "pass", MergeEligible: true, Tree: "t1", BaseCommit: "m1",
		Quarantine: &quarantineRecord{CheckedOn: "2020-01-01"}}
	if p := strings.Join(verifyProblems(old, "t1", "m1", "m1", "", "main"), "\n"); !strings.Contains(p, "prove the candidate again") {
		t.Errorf("verifyProblems did not refuse an old quarantine check: %q", p)
	}
}

// A main without a nightly list needs no nightly proof; a git error is not
// that: it refuses.
func TestVerify_ANightlyListThatCannotBeReadIsNotAnAbsentOne(t *testing.T) {
	git := func(dir string, args ...string) {
		t.Helper()
		cmd := exec.Command("git", args...)
		cmd.Dir = dir
		cmd.Env = append(os.Environ(), "GIT_AUTHOR_NAME=t", "GIT_AUTHOR_EMAIL=t@t", "GIT_COMMITTER_NAME=t", "GIT_COMMITTER_EMAIL=t@t")
		if out, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("git %v: %v: %s", args, err, out)
		}
	}
	dir := t.TempDir()
	git(dir, "init", "-q", "-b", "main")
	if err := os.WriteFile(filepath.Join(dir, "f"), []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	git(dir, "add", "f")
	git(dir, "commit", "-q", "-m", "one")
	tip, err := exec.Command("git", "-C", dir, "rev-parse", "HEAD").Output()
	if err != nil {
		t.Fatal(err)
	}
	t.Chdir(dir)
	if p := verifyNightlyProblems("", strings.TrimSpace(string(tip))); len(p) != 0 {
		t.Errorf("a main with no nightly list was refused: %v", p)
	}
	if p := strings.Join(verifyNightlyProblems("", "0000000000000000000000000000000000000001"), "\n"); !strings.Contains(p, "cannot be read") {
		t.Errorf("a git error was read as an absent list: %q", p)
	}
	if err := os.MkdirAll(filepath.Join(dir, "scripts"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "scripts", "proof-nightly-tests.txt"), []byte(mainNightlyList), 0o644); err != nil {
		t.Fatal(err)
	}
	git(dir, "add", ".")
	git(dir, "commit", "-q", "-m", "two")
	tip2, _ := exec.Command("git", "-C", dir, "rev-parse", "HEAD").Output()
	if p := strings.Join(verifyNightlyProblems("", strings.TrimSpace(string(tip2))), "\n"); !strings.Contains(p, "NIGHTLY=proof/<name>") {
		t.Errorf("a main with a nightly list: %q", p)
	}
}

func TestRecord_APlanItCannotHashIsAnError(t *testing.T) {
	dir := t.TempDir()
	quiet(t)
	args := []string{"-dir", dir, "-tier", "full", "-part", "local", "-plan", filepath.Join(dir, "absent.json")}
	if code := cmdRecord(args); code != 2 {
		t.Errorf("exit %d, want 2", code)
	}
}
