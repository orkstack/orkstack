package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

const mPkg = "example.com/m"

type mergeFixture struct {
	t          *testing.T
	dir        string
	parts      string
	jobsDir    string
	out        string
	planPath   string
	uiPlanPath string
	plan       *integrationPlan
	planSHA    string
}

func fullGateRecords() []gateRecord {
	var g []gateRecord
	for _, name := range fullGates {
		g = append(g, gateRecord{Gate: name, Result: "pass"})
	}
	return g
}

func unitLedger() ledger {
	return ledger{Suite: "unit", Tests: []entry{{Package: mPkg, Test: "TestUnit", Outcome: outcomePass, Seconds: 1}}, Problems: []string{}}
}

func uiPlanFixture() uiPlan {
	p := uiPlan{Format: 1}
	p.Tests = []uiPlanTest{{File: "a.spec.ts", Title: "t", Project: "chromium"}}
	p.Files = []uiPlanFile{{File: "a.spec.ts", Tests: 1, Shard: 1}}
	p.Shards = []uiShard{{Index: 1, Files: []string{"a.spec.ts"}}}
	return p
}

func uiLedgerFixture() uiLedger {
	return uiLedger{
		Suite: "ui-e2e", Listed: 1, Pass: 1, GuardRetryCeiling: 3,
		Tests: []uiEntry{{File: "a.spec.ts", Title: "t", Project: "chromium", Outcome: outcomePass, Seconds: 10}},
		Files: []uiFileEntry{{File: "a.spec.ts", Shard: 1, Tests: 1, Seconds: 10, WallSeconds: 10}},
	}
}

func newMergeFixture(t *testing.T, tier string, ci bool) *mergeFixture {
	t.Helper()
	dir := t.TempDir()
	f := &mergeFixture{t: t, dir: dir, parts: filepath.Join(dir, "parts"), out: filepath.Join(dir, "out")}
	f.plan = &integrationPlan{
		Format: planFormat, Tree: "tree1", Commit: "commit1", Tier: tier, ProofID: "pr-ci-1",
		BaseRef: mainBranch, BaseCommit: "base1",
		Expected: []testRef{{mPkg, "TestA"}, {mPkg, "TestB"}},
		Jobs: []job{
			{ID: "m-001", Package: mPkg, Tests: []string{"TestA"}, Cost: 10, Shard: 1, TimeoutSeconds: 600},
			{ID: "m-002", Package: mPkg, Tests: []string{"TestB"}, Cost: 10, Shard: 2, TimeoutSeconds: 600},
		},
		Shards:   []planShard{{Index: 1, Jobs: []string{"m-001"}}, {Index: 2, Jobs: []string{"m-002"}}},
		Packages: []planPackage{{ImportPath: mPkg, Dir: ".", Binary: "m.test", Listed: []string{"TestA", "TestB"}}},
		Nightly:  nightlyRef{Mode: nightlyInclude},
	}
	if ci {
		f.plan.PartsExpected = []string{"plan", "build", "fast", "integration:1", "integration:2", "ui-e2e:1", "activity-fixture:shell"}
	} else {
		f.plan.PartsExpected = []string{"local"}
	}
	f.planPath = filepath.Join(dir, "integration-plan.json")
	if err := writeJSON(f.planPath, f.plan); err != nil {
		t.Fatal(err)
	}
	f.planSHA, _ = fieldSHA256(f.planPath)
	f.uiPlanPath = filepath.Join(dir, "ui-plan.json")
	if err := writeJSON(f.uiPlanPath, uiPlanFixture()); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(f.parts, 0o755); err != nil {
		t.Fatal(err)
	}
	return f
}

func (f *mergeFixture) basePart(part string) partRecord {
	r := partRecord{
		Part: part, Attempt: 1, Tier: f.plan.Tier, Tree: "tree1", TreeEnd: "tree1",
		CleanStart: true, CleanEnd: true, BaseRef: mainBranch, BaseCommit: "base1",
		ImageTag: f.plan.ProofID, ImageTagOwn: true, PlanSHA256: f.planSHA,
		UIPlanSHA256: sha256File(f.uiPlanPath),
		Runner:       runnerRecord{OS: "linux", CPUs: 16},
	}
	if isStackPart(part) {
		r.Temporal.Mode = "own"
	}
	return r
}

func (f *mergeFixture) writePart(rec partRecord, files map[string]any) {
	f.t.Helper()
	dir := filepath.Join(f.parts, strings.NewReplacer(":", "-").Replace(rec.Part)+"-a"+itoa(rec.Attempt))
	if err := os.MkdirAll(dir, 0o755); err != nil {
		f.t.Fatal(err)
	}
	if err := writeJSON(filepath.Join(dir, "part.json"), rec); err != nil {
		f.t.Fatal(err)
	}
	for name, v := range files {
		if err := writeJSON(filepath.Join(dir, name), v); err != nil {
			f.t.Fatal(err)
		}
	}
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	var b []byte
	for n > 0 {
		b = append([]byte{byte('0' + n%10)}, b...)
		n /= 10
	}
	return string(b)
}

func (f *mergeFixture) writeLocal(integration ledger) {
	f.t.Helper()
	rec := f.basePart("local")
	rec.Gates = fullGateRecords()
	rec.Images = map[string]string{"activity-shell": "sha256:aaa"}
	f.writePart(rec, map[string]any{
		"integration-tests.json": integration,
		"unit-tests.json":        unitLedger(),
		"ui-e2e-tests.json":      uiLedgerFixture(),
	})
}

func (f *mergeFixture) writeCI() {
	f.t.Helper()
	plan := f.basePart("plan")
	f.writePart(plan, nil)
	build := f.basePart("build")
	build.Gates = []gateRecord{{Gate: "images", Result: "pass"}}
	build.Images = map[string]string{"activity-shell": "sha256:aaa"}
	f.writePart(build, nil)
	fast := f.basePart("fast")
	fast.Gates = []gateRecord{
		{Gate: "lint", Result: "pass"}, {Gate: "build", Result: "pass"}, {Gate: "unit", Result: "pass"},
		{Gate: "vet-integration", Result: "pass"}, {Gate: "ui-check", Result: "pass"}, {Gate: "ui-unit", Result: "pass"},
	}
	f.writePart(fast, map[string]any{"unit-tests.json": unitLedger()})
	ui := f.basePart("ui-e2e:1")
	ui.Gates = []gateRecord{{Gate: "ui-e2e", Result: "pass"}}
	f.writePart(ui, map[string]any{"ui-e2e-tests.json": uiLedgerFixture()})
	fix := f.basePart("activity-fixture:shell")
	fix.Gates = []gateRecord{{Gate: "activity-fixture", Result: "pass"}}
	f.writePart(fix, nil)
}

func (f *mergeFixture) writeShard(n int, tests []entry, problems []string, attempt int) {
	f.t.Helper()
	rec := f.basePart("integration:" + itoa(n))
	rec.Attempt = attempt
	rec.Gates = []gateRecord{{Gate: "integration", Result: "pass"}}
	rec.Images = map[string]string{"activity-shell": "sha256:aaa"}
	f.writePart(rec, map[string]any{"integration-tests.json": ledger{Suite: "integration", Tests: tests, Problems: problems}})
}

func (f *mergeFixture) writeAttemptEvents(dirName string, lines ...string) {
	f.t.Helper()
	dir := filepath.Join(f.parts, dirName)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		f.t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "integration.events.jsonl"), []byte(strings.Join(lines, "\n")+"\n"), 0o644); err != nil {
		f.t.Fatal(err)
	}
}

func (f *mergeFixture) writeJobs(attempt int, job githubJob) {
	f.t.Helper()
	if f.jobsDir == "" {
		f.jobsDir = filepath.Join(f.dir, "jobs")
	}
	if err := os.MkdirAll(f.jobsDir, 0o755); err != nil {
		f.t.Fatal(err)
	}
	if err := writeJSON(filepath.Join(f.jobsDir, "attempt-"+itoa(attempt)+".json"), githubAttempt{Attempt: attempt, Jobs: []githubJob{job}}); err != nil {
		f.t.Fatal(err)
	}
}

func (f *mergeFixture) merge(extra ...string) (proofRecord, int) {
	f.t.Helper()
	quiet(f.t)
	args := []string{"-plan", f.planPath, "-parts", f.parts, "-out", f.out, "-name", "test"}
	if f.plan.Tier != tierNightly {
		args = append(args, "-ui-plan", f.uiPlanPath)
	}
	if f.jobsDir != "" {
		args = append(args, "-jobs", f.jobsDir)
	}
	args = append(args, extra...)
	code := cmdMerge(args)
	var r proofRecord
	if err := readJSON(filepath.Join(f.out, "proof.json"), &r); err != nil {
		f.t.Fatal(err)
	}
	return r, code
}

func passEntry(test string) entry {
	return entry{Package: mPkg, Test: test, Outcome: outcomePass, Seconds: 1}
}

func TestMerge_ALocalProofPasses(t *testing.T) {
	f := newMergeFixture(t, tierFull, false)
	f.writeLocal(ledger{Suite: "integration", Tests: []entry{passEntry("TestA"), passEntry("TestB")}})
	r, code := f.merge()
	if code != 0 || r.Result != "pass" || !r.MergeEligible {
		t.Fatalf("code %d, result %s eligible %v: %v", code, r.Result, r.MergeEligible, r.Ineligible)
	}
	b, _ := os.ReadFile(filepath.Join(f.out, "README.md"))
	if !strings.HasSuffix(string(b), "Result: **PASS**\n") {
		t.Errorf("README does not close with the full tier's result line:\n%s", b)
	}
}

func TestMerge_AMissingTestFailsTheProof(t *testing.T) {
	f := newMergeFixture(t, tierFull, false)
	f.writeLocal(ledger{Suite: "integration", Tests: []entry{passEntry("TestA")}})
	r, code := f.merge()
	if code != 1 || r.Result != "fail" {
		t.Fatalf("code %d, result %s: a missing test must fail the proof", code, r.Result)
	}
	if !strings.Contains(strings.Join(r.Ineligible, "\n"), "TestB has no outcome: no shard reported it") {
		t.Errorf("ineligible = %v", r.Ineligible)
	}
	if r.MergeEligible {
		t.Error("a proof with a missing test is merge-eligible")
	}
}

func TestMerge_ATestReportedByTwoShardsFailsTheProof(t *testing.T) {
	f := newMergeFixture(t, tierFull, true)
	f.writeCI()
	f.writeShard(1, []entry{passEntry("TestA"), passEntry("TestB")}, nil, 1)
	f.writeShard(2, []entry{passEntry("TestA")}, nil, 1)
	r, code := f.merge()
	if code != 1 {
		t.Fatalf("exit %d, want 1", code)
	}
	if !strings.Contains(strings.Join(r.Ineligible, "\n"), "TestA has outcomes from shards integration:1 and integration:2") {
		t.Errorf("ineligible = %v", r.Ineligible)
	}
}

func TestMerge_AMissingPartFailsTheProof(t *testing.T) {
	f := newMergeFixture(t, tierFull, true)
	f.writeCI()
	f.writeShard(1, []entry{passEntry("TestA"), passEntry("TestB")}, nil, 1)
	r, code := f.merge()
	if code != 1 {
		t.Fatalf("exit %d, want 1", code)
	}
	if !strings.Contains(strings.Join(r.Ineligible, "\n"), "part integration:2 left no result") {
		t.Errorf("ineligible = %v", r.Ineligible)
	}
}

func TestMerge_AllowedSkipsBehave(t *testing.T) {
	allowed := passEntry("TestB")
	allowed.Outcome, allowed.AllowedSkip = outcomeSkip, "opt-in recorder"
	f := newMergeFixture(t, tierFull, false)
	f.writeLocal(ledger{Suite: "integration", Tests: []entry{passEntry("TestA"), allowed}})
	if r, code := f.merge(); code != 0 || !r.MergeEligible {
		t.Fatalf("an allowed skip failed the proof: code %d, %v", code, r.Ineligible)
	}

	// A bare skip is a shard problem, and the merger carries it verbatim.
	f2 := newMergeFixture(t, tierFull, false)
	bare := passEntry("TestB")
	bare.Outcome = outcomeSkip
	f2.writeLocal(ledger{Suite: "integration", Tests: []entry{passEntry("TestA"), bare},
		Problems: []string{"example.com/m.TestB skipped (job m-002)"}})
	r, code := f2.merge()
	if code != 1 || !strings.Contains(strings.Join(r.Ineligible, "\n"), "local: example.com/m.TestB skipped") {
		t.Fatalf("a bare skip was not carried: code %d, %v", code, r.Ineligible)
	}
}

func TestMerge_ALostRunnerRerunIsRecordedAndEligible(t *testing.T) {
	f := newMergeFixture(t, tierFull, true)
	f.writeCI()
	f.writeShard(1, []entry{passEntry("TestA")}, nil, 1)
	f.writeAttemptEvents("integration-2-a1",
		`{"Action":"run","Package":"example.com/m","Test":"TestB"}`,
		`{"Action":"pass","Package":"example.com/m","Test":"TestB","Elapsed":1}`,
		`{"Action":"run","Package":"example.com/m","Test":"TestC"}`)
	f.writeShard(2, []entry{passEntry("TestB")}, nil, 2)
	f.writeJobs(1, githubJob{Name: "integration (2)", Conclusion: "failure", RunnerLost: true,
		Annotations: []string{"The runner has received a shutdown signal"},
		Log:         "proofrun: start example.com/m TestB\nproofrun: outcome pass example.com/m TestB 1.00\nproofrun: start example.com/m TestC\n"})
	r, code := f.merge()
	if code != 0 || r.Result != "pass" || !r.MergeEligible {
		t.Fatalf("code %d, result %s eligible %v: %v", code, r.Result, r.MergeEligible, r.Ineligible)
	}
	if len(r.Reruns) != 1 || r.Reruns[0].Reason != "lost-runner" || r.Reruns[0].Attempt != 2 {
		t.Fatalf("reruns = %+v", r.Reruns)
	}
	if got := strings.Join(r.Reruns[0].LostAttempt.Unfinished, ","); got != "example.com/m.TestC" {
		t.Errorf("unfinished = %q", got)
	}
	if r.Reruns[0].LostAttempt.PassedBeforeLoss != 1 {
		t.Errorf("passed before loss = %d, want 1", r.Reruns[0].LostAttempt.PassedBeforeLoss)
	}
}

func TestMerge_ARerunAfterAResultIsIneligible(t *testing.T) {
	f := newMergeFixture(t, tierFull, true)
	f.writeCI()
	f.writeShard(1, []entry{passEntry("TestA")}, nil, 1)
	f.writeAttemptEvents("integration-2-a1",
		`{"Action":"run","Package":"example.com/m","Test":"TestB"}`,
		`{"Action":"fail","Package":"example.com/m","Test":"TestB","Elapsed":1}`)
	f.writeShard(2, []entry{passEntry("TestB")}, nil, 2)
	f.writeJobs(1, githubJob{Name: "integration (2)", Conclusion: "failure"})
	r, code := f.merge()
	if code != 0 || r.Result != "pass" {
		t.Fatalf("code %d, result %s: the second attempt passed", code, r.Result)
	}
	if r.MergeEligible {
		t.Fatal("a proof built from a rerun of a failing test is merge-eligible")
	}
	if !strings.Contains(strings.Join(r.Ineligible, "\n"), "part integration:2 was run again (attempt 2) after attempt 1 recorded its result") {
		t.Errorf("ineligible = %v", r.Ineligible)
	}
	if len(r.Reruns) != 0 {
		t.Errorf("a rerun after a result was recorded as an allowed rerun: %+v", r.Reruns)
	}
}

func TestMerge_ACancelledAttemptWithAStartedTestIsAResult(t *testing.T) {
	f := newMergeFixture(t, tierFull, true)
	f.writeCI()
	f.writeShard(1, []entry{passEntry("TestA")}, nil, 1)
	f.writeAttemptEvents("integration-2-a1", `{"Action":"run","Package":"example.com/m","Test":"TestB"}`)
	f.writeShard(2, []entry{passEntry("TestB")}, nil, 2)
	f.writeJobs(1, githubJob{Name: "integration (2)", Conclusion: "cancelled", Log: "proofrun: start example.com/m TestB\n"})
	r, _ := f.merge()
	if r.MergeEligible {
		t.Fatal("a cancel with a test running is rerun-eligible")
	}
	if !strings.Contains(strings.Join(r.Ineligible, "\n"), "was run again") {
		t.Errorf("ineligible = %v", r.Ineligible)
	}
}

func TestMerge_AShardOutcomeForADeferredTestFailsTheProof(t *testing.T) {
	f := newMergeFixture(t, tierFull, false)
	f.plan.Deferred = []deferredRef{{Package: mPkg, Test: "TestB", Entry: "example.com/m\tTestB\tTestA\t2026-10-18 Daniel Santos\tsoak"}}
	f.plan.Nightly = nightlyRef{Mode: nightlyInclude}
	if err := writeJSON(f.planPath, f.plan); err != nil {
		t.Fatal(err)
	}
	f.planSHA, _ = fieldSHA256(f.planPath)
	f.writeLocal(ledger{Suite: "integration", Tests: []entry{passEntry("TestA"), passEntry("TestB")}})
	r, _ := f.merge()
	if !strings.Contains(strings.Join(r.Ineligible, "\n"), "which the plan deferred") {
		t.Errorf("ineligible = %v", r.Ineligible)
	}
}

func TestMerge_DeferredTestsAreRecordedNotRun(t *testing.T) {
	f := newMergeFixture(t, tierFull, false)
	entryLine := "example.com/m\tTestB\tTestA\t2026-10-18 Daniel Santos\tsoak"
	f.plan.Deferred = []deferredRef{{Package: mPkg, Test: "TestB", Entry: entryLine}}
	f.plan.Nightly = nightlyRef{Mode: nightlyInclude}
	if err := writeJSON(f.planPath, f.plan); err != nil {
		t.Fatal(err)
	}
	f.planSHA, _ = fieldSHA256(f.planPath)
	f.writeLocal(ledger{Suite: "integration", Tests: []entry{passEntry("TestA")}})
	r, code := f.merge()
	if code != 0 || !r.MergeEligible {
		t.Fatalf("code %d, eligible %v: %v", code, r.MergeEligible, r.Ineligible)
	}
	var l ledger
	if err := readJSON(filepath.Join(f.out, "integration-tests.json"), &l); err != nil {
		t.Fatal(err)
	}
	found := false
	for _, e := range l.Tests {
		if e.Test == "TestB" {
			found = true
			if e.Outcome != "deferred" || e.Reason != entryLine {
				t.Errorf("deferred entry = %+v", e)
			}
		}
	}
	if !found || l.Summary.Deferred != 1 {
		t.Errorf("the deferred test is not in the merged ledger: %+v", l.Summary)
	}
}

func TestMerge_ARunnerSizeUISpecIsAKnownGap(t *testing.T) {
	f := newMergeFixture(t, tierFull, true)
	f.writeCI()
	f.writeShard(1, []entry{passEntry("TestA")}, nil, 1)
	f.writeShard(2, []entry{passEntry("TestB")}, nil, 1)
	ui := f.basePart("ui-e2e:1")
	ui.Gates = []gateRecord{{Gate: "ui-e2e", Result: "fail"}}
	ui.Runner = runnerRecord{OS: "linux", CPUs: 2}
	problem := "a.spec.ts › t [chromium] failed (Expected: < 5000, Received: 8123)"
	f.writePart(ui, map[string]any{
		"ui-e2e-tests.json": uiLedger{
			Suite: "ui-e2e", Listed: 1, GuardRetryCeiling: 3,
			Tests:    []uiEntry{{File: "a.spec.ts", Title: "t", Project: "chromium", Outcome: outcomeFail, Seconds: 8.1}},
			Files:    []uiFileEntry{{File: "a.spec.ts", Shard: 1, Tests: 1, Seconds: 8.1, WallSeconds: 8.1}},
			Problems: []string{problem},
		},
	})
	host := filepath.Join(f.dir, "host-speed.tsv")
	if err := os.WriteFile(host, []byte("ui\te2e/a.spec.ts\t5\t16\tthe gated size is interactive within 5 s\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	r, code := f.merge("-host-speed", host)
	if code != 0 || r.Result != "pass" || !r.MergeEligible {
		t.Fatalf("code %d, result %s eligible %v: %v", code, r.Result, r.MergeEligible, r.Ineligible)
	}
	if len(r.HostSpeedFailures) != 1 || r.HostSpeedFailures[0].RunnerCPUs != 2 || r.HostSpeedFailures[0].ReferenceCPUs != 16 {
		t.Fatalf("host_speed_failures = %+v", r.HostSpeedFailures)
	}
	if len(r.KnownGaps) != 1 || !strings.Contains(r.KnownGaps[0], "8123") {
		t.Fatalf("known_gaps = %v", r.KnownGaps)
	}
	b, _ := os.ReadFile(filepath.Join(f.out, "README.md"))
	if !strings.Contains(string(b), "Known gaps: runner size") {
		t.Errorf("README lacks the known-gaps section:\n%s", b)
	}
}

func TestMerge_AnUnlistedUISpecFailureStillFails(t *testing.T) {
	f := newMergeFixture(t, tierFull, true)
	f.writeCI()
	f.writeShard(1, []entry{passEntry("TestA")}, nil, 1)
	f.writeShard(2, []entry{passEntry("TestB")}, nil, 1)
	ui := f.basePart("ui-e2e:1")
	ui.Gates = []gateRecord{{Gate: "ui-e2e", Result: "fail"}}
	ui.Runner = runnerRecord{OS: "linux", CPUs: 2}
	f.writePart(ui, map[string]any{
		"ui-e2e-tests.json": uiLedger{
			Suite: "ui-e2e", Listed: 1, GuardRetryCeiling: 3,
			Tests:    []uiEntry{{File: "a.spec.ts", Title: "t", Project: "chromium", Outcome: outcomeFail, Seconds: 8.1}},
			Files:    []uiFileEntry{{File: "a.spec.ts", Shard: 1, Tests: 1, Seconds: 8.1, WallSeconds: 8.1}},
			Problems: []string{"a.spec.ts › t [chromium] failed (a real regression)"},
		},
	})
	host := filepath.Join(f.dir, "host-speed.tsv")
	if err := os.WriteFile(host, []byte("ui\te2e/other.spec.ts\t5\t16\tnot this one\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	r, code := f.merge("-host-speed", host)
	if code == 0 || r.Result != "fail" || r.MergeEligible {
		t.Fatalf("an unlisted UI failure passed: code %d, %+v", code, r.Ineligible)
	}
}
