package main

import (
	"os"
	"path/filepath"
	"reflect"
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

func TestMerge_ALocalProofIsOnePart(t *testing.T) {
	f := newMergeFixture(t, tierFull, false)
	f.writeLocal(ledger{Suite: "integration", Tests: []entry{passEntry("TestA"), passEntry("TestB")}})
	r, code := f.merge()
	if code != 0 || len(r.Parts) != 1 || r.Parts[0] != "local" {
		t.Fatalf("exit %d, parts %v: a local proof is exactly one part, local", code, r.Parts)
	}
}

func TestProof_ALocalOnePartProofAndAOneShardCIProofAgree(t *testing.T) {
	local := newMergeFixture(t, tierFull, false)
	local.writeLocal(ledger{Suite: "integration", Tests: []entry{passEntry("TestA"), passEntry("TestB")}})
	lp, lcode := local.merge()

	sharded := newMergeFixture(t, tierFull, true)
	sharded.writeCI()
	sharded.writeShard(1, []entry{passEntry("TestA")}, nil, 1)
	sharded.writeShard(2, []entry{passEntry("TestB")}, nil, 1)
	sp, scode := sharded.merge()

	if lcode != scode || lp.Result != sp.Result || lp.MergeEligible != sp.MergeEligible {
		t.Fatalf("local exit %d result %s eligible %v; CI exit %d result %s eligible %v",
			lcode, lp.Result, lp.MergeEligible, scode, sp.Result, sp.MergeEligible)
	}
	if !reflect.DeepEqual(lp.Ineligible, sp.Ineligible) {
		t.Errorf("ineligible reasons differ: local %v, CI %v", lp.Ineligible, sp.Ineligible)
	}
	if !reflect.DeepEqual(lp.Ledgers, sp.Ledgers) {
		t.Errorf("ledgers differ: local %v, CI %v", lp.Ledgers, sp.Ledgers)
	}
	strip := func(gates []gateRecord) []gateRecord {
		out := append([]gateRecord{}, gates...)
		for i := range out {
			out[i].Log = ""
		}
		return out
	}
	if !reflect.DeepEqual(strip(lp.Gates), strip(sp.Gates)) {
		t.Errorf("gates differ: local %v, CI %v", lp.Gates, sp.Gates)
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

// writeLocalWith is writeLocal with one part record edited first, for the
// identity rules verdict() applies to a local proof as it does to CI.
func (f *mergeFixture) writeLocalWith(mutate func(*partRecord), integration ledger) {
	f.t.Helper()
	rec := f.basePart("local")
	rec.Gates = fullGateRecords()
	rec.Images = map[string]string{"activity-shell": "sha256:aaa"}
	if mutate != nil {
		mutate(&rec)
	}
	f.writePart(rec, map[string]any{
		"integration-tests.json": integration,
		"unit-tests.json":        unitLedger(),
		"ui-e2e-tests.json":      uiLedgerFixture(),
	})
}

func TestMerge_EveryStackPartRanItsOwnTemporal(t *testing.T) {
	f := newMergeFixture(t, tierFull, false)
	f.writeLocalWith(func(r *partRecord) { r.Temporal = temporalRecord{Mode: "shared", Address: "localhost:7233"} },
		ledger{Suite: "integration", Tests: []entry{passEntry("TestA"), passEntry("TestB")}})
	r, _ := f.merge()
	if r.MergeEligible {
		t.Fatal("a local proof on a shared Temporal is merge-eligible")
	}
	joined := strings.Join(r.Ineligible, "\n")
	if !strings.Contains(joined, "Temporal was shared, not its own") {
		t.Errorf("ineligible = %v", r.Ineligible)
	}
	if r.Temporal.Mode != "shared" {
		t.Errorf("the merged proof says Temporal %q, want the part's shared", r.Temporal.Mode)
	}
}

func TestMerge_TheImageTagIsThePlansOwn(t *testing.T) {
	f := newMergeFixture(t, tierFull, false)
	f.writeLocalWith(func(r *partRecord) { r.ImageTag, r.ImageTagOwn = "borrowed-tag", false },
		ledger{Suite: "integration", Tests: []entry{passEntry("TestA"), passEntry("TestB")}})
	r, _ := f.merge()
	if r.MergeEligible {
		t.Fatal("a local proof on an outside image tag is merge-eligible")
	}
	joined := strings.Join(r.Ineligible, "\n")
	if !strings.Contains(joined, "activity images were tag \"borrowed-tag\" (own: false)") ||
		!strings.Contains(joined, "ORK_TEST_IMAGE_TAG was supplied from outside") {
		t.Errorf("ineligible = %v", r.Ineligible)
	}
}

func TestMerge_AProofGatesOverrideIsIneligible(t *testing.T) {
	// Naming every gate does not make the override invisible: the part records
	// that PROOF_GATES chose them, and the merged proof is ineligible whatever
	// it names.
	f := newMergeFixture(t, tierFull, false)
	f.writeLocalWith(func(r *partRecord) { r.GatesOverride = strings.Join(fullGates, ",") },
		ledger{Suite: "integration", Tests: []entry{passEntry("TestA"), passEntry("TestB")}})
	r, code := f.merge()
	if code != 0 || r.Result != "pass" {
		t.Fatalf("code %d, result %s: every gate ran and passed", code, r.Result)
	}
	if r.MergeEligible {
		t.Fatal("a proof whose gates PROOF_GATES chose is merge-eligible")
	}
	if !strings.Contains(strings.Join(r.Ineligible, "\n"), "PROOF_GATES="+strings.Join(fullGates, ",")+" chose the gates") {
		t.Errorf("ineligible = %v", r.Ineligible)
	}
}

func TestMerge_ImageDigestsAreIdenticalAcrossParts(t *testing.T) {
	f := newMergeFixture(t, tierFull, true)
	f.writeCI()
	f.writeShard(1, []entry{passEntry("TestA")}, nil, 1)
	f.writeShard(2, []entry{passEntry("TestB")}, nil, 1)
	// The second shard ran an image whose digest is not the build's.
	rec := f.basePart("integration:2")
	rec.Gates = []gateRecord{{Gate: "integration", Result: "pass"}}
	rec.Images = map[string]string{"activity-shell": "sha256:bbb"}
	f.writePart(rec, map[string]any{"integration-tests.json": ledger{Suite: "integration", Tests: []entry{passEntry("TestB")}}})
	r, _ := f.merge()
	if r.MergeEligible {
		t.Fatal("parts that ran different image digests are merge-eligible")
	}
	if !strings.Contains(strings.Join(r.Ineligible, "\n"), "activity-shell resolved to sha256:bbb") {
		t.Errorf("ineligible = %v", r.Ineligible)
	}
}

func TestMerge_ShardListingsMatchThePlan(t *testing.T) {
	f := newMergeFixture(t, tierFull, true)
	f.plan.Packages[0].ListingSHA256 = "plan-listing"
	if err := writeJSON(f.planPath, f.plan); err != nil {
		t.Fatal(err)
	}
	f.planSHA, _ = fieldSHA256(f.planPath)
	f.writeCI()
	// The shard compiled the package and its listing differs from the plan's.
	rec := f.basePart("integration:1")
	rec.Gates = []gateRecord{{Gate: "integration", Result: "pass"}}
	rec.Images = map[string]string{"activity-shell": "sha256:aaa"}
	f.writePart(rec, map[string]any{
		"integration-tests.json": ledger{
			Suite: "integration", Tests: []entry{passEntry("TestA")},
			Binaries: map[string]binaryRecord{mPkg: {SHA256: "sha", ListingSHA256: "other-listing"}},
		},
	})
	f.writeShard(2, []entry{passEntry("TestB")}, nil, 1)
	r, _ := f.merge()
	if r.MergeEligible {
		t.Fatal("a shard whose listing differs from the plan is merge-eligible")
	}
	if !strings.Contains(strings.Join(r.Ineligible, "\n"), "listed "+mPkg+" differently from the plan") {
		t.Errorf("ineligible = %v", r.Ineligible)
	}
}

func TestMerge_AQuarantinedFailureIsRecordedAndDoesNotFailTheProof(t *testing.T) {
	f := newMergeFixture(t, tierFull, false)
	quarantined := passEntry("TestB")
	quarantined.Outcome = "quarantined-fail"
	quarantined.Quarantine = "unit " + mPkg + " TestB Daniel Santos 2026-10-19 2026-11-18 known flake"
	if err := writeJSON(filepath.Join(f.dir, "quarantine.txt"), map[string]any{}); err != nil {
		t.Fatal(err)
	}
	f.plan.Quarantine = quarantineRef{File: filepath.Join(f.dir, "quarantine-empty.txt"), SHA256: ""}
	if err := os.WriteFile(filepath.Join(f.dir, "quarantine-empty.txt"),
		[]byte("integration\t"+mPkg+"\tTestB\tDaniel Santos <danlsgiga@gmail.com>\t2026-10-19\t2026-11-18\tknown flake\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := writeJSON(f.planPath, f.plan); err != nil {
		t.Fatal(err)
	}
	f.planSHA, _ = fieldSHA256(f.planPath)
	f.writeLocal(ledger{Suite: "integration", Tests: []entry{passEntry("TestA"), quarantined}})
	r, code := f.merge()
	if code != 0 || r.Result != "pass" || !r.MergeEligible {
		t.Fatalf("code %d, result %s eligible %v: %v", code, r.Result, r.MergeEligible, r.Ineligible)
	}
	if len(r.Quarantined) != 1 || !strings.Contains(r.Quarantined[0], "quarantined-fail") {
		t.Fatalf("quarantined = %v, want the entry and its quarantined-fail outcome", r.Quarantined)
	}
	b, _ := os.ReadFile(filepath.Join(f.out, "README.md"))
	if !strings.Contains(string(b), "## Quarantined") {
		t.Errorf("the README does not list the quarantined test:\n%s", b)
	}
}

func TestMerge_RecordsHostSpeedFailuresWithoutChangingTheVerdict(t *testing.T) {
	f := newMergeFixture(t, tierFull, true)
	f.writeCI()
	// Shard 1 ran on 2 CPUs and its only test failed a bound declared for 16.
	rec := f.basePart("integration:1")
	rec.Gates = []gateRecord{{Gate: "integration", Result: "fail"}}
	rec.Images = map[string]string{"activity-shell": "sha256:aaa"}
	rec.Runner = runnerRecord{OS: "linux", CPUs: 2}
	failed := passEntry("TestA")
	failed.Outcome = outcomeFail
	f.writePart(rec, map[string]any{
		"integration-tests.json": ledger{
			Suite: "integration", Tests: []entry{failed},
			Problems: []string{mPkg + ".TestA failed (job m-001)"},
		},
	})
	f.writeShard(2, []entry{passEntry("TestB")}, nil, 1)
	host := filepath.Join(f.dir, "host-speed.tsv")
	if err := os.WriteFile(host, []byte("test\t"+mPkg+"\tTestA\t60\t16\tthe stage completes within 60 s\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	r, code := f.merge("-host-speed", host)
	if code == 0 || r.Result != "fail" || r.MergeEligible {
		t.Fatalf("a failing bound passed: code %d, result %s, %v", code, r.Result, r.Ineligible)
	}
	if len(r.HostSpeedFailures) != 1 || r.HostSpeedFailures[0].Kind != "integration" ||
		r.HostSpeedFailures[0].RunnerCPUs != 2 || r.HostSpeedFailures[0].ReferenceCPUs != 16 {
		t.Fatalf("host_speed_failures = %+v", r.HostSpeedFailures)
	}
	if len(r.KnownGaps) == 0 || !strings.Contains(strings.Join(r.KnownGaps, " "), "2-CPU runner") {
		t.Errorf("known_gaps = %v", r.KnownGaps)
	}
}

func TestMerge_ADeferredEntryTheBaseListLacksFailsTheProof(t *testing.T) {
	f := newMergeFixture(t, tierFull, false)
	f.plan.Nightly = nightlyRef{Mode: nightlyDefer,
		Entries:     []string{mPkg + "\tTestB"},
		BaseEntries: nil,
	}
	f.plan.Deferred = []deferredRef{{Package: mPkg, Test: "TestB", Entry: mPkg + "\tTestB\tTestA\t2026-10-18 Daniel Santos\tsoak"}}
	if err := writeJSON(f.planPath, f.plan); err != nil {
		t.Fatal(err)
	}
	f.planSHA, _ = fieldSHA256(f.planPath)
	f.writeLocal(ledger{Suite: "integration", Tests: []entry{passEntry("TestA")}})
	r, _ := f.merge()
	if !strings.Contains(strings.Join(r.Ineligible, "\n"),
		"TestB was deferred, but the base's nightly list does not carry it") {
		t.Errorf("ineligible = %v", r.Ineligible)
	}
}

func TestMerge_ATamperedBaseListFailsTheProof(t *testing.T) {
	f := newMergeFixture(t, tierFull, false)
	baseFile := filepath.Join(f.dir, "nightly-base.txt")
	if err := os.WriteFile(baseFile, []byte(mPkg+"\tTestB\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	f.plan.Nightly = nightlyRef{Mode: nightlyDefer,
		Entries:     []string{mPkg + "\tTestB"},
		BaseEntries: []string{mPkg + "\tTestB"},
		BaseSHA256:  "not-the-hash-of-what-is-here",
	}
	f.plan.Deferred = []deferredRef{{Package: mPkg, Test: "TestB", Entry: mPkg + "\tTestB\tTestA\t2026-10-18 Daniel Santos\tsoak"}}
	if err := writeJSON(f.planPath, f.plan); err != nil {
		t.Fatal(err)
	}
	f.planSHA, _ = fieldSHA256(f.planPath)
	f.writeLocal(ledger{Suite: "integration", Tests: []entry{passEntry("TestA")}})
	r, _ := f.merge()
	if !strings.Contains(strings.Join(r.Ineligible, "\n"), "base's nightly list in the plan artifact is not the one the plan hashed") {
		t.Errorf("ineligible = %v", r.Ineligible)
	}
}

func TestMerge_TheAllowedSkipListMustBeThePlans(t *testing.T) {
	f := newMergeFixture(t, tierFull, false)
	allow := filepath.Join(f.dir, "allowed-skips.txt")
	if err := os.WriteFile(allow, []byte(""), 0o644); err != nil {
		t.Fatal(err)
	}
	f.plan.AllowedSkips = fileRef{File: allow, SHA256: "a-hash-of-other-bytes"}
	if err := writeJSON(f.planPath, f.plan); err != nil {
		t.Fatal(err)
	}
	f.planSHA, _ = fieldSHA256(f.planPath)
	f.writeLocal(ledger{Suite: "integration", Tests: []entry{passEntry("TestA"), passEntry("TestB")}})
	r, _ := f.merge()
	if !strings.Contains(strings.Join(r.Ineligible, "\n"), "is not the one the plan read") {
		t.Errorf("ineligible = %v", r.Ineligible)
	}
}

func TestMerge_AGateThatDidNotRunIsIneligible(t *testing.T) {
	f := newMergeFixture(t, tierFull, false)
	rec := f.basePart("local")
	rec.Gates = fullGateRecords()
	rec.Gates = rec.Gates[1:] // drop "lint"
	rec.Images = map[string]string{"activity-shell": "sha256:aaa"}
	f.writePart(rec, map[string]any{
		"integration-tests.json": ledger{Suite: "integration", Tests: []entry{passEntry("TestA"), passEntry("TestB")}},
		"unit-tests.json":        unitLedger(),
		"ui-e2e-tests.json":      uiLedgerFixture(),
	})
	r, _ := f.merge()
	if !strings.Contains(strings.Join(r.Ineligible, "\n"), "gate lint did not run") {
		t.Errorf("ineligible = %v", r.Ineligible)
	}
}
