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
	hostSpeed  string
	plan       *integrationPlan
	planSHA    string
	uiPlan     uiPlan
}

// emptySHA is what fileSHA256 gives an absent or empty list.
var emptySHA = bytesSHA256(nil)

// mListing is the fixture package's listing hash, as a binary records it.
var mListing = sha256String("TestA\nTestB")

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
		Shards: []planShard{{Index: 1, Jobs: []string{"m-001"}}, {Index: 2, Jobs: []string{"m-002"}}},
		Packages: []planPackage{{ImportPath: mPkg, Dir: ".", Binary: "m.test", Listed: []string{"TestA", "TestB"},
			ListingSHA256: mListing, BinarySHA256: "planbin"}},
		Nightly:   nightlyRef{Mode: nightlyInclude},
		CheckedOn: "2026-10-21",
		// Every anchor is pinned, as writePlan pins it: the lists are absent
		// in the fixture, which hashes as empty.
		AllowedSkips: fileRef{File: "", SHA256: emptySHA},
		Quarantine:   quarantineRef{File: filepath.Join(dir, "quarantine.txt"), SHA256: emptySHA, BaseSHA256: emptySHA},
	}
	if ci {
		f.plan.PartsExpected = []string{"plan", "build", "fast", "integration:1", "integration:2", "ui-e2e:1", "activity-fixture:shell"}
	} else {
		f.plan.PartsExpected = []string{"local"}
	}
	f.hostSpeed = filepath.Join(dir, "host-speed.tsv")
	f.plan.HostSpeed = quarantineRef{File: f.hostSpeed, SHA256: emptySHA, BaseSHA256: emptySHA}
	f.planPath = filepath.Join(dir, "integration-plan.json")
	f.uiPlanPath = filepath.Join(dir, "ui-plan.json")
	f.uiPlan = uiPlanFixture()
	f.save()
	if err := os.MkdirAll(f.parts, 0o755); err != nil {
		t.Fatal(err)
	}
	return f
}

// save writes the plans; parts written after it carry the plan's hash.
func (f *mergeFixture) save() {
	f.t.Helper()
	if err := writeJSON(f.planPath, f.plan); err != nil {
		f.t.Fatal(err)
	}
	f.planSHA, _ = fieldSHA256(f.planPath)
	if err := writeJSON(f.uiPlanPath, f.uiPlan); err != nil {
		f.t.Fatal(err)
	}
}

// writeFile writes a file beside the plan (the plan artifact's directory).
func (f *mergeFixture) writeFile(name, text string) string {
	f.t.Helper()
	p := filepath.Join(f.dir, name)
	if err := os.WriteFile(p, []byte(text), 0o644); err != nil {
		f.t.Fatal(err)
	}
	return p
}

// mBinaries is what a part that ran the fixture package records.
func mBinaries() map[string]binaryRecord {
	return map[string]binaryRecord{mPkg: {SHA256: "planbin", ListingSHA256: mListing}}
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
	if integration.Binaries == nil {
		integration.Binaries = mBinaries()
	}
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
	f.writePart(rec, map[string]any{"integration-tests.json": ledger{Suite: "integration", Tests: tests, Problems: problems, Binaries: mBinaries()}})
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

func (f *mergeFixture) writeJobs(attempt int, jobs ...githubJob) {
	f.t.Helper()
	if f.jobsDir == "" {
		f.jobsDir = filepath.Join(f.dir, "jobs")
	}
	if err := os.MkdirAll(f.jobsDir, 0o755); err != nil {
		f.t.Fatal(err)
	}
	if err := writeJSON(filepath.Join(f.jobsDir, "attempt-"+itoa(attempt)+".json"), githubAttempt{Attempt: attempt, Jobs: jobs}); err != nil {
		f.t.Fatal(err)
	}
}

func (f *mergeFixture) merge(extra ...string) (proofRecord, int) {
	f.t.Helper()
	quiet(f.t)
	args := []string{"-plan", f.planPath, "-parts", f.parts, "-out", f.out, "-name", "test", "-host-speed", f.hostSpeed}
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
	f.deferB(nightlyLine("TestB")+"\n", nightlyLine("TestB")+"\n")
	f.writeLocal(ledger{Suite: "integration", Tests: []entry{passEntry("TestA"), passEntry("TestB")}})
	r, code := f.merge()
	if code != 1 || !strings.Contains(strings.Join(r.Ineligible, "\n"), "which the plan deferred") {
		t.Errorf("code %d, ineligible = %v", code, r.Ineligible)
	}
}

func TestMerge_DeferredTestsAreRecordedNotRun(t *testing.T) {
	f := newMergeFixture(t, tierFull, false)
	f.deferB(nightlyLine("TestB")+"\n", nightlyLine("TestB")+"\n")
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
			if e.Outcome != "deferred" || e.Reason != nightlyLine("TestB") {
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
	f.anchorHostSpeed(hostRow, hostRow)
	problem := "a.spec.ts › t [chromium] failed (" + timingDetail + ")"
	r, code := mergeUIShard(t, f, 2, "fail", uiShardLedger(outcomeFail, timingDetail, []string{problem}))
	if code != 0 || r.Result != "pass" || !r.MergeEligible {
		t.Fatalf("code %d, result %s eligible %v: %v", code, r.Result, r.MergeEligible, r.Ineligible)
	}
	if len(r.HostSpeedFailures) != 1 || r.HostSpeedFailures[0].RunnerCPUs != 2 || r.HostSpeedFailures[0].ReferenceCPUs != 16 ||
		r.HostSpeedFailures[0].Observed != 8.123 {
		t.Fatalf("host_speed_failures = %+v", r.HostSpeedFailures)
	}
	if len(r.KnownGaps) != 1 || !strings.Contains(r.KnownGaps[0], "8123") {
		t.Fatalf("known_gaps = %v", r.KnownGaps)
	}
	b, _ := os.ReadFile(filepath.Join(f.out, "README.md"))
	if !strings.Contains(string(b), "Known gaps: runner size") || !strings.Contains(string(b), "recorded, not merge-blocking") || !r.HostSpeedFailures[0].Excused {
		t.Errorf("README lacks the known-gaps section:\n%s", b)
	}
}

func TestMerge_AnUnlistedUISpecFailureStillFails(t *testing.T) {
	f := newMergeFixture(t, tierFull, true)
	other := "ui\te2e/other.spec.ts\tt\tlessThan 5000\t5\t16\tnot this one\n"
	f.anchorHostSpeed(other, other)
	r, code := mergeUIShard(t, f, 2, "fail", uiShardLedger(outcomeFail, timingDetail,
		[]string{"a.spec.ts › t [chromium] failed (" + timingDetail + ")"}))
	if code == 0 || r.Result != "fail" || r.MergeEligible {
		t.Fatalf("an unlisted UI failure passed: code %d, %+v", code, r.Ineligible)
	}
}

func (f *mergeFixture) writeLocalWith(mutate func(*partRecord), integration ledger) {
	f.t.Helper()
	if integration.Binaries == nil {
		integration.Binaries = mBinaries()
	}
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
	q := f.writeQuarantine(quarantineRow("2026-11-18"), quarantineRow("2026-11-18"))
	f.writeLocal(ledger{Suite: "integration", Tests: []entry{passEntry("TestA"), q}})
	r, code := f.merge()
	if code != 0 || r.Result != "pass" || !r.MergeEligible {
		t.Fatalf("code %d, result %s eligible %v: %v", code, r.Result, r.MergeEligible, r.Ineligible)
	}
	if len(r.Quarantined) != 1 || r.Quarantine.CheckedOn != "2026-10-21" {
		t.Fatalf("quarantined = %v, quarantine = %+v", r.Quarantined, r.Quarantine)
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
	if err := os.WriteFile(host, []byte("test\t"+mPkg+"\tTestA\t-\t60\t16\tthe stage completes within 60 s\n"), 0o644); err != nil {
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
	// The change adds TestB to the tree's list; the base's list is empty.
	f.deferB(nightlyLine("TestB")+"\n", "")
	f.writeLocal(ledger{Suite: "integration", Tests: []entry{passEntry("TestA")}})
	r, code := f.merge()
	if code != 1 || !strings.Contains(strings.Join(r.Ineligible, "\n"),
		"TestB was deferred, but the base's nightly list does not carry it") {
		t.Errorf("code %d, ineligible = %v", code, r.Ineligible)
	}
}

func TestMerge_ATamperedBaseListFailsTheProof(t *testing.T) {
	f := newMergeFixture(t, tierFull, false)
	f.deferB(nightlyLine("TestB")+"\n", nightlyLine("TestB")+"\n")
	// The plan artifact's copy is edited after the plan hashed it.
	f.writeFile("nightly-base.txt", nightlyLine("TestB")+"\n"+nightlyLine("TestA")+"\n")
	f.writeLocal(ledger{Suite: "integration", Tests: []entry{passEntry("TestA")}})
	r, code := f.merge()
	if code != 1 || !strings.Contains(strings.Join(r.Ineligible, "\n"), "base's nightly list in the plan artifact is not the one the plan hashed") {
		t.Errorf("code %d, ineligible = %v", code, r.Ineligible)
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

// nightlyLine is a well-formed nightly-list row deferring test.

func nightlyLine(test string) string {
	return mPkg + "\t" + test + "\tTestA\t2026-10-18 Daniel Santos\tsoak"
}

// deferB pins a defer-mode plan whose two hashed nightly lists both carry

// TestB, as writePlan writes one: TestB is deferred.

func (f *mergeFixture) deferB(treeText, baseText string) {
	f.t.Helper()
	treeFile := f.writeFile("nightly.txt", treeText)
	baseFile := f.writeFile("nightly-base.txt", baseText)
	tree, err := readNightlyList(treeFile)
	if err != nil {
		f.t.Fatal(err)
	}
	base, err := readNightlyList(baseFile)
	if err != nil {
		f.t.Fatal(err)
	}
	treeSHA, _ := fileSHA256(treeFile)
	baseSHA, _ := fileSHA256(baseFile)
	f.plan.Nightly = nightlyRef{File: treeFile, SHA256: treeSHA, Mode: nightlyDefer, Entries: entryLines(tree),
		BaseSHA256: baseSHA, BaseEntries: entryLines(base)}
	f.plan.Deferred = []deferredRef{{Package: mPkg, Test: "TestB", Entry: nightlyLine("TestB")}}
	f.save()
}

// The deferral is re-derived from the hashed files themselves: a plan whose

// embedded copy claims the base carries an entry its hashed file lacks does

// not defer it (item 4 of review round 2).

func TestMerge_DeferralIsDecidedFromTheHashedFilesNotThePlansCopy(t *testing.T) {
	f := newMergeFixture(t, tierFull, false)
	f.deferB(nightlyLine("TestB")+"\n", "")
	f.plan.Nightly.BaseEntries = []string{mPkg + "\tTestB"} // the plan's copy says otherwise
	f.save()
	f.writeLocal(ledger{Suite: "integration", Tests: []entry{passEntry("TestA")}})
	r, code := f.merge()
	joined := strings.Join(r.Ineligible, "\n")
	if code != 1 || !strings.Contains(joined, "TestB was deferred, but the base's nightly list does not carry it") ||
		!strings.Contains(joined, "the nightly entries the plan embeds are not the hashed files' entries") {
		t.Errorf("code %d, ineligible = %v", code, r.Ineligible)
	}
}

// A plan that defers and pinned no base hash cannot have its deferral

// re-derived: it fails closed.

func TestMerge_ADeferringPlanWithoutTheBaseHashFailsClosed(t *testing.T) {
	f := newMergeFixture(t, tierFull, false)
	f.deferB(nightlyLine("TestB")+"\n", nightlyLine("TestB")+"\n")
	f.plan.Nightly.BaseSHA256 = ""
	f.save()
	f.writeLocal(ledger{Suite: "integration", Tests: []entry{passEntry("TestA")}})
	r, code := f.merge()
	if code != 1 || !strings.Contains(strings.Join(r.Ineligible, "\n"), "recorded no hash of the tree's nightly list or of the base's copy") {
		t.Errorf("code %d, ineligible = %v", code, r.Ineligible)
	}
}

func TestMerge_ADeferredEntryOutsideThePlanFailsTheProof(t *testing.T) {
	cases := []struct {
		name  string
		setup func(f *mergeFixture) []entry
		want  string
	}{
		{"a shard writes a deferred entry", func(f *mergeFixture) []entry {
			d := passEntry("TestB")
			d.Outcome, d.Reason = "deferred", nightlyLine("TestB")
			return []entry{passEntry("TestA"), d}
		}, "only the merger writes deferred entries"},
		{"the plan defers outside defer mode", func(f *mergeFixture) []entry {
			f.plan.Deferred = []deferredRef{{Package: mPkg, Test: "TestB", Entry: nightlyLine("TestB")}}
			f.save()
			return []entry{passEntry("TestA")}
		}, "only defer leaves a test out"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			f := newMergeFixture(t, tierFull, false)
			tests := c.setup(f)
			f.writeLocal(ledger{Suite: "integration", Tests: tests})
			r, code := f.merge()
			if code != 1 || r.Result != "fail" || !strings.Contains(strings.Join(r.Ineligible, "\n"), c.want) {
				t.Errorf("code %d result %s, ineligible = %v", code, r.Result, r.Ineligible)
			}
		})
	}
}

func TestMerge_AnUnplannedEntryFailsTheProof(t *testing.T) {
	f := newMergeFixture(t, tierFull, true)
	f.writeCI()
	f.writeShard(1, []entry{passEntry("TestA"), passEntry("TestZ")}, nil, 1)
	f.writeShard(2, []entry{passEntry("TestB")}, nil, 1)
	r, code := f.merge()
	if code != 1 || r.Result != "fail" || r.MergeEligible {
		t.Fatalf("code %d result %s eligible %v", code, r.Result, r.MergeEligible)
	}
	if !strings.Contains(strings.Join(r.Ineligible, "\n"), "shard integration:1 reported example.com/m.TestZ, which the plan did not expect") {
		t.Errorf("ineligible = %v", r.Ineligible)
	}
}

func TestMerge_ShardProblemsAreCarriedVerbatim(t *testing.T) {
	const skip = "example.com/m.TestA/inner skipped; a skip is never a pass — fix the prerequisite, or allow it in scripts/proof-allowed-skips.txt with its reason"
	f := newMergeFixture(t, tierFull, true)
	f.writeCI()
	f.writeShard(1, []entry{passEntry("TestA")}, []string{skip}, 1)
	f.writeShard(2, []entry{passEntry("TestB")}, nil, 1)
	r, code := f.merge()
	if code != 1 || !containsString(r.Ineligible, "integration:1: "+skip) {
		t.Fatalf("code %d, the shard's problem is not carried verbatim: %v", code, r.Ineligible)
	}
	var l ledger
	if err := readJSON(filepath.Join(f.out, "integration-tests.json"), &l); err != nil {
		t.Fatal(err)
	}
	if !containsString(l.Problems, "integration:1: "+skip) {
		t.Errorf("the merged ledger lacks the shard's problem: %v", l.Problems)
	}
}

// Identity is an eligibility rule (AGENTS.md §11.2): a part measured on

// another tree, an unclean tree, or another base leaves the result alone and

// makes the proof ineligible.

func TestMerge_EveryPartMeasuredOneTreeAndOneBase(t *testing.T) {
	cases := []struct {
		name   string
		mutate func(*partRecord)
		want   string
	}{
		{"another tree", func(r *partRecord) { r.Tree, r.TreeEnd = "tree2", "tree2" }, "it measured tree tree2 → tree2, and the plan's tree is tree1"},
		{"a tree that moved", func(r *partRecord) { r.TreeEnd = "tree2" }, "it measured tree tree1 → tree2"},
		{"a dirty start", func(r *partRecord) { r.CleanStart = false }, "its tree was not clean (start false, end true)"},
		{"a dirty end", func(r *partRecord) { r.CleanEnd = false }, "its tree was not clean (start true, end false)"},
		{"another base", func(r *partRecord) { r.BaseCommit = "base2" }, "it diffed against base2, and the plan's base is base1"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			f := newMergeFixture(t, tierFull, true)
			f.writeCI()
			f.writeShard(1, []entry{passEntry("TestA")}, nil, 1)
			rec := f.basePart("integration:2")
			rec.Gates = []gateRecord{{Gate: "integration", Result: "pass"}}
			rec.Images = map[string]string{"activity-shell": "sha256:aaa"}
			c.mutate(&rec)
			f.writePart(rec, map[string]any{"integration-tests.json": ledger{Suite: "integration", Tests: []entry{passEntry("TestB")}, Binaries: mBinaries()}})
			r, code := f.merge()
			if code != 0 || r.Result != "pass" {
				t.Errorf("code %d result %s: identity is eligibility, not a gate result (%v)", code, r.Result, r.Ineligible)
			}
			if r.MergeEligible || !strings.Contains(strings.Join(r.Ineligible, "\n"), "part integration:2 (attempt 1): "+c.want) {
				t.Errorf("eligible %v, ineligible = %v", r.MergeEligible, r.Ineligible)
			}
		})
	}
}

// Shared Temporal and an outside image tag are ineligibility, never a FAIL

// (AGENTS.md §11.2): the tests may all have passed.

func TestMerge_IdentityProblemsAreIneligibleNotFailures(t *testing.T) {
	for name, mutate := range map[string]func(*partRecord){
		"shared Temporal":      func(r *partRecord) { r.Temporal = temporalRecord{Mode: "shared"} },
		"an outside image tag": func(r *partRecord) { r.ImageTag, r.ImageTagOwn = "borrowed", false },
	} {
		t.Run(name, func(t *testing.T) {
			f := newMergeFixture(t, tierFull, false)
			f.writeLocalWith(mutate, ledger{Suite: "integration", Tests: []entry{passEntry("TestA"), passEntry("TestB")}})
			r, code := f.merge()
			if code != 0 || r.Result != "pass" || r.MergeEligible {
				t.Errorf("code %d result %s eligible %v: %v", code, r.Result, r.MergeEligible, r.Ineligible)
			}
		})
	}
}

func TestMerge_ABinaryDigestMismatchIsANoteNotAFailure(t *testing.T) {
	f := newMergeFixture(t, tierFull, true)
	f.writeCI()
	rec := f.basePart("integration:1")
	rec.Gates = []gateRecord{{Gate: "integration", Result: "pass"}}
	rec.Images = map[string]string{"activity-shell": "sha256:aaa"}
	f.writePart(rec, map[string]any{"integration-tests.json": ledger{Suite: "integration", Tests: []entry{passEntry("TestA")},
		Binaries: map[string]binaryRecord{mPkg: {SHA256: "another-compile", ListingSHA256: mListing}}}})
	f.writeShard(2, []entry{passEntry("TestB")}, nil, 1)
	r, code := f.merge()
	if code != 0 || r.Result != "pass" || !r.MergeEligible {
		t.Fatalf("a binary digest mismatch with an equal listing failed the proof: code %d, %v", code, r.Ineligible)
	}
	if len(r.BinaryDigestNotes) != 1 || !strings.Contains(r.BinaryDigestNotes[0], "separate compiles need not agree") {
		t.Errorf("binary_digest_notes = %v", r.BinaryDigestNotes)
	}
	b, _ := os.ReadFile(filepath.Join(f.out, "README.md"))
	if !strings.Contains(string(b), "## Notes") || !strings.Contains(string(b), "another-comp") {
		t.Errorf("the README lacks the note:\n%s", b)
	}
}

// A part that ran a package's jobs and recorded no listing of its binary

// fails closed, the local part included: before, a local ledger carried no

// binaries and the listing check silently never ran.

func TestMerge_AShardWithoutItsListingFailsClosed(t *testing.T) {
	for _, ci := range []bool{false, true} {
		f := newMergeFixture(t, tierFull, ci)
		tests := []entry{passEntry("TestA"), passEntry("TestB")}
		if ci {
			f.writeCI()
			rec := f.basePart("integration:1")
			rec.Gates = []gateRecord{{Gate: "integration", Result: "pass"}}
			rec.Images = map[string]string{"activity-shell": "sha256:aaa"}
			f.writePart(rec, map[string]any{"integration-tests.json": ledger{Suite: "integration", Tests: tests[:1], Binaries: map[string]binaryRecord{}}})
			f.writeShard(2, tests[1:], nil, 1)
		} else {
			f.writeLocal(ledger{Suite: "integration", Tests: tests, Binaries: map[string]binaryRecord{}})
		}
		r, code := f.merge()
		if code != 1 || !strings.Contains(strings.Join(r.Ineligible, "\n"), "recorded no listing of "+mPkg) {
			t.Errorf("ci=%v: code %d, ineligible = %v", ci, code, r.Ineligible)
		}
	}
}

func TestMerge_AnUnpinnedAllowlistFailsClosed(t *testing.T) {
	f := newMergeFixture(t, tierFull, false)
	f.plan.AllowedSkips = fileRef{File: "scripts/proof-allowed-skips.txt"}
	f.save()
	f.writeLocal(ledger{Suite: "integration", Tests: []entry{passEntry("TestA"), passEntry("TestB")}})
	r, code := f.merge()
	if code != 1 || !strings.Contains(strings.Join(r.Ineligible, "\n"), "recorded no hash of its allowed-skips list") {
		t.Errorf("code %d, ineligible = %v", code, r.Ineligible)
	}
}

// The plan part's own problems (a stale nightly entry, say) reach the

// verdict in CI, where no shard ledger carries them.

func TestMerge_ThePlansOwnProblemsFailTheProof(t *testing.T) {
	f := newMergeFixture(t, tierFull, true)
	f.plan.Problems = []string{"nightly list line 3 names example.com/m.TestGone, which its package does not list: remove the stale entry"}
	f.save()
	f.writeCI()
	f.writeShard(1, []entry{passEntry("TestA")}, nil, 1)
	f.writeShard(2, []entry{passEntry("TestB")}, nil, 1)
	r, code := f.merge()
	if code != 1 || !containsString(r.Ineligible, "plan: "+f.plan.Problems[0]) {
		t.Errorf("code %d, ineligible = %v", code, r.Ineligible)
	}
}

func TestMerge_ThePullRequestRecordIsCarried(t *testing.T) {
	f := newMergeFixture(t, tierFull, true)
	f.plan.PR = &prRecord{Number: 12, HeadSHA: "head1", BaseSHA: "base1", ContainsBase: true}
	f.save()
	f.writeCI()
	f.writeShard(1, []entry{passEntry("TestA")}, nil, 1)
	f.writeShard(2, []entry{passEntry("TestB")}, nil, 1)
	r, code := f.merge()
	if code != 0 || r.PR == nil || !reflect.DeepEqual(*r.PR, *f.plan.PR) {
		t.Errorf("code %d, pr = %+v, want %+v", code, r.PR, f.plan.PR)
	}
}

func TestMerge_ListsChangedAreCarriedIntoProofJSON(t *testing.T) {
	f := newMergeFixture(t, tierFull, true)
	f.plan.ListsChanged = []listChange{
		{File: "scripts/proof-host-speed.tsv", Added: []string{"ui\te2e/x.spec.ts\tt\t5\t16\tnew"}},
		{File: "scripts/proof.sh"},
	}
	f.save()
	f.writeCI()
	f.writeShard(1, []entry{passEntry("TestA")}, nil, 1)
	f.writeShard(2, []entry{passEntry("TestB")}, nil, 1)
	r, code := f.merge()
	if code != 0 || !r.MergeEligible || !reflect.DeepEqual(r.ListsChanged, f.plan.ListsChanged) {
		t.Fatalf("code %d eligible %v, lists_changed = %+v", code, r.MergeEligible, r.ListsChanged)
	}
	b, _ := os.ReadFile(filepath.Join(f.out, "README.md"))
	if !strings.Contains(string(b), "added: `ui\te2e/x.spec.ts") {
		t.Errorf("the README does not itemise the added row:\n%s", b)
	}

	// A failure to compute them is recorded, and never reads as "none".
	g := newMergeFixture(t, tierFull, true)
	g.plan.ListsChanged = []listChange{{Error: "base \"main\" is not a commit"}}
	g.save()
	g.writeCI()
	g.writeShard(1, []entry{passEntry("TestA")}, nil, 1)
	g.writeShard(2, []entry{passEntry("TestB")}, nil, 1)
	r, _ = g.merge()
	if r.MergeEligible || !strings.Contains(strings.Join(r.Ineligible, "\n"), "the gate files this change edits could not be listed") {
		t.Errorf("an unlisted change is eligible: %v", r.Ineligible)
	}
	b, _ = os.ReadFile(filepath.Join(g.out, "README.md"))
	if !strings.Contains(string(b), "the gate files could not be listed") {
		t.Errorf("the README does not print the error:\n%s", b)
	}
}

func TestMerge_TheReleaseTierRequiresTheObservabilityPart(t *testing.T) {
	if p := partsExpected(tierRelease, false, 2, 1, false); !containsString(p, "observability") {
		t.Fatalf("a release plan's parts %v lack observability", p)
	}
	for _, withObs := range []bool{false, true} {
		f := newMergeFixture(t, tierRelease, true)
		f.plan.PartsExpected = partsExpected(tierRelease, false, 2, 1, false)
		f.save()
		f.writeCI()
		for _, a := range fixtureActivities[1:] {
			fix := f.basePart("activity-fixture:" + a)
			fix.Gates = []gateRecord{{Gate: "activity-fixture", Result: "pass"}}
			f.writePart(fix, nil)
		}
		f.writeShard(1, []entry{passEntry("TestA")}, nil, 1)
		f.writeShard(2, []entry{passEntry("TestB")}, nil, 1)
		if withObs {
			obs := f.basePart("observability")
			obs.Gates = []gateRecord{{Gate: "observability", Result: "pass"}}
			f.writePart(obs, nil)
		}
		r, code := f.merge()
		joined := strings.Join(r.Ineligible, "\n")
		if withObs {
			if code != 0 || r.Result != "pass" || !r.EligibleButForTier {
				t.Errorf("with the observability part: code %d result %s, %v", code, r.Result, r.Ineligible)
			}
			continue
		}
		if code != 1 || !strings.Contains(joined, "part observability left no result") || !strings.Contains(joined, "gate observability did not run") {
			t.Errorf("without the observability part: code %d, %v", code, r.Ineligible)
		}
	}
}

func TestMerge_ReRunningEveryJobIsIneligible(t *testing.T) {
	f := newMergeFixture(t, tierFull, true)
	f.writeCI()
	f.writeShard(1, []entry{passEntry("TestA")}, nil, 1)
	f.writeShard(2, []entry{passEntry("TestB")}, nil, 1)
	// "Re-run all jobs": every part runs again, and passes again.
	entries, _ := os.ReadDir(f.parts)
	for _, e := range entries {
		var rec partRecord
		dir := filepath.Join(f.parts, e.Name())
		if err := readJSON(filepath.Join(dir, "part.json"), &rec); err != nil {
			t.Fatal(err)
		}
		rec.Attempt = 2
		files := map[string]any{}
		for _, name := range []string{"integration-tests.json", "unit-tests.json", "ui-e2e-tests.json"} {
			var v any
			if readJSON(filepath.Join(dir, name), &v) == nil {
				files[name] = v
			}
		}
		f.writePart(rec, files)
	}
	var jobs1, jobs2 []githubJob
	for _, part := range f.plan.PartsExpected {
		jobs1 = append(jobs1, githubJob{Part: part, Conclusion: "success"})
		jobs2 = append(jobs2, githubJob{Part: part, Conclusion: "success"})
	}
	f.writeJobs(1, jobs1...)
	f.writeJobs(2, jobs2...)
	r, code := f.merge()
	if code != 0 || r.Result != "pass" {
		t.Fatalf("code %d result %s: every attempt passed (%v)", code, r.Result, r.Ineligible)
	}
	if r.MergeEligible || len(r.Reruns) != 0 {
		t.Fatalf("re-running every job is eligible (%v) or recorded as allowed reruns (%+v)", r.MergeEligible, r.Reruns)
	}
	for _, part := range f.plan.PartsExpected {
		if !strings.Contains(strings.Join(r.Ineligible, "\n"), "part "+part+" was run again (attempt 2) after attempt 1 recorded its result") {
			t.Errorf("no reason for part %s: %v", part, r.Ineligible)
		}
	}
}

// timingDetail is how buildUILedger records a Playwright
// toBeLessThanOrEqual failure: the message, its newlines folded.
const timingDetail = "Error: live run page; ; expect(received).toBeLessThanOrEqual(expected); ; Expected: <= 5000; Received:    8123"

// hostRow is a host-speed row for the fixture's UI test a.spec.ts › t.
const hostRow = "ui\te2e/a.spec.ts\tt\tlessThan 5000\t5\t16\tthe gated size is interactive within 5 s\n"

// anchorHostSpeed writes the tree's table and the base's copy beside the plan,

// and pins both in the plan, as the plan part does.

func (f *mergeFixture) anchorHostSpeed(tree, base string) {
	f.t.Helper()
	f.writeFile("host-speed.tsv", tree)
	f.writeFile("host-speed-base.txt", base)
	f.plan.HostSpeed.SHA256, _ = fileSHA256(f.hostSpeed)
	f.plan.HostSpeed.BaseSHA256, _ = fileSHA256(filepath.Join(f.dir, "host-speed-base.txt"))
	f.save()
}

// uiShardLedger is a shard ledger whose one test has the given outcome.

func uiShardLedger(outcome, detail string, problems []string) uiLedger {
	l := uiLedger{
		Suite: "ui-e2e", Listed: 1, GuardRetryCeiling: 3,
		Files:    []uiFileEntry{{File: "a.spec.ts", Shard: 1, Tests: 1, Seconds: 8.1, WallSeconds: 8.1}},
		Problems: problems,
	}
	if outcome != outcomeNone {
		l.Tests = []uiEntry{{File: "a.spec.ts", Title: "t", Project: "chromium", Outcome: outcome, Seconds: 8.1, Detail: detail}}
	} else {
		l.Tests = []uiEntry{{File: "a.spec.ts", Title: "t", Project: "chromium", Outcome: outcomeNone}}
	}
	return l
}

// mergeUIShard merges a CI proof whose one UI shard ran on cpus CPUs and left

// shard as its ledger, with gate as its ui-e2e gate result.

func mergeUIShard(t *testing.T, f *mergeFixture, cpus int, gate string, shard uiLedger) (proofRecord, int) {
	t.Helper()
	f.writeCI()
	f.writeShard(1, []entry{passEntry("TestA")}, nil, 1)
	f.writeShard(2, []entry{passEntry("TestB")}, nil, 1)
	ui := f.basePart("ui-e2e:1")
	ui.Gates = []gateRecord{{Gate: "ui-e2e", Result: gate}}
	ui.Runner = runnerRecord{OS: "linux", CPUs: cpus}
	f.writePart(ui, map[string]any{"ui-e2e-tests.json": shard})
	return f.merge()
}

// Only a failed outcome whose failure is the declared bound's own timing

// assertion, on a CI UI part below the reference, under a row the base

// carries identically, is a known gap. Everything else in a listed spec fails

// the proof, whether the shard reported it or excused it itself (R2-M1).

func TestMerge_OnlyTheBoundsOwnTimingMissIsAKnownGap(t *testing.T) {
	timingProblem := "a.spec.ts › t [chromium] failed (" + timingDetail + ")"
	otherDetail := "Error: expect(locator).toBeVisible() failed; ; Locator: getByRole('heading')"
	cases := []struct {
		name       string
		ci         bool
		cpus       int
		tree, base string
		shard      uiLedger
		gate       string
		want       string
	}{
		{name: "a non-timing failure", ci: true, cpus: 2, tree: hostRow, base: hostRow,
			shard: uiShardLedger(outcomeFail, otherDetail, []string{"a.spec.ts › t [chromium] failed (" + otherDetail + ")"}), gate: "fail",
			want: "toBeVisible"},
		{name: "a skip", ci: true, cpus: 2, tree: hostRow, base: hostRow,
			shard: uiShardLedger(outcomeSkip, "", []string{"a.spec.ts › t [chromium] skipped; a skip is never a pass"}), gate: "fail",
			want: "skipped; a skip is never a pass"},
		{name: "a missing outcome", ci: true, cpus: 2, tree: hostRow, base: hostRow,
			shard: uiShardLedger(outcomeNone, "", []string{"a.spec.ts › t [chromium] has no outcome"}), gate: "fail",
			want: "has no outcome"},
		{name: "a skip the shard excused itself", ci: true, cpus: 2, tree: hostRow, base: hostRow,
			shard: func() uiLedger {
				l := uiShardLedger(outcomeSkip, "", nil)
				p := "a.spec.ts › t [chromium] skipped; a skip is never a pass"
				l.HostSpeedFailures = []hostSpeedFailure{{Test: p, Kind: "ui", Message: p, Bound: 5, ReferenceCPUs: 16}}
				l.KnownGaps = []string{p}
				return l
			}(), gate: "pass", want: "the shard excused"},
		{name: "the timing miss on a local proof", ci: false, cpus: 2, tree: hostRow, base: hostRow,
			shard: uiShardLedger(outcomeFail, timingDetail, []string{timingProblem}), gate: "fail", want: timingProblem},
		{name: "a row the change adds", ci: true, cpus: 2, tree: hostRow, base: "",
			shard: uiShardLedger(outcomeFail, timingDetail, []string{timingProblem}), gate: "fail", want: timingProblem},
		{name: "a bound the change loosens", ci: true, cpus: 2,
			tree: strings.Replace(hostRow, "lessThan 5000\t5\t", "lessThan 9000\t9\t", 1), base: hostRow,
			shard: uiShardLedger(outcomeFail, strings.Replace(timingDetail, "5000", "9000", 1), []string{"x"}), gate: "fail", want: "x"},
		{name: "a runner at the reference", ci: true, cpus: 16, tree: hostRow, base: hostRow,
			shard: uiShardLedger(outcomeFail, timingDetail, []string{timingProblem}), gate: "fail", want: timingProblem},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			f := newMergeFixture(t, tierFull, c.ci)
			f.anchorHostSpeed(c.tree, c.base)
			var r proofRecord
			var code int
			if c.ci {
				r, code = mergeUIShard(t, f, c.cpus, c.gate, c.shard)
			} else {
				f.writeLocalWith(func(p *partRecord) {
					p.Runner.CPUs = c.cpus
					for i := range p.Gates {
						if p.Gates[i].Gate == "ui-e2e" {
							p.Gates[i].Result = c.gate
						}
					}
				}, ledger{Suite: "integration", Tests: []entry{passEntry("TestA"), passEntry("TestB")}})
				// writeLocalWith wrote the passing UI ledger; replace it.
				if err := writeJSON(filepath.Join(f.parts, "local-a1", "ui-e2e-tests.json"), c.shard); err != nil {
					t.Fatal(err)
				}
				r, code = f.merge()
			}
			if code == 0 || r.Result != "fail" || r.MergeEligible {
				t.Fatalf("code %d result %s eligible %v: the miss was excused (%v)", code, r.Result, r.MergeEligible, r.Ineligible)
			}
			if !strings.Contains(strings.Join(r.Ineligible, "\n"), c.want) {
				t.Errorf("ineligible = %v, want a line with %q", r.Ineligible, c.want)
			}
			if len(r.KnownGaps) != 0 {
				t.Errorf("known_gaps = %v, want none", r.KnownGaps)
			}
		})
	}
}

// writeQuarantine pins a quarantine list and the base's copy, both carrying

// text, and returns a quarantined-fail entry for TestB's failing leaf.

func (f *mergeFixture) writeQuarantine(tree, base string) entry {
	f.t.Helper()
	f.writeFile("quarantine.txt", tree)
	f.writeFile("quarantine-base.txt", base)
	f.plan.Quarantine.SHA256, _ = fileSHA256(filepath.Join(f.dir, "quarantine.txt"))
	f.plan.Quarantine.BaseSHA256, _ = fileSHA256(filepath.Join(f.dir, "quarantine-base.txt"))
	f.save()
	q := passEntry("TestB")
	q.Outcome = outcomeQuarantinedFail
	q.Failures = []string{"TestB/a_pin"}
	q.Quarantine = "Daniel Santos <danlsgiga@gmail.com>, review by 2026-11-18: known flake"
	return q
}

func quarantineRow(reviewBy string) string {
	return "integration\t" + mPkg + "\tTestB/a_pin\tDaniel Santos <danlsgiga@gmail.com>\t2026-10-19\t" + reviewBy + "\tknown flake\n"
}

// The merger re-checks every quarantined-fail a shard wrote, judged on the

// proof's start date (the plan's checked_on), never the merge's own date.

func TestMerge_AQuarantinedFailureIsReCheckedAgainstTheAnchor(t *testing.T) {
	cases := []struct {
		name, tree, base string
		tamper           bool
		want             string
	}{
		{"an entry the base lacks", quarantineRow("2026-11-18"), "", false, "no current entry the base carries"},
		{"an entry expired on the proof's date", quarantineRow("2026-10-20"), quarantineRow("2026-10-20"), false, "no current entry the base carries"},
		{"a list that is not the plan's", quarantineRow("2026-11-18"), quarantineRow("2026-11-18"), true, "is not the one the plan read"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			f := newMergeFixture(t, tierFull, false)
			q := f.writeQuarantine(c.tree, c.base)
			if c.tamper {
				f.writeFile("quarantine.txt", c.tree+quarantineRow("2026-11-01"))
			}
			f.writeLocal(ledger{Suite: "integration", Tests: []entry{passEntry("TestA"), q}})
			r, code := f.merge()
			if code != 1 || r.Result != "fail" || !strings.Contains(strings.Join(r.Ineligible, "\n"), c.want) {
				t.Errorf("code %d result %s, ineligible = %v", code, r.Result, r.Ineligible)
			}
		})
	}
	// Judged on the proof's date: an entry current on 2026-10-21 still
	// excuses, whatever day the merge runs.
	f := newMergeFixture(t, tierFull, false)
	q := f.writeQuarantine(quarantineRow("2026-10-21"), quarantineRow("2026-10-21"))
	f.writeLocal(ledger{Suite: "integration", Tests: []entry{passEntry("TestA"), q}})
	if r, code := f.merge(); code != 0 || !r.MergeEligible {
		t.Errorf("an entry current on the proof's date did not excuse: code %d, %v", code, r.Ineligible)
	}
}

func TestMerge_AQuarantineEntryWithinSevenDaysWarnsInTheREADME(t *testing.T) {
	f := newMergeFixture(t, tierFull, false)
	q := f.writeQuarantine(quarantineRow("2026-10-25"), quarantineRow("2026-10-25"))
	f.writeLocal(ledger{Suite: "integration", Tests: []entry{passEntry("TestA"), q}})
	r, code := f.merge()
	if code != 0 || !r.MergeEligible {
		t.Fatalf("a warning changed the verdict: code %d, %v", code, r.Ineligible)
	}
	want := "WARNING: quarantine entry for example.com/m.TestB/a_pin expires on 2026-10-25"
	if len(r.QuarantineWarnings) != 1 || !strings.HasPrefix(r.QuarantineWarnings[0], want) {
		t.Fatalf("quarantine_warnings = %v", r.QuarantineWarnings)
	}
	b, _ := os.ReadFile(filepath.Join(f.out, "README.md"))
	if !strings.Contains(string(b), "## Warnings") || !strings.Contains(string(b), want) {
		t.Errorf("the README lacks the warning:\n%s", b)
	}
}

// twoUIShards is a UI plan of two files on two shards.

func twoUIShards() uiPlan {
	p := uiPlan{Format: 1}
	p.Tests = []uiPlanTest{{File: "a.spec.ts", Title: "t", Project: "chromium"}, {File: "b.spec.ts", Title: "u", Project: "chromium"}}
	p.Files = []uiPlanFile{{File: "a.spec.ts", Tests: 1, Shard: 1}, {File: "b.spec.ts", Tests: 1, Shard: 2}}
	p.Shards = []uiShard{{Index: 1, Files: []string{"a.spec.ts"}}, {Index: 2, Files: []string{"b.spec.ts"}}}
	return p
}

func uiFileLedger(file, title string, retries int) uiLedger {
	return uiLedger{
		Suite: "ui-e2e", Listed: 1, Pass: 1, GuardRetries: retries, GuardRetryCeiling: 3, Problems: []string{},
		Tests: []uiEntry{{File: file, Title: title, Project: "chromium", Outcome: outcomePass, Seconds: 10}},
		Files: []uiFileEntry{{File: file, Tests: 1, Seconds: 10, WallSeconds: 12}},
	}
}

// mergeTwoUIShards merges a CI proof with two UI shards leaving a and b.

func mergeTwoUIShards(t *testing.T, a, b *uiLedger) (proofRecord, int, *mergeFixture) {
	t.Helper()
	f := newMergeFixture(t, tierFull, true)
	f.uiPlan = twoUIShards()
	f.plan.PartsExpected = append(f.plan.PartsExpected, "ui-e2e:2")
	f.save()
	f.writeCI()
	f.writeShard(1, []entry{passEntry("TestA")}, nil, 1)
	f.writeShard(2, []entry{passEntry("TestB")}, nil, 1)
	for i, l := range []*uiLedger{a, b} {
		rec := f.basePart("ui-e2e:" + itoa(i+1))
		rec.Gates = []gateRecord{{Gate: "ui-e2e", Result: "pass"}}
		files := map[string]any{}
		if l != nil {
			files["ui-e2e-tests.json"] = *l
		}
		f.writePart(rec, files)
	}
	r, code := f.merge()
	return r, code, f
}

func TestMerge_UIListingIsCoveredExactlyOnce(t *testing.T) {
	a, b := uiFileLedger("a.spec.ts", "t", 0), uiFileLedger("b.spec.ts", "u", 0)
	if r, code, _ := mergeTwoUIShards(t, &a, &b); code != 0 || !r.MergeEligible {
		t.Fatalf("every listed test once: code %d, %v", code, r.Ineligible)
	}
	empty := uiLedger{Suite: "ui-e2e", GuardRetryCeiling: 3, Problems: []string{}}
	r, code, f := mergeTwoUIShards(t, &a, &empty)
	var l uiLedger
	_ = readJSON(filepath.Join(f.out, "ui-e2e-tests.json"), &l)
	if code != 1 || !containsString(l.Problems, "b.spec.ts › u [chromium] has no outcome") {
		t.Errorf("a missing test: code %d, problems %v (%v)", code, l.Problems, r.Ineligible)
	}
	dup := uiFileLedger("a.spec.ts", "t", 0)
	dup.Tests = append(dup.Tests, b.Tests...)
	r, code, _ = mergeTwoUIShards(t, &a, &dup)
	if code != 1 || !strings.Contains(strings.Join(r.Ineligible, "\n"), "has outcomes from shards ui-e2e:1 and ui-e2e:2") {
		t.Errorf("a duplicate test: code %d, %v", code, r.Ineligible)
	}
}

func TestMerge_TheGuardCeilingAppliesPerHost(t *testing.T) {
	// Each runner is its own host: two shards at the ceiling are within it,
	// though together they repeated twice as many navigations.
	a, b := uiFileLedger("a.spec.ts", "t", 3), uiFileLedger("b.spec.ts", "u", 3)
	r, code, _ := mergeTwoUIShards(t, &a, &b)
	if code != 0 || !r.MergeEligible || r.GuardRetries == nil || *r.GuardRetries != 3 {
		t.Fatalf("two shards at the ceiling: code %d, retries %v, %v", code, r.GuardRetries, r.Ineligible)
	}
	over := uiFileLedger("b.spec.ts", "u", 4)
	r, _, _ = mergeTwoUIShards(t, &a, &over)
	if r.MergeEligible || !strings.Contains(strings.Join(r.Ineligible, "\n"), "network-change retries above the ceiling") {
		t.Errorf("one host above the ceiling is eligible: %v", r.Ineligible)
	}
}

func TestMerge_CarriesEachSpecFilesWall(t *testing.T) {
	a, b := uiFileLedger("a.spec.ts", "t", 0), uiFileLedger("b.spec.ts", "u", 0)
	_, code, f := mergeTwoUIShards(t, &a, &b)
	if code != 0 {
		t.Fatalf("code %d", code)
	}
	var l uiLedger
	if err := readJSON(filepath.Join(f.out, "ui-e2e-tests.json"), &l); err != nil {
		t.Fatal(err)
	}
	want := []uiFileEntry{
		{File: "a.spec.ts", Shard: 1, Tests: 1, Seconds: 10, WallSeconds: 12},
		{File: "b.spec.ts", Shard: 2, Tests: 1, Seconds: 10, WallSeconds: 12},
	}
	if !reflect.DeepEqual(l.Files, want) {
		t.Errorf("files = %+v, want %+v", l.Files, want)
	}
}

// visibleRow declares a.spec.ts › t's header-visibility timeout as its
// runner-size bound, as run-page-ceiling.spec.ts's row does.
const visibleRow = "ui\te2e/a.spec.ts\tt\tvisibleWithin 60000 getByTestId('run-status')\t5\t16\tthe header never drew within 60 s\n"

// The class-B miss of run-page-ceiling.spec.ts — its run page's header not
// visible within the spec's 60 s wait on a 2-vCPU runner — is a known gap when
// a row declares exactly that assertion, and nothing else is: another
// locator's visibility timeout in the same test, a skip, a local proof and a
// row the change adds all still fail the proof.
func TestMerge_ADeclaredVisibilityTimeoutIsAKnownGapAndNothingElse(t *testing.T) {
	header := visibleFailure("getByTestId('run-status')", "60000")
	headerProblem := "a.spec.ts › t [chromium] failed (" + header + ")"
	f := newMergeFixture(t, tierFull, true)
	f.anchorHostSpeed(hostRow+visibleRow, hostRow+visibleRow)
	r, code := mergeUIShard(t, f, 2, "fail", uiShardLedger(outcomeFail, header, []string{headerProblem}))
	if code != 0 || r.Result != "pass" || !r.MergeEligible {
		t.Fatalf("the declared visibility timeout failed the proof: code %d, %v", code, r.Ineligible)
	}
	if len(r.HostSpeedFailures) != 1 || !r.HostSpeedFailures[0].Excused ||
		r.HostSpeedFailures[0].Assertion != "visibleWithin 60000 getByTestId('run-status')" {
		t.Fatalf("host_speed_failures = %+v", r.HostSpeedFailures)
	}

	inspect := visibleFailure("getByTestId('task-t3999-inspect')", "60000")
	cases := []struct {
		name       string
		ci         bool
		tree, base string
		shard      uiLedger
	}{
		{"another locator's visibility timeout", true, hostRow + visibleRow, hostRow + visibleRow,
			uiShardLedger(outcomeFail, inspect, []string{"a.spec.ts › t [chromium] failed (" + inspect + ")"})},
		{"a skip", true, hostRow + visibleRow, hostRow + visibleRow,
			uiShardLedger(outcomeSkip, "", []string{"a.spec.ts › t [chromium] skipped; a skip is never a pass"})},
		{"the same timeout on a local proof", false, hostRow + visibleRow, hostRow + visibleRow,
			uiShardLedger(outcomeFail, header, []string{headerProblem})},
		{"a row the change adds", true, hostRow + visibleRow, hostRow,
			uiShardLedger(outcomeFail, header, []string{headerProblem})},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			f := newMergeFixture(t, tierFull, c.ci)
			f.anchorHostSpeed(c.tree, c.base)
			var r proofRecord
			var code int
			if c.ci {
				r, code = mergeUIShard(t, f, 2, "fail", c.shard)
			} else {
				f.writeLocalWith(func(p *partRecord) {
					p.Runner.CPUs = 2
					for i := range p.Gates {
						if p.Gates[i].Gate == "ui-e2e" {
							p.Gates[i].Result = "fail"
						}
					}
				}, ledger{Suite: "integration", Tests: []entry{passEntry("TestA"), passEntry("TestB")}})
				if err := writeJSON(filepath.Join(f.parts, "local-a1", "ui-e2e-tests.json"), c.shard); err != nil {
					t.Fatal(err)
				}
				r, code = f.merge()
			}
			if code == 0 || r.Result != "fail" || r.MergeEligible || len(r.KnownGaps) != 0 {
				t.Errorf("code %d result %s eligible %v gaps %v: it was excused", code, r.Result, r.MergeEligible, r.KnownGaps)
			}
		})
	}
}
