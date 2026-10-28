package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// The runner-health floor (AGENTS.md §11.2): a part whose runner measured
// below the floor is runner-degraded, exactly as a part whose runner GitHub
// lost — its tests get no outcome of the tree, the proof is not a proof of
// the tree, and that one job may run again. A part above the floor whose
// tests fail stays a test failure, and a part with no health file keeps its
// meaning. The fixtures are testdata/runnerhealth.

// writeHealth copies a fixture into a part attempt's directory as its
// runner-health.json.
func (f *mergeFixture) writeHealth(dirName, fixture string) {
	f.t.Helper()
	b, err := os.ReadFile(filepath.Join(healthFixtures, fixture))
	if err != nil {
		f.t.Fatal(err)
	}
	dir := filepath.Join(f.parts, dirName)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		f.t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, runnerHealthFile), b, 0o644); err != nil {
		f.t.Fatal(err)
	}
}

// integrationOutcome is the merged integration ledger's outcome of test.
func (f *mergeFixture) integrationOutcome(test string) (string, string) {
	f.t.Helper()
	var l ledger
	if err := readJSON(filepath.Join(f.out, "integration-tests.json"), &l); err != nil {
		f.t.Fatal(err)
	}
	for _, e := range l.Tests {
		if e.Test == test {
			return e.Outcome, e.Reason
		}
	}
	return "", ""
}

func TestMerge_APartBelowTheFloorIsRunnerDegraded(t *testing.T) {
	f := newMergeFixture(t, tierFull, true)
	f.writeCI()
	f.writeShard(1, []entry{passEntry("TestA")}, nil, 1)
	// Its tests passed, on a runner that measured below the floor: nothing it
	// recorded is an outcome of the tree.
	f.writeShard(2, []entry{passEntry("TestB")}, nil, 1)
	f.writeHealth("integration-2-a1", "below-floor.json")
	f.writeJobs(1, githubJob{Name: "integration (2)", Conclusion: "failure", FailedStep: "integration shard"})
	r, code := f.merge()
	if code == 0 || r.Result != "fail" || r.MergeEligible {
		t.Fatalf("code %d result %s eligible %v: a degraded part made a proof", code, r.Result, r.MergeEligible)
	}
	reasons := strings.Join(r.Ineligible, "\n")
	want := "part integration:2 ran on a degraded runner (attempt 1, its runner measured below the floor: disk_write_mb_s 12.3 MB/s below the floor 40; fsync_p95_ms 184.2 ms above the floor 60): its tests have no outcome of the tree, and this is not a proof of the tree; re-run that job alone, within the plan and build artifacts' retention"
	if !strings.Contains(reasons, want) {
		t.Errorf("ineligible = %v\nwant %q", r.Ineligible, want)
	}
	if outcome, reason := f.integrationOutcome("TestB"); outcome != outcomeNone || !strings.Contains(reason, "no shard reported it") {
		t.Errorf("TestB = %s (%s), want no outcome of the tree", outcome, reason)
	}
	if outcome, _ := f.integrationOutcome("TestA"); outcome != outcomePass {
		t.Errorf("TestA = %s: the healthy shard's outcome was lost", outcome)
	}
	if len(r.DegradedRunners) != 2 {
		t.Fatalf("degraded_runners = %+v, want the two metrics below the floor", r.DegradedRunners)
	}
	d := r.DegradedRunners[0]
	if d.Part != "integration:2" || d.Attempt != 1 || d.Class != "github-hosted/ubuntu24/4cpu" || d.Metric != "disk_write_mb_s" || d.Unit != "MB/s" || d.Value != 12.3 || d.Floor != 40 {
		t.Errorf("degraded_runners[0] = %+v", d)
	}
	if d := r.DegradedRunners[1]; d.Metric != "fsync_p95_ms" || d.Value != 184.2 || d.Floor != 60 {
		t.Errorf("degraded_runners[1] = %+v", d)
	}
	if len(r.Reruns) != 0 {
		t.Errorf("a degraded part with no later attempt was recorded as a rerun: %+v", r.Reruns)
	}
	var health *partHealth
	for i := range r.RunnerHealth {
		if r.RunnerHealth[i].Part == "integration:2" {
			health = &r.RunnerHealth[i]
		}
	}
	if health == nil || health.Verdict != healthBelowFloor || health.Attempt != 1 || len(health.Metrics) != 6 {
		t.Errorf("runner_health for integration:2 = %+v", health)
	}
	readme, _ := os.ReadFile(filepath.Join(f.out, "README.md"))
	if !strings.Contains(string(readme), "## Degraded runners") || !strings.Contains(string(readme), "- integration:2 attempt 1 (github-hosted/ubuntu24/4cpu): disk_write_mb_s 12.3 MB/s, floor 40") {
		t.Errorf("the README does not list the degraded runner:\n%s", readme)
	}
	// verify names it, as the remedy is that one job again.
	if p := degradedProblems(r); len(p) != 1 || !strings.Contains(p[0], "part integration:2 ran on a degraded runner (attempt 1, github-hosted/ubuntu24/4cpu: disk_write_mb_s 12.3 MB/s, floor 40; fsync_p95_ms 184.2 ms, floor 60): it is not a proof of the tree; re-run that job alone") {
		t.Errorf("degradedProblems = %v", p)
	}
}

func TestMerge_APartAboveTheFloorWhoseTestsFailIsATestFailure(t *testing.T) {
	f := newMergeFixture(t, tierFull, true)
	f.writeCI()
	f.writeShard(1, []entry{passEntry("TestA")}, nil, 1)
	failed := entry{Package: mPkg, Test: "TestB", Outcome: outcomeFail, Seconds: 2402, Job: "m-002"}
	f.writeShard(2, []entry{failed}, []string{"example.com/m.TestB failed"}, 1)
	f.writeHealth("integration-2-a1", "above-floor.json")
	f.writeJobs(1, githubJob{Name: "integration (2)", Conclusion: "failure", FailedStep: "integration shard"})
	r, code := f.merge()
	if code == 0 || r.Result != "fail" {
		t.Fatalf("code %d result %s: a failing test on a healthy runner passed", code, r.Result)
	}
	if len(r.DegradedRunners) != 0 || len(r.Reruns) != 0 {
		t.Errorf("a healthy runner was degraded: %+v %+v", r.DegradedRunners, r.Reruns)
	}
	if outcome, _ := f.integrationOutcome("TestB"); outcome != outcomeFail {
		t.Errorf("TestB = %s, want its failure kept", outcome)
	}
	reasons := strings.Join(r.Ineligible, "\n")
	if !strings.Contains(reasons, "TestB") || strings.Contains(reasons, "degraded runner") {
		t.Errorf("ineligible = %v", r.Ineligible)
	}
	if len(r.RunnerHealth) != 1 || r.RunnerHealth[0].Part != "integration:2" || r.RunnerHealth[0].Verdict != healthAboveFloor {
		t.Errorf("runner_health = %+v", r.RunnerHealth)
	}
	if p := degradedProblems(r); len(p) != 0 {
		t.Errorf("degradedProblems = %v", p)
	}
}

func TestMerge_APartWithNoHealthFileIsAboveTheFloor(t *testing.T) {
	// Every part of the fixture predates the step: the proof keeps its meaning.
	f := newMergeFixture(t, tierFull, true)
	f.writeCI()
	f.writeShard(1, []entry{passEntry("TestA")}, nil, 1)
	f.writeShard(2, []entry{passEntry("TestB")}, nil, 1)
	f.writeJobs(1)
	r, code := f.merge()
	if code != 0 || r.Result != "pass" || !r.MergeEligible {
		t.Fatalf("code %d result %s eligible %v: %v", code, r.Result, r.MergeEligible, r.Ineligible)
	}
	if len(r.RunnerHealth) != 0 || len(r.DegradedRunners) != 0 {
		t.Errorf("parts with no health file recorded health: %+v %+v", r.RunnerHealth, r.DegradedRunners)
	}
}

func TestMerge_ADegradedAttemptMayRunAgain(t *testing.T) {
	f := newMergeFixture(t, tierFull, true)
	f.writeCI()
	f.writeShard(1, []entry{passEntry("TestA")}, nil, 1)
	// Attempt 1 ran to the end on a degraded runner and even passed; attempt
	// 2, on a healthy one, is the result.
	f.writeShard(2, []entry{passEntry("TestB")}, nil, 1)
	f.writeHealth("integration-2-a1", "below-floor.json")
	f.writeShard(2, []entry{passEntry("TestB")}, nil, 2)
	f.writeHealth("integration-2-a2", "above-floor.json")
	f.writeJobs(1, githubJob{Name: "integration (2)", Conclusion: "failure", FailedStep: "integration shard"})
	f.writeJobs(2, githubJob{Name: "integration (2)", Conclusion: "success"})
	r, code := f.merge()
	if code != 0 || r.Result != "pass" || !r.MergeEligible {
		t.Fatalf("code %d result %s eligible %v: %v", code, r.Result, r.MergeEligible, r.Ineligible)
	}
	if len(r.Reruns) != 1 || r.Reruns[0].Reason != "runner-degraded" || r.Reruns[0].Attempt != 2 || len(r.Reruns[0].EarlierAttempts) != 1 || r.Reruns[0].EarlierAttempts[0] != 1 {
		t.Fatalf("reruns = %+v", r.Reruns)
	}
	if d := r.Reruns[0].Degraded; len(d) != 2 || d[0].Metric != "disk_write_mb_s" || d[0].Attempt != 1 {
		t.Errorf("rerun degraded = %+v", d)
	}
	if len(r.DegradedRunners) != 2 || r.DegradedRunners[0].Attempt != 1 {
		t.Errorf("degraded_runners = %+v", r.DegradedRunners)
	}
	if len(r.RunnerHealth) != 2 || r.RunnerHealth[0].Attempt != 1 || r.RunnerHealth[0].Verdict != healthBelowFloor || r.RunnerHealth[1].Attempt != 2 || r.RunnerHealth[1].Verdict != healthAboveFloor {
		t.Errorf("runner_health = %+v", r.RunnerHealth)
	}
	readme, _ := os.ReadFile(filepath.Join(f.out, "README.md"))
	if !strings.Contains(string(readme), "- integration:2 attempt 2, after attempt [1] ran on a degraded runner (disk_write_mb_s 12.3 MB/s, floor 40; fsync_p95_ms 184.2 ms, floor 60); nothing that attempt recorded is an outcome of the tree") {
		t.Errorf("the README does not explain the rerun:\n%s", readme)
	}
	// A degraded attempt a later attempt replaced is no problem for verify.
	if p := degradedProblems(r); len(p) != 0 {
		t.Errorf("degradedProblems = %v", p)
	}
	// The same shape, with attempt 1 leaving no part.json at all (it died
	// after measuring): still a degraded rerun, never "not rerun-eligible".
	g := newMergeFixture(t, tierFull, true)
	g.writeCI()
	g.writeShard(1, []entry{passEntry("TestA")}, nil, 1)
	g.writeHealth("integration-2-a1", "below-floor.json")
	g.writeShard(2, []entry{passEntry("TestB")}, nil, 2)
	g.writeJobs(1, githubJob{Name: "integration (2)", Conclusion: "failure", FailedStep: "integration shard"})
	r, code = g.merge()
	if code != 0 || !r.MergeEligible || len(r.Reruns) != 1 || r.Reruns[0].Reason != "runner-degraded" {
		t.Errorf("code %d eligible %v reruns %+v: %v", code, r.MergeEligible, r.Reruns, r.Ineligible)
	}
}

func TestMerge_ARerunOnADegradedRunnerIsStillDegraded(t *testing.T) {
	// Attempt 1 was lost; attempt 2 ran on a degraded runner: the rerun was
	// allowed, and the proof is still not a proof of the tree.
	f := newMergeFixture(t, tierFull, true)
	f.writeCI()
	f.writeShard(1, []entry{passEntry("TestA")}, nil, 1)
	if err := os.MkdirAll(filepath.Join(f.parts, "integration-2-a1"), 0o755); err != nil {
		t.Fatal(err)
	}
	f.writeShard(2, []entry{passEntry("TestB")}, nil, 2)
	f.writeHealth("integration-2-a2", "below-floor.json")
	f.writeJobs(1, githubJob{Name: "integration (2)", Conclusion: "failure", Annotations: []string{runnerLost}})
	r, code := f.merge()
	if code == 0 || r.Result != "fail" || r.MergeEligible {
		t.Fatalf("code %d result %s eligible %v", code, r.Result, r.MergeEligible)
	}
	if !strings.Contains(strings.Join(r.Ineligible, "\n"), "part integration:2 ran on a degraded runner (attempt 2,") {
		t.Errorf("ineligible = %v", r.Ineligible)
	}
	if len(r.Reruns) != 0 {
		t.Errorf("reruns = %+v: a degraded last attempt is no result to record a rerun against", r.Reruns)
	}
}

func TestMerge_TheMergerReDerivesTheHealthVerdict(t *testing.T) {
	// The file says above-floor and below_floor false; its disk write value
	// is under its floor. The merger believes the numbers.
	f := newMergeFixture(t, tierFull, true)
	f.writeCI()
	f.writeShard(1, []entry{passEntry("TestA")}, nil, 1)
	f.writeShard(2, []entry{passEntry("TestB")}, nil, 1)
	f.writeHealth("integration-2-a1", "lying-verdict.json")
	f.writeJobs(1)
	r, _ := f.merge()
	if r.MergeEligible || len(r.DegradedRunners) != 1 || r.DegradedRunners[0].Metric != "disk_write_mb_s" {
		t.Errorf("eligible %v degraded %+v: %v", r.MergeEligible, r.DegradedRunners, r.Ineligible)
	}
}

// The floor binds the CI stack parts alone. A local proof's measurement, and
// the plan, build and fast parts', is recorded and changes no verdict: the
// host's floors are its measured costs themselves.
func TestMerge_TheFloorDoesNotBindALocalOrAFastPart(t *testing.T) {
	t.Run("local", func(t *testing.T) {
		f := newMergeFixture(t, tierFull, false)
		f.writeLocal(ledger{Suite: "integration", Tests: []entry{passEntry("TestA"), passEntry("TestB")}})
		f.writeHealth("local-a1", "below-floor.json")
		r, code := f.merge()
		if code != 0 || r.Result != "pass" || !r.MergeEligible {
			t.Fatalf("code %d result %s eligible %v: %v", code, r.Result, r.MergeEligible, r.Ineligible)
		}
		if len(r.DegradedRunners) != 0 || len(r.Reruns) != 0 {
			t.Errorf("a local part was degraded: %+v %+v", r.DegradedRunners, r.Reruns)
		}
		if len(r.RunnerHealth) != 1 || r.RunnerHealth[0].Part != "local" || r.RunnerHealth[0].Verdict != healthBelowFloor {
			t.Errorf("runner_health = %+v, want the local measurement recorded with its verdict", r.RunnerHealth)
		}
		readme, _ := os.ReadFile(filepath.Join(f.out, "README.md"))
		if !strings.Contains(string(readme), "part local attempt 1 measured below the floor (disk_write_mb_s 12.3 MB/s below the floor 40; fsync_p95_ms 184.2 ms above the floor 60); the floor does not bind this part, so it is recorded and nothing changes") {
			t.Errorf("the README does not note the local measurement:\n%s", readme)
		}
	})
	t.Run("fast", func(t *testing.T) {
		f := newMergeFixture(t, tierFull, true)
		f.writeCI()
		f.writeShard(1, []entry{passEntry("TestA")}, nil, 1)
		f.writeShard(2, []entry{passEntry("TestB")}, nil, 1)
		f.writeHealth("fast-a1", "below-floor.json")
		f.writeJobs(1)
		r, code := f.merge()
		if code != 0 || !r.MergeEligible || len(r.DegradedRunners) != 0 {
			t.Errorf("code %d eligible %v degraded %+v: %v", code, r.MergeEligible, r.DegradedRunners, r.Ineligible)
		}
		if len(r.RunnerHealth) != 1 || r.RunnerHealth[0].Part != "fast" || r.RunnerHealth[0].Verdict != healthBelowFloor {
			t.Errorf("runner_health = %+v", r.RunnerHealth)
		}
	})
}

func TestFloorBinds(t *testing.T) {
	for part, want := range map[string]bool{
		"integration:1": true, "ui-e2e:2": true, "activity-fixture:shell": true, "observability": true,
		"plan": false, "build": false, "fast": false, "local": false,
	} {
		if got := floorBinds(part); got != want {
			t.Errorf("floorBinds(%s) = %v, want %v", part, got, want)
		}
	}
}

func TestMerge_AHealthFileMeasuredAgainstOtherFloorsIsRefused(t *testing.T) {
	f := newMergeFixture(t, tierFull, true)
	f.writeCI()
	f.writeShard(1, []entry{passEntry("TestA")}, nil, 1)
	f.writeShard(2, []entry{passEntry("TestB")}, nil, 1)
	h := readHealthFixture(t, "above-floor.json")
	h.FloorsFile, h.FloorsSHA256 = "scripts/proof-runner-floors.tsv", strings.Repeat("ab", 32)
	if err := writeJSON(filepath.Join(f.parts, "integration-2-a1", runnerHealthFile), h); err != nil {
		t.Fatal(err)
	}
	f.writeJobs(1)
	floors := filepath.Join(healthFixtures, "floors.tsv")
	r, _ := f.merge("-runner-floors", floors)
	if r.MergeEligible || !strings.Contains(strings.Join(r.Ineligible, "\n"), "part integration:2 attempt 1 measured against runner-health floors that are not the tree's (scripts/proof-runner-floors.tsv; its sha256 abababababab, the tree's ") {
		t.Errorf("eligible %v: %v", r.MergeEligible, r.Ineligible)
	}
	// The tree's own hash is accepted, and a record naming no table (an
	// unfloored class) is not held to one.
	h.FloorsSHA256, _ = fileSHA256(floors)
	if err := writeJSON(filepath.Join(f.parts, "integration-2-a1", runnerHealthFile), h); err != nil {
		t.Fatal(err)
	}
	if r, code := f.merge("-runner-floors", floors); code != 0 || !r.MergeEligible {
		t.Errorf("the tree's own floors were refused: %v", r.Ineligible)
	}
	// A floors table the merge cannot read fails the proof, never reads as
	// "no floor".
	if r, _ := f.merge("-runner-floors", filepath.Join(healthFixtures, "below-floor.json")); r.MergeEligible || !strings.Contains(strings.Join(r.Ineligible, "\n"), "the runner-health floors cannot be read") {
		t.Errorf("an unreadable floors table: %v", r.Ineligible)
	}
}

func TestMerge_ACorruptHealthFileFailsClosed(t *testing.T) {
	f := newMergeFixture(t, tierFull, true)
	f.writeCI()
	f.writeShard(1, []entry{passEntry("TestA")}, nil, 1)
	f.writeShard(2, []entry{passEntry("TestB")}, nil, 1)
	if err := os.WriteFile(filepath.Join(f.parts, "integration-2-a1", runnerHealthFile), []byte("{not json"), 0o644); err != nil {
		t.Fatal(err)
	}
	f.writeJobs(1)
	r, code := f.merge()
	if code == 0 || r.MergeEligible || !strings.Contains(strings.Join(r.Ineligible, "\n"), "part integration:2 attempt 1 left a runner-health.json that cannot be read: it fails closed") {
		t.Errorf("code %d eligible %v: %v", code, r.MergeEligible, r.Ineligible)
	}
	if len(r.DegradedRunners) != 0 {
		t.Errorf("a corrupt file was read as degraded: %+v", r.DegradedRunners)
	}
}

// The record's copy of the health file stands in when the file is not beside
// the part (a local proof keeps runner-health.json in the proof directory and
// hands record -runner-health its path).
func TestMerge_TheRecordsHealthCopyStandsInForTheFile(t *testing.T) {
	f := newMergeFixture(t, tierFull, true)
	f.writeCI()
	f.writeShard(1, []entry{passEntry("TestA")}, nil, 1)
	rec := f.basePart("integration:2")
	rec.Gates = []gateRecord{{Gate: "integration", Result: "pass"}}
	rec.Health = readHealthFixture(t, "below-floor.json")
	f.writePart(rec, map[string]any{"integration-tests.json": ledger{Suite: "integration", Tests: []entry{passEntry("TestB")}, Binaries: mBinaries()}})
	f.writeJobs(1)
	r, _ := f.merge()
	if r.MergeEligible || len(r.DegradedRunners) != 2 {
		t.Errorf("eligible %v degraded %+v: %v", r.MergeEligible, r.DegradedRunners, r.Ineligible)
	}
}

// A nightly proof with a degraded part is a runner failure, not a failing
// main — and not a passing nightly either: merge-check names the part.
func TestVerify_ADegradedNightlyPartIsNamed(t *testing.T) {
	dir := nightlyProof(t, func(r *proofRecord) {
		r.DegradedRunners = []degradedRunner{{Part: "integration:2", Attempt: 1, Class: "github-hosted/ubuntu24/4cpu", Metric: "disk_write_mb_s", Unit: "MB/s", Value: 12.3, Floor: 40}}
		r.Ineligible = append(r.Ineligible, "part integration:2 ran on a degraded runner")
		r.Gates[0].Result = "fail"
	})
	p := strings.Join(nightlyProofProblems(dir, "main-tree"), "\n")
	if !strings.Contains(p, "the nightly proof: part integration:2 ran on a degraded runner (attempt 1, github-hosted/ubuntu24/4cpu: disk_write_mb_s 12.3 MB/s, floor 40): it is not a proof of the tree; re-run that job alone") {
		t.Errorf("problems = %q", p)
	}
}

func TestDegradedProblems_GroupsOneAttemptsMetrics(t *testing.T) {
	r := proofRecord{DegradedRunners: []degradedRunner{
		{Part: "ui-e2e:3", Attempt: 2, Class: "c", Metric: "cpu_single_mb_s", Unit: "MB/s", Value: 100, Floor: 300},
		{Part: "ui-e2e:3", Attempt: 2, Class: "c", Metric: "mem_available_mib", Unit: "MiB", Value: 900, Floor: 4096},
		{Part: "integration:1", Attempt: 1, Class: "c", Metric: "fsync_p95_ms", Unit: "ms", Value: 90, Floor: 60},
	}, Reruns: []rerunRecord{{Part: "integration:1", Attempt: 2, EarlierAttempts: []int{1}, Reason: "runner-degraded"}}}
	p := degradedProblems(r)
	if len(p) != 1 || p[0] != "part ui-e2e:3 ran on a degraded runner (attempt 2, c: cpu_single_mb_s 100 MB/s, floor 300; mem_available_mib 900 MiB, floor 4096): it is not a proof of the tree; re-run that job alone" {
		t.Errorf("degradedProblems = %q", p)
	}
}
