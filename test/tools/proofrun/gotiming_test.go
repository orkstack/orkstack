package main

import (
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/orkstack/orkstack/internal/testsupport/proof"
)

// goRow is a host-speed row for the fixture's Go test TestA: its live GetRun
// p95 is bounded at 2 s on a 16-CPU reference.
const goRow = "test\t" + mPkg + "\tTestA\ttiming getrun-live-p95\t2\t16\tthe live GetRun p95 holds 2 s\n"

// missTail is the failure output of a test whose only failure is one timing
// miss: the log lines it printed, then the helper's line as testing prints it.
func missTail(m ...proof.TimingMiss) []string {
	out := []string{"    soak_test.go:90: E4 4000-reduced: 2 generations", "    timing.go:231: proof timing/TestA.json:"}
	for _, x := range m {
		out = append(out, "    timing.go:238: "+proof.MissLine(x))
	}
	return out
}

var liveMiss = proof.TimingMiss{ID: "getrun-live-p95", Measured: 2.014, Bound: 2, Label: "reduced-fan-out"}

func recordOf(test string, checks ...proof.TimingCheck) proof.TimingRecord {
	return proof.TimingRecord{Format: proof.TimingFormat, Package: mPkg, Test: test, Asserted: true, Checks: checks}
}

func checkOf(m proof.TimingMiss) proof.TimingCheck {
	return proof.TimingCheck{ID: m.ID, Label: m.Label, Measured: m.Measured, Bound: m.Bound, Miss: m.Measured > m.Bound}
}

// failedA is TestA as a shard ledger records a plain timing failure.
func failedA(output []string) entry {
	e := passEntry("TestA")
	e.Outcome = outcomeFail
	e.Job = "m-001"
	e.Seconds = 150
	e.FailureOutput = output
	return e
}

// putTiming writes rec as the part's timing record for its test, under the
// part's artifacts, where the merger reads it.
func (f *mergeFixture) putTiming(part string, rec proof.TimingRecord) {
	f.t.Helper()
	dir := filepath.Join(f.parts, strings.NewReplacer(":", "-").Replace(part)+"-a1", "artifacts", proof.TimingGroup)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		f.t.Fatal(err)
	}
	b, _ := json.Marshal(rec)
	if err := os.WriteFile(filepath.Join(dir, proof.TimingFile(rec.Test)), b, 0o644); err != nil {
		f.t.Fatal(err)
	}
}

// goShard is one CI integration shard (1) on cpus CPUs whose ledger is tests
// with problems and whose gate is gate; the other shard passes.
func (f *mergeFixture) goShard(cpus int, gate string, tests []entry, problems []string) {
	f.t.Helper()
	f.writeCI()
	f.writeShard(2, []entry{passEntry("TestB")}, nil, 1)
	rec := f.basePart("integration:1")
	rec.Gates = []gateRecord{{Gate: "integration", Result: gate}}
	rec.Images = map[string]string{"activity-shell": "sha256:aaa"}
	rec.Runner = runnerRecord{OS: "linux", CPUs: cpus}
	f.writePart(rec, map[string]any{
		"integration-tests.json": ledger{Suite: "integration", Tests: tests, Problems: problems, Binaries: mBinaries()},
	})
}

const timingProblemA = mPkg + ".TestA failed (job m-001)"

func TestMerge_AGoTimingMissOnASmallerCIRunnerIsAKnownGap(t *testing.T) {
	f := newMergeFixture(t, tierFull, true)
	f.anchorHostSpeed(goRow, goRow)
	f.goShard(4, "fail", []entry{failedA(missTail(liveMiss))}, []string{timingProblemA})
	f.putTiming("integration:1", recordOf("TestA", checkOf(liveMiss)))
	r, code := f.merge()
	if code != 0 || r.Result != "pass" || !r.MergeEligible {
		t.Fatalf("code %d, result %s eligible %v: %v", code, r.Result, r.MergeEligible, r.Ineligible)
	}
	if len(r.HostSpeedFailures) != 1 {
		t.Fatalf("host_speed_failures = %+v", r.HostSpeedFailures)
	}
	g := r.HostSpeedFailures[0]
	if !g.Excused || g.Part != "integration:1" || g.Kind != "integration" || g.Test != mPkg+".TestA" || g.Assertion != "timing getrun-live-p95" ||
		g.Bound != 2 || g.Observed != 2.014 || g.RunnerCPUs != 4 || g.ReferenceCPUs != 16 {
		t.Errorf("host_speed_failures[0] = %+v", g)
	}
	if len(r.KnownGaps) != 1 || !strings.Contains(r.KnownGaps[0], "getrun-live-p95 measured 2.014 s over its 2 s bound on a 4-CPU runner") {
		t.Errorf("known_gaps = %v", r.KnownGaps)
	}
	readme, _ := os.ReadFile(filepath.Join(f.out, "README.md"))
	if !strings.Contains(string(readme), "Known gaps: runner size") || !strings.Contains(string(readme), "recorded, not merge-blocking") {
		t.Errorf("README lacks the known-gaps section:\n%s", readme)
	}
	// The failure stays what it was in the ledger: excused, never turned into a pass.
	var merged ledger
	if err := readJSON(filepath.Join(f.out, "integration-tests.json"), &merged); err != nil {
		t.Fatal(err)
	}
	for _, e := range merged.Tests {
		if e.Test == "TestA" && e.Outcome != outcomeFail {
			t.Errorf("TestA is recorded %q in the merged ledger: an excused miss is still a failure", e.Outcome)
		}
	}
}

// Everything short of the whole rule is the failure it always was: each case
// breaks exactly one condition of gotiming.go, and a positive control (the
// test above) passes with the others in place.
func TestMerge_AGoTimingMissThatBreaksAnyConditionStillFails(t *testing.T) {
	otherMiss := proof.TimingMiss{ID: "replay-slowest", Measured: 31, Bound: 30}
	cases := []struct {
		name       string
		ci         bool
		cpus       int
		tree, base string
		entry      entry
		record     *proof.TimingRecord // nil: no record
		extra      []entry             // other tests of the same shard
		problems   []string
		want       string // in the proof's ineligible reasons or the known gap's text
	}{
		{name: "a local proof", ci: false, cpus: 4, tree: goRow, base: goRow, entry: failedA(missTail(liveMiss)),
			record: ptr(recordOf("TestA", checkOf(liveMiss))), want: "only a CI integration part may excuse"},
		{name: "a runner as large as the reference", ci: true, cpus: 16, tree: goRow, base: goRow, entry: failedA(missTail(liveMiss)),
			record: ptr(recordOf("TestA", checkOf(liveMiss))), want: timingProblemA},
		{name: "a row the change adds", ci: true, cpus: 4, tree: goRow, base: "", entry: failedA(missTail(liveMiss)),
			record: ptr(recordOf("TestA", checkOf(liveMiss))), want: "does not carry a timing row"},
		{name: "a bound the change loosens", ci: true, cpus: 4, tree: strings.Replace(goRow, "\t2\t16\t", "\t3\t16\t", 1), base: goRow,
			entry: failedA(missTail(liveMiss)), record: ptr(recordOf("TestA", checkOf(liveMiss))), want: "does not carry a timing row"},
		{name: "a recorded bound that differs from the row's", ci: true, cpus: 4, tree: goRow, base: goRow,
			entry:  failedA(missTail(proof.TimingMiss{ID: "getrun-live-p95", Measured: 3.5, Bound: 3})),
			record: ptr(recordOf("TestA", checkOf(proof.TimingMiss{ID: "getrun-live-p95", Measured: 3.5, Bound: 3}))), want: "was bounded at 3 s, and the base's row says 2 s"},
		{name: "no timing record", ci: true, cpus: 4, tree: goRow, base: goRow, entry: failedA(missTail(liveMiss)),
			record: nil, want: "left no timing record"},
		{name: "a record of another test", ci: true, cpus: 4, tree: goRow, base: goRow, entry: failedA(missTail(liveMiss)),
			record: ptr(recordOf("TestB", checkOf(liveMiss))), want: "left no timing record"},
		{name: "a record of another package", ci: true, cpus: 4, tree: goRow, base: goRow, entry: failedA(missTail(liveMiss)),
			record: func() *proof.TimingRecord {
				r := recordOf("TestA", checkOf(liveMiss))
				r.Package = "example.com/other"
				return &r
			}(),
			want: "not this test's"},
		{name: "the test failed something besides its timing", ci: true, cpus: 4, tree: goRow, base: goRow, entry: failedA(missTail(liveMiss)),
			record: func() *proof.TimingRecord {
				r := recordOf("TestA", checkOf(liveMiss))
				r.NonTimingFailed = true
				return &r
			}(),
			want: "something besides its timing checks"},
		{name: "a record that only recorded", ci: true, cpus: 4, tree: goRow, base: goRow, entry: failedA(missTail(liveMiss)),
			record: func() *proof.TimingRecord { r := recordOf("TestA", checkOf(liveMiss)); r.Asserted = false; return &r }(),
			want:   "not this test's, or not an asserting one"},
		{name: "a subtest failed", ci: true, cpus: 4, tree: goRow, base: goRow,
			entry:  func() entry { e := failedA(missTail(liveMiss)); e.Failures = []string{"TestA/unbounded"}; return e }(),
			record: ptr(recordOf("TestA", checkOf(liveMiss))), want: "failed more than a timing check"},
		{name: "the test binary ended while it ran", ci: true, cpus: 4, tree: goRow, base: goRow,
			entry: func() entry {
				e := failedA(missTail(liveMiss))
				e.Reason = "its test binary ended while it ran"
				return e
			}(),
			record: ptr(recordOf("TestA", checkOf(liveMiss))), want: "failed more than a timing check"},
		{name: "another test of its job did not pass", ci: true, cpus: 4, tree: goRow, base: goRow, entry: failedA(missTail(liveMiss)),
			record:   ptr(recordOf("TestA", checkOf(liveMiss))),
			extra:    []entry{func() entry { e := passEntry("TestB"); e.Outcome = outcomeFail; e.Job = "m-001"; return e }()},
			problems: []string{mPkg + ".TestB failed (job m-001)"}, want: "TestB, of the same job, did not pass either"},
		{name: "a non-timing line after the miss", ci: true, cpus: 4, tree: goRow, base: goRow,
			entry:  failedA(append(missTail(liveMiss), "panic: close of closed channel")),
			record: ptr(recordOf("TestA", checkOf(liveMiss))), want: "lines from its first timing miss"},
		{name: "no timing-miss line in the output", ci: true, cpus: 4, tree: goRow, base: goRow,
			entry:  failedA([]string{"    soak_test.go:90: E4 threshold missed at 4000 items: GetRun p95 live 2.014s > 2s"}),
			record: ptr(recordOf("TestA", checkOf(liveMiss))), want: "holds no timing-miss line"},
		{name: "a miss line the record does not hold", ci: true, cpus: 4, tree: goRow, base: goRow,
			entry:  failedA(missTail(liveMiss, otherMiss)),
			record: ptr(recordOf("TestA", checkOf(liveMiss))), want: "lines from its first timing miss"},
		{name: "a miss line of other numbers than the record's", ci: true, cpus: 4, tree: goRow, base: goRow,
			entry:  failedA(missTail(proof.TimingMiss{ID: "getrun-live-p95", Measured: 2.1, Bound: 2, Label: "reduced-fan-out"})),
			record: ptr(recordOf("TestA", checkOf(liveMiss))), want: "the failure output says"},
		{name: "a check no row declares", ci: true, cpus: 4, tree: goRow, base: goRow,
			entry:  failedA(missTail(liveMiss, otherMiss)),
			record: ptr(recordOf("TestA", checkOf(liveMiss), checkOf(otherMiss))), want: `no row of the base's table declares the check "replay-slowest"`},
		{name: "a record whose miss is not over its bound", ci: true, cpus: 4, tree: goRow, base: goRow,
			entry: failedA(missTail(proof.TimingMiss{ID: "getrun-live-p95", Measured: 1.5, Bound: 2})),
			record: func() *proof.TimingRecord {
				r := recordOf("TestA", proof.TimingCheck{ID: "getrun-live-p95", Measured: 1.5, Bound: 2, Miss: true})
				return &r
			}(), want: "not a miss"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			f := newMergeFixture(t, tierFull, c.ci)
			f.anchorHostSpeed(c.tree, c.base)
			tests := append([]entry{c.entry}, c.extra...)
			problems := append([]string{timingProblemA}, c.problems...)
			part := "integration:1"
			if c.ci {
				f.goShard(c.cpus, "fail", tests, problems)
			} else {
				part = "local"
				all := append(tests, passEntry("TestB"))
				f.writeLocalWith(func(p *partRecord) {
					p.Runner.CPUs = c.cpus
					for i := range p.Gates {
						if p.Gates[i].Gate == "integration" {
							p.Gates[i].Result = "fail"
						}
					}
				}, ledger{Suite: "integration", Tests: all, Problems: problems})
			}
			if c.record != nil {
				f.putTiming(part, *c.record)
			}
			r, code := f.merge()
			if code == 0 || r.Result != "fail" || r.MergeEligible {
				t.Fatalf("code %d result %s eligible %v: the miss was excused (%v)", code, r.Result, r.MergeEligible, r.KnownGaps)
			}
			text := strings.Join(append(append(append([]string{}, r.Ineligible...), r.KnownGaps...), hostSpeedMessages(r)...), "\n")
			if !strings.Contains(text, c.want) {
				t.Errorf("no line with %q in ineligible %v, known gaps %v, host-speed failures %v", c.want, r.Ineligible, r.KnownGaps, hostSpeedMessages(r))
			}
			for _, g := range r.HostSpeedFailures {
				if g.Excused {
					t.Errorf("host_speed_failures holds an excused %+v for a miss that failed the proof", g)
				}
			}
		})
	}
}

func ptr[T any](v T) *T { return &v }

func hostSpeedMessages(r proofRecord) []string {
	var out []string
	for _, g := range r.HostSpeedFailures {
		out = append(out, g.Message)
	}
	return out
}

// An excused miss leaves the gate green only when it is the part's only
// defect: another problem of the same shard keeps the proof failing.
func TestMerge_AGoTimingMissDoesNotHideAnotherProblemOfItsShard(t *testing.T) {
	f := newMergeFixture(t, tierFull, true)
	f.anchorHostSpeed(goRow, goRow)
	other := passEntry("TestB")
	other.Outcome = outcomeFail
	other.Job = "m-002"
	f.goShard(4, "fail", []entry{failedA(missTail(liveMiss)), other}, []string{timingProblemA, mPkg + ".TestB failed (job m-002)"})
	f.putTiming("integration:1", recordOf("TestA", checkOf(liveMiss)))
	r, code := f.merge()
	if code == 0 || r.Result != "fail" || r.MergeEligible {
		t.Fatalf("code %d, result %s: another failure of the shard was hidden by the excused miss", code, r.Result)
	}
	text := strings.Join(r.Ineligible, "\n")
	if !strings.Contains(text, "TestB failed") || strings.Contains(text, "TestA failed") {
		t.Errorf("ineligible = %v, want TestB's failure and not TestA's", r.Ineligible)
	}
	if !strings.Contains(text, "gate integration failed") {
		t.Errorf("ineligible = %v: the shard's gate must stay red when it has another defect", r.Ineligible)
	}
}

// A shard that excused itself in its own ledger (a host-speed failure with
// no problem line) is not believed: the merger derives the excusal or none.
func TestMerge_AGoTimingExcusalIsTheMergersOwn(t *testing.T) {
	f := newMergeFixture(t, tierFull, true)
	f.anchorHostSpeed(goRow, goRow)
	// The shard dropped its own problem line and left no record: the
	// entry is still a failure the merger finds no excusal for.
	f.goShard(4, "pass", []entry{failedA(missTail(liveMiss))}, nil)
	r, code := f.merge()
	if code == 0 || r.Result != "fail" {
		t.Fatalf("code %d, result %s: a failure the shard stayed silent about passed", code, r.Result)
	}
	if !strings.Contains(strings.Join(r.Ineligible, "\n"), mPkg+".TestA") {
		t.Errorf("ineligible = %v, want the failed test named", r.Ineligible)
	}
}

// A test row whose assertion is "-" is recorded on a miss and never excused.
func TestMerge_ADashRowIsRecordedAndNeverExcused(t *testing.T) {
	row := "test\t" + mPkg + "\tTestA\t-\t60\t16\tthe stage completes within 60 s\n"
	f := newMergeFixture(t, tierFull, true)
	f.anchorHostSpeed(row, row)
	f.goShard(4, "fail", []entry{failedA(missTail(liveMiss))}, []string{timingProblemA})
	f.putTiming("integration:1", recordOf("TestA", checkOf(liveMiss)))
	r, code := f.merge()
	if code == 0 || r.Result != "fail" || r.MergeEligible {
		t.Fatalf("code %d, result %s: a dash row excused a failure", code, r.Result)
	}
	if len(r.HostSpeedFailures) != 1 || r.HostSpeedFailures[0].Excused || r.HostSpeedFailures[0].Assertion != "-" {
		t.Errorf("host_speed_failures = %+v", r.HostSpeedFailures)
	}
}

func TestFailureIsTheMisses(t *testing.T) {
	a := proof.TimingMiss{ID: "getrun-live-p95", Measured: 2.014, Bound: 2, Label: "unbounded"}
	b := proof.TimingMiss{ID: "replay-slowest", Measured: 31.5, Bound: 30}
	line := func(m proof.TimingMiss) string { return "    timing.go:238: " + proof.MissLine(m) }
	checks := func(ms ...proof.TimingMiss) []proof.TimingCheck {
		var out []proof.TimingCheck
		for _, m := range ms {
			out = append(out, checkOf(m))
		}
		return out
	}
	for _, c := range []struct {
		name   string
		output []string
		misses []proof.TimingCheck
		want   string // "" is accepted
	}{
		{"one miss", []string{"log", line(a)}, checks(a), ""},
		{"two misses in order", []string{"log", line(a), line(b)}, checks(a, b), ""},
		{"only the miss", []string{line(a)}, checks(a), ""},
		{"no output", nil, checks(a), "holds no timing-miss line"},
		{"two misses out of order", []string{line(b), line(a)}, checks(a, b), "the failure output says"},
		{"a line in place of the second miss", []string{line(a), "stray"}, checks(a, b), "after a timing miss"},
		{"a line between the misses", []string{line(a), "stray", line(b)}, checks(a, b), "lines from its first timing miss"},
		{"a line after the misses", []string{line(a), "stray"}, checks(a), "lines from its first timing miss"},
		{"a miss the record lacks", []string{line(a), line(b)}, checks(a), "lines from its first timing miss"},
		{"a record miss the output lacks", []string{line(a)}, checks(a, b), "lines from its first timing miss"},
		{"another label", []string{line(proof.TimingMiss{ID: a.ID, Measured: a.Measured, Bound: a.Bound, Label: "bounded"})}, checks(a), "the failure output says"},
	} {
		t.Run(c.name, func(t *testing.T) {
			got := failureIsTheMisses(c.output, c.misses)
			if (c.want == "") != (got == "") || !strings.Contains(got, c.want) {
				t.Errorf("failureIsTheMisses = %q, want %q", got, c.want)
			}
		})
	}
}

func TestIsCIIntegrationPart(t *testing.T) {
	for part, want := range map[string]bool{"integration:1": true, "integration:12": true, "local": false, "integration": false,
		"integration:0": false, "integration:x": false, "ui-e2e:1": false, "unit": false} {
		if got := isCIIntegrationPart(part); got != want {
			t.Errorf("isCIIntegrationPart(%q) = %v, want %v", part, got, want)
		}
	}
}

// A real `go test -json` run of a test that uses the helper: the lines the
// helper prints, as testing and test2json carry them, reach the ledger entry,
// and the merger excuses exactly that failure. It is the end-to-end check of
// the format the unit tests of each side agree on.
func TestGoTiming_ARealFailingTestIsExcusedFromItsOwnOutput(t *testing.T) {
	if testing.Short() {
		t.Skip("compiles and runs a test binary")
	}
	artifacts := t.TempDir()
	cmd := exec.Command("go", "test", "-tags=integration", "-json", "-count=1", "-run", "^TestTimingMiss$", "./testdata/timingsuite")
	cmd.Env = append(os.Environ(), proof.EnvDir+"="+artifacts)
	out, err := cmd.Output()
	if err == nil {
		t.Fatalf("the fixture test passed; output:\n%s", out)
	}
	c := newCollector()
	if err := readEvents(strings.NewReader(string(out)), c, "m-001", nil, nil, nil); err != nil {
		t.Fatal(err)
	}
	const pkg = "github.com/orkstack/orkstack/test/tools/proofrun/testdata/timingsuite"
	e, ok := c.results[testKey{pkg, "TestTimingMiss"}]
	if !ok || e.Outcome != outcomeFail || len(e.FailureOutput) == 0 {
		t.Fatalf("entry = %+v, ok %v", e, ok)
	}
	last := e.FailureOutput[len(e.FailureOutput)-1]
	if miss, ok := proof.ParseMissLine(last); !ok || miss.ID != "getrun-live-p95" || miss.Bound != 2 {
		t.Fatalf("the failing test's last output line is %q (%+v, %v); output kept:\n%s", last, miss, ok, strings.Join(e.FailureOutput, "\n"))
	}

	// The part as CI leaves it: the ledger entry, and artifacts/timing.
	partDir := t.TempDir()
	if err := os.MkdirAll(filepath.Join(partDir, "artifacts"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.CopyFS(filepath.Join(partDir, "artifacts", proof.TimingGroup), os.DirFS(filepath.Join(artifacts, proof.TimingGroup))); err != nil {
		t.Fatalf("the test left no timing directory: %v", err)
	}
	row := "test\t" + pkg + "\tTestTimingMiss\ttiming getrun-live-p95\t2\t16\tthe fixture's check\n"
	tab, err := parseHostSpeed(strings.NewReader(row), "t")
	if err != nil {
		t.Fatal(err)
	}
	m := &merger{hostSpeedExcusing: tab}
	shard := ledger{Suite: "integration", Tests: []entry{*e}}
	gaps, why := m.goTimingExcusal("integration:1", partDir, 4, shard, *e)
	if why != "" || len(gaps) != 1 || !gaps[0].Excused || gaps[0].Observed < 2 {
		t.Fatalf("a real helper miss was not excused: %q, %+v", why, gaps)
	}
	// And on the reference host it is a failure.
	if gaps, _ := m.goTimingExcusal("integration:1", partDir, 16, shard, *e); len(gaps) != 0 {
		t.Errorf("a miss on a 16-CPU runner was excused: %+v", gaps)
	}
}
