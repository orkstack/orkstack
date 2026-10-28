package main

import (
	"fmt"
	"regexp"
	"strings"
	"testing"
	"time"
)

func ev(action, pkg, test string, elapsed float64) event {
	return event{Action: action, Package: pkg, Test: test, Elapsed: elapsed}
}

func TestBuildLedger(t *testing.T) {
	const p = "example.com/p"
	c := newCollector()
	for _, e := range []event{
		ev("run", p, "TestPass", 0), ev("pass", p, "TestPass", 1.5),
		ev("run", p, "TestFail", 0), ev("fail", p, "TestFail", 2),
		ev("run", p, "TestSkip", 0), ev("skip", p, "TestSkip", 0),
		ev("run", p, "TestAllowed", 0), ev("skip", p, "TestAllowed", 0),
		ev("run", p, "TestSub", 0), ev("run", p, "TestSub/a", 0), ev("skip", p, "TestSub/a", 0), ev("pass", p, "TestSub", 0),
		ev("run", p, "TestStarted", 0),
		ev("output", p, "", 0),
	} {
		c.add(e, "j1")
	}
	listed := map[string]map[string]bool{p: {"TestPass": true, "TestFail": true, "TestSkip": true, "TestAllowed": true, "TestSub": true, "TestStarted": true, "TestNeverRan": true}}
	expected := []testKey{{p, "TestPass"}, {p, "TestFail"}, {p, "TestSkip"}, {p, "TestAllowed"}, {p, "TestSub"}, {p, "TestStarted"}, {p, "TestNeverRan"}, {p, "TestPass"}}
	allow := []allowedSkip{
		{Suite: "integration", Package: p, Test: "TestAllowed", Reason: "recorder", Line: 1},
		{Suite: "integration", Package: p, Test: "TestGone", Reason: "stale", Line: 2},
		{Suite: "unit", Package: p, Test: "TestSkip", Reason: "another suite", Line: 3},
		{Suite: "integration", Package: "example.com/other", Test: "TestX", Reason: "not compiled here", Line: 4},
	}
	l := buildLedger("integration", expected, listed, c, allow)
	s := l.Summary
	if s.Expected != 7 || s.Pass != 2 || s.Fail != 1 || s.Skip != 1 || s.AllowedSkip != 1 || s.NoOutcome != 2 || s.SubtestSkips != 1 {
		t.Errorf("summary = %+v", s)
	}
	problems := strings.Join(l.Problems, "\n")
	for _, f := range []string{
		"example.com/p.TestFail failed",
		"example.com/p.TestSkip skipped",
		"example.com/p.TestSub/a skipped",
		"example.com/p.TestStarted no outcome: the test started and never reported a result",
		"example.com/p.TestNeverRan no outcome: the test was listed and asked for",
		"allowed-skip entry (line 2) names example.com/p.TestGone",
	} {
		if !strings.Contains(problems, f) {
			t.Errorf("problems lack %q:\n%s", f, problems)
		}
	}
	for _, f := range []string{"TestAllowed skipped", "example.com/other", "line 3"} {
		if strings.Contains(problems, f) {
			t.Errorf("problems should not mention %q:\n%s", f, problems)
		}
	}
	if l.ok() {
		t.Error("a ledger with problems is ok")
	}
}

func TestParseAllowlist(t *testing.T) {
	good := "# comment\n\nany\tex.com/p\tTestX\tthe child half of a helper process\n"
	got, err := parseAllowlist(strings.NewReader(good), "t")
	if err != nil || len(got) != 1 || got[0].Reason != "the child half of a helper process" || got[0].Line != 3 {
		t.Fatalf("got %+v, %v", got, err)
	}
	for _, bad := range []string{
		"any\tex.com/p\tTestX\n",          // no reason
		"any\tex.com/p\tTestX\t  \n",      // blank reason
		"nightly\tex.com/p\tTestX\twhy\n", // unknown suite
	} {
		if _, err := parseAllowlist(strings.NewReader(bad), "t"); err == nil {
			t.Errorf("accepted %q", bad)
		}
	}
}

func TestParseListing(t *testing.T) {
	out := "TestA\nExampleB\nFuzzC\nBenchmarkD\nok  \tex.com/p\t0.01s\n"
	if got := parseListing(out); strings.Join(got, ",") != "TestA,ExampleB,FuzzC" {
		t.Errorf("got %v", got)
	}
}

func TestPlan(t *testing.T) {
	const a, b = "ex.com/a", "ex.com/b"
	measured := costs{{a, "TestSlow"}: 300, {a, "TestTen"}: 10, {b, "TestQuick"}: 1}
	var fast []string
	for i := 0; i < 20; i++ {
		name := fmt.Sprintf("TestFast%02d", i)
		fast = append(fast, name)
		measured[testKey{a, name}] = 9
	}
	listing := append([]string{"TestSlow", "TestNew", "TestTen"}, fast...)
	jobs := plan(map[string][]string{a: listing, b: {"TestQuick"}}, measured, map[string]map[string]string{a: {"TestNew": "unit"}})
	var got []string
	for _, j := range jobs {
		got = append(got, fmt.Sprintf("%s:%s(%d)", j.Package[len("ex.com/"):], j.Tests[0], len(j.Tests)))
	}
	// Longest first: the slow test; the first batch, closed before it passed
	// 150s (16 x 9s); the never-measured unit test (assumed 30s, alone); the rest
	// of the batch (4 x 9s); the 10s test, which is slow and so alone; and
	// the other package's batch.
	want := []string{"a:TestSlow(1)", "a:TestFast00(16)", "a:TestFast16(4)", "a:TestNew(1)", "a:TestTen(1)", "b:TestQuick(1)"}
	if strings.Join(got, " ") != strings.Join(want, " ") {
		t.Fatalf("jobs = %v, want %v", got, want)
	}
	timeouts := map[string]time.Duration{}
	for _, j := range jobs {
		timeouts[j.Tests[0]] = j.Timeout
	}
	if timeouts["TestSlow"] != 15*time.Minute || timeouts["TestFast00"] != 10*time.Minute || timeouts["TestNew"] != 30*time.Minute {
		t.Errorf("timeouts = %v", timeouts)
	}
	if jobs[0].ID != "a-001" || jobs[len(jobs)-1].ID != "b-006" {
		t.Errorf("ids = %s .. %s", jobs[0].ID, jobs[len(jobs)-1].ID)
	}
}

func TestRunPatternIsAnchoredAndQuoted(t *testing.T) {
	re := regexp.MustCompile(runPattern([]string{"TestA", "TestB.x"}))
	for name, want := range map[string]bool{"TestA": true, "TestB.x": true, "TestAB": false, "XTestA": false, "TestBxx": false, "TestB_x": false} {
		if re.MatchString(name) != want {
			t.Errorf("%s: match = %v, want %v", name, !want, want)
		}
	}
}

func TestCostsFromGoTestLog(t *testing.T) {
	log := `=== RUN   TestA
--- PASS: TestA (12.50s)
    --- PASS: TestA/sub (1.00s)
--- SKIP: TestRecorder (0.00s)
--- FAIL: TestB (3.00s)
FAIL
FAIL	github.com/x/one	15.6s
=== RUN   TestC
--- PASS: TestC (0.10s)
ok  	github.com/x/two	0.2s
?   	github.com/x/three	[no test files]
`
	lines, err := costsFromGoTestLog(strings.NewReader(log))
	if err != nil {
		t.Fatal(err)
	}
	var got []string
	for _, l := range lines {
		got = append(got, l.Package+"."+l.Test)
	}
	if strings.Join(got, " ") != "github.com/x/one.TestA github.com/x/one.TestRecorder github.com/x/one.TestB github.com/x/two.TestC" {
		t.Errorf("got %v", got)
	}
	var sb strings.Builder
	writeSlowList(&sb, lines, 10, "proof/x (commit abc)")
	if !strings.Contains(sb.String(), "github.com/x/one\tTestA\t12.50\n") || strings.Contains(sb.String(), "TestC") {
		t.Errorf("slow list:\n%s", sb.String())
	}
	if !strings.Contains(sb.String(), "# source: proof/x (commit abc)") {
		t.Errorf("slow list names no source:\n%s", sb.String())
	}
}

func TestMakespan(t *testing.T) {
	jobs := []job{{Cost: 300}, {Cost: 100}, {Cost: 100}, {Cost: 100}, {Cost: 50}}
	if got := makespan(jobs, 2); got != 350 {
		t.Errorf("makespan on 2 slots = %v, want 350", got)
	}
	if got := makespan(jobs, 1); got != 650 || totalCost(jobs) != 650 {
		t.Errorf("makespan on 1 slot = %v, total %v, want 650", got, totalCost(jobs))
	}
}

func out(pkg, test, text string) event {
	return event{Action: "output", Package: pkg, Test: test, Output: text}
}

// A failing top-level test keeps the last lines it printed, its subtests'
// included and the testing package's own framing left out; a test that
// passes or skips keeps none.
func TestCollector_AFailingTestKeepsItsOutputTail(t *testing.T) {
	const p = "example.com/p"
	c := newCollector()
	for _, e := range []event{
		ev("run", p, "TestFail", 0),
		out(p, "TestFail", "=== RUN   TestFail\n"),
		out(p, "TestFail", "    f_test.go:10: first\n"),
		ev("run", p, "TestFail/sub", 0),
		out(p, "TestFail/sub", "=== RUN   TestFail/sub\n"),
		out(p, "TestFail/sub", "    f_test.go:12: from a subtest\n"),
		out(p, "TestFail/sub", "    --- FAIL: TestFail/sub (0.00s)\n"),
		ev("fail", p, "TestFail/sub", 0),
		out(p, "TestFail", "\n"),
		out(p, "TestFail", "    f_test.go:14: last\r\n"),
		out(p, "TestFail", "--- FAIL: TestFail (0.01s)\n"),
		ev("fail", p, "TestFail", 0.01),

		ev("run", p, "TestPass", 0),
		out(p, "TestPass", "    f_test.go:20: chatter\n"),
		ev("pass", p, "TestPass", 0),
		ev("run", p, "TestSkip", 0),
		out(p, "TestSkip", "    f_test.go:30: skipping\n"),
		ev("skip", p, "TestSkip", 0),
	} {
		c.add(e, "j1")
	}
	want := []string{"    f_test.go:10: first", "    f_test.go:12: from a subtest", "    f_test.go:14: last"}
	if got := c.results[testKey{p, "TestFail"}].FailureOutput; strings.Join(got, "|") != strings.Join(want, "|") {
		t.Errorf("TestFail kept %q, want %q", got, want)
	}
	for _, name := range []string{"TestPass", "TestSkip"} {
		if got := c.results[testKey{p, name}].FailureOutput; len(got) != 0 {
			t.Errorf("%s kept %q: only a failure keeps its output", name, got)
		}
	}
	if len(c.tails) != 0 {
		t.Errorf("the collector still holds the output of %d finished tests", len(c.tails))
	}
}

func TestCollector_TheOutputTailIsBounded(t *testing.T) {
	const p = "example.com/p"
	c := newCollector()
	c.add(ev("run", p, "TestFail", 0), "j1")
	for i := range failureTailLines + 25 {
		c.add(out(p, "TestFail", fmt.Sprintf("line %d\n", i)), "j1")
	}
	c.add(out(p, "TestFail", strings.Repeat("é", failureTailWidth)+"\n"), "j1")
	c.add(ev("fail", p, "TestFail", 1), "j1")
	got := c.results[testKey{p, "TestFail"}].FailureOutput
	if len(got) != failureTailLines {
		t.Fatalf("kept %d lines, want %d", len(got), failureTailLines)
	}
	if got[len(got)-2] != fmt.Sprintf("line %d", failureTailLines+24) {
		t.Errorf("the tail does not end at the last lines: %q", got[len(got)-2])
	}
	if last := got[len(got)-1]; len(last) > failureTailWidth+len("…") || !strings.HasSuffix(last, "…") {
		t.Errorf("a long line was kept at %d bytes: %.40q", len(last), last)
	}
}

func TestCollector_ParallelTestsKeepTheirOwnOutput(t *testing.T) {
	const p = "example.com/p"
	c := newCollector()
	for _, e := range []event{
		ev("run", p, "TestA", 0), ev("run", p, "TestB", 0),
		out(p, "TestA", "    a_test.go:1: a one\n"), out(p, "TestB", "    b_test.go:1: b one\n"),
		out(p, "TestA", "    a_test.go:2: a two\n"),
		ev("fail", p, "TestA", 1), ev("fail", p, "TestB", 1),
	} {
		c.add(e, "j1")
	}
	if got := strings.Join(c.results[testKey{p, "TestA"}].FailureOutput, "|"); got != "    a_test.go:1: a one|    a_test.go:2: a two" {
		t.Errorf("TestA kept %q", got)
	}
	if got := strings.Join(c.results[testKey{p, "TestB"}].FailureOutput, "|"); got != "    b_test.go:1: b one" {
		t.Errorf("TestB kept %q", got)
	}
}

func TestFrameworkLine(t *testing.T) {
	for line, want := range map[string]bool{
		"=== RUN   TestX": true, "=== PAUSE TestX": true, "=== CONT  TestX": true, "=== NAME  TestX": true,
		"--- FAIL: TestX (0.00s)": true, "    --- FAIL: TestX/sub (0.00s)": true, "--- PASS: TestX (0.00s)": true, "--- SKIP: TestX (0.00s)": true,
		"    x_test.go:1: --- FAIL: not framing": false, "panic: boom": false, "FAIL": false, "": false,
	} {
		if got := frameworkLine(line); got != want {
			t.Errorf("frameworkLine(%q) = %v, want %v", line, got, want)
		}
	}
}
