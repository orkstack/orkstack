package main

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"
)

// The race gate runs the unit suites under the race detector (AGENTS.md
// §11.2). It is the unit gate's ledger with three differences, each pinned
// here: a data race fails it whatever the tests reported, a test skipped by
// -short is recorded as a skip that the merger holds to the unit gate's
// result, and a repeated run (-count above 1) cannot hide a failure behind a
// later pass.

const racePkg = "example.com/r"

func raceOutput(pkg, test string) event {
	return event{Action: "output", Package: pkg, Test: test, Output: "WARNING: DATA RACE\n"}
}

func TestCollector_RecordsEveryRaceReportWhereItWasPrinted(t *testing.T) {
	c := newCollector()
	for _, e := range []event{
		ev("run", racePkg, "TestA", 0),
		raceOutput(racePkg, "TestA"),
		raceOutput(racePkg, "TestA"),
		// Printed by a goroutine that outlived every test: no test name.
		raceOutput(racePkg, ""),
		// A subtest's report belongs to its own name.
		raceOutput(racePkg, "TestA/inner"),
		// Only the detector's own line is a report: a test may mention it.
		{Action: "output", Package: racePkg, Test: "TestA", Output: "    x_test.go:9: the log says WARNING: DATA RACE in prose\n"},
		{Action: "output", Package: racePkg, Test: "TestA", Output: "Read at 0x00c000 by goroutine 8:\n"},
	} {
		c.add(e, "race")
	}
	if got := c.races[testKey{racePkg, "TestA"}]; got != 2 {
		t.Errorf("TestA has %d reports, want 2", got)
	}
	if got := c.races[testKey{racePkg, ""}]; got != 1 {
		t.Errorf("the package has %d reports outside any test, want 1", got)
	}
	if got := c.races[testKey{racePkg, "TestA/inner"}]; got != 1 {
		t.Errorf("TestA/inner has %d reports, want 1", got)
	}
	if len(c.races) != 3 {
		t.Errorf("races = %v, want exactly three keys", c.races)
	}
}

// A repeat that passes after one that failed leaves the failure standing, and
// with it the lines the failing repeat printed.
func TestCollector_AStickyCollectorKeepsTheFailingRepeatsOutput(t *testing.T) {
	out := func(line string) event {
		return event{Action: "output", Package: racePkg, Test: "TestOnce", Output: line + "\n"}
	}
	c := newCollector()
	c.sticky = true
	for _, e := range []event{
		ev("run", racePkg, "TestOnce", 0), out("boom: the first repeat failed"), ev("fail", racePkg, "TestOnce", 1),
		ev("run", racePkg, "TestOnce", 0), out("calm"), ev("pass", racePkg, "TestOnce", 1),
	} {
		c.add(e, "j")
	}
	e := c.results[testKey{racePkg, "TestOnce"}]
	if e.Outcome != outcomeFail {
		t.Fatalf("TestOnce = %s, want fail", e.Outcome)
	}
	if got := strings.Join(e.FailureOutput, "\n"); !strings.Contains(got, "boom: the first repeat failed") || strings.Contains(got, "calm") {
		t.Errorf("failure output = %q, want the failing repeat's lines only", got)
	}
}

// -count N prints one pass or fail per run of one test name. The unit
// collector keeps the last (nothing repeats there); a repeated run keeps the
// worst, so a test that failed once in three is a failure.
func TestCollector_AStickyCollectorKeepsTheWorstRepeat(t *testing.T) {
	events := []event{
		ev("run", racePkg, "TestOnce", 0), ev("pass", racePkg, "TestOnce", 1),
		ev("run", racePkg, "TestOnce", 0), ev("fail", racePkg, "TestOnce", 2),
		ev("run", racePkg, "TestOnce", 0), ev("pass", racePkg, "TestOnce", 1),
		ev("run", racePkg, "TestSkips", 0), ev("pass", racePkg, "TestSkips", 1),
		ev("run", racePkg, "TestSkips", 0), ev("skip", racePkg, "TestSkips", 0),
		ev("run", racePkg, "TestSkips", 0), ev("pass", racePkg, "TestSkips", 1),
		ev("run", racePkg, "TestAll", 0), ev("pass", racePkg, "TestAll", 1),
		ev("run", racePkg, "TestAll", 0), ev("pass", racePkg, "TestAll", 1),
	}
	plain, sticky := newCollector(), newCollector()
	sticky.sticky = true
	for _, e := range events {
		plain.add(e, "j")
		sticky.add(e, "j")
	}
	if got := plain.results[testKey{racePkg, "TestOnce"}].Outcome; got != outcomePass {
		t.Errorf("the unit collector's TestOnce = %s: its behaviour must not change", got)
	}
	for test, want := range map[string]string{"TestOnce": outcomeFail, "TestSkips": outcomeSkip, "TestAll": outcomePass} {
		if got := sticky.results[testKey{racePkg, test}].Outcome; got != want {
			t.Errorf("sticky %s = %s, want %s", test, got, want)
		}
	}
	// The seconds of a repeated test are the sum of its runs.
	if got := sticky.results[testKey{racePkg, "TestOnce"}].Seconds; got != 4 {
		t.Errorf("sticky TestOnce took %v s, want the 4 s of its three runs", got)
	}
	// A failing subtest of any repeat stays in the record.
	sub := newCollector()
	sub.sticky = true
	for _, e := range []event{
		ev("run", racePkg, "TestP", 0), ev("run", racePkg, "TestP/a", 0), ev("fail", racePkg, "TestP/a", 1), ev("fail", racePkg, "TestP", 1),
		ev("run", racePkg, "TestP", 0), ev("run", racePkg, "TestP/a", 0), ev("pass", racePkg, "TestP/a", 1), ev("pass", racePkg, "TestP", 1),
	} {
		sub.add(e, "j")
	}
	if got := sub.results[testKey{racePkg, "TestP"}].Outcome; got != outcomeFail {
		t.Errorf("sticky TestP = %s, want fail", got)
	}
	// A subtest repeated three times is one failing leaf and one skipped
	// subtest, not three of either.
	rep := newCollector()
	rep.sticky = true
	for i := 0; i < 3; i++ {
		for _, e := range []event{
			ev("run", racePkg, "TestQ", 0), ev("run", racePkg, "TestQ/bad", 0), ev("fail", racePkg, "TestQ/bad", 1),
			ev("run", racePkg, "TestQ/big", 0), ev("skip", racePkg, "TestQ/big", 0), ev("fail", racePkg, "TestQ", 1),
		} {
			rep.add(e, "j")
		}
	}
	listed, expected := raceListing("TestQ")
	l := buildRaceLedger(suiteRaceRepeat, true, expected, listed, rep, nil)
	if l.Summary.SubtestSkips != 1 || len(l.Tests) != 1 || len(l.Tests[0].Failures) != 1 {
		t.Errorf("repeated subtests: summary %+v, failures %v: each leaf counts once", l.Summary, l.Tests[0].Failures)
	}
}

func raceListing(names ...string) (map[string]map[string]bool, []testKey) {
	listed := map[string]map[string]bool{racePkg: {}}
	var expected []testKey
	for _, n := range names {
		listed[racePkg][n] = true
		expected = append(expected, testKey{racePkg, n})
	}
	return listed, expected
}

func TestRaceLedger_ADataRaceIsAProblemWhateverTheTestsReported(t *testing.T) {
	// The detector can report a race in a test that goes on to pass (the
	// report is printed, the test's own assertions hold), and one after the
	// last test ended. Neither may read as a green gate.
	listed, expected := raceListing("TestPasses", "TestOther")
	c := newCollector()
	for _, e := range []event{
		ev("run", racePkg, "TestPasses", 0), raceOutput(racePkg, "TestPasses"), ev("pass", racePkg, "TestPasses", 1),
		ev("run", racePkg, "TestOther", 0), ev("pass", racePkg, "TestOther", 1),
		raceOutput(racePkg, ""),
	} {
		c.add(e, "race")
	}
	l := buildRaceLedger(suiteRace, true, expected, listed, c, nil)
	if l.ok() {
		t.Fatalf("a ledger with data races is ok: %+v", l)
	}
	problems := strings.Join(l.Problems, "\n")
	for _, want := range []string{
		racePkg + ".TestPasses reported a data race",
		racePkg + " reported a data race outside any test",
	} {
		if !strings.Contains(problems, want) {
			t.Errorf("problems lack %q:\n%s", want, problems)
		}
	}
	if strings.Contains(problems, "TestOther") {
		t.Errorf("a clean test is named in the problems:\n%s", problems)
	}
	if l.Suite != suiteRace {
		t.Errorf("suite = %q, want %q", l.Suite, suiteRace)
	}
}

func TestRaceLedger_AShortSkipIsRecordedAsASkipAndIsNoProblemOfItsOwn(t *testing.T) {
	listed, expected := raceListing("TestRuns", "TestScale", "TestSub", "TestAllowed")
	c := newCollector()
	for _, e := range []event{
		ev("run", racePkg, "TestRuns", 0), ev("pass", racePkg, "TestRuns", 1),
		ev("run", racePkg, "TestScale", 0), ev("skip", racePkg, "TestScale", 0),
		ev("run", racePkg, "TestSub", 0), ev("run", racePkg, "TestSub/big", 0), ev("skip", racePkg, "TestSub/big", 0), ev("pass", racePkg, "TestSub", 0),
		ev("run", racePkg, "TestAllowed", 0), ev("skip", racePkg, "TestAllowed", 0),
	} {
		c.add(e, "race")
	}
	allow := []allowedSkip{{Suite: "any", Package: racePkg, Test: "TestAllowed", Reason: "helper process", Line: 1}}
	l := buildRaceLedger(suiteRace, true, expected, listed, c, allow)
	if !l.ok() {
		t.Fatalf("a ledger whose only skips are -short skips has problems: %v", l.Problems)
	}
	s := l.Summary
	if s.Pass != 2 || s.Skip != 1 || s.AllowedSkip != 1 || s.SubtestSkips != 1 {
		t.Errorf("summary = %+v, want 2 pass (TestRuns, TestSub), 1 skip (TestScale), 1 allowed skip, 1 subtest skip", s)
	}
	for _, e := range l.Tests {
		switch e.Test {
		case "TestScale":
			if e.Outcome != outcomeSkip || !strings.Contains(e.Reason, "-short") {
				t.Errorf("TestScale = %+v: a skip is never recorded as a pass, and says why it is not a problem here", e)
			}
		case "TestAllowed":
			if e.AllowedSkip != "helper process" {
				t.Errorf("TestAllowed = %+v", e)
			}
		}
	}
	// A test that never reported is still a problem: -short excuses a skip,
	// not the absence of an outcome.
	listed, expected = raceListing("TestGone")
	if l := buildRaceLedger(suiteRace, true, expected, listed, newCollector(), nil); l.ok() {
		t.Error("a listed test with no outcome is not a problem")
	}
}

// The unit gate's allowed skips (suite unit) describe a test that is no
// evidence by design in any lane; the race lane runs the same tests, so they
// apply to it too, and a stale one is stale there as well.
func TestRaceLedger_UnitAllowedSkipsApplyToTheRaceSuites(t *testing.T) {
	listed, expected := raceListing("TestRecorder")
	c := newCollector()
	for _, e := range []event{ev("run", racePkg, "TestRecorder", 0), ev("skip", racePkg, "TestRecorder", 0)} {
		c.add(e, "race")
	}
	allow := []allowedSkip{
		{Suite: "unit", Package: racePkg, Test: "TestRecorder", Reason: "opt-in recorder", Line: 1},
		{Suite: "integration", Package: racePkg, Test: "TestElsewhere", Reason: "not this lane", Line: 2},
	}
	for _, suite := range []string{suiteRace, suiteRaceRepeat} {
		l := buildRaceLedger(suite, true, expected, listed, c, allow)
		if l.Summary.AllowedSkip != 1 || !l.ok() {
			t.Errorf("%s: summary %+v, problems %v", suite, l.Summary, l.Problems)
		}
	}
	stale := []allowedSkip{{Suite: "unit", Package: racePkg, Test: "TestRenamed", Reason: "gone", Line: 7}}
	l := buildRaceLedger(suiteRace, true, expected, listed, c, stale)
	if l.ok() || !strings.Contains(strings.Join(l.Problems, "\n"), "allowed-skip entry (line 7) names "+racePkg+".TestRenamed") {
		t.Errorf("a stale unit entry is not stale in the race lane: %v", l.Problems)
	}
}

// A race failure is never quarantined: the race ledger passes no quarantine
// to the builder, so a failing test is a failure even when the unit gate's
// quarantine names it.
func TestRaceLedger_NoQuarantineExcusesAFailure(t *testing.T) {
	listed, expected := raceListing("TestPin")
	c := newCollector()
	for _, e := range []event{
		ev("run", racePkg, "TestPin", 0), ev("run", racePkg, "TestPin/a_pin", 0), ev("fail", racePkg, "TestPin/a_pin", 1), ev("fail", racePkg, "TestPin", 1),
	} {
		c.add(e, "race")
	}
	l := buildRaceLedger(suiteRace, true, expected, listed, c, nil)
	if l.ok() || l.Summary.Fail != 1 || l.Summary.Quarantined != 0 {
		t.Errorf("summary %+v, problems %v: the failure must stand", l.Summary, l.Problems)
	}
}

func TestRaceSuites_NameTheirLedgerAndEvents(t *testing.T) {
	for _, c := range []struct {
		race  bool
		count int
		want  string
	}{{false, 1, suiteUnit}, {true, 1, suiteRace}, {true, 3, suiteRaceRepeat}} {
		if got := unitSuiteName(c.race, c.count); got != c.want {
			t.Errorf("race=%v count=%d: suite %q, want %q", c.race, c.count, got, c.want)
		}
	}
	if suiteRace != "race" || suiteRaceRepeat != "race-repeat" || suiteFuzz != "fuzz" {
		t.Error("the suite names are the gate names, the ledger files' prefixes and the part names; they do not change")
	}
}

// ---- the runner, on throwaway packages --------------------------------------

func TestUnitRunner_RaceFlagsAreValidated(t *testing.T) {
	quiet(t)
	for _, c := range []struct {
		name string
		args []string
	}{
		{"-short without -race", []string{"-short"}},
		{"-count above 1 without -race", []string{"-count", "3"}},
		{"-count below 1", []string{"-race", "-count", "0"}},
	} {
		t.Run(c.name, func(t *testing.T) {
			out := t.TempDir()
			args := append([]string{"-out", out, "-allow", ""}, c.args...)
			if code := cmdUnit(append(args, "./testdata/fakesuite")); code != 2 {
				t.Errorf("exit %d, want 2", code)
			}
			if entries, _ := os.ReadDir(out); len(entries) != 0 {
				t.Errorf("a refused invocation wrote %d files", len(entries))
			}
		})
	}
}

func TestUnitRunner_TheRaceGateFailsOnADataRaceAndNamesTheTest(t *testing.T) {
	if testing.Short() {
		t.Skip("runs go test -race")
	}
	requireRaceToolchain(t)
	quiet(t)
	out := t.TempDir()
	code := cmdUnit([]string{"-out", out, "-allow", "", "-race", "-short", "./testdata/racesuite"})
	if code != 1 {
		t.Fatalf("exit %d, want 1: the suite has a deliberate data race", code)
	}
	var l ledger
	if err := readJSON(filepath.Join(out, "race-tests.json"), &l); err != nil {
		t.Fatalf("the race gate's ledger is race-tests.json: %v", err)
	}
	if l.Suite != suiteRace {
		t.Errorf("suite = %q", l.Suite)
	}
	problems := strings.Join(l.Problems, "\n")
	if !strings.Contains(problems, "TestRacy reported a data race") {
		t.Errorf("the race is not named:\n%s", problems)
	}
	// TestClean ran and passed beside it, and is not blamed.
	var clean entry
	for _, e := range l.Tests {
		if e.Test == "TestClean" {
			clean = e
		}
	}
	if clean.Outcome != outcomePass {
		t.Errorf("TestClean = %+v", clean)
	}
	if strings.Contains(problems, "TestClean") {
		t.Errorf("a clean test is named in the problems:\n%s", problems)
	}
	// -short reached the binary: the scale test skipped, and the ledger says
	// so without making it a problem of its own.
	var scale entry
	for _, e := range l.Tests {
		if e.Test == "TestScaleSkippedByShort" {
			scale = e
		}
	}
	if scale.Outcome != outcomeSkip {
		t.Errorf("TestScaleSkippedByShort = %+v: -short did not reach the test binary", scale)
	}
	if _, err := os.Stat(filepath.Join(out, "race.events.jsonl")); err != nil {
		t.Errorf("the gate keeps its events as race.events.jsonl: %v", err)
	}
	if _, err := os.Stat(filepath.Join(out, "unit-tests.json")); err == nil {
		t.Error("the race gate wrote the unit gate's ledger")
	}
}

func TestUnitRunner_TheRaceGatePassesACleanPackage(t *testing.T) {
	if testing.Short() {
		t.Skip("runs go test -race")
	}
	requireRaceToolchain(t)
	quiet(t)
	out := t.TempDir()
	if code := cmdUnit([]string{"-out", out, "-allow", "", "-race", "-short", "./testdata/fakesuite"}); code != 0 {
		t.Fatalf("exit %d, want 0", code)
	}
	var l ledger
	if err := readJSON(filepath.Join(out, "race-tests.json"), &l); err != nil {
		t.Fatal(err)
	}
	if l.Summary.Expected != 1 || l.Summary.Pass != 1 || !l.ok() {
		t.Errorf("ledger = %+v", l)
	}
}

func TestUnitRunner_ARepeatedRaceRunFailsATestThatFailsOnlyOnce(t *testing.T) {
	if testing.Short() {
		t.Skip("runs go test -race")
	}
	requireRaceToolchain(t)
	quiet(t)
	out := t.TempDir()
	code := cmdUnit([]string{"-out", out, "-allow", "", "-race", "-short", "-count", "3", "./testdata/flakysuite"})
	if code != 1 {
		t.Fatalf("exit %d, want 1: TestFailsOnTheSecondRun fails once in three", code)
	}
	var l ledger
	if err := readJSON(filepath.Join(out, "race-repeat-tests.json"), &l); err != nil {
		t.Fatalf("a repeated race run's ledger is race-repeat-tests.json: %v", err)
	}
	if l.Suite != suiteRaceRepeat {
		t.Errorf("suite = %q", l.Suite)
	}
	var got entry
	for _, e := range l.Tests {
		if e.Test == "TestFailsOnTheSecondRun" {
			got = e
		}
	}
	if got.Outcome != outcomeFail {
		t.Errorf("TestFailsOnTheSecondRun = %+v: a later pass hid the failure", got)
	}
}

// ---- review findings: what the gate runs is what -short, -count and the toolchain say ----

// raceToolchainMissing says what the race detector lacks on this host, or
// returns "" when it has what it needs: cgo, and a C compiler on PATH. The
// unit gate runs the tests below, so a host without them is a missing
// prerequisite, named, and never an opaque `go: -race requires cgo`.
func raceToolchainMissing() string {
	out, err := exec.Command("go", "env", "CGO_ENABLED", "CC").Output()
	if err != nil {
		return "go env: " + err.Error()
	}
	fields := strings.Fields(string(out))
	if len(fields) < 1 || fields[0] != "1" {
		return "CGO_ENABLED is not 1"
	}
	if len(fields) < 2 {
		return "go env CC names no C compiler"
	}
	if _, err := exec.LookPath(fields[1]); err != nil {
		return "no C compiler on PATH: " + err.Error()
	}
	return ""
}

func requireRaceToolchain(t *testing.T) {
	t.Helper()
	if why := raceToolchainMissing(); why != "" {
		t.Fatalf("prerequisite missing: the race detector needs cgo (CGO_ENABLED=1) and a C compiler on PATH (go env CC): %s", why)
	}
}

func TestRaceToolchain_ACgoLessHostIsAMissingPrerequisiteWithAReason(t *testing.T) {
	t.Setenv("CGO_ENABLED", "0")
	if why := raceToolchainMissing(); !strings.Contains(why, "CGO_ENABLED") {
		t.Errorf("a host with cgo off is missing nothing? reason %q", why)
	}
}

// go test's -timeout runs from the start of a test binary to its end, across
// every -count repeat of its tests, so a repeat of N runs needs N runs' time.
func TestRaceTimeout_ScalesWithTheRepeatCount(t *testing.T) {
	for count, want := range map[int]string{1: "30m0s", 2: "1h0m0s", 3: "1h30m0s", 5: "2h30m0s"} {
		if got := raceTimeoutFor(count); got != want {
			t.Errorf("raceTimeoutFor(%d) = %s, want %s", count, got, want)
		}
	}
}

func TestUnitRunner_TheRaceTimeoutSpansEveryRepeat(t *testing.T) {
	if testing.Short() {
		t.Skip("runs go test -race")
	}
	requireRaceToolchain(t)
	quiet(t)
	for _, c := range []struct{ count, minutes int }{{1, 30}, {3, 90}} {
		t.Run(fmt.Sprintf("count %d", c.count), func(t *testing.T) {
			out := t.TempDir()
			args := []string{"-out", out, "-allow", "", "-race", "-short"}
			if c.count > 1 {
				args = append(args, "-count", strconv.Itoa(c.count))
			}
			if code := cmdUnit(append(args, "./testdata/deadlinesuite")); code != 0 {
				t.Fatalf("exit %d, want 0", code)
			}
			events, err := os.ReadFile(filepath.Join(out, unitSuiteName(true, c.count)+".events.jsonl"))
			if err != nil {
				t.Fatal(err)
			}
			want := fmt.Sprintf("timeout-minutes=%d", c.minutes)
			if !strings.Contains(string(events), want) {
				t.Errorf("the test binary was not given a %d-minute -timeout for %d run(s): no %q in its events:\n%s", c.minutes, c.count, want, events)
			}
		})
	}
}

// make test-race is the gate's go test without its ledger: the same flags,
// the timeout included, for the same repeat count.
func TestMakefile_TestRaceGivesGoTestTheGatesTimeout(t *testing.T) {
	timeoutFlag := regexp.MustCompile(`-timeout (\S+)`)
	for _, count := range []int{1, 3} {
		t.Run(fmt.Sprintf("RACE_COUNT=%d", count), func(t *testing.T) {
			out, err := exec.Command("make", "-n", "-s", "-C", mustRepoRoot(), "test-race",
				fmt.Sprintf("RACE_COUNT=%d", count), "RACE_PACKAGES=./pkg/triggers").CombinedOutput()
			if err != nil {
				t.Fatalf("make -n test-race: %v\n%s", err, out)
			}
			m := timeoutFlag.FindStringSubmatch(string(out))
			if m == nil {
				t.Fatalf("make test-race runs go test with no -timeout, so the default ten minutes bound a repeat of the engine's suite:\n%s", out)
			}
			got, err := time.ParseDuration(m[1])
			if err != nil {
				t.Fatalf("-timeout %q: %v", m[1], err)
			}
			want, _ := time.ParseDuration(raceTimeoutFor(count))
			if got != want {
				t.Errorf("make test-race RACE_COUNT=%d gives -timeout %s, the gate gives %s", count, got, want)
			}
			for _, flag := range []string{"-race", "-short", fmt.Sprintf("-count=%d", count)} {
				if !strings.Contains(string(out), flag) {
					t.Errorf("make test-race lacks %s:\n%s", flag, out)
				}
			}
		})
	}
}

// -short is what makes a skip a thing the race ledger records instead of
// refuses. A race run without it follows the unit gate's rules, a skip is a
// problem there, and the ledger says which kind of run it was.
func TestRaceLedger_ASkipIsOnlyRecordedWhenTheRunHadShort(t *testing.T) {
	listed, expected := raceListing("TestSkips", "TestSub")
	events := []event{
		ev("run", racePkg, "TestSkips", 0), ev("skip", racePkg, "TestSkips", 0),
		ev("run", racePkg, "TestSub", 0), ev("run", racePkg, "TestSub/inner", 0), ev("skip", racePkg, "TestSub/inner", 0), ev("pass", racePkg, "TestSub", 0),
	}
	collect := func() *collector {
		c := newCollector()
		for _, e := range events {
			c.add(e, "race")
		}
		return c
	}
	with := buildRaceLedger(suiteRace, true, expected, listed, collect(), nil)
	if !with.ok() || !with.Short {
		t.Errorf("a -short run: ok %v, short %v, problems %v: the skips are recorded, not refused", with.ok(), with.Short, with.Problems)
	}
	without := buildRaceLedger(suiteRace, false, expected, listed, collect(), nil)
	if without.ok() || without.Short {
		t.Fatalf("a run without -short: ok %v, short %v: a skip is never a pass", without.ok(), without.Short)
	}
	problems := strings.Join(without.Problems, "\n")
	for _, want := range []string{racePkg + ".TestSkips skipped", racePkg + ".TestSub/inner skipped", "a skip is never a pass"} {
		if !strings.Contains(problems, want) {
			t.Errorf("problems lack %q:\n%s", want, problems)
		}
	}
	// The reason a skip carries says what is known, and no more: the run had
	// -short, and the test's own reason is in the gate's log.
	for _, e := range with.Tests {
		if e.Test == "TestSkips" && (!strings.Contains(e.Reason, "run with -short") || strings.Contains(e.Reason, "skipped under -short")) {
			t.Errorf("TestSkips reason = %q: it claims a cause the ledger does not know", e.Reason)
		}
	}
}

func TestUnitRunner_TheLedgerSaysWhetherTheRunHadShort(t *testing.T) {
	if testing.Short() {
		t.Skip("runs go test -race")
	}
	requireRaceToolchain(t)
	quiet(t)
	for _, short := range []bool{true, false} {
		t.Run(fmt.Sprintf("short=%v", short), func(t *testing.T) {
			out := t.TempDir()
			args := []string{"-out", out, "-allow", "", "-race"}
			if short {
				args = append(args, "-short")
			}
			if code := cmdUnit(append(args, "./testdata/fakesuite")); code != 0 {
				t.Fatalf("exit %d, want 0", code)
			}
			var l ledger
			if err := readJSON(filepath.Join(out, "race-tests.json"), &l); err != nil {
				t.Fatal(err)
			}
			if l.Short != short {
				t.Errorf("ledger short = %v, want %v", l.Short, short)
			}
		})
	}
}

// -count repeats a test in one process; a repeat that starts and never
// reports (the binary ended, hung or timed out in it) must not leave the
// earlier repeat's pass standing as the test's outcome.
func TestCollector_AStickyRepeatThatNeverReportedIsNoOutcome(t *testing.T) {
	listed, expected := raceListing("TestDies", "TestFine")
	c := newCollector()
	c.sticky = true
	for _, e := range []event{
		ev("run", racePkg, "TestDies", 0), ev("pass", racePkg, "TestDies", 1),
		ev("run", racePkg, "TestDies", 0), // the binary ends here
		ev("run", racePkg, "TestFine", 0), ev("pass", racePkg, "TestFine", 1),
		ev("run", racePkg, "TestFine", 0), ev("pass", racePkg, "TestFine", 1),
	} {
		c.add(e, "race")
	}
	l := buildRaceLedger(suiteRaceRepeat, true, expected, listed, c, nil)
	if l.ok() || l.Summary.NoOutcome != 1 || l.Summary.Pass != 1 {
		t.Fatalf("ledger %+v, problems %v: want TestDies with no outcome and TestFine passed", l.Summary, l.Problems)
	}
	problems := strings.Join(l.Problems, "\n")
	if !strings.Contains(problems, racePkg+".TestDies no outcome") || strings.Contains(problems, "TestFine") {
		t.Errorf("problems do not name the test that never reported, and only it:\n%s", problems)
	}
	// A repeat that reports clears the mark, whatever it reports first.
	again := newCollector()
	again.sticky = true
	for _, e := range []event{
		ev("run", racePkg, "TestDies", 0), ev("run", racePkg, "TestDies", 0), ev("pass", racePkg, "TestDies", 1),
	} {
		again.add(e, "race")
	}
	if l := buildRaceLedger(suiteRaceRepeat, true, []testKey{{racePkg, "TestDies"}}, map[string]map[string]bool{racePkg: {"TestDies": true}}, again, nil); l.ok() {
		// two runs started, one reported: the second never did.
		t.Errorf("a repeat that started twice and reported once is ok: %+v", l.Summary)
	}
}

func TestUnitRunner_ARepeatThatEndsTheBinaryNamesItsTest(t *testing.T) {
	if testing.Short() {
		t.Skip("runs go test -race")
	}
	requireRaceToolchain(t)
	quiet(t)
	out := t.TempDir()
	code := cmdUnit([]string{"-out", out, "-allow", "", "-race", "-short", "-count", "3", "./testdata/diesuite"})
	if code != 1 {
		t.Fatalf("exit %d, want 1: the binary ends in the second run", code)
	}
	var l ledger
	if err := readJSON(filepath.Join(out, "race-repeat-tests.json"), &l); err != nil {
		t.Fatal(err)
	}
	problems := strings.Join(l.Problems, "\n")
	if !strings.Contains(problems, "TestDiesOnTheSecondRun no outcome") {
		t.Errorf("the test whose repeat never reported is not named:\n%s", problems)
	}
	for _, e := range l.Tests {
		if e.Test == "TestDiesOnTheSecondRun" && e.Outcome == outcomePass {
			t.Errorf("TestDiesOnTheSecondRun = %+v: the first repeat's pass stands for a test that died in the second", e)
		}
	}
}
