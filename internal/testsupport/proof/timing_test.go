package proof_test

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/orkstack/orkstack/internal/testsupport/proof"
)

// fakeTB stands in for a test: it records what the helper reports and runs
// its cleanups last-in first-out when the "test" ends, as testing does.
type fakeTB struct {
	testing.TB
	name     string
	failed   bool
	skipped  bool
	fatal    []string
	out      []string // every Logf and Errorf, in order, as "log:" or "error:"
	cleanups []func()
}

func (f *fakeTB) Helper()      {}
func (f *fakeTB) Name() string { return f.name }
func (f *fakeTB) Failed() bool { return f.failed }
func (f *fakeTB) Skipped() bool {
	return f.skipped
}
func (f *fakeTB) Cleanup(fn func()) { f.cleanups = append(f.cleanups, fn) }
func (f *fakeTB) Logf(format string, args ...any) {
	f.out = append(f.out, "log: "+fmt.Sprintf(format, args...))
}

func (f *fakeTB) Errorf(format string, args ...any) {
	f.failed = true
	f.out = append(f.out, "error: "+fmt.Sprintf(format, args...))
}

func (f *fakeTB) Fatalf(format string, args ...any) {
	f.failed = true
	f.fatal = append(f.fatal, fmt.Sprintf(format, args...))
}

// end runs the cleanups, last registered first.
func (f *fakeTB) end() {
	for i := len(f.cleanups) - 1; i >= 0; i-- {
		f.cleanups[i]()
	}
}

func (f *fakeTB) errors() []string {
	var out []string
	for _, line := range f.out {
		if strings.HasPrefix(line, "error: ") {
			out = append(out, strings.TrimPrefix(line, "error: "))
		}
	}
	return out
}

func readRecord(t *testing.T, dir, test string) proof.TimingRecord {
	t.Helper()
	b, err := os.ReadFile(filepath.Join(dir, proof.TimingGroup, proof.TimingFile(test)))
	if err != nil {
		t.Fatalf("the test left no timing record: %v", err)
	}
	var rec proof.TimingRecord
	if err := json.Unmarshal(b, &rec); err != nil {
		t.Fatalf("timing record: %v\n%s", err, b)
	}
	return rec
}

func TestTiming_AMissIsReportedLastAsOneParseableLineAfterTheRecord(t *testing.T) {
	dir := t.TempDir()
	t.Setenv(proof.EnvDir, dir)
	f := &fakeTB{name: "TestIntegration_Soak"}
	timing := proof.TimingBounds(f)

	if !timing.Check("getrun-live-p95", 1.5, 2, "unbounded") {
		t.Error("a check within its bound reported a miss")
	}
	if timing.Check("getrun-live-p95", 2.0144, 2, "max-parallel") {
		t.Error("a check over its bound reported a hold")
	}
	if f.failed {
		t.Fatal("a miss failed the test where it was measured: it is reported only when the test ends")
	}
	f.end()

	errs := f.errors()
	if len(errs) != 1 {
		t.Fatalf("errors = %q, want exactly the one miss", errs)
	}
	miss, ok := proof.ParseMissLine("    timing.go:231: " + errs[0])
	if !ok || miss.ID != "getrun-live-p95" || miss.Measured != 2.014 || miss.Bound != 2 || miss.Label != "max-parallel" {
		t.Fatalf("the reported line %q parses as %+v, %v", errs[0], miss, ok)
	}
	if want := "timing miss: id=getrun-live-p95 measured=2.014s bound=2s label=max-parallel"; errs[0] != want {
		t.Errorf("miss line = %q, want %q", errs[0], want)
	}
	// The record is logged before the miss, and nothing follows the miss: a
	// failing test's output ends with the helper's lines.
	last := f.out[len(f.out)-1]
	if !strings.HasPrefix(last, "error: timing miss: ") {
		t.Errorf("the last thing the test said was %q", last)
	}
	rec := readRecord(t, dir, f.name)
	if rec.Format != proof.TimingFormat || rec.Test != f.name || !rec.Asserted || rec.NonTimingFailed {
		t.Errorf("record = %+v", rec)
	}
	if rec.Package != "github.com/orkstack/orkstack/internal/testsupport/proof" {
		t.Errorf("record package = %q: the ledger names a test under its package's import path", rec.Package)
	}
	if len(rec.Checks) != 2 || rec.Checks[0].Miss || !rec.Checks[1].Miss || rec.Checks[1].Measured != 2.014 ||
		rec.Checks[1].Bound != 2 || rec.Checks[1].Label != "max-parallel" {
		t.Errorf("checks = %+v", rec.Checks)
	}
}

func TestTiming_ARecordSaysWhetherTheTestFailedAnythingElse(t *testing.T) {
	for _, c := range []struct {
		name   string
		before bool // the test fails something before the helper settles
		other  bool // NonTimingFailed
	}{
		{"nothing else failed", false, false},
		{"something else failed", true, true},
	} {
		t.Run(c.name, func(t *testing.T) {
			dir := t.TempDir()
			t.Setenv(proof.EnvDir, dir)
			f := &fakeTB{name: "TestX"}
			timing := proof.TimingBounds(f)
			timing.Check("replay-slowest", 31, 30, "")
			if c.before {
				f.Errorf("a correctness assertion failed")
			}
			// A cleanup registered later runs earlier, and its failure is
			// seen too.
			f.Cleanup(func() {})
			f.end()
			if rec := readRecord(t, dir, "TestX"); rec.NonTimingFailed != c.other {
				t.Errorf("non_timing_failed = %v, want %v", rec.NonTimingFailed, c.other)
			}
			// The miss is reported either way: the merger decides.
			if errs := f.errors(); len(errs) == 0 || !strings.HasPrefix(errs[len(errs)-1], "timing miss: id=replay-slowest ") {
				t.Errorf("errors = %q", errs)
			}
		})
	}
}

func TestTiming_AFailureOfALaterCleanupIsNotATimingFailure(t *testing.T) {
	dir := t.TempDir()
	t.Setenv(proof.EnvDir, dir)
	f := &fakeTB{name: "TestX"}
	timing := proof.TimingBounds(f)
	f.Cleanup(func() { f.Errorf("teardown failed") }) // registered after, so it runs before
	timing.Check("events0-read", 31, 30, "")
	f.end()
	if rec := readRecord(t, dir, "TestX"); !rec.NonTimingFailed {
		t.Error("a cleanup that ran before the helper's failed the test, and the record says nothing else failed")
	}
}

// The soaks take a lock before their stack exists, so its release is
// registered before the helper and runs after the helper's report. It is
// silent, so a failing test's output still ends with the helper's line; the
// stack's own teardown, registered after the helper, speaks before it.
func TestTiming_ASilentCleanupRegisteredBeforeTheHelperLeavesTheTailIntact(t *testing.T) {
	dir := t.TempDir()
	t.Setenv(proof.EnvDir, dir)
	f := &fakeTB{name: "TestSoak"}
	released := false
	f.Cleanup(func() { released = true }) // the lock's release: first registered, last to run
	timing := proof.TimingBounds(f)
	f.Cleanup(func() { f.Logf("stack torn down") }) // the stack's teardown: runs before the report
	timing.Check("getrun-live-p95", 2.5, 2, "unbounded")
	f.end()
	if !released {
		t.Fatal("the cleanup registered before the helper did not run")
	}
	last := f.out[len(f.out)-1]
	if !strings.HasPrefix(last, "error: timing miss: ") {
		t.Errorf("the failing test's output ends with %q, want the helper's line", last)
	}
	if f.out[0] != "log: stack torn down" {
		t.Errorf("the teardown's output is %q first, want it before the helper's report: %q", f.out[0], f.out)
	}
}

func TestTiming_AHeldTestLeavesARecordAndNoFailure(t *testing.T) {
	dir := t.TempDir()
	t.Setenv(proof.EnvDir, dir)
	f := &fakeTB{name: "TestX"}
	timing := proof.TimingBounds(f)
	timing.Check("getrun-live-p95", 0.8, 2, "")
	timing.Check("replay-slowest", 30, 30, "") // at the bound holds
	f.end()
	if f.failed || len(f.errors()) != 0 {
		t.Errorf("a held test failed: %q", f.errors())
	}
	rec := readRecord(t, dir, "TestX")
	if len(rec.Checks) != 2 || len(rec.Misses()) != 0 {
		t.Errorf("record = %+v", rec)
	}
}

func TestTiming_ATestWithNoCheckLeavesNothing(t *testing.T) {
	dir := t.TempDir()
	t.Setenv(proof.EnvDir, dir)
	f := &fakeTB{name: "TestX"}
	proof.TimingBounds(f)
	f.end()
	if entries, _ := os.ReadDir(dir); len(entries) != 0 || len(f.out) != 0 {
		t.Errorf("a test that measured nothing left %v and said %q", entries, f.out)
	}
}

func TestTiming_ASkippedTestLeavesNothing(t *testing.T) {
	dir := t.TempDir()
	t.Setenv(proof.EnvDir, dir)
	f := &fakeTB{name: "TestX", skipped: true}
	proof.TimingBounds(f).Check("replay-slowest", 31, 30, "")
	f.end()
	if entries, _ := os.ReadDir(dir); len(entries) != 0 || f.failed {
		t.Errorf("a skipped test left %v, failed %v", entries, f.failed)
	}
}

func TestTiming_RecordOnlyKeepsTheRecordAndFailsNothing(t *testing.T) {
	dir := t.TempDir()
	t.Setenv(proof.EnvDir, dir)
	f := &fakeTB{name: "TestChaos"}
	timing := proof.TimingBounds(f)
	timing.RecordOnly()
	timing.Check("getrun-live-p95", 5, 2, "")
	f.end()
	if f.failed {
		t.Errorf("a record-only test failed: %q", f.errors())
	}
	if rec := readRecord(t, dir, "TestChaos"); rec.Asserted || len(rec.Misses()) != 1 {
		t.Errorf("record = %+v: a record-only test keeps its misses and says it asserted nothing", rec)
	}
}

func TestTiming_WithoutAProofDirectoryItStillReports(t *testing.T) {
	t.Setenv(proof.EnvDir, "")
	f := &fakeTB{name: "TestX"}
	proof.TimingBounds(f).Check("replay-slowest", 31, 30, "")
	f.end()
	if errs := f.errors(); len(errs) != 1 || !strings.HasPrefix(errs[0], "timing miss: ") {
		t.Errorf("errors = %q: the miss is reported with or without a record", errs)
	}
}

func TestTiming_ASubtestsRecordNamesTheSubtest(t *testing.T) {
	dir := t.TempDir()
	t.Setenv(proof.EnvDir, dir)
	f := &fakeTB{name: "TestA/unbounded"}
	proof.TimingBounds(f).Check("replay-slowest", 1, 30, "")
	f.end()
	if _, err := os.Stat(filepath.Join(dir, proof.TimingGroup, "TestA%2Funbounded.json")); err != nil {
		t.Errorf("a subtest's record must not name a directory: %v", err)
	}
	if rec := readRecord(t, dir, "TestA/unbounded"); rec.Test != "TestA/unbounded" {
		t.Errorf("record test = %q", rec.Test)
	}
}

func TestTiming_AnInvalidIDFailsTheTestAtOnce(t *testing.T) {
	for _, id := range []string{"", "GetRun", "a b", "1st", "a_b", "x\ny"} {
		f := &fakeTB{name: "TestX"}
		if proof.TimingBounds(f).Check(id, 1, 2, "") || len(f.fatal) != 1 {
			t.Errorf("id %q was accepted", id)
		}
	}
}

func TestTiming_ANaNOrInfiniteMeasurementIsAMiss(t *testing.T) {
	dir := t.TempDir()
	t.Setenv(proof.EnvDir, dir)
	f := &fakeTB{name: "TestX"}
	timing := proof.TimingBounds(f)
	zero := 0.0
	nan, inf := zero/zero, 1/zero // a runtime NaN and a runtime +Inf are not constants
	if timing.Check("replay-slowest", nan, 30, "") {
		t.Error("NaN held a bound")
	}
	if timing.Check("replay-slowest", inf, 30, "") {
		t.Error("+Inf held a bound")
	}
	f.end()
	if rec := readRecord(t, dir, "TestX"); len(rec.Misses()) != 2 {
		t.Errorf("record = %+v", rec)
	}
}

func TestTiming_ChecksFromManyGoroutinesAreAllKept(t *testing.T) {
	dir := t.TempDir()
	t.Setenv(proof.EnvDir, dir)
	f := &fakeTB{name: "TestX"}
	timing := proof.TimingBounds(f)
	var wg sync.WaitGroup
	for range 32 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			timing.Check("replay-slowest", 1, 30, "")
		}()
	}
	wg.Wait()
	f.end()
	if rec := readRecord(t, dir, "TestX"); len(rec.Checks) != 32 {
		t.Errorf("%d checks kept of 32", len(rec.Checks))
	}
}

func TestParseMissLine_IsExactlyWhatMissLineWrites(t *testing.T) {
	for _, m := range []proof.TimingMiss{
		{ID: "getrun-live-p95", Measured: 2.014, Bound: 2, Label: "reduced-fan-out"},
		{ID: "replay-slowest", Measured: 31.5, Bound: 30},
		{ID: "events0-read", Measured: 0.001, Bound: 0.5, Label: "a label with spaces and = signs"},
	} {
		line := proof.MissLine(m)
		got, ok := proof.ParseMissLine(line)
		if !ok || got != m {
			t.Errorf("%q parses as %+v, %v; want %+v", line, got, ok, m)
		}
		// The testing package prefixes the position, and test2json ends the line.
		if got, ok := proof.ParseMissLine("    timing.go:231: " + line + "\n"); !ok || got != m {
			t.Errorf("the line as the test log prints it parses as %+v, %v", got, ok)
		}
	}
	if line := proof.MissLine(proof.TimingMiss{ID: "x", Measured: 1, Bound: 2, Label: "two\nlines"}); strings.Contains(line, "\n") {
		t.Errorf("a miss line spans lines: %q", line)
	}
}

func TestParseMissLine_RefusesEveryOtherLine(t *testing.T) {
	for name, line := range map[string]string{
		"an empty line":           "",
		"a log line":              "    soak_test.go:12: GetRun live: [0.5 0.6]",
		"the old miss wording":    "E4 threshold missed at 4000 items (fan-out): GetRun p95 live 2.014s > 2s",
		"no id":                   "timing miss: measured=2.014s bound=2s",
		"an upper-case id":        "timing miss: id=GetRun measured=2.014s bound=2s",
		"no unit":                 "timing miss: id=a measured=2.014 bound=2s",
		"a negative number":       "timing miss: id=a measured=-2.014s bound=2s",
		"text before the prefix":  "x timing miss: id=a measured=2.014s bound=2s",
		"text after the bound":    "timing miss: id=a measured=2.014s bound=2s extra",
		"a position without .go":  "    soak:12: timing miss: id=a measured=2.014s bound=2s",
		"a second miss on a line": "timing miss: id=a measured=2.014s bound=2s label=x\ntiming miss: id=b measured=3s bound=2s",
	} {
		if m, ok := proof.ParseMissLine(line); ok {
			t.Errorf("%s: %q parsed as %+v", name, line, m)
		}
	}
}

func TestValidTimingID(t *testing.T) {
	for id, want := range map[string]bool{"getrun-live-p95": true, "a": true, "events0-read": true,
		"": false, "A": false, "-a": false, "a b": false, "a_b": false} {
		if got := proof.ValidTimingID(id); got != want {
			t.Errorf("ValidTimingID(%q) = %v, want %v", id, got, want)
		}
	}
}
