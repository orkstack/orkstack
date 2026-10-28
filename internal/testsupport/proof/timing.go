package proof

import (
	"fmt"
	"math"
	"net/url"
	"regexp"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"testing"
)

// A test whose pass depends on how fast the host is reports that check
// through Timing, not through t.Errorf. A wall-clock budget calibrated on the
// reference host (docs/PROOFS.md §12) is then recorded in a form the proof's
// merger can read: it excuses the miss on a CI runner smaller than the
// reference, under a row of scripts/proof-host-speed.tsv the base carries, and
// only when the test failed nothing else. Counts, bytes, history shares,
// deadlock-detector trips and every correctness assertion never go through
// here: they keep failing the test where they are made.
//
// What the merger needs from a test, and how Timing gives it:
//
//   - The test's own record, timing/<test>.json under ORK_PROOF_DIR: every
//     check with its measured value and bound, and whether the test failed
//     anything that is not a timing check. It is written when the test ends,
//     so "anything" includes what a later check or a cleanup failed.
//   - The failure itself, last. A miss does not fail the test where it is
//     measured. The test's last cleanup first writes the record, then reports
//     each miss as one line of the fixed form MissLine writes, so a failing
//     test's output ends with exactly the helper's lines and nothing the test
//     itself said after them. The merger accepts a failure only when
//     everything after the first such line is such a line.
//
// Timing must therefore be created by TimingBounds before anything else the
// test registers a cleanup for: cleanups run last-in first-out, and the
// helper's must be the last to run.

// TimingGroup is the directory under ORK_PROOF_DIR a test's record is
// written to.
const TimingGroup = "timing"

// TimingFormat is the version of TimingRecord.
const TimingFormat = 1

// TimingCheck is one wall-clock check of a test.
type TimingCheck struct {
	ID    string `json:"id"`
	Label string `json:"label,omitempty"`
	// Measured and Bound are seconds; Measured is rounded to milliseconds,
	// the precision MissLine prints, so the record and the line agree.
	Measured float64 `json:"measured_seconds"`
	Bound    float64 `json:"bound_seconds"`
	Miss     bool    `json:"miss"`
}

// TimingRecord is timing/<test>.json.
type TimingRecord struct {
	Format  int    `json:"format"`
	Package string `json:"package"`
	Test    string `json:"test"`
	// Asserted is false for a test that only records its timing
	// (Timing.RecordOnly): its misses fail nothing.
	Asserted bool `json:"asserted"`
	// NonTimingFailed says the test had already failed something that is not
	// one of its timing checks when the record was written.
	NonTimingFailed bool          `json:"non_timing_failed"`
	Checks          []TimingCheck `json:"checks"`
}

// Misses are the record's checks that missed their bound.
func (r TimingRecord) Misses() []TimingCheck {
	var out []TimingCheck
	for _, c := range r.Checks {
		if c.Miss {
			out = append(out, c)
		}
	}
	return out
}

// TimingFile is the file name of test's record: the test's name, escaped so a
// subtest's "/" cannot name a directory.
func TimingFile(test string) string {
	return url.PathEscape(test) + ".json"
}

// missPrefix opens every miss line.
const missPrefix = "timing miss: "

// timingID is the shape of a check's id, and of the id column of a
// scripts/proof-host-speed.tsv row's `timing ID` assertion.
var timingID = regexp.MustCompile(`^[a-z][a-z0-9-]*$`)

// ValidTimingID reports whether id may name a timing check.
func ValidTimingID(id string) bool { return timingID.MatchString(id) }

// TimingMiss is one miss, as the line MissLine writes carries it.
type TimingMiss struct {
	ID       string
	Measured float64 // seconds
	Bound    float64 // seconds
	Label    string
}

// MissLine is the one line a missed check is reported as, without the
// "file.go:N: " the testing package puts in front of it:
//
//	timing miss: id=getrun-live-p95 measured=2.014s bound=2s label=reduced-fan-out
//
// The label is free text without a newline and is last; the measurement has
// the millisecond precision the record keeps.
func MissLine(m TimingMiss) string {
	line := fmt.Sprintf("%sid=%s measured=%ss bound=%ss", missPrefix, m.ID,
		strconv.FormatFloat(m.Measured, 'f', 3, 64), strconv.FormatFloat(m.Bound, 'f', -1, 64))
	if label := oneLine(m.Label); label != "" {
		line += " label=" + label
	}
	return line
}

// missLine reads a line of test output: what MissLine writes, after the
// "file.go:N: " the testing package prefixes to it.
var missLine = regexp.MustCompile(`^\s*(?:\S+\.go:[0-9]+: )?timing miss: id=([a-z][a-z0-9-]*) measured=([0-9]+(?:\.[0-9]+)?)s bound=([0-9]+(?:\.[0-9]+)?)s(?: label=(.*))?$`)

// ParseMissLine reads one line of a test's output as a timing miss. It
// returns false for any other line, whatever it says about timing.
func ParseMissLine(line string) (TimingMiss, bool) {
	m := missLine.FindStringSubmatch(strings.TrimRight(line, "\r\n"))
	if m == nil {
		return TimingMiss{}, false
	}
	measured, err1 := strconv.ParseFloat(m[2], 64)
	bound, err2 := strconv.ParseFloat(m[3], 64)
	if err1 != nil || err2 != nil {
		return TimingMiss{}, false
	}
	return TimingMiss{ID: m[1], Measured: measured, Bound: bound, Label: m[4]}, true
}

func oneLine(s string) string {
	return strings.Join(strings.Fields(s), " ")
}

// Timing collects one test's wall-clock checks. Its methods are safe for
// concurrent use.
type Timing struct {
	t        testing.TB
	pkg      string
	mu       sync.Mutex
	checks   []TimingCheck
	record   bool // RecordOnly
	settling bool
}

// TimingBounds returns the handle a test reports its wall-clock checks
// through, and registers the cleanup that writes the test's record and
// reports its misses. Call it first in the test, before anything else
// registers a cleanup (see the package comment), and once per test.
func TimingBounds(t testing.TB) *Timing {
	t.Helper()
	m := &Timing{t: t, pkg: callerPackage(2)}
	t.Cleanup(m.settle)
	return m
}

// RecordOnly makes the test keep its record but fail nothing for a miss: the
// measurement is evidence, not a claim of that test.
func (m *Timing) RecordOnly() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.record = true
}

// Check records one wall-clock check and reports whether it held. id names
// the check (a lowercase word with hyphens: the id column of a host-speed
// row), measured and bound are seconds, and label says which sample of the
// test it was when the test takes more than one. A miss does not fail the
// test here: the test's last cleanup reports every miss together, after the
// record.
func (m *Timing) Check(id string, measured, bound float64, label string) bool {
	m.t.Helper()
	if !ValidTimingID(id) {
		m.t.Fatalf("proof: timing id %q is not a lowercase word with hyphens", id)
		return false
	}
	ok := measured <= bound // a NaN is a miss
	rounded := math.Round(measured*1000) / 1000
	if math.IsNaN(measured) || math.IsInf(measured, 0) {
		rounded = 0
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.checks = append(m.checks, TimingCheck{ID: id, Label: oneLine(label), Measured: rounded, Bound: bound, Miss: !ok})
	return ok
}

// settle is the test's last cleanup: it writes the record, then reports each
// miss as the fixed line, and nothing after.
func (m *Timing) settle() {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.settling || m.t.Skipped() || len(m.checks) == 0 {
		return
	}
	m.settling = true
	// Whatever failed so far is not a timing check's: the misses are
	// reported only below.
	rec := TimingRecord{
		Format: TimingFormat, Package: m.pkg, Test: m.t.Name(), Asserted: !m.record,
		NonTimingFailed: m.t.Failed(), Checks: append([]TimingCheck(nil), m.checks...),
	}
	WriteJSON(m.t, TimingGroup, TimingFile(rec.Test), rec)
	if m.record {
		return
	}
	for _, c := range rec.Misses() {
		m.t.Errorf("%s", MissLine(TimingMiss{ID: c.ID, Measured: c.Measured, Bound: c.Bound, Label: c.Label}))
	}
}

// callerPackage is the import path of the package of the function skip frames
// above its own caller, without a trailing _test: the package a test
// binary's ledger names its tests under.
func callerPackage(skip int) string {
	pc, _, _, ok := runtime.Caller(skip)
	if !ok {
		return ""
	}
	fn := runtime.FuncForPC(pc)
	if fn == nil {
		return ""
	}
	name := fn.Name()
	slash := strings.LastIndex(name, "/")
	dot := strings.Index(name[slash+1:], ".")
	if dot < 0 {
		return ""
	}
	return strings.TrimSuffix(name[:slash+1+dot], "_test")
}
