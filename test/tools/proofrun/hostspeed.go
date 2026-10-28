package main

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"os"
	"regexp"
	"sort"
	"strconv"
	"strings"

	"github.com/orkstack/orkstack/internal/testsupport/proof"
)

// scripts/proof-host-speed.tsv names the assertions whose pass depends on the
// host's speed: a bound measured on a reference host with a stated CPU count.
// A UI spec's wall-clock bound missed on a CI part below that count is a
// recorded known gap, not a merge-blocking failure (the user's decision on
// R3-M5). The excusal is deliberately narrow:
//
//   - only a `failed` outcome, never a skip or a missing outcome;
//   - only when the failure is exactly the assertion the row declares, never
//     another assertion of the same test. A row declares one of two:
//     `lessThan MS`, the bound's own timing assertion
//     (`expect(received).toBeLessThan[OrEqual](expected)` with Expected MS),
//     or `visibleWithin MS LOCATOR`, a `toBeVisible({ timeout: MS })` on that
//     exact locator that timed out — the class-B miss of a page that never
//     drew within the spec's own wait on a small runner;
//   - only on a CI `ui-e2e:K` part, never the `local` part;
//   - only for a row the base's copy of the table carries too, so a change
//     cannot excuse its own failure (like the quarantine list).
//
// A Go test row declares `timing ID`: one wall-clock check of that test,
// reported through internal/testsupport/proof's Timing (gotiming.go). It is
// excused on a CI `integration:K` part under the same anchoring, and only
// when the ledger and the test's own timing record show nothing but that
// check failed. A Go test row with the assertion "-" is recorded on a miss
// and never excused.

// hostBound is one row of the table.
type hostBound struct {
	Kind          string // "ui" or "test"
	Name          string // a spec path under ui/, or a Go package
	Member        string // the Playwright test title, or the Go test name
	Assertion     hostAssertion
	Bound         float64
	ReferenceCPUs int
	Note          string
	Line          int
}

// hostAssertion is the one failure a row may excuse.
type hostAssertion struct {
	Kind    string  // "lessThan" or "visibleWithin" (UI); "timing" or "-" (a Go test row)
	MS      float64 // the assertion's Expected (lessThan) or its timeout (visibleWithin)
	Locator string  // visibleWithin: the locator exactly as Playwright prints it
	ID      string  // timing: the check's id, as the test's Timing reports it
}

func (a hostAssertion) String() string {
	switch a.Kind {
	case "lessThan":
		return fmt.Sprintf("lessThan %g", a.MS)
	case "visibleWithin":
		return fmt.Sprintf("visibleWithin %g %s", a.MS, a.Locator)
	case "timing":
		return "timing " + a.ID
	}
	return a.Kind
}

// parseAssertion reads the assertion column.
func parseAssertion(kind, text string) (hostAssertion, error) {
	if kind == "test" {
		if text == "-" {
			return hostAssertion{Kind: "-"}, nil
		}
		id, ok := strings.CutPrefix(text, "timing ")
		if !ok || !proof.ValidTimingID(id) {
			return hostAssertion{}, fmt.Errorf("a test row's assertion is \"-\" or \"timing ID\" (a lowercase word with hyphens), got %q", text)
		}
		return hostAssertion{Kind: "timing", ID: id}, nil
	}
	f := strings.SplitN(text, " ", 3)
	var a hostAssertion
	switch {
	case len(f) == 2 && f[0] == "lessThan":
		a.Kind = f[0]
	case len(f) == 3 && f[0] == "visibleWithin" && strings.TrimSpace(f[2]) != "":
		a.Kind, a.Locator = f[0], strings.TrimSpace(f[2])
	default:
		return hostAssertion{}, fmt.Errorf("assertion %q is not \"lessThan MS\" or \"visibleWithin MS LOCATOR\"", text)
	}
	ms, err := strconv.ParseFloat(f[1], 64)
	if err != nil || ms <= 0 {
		return hostAssertion{}, fmt.Errorf("assertion %q: %q is not a positive number of milliseconds", text, f[1])
	}
	a.MS = ms
	return a, nil
}

// same reports whether two rows declare the same bound: a row the change
// edits (another assertion, a looser bound, a higher reference) is not the
// base's row.
func (b hostBound) same(o hostBound) bool {
	return b.Kind == o.Kind && b.Name == o.Name && b.Member == o.Member && b.Assertion == o.Assertion &&
		b.Bound == o.Bound && b.ReferenceCPUs == o.ReferenceCPUs
}

// hostSpeedTable is the parsed table, keyed for lookups.
type hostSpeedTable struct {
	UI    map[string]hostBound // spec + "\x00" + title + "\x00" + assertion
	Tests map[string]hostBound // package + "\x00" + test + "\x00" + assertion
}

// testRowKey is a Go test row's key: a test may carry one row per check.
func testRowKey(pkg, test string, a hostAssertion) string {
	return pkg + "\x00" + test + "\x00" + a.String()
}

func emptyHostSpeed() hostSpeedTable {
	return hostSpeedTable{UI: map[string]hostBound{}, Tests: map[string]hostBound{}}
}

// readHostSpeed parses the table; an absent file is an empty table, because
// the base of a change may predate it.
func readHostSpeed(file string) (hostSpeedTable, error) {
	if file == "" {
		return emptyHostSpeed(), nil
	}
	b, err := os.ReadFile(file)
	if os.IsNotExist(err) {
		return emptyHostSpeed(), nil
	}
	if err != nil {
		return emptyHostSpeed(), err
	}
	return parseHostSpeed(bytes.NewReader(b), file)
}

// parseHostSpeed is strict: seven TAB-separated columns, kind, name, member,
// assertion, bound (seconds), reference CPUs and a note.
func parseHostSpeed(r io.Reader, name string) (hostSpeedTable, error) {
	out := emptyHostSpeed()
	sc := bufio.NewScanner(r)
	n := 0
	for sc.Scan() {
		n++
		text := sc.Text()
		if strings.TrimSpace(text) == "" || strings.HasPrefix(strings.TrimSpace(text), "#") {
			continue
		}
		f := strings.Split(text, "\t")
		if len(f) != 7 {
			return emptyHostSpeed(), fmt.Errorf("%s:%d: want \"kind<TAB>name<TAB>member<TAB>assertion<TAB>bound<TAB>reference_cpus<TAB>note\", got %d columns", name, n, len(f))
		}
		for i := range f {
			f[i] = strings.TrimSpace(f[i])
			if f[i] == "" {
				return emptyHostSpeed(), fmt.Errorf("%s:%d: column %d is empty", name, n, i+1)
			}
		}
		assertion, err := parseAssertion(f[0], f[3])
		if err != nil {
			return emptyHostSpeed(), fmt.Errorf("%s:%d: %v", name, n, err)
		}
		bound, err := strconv.ParseFloat(f[4], 64)
		if err != nil || bound <= 0 {
			return emptyHostSpeed(), fmt.Errorf("%s:%d: bound %q is not a positive number of seconds", name, n, f[4])
		}
		cpus, err := strconv.Atoi(f[5])
		if err != nil || cpus <= 0 {
			return emptyHostSpeed(), fmt.Errorf("%s:%d: reference CPUs %q is not a positive integer", name, n, f[5])
		}
		b := hostBound{Kind: f[0], Name: f[1], Member: f[2], Assertion: assertion, Bound: bound, ReferenceCPUs: cpus, Note: f[6], Line: n}
		switch f[0] {
		case "ui":
			key := f[1] + "\x00" + f[2] + "\x00" + assertion.String()
			if prev, dup := out.UI[key]; dup {
				return emptyHostSpeed(), fmt.Errorf("%s:%d: the same row is on line %d", name, n, prev.Line)
			}
			out.UI[key] = b
		case "test":
			key := testRowKey(f[1], f[2], assertion)
			if prev, dup := out.Tests[key]; dup {
				return emptyHostSpeed(), fmt.Errorf("%s:%d: the same row is on line %d", name, n, prev.Line)
			}
			out.Tests[key] = b
		default:
			return emptyHostSpeed(), fmt.Errorf("%s:%d: kind %q is not ui or test", name, n, f[0])
		}
	}
	return out, sc.Err()
}

// anchoredHostSpeed keeps the rows the base's copy declares identically: a
// row the change adds, or a bound it loosens, excuses nothing.
func anchoredHostSpeed(tree, base hostSpeedTable) hostSpeedTable {
	out := emptyHostSpeed()
	for k, b := range tree.UI {
		if o, ok := base.UI[k]; ok && b.same(o) {
			out.UI[k] = b
		}
	}
	for k, b := range tree.Tests {
		if o, ok := base.Tests[k]; ok && b.same(o) {
			out.Tests[k] = b
		}
	}
	return out
}

// specMatches compares a table row's spec path with a ledger entry's file,
// which Playwright reports relative to its testDir.
func specMatches(spec, file string) bool {
	return spec == file || strings.HasSuffix(spec, "/"+file) || strings.HasSuffix(file, "/"+spec)
}

// testRows are the rows that declare the Go test pkg.test, in table order.
func (t hostSpeedTable) testRows(pkg, test string) []hostBound {
	var out []hostBound
	for _, b := range t.Tests {
		if b.Name == pkg && b.Member == test {
			out = append(out, b)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Line < out[j].Line })
	return out
}

// uiBounds are the rows that declare e's test, in table order.
func (t hostSpeedTable) uiBounds(e uiEntry) []hostBound {
	var out []hostBound
	for _, b := range t.UI {
		if b.Member == e.Title && specMatches(b.Name, e.File) {
			out = append(out, b)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Line < out[j].Line })
	return out
}

// uiBound is the first row that declares e's test.
func (t hostSpeedTable) uiBound(e uiEntry) (hostBound, bool) {
	if bs := t.uiBounds(e); len(bs) > 0 {
		return bs[0], true
	}
	return hostBound{}, false
}

// miss reports whether a failure detail is exactly the row's declared
// assertion, and what it observed in seconds (0 when it says nothing).
func (b hostBound) miss(detail string) (float64, bool) {
	switch b.Assertion.Kind {
	case "lessThan":
		return lessThanMiss(detail, b.Assertion.MS)
	case "visibleWithin":
		if visibleWithinMiss(detail, b.Assertion.MS, b.Assertion.Locator) {
			return b.Assertion.MS / 1000, true
		}
	}
	return 0, false
}

var (
	ansiEscape     = regexp.MustCompile("\x1b\\[[0-9;]*m")
	pwBoundMatcher = regexp.MustCompile(`^(?:Error: )?(?:[^;]*; ; )?expect\(received\)\.toBeLessThan(?:OrEqual)?\(expected\)`)
	pwExpected     = regexp.MustCompile(`Expected:\s*<=?\s*([0-9]+(?:\.[0-9]+)?)`)
	pwReceived     = regexp.MustCompile(`Received:\s*([0-9]+(?:\.[0-9]+)?)`)
)

// timingMiss is lessThanMiss with the bound in seconds.
func timingMiss(detail string, boundSeconds float64) (float64, bool) {
	return lessThanMiss(detail, boundSeconds*1000)
}

// lessThanMiss reports whether a Playwright failure detail is a bound's own
// timing assertion: `expect(received).toBeLessThan[OrEqual](expected)` with
// Expected exactly ms and Received at or above it. It returns what was
// observed, in seconds. Any other failure — another assertion, a test
// timeout, a thrown error — is not a timing miss.
func lessThanMiss(detail string, ms float64) (float64, bool) {
	d := ansiEscape.ReplaceAllString(detail, "")
	if strings.HasPrefix(d, "flaky:") || strings.HasPrefix(d, "Test timeout") || !pwBoundMatcher.MatchString(d) {
		return 0, false
	}
	exp := pwExpected.FindStringSubmatch(d)
	rec := pwReceived.FindStringSubmatch(d)
	if exp == nil || rec == nil {
		return 0, false
	}
	expected, err1 := strconv.ParseFloat(exp[1], 64)
	received, err2 := strconv.ParseFloat(rec[1], 64)
	if err1 != nil || err2 != nil || expected != ms || received < expected {
		return 0, false
	}
	return received / 1000, true
}

var (
	pwVisibleMatcher = regexp.MustCompile(`^(?:Error: )?expect\(locator\)\.toBeVisible\(\) failed`)
	pwOldVisible     = regexp.MustCompile(`^(?:Error: )?Timed out ([0-9]+)ms waiting for expect\(locator\)\.toBeVisible\(\)`)
	pwLocatorLine    = regexp.MustCompile(`(?:^|; )Locator:\s+(.*?)\s*(?:;|$)`)
	pwTimeoutLine    = regexp.MustCompile(`(?:^|; )Timeout:\s+([0-9]+)ms\s*(?:;|$)`)
	pwErrorLine      = regexp.MustCompile(`(?:^|; )Error:\s+(.*?)\s*(?:;|$)`)
	pwHiddenCause    = regexp.MustCompile(`(?:^|; )Received:\s+(?:hidden|<element\(s\) not found>)\s*(?:;|$)|unexpected value "hidden"`)
	pwStrictMode     = regexp.MustCompile(`strict mode violation|resolved to [0-9]+ elements`)
)

// visibleWithinMiss reports whether a Playwright failure detail (newlines
// folded to "; " by the ledger) is `expect(locator).toBeVisible()` on exactly
// locator, timing out after exactly ms: Playwright 1.5x prints
// "expect(locator).toBeVisible() failed; ; Locator: L; Expected: visible;
// Timeout: Nms; Error: element(s) not found", and older releases "Timed out
// Nms waiting for expect(locator).toBeVisible(); ; Locator: L; Expected:
// visible; Received: <element(s) not found>". Playwright prints the Timeout
// line on every expect failure, so the detail must BEGIN with that assertion
// (a test timeout that carries the pending expect does not), carry a positive
// timeout cause (the element was not found, or was hidden), and have no other
// error after it: a strict-mode violation or any other error is a different
// failure. A `.not` assertion, another locator, another timeout or a timeout
// that did not fire is not this miss.
func visibleWithinMiss(detail string, ms float64, locator string) bool {
	d := ansiEscape.ReplaceAllString(detail, "")
	if strings.Contains(d, ".not.toBeVisible") || pwStrictMode.MatchString(d) {
		return false
	}
	loc := pwLocatorLine.FindStringSubmatch(d)
	if loc == nil || loc[1] != locator || !strings.Contains(d, "Expected: visible") {
		return false
	}
	var timeout string
	switch {
	case pwVisibleMatcher.MatchString(d):
		t := pwTimeoutLine.FindStringSubmatch(d)
		if t == nil {
			return false
		}
		timeout = t[1]
		cause := false
		rest := pwVisibleMatcher.ReplaceAllString(d, "")
		for _, e := range pwErrorLine.FindAllStringSubmatch(rest, -1) {
			if e[1] != "element(s) not found" {
				return false
			}
			cause = true
		}
		if !cause && !pwHiddenCause.MatchString(d) {
			return false
		}
	default:
		t := pwOldVisible.FindStringSubmatch(d)
		if t == nil || !pwHiddenCause.MatchString(d) {
			return false
		}
		timeout = t[1]
	}
	got, err := strconv.ParseFloat(timeout, 64)
	return err == nil && got == ms
}

// isCIUIPart reports whether part is a CI UI shard, the only place a
// runner-size miss may be excused.
func isCIUIPart(part string) bool {
	if !strings.HasPrefix(part, "ui-e2e:") {
		return false
	}
	n, err := strconv.Atoi(strings.TrimPrefix(part, "ui-e2e:"))
	return err == nil && n > 0
}

// uiKey is a UI entry's ledger name, the prefix of its problem line.
func uiKey(e uiEntry) string {
	return fmt.Sprintf("%s › %s [%s]", e.File, e.Title, e.Project)
}

// uiFailedProblem is the problem line buildUILedger writes for a failure.
func uiFailedProblem(e uiEntry) string {
	return uiKey(e) + " failed (" + orDash(e.Detail) + ")"
}

// uiRunnerSizeMiss decides whether e, on part, running on a host with cpus
// CPUs, is a runner-size miss the (anchored) table excuses.
func (t hostSpeedTable) uiRunnerSizeMiss(part string, cpus int, e uiEntry) (hostSpeedFailure, bool) {
	if !isCIUIPart(part) || e.Outcome != outcomeFail {
		return hostSpeedFailure{}, false
	}
	for _, b := range t.uiBounds(e) {
		if cpus <= 0 || cpus >= b.ReferenceCPUs {
			continue
		}
		observed, ok := b.miss(e.Detail)
		if !ok {
			continue
		}
		return hostSpeedFailure{
			Part: part, Test: uiKey(e), Kind: "ui", Assertion: b.Assertion.String(), Bound: b.Bound, Observed: observed,
			RunnerCPUs: cpus, ReferenceCPUs: b.ReferenceCPUs, Message: uiFailedProblem(e), Excused: true,
		}, true
	}
	return hostSpeedFailure{}, false
}
