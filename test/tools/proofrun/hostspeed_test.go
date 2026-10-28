package main

import (
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"testing"
)

func TestTimingMiss_IsOnlyTheBoundsOwnAssertion(t *testing.T) {
	const ansi = "\x1b[2mexpect(\x1b[22m\x1b[31mreceived\x1b[39m\x1b[2m).\x1b[22mtoBeLessThanOrEqual\x1b[2m(\x1b[22m\x1b[32mexpected\x1b[39m\x1b[2m)\x1b[22m; ; Expected: <= \x1b[32m5000\x1b[39m; Received:    \x1b[31m8123\x1b[39m"
	cases := []struct {
		name     string
		detail   string
		bound    float64
		observed float64
		ok       bool
	}{
		{"toBeLessThanOrEqual", timingDetail, 5, 8.123, true},
		{"toBeLessThan", "expect(received).toBeLessThan(expected); ; Expected: < 5000; Received:   5000", 5, 5, true},
		{"with ANSI colours", ansi, 5, 8.123, true},
		{"another bound", timingDetail, 9, 0, false},
		{"received under the bound", "expect(received).toBeLessThanOrEqual(expected); Expected: <= 5000; Received: 4999", 5, 0, false},
		{"another matcher", "expect(received).toBe(expected); Expected: 5000; Received: 8123", 5, 0, false},
		{"a visibility failure", "Error: expect(locator).toBeVisible() failed", 5, 0, false},
		{"a test timeout", "Test timeout of 30000ms exceeded.", 5, 0, false},
		{"a flaky result", "flaky: " + timingDetail, 5, 0, false},
		{"no detail", "", 5, 0, false},
		{"a test timeout ahead of the matcher", "Test timeout of 30000ms exceeded.; ; " + timingDetail, 5, 0, false},
		{"the matcher not at the start", "Test timeout of 30000ms exceeded.; " + timingDetail, 5, 0, false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got, ok := timingMiss(c.detail, c.bound)
			if ok != c.ok || got != c.observed {
				t.Errorf("timingMiss = %v %v, want %v %v", got, ok, c.observed, c.ok)
			}
		})
	}
}

func TestParseHostSpeed_IsStrict(t *testing.T) {
	good := "# comment\nui\te2e/a.spec.ts\tthe title\tlessThan 5000\t5\t16\ta note\n" +
		"ui\te2e/a.spec.ts\tthe title\tvisibleWithin 60000 getByTestId('run-status')\t5\t16\ta note\n" +
		"test\texample.com/m\tTestA\t-\t60\t16\ta note\n" +
		"test\texample.com/m\tTestB\ttiming getrun-live-p95\t2\t16\ta note\n" +
		"test\texample.com/m\tTestB\ttiming replay-slowest\t30\t16\ta note\n"
	tab, err := parseHostSpeed(strings.NewReader(good), "t")
	if err != nil || len(tab.UI) != 2 || len(tab.Tests) != 3 {
		t.Fatalf("a good table: %v %+v", err, tab)
	}
	if rows := tab.testRows("example.com/m", "TestB"); len(rows) != 2 || rows[0].Assertion.ID != "getrun-live-p95" ||
		rows[0].Assertion.String() != "timing getrun-live-p95" || rows[1].Assertion.ID != "replay-slowest" {
		t.Errorf("TestB's rows = %+v, want its two timing checks in table order", rows)
	}
	for name, bad := range map[string]string{
		"the old five columns":              "ui\te2e/a.spec.ts\t5\t16\ta note\n",
		"the six columns before assertions": "ui\te2e/a.spec.ts\tt\t5\t16\ta note\n",
		"an unknown assertion":              "ui\te2e/a.spec.ts\tt\ttoBeFast 5000\t5\t16\tnote\n",
		"visibleWithin without a locator":   "ui\te2e/a.spec.ts\tt\tvisibleWithin 60000\t5\t16\tnote\n",
		"a test row with a UI assertion":    "test\texample.com/m\tTestA\tlessThan 5\t60\t16\tnote\n",
		"a timing row with no id":           "test\texample.com/m\tTestA\ttiming\t60\t16\tnote\n",
		"a timing row with a bad id":        "test\texample.com/m\tTestA\ttiming GetRun\t60\t16\tnote\n",
		"a timing row with two ids":         "test\texample.com/m\tTestA\ttiming a b\t60\t16\tnote\n",
		"a UI row with a timing assertion":  "ui\te2e/a.spec.ts\tt\ttiming getrun-live-p95\t5\t16\tnote\n",
		"a duplicate timing row":            "test\texample.com/m\tTestA\ttiming a\t2\t16\tn\ntest\texample.com/m\tTestA\ttiming a\t2\t16\tn\n",
		"a duplicate row":                   "ui\te2e/a.spec.ts\tt\tlessThan 5000\t5\t16\tn\nui\te2e/a.spec.ts\tt\tlessThan 5000\t5\t16\tn\n",
		"an unknown kind":                   "spec\te2e/a.spec.ts\tt\tlessThan 5000\t5\t16\tnote\n",
		"an empty title":                    "ui\te2e/a.spec.ts\t \tlessThan 5000\t5\t16\tnote\n",
		"a bound of zero":                   "ui\te2e/a.spec.ts\tt\tlessThan 5000\t0\t16\tnote\n",
		"no reference":                      "ui\te2e/a.spec.ts\tt\tlessThan 5000\t5\tmany\tnote\n",
	} {
		if _, err := parseHostSpeed(strings.NewReader(bad), "t"); err == nil {
			t.Errorf("%s was accepted", name)
		}
	}
}

func TestAnchoredHostSpeed_KeepsOnlyRowsTheBaseDeclaresIdentically(t *testing.T) {
	row := func(bound, cpus string) string {
		return "ui\te2e/a.spec.ts\tt\tlessThan " + bound + "000\t" + bound + "\t" + cpus + "\tnote\n"
	}
	parse := func(s string) hostSpeedTable {
		tab, err := parseHostSpeed(strings.NewReader(s), "t")
		if err != nil {
			t.Fatal(err)
		}
		return tab
	}
	cases := []struct {
		name, tree, base string
		kept             bool
	}{
		{"the same row", row("5", "16"), row("5", "16"), true},
		{"a note edited", row("5", "16"), strings.Replace(row("5", "16"), "note", "older note", 1), true},
		{"a row the change adds", row("5", "16"), "", false},
		{"a looser bound", row("9", "16"), row("5", "16"), false},
		{"a higher reference", row("5", "64"), row("5", "16"), false},
		{"a row the change removes", "", row("5", "16"), false},
	}
	for _, c := range cases {
		got := anchoredHostSpeed(parse(c.tree), parse(c.base))
		if (len(got.UI) == 1) != c.kept {
			t.Errorf("%s: kept %v, want %v", c.name, len(got.UI) == 1, c.kept)
		}
	}
}

// A Go row counts only when the base declares the same test, check, bound and
// reference: a row the change adds, a bound it loosens, a check it renames or
// a reference it raises excuses nothing.
func TestAnchoredHostSpeed_KeepsOnlyGoRowsTheBaseDeclaresIdentically(t *testing.T) {
	row := func(id, bound, cpus string) string {
		return "test\texample.com/m\tTestA\ttiming " + id + "\t" + bound + "\t" + cpus + "\tnote\n"
	}
	parse := func(s string) hostSpeedTable {
		tab, err := parseHostSpeed(strings.NewReader(s), "t")
		if err != nil {
			t.Fatal(err)
		}
		return tab
	}
	cases := []struct {
		name, tree, base string
		kept             int
	}{
		{"the same row", row("a", "2", "16"), row("a", "2", "16"), 1},
		{"a note edited", row("a", "2", "16"), strings.Replace(row("a", "2", "16"), "note", "older", 1), 1},
		{"a row the change adds", row("a", "2", "16"), "", 0},
		{"a looser bound", row("a", "3", "16"), row("a", "2", "16"), 0},
		{"a higher reference", row("a", "2", "64"), row("a", "2", "16"), 0},
		{"another check", row("b", "2", "16"), row("a", "2", "16"), 0},
		{"one row of two is new", row("a", "2", "16") + row("b", "2", "16"), row("a", "2", "16"), 1},
		{"a dash row the base turned into a timing row", row("a", "2", "16"), "test\texample.com/m\tTestA\t-\t2\t16\tn\n", 0},
	}
	for _, c := range cases {
		if got := anchoredHostSpeed(parse(c.tree), parse(c.base)); len(got.Tests) != c.kept {
			t.Errorf("%s: kept %d rows, want %d", c.name, len(got.Tests), c.kept)
		}
	}
}

// The committed table names real Go tests: each test row's package exists, one
// of its test files declares the test, and (for a timing row) a test file of
// the package reports a check of that id through the proof helper, so a
// renamed test or check cannot leave a row that excuses nothing.
func TestRepositoryHostSpeed_NamesRealGoTests(t *testing.T) {
	root := mustRepoRoot()
	tab, err := readHostSpeed(filepath.Join(root, "scripts", "proof-host-speed.tsv"))
	if err != nil {
		t.Fatal(err)
	}
	const module = "github.com/orkstack/orkstack/"
	for _, b := range tab.Tests {
		dir := filepath.Join(root, strings.TrimPrefix(b.Name, module))
		if !strings.HasPrefix(b.Name, module) {
			t.Errorf("line %d: %s is not a package of this module", b.Line, b.Name)
			continue
		}
		files, err := filepath.Glob(filepath.Join(dir, "*_test.go"))
		if err != nil || len(files) == 0 {
			t.Errorf("line %d: %s has no test file", b.Line, b.Name)
			continue
		}
		var src strings.Builder
		for _, f := range files {
			text, err := os.ReadFile(f)
			if err != nil {
				t.Fatal(err)
			}
			src.Write(text)
			src.WriteByte('\n')
		}
		if !strings.Contains(src.String(), "\nfunc "+b.Member+"(t *testing.T)") {
			t.Errorf("line %d: %s declares no test %s", b.Line, b.Name, b.Member)
		}
		if b.Assertion.Kind == "timing" && !strings.Contains(src.String(), `Check("`+b.Assertion.ID+`"`) {
			t.Errorf("line %d: no test file of %s reports a timing check %q", b.Line, b.Name, b.Assertion.ID)
		}
	}
}

// The committed table names real tests: each UI row's spec file exists and
// declares a test with exactly the row's title, so a renamed test cannot
// leave a row that excuses nothing, or another test.
func TestRepositoryHostSpeed_NamesRealSpecTests(t *testing.T) {
	root := mustRepoRoot()
	tab, err := readHostSpeed(filepath.Join(root, "scripts", "proof-host-speed.tsv"))
	if err != nil {
		t.Fatal(err)
	}
	if len(tab.UI) == 0 {
		t.Fatal("the table declares no UI bound")
	}
	for _, b := range tab.UI {
		spec, err := os.ReadFile(filepath.Join(root, "ui", b.Name))
		if err != nil {
			t.Errorf("line %d: %v", b.Line, err)
			continue
		}
		// A spec may declare its test through a local module it imports
		// (ui/e2e/runPageCeiling.ts): the title is then the spec's own
		// `title: '…'`, and the assertions are the module's.
		src := string(spec)
		modules := ""
		for _, m := range regexp.MustCompile(`from '\./([A-Za-z0-9_-]+)'`).FindAllStringSubmatch(src, -1) {
			if b, err := os.ReadFile(filepath.Join(root, "ui", "e2e", m[1]+".ts")); err == nil {
				modules += string(b)
			}
		}
		declared := strings.Contains(src, "test('"+b.Member+"'") ||
			(strings.Contains(src, "title: '"+b.Member+"'") && strings.Contains(modules, "test(c.title,"))
		if !declared {
			t.Errorf("line %d: %s declares no test titled %q", b.Line, b.Name, b.Member)
		}
		src += modules
		// The declared assertion is one the spec really makes, so a row can
		// never excuse an assertion the spec does not have.
		flat := strings.ReplaceAll(src, "_", "")
		switch b.Assertion.Kind {
		case "lessThan":
			if !specAssertsBound(src, b.Assertion.MS) {
				t.Errorf("line %d: %s has no toBeLessThan[OrEqual](%g): the row's bound is not one the spec asserts", b.Line, b.Name, b.Assertion.MS)
			}
		case "visibleWithin":
			want := fmt.Sprintf("%s).toBeVisible({ timeout: %g })", b.Assertion.Locator, b.Assertion.MS)
			if !strings.Contains(flat, want) {
				t.Errorf("line %d: %s has no %q", b.Line, b.Name, want)
			}
		}
	}
}

// visibleFailure is how buildUILedger records Playwright 1.62's failed
// `toBeVisible({ timeout })`: the message, its newlines folded to "; ".
func visibleFailure(locator, ms string) string {
	return "Error: expect(locator).toBeVisible() failed; ; Locator: " + locator + "; Expected: visible; Timeout: " + ms +
		"ms; Error: element(s) not found; ; Call log:;   - Expect \"toBeVisible\" with timeout " + ms + "ms;   - waiting for " + locator
}

func TestVisibleWithinMiss_IsOnlyThatLocatorsTimeout(t *testing.T) {
	const runStatus = "getByTestId('run-status')"
	cases := []struct {
		name   string
		detail string
		ok     bool
	}{
		{"the run page's header never drew", visibleFailure(runStatus, "60000"), true},
		{"with ANSI colours", "\x1b[2mexpect(\x1b[22m\x1b[31mlocator\x1b[39m\x1b[2m).\x1b[22mtoBeVisible\x1b[2m(\x1b[22m\x1b[2m)\x1b[22m failed; ; Locator: getByTestId('run-status'); Expected: visible; Timeout: 60000ms; Error: element(s) not found", true},
		{"an older Playwright's wording", "Error: Timed out 60000ms waiting for expect(locator).toBeVisible(); ; Locator: getByTestId('run-status'); Expected: visible; Received: <element(s) not found>", true},
		{"another locator of the same spec", visibleFailure("getByTestId('task-t3999-inspect')", "60000"), false},
		{"a locator that only starts the same", visibleFailure("getByTestId('run-status-badge')", "60000"), false},
		{"another timeout", visibleFailure(runStatus, "30000"), false},
		{"a not-visible assertion", strings.Replace(visibleFailure(runStatus, "60000"), "toBeVisible", "not.toBeVisible", 1), false},
		{"the timing bound", timingDetail, false},
		{"a test timeout", "Test timeout of 1800000ms exceeded.", false},
		{"a flaky result", "flaky: " + visibleFailure(runStatus, "60000"), false},
		{"a strict mode violation", "Error: expect(locator).toBeVisible() failed; ; Locator: " + runStatus + "; Expected: visible; Error: strict mode violation: " + runStatus + " resolved to 2 elements; Timeout: 60000ms", false},
		{"resolved to two elements", visibleFailure(runStatus, "60000") + "; resolved to 2 elements", false},
		{"a test timeout carrying the pending expect", "Test timeout of 1800000ms exceeded.; Error: " + visibleFailure(runStatus, "60000"), false},
		{"another error after the timeout line", "Error: expect(locator).toBeVisible() failed; ; Locator: " + runStatus + "; Expected: visible; Timeout: 60000ms; Error: page.goto: net::ERR_CONNECTION_REFUSED", false},
		{"no timeout cause", "Error: expect(locator).toBeVisible() failed; ; Locator: " + runStatus + "; Expected: visible; Timeout: 60000ms", false},
		{"hidden at the timeout", "Error: expect(locator).toBeVisible() failed; ; Locator: " + runStatus + "; Expected: visible; Received: hidden; Timeout: 60000ms", true},
		{"an unexpected hidden value", "Error: expect(locator).toBeVisible() failed; ; Locator: " + runStatus + "; Expected: visible; Timeout: 60000ms; Error: unexpected value \"hidden\"", false},
		{"not anchored at the start", "prefix; " + visibleFailure(runStatus, "60000"), false},
		{"old wording without Expected: visible", "Error: Timed out 60000ms waiting for expect(locator).toBeVisible(); ; Locator: " + runStatus + "; Received: <element(s) not found>", false},
		{"old wording with no cause", "Error: Timed out 60000ms waiting for expect(locator).toBeVisible(); ; Locator: " + runStatus + "; Expected: visible", false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := visibleWithinMiss(c.detail, 60000, runStatus); got != c.ok {
				t.Errorf("visibleWithinMiss = %v, want %v", got, c.ok)
			}
		})
	}
}

var (
	specLessThanArg = regexp.MustCompile(`\.toBeLessThan(?:OrEqual)?\(\s*([A-Za-z_][A-Za-z0-9_]*|[0-9][0-9_]*(?:\.[0-9]+)?)\s*\)`)
	specConstNumber = regexp.MustCompile(`(?m)^\s*(?:export\s+)?const\s+([A-Za-z_][A-Za-z0-9_]*)\s*(?::\s*number\s*)?=\s*([0-9][0-9_]*(?:\.[0-9]+)?)\s*;`)
)

// specAssertsBound reports whether the spec source asserts toBeLessThan[OrEqual]
// of exactly ms, directly or through a numeric const it declares.
func specAssertsBound(src string, ms float64) bool {
	consts := map[string]float64{}
	for _, m := range specConstNumber.FindAllStringSubmatch(src, -1) {
		if v, err := strconv.ParseFloat(strings.ReplaceAll(m[2], "_", ""), 64); err == nil {
			consts[m[1]] = v
		}
	}
	for _, m := range specLessThanArg.FindAllStringSubmatch(src, -1) {
		arg := m[1]
		v, ok := consts[arg]
		if !ok {
			var err error
			if v, err = strconv.ParseFloat(strings.ReplaceAll(arg, "_", ""), 64); err != nil {
				continue
			}
		}
		if v == ms {
			return true
		}
	}
	return false
}

func TestSpecAssertsBound_TiesARowToTheSpecsOwnBound(t *testing.T) {
	src := "const BUDGET_MS = 5_000;\nconst OTHER = 60_000;\nexpect(x, 'm').toBeLessThanOrEqual(BUDGET_MS);\nexpect(y).toBeLessThan(250)\n"
	for ms, want := range map[float64]bool{5000: true, 250: true, 60000: false, 9000: false} {
		if got := specAssertsBound(src, ms); got != want {
			t.Errorf("bound %g: %v, want %v", ms, got, want)
		}
	}
	if specAssertsBound("expect(x).toBeLessThan(UNDECLARED)", 5000) {
		t.Error("an undeclared name was read as a bound")
	}
}
