package main

import (
	"fmt"
	"os"
	"path/filepath"
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
		"test\texample.com/m\tTestA\t-\t60\t16\ta note\n"
	tab, err := parseHostSpeed(strings.NewReader(good), "t")
	if err != nil || len(tab.UI) != 2 || len(tab.Tests) != 1 {
		t.Fatalf("a good table: %v %+v", err, tab)
	}
	for name, bad := range map[string]string{
		"the old five columns":              "ui\te2e/a.spec.ts\t5\t16\ta note\n",
		"the six columns before assertions": "ui\te2e/a.spec.ts\tt\t5\t16\ta note\n",
		"an unknown assertion":              "ui\te2e/a.spec.ts\tt\ttoBeFast 5000\t5\t16\tnote\n",
		"visibleWithin without a locator":   "ui\te2e/a.spec.ts\tt\tvisibleWithin 60000\t5\t16\tnote\n",
		"a test row with an assertion":      "test\texample.com/m\tTestA\tlessThan 5\t60\t16\tnote\n",
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
		src, err := os.ReadFile(filepath.Join(root, "ui", b.Name))
		if err != nil {
			t.Errorf("line %d: %v", b.Line, err)
			continue
		}
		if !strings.Contains(string(src), "test('"+b.Member+"'") {
			t.Errorf("line %d: %s declares no test titled %q", b.Line, b.Name, b.Member)
		}
		// The declared assertion is one the spec really makes, so a row can
		// never excuse an assertion the spec does not have.
		flat := strings.ReplaceAll(string(src), "_", "")
		switch b.Assertion.Kind {
		case "lessThan":
			if !strings.Contains(flat, "toBeLessThan") {
				t.Errorf("line %d: %s asserts no bound with toBeLessThan[OrEqual]", b.Line, b.Name)
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
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := visibleWithinMiss(c.detail, 60000, runStatus); got != c.ok {
				t.Errorf("visibleWithinMiss = %v, want %v", got, c.ok)
			}
		})
	}
}
