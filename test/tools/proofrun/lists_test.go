package main

import (
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func quarantineLine(suite, pkg, test, owner, since, review, reason string) string {
	return strings.Join([]string{suite, pkg, test, owner, since, review, reason}, "\t")
}

func TestParseQuarantine_AcceptsTheApprovedShape(t *testing.T) {
	const p = "github.com/orkstack/orkstack/internal/engine"
	text := quarantineLine("unit", p, "TestPin/a_pin", "Daniel Santos <danlsgiga@gmail.com>", "2026-10-19", "2026-11-18", "known flake")
	entries, err := parseQuarantine(strings.NewReader(text), "q")
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 {
		t.Fatalf("entries = %d", len(entries))
	}
	e := entries[0]
	if e.Suite != "unit" || e.Package != p || e.Test != "TestPin/a_pin" || e.Owner != "Daniel Santos <danlsgiga@gmail.com>" ||
		e.Since != "2026-10-19" || e.ReviewBy != "2026-11-18" || e.Reason != "known flake" {
		t.Errorf("entry = %+v", e)
	}
}

func TestParseQuarantine_RefusesRowsThatDoNotCarryTheDebt(t *testing.T) {
	const p = "example.com/p"
	base := quarantineLine("unit", p, "TestX", "Daniel Santos <danlsgiga@gmail.com>", "2026-10-19", "2026-11-18", "why")
	cases := map[string]string{
		"six columns":        strings.Join(strings.Split(base, "\t")[:6], "\t"),
		"empty reason":       quarantineLine("unit", p, "TestX", "Daniel Santos <danlsgiga@gmail.com>", "2026-10-19", "2026-11-18", ""),
		"bad suite":          quarantineLine("soak", p, "TestX", "Daniel Santos <danlsgiga@gmail.com>", "2026-10-19", "2026-11-18", "why"),
		"owner without mail": quarantineLine("unit", p, "TestX", "Daniel", "2026-10-19", "2026-11-18", "why"),
		"bad since":          quarantineLine("unit", p, "TestX", "Daniel Santos <danlsgiga@gmail.com>", "19/10/2026", "2026-11-18", "why"),
		"review before":      quarantineLine("unit", p, "TestX", "Daniel Santos <danlsgiga@gmail.com>", "2026-10-19", "2026-10-18", "why"),
		"review over 30d":    quarantineLine("unit", p, "TestX", "Daniel Santos <danlsgiga@gmail.com>", "2026-10-19", "2026-11-19", "why"),
		"not a test name":    quarantineLine("unit", p, "not-a-test", "Daniel Santos <danlsgiga@gmail.com>", "2026-10-19", "2026-11-18", "why"),
	}
	for name, text := range cases {
		if _, err := parseQuarantine(strings.NewReader(text), "q"); err == nil {
			t.Errorf("%s: parsed, want a refusal", name)
		}
	}
}

func TestParseQuarantine_RefusesADuplicateAndASixthEntry(t *testing.T) {
	const p = "example.com/p"
	line := quarantineLine("unit", p, "TestX", "Daniel Santos <danlsgiga@gmail.com>", "2026-10-19", "2026-11-18", "why")
	if _, err := parseQuarantine(strings.NewReader(line+"\n"+line), "q"); err == nil {
		t.Error("a duplicate parsed")
	}
	var six []string
	for _, test := range []string{"TestA", "TestB", "TestC", "TestD", "TestE", "TestF"} {
		six = append(six, quarantineLine("unit", p, test, "Daniel Santos <danlsgiga@gmail.com>", "2026-10-19", "2026-11-18", "why"))
	}
	if _, err := parseQuarantine(strings.NewReader(strings.Join(six, "\n")), "q"); err == nil || !strings.Contains(err.Error(), "budget") {
		t.Fatalf("six entries: %v", err)
	}
}

func TestQuarantineSet_OnlyBaseEntriesAndUnexpiredDatesExcuse(t *testing.T) {
	const p = "example.com/p"
	tree := []quarantineEntry{
		{Suite: "unit", Package: p, Test: "TestInBase", Since: "2026-10-01", ReviewBy: "2026-10-31"},
		{Suite: "unit", Package: p, Test: "TestNewHere", Since: "2026-10-19", ReviewBy: "2026-11-18"},
	}
	base := []quarantineEntry{
		{Suite: "unit", Package: p, Test: "TestInBase"},
	}
	on := time.Date(2026, 10, 19, 0, 0, 0, 0, time.UTC)
	set := quarantineSet(tree, base, on)
	if _, ok := set[testKey{p, "TestInBase"}]; !ok {
		t.Error("an entry the base carries did not excuse")
	}
	if _, ok := set[testKey{p, "TestNewHere"}]; ok {
		t.Error("an entry the change adds excused itself")
	}
	expired := quarantineSet(tree, base, time.Date(2026, 11, 1, 0, 0, 0, 0, time.UTC))
	if _, ok := expired[testKey{p, "TestInBase"}]; ok {
		t.Error("an expired entry excused")
	}
}

func TestQuarantineProblems_StaleNamesAndAllowedSkips(t *testing.T) {
	const p = "example.com/p"
	listed := map[string]map[string]bool{p: {"TestLive": true}}
	entries := []quarantineEntry{
		{Suite: "unit", Package: p, Test: "TestGone", Line: 1},
		{Suite: "unit", Package: p, Test: "TestLive", Line: 2},
	}
	allow := []allowedSkip{{Suite: "unit", Package: p, Test: "TestLive", Reason: "recorder"}}
	problems := quarantineProblems(entries, listed, allow)
	if len(problems) != 2 {
		t.Fatalf("problems = %v", problems)
	}
	if !strings.Contains(problems[0], "does not list") || !strings.Contains(problems[1], "also an allowed skip") {
		t.Errorf("problems = %v", problems)
	}
}

func TestBuildLedgerQ_ExcusesOnlyWhenEveryFailingLeafIsQuarantined(t *testing.T) {
	const p = "example.com/p"
	c := newCollector()
	for _, e := range []event{
		ev("run", p, "TestPin", 0),
		ev("run", p, "TestPin/a_pin", 0), ev("fail", p, "TestPin/a_pin", 1),
		ev("fail", p, "TestPin", 1),
		ev("run", p, "TestMany", 0),
		ev("run", p, "TestMany/a", 0), ev("fail", p, "TestMany/a", 1),
		ev("run", p, "TestMany/b", 0), ev("fail", p, "TestMany/b", 1),
		ev("fail", p, "TestMany", 1),
	} {
		c.add(e, "j1")
	}
	listed := map[string]map[string]bool{p: {"TestPin": true, "TestMany": true}}
	expected := []testKey{{p, "TestPin"}, {p, "TestMany"}}
	q := map[testKey]quarantineEntry{
		{p, "TestPin/a_pin"}: {Suite: "unit", Package: p, Test: "TestPin/a_pin", Owner: "Daniel Santos <danlsgiga@gmail.com>", ReviewBy: "2026-11-18", Reason: "known flake"},
		{p, "TestMany/a"}:    {Suite: "unit", Package: p, Test: "TestMany/a", Reason: "half"},
	}
	l := buildLedgerQ("unit", expected, listed, c, nil, q)
	if l.Summary.Quarantined != 1 || l.Summary.Fail != 1 {
		t.Fatalf("summary = %+v", l.Summary)
	}
	if len(l.Problems) != 1 || !strings.Contains(l.Problems[0], "TestMany") {
		t.Errorf("problems = %v", l.Problems)
	}
	for _, e := range l.Tests {
		switch e.Test {
		case "TestPin":
			if e.Outcome != outcomeQuarantinedFail || len(e.Failures) != 1 || e.Failures[0] != "TestPin/a_pin" {
				t.Errorf("TestPin = %+v", e)
			}
		case "TestMany":
			if e.Outcome != outcomeFail || len(e.Failures) != 2 {
				t.Errorf("TestMany = %+v", e)
			}
		}
	}
}

func TestBuildLedgerQ_AnUnquarantinedTopLevelFailureIsNeverExcused(t *testing.T) {
	const p = "example.com/p"
	c := newCollector()
	c.add(ev("run", p, "TestPlain", 0), "j1")
	c.add(ev("fail", p, "TestPlain", 1), "j1")
	listed := map[string]map[string]bool{p: {"TestPlain": true}}
	q := map[testKey]quarantineEntry{{p, "TestPlain"}: {Suite: "unit", Package: p, Test: "TestPlain"}}
	l := buildLedgerQ("unit", []testKey{{p, "TestPlain"}}, listed, c, nil, q)
	if l.Summary.Fail != 1 || l.Summary.Quarantined != 0 {
		t.Errorf("summary = %+v", l.Summary)
	}
}

// declaredGoTests is every top-level Test a package declares, parsed from its
// test files without compiling anything. The repository lists are facts of the
// tree, so `make check` checks them against the tree.
func declaredGoTests(t *testing.T, root, pkg string) map[string]bool {
	t.Helper()
	dir := filepath.Join(root, strings.TrimPrefix(pkg, modulePath+"/"))
	files, err := readDirGo(dir)
	if err != nil {
		t.Errorf("read %s: %v", pkg, err)
		return map[string]bool{}
	}
	out := map[string]bool{}
	for _, f := range files {
		if !strings.HasSuffix(f.Name, "_test.go") {
			continue
		}
		for _, name := range parseGo(f).Tests {
			out[name] = true
		}
	}
	return out
}

// TestRepositoryNightlyList_CoveringTestsExistAndAreNotListed holds the
// committed nightly list to the tree: every moved test is declared by its
// package, every covering merge-gate test exists there too, and no covering
// test is itself on the list (plan §4).
func TestRepositoryNightlyList_CoveringTestsExistAndAreNotListed(t *testing.T) {
	root, err := repoRoot()
	if err != nil {
		t.Fatal(err)
	}
	entries, err := readNightlyList(filepath.Join(root, "scripts/proof-nightly-tests.txt"))
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) == 0 {
		t.Fatalf("the nightly list names no test; if the tier is gone, say so in PROOFS.md")
	}
	on := map[testKey]bool{}
	for _, e := range entries {
		on[e.key()] = true
	}
	for _, e := range entries {
		declared := declaredGoTests(t, root, e.Package)
		if !declared[e.Test] {
			t.Errorf("nightly list line %d names %s.%s, which its package does not declare", e.Line, e.Package, e.Test)
		}
		for _, covering := range e.Covering {
			if !declared[covering] {
				t.Errorf("nightly list line %d: covering test %s.%s is not declared by that package", e.Line, e.Package, covering)
			}
			if on[testKey{e.Package, covering}] {
				t.Errorf("nightly list line %d: covering test %s.%s is itself on the nightly list", e.Line, e.Package, covering)
			}
		}
	}
}

// TestRepositoryQuarantine_NamesListedTests checks the committed quarantine
// list's names and structure, never its dates: a date is a fact of the
// calendar, so only a proof judges it (plan §6.3).
func TestRepositoryQuarantine_NamesListedTests(t *testing.T) {
	root, err := repoRoot()
	if err != nil {
		t.Fatal(err)
	}
	entries, err := readQuarantine(filepath.Join(root, "scripts/proof-quarantine.txt"))
	if err != nil {
		t.Fatal(err)
	}
	allows, err := readAllowlist(filepath.Join(root, "scripts/proof-allowed-skips.txt"))
	if err != nil {
		t.Fatal(err)
	}
	allowed := map[testKey]bool{}
	for _, a := range allows {
		allowed[testKey{a.Package, a.Test}] = true
	}
	for _, e := range entries {
		if allowed[testKey{e.Package, e.Test}] {
			t.Errorf("quarantine entry %s.%s is also an allowed skip", e.Package, e.Test)
			continue
		}
		if e.Suite == "ui-e2e" {
			continue // a Playwright spec title, checked by the UI listing in a proof
		}
		top := strings.SplitN(e.Test, "/", 2)[0]
		declared := declaredGoTests(t, root, e.Package)
		if !declared[top] {
			t.Errorf("quarantine entry %s.%s: %s declares no such test", e.Package, e.Test, e.Package)
		}
	}
}

func TestQuarantine_AnEntryWithinSevenDaysOfItsDateWarns(t *testing.T) {
	const p = "example.com/p"
	entries := []quarantineEntry{
		{Suite: "unit", Package: p, Test: "TestSoon", ReviewBy: "2026-10-28"},
		{Suite: "unit", Package: p, Test: "TestToday", ReviewBy: "2026-10-21"},
		{Suite: "unit", Package: p, Test: "TestLater", ReviewBy: "2026-10-29"},
		{Suite: "unit", Package: p, Test: "TestExpired", ReviewBy: "2026-10-20"},
	}
	on := time.Date(2026, 10, 21, 0, 0, 0, 0, time.UTC)
	var got []string
	for _, e := range quarantineExpiring(entries, on) {
		got = append(got, e.Test)
	}
	if strings.Join(got, ",") != "TestSoon,TestToday" {
		t.Errorf("warned for %v, want TestSoon and TestToday (seven days and the day itself)", got)
	}
	var b strings.Builder
	printQuarantineWarnings(&b, quarantineExpiring(entries, on))
	if !strings.Contains(b.String(), "WARNING: quarantine entry for example.com/p.TestSoon expires on 2026-10-28 and stops excusing its failure") {
		t.Errorf("warning = %q", b.String())
	}
}

func TestNightlyProblems_ScopedRunSkipsUncompiledPackages(t *testing.T) {
	const engine, arts = "github.com/orkstack/orkstack/internal/engine", "github.com/orkstack/orkstack/pkg/artifacts"
	listed := map[string]map[string]bool{arts: {"TestA": true, "TestB": true}}
	entry := func(pkg, test string, covering ...string) nightlyEntry {
		return nightlyEntry{Package: pkg, Test: test, Covering: covering, Line: 7}
	}
	tests := []struct {
		name     string
		entries  []nightlyEntry
		complete bool
		want     string // substring of the one expected problem; empty means none
	}{
		{"scoped run skips an uncompiled package", []nightlyEntry{entry(engine, "TestIntegration_X", "TestY")}, false, ""},
		{"complete run refuses a non-integration package", []nightlyEntry{entry(engine, "TestIntegration_X", "TestY")}, true, "not an integration package of this proof"},
		{"scoped run still refuses a stale entry in a compiled package", []nightlyEntry{entry(arts, "TestGone", "TestA")}, false, "remove the stale entry"},
		{"scoped run still refuses a bad covering test", []nightlyEntry{entry(arts, "TestA", "TestMissing")}, false, "is not a listed test of that package"},
		{"complete run accepts a valid compiled entry", []nightlyEntry{entry(arts, "TestA", "TestB")}, true, ""},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := nightlyProblems(tc.entries, listed, tc.complete)
			if tc.want == "" {
				if len(got) != 0 {
					t.Fatalf("want no problem, got %q", got)
				}
				return
			}
			if len(got) != 1 || !strings.Contains(got[0], tc.want) {
				t.Fatalf("want one problem containing %q, got %q", tc.want, got)
			}
		})
	}
}
