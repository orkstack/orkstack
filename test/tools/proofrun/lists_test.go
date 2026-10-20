package main

import (
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
