package main

import (
	"testing"
)

// unitRun feeds events to a collector and builds the unit ledger with a
// current quarantine entry for p.TestPin/a_pin, as cmdUnit does.
func unitRun(events []event, listed map[string]map[string]bool) (ledger, *collector) {
	c := newCollector()
	for _, e := range events {
		c.add(e, "unit")
	}
	var expected []testKey
	for pkg, names := range listed {
		for n := range names {
			expected = append(expected, testKey{pkg, n})
		}
	}
	q := map[testKey]quarantineEntry{{"example.com/p", "TestPin/a_pin"}: {Suite: "unit", Package: "example.com/p", Test: "TestPin/a_pin"}}
	return buildLedgerQ("unit", expected, listed, c, nil, q), c
}

func pinFailure() []event {
	const p = "example.com/p"
	return []event{
		ev("run", p, "TestPin", 0), ev("run", p, "TestPin/a_pin", 0), ev("fail", p, "TestPin/a_pin", 1), ev("fail", p, "TestPin", 1),
		{Action: "fail", Package: p},
		ev("run", "example.com/q", "TestQ", 0), ev("pass", "example.com/q", "TestQ", 0),
		{Action: "pass", Package: "example.com/q"},
	}
}

func TestUnit_OnlyQuarantinedFailuresDoNotFailTheRun(t *testing.T) {
	listed := map[string]map[string]bool{"example.com/p": {"TestPin": true}, "example.com/q": {"TestQ": true}}
	l, c := unitRun(pinFailure(), listed)
	if l.Summary.Quarantined != 1 || !l.ok() {
		t.Fatalf("ledger = %+v", l)
	}
	// go test exits 1 because a test failed; every failure is quarantined, so
	// the exit says nothing the ledger does not, and is dropped.
	if !unitExitExcused(l, c.pkgEnds, listed) {
		t.Error("an exit caused only by a quarantined failure still fails the run")
	}
}

func TestUnit_ABuildFailureBesideAQuarantinedFailureStillFails(t *testing.T) {
	listed := map[string]map[string]bool{"example.com/p": {"TestPin": true}, "example.com/q": {"TestQ": true}}
	cases := map[string][]event{
		// A package that failed to build reports only a package-level fail.
		"a build failure": append(pinFailure(), event{Action: "fail", Package: "example.com/broken"}),
		// A package whose binary died (or a TestMain that failed) never
		// reported its own terminal event.
		"a package with no terminal event": pinFailure()[:7],
		// A package-level failure outside any test, beside the quarantined one.
		"a package-level failure": func() []event {
			e := pinFailure()
			e[len(e)-1] = event{Action: "fail", Package: "example.com/q"}
			return e
		}(),
	}
	for name, events := range cases {
		t.Run(name, func(t *testing.T) {
			l, c := unitRun(events, listed)
			if unitExitExcused(l, c.pkgEnds, listed) {
				t.Error("the exit was dropped beside a failure the ledger does not explain")
			}
		})
	}
	// An ordinary failure is never excused, quarantine or not.
	l, c := unitRun(append(pinFailure(), ev("run", "example.com/q", "TestR", 0), ev("fail", "example.com/q", "TestR", 1)),
		map[string]map[string]bool{"example.com/p": {"TestPin": true}, "example.com/q": {"TestQ": true, "TestR": true}})
	if unitExitExcused(l, c.pkgEnds, nil) {
		t.Error("an unquarantined failure was excused")
	}
}
