package main

import (
	"fmt"
	"sort"
	"strings"
)

// The race gate and its nightly repeat (AGENTS.md §11.2, docs/PROOFS.md §2.3).
//
// The race gate is the unit suites run under the race detector with -short:
// the scale tests the unit gate runs without -short stay out of it, because
// the detector multiplies a CPU-bound test's time several times over and a
// scale test asserts its own cost, not a concurrent behaviour. The ledger is
// the unit gate's — every listed test has an outcome, a failure fails the
// gate, a skip is never a pass — with the three differences below.
//
//   - A data race fails the gate whatever the tests reported. The detector
//     prints "WARNING: DATA RACE" and marks the running test failed, but a
//     report may also come from a goroutine that outlived every test, and a
//     gate that read only the test outcomes would pass it.
//   - A skip in a run that had -short is recorded as a skip, with a reason,
//     and is not a problem of the race ledger. The ledger records that the
//     run had -short (ledger.Short), and without it a skip is a problem, as
//     in the unit gate: the rule is -short's, not the race lane's. A skip is
//     not a pass either: the full proof's merger requires the unit gate, which
//     runs the same test without -short, to have passed it
//     (mergelanes.go, checkRaceAgainstUnit), so no test is covered by neither
//     lane. The nightly repeat has no unit ledger to hold its skips to; they
//     only narrow what it hunts, and its ledger counts them.
//   - No quarantine excuses a failure. A race failure is a finding, and the
//     quarantine list names flaky tests of the unit and integration suites.
//
// The nightly repeat is the same gate over a few concurrency-heavy packages
// with -count above 1, to catch a race that one run in several exposes. Its
// collector is sticky (ledger.go), so a failure in any repeat stands.

// The suites that have a ledger beside unit and integration. A suite's name
// is its gate's name, its ledger file's prefix (<suite>-tests.json) and, in a
// CI proof, its part's name.
const (
	suiteUnit       = "unit"
	suiteRace       = "race"
	suiteRaceRepeat = "race-repeat"
	suiteFuzz       = "fuzz"
)

// shortSkipReason is what a race ledger's skip entry says when no
// allowed-skip entry names it. It says what the ledger knows: the run had
// -short. Why the test skipped is the test's own, in the gate's log.
const shortSkipReason = "skipped in a run with -short (the test's own reason is in the gate's log); the unit gate runs this test without -short, and the full proof's merger requires that gate to have passed it"

// raceReportLine is the first line of every report the race detector prints.
const raceReportLine = "WARNING: DATA RACE"

// isRaceReport reports whether a test2json output chunk holds the detector's
// own report line. A test that mentions the words in a log line is not a
// report: only a line that is exactly the detector's counts.
func isRaceReport(output string) bool {
	if !strings.Contains(output, raceReportLine) {
		return false
	}
	for _, line := range strings.Split(output, "\n") {
		if strings.TrimSpace(line) == raceReportLine {
			return true
		}
	}
	return false
}

// outcomeSeverity orders the outcomes a repeated test can report: the worst
// one stands.
func outcomeSeverity(outcome string) int {
	switch outcome {
	case outcomeFail:
		return 3
	case outcomeSkip:
		return 2
	case outcomePass:
		return 1
	}
	return 0
}

func isRaceSuite(suite string) bool { return suite == suiteRace || suite == suiteRaceRepeat }

// skipEntryApplies reports whether an allowed-skip entry of entrySuite
// covers a ledger of ledgerSuite. An entry covers its own suite and `any`;
// the race suites run the unit gate's tests, so the unit entries cover them
// too, and a stale one is as stale there.
func skipEntryApplies(entrySuite, ledgerSuite string) bool {
	return entrySuite == ledgerSuite || entrySuite == "any" || (entrySuite == suiteUnit && isRaceSuite(ledgerSuite))
}

// unitSuiteName is the suite `proofrun unit` runs: the plain unit gate, the
// race gate, or the race gate repeated.
func unitSuiteName(race bool, count int) string {
	switch {
	case !race:
		return suiteUnit
	case count > 1:
		return suiteRaceRepeat
	}
	return suiteRace
}

// buildRaceLedger is the race suites' ledger: the unit builder with the skips
// of a -short run recorded rather than refused (short says the run had
// -short; without it the unit gate's rule stands), no quarantine, and a
// problem for every data race the detector reported.
func buildRaceLedger(suite string, short bool, expected []testKey, listed map[string]map[string]bool, c *collector, allow []allowedSkip) ledger {
	l := buildLedgerOpts(suite, expected, listed, c, allow, nil, short)
	l.Short = short
	l.Problems = append(l.Problems, raceProblems(c)...)
	return l
}

// raceProblems names every test, and the packages outside any test, that the
// detector reported a race in, in a stable order.
func raceProblems(c *collector) []string {
	keys := make([]testKey, 0, len(c.races))
	for k := range c.races {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool {
		if keys[i].Package != keys[j].Package {
			return keys[i].Package < keys[j].Package
		}
		return keys[i].Test < keys[j].Test
	})
	var out []string
	for _, k := range keys {
		n := c.races[k]
		plural := ""
		if n != 1 {
			plural = "s"
		}
		if k.Test == "" {
			out = append(out, fmt.Sprintf("%s reported a data race outside any test (%d report%s): the detector's output is in the gate's log", k.Package, n, plural))
			continue
		}
		out = append(out, fmt.Sprintf("%s reported a data race (%d report%s): the detector's output is in the gate's log", k, n, plural))
	}
	return out
}
