package main

import (
	"fmt"
	"path/filepath"
	"sort"
)

// The ledgers of the race gate (full and release tiers) and of the nightly
// tier's two lanes. Each is the ledger of one gate, read from the part that
// ran it — the CI part of the gate's name, or the local part — and held to
// the same rules as the unit ledger: every entry is a pass, an allowed skip
// the plan's list names, or a problem the ledger itself carries.

// laneLedgers returns the lane ledgers this merge read, by suite.
func (m *merger) laneLedgers() map[string]*ledger {
	out := map[string]*ledger{}
	for suite, l := range map[string]*ledger{suiteRace: m.race, suiteRaceRepeat: m.raceRepeat, suiteFuzz: m.fuzz} {
		if l != nil {
			out[suite] = l
		}
	}
	return out
}

// mergeLanes reads the ledgers the plan's tier expects: the race gate's for
// the full and release tiers, and the race repeat's and the fuzz lane's for
// the nightly one.
func (m *merger) mergeLanes() {
	if m.plan.Tier == tierNightly {
		m.raceRepeat = m.mergeLane(suiteRaceRepeat)
		m.fuzz = m.mergeLane(suiteFuzz)
		return
	}
	m.race = m.mergeLane(suiteRace)
	m.checkRaceAgainstUnit()
}

// mergeLane reads one lane's ledger from its part, or from the local part.
func (m *merger) mergeLane(suite string) *ledger {
	for _, part := range []string{suite, "local"} {
		if _, ok := m.parts[part]; !ok {
			continue
		}
		for _, reading := range m.resultReadings(part) {
			if reading.Record == nil {
				continue
			}
			var l ledger
			if err := readJSON(filepath.Join(reading.Dir, suite+"-tests.json"), &l); err != nil {
				m.problems = append(m.problems, fmt.Sprintf("part %s left no %s ledger", part, suite))
				return nil
			}
			for _, e := range l.Tests {
				switch {
				case e.Outcome == outcomeQuarantinedFail:
					// No quarantine excuses a lane: its builders pass none.
					msg := fmt.Sprintf("%s: %s is quarantined-fail in the %s ledger, and a %s failure is never quarantined", part, testKey{e.Package, e.Test}, suite, suite)
					l.Problems = append(l.Problems, msg)
					m.problems = append(m.problems, msg)
				case isRaceSuite(suite) && e.Outcome == outcomeSkip && e.AllowedSkip == "":
					// A -short skip: checkRaceAgainstUnit holds it to the
					// unit gate's result.
				default:
					if msg := m.unexplainedEntry(suite, part, e, l.Problems); msg != "" {
						l.Problems = append(l.Problems, msg)
						m.problems = append(m.problems, msg)
					}
				}
			}
			return &l
		}
	}
	m.problems = append(m.problems, fmt.Sprintf("part %s left no result", suite))
	return nil
}

// checkRaceAgainstUnit holds the race ledger to the unit ledger it shadows:
// it lists every test the unit ledger does, and each test it recorded as
// skipped under -short is one the unit gate, which runs without -short,
// passed. A test neither lane ran would otherwise pass unseen.
func (m *merger) checkRaceAgainstUnit() {
	if m.race == nil || m.unit == nil {
		return
	}
	unit := map[testKey]entry{}
	for _, e := range m.unit.Tests {
		unit[testKey{e.Package, e.Test}] = e
	}
	race := map[testKey]entry{}
	for _, e := range m.race.Tests {
		race[testKey{e.Package, e.Test}] = e
	}
	var missing []testKey
	for k := range unit {
		if _, ok := race[k]; !ok {
			missing = append(missing, k)
		}
	}
	sort.Slice(missing, func(i, j int) bool { return missing[i].String() < missing[j].String() })
	const show = 10
	for i, k := range missing {
		if i == show {
			m.addRaceProblem(fmt.Sprintf("race: and %d more tests are in the unit ledger and not in the race ledger", len(missing)-show))
			break
		}
		m.addRaceProblem(fmt.Sprintf("race: %s is in the unit ledger and not in the race ledger: the gate did not run it", k))
	}
	skipped := make([]entry, 0)
	for _, e := range m.race.Tests {
		if e.Outcome == outcomeSkip && e.AllowedSkip == "" {
			skipped = append(skipped, e)
		}
	}
	for _, e := range skipped {
		k := testKey{e.Package, e.Test}
		u, ok := unit[k]
		switch {
		case !ok:
			m.addRaceProblem(fmt.Sprintf("race: %s was skipped under -short, and the unit ledger does not list it: no lane ran it", k))
		case u.Outcome != outcomePass:
			m.addRaceProblem(fmt.Sprintf("race: %s was skipped under -short, and the unit ledger has outcome %q for it: no lane ran it to a pass", k, orDash(u.Outcome)))
		}
	}
}

// addRaceProblem records a cross-ledger problem on the race ledger and the
// merge, so the ledger reads as failing wherever it is shown.
func (m *merger) addRaceProblem(msg string) {
	m.race.Problems = append(m.race.Problems, msg)
	m.problems = append(m.problems, msg)
}
