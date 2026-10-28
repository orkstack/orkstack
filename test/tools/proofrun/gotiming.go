package main

import (
	"fmt"
	"math"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/orkstack/orkstack/internal/testsupport/proof"
)

// A Go integration test's wall-clock budget is a runner-size miss, excused
// like a UI spec's (hostspeed.go), when scripts/proof-host-speed.tsv declares
// the check in a row the base carries (`timing ID`) and every one of these
// holds. The merger derives each from the part's own records and never takes
// the shard's word for it:
//
//   - the part is a CI `integration:K` part whose runner has fewer CPUs than
//     the row's reference, never the `local` part, in any tier: the
//     pre-release gate excuses exactly what the merge gate does;
//   - the test's ledger entry is a plain failure (no crash reason, no failing
//     subtest) and no other test of its job did not pass, so the test and
//     nothing else failed;
//   - the test left its timing record (internal/testsupport/proof's Timing,
//     timing/<test>.json under the part's artifacts), the record is the
//     test's own, and it says the test failed nothing but its timing checks.
//     A test that failed with no record has no sample to excuse: the
//     missing-sample rule;
//   - the failure output the entry kept is exactly the helper's lines: after
//     the first timing-miss line there is nothing but timing-miss lines, and
//     they are the record's misses, one for one;
//   - every miss is a check a row declares, measured over its bound, with the
//     bound the record carries equal to the row's, and no more than the bound
//     scaled by the reference's CPUs over the runner's (runnerCeiling): a
//     runner with a k-th of the reference's CPUs may take k times as long.
//
// Anything short of that is the ordinary failure it always was.

// timingRecord is the record a Go test leaves of its wall-clock checks.
type timingRecord = proof.TimingRecord

// isCIIntegrationPart reports whether part is a CI integration shard, the
// only place a Go runner-size miss may be excused.
func isCIIntegrationPart(part string) bool {
	n, ok := strings.CutPrefix(part, "integration:")
	if !ok {
		return false
	}
	k, err := strconv.Atoi(n)
	return err == nil && k > 0
}

// ownProblem is the problem line buildLedgerQ writes for a plain failure of e:
// the same function writes it.
func ownProblem(e entry) string {
	return failedProblem(testKey{e.Package, e.Test}, e)
}

// timingRecordPath is where the part left e's timing record.
func timingRecordPath(partDir string, e entry) string {
	return filepath.Join(partDir, "artifacts", proof.TimingGroup, proof.TimingFile(e.Test))
}

// goHostSpeed records, for every failed entry of shard that a host-speed row
// declares and that ran on a runner below the row's reference, the known gap
// it is, and returns the entries it excuses. An entry the table does not
// excuse still fails the proof (rule 13: it is recorded, and says why not).
func (m *merger) goHostSpeed(part string, reading partReading, shard ledger) map[testKey]bool {
	excused := map[testKey]bool{}
	if reading.Record == nil {
		return excused
	}
	cpus := reading.Record.Runner.CPUs
	for _, e := range shard.Tests {
		if e.Outcome != outcomeFail {
			continue
		}
		var declared *hostBound
		for _, b := range m.hostSpeed.testRows(e.Package, e.Test) {
			if cpus > 0 && cpus < b.ReferenceCPUs {
				b := b
				declared = &b
				break
			}
		}
		if declared == nil {
			continue
		}
		gaps, why := m.goTimingExcusal(part, reading.Dir, cpus, shard, e)
		if len(gaps) > 0 {
			excused[testKey{e.Package, e.Test}] = true
			for _, f := range gaps {
				m.hostSpeedFailures = append(m.hostSpeedFailures, f)
				m.knownGaps = append(m.knownGaps, fmt.Sprintf("%s: %s (bound %gs, reference %d CPUs)", part, f.Message, f.Bound, f.ReferenceCPUs))
			}
			continue
		}
		message := fmt.Sprintf("%s failed on a %d-CPU runner (bound %.0fs, reference %d CPUs)", e.Test, cpus, declared.Bound, declared.ReferenceCPUs)
		if declared.Note != "" {
			message += ": " + declared.Note
		}
		if declared.Assertion.Kind == "timing" {
			message += "; not excused: " + why
		}
		m.hostSpeedFailures = append(m.hostSpeedFailures, hostSpeedFailure{
			Part: part, Test: e.Package + "." + e.Test, Kind: "integration", Assertion: declared.Assertion.String(), Bound: declared.Bound,
			RunnerCPUs: cpus, ReferenceCPUs: declared.ReferenceCPUs, Message: message,
		})
		m.knownGaps = append(m.knownGaps, part+": "+message)
	}
	return excused
}

// goTimingExcusal decides whether the failed entry e of shard, run by part on
// a runner of cpus CPUs, is excused by the anchored table. It returns the
// known gaps to record (one per missed check) when it is, and otherwise why
// it is not.
func (m *merger) goTimingExcusal(part string, partDir string, cpus int, shard ledger, e entry) ([]hostSpeedFailure, string) {
	if !isCIIntegrationPart(part) {
		return nil, "only a CI integration part may excuse a runner-size miss"
	}
	rows := map[string]hostBound{}
	for _, b := range m.hostSpeedExcusing.testRows(e.Package, e.Test) {
		if b.Assertion.Kind == "timing" {
			rows[b.Assertion.ID] = b
		}
	}
	if len(rows) == 0 {
		return nil, "the base's host-speed table does not carry a timing row for this test"
	}
	if cpus <= 0 {
		return nil, "the part recorded no CPU count for its runner"
	}
	if e.Outcome != outcomeFail || e.Reason != "" || len(e.Failures) > 0 {
		return nil, "the test failed more than a timing check: it crashed, or a subtest failed"
	}
	for _, o := range shard.Tests {
		if o.Job == e.Job && (o.Package != e.Package || o.Test != e.Test) && o.Outcome != outcomePass &&
			!(o.Outcome == outcomeSkip && o.AllowedSkip != "") {
			return nil, fmt.Sprintf("%s.%s, of the same job, did not pass either", o.Package, o.Test)
		}
	}
	var rec proof.TimingRecord
	if err := readJSON(timingRecordPath(partDir, e), &rec); err != nil {
		return nil, "the test left no timing record, so there is no sample to excuse (" + err.Error() + ")"
	}
	if rec.Format != proof.TimingFormat || rec.Package != e.Package || rec.Test != e.Test || !rec.Asserted {
		return nil, "the timing record is not this test's, or not an asserting one"
	}
	if rec.NonTimingFailed {
		return nil, "the test failed something besides its timing checks"
	}
	misses := rec.Misses()
	if len(misses) == 0 {
		return nil, "the timing record holds no miss"
	}
	for _, c := range misses {
		if math.IsNaN(c.Measured) || !(c.Measured > c.Bound) {
			return nil, fmt.Sprintf("the timing record's miss of %s measured %g s against a bound of %g s: not a miss", c.ID, c.Measured, c.Bound)
		}
	}
	if why := failureIsTheMisses(e.FailureOutput, misses); why != "" {
		return nil, why
	}
	var out []hostSpeedFailure
	for _, c := range misses {
		row, ok := rows[c.ID]
		switch {
		case !ok:
			return nil, fmt.Sprintf("no row of the base's table declares the check %q", c.ID)
		case c.Bound != row.Bound:
			return nil, fmt.Sprintf("the check %q was bounded at %g s, and the base's row says %g s", c.ID, c.Bound, row.Bound)
		case cpus >= row.ReferenceCPUs:
			return nil, fmt.Sprintf("the runner has %d CPUs, and the row's reference is %d: a miss there is a failure", cpus, row.ReferenceCPUs)
		}
		// A runner with a k-th of the reference's CPUs may take k times as
		// long, at worst: a CPU-bound check scales no worse than linearly in
		// CPUs. A miss beyond that is not the runner's size.
		if ceiling := runnerCeiling(row, cpus); c.Measured > ceiling {
			return nil, fmt.Sprintf("the check %q measured %g s, more than a %d-CPU runner may take against a bound of %g s set on %d CPUs (%g s)",
				c.ID, c.Measured, cpus, row.Bound, row.ReferenceCPUs, ceiling)
		}
		message := fmt.Sprintf("%s.%s: %s measured %.3f s over its %g s bound on a %d-CPU runner", e.Package, e.Test, c.ID, c.Measured, c.Bound, cpus)
		if c.Label != "" {
			message += " (" + c.Label + ")"
		}
		out = append(out, hostSpeedFailure{
			Part: part, Test: e.Package + "." + e.Test, Kind: "integration", Assertion: row.Assertion.String(), Bound: row.Bound,
			Observed: c.Measured, RunnerCPUs: cpus, ReferenceCPUs: row.ReferenceCPUs, Message: message, Excused: true, Record: &rec,
		})
	}
	return out, ""
}

// runnerCeiling is the longest a check bounded by row may take on a runner of
// cpus CPUs and still be excused as the runner's size: the bound scaled by the
// reference's CPUs over the runner's.
func runnerCeiling(row hostBound, cpus int) float64 {
	return row.Bound * float64(row.ReferenceCPUs) / float64(cpus)
}

// failureIsTheMisses reports "" when the output a failing test kept ends with
// exactly the timing-miss lines misses describe: from the first line of that
// form to the end there is nothing else, and they are the record's misses in
// order. Otherwise it says what else the test printed or did not print.
func failureIsTheMisses(output []string, misses []proof.TimingCheck) string {
	first := -1
	for i, line := range output {
		if _, ok := proof.ParseMissLine(line); ok {
			first = i
			break
		}
	}
	if first < 0 {
		return "the failure output holds no timing-miss line"
	}
	tail := output[first:]
	if len(tail) != len(misses) {
		return fmt.Sprintf("the failure output holds %d lines from its first timing miss, and the record holds %d misses", len(tail), len(misses))
	}
	for i, line := range tail {
		got, ok := proof.ParseMissLine(line)
		want := misses[i]
		switch {
		case !ok:
			return fmt.Sprintf("the failure output has %q after a timing miss: not the helper's line", line)
		case got.ID != want.ID || got.Measured != want.Measured || got.Bound != want.Bound || got.Label != want.Label:
			return fmt.Sprintf("the failure output says %q, and the record says %s measured %g bound %g", line, want.ID, want.Measured, want.Bound)
		}
	}
	return ""
}
