package main

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// Nightly 41080705584 (2026-10-30, a dispatch on main): attempt 1's
// `integration (2)` measured an fsync p95 of 99.43 ms against the floor's
// 30 ms, stopped before any gate and left the one annotation a degraded part
// prints; nothing else failed but the `proof` aggregator. It needed a rerun by
// hand. testdata/rerun-nightly-41080705584 holds GitHub's own answers for that
// attempt, as the planner reads them: the job listing, the two failed jobs'
// check-run annotations, and the degraded job's log, terminal escape sequences
// and all. The nightly calls proof.yml as a reusable workflow, so every part's
// job carries the caller's `nightly / ` prefix, the aggregator's too.

const nightlyRun = "41080705584"

// recordedNightly answers the planner's reads of the recorded attempt, by
// exact path, as `gh api` would on the runner.
func recordedNightly(t *testing.T) ghAPI {
	t.Helper()
	dir := filepath.Join("testdata", "rerun-nightly-"+nightlyRun)
	return func(path string, paginate bool) ([]byte, error) {
		const repo = "repos/orkstack/orkstack/"
		var file string
		switch {
		case path == repo+"actions/runs/"+nightlyRun+"/attempts/1/jobs?per_page=100" && paginate:
			file = "api-jobs-a1.json"
		case strings.HasPrefix(path, repo+"check-runs/") && strings.HasSuffix(path, "/annotations?per_page=100") && paginate:
			file = "annotations-" + strings.TrimSuffix(strings.TrimPrefix(path, repo+"check-runs/"), "/annotations?per_page=100") + ".json"
		case strings.HasPrefix(path, repo+"actions/jobs/") && strings.HasSuffix(path, "/logs") && !paginate:
			file = "log-" + strings.TrimSuffix(strings.TrimPrefix(path, repo+"actions/jobs/"), "/logs") + ".txt"
		default:
			return nil, fmt.Errorf("the recorded run has no answer for %s", path)
		}
		b, err := os.ReadFile(filepath.Join(dir, file))
		if err != nil {
			return nil, fmt.Errorf("the recorded run has no %s: %w", file, err)
		}
		return b, nil
	}
}

func TestRerunPlan_TheRecordedNightlyWhoseShardRanBelowTheFloorRunsAgain(t *testing.T) {
	d := planRerun(recordedNightly(t), "orkstack/orkstack", nightlyRun, 1, defaultMaxRunAttempts)
	if got, want := d.output(), "rerun=true\nreason=eligible\nparts=integration:2=runner-degraded\n"; got != want {
		t.Errorf("decision = %q, want %q", got, want)
	}
}

// What the planner makes of a nightly's records is what it makes of a
// candidate's: the reusable workflow's prefix names the same part, the
// aggregator is still the aggregator, and the nightly's own lanes are judged
// by the merger's classification. A job of nightly.yml itself (the scan, the
// Quickstart, the flake report) is not a part, so its failure is never
// answered by a rerun.
func TestRerunPlan_ANightlysRecordsAreClassifiedAsTheMergerWould(t *testing.T) {
	nightly := func(j fakeJob) fakeJob {
		j.name = "nightly / " + j.name
		return j
	}
	aggregator := nightly(proofJob(9))
	for _, c := range []struct {
		name   string
		jobs   []fakeJob
		rerun  bool
		reason string
		parts  string
	}{
		{"a lost runner", []fakeJob{nightly(lostJob(2, "integration (4)")), aggregator}, true, "eligible", "integration:4=lost-runner"},
		{"a degraded runner", []fakeJob{nightly(degradedJob(2, "ui-e2e (1)")), aggregator}, true, "eligible", "ui-e2e:1=runner-degraded"},
		{"a fetch that gave up", []fakeJob{nightly(setupJob(2, "integration (1)", "fetch build")), aggregator}, true, "eligible", "integration:1=setup-failed"},
		{"the race repeat's lost runner", []fakeJob{nightly(lostJob(2, "race-repeat")), aggregator}, true, "eligible", "race-repeat=lost-runner"},
		{"only the aggregator failed", []fakeJob{nightly(okJob(2, "integration (1)")), aggregator}, false, "no-eligible-part", ""},
		{"a failing soak", []fakeJob{
			nightly(fakeJob{id: 2, name: "integration (3)", conclusion: "failure", steps: []githubStep{{Name: "integration shard", Conclusion: "failure"}},
				log: "2026-10-30T10:00:00Z proofrun: start p TestSoak\n2026-10-30T10:30:00Z proofrun: outcome fail p TestSoak 1800.00\n"}),
			aggregator}, false, "not-eligible", ""},
		{"a fuzz crasher", []fakeJob{
			nightly(fakeJob{id: 2, name: "fuzz", conclusion: "failure", steps: []githubStep{{Name: "fuzz", Conclusion: "failure"}}}), aggregator}, false, "not-eligible", ""},
		{"a race the repeat found", []fakeJob{
			nightly(fakeJob{id: 2, name: "race-repeat", conclusion: "failure", steps: []githubStep{{Name: "race-repeat", Conclusion: "failure"}}}), aggregator}, false, "not-eligible", ""},
		{"a degraded marker on the race repeat, which no floor binds", []fakeJob{
			nightly(func() fakeJob { j := degradedJob(2, "race-repeat"); j.steps[1].Name = "race-repeat"; return j }()), aggregator}, false, "not-eligible", ""},
		{"the vulnerability scan", []fakeJob{{id: 3, name: "vuln", conclusion: "failure"}, nightly(lostJob(2, "integration (1)")), aggregator}, false, "other-job-failed", ""},
		{"the Quickstart", []fakeJob{{id: 3, name: "quickstart", conclusion: "failure"}, aggregator}, false, "other-job-failed", ""},
		{"the flake report", []fakeJob{{id: 3, name: "flakes", conclusion: "failure"}, nightly(lostJob(2, "integration (1)")), aggregator}, false, "other-job-failed", ""},
		{"the nightly's plan", []fakeJob{nightly(lostJob(1, "plan")), aggregator}, false, "other-job-failed", ""},
	} {
		t.Run(c.name, func(t *testing.T) {
			d := decide(t, &fakeAPI{jobs: c.jobs}, planAttempt, 3)
			if d.Rerun != c.rerun || d.Reason != c.reason || partsOf(d) != c.parts {
				t.Errorf("decision = %+v, want rerun %v (%s) of %q", d, c.rerun, c.reason, c.parts)
			}
		})
	}
}

// A job named for the aggregator only under some other shape, or with more
// words, is not the aggregator: it names no part, so it stops the rerun.
func TestRerunPlan_OnlyTheAggregatorItselfIsPassedOver(t *testing.T) {
	for _, name := range []string{"proof-extra", "proofs", "nightly / proof x", "nightly / proof (1)"} {
		j := proofJob(9)
		j.name = name
		d := decide(t, &fakeAPI{jobs: []fakeJob{lostJob(2, "integration (2)"), j}}, planAttempt, 3)
		if d.Rerun || d.Reason != "other-job-failed" {
			t.Errorf("a failed job %q: decision = %+v, want other-job-failed", name, d)
		}
	}
}
