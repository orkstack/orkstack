package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// The rerun rules (plan §2.6 rule 2): each test lays out shard 2's first
// attempt as GitHub and the artifacts left it, a second attempt that passed,
// and asks what the merged proof says.

const runnerLost = "The runner has received a shutdown signal. This can happen when the runner service is stopped, or a manually started runner is canceled."

// rerunShard2 writes a passing CI proof whose integration:2 passed on its
// second attempt, after a first attempt GitHub recorded as job.
func rerunShard2(t *testing.T, job *githubJob, events ...string) (proofRecord, int, *mergeFixture) {
	t.Helper()
	f := newMergeFixture(t, tierFull, true)
	f.writeCI()
	f.writeShard(1, []entry{passEntry("TestA")}, nil, 1)
	if len(events) > 0 {
		f.writeAttemptEvents("integration-2-a1", events...)
	} else if err := os.MkdirAll(filepath.Join(f.parts, "integration-2-a1"), 0o755); err != nil {
		t.Fatal(err)
	}
	f.writeShard(2, []entry{passEntry("TestB")}, nil, 2)
	if job != nil {
		job.Name = "integration (2)"
		f.writeJobs(1, *job)
	} else {
		f.writeJobs(1)
	}
	r, code := f.merge()
	return r, code, f
}

func TestMerge_ATimedOutAttemptIsAResult(t *testing.T) {
	r, code, _ := rerunShard2(t, &githubJob{Conclusion: "failure",
		Annotations: []string{"The job running on runner GitHub Actions 7 has exceeded the maximum execution time of 45 minutes."},
		Log:         "2026-10-21T10:00:00.0000000Z proofrun: start example.com/m TestB\n"})
	if code != 0 || r.Result != "pass" {
		t.Fatalf("code %d result %s: the second attempt passed", code, r.Result)
	}
	if r.MergeEligible || len(r.Reruns) != 0 {
		t.Fatalf("a timed-out attempt with a running test was rerun-eligible: %v %+v", r.Ineligible, r.Reruns)
	}
	if !strings.Contains(strings.Join(r.Ineligible, "\n"), "after attempt 1 recorded its result (a test was running when the job stopped: example.com/m.TestB)") {
		t.Errorf("ineligible = %v", r.Ineligible)
	}
}

func TestMerge_ACancelledAttemptBeforeAnyTestStartedIsNotRerunEligible(t *testing.T) {
	r, code, _ := rerunShard2(t, &githubJob{Conclusion: "cancelled", Log: "2026-10-21T10:00:00Z Pulling images\n"})
	if code != 0 || r.Result != "pass" || r.MergeEligible || len(r.Reruns) != 0 {
		t.Fatalf("code %d result %s eligible %v reruns %+v", code, r.Result, r.MergeEligible, r.Reruns)
	}
	if !strings.Contains(strings.Join(r.Ineligible, "\n"), "which GitHub did not record as a lost runner (it was cancelled before any test started): only a lost runner may run again; this run cannot become merge-eligible: start a new run") {
		t.Errorf("ineligible = %v", r.Ineligible)
	}
}

func TestMerge_ASetupFailureBeforeTheProofStepIsNotRerunEligible(t *testing.T) {
	r, code, _ := rerunShard2(t, &githubJob{Conclusion: "failure", FailedStep: "Set up the proof host",
		Steps: []githubStep{{Name: "Set up the proof host", Conclusion: "failure"}, {Name: "integration shard", Conclusion: "skipped"}}})
	if code != 0 || r.Result != "pass" || r.MergeEligible || len(r.Reruns) != 0 {
		t.Fatalf("code %d result %s eligible %v reruns %+v", code, r.Result, r.MergeEligible, r.Reruns)
	}
	if !strings.Contains(strings.Join(r.Ineligible, "\n"), "(a setup failure before its proof step (Set up the proof host)): only a lost runner may run again; this run cannot become merge-eligible: start a new run") {
		t.Errorf("ineligible = %v", r.Ineligible)
	}
}

func TestMerge_AFailedJobWithoutAnArtifactIsAResult(t *testing.T) {
	// The proof step ran and the job failed, and its artifact never uploaded:
	// that is a result that never reached its upload, not a lost runner.
	r, code, _ := rerunShard2(t, &githubJob{Conclusion: "failure", FailedStep: "integration shard",
		Steps: []githubStep{{Name: "Set up the proof host", Conclusion: "success"}, {Name: "integration shard", Conclusion: "failure"}}})
	if code != 0 || r.Result != "pass" || r.MergeEligible || len(r.Reruns) != 0 {
		t.Fatalf("code %d result %s eligible %v reruns %+v", code, r.Result, r.MergeEligible, r.Reruns)
	}
	if !strings.Contains(strings.Join(r.Ineligible, "\n"), "after attempt 1 recorded its result (it ended failure after its proof step started, and its result never reached its upload)") {
		t.Errorf("ineligible = %v", r.Ineligible)
	}
}

func TestMerge_AnUnreadableJobRecordFailsClosed(t *testing.T) {
	cases := map[string]func(f *mergeFixture){
		"a record that is not JSON": func(f *mergeFixture) {
			if err := os.WriteFile(filepath.Join(f.jobsDir, "attempt-1.json"), []byte("{not json"), 0o644); err != nil {
				t.Fatal(err)
			}
		},
		"a record whose log could not be read": func(f *mergeFixture) {
			f.writeJobs(1, githubJob{Name: "integration (2)", Conclusion: "failure",
				Annotations: []string{runnerLost}, ReadError: "GET …/jobs/42/logs: 410 Gone"})
		},
		"no record of the attempt": func(f *mergeFixture) { f.writeJobs(1) },
	}
	for name, corrupt := range cases {
		t.Run(name, func(t *testing.T) {
			f := newMergeFixture(t, tierFull, true)
			f.writeCI()
			f.writeShard(1, []entry{passEntry("TestA")}, nil, 1)
			if err := os.MkdirAll(filepath.Join(f.parts, "integration-2-a1"), 0o755); err != nil {
				t.Fatal(err)
			}
			f.writeShard(2, []entry{passEntry("TestB")}, nil, 2)
			f.writeJobs(1)
			corrupt(f)
			r, _ := f.merge()
			if r.MergeEligible || len(r.Reruns) != 0 {
				t.Fatalf("an attempt GitHub's record cannot vouch for was rerun-eligible: %+v", r.Reruns)
			}
			if !strings.Contains(strings.Join(r.Ineligible, "\n"), "after attempt 1 recorded its result") {
				t.Errorf("ineligible = %v", r.Ineligible)
			}
		})
	}
}

func TestMerge_GitHubsRunnerLostAnnotationMakesALostRunner(t *testing.T) {
	// Only GitHub's annotation says so: no runner_lost flag, a failure
	// conclusion, and a test that had started.
	r, code, _ := rerunShard2(t, &githubJob{Conclusion: "failure", Annotations: []string{runnerLost},
		Log: "2026-10-21T10:00:00Z proofrun: start example.com/m TestB\n"},
		`{"Action":"run","Package":"example.com/m","Test":"TestB"}`)
	if code != 0 || r.Result != "pass" || !r.MergeEligible {
		t.Fatalf("code %d result %s eligible %v: %v", code, r.Result, r.MergeEligible, r.Ineligible)
	}
	if len(r.Reruns) != 1 || r.Reruns[0].Reason != "lost-runner" || r.Reruns[0].LostAttempt.Annotation != runnerLost {
		t.Fatalf("reruns = %+v", r.Reruns)
	}
}

func TestMerge_ALostAttemptWhoseLogRecordsAFailureIsAResult(t *testing.T) {
	r, _, _ := rerunShard2(t, &githubJob{Conclusion: "failure", Annotations: []string{runnerLost},
		Log: "2026-10-21T10:00:00Z proofrun: start example.com/m TestB\n2026-10-21T10:00:09Z proofrun: outcome fail example.com/m TestB 9.00\n"})
	if r.MergeEligible || len(r.Reruns) != 0 {
		t.Fatalf("a lost runner whose log recorded a failure was rerun-eligible: %+v", r.Reruns)
	}
	if !strings.Contains(strings.Join(r.Ineligible, "\n"), "after attempt 1 recorded its result (its log records a failing test: example.com/m.TestB)") {
		t.Errorf("ineligible = %v", r.Ineligible)
	}
}

func TestMerge_AnAttemptWhoseEventsRecordAFailureIsNotALostRunner(t *testing.T) {
	// No part.json, a fail in its events, and GitHub's runner-lost
	// annotation: the failure is a result, whatever happened to the runner.
	r, _, _ := rerunShard2(t, &githubJob{Conclusion: "failure", Annotations: []string{runnerLost}},
		`{"Action":"run","Package":"example.com/m","Test":"TestB"}`,
		`{"Action":"fail","Package":"example.com/m","Test":"TestB","Elapsed":2}`)
	if r.MergeEligible || len(r.Reruns) != 0 {
		t.Fatalf("an attempt whose events record a failure was a lost runner: %+v", r.Reruns)
	}
	if !strings.Contains(strings.Join(r.Ineligible, "\n"), "after attempt 1 recorded its result (its events record a failing test)") {
		t.Errorf("ineligible = %v", r.Ineligible)
	}
}

func TestMerge_ALostRunnerRerunNamesTheTestsItLeftUnfinished(t *testing.T) {
	r, code, f := rerunShard2(t, &githubJob{Conclusion: "failure", Annotations: []string{runnerLost},
		Log: "2026-10-21T10:00:00Z proofrun: start example.com/m TestA\n" +
			"2026-10-21T10:00:01Z proofrun: outcome pass example.com/m TestA 1.00\n" +
			"2026-10-21T10:00:01Z proofrun: start example.com/m TestB\n" +
			"2026-10-21T10:00:01Z proofrun: start example.com/m TestC\n"})
	if code != 0 || !r.MergeEligible || len(r.Reruns) != 1 {
		t.Fatalf("code %d eligible %v reruns %+v: %v", code, r.MergeEligible, r.Reruns, r.Ineligible)
	}
	lost := r.Reruns[0].LostAttempt
	if lost.PassedBeforeLoss != 1 || strings.Join(lost.Unfinished, ",") != "example.com/m.TestB,example.com/m.TestC" {
		t.Errorf("lost attempt = %+v", lost)
	}
	b, _ := os.ReadFile(filepath.Join(f.out, "README.md"))
	if !strings.Contains(string(b), "## Reruns") || !strings.Contains(string(b), "1 tests had passed, 2 had started and not finished: example.com/m.TestB, example.com/m.TestC") {
		t.Errorf("the README does not name the unfinished tests:\n%s", b)
	}
}

func TestMerge_OnlyALostRunnerIsRecordedAsARerun(t *testing.T) {
	cases := []struct {
		name string
		job  githubJob
		lost bool
	}{
		{"lost runner", githubJob{Conclusion: "failure", Annotations: []string{runnerLost}}, true},
		{"lost communication", githubJob{Conclusion: "failure", Annotations: []string{"The self-hosted runner lost communication with the server."}}, true},
		{"a failure after the proof step", githubJob{Conclusion: "failure", FailedStep: "integration shard"}, false},
		{"a setup failure", githubJob{Conclusion: "failure", FailedStep: "Set up the proof host"}, false},
		{"an early cancel", githubJob{Conclusion: "cancelled"}, false},
		{"a cancel with a running test", githubJob{Conclusion: "cancelled", Log: "proofrun: start example.com/m TestB\n"}, false},
		{"a success that left nothing", githubJob{Conclusion: "success"}, false},
		{"a record naming no step", githubJob{Conclusion: "failure"}, false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			job := c.job
			r, _, _ := rerunShard2(t, &job)
			for _, rr := range r.Reruns {
				if rr.Reason != "lost-runner" {
					t.Errorf("a rerun recorded with reason %q", rr.Reason)
				}
			}
			if got := len(r.Reruns) == 1; got != c.lost || r.MergeEligible != c.lost {
				t.Errorf("reruns %+v eligible %v, want lost=%v (%v)", r.Reruns, r.MergeEligible, c.lost, r.Ineligible)
			}
		})
	}
}

func TestLogProgress_ReadsTimestampedLines(t *testing.T) {
	log := "2026-10-21T10:00:00.1Z proofrun: start p TestA\n" +
		"2026-10-21T10:00:00.2Z proofrun: outcome fail p TestA 0.10\n" +
		"2026-10-21T10:00:00.3Z proofrun: start p TestB\n" +
		"some test output mentioning proofrun: outcome nothing\n"
	failed, unfinished := logProgress(log)
	if strings.Join(failed, ",") != "p.TestA" || strings.Join(unfinished, ",") != "p.TestB" {
		t.Errorf("failed %v unfinished %v", failed, unfinished)
	}
}
