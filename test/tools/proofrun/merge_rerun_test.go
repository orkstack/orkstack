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
	if !strings.Contains(strings.Join(r.Ineligible, "\n"), "which GitHub did not record as a lost runner, whose runner-health did not measure below the floor and which did not fail in a fetch or install step before its proof step (it was cancelled before any test started): only a lost, degraded or setup-failed attempt may run again; this run cannot become merge-eligible: start a new run") {
		t.Errorf("ineligible = %v", r.Ineligible)
	}
}

func TestMerge_ASetupFailureOutsideTheFetchAndInstallStepsIsNotRerunEligible(t *testing.T) {
	r, code, _ := rerunShard2(t, &githubJob{Conclusion: "failure", FailedStep: "Run actions/setup-go@v7.0.0",
		Steps: []githubStep{{Name: "Run actions/setup-go@v7.0.0", Conclusion: "failure"}, {Name: "integration shard", Conclusion: "skipped"}}})
	if code != 0 || r.Result != "pass" || r.MergeEligible || len(r.Reruns) != 0 {
		t.Fatalf("code %d result %s eligible %v reruns %+v", code, r.Result, r.MergeEligible, r.Reruns)
	}
	if !strings.Contains(strings.Join(r.Ineligible, "\n"), "(a setup failure before its proof step (Run actions/setup-go@v7.0.0), which is not a fetch or install step this part may run again after): only a lost, degraded or setup-failed attempt may run again; this run cannot become merge-eligible: start a new run") {
		t.Errorf("ineligible = %v", r.Ineligible)
	}
	if len(r.SetupFailures) != 0 {
		t.Errorf("setup_failures = %+v: only a fetch or install step is one", r.SetupFailures)
	}
}

// A job that never reached its proof step because a fetch or an install step
// failed (after the fetch's own two bounded attempts) left no result and ran
// no test: like a lost or a degraded runner, that one job may run again, as a
// new part of the same tree.
func TestMerge_AFetchOrInstallFailureBeforeTheProofStepMayRunAgain(t *testing.T) {
	failedFetch := &githubJob{Conclusion: "failure", FailedStep: "fetch plan",
		Steps: []githubStep{
			{Name: "proof host", Conclusion: "success"}, {Name: "fetch plan", Conclusion: "failure"},
			{Name: "fetch build", Conclusion: "skipped"}, {Name: "integration shard", Conclusion: "skipped"},
		},
		Log: "2026-10-21T10:00:00Z fetch artifact plan-a1: attempt 2 timed out after 360s (limit 360s)\n"}
	r, code, f := rerunShard2(t, failedFetch)
	if code != 0 || r.Result != "pass" || !r.MergeEligible {
		t.Fatalf("code %d result %s eligible %v: %v", code, r.Result, r.MergeEligible, r.Ineligible)
	}
	if len(r.Reruns) != 1 {
		t.Fatalf("reruns = %+v", r.Reruns)
	}
	rr := r.Reruns[0]
	if rr.Reason != "setup-failed" || rr.Part != "integration:2" || rr.Attempt != 2 || len(rr.EarlierAttempts) != 1 || rr.EarlierAttempts[0] != 1 {
		t.Errorf("rerun = %+v", rr)
	}
	if rr.LostAttempt.Conclusion != "failure" || rr.LostAttempt.FailedStep != "fetch plan" || rr.LostAttempt.PassedBeforeLoss != 0 || len(rr.LostAttempt.Unfinished) != 0 {
		t.Errorf("the failed attempt = %+v", rr.LostAttempt)
	}
	want := []setupFailure{{Part: "integration:2", Attempt: 1, Step: "fetch plan", Conclusion: "failure"}}
	if len(r.SetupFailures) != 1 || r.SetupFailures[0] != want[0] {
		t.Errorf("setup_failures = %+v, want %+v", r.SetupFailures, want)
	}
	readme, err := os.ReadFile(filepath.Join(f.out, "README.md"))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(readme), `- integration:2 attempt 2, after attempt [1] failed in its setup step "fetch plan" before its proof step started; no test of that attempt ran`) {
		t.Errorf("the README does not explain the rerun:\n%s", readme)
	}
	if !strings.Contains(string(readme), "## Setup failures") || !strings.Contains(string(readme), `- integration:2 attempt 1: the step "fetch plan" ended failure before the proof step started`) {
		t.Errorf("the README does not list the setup failure:\n%s", readme)
	}
}

// With no later attempt, the setup failure is recorded and the part has no
// result: the proof fails, and says that one job is the remedy.
func TestMerge_ASetupFailureWithNoLaterAttemptLeavesNoResult(t *testing.T) {
	f := newMergeFixture(t, tierFull, true)
	f.writeCI()
	f.writeShard(1, []entry{passEntry("TestA")}, nil, 1)
	f.writeJobs(1, githubJob{Name: "integration (2)", Conclusion: "failure", FailedStep: "fetch build",
		Steps: []githubStep{{Name: "fetch build", Conclusion: "failure"}, {Name: "integration shard", Conclusion: "skipped"}}})
	r, code := f.merge()
	if code == 0 || r.Result != "fail" || r.MergeEligible {
		t.Fatalf("code %d result %s eligible %v: a part that never ran made a proof", code, r.Result, r.MergeEligible)
	}
	reasons := strings.Join(r.Ineligible, "\n")
	want := `part integration:2 left no result because its job failed in a setup step (attempt 1, its step "fetch build" failed before its proof step started); re-run that job alone, within the plan and build artifacts' retention`
	if !strings.Contains(reasons, want) {
		t.Errorf("ineligible = %v\nwant %q", r.Ineligible, want)
	}
	if len(r.Reruns) != 0 {
		t.Errorf("a setup failure with no later attempt was recorded as a rerun: %+v", r.Reruns)
	}
	if len(r.SetupFailures) != 1 || r.SetupFailures[0].Step != "fetch build" || r.SetupFailures[0].Attempt != 1 {
		t.Errorf("setup_failures = %+v", r.SetupFailures)
	}
}

// A second attempt after a setup failure is allowed once; an attempt that
// recorded a result is never answered by one, and a setup failure does not
// excuse the attempt after it.
func TestMerge_ASetupFailureDoesNotExcuseAnAttemptThatRanBeforeIt(t *testing.T) {
	f := newMergeFixture(t, tierFull, true)
	f.writeCI()
	f.writeShard(1, []entry{passEntry("TestA")}, nil, 1)
	// Attempt 1 ran its proof step and its job failed; attempt 2 failed in a
	// setup step; attempt 3 passed. Attempt 1 recorded a result.
	if err := os.MkdirAll(filepath.Join(f.parts, "integration-2-a1"), 0o755); err != nil {
		t.Fatal(err)
	}
	f.writeShard(2, []entry{passEntry("TestB")}, nil, 3)
	f.writeJobs(1, githubJob{Name: "integration (2)", Conclusion: "failure", FailedStep: "integration shard",
		Steps: []githubStep{{Name: "proof host", Conclusion: "success"}, {Name: "integration shard", Conclusion: "failure"}}})
	f.writeJobs(2, githubJob{Name: "integration (2)", Conclusion: "failure", FailedStep: "proof host",
		Steps: []githubStep{{Name: "proof host", Conclusion: "failure"}, {Name: "integration shard", Conclusion: "skipped"}}})
	r, _ := f.merge()
	if r.MergeEligible {
		t.Fatalf("a part that recorded a result at attempt 1 was run again and is eligible: %+v", r.Reruns)
	}
	if !strings.Contains(strings.Join(r.Ineligible, "\n"), "part integration:2 was run again (attempt 3) after attempt 1 recorded its result") {
		t.Errorf("ineligible = %v", r.Ineligible)
	}
}

// classify: which step of which kind of part job is a setup failure. The
// jobs API names only the steps of the workflow, so the table is by kind.
func TestClassify_ASetupFailureIsAFetchOrInstallStepOfAPartJobAlone(t *testing.T) {
	fail := func(job, step string) githubJob {
		return githubJob{Name: job, Conclusion: "failure", FailedStep: step,
			Steps: []githubStep{{Name: step, Conclusion: "failure"}, {Name: "integration shard", Conclusion: "skipped"}, {Name: "UI shard", Conclusion: "skipped"}, {Name: "activity fixture", Conclusion: "skipped"}, {Name: "observability", Conclusion: "skipped"}, {Name: "fast", Conclusion: "skipped"}, {Name: "build", Conclusion: "skipped"}, {Name: "plan", Conclusion: "skipped"}}}
	}
	for _, c := range []struct {
		job, step string
		want      attemptClass
	}{
		{"integration (2)", "proof host", attemptSetupFailed},
		{"integration (2)", "fetch plan", attemptSetupFailed},
		{"integration (2)", "fetch build", attemptSetupFailed},
		{"integration (2)", "bun install", attemptNotEligible},
		{"integration (2)", "fetch browser image", attemptNotEligible},
		{"integration (2)", "Run actions/setup-go@v7.0.0", attemptNotEligible},
		{"ui-e2e (1)", "bun install", attemptSetupFailed},
		{"ui-e2e (1)", "proof host", attemptSetupFailed},
		{"ui-e2e (1)", "fetch plan", attemptSetupFailed},
		{"ui-e2e (1)", "fetch build", attemptSetupFailed},
		{"ui-e2e (1)", "fetch browser image", attemptSetupFailed},
		{"ui-e2e (1)", "Chromium (host)", attemptSetupFailed},
		{"ui-e2e (1)", "host chromium shard", attemptNotEligible},
		{"activity-fixture (shell)", "proof host", attemptSetupFailed},
		{"activity-fixture (shell)", "fetch plan", attemptSetupFailed},
		{"activity-fixture (shell)", "fetch build", attemptSetupFailed},
		{"activity-fixture (shell)", "fetch browser image", attemptNotEligible},
		{"observability", "observability stack", attemptSetupFailed},
		{"observability", "fetch plan", attemptSetupFailed},
		{"observability", "fetch build", attemptSetupFailed},
		{"observability", "proof host", attemptNotEligible},
		{"fast", "bun install", attemptSetupFailed},
		{"fast", "bun install (site)", attemptSetupFailed},
		{"fast", "fetch plan", attemptNotEligible},
		// The plan and the build are what every other part needs: their setup
		// failures are never answered by running the job again.
		{"plan", "bun install", attemptNotEligible},
		{"build", "fetch plan", attemptNotEligible},
		{"build", "proof host", attemptNotEligible},
		// The proof step itself failing is a result, never a setup failure.
		{"integration (2)", "integration shard", attemptResult},
		{"ui-e2e (1)", "UI shard", attemptResult},
	} {
		j := fail(c.job, c.step)
		if c.want == attemptResult {
			j.Steps = []githubStep{{Name: "proof host", Conclusion: "success"}, {Name: c.step, Conclusion: "failure"}}
		}
		if got, why := classify(&j, partReading{}); got != c.want {
			t.Errorf("%s failing in %q classifies as %v (%s), want %v", c.job, c.step, got, why, c.want)
		}
	}
}

// Everything that ranks above a setup failure still does: a failure is
// never a setup failure when the attempt recorded a result, was cancelled, or
// could not be read.
func TestClassify_ASetupFailureYieldsToEveryRowAboveIt(t *testing.T) {
	base := func() githubJob {
		return githubJob{Name: "integration (2)", Conclusion: "failure", FailedStep: "fetch plan",
			Steps: []githubStep{{Name: "fetch plan", Conclusion: "failure"}, {Name: "integration shard", Conclusion: "skipped"}}}
	}
	if got, why := classify(ptr(base()), partReading{}); got != attemptSetupFailed {
		t.Fatalf("the base case classifies as %v (%s)", got, why)
	}
	for _, c := range []struct {
		name   string
		mutate func(j *githubJob)
		read   partReading
		want   attemptClass
	}{
		{"cancelled", func(j *githubJob) { j.Conclusion = "cancelled" }, partReading{}, attemptNotEligible},
		{"a success conclusion", func(j *githubJob) { j.Conclusion = "success" }, partReading{}, attemptNotEligible},
		{"a read error", func(j *githubJob) { j.ReadError = "annotations: the API call failed" }, partReading{}, attemptResult},
		{"a failing test in the log", func(j *githubJob) { j.Log = "proofrun: outcome fail p TestA 1.00\n" }, partReading{}, attemptResult},
		{"a lost runner", func(j *githubJob) { j.Annotations = []string{runnerLost} }, partReading{}, attemptLost},
		{"no failed step", func(j *githubJob) { j.FailedStep = "" }, partReading{}, attemptNotEligible},
		{"a part.json", func(j *githubJob) {}, partReading{Record: &partRecord{}}, attemptResult},
		{"a corrupt part.json", func(j *githubJob) {}, partReading{Corrupt: true}, attemptResult},
		{"a failure in its events", func(j *githubJob) {}, partReading{Failed: true}, attemptResult},
		{"a test that never finished", func(j *githubJob) {}, partReading{Unfinished: true}, attemptResult},
		{"the proof step recorded as run", func(j *githubJob) {
			j.Steps = []githubStep{{Name: "fetch plan", Conclusion: "failure"}, {Name: "integration shard", Conclusion: "success"}}
		}, partReading{}, attemptResult},
		{"the proof step recorded as failed", func(j *githubJob) {
			j.Steps = []githubStep{{Name: "fetch plan", Conclusion: "failure"}, {Name: "integration shard", Conclusion: "failure"}}
		}, partReading{}, attemptResult},
	} {
		t.Run(c.name, func(t *testing.T) {
			j := base()
			c.mutate(&j)
			if got, why := classify(&j, c.read); got != c.want {
				t.Errorf("classifies as %v (%s), want %v", got, why, c.want)
			}
		})
	}
}

func ptr[T any](v T) *T { return &v }

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

func TestMerge_OnlyALostDegradedOrSetupFailedAttemptIsRecordedAsARerun(t *testing.T) {
	cases := []struct {
		name   string
		job    githubJob
		reason string // the reason the rerun is recorded with; "" for none
	}{
		{"lost runner", githubJob{Conclusion: "failure", Annotations: []string{runnerLost}}, "lost-runner"},
		{"lost communication", githubJob{Conclusion: "failure", Annotations: []string{"The self-hosted runner lost communication with the server."}}, "lost-runner"},
		{"a fetch failure", githubJob{Conclusion: "failure", FailedStep: "fetch build"}, "setup-failed"},
		{"a host setup failure", githubJob{Conclusion: "failure", FailedStep: "proof host"}, "setup-failed"},
		{"a failure after the proof step", githubJob{Conclusion: "failure", FailedStep: "integration shard"}, ""},
		{"a setup failure outside the fetch and install steps", githubJob{Conclusion: "failure", FailedStep: "Run actions/checkout@v7.0.1"}, ""},
		{"an install step of another kind of part", githubJob{Conclusion: "failure", FailedStep: "Chromium (host)"}, ""},
		{"an early cancel", githubJob{Conclusion: "cancelled"}, ""},
		{"a cancel in a fetch step", githubJob{Conclusion: "cancelled", FailedStep: "fetch plan"}, ""},
		{"a cancel with a running test", githubJob{Conclusion: "cancelled", Log: "proofrun: start example.com/m TestB\n"}, ""},
		{"a success that left nothing", githubJob{Conclusion: "success"}, ""},
		{"a record naming no step", githubJob{Conclusion: "failure"}, ""},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			job := c.job
			r, _, _ := rerunShard2(t, &job)
			for _, rr := range r.Reruns {
				if rr.Reason != c.reason {
					t.Errorf("a rerun recorded with reason %q, want %q", rr.Reason, c.reason)
				}
			}
			if got := len(r.Reruns) == 1; got != (c.reason != "") || r.MergeEligible != (c.reason != "") {
				t.Errorf("reruns %+v eligible %v, want reason %q (%v)", r.Reruns, r.MergeEligible, c.reason, r.Ineligible)
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

// A fetch that needed its second attempt is a retry inside one attempt of a
// part — never a rerun. It costs no eligibility, and it is on the record: the
// action's own lines, prefixed with the part, in proof.json and the README.
func TestMerge_ASetupRetryIsRecordedAndEligible(t *testing.T) {
	f := newMergeFixture(t, tierFull, true)
	f.writeCI()
	f.writeShard(1, []entry{passEntry("TestA")}, nil, 1)
	rec := f.basePart("integration:2")
	rec.Gates = []gateRecord{{Gate: "integration", Result: "pass"}}
	rec.Images = map[string]string{"activity-shell": "sha256:aaa"}
	rec.Setup = []string{
		"fetch artifact plan-a1: attempt 1 timed out after 180s (limit 180s)",
		"fetch artifact plan-a1: attempt 2 ok in 14s (limit 360s)",
		"fetch artifact part-build-a1: attempt 1 ok in 9s (limit 180s)",
	}
	f.writePart(rec, map[string]any{"integration-tests.json": ledger{Suite: "integration", Tests: []entry{passEntry("TestB")}, Binaries: mBinaries()}})
	f.writeJobs(1)
	r, code := f.merge()
	if code != 0 || r.Result != "pass" || !r.MergeEligible {
		t.Fatalf("code %d result %s eligible %v: %v", code, r.Result, r.MergeEligible, r.Ineligible)
	}
	if len(r.Reruns) != 0 {
		t.Errorf("a retried fetch was recorded as a rerun: %+v", r.Reruns)
	}
	want := []string{
		"integration:2: fetch artifact plan-a1: attempt 1 timed out after 180s (limit 180s)",
		"integration:2: fetch artifact plan-a1: attempt 2 ok in 14s (limit 360s)",
	}
	if strings.Join(r.SetupRetries, "\n") != strings.Join(want, "\n") {
		t.Errorf("setup_retries =\n  %s\nwant\n  %s", strings.Join(r.SetupRetries, "\n  "), strings.Join(want, "\n  "))
	}
	readme, err := os.ReadFile(filepath.Join(f.out, "README.md"))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(readme), "- "+want[1]) {
		t.Error("the README does not list the retried fetch under Notes")
	}
}

func TestSetupRetries(t *testing.T) {
	for _, c := range []struct {
		name  string
		setup []string
		want  []string
	}{
		{"no setup log", nil, nil},
		{"every fetch first time", []string{
			"fetch artifact plan-a1: attempt 1 ok in 9s (limit 180s)",
			"fetch image mcr.microsoft.com/playwright:v1@sha256:ab: attempt 1 ok in 70s (limit 180s)",
		}, nil},
		{"one retried, one not, one gave up", []string{
			"fetch artifact plan-a1: attempt 1 failed, exit 1 after 2s (limit 180s)",
			"fetch artifact plan-a1: attempt 2 ok in 3s (limit 360s)",
			"fetch artifact part-build-a1: attempt 1 ok in 9s (limit 180s)",
			"fetch image mcr.microsoft.com/playwright:v1@sha256:ab: attempt 1 timed out after 180s (limit 180s)",
			"fetch image mcr.microsoft.com/playwright:v1@sha256:ab: attempt 2 timed out after 360s (limit 360s)",
			"fetch image mcr.microsoft.com/playwright:v1@sha256:ab: gave up after 2 attempts; a setup failure in a fetch step is a setup-failed part (docs/PROOFS.md §8)",
			"not an attempt line",
		}, []string{
			"ui-e2e:3: fetch artifact plan-a1: attempt 1 failed, exit 1 after 2s (limit 180s)",
			"ui-e2e:3: fetch artifact plan-a1: attempt 2 ok in 3s (limit 360s)",
			"ui-e2e:3: fetch image mcr.microsoft.com/playwright:v1@sha256:ab: attempt 1 timed out after 180s (limit 180s)",
			"ui-e2e:3: fetch image mcr.microsoft.com/playwright:v1@sha256:ab: attempt 2 timed out after 360s (limit 360s)",
			"ui-e2e:3: fetch image mcr.microsoft.com/playwright:v1@sha256:ab: gave up after 2 attempts; a setup failure in a fetch step is a setup-failed part (docs/PROOFS.md §8)",
		}},
	} {
		t.Run(c.name, func(t *testing.T) {
			if got := setupRetries("ui-e2e:3", c.setup); strings.Join(got, "\n") != strings.Join(c.want, "\n") {
				t.Errorf("got\n  %s\nwant\n  %s", strings.Join(got, "\n  "), strings.Join(c.want, "\n  "))
			}
		})
	}
}
