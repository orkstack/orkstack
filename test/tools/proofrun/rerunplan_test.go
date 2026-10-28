package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"testing"
)

// The planner of automatic reruns (.github/workflows/proof-rerun.yml, `proofrun
// rerun-plan`) reads one attempt of a proof run through `gh api`, here a fake,
// and decides whether "Re-run failed jobs" is worth an attempt. It reuses the
// merger's classify, so it reruns exactly what the merger would call
// rerun-eligible; and the merger stays the only authority, so a wrong rerun can
// only make a proof ineligible, never pass one.

const (
	planRepo    = "o/r"
	planRun     = "77"
	planAttempt = 1
)

// fakeJob is one job of the fake run, and what the fake API says of it.
type fakeJob struct {
	id          int64
	name        string
	conclusion  string
	steps       []githubStep
	annotations []apiAnnotation
	annErr      bool
	log         string
	logErr      bool
}

// fakeAPI answers the three reads the planner makes, by exact path, and
// records every path it was asked for.
type fakeAPI struct {
	jobs    []fakeJob
	jobsErr error
	pages   int // split the job listing into this many pages (0: one)
	attempt int // the attempt it holds (0: planAttempt)
	calls   []string
}

func (f *fakeAPI) get(path string, paginate bool) ([]byte, error) {
	f.calls = append(f.calls, path)
	attempt := f.attempt
	if attempt == 0 {
		attempt = planAttempt
	}
	listing := fmt.Sprintf("repos/%s/actions/runs/%s/attempts/%d/jobs?per_page=100", planRepo, planRun, attempt)
	if path == listing {
		if f.jobsErr != nil {
			return nil, f.jobsErr
		}
		var all []map[string]any
		for _, j := range f.jobs {
			var steps []map[string]string
			for _, s := range j.steps {
				steps = append(steps, map[string]string{"name": s.Name, "conclusion": s.Conclusion})
			}
			all = append(all, map[string]any{
				"id": j.id, "name": j.name, "status": "completed", "conclusion": j.conclusion,
				"check_run_url": fmt.Sprintf("https://api.github.com/repos/%s/check-runs/%d", planRepo, j.id+1000),
				"steps":         steps,
			})
		}
		n := f.pages
		if n < 1 {
			n = 1
		}
		var out strings.Builder
		for p := 0; p < n; p++ {
			var page []map[string]any
			for i, j := range all {
				if i%n == p {
					page = append(page, j)
				}
			}
			b, _ := json.Marshal(map[string]any{"total_count": len(all), "jobs": page})
			out.Write(b)
			out.WriteString("\n")
		}
		return []byte(out.String()), nil
	}
	for _, j := range f.jobs {
		if path == fmt.Sprintf("repos/%s/check-runs/%d/annotations?per_page=100", planRepo, j.id+1000) {
			if j.annErr {
				return nil, errors.New("HTTP 502")
			}
			b, _ := json.Marshal(j.annotations)
			if j.annotations == nil {
				b = []byte("[]")
			}
			return b, nil
		}
		if path == fmt.Sprintf("repos/%s/actions/jobs/%d/logs", planRepo, j.id) {
			if j.logErr {
				return nil, errors.New("HTTP 410")
			}
			return []byte(j.log), nil
		}
	}
	return nil, fmt.Errorf("the fake API has no %s", path)
}

func failAnn(msg string) apiAnnotation {
	return apiAnnotation{Message: msg, AnnotationLevel: "failure"}
}

// proofJob is the aggregator, red because a part is.
func proofJob(id int64) fakeJob {
	return fakeJob{id: id, name: "proof", conclusion: "failure",
		steps:       []githubStep{{Name: "merge", Conclusion: "failure"}},
		annotations: []apiAnnotation{failAnn("Process completed with exit code 1.")}}
}

func okJob(id int64, name string) fakeJob {
	return fakeJob{id: id, name: name, conclusion: "success"}
}

// lostJob is a part job whose runner GitHub lost.
func lostJob(id int64, name string) fakeJob {
	return fakeJob{id: id, name: name, conclusion: "failure",
		steps:       []githubStep{{Name: "integration shard", Conclusion: "failure"}},
		annotations: []apiAnnotation{failAnn(runnerLost)}}
}

// setupJob is a part job that failed in the named step, its proof step never
// started.
func setupJob(id int64, name, step string) fakeJob {
	return fakeJob{id: id, name: name, conclusion: "failure",
		steps: []githubStep{
			{Name: "Set up job", Conclusion: "success"}, {Name: step, Conclusion: "failure"},
			{Name: "integration shard", Conclusion: "skipped"}, {Name: "UI shard", Conclusion: "skipped"},
		},
		annotations: []apiAnnotation{failAnn("Process completed with exit code 124.")},
		log:         "2026-10-28T10:00:00Z fetch artifact plan-a1: gave up after 2 attempts\n"}
}

// degradedJob is a part job that stopped on a runner below the floor: its
// proof step failed, no test started, and the part left its one annotation.
func degradedJob(id int64, name string) fakeJob {
	return fakeJob{id: id, name: name, conclusion: "failure",
		steps: []githubStep{{Name: "Set up job", Conclusion: "success"}, {Name: "integration shard", Conclusion: "failure"}},
		annotations: []apiAnnotation{
			failAnn(runnerBelowFloorAnnotation), failAnn("Process completed with exit code 1."),
		},
		log: "== runner-health: class github-hosted/ubuntu24/4cpu\n   runner-health: this runner is BELOW the floor\n"}
}

func decide(t *testing.T, f *fakeAPI, attempt, max int) rerunDecision {
	t.Helper()
	return planRerun(f.get, planRepo, planRun, attempt, max)
}

func partsOf(d rerunDecision) string {
	var out []string
	for _, p := range d.Parts {
		out = append(out, p.Part+"="+p.Class)
	}
	return strings.Join(out, ",")
}

func TestRerunPlan_RerunsWhenEveryFailedJobIsTheAggregatorOrAnEligiblePart(t *testing.T) {
	for _, c := range []struct {
		name  string
		jobs  []fakeJob
		parts string
	}{
		{"a lost runner", []fakeJob{okJob(1, "plan"), lostJob(2, "integration (2)"), proofJob(9)}, "integration:2=lost-runner"},
		{"a fetch that gave up", []fakeJob{setupJob(2, "ui-e2e (1)", "fetch browser image"), proofJob(9)}, "ui-e2e:1=setup-failed"},
		{"the host setup", []fakeJob{setupJob(2, "activity-fixture (git)", "proof host"), proofJob(9)}, "activity-fixture:git=setup-failed"},
		{"the observability stack", []fakeJob{setupJob(2, "observability", "observability stack"), proofJob(9)}, "observability=setup-failed"},
		{"the fast part's install", []fakeJob{setupJob(2, "fast", "bun install (site)"), proofJob(9)}, "fast=setup-failed"},
		{"a degraded runner", []fakeJob{degradedJob(2, "integration (3)"), proofJob(9)}, "integration:3=runner-degraded"},
		{"the race part's lost runner", []fakeJob{lostJob(2, "race"), proofJob(9)}, "race=lost-runner"},
		{"three classes at once", []fakeJob{
			okJob(1, "integration (1)"), lostJob(2, "integration (2)"), degradedJob(3, "ui-e2e (4)"),
			setupJob(4, "activity-fixture (json)", "fetch build"), proofJob(9),
		}, "activity-fixture:json=setup-failed,integration:2=lost-runner,ui-e2e:4=runner-degraded"},
		{"the aggregator listed first", []fakeJob{proofJob(9), lostJob(2, "integration (2)")}, "integration:2=lost-runner"},
	} {
		t.Run(c.name, func(t *testing.T) {
			f := &fakeAPI{jobs: c.jobs}
			d := decide(t, f, planAttempt, 3)
			if !d.Rerun || d.Reason != "eligible" || partsOf(d) != c.parts {
				t.Errorf("decision = %+v, want a rerun of %s", d, c.parts)
			}
		})
	}
}

func TestRerunPlan_NeverRerunsWhenAnyFailedJobIsNotEligible(t *testing.T) {
	for _, c := range []struct {
		name   string
		jobs   []fakeJob
		reason string
	}{
		{"only the aggregator failed", []fakeJob{okJob(2, "integration (2)"), proofJob(9)}, "no-eligible-part"},
		{"nothing failed", []fakeJob{okJob(2, "integration (2)")}, "no-eligible-part"},
		{"a part that recorded a failing test", []fakeJob{
			lostJob(2, "integration (2)"),
			{id: 3, name: "integration (3)", conclusion: "failure", steps: []githubStep{{Name: "integration shard", Conclusion: "failure"}},
				log: "2026-10-28T10:00:00Z proofrun: start p TestA\n2026-10-28T10:00:09Z proofrun: outcome fail p TestA 9.00\n"},
			proofJob(9)}, "not-eligible"},
		{"a part that failed after its proof step started", []fakeJob{
			{id: 2, name: "ui-e2e (1)", conclusion: "failure", steps: []githubStep{{Name: "UI shard", Conclusion: "failure"}}}, proofJob(9)}, "not-eligible"},
		{"a failed step outside the fetch and install steps", []fakeJob{
			setupJob(2, "integration (2)", "Run actions/checkout@v7.0.1"), proofJob(9)}, "not-eligible"},
		{"an install step of another kind of part", []fakeJob{setupJob(2, "integration (2)", "Chromium (host)"), proofJob(9)}, "not-eligible"},
		{"a cancelled part", []fakeJob{{id: 2, name: "integration (2)", conclusion: "cancelled"}, proofJob(9)}, "not-eligible"},
		{"a cancel in a fetch step", []fakeJob{
			{id: 2, name: "integration (2)", conclusion: "cancelled", steps: []githubStep{{Name: "fetch plan", Conclusion: "failure"}}}, proofJob(9)}, "not-eligible"},
		{"a timed-out test", []fakeJob{
			{id: 2, name: "integration (2)", conclusion: "cancelled", steps: []githubStep{{Name: "integration shard", Conclusion: "failure"}},
				annotations: []apiAnnotation{failAnn("The job running on runner GitHub Actions 7 has exceeded the maximum execution time of 45 minutes.")},
				log:         "2026-10-28T10:00:00Z proofrun: start p TestA\n"}, proofJob(9)}, "not-eligible"},
		{"the base job failed", []fakeJob{{id: 1, name: "base", conclusion: "failure"}, lostJob(2, "integration (2)"), proofJob(9)}, "other-job-failed"},
		{"the plan job failed", []fakeJob{lostJob(1, "plan"), proofJob(9)}, "other-job-failed"},
		{"the build job failed", []fakeJob{setupJob(1, "build", "fetch plan"), lostJob(2, "integration (2)"), proofJob(9)}, "other-job-failed"},
		{"a job nobody knows", []fakeJob{{id: 1, name: "deploy", conclusion: "failure"}, lostJob(2, "integration (2)"), proofJob(9)}, "other-job-failed"},
		{"a matrix entry the part names do not allow", []fakeJob{lostJob(2, "integration (2::notice::x)"), proofJob(9)}, "other-job-failed"},
		{"an activity the part names do not allow", []fakeJob{lostJob(2, "activity-fixture (rm -rf)"), proofJob(9)}, "other-job-failed"},
		{"an unreadable annotation list", []fakeJob{
			{id: 2, name: "integration (2)", conclusion: "failure", steps: []githubStep{{Name: "fetch plan", Conclusion: "failure"}}, annErr: true}, proofJob(9)}, "not-eligible"},
		{"one eligible part among a part with a result", []fakeJob{
			setupJob(2, "integration (2)", "fetch plan"),
			{id: 3, name: "ui-e2e (2)", conclusion: "failure", steps: []githubStep{{Name: "UI shard", Conclusion: "failure"}}}, proofJob(9)}, "not-eligible"},
	} {
		t.Run(c.name, func(t *testing.T) {
			f := &fakeAPI{jobs: c.jobs}
			d := decide(t, f, planAttempt, 3)
			if d.Rerun || d.Reason != c.reason || len(d.Parts) != 0 {
				t.Errorf("decision = %+v, want no rerun (%s)", d, c.reason)
			}
		})
	}
}

// What a planner may rerun is what the merger would call rerun-eligible: for
// every fetch or install step the merger lists, the planner reruns; for the
// plan and build, and for any step outside the table, it does not.
func TestRerunPlan_AgreesWithTheMergersSetupTable(t *testing.T) {
	jobNames := map[string]string{
		"integration": "integration (2)", "ui-e2e": "ui-e2e (1)", "activity-fixture": "activity-fixture (shell)",
		"observability": "observability", "fast": "fast", "race": "race", "race-repeat": "race-repeat", "fuzz": "fuzz",
	}
	for kind, steps := range setupStepNames {
		for step := range steps {
			f := &fakeAPI{jobs: []fakeJob{setupJob(2, jobNames[kind], step), proofJob(9)}}
			if d := decide(t, f, planAttempt, 3); !d.Rerun {
				t.Errorf("%s failing in %q is not rerun: %+v", kind, step, d)
			}
		}
		f := &fakeAPI{jobs: []fakeJob{setupJob(2, jobNames[kind], "Run actions/setup-go@v7.0.0"), proofJob(9)}}
		if d := decide(t, f, planAttempt, 3); d.Rerun {
			t.Errorf("%s failing in an unlisted step is rerun: %+v", kind, d)
		}
	}
}

func TestRerunPlan_TheAttemptCapStopsBeforeAnyRead(t *testing.T) {
	for _, attempt := range []int{3, 4, 10} {
		f := &fakeAPI{jobs: []fakeJob{lostJob(2, "integration (2)"), proofJob(9)}}
		d := planRerun(f.get, planRepo, planRun, attempt, 3)
		if d.Rerun || d.Reason != "attempt-cap" {
			t.Errorf("attempt %d: decision = %+v", attempt, d)
		}
		if len(f.calls) != 0 {
			t.Errorf("attempt %d: the planner read %v before honouring the cap", attempt, f.calls)
		}
	}
	// Attempts 1 and 2 may be answered: at most two automatic reruns.
	for _, attempt := range []int{1, 2} {
		f := &fakeAPI{attempt: attempt, jobs: []fakeJob{lostJob(2, "integration (2)"), proofJob(9)}}
		if d := decide(t, f, attempt, 3); !d.Rerun {
			t.Errorf("attempt %d: decision = %+v, want a rerun", attempt, d)
		}
	}
}

func TestRerunPlan_ReadsTheAttemptItWasToldAbout(t *testing.T) {
	f := &fakeAPI{jobs: []fakeJob{proofJob(9)}}
	_ = decide(t, f, planAttempt, 3)
	want := "repos/o/r/actions/runs/77/attempts/1/jobs?per_page=100"
	if len(f.calls) == 0 || f.calls[0] != want {
		t.Errorf("calls = %v, want %s first", f.calls, want)
	}
}

func TestRerunPlan_ADegradedMarkerIsAcceptedOnlyFromTheProofStepsOwnAnnotation(t *testing.T) {
	mk := func(mutate func(j *fakeJob)) fakeJob {
		j := degradedJob(2, "integration (3)")
		mutate(&j)
		return j
	}
	for _, c := range []struct {
		name string
		job  fakeJob
		want bool
	}{
		{"the genuine annotation", mk(func(j *fakeJob) {}), true},
		{"a UI part", mk(func(j *fakeJob) { j.name = "ui-e2e (1)"; j.steps[1].Name = "UI shard" }), true},
		{"a test started", mk(func(j *fakeJob) { j.log += "2026-10-28T10:00:00Z proofrun: start p TestA\n" }), false},
		{"a test finished", mk(func(j *fakeJob) { j.log += "2026-10-28T10:00:00Z proofrun: outcome pass p TestA 1.00\n" }), false},
		{"the job's log unreadable", mk(func(j *fakeJob) { j.logErr = true }), false},
		{"the failed step is not the proof step", mk(func(j *fakeJob) {
			j.steps = []githubStep{{Name: "Run actions/checkout@v7.0.1", Conclusion: "failure"}, {Name: "integration shard", Conclusion: "skipped"}}
		}), false},
		{"the marker at a lower level", mk(func(j *fakeJob) { j.annotations[0].AnnotationLevel = "notice" }), false},
		{"the marker with more words", mk(func(j *fakeJob) { j.annotations[0].Message = runnerBelowFloorAnnotation + "; run it again" }), false},
		{"a different message", mk(func(j *fakeJob) { j.annotations[0].Message = "proof: runner below the floor" }), false},
		{"no marker", mk(func(j *fakeJob) { j.annotations = j.annotations[1:] }), false},
		{"a cancelled job", mk(func(j *fakeJob) { j.conclusion = "cancelled" }), false},
		{"the build part, which the floor does not bind", mk(func(j *fakeJob) { j.name = "build"; j.steps[1].Name = "build" }), false},
		{"the fast part, which the floor does not bind", mk(func(j *fakeJob) { j.name = "fast"; j.steps[1].Name = "fast" }), false},
	} {
		t.Run(c.name, func(t *testing.T) {
			f := &fakeAPI{jobs: []fakeJob{c.job, proofJob(9)}}
			d := decide(t, f, planAttempt, 3)
			if d.Rerun != c.want {
				t.Errorf("decision = %+v, want a rerun: %v", d, c.want)
			}
			if c.want && !strings.HasSuffix(partsOf(d), "=runner-degraded") {
				t.Errorf("parts = %s, want a degraded part", partsOf(d))
			}
		})
	}
}

func TestRerunPlan_AFailedReadFailsClosed(t *testing.T) {
	f := &fakeAPI{jobsErr: errors.New("HTTP 403: Resource not accessible by integration")}
	d := decide(t, f, planAttempt, 3)
	if d.Rerun || d.Reason != "unreadable" {
		t.Errorf("decision = %+v", d)
	}
	for _, body := range []string{"{not json", `{"jobs": 7}`, ""} {
		d := planRerun(func(path string, _ bool) ([]byte, error) { return []byte(body), nil }, planRepo, planRun, planAttempt, 3)
		if d.Rerun {
			t.Errorf("a job listing %q was rerun: %+v", body, d)
		}
	}
	// A check run URL that names no check run: its annotations cannot be read.
	d = planRerun(func(path string, _ bool) ([]byte, error) {
		if strings.HasSuffix(path, "/jobs?per_page=100") {
			return []byte(`{"jobs":[{"id":2,"name":"integration (2)","conclusion":"failure","check_run_url":"","steps":[{"name":"fetch plan","conclusion":"failure"}]}]}`), nil
		}
		return nil, errors.New("not asked")
	}, planRepo, planRun, planAttempt, 3)
	if d.Rerun {
		t.Errorf("a job with no check run was rerun: %+v", d)
	}
}

func TestRerunPlan_ReadsEveryPageOfTheJobListing(t *testing.T) {
	f := &fakeAPI{pages: 3, jobs: []fakeJob{
		okJob(1, "integration (1)"), lostJob(2, "integration (2)"), okJob(3, "integration (3)"),
		setupJob(4, "ui-e2e (1)", "bun install"), okJob(5, "fast"), proofJob(9),
	}}
	d := decide(t, f, planAttempt, 3)
	if !d.Rerun {
		t.Fatalf("decision = %+v", d)
	}
	if got := partsOf(d); got != "integration:2=lost-runner,ui-e2e:1=setup-failed" {
		t.Errorf("parts = %s", got)
	}
}

func TestRerunPlan_ReadsOnlyWhatAFailedPartNeeds(t *testing.T) {
	f := &fakeAPI{jobs: []fakeJob{okJob(1, "integration (1)"), lostJob(2, "integration (2)"), proofJob(9)}}
	_ = decide(t, f, planAttempt, 3)
	for _, call := range f.calls {
		if strings.Contains(call, "check-runs/1001") || strings.Contains(call, "/jobs/1/") || strings.Contains(call, "check-runs/1009") || strings.Contains(call, "/jobs/9/") {
			t.Errorf("the planner read %s, of a job that succeeded or is the aggregator", call)
		}
	}
}

// The planner's output goes to a workflow's step output and its log: it holds
// only words the planner validated, never an annotation or a log line, because
// a line that starts `::` is a command to the runner.
func TestRerunPlan_TheOutputHoldsOnlyValidatedVocabulary(t *testing.T) {
	hostile := []apiAnnotation{
		failAnn("::error::pwned"), failAnn("line\n::set-output name=rerun::true"), failAnn(runnerLost + "\n::add-mask::x"),
	}
	jobs := []fakeJob{
		{id: 2, name: "integration (2)", conclusion: "failure",
			steps:       []githubStep{{Name: "::notice::step", Conclusion: "failure"}},
			annotations: hostile, log: "::error::from the log\n##[error]x\n"},
		{id: 3, name: "ui-e2e (1::error::x)", conclusion: "failure", annotations: hostile},
		lostJob(4, "activity-fixture (git)"),
		proofJob(9),
	}
	jobs[2].annotations = append(append([]apiAnnotation{}, jobs[2].annotations...), hostile...)
	for _, only := range [][]fakeJob{{jobs[2], proofJob(9)}, jobs} {
		f := &fakeAPI{jobs: only}
		out := decide(t, f, planAttempt, 3).output()
		if !regexp.MustCompile(`^rerun=(true|false)\nreason=[a-z-]+\nparts=[A-Za-z0-9:=,-]*\n$`).MatchString(out) {
			t.Errorf("the output is not the planner's vocabulary alone:\n%q", out)
		}
		if strings.Contains(out, "pwned") || strings.Contains(out, "::") || strings.Contains(out, "##[") {
			t.Errorf("the output carries annotation or log text:\n%q", out)
		}
	}
	f := &fakeAPI{jobs: []fakeJob{lostJob(2, "integration (2)"), setupJob(3, "ui-e2e (1)", "fetch plan"), proofJob(9)}}
	if got, want := decide(t, f, planAttempt, 3).output(), "rerun=true\nreason=eligible\nparts=integration:2=lost-runner,ui-e2e:1=setup-failed\n"; got != want {
		t.Errorf("output = %q, want %q", got, want)
	}
	f = &fakeAPI{jobs: []fakeJob{proofJob(9)}}
	if got, want := decide(t, f, planAttempt, 3).output(), "rerun=false\nreason=no-eligible-part\nparts=\n"; got != want {
		t.Errorf("output = %q, want %q", got, want)
	}
}

// ---- the subcommand ------------------------------------------------------------

// captureStdout runs fn and returns what it wrote to the commands' standard
// output.
func captureStdout(t *testing.T, fn func() int) (string, int) {
	t.Helper()
	var b bytes.Buffer
	prev := stdout
	stdout = &b
	defer func() { stdout = prev }()
	code := fn()
	return b.String(), code
}

func TestCmdRerunPlan_PrintsTheDecisionForTheRunItIsGiven(t *testing.T) {
	f := &fakeAPI{jobs: []fakeJob{lostJob(2, "integration (2)"), proofJob(9)}}
	old := rerunPlanGH
	rerunPlanGH = f.get
	defer func() { rerunPlanGH = old }()
	out, code := captureStdout(t, func() int {
		return cmdRerunPlan([]string{"-repo", planRepo, "-run", planRun, "-attempt", "1"})
	})
	if code != 0 || out != "rerun=true\nreason=eligible\nparts=integration:2=lost-runner\n" {
		t.Errorf("code %d output %q", code, out)
	}
	out, code = captureStdout(t, func() int {
		return cmdRerunPlan([]string{"-repo", planRepo, "-run", planRun, "-attempt", "3"})
	})
	if code != 0 || out != "rerun=false\nreason=attempt-cap\nparts=\n" {
		t.Errorf("at the cap: code %d output %q", code, out)
	}
	out, code = captureStdout(t, func() int {
		return cmdRerunPlan([]string{"-repo", planRepo, "-run", planRun, "-attempt", "2", "-max-attempts", "2"})
	})
	if code != 0 || out != "rerun=false\nreason=attempt-cap\nparts=\n" {
		t.Errorf("with -max-attempts 2: code %d output %q", code, out)
	}
	f.jobsErr = errors.New("HTTP 500")
	out, code = captureStdout(t, func() int {
		return cmdRerunPlan([]string{"-repo", planRepo, "-run", planRun, "-attempt", "1"})
	})
	if code != 0 || out != "rerun=false\nreason=unreadable\nparts=\n" {
		t.Errorf("an unreadable run: code %d output %q", code, out)
	}
}

func TestCmdRerunPlan_RefusesAnInvocationItCannotTrust(t *testing.T) {
	quiet(t)
	f := &fakeAPI{}
	old := rerunPlanGH
	rerunPlanGH = f.get
	defer func() { rerunPlanGH = old }()
	for _, args := range [][]string{
		nil,
		{"-run", planRun, "-attempt", "1"},
		{"-repo", planRepo, "-attempt", "1"},
		{"-repo", planRepo, "-run", planRun},
		{"-repo", planRepo, "-run", planRun, "-attempt", "0"},
		{"-repo", planRepo, "-run", planRun, "-attempt", "1", "-max-attempts", "0"},
		{"-repo", "no-slash", "-run", planRun, "-attempt", "1"},
		{"-repo", "o/r/../x", "-run", planRun, "-attempt", "1"},
		{"-repo", "o/r?x=1", "-run", planRun, "-attempt", "1"},
		{"-repo", planRepo, "-run", "7/7", "-attempt", "1"},
		{"-repo", planRepo, "-run", "abc", "-attempt", "1"},
		{"-repo", planRepo, "-run", planRun, "-attempt", "1", "extra"},
	} {
		out, code := captureStdout(t, func() int { return cmdRerunPlan(args) })
		if code != 2 || out != "" {
			t.Errorf("%v: code %d output %q, want exit 2 and no output", args, code, out)
		}
	}
	if len(f.calls) != 0 {
		t.Errorf("an invalid invocation read %v", f.calls)
	}
}

func TestGhArgs_AsksForEveryPageOnlyWhenToldTo(t *testing.T) {
	if got := strings.Join(ghArgs("repos/o/r/actions/jobs/1/logs", false), " "); got != "api repos/o/r/actions/jobs/1/logs" {
		t.Errorf("args = %q", got)
	}
	if got := strings.Join(ghArgs("repos/o/r/actions/runs/1/attempts/1/jobs?per_page=100", true), " "); got != "api --paginate repos/o/r/actions/runs/1/attempts/1/jobs?per_page=100" {
		t.Errorf("args = %q", got)
	}
}
