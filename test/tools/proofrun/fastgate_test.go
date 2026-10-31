package main

import (
	"fmt"
	"strings"
	"testing"
)

// The stack shards — `integration`, `ui-e2e` and `activity-fixture` — start
// only once the `fast` part has passed (docs/PROOFS.md §5). A red `fast` is
// never a passing proof: its unit gate exits 0 when every failure is a
// quarantined one, and the merger fails the proof on any other red gate of
// it. So a deterministic unit or lint failure stops the shards before they
// start, instead of spending their runner time on a proof that cannot pass.
// A `fast` that failed in a setup step, or whose runner GitHub lost, left no
// result; "Re-run failed jobs" runs it again together with the shards that
// need it, which never ran, and the merger reads the run as one proof.

// shardGate is the condition each stack shard runs on, before the proof
// guard: what the run must have and what `fast` must have done.
var shardGate = map[string]string{
	"integration": "!cancelled() && needs.base.result == 'success' && needs.plan.result == 'success' && needs.build.result == 'success'" +
		" && (needs.fast.result == 'success' || (inputs.tier == 'nightly' && needs.fast.result == 'skipped'))",
	"ui-e2e": "!cancelled() && needs.base.result == 'success' && needs.plan.result == 'success' && needs.build.result == 'success'" +
		" && (needs.fast.result == 'success' || (inputs.tier == 'nightly' && needs.fast.result == 'skipped'))" +
		" && needs.plan.outputs.ui_matrix != '[]'",
	"activity-fixture": "inputs.tier != 'nightly' && !cancelled() && needs.base.result == 'success' && needs.plan.result == 'success'" +
		" && needs.build.result == 'success' && needs.fast.result == 'success'",
}

func shardGateProblems(w *wfFile) []string {
	var p []string
	for job, gate := range shardGate {
		j := w.Jobs[job]
		if j == nil {
			p = append(p, "proof.yml has no "+job+" job")
			continue
		}
		needs := map[string]bool{}
		for _, n := range j.needs() {
			needs[n] = true
		}
		for _, n := range []string{"base", "plan", "build", "fast"} {
			if !needs[n] {
				p = append(p, fmt.Sprintf("jobs.%s does not need %s", job, n))
			}
		}
		if want := gate + " && (" + proofGuard + ")"; normalizeIf(j.If) != want {
			p = append(p, fmt.Sprintf("jobs.%s.if = %q, want %q", job, normalizeIf(j.If), want))
		}
	}
	// Nothing else waits for `fast`: the plan, the build and the race gate run
	// beside it, so a green run's wall grows by the shards' wait alone.
	for _, job := range []string{"base", "plan", "build", "race", "race-repeat", "fuzz", "observability"} {
		for _, n := range w.Jobs[job].needs() {
			if n == "fast" {
				p = append(p, fmt.Sprintf("jobs.%s waits for fast", job))
			}
		}
	}
	return p
}

func TestWorkflows_TheShardsStartOnlyOnceTheFastPartPassed(t *testing.T) {
	replace := func(job, old, new string) func(w *wfFile) {
		return func(w *wfFile) { w.Jobs[job].If = strings.Replace(w.Jobs[job].If, old, new, 1) }
	}
	expectEachMutationRefused(t, "proof.yml", shardGateProblems, []mutation{
		{"integration does not need fast", func(w *wfFile) { w.Jobs["integration"].Needs = []any{"base", "plan", "build"} }},
		{"ui-e2e does not need fast", func(w *wfFile) { w.Jobs["ui-e2e"].Needs = []any{"base", "plan", "build"} }},
		{"activity-fixture does not need fast", func(w *wfFile) { w.Jobs["activity-fixture"].Needs = []any{"base", "plan", "build"} }},
		{"a shard that runs on any end of fast", replace("integration", "needs.fast.result == 'success' ||", "needs.fast.result != 'skipped' ||")},
		{"a shard that runs after a failed fast", replace("ui-e2e", "needs.fast.result == 'success' ||", "needs.fast.result == 'failure' ||")},
		{"a shard that runs on a skipped fast in any tier", replace("integration", "inputs.tier == 'nightly' && ", "")},
		{"a fixture that runs on a skipped fast", replace("activity-fixture", "needs.fast.result == 'success'", "needs.fast.result != 'failure'")},
		{"a shard that runs on a cancelled run", replace("integration", "!cancelled()", "always()")},
		{"a shard that runs without the build", replace("integration", " && needs.build.result == 'success'", "")},
		{"a shard that runs without the plan", replace("activity-fixture", " && needs.plan.result == 'success'", "")},
		{"a UI shard with no files", replace("ui-e2e", " && needs.plan.outputs.ui_matrix != '[]'", "")},
		{"the plan waits for fast", func(w *wfFile) { w.Jobs["plan"].Needs = []any{"base", "fast"} }},
		{"the race gate waits for fast", func(w *wfFile) { w.Jobs["race"].Needs = []any{"base", "fast"} }},
		{"observability waits for fast", func(w *wfFile) { w.Jobs["observability"].Needs = []any{"base", "plan", "build", "fast"} }},
	})
}

// ranJob is GitHub's record of a job that ran in attempt, with the steps it
// recorded; a later attempt that carried it forward repeats these fields.
func ranJob(name string, attempt int, start, end, conclusion string, steps ...githubStep) githubJob {
	j := githubJob{Name: name, Conclusion: conclusion, ID: int64(1000*attempt + len(name)), RunAttempt: attempt,
		StartedAt: start, CompletedAt: end, RunnerName: "GitHub Actions " + name, Steps: steps}
	for _, s := range steps {
		if s.Conclusion == "failure" && j.FailedStep == "" {
			j.FailedStep = s.Name
		}
	}
	return j
}

// skippedJob is GitHub's record of a job it skipped because a job it needs did
// not succeed: one record for a whole matrix, its name without a shard, no
// steps and no runner.
func skippedJob(name string, attempt int) githubJob {
	return githubJob{Name: name, Conclusion: "skipped", RunAttempt: attempt, StartedAt: "2026-10-30T10:03:00Z", CompletedAt: "2026-10-30T10:03:00Z"}
}

func okSteps(proofStep string) []githubStep {
	return []githubStep{{Name: "Set up job", Conclusion: "success"}, {Name: proofStep, Conclusion: "success"}}
}

// fastSetupFailed is attempt 1 of a run whose `fast` failed installing the
// site's dependencies, so the shards that need it were skipped.
func fastSetupFailed() []githubJob {
	return []githubJob{
		ranJob("base", 1, "2026-10-30T10:00:00Z", "2026-10-30T10:00:05Z", "success", okSteps("base")...),
		ranJob("plan", 1, "2026-10-30T10:00:06Z", "2026-10-30T10:02:30Z", "success", okSteps("plan")...),
		ranJob("build", 1, "2026-10-30T10:02:31Z", "2026-10-30T10:04:00Z", "success", okSteps("build")...),
		ranJob("race", 1, "2026-10-30T10:00:06Z", "2026-10-30T10:09:00Z", "success", okSteps("race")...),
		ranJob("fast", 1, "2026-10-30T10:00:06Z", "2026-10-30T10:03:00Z", "failure",
			githubStep{Name: "Set up job", Conclusion: "success"}, githubStep{Name: "bun install", Conclusion: "success"},
			githubStep{Name: "bun install (site)", Conclusion: "failure"}, githubStep{Name: "fast", Conclusion: "skipped"}),
		skippedJob("integration", 1), skippedJob("ui-e2e", 1), skippedJob("activity-fixture", 1),
		skippedJob("observability", 1), skippedJob("race-repeat", 1), skippedJob("fuzz", 1),
		ranJob("proof", 1, "2026-10-30T10:09:05Z", "2026-10-30T10:10:00Z", "failure", githubStep{Name: "merge", Conclusion: "failure"}),
	}
}

// carriedInto is job as "Re-run failed jobs" lists it in a later attempt it
// did not run it in: a new id and that attempt's number, the rest unchanged.
func carriedInto(j githubJob, attempt int) githubJob {
	j.ID += 1
	j.RunAttempt = attempt
	return j
}

// writeShardsAt writes every stack part of the fixture's plan as a passing
// attempt.
func (f *mergeFixture) writeShardsAt(attempt int) {
	f.t.Helper()
	f.writeShard(1, []entry{passEntry("TestA")}, nil, attempt)
	f.writeShard(2, []entry{passEntry("TestB")}, nil, attempt)
	ui := f.basePart("ui-e2e:1")
	ui.Attempt = attempt
	ui.Gates = []gateRecord{{Gate: "ui-e2e", Result: "pass"}}
	ui.Images = map[string]string{"activity-shell": "sha256:aaa"}
	f.writePart(ui, map[string]any{"ui-e2e-tests.json": uiLedgerFixture()})
	for _, a := range fixtureActivities {
		fix := f.basePart("activity-fixture:" + a)
		fix.Attempt = attempt
		fix.Gates = []gateRecord{{Gate: "activity-fixture", Result: "pass"}}
		fix.Images = map[string]string{"activity-shell": "sha256:aaa"}
		f.writePart(fix, nil)
	}
}

// writeAttempt1Parts writes what attempt 1 of such a run left: the plan, the
// build and the race gate; `fast` and the shards left nothing.
func (f *mergeFixture) writeAttempt1Parts() {
	f.t.Helper()
	f.writePart(f.basePart("plan"), nil)
	build := f.basePart("build")
	build.Gates = []gateRecord{{Gate: "images", Result: "pass"}}
	build.Images = map[string]string{"activity-shell": "sha256:aaa"}
	f.writePart(build, nil)
	f.writeRace(raceGateRecords(), raceLedger())
}

// "Re-run failed jobs" after the setup failure runs `fast` and the shards
// that need it, and carries the plan, the build and the race gate forward:
// the shards' first attempt is the one that ran them, so the proof is
// merge-eligible once they pass, with one rerun, of `fast`.
func TestMerge_AFastSetupFailureRunsAgainWithTheShardsThatWaitedForIt(t *testing.T) {
	f := newMergeFixture(t, tierFull, true)
	f.writeAttempt1Parts()
	fast := f.basePart("fast")
	fast.Attempt = 2
	fast.Gates = fastGateRecords()
	f.writePart(fast, map[string]any{"unit-tests.json": unitLedger()})
	f.writeShardsAt(2)

	first := fastSetupFailed()
	f.writeJobs(1, first...)
	second := []githubJob{
		carriedInto(first[0], 2), carriedInto(first[1], 2), carriedInto(first[2], 2), carriedInto(first[3], 2),
		ranJob("fast", 2, "2026-10-30T10:15:00Z", "2026-10-30T10:27:00Z", "success", okSteps("fast")...),
		ranJob("integration (1)", 2, "2026-10-30T10:27:10Z", "2026-10-30T10:36:00Z", "success", okSteps("integration shard")...),
		ranJob("integration (2)", 2, "2026-10-30T10:27:10Z", "2026-10-30T10:38:00Z", "success", okSteps("integration shard")...),
		ranJob("ui-e2e (1)", 2, "2026-10-30T10:27:10Z", "2026-10-30T10:34:00Z", "success", okSteps("UI shard")...),
		skippedJob("observability", 2), skippedJob("race-repeat", 2), skippedJob("fuzz", 2),
	}
	for _, a := range fixtureActivities {
		second = append(second, ranJob("activity-fixture ("+a+")", 2, "2026-10-30T10:27:10Z", "2026-10-30T10:31:00Z", "success", okSteps("activity fixture")...))
	}
	f.writeJobs(2, second...)

	r, code := f.merge()
	if code != 0 || r.Result != "pass" || !r.MergeEligible {
		t.Fatalf("code %d result %s eligible %v:\n%s", code, r.Result, r.MergeEligible, strings.Join(r.Ineligible, "\n"))
	}
	if len(r.Reruns) != 1 || r.Reruns[0].Part != "fast" || r.Reruns[0].Attempt != 2 || r.Reruns[0].Reason != reasonSetupFailed {
		t.Errorf("reruns = %+v, want fast's alone, setup-failed", r.Reruns)
	}
	if len(r.SetupFailures) != 1 || r.SetupFailures[0] != (setupFailure{Part: "fast", Attempt: 1, Step: "bun install (site)", Conclusion: "failure"}) {
		t.Errorf("setup_failures = %+v", r.SetupFailures)
	}
	carried := map[string]bool{}
	for _, c := range r.CarriedForward {
		carried[c.Part] = true
	}
	if len(r.CarriedForward) != 3 || !carried["plan"] || !carried["build"] || !carried["race"] {
		t.Errorf("carried_forward = %+v, want the plan, the build and the race gate", r.CarriedForward)
	}
	// The planner reads attempt 1 the same way: `fast` alone may run again,
	// and the shards GitHub skipped are no failure of their own.
	var api []fakeJob
	for _, j := range first {
		api = append(api, fakeJob{id: j.ID, name: j.Name, conclusion: j.Conclusion, steps: j.Steps})
	}
	if d := decide(t, &fakeAPI{jobs: api}, planAttempt, 3); d.output() != "rerun=true\nreason=eligible\nparts=fast=setup-failed\n" {
		t.Errorf("the planner decides %q", d.output())
	}
}

// A `fast` that recorded a failing gate is a result: the shards never start,
// the proof fails on the gate, and each shard says why it has no result
// rather than asking for a rerun no rule allows.
func TestMerge_AFastProductFailureLeavesTheShardsNotRun(t *testing.T) {
	f := newMergeFixture(t, tierFull, true)
	f.writeAttempt1Parts()
	gates := fastGateRecords()
	for i := range gates {
		if gates[i].Gate == "unit" {
			gates[i].Result = "fail"
		}
	}
	f.writeFast(gates)
	jobs := fastSetupFailed()
	for i := range jobs {
		if jobs[i].Name == "fast" {
			jobs[i] = ranJob("fast", 1, "2026-10-30T10:00:06Z", "2026-10-30T10:12:00Z", "failure",
				githubStep{Name: "bun install", Conclusion: "success"}, githubStep{Name: "fast", Conclusion: "failure"})
		}
	}
	f.writeJobs(1, jobs...)
	r, code := f.merge()
	if code == 0 || r.Result != "fail" || r.MergeEligible {
		t.Fatalf("code %d result %s eligible %v", code, r.Result, r.MergeEligible)
	}
	reasons := strings.Join(r.Ineligible, "\n")
	if !strings.Contains(reasons, "gate unit failed") {
		t.Errorf("ineligible lacks the failed gate:\n%s", reasons)
	}
	for _, part := range []string{"integration:1", "integration:2", "ui-e2e:1", "activity-fixture:shell"} {
		want := "part " + part + " did not run: GitHub skipped its job at attempt 1 because a job it needs did not succeed; it runs when that job runs again, or in a new run"
		if !strings.Contains(reasons, want) {
			t.Errorf("ineligible lacks %q:\n%s", want, reasons)
		}
	}
	if strings.Contains(reasons, "part integration:1 left no result") {
		t.Errorf("a shard GitHub skipped is reported as one that left no result:\n%s", reasons)
	}
	var api []fakeJob
	for _, j := range jobs {
		api = append(api, fakeJob{id: j.ID, name: j.Name, conclusion: j.Conclusion, steps: j.Steps})
	}
	if d := decide(t, &fakeAPI{jobs: api}, planAttempt, 3); d.Rerun || d.Reason != "not-eligible" {
		t.Errorf("the planner reruns a fast that recorded its result: %+v", d)
	}
}

// A record GitHub skipped says nothing of an attempt that left an artifact:
// the artifact is the attempt, and a skipped record of a shard's own name is
// never read as one that never ran.
func TestMerge_ASkippedRecordNeverHidesAnAttemptThatRan(t *testing.T) {
	f := newMergeFixture(t, tierFull, true)
	f.writeCI()
	f.writeShard(1, []entry{passEntry("TestA")}, nil, 1)
	f.writeShard(2, []entry{passEntry("TestB")}, nil, 1)
	f.writeShard(2, []entry{passEntry("TestB")}, nil, 2)
	f.writeJobs(1, skippedJob("integration (2)", 1))
	r, _ := f.merge()
	if r.MergeEligible || !strings.Contains(strings.Join(r.Ineligible, "\n"), "part integration:2 was run again (attempt 2) after attempt 1 recorded its result (it left its part.json)") {
		t.Errorf("eligible %v:\n%s", r.MergeEligible, strings.Join(r.Ineligible, "\n"))
	}
}
