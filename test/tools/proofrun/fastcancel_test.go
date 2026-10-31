package main

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// The fast part fails fast for the whole run (docs/PROOFS.md §5): when one of
// its own gates fails, the run is cancelled, as release.yml's artifacts
// cancels on a failure a rerun would repeat, with the same command and
// permission. The job graph is unchanged, so a green run waits for nothing.
// A red `fast` gate is never a passing proof (the unit gate exits 0 when
// every failure is a quarantined one, and the merger fails the proof on any
// other red gate of it), so the cancel stops only a run that cannot pass. A
// `fast` that failed in a setup step, or whose runner GitHub lost, recorded no
// gate and cancels nothing: that one job may run again.
//
// `fast` runs the candidate's code, which can change what later steps of its
// job run ($GITHUB_PATH, $GITHUB_ENV), so the token that may cancel is never
// in that job: `fast` only says, as an output, whether a gate failed, and a
// job of its own, `cancel`, which checks out and runs nothing of the
// repository, holds `actions: write` and asks GitHub to cancel. A forged
// output could only cancel the run that forged it.

// The ids and conditions the fast part and the cancel job are held to.
const (
	fastProofStep  = "fast"
	fastGatesStep  = "gates"
	fastGatesIf    = "failure() && steps.fast.outcome == 'failure'"
	fastGateOutput = "${{ steps.gates.outputs.failed }}"
	cancelJobIf    = "!cancelled() && needs.fast.result == 'failure' && needs.fast.outputs.failed_gate == 'true' && (" + proofGuard + ")"
)

func fastCancelProblems(w *wfFile) []string {
	var p []string
	for _, name := range w.jobNames() {
		job := w.Jobs[name]
		for _, n := range job.needs() {
			if n == "fast" && name != "proof" && name != "cancel" {
				p = append(p, fmt.Sprintf("jobs.%s waits for fast: the graph stays as it is, and the run is cancelled instead", name))
			}
		}
		if name != "cancel" && permLevel(job.Permissions["actions"]) > 1 {
			p = append(p, fmt.Sprintf("jobs.%s writes actions; only the cancel job may", name))
		}
		for i, s := range job.Steps {
			if strings.Contains(s.Run, cancelRun) && name != "cancel" {
				p = append(p, fmt.Sprintf("jobs.%s %s cancels the run; only the cancel job may", name, s.label(i)))
			}
		}
	}

	fast := w.Jobs["fast"]
	if fast == nil {
		return append(p, "proof.yml has no fast job")
	}
	part := stepIndex(fast.Steps, func(s wfStep) bool { return strings.Contains(s.Run, "--part fast") })
	upload := stepIndex(fast.Steps, func(s wfStep) bool { return strings.HasPrefix(s.Uses, "actions/upload-artifact@") })
	gates := stepIndex(fast.Steps, func(s wfStep) bool { return s.ID == fastGatesStep })
	switch {
	case part < 0:
		return append(p, "jobs.fast never runs its part")
	case gates < 0:
		return append(p, "jobs.fast never says whether one of its gates failed")
	}
	if fast.Steps[part].ID != fastProofStep {
		p = append(p, fmt.Sprintf("jobs.fast's part step has id %q, want %q: its outcome decides", fast.Steps[part].ID, fastProofStep))
	}
	if fast.ContinueOnError != nil || fast.Steps[part].ContinueOnError != nil {
		p = append(p, "jobs.fast, or its part step, continues on error: a failing gate would no longer fail it")
	}
	g := fast.Steps[gates]
	if gates != len(fast.Steps)-1 || upload < 0 || upload > gates {
		p = append(p, "jobs.fast's gates step is not its last, after its upload")
	}
	if got := normalizeIf(g.If); got != fastGatesIf {
		p = append(p, fmt.Sprintf("jobs.fast's gates step runs under %q, want %q: only the part step failing, never a setup step", got, fastGatesIf))
	}
	if !strings.Contains(g.Run, "gates.tsv") || !strings.Contains(g.Run, `\tfail\t'`) || !strings.Contains(g.Run, `failed=true`) || !strings.Contains(g.Run, `"$GITHUB_OUTPUT"`) {
		p = append(p, "jobs.fast's gates step does not read the part's gates.tsv for a failing gate into its output")
	}
	if fast.Outputs["failed_gate"] != fastGateOutput {
		p = append(p, fmt.Sprintf("jobs.fast.outputs.failed_gate = %q, want %q", fast.Outputs["failed_gate"], fastGateOutput))
	}
	if permLevel(fast.Permissions["actions"]) > 1 {
		p = append(p, "jobs.fast writes actions: it runs the candidate's code")
	}
	for i, st := range fast.Steps {
		if holdsToken(st) {
			p = append(p, fmt.Sprintf("jobs.fast %s is handed a token: it runs the candidate's code", st.label(i)))
		}
	}

	cancel := w.Jobs["cancel"]
	if cancel == nil {
		return append(p, "proof.yml has no cancel job")
	}
	if needs := cancel.needs(); len(needs) != 1 || needs[0] != "fast" {
		p = append(p, fmt.Sprintf("jobs.cancel needs %v, want fast alone", needs))
	}
	if got := normalizeIf(cancel.If); got != cancelJobIf {
		p = append(p, fmt.Sprintf("jobs.cancel runs under %q, want %q", got, cancelJobIf))
	}
	if len(cancel.Permissions) != 1 || cancel.Permissions["actions"] != "write" {
		p = append(p, fmt.Sprintf("jobs.cancel.permissions = %v, want actions: write alone", cancel.Permissions))
	}
	if len(cancel.Steps) != 1 {
		return append(p, fmt.Sprintf("jobs.cancel has %d steps, want the one that cancels", len(cancel.Steps)))
	}
	s := cancel.Steps[0]
	if s.Uses != "" || strings.Count(s.Run, cancelRun) != 1 || repoCodeInRun.MatchString(s.Run) || strings.Contains(s.Run, "${{") {
		p = append(p, "jobs.cancel's step does more than the one cancel, or runs something of the repository")
	}
	if envString(s, "RUN_ID") != "${{ github.run_id }}" || envString(s, "REPO") != "${{ github.repository }}" || envString(s, "GH_TOKEN") != "${{ github.token }}" {
		p = append(p, "jobs.cancel cancels without this run's id, repository and token")
	}
	return p
}

func TestWorkflows_TheRunIsCancelledOnlyOnAFailingGateOfFastsOwn(t *testing.T) {
	expectNoProblems(t, "proof.yml", fastCancelProblems(loadWorkflow(t, "proof.yml")))
	gates := func(w *wfFile) *wfStep {
		job := w.Jobs["fast"]
		return &job.Steps[stepIndex(job.Steps, func(s wfStep) bool { return s.ID == fastGatesStep })]
	}
	partStep := func(w *wfFile) *wfStep {
		job := w.Jobs["fast"]
		return &job.Steps[stepIndex(job.Steps, func(s wfStep) bool { return strings.Contains(s.Run, "--part fast") })]
	}
	expectEachMutationRefused(t, "proof.yml", fastCancelProblems, []mutation{
		{"no cancel job", func(w *wfFile) { delete(w.Jobs, "cancel") }},
		{"no gates step", func(w *wfFile) { job := w.Jobs["fast"]; job.Steps = job.Steps[:len(job.Steps)-1] }},
		{"the shards wait for fast", func(w *wfFile) { w.Jobs["integration"].Needs = []any{"base", "plan", "build", "fast"} }},
		{"a cancel on any failure of fast, a setup step's included", func(w *wfFile) {
			w.Jobs["cancel"].If = strings.Replace(w.Jobs["cancel"].If, " && needs.fast.outputs.failed_gate == 'true'", "", 1)
		}},
		{"a cancel on a cancelled run", func(w *wfFile) {
			w.Jobs["cancel"].If = strings.Replace(w.Jobs["cancel"].If, "!cancelled()", "always()", 1)
		}},
		{"the gates read on a setup failure", func(w *wfFile) { gates(w).If = "${{ failure() }}" }},
		{"the gates read on every outcome", func(w *wfFile) { gates(w).If = "${{ always() }}" }},
		{"a failing gate never read", func(w *wfFile) { gates(w).Run = "echo \"failed=true\" >>\"$GITHUB_OUTPUT\"" }},
		{"the output not wired", func(w *wfFile) { delete(w.Jobs["fast"].Outputs, "failed_gate") }},
		{"the part step renamed", func(w *wfFile) { partStep(w).ID = "part" }},
		{"the part step continues on error", func(w *wfFile) { partStep(w).ContinueOnError = true }},
		{"the cancel in fast itself", func(w *wfFile) {
			w.Jobs["fast"].Permissions["actions"] = "write"
		}},
		{"a token for the part step", func(w *wfFile) {
			s := partStep(w)
			if s.Env == nil {
				s.Env = map[string]any{}
			}
			s.Env["GH_TOKEN"] = "${{ github.token }}"
		}},
		{"the cancel job checks out the candidate", func(w *wfFile) {
			job := w.Jobs["cancel"]
			job.Steps = append([]wfStep{{Uses: "actions/checkout@v7.0.1", With: map[string]any{"ref": "${{ github.sha }}"}}}, job.Steps...)
		}},
		{"the cancel job runs the repository's code", func(w *wfFile) { w.Jobs["cancel"].Steps[0].Run += "\nscripts/proof.sh --part fast" }},
		{"the cancel job reads more", func(w *wfFile) { w.Jobs["cancel"].Permissions["contents"] = "read" }},
		{"a shard writes actions", func(w *wfFile) { w.Jobs["integration"].Permissions["actions"] = "write" }},
		{"the race gate cancels too", func(w *wfFile) {
			w.Jobs["race"].Steps = append(w.Jobs["race"].Steps, w.Jobs["cancel"].Steps[0])
		}},
		{"another run cancelled", func(w *wfFile) { w.Jobs["cancel"].Steps[0].Env["RUN_ID"] = "${{ github.event.workflow_run.id }}" }},
	})
}

// runFastGates runs the fast part's gates step itself over a part directory
// whose gates.tsv is gates ("" leaves the part without one: it never got that
// far), and returns what it wrote to the step's output.
func runFastGates(t *testing.T, gates string) string {
	t.Helper()
	if _, err := exec.LookPath("bash"); err != nil {
		t.Skip("bash is not installed")
	}
	job := loadWorkflow(t, "proof.yml").Jobs["fast"]
	at := stepIndex(job.Steps, func(s wfStep) bool { return s.ID == fastGatesStep })
	if at < 0 {
		t.Fatal("jobs.fast has no gates step")
	}
	work := t.TempDir()
	dir := filepath.Join(work, "proof", "pr-ci-77-part-fast-a2")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	if gates != "" {
		if err := os.WriteFile(filepath.Join(dir, "gates.tsv"), []byte(gates), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	output := filepath.Join(work, "output")
	cmd := exec.Command("bash", "-c", job.Steps[at].Run)
	cmd.Dir = work
	cmd.Env = []string{"PATH=" + os.Getenv("PATH"), "HOME=" + work, "PROOF_NAME=pr-ci-77", "GITHUB_RUN_ATTEMPT=2", "GITHUB_OUTPUT=" + output}
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("the gates step failed: %v\n%s", err, out)
	}
	b, _ := os.ReadFile(output)
	return strings.TrimSpace(string(b))
}

func TestWorkflows_TheFastPartSaysAGateFailedOnlyWhenOneDid(t *testing.T) {
	pass := "lint\tpass\t40\tlint.log\tmake lint\nbuild\tpass\t30\tbuild.log\tgo build ./...\nunit\tpass\t500\tunit.log\tproofrun unit\n"
	for _, c := range []struct {
		name   string
		gates  string
		failed bool
	}{
		{"a failing unit gate", strings.Replace(pass, "unit\tpass", "unit\tfail", 1), true},
		{"a failing lint gate", strings.Replace(pass, "lint\tpass", "lint\tfail", 1), true},
		{"the site's gate failing, recorded last", pass + "site\tfail\t0\tsite.log\tbun run check && bun run build\n", true},
		// The unit gate passes when every failure is a quarantined one: no
		// gate failed, and the proof may pass.
		{"every gate passed (a quarantined failure only)", pass, false},
		// The part stopped before any gate (exit 77: a prerequisite absent):
		// no gate failed, so nothing says the proof cannot pass.
		{"no gate recorded", "", false},
		{"a gate named fail", "fail\tpass\t1\tfail.log\tx\n", false},
	} {
		t.Run(c.name, func(t *testing.T) {
			got := runFastGates(t, c.gates)
			want := ""
			if c.failed {
				want = "failed=true"
			}
			if got != want {
				t.Errorf("the step's output = %q, want %q", got, want)
			}
		})
	}
}

// Run 40965510239 (2026-10-29, release.yml's dry run of v0.1.0): its gate's
// `fast` failed on a product failure, the unit test the B7 confinement change
// broke on ext4, and the run was cancelled while integration (1), (2), (3),
// (7), (8), (9) and ui-e2e (1) were running; `gate / proof` still ran, under
// always(), and failed. It is the run the cancel job makes, recorded:
// testdata/cancelled-40965510239 holds GitHub's job listing and annotations
// for the gate's jobs, and attempt-1.json, the records today's "job records"
// step writes from them and the failed jobs' logs (each kept bounded); two
// cancelled shards' artifacts stand for the parts. Integration (9), sixteen
// tests into its shard, stands in for the fixture's integration:2.

const cancelledRun = "cancelled-40965510239"

// cancelledRecords is attempt 1's job records of the recorded run, by the
// fixture's part names: only the jobs of fast, integration (1), integration
// (9) as integration (2), ui-e2e (1) and proof.
func cancelledRecords(t *testing.T) []githubJob {
	t.Helper()
	var a githubAttempt
	if err := readJSON(filepath.Join("testdata", cancelledRun, "attempt-1.json"), &a); err != nil {
		t.Fatal(err)
	}
	var out []githubJob
	for _, j := range a.Jobs {
		switch j.Name {
		case "gate / integration (9)":
			j.Name = "gate / integration (2)"
		case "gate / fast", "gate / integration (1)", "gate / ui-e2e (1)", "gate / proof":
		default:
			continue
		}
		out = append(out, j)
	}
	if len(out) != 5 {
		t.Fatalf("the recorded run holds %d of the five jobs", len(out))
	}
	return out
}

// copyPart copies a recorded part attempt's files into the fixture's parts as
// dirName: no part.json, as a cancelled shard leaves none.
func (f *mergeFixture) copyPart(from, dirName string) {
	f.t.Helper()
	src := filepath.Join("testdata", cancelledRun, from)
	entries, err := os.ReadDir(src)
	if err != nil {
		f.t.Fatal(err)
	}
	dst := filepath.Join(f.parts, dirName)
	if err := os.MkdirAll(dst, 0o755); err != nil {
		f.t.Fatal(err)
	}
	for _, e := range entries {
		b, err := os.ReadFile(filepath.Join(src, e.Name()))
		if err != nil {
			f.t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(dst, e.Name()), b, 0o644); err != nil {
			f.t.Fatal(err)
		}
	}
}

// mergeCancelled lays the run out as it ended: plan, build and race as they
// were, fast with its failing unit gate, the activity fixtures passed,
// integration (1) and (2) and ui-e2e (1) cancelled with the artifacts they
// left; edit changes GitHub's records first.
func mergeCancelled(t *testing.T, edit func(jobs []githubJob)) (proofRecord, int) {
	t.Helper()
	f := newMergeFixture(t, tierFull, true)
	f.writeAttempt1Parts()
	gates := fastGateRecords()
	for i := range gates {
		if gates[i].Gate == "unit" {
			gates[i].Result = "fail"
		}
	}
	f.writeFast(gates)
	for _, a := range fixtureActivities {
		fix := f.basePart("activity-fixture:" + a)
		fix.Gates = []gateRecord{{Gate: "activity-fixture", Result: "pass"}}
		fix.Images = map[string]string{"activity-shell": "sha256:aaa"}
		f.writePart(fix, nil)
	}
	f.copyPart("part-integration-1-a1", "part-integration-1-a1")
	f.copyPart("part-integration-9-a1", "part-integration-2-a1")
	jobs := cancelledRecords(t)
	if edit != nil {
		edit(jobs)
	}
	f.writeJobs(1, jobs...)
	return f.merge()
}

// writeAttempt1Parts writes the plan, the build and the race gate, as they
// ran.
func (f *mergeFixture) writeAttempt1Parts() {
	f.t.Helper()
	f.writePart(f.basePart("plan"), nil)
	build := f.basePart("build")
	build.Gates = []gateRecord{{Gate: "images", Result: "pass"}}
	build.Images = map[string]string{"activity-shell": "sha256:aaa"}
	f.writePart(build, nil)
	f.writeRace(raceGateRecords(), raceLedger())
}

// The cancelled run is a failing proof, never rerun-eligible: fast's failing
// gate is a result, and each cancelled shard is a part with no result that no
// rule lets run again. Nothing reads as a lost runner, a degraded runner or a
// setup failure, whatever fast's own job ended as once its cancel reached it.
func TestMerge_ARunFastCancelledIsAFailingProofNeverRunAgain(t *testing.T) {
	soak := "2026-10-29T22:42:26.0000000Z proofrun: start github.com/orkstack/orkstack/internal/engine TestIntegration_SoakTasks"
	for _, c := range []struct {
		name string
		edit func(jobs []githubJob)
		want []string
	}{
		{"as GitHub recorded it", nil, []string{
			"gate unit failed",
			"part integration:1 left no result (attempt 1, it was cancelled before any test started); it is not rerun-eligible (only a lost, degraded or setup-failed attempt is): start a new run",
			"part integration:2 left no result (attempt 1, it was cancelled before any test started)",
			"part ui-e2e:1 left no result (attempt 1, it was cancelled before any test started)",
		}},
		// What today's integration gate also prints to the job's log
		// (proof-part.sh, PART_STREAM): the test that was running.
		{"with the progress line a shard now prints", func(jobs []githubJob) {
			for i := range jobs {
				if jobs[i].Name == "gate / integration (1)" {
					jobs[i].Log = soak + "\n" + jobs[i].Log
				}
			}
		}, []string{
			"gate unit failed",
			"part integration:1 left no usable result at attempt 1 (a test was running when the job stopped: github.com/orkstack/orkstack/internal/engine.TestIntegration_SoakTasks); it is not rerun-eligible: start a new run",
		}},
		// Whatever GitHub records for fast's own job (a cancel by hand can
		// reach it too), the part.json it left is its result.
		{"fast's own job recorded cancelled", func(jobs []githubJob) {
			for i := range jobs {
				if jobs[i].Name == "gate / fast" {
					jobs[i].Conclusion = "cancelled"
					jobs[i].Annotations = []string{"The run was canceled by @github-actions[bot].", "The operation was canceled."}
				}
			}
		}, []string{"gate unit failed", "part integration:1 left no result (attempt 1, it was cancelled before any test started)"}},
		// A cancel that comes before a shard has started (its plan or
		// build still running) leaves it no record and no artifact.
		{"a shard the cancel stopped before it started", func(jobs []githubJob) {
			for i := range jobs {
				if jobs[i].Name == "gate / ui-e2e (1)" {
					jobs[i].Name = "gate / ui-e2e"
					jobs[i].Conclusion, jobs[i].Steps, jobs[i].RunnerName, jobs[i].Log = "skipped", nil, "", ""
				}
			}
		}, []string{"part ui-e2e:1 left no result: it is not rerun-eligible (only a lost, degraded or setup-failed attempt is); this run cannot become merge-eligible: start a new run"}},
	} {
		t.Run(c.name, func(t *testing.T) {
			r, code := mergeCancelled(t, c.edit)
			reasons := strings.Join(r.Ineligible, "\n")
			if code == 0 || r.Result != "fail" || r.MergeEligible {
				t.Fatalf("code %d result %s eligible %v:\n%s", code, r.Result, r.MergeEligible, reasons)
			}
			for _, w := range c.want {
				if !strings.Contains(reasons, w) {
					t.Errorf("ineligible lacks %q:\n%s", w, reasons)
				}
			}
			for _, never := range []string{"lost its runner", "degraded runner", "failed in a setup step", "re-run that job alone"} {
				if strings.Contains(reasons, never) {
					t.Errorf("a cancelled part is reported as rerun-eligible (%q):\n%s", never, reasons)
				}
			}
			if len(r.Reruns) != 0 || len(r.DegradedRunners) != 0 || len(r.SetupFailures) != 0 {
				t.Errorf("reruns %+v degraded %+v setup failures %+v", r.Reruns, r.DegradedRunners, r.SetupFailures)
			}
		})
	}
}

// "Re-run failed jobs" after the cancel runs fast and every cancelled shard
// again, and the proof they pass is still not merge-eligible: each ran again
// after an attempt that no rule lets run again.
func TestMerge_ARunFastCancelledStaysIneligibleAfterARerun(t *testing.T) {
	f := newMergeFixture(t, tierFull, true)
	f.writeAttempt1Parts()
	gates := fastGateRecords()
	for i := range gates {
		if gates[i].Gate == "unit" {
			gates[i].Result = "fail"
		}
	}
	f.writeFast(gates)
	f.copyPart("part-integration-1-a1", "part-integration-1-a1")
	f.copyPart("part-integration-9-a1", "part-integration-2-a1")
	fast := f.basePart("fast")
	fast.Attempt = 2
	fast.Gates = fastGateRecords()
	f.writePart(fast, map[string]any{"unit-tests.json": unitLedger()})
	f.writeShardsAt(2)
	first := cancelledRecords(t)
	f.writeJobs(1, first...)
	var second []githubJob
	for _, j := range first {
		j.ID, j.RunAttempt = j.ID+1, 2
		j.StartedAt, j.CompletedAt, j.Conclusion, j.Annotations, j.Log = "2026-10-30T09:00:00Z", "2026-10-30T09:20:00Z", "success", nil, ""
		j.Steps = []githubStep{{Name: "Set up job", Conclusion: "success"}}
		second = append(second, j)
	}
	f.writeJobs(2, second...)
	r, _ := f.merge()
	reasons := strings.Join(r.Ineligible, "\n")
	if r.MergeEligible || len(r.Reruns) != 0 {
		t.Fatalf("a run cancelled on a fast failure became merge-eligible by a rerun: eligible %v reruns %+v", r.MergeEligible, r.Reruns)
	}
	for _, w := range []string{
		"part fast was run again (attempt 2) after attempt 1 recorded its result (it left its part.json)",
		"part integration:1 was run again (attempt 2) after attempt 1, which GitHub did not record as a lost runner",
		"part ui-e2e:1 was run again (attempt 2) after attempt 1, which GitHub did not record as a lost runner",
	} {
		if !strings.Contains(reasons, w) {
			t.Errorf("ineligible lacks %q:\n%s", w, reasons)
		}
	}
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

// The planner never answers a cancelled run: proof-rerun.yml's guard takes
// only a run that concluded `failure`, and the recorded run concluded
// `cancelled`. Asked anyway, it reads the attempt as the merger does: fast's
// failing part step is a result, and a cancelled shard may not run again.
func TestRerunPlan_ARunFastCancelledIsNeverRunAgain(t *testing.T) {
	var run struct {
		Conclusion string `json:"conclusion"`
	}
	if err := readJSON(filepath.Join("testdata", cancelledRun, "api-run.json"), &run); err != nil {
		t.Fatal(err)
	}
	if run.Conclusion != "cancelled" || !strings.Contains(normalizeIf(loadWorkflow(t, proofRerunFile).Jobs["rerun"].If), "github.event.workflow_run.conclusion == 'failure'") {
		t.Errorf("the recorded run concluded %q, and proof-rerun.yml's guard would answer it", run.Conclusion)
	}
	logs := map[int64]string{}
	var a githubAttempt
	if err := readJSON(filepath.Join("testdata", cancelledRun, "attempt-1.json"), &a); err != nil {
		t.Fatal(err)
	}
	for _, j := range a.Jobs {
		logs[j.ID] = j.Log
	}
	dir := filepath.Join("testdata", cancelledRun)
	gh := func(path string, paginate bool) ([]byte, error) {
		const repo = "repos/orkstack/orkstack/"
		switch {
		case path == repo+"actions/runs/40965510239/attempts/1/jobs?per_page=100":
			return os.ReadFile(filepath.Join(dir, "api-jobs-a1.json"))
		case strings.HasPrefix(path, repo+"check-runs/") && strings.HasSuffix(path, "/annotations?per_page=100"):
			b, err := os.ReadFile(filepath.Join(dir, "annotations-"+strings.TrimSuffix(strings.TrimPrefix(path, repo+"check-runs/"), "/annotations?per_page=100")+".json"))
			if os.IsNotExist(err) {
				return []byte("[]"), nil
			}
			return b, err
		case strings.HasPrefix(path, repo+"actions/jobs/") && strings.HasSuffix(path, "/logs"):
			var id int64
			fmt.Sscanf(strings.TrimPrefix(path, repo+"actions/jobs/"), "%d", &id)
			if log := logs[id]; log != "" {
				return []byte(log), nil
			}
			return nil, fmt.Errorf("gh: Not Found (HTTP 404)")
		}
		return nil, fmt.Errorf("the recorded run has no answer for %s", path)
	}
	if d := planRerun(gh, "orkstack/orkstack", "40965510239", 1, defaultMaxRunAttempts); d.Rerun || d.Reason != "not-eligible" {
		t.Errorf("decision = %+v, want no rerun (not-eligible)", d)
	}
}
