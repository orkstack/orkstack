package main

import (
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/orkstack/orkstack/internal/testsupport/must"
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

// The ids the fast part and the cancel job are held to, and the output that
// says a gate failed.
const ()

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
	must.OK(t, readJSON(filepath.Join("testdata", cancelledRun, "attempt-1.json"), &a))
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
	must.OK(f.t, err)
	dst := filepath.Join(f.parts, dirName)
	must.OK(f.t, os.MkdirAll(dst, 0o755))
	for _, e := range entries {
		b, err := os.ReadFile(filepath.Join(src, e.Name()))
		must.OK(f.t, err)
		must.OK(f.t, os.WriteFile(filepath.Join(dst, e.Name()), b, 0o644))
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
	// Each wanted reason is the fragments one reason says, wherever they fall
	// in its sentence.
	for _, c := range []struct {
		name string
		edit func(jobs []githubJob)
		want [][]string
	}{
		{"as GitHub recorded it", nil, [][]string{
			{"gate unit failed"},
			{"part integration:1", "left no result", "cancelled", "not rerun-eligible"},
			{"part integration:2", "left no result", "cancelled"},
			{"part ui-e2e:1", "left no result", "cancelled"},
		}},
		// What today's integration gate also prints to the job's log
		// (proof-part.sh, PART_STREAM): the test that was running.
		{"with the progress line a shard now prints", func(jobs []githubJob) {
			for i := range jobs {
				if jobs[i].Name == "gate / integration (1)" {
					jobs[i].Log = soak + "\n" + jobs[i].Log
				}
			}
		}, [][]string{
			{"gate unit failed"},
			{"part integration:1", "no usable result", "a test was running", "github.com/orkstack/orkstack/internal/engine.TestIntegration_SoakTasks", "not rerun-eligible"},
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
		}, [][]string{{"gate unit failed"}, {"part integration:1", "left no result", "cancelled"}}},
		// A cancel that comes before a shard has started (its plan or
		// build still running) leaves it no record and no artifact.
		{"a shard the cancel stopped before it started", func(jobs []githubJob) {
			for i := range jobs {
				if jobs[i].Name == "gate / ui-e2e (1)" {
					jobs[i].Name = "gate / ui-e2e"
					jobs[i].Conclusion, jobs[i].Steps, jobs[i].RunnerName, jobs[i].Log = "skipped", nil, "", ""
				}
			}
		}, [][]string{{"part ui-e2e:1", "left no result", "not rerun-eligible", "merge-eligible"}}},
	} {
		t.Run(c.name, func(t *testing.T) {
			r, code := mergeCancelled(t, c.edit)
			reasons := strings.Join(r.Ineligible, "\n")
			if code == 0 || r.Result != "fail" || r.MergeEligible {
				t.Fatalf("code %d result %s eligible %v:\n%s", code, r.Result, r.MergeEligible, reasons)
			}
			for _, frags := range c.want {
				if !ineligibleSays(r.Ineligible, frags...) {
					t.Errorf("ineligible has no reason saying %q:\n%s", frags, reasons)
				}
			}
			for _, never := range []string{"lost its runner", "failed before its proof step", "re-run that job alone"} {
				if strings.Contains(reasons, never) {
					t.Errorf("a cancelled part is reported as rerun-eligible (%q):\n%s", never, reasons)
				}
			}
			if len(r.Reruns) != 0 || len(r.SetupFailures) != 0 {
				t.Errorf("reruns %+v setup failures %+v", r.Reruns, r.SetupFailures)
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
	for _, frags := range [][]string{
		{"part fast", "run again", "attempt 2", "recorded its result"},
		{"part integration:1", "run again", "attempt 2", "lost runner"},
		{"part ui-e2e:1", "run again", "attempt 2", "lost runner"},
	} {
		if !ineligibleSays(r.Ineligible, frags...) {
			t.Errorf("ineligible has no reason saying %q:\n%s", frags, reasons)
		}
	}
}

// ineligibleSays reports whether one reason of the proof holds every fragment.
func ineligibleSays(reasons []string, frags ...string) bool {
	return slices.ContainsFunc(reasons, func(reason string) bool {
		for _, f := range frags {
			if !strings.Contains(reason, f) {
				return false
			}
		}
		return true
	})
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
