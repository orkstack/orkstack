//go:build integration

package engine_test

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"go.temporal.io/api/enums/v1"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/worker"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/testsupport/must"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// resumeSoakTasks is the largest run's tasks: the 4,000-task ceiling, with
// room below it.
const resumeSoakTasks = 3991

// resumeSoakDocument is n plain tasks that start together, in the task soak's
// own shape (taskSoakDocument), which its apply accepts.
func resumeSoakDocument(name string, n int) string {
	var b strings.Builder
	fmt.Fprintf(&b, "apiVersion: orkstack/v1\nkind: Orkflow\nname: %s\nversion: \"1.0.0\"\ntasks:\n", name)
	for i := 1; i <= n; i++ {
		fmt.Fprintf(&b, "  - id: w%d\n    uses: shell\n    network: none\n    timeout_seconds: 600\n    needs: []\n"+
			"    with:\n      op: exec\n      command: [echo, ok]\n", i)
	}
	return b.String()
}

// lastToEnd is the soak's Orker: each task commits and succeeds, but for the
// one whose commit would be the run's last, which fails instead. Nothing is
// held open, so the run carries on as any run does. Run again by a resume,
// every task commits.
func lastToEnd(s *resumeStack, n int32) chaosBehavior {
	var (
		mu    sync.Mutex
		ended = map[string]bool{}
	)
	commit := s.committing(nil)
	return func(ctx context.Context, c *chaosStack, orker string, in *orkstackv1.ExecuteTaskInput) (*orkstackv1.ExecuteTaskResult, error) {
		if !strings.Contains(in.GetIdempotencyKey(), "@r") {
			mu.Lock()
			last := !ended[in.GetTaskId()] && int32(len(ended)) == n-1
			ended[in.GetTaskId()] = true
			mu.Unlock()
			if last {
				return nil, temporal.NewNonRetryableApplicationError(in.GetTaskId()+" failed", "ActivityFailed", nil)
			}
		}
		return commit(ctx, c, orker, in)
	}
}

// TestIntegration_SoakResumeTheLargestRun: a run at the task ceiling, whose
// last task to end failed after the run had carried on into later histories,
// is resumed. Every task that succeeded is kept, read back or from the saved
// state, and none runs again; the one that failed, and any the failure canceled, run once more,
// and the run reads whole, the ending the resume set aside included.
// TestIntegration_ResumeManyKeptTasks proves the same of 400 kept tasks in one
// history, in the merge gate.
func TestIntegration_SoakResumeTheLargestRun(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), taskSoakRunWithin+60*time.Minute)
	defer cancel()
	made := &dispatches{}
	var s *resumeStack
	soak, _ := startTaskSoakWith(t, ctx, taskSoakShape{name: "resume", lanes: func(n int) int { return n }}, resumeSoakTasks,
		func(c *chaosStack) soakSeams {
			// The archive the soak's stack writes to: the same bucket and keys.
			archive, err := artifacts.NewRunArchive(c.cfg)
			must.OKf(t, err, "run archive")
			endings, ok := archive.(artifacts.RunEndingStore)
			if !ok {
				t.Fatalf("run archive %T keeps one ending a run", archive)
			}
			s = resumeStackOn(t, ctx, c, endings)
			return soakSeams{
				register: func(w worker.Worker) { s.registeringResume(c.control, resumeFaults{})(w) },
				behavior: func(chaosBehavior) chaosBehavior { return made.noting(lastToEnd(s, resumeSoakTasks)) },
			}
		})
	const name = "resume-largest"
	soak.api.apply(t, ctx, resumeSoakDocument(name, resumeSoakTasks))
	runID := soak.api.submit(t, ctx, name).GetRun().GetId()
	s.transcript.run(runID)
	// The task soak's own bound: a run this size carries on many times.
	s.awaitFailedWithin(runID, taskSoakRunWithin)
	before := s.chain(runID)
	if len(before) < 2 {
		t.Fatalf("the run's executions are %v; a run of %d tasks was meant to carry on", before, resumeSoakTasks)
	}
	// What the base left: one task failed, and the failure may have canceled
	// some that were still running.
	base := s.read(runID)
	succeeded, failed, canceled := map[string]bool{}, map[string]bool{}, map[string]bool{}
	for _, row := range base.GetTaskRuns() {
		switch row.GetStatus() {
		case orkstackv1.Status_STATUS_SUCCEEDED:
			succeeded[row.GetTaskId()] = true
		case orkstackv1.Status_STATUS_FAILED:
			failed[row.GetTaskId()] = true
		case orkstackv1.Status_STATUS_CANCELED:
			canceled[row.GetTaskId()] = true
		default:
			t.Fatalf("task %s of the failed run reads %v", row.GetTaskId(), row.GetStatus())
		}
	}
	if len(failed) != 1 || len(succeeded)+len(failed)+len(canceled) != resumeSoakTasks {
		t.Fatalf("the run ended with %d succeeded, %d failed and %d canceled; want one failed of %d",
			len(succeeded), len(failed), len(canceled), resumeSoakTasks)
	}

	door := s.serveResume(pastContainers)
	started := time.Now()
	preview := door.preview(runID)
	// What succeeded is kept: read back from its commit when it ended in the
	// run's last history, and from the saved state when it ended in an
	// earlier one (design §8).
	category := map[string]orkstackv1.ResumeCategory{}
	for _, task := range preview.GetTasks() {
		category[task.GetTaskId()] = task.GetCategory()
	}
	readBack, earlier := 0, 0
	for id := range succeeded {
		switch category[id] {
		case orkstackv1.ResumeCategory_RESUME_CATEGORY_KEPT:
			readBack++
		case orkstackv1.ResumeCategory_RESUME_CATEGORY_EARLIER:
			earlier++
		default:
			t.Fatalf("%s succeeded, and the preview has it %v", id, category[id])
		}
	}
	counts := map[orkstackv1.ResumeCategory]int{}
	for _, c := range preview.GetCounts() {
		counts[c.GetCategory()] = int(c.GetCount())
	}
	if counts[orkstackv1.ResumeCategory_RESUME_CATEGORY_KEPT] != readBack || counts[orkstackv1.ResumeCategory_RESUME_CATEGORY_EARLIER] != earlier ||
		int(preview.GetKeptCount()) != readBack {
		t.Fatalf("the preview counts %v and keeps %d; its tasks say %d read back and %d from an earlier part", counts, preview.GetKeptCount(), readBack, earlier)
	}
	door.confirm(preview)
	if status, err := s.wait(runID, 60*time.Minute); status != enums.WORKFLOW_EXECUTION_STATUS_COMPLETED || err != nil {
		t.Fatalf("the resumed run closed %v: %v", status, err)
	}
	took := time.Since(started)
	run := s.read(runID)
	scope := preview.GetSeqBase()
	s.requireResumedOnce(runID, scope)
	for _, row := range run.GetTaskRuns() {
		id := row.GetTaskId()
		again := made.of(runID, id, scope)
		switch {
		case row.GetStatus() != orkstackv1.Status_STATUS_SUCCEEDED:
			t.Fatalf("%s reads %v after the resume", id, row.GetStatus())
		case succeeded[id] && again != 0:
			t.Fatalf("%s succeeded before, and ran %d times under the resume", id, again)
		case category[id] == orkstackv1.ResumeCategory_RESUME_CATEGORY_KEPT && !row.GetAdopted():
			t.Fatalf("%s was read back, and its row does not say it was kept", id)
		case !succeeded[id] && again != 1:
			t.Fatalf("%s did not succeed before; it ran %d times under the resume, want once", id, again)
		}
	}
	kept := s.published(runID, orkstackv1.EventType_EVENT_TYPE_RUN_RESULTS_KEPT)[0].GetResultsKept()
	if int(kept.GetKeptCount()) != readBack {
		t.Fatalf("RUN_RESULTS_KEPT names %d results, want the %d read back", kept.GetKeptCount(), readBack)
	}
	after := s.chain(runID)
	s.transcript.figure("kept_read_back", readBack)
	s.transcript.figure("kept_from_earlier_parts", earlier)
	s.transcript.figure("canceled_by_the_failure", len(canceled))
	s.transcript.figure("read_back_seconds", kept.GetReadBackSeconds())
	s.transcript.figure("resume_to_success_seconds", took.Seconds())
	s.transcript.figure("executions_before", len(before))
	s.transcript.figure("executions_after", len(after))
	t.Logf("EVIDENCE %d results kept: %d read back in %.1f s and %d from earlier parts of the run; %d canceled by the failure ran again; "+
		"the resume took %s to succeed; %d executions before it, %d after",
		readBack+earlier, readBack, kept.GetReadBackSeconds(), earlier, len(canceled), took.Round(time.Second), len(before), len(after))
}
