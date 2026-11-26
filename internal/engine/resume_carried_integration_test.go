//go:build integration

package engine_test

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"connectrpc.com/connect"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/worker"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/api"
	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/internal/testsupport/must"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// A run that carried on before it failed is resumed on a real cluster, fails
// again, carries on meanwhile, and is resumed a second time: what ended before
// it carried on never runs again, what ended since is kept or runs under the
// resume's own keys, and its stream is whole across every history of it.
func TestIntegration_ARunThatCarriedOnIsResumedTwice(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Minute)
	defer cancel()
	const tasks = 9
	var document strings.Builder
	document.WriteString("\napiVersion: orkstack/v1\nkind: Orkflow\nname: resume-carried\ntasks:\n")
	for i := 1; i <= tasks; i++ {
		fmt.Fprintf(&document, "  - id: c%d\n    uses: shell\n    with: {op: exec, command: \"echo %d\"}\n", i, i)
		if i > 1 {
			fmt.Fprintf(&document, "    needs: [c%d]\n", i-1)
		}
	}
	last := fmt.Sprintf("c%d", tasks)

	commits := artifacts.NewMemoryCommitStore()
	var mu sync.Mutex
	dispatched := map[string]int{}
	fails := 2
	behavior := func(actx context.Context, s *canStack, in *orkstackv1.ExecuteTaskInput) (*orkstackv1.ExecuteTaskResult, error) {
		mu.Lock()
		dispatched[in.GetIdempotencyKey()]++
		fail := in.GetTaskId() == last && fails > 0
		if fail {
			fails--
		}
		mu.Unlock()
		if fail {
			return nil, temporal.NewNonRetryableApplicationError(last+" failed", "ActivityFailed", nil)
		}
		record, err := commits.Create(actx, in.GetCommitUri(), &orkstackv1.ActivityCommit{
			SchemaVersion: artifacts.CommitSchemaVersion, Namespace: artifacts.ResolveNamespace(in.GetNamespace()),
			RunId: in.GetRunId(), TaskId: in.GetTaskId(), OrkflowDefinitionDigest: in.GetOrkflowDefinitionDigest(),
			IdempotencyKey: in.GetIdempotencyKey(), Image: in.GetImage(), Attempt: 1, OrkerId: "orker-it",
		})
		if err != nil {
			return nil, err
		}
		result := succeeded(in.GetTaskId(), map[string]any{"ok": true})
		result.Commit = &orkstackv1.CommitRef{Uri: in.GetCommitUri(), Sha256: record.SHA256}
		return result, nil
	}
	records, err := artifacts.NewRecordStore(ctx, s3Config())
	must.OKf(t, err, "record store")
	var s *canStack
	s = startCANStack(t, ctx, "resume-carried", true, behavior, func(w worker.Worker) {
		acts := engine.NewResumeActivities(commits, nil).WithHistory(lazyHistory{&s}, lazyNamespace(&s), records)
		w.RegisterActivityWithOptions(acts.ReadResetFacts, activity.RegisterOptions{Name: engine.ActivityReadResetFacts})
		w.RegisterActivityWithOptions(acts.LoadResumePlan, activity.RegisterOptions{Name: engine.ActivityLoadResumePlan})
		w.RegisterActivityWithOptions(acts.AdoptRecorded, activity.RegisterOptions{Name: engine.ActivityAdoptRecorded})
	})
	endings, err := artifacts.NewRunArchive(s3Config())
	must.OKf(t, err, "run archive")
	archiver := engine.NewRunArchiver(endings)
	s.reader.WithEndings(archiver)
	runID := fmt.Sprintf("ork-resume-carried-%d", time.Now().UnixNano())
	budget := engine.DefaultContinuationBudget(60, 8<<20, time.Minute)
	must.OKf(t, s.starter.StartRun(ctx, runID, compile(t, document.String()), nil, s.plan(10_000, &budget)), "StartRun")
	ended := func(round string, fail bool) {
		t.Helper()
		if err := s.client.GetWorkflow(ctx, runID, "").Get(ctx, nil); (err != nil) != fail {
			t.Fatalf("%s: the run ended %v; want failed %v", round, err, fail)
		}
	}
	ended("at first", true)
	described, err := describeExecution(s.client, ctx, runID, "")
	must.OKf(t, err, "describe the run")
	carried := s.chainOf(runID, described.GetWorkflowExecutionInfo().GetExecution().GetRunId())
	if len(carried) < 3 {
		t.Fatalf("the run had %d histories, want it to have carried on twice before it failed", len(carried))
	}

	store, err := artifacts.NewLocalStore(t.TempDir())
	must.OKf(t, err, "artifact store")
	settings := api.DefaultResumeSettings()
	settings.ConfirmTimeout = 2 * time.Minute
	svc := api.NewService(api.Config{
		ResumeSettings: settings,
		Reader:         s.reader, Starter: s.starter, Pools: servedPools{}, Commits: commits, ArtifactStore: store,
		Attempts: artifacts.NewMemoryAttemptStore(), Resume: s.starter.WithClusterIdentity(s.namespace, "ork-resume-it"),
		ResumeRecords: records, ResumeClaims: &resumeClaims{claims: map[string]engine.Claim{}}, ResumeSnapshots: s.snapshots,
		ResumeEndings: endings.(artifacts.RunEndingStore), ResumeArchiver: engine.NewHistoryArchiver(s.reader, archiver),
		Releases: runningRelease{build: s.deployment.BuildID},
		// Past everything the base may still be running.
		Now: func() time.Time { return time.Now().Add(time.Hour) },
	})
	resume := func(used int32) *orkstackv1.ResumeRunResponse {
		t.Helper()
		preview, err := svc.ResumeRun(ctx, connect.NewRequest(&orkstackv1.ResumeRunRequest{RunId: runID, DryRun: true}))
		must.OKf(t, err, "preview")
		earlier, again := 0, []string{}
		for _, task := range preview.Msg.GetTasks() {
			switch task.GetCategory() {
			case orkstackv1.ResumeCategory_RESUME_CATEGORY_EARLIER:
				earlier++
			case orkstackv1.ResumeCategory_RESUME_CATEGORY_RERUN_FAILED:
				again = append(again, task.GetTaskId())
			}
		}
		if preview.Msg.GetResumesUsed() != used || earlier == 0 || len(again) != 1 || again[0] != last {
			t.Fatalf("preview = %v: %d resumes used, %d earlier, %v again", preview.Msg, preview.Msg.GetResumesUsed(), earlier, again)
		}
		made, err := svc.ResumeRun(ctx, connect.NewRequest(&orkstackv1.ResumeRunRequest{RunId: runID,
			ExpectedTerminalSequence: preview.Msg.GetTerminalSequence(), ExpectedPlanDigest: preview.Msg.GetPlanDigest()}))
		must.OKf(t, err, "resume")
		return made.Msg
	}
	first := resume(0)
	ended("resumed", true)
	second := resume(1)
	ended("resumed again", false)

	live, err := s.reader.Events(ctx, runID, 0)
	must.OKf(t, err, "the live stream")
	for i, e := range live {
		if e.GetSequence() != int64(i+1) {
			t.Fatalf("event %d of the stream is %d, %v", i+1, e.GetSequence(), e.GetType())
		}
	}
	for _, at := range []int64{first.GetSeqBase(), second.GetSeqBase()} {
		if live[at-1].GetType() != orkstackv1.EventType_EVENT_TYPE_RUN_RESUMED {
			t.Fatalf("event %d is %v, want RUN_RESUMED", at, live[at-1].GetType())
		}
	}
	run, err := s.reader.GetRun(ctx, runID)
	must.OKf(t, err, "GetRun")
	if run.GetStatus() != orkstackv1.Status_STATUS_SUCCEEDED || len(run.GetResumes()) != 2 || len(run.GetTaskRuns()) != tasks {
		t.Fatalf("run %v with %d resumes and %d tasks", run.GetStatus(), len(run.GetResumes()), len(run.GetTaskRuns()))
	}
	mu.Lock()
	defer mu.Unlock()
	if len(dispatched) != tasks+2 {
		t.Fatalf("dispatched %v; want each task once, and %s once more each resume", dispatched, last)
	}
	for key, n := range dispatched {
		if n != 1 {
			t.Errorf("%s was dispatched %d times", key, n)
		}
	}
}
