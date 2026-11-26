//go:build integration

package engine_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"go.temporal.io/api/enums/v1"
	"go.temporal.io/sdk/temporal"
	"google.golang.org/protobuf/types/known/durationpb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/api"
	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/internal/testsupport/faultproxy"
	"github.com/orkstack/orkstack/internal/testsupport/must"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// A resume keeps what its run committed, read back from object storage before
// anything runs again (E85 §4.3, §4.4), whatever happens to the store, to the
// engine or to the Orker while it does.

// resetWith resets the run's base to rec, changed by edit: a recorded plan as
// ResumeRun made it but for what no setting changes, its read-back's bounds.
func (s *resumeStack) resetWith(held engine.ResumeReset, rec *orkstackv1.ResumePlanRecord, edit func(*orkstackv1.ResumeReadback)) {
	s.t.Helper()
	edit(rec.GetBlock().GetReadback())
	rec.PlanDigest = engine.ResumePlanDigestOf(rec)
	key := engine.ResumePlanKey{Scope: rec.GetScope(), N: int(rec.GetN()), PlanDigest: s.rewritePlan(rec)}
	s.resetRun(held.RunID, held.BaseExecutionRunID, key.String(), held.WorkflowTaskFinishEventID)
}

// failedWithHandler starts an Orker on an engine that reads kept results
// through proxy, and answers a failed run of build and lint, then deploy,
// which fails until a resume runs it, and cleanup, their failure handler.
func (s *resumeStack) failedWithHandler(proxy *faultproxy.Proxy) (string, *dispatches, *chaosReplica) {
	s.t.Helper()
	replica := s.startResumingReplica("replica-a", replicaOptions{}, resumeFaults{results: proxy})
	s.startProjection(nil)
	made := &dispatches{}
	s.startOrker("orker-a", made.noting(s.committing(failsUntilResumed("deploy"))))
	runID := s.startFailing(resumeDocument(s.t, "resume-handled", resumeTask{id: "build"}, resumeTask{id: "lint"},
		resumeTask{id: "deploy", needs: []string{"build", "lint"}},
		resumeTask{id: "cleanup", needs: []string{"deploy"}, join: "any_failed"}))
	s.requireRow(s.read(runID), "cleanup", orkstackv1.Status_STATUS_SUCCEEDED, "")
	return runID, made, replica
}

// awaitMet waits until proxy has turned away or held n connections.
func (s *resumeStack) awaitMet(proxy *faultproxy.Proxy, n int, within time.Duration) {
	s.t.Helper()
	s.eventually(within, "the read-back to meet the outage", func() (bool, string) {
		st := proxy.Stats()
		return st.Refused+st.Stalled >= n, fmt.Sprintf("%+v", st)
	})
}

// TestIntegration_ResumeReadBackRefusedThenResumedAgain: the store refuses
// every read of a resume's whole read-back. The run fails COMMIT_READ_FAILED
// on the result it could not read, its failure handler does not start,
// nothing runs again and its history has hardly grown; the next resume keeps
// the same result and runs nothing but what failed. The read-back's deadline
// is 40 s here; its default is 10 minutes, with the same spacing.
func TestIntegration_ResumeReadBackRefusedThenResumedAgain(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	s := newResumeStack(t, ctx, "resume/refused")
	proxy := s.minioProxy()
	runID, made, _ := s.failedWithHandler(proxy)
	held, rec := s.recordedReset(s.serveResume(pastContainers).preview(runID))
	proxy.Cut()
	s.resetWith(held, rec, func(r *orkstackv1.ResumeReadback) { r.Deadline = durationpb.New(40 * time.Second) })
	if status, err := s.wait(runID, 3*time.Minute); status != enums.WORKFLOW_EXECUTION_STATUS_FAILED {
		t.Fatalf("the resumed run closed %v: %v", status, err)
	}
	if st := proxy.Stats(); st.Refused < 2 {
		t.Fatalf("the store refused %d reads; the read-back was meant to try again", st.Refused)
	}
	scope := int64(rec.GetScope())
	run := s.read(runID)
	s.requireRow(run, "build", orkstackv1.Status_STATUS_FAILED, engine.ReasonCommitReadFailed)
	s.requireRow(run, "lint", orkstackv1.Status_STATUS_CANCELED, engine.ReasonResumeNotReadBack)
	s.requireNothingRan(runID, made, scope)
	execution, _ := s.current(runID)
	if grew := len(s.history(runID, execution).GetEvents()) - int(rec.GetRStartedEventId()); grew >= 300 {
		t.Fatalf("the read-back took %d events of history, want fewer than 300", grew)
	}

	proxy.Restore()
	door := s.serveResume(pastContainers)
	preview := door.preview(runID)
	// build failed this time and its commit is there: the resume keeps both
	// and runs deploy alone.
	if preview.GetResumesUsed() != 1 || preview.GetKeptCount() != 2 || preview.GetSeqBase() <= scope {
		t.Fatalf("preview = %v; want one resume used, two results kept and a scope above %d", preview, scope)
	}
	again := door.confirm(preview).GetSeqBase()
	run = s.awaitSucceeded(runID)
	s.requireRow(run, "build", orkstackv1.Status_STATUS_SUCCEEDED, "")
	s.requireRow(run, "deploy", orkstackv1.Status_STATUS_SUCCEEDED, "")
	for _, id := range []string{"build", "lint"} {
		if n := made.of(runID, id, 0) + made.of(runID, id, scope) + made.of(runID, id, again); n != 1 {
			t.Fatalf("%s ran %d times over the run and both resumes, want once", id, n)
		}
	}
	if n := made.of(runID, "deploy", again); n != 1 {
		t.Fatalf("deploy ran %d times under the second resume, want once", n)
	}
}

// requireNothingRan fails if a task ran under scope, or a failure handler ran
// again.
func (s *resumeStack) requireNothingRan(runID string, made *dispatches, scope int64) {
	s.t.Helper()
	for _, id := range []string{"build", "lint", "deploy", "cleanup"} {
		if n := made.of(runID, id, scope); n != 0 {
			s.t.Fatalf("%s ran %d times under scope %d, before its run's results were read back", id, n, scope)
		}
	}
	if n := made.of(runID, "cleanup", 0); n != 1 {
		s.t.Fatalf("the failure handler ran %d times, want the base's once", n)
	}
}

// TestIntegration_ResumeReadBackSurvivesASlowStore: the store does not answer
// for longer than one read's bound, and then does. The result is kept.
func TestIntegration_ResumeReadBackSurvivesASlowStore(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()
	s := newResumeStack(t, ctx, "resume/slow")
	proxy := s.minioProxy()
	runID, made, _ := s.failedWithHandler(proxy)
	held, rec := s.recordedReset(s.serveResume(pastContainers).preview(runID))
	proxy.Stall()
	s.resetWith(held, rec, func(r *orkstackv1.ResumeReadback) { r.GetTimeout = durationpb.New(5 * time.Second) })
	s.awaitMet(proxy, 2, 2*time.Minute)
	if n := len(s.published(runID, orkstackv1.EventType_EVENT_TYPE_RUN_RESULTS_KEPT)); n != 0 {
		t.Fatal("RUN_RESULTS_KEPT was published while no read had been answered")
	}
	proxy.Restore()
	s.requireResumedPair(runID, made, int64(rec.GetScope()))
	// The resumed run succeeded, so its failure handler has nothing to do.
	s.requireRow(s.read(runID), "cleanup", orkstackv1.Status_STATUS_SKIPPED, "")
	if n := made.of(runID, "cleanup", int64(rec.GetScope())); n != 0 {
		t.Fatalf("the failure handler ran %d times in a resume that succeeded", n)
	}
}

// TestIntegration_ResumeRunsItsFailureHandlerAgain: a resumed run whose task
// fails again runs its failure handler again, under the resume's own key, and
// ends with that failure.
func TestIntegration_ResumeRunsItsFailureHandlerAgain(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()
	s := newResumeStack(t, ctx, "resume/handler")
	s.startEngine()
	made := &dispatches{}
	s.startOrker("orker-a", made.noting(s.committing(func(in *orkstackv1.ExecuteTaskInput) error {
		if in.GetTaskId() == "deploy" {
			return temporal.NewNonRetryableApplicationError("deploy failed", "ActivityFailed", nil)
		}
		return nil
	})))
	runID := s.startFailing(resumeDocument(t, "resume-handler", resumeTask{id: "build"},
		resumeTask{id: "deploy", needs: []string{"build"}}, resumeTask{id: "cleanup", needs: []string{"deploy"}, join: "any_failed"}))
	door := s.serveResume(pastContainers)
	preview := door.preview(runID)
	door.confirm(preview)
	if status, err := s.wait(runID, 2*time.Minute); status != enums.WORKFLOW_EXECUTION_STATUS_FAILED {
		t.Fatalf("the resumed run closed %v: %v", status, err)
	}
	scope := preview.GetSeqBase()
	s.requireResumedOnce(runID, scope)
	s.requireRow(s.read(runID), "cleanup", orkstackv1.Status_STATUS_SUCCEEDED, "")
	for id, again := range map[string]int{"build": 0, "deploy": 1, "cleanup": 1} {
		if first, n := made.of(runID, id, 0), made.of(runID, id, scope); first != 1 || n != again {
			t.Errorf("%s ran %d times as submitted and %d under the resume, want 1 and %d", id, first, n, again)
		}
	}
	s.requireOneEnding(runID, scope, orkstackv1.EventType_EVENT_TYPE_RUN_FAILED)
}

// TestIntegration_ResumeCanceledDuringItsReadBack: an operator cancels a
// resumed run while the store holds its read. The run ends canceled, every
// row it had not read back says so and stays kept, its failure handler does
// not start, and the events go on without a gap.
func TestIntegration_ResumeCanceledDuringItsReadBack(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()
	s := newResumeStack(t, ctx, "resume/canceled")
	proxy := s.minioProxy()
	runID, made, _ := s.failedWithHandler(proxy)
	preview := s.serveResume(pastContainers).preview(runID)
	held, rec := s.recordedReset(preview)
	proxy.Stall()
	s.resetWith(held, rec, func(*orkstackv1.ResumeReadback) {})
	s.awaitMet(proxy, 1, 2*time.Minute)
	must.OKf(t, s.control.CancelWorkflow(ctx, runID, ""), "cancel")
	if status, err := s.wait(runID, 3*time.Minute); status != enums.WORKFLOW_EXECUTION_STATUS_CANCELED {
		t.Fatalf("the canceled run closed %v: %v", status, err)
	}
	run := s.read(runID)
	if run.GetStatus() != orkstackv1.Status_STATUS_CANCELED {
		t.Fatalf("the run reads %v %v, want CANCELED", run.GetStatus(), run.GetError())
	}
	for _, id := range []string{"build", "lint"} {
		if row := s.requireRow(run, id, orkstackv1.Status_STATUS_CANCELED, engine.ReasonResumeNotReadBack); !row.GetReadingBack() {
			t.Fatalf("%s's row, not read back, is not marked so", id)
		}
	}
	s.requireNothingRan(runID, made, int64(rec.GetScope()))
	s.requireOneEnding(runID, int64(rec.GetScope()), orkstackv1.EventType_EVENT_TYPE_RUN_CANCELED)
}

// requireOneEnding fails unless the run's events read whole and those from
// scope on end in one event of type typ.
func (s *resumeStack) requireOneEnding(runID string, scope int64, typ orkstackv1.EventType) {
	s.t.Helper()
	events := s.requireWholeStream(runID)
	ends := 0
	for _, e := range events {
		if e.GetSequence() >= scope && e.GetType() == typ {
			ends++
		}
	}
	if len(events) == 0 || events[len(events)-1].GetType() != typ || ends != 1 {
		s.t.Fatalf("run %s's events from %d end %d times in %v: %v", runID, scope, ends, typ, eventKeys(events))
	}
}

// TestIntegration_ResumeEngineKilledDuringItsReadBack: the engine replica
// reading a run's kept results dies with the read unanswered. Another
// replica reads them, each result is applied once, and the resume is
// announced once.
func TestIntegration_ResumeEngineKilledDuringItsReadBack(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()
	s := newResumeStack(t, ctx, "resume/engine-killed")
	proxy := s.minioProxy()
	runID, made, replica := s.failedWithHandler(proxy)
	preview := s.serveResume(pastContainers).preview(runID)
	proxy.Stall()
	confirmed := make(chan error, 1)
	door := s.serveResume(pastContainers)
	go func() {
		_, refused, err := door.try(ctx, confirmOf(preview))
		if err == nil && refused != nil {
			err = fmt.Errorf("refused: %v", refused)
		}
		confirmed <- err
	}()
	s.awaitMet(proxy, 1, 2*time.Minute)
	s.startResumingReplica("replica-b", replicaOptions{}, resumeFaults{})
	replica.stop()
	s.requireResumedPair(runID, made, preview.GetSeqBase())
	must.OKf(t, <-confirmed, "the confirm that resumed it")
}

// TestIntegration_ResumeFrontendDownDuringItsReadBack: the engine's only
// replica loses the cluster while it reads kept results, and gets it back.
// The run carries on where it was.
func TestIntegration_ResumeFrontendDownDuringItsReadBack(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()
	s := newResumeStack(t, ctx, "resume/frontend")
	store, frontend := s.minioProxy(), s.temporalProxy()
	s.startResumingReplica("replica-a", replicaOptions{temporal: frontend.Addr()}, resumeFaults{results: store})
	s.startProjection(nil)
	made := &dispatches{}
	s.startOrker("orker-a", made.noting(s.committing(failsUntilResumed("deploy"))))
	runID := s.startFailing(s.buildAndDeploy())
	held, rec := s.recordedReset(s.serveResume(pastContainers).preview(runID))
	store.Stall()
	s.resetWith(held, rec, func(r *orkstackv1.ResumeReadback) { r.GetTimeout = durationpb.New(5 * time.Second) })
	s.awaitMet(store, 1, 2*time.Minute)
	frontend.Cut()
	store.Restore()
	s.eventually(time.Minute, "the engine to meet the outage", func() (bool, string) {
		st := frontend.Stats()
		return st.Refused >= 2, fmt.Sprintf("%+v", st)
	})
	frontend.Restore()
	s.requireResumedPair(runID, made, int64(rec.GetScope()))
}

// TestIntegration_ResumeKeepsNothingItCannotFind: a kept result's commit is
// gone. Gone before the confirm, the plan has changed; gone after the reset,
// the run fails COMMIT_MISSING with nothing run again. Either way the next
// preview runs that task again.
func TestIntegration_ResumeKeepsNothingItCannotFind(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	s := newResumeStack(t, ctx, "resume/gone")
	s.startEngine()
	made := &dispatches{}
	s.startOrker("orker-a", made.noting(s.committing(failsUntilResumed("deploy"))))
	remove := func(runID string) {
		t.Helper()
		key, err := artifacts.ScopedCommitKey("default", runID, "build", 0)
		must.OK(t, err)
		if keys := s.listObjects(key); len(keys) != 1 {
			t.Fatalf("commits of build under %s = %v, want its one", key, keys)
		}
		_, err = newRawS3ClientForCommitE2E(t, s.cfg).DeleteObject(ctx, &s3.DeleteObjectInput{Bucket: aws.String(s.cfg.S3Bucket), Key: aws.String(key)})
		must.OKf(t, err, "delete %s", key)
	}
	rerunsBoth := func(door *resumeDoor, runID string) {
		t.Helper()
		preview := door.preview(runID)
		if preview.GetKeptCount() != 0 {
			t.Fatalf("the preview keeps %d results, want none: build's is gone", preview.GetKeptCount())
		}
		scope := door.confirm(preview).GetSeqBase()
		run := s.awaitSucceeded(runID)
		s.requireRow(run, "build", orkstackv1.Status_STATUS_SUCCEEDED, "")
		if b, d := made.of(runID, "build", scope), made.of(runID, "deploy", scope); b != 1 || d != 1 {
			t.Fatalf("build ran %d times and deploy %d under %d, want each once", b, d, scope)
		}
	}
	// Between the preview and the confirm.
	runID := s.startFailing(s.buildAndDeploy())
	door := s.serveResume(pastContainers)
	preview := door.preview(runID)
	remove(runID)
	door.requireRefused(confirmOf(preview), "PLAN_CHANGED", "")
	rerunsBoth(door, runID)

	// Between the reset and the read-back.
	runID = s.startFailing(s.buildAndDeploy())
	door = s.serveResume(pastContainers, func(cfg *api.Config) {
		cfg.Resume = &countingCluster{ResumeCluster: s.counting(), before: func(context.Context, engine.ResumeReset) error {
			remove(runID)
			return nil
		}}
	})
	preview = door.preview(runID)
	door.confirm(preview)
	if status, err := s.wait(runID, 2*time.Minute); status != enums.WORKFLOW_EXECUTION_STATUS_FAILED {
		t.Fatalf("the resumed run closed %v: %v", status, err)
	}
	s.requireRow(s.read(runID), "build", orkstackv1.Status_STATUS_FAILED, engine.ReasonCommitMissing)
	if n := made.of(runID, "deploy", preview.GetSeqBase()); n != 0 {
		t.Fatalf("deploy ran %d times although build could not be read back", n)
	}
	rerunsBoth(s.serveResume(pastContainers), runID)
}

// TestIntegration_ResumeCheckUnavailable: the store does not answer the
// service's checks. The preview and the confirm are refused for now, and
// nothing is recorded or reset; once it answers, the same plan is confirmed.
func TestIntegration_ResumeCheckUnavailable(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()
	s := newResumeStack(t, ctx, "resume/unavailable")
	runID, made := s.failedPair()
	base, _ := s.current(runID)
	proxy := s.minioProxy()
	commits, values := s.storesVia(proxy)
	door := s.serveResume(pastContainers, func(cfg *api.Config) { cfg.Commits, cfg.ArtifactStore = commits, values })
	preview := door.preview(runID)
	proxy.Cut()
	door.refusedPreview(runID, "RESUME_CHECK_UNAVAILABLE", "")
	r := door.requireRefused(confirmOf(preview), "RESUME_CHECK_UNAVAILABLE", "")
	if r.GetClass() != orkstackv1.ResumeRefusalClass_RESUME_REFUSAL_CLASS_WAIT || r.GetRetryAfterSeconds() <= 0 {
		t.Fatalf("refusal = %v; want it to say when to try again", r)
	}
	if execution, _ := s.current(runID); execution != base {
		t.Fatalf("the run was reset to %s while its checks could not be made", execution)
	}
	if plans, err := s.records.ListResumePlans(ctx, "default", runID); err != nil || len(plans) != 0 {
		t.Fatalf("plans = %v, %v; want none recorded", plans, err)
	}
	proxy.Restore()
	door.confirm(preview)
	s.requireResumedPair(runID, made, preview.GetSeqBase())
}

// TestIntegration_ResumeOrkerKilledUnderARerunTask: the Orker running a task
// a resume runs again is gone with the attempt unanswered. The task is
// retried by its own policy on another Orker, under the same key, and commits
// once.
func TestIntegration_ResumeOrkerKilledUnderARerunTask(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	s := newResumeStack(t, ctx, "resume/rerun-killed")
	s.startEngine()
	a := s.startOrkerProcess("a")
	wf := resumeDocument(t, "resume-rerun-killed",
		resumeTask{id: "build", command: succeedCommand},
		// Fails as submitted; its first attempt under the resume hangs until
		// its Orker is killed, and the next succeeds.
		resumeTask{id: "deploy", needs: []string{"build"}, attempts: 2, command: `grep -q '"idempotencyKey": *"[^"]*@r' /orkstack/input/task.json || exit 1; ` +
			`grep -qE '"attempt": *1(,|$)' /orkstack/input/task.json || { ` + succeedCommand + `; exit 0; }; ` + ignoresTERM})
	runID := s.startFailing(wf)
	door := s.serveResume(pastContainers)
	preview := door.preview(runID)
	door.confirm(preview)
	s.awaitContainers("ork.orker="+a.id, 1, 2*time.Minute)
	s.kill(a)
	b := s.startOrkerProcess("b")
	run := s.awaitSucceeded(runID)
	s.requireResumedOnce(runID, preview.GetSeqBase())
	if row := s.requireRow(run, "deploy", orkstackv1.Status_STATUS_SUCCEEDED, ""); row.GetOrkerId() != b.id {
		t.Fatalf("deploy succeeded on %s, want %s", row.GetOrkerId(), b.id)
	}
	s.requireAttempts(runID, "build", 1)
	s.requireAttempts(runID, "deploy", 4) // two as submitted, two under the resume
	commit, err := artifacts.ScopedCommitKey("default", runID, "deploy", uint64(preview.GetSeqBase()))
	must.OK(t, err)
	if keys := s.listObjects(commit); len(keys) != 1 {
		t.Fatalf("commits of deploy under the resume = %v, want one", keys)
	}
}
