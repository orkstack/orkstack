//go:build integration

package engine_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/google/uuid"
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/api/enums/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/temporal"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/api"
	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/internal/task"
	"github.com/orkstack/orkstack/internal/testsupport/must"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// A resume is one reset of the run, whoever asks for it, whatever dies while
// it is made, and whatever else resets the run (E85 §30).

// buildAndDeploy is build, then deploy, which fails until a resume runs it.
func (s *resumeStack) buildAndDeploy() *orkstackv1.Orkflow {
	return resumeDocument(s.t, "resume-pair", resumeTask{id: "build"}, resumeTask{id: "deploy", needs: []string{"build"}})
}

// failedPair starts the engine and an Orker, and answers a failed run of
// buildAndDeploy with every attempt made of it.
func (s *resumeStack) failedPair() (string, *dispatches) {
	s.t.Helper()
	s.startEngine()
	made := &dispatches{}
	s.startOrker("orker-a", made.noting(s.committing(failsUntilResumed("deploy"))))
	return s.startFailing(s.buildAndDeploy()), made
}

// requireResumedPair fails unless the run succeeded under scope, with build
// run once, as it was submitted, and deploy once more, under scope.
func (s *resumeStack) requireResumedPair(runID string, made *dispatches, scope int64) {
	s.t.Helper()
	run := s.awaitSucceeded(runID)
	s.requireResumedOnce(runID, scope)
	if !s.requireRow(run, "build", orkstackv1.Status_STATUS_SUCCEEDED, "").GetAdopted() {
		s.t.Fatal("build was kept, and its row does not say so")
	}
	s.requireRow(run, "deploy", orkstackv1.Status_STATUS_SUCCEEDED, "")
	if b, d, again := made.of(runID, "build", 0), made.of(runID, "deploy", 0), made.of(runID, "deploy", scope); b != 1 || d != 1 || again != 1 {
		s.t.Fatalf("build ran %d times, deploy %d, and %d under the resume; want each once", b, d, again)
	}
	if n := made.of(runID, "build", scope); n != 0 {
		s.t.Fatalf("build, which was kept, ran %d times under the resume", n)
	}
}

// requireOneReset fails unless the base was reset exactly once, to the run's
// current execution.
func (s *resumeStack) requireOneReset(runID, base string) {
	s.t.Helper()
	after, err := s.counting().DescribeResumeExecution(s.ctx, runID, base)
	must.OKf(s.t, err, "describe the base")
	current, _ := s.current(runID)
	if after.ResetRunID == "" || after.ResetRunID != current || current == base {
		s.t.Fatalf("the base %s names reset %q and the run's current execution is %s; want the one reset", base, after.ResetRunID, current)
	}
}

// TestIntegration_ResumeConcurrentConfirm: confirms of one preview made at
// the same moment, through two engine replicas and the production claim
// workflow, make one reset. Each is answered with the resume or told to wait
// or look again, and a repeat finds it already made.
func TestIntegration_ResumeConcurrentConfirm(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()
	s := newResumeStack(t, ctx, "resume/concurrent")
	runID, made := s.failedPair()
	base, _ := s.current(runID)
	const each = 3
	// The first reset is held until every other confirm has its answer, so
	// they are all made while one is in flight.
	others := make(chan struct{})
	cluster := s.counting()
	cluster.before = func(ctx context.Context, _ engine.ResumeReset) error {
		select {
		case <-others:
		case <-time.After(3 * time.Minute):
			t.Error("the other confirms were not answered while the first was in flight")
		case <-ctx.Done():
		}
		return nil
	}
	counted := func(cfg *api.Config) { cfg.Resume = cluster }
	doors := []*resumeDoor{s.serveResume(pastContainers, counted), s.serveResume(pastContainers, counted)}
	preview := doors[0].preview(runID)

	type answer struct {
		resp    *orkstackv1.ResumeRunResponse
		refused *orkstackv1.ResumeRefusal
		err     error
	}
	answers := make(chan answer)
	start := make(chan struct{})
	var wg sync.WaitGroup
	for _, door := range doors {
		for range each {
			wg.Go(func() {
				<-start
				resp, refused, err := door.try(ctx, confirmOf(preview))
				answers <- answer{resp, refused, err}
			})
		}
	}
	close(start)
	resumed := 0
	for i := range each * len(doors) {
		if i == each*len(doors)-1 {
			close(others)
		}
		a := <-answers
		switch {
		case a.err != nil:
			t.Errorf("a concurrent confirm = %v", a.err)
		case a.resp != nil:
			resumed++
			if a.resp.GetSeqBase() != preview.GetSeqBase() {
				t.Errorf("a confirm answered a resume at %d, want %d", a.resp.GetSeqBase(), preview.GetSeqBase())
			}
		case !slices.Contains([]string{"RESUME_IN_PROGRESS", "RUN_CHANGED"}, a.refused.GetReason()):
			t.Errorf("a concurrent confirm was refused %v; want RESUME_IN_PROGRESS or RUN_CHANGED", a.refused)
		}
	}
	wg.Wait()
	if resumed != 1 {
		t.Errorf("%d confirms were answered with the resume, want the one that made it", resumed)
	}
	if n := cluster.resets(); n != 1 {
		t.Errorf("%d resets were sent, want 1", n)
	}
	s.requireResumedPair(runID, made, preview.GetSeqBase())
	s.requireOneReset(runID, base)
	if again := doors[1].confirm(preview); !again.GetAlreadyResumed() {
		t.Fatalf("the same confirm again = %v, want the resume already made", again)
	}
	if n := cluster.resets(); n != 1 {
		t.Fatalf("%d resets after the repeat, want 1", n)
	}
}

// diedMidway is a door whose request dies around its reset: the engine that
// was answering it is gone, and nothing it held in memory is left.
func (s *resumeStack) diedMidway(preview *orkstackv1.ResumeRunResponse, cluster *countingCluster, clock time.Duration, entered <-chan struct{}) {
	s.t.Helper()
	ctx, cancel := context.WithCancel(s.ctx)
	defer cancel()
	door := s.serveResume(clock, func(cfg *api.Config) { cfg.Resume = cluster })
	done := make(chan struct{})
	go func() {
		defer close(done)
		_, _, _ = door.try(ctx, confirmOf(preview))
	}()
	select {
	case <-entered:
	case <-done:
		s.t.Fatal("the confirm ended before it reached its reset")
	case <-time.After(3 * time.Minute):
		s.t.Fatal("the confirm never reached its reset")
	}
	cancel()
	<-done
}

// TestIntegration_ResumeEngineKilledBeforeItsReset: an engine dies after it
// recorded a resume and before it sent the reset. Nothing of the run was
// touched; another replica says when it can be settled, and then sends the
// very plan that was recorded. The service's clock is moved past the window.
func TestIntegration_ResumeEngineKilledBeforeItsReset(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()
	s := newResumeStack(t, ctx, "resume/killed-before")
	runID, made := s.failedPair()
	base, _ := s.current(runID)
	preview := s.serveResume(pastContainers).preview(runID)
	entered := make(chan struct{})
	s.diedMidway(preview, &countingCluster{ResumeCluster: s.counting(), before: func(ctx context.Context, _ engine.ResumeReset) error {
		close(entered)
		<-ctx.Done()
		return ctx.Err()
	}}, pastContainers, entered)

	cluster := s.counting()
	door := s.serveResume(pastContainers, func(cfg *api.Config) { cfg.Resume = cluster })
	if again := door.preview(runID); again.GetPlanDigest() != preview.GetPlanDigest() {
		t.Fatalf("the plan after an engine died is %s, want the one it recorded, %s", again.GetPlanDigest(), preview.GetPlanDigest())
	}
	r := door.requireRefused(confirmOf(preview), "RESUME_IN_PROGRESS", "")
	if r.GetRetryAfterSeconds() <= 0 {
		t.Fatalf("refusal = %v; want it to say when", r)
	}
	if execution, _ := s.current(runID); execution != base || cluster.resets() != 0 {
		t.Fatalf("the run's execution is %s and %d resets were sent, want %s and none", execution, cluster.resets(), base)
	}
	door.clock.move(time.Duration(r.GetRetryAfterSeconds())*time.Second + time.Second)
	resumed := door.confirm(preview)
	if resumed.GetSeqBase() != preview.GetSeqBase() || cluster.resets() != 1 {
		t.Fatalf("resume at %d with %d resets; want the recorded plan's, %d, sent once", resumed.GetSeqBase(), cluster.resets(), preview.GetSeqBase())
	}
	s.requireResumedPair(runID, made, preview.GetSeqBase())
	s.requireOneReset(runID, base)
	if plans, err := s.records.ListResumePlans(ctx, "default", runID); err != nil || len(plans) != 1 {
		t.Fatalf("plans = %v, %v; want the one recorded", plans, err)
	}
}

// TestIntegration_ResumeEngineKilledAfterItsReset: an engine dies after the
// cluster applied its reset and before it answered. The run carries on, and
// the same confirm on another replica finds the resume made, at once and
// with no second reset.
func TestIntegration_ResumeEngineKilledAfterItsReset(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()
	s := newResumeStack(t, ctx, "resume/killed-after")
	runID, made := s.failedPair()
	base, _ := s.current(runID)
	preview := s.serveResume(pastContainers).preview(runID)
	entered := make(chan struct{})
	s.diedMidway(preview, &countingCluster{ResumeCluster: s.counting(), after: func(ctx context.Context, _ string) error {
		close(entered)
		<-ctx.Done()
		return ctx.Err()
	}}, pastContainers, entered)

	cluster := s.counting()
	door := s.serveResume(pastContainers, func(cfg *api.Config) { cfg.Resume = cluster })
	if again := door.confirm(preview); !again.GetAlreadyResumed() || again.GetSeqBase() != preview.GetSeqBase() || cluster.resets() != 0 {
		t.Fatalf("the confirm after an engine died = %v with %d resets; want the resume already made", again, cluster.resets())
	}
	s.requireResumedPair(runID, made, preview.GetSeqBase())
	s.requireOneReset(runID, base)
}

// recordedReset records a confirmed resume without resetting the run, and
// answers the reset that would have made it.
func (s *resumeStack) recordedReset(preview *orkstackv1.ResumeRunResponse) (engine.ResumeReset, *orkstackv1.ResumePlanRecord) {
	s.t.Helper()
	var held engine.ResumeReset
	door := s.serveResume(pastContainers, func(cfg *api.Config) {
		cfg.Resume = &countingCluster{ResumeCluster: s.counting(), before: func(_ context.Context, reset engine.ResumeReset) error {
			held = reset
			return engine.ErrResetOutcomeUnknown
		}}
	})
	door.requireRefused(confirmOf(preview), "RESUME_UNCONFIRMED", "reset_outcome_unknown")
	key, ok := engine.ParseResumePlanKey(held.Reason)
	if !ok {
		s.t.Fatalf("the reset's reason %q is no plan key", held.Reason)
	}
	read, _, err := s.records.ReadResumePlan(s.ctx, "default", preview.GetRunId(), key.Scope, key.N)
	if err != nil || read.Answer != artifacts.RecordFound {
		s.t.Fatalf("the recorded plan = %v, %v", read.Answer, err)
	}
	return held, read.Value
}

// resetRun resets the run's base as anyone holding the cluster's credential
// can, and answers the new execution.
func (s *resumeStack) resetRun(runID, base, reason string, finishEventID int64) string {
	s.t.Helper()
	reset, err := s.control.WorkflowService().ResetWorkflowExecution(s.ctx, &workflowservice.ResetWorkflowExecutionRequest{
		Namespace:         s.namespace,
		WorkflowExecution: &commonpb.WorkflowExecution{WorkflowId: runID, RunId: base},
		Reason:            reason, WorkflowTaskFinishEventId: finishEventID, RequestId: uuid.NewString(),
		ResetReapplyExcludeTypes: []enums.ResetReapplyExcludeType{
			enums.RESET_REAPPLY_EXCLUDE_TYPE_SIGNAL, enums.RESET_REAPPLY_EXCLUDE_TYPE_UPDATE, enums.RESET_REAPPLY_EXCLUDE_TYPE_NEXUS,
		},
	})
	must.OKf(s.t, err, "reset %s", runID)
	return reset.GetRunId()
}

// rewritePlan replaces a stored plan's bytes, which are write-once to
// everything that uses the store, and answers the digest of the new ones.
func (s *resumeStack) rewritePlan(rec *orkstackv1.ResumePlanRecord) string {
	s.t.Helper()
	key, err := artifacts.ResumePlanKey("default", rec.GetRunId(), rec.GetScope(), int(rec.GetN()))
	must.OK(s.t, err)
	data, err := artifacts.MarshalResumePlan(rec)
	must.OK(s.t, err)
	_, err = newRawS3ClientForCommitE2E(s.t, s.cfg).PutObject(s.ctx, &s3.PutObjectInput{
		Bucket: aws.String(s.cfg.S3Bucket), Key: aws.String(key), Body: bytes.NewReader(data),
	})
	must.OKf(s.t, err, "replace %s", key)
	return artifacts.ResumePlanDigest(data)
}

// TestIntegration_ResumeForgedResets: a reset that claims a plan it does not
// carry runs nothing. Where the run's sequence is known it ends FAILED with
// its own reason and an event, its tasks reading as its base ended them, and
// it can be resumed again; where it is not, it ends with no event. A reset
// with no plan's reason is no resume at all.
func TestIntegration_ResumeForgedResets(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 14*time.Minute)
	defer cancel()
	s := newResumeStack(t, ctx, "resume/forged")
	s.startEngine()
	made := &dispatches{}
	s.startOrker("orker-a", made.noting(s.committing(failsUntilResumed("deploy"))))

	for _, tc := range []struct {
		name string
		// forge resets the run and answers the scope its reset names, or 0.
		forge func(runID string, held engine.ResumeReset, rec *orkstackv1.ResumePlanRecord) int64
	}{
		{"an altered record", func(runID string, held engine.ResumeReset, rec *orkstackv1.ResumePlanRecord) int64 {
			rec.Reason = "altered after it was confirmed"
			s.rewritePlan(rec)
			s.resetRun(runID, held.BaseExecutionRunID, held.Reason, held.WorkflowTaskFinishEventID)
			return int64(rec.GetScope())
		}},
		{"an altered plan under its own digest", func(runID string, held engine.ResumeReset, rec *orkstackv1.ResumePlanRecord) int64 {
			rec.Block.FromTaskId = "deploy"
			key := engine.ResumePlanKey{Scope: rec.GetScope(), N: int(rec.GetN()), PlanDigest: s.rewritePlan(rec)}
			s.resetRun(runID, held.BaseExecutionRunID, key.String(), held.WorkflowTaskFinishEventID)
			return int64(rec.GetScope())
		}},
		{"a record that is gone", func(runID string, held engine.ResumeReset, rec *orkstackv1.ResumePlanRecord) int64 {
			s.removePlan(rec)
			s.resetRun(runID, held.BaseExecutionRunID, held.Reason, held.WorkflowTaskFinishEventID)
			return int64(rec.GetScope())
		}},
		{"a genuine plan at another reset point", func(runID string, held engine.ResumeReset, rec *orkstackv1.ResumePlanRecord) int64 {
			s.resetRun(runID, held.BaseExecutionRunID, held.Reason, s.workflowTaskBefore(runID, held))
			return int64(rec.GetScope())
		}},
		{"a plan key of another format", func(runID string, held engine.ResumeReset, _ *orkstackv1.ResumePlanRecord) int64 {
			s.resetRun(runID, held.BaseExecutionRunID, strings.Replace(held.Reason, ":v1:", ":v9:", 1), held.WorkflowTaskFinishEventID)
			return 0
		}},
	} {
		t.Logf("a reset under %s", tc.name)
		runID := s.startFailing(s.buildAndDeploy())
		before := s.read(runID)
		terminal := s.published(runID, orkstackv1.EventType_EVENT_TYPE_RUN_FAILED)[0].GetSequence()
		held, rec := s.recordedReset(s.serveResume(pastContainers).preview(runID))
		scope := tc.forge(runID, held, rec)
		if status, err := s.wait(runID, 2*time.Minute); status != enums.WORKFLOW_EXECUTION_STATUS_FAILED {
			t.Fatalf("%s: the reset run closed %v: %v", tc.name, status, err)
		}
		run := s.read(runID)
		if run.GetStatus() != orkstackv1.Status_STATUS_FAILED || run.GetError().GetCode() != engine.ReasonResumePlanInvalid {
			t.Fatalf("%s: the run reads %v %v, want FAILED %s", tc.name, run.GetStatus(), run.GetError(), engine.ReasonResumePlanInvalid)
		}
		for _, row := range before.GetTaskRuns() {
			s.requireRow(run, row.GetTaskId(), row.GetStatus(), row.GetError().GetCode())
		}
		under := made.of(runID, "deploy", int64(rec.GetScope())) + made.of(runID, "build", int64(rec.GetScope()))
		if n := made.of(runID, "deploy", 0) + made.of(runID, "build", 0); n != 2 || under != 0 {
			t.Fatalf("%s: %d attempts as submitted, %d under the plan; want 2 and none", tc.name, n, under)
		}
		if n := len(s.published(runID, orkstackv1.EventType_EVENT_TYPE_RUN_RESUMED)); n != 0 {
			t.Fatalf("%s: RUN_RESUMED was published %d times for a plan that was not run", tc.name, n)
		}
		failed := s.published(runID, orkstackv1.EventType_EVENT_TYPE_RUN_FAILED)
		if scope == 0 {
			if len(failed) != 1 {
				t.Fatalf("%s: RUN_FAILED was published %v; want the base's alone, as no later sequence is known", tc.name, failed)
			}
			continue
		}
		if len(failed) != 2 || failed[1].GetSequence() != scope || scope != terminal+1 {
			t.Fatalf("%s: RUN_FAILED %v; want the base's and one at %d, right after it", tc.name, failed, scope)
		}
		// Nothing ran, so it can be resumed as before it was reset.
		door := s.serveResume(pastContainers)
		preview := door.preview(runID)
		if preview.GetResumesUsed() != 0 || preview.GetTerminalSequence() != scope {
			t.Fatalf("%s: preview = %v; want no resume counted, after %d", tc.name, preview, scope)
		}
		resumed := door.confirm(preview)
		s.requireResumedPair(runID, made, resumed.GetSeqBase())
	}

	t.Log("a plan key of which nothing is recorded")
	runID := s.startFailing(s.buildAndDeploy())
	door := s.serveResume(pastContainers)
	preview := door.preview(runID)
	base, _ := s.current(runID)
	scanned, err := engine.ScanResumeBase(s.history(runID, base).GetEvents(), nil)
	must.OKf(t, err, "scan the base")
	s.resetRun(runID, base, engine.ResumePlanKey{Scope: uint64(preview.GetSeqBase()), N: 0, PlanDigest: strings.Repeat("ab", 32)}.String(),
		scanned.ResetPoint.CompletedEventID)
	if status, err := s.wait(runID, 2*time.Minute); status != enums.WORKFLOW_EXECUTION_STATUS_FAILED {
		t.Fatalf("a reset under nothing recorded closed %v: %v", status, err)
	}
	if run := s.read(runID); run.GetError().GetCode() != engine.ReasonResumePlanInvalid {
		t.Fatalf("the run reads %v, want %s", run.GetError(), engine.ReasonResumePlanInvalid)
	}
	// Its chain of resumes cannot be followed, so no resume is numbered after
	// it: refused for good, with nothing recorded.
	sequence := s.published(runID, orkstackv1.EventType_EVENT_TYPE_RUN_FAILED)[1].GetSequence()
	r := door.requireRefused(&orkstackv1.ResumeRunRequest{RunId: runID, ExpectedTerminalSequence: sequence}, "STATE_UNREADABLE", "")
	if r.GetClass() != orkstackv1.ResumeRefusalClass_RESUME_REFUSAL_CLASS_PERMANENT {
		t.Fatalf("refusal = %v, want it permanent", r)
	}
	if plans, err := s.records.ListResumePlans(ctx, "default", runID); err != nil || len(plans) != 0 {
		t.Fatalf("plans = %v, %v; want none recorded", plans, err)
	}

	t.Log("a reset by hand")
	runID = s.startFailing(s.buildAndDeploy())
	held, _ := s.recordedReset(s.serveResume(pastContainers).preview(runID))
	s.resetRun(runID, held.BaseExecutionRunID, "an operator's reset", held.WorkflowTaskFinishEventID)
	if status, err := s.wait(runID, 2*time.Minute); status != enums.WORKFLOW_EXECUTION_STATUS_FAILED {
		t.Fatalf("a reset by hand closed %v: %v", status, err)
	}
	// It ran as it was submitted: build's commit adopted under its own key,
	// deploy failing again.
	if b, d := made.of(runID, "build", 0), made.of(runID, "deploy", 0); b != 2 || d != 2 {
		t.Fatalf("build was dispatched %d times and deploy %d, want each once more", b, d)
	}
	if n := len(s.published(runID, orkstackv1.EventType_EVENT_TYPE_RUN_RESUMED)); n != 0 {
		t.Fatalf("RUN_RESUMED was published %d times for a reset by hand", n)
	}
}

// removePlan deletes a stored plan.
func (s *resumeStack) removePlan(rec *orkstackv1.ResumePlanRecord) {
	s.t.Helper()
	key, err := artifacts.ResumePlanKey("default", rec.GetRunId(), rec.GetScope(), int(rec.GetN()))
	must.OK(s.t, err)
	_, err = newRawS3ClientForCommitE2E(s.t, s.cfg).DeleteObject(s.ctx, &s3.DeleteObjectInput{Bucket: aws.String(s.cfg.S3Bucket), Key: aws.String(key)})
	must.OKf(s.t, err, "delete %s", key)
}

// workflowTaskBefore is the completion of the base's last workflow task
// before the resume's own reset point.
func (s *resumeStack) workflowTaskBefore(runID string, held engine.ResumeReset) int64 {
	s.t.Helper()
	var last int64
	for _, e := range s.history(runID, held.BaseExecutionRunID).GetEvents() {
		if e.GetEventType() == enums.EVENT_TYPE_WORKFLOW_TASK_COMPLETED && e.GetEventId() < held.WorkflowTaskFinishEventID {
			last = e.GetEventId()
		}
	}
	if last == 0 {
		s.t.Fatalf("the base completed no workflow task before its reset point, %d", held.WorkflowTaskFinishEventID)
	}
	return last
}

// TestIntegration_ResumePlanUnreadable: the store does not answer a reset
// run's read of its plan for the run's whole restore timeout. The run ends
// FAILED RESUME_PLAN_UNREADABLE with an event, nothing run again, the confirm
// is told so, and the run can be resumed once the store answers.
func TestIntegration_ResumePlanUnreadable(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 14*time.Minute)
	defer cancel()
	s := newResumeStack(t, ctx, "resume/unreadable")
	proxy := s.minioProxy()
	s.startResumingReplica("replica-a", replicaOptions{}, resumeFaults{plans: proxy})
	s.startProjection(nil)
	made := &dispatches{}
	s.startOrker("orker-a", made.noting(s.committing(failsUntilResumed("deploy"))))
	runID := s.recordedRunID()
	plan := s.resumePlan()
	// The bound of a plan's load, which is five minutes where none is pinned;
	// the run itself never carries on.
	budget := engine.DefaultContinuationBudget(engine.DefaultHistoryMaxEvents, engine.DefaultHistoryMaxBytes, time.Minute)
	plan.Continuation = &budget
	s.start(runID, s.buildAndDeploy(), nil, plan)
	s.awaitFailed(runID)

	door := s.serveResume(pastContainers, func(cfg *api.Config) {
		// Long past the load's own minute: the confirm must see it end.
		cfg.ResumeSettings.ConfirmTimeout = 5 * time.Minute
	})
	preview := door.preview(runID)
	proxy.Cut()
	door.requireRefused(confirmOf(preview), "RUN_CHANGED", "resume_ended")
	if status, err := s.wait(runID, 5*time.Minute); status != enums.WORKFLOW_EXECUTION_STATUS_FAILED {
		t.Fatalf("the reset run closed %v: %v", status, err)
	}
	scope := preview.GetSeqBase()
	if run := s.read(runID); run.GetError().GetCode() != engine.ReasonResumePlanUnreadable {
		t.Fatalf("the run reads %v, want %s", run.GetError(), engine.ReasonResumePlanUnreadable)
	}
	s.requireOneEnding(runID, scope, orkstackv1.EventType_EVENT_TYPE_RUN_FAILED)
	if n := made.of(runID, "deploy", scope) + made.of(runID, "build", scope); n != 0 {
		t.Fatalf("%d tasks ran under a plan that was never read", n)
	}
	proxy.Restore()
	again := door.preview(runID)
	if again.GetResumesUsed() != 0 {
		t.Fatalf("%d resumes counted, want none: this one never got going", again.GetResumesUsed())
	}
	door.confirm(again)
	s.requireResumedPair(runID, made, again.GetSeqBase())
}

// TestIntegration_ResumeConfirmsOfDifferentPlans: while one plan's reset is
// in flight, a confirm of another plan of the same ending is told to wait;
// once the first is made, the second is of a run that has changed. One reset
// is made, and it carries the first plan.
func TestIntegration_ResumeConfirmsOfDifferentPlans(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()
	s := newResumeStack(t, ctx, "resume/two-plans")
	runID, made := s.failedPair()
	base, _ := s.current(runID)
	entered, release := make(chan struct{}), make(chan struct{})
	cluster := s.counting()
	cluster.before = func(ctx context.Context, _ engine.ResumeReset) error {
		close(entered)
		select {
		case <-release:
		case <-ctx.Done():
		}
		return nil
	}
	first := s.serveResume(pastContainers, func(cfg *api.Config) { cfg.Resume = cluster })
	other := s.counting()
	second := s.serveResume(pastContainers, func(cfg *api.Config) { cfg.Resume = other })
	preview := first.preview(runID)
	from, refused := second.ask(&orkstackv1.ResumeRunRequest{RunId: runID, DryRun: true, FromTaskId: "build"})
	if refused != nil || from.GetPlanDigest() == preview.GetPlanDigest() {
		t.Fatalf("a retry from build = %v, refused %v; want another plan", from, refused)
	}
	confirmed := make(chan error, 1)
	go func() {
		_, refused, err := first.try(ctx, confirmOf(preview))
		if err == nil && refused != nil {
			err = errors.New(refused.String())
		}
		confirmed <- err
	}()
	select {
	case <-entered:
	case <-time.After(3 * time.Minute):
		t.Fatal("the first confirm never reached its reset")
	}
	retry := confirmOf(from)
	retry.FromTaskId = "build"
	second.requireRefused(retry, "RESUME_IN_PROGRESS", "")
	close(release)
	must.OKf(t, <-confirmed, "the first confirm")
	if _, refused := second.ask(retry); refused.GetReason() != "RUN_CHANGED" {
		t.Fatalf("the other plan, after the first was made, was answered %v; want RUN_CHANGED", refused)
	}
	if n := cluster.resets() + other.resets(); n != 1 {
		t.Fatalf("%d resets were sent, want 1", n)
	}
	s.requireResumedPair(runID, made, preview.GetSeqBase())
	s.requireOneReset(runID, base)
}

// TestIntegration_ResumeStaysOnItsRelease: a run pinned to a release whose
// engines are gone is not resumed while they are; once one is back, it
// carries on there, although another release is current.
func TestIntegration_ResumeStaysOnItsRelease(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 12*time.Minute)
	defer cancel()
	s := newResumeStack(t, ctx, "resume/release")
	name := "ork-resume-" + uuid.NewString()
	v1, v2 := task.Deployment{Name: name, BuildID: "v1"}, task.Deployment{Name: name, BuildID: "v2"}
	made := &dispatches{}
	behavior := made.noting(s.committing(failsUntilResumed("deploy")))
	engineV1 := s.startResumingReplica("v1-a", replicaOptions{deployment: v1}, resumeFaults{})
	s.startProjection(nil)
	s.startOrkerVersioned("orker-v1", s.orkerQueue, s.control, v1, behavior)
	setCurrentVersion(t, ctx, s.control, v1, s.runQueue, s.orkerQueue)
	runID := s.startFailing(s.buildAndDeploy())
	releases, err := engine.NewTemporalReleaseCluster(s.control, s.reader, s.namespace, name, s.runQueue, s.orkerQueue)
	must.OKf(t, err, "the deployment's releases")
	door := s.serveResume(pastContainers, func(cfg *api.Config) { cfg.Releases = releases })
	preview := door.preview(runID)

	s.startResumingReplica("v2-a", replicaOptions{deployment: v2}, resumeFaults{})
	s.startOrkerVersioned("orker-v2", s.orkerQueue, s.control, v2, behavior)
	setCurrentVersion(t, ctx, s.control, v2, s.runQueue, s.orkerQueue)
	engineV1.stop()
	// The cluster shows a poller for a while after it stops.
	s.eventually(5*time.Minute, "v1's engines to be gone", func() (bool, string) {
		obs, err := releases.ObserveReleases(ctx)
		return err == nil && obs.EngineReplicas["v1"] == 0 && obs.EngineReplicas["v2"] == 1, fmt.Sprintf("%v %v", obs.EngineReplicas, err)
	})
	r := door.refusedPreview(runID, "RELEASE_UNAVAILABLE", "")
	if r.GetClass() != orkstackv1.ResumeRefusalClass_RESUME_REFUSAL_CLASS_WAIT {
		t.Fatalf("refusal = %v, want it to wait", r)
	}
	door.requireRefused(confirmOf(preview), "RELEASE_UNAVAILABLE", "")

	s.startResumingReplica("v1-b", replicaOptions{deployment: v1}, resumeFaults{})
	s.eventually(time.Minute, "v1's engine to be back", func() (bool, string) {
		obs, err := releases.ObserveReleases(ctx)
		return err == nil && obs.EngineReplicas["v1"] == 1, fmt.Sprintf("%v %v", obs.EngineReplicas, err)
	})
	door.confirm(preview)
	s.requireResumedPair(runID, made, preview.GetSeqBase())
	resumed, err := s.counting().DescribeResumeExecution(ctx, runID, "")
	must.OKf(t, err, "describe the resumed run")
	if resumed.BuildID != "v1" {
		t.Fatalf("the resumed run is on release %q, want the one it started on, v1", resumed.BuildID)
	}
}

// TestIntegration_ResumeMarkedForAnotherEnding: the mark a resume leaves
// before its reset stands there naming another ending. The resume is refused
// for good, and nothing is reset.
func TestIntegration_ResumeMarkedForAnotherEnding(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()
	s := newResumeStack(t, ctx, "resume/mark-taken")
	runID, made := s.failedPair()
	base, _ := s.current(runID)
	cluster := s.counting()
	door := s.serveResume(pastContainers, func(cfg *api.Config) { cfg.Resume = cluster })
	preview := door.preview(runID)
	must.OKf(t, s.endings.MarkResumed(ctx, runID, uint64(preview.GetSeqBase()), 7), "mark another ending")
	r := door.requireRefused(confirmOf(preview), "STATE_UNREADABLE", "")
	if r.GetClass() != orkstackv1.ResumeRefusalClass_RESUME_REFUSAL_CLASS_PERMANENT {
		t.Fatalf("refusal = %v, want it permanent", r)
	}
	if execution, _ := s.current(runID); execution != base || cluster.resets() != 0 || made.of(runID, "deploy", preview.GetSeqBase()) != 0 {
		t.Fatalf("the run is at %s with %d resets sent; want it untouched at %s", execution, cluster.resets(), base)
	}
}

// TestIntegration_ResumeOfARunThatMoved: a confirm of a plan that is no
// longer what would run, or of a base that was reset since the preview, is
// refused, and resets nothing.
func TestIntegration_ResumeOfARunThatMoved(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()
	s := newResumeStack(t, ctx, "resume/moved")
	runID, made := s.failedPair()
	cluster := s.counting()
	door := s.serveResume(pastContainers, func(cfg *api.Config) { cfg.Resume = cluster })
	preview := door.preview(runID)

	stale := confirmOf(preview)
	stale.FromTaskId = "build"
	door.requireRefused(stale, "PLAN_CHANGED", "")
	old := confirmOf(preview)
	old.ExpectedTerminalSequence--
	if _, refused := door.ask(old); refused.GetReason() != "RUN_CHANGED" {
		t.Fatalf("a confirm of another ending was answered %v, want RUN_CHANGED", refused)
	}
	base, _ := s.current(runID)
	point, err := s.counting().ResumeExecutionHistory(ctx, runID, base)
	must.OKf(t, err, "the base's history")
	scanned, err := engine.ScanResumeBase(point, nil)
	must.OKf(t, err, "scan the base")
	s.resetRun(runID, base, "an operator's reset", scanned.ResetPoint.CompletedEventID)
	s.awaitFailed(runID)
	// Reset by hand, the run has another ending than the one archived, and may
	// end at the sequence the caller saw: what it was shown is not what it is.
	if _, refused := door.ask(confirmOf(preview)); !slices.Contains([]string{"RUN_CHANGED", "PLAN_CHANGED", "STATE_UNREADABLE"}, refused.GetReason()) {
		t.Fatalf("a confirm of a base reset by hand was answered %v, want RUN_CHANGED, PLAN_CHANGED or STATE_UNREADABLE", refused)
	}
	if n := cluster.resets(); n != 0 {
		t.Fatalf("%d resets were sent, want none", n)
	}
	if n := made.of(runID, "deploy", preview.GetSeqBase()); n != 0 {
		t.Fatalf("deploy ran %d times under a plan that was refused", n)
	}
}

// TestIntegration_ResumeTwentyTimes: a run whose last task keeps failing is
// resumed until the cap. Its first task's result is kept throughout, the
// second scope is never the first's, and the resume after the twentieth is
// refused for good. The service's clock is moved past each ending's
// containers' bound and the confirms a run takes a minute.
func TestIntegration_ResumeTwentyTimes(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Minute)
	defer cancel()
	s := newResumeStack(t, ctx, "resume/twenty")
	s.startEngine()
	made := &dispatches{}
	s.startOrker("orker-a", made.noting(s.committing(func(in *orkstackv1.ExecuteTaskInput) error {
		if in.GetTaskId() == "deploy" {
			return temporal.NewNonRetryableApplicationError("deploy failed", "ActivityFailed", nil)
		}
		return nil
	})))
	runID := s.startFailing(s.buildAndDeploy())
	door := s.serveResume(pastContainers)
	scopes := map[int64]bool{}
	for i := range engine.MaxResumesPerRun {
		door.clock.move(time.Minute)
		preview := door.preview(runID)
		if int(preview.GetResumesUsed()) != i || preview.GetKeptCount() != 1 {
			t.Fatalf("preview %d = %d used, %d kept; want %d and 1", i, preview.GetResumesUsed(), preview.GetKeptCount(), i)
		}
		scope := door.confirm(preview).GetSeqBase()
		if scopes[scope] {
			t.Fatalf("resume %d took scope %d again", i, scope)
		}
		scopes[scope] = true
		if status, err := s.wait(runID, 2*time.Minute); status != enums.WORKFLOW_EXECUTION_STATUS_FAILED {
			t.Fatalf("resume %d closed %v: %v", i, status, err)
		}
		if n := made.of(runID, "deploy", scope); n != 1 {
			t.Fatalf("resume %d ran deploy %d times under its scope, want once", i, n)
		}
	}
	if n := made.of(runID, "build", 0); n != 1 {
		t.Fatalf("build ran %d times as submitted, want once", n)
	}
	for scope := range scopes {
		if n := made.of(runID, "build", scope); n != 0 {
			t.Fatalf("build, which was kept, ran %d times under scope %d", n, scope)
		}
	}
	run := s.read(runID)
	s.requireRow(run, "build", orkstackv1.Status_STATUS_SUCCEEDED, "")
	if n := len(s.published(runID, orkstackv1.EventType_EVENT_TYPE_RUN_RESUMED)); n != engine.MaxResumesPerRun {
		t.Fatalf("RUN_RESUMED was published %d times, want %d", n, engine.MaxResumesPerRun)
	}
	// Each resume set an ending aside, and the run still reads as one.
	s.requireWholeStream(runID)
	door.clock.move(time.Minute)
	if r := door.refusedPreview(runID, "TOO_MANY_RESUMES", ""); r.GetClass() != orkstackv1.ResumeRefusalClass_RESUME_REFUSAL_CLASS_PERMANENT {
		t.Fatalf("refusal = %v, want it permanent", r)
	}
}

// TestIntegration_ResumeRefusedWhileItsPoolIsUnserved: a resume whose tasks
// no Orker would run again is refused until one is there, and records nothing.
func TestIntegration_ResumeRefusedWhileItsPoolIsUnserved(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()
	s := newResumeStack(t, ctx, "resume/unserved")
	runID, made := s.failedPair()
	preview := s.serveResume(pastContainers).preview(runID)
	unserved := &poolNobodyServes{}
	door := s.serveResume(pastContainers, func(cfg *api.Config) { cfg.Pools = unserved })
	r := door.refusedPreview(runID, "RERUN_POOL_UNSERVED", "")
	if len(r.GetTasks()) != 1 || r.GetTasks()[0] != "deploy" || r.GetClass() != orkstackv1.ResumeRefusalClass_RESUME_REFUSAL_CLASS_WAIT {
		t.Fatalf("refusal = %v; want it to wait for deploy's pool alone", r)
	}
	door.requireRefused(confirmOf(preview), "RERUN_POOL_UNSERVED", "")
	if plans, err := s.records.ListResumePlans(ctx, "default", runID); err != nil || len(plans) != 0 {
		t.Fatalf("plans = %v, %v; a refused confirm records none", plans, err)
	}
	unserved.serve()
	door.confirm(preview)
	s.requireResumedPair(runID, made, preview.GetSeqBase())
}

// poolNobodyServes answers for a pool whose last Orker left: the cluster
// shows a poller for minutes after one stops, which no test waits out.
type poolNobodyServes struct {
	mu     sync.Mutex
	served bool
}

func (p *poolNobodyServes) serve() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.served = true
}

func (p *poolNobodyServes) ObservePool(ctx context.Context, queue string) (engine.PoolLiveness, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if !p.served {
		return engine.PoolLiveness{}, nil
	}
	return servedPools{}.ObservePool(ctx, queue)
}
