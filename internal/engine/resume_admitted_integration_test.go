//go:build integration

package engine_test

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"

	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/worker"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/api"
	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/internal/identity"
	"github.com/orkstack/orkstack/internal/testsupport/must"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// A resume of a run whose results the engine records (P14r-3; design §28): on
// the proof's own Temporal, with the real ResumeRun, reset and read-back, and
// the records in the stack's bucket. The Orker is in-process: it records each
// result where the engine records an admitted run's, and writes nothing where
// an Orker commits, so a resume that kept anything an Orker wrote would find
// nothing. An answer is recorded as the await door records one, then the run
// is told.

// admittedPlan is the stack's plan of a run whose results the engine records.
func (s *resumeStack) admittedPlan() engine.RunPlan {
	plan := s.resumePlan()
	budget := engine.DefaultRecordBudget
	plan.Identity = &engine.IdentityPlan{AdmissionSHA256: strings.Repeat("a", 64), ValueRecords: 1, RecordBudget: &budget,
		ResolutionsPerPass: engine.DefaultResolutionsPerPass, NonesPerAwait: engine.DefaultNonesPerAwait}
	return plan
}

// recordedResults are what the in-process Orker recorded, by task and scope.
type recordedResults struct {
	mu      sync.Mutex
	results map[string]artifacts.RecordedCommit
}

func (r *recordedResults) of(task string) artifacts.RecordedCommit {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.results[task]
}

// recording is an Orker of an admitted run: each result it is given to make
// is recorded as the engine records one, at the attested twin of its commit
// key, under the run's admission, and nothing is committed where an Orker
// commits. fail answers an error for a task it fails.
func (s *resumeStack) recording(admission string, kept *recordedResults, fail func(*orkstackv1.ExecuteTaskInput) error) chaosBehavior {
	root, err := artifacts.CommitRoot(s.cfg)
	must.OK(s.t, err)
	return func(ctx context.Context, _ *chaosStack, orker string, in *orkstackv1.ExecuteTaskInput) (*orkstackv1.ExecuteTaskResult, error) {
		if fail != nil {
			if err := fail(in); err != nil {
				return nil, err
			}
		}
		raw := `{"ok":true,"task":"` + in.GetTaskId() + `"}`
		ref, err := s.stores.store.Put(ctx, "values.json", strings.NewReader(raw), artifacts.PutOptions{ContentType: "application/json"})
		if err != nil {
			return nil, err
		}
		values := &orkstackv1.ArtifactRef{Name: "values.json", Uri: ref.URI, Sha256: ref.SHA256, SizeBytes: ref.SizeBytes, ContentType: "application/json"}
		key := strings.TrimPrefix(in.GetCommitUri(), strings.TrimSuffix(root, "/")+"/")
		rec, err := s.records.RecordCommit(ctx, key, &orkstackv1.ActivityCommit{
			SchemaVersion: artifacts.CommitSchemaVersion, Namespace: artifacts.ResolveNamespace(in.GetNamespace()),
			RunId: in.GetRunId(), TaskId: in.GetTaskId(), OrkflowDefinitionDigest: in.GetOrkflowDefinitionDigest(),
			IdempotencyKey: in.GetIdempotencyKey(), Image: in.GetImage(), Attempt: 1, OrkerId: orker, Values: values,
			Recorded: &orkstackv1.CommitRecording{AdmissionSha256: admission, OrkerId: orker},
		})
		if err != nil {
			return nil, err
		}
		kept.mu.Lock()
		kept.results[in.GetTaskId()] = rec
		kept.mu.Unlock()
		attested, err := artifacts.AttestedCommitURI(in.GetCommitUri())
		if err != nil {
			return nil, err
		}
		result := succeeded(in.GetTaskId(), map[string]any{"ok": true, "task": in.GetTaskId()})
		result.ValuesJson, result.ValuesRef, result.OrkerId = []byte(raw), values, orker
		result.Commit = &orkstackv1.CommitRef{Uri: attested, Sha256: rec.Record.SHA256}
		return result, nil
	}
}

// approve records an answer to task of runID under scope, as the await door
// records one, following what follows names, and tells the run.
func (s *resumeStack) approve(runID, task string, scope uint64, admission string, follows map[string]artifacts.RecordedCommit) {
	s.t.Helper()
	ctx := s.ctx
	detail, err := s.records.WriteResolutionDetail(ctx, &orkstackv1.ResolutionDetail{SchemaVersion: 1, Namespace: "default", RunId: runID, TaskId: task,
		ResumeScope: scope, Via: orkstackv1.AwaitResolvedVia_AWAIT_RESOLVED_VIA_API, Principal: "alice", PrincipalVerified: true})
	must.OKf(s.t, err, "the answer's detail")
	ref, err := s.stores.store.Put(ctx, "values.json", strings.NewReader(`{"approved":true}`), artifacts.PutOptions{ContentType: "application/json"})
	must.OK(s.t, err)
	rec := &orkstackv1.RecordedValues{SchemaVersion: 1, Kind: orkstackv1.RecordedValuesKind_RECORDED_VALUES_KIND_RESOLUTION, Namespace: "default",
		RunId: runID, TaskId: task, ResumeScope: scope, AdmissionSha256: admission, InputBinding: make([]byte, 32), DetailSha256: detail,
		Values: &orkstackv1.ArtifactRef{Uri: ref.URI, Sha256: ref.SHA256, SizeBytes: ref.SizeBytes}}
	for id, c := range follows {
		result, err := identity.ResultSHA256(c.Record.Commit.GetValues().GetSha256(), nil)
		must.OK(s.t, err)
		rec.Follows = append(rec.Follows, &orkstackv1.RecordedSource{TaskId: id, RecordSha256: c.Record.SHA256, ResultSha256: result})
	}
	_, err = s.records.WriteRecordedValues(ctx, rec)
	must.OKf(s.t, err, "the answer's record")
	must.OKf(s.t, s.starter.SignalAwait(ctx, runID, &orkstackv1.AwaitSignal{TaskId: task}), "tell the run of %s", task)
}

// awaitRow waits until task of runID is at status.
func (s *resumeStack) awaitRow(runID, task string, status orkstackv1.Status) *orkstackv1.TaskRun {
	s.t.Helper()
	var row *orkstackv1.TaskRun
	s.eventually(3*time.Minute, task+" "+status.String(), func() (bool, string) {
		run, err := s.reader.GetRun(s.ctx, runID)
		if err != nil {
			return false, err.Error()
		}
		row = taskRunOfRun(run, task)
		return row.GetStatus() == status, row.GetStatus().String()
	})
	return row
}

// TestIntegration_ResumeAnAdmittedRun: a run whose results the engine records
// fails; a resume that retries from gen keeps build from the engine's record,
// settles approve from its record without asking for it again, opens approve2
// again because what it approved runs again, and runs gen, approve2's answer
// under the resume and deploy again, each at the resume's scope.
func TestIntegration_ResumeAnAdmittedRun(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	s := newResumeStack(t, ctx, "resume/admitted")
	s.startResumingReplica("replica-a", replicaOptions{extra: []func(worker.Worker){func(w worker.Worker) {
		w.RegisterActivityWithOptions(engine.NewResolutionReadActivities(s.records).ReadResolution, activity.RegisterOptions{Name: engine.ActivityReadResolution})
	}}}, resumeFaults{})
	s.startProjection(nil)
	plan := s.admittedPlan()
	admission := plan.Identity.AdmissionSHA256
	kept := &recordedResults{results: map[string]artifacts.RecordedCommit{}}
	made := &dispatches{}
	s.startOrker("orker-a", made.noting(s.recording(admission, kept, failsUntilResumed("deploy"))))

	wf := compile(t, `apiVersion: orkstack/v1
kind: Orkflow
name: resume-admitted
version: "1.0.0"
tasks:
  - id: build
    uses: shell
    with:
      op: exec
  - id: gen
    uses: shell
    with:
      op: exec
  - id: approve
    needs:
      - build
    await:
      timeout_seconds: 600
  - id: approve2
    needs:
      - gen
    await:
      timeout_seconds: 600
  - id: deploy
    needs:
      - approve
      - approve2
    uses: shell
    with:
      op: exec
`)
	wf.Namespace = "default"
	stampDigest(t, wf)
	runID := s.recordedRunID()
	s.start(runID, wf, nil, plan)
	s.awaitRow(runID, "approve", orkstackv1.Status_STATUS_AWAITING)
	s.approve(runID, "approve", 0, admission, map[string]artifacts.RecordedCommit{"build": kept.of("build")})
	s.awaitRow(runID, "approve2", orkstackv1.Status_STATUS_AWAITING)
	s.approve(runID, "approve2", 0, admission, map[string]artifacts.RecordedCommit{"gen": kept.of("gen")})
	s.awaitFailed(runID)

	door := s.serveResume(pastContainers, func(c *api.Config) { c.RecordedResults = s.records })
	preview, refused := door.ask(&orkstackv1.ResumeRunRequest{RunId: runID, DryRun: true, FromTaskId: "gen"})
	if refused != nil {
		t.Fatalf("the preview was refused: %v", refused)
	}
	categories := map[string]orkstackv1.ResumeCategory{}
	for _, task := range preview.GetTasks() {
		categories[task.GetTaskId()] = task.GetCategory()
	}
	if categories["build"] != orkstackv1.ResumeCategory_RESUME_CATEGORY_KEPT || categories["approve"] != orkstackv1.ResumeCategory_RESUME_CATEGORY_SETTLED ||
		categories["approve2"] == orkstackv1.ResumeCategory_RESUME_CATEGORY_SETTLED {
		t.Fatalf("preview = %v; want build kept, approve settled and approve2 asked again", categories)
	}
	confirm := confirmOf(preview)
	confirm.FromTaskId = "gen"
	if _, refused := door.ask(confirm); refused != nil {
		t.Fatalf("the resume was refused: %v", refused)
	}
	scope := uint64(preview.GetSeqBase())
	row := s.awaitRow(runID, "approve2", orkstackv1.Status_STATUS_AWAITING)
	if row.GetResultEpoch() != int64(scope) {
		t.Fatalf("approve2 waits again at scope %d, want %d", row.GetResultEpoch(), scope)
	}
	s.approve(runID, "approve2", scope, admission, map[string]artifacts.RecordedCommit{"gen": kept.of("gen")})
	run := s.awaitSucceeded(runID)
	s.requireResumedOnce(runID, int64(scope))
	for id, again := range map[string]int{"build": 0, "gen": 1, "deploy": 1} {
		s.requireRow(run, id, orkstackv1.Status_STATUS_SUCCEEDED, "")
		if first, n := made.of(runID, id, 0), made.of(runID, id, int64(scope)); first != 1 || n != again {
			t.Errorf("%s ran %d times as submitted and %d under the resume, want 1 and %d", id, first, n, again)
		}
	}
	approve := s.requireRow(run, "approve", orkstackv1.Status_STATUS_SUCCEEDED, "")
	if approve.GetResultEpoch() != 0 || approve.GetAwait().GetResolvedBy() != "alice" {
		t.Errorf("approve = %v; want its first answer kept", approve)
	}
	if again := s.requireRow(run, "approve2", orkstackv1.Status_STATUS_SUCCEEDED, ""); again.GetResultEpoch() != int64(scope) {
		t.Errorf("approve2 = %v; want answered under the resume", again)
	}
	if c := s.requireRow(run, "build", orkstackv1.Status_STATUS_SUCCEEDED, "").GetCommit(); !artifacts.IsAttestedURI(c.GetUri()) {
		t.Errorf("build's commit = %v; want the engine's record", c)
	}
}
