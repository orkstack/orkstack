//go:build integration

package engine_test

import (
	"context"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"go.temporal.io/api/enums/v1"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/worker"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/api"
	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/internal/identity"
	"github.com/orkstack/orkstack/internal/testsupport/must"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// A resume of a run whose results the engine records, whose composed task's
// values were not recorded though its child succeeded (P14r-4; design §28.3,
// decisions P14R4-1 and P14R4-2): on the proof's own Temporal, with the real
// ResumeRun, the real ValuesRecorder, the child's real outputs' record and the
// reset run's real read-back.

// childTree admits a parent of one composed task and its child as a submit
// admits them, and answers what the cluster says of them as the engine's
// identity checks read it: whether each is executing, from the cluster itself.
type childTree struct {
	*engine.TemporalStarter
	s       *resumeStack
	records *artifacts.RecordStore

	mu                 sync.Mutex
	parent, child      string
	parentSHA, ownSHA  string
	parentTasks, tasks []string
}

func (r *childTree) StartRun(ctx context.Context, runID string, wf *orkstackv1.Orkflow, input *orkstackv1.Payload, plan engine.RunPlan) error {
	_, err := r.StartRunExecution(ctx, runID, wf, input, plan)
	return err
}

func (r *childTree) StartRunExecution(ctx context.Context, runID string, wf *orkstackv1.Orkflow, input *orkstackv1.Payload, plan engine.RunPlan) (string, error) {
	sub := plan.Children["sub"]
	base := artifacts.Admission{Namespace: artifacts.DefaultNamespace, Orkflow: wf.GetName(), OrkflowVersion: wf.GetVersion(), DefinitionDigest: strings.Repeat("d", 64),
		Origin: artifacts.AdmittedOrigin{Kind: "submit"}, Nonce: strings.Repeat("0f", 16), AdmittedAt: time.Now().UTC(), EngineBuild: "b",
		SubjectFormat: artifacts.SubjectFormatOrk, SpecVersion: 1, ValueRecords: 1}
	row := artifacts.AdmittedTask{Pool: "main", SpecSHA256: strings.Repeat("a", 64)}
	parent, child := base, base
	parent.RunID, parent.Starter, parent.Children = runID, artifacts.Starter{Kind: artifacts.StarterEngine}, map[string]string{"sub": sub.RunID}
	parent.Tasks = map[string]artifacts.AdmittedTask{}
	for _, id := range r.parentTasks {
		parent.Tasks[id] = row
	}
	child.RunID, child.RootRunID, child.Tasks = sub.RunID, runID, map[string]artifacts.AdmittedTask{}
	for _, id := range r.tasks {
		child.Tasks[id], child.Completes = row, append(child.Completes, id)
	}
	parentSHA, _, err := r.records.WriteAdmission(ctx, parent)
	if err != nil {
		return "", err
	}
	child.Starter = artifacts.Starter{Kind: artifacts.StarterParentRun, RunID: runID, AdmissionSHA256: parentSHA, TaskID: "sub"}
	childSHA, _, err := r.records.WriteAdmission(ctx, child)
	if err != nil {
		return "", err
	}
	budget := engine.DefaultRecordBudget
	recorded := func(sha string) *engine.IdentityPlan {
		return &engine.IdentityPlan{AdmissionSHA256: sha, ValueRecords: 1, RecordBudget: &budget, CommitWaitSeconds: 120,
			ResolutionsPerPass: engine.DefaultResolutionsPerPass, NonesPerAwait: engine.DefaultNonesPerAwait}
	}
	plan.Identity, sub.Plan.Identity = recorded(parentSHA), recorded(childSHA)
	r.mu.Lock()
	r.parent, r.child, r.parentSHA, r.ownSHA = runID, sub.RunID, parentSHA, childSHA
	r.mu.Unlock()
	return r.TemporalStarter.StartRunExecution(ctx, runID, wf, input, plan)
}

// facts is what the cluster says of run: executing or not as the cluster
// itself answers, under the ids its admissions' chain names.
func (r *childTree) facts(ctx context.Context, run string) (identity.ExecutionFacts, error) {
	described, err := describeExecution(r.s.control, ctx, run, "")
	if err != nil {
		return identity.ExecutionFacts{}, err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	facts := identity.ExecutionFacts{WorkflowID: run, Open: described.GetWorkflowExecutionInfo().GetStatus() == enums.WORKFLOW_EXECUTION_STATUS_RUNNING,
		ExecutionID: "e-" + run, FirstExecutionID: "e-" + run}
	if run == r.child {
		facts.Parent = &identity.ExecutionRef{WorkflowID: r.parent, ExecutionID: "e-" + r.parent}
	}
	return facts, nil
}

func (r *childTree) Describe(ctx context.Context, run, _ string) (identity.ExecutionFacts, error) {
	return r.facts(ctx, run)
}

func (r *childTree) ReadAdmission(ctx context.Context, ns, run, sha string) (artifacts.RecordRead[artifacts.Admission], error) {
	return r.records.ReadAdmission(ctx, ns, run, sha)
}

func (r *childTree) ReadChain(context.Context, string, string) (artifacts.RecordRead[artifacts.Chain], error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return artifacts.RecordRead[artifacts.Chain]{Answer: artifacts.RecordFound,
		Value: artifacts.Chain{FirstExecutionID: "e-" + r.parent, AdmissionSHA256: r.parentSHA}}, nil
}

func (r *childTree) CompleteChain(context.Context, string, string, artifacts.Chain) error { return nil }

func (r *childTree) FirstStartAdmission(context.Context, string, string) (string, error) {
	return "", nil
}

// refusingComposed is the real recorder, whose composed tasks' records are
// refused while refusing is set: the one fault no real store produces on
// demand for one record (AGENTS.md §11.1). Outputs are recorded as they are.
type refusingComposed struct {
	*identity.ValuesRecorder
	refusing atomic.Bool
}

func (r *refusingComposed) RecordComposed(ctx context.Context, q identity.ComposedAsk) (identity.RecordedOutcome, error) {
	if r.refusing.Load() {
		return identity.RecordedOutcome{}, &identity.ValuesRefusal{Reason: identity.ReasonChildValuesNotRecorded, Message: "refused by the test"}
	}
	return r.ValuesRecorder.RecordComposed(ctx, q)
}

// recordingByAdmission is an Orker of a recorded tree: each result is recorded
// as the engine records one, at the attested twin of its commit key, under the
// admission of the run the attempt is of. fail answers an error for a task it
// fails.
func (s *resumeStack) recordingByAdmission(made *dispatches, fail func(*orkstackv1.ExecuteTaskInput) error) chaosBehavior {
	root, err := artifacts.CommitRoot(s.cfg)
	must.OK(s.t, err)
	return made.noting(func(ctx context.Context, _ *chaosStack, orker string, in *orkstackv1.ExecuteTaskInput) (*orkstackv1.ExecuteTaskResult, error) {
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
		admission := in.GetIdentity().GetAdmissionSha256()
		rec, err := s.records.RecordCommit(ctx, key, &orkstackv1.ActivityCommit{
			SchemaVersion: artifacts.CommitSchemaVersion, Namespace: artifacts.ResolveNamespace(in.GetNamespace()),
			RunId: in.GetRunId(), TaskId: in.GetTaskId(), OrkflowDefinitionDigest: in.GetOrkflowDefinitionDigest(),
			IdempotencyKey: in.GetIdempotencyKey(), Image: in.GetImage(), Attempt: 1, OrkerId: orker, Values: values,
			Recorded: &orkstackv1.CommitRecording{AdmissionSha256: admission, OrkerId: orker},
		})
		if err != nil {
			return nil, err
		}
		attested, err := artifacts.AttestedCommitURI(in.GetCommitUri())
		if err != nil {
			return nil, err
		}
		result := succeeded(in.GetTaskId(), map[string]any{"ok": true, "task": in.GetTaskId()})
		result.ValuesJson, result.ValuesRef, result.OrkerId = []byte(raw), values, orker
		result.Commit = &orkstackv1.CommitRef{Uri: attested, Sha256: rec.Record.SHA256}
		return result, nil
	})
}

// TestIntegration_ResumeAnAdmittedRunSettlesItsChild: a run whose results the
// engine records fails because its composed task's values could not be
// recorded, though its child succeeded and its outputs are on record, and
// because the task that runs once it ends, whatever its end, failed too. A
// resume keeps the composed task, recording
// its values from the child's outputs at confirm though the run has ended,
// runs the failed task again and nothing else, and the run succeeds with the
// composed task as its record says, its child run once.
func TestIntegration_ResumeAnAdmittedRunSettlesItsChild(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	s := newResumeStack(t, ctx, "resume/admitted-child")
	compose := engine.NewComposeActivities(s.stores.store)
	tree := &childTree{TemporalStarter: s.starter, s: s, records: s.records, parentTasks: []string{"first", "after"}, tasks: []string{"t0"}}
	rec := &refusingComposed{ValuesRecorder: &identity.ValuesRecorder{Records: s.records, Starts: tree, EngineBuild: "b", Now: time.Now,
		Describe: func(ctx context.Context, run string) (identity.ExecutionFacts, error) { return tree.facts(ctx, run) },
		Definition: func(context.Context, artifacts.Admission) (*orkstackv1.Orkflow, error) {
			return &orkstackv1.Orkflow{Name: "little"}, nil
		},
		Render: compose.RenderOutputs, Store: compose.StoreOutputs}}
	rec.refusing.Store(true)
	s.startResumingReplica("replica-a", replicaOptions{extra: []func(worker.Worker){func(w worker.Worker) {
		values := engine.NewValueRecordActivities(rec)
		w.RegisterActivityWithOptions(values.RecordOutputs, activity.RegisterOptions{Name: engine.ActivityRecordOutputs})
		w.RegisterActivityWithOptions(values.RecordComposed, activity.RegisterOptions{Name: engine.ActivityRecordComposed})
	}}}, resumeFaults{})
	s.startProjection(nil)
	made := &dispatches{}
	s.startOrker("orker-a", s.recordingByAdmission(made, failsUntilResumed("after")))

	child := compile(t, `apiVersion: orkstack/v1
kind: Orkflow
name: little
version: "1.0.0"
tasks:
  - id: t0
    uses: shell
    with:
      op: exec
`)
	child.Namespace = "default"
	stampDigest(t, child)
	parent := compile(t, `apiVersion: orkstack/v1
kind: Orkflow
name: resume-admitted-child
version: "1.0.0"
tasks:
  - id: first
    uses: shell
    with:
      op: exec
  - id: sub
    needs:
      - first
    uses: orkflow:little@1.0.0
  - id: after
    needs:
      - sub
    join: all_done
    uses: shell
    with:
      op: exec
`)
	parent.Namespace = "default"
	for _, task := range parent.GetTasks() {
		if task.GetOrkflow() != nil {
			task.Orkflow.DefinitionDigest = child.GetDefinitionDigest()
		}
	}
	stampDigest(t, parent)
	runID := s.recordedRunID()
	plan := s.admittedPlan()
	cp, err := engine.NewChildRunPlan(child, s.resumePlan(), false, false)
	must.OK(t, err)
	cp.RunID = runID + "-child"
	plan.Children = map[string]*engine.ChildRunPlan{"sub": cp}
	must.OKf(t, tree.StartRun(ctx, runID, parent, nil, plan), "StartRun")
	s.awaitFailed(runID)
	failed := s.read(runID)
	s.requireRow(failed, "sub", orkstackv1.Status_STATUS_FAILED, engine.ChildValuesNotRecordedCode)
	if c := s.awaitSucceeded(cp.RunID); c.GetStatus() != orkstackv1.Status_STATUS_SUCCEEDED {
		t.Fatalf("the child %v", c.GetStatus())
	}
	rec.refusing.Store(false)

	door := s.serveResume(pastContainers, func(c *api.Config) { c.RecordedResults, c.ComposedRecords = s.records, rec })
	preview, refused := door.ask(&orkstackv1.ResumeRunRequest{RunId: runID, DryRun: true})
	if refused != nil {
		t.Fatalf("the preview was refused: %v", refused)
	}
	categories := map[string]orkstackv1.ResumeCategory{}
	for _, task := range preview.GetTasks() {
		categories[task.GetTaskId()] = task.GetCategory()
	}
	if categories["first"] != orkstackv1.ResumeCategory_RESUME_CATEGORY_KEPT || categories["sub"] != orkstackv1.ResumeCategory_RESUME_CATEGORY_SETTLED ||
		categories["after"] == orkstackv1.ResumeCategory_RESUME_CATEGORY_KEPT {
		t.Fatalf("preview = %v; want first kept, sub settled from its child and after run again", categories)
	}
	key, err := artifacts.RecordKey(artifacts.RecordComposed, artifacts.DefaultNamespace, runID, "sub", 0)
	must.OK(t, err)
	want := artifacts.RecordWant{Kind: orkstackv1.RecordedValuesKind_RECORDED_VALUES_KIND_COMPOSED, Namespace: artifacts.DefaultNamespace,
		RunID: runID, TaskID: "sub", AdmissionSHA256: tree.parentSHA}
	if read, err := s.records.ReadRecordedValues(ctx, artifacts.SlotRun, nil, key, want); err != nil || read.Answer != artifacts.RecordNone {
		t.Fatalf("before the confirm, sub's record: %v, %v; want none", read.Answer, err)
	}
	if _, refused := door.ask(confirmOf(preview)); refused != nil {
		t.Fatalf("the resume was refused: %v", refused)
	}
	scope := uint64(preview.GetSeqBase())
	run := s.awaitSucceeded(runID)
	s.requireResumedOnce(runID, int64(scope))
	read, err := s.records.ReadRecordedValues(ctx, artifacts.SlotRun, nil, key, want)
	if err != nil || read.Answer != artifacts.RecordFound {
		t.Fatalf("sub's record: %v, %v", read.Answer, err)
	}
	if got, _, _ := read.Value.Child(); got != cp.RunID {
		t.Errorf("sub's record names child %q, want %q", got, cp.RunID)
	}
	sub := s.requireRow(run, "sub", orkstackv1.Status_STATUS_SUCCEEDED, "")
	if sub.GetChild().GetRunId() != cp.RunID || sub.GetValuesRef().GetSha256() != read.Value.ValuesRef().GetSha256() || sub.GetResultEpoch() != 0 {
		t.Errorf("sub = %v; want kept as its record says", sub)
	}
	for id, again := range map[string]int{"first": 0, "after": 1} {
		s.requireRow(run, id, orkstackv1.Status_STATUS_SUCCEEDED, "")
		if first, n := made.of(runID, id, 0), made.of(runID, id, int64(scope)); first != 1 || n != again {
			t.Errorf("%s ran %d times as submitted and %d under the resume, want 1 and %d", id, first, n, again)
		}
	}
	if n := made.of(cp.RunID, "t0", 0); n != 1 {
		t.Errorf("the child's task ran %d times, want once", n)
	}
}
