//go:build integration

package engine_test

import (
	"bytes"
	"context"
	"fmt"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"go.temporal.io/api/enums/v1"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/worker"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/api"
	"github.com/orkstack/orkstack/internal/dsl"
	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/internal/identity"
	"github.com/orkstack/orkstack/internal/testsupport/must"
	"github.com/orkstack/orkstack/internal/testsupport/s3bucket"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// recordedTree makes the runs of one submission a tree whose results the engine
// records: it admits them as they start, and each task's result is put on record
// as the engine puts it. The admissions and the records are the real ones; who
// holds an attempt, which the suites of a commit prove, is not asked.
type recordedTree struct {
	*engine.TemporalStarter
	t   *testing.T
	ctx context.Context
	raw *s3.Client
	cfg artifacts.Config
	n   int

	mu                sync.Mutex
	parent, child     string
	parentSHA, ownSHA string
	records           *artifacts.RecordStore
}

func (r *recordedTree) put(key string, body []byte) error {
	_, err := r.raw.PutObject(r.ctx, &s3.PutObjectInput{Bucket: aws.String(r.cfg.S3Bucket), Key: aws.String(key), Body: bytes.NewReader(body)})
	return err
}

// admit writes a's record, as a submit does before it starts a run.
func (r *recordedTree) admit(a artifacts.Admission) (string, error) {
	body, sha, err := artifacts.EncodeAdmission(a)
	if err != nil {
		return "", err
	}
	key, err := artifacts.AdmissionKey(a.Namespace, a.RunID, sha)
	if err != nil {
		return "", err
	}
	return sha, r.put(key, body)
}

func (r *recordedTree) StartRun(ctx context.Context, runID string, wf *orkstackv1.Orkflow, input *orkstackv1.Payload, plan engine.RunPlan) error {
	_, err := r.StartRunExecution(ctx, runID, wf, input, plan)
	return err
}

func (r *recordedTree) StartRunExecution(ctx context.Context, runID string, wf *orkstackv1.Orkflow, input *orkstackv1.Payload, plan engine.RunPlan) (string, error) {
	sub := plan.Children["sub"]
	base := artifacts.Admission{Namespace: artifacts.DefaultNamespace, Orkflow: "o", OrkflowVersion: "1", DefinitionDigest: strings.Repeat("d", 64),
		Origin: artifacts.AdmittedOrigin{Kind: "submit"}, Nonce: strings.Repeat("0f", 16), AdmittedAt: time.Now().UTC(), EngineBuild: "b",
		SubjectFormat: artifacts.SubjectFormatOrk, SpecVersion: 1, ValueRecords: 1}
	row := artifacts.AdmittedTask{Pool: "main", SpecSHA256: strings.Repeat("a", 64)}
	parent, child := base, base
	parent.RunID, parent.Starter, parent.Children = runID, artifacts.Starter{Kind: artifacts.StarterEngine}, map[string]string{"sub": sub.RunID}
	parent.Tasks = map[string]artifacts.AdmittedTask{"first": row}
	child.RunID, child.RootRunID, child.Tasks = sub.RunID, runID, map[string]artifacts.AdmittedTask{}
	for _, id := range recordedChildIDs() {
		child.Tasks[id], child.Completes = row, append(child.Completes, id)
	}
	parentSHA, err := r.admit(parent)
	if err != nil {
		return "", err
	}
	child.Starter = artifacts.Starter{Kind: artifacts.StarterParentRun, RunID: runID, AdmissionSHA256: parentSHA, TaskID: "sub"}
	childSHA, err := r.admit(child)
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

// recorded is behavior with its result put on record, as the engine's answer to
// the Orker's commit leaves it.
func (r *recordedTree) recorded(behavior chaosBehavior) chaosBehavior {
	return func(ctx context.Context, s *chaosStack, orker string, in *orkstackv1.ExecuteTaskInput) (*orkstackv1.ExecuteTaskResult, error) {
		got, err := behavior(ctx, s, orker, in)
		if err != nil {
			return nil, err
		}
		key, err := artifacts.CommitKey(artifacts.DefaultNamespace, in.GetRunId(), in.GetTaskId())
		if err != nil {
			return nil, err
		}
		if key, err = artifacts.AttestedCommitKey(key); err != nil {
			return nil, err
		}
		doc, err := artifacts.MarshalCommit(&orkstackv1.ActivityCommit{SchemaVersion: artifacts.CommitSchemaVersion, Namespace: artifacts.DefaultNamespace,
			RunId: in.GetRunId(), TaskId: in.GetTaskId(), Values: &orkstackv1.ArtifactRef{Uri: "s3://none/values", Sha256: strings.Repeat("c", 64), SizeBytes: 2},
			Recorded: &orkstackv1.CommitRecording{AdmissionSha256: in.GetIdentity().GetAdmissionSha256()}})
		if err != nil {
			return nil, err
		}
		return got, r.put(key, doc)
	}
}

// The cluster's facts of the tree, as the engine reads them.
func (r *recordedTree) facts(run string) identity.ExecutionFacts {
	r.mu.Lock()
	defer r.mu.Unlock()
	facts := identity.ExecutionFacts{WorkflowID: run, Open: true, ExecutionID: "e-" + run, FirstExecutionID: "e-" + run}
	if run == r.child {
		facts.Parent = &identity.ExecutionRef{WorkflowID: r.parent, ExecutionID: "e-" + r.parent}
	}
	return facts
}

func (r *recordedTree) Describe(_ context.Context, run, _ string) (identity.ExecutionFacts, error) {
	return r.facts(run), nil
}

func (r *recordedTree) ReadAdmission(ctx context.Context, ns, run, sha string) (artifacts.RecordRead[artifacts.Admission], error) {
	return r.records.ReadAdmission(ctx, ns, run, sha)
}

func (r *recordedTree) ReadChain(context.Context, string, string) (artifacts.RecordRead[artifacts.Chain], error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return artifacts.RecordRead[artifacts.Chain]{Answer: artifacts.RecordFound,
		Value: artifacts.Chain{FirstExecutionID: "e-" + r.parent, AdmissionSHA256: r.parentSHA}}, nil
}

func (r *recordedTree) CompleteChain(context.Context, string, string, artifacts.Chain) error {
	return nil
}

func (r *recordedTree) FirstStartAdmission(context.Context, string, string) (string, error) {
	return "", nil
}

// recordedChildStages and recordedChildWidth shape the child of
// TestIntegration_ARecordedChild: stages of plain tasks of a first-party
// image, each closed by one task that needs them all, so that the run has a
// moment with nothing in flight between two stages, where it can carry on. 300
// tasks is well within what one run tree's plan carries of such a child
// (docs/DSL.md, "How many tasks an Orkflow may declare").
const (
	recordedChildStages = 10
	recordedChildWidth  = 29
	recordedChildTasks  = recordedChildStages * (recordedChildWidth + 1)
)

// recordedChildEvents is its continuation budget, which a few stages cross.
const recordedChildEvents = 1500

// recordedChildIDs are the staged child's tasks, in its document's order.
func recordedChildIDs() []string {
	var ids []string
	for stage := range recordedChildStages {
		for i := range recordedChildWidth {
			ids = append(ids, fmt.Sprintf("t%d", stage*recordedChildWidth+i))
		}
		ids = append(ids, fmt.Sprintf("s%d", stage))
	}
	return ids
}

// recordedChildDocument is the staged child.
func recordedChildDocument(name string) string {
	var b strings.Builder
	fmt.Fprintf(&b, "apiVersion: orkstack/v1\nkind: Orkflow\nname: %s\nversion: \"1.0.0\"\ntasks:\n", name)
	task := func(id string, needs []string) {
		fmt.Fprintf(&b, "  - id: %s\n    uses: shell\n    network: none\n    timeout_seconds: 600\n    retry: {maximum_attempts: 3}\n", id)
		if len(needs) > 0 {
			fmt.Fprintf(&b, "    needs: [%s]\n", strings.Join(needs, ", "))
		}
		b.WriteString("    with: {op: exec, command: [echo, ok]}\n")
	}
	var barrier []string
	for stage := range recordedChildStages {
		var work []string
		for i := range recordedChildWidth {
			id := fmt.Sprintf("t%d", stage*recordedChildWidth+i)
			task(id, barrier)
			work = append(work, id)
		}
		end := fmt.Sprintf("s%d", stage)
		task(end, work)
		barrier = []string{end}
	}
	return b.String()
}

// TestIntegration_ARecordedChild: a child run of a tree whose results the
// engine records carries on as any run does, in two generations or more, with
// no refusal at the cluster's limits, no deadlock trip and each generation
// replaying; every task runs once, the child's end and the composed task that
// takes it are recorded, and the composed task holds its record.
func TestIntegration_ARecordedChild(t *testing.T) {
	const n = recordedChildTasks
	ctx, cancel := context.WithTimeout(context.Background(), taskSoakShapeTimeout)
	defer cancel()
	holdLock(t, ctx, taskSoakLock())
	var shape taskSoakShape
	for _, candidate := range taskSoakShapes {
		if candidate.name == "flat-short" {
			shape = candidate
		}
	}
	// The engine's side, on in-process Orkers: a real one asks an engine for
	// every task of such a run (the suites of cmd/orkctl run one).
	shape.real = false
	var tree *recordedTree
	e, _ := startTaskSoakWith(t, ctx, shape, n, func(s *chaosStack) soakSeams {
		raw, err := s3bucket.Client(ctx, s.cfg)
		must.OK(t, err)
		records, err := artifacts.NewRecordStore(ctx, s.cfg)
		must.OK(t, err)
		store, err := artifacts.New(s.cfg)
		must.OK(t, err)
		tree = &recordedTree{TemporalStarter: s.starter, t: t, ctx: ctx, raw: raw, cfg: s.cfg, n: n, records: records}
		compose := engine.NewComposeActivities(store)
		rec := &identity.ValuesRecorder{Records: records, Starts: tree, EngineBuild: "b", Now: time.Now,
			Describe: func(_ context.Context, run string) (identity.ExecutionFacts, error) { return tree.facts(run), nil },
			Definition: func(context.Context, artifacts.Admission) (*orkstackv1.Orkflow, error) {
				return &orkstackv1.Orkflow{Name: "o"}, nil
			},
			Render: compose.RenderOutputs, Store: compose.StoreOutputs}
		return soakSeams{starter: tree, behavior: tree.recorded, api: func(c *api.Config) { c.ContinuationEvents = recordedChildEvents }, register: func(w worker.Worker) {
			values := engine.NewValueRecordActivities(rec)
			w.RegisterActivityWithOptions(values.RecordOutputs, activity.RegisterOptions{Name: engine.ActivityRecordOutputs})
			w.RegisterActivityWithOptions(values.RecordComposed, activity.RegisterOptions{Name: engine.ActivityRecordComposed})
		}}
	})
	doc := recordedChildDocument("soak-recorded")
	compiled, err := dsl.Compile([]byte(doc))
	must.OK(t, err)
	var ids []string
	for _, task := range compiled.GetTasks() {
		ids = append(ids, task.GetId())
	}
	if !slices.Equal(ids, recordedChildIDs()) {
		t.Fatalf("the child declares %v; its admission names %v", ids, recordedChildIDs())
	}
	e.api.apply(t, ctx, doc)
	e.api.apply(t, ctx, `apiVersion: orkstack/v1
kind: Orkflow
name: tree
version: "1.0.0"
tasks:
  - id: first
    uses: shell
    network: none
    with:
      op: exec
      command:
        - echo
        - ok
  - id: sub
    uses: orkflow:soak-recorded@1.0.0
    needs:
      - first
`)
	started := time.Now()
	runID := e.api.submit(t, ctx, "tree").GetRun().GetId()
	e.transcript.run(runID)
	if status, err := e.wait(runID, taskSoakRunWithin); status != enums.WORKFLOW_EXECUTION_STATUS_COMPLETED || err != nil {
		t.Fatalf("the tree closed %v: %v", status, err)
	}
	run, err := e.reader.GetRun(ctx, runID)
	must.OK(t, err)
	var sub *orkstackv1.TaskRun
	for _, tr := range run.GetTaskRuns() {
		if tr.GetTaskId() == "sub" {
			sub = tr
		}
	}
	if run.GetStatus() != orkstackv1.Status_STATUS_SUCCEEDED || sub.GetStatus() != orkstackv1.Status_STATUS_SUCCEEDED || sub.GetRecord().GetSha256() == "" {
		t.Fatalf("the tree %v, its composed task %v with record %v", run.GetStatus(), sub.GetStatus(), sub.GetRecord())
	}
	child, err := e.reader.GetRun(ctx, tree.child)
	must.OK(t, err)
	succeeded := 0
	for _, tr := range child.GetTaskRuns() {
		if tr.GetStatus() == orkstackv1.Status_STATUS_SUCCEEDED {
			succeeded++
		}
	}
	if child.GetStatus() != orkstackv1.Status_STATUS_SUCCEEDED || succeeded != n {
		t.Fatalf("the child %v with %d of %d tasks SUCCEEDED", child.GetStatus(), succeeded, n)
	}
	for key, what := range map[string]func() (string, error){
		"the child's outputs": func() (string, error) { return artifacts.OutputsRecordKey(artifacts.DefaultNamespace, tree.child) },
		"the composed task": func() (string, error) {
			return artifacts.RecordKey(artifacts.RecordComposed, artifacts.DefaultNamespace, runID, "sub", 0)
		}} {
		at, err := what()
		must.OK(t, err)
		if _, err := tree.raw.HeadObject(ctx, &s3.HeadObjectInput{Bucket: aws.String(tree.cfg.S3Bucket), Key: aws.String(at)}); err != nil {
			t.Errorf("%s: no record at %s: %v", key, at, err)
		}
	}

	width := int64(engine.DefaultRunMaxInFlight)
	chain, histories := e.histories(tree.child)
	if len(chain) < 2 {
		t.Errorf("the child ran in %d generations; it must carry on at least once", len(chain))
	}
	var plan engine.RunPlan
	must.OK(t, converter.GetDefaultDataConverter().FromPayload(histories[0].GetEvents()[0].GetWorkflowExecutionStartedEventAttributes().GetInput().GetPayloads()[3], &plan))
	if plan.SnapshotOverlay == nil || plan.Identity == nil || plan.Identity.ValueRecords != 1 {
		t.Errorf("the child's plan: overlays %t, identity %+v", plan.SnapshotOverlay != nil, plan.Identity)
	}
	before := 0
	for i, h := range histories {
		_, peak := pendingPeaks(h)
		limit, _, deadlocks, _ := soakTaskFailures(h)
		before += analyzeClosedForm(t, h, n, before, width).Dispatches
		if peak > int(engine.DefaultRunMaxInFlight) || limit != 0 || deadlocks != 0 {
			t.Errorf("generation %d: %d pending at most, %d refusals at the cluster's limit, %d deadlock trips", i, peak, limit, deadlocks)
		}
		must.OKf(t, replayRecorded(h, tree.child), "generation %d does not replay", i)
	}
	if before != n {
		t.Errorf("%d tasks dispatched across the generations, want %d", before, n)
	}
	t.Logf("EVIDENCE a recorded child: %d tasks in %d generations, %s", n, len(chain), time.Since(started).Round(time.Second))
}
