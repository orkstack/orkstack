package api_test

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/api"
	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/internal/identity"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// childRecorder answers RecordComposed as it is told, and keeps each ask. It
// stands in for identity.ValuesRecorder, whose own steps its tests and the
// integration test prove; here only what ResumeRun asks of it is held.
type childRecorder struct {
	mu   sync.Mutex
	asks []identity.ComposedAsk
	err  error
}

func (r *childRecorder) RecordComposed(_ context.Context, q identity.ComposedAsk) (identity.RecordedOutcome, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.asks = append(r.asks, q)
	return identity.RecordedOutcome{}, r.err
}

const resumeChildAdmission = "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"

// childFixture is an admitted run whose composed task c failed because its
// values could not be recorded, while its child ork-child ended as the cluster
// says, with its outputs on record when outputs is set.
func childFixture(t *testing.T, child enumspb.WorkflowExecutionStatus, outputs bool, rec *childRecorder, ended ...orkstackv1.Status) *resumeFixture {
	t.Helper()
	row := childRow(stFail)
	row.Error = &orkstackv1.Error{Code: engine.ChildValuesUnstoredCode}
	row.Child.RunId = "ork-forged"
	f, _ := composedFixture(t, composedShape{tasks: []*orkstackv1.Task{childTask("c"), imageTask("x")}, rows: []*orkstackv1.TaskRun{row, rowOf("x", stFail)},
		change: func(t *testing.T, f *resumeFixture) {
			f.plan.Children = map[string]*engine.ChildRunPlan{"c": {RunID: "ork-child", Plan: engine.RunPlan{Identity: &engine.IdentityPlan{AdmissionSHA256: resumeChildAdmission}}}}
			f.cluster.children = map[string]engine.ResumeExecution{"ork-child": {Status: child, StartTime: resumeEpoch, CloseTime: resumeEpoch.Add(time.Minute)}}
			if outputs {
				ref, err := f.store.Put(context.Background(), "values.json", strings.NewReader(`{"d":1}`), artifacts.PutOptions{})
				if err != nil {
					t.Fatal(err)
				}
				out := &orkstackv1.RecordedValues{SchemaVersion: 1, Kind: orkstackv1.RecordedValuesKind_RECORDED_VALUES_KIND_OUTPUTS, Namespace: "default",
					RunId: "ork-child", AdmissionSha256: resumeChildAdmission, InputBinding: make([]byte, 32),
					Values: &orkstackv1.ArtifactRef{Uri: ref.URI, Sha256: ref.SHA256, SizeBytes: ref.SizeBytes}}
				if _, err := f.recordStore.WriteRecordedValues(context.Background(), out); err != nil {
					t.Fatal(err)
				}
			}
			f.configure = func(cfg *api.Config) { cfg.ComposedRecords = rec }
		}}, true)
	f.resets(true)
	status := orkstackv1.Status_STATUS_SUCCEEDED
	if len(ended) > 0 {
		status = ended[0]
	}
	f.runReader.runs["ork-child"] = &orkstackv1.Run{Id: "ork-child", Status: status}
	return f
}

// TestResumeRun_SettlesAComposedTaskFromItsChild: a composed task of an
// admitted run whose values were not recorded is settled from its child's
// outputs on record once that child succeeded, and its values are recorded
// before the reset, by the steps any composed task's are; a preview records
// nothing. A child still open waits with advice, one that did not succeed or
// left no outputs refuses, and a record refused refuses.
func TestResumeRun_SettlesAComposedTaskFromItsChild(t *testing.T) {
	t.Run("settled and recorded", func(t *testing.T) {
		rec := &childRecorder{}
		f := childFixture(t, enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED, true, rec)
		p := f.preview(t)
		if c := p.GetTasks()[0]; c.GetTaskId() != "c" || c.GetCategory() != orkstackv1.ResumeCategory_RESUME_CATEGORY_SETTLED {
			t.Fatalf("preview = %v, want c settled", p)
		}
		if len(rec.asks) != 0 {
			t.Fatalf("a preview recorded %v", rec.asks)
		}
		if _, err := f.confirm(t, nil); err != nil || len(f.cluster.resets) != 1 {
			t.Fatalf("confirm = %v, %d resets", err, len(f.cluster.resets))
		}
		if len(rec.asks) != 1 {
			t.Fatalf("asks = %v, want one", rec.asks)
		}
		q := rec.asks[0]
		if q.TaskID != "c" || q.RunID != "ork-r" || q.ChildAdmissionSHA256 != resumeChildAdmission || q.AdmissionSHA256 != resumeAdmission ||
			len(q.ValuesSHA256) != 64 || q.Scopes.Resume != 0 {
			t.Errorf("ask = %+v", q)
		}
	})
	for name, tc := range map[string]struct {
		child         enumspb.WorkflowExecutionStatus
		outputs       bool
		recordErr     error
		reason, cause string
		ended         []orkstackv1.Status
	}{
		"its child is still open":   {enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING, true, nil, "STILL_STOPPING", "child_running", nil},
		"its child failed":          {enumspb.WORKFLOW_EXECUTION_STATUS_FAILED, true, nil, "COMPOSED_TASK", "child_not_succeeded", nil},
		"no outputs on record":      {enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED, false, nil, "COMPOSED_TASK", "child_outputs_unavailable", nil},
		"its record is refused":     {enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED, true, &identity.ValuesRefusal{Reason: "CHILD_VALUES_NOT_RECORDED"}, "COMPOSED_TASK", "child_outputs_unavailable", nil},
		"its record is not written": {enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED, true, errors.New("no answer"), "RESUME_CHECK_UNAVAILABLE", "", nil},
		"its execution completed and its run failed": {enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED, true, nil, "COMPOSED_TASK", "child_not_succeeded",
			[]orkstackv1.Status{orkstackv1.Status_STATUS_FAILED}},
	} {
		t.Run(name, func(t *testing.T) {
			rec := &childRecorder{err: tc.recordErr}
			f := childFixture(t, tc.child, tc.outputs, rec, tc.ended...)
			_, err := f.confirm(t, nil)
			r := refusalOf(t, err)
			if r.GetReason() != tc.reason || r.GetCause() != tc.cause || len(f.cluster.resets) != 0 {
				t.Fatalf("refusal = %v, want %s %s and no reset", r, tc.reason, tc.cause)
			}
			if tc.cause == "child_running" && (!strings.Contains(r.GetMessage(), "orkctl run cancel ork-child") || r.GetRetryAfterSeconds() <= 0) {
				t.Errorf("message = %q", r.GetMessage())
			}
		})
	}
}

// clusterFacts stands in for what the cluster says of the run and its child,
// the one part of identity.ValuesRecorder no test here can hold for real; the
// admissions and records it reads are the store's.
type clusterFacts struct {
	records *artifacts.RecordStore
	parent  string
	open    bool
}

func (c *clusterFacts) facts(run string) identity.ExecutionFacts {
	f := identity.ExecutionFacts{WorkflowID: run, Open: c.open, ExecutionID: "e-" + run, FirstExecutionID: "e-" + run}
	if run == "ork-child" {
		f.Open, f.Parent = false, &identity.ExecutionRef{WorkflowID: "ork-r", ExecutionID: "e-ork-r"}
	}
	return f
}

func (c *clusterFacts) Describe(_ context.Context, run, _ string) (identity.ExecutionFacts, error) {
	return c.facts(run), nil
}

func (c *clusterFacts) ReadAdmission(ctx context.Context, ns, run, sha string) (artifacts.RecordRead[artifacts.Admission], error) {
	return c.records.ReadAdmission(ctx, ns, run, sha)
}

func (c *clusterFacts) ReadChain(context.Context, string, string) (artifacts.RecordRead[artifacts.Chain], error) {
	return artifacts.RecordRead[artifacts.Chain]{Answer: artifacts.RecordFound, Value: artifacts.Chain{FirstExecutionID: "e-ork-r", AdmissionSHA256: c.parent}}, nil
}

func (c *clusterFacts) CompleteChain(context.Context, string, string, artifacts.Chain) error {
	return nil
}

func (c *clusterFacts) FirstStartAdmission(context.Context, string, string) (string, error) {
	return "", nil
}

// TestResumeRun_RecordsAComposedTaskFromItsChildForReal: the confirm records
// the composed task's values with the real recorder, from the admissions and
// the child's outputs' record in the store, though the run has ended; the
// record is the child's outputs and names its child.
func TestResumeRun_RecordsAComposedTaskFromItsChildForReal(t *testing.T) {
	ctx := context.Background()
	facts := &clusterFacts{}
	row := childRow(stFail)
	row.Error = &orkstackv1.Error{Code: engine.ChildValuesNotRecordedCode}
	f, _ := composedFixture(t, composedShape{tasks: []*orkstackv1.Task{childTask("c"), imageTask("x")}, rows: []*orkstackv1.TaskRun{row, rowOf("x", stFail)},
		change: func(t *testing.T, f *resumeFixture) {
			facts.records = f.recordStore
			base := artifacts.Admission{Namespace: "default", Orkflow: "wf", OrkflowVersion: "1", DefinitionDigest: strings.Repeat("d", 64),
				Origin: artifacts.AdmittedOrigin{Kind: "submit"}, Nonce: strings.Repeat("0f", 16), AdmittedAt: resumeEpoch, EngineBuild: "b",
				SubjectFormat: artifacts.SubjectFormatOrk, SpecVersion: 1, ValueRecords: 1}
			parent, child := base, base
			parent.RunID, parent.Starter, parent.Children = "ork-r", artifacts.Starter{Kind: artifacts.StarterEngine}, map[string]string{"c": "ork-child"}
			parent.Tasks = map[string]artifacts.AdmittedTask{"x": {Pool: "main", SpecSHA256: strings.Repeat("a", 64)}}
			parentSHA, _, err := f.recordStore.WriteAdmission(ctx, parent)
			if err != nil {
				t.Fatal(err)
			}
			child.RunID, child.RootRunID, child.Tasks = "ork-child", "ork-r", map[string]artifacts.AdmittedTask{}
			child.Starter = artifacts.Starter{Kind: artifacts.StarterParentRun, RunID: "ork-r", AdmissionSHA256: parentSHA, TaskID: "c"}
			childSHA, _, err := f.recordStore.WriteAdmission(ctx, child)
			if err != nil {
				t.Fatal(err)
			}
			facts.parent = parentSHA
			f.plan.Identity.AdmissionSHA256 = parentSHA
			f.plan.Children = map[string]*engine.ChildRunPlan{"c": {RunID: "ork-child", Plan: engine.RunPlan{Identity: &engine.IdentityPlan{AdmissionSHA256: childSHA}}}}
			f.cluster.children = map[string]engine.ResumeExecution{"ork-child": {Status: enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED, StartTime: resumeEpoch}}
			ref, err := f.store.Put(ctx, "values.json", strings.NewReader(`{"d":1}`), artifacts.PutOptions{})
			if err != nil {
				t.Fatal(err)
			}
			if _, err := f.recordStore.WriteRecordedValues(ctx, &orkstackv1.RecordedValues{SchemaVersion: 1,
				Kind: orkstackv1.RecordedValuesKind_RECORDED_VALUES_KIND_OUTPUTS, Namespace: "default", RunId: "ork-child", AdmissionSha256: childSHA,
				InputBinding: make([]byte, 32), Values: &orkstackv1.ArtifactRef{Uri: ref.URI, Sha256: ref.SHA256, SizeBytes: ref.SizeBytes}}); err != nil {
				t.Fatal(err)
			}
			rec := &identity.ValuesRecorder{Records: f.recordStore, Starts: facts, EngineBuild: "b", Now: func() time.Time { return resumeEpoch },
				Describe: func(_ context.Context, run string) (identity.ExecutionFacts, error) { return facts.facts(run), nil }}
			f.configure = func(cfg *api.Config) { cfg.ComposedRecords = rec }
		}}, true)
	f.resets(true)
	f.runReader.runs["ork-child"] = &orkstackv1.Run{Id: "ork-child", Status: orkstackv1.Status_STATUS_SUCCEEDED}
	if _, err := f.confirm(t, nil); err != nil || len(f.cluster.resets) != 1 {
		t.Fatalf("confirm = %v, %d resets", err, len(f.cluster.resets))
	}
	key, err := artifacts.RecordKey(artifacts.RecordComposed, "default", "ork-r", "c", 0)
	if err != nil {
		t.Fatal(err)
	}
	read, err := f.recordStore.ReadRecordedValues(ctx, artifacts.SlotRun, nil, key, artifacts.RecordWant{
		Kind: orkstackv1.RecordedValuesKind_RECORDED_VALUES_KIND_COMPOSED, Namespace: "default", RunID: "ork-r", TaskID: "c", AdmissionSHA256: facts.parent})
	if err != nil || read.Answer != artifacts.RecordFound {
		t.Fatalf("the composed task's record: %+v, %v", read, err)
	}
	if child, _, _ := read.Value.Child(); child != "ork-child" {
		t.Errorf("the record names child %q", child)
	}
}

// TestResumeRun_RecordsOnlyWhatIsNotOnRecord: a composed task whose own record
// is there, settled by a confirm whose reset never ran, is kept from it and
// not recorded again: on a run resumed since, its scope is no longer the
// newest, and a record asked for there would be refused.
func TestResumeRun_RecordsOnlyWhatIsNotOnRecord(t *testing.T) {
	rec := &childRecorder{}
	f := childFixture(t, enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED, true, rec)
	ref, err := f.store.Put(context.Background(), "values.json", strings.NewReader(`{"d":1}`), artifacts.PutOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.recordStore.WriteRecordedValues(context.Background(), &orkstackv1.RecordedValues{SchemaVersion: 1,
		Kind: orkstackv1.RecordedValuesKind_RECORDED_VALUES_KIND_COMPOSED, Namespace: "default", RunId: "ork-r", TaskId: "c",
		AdmissionSha256: resumeAdmission, InputBinding: make([]byte, 32), Values: &orkstackv1.ArtifactRef{Uri: ref.URI, Sha256: ref.SHA256, SizeBytes: ref.SizeBytes},
		ChildRunId: "ork-child", ChildAdmissionSha256: resumeChildAdmission, ChildOutputsSha256: resumeAdmission}); err != nil {
		t.Fatal(err)
	}
	if _, err := f.confirm(t, nil); err != nil || len(f.cluster.resets) != 1 {
		t.Fatalf("confirm = %v, %d resets", err, len(f.cluster.resets))
	}
	if len(rec.asks) != 0 {
		t.Fatalf("recorded again: %+v", rec.asks)
	}
}

// forceCanceledChildFixture is an admitted run a force cancel ended while its
// composed task c was RUNNING and its task x unfinished; its child ork-child is
// as the cluster says, its outputs on record. open says the run's history saw
// the child start and not end.
func forceCanceledChildFixture(t *testing.T, child enumspb.WorkflowExecutionStatus, open bool) *resumeFixture {
	t.Helper()
	keys := forceCancelKeys(t)
	row := childRow(orkstackv1.Status_STATUS_RUNNING)
	row.FinishedAt = nil
	f, _ := composedFixture(t, composedShape{tasks: []*orkstackv1.Task{childTask("c"), imageTask("x")},
		rows: []*orkstackv1.TaskRun{row, rowOf("x", orkstackv1.Status_STATUS_CANCELED)},
		change: func(t *testing.T, f *resumeFixture) {
			b := &resumeHistory{t: t, events: failedHistory(t, "x")}
			if open {
				b.events = b.events[:len(b.events)-1]
				b.add(enumspb.EVENT_TYPE_START_CHILD_WORKFLOW_EXECUTION_INITIATED, func(ev *historypb.HistoryEvent) {
					ev.Attributes = &historypb.HistoryEvent_StartChildWorkflowExecutionInitiatedEventAttributes{
						StartChildWorkflowExecutionInitiatedEventAttributes: &historypb.StartChildWorkflowExecutionInitiatedEventAttributes{WorkflowId: "ork-child"}}
				})
				b.add(enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_FAILED, nil)
			}
			forceCanceledFrom(t, keys, nil, b.events)(f)
			signed := f.configure
			f.plan.Children = map[string]*engine.ChildRunPlan{"c": {RunID: "ork-child", Plan: engine.RunPlan{Identity: &engine.IdentityPlan{AdmissionSHA256: resumeChildAdmission}}}}
			f.cluster.children = map[string]engine.ResumeExecution{"ork-child": {Status: child, StartTime: time.Now().Add(-time.Hour), CloseTime: time.Now().Add(-30 * time.Minute)}}
			ref, err := f.store.Put(context.Background(), "values.json", strings.NewReader(`{"d":1}`), artifacts.PutOptions{})
			if err != nil {
				t.Fatal(err)
			}
			if _, err := f.recordStore.WriteRecordedValues(context.Background(), &orkstackv1.RecordedValues{SchemaVersion: 1,
				Kind: orkstackv1.RecordedValuesKind_RECORDED_VALUES_KIND_OUTPUTS, Namespace: "default", RunId: "ork-child", AdmissionSha256: resumeChildAdmission,
				InputBinding: make([]byte, 32), Values: &orkstackv1.ArtifactRef{Uri: ref.URI, Sha256: ref.SHA256, SizeBytes: ref.SizeBytes}}); err != nil {
				t.Fatal(err)
			}
			f.configure = func(cfg *api.Config) {
				signed(cfg)
				cfg.ResumeForceCancels = keys
				cfg.ComposedRecords = &childRecorder{}
			}
		}}, true)
	f.resets(true)
	f.runReader.runs["ork-child"] = &orkstackv1.Run{Id: "ork-child", Status: orkstackv1.Status_STATUS_SUCCEEDED}
	return f
}

// TestResumeRun_AForceCanceledRunsComposedTask: a composed task a force cancel
// left RUNNING is kept from its child's outputs once that child succeeded, and
// while that child is still open, the resume waits for it (child_running).
func TestResumeRun_AForceCanceledRunsComposedTask(t *testing.T) {
	t.Run("kept from its child", func(t *testing.T) {
		f := forceCanceledChildFixture(t, enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED, false)
		got, err := f.resume(&orkstackv1.ResumeRunRequest{DryRun: true, FromTaskId: "x"})
		if err != nil {
			t.Fatalf("preview = %v", err)
		}
		for _, task := range got.GetTasks() {
			if task.GetTaskId() == "c" && task.GetCategory() != orkstackv1.ResumeCategory_RESUME_CATEGORY_SETTLED {
				t.Fatalf("c = %v, want settled from its child", task)
			}
		}
	})
	t.Run("its child still open", func(t *testing.T) {
		f := forceCanceledChildFixture(t, enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING, true)
		_, err := f.resume(&orkstackv1.ResumeRunRequest{DryRun: true, FromTaskId: "x"})
		if r := refusalOf(t, err); r.GetReason() != "STILL_STOPPING" || r.GetCause() != "child_running" ||
			!strings.Contains(r.GetMessage(), "orkctl run cancel ork-child") {
			t.Fatalf("refusal = %v, want STILL_STOPPING child_running", r)
		}
	})
}
