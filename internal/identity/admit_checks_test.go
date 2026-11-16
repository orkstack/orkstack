package identity

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	workflowpb "go.temporal.io/api/workflow/v1"
	"go.temporal.io/api/workflowservice/v1"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/orkstack/orkstack/internal/task"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

var started = time.Date(2026, 11, 16, 12, 0, 0, 0, time.UTC)

// TestFactsFromDescribe: a describe gives the execution, its chain's first
// execution, its parent and its pending ExecuteTask attempts, and nothing
// of other activities.
func TestFactsFromDescribe(t *testing.T) {
	resp := &workflowservice.DescribeWorkflowExecutionResponse{
		WorkflowExecutionInfo: &workflowpb.WorkflowExecutionInfo{
			Execution:       &commonpb.WorkflowExecution{WorkflowId: "ork-1", RunId: "exec-2"},
			FirstRunId:      "exec-1",
			ParentExecution: &commonpb.WorkflowExecution{WorkflowId: "ork-0", RunId: "p-1"},
			Status:          enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
		},
		PendingActivities: []*workflowpb.PendingActivityInfo{
			{ActivityId: "ork/build", ActivityType: &commonpb.ActivityType{Name: task.ActivityExecuteTask},
				State: enumspb.PENDING_ACTIVITY_STATE_STARTED, Attempt: 2, LastWorkerIdentity: "o-1", LastStartedTime: timestamppb.New(started)},
			{ActivityId: "publish", ActivityType: &commonpb.ActivityType{Name: "PublishEvent"}, State: enumspb.PENDING_ACTIVITY_STATE_STARTED},
		},
	}
	f := FactsFromDescribe(resp)
	if !f.Open || f.WorkflowID != "ork-1" || f.ExecutionID != "exec-2" || f.FirstExecutionID != "exec-1" || f.Parent == nil || *f.Parent != (ExecutionRef{WorkflowID: "ork-0", ExecutionID: "p-1"}) {
		t.Fatalf("facts = %+v", f)
	}
	if len(f.Pending) != 1 || f.Pending[0].ActivityID != "ork/build" || !f.Pending[0].Started || f.Pending[0].Attempt != 2 || !f.Pending[0].StartedAt.Equal(started) {
		t.Fatalf("pending = %+v", f.Pending)
	}
	resp.WorkflowExecutionInfo.Status = enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED
	resp.WorkflowExecutionInfo.CloseTime = timestamppb.New(started)
	if FactsFromDescribe(resp).Open {
		t.Fatal("a closed execution is open")
	}
}

// TestHeldAttempt: the attempt is held only by a describe of the run, open,
// with a pending ExecuteTask activity of the task's id, started, at the
// asked attempt, whose last worker is the caller's Orker; a describe that
// says too little to decide is unknown, never "not held".
func TestHeldAttempt(t *testing.T) {
	open := ExecutionFacts{WorkflowID: "ork-1", Open: true, ExecutionID: "e", FirstExecutionID: "e", Pending: []PendingAttempt{
		{ActivityID: "ork/build", Started: true, Attempt: 2, LastWorker: "o-1", StartedAt: started},
		{ActivityID: "ork/convert/3", Started: true, Attempt: 1, LastWorker: "o-2", StartedAt: started},
		{ActivityID: "ork/wait", Started: false, Attempt: 1, LastWorker: "o-1"},
		{ActivityID: "ork/nostart", Started: true, Attempt: 1, LastWorker: "o-1"},
	}}
	ask := AttemptAsk{TaskID: "build", Item: artifacts.NoItem, Attempt: 2, OrkerID: "o-1"}
	if got, v := HeldAttempt(open, "ork-1", ask); v != AttemptHeld || !got.StartedAt.Equal(started) {
		t.Fatalf("the held attempt = %+v, %v", got, v)
	}
	item := AttemptAsk{TaskID: "convert", Item: artifacts.ItemAt(3), Attempt: 1, OrkerID: "o-2"}
	if _, v := HeldAttempt(open, "ork-1", item); v != AttemptHeld {
		t.Fatalf("an item's attempt = %v", v)
	}
	for _, c := range []struct {
		name   string
		mutate func(*ExecutionFacts, *AttemptAsk)
		want   AttemptVerdict
	}{
		{"a closed execution", func(f *ExecutionFacts, _ *AttemptAsk) { f.Open = false }, AttemptNotHeld},
		{"another run's describe", func(f *ExecutionFacts, _ *AttemptAsk) { f.WorkflowID = "ork-2" }, AttemptNotHeld},
		{"a describe of no run", func(f *ExecutionFacts, _ *AttemptAsk) { f.WorkflowID = "" }, AttemptUnknown},
		{"a describe of no execution", func(f *ExecutionFacts, _ *AttemptAsk) { f.ExecutionID = "" }, AttemptUnknown},
		{"another attempt", func(_ *ExecutionFacts, a *AttemptAsk) { a.Attempt = 1 }, AttemptNotHeld},
		{"another Orker", func(_ *ExecutionFacts, a *AttemptAsk) { a.OrkerID = "o-2" }, AttemptNotHeld},
		{"another task", func(_ *ExecutionFacts, a *AttemptAsk) { a.TaskID = "test" }, AttemptNotHeld},
		{"an item of the task", func(_ *ExecutionFacts, a *AttemptAsk) { a.Item = artifacts.ItemAt(0) }, AttemptNotHeld},
		{"not started", func(_ *ExecutionFacts, a *AttemptAsk) { a.TaskID = "wait"; a.Attempt = 1 }, AttemptNotHeld},
		{"started with no time", func(_ *ExecutionFacts, a *AttemptAsk) { a.TaskID = "nostart"; a.Attempt = 1 }, AttemptUnknown},
		{"no Orker", func(_ *ExecutionFacts, a *AttemptAsk) { a.OrkerID = "" }, AttemptNotHeld},
		{"a task id with a slash, as another's item", func(f *ExecutionFacts, a *AttemptAsk) {
			f.Pending = append(f.Pending, PendingAttempt{ActivityID: "ork/convert/3", Started: true, Attempt: 2, LastWorker: "o-1", StartedAt: started})
			a.TaskID = "convert/3"
		}, AttemptNotHeld},
		{"a negative item", func(f *ExecutionFacts, a *AttemptAsk) {
			f.Pending = append(f.Pending, PendingAttempt{ActivityID: "ork/build/-1", Started: true, Attempt: 2, LastWorker: "o-1", StartedAt: started})
			a.Item = artifacts.ItemAt(-1)
		}, AttemptNotHeld},
		{"attempt 0", func(f *ExecutionFacts, a *AttemptAsk) {
			f.Pending = append(f.Pending, PendingAttempt{ActivityID: "ork/build", Started: true, Attempt: 0, LastWorker: "o-1", StartedAt: started})
			a.Attempt = 0
		}, AttemptNotHeld},
	} {
		f := open
		f.Pending = append([]PendingAttempt(nil), open.Pending...)
		a := ask
		c.mutate(&f, &a)
		if got, v := HeldAttempt(f, "ork-1", a); v != c.want {
			t.Errorf("%s: %+v, %v; want %v", c.name, got, v, c.want)
		}
	}
}

// startWorld is the cluster and the store a start check reads.
type startWorld struct {
	executions  map[ExecutionRef]ExecutionFacts
	describeErr error
	admissions  map[string]artifacts.RecordRead[artifacts.Admission]
	chains      map[string]artifacts.RecordRead[artifacts.Chain]
	chainErr    error
	completeErr error
	completed   []artifacts.Chain
	firstStart  map[string]string
	firstErr    error
}

func (w *startWorld) Describe(_ context.Context, workflowID, executionID string) (ExecutionFacts, error) {
	if w.describeErr != nil {
		return ExecutionFacts{}, w.describeErr
	}
	f, ok := w.executions[ExecutionRef{WorkflowID: workflowID, ExecutionID: executionID}]
	if !ok {
		return ExecutionFacts{}, ErrNoSuchExecution
	}
	return f, nil
}

func (w *startWorld) ReadAdmission(_ context.Context, _, runID, sha string) (artifacts.RecordRead[artifacts.Admission], error) {
	r, ok := w.admissions[runID+"/"+sha]
	if !ok {
		return artifacts.RecordRead[artifacts.Admission]{Answer: artifacts.RecordNone}, nil
	}
	return r, nil
}

func (w *startWorld) ReadChain(_ context.Context, _, runID string) (artifacts.RecordRead[artifacts.Chain], error) {
	if w.chainErr != nil {
		return artifacts.RecordRead[artifacts.Chain]{}, w.chainErr
	}
	if r, ok := w.chains[runID]; ok {
		return r, nil
	}
	return artifacts.RecordRead[artifacts.Chain]{Answer: artifacts.RecordNone}, nil
}

func (w *startWorld) CompleteChain(_ context.Context, _, _ string, c artifacts.Chain) error {
	if w.completeErr != nil {
		return w.completeErr
	}
	w.completed = append(w.completed, c)
	return nil
}

func (w *startWorld) FirstStartAdmission(_ context.Context, runID, firstExecutionID string) (string, error) {
	if w.firstErr != nil {
		return "", w.firstErr
	}
	return w.firstStart[runID+"/"+firstExecutionID], nil
}

// admitted is a complete record of run started by starter, and its
// digest.
func admitted(t *testing.T, run string, starter artifacts.Starter, children map[string]string) (artifacts.Admission, string) {
	t.Helper()
	a := artifacts.Admission{RunID: run, Namespace: "ns", Orkflow: "deploy", OrkflowVersion: "1",
		DefinitionDigest: strings.Repeat("d", 64), Starter: starter, Origin: artifacts.AdmittedOrigin{Kind: "submit"},
		Nonce: strings.Repeat("0f", 16), AdmittedAt: started, EngineBuild: "b7", SubjectFormat: artifacts.SubjectFormatOrk,
		SpecVersion: 1, Tasks: map[string]artifacts.AdmittedTask{"build": {Pool: "main", SpecSHA256: strings.Repeat("a", 64)}},
		Children: children}
	_, sha, err := artifacts.EncodeAdmission(a)
	if err != nil {
		t.Fatal(err)
	}
	return a, sha
}

func found[T any](v T) artifacts.RecordRead[T] {
	return artifacts.RecordRead[T]{Answer: artifacts.RecordFound, Value: v}
}

var otherSHA = strings.Repeat("b", 64)

// TestCheckStart_Engine: a run an engine started is its admission's when
// it has no parent and its chain record names its first execution and
// this admission; an absent record is completed from the first start's
// plan; anything that could not be read is unavailable, never a match.
func TestCheckStart_Engine(t *testing.T) {
	ctx := context.Background()
	adm, sha := admitted(t, "ork-1", artifacts.Starter{Kind: artifacts.StarterEngine}, nil)
	root := ExecutionFacts{WorkflowID: "ork-1", Open: true, ExecutionID: "e2", FirstExecutionID: "e1"}
	chain := artifacts.Chain{FirstExecutionID: "e1", AdmissionSHA256: sha}
	canceled := fmt.Errorf("read: %w", context.Canceled)
	for _, c := range []struct {
		name     string
		world    func(*startWorld)
		facts    func(*ExecutionFacts)
		want     StartVerdict
		complete bool
	}{
		{"the chain record names it", func(w *startWorld) { w.chains["ork-1"] = found(chain) }, nil, StartAdmitted, false},
		{"another first execution", func(w *startWorld) {
			w.chains["ork-1"] = found(artifacts.Chain{FirstExecutionID: "e0", AdmissionSHA256: sha})
		}, nil, StartNotAdmitted, false},
		{"another admission", func(w *startWorld) {
			w.chains["ork-1"] = found(artifacts.Chain{FirstExecutionID: "e1", AdmissionSHA256: otherSHA})
		}, nil, StartNotAdmitted, false},
		{"a parent", func(w *startWorld) { w.chains["ork-1"] = found(chain) }, func(f *ExecutionFacts) {
			f.Parent = &ExecutionRef{WorkflowID: "x", ExecutionID: "y"}
		}, StartNotAdmitted, false},
		{"a malformed parent", func(w *startWorld) { w.chains["ork-1"] = found(chain) }, func(f *ExecutionFacts) {
			f.Parent = &ExecutionRef{}
		}, StartUnavailable, false},
		{"no first execution", func(w *startWorld) { w.chains["ork-1"] = found(chain) }, func(f *ExecutionFacts) { f.FirstExecutionID = "" }, StartUnavailable, false},
		{"another run's facts", func(w *startWorld) { w.chains["ork-1"] = found(chain) }, func(f *ExecutionFacts) { f.WorkflowID = "ork-2" }, StartNotAdmitted, false},
		{"facts of no run", func(w *startWorld) { w.chains["ork-1"] = found(chain) }, func(f *ExecutionFacts) { f.WorkflowID = "" }, StartUnavailable, false},
		{"a chain record that cannot be read", func(w *startWorld) {
			w.chains["ork-1"] = artifacts.RecordRead[artifacts.Chain]{Answer: artifacts.RecordInvalid}
		}, nil, StartUnavailable, false},
		{"a chain record of a later format", func(w *startWorld) {
			w.chains["ork-1"] = artifacts.RecordRead[artifacts.Chain]{Answer: artifacts.RecordUnsupported}
		}, nil, StartUnavailable, false},
		{"a canceled chain read", func(w *startWorld) { w.chainErr = canceled }, nil, StartUnavailable, false},
		{"absent, and the first start names it", func(w *startWorld) { w.firstStart["ork-1/e1"] = sha }, nil, StartAdmitted, true},
		{"absent, and the first start names another", func(w *startWorld) { w.firstStart["ork-1/e1"] = otherSHA }, nil, StartNotAdmitted, false},
		{"absent, and the first start names none", func(*startWorld) {}, nil, StartNotAdmitted, false},
		{"absent, and the first start cannot be read", func(w *startWorld) { w.firstErr = errors.New("history gone") }, nil, StartUnavailable, false},
		{"absent, and the first start read is canceled", func(w *startWorld) { w.firstErr = canceled }, nil, StartUnavailable, false},
		{"absent, and a racing completion differs", func(w *startWorld) {
			w.firstStart["ork-1/e1"] = sha
			w.completeErr = artifacts.ErrChainDiffers
		}, nil, StartNotAdmitted, false},
		{"absent, and the record there cannot be read", func(w *startWorld) {
			w.firstStart["ork-1/e1"] = sha
			w.completeErr = &artifacts.ChainInvalidError{Object: "chain.json", Reason: "too large"}
		}, nil, StartUnavailable, false},
		{"absent, and the write fails", func(w *startWorld) {
			w.firstStart["ork-1/e1"] = sha
			w.completeErr = &artifacts.RecordStoreUnanswered{Class: artifacts.UnansweredUnavailable, Err: errors.New("down")}
		}, nil, StartUnavailable, false},
	} {
		w := &startWorld{chains: map[string]artifacts.RecordRead[artifacts.Chain]{}, firstStart: map[string]string{}}
		c.world(w)
		f := root
		if c.facts != nil {
			c.facts(&f)
		}
		got := CheckStart(ctx, w, adm, sha, f)
		if strings.Contains(got.Reason, sha) || strings.Contains(got.Reason, otherSHA) {
			t.Errorf("%s: the reason names a whole digest: %s", c.name, got.Reason)
		}
		if got.Verdict != c.want || got.Reason == "" {
			t.Errorf("%s: %+v, want %v", c.name, got, c.want)
		}
		if c.complete != (len(w.completed) == 1) || (c.complete && w.completed[0] != chain) {
			t.Errorf("%s: completed %+v", c.name, w.completed)
		}
	}
	// An admission that is not the record named is never checked.
	if got := CheckStart(ctx, &startWorld{chains: map[string]artifacts.RecordRead[artifacts.Chain]{"ork-1": found(artifacts.Chain{FirstExecutionID: "e1", AdmissionSHA256: otherSHA})}},
		adm, otherSHA, root); got.Verdict != StartUnavailable {
		t.Fatalf("an admission under another's digest = %+v", got)
	}
}

// TestCheckStart_FireAndParent: a fired run's parent is exactly its fire;
// a child's parent is an execution of the run its record names, whose own
// start is checked by the same table.
func TestCheckStart_FireAndParent(t *testing.T) {
	ctx := context.Background()
	fired, firedSHA := admitted(t, "ork-1", artifacts.Starter{Kind: artifacts.StarterFire, WorkflowID: "fire-1", ExecutionID: "fe"}, nil)
	ff := ExecutionFacts{WorkflowID: "ork-1", Open: true, ExecutionID: "e1", FirstExecutionID: "e1", Parent: &ExecutionRef{WorkflowID: "fire-1", ExecutionID: "fe"}}
	if got := CheckStart(ctx, &startWorld{}, fired, firedSHA, ff); got.Verdict != StartAdmitted {
		t.Fatalf("a fired run = %+v", got)
	}
	ff.Parent = &ExecutionRef{WorkflowID: "fire-1", ExecutionID: "other"}
	if got := CheckStart(ctx, &startWorld{}, fired, firedSHA, ff); got.Verdict != StartNotAdmitted {
		t.Fatalf("another fire's execution = %+v", got)
	}
	ff.Parent = nil
	if got := CheckStart(ctx, &startWorld{}, fired, firedSHA, ff); got.Verdict != StartNotAdmitted {
		t.Fatalf("a fired run with no parent = %+v", got)
	}

	parent, parentSHA := admitted(t, "ork-p", artifacts.Starter{Kind: artifacts.StarterEngine}, map[string]string{"compose": "ork-c"})
	bare, bareSHA := admitted(t, "ork-p", artifacts.Starter{Kind: artifacts.StarterEngine}, nil)
	elsewhere, elsewhereSHA := admitted(t, "ork-p", artifacts.Starter{Kind: artifacts.StarterEngine}, map[string]string{"other": "ork-c"})
	another, anotherSHA := admitted(t, "ork-p", artifacts.Starter{Kind: artifacts.StarterEngine}, map[string]string{"compose": "ork-other"})
	childOf := func(sha string) (artifacts.Admission, string) {
		return admitted(t, "ork-c", artifacts.Starter{Kind: artifacts.StarterParentRun, RunID: "ork-p", AdmissionSHA256: sha, TaskID: "compose"}, nil)
	}
	cf := ExecutionFacts{WorkflowID: "ork-c", Open: true, ExecutionID: "c1", FirstExecutionID: "c1", Parent: &ExecutionRef{WorkflowID: "ork-p", ExecutionID: "p2"}}
	world := func(parentSHA string, parent artifacts.Admission) *startWorld {
		return &startWorld{
			executions: map[ExecutionRef]ExecutionFacts{{WorkflowID: "ork-p", ExecutionID: "p2"}: {WorkflowID: "ork-p", Open: true, ExecutionID: "p2", FirstExecutionID: "p1"}},
			admissions: map[string]artifacts.RecordRead[artifacts.Admission]{"ork-p/" + parentSHA: found(parent)},
			chains:     map[string]artifacts.RecordRead[artifacts.Chain]{"ork-p": found(artifacts.Chain{FirstExecutionID: "p1", AdmissionSHA256: parentSHA})},
			firstStart: map[string]string{},
		}
	}
	for _, c := range []struct {
		name      string
		parent    artifacts.Admission
		parentSHA string
		edit      func(*startWorld, *ExecutionFacts)
		want      StartVerdict
	}{
		{"a child of an admitted parent", parent, parentSHA, nil, StartAdmitted},
		{"a child of a parent that lists none", bare, bareSHA, nil, StartAdmitted},
		{"a parent that lists other children", elsewhere, elsewhereSHA, nil, StartNotAdmitted},
		{"the parent composes another child", another, anotherSHA, nil, StartNotAdmitted},
		{"no parent", parent, parentSHA, func(_ *startWorld, f *ExecutionFacts) { f.Parent = nil }, StartNotAdmitted},
		{"a sibling's facts", parent, parentSHA, func(_ *startWorld, f *ExecutionFacts) { f.WorkflowID = "ork-sibling" }, StartNotAdmitted},
		{"a parent with no execution", parent, parentSHA, func(_ *startWorld, f *ExecutionFacts) {
			f.Parent = &ExecutionRef{WorkflowID: "ork-p"}
		}, StartUnavailable},
		{"a parent of another run", parent, parentSHA, func(_ *startWorld, f *ExecutionFacts) {
			f.Parent = &ExecutionRef{WorkflowID: "ork-x", ExecutionID: "p2"}
		}, StartNotAdmitted},
		{"a parent the cluster does not hold", parent, parentSHA, func(w *startWorld, _ *ExecutionFacts) { w.executions = nil }, StartNotAdmitted},
		{"a cluster that does not answer", parent, parentSHA, func(w *startWorld, _ *ExecutionFacts) { w.describeErr = errors.New("unavailable") }, StartUnavailable},
		{"a canceled describe", parent, parentSHA, func(w *startWorld, _ *ExecutionFacts) { w.describeErr = context.Canceled }, StartUnavailable},
		{"the parent's facts of another run", parent, parentSHA, func(w *startWorld, _ *ExecutionFacts) {
			w.executions[ExecutionRef{WorkflowID: "ork-p", ExecutionID: "p2"}] = ExecutionFacts{WorkflowID: "ork-q", Open: true, ExecutionID: "p2", FirstExecutionID: "p1"}
		}, StartNotAdmitted},
		{"the parent's admission absent", parent, parentSHA, func(w *startWorld, _ *ExecutionFacts) { w.admissions = nil }, StartNotAdmitted},
		{"the parent's admission unreadable", parent, parentSHA, func(w *startWorld, _ *ExecutionFacts) {
			w.admissions["ork-p/"+parentSHA] = artifacts.RecordRead[artifacts.Admission]{Answer: artifacts.RecordInvalid}
		}, StartUnavailable},
		{"the parent's admission of a later format", parent, parentSHA, func(w *startWorld, _ *ExecutionFacts) {
			w.admissions["ork-p/"+parentSHA] = artifacts.RecordRead[artifacts.Admission]{Answer: artifacts.RecordUnsupported}
		}, StartUnavailable},
		{"the parent's own start not its record's", parent, parentSHA, func(w *startWorld, _ *ExecutionFacts) {
			w.chains["ork-p"] = found(artifacts.Chain{FirstExecutionID: "p0", AdmissionSHA256: parentSHA})
		}, StartNotAdmitted},
	} {
		w, f := world(c.parentSHA, c.parent), cf
		if c.edit != nil {
			c.edit(w, &f)
		}
		child, childSHA := childOf(c.parentSHA)
		got := CheckStart(ctx, w, child, childSHA, f)
		if strings.Contains(got.Reason, c.parentSHA) || strings.Contains(got.Reason, childSHA) {
			t.Errorf("%s: the reason names a whole digest: %s", c.name, got.Reason)
		}
		if got.Verdict != c.want {
			t.Errorf("%s: %+v, want %v", c.name, got, c.want)
		}
	}
}

// TestCheckStart_AWalkPastTheBoundIsUnavailable: a chain of parents
// deeper than this release composes is unavailable, not refused.
func TestCheckStart_AWalkPastTheBoundIsUnavailable(t *testing.T) {
	w := &startWorld{executions: map[ExecutionRef]ExecutionFacts{}, admissions: map[string]artifacts.RecordRead[artifacts.Admission]{}}
	run := func(i int) string { return "ork-" + string(rune('a'+i)) }
	// From the top down, so each child names its parent's real digest.
	top := MaxStartWalk + 1
	sha := map[int]string{}
	top0, topSHA := admitted(t, run(top), artifacts.Starter{Kind: artifacts.StarterEngine}, nil)
	w.admissions[run(top)+"/"+topSHA] = found(top0)
	sha[top] = topSHA
	var first artifacts.Admission
	for i := top - 1; i >= 0; i-- {
		a, s := admitted(t, run(i), artifacts.Starter{Kind: artifacts.StarterParentRun, RunID: run(i + 1), AdmissionSHA256: sha[i+1], TaskID: "t"}, nil)
		w.admissions[run(i)+"/"+s] = found(a)
		sha[i] = s
		first = a
	}
	for i := 0; i <= top; i++ {
		w.executions[ExecutionRef{WorkflowID: run(i), ExecutionID: "x"}] = ExecutionFacts{WorkflowID: run(i), Open: true, ExecutionID: "x", FirstExecutionID: "x",
			Parent: &ExecutionRef{WorkflowID: run(i + 1), ExecutionID: "x"}}
	}
	got := CheckStart(context.Background(), w, first, sha[0], w.executions[ExecutionRef{WorkflowID: run(0), ExecutionID: "x"}])
	if got.Verdict != StartUnavailable || !strings.Contains(got.Reason, "longer than") {
		t.Fatalf("a walk past the bound = %+v", got)
	}
}

// TestCheckBinding: a task is bound to its admission when the spec digest
// it was dispatched with is the admission's for it; otherwise the answer
// carries the admitting engine's build.
func TestCheckBinding(t *testing.T) {
	adm, _ := admitted(t, "ork-1", artifacts.Starter{Kind: artifacts.StarterEngine}, nil)
	spec := adm.Tasks["build"].SpecSHA256
	if build, ok := CheckBinding(adm, "build", spec); !ok || build != "" {
		t.Fatalf("the admitted digest = %q, %v", build, ok)
	}
	if build, ok := CheckBinding(adm, "build", otherSHA); ok || build != "b7" {
		t.Fatalf("another digest = %q, %v", build, ok)
	}
	if _, ok := CheckBinding(adm, "absent", spec); ok {
		t.Fatal("a task not in the table is bound")
	}
}
