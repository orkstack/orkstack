package api_test

import (
	"errors"
	"fmt"
	"strings"
	"testing"

	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/internal/payloadcrypt"
	"github.com/orkstack/orkstack/internal/task"
)

// carriedOnHistory is failedHistory of a run that had carried on from carry:
// it dispatched tasks, and published the end of each.
func carriedOnHistory(t testing.TB, carry *orkstackv1.RunContinuation, tasks ...string) []*historypb.HistoryEvent {
	b := &resumeHistory{t: t}
	in := b.payloads("", &orkstackv1.Orkflow{}, &orkstackv1.Payload{}, engine.RunPlan{}, carry)
	b.add(enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED, func(ev *historypb.HistoryEvent) {
		ev.Attributes = &historypb.HistoryEvent_WorkflowExecutionStartedEventAttributes{
			WorkflowExecutionStartedEventAttributes: &historypb.WorkflowExecutionStartedEventAttributes{Input: in, ContinuedExecutionRunId: "exec-0"}}
	})
	b.workflowTask()
	for _, id := range tasks {
		b.completed(b.schedule(task.ActivityExecuteTask, &orkstackv1.ExecuteTaskInput{TaskId: id, CancellationGracePeriodSeconds: 30}))
		b.workflowTask()
	}
	b.add(enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_FAILED, nil)
	return b.events
}

// refusedAs asserts err refuses a resume as reason, of class.
func refusedAs(t *testing.T, err error, reason string, class orkstackv1.ResumeRefusalClass) {
	t.Helper()
	if r := refusalOf(t, err); r.GetReason() != reason || r.GetClass() != class {
		t.Fatalf("refused %s %v, want %s %v", r.GetReason(), r.GetClass(), reason, class)
	}
}

// carriedFixture is a run that had carried on with a done, and whose b failed
// since.
func carriedFixture(t *testing.T, carry *orkstackv1.RunContinuation, change func(*resumeFixture)) *resumeFixture {
	t.Helper()
	f := newResumeFixture(t, func(f *resumeFixture) {
		f.cluster.histories["exec-1"] = carriedOnHistory(t, carry, "b")
		if change != nil {
			change(f)
		}
	})
	f.runReader.saved = map[string]*orkstackv1.Run{"ork-r": {Id: "ork-r", TaskRuns: []*orkstackv1.TaskRun{
		{TaskId: "a", Status: orkstackv1.Status_STATUS_SUCCEEDED}, {TaskId: "b", Status: orkstackv1.Status_STATUS_PENDING}}}}
	return f
}

// A run that had carried on is resumed: what ended in its saved state is the
// earlier part's, and is not a task to retry from.
func TestResumeRun_OfARunThatCarriedOn(t *testing.T) {
	f := carriedFixture(t, &orkstackv1.RunContinuation{Generation: 1}, nil)
	got, err := f.resume(&orkstackv1.ResumeRunRequest{DryRun: true})
	if err != nil {
		t.Fatal(err)
	}
	categories := map[string]orkstackv1.ResumeCategory{}
	for _, task := range got.GetTasks() {
		categories[task.GetTaskId()] = task.GetCategory()
	}
	if categories["a"] != orkstackv1.ResumeCategory_RESUME_CATEGORY_EARLIER || categories["b"] != orkstackv1.ResumeCategory_RESUME_CATEGORY_RERUN_FAILED {
		t.Fatalf("categories = %v", categories)
	}
	_, err = f.resume(&orkstackv1.ResumeRunRequest{DryRun: true, FromTaskId: "a"})
	refusedAs(t, err, "RETRY_FROM_TOO_EARLY", orkstackv1.ResumeRefusalClass_RESUME_REFUSAL_CLASS_PERMANENT)
}

// Its saved state is what the resumed run starts from, so one that does not
// verify, or is gone, refuses the resume for good, and a store that does not
// answer for now.
func TestResumeRun_OfARunWhoseSavedStateCannotBeRead(t *testing.T) {
	for name, tc := range map[string]struct {
		err    error
		reason string
		class  orkstackv1.ResumeRefusalClass
	}{
		"damaged":     {fmt.Errorf("read: %w", engine.ErrContinuationObjectInvalid), "STATE_UNREADABLE", orkstackv1.ResumeRefusalClass_RESUME_REFUSAL_CLASS_PERMANENT},
		"unavailable": {errors.New("connection refused"), "RESUME_CHECK_UNAVAILABLE", orkstackv1.ResumeRefusalClass_RESUME_REFUSAL_CLASS_WAIT},
	} {
		t.Run(name, func(t *testing.T) {
			f := carriedFixture(t, &orkstackv1.RunContinuation{Generation: 1}, nil)
			f.runReader.savedErr = tc.err
			_, err := f.resume(&orkstackv1.ResumeRunRequest{DryRun: true})
			refusedAs(t, err, tc.reason, tc.class)
			if strings.Contains(fmt.Sprint(err), "connection refused") {
				t.Errorf("the refusal quotes the store: %v", err)
			}
		})
	}
}

// A run resumed before it carried on is of that resume: its scope, the resumes
// it counts and the ending its next one sets aside are its saved state's,
// whether or not the cluster still holds the history that was reset.
func TestResumeRun_OfAResumedRunThatCarriedOn(t *testing.T) {
	f := carriedFixture(t, &orkstackv1.RunContinuation{Generation: 1, Resume: &orkstackv1.RunResumeCarry{SeqBase: 5, ResumeCount: 3}}, func(f *resumeFixture) {
		f.endings.archived = map[uint64]bool{5: true}
	})
	got, err := f.resume(&orkstackv1.ResumeRunRequest{DryRun: true})
	if err != nil || got.GetResumesUsed() != 3 {
		t.Fatalf("resumes used %d, %v; want the 3 its saved state counts", got.GetResumesUsed(), err)
	}
	// Nor does a plan record that is gone.
	f = carriedFixture(t, &orkstackv1.RunContinuation{Generation: 1, Resume: &orkstackv1.RunResumeCarry{SeqBase: 5, ResumeCount: 3}}, func(f *resumeFixture) {
		f.cluster.resetKeys = map[string]string{"exec-1": engine.ResumePlanKey{Scope: 5, PlanDigest: strings.Repeat("ab", 32)}.String()}
	})
	if got, err := f.resume(&orkstackv1.ResumeRunRequest{DryRun: true}); err != nil || got.GetResumesUsed() != 3 {
		t.Fatalf("resumes used %d, %v; want the 3 its saved state counts, whatever its records say", got.GetResumesUsed(), err)
	}
}

// A resume of a run that had carried on, whose plan could not be run, is of
// that run still: what its earlier part ended is the saved state's, and what
// its last part dispatched is the history it was reset from.
func TestResumeRun_OfACarriedRunWhoseResumeNeverRan(t *testing.T) {
	carry := &orkstackv1.RunContinuation{Generation: 1}
	f := carriedFixture(t, carry, func(f *resumeFixture) {
		f.cluster.histories["exec-0"] = carriedOnHistory(t, carry, "b")
		f.cluster.histories["exec-1"] = resetFrom(t, carriedOnHistory(t, carry), "exec-0", "exec-1", engine.ReasonResumePlanInvalid)
		f.run.Error = &orkstackv1.Error{Code: engine.ReasonResumePlanInvalid}
	})
	got, err := f.resume(&orkstackv1.ResumeRunRequest{DryRun: true})
	if err != nil {
		t.Fatal(err)
	}
	categories := map[string]orkstackv1.ResumeCategory{}
	for _, task := range got.GetTasks() {
		categories[task.GetTaskId()] = task.GetCategory()
	}
	if categories["a"] != orkstackv1.ResumeCategory_RESUME_CATEGORY_EARLIER || categories["b"] != orkstackv1.ResumeCategory_RESUME_CATEGORY_RERUN_FAILED {
		t.Fatalf("categories = %v, want a earlier and b run again", categories)
	}
}

// A sealed run is resumed as any other, its history read with its own
// payloads; one whose key is gone, or whose payloads do not open, never can be,
// and a key that cannot be had now is a wait.
func TestResumeRun_OfASealedRun(t *testing.T) {
	for name, tc := range map[string]struct {
		err    error
		reason string
		class  orkstackv1.ResumeRefusalClass
	}{
		"its key is gone":        {payloadcrypt.ErrKeyless, "STATE_UNREADABLE", orkstackv1.ResumeRefusalClass_RESUME_REFUSAL_CLASS_PERMANENT},
		"it does not open":       {fmt.Errorf("start: %w", payloadcrypt.ErrUnreadable), "STATE_UNREADABLE", orkstackv1.ResumeRefusalClass_RESUME_REFUSAL_CLASS_PERMANENT},
		"its key is not had now": {errors.New("the key service does not answer"), "RESUME_CHECK_UNAVAILABLE", orkstackv1.ResumeRefusalClass_RESUME_REFUSAL_CLASS_WAIT},
	} {
		t.Run(name, func(t *testing.T) {
			f := newResumeFixture(t, func(f *resumeFixture) { f.cluster.payloadsErr = tc.err })
			_, err := f.resume(&orkstackv1.ResumeRunRequest{DryRun: true})
			refusedAs(t, err, tc.reason, tc.class)
			// The reset key is read from the same start.
			f = newResumeFixture(t, func(f *resumeFixture) { f.cluster.resetKeyErr = tc.err })
			_, err = f.resume(&orkstackv1.ResumeRunRequest{DryRun: true})
			refusedAs(t, err, tc.reason, tc.class)
		})
	}
}
