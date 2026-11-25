package engine

import (
	"errors"
	"testing"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
)

// TestRefuseResumeFromHistory: what is refused before anything is read from
// storage, and what is left to the plan because an answer could change it.
func TestRefuseResumeFromHistory(t *testing.T) {
	for _, tc := range []struct {
		name  string
		tasks []*orkstackv1.Task
		rows  []*orkstackv1.TaskRun
		set   func(*ResumePlanInput)
		want  ResumeRefusalCode
		cause ComposedCause
	}{
		{name: "a plain failure is not refused",
			tasks: []*orkstackv1.Task{ptask("a")}, rows: []*orkstackv1.TaskRun{prow("a", sFail)}},
		{name: "the base's own refusals come first",
			tasks: []*orkstackv1.Task{ptask("a")}, rows: []*orkstackv1.TaskRun{prow("a", sFail)},
			set: func(in *ResumePlanInput) { in.Facts.ResetPoint = ResetPoint{} }, want: RefuseNeverStarted},
		{name: "an unknown task",
			tasks: []*orkstackv1.Task{ptask("a")}, rows: []*orkstackv1.TaskRun{prow("a", sFail)},
			set: func(in *ResumePlanInput) { in.Action, in.FromTask = ResumeActionRetryFrom, "z" }, want: RefuseFromTaskUnknown},
		{name: "a composed task downstream of a failed task",
			tasks: []*orkstackv1.Task{ptask("a"), composedTask("c", "a")},
			rows:  []*orkstackv1.TaskRun{prow("a", sFail), prow("c", sCanc)}, want: RefuseComposedTask, cause: ComposedDownstream},
		{name: "a composed task downstream of an unfinished task",
			tasks: []*orkstackv1.Task{ptask("x"), ptask("a"), composedTask("c", "a")},
			rows:  []*orkstackv1.TaskRun{prow("x", sFail), prow("a", sCanc), prow("c", sCanc)}, want: RefuseComposedTask, cause: ComposedDownstream},
		{name: "a composed task downstream of the chosen task",
			tasks: []*orkstackv1.Task{ptask("x"), ptask("a"), composedTask("c", "a")},
			rows:  []*orkstackv1.TaskRun{prow("x", sFail), prow("a", sOK), prow("c", sOK)},
			set:   func(in *ResumePlanInput) { in.Action, in.FromTask = ResumeActionRetryFrom, "a" },
			want:  RefuseComposedTask, cause: ComposedDownstream},
		{name: "the chosen task is composed",
			tasks: []*orkstackv1.Task{ptask("x"), composedTask("c")},
			rows:  []*orkstackv1.TaskRun{prow("x", sFail), prow("c", sOK)},
			set:   func(in *ResumePlanInput) { in.Action, in.FromTask = ResumeActionRetryFrom, "c" },
			want:  RefuseComposedTask, cause: ComposedSelected},
		{name: "a failed composed task may be on record",
			tasks: []*orkstackv1.Task{composedTask("c")}, rows: []*orkstackv1.TaskRun{prow("c", sFail)}},
		{name: "downstream of a result that may be gone",
			tasks: []*orkstackv1.Task{ptask("x"), ptask("a"), composedTask("c", "a")},
			rows:  []*orkstackv1.TaskRun{prow("x", sFail), prow("a", sOK), prow("c", sOK)}},
		{name: "downstream of a failed await, which may be answered on record",
			tasks: []*orkstackv1.Task{awaitTask("w"), composedTask("c", "w")},
			rows:  []*orkstackv1.TaskRun{prow("w", sFail), prow("c", sCanc)}},
		{name: "a task that failed before it ran is the plan's to refuse",
			tasks: []*orkstackv1.Task{ptask("a"), ptask("b", "a")},
			rows:  []*orkstackv1.TaskRun{prow("a", sOK), prow("b", sFail)},
			set:   func(in *ResumePlanInput) { delete(in.Facts.Dispatched, nodeUnit("b")) }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newPlan(tc.tasks, tc.rows...)
			if tc.set != nil {
				tc.set(&f.in)
			}
			f.in.Evidence = ResumeEvidence{} // nothing has been read yet
			err := RefuseResumeFromHistory(f.in)
			if tc.want == "" {
				if err != nil {
					t.Fatalf("refused: %v", err)
				}
				return
			}
			var r *ResumeRefusal
			if !errors.As(err, &r) || r.Code != tc.want || (tc.cause != "" && r.Cause != string(tc.cause)) {
				t.Fatalf("= %v, want %s %s", err, tc.want, tc.cause)
			}
		})
	}
}

// TestRefuseResumeFromHistory_Inconsistent: an input PlanResume could not
// account for is an error here too, never "not refused".
func TestRefuseResumeFromHistory_Inconsistent(t *testing.T) {
	f := newPlan([]*orkstackv1.Task{ptask("a")}, prow("a", sFail))
	f.in.Definition = nil
	if err := RefuseResumeFromHistory(f.in); err == nil {
		t.Fatal("no definition was accepted")
	}
	f = newPlan([]*orkstackv1.Task{ptask("a")}, prow("a", sFail), prow("ghost", sOK))
	err := RefuseResumeFromHistory(f.in)
	var r *ResumeRefusal
	if err == nil || errors.As(err, &r) {
		t.Fatalf("an undeclared row: %v, want an error that is no refusal", err)
	}
}
