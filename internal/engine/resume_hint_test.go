package engine

import (
	"fmt"
	"slices"
	"testing"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
)

// TestResumeHintOf: what a run's own record says of resuming it, with no
// history read: the planner's own answer for what it can tell.
func TestResumeHintOf(t *testing.T) {
	resumable, not, none := orkstackv1.ResumeHintState_RESUME_HINT_STATE_RESUMABLE,
		orkstackv1.ResumeHintState_RESUME_HINT_STATE_NOT_RESUMABLE, orkstackv1.ResumeHintState_RESUME_HINT_STATE_UNSPECIFIED
	failed := func(code string) *orkstackv1.Run {
		return &orkstackv1.Run{Id: "ork-x", Status: sFail, Error: &orkstackv1.Error{Code: code}}
	}
	child := failed("RUN_FAILED")
	child.Origin = &orkstackv1.RunOrigin{Kind: orkstackv1.RunOriginKind_RUN_ORIGIN_KIND_PARENT_RUN, Id: "ork-p"}
	worn := failed("RUN_FAILED")
	for range MaxResumesPerRun {
		worn.Resumes = append(worn.Resumes, &orkstackv1.RunResume{})
	}
	for name, tc := range map[string]struct {
		run     *orkstackv1.Run
		state   orkstackv1.ResumeHintState
		refusal ResumeRefusalCode
	}{
		"a running run":              {&orkstackv1.Run{Status: sRun}, none, ""},
		"a run that succeeded":       {&orkstackv1.Run{Status: sOK}, none, ""},
		"a canceled run":             {&orkstackv1.Run{Status: sCanc, Error: &orkstackv1.Error{Code: "RUN_CANCELED"}}, none, ""},
		"a failed task":              {failed("RUN_FAILED"), resumable, ""},
		"its timeout":                {failed(RunTimedOutCode), resumable, ""},
		"a history limit":            {failed(RunHistoryLimitCode), not, RefuseNotATaskFailure},
		"a reason nobody recorded":   {failed(""), not, RefuseFailureNotRecorded},
		"a child run":                {child, not, RefuseChildRun},
		"a run resumed to its limit": {worn, not, RefuseTooManyResumes},
	} {
		t.Run(name, func(t *testing.T) {
			h := ResumeHintOf(tc.run)
			if h.GetState() != tc.state || h.GetRefusal() != string(tc.refusal) || (tc.state == not) != (h.GetMessage() != "") ||
				int(h.GetResumesUsed()) != len(tc.run.GetResumes()) {
				t.Fatalf("hint = %v, want %s %s", h, tc.state, tc.refusal)
			}
			if tc.state == none && h != nil {
				t.Fatalf("a run that is not failed has a hint: %v", h)
			}
		})
	}
}

// TestResumeHintOf_TaskActions: a failed task is what Resume reruns, and a
// task that ran can be retried from, but one that runs another Orkflow.
func TestResumeHintOf_TaskActions(t *testing.T) {
	run := &orkstackv1.Run{Id: "ork-x", Status: sFail, Error: &orkstackv1.Error{Code: "RUN_FAILED"},
		TaskRuns: []*orkstackv1.TaskRun{prow("ok", sOK), prow("bad", sFail), prow("late", sCanc), prow("never", sPend), prow("skipped", sSkip),
			{TaskId: "composed", Status: sOK, Child: &orkstackv1.TaskChild{RunId: "ork-c"}}}}
	var got []string
	for _, a := range ResumeHintOf(run).GetTaskActions() {
		got = append(got, fmt.Sprintf("%s resume=%t from=%t", a.GetTaskId(), a.GetResume(), a.GetRetryFrom()))
	}
	want := []string{"ok resume=false from=true", "bad resume=true from=true", "late resume=false from=true"}
	if !slices.Equal(got, want) {
		t.Fatalf("actions = %q, want %q", got, want)
	}
}
