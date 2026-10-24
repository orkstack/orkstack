package api

import (
	"fmt"

	"connectrpc.com/connect"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/engine"
)

// orkflowMaxTasksOf defaults Config.OrkflowMaxTasks.
func orkflowMaxTasksOf(configured int32) int32 {
	if configured <= 0 {
		return engine.DefaultOrkflowMaxTasks
	}
	return configured
}

// orkflowMaxTasksSetting names the setting that moves the task ceiling, in
// both of its spellings, so a refusal tells an author whom to ask and an
// operator what to change.
const orkflowMaxTasksSetting = "engine.orkflow_max_tasks, ORK_ORKFLOW_MAX_TASKS"

// Rule 43: the document declares at most the deployment's task ceiling,
// counted as wf.Tasks (a fan-out and a composed task each count once), and
// the check runs at validate, apply, plan and again at submit.
//
// checkTaskCeiling is validate, apply and plan's task-count check: the tasks
// the document declares against the deployment's ceiling.
//
// What counts is wf.Tasks — every declared task, whatever its kind. A
// fan-out is ONE task: its items are bounded by max_items and
// ORK_EACH_MAX_ITEMS (checkEachLimits). A composed task is ONE task: its
// child is a separate run with its own history, and the child's own
// definition met this ceiling when it was applied.
//
// The issue is anchored at the first task past the ceiling, so an editor can
// point at where the document stops fitting.
func (s *Service) checkTaskCeiling(wf *orkstackv1.Orkflow) error {
	count := len(wf.GetTasks())
	if count <= int(s.orkflowMaxTasks) {
		return nil
	}
	return connect.NewError(connect.CodeInvalidArgument, &validationFieldError{
		Field: fmt.Sprintf("tasks[%d]", s.orkflowMaxTasks),
		Err: fmt.Errorf("orkflow declares %d tasks; this deployment allows at most %d (%s). "+
			"Split it into Orkflows composed with uses: orkflow:, or move a repeated task into a fan-out (each:)",
			count, s.orkflowMaxTasks, orkflowMaxTasksSetting),
	})
}

// checkTaskCeilingAtSubmit re-checks a stored definition's task count against
// the deployment's ceiling at submit: lowering ORK_ORKFLOW_MAX_TASKS refuses
// new runs of a version applied under the old value, before any id is minted.
func (s *Service) checkTaskCeilingAtSubmit(wf *orkstackv1.Orkflow) error {
	count := len(wf.GetTasks())
	if count <= int(s.orkflowMaxTasks) {
		return nil
	}
	return connect.NewError(connect.CodeFailedPrecondition, fmt.Errorf(
		"orkflow %s declares %d tasks; this deployment allows at most %d (%s was lowered after version %s was applied)",
		wf.GetName(), count, s.orkflowMaxTasks, orkflowMaxTasksSetting, wf.GetVersion()))
}
