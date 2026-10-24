package api

import (
	"fmt"

	"connectrpc.com/connect"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/dsl"
	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/internal/task"
)

// The item selector (docs/UI_CLI_PARITY.md "Fan-out"): every per-task read that takes
// an `item` applies one set of rules, in this order, after the run's
// namespace check and the task-exists check the read already makes.
//
//  1. item set with task_id empty → InvalidArgument.
//  2. item negative → InvalidArgument.
//  3. item set on a task that is not a fan-out → InvalidArgument.
//  4. item set on a fan-out not yet expanded, or item >= count → NotFound.
//  5. item unset on a fan-out: answered by each read (every item for a list,
//     InvalidArgument for a read of one stream or one object).

// fanOutOf is the fan-out row of taskID, or nil when the task is not one.
func fanOutOf(run *orkstackv1.Run, taskID string) *orkstackv1.TaskRun {
	taskRun, ok := taskRunOf(run, taskID)
	if !ok || taskRun.GetEach() == nil {
		return nil
	}
	return taskRun
}

// checkItemRequiresTask is rule 1, checkable before the task is looked up.
func checkItemRequiresTask(taskID string, item *int32) error {
	if item != nil && taskID == "" {
		return connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("item requires task_id"))
	}
	return nil
}

// itemRunOf applies rules 1–4 and returns the selected item's row, or nil
// when item is unset. The task must already be known to exist in run.
func itemRunOf(run *orkstackv1.Run, taskID string, item *int32) (*orkstackv1.TaskRun, error) {
	if err := checkItemRequiresTask(taskID, item); err != nil {
		return nil, err
	}
	if item == nil {
		return nil, nil
	}
	if *item < 0 {
		return nil, connect.NewError(connect.CodeInvalidArgument,
			fmt.Errorf("item %d is negative; items are numbered from 0", *item))
	}
	node := fanOutOf(run, taskID)
	if node == nil {
		return nil, connect.NewError(connect.CodeInvalidArgument,
			fmt.Errorf("task %q is not a fan-out; it has no items", taskID))
	}
	if !node.GetEach().GetExpanded() {
		return nil, connect.NewError(connect.CodeNotFound,
			fmt.Errorf("task %q of run %s has not expanded yet", taskID, run.GetId()))
	}
	count := node.GetEach().GetCount()
	if *item >= count || int(*item) >= len(node.GetItems()) {
		if count == 0 {
			return nil, connect.NewError(connect.CodeNotFound,
				fmt.Errorf("task %q of run %s has 0 items", taskID, run.GetId()))
		}
		return nil, connect.NewError(connect.CodeNotFound,
			fmt.Errorf("task %q of run %s has %d items (0–%d)", taskID, run.GetId(), count, count-1))
	}
	return node.GetItems()[*item], nil
}

// needsItem is rule 5 for a read of ONE stream or ONE object of a fan-out.
func needsItem(run *orkstackv1.Run, taskID, command string) error {
	node := fanOutOf(run, taskID)
	if node == nil {
		return nil
	}
	return connect.NewError(connect.CodeInvalidArgument, fmt.Errorf(
		"task %q is a fan-out of %d items; name the item (%s --item N)",
		taskID, node.GetEach().GetCount(), command))
}

// eachMaxItemsOf is Config.EachMaxItems with 0 meaning the deployment default
// (engine.DefaultEachMaxItems, 1000), and never above the built-in maximum
// (dsl.MaxEachItems, 10000). Boot already refuses a value above the maximum,
// or above 1000 without continuation (cmd/ork); this clamp is defensive.
func eachMaxItemsOf(configured int32) int32 {
	switch {
	case configured <= 0:
		return engine.DefaultEachMaxItems
	case configured > dsl.MaxEachItems:
		return dsl.MaxEachItems
	}
	return configured
}

// itemInputFloor is the per-item input floor a run of this deployment pins
// (engine.eachBounds): the continuation budget's, 0 with continuation off.
// Apply checks a fan-out's own share of every item's input against the same
// bound the run will hold it to.
func (s *Service) itemInputFloor() int64 {
	if s.continuation == nil {
		return 0
	}
	return s.continuation.ItemInputFloorBytes
}

// checkEachLimits is apply and validate's fan-out check: the
// declared total against ORK_EACH_MAX_ITEMS, and each fan-out's OWN part of
// every item's input — the image, command, with, env, secrets and resources
// every item carries — against the per-item bound the declared total allows.
// Both are field issues: ValidateOrkflow reports them, ApplyOrkflow refuses.
// The run input and upstream refs are only known at run time, where the
// engine's own check finishes the job.
func (s *Service) checkEachLimits(wf *orkstackv1.Orkflow) error {
	total := dsl.EachItemsDeclared(wf)
	if total == 0 {
		return nil
	}
	if total > s.eachMaxItems {
		return connect.NewError(connect.CodeInvalidArgument, &validationFieldError{
			Field: dsl.EachSumAnchor(wf, s.eachMaxItems),
			Err: fmt.Errorf("orkflow declares %d items across its fan-outs (%s); this deployment allows %d (ORK_EACH_MAX_ITEMS)",
				total, dsl.EachContributors(wf), s.eachMaxItems),
		})
	}
	inputLimit, _ := engine.EachItemBounds(wf, s.limits, s.itemInputFloor())
	for i, t := range wf.GetTasks() {
		if t.GetEach() == nil {
			continue
		}
		size, err := task.InputSize(&orkstackv1.ExecuteTaskInput{
			Image: t.GetImage(), Command: t.GetCommand(), With: t.GetWith(),
			Env: t.GetEnv(), Secrets: t.GetSecrets(), Resources: t.GetResources(),
		})
		if err != nil {
			return connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("task %q: measure the item input: %w", t.GetId(), err))
		}
		if size > inputLimit {
			return connect.NewError(connect.CodeInvalidArgument, &validationFieldError{
				Field: fmt.Sprintf("tasks[%d].with", i),
				Err: fmt.Errorf("every item of %q would carry at least %d bytes of activity input; this Orkflow's "+
					"fan-outs declare %d items, which allows %d bytes per item", t.GetId(), size, total, inputLimit),
			})
		}
	}
	return nil
}

// checkEachCapAtSubmit re-checks the stored definition's declared total
// against the deployment's cap at submit: lowering ORK_EACH_MAX_ITEMS refuses
// new runs of a version applied under the old value.
func (s *Service) checkEachCapAtSubmit(wf *orkstackv1.Orkflow) error {
	total := dsl.EachItemsDeclared(wf)
	if total == 0 || total <= s.eachMaxItems {
		return nil
	}
	return connect.NewError(connect.CodeFailedPrecondition, fmt.Errorf(
		"orkflow %s declares %d items across its fan-outs (%s); this deployment now allows %d "+
			"(ORK_EACH_MAX_ITEMS was lowered after version %s was applied)",
		wf.GetName(), total, dsl.EachContributors(wf), s.eachMaxItems, wf.GetVersion()))
}
