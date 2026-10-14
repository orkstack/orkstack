package engine

import "go.temporal.io/sdk/workflow"

// CancelBeforeDispatchChange is the workflow.GetVersion change id of the
// check that ends a canceled run before it dispatches, skips or opens
// anything more.
//
// The race it closes: an operator's cancel recorded in the same workflow task
// as the completion that made a task ready — or recorded while a switch's
// writes ran, which turns the switch back (continueAsNew step 6). Before the
// check the run went on to dispatch that task: its TASK_SCHEDULED emit wrote
// its snapshot on the canceled context, the write came back canceled, the
// event's sequence number was burned — a gap in the run's stream — and the
// run ended RUN_CANCELED owned by the task, with the failed write's text as
// its message.
//
// It is asked LAZILY, only when the context is already canceled at a dispatch
// point: a run never canceled there records no marker, and a history recorded
// before the check replays its DefaultVersion branch, which dispatches as
// before. A carried generation answers from its carry (version).
const CancelBeforeDispatchChange = "cancel-before-dispatch"

// cancelBeforeDispatchDisabledForTest is a TEST-ONLY switch, the precedent of
// eachDispatchDescending: when set, the check is skipped WITHOUT asking its
// version, so a negative replay test proves the committed fixture can only
// replay because of it. Never set in production code.
var cancelBeforeDispatchDisabledForTest = false

// canceledBeforeDispatch reports whether the run must end canceled instead
// of starting more work: its context is canceled and the run was recorded
// by a build that checks.
func (s *runState) canceledBeforeDispatch(ctx workflow.Context) bool {
	if ctx.Err() == nil || cancelBeforeDispatchDisabledForTest {
		return false
	}
	return s.version(ctx, CancelBeforeDispatchChange, 1) >= 1
}
