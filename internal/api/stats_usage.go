package api

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"connectrpc.com/connect"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/engine"
)

// GetUsage reports how much of the fleet one namespace RESERVED over one
// window, in GetFlakeIndex's order: resolve the namespace, resolve and
// validate the window and every other field, and only then measure — so a
// refused request can never collect a previous caller's numbers.
//
// The measurement is the UsageMeter's, shared with the admission gate; the
// budget is policy, read from the namespace document on EVERY call and
// stamped on a clone, so an edited budget takes effect on the next request
// however warm the measurement is.
func (h *StatsHandler) GetUsage(
	ctx context.Context,
	req *connect.Request[orkstackv1.GetUsageRequest],
) (*connect.Response[orkstackv1.GetUsageResponse], error) {
	msg := req.Msg
	ns, err := h.svc.namespace(ctx, msg.GetNamespace())
	if err != nil {
		return nil, err
	}
	window, err := h.windowOf(ns.GetName(), msg.GetSince(), msg.GetUntil(), msg.GetLookback())
	if err != nil {
		return nil, err
	}
	groupBy, err := resolveUsageGroupBy(msg.GetGroupBy())
	if err != nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, err)
	}
	measured, err := h.svc.usage.Measure(ctx, window, groupBy,
		usageKeyFor(ns.GetName(), msg.GetSince(), msg.GetUntil(), msg.GetLookback(), groupBy))
	if err != nil {
		return nil, usageError(err)
	}
	response := cloneUsage(measured)
	budget := ns.GetBudget()
	response.Budget = budget
	response.BudgetWindow = msg.GetSince() == nil && msg.GetUntil() == nil && msg.GetLookback() != nil &&
		budget != nil && msg.GetLookback().AsDuration() == budget.GetPeriod().AsDuration()
	if response.GetBudgetWindow() {
		response.BudgetReached = engine.BudgetReached(budget, engine.BudgetUsage{
			CPUSeconds: response.GetTotal().GetCpuSeconds(),
			GiBSeconds: response.GetTotal().GetGibSeconds(),
		})
	}
	return connect.NewResponse(response), nil
}

// resolveUsageGroupBy accepts ORKFLOW, TASK and POOL, each once, and
// prepends ORKFLOW when TASK is named without it — a task id names a task
// only within its orkflow. Empty means the total alone.
func resolveUsageGroupBy(requested []orkstackv1.FailureGroupBy) ([]orkstackv1.FailureGroupBy, error) {
	seen := map[orkstackv1.FailureGroupBy]bool{}
	out := make([]orkstackv1.FailureGroupBy, 0, len(requested)+1)
	for _, dim := range requested {
		name := strings.TrimPrefix(dim.String(), "FAILURE_GROUP_BY_")
		switch dim {
		case orkstackv1.FailureGroupBy_FAILURE_GROUP_BY_ORKFLOW,
			orkstackv1.FailureGroupBy_FAILURE_GROUP_BY_TASK,
			orkstackv1.FailureGroupBy_FAILURE_GROUP_BY_POOL:
		default:
			return nil, fmt.Errorf("group_by %s: usage is grouped by ORKFLOW, TASK or POOL", name)
		}
		if seen[dim] {
			return nil, fmt.Errorf("group_by %s is named twice: usage is grouped by ORKFLOW, TASK or POOL, each once", name)
		}
		seen[dim] = true
		out = append(out, dim)
	}
	if seen[orkstackv1.FailureGroupBy_FAILURE_GROUP_BY_TASK] && !seen[orkstackv1.FailureGroupBy_FAILURE_GROUP_BY_ORKFLOW] {
		out = append([]orkstackv1.FailureGroupBy{orkstackv1.FailureGroupBy_FAILURE_GROUP_BY_ORKFLOW}, out...)
	}
	return out, nil
}

// usageError maps a measurement's failure to what GetUsage answers.
func usageError(err error) error {
	var bound *UsageScanBoundError
	var connectErr *connect.Error
	switch {
	case errors.As(err, &bound):
		return connect.NewError(connect.CodeResourceExhausted,
			fmt.Errorf("%s; narrow the window with since/until", bound.Error()))
	case errors.Is(err, errUsageNoArchive):
		return errNoArchive()
	case errors.Is(err, errUsageNoAttemptStore):
		return connect.NewError(connect.CodeFailedPrecondition, errUsageNoAttemptStore)
	case errors.As(err, &connectErr):
		return err
	default:
		return connect.NewError(connect.CodeUnavailable, err)
	}
}
