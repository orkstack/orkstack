package engine

import (
	"context"
	"fmt"
	"maps"
	"slices"
	"strings"

	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/dsl"
	"github.com/orkstack/orkstack/internal/stablejson"
)

// A composed child's own run-level limit (E88 §5.4, §22.3). Its key may read
// the child's inputs, which its parent renders only when it starts it, so the
// parent renders the key then, in a local activity whose answer its history
// records, and pins the slot into the child's plan. From there the child is a
// run with a limit like any other. Entered only by a child plan that carries a
// limit, which no history recorded before holds.

// The refusals of a child's limit: each fails the composed task, and the
// child never starts.
const (
	TaskConcurrencyKeyInvalidCode       = "TASK_CONCURRENCY_KEY_INVALID"
	TaskConcurrencyKeyConflictCode      = "TASK_CONCURRENCY_KEY_CONFLICT"
	TaskConcurrencyKeysExhaustedCode    = "TASK_CONCURRENCY_KEYS_EXHAUSTED"
	TaskConcurrencyCheckUnavailableCode = "TASK_CONCURRENCY_CHECK_UNAVAILABLE"
	// ConcurrencyKeyConflictCode refuses at submit a child whose key is an
	// ancestor's, where both are known then.
	ConcurrencyKeyConflictCode = "CONCURRENCY_KEY_CONFLICT"
)

// ChildConcurrencyReserveBytes bounds what a child's start adds to its planned
// bytes for concurrency limits: its rendered key and gate, and one gate a run
// above it, to the deepest tree a plan may hold.
const ChildConcurrencyReserveBytes = 2 << 10

// ActivityRenderConcurrencyKeys renders a child's key, on the engine worker
// that runs the parent's workflow task.
const ActivityRenderConcurrencyKeys = "RenderConcurrencyKeys"

// ChildConcurrency is a planned child's run-level limit before its key is
// rendered: the slot as its submit decided it, its key the template, and what
// the child's plan carries of a limit besides.
type ChildConcurrency struct {
	// Slot is the child's own limit; its Key is empty for a child whose tasks
	// alone have one.
	Slot ConcurrencySlotPlan `json:"slot"`
	// Tasks are the limits of the child's tasks, by task id, their keys the
	// templates (E88 §22.3).
	Tasks map[string]ConcurrencySlotPlan `json:"tasks,omitempty"`
	// Plan is the child's ConcurrencyPlan with no slot.
	Plan ConcurrencyPlan `json:"plan"`
	// MaxActiveKeys bounds the namespace's keys in use.
	MaxActiveKeys int64 `json:"max_active_keys"`
}

// GateIDs is every gate the plan's slots name.
func (p *ConcurrencyPlan) GateIDs() []string {
	if p == nil {
		return nil
	}
	var out []string
	for _, slot := range p.Slots {
		if slot.GateID != "" && !slices.Contains(out, slot.GateID) {
			out = append(out, slot.GateID)
		}
	}
	return out
}

// concurrencyAncestors is the gates a child of this run may not ask for: every
// one this run's own ancestors and this run declare. A child waiting for a
// slot its own ancestor holds would wait for itself.
func (s *runState) concurrencyAncestors() []string {
	return slices.Concat(s.plan.ConcurrencyAncestors, s.plan.Concurrency.GateIDs())
}

// RenderConcurrencyKeysInput is one child's key to render.
type RenderConcurrencyKeysInput struct {
	Limit ChildConcurrency `json:"limit"`
	// Inputs is the child's run input, defaults applied.
	Inputs     stablejson.RawJSON `json:"inputs"`
	Orkflow    string             `json:"orkflow"`
	Version    string             `json:"version"`
	OriginKind string             `json:"origin_kind"`
	OriginID   string             `json:"origin_id"`
	Ancestors  []string           `json:"ancestors,omitempty"`
	// RunGate is the child's own limit's gate, which none of its tasks may
	// ask for; empty for its own.
	RunGate string `json:"run_gate,omitempty"`
}

// ActivityRenderConcurrencyTaskKeys renders a child's tasks' keys.
const ActivityRenderConcurrencyTaskKeys = "RenderConcurrencyTaskKeys"

// ChildTaskSlots are a child's tasks' slots, by task id.
type ChildTaskSlots struct {
	Tasks map[string]ConcurrencySlotPlan `json:"tasks"`
}

// RenderConcurrencyTaskKeys renders each of a child's tasks' keys as
// RenderConcurrencyKeys renders its own, in task id order: refused when it does
// not render, is an ancestor's or the child's own key, or would be one key too
// many.
func (a *GateActivities) RenderConcurrencyTaskKeys(ctx context.Context, in RenderConcurrencyKeysInput) (ChildTaskSlots, error) {
	out := ChildTaskSlots{Tasks: map[string]ConcurrencySlotPlan{}}
	for _, id := range slices.Sorted(maps.Keys(in.Limit.Tasks)) {
		one := in
		one.Limit.Slot = in.Limit.Tasks[id]
		slot, err := a.RenderConcurrencyKeys(ctx, one)
		if err != nil {
			return out, err
		}
		out.Tasks[id] = slot
	}
	return out, nil
}

// RenderConcurrencyKeys renders the child's key, refuses one an ancestor
// declares, holds the namespace to its bound of keys in use, and returns the
// slot. The bound is best-effort, as a submit's is.
func (a *GateActivities) RenderConcurrencyKeys(ctx context.Context, in RenderConcurrencyKeysInput) (ConcurrencySlotPlan, error) {
	slot := in.Limit.Slot
	refuse := func(code string, format string, args ...any) error {
		return composeActivityError(code, orkstackv1.FailureCategory_FAILURE_CATEGORY_CONTRACT_VIOLATION, fmt.Sprintf(format, args...))
	}
	// Numbers as a submit renders them: a key reads the same whether its
	// Orkflow runs on its own or composed.
	inputs, err := stablejson.DecodeRecorded(in.Inputs)
	if err != nil {
		return slot, refuse(TaskConcurrencyKeyInvalidCode, "the child run's input is not JSON: %v", err)
	}
	object, _ := inputs.(map[string]any)
	key := slot.Key
	if strings.Contains(key, "{{") {
		key, err = dsl.RenderConcurrencyKey(key, object, dsl.ConcurrencyKeyRun{
			Orkflow: in.Orkflow, Version: in.Version, OriginKind: in.OriginKind, OriginID: in.OriginID})
	} else {
		// A key with no template is the one its Orkflow was created with, or
		// its own when it declares none, which no authored key may be.
		err = dsl.CheckConcurrencyKey(key, dsl.KeyPlanned)
	}
	if err != nil {
		return slot, refuse(TaskConcurrencyKeyInvalidCode, "%v", err)
	}
	return a.childSlot(ctx, in, key)
}

func (a *GateActivities) childSlot(ctx context.Context, in RenderConcurrencyKeysInput, key string) (ConcurrencySlotPlan, error) {
	slot, plan := in.Limit.Slot, in.Limit.Plan
	slot.Key, slot.GateID = key, GateID(plan.Namespace, plan.Incarnation, key)
	if slices.Contains(in.Ancestors, slot.GateID) || slot.GateID == in.RunGate {
		return slot, composeActivityError(TaskConcurrencyKeyConflictCode, orkstackv1.FailureCategory_FAILURE_CATEGORY_CONTRACT_VIOLATION,
			fmt.Sprintf("its concurrency key %s is one this run, or a run above it, holds: the child would wait for a slot its own parent keeps", key))
	}
	reader := NewTemporalConcurrencyReader(a.client, 0)
	line, err := reader.GateLine(ctx, slot.GateID)
	if err != nil {
		return slot, childCheckUnavailable(ctx, slot, err)
	}
	if line.Present && line.Holders+line.Queued > 0 {
		return slot, nil // a key in use already counts
	}
	active, err := reader.ActiveGates(ctx, plan.Namespace)
	if err != nil {
		return slot, childCheckUnavailable(ctx, slot, err)
	}
	if in.Limit.MaxActiveKeys > 0 && active >= in.Limit.MaxActiveKeys {
		return slot, composeActivityError(TaskConcurrencyKeysExhaustedCode, orkstackv1.FailureCategory_FAILURE_CATEGORY_UNSPECIFIED,
			fmt.Sprintf("its concurrency key %s is not in use, and namespace %q already has %d concurrency keys with runs holding or queued, "+
				"its bound (engine.concurrency.max_active_keys_per_namespace)", key, plan.Namespace, active))
	}
	return slot, nil
}

// childCheckUnavailable is a check the cluster could not answer, tried again:
// the cluster's own error is logged, and never becomes the task's.
func childCheckUnavailable(ctx context.Context, slot ConcurrencySlotPlan, err error) error {
	if activity.IsActivity(ctx) {
		activity.GetLogger(ctx).Warn("a composed child's concurrency key could not be checked", "gate", slot.GateID, "error", err)
	}
	return temporal.NewApplicationError(fmt.Sprintf("the line for key %s, or the namespace's keys in use, could not be read", slot.Key), "ConcurrencyCheck")
}

// limitChild gives a child's start plan the run-level slot its limit renders
// to, or fails the composed task. A plan with no limit is untouched.
func (s *runState) limitChild(ctx workflow.Context, id string, c *childState, input *orkstackv1.Payload, childPlan *RunPlan) error {
	childPlan.ConcurrencyAncestors = s.concurrencyAncestors()
	limit := c.plan.Concurrency
	if limit == nil {
		return nil
	}
	fail := func(code string, category orkstackv1.FailureCategory, message string) error {
		return s.failComposed(ctx, id, code, category,
			fmt.Sprintf("task %q: child orkflow %s's concurrency limit: %s", id, c.label(), message), "")
	}
	inputs, err := stablejson.Marshal(ApplyInputDefaults(c.wf, input).GetData().AsMap())
	if err != nil {
		return fail(TaskConcurrencyKeyInvalidCode, orkstackv1.FailureCategory_FAILURE_CATEGORY_CONTRACT_VIOLATION, err.Error())
	}
	in := RenderConcurrencyKeysInput{
		Limit: *limit, Inputs: inputs, Orkflow: c.wf.GetName(), Version: c.wf.GetVersion(),
		OriginKind: RunOriginKindParentRun, OriginID: s.runID, Ancestors: childPlan.ConcurrencyAncestors,
	}
	// render runs one of the two renders, and ends the task as either fails.
	render := func(activity string, out any) error {
		localCtx := composeLocalContext(ctx)
		err := s.getLocal(localCtx, workflow.ExecuteLocalActivity(localCtx, activity, in), out)
		if categoryOf(err).canceled {
			return s.canceledBeforeChildStart(ctx, id, err)
		}
		if err != nil {
			code, category, message := renderFailure("TASK_CONCURRENCY_CHECK", err)
			return fail(code, category, message)
		}
		return nil
	}
	plan := limit.Plan
	plan.Slots, plan.Run, plan.Tasks = nil, nil, nil
	if limit.Slot.Key != "" {
		var slot ConcurrencySlotPlan
		if err := render(ActivityRenderConcurrencyKeys, &slot); err != nil {
			return err
		}
		if slot.GateID == "" || slot.Key == "" {
			return fail(TaskConcurrencyCheckUnavailableCode, orkstackv1.FailureCategory_FAILURE_CATEGORY_INFRASTRUCTURE_ERROR,
				"its key was not rendered")
		}
		run := 0
		plan.Slots, plan.Run, in.RunGate = []ConcurrencySlotPlan{slot}, &run, slot.GateID
	}
	if len(limit.Tasks) > 0 {
		var tasks ChildTaskSlots
		if err := render(ActivityRenderConcurrencyTaskKeys, &tasks); err != nil {
			return err
		}
		plan.Tasks = map[string]int{}
		for _, task := range slices.Sorted(maps.Keys(limit.Tasks)) {
			slot, ok := tasks.Tasks[task]
			if !ok || slot.GateID == "" {
				return fail(TaskConcurrencyCheckUnavailableCode, orkstackv1.FailureCategory_FAILURE_CATEGORY_INFRASTRUCTURE_ERROR,
					fmt.Sprintf("task %s's key was not rendered", task))
			}
			ix := slices.Index(plan.Slots, slot)
			if ix < 0 {
				ix, plan.Slots = len(plan.Slots), append(plan.Slots, slot)
			}
			plan.Tasks[task] = ix
		}
	}
	childPlan.Concurrency = &plan
	// What the child's release appends after its failure path, as a
	// submit reserves it (§5.2.7).
	childPlan.History = ReserveConcurrencyRelease(childPlan.History, &plan)
	return nil
}

// ReserveConcurrencyRelease is limit with what releasing plan's slots appends
// added to its reserve: per gate, a signal's two events and per_slot_bytes,
// charged as events at the reserve's own rate. limit itself is not changed:
// the runs of a tree share it.
func ReserveConcurrencyRelease(limit *HistoryLimit, plan *ConcurrencyPlan) *HistoryLimit {
	if limit == nil || plan == nil {
		return limit
	}
	out := *limit
	perGate := int64(2)
	if out.Reserve.BytesPerEvent > 0 {
		perGate += (plan.PerSlotBytes + out.Reserve.BytesPerEvent - 1) / out.Reserve.BytesPerEvent
	}
	out.Reserve.Fixed += perGate * int64(len(plan.Slots))
	return &out
}
