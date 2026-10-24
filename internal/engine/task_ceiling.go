package engine

// The flat-stage bound (docs/DEPLOY.md §5, "Task ceiling and the flat-stage
// bound"): the most plain tasks a run may declare so that no generation's
// history plus its failure path's reserve reaches the history guard
// (history_guard.go) before or during its hold. Only cmd/ork's boot check
// and tests read it; workflow code never does.
//
// A generation cannot switch until it has grown by its budget, and until its
// hold starts nothing stops it dispatching, so the worst generation is the
// first (its base is the largest, and it holds the most PENDING tasks). Per
// step, with the guard's exact charges (0.3):
//
//   - a dispatch of a PENDING task adds d of history and f − p of reserve:
//     r₁ = d + f − p;
//   - a completion lands before the loop collects it, adding u of history
//     while its f is still reserved: +u;
//   - a completion together with the dispatch it unblocks adds
//     r₂ = d + c − p, for d + c events of growth.
//
// The ready loop reaches the Selector, which collects completions, only after
// a pass's dispatches, so the largest rise is reached when every in-flight
// task's completion lands, uncollected, during the last dispatch before the
// budget is due. At most min(W, ⌈G/d⌉) tasks are in flight then:
//
//	Δ(W) = (r₁ + u)·⌈G/d⌉                              when d·W ≥ G
//	Δ(W) = r₁·W + r₂·⌈(G − d·W)/(d + c)⌉ + u·W         otherwise
//	T_max(W) = ⌊(L − 1 − B − F − Δ(W)) / p⌋
//
// L is the cluster's event limit, F, p, f the reserve's Fixed, PerPending and
// PerInFlight, G the continuation budget's events and B a generation's base
// before its main loop: RestoreHistoryBound(budget) plus generation0Start.
// Once the budget is due at or above the hold line, each collection lowers
// the sum and only an in-flight task can land, once; below it the run
// dispatches only up to the line. So a generation never reaches the guard
// when B + F + p·T + Δ(W) ≤ L − 1.

// PlainDispatchEvents, PlainCompletionEvents and LandedCompletionEvents are
// the closed form's d, c and u: the history events of one plain task's
// dispatch on the lean event path (TASK_SCHEDULED 7, TASK_STARTED 6, the
// ExecuteTask scheduling event 1), of its completion (the activity's 2, at
// most 3 for the workflow task that delivers it, TASK_SUCCEEDED 7), and the
// part of the completion recorded when it lands, before the loop collects
// it (the activity's 2 and at most 3 for a workflow task of its own).
// Measured costs of today's event path, not command-stream numbers (no
// workflow code reads them). The task soak re-measures d, c and the landing
// peak, and fails if any is exceeded.
const (
	PlainDispatchEvents    int64 = 14
	PlainCompletionEvents  int64 = 12
	LandedCompletionEvents int64 = 5
)

// UncontinuedEventsPerTask is the history a task needs when a run cannot
// carry on (engine.continuation.enabled off), so its tasks share one
// history: about 26 measured events per plain task, 8 reserved per pending
// task, and a margin. With continuation off, cmd/ork defaults the task
// ceiling to at most engine.history.max_events divided by it (1,024 at the
// default limit) and refuses an explicit value above that quotient. A boot
// rule, not a command-stream number.
const UncontinuedEventsPerTask int64 = 50

// generation0Start is what only generation 0 records before its main loop:
// its start event, its first workflow task, RUN_CREATED and RUN_STARTED (7
// each) and the definition gate's marker, 18 in all, rounded up.
const generation0Start int64 = 24

// ShapeMaxTasks is the closed form's T_max(W): the most plain tasks a run
// may declare so that no generation's history plus fail-path reserve reaches
// the guard, in events, before or during its hold, for a shape keeping at
// most concurrency tasks in flight — counting every in-flight completion
// landed and not yet collected, and a base of RestoreHistoryBound(budget) +
// generation0Start. limit and budget are what pinRunTreeLimits would build.
// A bound the deployment must stay within, not a promise that such a run
// completes. Zero when even an empty stage cannot fit. Pure.
func ShapeMaxTasks(limit HistoryLimit, budget ContinuationBudget, concurrency int64) int64 {
	r := limit.Reserve
	return shapeMaxTasks(shapeTerms{
		limit:    limit.MaxEvents,
		base:     RestoreHistoryBound(budget) + generation0Start,
		fixed:    r.Fixed,
		pending:  r.PerPending,
		inFlight: r.PerInFlight,
		budget:   budget.Events,
		dispatch: PlainDispatchEvents,
		complete: PlainCompletionEvents,
		landed:   LandedCompletionEvents,
		width:    concurrency,
	})
}

// shapeMaxTasksBytes is the same closed form in the bytes dimension, for
// dispatches whose activity input is dispatchBytes: every reserved event is
// charged BytesPerEvent, every in-flight entry PerInFlightBytes besides; a
// dispatch records its input and its events, a completion its events and a
// result of at most PerInFlightBytes, a landing its u events and its result
// while its PerInFlightBytes is still reserved; the budget is the bytes
// budget and the base the checkpoint cap plus the base's events. Derived,
// not measured; a test fails if it is ever below the events form, which
// would make bytes the binding dimension. Pure.
func shapeMaxTasksBytes(limit HistoryLimit, budget ContinuationBudget, concurrency, dispatchBytes int64) int64 {
	r := limit.Reserve
	perEvent := r.BytesPerEvent
	return shapeMaxTasks(shapeTerms{
		limit:    limit.MaxBytes,
		base:     budget.CheckpointMaxBytes + (RestoreHistoryBound(budget)+generation0Start)*perEvent,
		fixed:    r.Fixed * perEvent,
		pending:  r.PerPending * perEvent,
		inFlight: r.PerInFlight*perEvent + r.PerInFlightBytes,
		budget:   budget.Bytes,
		dispatch: dispatchBytes + PlainDispatchEvents*perEvent,
		complete: PlainCompletionEvents*perEvent + r.PerInFlightBytes,
		landed:   LandedCompletionEvents*perEvent + r.PerInFlightBytes,
		width:    concurrency,
	})
}

// shapeTerms is one dimension of the closed form: L, B, F, p, f, G, d, c, u
// and W.
type shapeTerms struct {
	limit, base, fixed, pending, inFlight int64
	budget, dispatch, complete, landed    int64
	width                                 int64
}

// shapeRise is Δ(W): the most the sum of history and reserve rises before a
// generation's budget is due, landings included.
func (t shapeTerms) shapeRise() int64 {
	r1 := t.dispatch + t.inFlight - t.pending
	r2 := t.dispatch + t.complete - t.pending
	due := ceilDiv(t.budget, t.dispatch) // dispatches until the budget is due
	if t.dispatch*t.width >= t.budget {
		return (r1 + t.landed) * due
	}
	rest := ceilDiv(t.budget-t.dispatch*t.width, t.dispatch+t.complete)
	return r1*t.width + r2*rest + t.landed*t.width
}

func shapeMaxTasks(t shapeTerms) int64 {
	if t.pending <= 0 || t.dispatch <= 0 || t.width <= 0 {
		return 0
	}
	room := t.limit - 1 - t.base - t.fixed - t.shapeRise()
	if room < 0 {
		return 0
	}
	return room / t.pending
}

// ShapePeak is the closed form's predicted peak of history plus reserve for
// a generation whose base is base events and which starts with pending
// PENDING plain tasks, keeping at most concurrency in flight. Once its
// budget is due a generation at or above the hold line holds, and its sum
// never exceeds base + F + p·pending + Δ(W); one below the line dispatches
// on until it reaches the line, at most r₁ past it, plus at most u·W of
// landings. The peak is the larger of the two. The task soak compares every
// generation's measured peak with it. Pure.
func ShapePeak(limit HistoryLimit, budget ContinuationBudget, concurrency, base, pending int64) int64 {
	r := limit.Reserve
	terms := shapeTerms{
		fixed: r.Fixed, pending: r.PerPending, inFlight: r.PerInFlight,
		budget: budget.Events, dispatch: PlainDispatchEvents, complete: PlainCompletionEvents,
		landed: LandedCompletionEvents, width: concurrency,
	}
	held := base + r.Fixed + r.PerPending*pending + terms.shapeRise()
	r1 := PlainDispatchEvents + r.PerInFlight - r.PerPending
	line := limit.MaxEvents*budget.HoldPercent/100 + r1 + LandedCompletionEvents*concurrency
	return max(held, line)
}
