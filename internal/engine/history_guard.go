package engine

import (
	"errors"
	"fmt"
	"strconv"
	"strings"

	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/task"
)

// The history guard (docs/DEPLOY.md §1 engine.history.*): a run fails
// explicitly, RUN_HISTORY_LIMIT, before its history reaches the cluster's
// per-execution limits, keeping back what its own failure path still needs.
// Without it the cluster terminates the execution at its limit, and the
// reader reports a terminated run as a cancel nobody asked for (AGENTS.md §15:
// every terminal failure has a machine-readable and a human-readable reason).
//
// Everything here is a pure function of the workflow's arguments and its
// recorded history: the raw counters come from WorkflowTaskStarted, in live
// execution and in replay alike, and every number the crossing depends on is
// carried in the plan (RunPlan.History), never read from a constant.

const (
	// HistoryGuardChange is the workflow.GetVersion change id of the guard.
	// It is asked LAZILY, the first time a run crosses its line (the
	// precedents are lean-event-path and cancel-siblings-on-run-end): a run
	// that never crosses records no marker, and a history recorded without
	// the guard replays the crossing with DefaultVersion and never fails here.
	HistoryGuardChange = "run-history-guard"

	// RunHistoryLimitCode is the run's Error.code, and the workflow failure's
	// ApplicationError type, when the guard stopped the run.
	RunHistoryLimitCode = "RUN_HISTORY_LIMIT"

	// historyLimitListMax is the most ids one list of the message names.
	historyLimitListMax = 5

	// handlerCanceledAtHistoryLimit is what the failing phase records on a
	// failure handler it never starts because the run's history would not
	// hold it (check point 4).
	handlerCanceledAtHistoryLimit = "the run reached its history limit before this failure handler could start"
)

// TEST-ONLY switches, the precedent of eachDispatchDescending and
// leanEventPathForTest. Never set in production code.
//
//   - historyGuardDisabledForTest: the guard never checks, so a negative
//     replay test proves the history-limit fixture sees the guard removed.
//   - historyGuardPlanForTest: the limit every run uses instead of its plan's,
//     so a negative replay test can hand a committed history a tiny one.
//   - historyGuardForcedForTest: the guard takes version 1 WITHOUT asking
//     workflow.GetVersion, which is how a negative replay test proves the
//     committed fixtures can only replay because of the gate.
var (
	historyGuardDisabledForTest = false
	historyGuardPlanForTest     *HistoryLimit
	historyGuardForcedForTest   = false
)

// historyCount is this workflow task's own count of the commands it issued
// since the raw counters were last read.
type historyCount struct {
	seenLength    int   // raw GetCurrentHistoryLength last observed
	pendingEvents int64 // commands this workflow task issued since then
	pendingBytes  int64
}

// reserveCount is one pass's reserve: failReserve counted once (counted), or
// the charges of the dispatches made since. bytes is what is charged on top
// of events·BytesPerEvent.
type reserveCount struct {
	counted bool
	events  int64
	bytes   int64
}

// historyLimit is the limit this run checks against: its plan's, or nil for a
// plan built before the field existed.
func (s *runState) historyLimit() *HistoryLimit {
	if historyGuardPlanForTest != nil {
		return historyGuardPlanForTest
	}
	return s.plan.History
}

// cancelSiblingsChange is the workflow.GetVersion change id of fail()'s
// cancellation of the run's other tasks (fail, failWithHandlers).
const cancelSiblingsChange = "cancel-siblings-on-run-end"

// version is workflow.GetVersion with the run's memory: a decision carried
// from an earlier generation (restore) is returned as-is and NEVER asked
// again — asking on a fresh history would record the build's newest version,
// which on an unversioned fleet can differ from what generation 0 decided. A
// decision not yet made is asked once and remembered, so the next carry holds
// it; the SDK would answer a second call identically, and asking once keeps
// the marker's position where the first use is.
func (s *runState) version(ctx workflow.Context, change string, max workflow.Version) workflow.Version {
	if v, ok := s.versions[change]; ok {
		return v
	}
	v := workflow.GetVersion(ctx, change, workflow.DefaultVersion, max)
	if s.versions == nil {
		s.versions = map[string]workflow.Version{}
	}
	s.versions[change] = v
	return v
}

// syncHistory reads the raw counters and, when the raw length has moved,
// forgets every pending count: a new workflow task has started, and everything
// noted before it is now in the raw numbers. In production the raw length
// changes at EVERY workflow task (each one records its own WorkflowTaskStarted,
// a heartbeat's included). In TestWorkflowEnvironment it changes only when a
// test sets it, so pending counts accumulate between sets: a conservative
// over-count, and a unit test that asserts an exact crossing sets a new length
// at each step it controls.
func (s *runState) syncHistory(ctx workflow.Context) (raw, rawBytes int64) {
	info := workflow.GetInfo(ctx)
	raw, rawBytes = int64(info.GetCurrentHistoryLength()), int64(info.GetCurrentHistorySize())
	if int(raw) != s.hist.seenLength {
		s.hist = historyCount{seenLength: int(raw)}
	}
	return raw, rawBytes
}

// historyNow is the history this execution will hold once this workflow
// task's commands are recorded. Pure function of recorded history and this code.
func (s *runState) historyNow(ctx workflow.Context) (events, bytes int64) {
	raw, rawBytes := s.syncHistory(ctx)
	return raw + s.hist.pendingEvents, rawBytes + s.hist.pendingBytes
}

// noteCommand counts one command this workflow task issued. It syncs first,
// so a count noted in a new workflow task never inherits the previous one's.
// A run without a limit counts nothing.
func (s *runState) noteCommand(ctx workflow.Context, events, bytes int64) {
	if s.historyLimit() == nil {
		return
	}
	s.syncHistory(ctx)
	s.hist.pendingEvents += events
	s.hist.pendingBytes += bytes
}

// beginPass forgets the pass's count: called at the top of every RunWorkflow
// main-loop iteration and of every runFailureHandlers iteration.
func (s *runState) beginPass() {
	s.passReserve = reserveCount{}
	s.passCharge = reserveCount{}
	s.pinClosureFresh = false // pinClosureAllows' set is rebuilt at most once per pass
}

// chargeReserve adds what one dispatch can add to failReserve. Before the
// pass has counted, the count will see the dispatch itself, so nothing is
// added.
func (s *runState) chargeReserve(events, bytes int64) {
	if !s.passReserve.counted {
		return
	}
	s.passCharge.events += events
	s.passCharge.bytes += bytes
}

// The dispatch charges, each exactly what the step adds to failReserve: a
// task contributes one term for its state — PerPending while it is PENDING
// (never dispatched), then the term of what it became, and nothing once
// terminal. A dispatched task's own terminal emit is inside PerInFlight, so a
// dispatch that takes a task out of PENDING charges the difference; an item,
// a detached publish and a pin were in no term before, so they charge the
// whole of theirs. The formula decides the guard's crossing, so changing it
// is a new command path with its own gate.

// chargeTaskDispatch is schedule's and startChild's charge.
func (s *runState) chargeTaskDispatch() {
	if limit := s.historyLimit(); limit != nil {
		r := limit.Reserve
		s.chargeReserve(r.PerInFlight-r.PerPending, r.PerInFlightBytes)
	}
}

// chargeItemDispatch is scheduleItem's charge.
func (s *runState) chargeItemDispatch() {
	if limit := s.historyLimit(); limit != nil {
		s.chargeReserve(limit.Reserve.PerInFlight, limit.Reserve.PerInFlightBytes)
	}
}

// chargeDetachedPublish is publishItemDetached's charge.
func (s *runState) chargeDetachedPublish() {
	if limit := s.historyLimit(); limit != nil {
		s.chargeReserve(limit.Reserve.PerInFlight, 0)
	}
}

// chargePinOpen is openPin's charge.
func (s *runState) chargePinOpen() {
	if limit := s.historyLimit(); limit != nil {
		s.chargeReserve(limit.Reserve.PerOpenPin, 0)
	}
}

// chargeAwaitOpen is openAwait's charge.
func (s *runState) chargeAwaitOpen() {
	if limit := s.historyLimit(); limit != nil {
		s.chargeReserve(limit.Reserve.PerAwait-limit.Reserve.PerPending, 0)
	}
}

// chargeFanOutExpand is expandEach's charge.
func (s *runState) chargeFanOutExpand() {
	if limit := s.historyLimit(); limit != nil {
		s.chargeReserve(limit.Reserve.PerFanOut-limit.Reserve.PerPending, 0)
	}
}

// valuesBound is the plan's values bound, at most the §2.2 payload maximum:
// the byte charge of one task's scheduled input.
func (s *runState) valuesBound() int64 {
	bound := s.plan.Limits.Proto().GetValuesMaxBytes()
	if bound <= 0 || bound > task.MaxActivityInputBytes {
		bound = task.MaxActivityInputBytes
	}
	return bound
}

// handlerStartBytesOf is what starting the failure handler t can put into
// history: its activity input (the §2.2 bound), a child start, or an await's
// handler start.
func handlerStartBytesOf(t *orkstackv1.Task, r HistoryReserve) int64 {
	switch {
	case t.GetAwait() != nil:
		return r.AwaitHandlerStartBytes
	case t.GetOrkflow() != nil:
		return r.ChildStartBytes
	default:
		return task.MaxActivityInputBytes
	}
}

// countHandlers sets handlerCount and handlerStartBytes, once, from the
// definition and the plan's reserve. A run without a limit counts nothing.
func (s *runState) countHandlers() {
	limit := s.historyLimit()
	if limit == nil {
		return
	}
	for _, id := range s.order {
		t := s.tasks[id]
		if !isFailureHandler(t) {
			continue
		}
		s.handlerCount++
		s.handlerStartBytes += handlerStartBytesOf(t, limit.Reserve)
	}
}

// failReserve is exactly what this run's own failure path would still
// append, as an upper bound: every in-flight entry and detached publish, every
// PENDING (never dispatched) task, every open await, every pin opened and not
// yet closed, every expanded and unfinished fan-out and every non-terminal
// failure handler. bytes is what is charged on top of events·BytesPerEvent:
// one completion result per in-flight entry and the start of every pending
// handler. O(T): counted at most once per pass (reserveNow).
func (s *runState) failReserve(inFlight map[string]workflow.Future, r HistoryReserve) (events, bytes int64) {
	events = r.Fixed + r.PerInFlight*int64(len(inFlight)+len(s.pendingItemPublishes))
	bytes = r.PerInFlightBytes * int64(len(inFlight))
	for _, id := range s.order {
		status := s.taskRuns[id].GetStatus()
		pending := status == orkstackv1.Status_STATUS_PENDING
		if pending {
			events += r.PerPending
		}
		if a := s.awaits[id]; a != nil && a.open {
			events += r.PerAwait
		}
		if e := s.eaches[id]; e != nil && e.expanded && e.count > 0 &&
			status != orkstackv1.Status_STATUS_SUCCEEDED {
			events += r.PerFanOut
		}
		if t := s.tasks[id]; isFailureHandler(t) && !isTerminal(status) {
			events += r.PerHandler
			if pending {
				bytes += handlerStartBytesOf(t, r)
			}
		}
	}
	for _, name := range s.pinOrder {
		if p := s.pins[name]; p.opened && (p.closed == nil || !p.closed.IsReady()) {
			events += r.PerOpenPin
		}
	}
	return events, bytes
}

// reserveNow is an upper bound on failReserve(inFlight) that costs O(1) after
// the first call of a pass. failReserve is counted at most once per pass —
// O(T), the same order as the pass's own readyTasks scan, so a pass stays O(T)
// — and every dispatch since adds its charge (chargeReserve). Every OTHER
// change of state — a completion, a skip, a resolution, a pin closing, an item
// settling, a refill's completion in the Selector — only lowers failReserve,
// so the sum stays an upper bound until the next beginPass.
func (s *runState) reserveNow(inFlight map[string]workflow.Future) (events, bytes int64) {
	limit := s.historyLimit()
	if limit == nil {
		return 0, 0
	}
	if !s.passReserve.counted {
		e, b := s.failReserve(inFlight, limit.Reserve)
		s.passReserve = reserveCount{counted: true, events: e, bytes: b}
		s.passCharge = reserveCount{}
	}
	events = s.passReserve.events + s.passCharge.events
	return events, events*limit.Reserve.BytesPerEvent + s.passReserve.bytes + s.passCharge.bytes
}

// quickReserve is an O(1) bound on failReserve: it counts every task, await,
// handler, pin and fan-out as if each were still to be paid for. Loose for a
// large run; it only spares small and young runs the per-pass count.
func (s *runState) quickReserve(inFlight map[string]workflow.Future, r HistoryReserve) (events, bytes int64) {
	events = r.Fixed +
		r.PerInFlight*int64(len(inFlight)+len(s.pendingItemPublishes)) +
		r.PerPending*int64(len(s.order)) +
		r.PerAwait*int64(len(s.awaits)) +
		r.PerHandler*s.handlerCount +
		r.PerOpenPin*int64(len(s.pins)) +
		r.PerFanOut*int64(len(s.eaches))
	bytes = events*r.BytesPerEvent + r.PerInFlightBytes*int64(len(inFlight)) + s.handlerStartBytes
	return events, bytes
}

// historyCrossing is where the run stands against percent% of its limit:
// the history this workflow task will have recorded, and the reserve the
// decision used — the quick bound when it does not cross, else the per-pass
// count.
type historyCrossing struct {
	events, bytes               int64
	reserveEvents, reserveBytes int64
	maxEvents, maxBytes         int64
}

func (c historyCrossing) crossed() bool {
	return c.events+c.reserveEvents >= c.maxEvents || c.bytes+c.reserveBytes >= c.maxBytes
}

// crossing measures the run against percent% of its limit. ok is false for a
// run without a limit.
func (s *runState) crossing(ctx workflow.Context, inFlight map[string]workflow.Future, percent int64) (c historyCrossing, ok bool) {
	limit := s.historyLimit()
	if limit == nil {
		return historyCrossing{}, false
	}
	c.events, c.bytes = s.historyNow(ctx)
	c.maxEvents, c.maxBytes = limit.MaxEvents*percent/100, limit.MaxBytes*percent/100
	c.reserveEvents, c.reserveBytes = s.quickReserve(inFlight, limit.Reserve)
	if !c.crossed() {
		return c, true
	}
	c.reserveEvents, c.reserveBytes = s.reserveNow(inFlight)
	return c, true
}

// nearHistoryLimit reports whether historyNow plus what this run's own failure
// path would still append reaches percent% of the plan's ceiling, in events or
// bytes. A quick O(1) bound first (it counts every task, so it is loose for a
// large run and only spares small and young runs any count); then reserveNow.
// Shared by the guard (percent = 100) and the hold (updateContinuation,
// percent = the budget's HoldPercent).
func (s *runState) nearHistoryLimit(ctx workflow.Context, inFlight map[string]workflow.Future, percent int64) bool {
	c, ok := s.crossing(ctx, inFlight, percent)
	return ok && c.crossed()
}

// historyExhausted reports, as a non-retryable ApplicationError of type
// RUN_HISTORY_LIMIT, that nearHistoryLimit(…, 100) holds. The version is asked
// LAZILY, the first time the threshold is crossed (precedents: lean-event-path,
// cancel-siblings-on-run-end); a history recorded without the guard replays the
// crossing with DefaultVersion and never fails here.
func (s *runState) historyExhausted(ctx workflow.Context, inFlight map[string]workflow.Future) error {
	if s.guardOff || historyGuardDisabledForTest {
		return nil
	}
	c, ok := s.crossing(ctx, inFlight, 100)
	if !ok || !c.crossed() {
		return nil
	}
	if !s.guardAdopted(ctx) {
		return nil
	}
	blockers := s.historyBlockers(inFlight)
	message := historyLimitMessage(s.runID, c.events, c.maxEvents, c.bytes, c.maxBytes,
		c.reserveEvents, blockers, s.carryReason())
	detail := HistoryCrossingDetail{
		RawEvents: int64(s.hist.seenLength), RawBytes: c.bytes - s.hist.pendingBytes,
		Events: c.events, Bytes: c.bytes,
		ReserveEvents: c.reserveEvents, ReserveBytes: c.reserveBytes,
		MaxEvents: c.maxEvents, MaxBytes: c.maxBytes,
		InFlight: int64(len(inFlight)), DetachedPublishes: int64(len(s.pendingItemPublishes)),
	}
	// Operator-facing, replay-safe: the guard's own accounting at the
	// crossing.
	workflow.GetLogger(ctx).Warn("run reached its history limit",
		"run_id", s.runID, "events", c.events, "bytes", c.bytes,
		"reserve_events", c.reserveEvents, "reserve_bytes", c.reserveBytes,
		"max_events", c.maxEvents, "max_bytes", c.maxBytes, "in_flight", len(inFlight))
	return temporal.NewNonRetryableApplicationError(message, RunHistoryLimitCode, nil, blockers, detail)
}

// HistoryCrossingDetail is the guard's own accounting where a run crossed
// its line: the second detail of the RUN_HISTORY_LIMIT workflow failure
// (the first is the blockers sentence). Operator-facing only — it is what a
// history inspection, and the reserve's validation test, read to compare the
// reserve the guard kept back with what the run's failure path then used.
type HistoryCrossingDetail struct {
	// RawEvents and RawBytes are the history the workflow task that crossed
	// started with; Events and Bytes add what that task had already issued.
	RawEvents         int64 `json:"raw_events"`
	RawBytes          int64 `json:"raw_bytes"`
	Events            int64 `json:"events"`
	Bytes             int64 `json:"bytes"`
	ReserveEvents     int64 `json:"reserve_events"`
	ReserveBytes      int64 `json:"reserve_bytes"`
	MaxEvents         int64 `json:"max_events"`
	MaxBytes          int64 `json:"max_bytes"`
	InFlight          int64 `json:"in_flight"`
	DetachedPublishes int64 `json:"detached_publishes"`
}

// guardAdopted asks the guard's version once a line is crossed. A history
// that recorded no marker at that point predates the guard: it is switched
// off for the rest of the run, so the recorded commands replay.
func (s *runState) guardAdopted(ctx workflow.Context) bool {
	if historyGuardForcedForTest {
		return true
	}
	if s.version(ctx, HistoryGuardChange, 1) < 1 {
		s.guardOff = true
		return false
	}
	return true
}

// handlerWouldExhaustHistory is check point 4, the failing phase's: before a
// failure handler is dispatched, a terminal-only check — the history now,
// Fixed, every in-flight entry's completion and every still-pending
// handler's markCanceled — plus, in bytes, the handler's own start. O(1):
// pendingHandlers is counted once when the phase starts and decremented as
// each handler leaves PENDING.
func (s *runState) handlerWouldExhaustHistory(ctx workflow.Context, inFlight map[string]workflow.Future, id string) bool {
	if s.guardOff || historyGuardDisabledForTest {
		return false
	}
	limit := s.historyLimit()
	if limit == nil {
		return false
	}
	r := limit.Reserve
	events, bytes := s.historyNow(ctx)
	reserve := r.Fixed + r.PerInFlight*int64(len(inFlight)) + r.PerPending*s.pendingHandlers
	reserveBytes := reserve*r.BytesPerEvent + r.PerInFlightBytes*int64(len(inFlight)) +
		handlerStartBytesOf(s.tasks[id], r)
	if events+reserve < limit.MaxEvents && bytes+reserveBytes < limit.MaxBytes {
		return false
	}
	return s.guardAdopted(ctx)
}

// cancelHandlersAtHistoryLimit marks every failure handler still PENDING
// CANCELED, because the run's history would not hold its start. Handlers
// already in flight finish; their cost is reserved.
func (s *runState) cancelHandlersAtHistoryLimit(ctx workflow.Context) {
	for _, id := range s.order {
		if s.taskRuns[id].GetStatus() != orkstackv1.Status_STATUS_PENDING {
			continue
		}
		if err := s.markCanceled(ctx, id, handlerCanceledAtHistoryLimit); err != nil {
			workflow.GetLogger(ctx).Error("mark canceled failed",
				"run_id", s.runID, "task_id", id, "error", err)
		}
	}
	s.pendingHandlers = 0
}

// historyBlockers names what the run still had going when it stopped: what
// was in flight (a fan-out item as task[index]), the pins open and the awaits
// open, at most historyLimitListMax ids per list, in declaration order. It is
// the run error's details and the message's last sentence. O(T), once, at
// the failure.
func (s *runState) historyBlockers(inFlight map[string]workflow.Future) string {
	var running, pins, awaiting []string
	for _, id := range s.order {
		if _, ok := inFlight[id]; ok {
			running = append(running, id)
		}
		if e := s.eaches[id]; e != nil {
			for index := int32(0); index < e.count; index++ {
				if _, ok := inFlight[itemFlightKey(id, index)]; ok {
					running = append(running, fmt.Sprintf("%s[%d]", id, index))
				}
			}
		}
		if a := s.awaits[id]; a != nil && a.open {
			awaiting = append(awaiting, id)
		}
	}
	for _, name := range s.pinOrder {
		if p := s.pins[name]; p.opened && (p.closed == nil || !p.closed.IsReady()) {
			pins = append(pins, name)
		}
	}
	var parts []string
	for _, list := range []struct {
		label string
		ids   []string
	}{{"still running", running}, {"pins open", pins}, {"awaiting", awaiting}} {
		if len(list.ids) > 0 {
			parts = append(parts, list.label+": "+boundedList(list.ids))
		}
	}
	return strings.Join(parts, "; ")
}

// boundedList joins at most historyLimitListMax ids, then "…".
func boundedList(ids []string) string {
	if len(ids) <= historyLimitListMax {
		return strings.Join(ids, ", ")
	}
	return strings.Join(ids[:historyLimitListMax], ", ") + ", …"
}

// historyLimitMessage is the run's sentence for RUN_HISTORY_LIMIT, in run
// vocabulary only. The dimension that crossed is the events one when the
// reserve reaches the event limit, otherwise the bytes one. carryReason, when
// set, says why the run could not carry on past this point.
func historyLimitMessage(runID string, events, maxEvents, bytes, maxBytes, reserveEvents int64,
	blockers, carryReason string,
) string {
	var b strings.Builder
	if events+reserveEvents >= maxEvents {
		fmt.Fprintf(&b, "run %s stopped before reaching the cluster's history limit: %s of %s events recorded, "+
			"%s kept back so it could end cleanly",
			runID, groupDigits(events), groupDigits(maxEvents), groupDigits(maxEvents-events))
	} else {
		fmt.Fprintf(&b, "run %s stopped before reaching the cluster's history limit: %s of %s MiB of history "+
			"recorded, %s MiB kept back so it could end cleanly",
			runID, mebibytes(bytes), mebibytes(maxBytes), mebibytes(maxBytes-bytes))
	}
	if blockers != "" {
		b.WriteString(". ")
		b.WriteString(strings.ToUpper(blockers[:1]))
		b.WriteString(blockers[1:])
	}
	if carryReason != "" {
		b.WriteString("; ")
		b.WriteString(carryReason)
	}
	return b.String()
}

// groupDigits renders n with thousands separators: 51200 is "51,200".
func groupDigits(n int64) string {
	if n < 0 {
		return "-" + groupDigits(-n)
	}
	digits := strconv.FormatInt(n, 10)
	var b strings.Builder
	for i, d := range digits {
		if i > 0 && (len(digits)-i)%3 == 0 {
			b.WriteByte(',')
		}
		b.WriteRune(d)
	}
	return b.String()
}

// mebibytes renders a byte count in MiB to one decimal.
func mebibytes(n int64) string {
	return strconv.FormatFloat(float64(n)/(1<<20), 'f', 1, 64)
}

// isHistoryLimit reports whether cause is the guard's own error — never a
// task's, a child run's or an item's that merely carries the type.
func isHistoryLimit(cause error) bool {
	var applicationErr *temporal.ApplicationError
	if !errors.As(cause, &applicationErr) || applicationErr.Type() != RunHistoryLimitCode {
		return false
	}
	var activityErr *temporal.ActivityError
	var childErr *temporal.ChildWorkflowExecutionError
	return !errors.As(cause, &activityErr) && !errors.As(cause, &childErr)
}

// historyLimitFailure is the run's error for the guard's cause.
func historyLimitFailure(cause error) *orkstackv1.Error {
	var applicationErr *temporal.ApplicationError
	_ = errors.As(cause, &applicationErr)
	var blockers string
	if applicationErr.HasDetails() {
		_ = applicationErr.Details(&blockers)
	}
	return &orkstackv1.Error{
		Code:      RunHistoryLimitCode,
		Message:   applicationErr.Message(),
		Details:   blockers,
		Category:  orkstackv1.FailureCategory_FAILURE_CATEGORY_INFRASTRUCTURE_ERROR,
		Retryable: false,
	}
}
