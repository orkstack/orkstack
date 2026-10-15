package engine

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/task"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// A run's continue-as-new chain: each Temporal execution of it is one
// generation, all under one workflow id — its run id. Generation N+1 starts
// with the carry (orkstack.v1.RunContinuation, RunWorkflow's fifth argument)
// generation N handed it. Every word here — generation, carry, checkpoint,
// segment — is internal (AGENTS.md §7 vocabulary): user-facing text speaks of
// the run's "saved state", and of a run that "carries on" or is "carried
// forward".
//
// A run whose plan carries a continuation budget (RunPlan.Continuation)
// continues as new once its history has grown past it, at its next moment
// with nothing of it in flight (continueAsNew, the one switch site), and its
// next generation starts from the saved state the switch wrote (restore,
// continuation_restore.go). A carried start this release cannot continue —
// an unknown carry format, a malformed carry or plan, saved state that fails
// verification or cannot be read — ends explicitly, with a published
// terminal event (endUnrestorable), so a chain never silently re-runs.

// ContinuationCarryVersion is the only carry format this release knows.
const ContinuationCarryVersion int32 = 1

// The run's own codes for a generation that could not start from its saved
// state. Each is a non-retryable ApplicationError type and the run's
// Error.code; the reader (closedExecution) and a composed parent
// (childFailure) map them.
const (
	// RunStateUnsupportedCode: the saved state's format is one this release
	// cannot carry on — a rollback below the release that wrote it.
	RunStateUnsupportedCode = "RUN_STATE_UNSUPPORTED"
	// RunStateCorruptCode: the saved state failed verification and was not
	// used.
	RunStateCorruptCode = "RUN_STATE_CORRUPT"
	// RunStateUnavailableCode: the saved state could not be read from object
	// storage in time.
	RunStateUnavailableCode = "RUN_STATE_UNAVAILABLE"
)

// ActivityWriteFailedRunSnapshot is the registered name of the local activity
// that writes the terminal snapshot of a generation that could not start.
const ActivityWriteFailedRunSnapshot = "WriteFailedRunSnapshot"

// runStateCategory is the failure category of each RUN_STATE_* code.
func runStateCategory(code string) orkstackv1.FailureCategory {
	if code == RunStateUnavailableCode {
		return orkstackv1.FailureCategory_FAILURE_CATEGORY_INFRASTRUCTURE_ERROR
	}
	return orkstackv1.FailureCategory_FAILURE_CATEGORY_CONTRACT_VIOLATION
}

// isRunStateCode reports whether code is one of the RUN_STATE_* codes.
func isRunStateCode(code string) bool {
	switch code {
	case RunStateUnsupportedCode, RunStateCorruptCode, RunStateUnavailableCode:
		return true
	}
	return false
}

// unsupportedCarryMessage is the refusal of a carry this release cannot
// continue: run vocabulary only.
func unsupportedCarryMessage(version int32) string {
	return fmt.Sprintf("this release cannot carry on a run that a newer release carried forward "+
		"(saved-state format %d); roll the engines forward", version)
}

// endUnrestorable ends a carried generation that cannot continue, explicitly:
// the run is FAILED with code (or CANCELED, when the run was being canceled),
// a terminal snapshot is written at the first sequence no emit owns, and the
// terminal event is published there, so the run reaches the archive.
//
// It reads only the carry's fields, through nil-safe proto getters, and never
// dereferences a plan pointer: a malformed carry or plan must end the run,
// never panic the workflow task into a retry loop (a stuck run, AGENTS.md
// §11).
func (s *runState) endUnrestorable(
	ctx workflow.Context,
	carry *orkstackv1.RunContinuation,
	code, message string,
) (*orkstackv1.Run, error) {
	canceled := ctx.Err() != nil || code == "RUN_CANCELED"
	ectx, cancel := workflow.NewDisconnectedContext(ctx)
	defer cancel()

	s.eventSeq = int(max(0, carry.GetEventSequence()))
	s.createdAt = carry.GetCreatedAt()
	s.startedAt = carry.GetStartedAt()
	s.finished = timestamppb.New(workflow.Now(ectx))
	s.status = orkstackv1.Status_STATUS_FAILED
	// The category comes from the same mapping the reader applies to the
	// execution's close (runLevelFailureCategory), so a RUN_DEFINITION_* or
	// RUN_STATE_* code reads with one category whichever generation it ends
	// and whichever path serves it (AGENTS.md §4).
	s.runError = &orkstackv1.Error{Code: code, Message: message, Category: runLevelFailureCategory(code)}
	eventType := orkstackv1.EventType_EVENT_TYPE_RUN_FAILED
	if canceled {
		s.status = orkstackv1.Status_STATUS_CANCELED
		s.runError = &orkstackv1.Error{Code: "RUN_CANCELED", Message: "run canceled",
			Category: orkstackv1.FailureCategory_FAILURE_CATEGORY_CANCELLED}
		eventType = orkstackv1.EventType_EVENT_TYPE_RUN_CANCELED
	}

	// The switch snapshot is used only when it names exactly this run's own
	// object of the generation that closed; anything else is treated as
	// absent, and nothing is read from it.
	namespace := s.wf.GetNamespace()
	base := carry.GetSnapshot()
	if base != nil && !artifacts.ContinuationURIMatches(base.GetUri(), namespace, s.runID,
		carry.GetGeneration()-1, artifacts.ContinuationKindSnapshot, base.GetSha256()) {
		base = nil
	}
	sequence := int64(s.eventSeq) + 1
	localCtx := workflow.WithLocalActivityOptions(ectx, workflow.LocalActivityOptions{
		StartToCloseTimeout:    snapshotAttemptTimeout,
		ScheduleToCloseTimeout: snapshotActivityTimeout,
		RetryPolicy:            &temporal.RetryPolicy{MaximumAttempts: 3},
	})
	var snapshotURI string
	if err := workflow.ExecuteLocalActivity(localCtx, ActivityWriteFailedRunSnapshot,
		namespace, s.runID, base, s.snapshot(), sequence).Get(localCtx, &snapshotURI); err != nil {
		// Object storage is down: the event is not published, and the reader
		// synthesizes the run's end from the execution's own close, mapped to
		// the same code (closedExecution).
		workflow.GetLogger(ctx).Error("write terminal snapshot of a run that cannot carry on failed",
			"run_id", s.runID, "code", s.runError.GetCode(), "error", err)
	} else {
		s.eventSeq = int(sequence)
		event := &orkstackv1.Event{
			RunId:     s.runID,
			Sequence:  sequence,
			Type:      eventType,
			Status:    s.status,
			Message:   s.runError.GetMessage(),
			Timestamp: timestamppb.New(workflow.Now(ectx)),
		}
		activityCtx := workflow.WithActivityOptions(ectx, eventActivityOptions())
		if err := workflow.ExecuteActivity(activityCtx, ActivityPublishEvent, event, snapshotURI,
			fmt.Sprintf("%s#%d", s.runID, sequence)).Get(ectx, nil); err != nil {
			workflow.GetLogger(ctx).Error("publish event failed",
				"run_id", s.runID, "event", eventType.String(), "error", err)
		}
	}
	if canceled {
		return s.snapshot(), temporal.NewCanceledError()
	}
	return s.snapshot(), temporal.NewNonRetryableApplicationError(message, code, nil)
}

// WriteFailedRunSnapshot writes the terminal snapshot of a generation that
// could not start from its saved state, at sequence: the first one no emit of
// any generation owns. It starts from base — the switch snapshot, the full
// state the run had when it switched — verified by size and SHA-256, overlays
// the header's status, error and finished_at, and marks every row that was
// not terminal at the switch CANCELED with the run's reason; a row that was
// terminal keeps its outcome. A base that is absent, unreadable or fails
// verification leaves the header alone, with no task rows. The overlay is a
// pure function of its inputs, so a retried write writes the same bytes.
func (a *SnapshotActivities) WriteFailedRunSnapshot(
	ctx context.Context,
	namespace, runID string,
	base *orkstackv1.ContinuationObject,
	header *orkstackv1.Run,
	sequence int64,
) (string, error) {
	if header == nil {
		return "", fmt.Errorf("engine: write failed run snapshot: header is required")
	}
	run := proto.Clone(header).(*orkstackv1.Run)
	run.TaskRuns = nil
	if base != nil {
		if prior, err := a.readSwitchSnapshot(ctx, base); err == nil {
			run.TaskRuns = cancelUnfinishedRows(prior.GetTaskRuns(), header.GetFinishedAt(),
				header.GetError().GetCode(), header.GetError().GetMessage())
		}
	}
	data, err := proto.Marshal(run)
	if err != nil {
		return "", fmt.Errorf("engine: marshal failed run snapshot for run %s: %w", runID, err)
	}
	uri, err := a.store.WriteSnapshot(ctx, namespace, runID, sequence, data)
	if err != nil {
		return "", fmt.Errorf("engine: persist failed run snapshot for run %s: %w", runID, err)
	}
	// A restore that failed is counted once, by its code (ork.run.continuations).
	if code := header.GetError().GetCode(); isRunStateCode(code) {
		a.observeRestore(ctx, ContinuationRestoreObservation{Namespace: namespace, Outcome: code})
	}
	return uri, nil
}

func (a *SnapshotActivities) readSwitchSnapshot(ctx context.Context, base *orkstackv1.ContinuationObject) (*orkstackv1.Run, error) {
	data, err := a.store.ReadSnapshot(ctx, base.GetUri())
	if err != nil {
		return nil, err
	}
	return DecodeSwitchSnapshot(data, base)
}

// cancelUnfinishedRows marks every row and item that is not terminal
// CANCELED, with the run's reason; terminal rows keep their outcome.
func cancelUnfinishedRows(rows []*orkstackv1.TaskRun, finished *timestamppb.Timestamp, code, message string) []*orkstackv1.TaskRun {
	reason := &orkstackv1.Error{Code: code, Message: "the run stopped before this task finished: " + message}
	cancelRow := func(row *orkstackv1.TaskRun) {
		if isTerminal(row.GetStatus()) {
			return
		}
		row.Status = orkstackv1.Status_STATUS_CANCELED
		row.Error = proto.Clone(reason).(*orkstackv1.Error)
		if row.GetFinishedAt() == nil {
			row.FinishedAt = finished
		}
	}
	for _, row := range rows {
		for _, item := range row.GetItems() {
			cancelRow(item)
		}
		cancelRow(row)
	}
	return rows
}

// ContinueAsNewChange is the workflow.GetVersion change id of the writer. It
// is asked LAZILY, the first time a generation 0 grows past its budget: a run
// that never does records no marker, and a history recorded before the writer
// replays the crossing with DefaultVersion and never switches. A carried
// generation holds the decision in its carry and never asks it again.
const ContinueAsNewChange = "continue-as-new"

// continuationReadYield is the timer that ends a bootstrap batch's workflow
// task when more of the saved state is to be read: the eachDispatchYield
// precedent. Only whether it exists is replay-visible (the replayer matches a
// timer by its id), and ChunksPerTask decides that.
const continuationReadYield = time.Millisecond

// checkpointPartRawBytes is the decoded size a checkpoint part closes below
// (a row larger than it goes alone). A local activity's output layout, which
// its recorded result fixes: never a command-stream number.
const checkpointPartRawBytes = 1 << 20

// The local activities of a switch's writes and of a bootstrap's reads.
// Internal: no user-facing surface names them.
const (
	ActivityWriteEventSegment     = "WriteEventSegment"
	ActivityWriteSwitchSnapshot   = "WriteSwitchSnapshot"
	ActivityWriteCheckpoint       = "WriteCheckpoint"
	ActivityReadContinuationChunk = "ReadContinuationChunk"
)

// A switch's write budget: 30 s per attempt, three attempts, two minutes end
// to end. A failed write never switches; the run tries again after more
// growth (failedAttempt).
const (
	continuationWriteAttemptTimeout = 30 * time.Second
	continuationWriteTimeout        = 120 * time.Second
)

// TEST-ONLY switches, the precedent of eachDispatchDescending. Never set in
// production code.
//
//   - continuationPlanForTest: the budget every run uses instead of its
//     plan's, so a negative replay test can hand a committed history one.
//   - continueAsNewForcedForTest: the writer takes version 1 WITHOUT asking
//     workflow.GetVersion, so a negative replay test proves the committed
//     fixtures can only replay because of the gate.
//   - continuationAtEveryIdleForTest: every natural idle point is due, so a
//     test can switch (and restore) at each one.
var (
	continuationPlanForTest        *ContinuationBudget
	continueAsNewForcedForTest     = false
	continuationAtEveryIdleForTest = false
)

// continuationBudget is the budget this run decides with: its plan's, or nil.
func (s *runState) continuationBudget() *ContinuationBudget {
	if continuationPlanForTest != nil {
		return continuationPlanForTest
	}
	return s.plan.Continuation
}

// reached reports whether history has grown to at in EITHER dimension
// (events >= at[0] OR bytes >= at[1]). The zero value is always reached.
func reached(events, bytes int64, at [2]int64) bool { return events >= at[0] || bytes >= at[1] }

// recordEvent keeps one event this generation published, for the segment
// its switch writes. Called where PublishEvent is scheduled, and only for a
// run that can continue: an event whose snapshot write failed is never
// published and never recorded, so the segment holds exactly what Temporal
// recorded in this generation.
func (s *runState) recordEvent(event *orkstackv1.Event) {
	if s.continuationBudget() == nil {
		return
	}
	s.genEvents = append(s.genEvents, event)
}

// markGenerationBase records the history this generation starts its growth
// from: after the bootstrap for a carried generation, so the saved state's
// chunk markers are part of the base, never the growth. The copy itself is
// bounded by its cap and always counted by the guard and the hold line.
func (s *runState) markGenerationBase(ctx workflow.Context) {
	s.baseEvents, s.baseBytes = s.historyNow(ctx)
}

// logFields is the replay-safe log line's fields: the run, its history now,
// what it has in flight, and extra.
func (s *runState) logFields(ctx workflow.Context, inFlight map[string]workflow.Future, extra ...any) []any {
	e, b := s.historyNow(ctx)
	fields := []any{"run_id", s.runID, "history_events", e, "history_bytes", b, "in_flight", len(inFlight)}
	return append(fields, extra...)
}

// logInfo writes one replay-aware Info line: the SDK suppresses it while
// replaying, so it is written once per decision. Operator-only (AGENTS.md
// §7): it may name what no user-facing surface does.
func (s *runState) logInfo(ctx workflow.Context, inFlight map[string]workflow.Future, message string, extra ...any) {
	workflow.GetLogger(ctx).Info(message, s.logFields(ctx, inFlight, extra...)...)
}

// updateContinuation decides, from history this workflow task will have
// recorded, whether the generation's switch is due and whether to hold new
// dispatch to reach an idle point. O(1) after the first call of a pass: the
// hold line reads reserveNow.
//
// Past its budget a run switches at its NEXT natural idle point and never
// pauses work to make one. It holds new dispatch only once its history plus
// its failure path's reserve reaches HoldPercent of the cluster's limit, in
// events or in bytes — a wide run, whose reserve is large, reaches that line
// earlier, automatically.
func (s *runState) updateContinuation(ctx workflow.Context, inFlight map[string]workflow.Future) {
	budget, limit := s.continuationBudget(), s.historyLimit()
	if budget == nil || limit == nil || s.noSwitch != "" {
		return // a hand-built plan with a budget but no ceiling never continues
	}
	if !budget.valid() {
		s.noSwitch = "its plan's continuation budget is malformed" // no command: a hand-built plan only
		return
	}
	e, b := s.historyNow(ctx)
	grown := e-s.baseEvents >= budget.Events || b-s.baseBytes >= budget.Bytes
	if continuationAtEveryIdleForTest {
		// Every idle point after the generation's first command is due.
		grown = e > s.baseEvents
	}
	if !grown {
		return
	}
	if !s.switchDue {
		if !s.writerAdopted(ctx) {
			s.noSwitch = "recorded by a build without continuation"
			return
		}
		s.switchDue = true // switches at the next natural idle point (canSwitchNow)
		s.logInfo(ctx, inFlight, "run will carry on into a fresh history at its next idle point",
			"budget_events", budget.Events, "budget_bytes", budget.Bytes)
	}
	if s.generation+1 >= budget.MaxGenerations {
		// User-facing through carryReason, so it names no count (A2); the
		// operator's log line carries the generation.
		s.setNoSwitch(ctx, inFlight, generationCeilingReason)
		return
	}
	if s.quiescing || !reached(e, b, s.quiesceRetryAt) || !reached(e, b, s.switchRetryAt) {
		return
	}
	if s.nearHistoryLimit(ctx, inFlight, budget.HoldPercent) {
		s.quiescing = true
		s.holdStartedAt = workflow.Now(ctx)
		reserveEvents, _ := s.reserveNow(inFlight)
		s.logInfo(ctx, inFlight, "run pausing new tasks so it can carry on into a fresh history",
			"reserve_events", reserveEvents, "held", len(s.readyTasks()))
	}
}

// writerAdopted asks the writer's version once a generation 0 crosses its
// budget; a carried generation answers from its carry.
func (s *runState) writerAdopted(ctx workflow.Context) bool {
	if continueAsNewForcedForTest {
		if s.versions == nil {
			s.versions = map[string]workflow.Version{}
		}
		s.versions[ContinueAsNewChange] = 1
		return true
	}
	return s.version(ctx, ContinueAsNewChange, 1) >= 1
}

// setNoSwitch stops this generation from ever switching, and says why. It
// clears quiescing: a hold that can no longer end in a switch must not hold.
func (s *runState) setNoSwitch(ctx workflow.Context, inFlight map[string]workflow.Future, reason string) {
	s.noSwitch = reason
	s.quiescing = false
	workflow.GetLogger(ctx).Warn("run will not carry on into a fresh history",
		s.logFields(ctx, inFlight, "reason", reason, "generation", s.generation)...)
}

// generationCeilingReason is noSwitch at the plan's MaxGenerations. It
// reaches RUN_HISTORY_LIMIT's message, a user-facing surface, so it never
// says how many times the run carried forward (AGENTS.md §7, A2).
const generationCeilingReason = "it has already carried forward as many times as a run may"

// failedAttempt records a switch whose writes failed (object storage
// unavailable): the hold ends, and no switch is tried again until the run
// has grown by its retry spacing. If the store stays down, the next emit's
// snapshot write fails the run exactly as it always has — never a silent
// success.
func (s *runState) failedAttempt(ctx workflow.Context, inFlight map[string]workflow.Future, err error) {
	budget := s.continuationBudget()
	e, b := s.historyNow(ctx)
	s.quiescing = false
	s.switchRetryAt = [2]int64{e + budget.RetryEvents, b + budget.RetryBytes}
	workflow.GetLogger(ctx).Warn("run could not save its state to carry on; retrying after more growth",
		s.logFields(ctx, inFlight, "error", err)...)
}

// hasUnfinishedTasks reports whether any task is not terminal.
func (s *runState) hasUnfinishedTasks() bool {
	for _, id := range s.order {
		if !isTerminal(s.taskRuns[id].GetStatus()) {
			return true
		}
	}
	return false
}

// canSwitchNow is the switch condition: the switch is due and allowed, and
// nothing of the run is in flight — no activity, item or child run, and no
// pin opening, open or closing — while work remains and the run is not being
// canceled. Open awaits may cross: their deadlines are absolute instants.
func (s *runState) canSwitchNow(ctx workflow.Context, inFlight map[string]workflow.Future) bool {
	if !s.switchDue || s.noSwitch != "" || len(inFlight) != 0 || ctx.Err() != nil {
		return false
	}
	e, b := s.historyNow(ctx)
	return reached(e, b, s.switchRetryAt) && s.openingPins() == 0 && s.closingPins() == 0 &&
		s.openedUnclosedPins() == 0 && s.hasUnfinishedTasks()
}

// applyQueuedResolutions applies every resolution already delivered to the
// run — real resolutions, unlike drainResolutions' — so none is left
// unread in a generation that ends. applied says state changed; a resolve
// error is the task's.
func (s *runState) applyQueuedResolutions(ctx workflow.Context) (applied bool, taskID string, err error) {
	if s.resolutions == nil {
		return false, "", nil
	}
	for {
		var sig *orkstackv1.AwaitSignal
		if !s.resolutions.ReceiveAsync(&sig) {
			return applied, "", nil
		}
		applied = true
		if err := s.resolve(ctx, sig); err != nil {
			return true, sig.GetTaskId(), err
		}
	}
}

// ContinuationObservationInput is what a switch's or a bootstrap's local
// activity is told so it can measure itself (ContinuationObserver). A local
// activity's input is never recorded in history, so it costs nothing.
type ContinuationObservationInput struct {
	Namespace        string    `json:"namespace"`
	Generation       int64     `json:"generation"`
	HoldSeconds      float64   `json:"hold_seconds,omitempty"`
	RestoreStartedAt time.Time `json:"restore_started_at,omitempty"`
	Final            bool      `json:"final,omitempty"`
}

// continuationWriteContext is the local-activity options of a switch's
// writes.
func continuationWriteContext(ctx workflow.Context) workflow.Context {
	return workflow.WithLocalActivityOptions(ctx, workflow.LocalActivityOptions{
		StartToCloseTimeout:    continuationWriteAttemptTimeout,
		ScheduleToCloseTimeout: continuationWriteTimeout,
		RetryPolicy:            &temporal.RetryPolicy{MaximumAttempts: 3},
	})
}

// continueAsNew is the one switch site. It writes the generation's event
// segment, a full snapshot of its exact state and its checkpoint, each
// content-addressed and create-if-absent, then hands the next generation a
// small carry. canErr is the continue-as-new to return; fatal is a
// resolution's failure the run must take; all nil means the attempt changed
// state or was given up, and the loop goes on. No step after the last
// resolution check blocks, so that check and the continue-as-new command
// share one workflow task, and a signal recorded after that task started
// makes the server reject the command, so the retried task sees it.
func (s *runState) continueAsNew(ctx workflow.Context, inFlight map[string]workflow.Future) (canErr error, fatalTask string, fatal error) {
	budget := s.continuationBudget()
	// 1. Every detached publish, so nothing of the run is in flight.
	if len(s.pendingItemPublishes) > 0 {
		s.drainItemPublishes(ctx)
	}
	// 2. Every resolution already delivered: applied, so the switch carries
	// the state it made.
	if applied, id, err := s.applyQueuedResolutions(ctx); err != nil {
		return nil, id, err
	} else if applied {
		return nil, "", nil
	}

	namespace := s.wf.GetNamespace()
	obs := ContinuationObservationInput{Namespace: namespace, Generation: s.generation}
	if s.quiescing {
		obs.HoldSeconds = workflow.Now(ctx).Sub(s.holdStartedAt).Seconds()
	}
	writeCtx := continuationWriteContext(ctx)

	// 3. The event segment: exactly what this generation published.
	segment := &orkstackv1.RunEventSegment{
		RunId:         s.runID,
		Generation:    s.generation,
		FirstSequence: s.genFirstSeq,
		LastSequence:  int64(s.eventSeq),
		Events:        s.genEvents,
	}
	var segmentObj *orkstackv1.ContinuationObject
	if err := workflow.ExecuteLocalActivity(writeCtx, ActivityWriteEventSegment, segment, namespace, obs).
		Get(writeCtx, &segmentObj); err != nil {
		s.failedAttempt(ctx, inFlight, err)
		return nil, "", nil
	}

	// 4. The switch snapshot: a full snapshot of the exact state, items
	// included, under a content-addressed key — never a {seq}.pb key, which
	// the next generation's own emits own. A copy: a local activity's Get
	// returns as soon as a cancel reaches it while the activity runs on, so
	// the writes must never encode the live rows fail() would then change.
	run := proto.Clone(s.snapshot()).(*orkstackv1.Run)
	var switchSnapshot *orkstackv1.ContinuationObject
	if err := workflow.ExecuteLocalActivity(writeCtx, ActivityWriteSwitchSnapshot, run, namespace, s.generation, obs).
		Get(writeCtx, &switchSnapshot); err != nil {
		s.failedAttempt(ctx, inFlight, err)
		return nil, "", nil
	}

	// 5. The checkpoint, refused over its cap — which a definition passed
	// by reference shares: the next generation copies both into its
	// history (AGENTS.md §2.2).
	definition := s.plan.definitionBytes()
	var checkpoint *orkstackv1.ContinuationObject
	if err := workflow.ExecuteLocalActivity(writeCtx, ActivityWriteCheckpoint, run, s.generation,
		budget.CheckpointMaxBytes-definition, obs).Get(writeCtx, &checkpoint); err != nil {
		s.failedAttempt(ctx, inFlight, err)
		return nil, "", nil
	}
	if copied := checkpoint.GetSizeBytes() + definition; copied > budget.CheckpointMaxBytes {
		s.setNoSwitch(ctx, inFlight, fmt.Sprintf("its saved state is %s, more than the %s a run carries forward",
			savedStateSize(copied), savedStateSize(budget.CheckpointMaxBytes)))
		return nil, "", nil
	}

	// 6. Re-check: a resolution delivered while the writes blocked, or a
	// cancel, turns the switch back; the written objects are orphans,
	// reclaimed with the run's prefix.
	if applied, id, err := s.applyQueuedResolutions(ctx); err != nil {
		return nil, id, err
	} else if applied || ctx.Err() != nil {
		return nil, "", nil
	}

	// 7. The carry, refused over AGENTS.md §2.2's bound before any command.
	decisions := make(map[string]int32, len(s.versions))
	for change, v := range s.versions {
		decisions[change] = int32(v)
	}
	carry := &orkstackv1.RunContinuation{
		Version:       ContinuationCarryVersion,
		Generation:    s.generation + 1,
		EventSequence: int64(s.eventSeq),
		CreatedAt:     s.createdAt,
		StartedAt:     s.startedAt,
		Checkpoint:    checkpoint,
		Segments: append(append([]*orkstackv1.EventSegmentRef(nil), s.segments...), &orkstackv1.EventSegmentRef{
			Generation:    s.generation,
			ExecutionId:   workflow.GetInfo(ctx).WorkflowExecution.RunID,
			FirstSequence: s.genFirstSeq,
			LastSequence:  int64(s.eventSeq),
			EventCount:    int64(len(s.genEvents)),
			Object:        segmentObj,
		}),
		VersionDecisions: decisions,
		Snapshot:         switchSnapshot,
	}
	if size, err := task.PayloadSize(carry); err != nil || size > task.MaxActivityInputBytes {
		s.setNoSwitch(ctx, inFlight, "its carried bookkeeping is larger than 256 KiB")
		return nil, "", nil
	}

	// 8. Generation 0 only: the run's created_at, for every list that orders
	// a chain by it (MemoCreatedAt). Non-blocking, after the carry's check,
	// so a refused carry never leaves a stray memo upsert.
	if s.generation == 0 {
		if err := workflow.UpsertMemo(ctx, map[string]any{
			MemoCreatedAt: s.createdAt.AsTime().UTC().Format(time.RFC3339Nano),
		}); err != nil {
			s.setNoSwitch(ctx, inFlight, "its creation time could not be recorded")
			return nil, "", nil
		}
	}

	// 9. The switch. The run-id argument is empty, so a release older than
	// generations fails loudly rather than run the run again; every other
	// argument is the original, which RunStart and the await doors read.
	s.logInfo(ctx, inFlight, "run carried on into a fresh history",
		"hold_seconds", obs.HoldSeconds, "state_bytes", checkpoint.GetSizeBytes(), "generation", s.generation+1)
	s.observeDecisionState("switch")
	return workflow.NewContinueAsNewError(ctx, RunWorkflowType, "", s.wfArg, s.inputArg, s.plan, carry), "", nil
}

// savedStateSize names a saved state's size, or its cap, in units that tell
// two sizes apart: MiB from 1 MiB up, KiB below — a cap near
// continuation.bytes' 16 KiB floor in MiB would read "0.1 MiB, more than
// the 0.1 MiB". A message's text, never a command-stream number.
func savedStateSize(n int64) string {
	if n >= 1<<20 {
		return mebibytes(n) + " MiB"
	}
	return strconv.FormatFloat(float64(n)/(1<<10), 'f', 1, 64) + " KiB"
}

// carryReason is what a RUN_HISTORY_LIMIT message appends when the run could
// not carry on past that point, in run vocabulary; empty when it never had
// to.
func (s *runState) carryReason() string {
	if s.continuationBudget() == nil || !s.switchDue {
		return ""
	}
	switch {
	case s.noSwitch != "" && s.noSwitch != "recorded by a build without continuation":
		return "it cannot carry on into a fresh history: " + s.noSwitch
	case s.gaveUp != "":
		return "it could not reach a moment with nothing running, which a run needs to carry on into a " +
			"fresh history, because " + s.gaveUp
	}
	return ""
}

// RestoreHistoryBound is the most history events one generation's bootstrap
// appends, from its budget alone:
//   - one marker per chunk of its largest copy: N = ceil(CheckpointMaxBytes / ChunkBytes);
//   - five events per batch yield (TimerStarted, TimerFired, and the
//     WorkflowTaskCompleted/Scheduled/Started around it), ceil(N / ChunksPerTask);
//   - 24 for its first and last workflow tasks and an endUnrestorable — by
//     count 12 of them: 3 opening its first task, its terminal snapshot's
//     marker, 7 for PublishEvent's round trip, and the close;
//   - while reads fail, at most failRate events per second for the whole
//     restore timeout, where failRate = max(3/8, 6/minServerBackoff):
//   - a workflow-task heartbeat (WorkflowTaskCompleted/Scheduled/Started)
//     every 8 s while an attempt or an in-process retry runs (80% of the
//     cluster's default 10 s workflow-task timeout);
//   - one server-timer retry cycle (failure marker, WorkflowTaskCompleted,
//     TimerStarted, TimerFired, WorkflowTaskScheduled, WorkflowTaskStarted)
//     per backoff, and only a backoff above the 10 s workflow-task timeout
//     takes a server timer — minServerBackoff is the first
//     BackoffInitial·2^k above 10 s (16 s with the defaults), at most
//     BackoffMax.
//     The two regimes never overlap in time: the task is open in the first
//     and closed in the second.
//
// A definition passed by reference shares the copy's cap with the
// checkpoint (copyOverCap) and is read as a second object first, so its d
// bytes and the checkpoint's k, d+k at most the cap, can each end in a
// partial chunk: ceil(d/C) + ceil(k/C) ≤ N + 1 markers. Both reads share one
// batch counter, so N+1 reads take floor(N/P) yields, never more than the
// ceil(N/P) charged above. The split adds that one marker and no yield, and
// the 24's spare 12 hold it (TestRestoreHistoryBound_DefinitionAndCheckpointSplitAtTheCap
// pins the worst case at every batch size).
//
// It assumes the cluster's default 10 s workflow-task timeout: StartRun sets
// none. The recreated deadline timer of each await open at the switch adds
// one more event per await to the bootstrap's history, outside this bound;
// those are counted (noteCommand) into the generation's base, and each await
// is in the guard's reserve.
func RestoreHistoryBound(b ContinuationBudget) int64 {
	if b.ChunkBytes <= 0 || b.ChunksPerTask <= 0 {
		return 0
	}
	chunks := ceilDiv(b.CheckpointMaxBytes, b.ChunkBytes)
	yields := ceilDiv(chunks, b.ChunksPerTask)
	events := chunks + 5*yields + 24
	timeout := b.Restore.TimeoutSeconds
	// The heartbeat regime: 3 events per 8 s.
	failEvents := ceilDiv(timeout*3, 8)
	// The server-timer regime: 6 events per minServerBackoff.
	const workflowTaskTimeoutMillis = 10_000
	backoff := b.Restore.BackoffInitialMillis
	maxBackoff := b.Restore.BackoffMaxSeconds * 1000
	for backoff > 0 && backoff <= workflowTaskTimeoutMillis && backoff < maxBackoff {
		backoff *= 2
	}
	backoff = min(backoff, maxBackoff)
	if backoff > workflowTaskTimeoutMillis {
		if timer := ceilDiv(timeout*6000, backoff); timer > failEvents {
			failEvents = timer
		}
	}
	return events + failEvents
}

func ceilDiv(a, b int64) int64 {
	if b <= 0 {
		return 0
	}
	return (a + b - 1) / b
}

// continuationStateHookForTest is a TEST-ONLY hook, the precedent of
// eachDispatchDescending: when set, it sees the run's decision state at each
// switch (phase "switch", just before the continue-as-new) and after each
// restore (phase "restore"), so a test can prove the two equal. Never set in
// production code.
var continuationStateHookForTest func(phase string, state DecisionState)

// DecisionState is every field a generation decides with that a switch must
// carry exactly: each task's row, the projection of each result workflow code
// reads back, each output's presence and value, each fan-out's counters,
// each await's deadline (and whether its timer can be cancelled), the event
// sequence, created_at and the version decisions. Test-only: it is built
// only for continuationStateHookForTest.
type DecisionState struct {
	TaskRuns  map[string]string
	Results   map[string]string
	Outputs   map[string]string
	Eaches    map[string][7]int32
	Awaits    map[string]string
	EventSeq  int
	CreatedAt string
	Versions  map[string]int
}

func (s *runState) observeDecisionState(phase string) {
	if continuationStateHookForTest == nil {
		return
	}
	state := DecisionState{
		TaskRuns: map[string]string{}, Results: map[string]string{}, Outputs: map[string]string{},
		Eaches: map[string][7]int32{}, Awaits: map[string]string{}, Versions: map[string]int{},
		EventSeq: s.eventSeq, CreatedAt: s.createdAt.AsTime().UTC().Format(time.RFC3339Nano),
	}
	marshal := proto.MarshalOptions{Deterministic: true}
	for _, id := range s.order {
		row, _ := marshal.Marshal(s.taskRuns[id])
		state.TaskRuns[id] = string(row)
		if r, ok := s.results[id]; ok {
			projected, _ := marshal.Marshal(&orkstackv1.ExecuteTaskResult{TaskId: r.GetTaskId(), ValuesRef: r.GetValuesRef(),
				Commit: r.GetCommit(), Artifacts: r.GetArtifacts()})
			state.Results[id] = string(projected)
		}
		if values, ok := s.outputs[id]; ok {
			encoded, _ := json.Marshal(values)
			state.Outputs[id] = string(encoded)
		}
		if e := s.eaches[id]; e != nil {
			expanded := int32(0)
			if e.expanded {
				expanded = 1
			}
			state.Eaches[id] = [7]int32{expanded, e.count, e.next, e.settled, e.inFlight, e.limit, e.stride}
		}
		if a := s.awaits[id]; a != nil && a.open {
			state.Awaits[id] = fmt.Sprintf("deadline=%s timeout=%s cancel=%t", a.deadlineAt.UTC().Format(time.RFC3339Nano),
				a.timeout, a.cancelDeadline != nil)
		}
	}
	for change, v := range s.versions {
		state.Versions[change] = int(v)
	}
	continuationStateHookForTest(phase, state)
}

// fieldClass is what a switch does with one field of the run's state.
type fieldClass int

const (
	// fieldDerived is rebuilt by newRunState from RunWorkflow's arguments.
	fieldDerived fieldClass = iota + 1
	// fieldRestored crosses the switch, in the carry or the checkpoint
	// (restore).
	fieldRestored
	// fieldEphemeral is zero or fresh at a switch — guaranteed by the switch
	// condition (nothing in flight, never in the failing phase) or rebuilt
	// without a command.
	fieldEphemeral
)

// runStateFieldClass classifies every field of the state a generation holds
// (TestRunState_EveryFieldIsClassified reflects over the structs and fails
// on any field missing here), so a field added later cannot be silently
// dropped at a switch — which matters because on an unversioned fleet the
// next generation may run a newer build.
var runStateFieldClass = map[string]fieldClass{
	// Derived: structure and spec, rebuilt from the arguments.
	"runState.runID": fieldDerived, "runState.wf": fieldDerived, "runState.plan": fieldDerived,
	"runState.input": fieldDerived, "runState.inputs": fieldDerived, "runState.order": fieldDerived,
	"runState.tasks": fieldDerived, "runState.closure": fieldDerived, "runState.leaves": fieldDerived,
	"runState.pins": fieldDerived, "runState.pinOrder": fieldDerived, "runState.awaits": fieldDerived,
	"runState.resolutions": fieldDerived, "runState.eaches": fieldDerived, "runState.eachShare": fieldDerived,
	"runState.itemInputLimit": fieldDerived, "runState.itemArtifactMax": fieldDerived,
	"runState.children": fieldDerived, "runState.hasHandlers": fieldDerived, "runState.declaredRows": fieldDerived,
	"runState.handlerCount": fieldDerived, "runState.handlerStartBytes": fieldDerived,
	"eachState.spec": fieldDerived, "awaitState.spec": fieldDerived,
	"pinState.name": fieldDerived, "pinState.pin": fieldDerived, "pinState.tasks": fieldDerived,

	// Restored: from the carry or the checkpoint.
	"runState.taskRuns": fieldRestored, "runState.results": fieldRestored, "runState.outputs": fieldRestored,
	"runState.createdAt": fieldRestored, "runState.startedAt": fieldRestored, "runState.status": fieldRestored,
	"runState.eventSeq": fieldRestored, "runState.leanDecided": fieldRestored, "runState.lean": fieldRestored,
	"runState.versions": fieldRestored, "runState.generation": fieldRestored, "runState.segments": fieldRestored,
	"runState.heldFanOuts": fieldRestored, "runState.wfArg": fieldRestored, "runState.inputArg": fieldRestored,
	"runState.genFirstSeq": fieldRestored,
	"eachState.expanded":   fieldRestored, "eachState.count": fieldRestored, "eachState.next": fieldRestored,
	"eachState.settled": fieldRestored, "eachState.limit": fieldRestored, "eachState.stride": fieldRestored,
	"awaitState.open": fieldRestored, "awaitState.timeout": fieldRestored, "awaitState.deadlineAt": fieldRestored,
	"awaitState.deadline": fieldRestored, "awaitState.cancelDeadline": fieldRestored,

	// Ephemeral: zero or fresh at a switch.
	"runState.runError": fieldEphemeral, "runState.finished": fieldEphemeral, "runState.output": fieldEphemeral,
	"runState.pendingItemPublishes": fieldEphemeral, "runState.publishCtx": fieldEphemeral,
	"runState.runCtx": fieldEphemeral, "runState.handlerCtx": fieldEphemeral, "runState.cancelHandlers": fieldEphemeral,
	"runState.detached": fieldEphemeral, "runState.cancelDetached": fieldEphemeral,
	"runState.switchDue": fieldEphemeral, "runState.quiescing": fieldEphemeral, "runState.holdStartedAt": fieldEphemeral,
	"runState.switchRetryAt": fieldEphemeral, "runState.quiesceRetryAt": fieldEphemeral, "runState.noSwitch": fieldEphemeral,
	"runState.baseEvents": fieldEphemeral, "runState.baseBytes": fieldEphemeral, "runState.genEvents": fieldEphemeral,
	"runState.capLogged": fieldEphemeral, "runState.admitted": fieldEphemeral, "runState.pinClosure": fieldEphemeral,
	"runState.pinClosureFresh": fieldEphemeral, "runState.gaveUp": fieldEphemeral, "runState.restoreErr": fieldEphemeral,
	"runState.bootstrapStarted": fieldEphemeral, "runState.bootstrapDeadline": fieldEphemeral,
	"runState.bootstrapBatch": fieldEphemeral,
	"runState.passReserve":    fieldEphemeral, "runState.passCharge": fieldEphemeral, "runState.pendingHandlers": fieldEphemeral,
	"runState.hist": fieldEphemeral, "runState.guardOff": fieldEphemeral, "runState.snapBase": fieldEphemeral,
	"eachState.inFlight": fieldEphemeral,
	// A pin's runtime: no pin is opening, open or closing at a switch, and
	// every pin starts unopened in the next generation.
	"pinState.opened": fieldEphemeral, "pinState.triggeredBy": fieldEphemeral, "pinState.openedAt": fieldEphemeral,
	"pinState.timeout": fieldEphemeral, "pinState.ready": fieldEphemeral, "pinState.readySettable": fieldEphemeral,
	"pinState.readyConsumed": fieldEphemeral, "pinState.ctx": fieldEphemeral, "pinState.openErr": fieldEphemeral,
	"pinState.done": fieldEphemeral, "pinState.doneSent": fieldEphemeral, "pinState.closed": fieldEphemeral,
	"pinState.closedSettable": fieldEphemeral, "pinState.lost": fieldEphemeral,
}
