package engine

import (
	"context"
	"fmt"

	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
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
// This release reads chains but cannot continue one: a carried start ends
// explicitly, with a published terminal event (endUnrestorable), so a chain
// some newer release created never silently re-runs here.

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
	s.runError = &orkstackv1.Error{Code: code, Message: message, Category: runStateCategory(code)}
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
