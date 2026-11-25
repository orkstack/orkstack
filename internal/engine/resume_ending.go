package engine

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"time"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/pkg/artifacts"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// ActivityWriteResumeEnding writes the terminal snapshot of a reset run whose
// plan cannot be run (E85 R4-1).
const ActivityWriteResumeEnding = "WriteResumeEnding"

// ResumeEndingInput is a reset run's ending: its base's last snapshot, at
// From, rewritten as this run's at To with its failure.
type ResumeEndingInput struct {
	Namespace  string    `json:"namespace"`
	RunID      string    `json:"run_id"`
	From       int64     `json:"from"`
	To         int64     `json:"to"`
	Code       string    `json:"code"`
	Message    string    `json:"message"`
	FinishedAt time.Time `json:"finished_at"`
}

// snapshotLocator is a snapshot store that names a snapshot's URI by its
// sequence (artifacts.BlobRunSnapshotStore).
type snapshotLocator interface {
	SnapshotURI(namespace, runID string, sequence int64) (string, error)
	CreateSnapshot(ctx context.Context, namespace, runID string, sequence int64, data []byte) (string, error)
}

// WriteResumeEnding reads the base's last snapshot at in.From and writes it
// as the run's terminal snapshot at in.To, FAILED with in's reason and its
// rows unchanged, so the run reads exactly as its base ended (R4-1). Only
// the URI crosses into history: a full snapshot can exceed the payload
// bound. A base snapshot that is gone, or does not decode, is final.
func (a *SnapshotActivities) WriteResumeEnding(ctx context.Context, in ResumeEndingInput) (string, error) {
	loc, ok := a.store.(snapshotLocator)
	if !ok {
		return "", temporal.NewNonRetryableApplicationError("this engine's snapshot store cannot name a snapshot by its sequence", ReasonResumePlanUnreadable, nil)
	}
	from, err := loc.SnapshotURI(in.Namespace, in.RunID, in.From)
	if err != nil {
		return "", temporal.NewNonRetryableApplicationError(fmt.Sprintf("the base's last snapshot: %v", err), ReasonResumePlanUnreadable, err)
	}
	data, err := a.store.ReadSnapshot(ctx, from)
	if errors.Is(err, artifacts.ErrNotFound) {
		return "", temporal.NewNonRetryableApplicationError(fmt.Sprintf("the base's last snapshot %s is gone", from), ReasonResumePlanUnreadable, err)
	}
	if err != nil {
		return "", fmt.Errorf("read the base's last snapshot %s: %w", from, err)
	}
	run := &orkstackv1.Run{}
	if err := proto.Unmarshal(data, run); err != nil {
		return "", temporal.NewNonRetryableApplicationError(fmt.Sprintf("the base's last snapshot %s does not decode: %v", from, err), ReasonResumePlanUnreadable, err)
	}
	// Only the base's own last, terminal snapshot is an ending's rows.
	if run.GetId() != in.RunID || !isTerminal(run.GetStatus()) {
		return "", temporal.NewNonRetryableApplicationError(
			fmt.Sprintf("the snapshot at %s is not run %s's last, terminal one", from, in.RunID), ReasonResumePlanUnreadable, nil)
	}
	run.Status = orkstackv1.Status_STATUS_FAILED
	run.Error = &orkstackv1.Error{Code: in.Code, Message: in.Message, Category: runLevelFailureCategory(in.Code)}
	run.FinishedAt = timestamppb.New(in.FinishedAt)
	out, err := proto.Marshal(run)
	if err != nil {
		return "", fmt.Errorf("encode the ending of run %s: %w", in.RunID, err)
	}
	uri, err := loc.CreateSnapshot(ctx, in.Namespace, in.RunID, in.To, out)
	if errors.Is(err, artifacts.ErrSnapshotExists) {
		// A retry of this same write, whose first answer was lost, finds its
		// own bytes there: that is this ending, written.
		if there, readErr := loc.SnapshotURI(in.Namespace, in.RunID, in.To); readErr == nil {
			if stored, getErr := a.store.ReadSnapshot(ctx, there); getErr == nil && bytes.Equal(stored, out) {
				return there, nil
			}
		}
		return "", temporal.NewNonRetryableApplicationError(
			fmt.Sprintf("run %s already has a snapshot at %d, so this ending is no longer its last", in.RunID, in.To), ReasonResumePlanInvalid, err)
	}
	if err != nil {
		return "", fmt.Errorf("write the ending of run %s: %w", in.RunID, err)
	}
	return uri, nil
}

// endResumeUnrunnable ends a reset run whose plan cannot be run (§30.3):
// FAILED with the failure's reason, at the key's seq_base, with its base's
// rows (R4-1). When no key was read, no sequence above the base's events is
// known, so the run fails with no event rather than reuse one (R4-2); so it
// does too when the base's last snapshot cannot be read. Either way nothing
// has run again, and the run can be resumed.
func (s *runState) endResumeUnrunnable(ctx workflow.Context, failure error) (*orkstackv1.Run, error) {
	if errors.Is(failure, errResumeCanceled) {
		// Canceled before the plan was known: like R4-2, no sequence above
		// the base's events is safe to publish at, so the run closes
		// canceled with no event, and a reader takes its ending from the
		// closed execution.
		return nil, temporal.NewCanceledError()
	}
	var f *resumeLoadFailure
	if !errors.As(failure, &f) {
		f = &resumeLoadFailure{code: ReasonResumePlanUnreadable, message: failure.Error()}
	}
	final := runLevelError(f.code, f.message)
	// The key's scope is the ending's sequence only when it lies above every
	// event this execution has: a scope at or below them is no seq_base of
	// this base (a stale plan, or a reason shaped like a key), and the run
	// fails with no event rather than reuse a sequence (H1 of PR4a review).
	if f.scope == 0 || int64(f.scope) <= int64(s.eventSeq) {
		workflow.GetLogger(ctx).Error("a reset run cannot read the reset that made it; it fails with no event",
			"run_id", s.runID, "code", f.code, "error", f.message)
		return nil, final
	}
	now := workflow.Now(ctx)
	var uri string
	err := s.getLocal(ctx, workflow.ExecuteLocalActivity(workflow.WithLocalActivityOptions(ctx, snapshotLocalActivityOptions()), ActivityWriteResumeEnding,
		ResumeEndingInput{
			Namespace: s.wf.GetNamespace(), RunID: s.runID,
			From: int64(f.scope) - 1, To: int64(f.scope),
			Code: f.code, Message: f.message, FinishedAt: now,
		}), &uri)
	if err != nil {
		workflow.GetLogger(ctx).Error("a reset run cannot write its ending from its base's last snapshot; it fails with no event",
			"run_id", s.runID, "code", f.code, "error", err)
		return nil, final
	}
	s.eventSeq = int(f.scope) - 1
	s.status = orkstackv1.Status_STATUS_FAILED
	s.runError = &orkstackv1.Error{Code: f.code, Message: f.message, Category: runLevelFailureCategory(f.code)}
	s.finished = timestamppb.New(now)
	if _, err := s.emitTaskEvent(ctx, orkstackv1.EventType_EVENT_TYPE_RUN_FAILED, "",
		orkstackv1.Status_STATUS_FAILED, f.message, uri, false); err != nil {
		workflow.GetLogger(ctx).Error("emit the ending of a reset run failed", "run_id", s.runID, "error", err)
	}
	return nil, final
}
