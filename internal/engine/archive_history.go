package engine

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"go.temporal.io/api/workflowservice/v1"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// A run's own terminal publish archives it. A run that was terminated from
// outside runs no code at its end, and one whose ending object storage refused
// publishes no such event or cannot complete it, so nothing has archived
// either when it closes. archive.Sweeper finds them from the cluster's index
// of finished executions, and HistoryArchiver writes their archives: the same
// body from the same record, since a finished execution's history is complete
// and never changes.

// RunRecordIncompleteCode is the run's Error.code when its archive could not
// recover the record of its tasks and the run has no error of its own to say
// so beside.
const RunRecordIncompleteCode = "RUN_RECORD_INCOMPLETE"

// What a run's error says when its archive could not recover the record of its
// tasks. Each reaches a client, in run vocabulary.
const (
	recordLostSentence = "the record of this run's tasks was lost from object storage before the run was " +
		"archived, so its tasks are not shown"
	recordStaleSentence = "the newest record of this run's tasks was lost from object storage before the run " +
		"was archived, so its tasks are shown as of an earlier event"
	recordNeverSentence = "no record of this run's tasks was ever written to object storage, so its tasks are not shown"
)

// EachFinishedRun hands visit every run the cluster holds that finished before
// closedBefore, page by page, until visit fails or the index ends. It lists a
// chain's last execution only: an execution that continued as new did not end
// the run. A listing is never read as proof of anything but that the run may
// need looking at.
func (r *TemporalRunReader) EachFinishedRun(
	ctx context.Context,
	closedBefore time.Time,
	pageSize int,
	visit func(runID string, closedAt time.Time) error,
) error {
	query := fmt.Sprintf("WorkflowType = %q AND ExecutionStatus IN (%s) AND CloseTime < %q",
		RunWorkflowType, quotedExecutionStatuses(terminalExecutionStatuses), statsTime(closedBefore))
	var token []byte
	for {
		response, err := r.client.ListWorkflow(ctx, &workflowservice.ListWorkflowExecutionsRequest{
			Query:         query,
			PageSize:      int32(pageSize),
			NextPageToken: token,
		})
		if err != nil {
			return fmt.Errorf("engine: list finished runs: %w", err)
		}
		for _, execution := range response.GetExecutions() {
			if err := visit(execution.GetExecution().GetWorkflowId(), execution.GetCloseTime().AsTime()); err != nil {
				return err
			}
		}
		if token = response.GetNextPageToken(); len(token) == 0 {
			return nil
		}
	}
}

// readForArchive reads a finished run the way its archive holds it: the run
// GetRun would answer and the events EventsForArchive would, from one walk,
// with what cannot be recovered stated instead of failing the read. A failure
// that may heal is an error and nothing is decided from it.
func (r *TemporalRunReader) readForArchive(ctx context.Context, runID string) (*orkstackv1.Run, ArchiveRead, error) {
	st := newScanState(true)
	st.forArchive = true
	projection, err := r.scanWith(ctx, runID, false, 0, false, nil, st)
	if err != nil {
		return nil, ArchiveRead{}, err
	}
	if !st.closed {
		return nil, ArchiveRead{}, fmt.Errorf("engine: archive run %s: its history holds no close: it has not finished", runID)
	}
	return projection.run, ArchiveRead{Events: projection.events, Gaps: st.gaps, CanceledBy: projection.canceledBy}, nil
}

// loadRecordForArchive loads the newest record of the run's tasks it can read,
// at the run's close: the one its last publish named, else the one before it,
// the bounded fallback a live read makes. A record that is gone or does not
// decode can never come back, so the archive states the loss; a read that may
// heal fails the walk, because the archive is immutable and an outage's hole
// written into it would stay.
func (r *TemporalRunReader) loadRecordForArchive(ctx context.Context, runID string, st *scanState) error {
	projection := st.projection
	if projection.snapshotURI == "" {
		st.recordLoss = recordNeverSentence
		if st.carryRefused {
			st.recordLoss = recordLostSentence
		}
		return nil
	}
	for i, uri := range []string{projection.snapshotURI, projection.priorSnapshotURI} {
		if uri == "" {
			continue
		}
		run, err := r.readSnapshotAt(ctx, runID, uri, projection)
		if err == nil {
			projection.run = mergeRunSnapshot(projection.run, run)
			projection.snapshotLoaded = true
			if i > 0 {
				st.recordLoss = recordStaleSentence
			}
			return nil
		}
		if !snapshotLost(err) {
			return err
		}
		slog.Warn("archiving a run from its history: a record of its tasks cannot be recovered",
			"run_id", runID, "uri", uri, "error", err)
	}
	st.recordLoss = recordLostSentence
	return nil
}

// snapshotLost reports a snapshot read whose failure retrying cannot heal: the
// object is gone, or it was read and did not verify or decode.
func snapshotLost(err error) bool {
	if errors.Is(err, errSnapshotStoreFailed) {
		return false
	}
	return errors.Is(err, artifacts.ErrNotFound) || errors.Is(err, errSnapshotUndecodable) ||
		errors.Is(err, ErrContinuationObjectInvalid) || errors.Is(err, ErrSnapshotOverlayInvalid)
}

// settleRunForArchive finishes the run's close for its archive, after closeRun.
// closeRun trusts a run that announced its own end to carry its outcome in the
// record it loaded, which here may be an older one or none: the outcome is then
// the terminal event's, or the close's. What was lost goes on the run's error.
func settleRunForArchive(projection *runProjection, st *scanState, status orkstackv1.Status, cause *orkstackv1.Error) {
	run := projection.run
	if !isTerminal(run.GetStatus()) {
		run.Status = status
		if isTerminal(projection.lastRunStatus) {
			run.Status = projection.lastRunStatus
		}
		if run.GetError() == nil {
			run.Error = cause
		}
	}
	if st.recordLoss == "" {
		return
	}
	if run.GetError() == nil {
		run.Error = &orkstackv1.Error{
			Code:     RunRecordIncompleteCode,
			Message:  st.recordLoss,
			Category: orkstackv1.FailureCategory_FAILURE_CATEGORY_INFRASTRUCTURE_ERROR,
		}
		return
	}
	if run.Error.Details != "" {
		run.Error.Details += "; " + st.recordLoss
		return
	}
	run.Error.Details = st.recordLoss
}

// HistoryArchiver writes a finished run's archive from its Temporal history.
type HistoryArchiver struct {
	reader  *TemporalRunReader
	archive *RunArchiver
}

// NewHistoryArchiver builds a writer over the reader and the deployment's
// archive.
func NewHistoryArchiver(reader *TemporalRunReader, archive *RunArchiver) *HistoryArchiver {
	return &HistoryArchiver{reader: reader, archive: archive}
}

// ArchiveFromHistory archives one finished run. A run the cluster no longer
// holds is an error matching artifacts.ErrNotFound. Like every archive write it
// is safe to repeat and safe beside a second writer (RunArchiver.Archive).
func (h *HistoryArchiver) ArchiveFromHistory(ctx context.Context, runID string) error {
	run, read, err := h.reader.readForArchive(ctx, runID)
	if err != nil {
		return err
	}
	applyCanceledBy(run, read.CanceledBy)
	return h.archive.Archive(ctx, run, read.Events, read.Gaps...)
}
