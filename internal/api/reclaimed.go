package api

import (
	"context"
	"errors"
	"fmt"
	"slices"

	"connectrpc.com/connect"

	"github.com/orkstack/orkstack/internal/task"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// reclaimRecordReader is an archive that keeps the artifact GC's records.
type reclaimRecordReader interface {
	ReclaimRecord(ctx context.Context, runID string) (artifacts.ReclaimRecord, error)
}

// reclaimedByGC reports whether a recorded object the store no longer holds
// was deleted by the artifact GC: its producing run has a GC record that does
// not list it as kept. Anything it cannot read answers false, and the caller
// reports the object missing as it always has.
func (s *Service) reclaimedByGC(ctx context.Context, uri string) bool {
	reader, ok := s.archive.(reclaimRecordReader)
	if !ok {
		return false
	}
	objectID, ok := artifacts.ObjectIDFromURI(uri)
	if !ok {
		return false
	}
	producer, ok := artifacts.ProducingRunFromObjectKey(objectID)
	if !ok {
		return false
	}
	rec, err := reader.ReclaimRecord(ctx, producer)
	if err != nil {
		if !errors.Is(err, artifacts.ErrNotFound) {
			s.logger.Warn("could not read the artifact GC record of an object's producer", "run_id", producer, "error", err)
		}
		return false
	}
	return !rec.LateCommit && !slices.Contains(rec.Kept, objectID)
}

// artifactReclaimed is the answer to a read of an object the artifact GC
// deleted.
func artifactReclaimed(what string) *connect.Error {
	return connect.NewError(connect.CodeFailedPrecondition, fmt.Errorf(
		"%s: %s is no longer served: artifact garbage collection deleted it after its run was retired; only its recorded digest remains",
		task.ReasonArtifactReclaimed, what))
}
