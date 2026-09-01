package engine

import (
	"context"
	"fmt"

	"google.golang.org/protobuf/proto"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// ActivityWriteSnapshot is the registered name of the local activity that
// moves a complete run snapshot out of Temporal history.
const ActivityWriteSnapshot = "WriteSnapshot"

// SnapshotStore persists immutable run snapshots and retrieves them by URI.
// It is the engine's seam over artifacts.RunSnapshotStore: production wiring
// passes artifacts.NewRunSnapshotStore's store, which files every snapshot as
// a platform record under the metadata/snapshots prefix — beside commits,
// logs and run bodies, and deliberately outside the workload artifact
// namespace — so run-scoped cleanup can reclaim it by prefix.
type SnapshotStore interface {
	WriteSnapshot(ctx context.Context, namespace, runID string, sequence int64, data []byte) (string, error)
	ReadSnapshot(ctx context.Context, uri string) ([]byte, error)
}

// compile-time checks: the artifacts platform-record stores are what
// production hands to this seam.
var (
	_ SnapshotStore = (*artifacts.LocalRunSnapshotStore)(nil)
	_ SnapshotStore = (*artifacts.S3RunSnapshotStore)(nil)
)

// SnapshotActivities owns the local activity that durably externalizes a run
// snapshot. Its result is only a URI, so the local-activity marker stays small.
type SnapshotActivities struct {
	store SnapshotStore
}

// NewSnapshotActivities returns snapshot activities backed by store.
func NewSnapshotActivities(store SnapshotStore) *SnapshotActivities {
	return &SnapshotActivities{store: store}
}

// WriteSnapshot serializes and stores a complete run snapshot.
func (a *SnapshotActivities) WriteSnapshot(
	ctx context.Context,
	run *orkstackv1.Run,
	sequence int64,
) (string, error) {
	if run == nil {
		return "", fmt.Errorf("engine: write snapshot: run is required")
	}
	data, err := proto.Marshal(run)
	if err != nil {
		return "", fmt.Errorf("engine: marshal snapshot for run %s: %w", run.GetId(), err)
	}
	uri, err := a.store.WriteSnapshot(ctx, run.GetNamespace(), run.GetId(), sequence, data)
	if err != nil {
		return "", fmt.Errorf("engine: persist snapshot for run %s: %w", run.GetId(), err)
	}
	return uri, nil
}
