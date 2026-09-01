package engine

import (
	"bytes"
	"context"
	"fmt"
	"io"

	"google.golang.org/protobuf/proto"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// ActivityWriteSnapshot is the registered name of the local activity that
// moves a complete run snapshot out of Temporal history.
const ActivityWriteSnapshot = "WriteSnapshot"

// SnapshotStore persists immutable run snapshots and retrieves them by URI.
type SnapshotStore interface {
	WriteSnapshot(ctx context.Context, namespace, runID string, sequence int64, data []byte) (string, error)
	ReadSnapshot(ctx context.Context, uri string) ([]byte, error)
}

// NewSnapshotStore returns a snapshot store backed by the artifact store.
func NewSnapshotStore(store artifacts.Store) SnapshotStore {
	return &snapshotStore{store: store}
}

type snapshotStore struct {
	store artifacts.Store
}

func (s *snapshotStore) WriteSnapshot(
	ctx context.Context,
	namespace, runID string,
	sequence int64,
	data []byte,
) (string, error) {
	if namespace == "" {
		namespace = artifacts.DefaultNamespace
	}
	if runID == "" || len(data) == 0 {
		return "", fmt.Errorf("engine: write snapshot: run id and data are required")
	}
	key := fmt.Sprintf("snapshots/%s/%s/%d.pb", namespace, runID, sequence)
	ref, err := s.store.Put(ctx, key, bytes.NewReader(data), artifacts.PutOptions{
		ContentType: "application/x-protobuf",
	})
	if err != nil {
		return "", fmt.Errorf("engine: write snapshot %s: %w", key, err)
	}
	return ref.URI, nil
}

func (s *snapshotStore) ReadSnapshot(ctx context.Context, uri string) ([]byte, error) {
	if uri == "" {
		return nil, fmt.Errorf("engine: read snapshot: uri is required")
	}
	reader, err := s.store.Get(ctx, uri)
	if err != nil {
		return nil, fmt.Errorf("engine: read snapshot %s: %w", uri, err)
	}
	defer reader.Close()
	data, err := io.ReadAll(reader)
	if err != nil {
		return nil, fmt.Errorf("engine: read snapshot %s: %w", uri, err)
	}
	return data, nil
}

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
