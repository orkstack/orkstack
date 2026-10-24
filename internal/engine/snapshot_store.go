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
	// WriteSnapshotOverlay stores a snapshot overlay at {seq}.ovl
	// (artifacts.RunSnapshotStore), for a run that writes overlays
	// (snapshot_overlay.go).
	WriteSnapshotOverlay(ctx context.Context, namespace, runID string, sequence int64, data []byte) (string, error)
	ReadSnapshot(ctx context.Context, uri string) ([]byte, error)
	// WriteContinuation and ReadContinuationRange are a run's continuation
	// objects (artifacts.RunSnapshotStore): written create-if-absent under a
	// content-addressed key at a switch, and read in ranges by the next
	// generation's bootstrap.
	WriteContinuation(ctx context.Context, namespace, runID string, generation int64, kind string, data []byte) (string, error)
	ReadContinuationRange(ctx context.Context, uri string, wantSize, offset, length int64) ([]byte, error)
	// ContinuationURI resolves the URI of the continuation object at a key
	// without touching the backend (artifacts.RunSnapshotStore): how a read
	// of a run's definition, known by its run id and digest only, finds it.
	ContinuationURI(namespace, runID string, generation int64, kind, sha256hex string) (string, error)
}

// compile-time checks: the artifacts platform-record stores are what
// production hands to this seam.
var (
	_ SnapshotStore = (*artifacts.LocalRunSnapshotStore)(nil)
	_ SnapshotStore = (*artifacts.S3RunSnapshotStore)(nil)
)

// SnapshotActivities owns the local activities that durably externalize a
// run's snapshots and its continuation objects. Each result is a URI or an
// object reference, so its local-activity marker stays small — except a
// bootstrap read's, whose chunk is the one bounded copy of saved state
// AGENTS.md §2.2 allows into history.
//
// beforeContinuationWrite and beforeContinuationRead are TEST-ONLY hooks of
// this one instance (WithContinuationHooksForTest), so two in-process
// replicas in one test never share them; never set in production.
type SnapshotActivities struct {
	store    SnapshotStore
	observer ContinuationObserver
	// bases is the full snapshots overlays are written against
	// (WriteSnapshotOrOverlay): the process's shared cache once cmd/ork
	// wires one (WithBaseCache), a private one until then.
	bases *SnapshotBaseCache

	beforeContinuationWrite func(ctx context.Context, kind string) error
	beforeContinuationRead  func(ctx context.Context, offset int64) error
}

// NewSnapshotActivities returns snapshot activities backed by store.
func NewSnapshotActivities(store SnapshotStore) *SnapshotActivities {
	return &SnapshotActivities{store: store, bases: NewSnapshotBaseCache()}
}

// WithBaseCache shares cache, the process's one base cache, with the readers
// that resolve the overlays these activities write (docs/DEPLOY.md §5).
func (a *SnapshotActivities) WithBaseCache(cache *SnapshotBaseCache) *SnapshotActivities {
	a.bases = cache
	return a
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
