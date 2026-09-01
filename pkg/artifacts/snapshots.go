package artifacts

import (
	"context"
	"fmt"
	"path"
	"sort"
	"strconv"
)

// A run snapshot is a platform record: the engine's own copy of a run's state,
// externalized from Temporal history so the history carries only a reference
// (AGENTS.md §2.2). It is written by the RunWorkflow's snapshot local activity
// and read back by every engine replica that serves the run.
const (
	// SnapshotPrefix roots the run snapshots in the store. It sits under
	// metadata/, beside metadata/logs, metadata/commits, metadata/runs and
	// metadata/orkflows — and crucially NOT under the orkflows/ (S3) or
	// objects/ (local) namespaces where workload artifacts land, so a workload
	// artifact can never collide with a snapshot key and an operator can
	// express a bucket lifecycle rule over this one prefix.
	SnapshotPrefix = "metadata/snapshots"

	// SnapshotContentType is what a snapshot object is served as.
	SnapshotContentType = "application/x-protobuf"

	// snapshotSuffix is the fixed leaf suffix of every snapshot key.
	snapshotSuffix = ".pb"
)

// RunSnapshotPrefix renders the prefix containing every snapshot of a run:
// metadata/snapshots/{namespace}/{run-id}/. Run-scoped cleanup deletes this
// prefix, which is what keeps externalized snapshots from outliving the
// Temporal history that names them.
func RunSnapshotPrefix(namespace, runID string) (string, error) {
	namespaceSegment, err := encodeArchiveSegment(ResolveNamespace(namespace))
	if err != nil {
		return "", fmt.Errorf("artifacts: run snapshot prefix: namespace: %w", err)
	}
	runSegment, err := encodeArchiveSegment(runID)
	if err != nil {
		return "", fmt.Errorf("artifacts: run snapshot prefix: run id: %w", err)
	}
	return path.Join(SnapshotPrefix, namespaceSegment, runSegment) + "/", nil
}

// RunSnapshotKey renders the deterministic object key for one snapshot:
// metadata/snapshots/{namespace}/{run-id}/{sequence}.pb. The sequence is the
// workflow's own event counter, so the key is a pure function of coordinates
// the workflow already owns and every replica derives the same key.
func RunSnapshotKey(namespace, runID string, sequence int64) (string, error) {
	prefix, err := RunSnapshotPrefix(namespace, runID)
	if err != nil {
		return "", err
	}
	if sequence < 1 {
		return "", fmt.Errorf("artifacts: run snapshot key: sequence %d is not 1-based", sequence)
	}
	return prefix + strconv.FormatInt(sequence, 10) + snapshotSuffix, nil
}

// RunSnapshotStore persists immutable run snapshots and retrieves them by the
// URI a write returned. It is a separate interface from Store because the two
// have opposite shapes: an artifact is workload data filed under provenance
// and content digest, while a snapshot is a platform record at a key the
// engine derives — the same split that keeps commits, logs and run bodies out
// of the workload namespace.
//
// Implementations hold no cache and no per-run state: the replica that wrote
// a snapshot and the replica that reads it are different processes, and any
// replica must answer for any run.
type RunSnapshotStore interface {
	// WriteSnapshot stores one immutable snapshot and returns its URI.
	// Writing the same coordinates again overwrites idempotently and returns
	// the same URI, which is what makes workflow-side retries safe.
	WriteSnapshot(ctx context.Context, namespace, runID string, sequence int64, data []byte) (string, error)
	// ReadSnapshot fetches the bytes at a URI WriteSnapshot returned. A
	// missing snapshot returns an error matching ErrNotFound; a URI outside
	// the snapshot prefix returns an error matching ErrInvalidURI.
	ReadSnapshot(ctx context.Context, uri string) ([]byte, error)
	// DeleteSnapshot removes one snapshot. It is idempotent: deleting a
	// snapshot that is already gone returns nil.
	DeleteSnapshot(ctx context.Context, uri string) error
}

// RunSnapshotStoreFactory builds a RunSnapshotStore from a Config. A backend
// that can hold run snapshots registers one, mirroring Factory, ArchiveFactory
// and LogStoreFactory.
type RunSnapshotStoreFactory func(cfg Config) (RunSnapshotStore, error)

var snapshotStoreRegistry = map[string]RunSnapshotStoreFactory{}

// RegisterRunSnapshotStore adds a snapshot-store factory under name.
func RegisterRunSnapshotStore(name string, f RunSnapshotStoreFactory) error {
	if name == "" {
		return fmt.Errorf("artifacts: register snapshot store: empty name")
	}
	if f == nil {
		return fmt.Errorf("artifacts: register snapshot store %q: nil factory", name)
	}
	registryMu.Lock()
	defer registryMu.Unlock()
	if _, exists := snapshotStoreRegistry[name]; exists {
		return fmt.Errorf("artifacts: register snapshot store %q: already registered", name)
	}
	snapshotStoreRegistry[name] = f
	return nil
}

// RunSnapshotStoreBackends lists the backends that can hold snapshots, sorted.
func RunSnapshotStoreBackends() []string {
	registryMu.RLock()
	defer registryMu.RUnlock()
	names := make([]string, 0, len(snapshotStoreRegistry))
	for name := range snapshotStoreRegistry {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

// NewRunSnapshotStore builds the RunSnapshotStore named by cfg.Backend, so one
// ORK_ARTIFACT_BACKEND selects where artifacts, run history, task logs and
// run snapshots all live.
func NewRunSnapshotStore(cfg Config) (RunSnapshotStore, error) {
	name := cfg.Backend
	if name == "" {
		name = DefaultBackend
		cfg.Backend = name
	}
	registryMu.RLock()
	f, ok := snapshotStoreRegistry[name]
	registryMu.RUnlock()
	if !ok {
		return nil, fmt.Errorf("artifacts: snapshot store backend %q: %w (registered: %v)",
			name, ErrUnknownBackend, RunSnapshotStoreBackends())
	}
	store, err := f(cfg)
	if err != nil {
		return nil, fmt.Errorf("artifacts: snapshot store backend %q: %w", name, err)
	}
	return store, nil
}

func init() {
	mustRegisterRunSnapshotStore(BackendLocal, func(cfg Config) (RunSnapshotStore, error) {
		return NewLocalRunSnapshotStore(localDir(cfg))
	})
	mustRegisterRunSnapshotStore(BackendS3, func(cfg Config) (RunSnapshotStore, error) {
		return NewS3RunSnapshotStore(context.Background(), cfg)
	})
}

func mustRegisterRunSnapshotStore(name string, f RunSnapshotStoreFactory) {
	if err := RegisterRunSnapshotStore(name, f); err != nil {
		panic(err)
	}
}
