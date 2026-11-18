package archive

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"time"

	"github.com/orkstack/orkstack/pkg/artifacts"
)

// RestoreStore is the object store as a restore reconcile needs it.
// *artifacts.BlobRunArchive implements it.
type RestoreStore interface {
	ListUnderAfter(ctx context.Context, prefix, after string, limit int) ([]artifacts.StoredObject, bool, error)
	CleanupState(ctx context.Context, runID string) (artifacts.RunCleanupState, error)
	HasRun(ctx context.Context, runID string) (bool, error)
}

// RestoreCluster is the cluster as a restore reconcile needs it: the same
// lookup and delete retention cleanup uses, and the runs it holds open.
type RestoreCluster interface {
	LookupWorkflow(ctx context.Context, namespace, workflowID string) (found bool, closeTime time.Time, err error)
	DeleteWorkflow(ctx context.Context, namespace, workflowID string) error
	// OpenRunIDs lists at most limit runs the cluster holds open, and
	// whether more are open.
	OpenRunIDs(ctx context.Context, limit int) ([]string, bool, error)
}

// MaxRestorePage bounds one reconcile page: the cleanup records it lists.
const MaxRestorePage = 1000

// MaxRestoreOpenRuns bounds the open runs the first page checks for a
// finished archive.
const MaxRestoreOpenRuns = 10000

// restoreListMargin widens the listing's cheap filter. A cleanup record is
// written after the execution's delete, so its store write time is at or
// after its temporal_deleted_at by the engine's clock; the margin absorbs a
// skew between the engine's clock and the store's. The record's own time
// decides; the margin only spares reading records written long before.
const restoreListMargin = 24 * time.Hour

// RestoreRequest is one page of a restore reconcile.
type RestoreRequest struct {
	// RestoredAt is the instant Temporal was restored to (T_t).
	RestoredAt time.Time
	// DryRun lists and deletes nothing.
	DryRun bool
	// After is the page cursor: the last cleanup record of the previous
	// page; "" for the first.
	After string
	// Limit bounds the cleanup records the page lists, 1..MaxRestorePage.
	Limit int
}

// RestoreRetired is a run retention cleanup retired after RestoredAt whose
// execution the restored cluster holds again.
type RestoreRetired struct {
	RunID     string
	RetiredAt time.Time
	// Archived says the bucket holds the run's archive body; only such a
	// run's executions are deleted, because the archive then answers for it.
	Archived bool
	// Deleted says this page deleted its executions.
	Deleted bool
	// Open says the restored execution is running: a run replaying its tail,
	// or one resumed since the restore. It is never deleted, because a delete
	// would terminate it and lose what it does from here.
	Open bool
}

// RestoreResult is one page's answer.
type RestoreResult struct {
	Retired []RestoreRetired
	// Finished are the runs the restored cluster shows open while the bucket
	// holds their finished archive: they run their tail, and emit its events
	// and webhooks, again. Listed on the first page only; never acted on.
	Finished []string
	// FinishedTruncated says more runs were open than MaxRestoreOpenRuns, so
	// Finished is incomplete.
	FinishedTruncated bool
	// Listed is how many cleanup records the page read.
	Listed int
	// Next is the cursor of the next page; "" when this was the last.
	Next string
}

// ReconcileRestore reconciles the restored cluster with the bucket after a
// restore of Temporal to req.RestoredAt (docs/RUNBOOKS.md, "Backup and
// disaster recovery"). It finds runs retention cleanup retired after that
// instant, by their cleanup records, whose executions the restore brought
// back, and, unless req.DryRun, deletes those executions where the bucket
// holds the run's archive and the execution is closed, with the delete
// cleanup itself uses. It never deletes an object, a running execution, or a
// run with no cleanup record. A cleanup record written more than a day before
// the restore instant (restoreListMargin) is not read: it cannot be one. A
// read it cannot make fails the page rather than skip a run; a failed delete
// fails it too, and the result then holds the runs deleted before it.
func ReconcileRestore(ctx context.Context, store RestoreStore, cluster RestoreCluster, temporalNamespace string, req RestoreRequest) (RestoreResult, error) {
	if store == nil || cluster == nil {
		return RestoreResult{}, errors.New("archive: restore reconcile needs the run archive and the cluster")
	}
	if req.RestoredAt.IsZero() {
		return RestoreResult{}, errors.New("archive: restore reconcile needs the instant Temporal was restored to")
	}
	if req.Limit < 1 || req.Limit > MaxRestorePage {
		return RestoreResult{}, fmt.Errorf("archive: restore reconcile page limit %d is outside 1..%d", req.Limit, MaxRestorePage)
	}
	var out RestoreResult
	if req.After == "" {
		finished, truncated, err := finishedAfterRestore(ctx, store, cluster)
		if err != nil {
			return RestoreResult{}, err
		}
		out.Finished, out.FinishedTruncated = finished, truncated
	}
	objects, more, err := store.ListUnderAfter(ctx, artifacts.RunCleanupNamespace+"/", req.After, req.Limit)
	if err != nil {
		return RestoreResult{}, fmt.Errorf("archive: restore reconcile: list cleanup records: %w", err)
	}
	out.Listed = len(objects)
	for _, object := range objects {
		if object.Modified.Before(req.RestoredAt.Add(-restoreListMargin)) {
			continue
		}
		runID, ok := artifacts.RunIDFromCleanupKey(object.Key)
		if !ok {
			return RestoreResult{}, fmt.Errorf("archive: restore reconcile: %q is not a cleanup record's key", object.Key)
		}
		state, err := store.CleanupState(ctx, runID)
		if err != nil {
			return RestoreResult{}, fmt.Errorf("archive: restore reconcile: run %s: %w", runID, err)
		}
		if state.RunID != runID {
			return RestoreResult{}, fmt.Errorf("archive: restore reconcile: the cleanup record under %q names run %q", object.Key, state.RunID)
		}
		if !state.TemporalDeletedAt.After(req.RestoredAt) {
			continue
		}
		found, closedAt, err := cluster.LookupWorkflow(ctx, temporalNamespace, runID)
		if err != nil {
			return RestoreResult{}, fmt.Errorf("archive: restore reconcile: run %s: look up its execution: %w", runID, err)
		}
		if !found {
			continue
		}
		archived, err := store.HasRun(ctx, runID)
		if err != nil {
			return RestoreResult{}, fmt.Errorf("archive: restore reconcile: run %s: read its archive: %w", runID, err)
		}
		row := RestoreRetired{RunID: runID, RetiredAt: state.TemporalDeletedAt, Archived: archived, Open: closedAt.IsZero()}
		if archived && !row.Open && !req.DryRun {
			if err := cluster.DeleteWorkflow(ctx, temporalNamespace, runID); err != nil {
				// What the page already deleted is returned with the error:
				// a destructive call says what it did before it failed.
				return out, fmt.Errorf("archive: restore reconcile: run %s: delete its executions: %w", runID, err)
			}
			row.Deleted = true
		}
		out.Retired = append(out.Retired, row)
	}
	if more && len(objects) > 0 {
		out.Next = objects[len(objects)-1].Key
	}
	return out, nil
}

// finishedAfterRestore is every run the cluster holds open whose archive
// body the bucket holds: it finished after the instant Temporal was restored
// to. The listing is bounded; truncated says it was cut short.
func finishedAfterRestore(ctx context.Context, store RestoreStore, cluster RestoreCluster) ([]string, bool, error) {
	open, truncated, err := cluster.OpenRunIDs(ctx, MaxRestoreOpenRuns)
	if err != nil {
		return nil, false, fmt.Errorf("archive: restore reconcile: list the open runs: %w", err)
	}
	var out []string
	for _, runID := range open {
		archived, err := store.HasRun(ctx, runID)
		if err != nil {
			return nil, false, fmt.Errorf("archive: restore reconcile: run %s: read its archive: %w", runID, err)
		}
		if archived {
			out = append(out, runID)
		}
	}
	slices.Sort(out)
	return out, truncated, nil
}

// RestoreReconciler is ReconcileRestore bound to one run archive, cluster and
// Temporal namespace: what the API's SystemService.ReconcileRestore calls.
type RestoreReconciler struct {
	store     RestoreStore
	cluster   RestoreCluster
	namespace string
}

// NewRestoreReconciler binds ReconcileRestore to store, cluster and the
// deployment's Temporal namespace.
func NewRestoreReconciler(store RestoreStore, cluster RestoreCluster, temporalNamespace string) *RestoreReconciler {
	return &RestoreReconciler{store: store, cluster: cluster, namespace: temporalNamespace}
}

// ReconcileRestore reconciles one page; see the package function.
func (r *RestoreReconciler) ReconcileRestore(ctx context.Context, req RestoreRequest) (RestoreResult, error) {
	return ReconcileRestore(ctx, r.store, r.cluster, r.namespace, req)
}
