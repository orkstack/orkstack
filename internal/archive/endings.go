package archive

import (
	"context"
	"fmt"
	"time"

	"github.com/orkstack/orkstack/pkg/artifacts"
)

// EndingBodies is what is archived of runs' endings
// (artifacts.BlobRunArchive).
type EndingBodies interface {
	RunEndings(ctx context.Context, runID string) (artifacts.RunEndings, error)
	HasEnding(ctx context.Context, runID string, scope uint64) (bool, error)
}

// EndingCluster says which ending a run's current execution is of
// (engine.TemporalRunReader).
type EndingCluster interface {
	// NewestEnding is the scope of the run's current execution, 0 for one no
	// resume made, and when it closed, zero while it runs. found is false for
	// a run the cluster does not hold.
	NewestEnding(ctx context.Context, namespace, runID string) (scope uint64, closedAt time.Time, found bool, err error)
}

// NewestEndings answers whether a run's newest ending is archived. A run no
// resume marked has one ending. Of a marked run the cluster says which ending
// is the newest, because a mark's reset may never have run.
type NewestEndings struct {
	bodies     EndingBodies
	cluster    EndingCluster
	temporalNS string
}

var _ RunBodyChecker = (*NewestEndings)(nil)

// NewNewestEndings is the check over bodies and cluster. temporalNamespace is
// the deployment's Temporal partition.
func NewNewestEndings(bodies EndingBodies, cluster EndingCluster, temporalNamespace string) *NewestEndings {
	return &NewestEndings{bodies: bodies, cluster: cluster, temporalNS: temporalNamespace}
}

// HasRun reports whether runID's newest ending has a body. A failure to
// answer is an error, never false.
func (n *NewestEndings) HasRun(ctx context.Context, runID string) (bool, error) {
	endings, err := n.bodies.RunEndings(ctx, runID)
	if err != nil {
		return false, err
	}
	scope := uint64(0)
	if endings.Newest() > 0 {
		newest, _, found, err := n.cluster.NewestEnding(ctx, n.temporalNS, runID)
		switch {
		case err != nil:
			return false, fmt.Errorf("archive: which ending of run %q is its newest: %w", runID, err)
		case found:
			scope = newest
		case len(endings.Archived) > 0:
			// Nothing more of it will ever be archived.
			scope = endings.Archived[len(endings.Archived)-1]
		}
	}
	return n.bodies.HasEnding(ctx, runID, scope)
}

// Resumed reports whether a resume marked the run. It asks no cluster.
func (n *NewestEndings) Resumed(ctx context.Context, runID string) (bool, error) {
	endings, err := n.bodies.RunEndings(ctx, runID)
	return endings.Newest() > 0, err
}

// resumedRun is what the sweep learned of a run a resume marked.
type resumedRun struct {
	// has: its newest ending is archived, or has not ended.
	has bool
	// closedAt is when its newest ending closed.
	closedAt time.Time
}

// newest is what the sweep asks of one marked run.
func (n *NewestEndings) newest(ctx context.Context, runID string) (resumedRun, error) {
	scope, closedAt, found, err := n.cluster.NewestEnding(ctx, n.temporalNS, runID)
	switch {
	case err != nil:
		return resumedRun{}, fmt.Errorf("archive: which ending of run %q is its newest: %w", runID, err)
	case found && closedAt.IsZero():
		return resumedRun{has: true}, nil
	}
	if !found {
		has, err := n.HasRun(ctx, runID)
		return resumedRun{has: has}, err
	}
	has, err := n.bodies.HasEnding(ctx, runID, scope)
	return resumedRun{has: has, closedAt: closedAt}, err
}

// EndingRecords is the artifact GC's resume check: a run marked as resumed
// since its newest archived ending has an ending nothing has archived, so
// nothing it references is reclaimed. The GC asks it only of a run the cluster
// no longer holds.
type EndingRecords struct{ bodies EndingBodies }

var _ ResumeRecords = EndingRecords{}

// NewEndingRecords is the check over bodies.
func NewEndingRecords(bodies EndingBodies) EndingRecords { return EndingRecords{bodies: bodies} }

// ResumedAfter implements ResumeRecords.
func (r EndingRecords) ResumedAfter(ctx context.Context, runID string, _ time.Time) (bool, error) {
	endings, err := r.bodies.RunEndings(ctx, runID)
	if err != nil {
		return false, err
	}
	newest := uint64(0)
	if n := len(endings.Archived); n > 0 {
		newest = endings.Archived[n-1]
	}
	return endings.Newest() > newest, nil
}

// reclaimStore and restoreStore are stores whose HasRun is about a run's
// newest ending.
type reclaimStore struct {
	ReclaimStore
	newest *NewestEndings
}

func (s reclaimStore) HasRun(ctx context.Context, runID string) (bool, error) {
	return s.newest.HasRun(ctx, runID)
}

type restoreStore struct {
	RestoreStore
	newest *NewestEndings
}

func (s restoreStore) HasRun(ctx context.Context, runID string) (bool, error) {
	return s.newest.HasRun(ctx, runID)
}

// Reclaim is store asking about newest endings.
func (n *NewestEndings) Reclaim(store ReclaimStore) ReclaimStore { return reclaimStore{store, n} }

// Restore is store asking about newest endings.
func (n *NewestEndings) Restore(store RestoreStore) RestoreStore { return restoreStore{store, n} }
