package engine

import (
	"context"
	"time"

	"go.temporal.io/sdk/client"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/identity"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// IdentityChecks is what an engine's identity checks read, wired once: the
// cluster's word on a run's executions and first start, its admissions
// through one cache, and the records of what its tasks follow. The mint and
// CommitResult are built over it (cmd/ork), and so is every test that runs
// them, so a test checks what the engine checks.
type IdentityChecks struct {
	Admissions *identity.AdmissionCache
	Starts     identity.ClusterStarts
	Follows    *identity.FollowsCheck
}

// NewClusterStarts is the start check's reader over records and the cluster:
// a run with no chain record yet is held to the admission its first start's
// plan names.
func NewClusterStarts(records *artifacts.RecordStore, temporal client.Client) identity.ClusterStarts {
	return identity.ClusterStarts{RecordStore: records, Temporal: temporal, Plans: NewTemporalRunReader(temporal)}
}

// NewIdentityChecks wires the checks over records and the cluster, keeping at
// most admissionCacheBytes of admissions.
func NewIdentityChecks(records *artifacts.RecordStore, temporal client.Client, admissionCacheBytes int64) IdentityChecks {
	admissions := identity.NewAdmissionCache(records, admissionCacheBytes)
	return IdentityChecks{
		Admissions: admissions,
		Starts:     NewClusterStarts(records, temporal),
		Follows:    &identity.FollowsCheck{Store: cachedAdmissions{RecordStore: records, admissions: admissions}},
	}
}

// Describe is the cluster's word on runID's current execution.
func (c IdentityChecks) Describe(ctx context.Context, runID string) (identity.ExecutionFacts, error) {
	return c.Starts.Describe(ctx, runID, "")
}

// Recorder is CommitResult's steps 2 and 4 to 8 over the checks, writing to
// records: pinned answers the image the admitted definition pins for a task.
func (c IdentityChecks) Recorder(records *artifacts.RecordStore, pinned func(context.Context, artifacts.Admission, string) (string, error),
	build string, now func() time.Time) *identity.Recorder {
	return &identity.Recorder{
		Stores:      recorderStores{admissions: c.Admissions, records: records},
		Starts:      c.Starts,
		Describe:    c.Describe,
		Follows:     c.Follows,
		PinnedImage: pinned,
		EngineBuild: build,
		Now:         now,
	}
}

// cachedAdmissions reads admissions through the checks' cache, and every
// other record from the record store.
type cachedAdmissions struct {
	*artifacts.RecordStore
	admissions *identity.AdmissionCache
}

func (s cachedAdmissions) ReadAdmission(ctx context.Context, namespace, runID, sha256Hex string) (artifacts.RecordRead[artifacts.Admission], error) {
	return s.admissions.ReadAdmission(ctx, namespace, runID, sha256Hex)
}

// recorderStores reads admissions through the checks' cache and writes
// records to the record store.
type recorderStores struct {
	admissions *identity.AdmissionCache
	records    *artifacts.RecordStore
}

func (s recorderStores) ReadAdmission(ctx context.Context, namespace, runID, sha256Hex string) (artifacts.RecordRead[artifacts.Admission], error) {
	return s.admissions.ReadAdmission(ctx, namespace, runID, sha256Hex)
}

func (s recorderStores) RecordCommit(ctx context.Context, commitKey string, c *orkstackv1.ActivityCommit) (artifacts.RecordedCommit, error) {
	return s.records.RecordCommit(ctx, commitKey, c)
}
