package engine

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"

	deploymentpb "go.temporal.io/api/deployment/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"
)

// UnversionedRelease is the release a pool's breakdown names its unversioned
// work under: what its total holds beyond every release's own count.
const UnversionedRelease = "unversioned"

// releaseSnapshotFor is how long one read of the deployment's releases
// answers every pool: one pass reads each pool within moments, so a pass
// asks the cluster once, and the next pass, at least MinPoolMetricsInterval
// later, asks again.
const releaseSnapshotFor = MinPoolMetricsInterval / 2

// ReleaseBacklog is one release's share of a pool's backlog (docs/DEPLOY.md,
// "A pool's backlog, as metrics"). HasAge is false where no age was measured:
// for unversioned work, which is a difference of two counts.
type ReleaseBacklog struct {
	Release    string
	Backlog    int64
	BacklogAge time.Duration
	HasAge     bool
}

// poolReleaseClient is what the breakdown asks of the cluster, bound to the
// client's namespace: the deployment's releases, and each release's task
// queues' statistics.
type poolReleaseClient interface {
	DescribeWorkerDeployment(ctx context.Context, in *workflowservice.DescribeWorkerDeploymentRequest) (*workflowservice.DescribeWorkerDeploymentResponse, error)
	DescribeWorkerDeploymentVersion(ctx context.Context, in *workflowservice.DescribeWorkerDeploymentVersionRequest) (*workflowservice.DescribeWorkerDeploymentVersionResponse, error)
}

// releaseSnapshot is one read of every live release's activity statistics,
// by channel then release.
type releaseSnapshot struct {
	at    time.Time
	stats map[string]map[string]*taskqueuepb.TaskQueueStats
	err   error
}

// poolReleases is the breakdown's state: the client, the deployment it reads
// and its last snapshot.
type poolReleases struct {
	client     poolReleaseClient
	deployment string
	mu         sync.Mutex
	last       *releaseSnapshot
}

// WithReleases turns on the breakdown by release for deployment, the
// deployment's name (ORK_DEPLOYMENT_NAME) on an engine whose versioning is
// on. Without it, Releases reports none.
func (b *TemporalPoolBacklog) WithReleases(c poolReleaseClient, deployment string) *TemporalPoolBacklog {
	b.releases = &poolReleases{client: c, deployment: deployment}
	return b
}

// WithTemporalReleases is WithReleases through the client the backlog already
// reads with; an empty deployment (versioning off) leaves it off.
func (b *TemporalPoolBacklog) WithTemporalReleases(deployment string) *TemporalPoolBacklog {
	if c, ok := b.client.(poolReleaseClient); ok && deployment != "" {
		return b.WithReleases(c, deployment)
	}
	return b
}

// Releases breaks pool's backlog, total as Read answered it, down by
// release: each live release's backlog and oldest task's age on the pool's
// channel, by release name, then the unversioned rest. The two reads are
// not taken at one instant, so the rest is never below zero. A release whose
// statistics cannot be read makes the breakdown an error: a breakdown that
// silently left it out would count its work as unversioned. With the
// breakdown off, it reports none.
func (b *TemporalPoolBacklog) Releases(ctx context.Context, pool string, total int64) ([]ReleaseBacklog, error) {
	if b.releases == nil {
		return nil, nil
	}
	queue, ok := b.queues[pool]
	if !ok {
		return nil, fmt.Errorf("pool %q is not measured", pool)
	}
	snap := b.releases.snapshot(ctx, b.now())
	if snap.err != nil {
		return nil, snap.err
	}
	names := make([]string, 0, len(snap.stats[queue]))
	for release := range snap.stats[queue] {
		names = append(names, release)
	}
	sort.Strings(names)
	out := make([]ReleaseBacklog, 0, len(names)+1)
	var counted int64
	for _, release := range names {
		s := snap.stats[queue][release]
		out = append(out, ReleaseBacklog{
			Release:    release,
			Backlog:    s.GetApproximateBacklogCount(),
			BacklogAge: s.GetApproximateBacklogAge().AsDuration(),
			HasAge:     s.GetApproximateBacklogAge() != nil,
		})
		counted += s.GetApproximateBacklogCount()
	}
	return append(out, ReleaseBacklog{Release: UnversionedRelease, Backlog: max(total-counted, 0)}), nil
}

// releaseSnapshotTimeout bounds one read of the releases, which every pool of a
// pass shares.
const releaseSnapshotTimeout = 20 * time.Second

// snapshot is the releases' statistics, read again once releaseSnapshotFor
// has passed. Callers of one pass wait for its one read, which runs under its
// own bound rather than its first caller's cancellation, since every pool of
// the pass reads it. Only an answer is kept: a read that failed is tried
// again by the next caller.
func (r *poolReleases) snapshot(ctx context.Context, now time.Time) *releaseSnapshot {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.last != nil && now.Sub(r.last.at) < releaseSnapshotFor {
		return r.last
	}
	readCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), releaseSnapshotTimeout)
	defer cancel()
	snap := r.read(readCtx, now)
	if snap.err == nil {
		r.last = snap
	}
	return snap
}

// read asks for the deployment's releases, then each live one's statistics.
// A deployment the cluster does not know yet has no release, so all its work
// is unversioned. Drained and inactive releases take no work and are not
// asked.
func (r *poolReleases) read(ctx context.Context, now time.Time) *releaseSnapshot {
	snap := &releaseSnapshot{at: now, stats: map[string]map[string]*taskqueuepb.TaskQueueStats{}}
	described, err := r.client.DescribeWorkerDeployment(ctx, &workflowservice.DescribeWorkerDeploymentRequest{DeploymentName: r.deployment})
	var notFound *serviceerror.NotFound
	switch {
	case errors.As(err, &notFound):
		return snap
	case err != nil:
		snap.err = fmt.Errorf("read the releases of deployment %q: %w", r.deployment, err)
		return snap
	}
	// A release that takes or holds work is current, ramping or draining;
	// drained, inactive and created ones hold none. A status this release
	// does not know is read too, rather than counted as unversioned.
	var live []*deploymentpb.WorkerDeploymentVersion
	for _, summary := range described.GetWorkerDeploymentInfo().GetVersionSummaries() {
		switch summary.GetStatus() {
		case enumspb.WORKER_DEPLOYMENT_VERSION_STATUS_DRAINED,
			enumspb.WORKER_DEPLOYMENT_VERSION_STATUS_INACTIVE,
			enumspb.WORKER_DEPLOYMENT_VERSION_STATUS_CREATED:
			continue
		}
		live = append(live, summary.GetDeploymentVersion())
	}
	answers := make([]*workflowservice.DescribeWorkerDeploymentVersionResponse, len(live))
	failures := make([]error, len(live))
	boundedParallel(len(live), func(i int) {
		answers[i], failures[i] = r.client.DescribeWorkerDeploymentVersion(ctx, &workflowservice.DescribeWorkerDeploymentVersionRequest{
			DeploymentVersion:    live[i],
			ReportTaskQueueStats: true,
		})
	})
	for i, version := range live {
		if failures[i] != nil {
			snap.err = fmt.Errorf("read release %q's statistics: %w", version.GetBuildId(), failures[i])
			snap.stats = nil
			return snap
		}
		for _, tq := range answers[i].GetVersionTaskQueues() {
			if tq.GetType() != enumspb.TASK_QUEUE_TYPE_ACTIVITY {
				continue
			}
			if tq.GetStats() == nil {
				// Asked for and not given: counting the release's tasks as
				// unversioned would say something untrue.
				snap.err = fmt.Errorf("release %q gave no statistics for %q", version.GetBuildId(), tq.GetName())
				snap.stats = nil
				return snap
			}
			if snap.stats[tq.GetName()] == nil {
				snap.stats[tq.GetName()] = map[string]*taskqueuepb.TaskQueueStats{}
			}
			snap.stats[tq.GetName()][version.GetBuildId()] = tq.GetStats()
		}
	}
	return snap
}
