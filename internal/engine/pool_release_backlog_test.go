package engine

import (
	"context"
	"errors"
	"slices"
	"sync"
	"testing"
	"time"

	deploymentpb "go.temporal.io/api/deployment/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"
	"google.golang.org/protobuf/types/known/durationpb"

	"github.com/orkstack/orkstack/internal/testsupport/must"
)

// fakeReleaseStatsClient answers the deployment's versions and, per version,
// its task queues' statistics.
type fakeReleaseStatsClient struct {
	fakePoolStatsClient
	mu            sync.Mutex
	versions      map[string]enumspb.WorkerDeploymentVersionStatus  // build id → status
	queues        map[string]map[string]*taskqueuepb.TaskQueueStats // build id → channel → stats
	noDeploy      bool
	failBuild     string
	describes     int
	versionsAsked []string
}

func (f *fakeReleaseStatsClient) DescribeWorkerDeployment(_ context.Context, in *workflowservice.DescribeWorkerDeploymentRequest) (*workflowservice.DescribeWorkerDeploymentResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.describes++
	if f.noDeploy {
		return nil, serviceerror.NewNotFound("no worker deployment " + in.GetDeploymentName())
	}
	var summaries []*deploymentpb.WorkerDeploymentInfo_WorkerDeploymentVersionSummary
	for build, status := range f.versions {
		summaries = append(summaries, &deploymentpb.WorkerDeploymentInfo_WorkerDeploymentVersionSummary{
			DeploymentVersion: &deploymentpb.WorkerDeploymentVersion{DeploymentName: in.GetDeploymentName(), BuildId: build},
			Status:            status,
		})
	}
	return &workflowservice.DescribeWorkerDeploymentResponse{
		WorkerDeploymentInfo: &deploymentpb.WorkerDeploymentInfo{Name: in.GetDeploymentName(), VersionSummaries: summaries},
	}, nil
}

func (f *fakeReleaseStatsClient) DescribeWorkerDeploymentVersion(_ context.Context, in *workflowservice.DescribeWorkerDeploymentVersionRequest) (*workflowservice.DescribeWorkerDeploymentVersionResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	build := in.GetDeploymentVersion().GetBuildId()
	f.versionsAsked = append(f.versionsAsked, build)
	if !in.GetReportTaskQueueStats() {
		return nil, errors.New("asked without task queue stats")
	}
	if build == f.failBuild {
		return nil, serviceerror.NewUnavailable("frontend down")
	}
	var tqs []*workflowservice.DescribeWorkerDeploymentVersionResponse_VersionTaskQueue
	for name, stats := range f.queues[build] {
		tqs = append(tqs,
			&workflowservice.DescribeWorkerDeploymentVersionResponse_VersionTaskQueue{Name: name, Type: enumspb.TASK_QUEUE_TYPE_ACTIVITY, Stats: stats},
			// The same channel's workflow side, which a pool's backlog never counts.
			&workflowservice.DescribeWorkerDeploymentVersionResponse_VersionTaskQueue{Name: name, Type: enumspb.TASK_QUEUE_TYPE_WORKFLOW,
				Stats: &taskqueuepb.TaskQueueStats{ApproximateBacklogCount: 1000}})
	}
	return &workflowservice.DescribeWorkerDeploymentVersionResponse{VersionTaskQueues: tqs}, nil
}

func stats(n int64, age time.Duration) *taskqueuepb.TaskQueueStats {
	return &taskqueuepb.TaskQueueStats{ApproximateBacklogCount: n, ApproximateBacklogAge: durationpb.New(age)}
}

// The breakdown of a pool's backlog by release: each live release's activity
// backlog and oldest task's age on the pool's channel, read once a pass for
// every pool, and the rest of the pool's total as unversioned work, never
// below zero and with no age. A drained or inactive release has nothing to
// count and is not asked.
func TestPoolBacklog_ReleasesBreakTheTotalDownByRelease(t *testing.T) {
	client := &fakeReleaseStatsClient{
		versions: map[string]enumspb.WorkerDeploymentVersionStatus{
			"b-new":  enumspb.WORKER_DEPLOYMENT_VERSION_STATUS_CURRENT,
			"b-old":  enumspb.WORKER_DEPLOYMENT_VERSION_STATUS_DRAINING,
			"b-gone": enumspb.WORKER_DEPLOYMENT_VERSION_STATUS_DRAINED,
			"b-idle": enumspb.WORKER_DEPLOYMENT_VERSION_STATUS_INACTIVE,
		},
		queues: map[string]map[string]*taskqueuepb.TaskQueueStats{
			"b-new": {"ork-pool-gpu": stats(5, 10*time.Second), "ork-tasks": stats(2, time.Second)},
			"b-old": {"ork-pool-gpu": stats(3, 20*time.Minute)},
		},
	}
	b, err := newPoolBacklog(client, "", []string{"gpu"})
	must.OK(t, err)
	b.WithReleases(client, "orkstack")

	got, err := b.Releases(context.Background(), "gpu", 12)
	must.OK(t, err)
	want := []ReleaseBacklog{
		{Release: "b-new", Backlog: 5, BacklogAge: 10 * time.Second, HasAge: true},
		{Release: "b-old", Backlog: 3, BacklogAge: 20 * time.Minute, HasAge: true},
		{Release: UnversionedRelease, Backlog: 4},
	}
	if !slices.Equal(got, want) {
		t.Fatalf("Releases(gpu) = %+v; want %+v", got, want)
	}
	// A second pool of the same pass reads the same answer, asked once.
	main, err := b.Releases(context.Background(), "main", 1)
	must.OK(t, err)
	if !slices.Equal(main, []ReleaseBacklog{{Release: "b-new", Backlog: 2, BacklogAge: time.Second, HasAge: true}, {Release: UnversionedRelease}}) {
		t.Fatalf("Releases(main) = %+v", main)
	}
	if client.describes != 1 || len(client.versionsAsked) != 2 {
		t.Fatalf("asked the deployment %d times and versions %v; want once, and only the two live releases", client.describes, client.versionsAsked)
	}
}

// Unversioned work is what the total holds beyond every release's own count;
// the two reads are not taken at one instant, so a total read below the
// releases' sum gives zero unversioned work, never a negative count.
func TestPoolBacklog_UnversionedWorkIsNeverNegative(t *testing.T) {
	client := &fakeReleaseStatsClient{
		versions: map[string]enumspb.WorkerDeploymentVersionStatus{"b1": enumspb.WORKER_DEPLOYMENT_VERSION_STATUS_CURRENT},
		queues:   map[string]map[string]*taskqueuepb.TaskQueueStats{"b1": {"ork-tasks": stats(9, time.Minute)}},
	}
	b, err := newPoolBacklog(client, "", nil)
	must.OK(t, err)
	b.WithReleases(client, "orkstack")
	got, err := b.Releases(context.Background(), "main", 7)
	must.OK(t, err)
	if len(got) != 2 || got[1] != (ReleaseBacklog{Release: UnversionedRelease}) {
		t.Fatalf("Releases = %+v; want b1's 9 and an unversioned 0", got)
	}
}

// A deployment the cluster has no versions of yet holds only unversioned
// work; a version whose statistics cannot be read is an error, never a
// breakdown that silently leaves it out; and an engine with versioning off
// reports no breakdown at all.
func TestPoolBacklog_ReleasesWhenTheDeploymentCannotBeRead(t *testing.T) {
	none := &fakeReleaseStatsClient{noDeploy: true}
	b, err := newPoolBacklog(none, "", nil)
	must.OK(t, err)
	b.WithReleases(none, "orkstack")
	got, err := b.Releases(context.Background(), "main", 3)
	must.OK(t, err)
	if !slices.Equal(got, []ReleaseBacklog{{Release: UnversionedRelease, Backlog: 3}}) {
		t.Fatalf("no deployment yet: Releases = %+v; want all 3 unversioned", got)
	}

	failing := &fakeReleaseStatsClient{
		versions:  map[string]enumspb.WorkerDeploymentVersionStatus{"b1": enumspb.WORKER_DEPLOYMENT_VERSION_STATUS_CURRENT},
		failBuild: "b1",
	}
	b, err = newPoolBacklog(failing, "", nil)
	must.OK(t, err)
	b.WithReleases(failing, "orkstack")
	if got, err := b.Releases(context.Background(), "main", 3); err == nil || got != nil {
		t.Fatalf("an unreadable version: Releases = %+v, %v; want no breakdown and an error", got, err)
	}

	off, err := newPoolBacklog(&fakePoolStatsClient{}, "", nil)
	must.OK(t, err)
	if got, err := off.Releases(context.Background(), "main", 3); err != nil || got != nil {
		t.Fatalf("versioning off: Releases = %+v, %v; want none and no error", got, err)
	}
}

// A read that failed is not kept: the next caller of the same pass reads
// again, and the first caller going away does not end the shared read.
func TestPoolBacklog_AFailedReleaseReadIsTriedAgain(t *testing.T) {
	client := &fakeReleaseStatsClient{
		versions:  map[string]enumspb.WorkerDeploymentVersionStatus{"b1": enumspb.WORKER_DEPLOYMENT_VERSION_STATUS_CURRENT},
		queues:    map[string]map[string]*taskqueuepb.TaskQueueStats{"b1": {"ork-tasks": stats(2, time.Second)}},
		failBuild: "b1",
	}
	b, err := newPoolBacklog(client, "", nil)
	must.OK(t, err)
	b.WithReleases(client, "orkstack")
	if _, err := b.Releases(context.Background(), "main", 3); err == nil {
		t.Fatal("an unreadable release: Releases = nil error; want it named")
	}
	client.mu.Lock()
	client.failBuild = ""
	client.mu.Unlock()
	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	got, err := b.Releases(cancelled, "main", 3)
	must.OK(t, err)
	if !slices.Equal(got, []ReleaseBacklog{{Release: "b1", Backlog: 2, BacklogAge: time.Second, HasAge: true}, {Release: UnversionedRelease, Backlog: 1}}) {
		t.Fatalf("Releases after the failure, by a caller that went away = %+v; want b1's 2 read again", got)
	}
}
