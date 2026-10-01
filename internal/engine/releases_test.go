package engine

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"sync"
	"testing"
	"time"

	commonpb "go.temporal.io/api/common/v1"
	deploymentpb "go.temporal.io/api/deployment/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	workflowpb "go.temporal.io/api/workflow/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/worker"
	"google.golang.org/protobuf/types/known/timestamppb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/task"
)

// --- fakes ------------------------------------------------------------------

type fakeDeploymentHandle struct {
	client.WorkerDeploymentHandle

	mu         sync.Mutex
	desc       client.WorkerDeploymentDescribeResponse
	descErr    error
	versions   map[string]client.WorkerDeploymentVersionDescription
	versionErr map[string]error
	setCurrent []client.WorkerDeploymentSetCurrentVersionOptions
	setRamping []client.WorkerDeploymentSetRampingVersionOptions
}

func (h *fakeDeploymentHandle) Describe(context.Context, client.WorkerDeploymentDescribeOptions) (client.WorkerDeploymentDescribeResponse, error) {
	return h.desc, h.descErr
}

func (h *fakeDeploymentHandle) DescribeVersion(_ context.Context, o client.WorkerDeploymentDescribeVersionOptions) (client.WorkerDeploymentVersionDescription, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if err := h.versionErr[o.BuildID]; err != nil {
		return client.WorkerDeploymentVersionDescription{}, err
	}
	return h.versions[o.BuildID], nil
}

func (h *fakeDeploymentHandle) SetCurrentVersion(_ context.Context, o client.WorkerDeploymentSetCurrentVersionOptions) (client.WorkerDeploymentSetCurrentVersionResponse, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.setCurrent = append(h.setCurrent, o)
	return client.WorkerDeploymentSetCurrentVersionResponse{}, nil
}

func (h *fakeDeploymentHandle) SetRampingVersion(_ context.Context, o client.WorkerDeploymentSetRampingVersionOptions) (client.WorkerDeploymentSetRampingVersionResponse, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.setRamping = append(h.setRamping, o)
	return client.WorkerDeploymentSetRampingVersionResponse{}, nil
}

type fakeDeploymentClient struct {
	client.WorkerDeploymentClient
	handle *fakeDeploymentHandle
	names  []string
}

func (c *fakeDeploymentClient) GetHandle(name string) client.WorkerDeploymentHandle {
	c.names = append(c.names, name)
	return c.handle
}

type fakeReleaseClient struct {
	deployments *fakeDeploymentClient

	mu           sync.Mutex
	enhanced     map[string]client.TaskQueueDescription
	enhancedErr  map[string]error
	enhancedReqs []client.DescribeTaskQueueEnhancedOptions
	routing      []*workflowservice.DescribeTaskQueueResponse // consumed one per call; the last repeats
	routingReqs  []*workflowservice.DescribeTaskQueueRequest
}

func (c *fakeReleaseClient) WorkerDeploymentClient() client.WorkerDeploymentClient {
	return c.deployments
}

func (c *fakeReleaseClient) DescribeTaskQueueEnhanced(_ context.Context, o client.DescribeTaskQueueEnhancedOptions) (client.TaskQueueDescription, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.enhancedReqs = append(c.enhancedReqs, o)
	if err := c.enhancedErr[o.TaskQueue]; err != nil {
		return client.TaskQueueDescription{}, err
	}
	return c.enhanced[o.TaskQueue], nil
}

func (c *fakeReleaseClient) DescribeTaskQueue(_ context.Context, in *workflowservice.DescribeTaskQueueRequest) (*workflowservice.DescribeTaskQueueResponse, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.routingReqs = append(c.routingReqs, in)
	if len(c.routing) == 0 {
		return &workflowservice.DescribeTaskQueueResponse{}, nil
	}
	answer := c.routing[0]
	if len(c.routing) > 1 {
		c.routing = c.routing[1:]
	}
	return answer, nil
}

var releaseNow = time.Date(2026, 9, 24, 9, 15, 0, 0, time.UTC)

func newFakeReleaseCluster(t *testing.T, orkerQueue string) (*TemporalReleaseCluster, *fakeReleaseClient, *fakeDeploymentHandle) {
	t.Helper()
	handle := &fakeDeploymentHandle{
		versions:   map[string]client.WorkerDeploymentVersionDescription{},
		versionErr: map[string]error{},
	}
	fake := &fakeReleaseClient{
		deployments: &fakeDeploymentClient{handle: handle},
		enhanced:    map[string]client.TaskQueueDescription{},
		enhancedErr: map[string]error{},
	}
	cluster, err := newReleaseCluster(fake, nil, "orkstack", task.DefaultQueue, orkerQueue)
	if err != nil {
		t.Fatalf("newReleaseCluster: %v", err)
	}
	cluster.Now = func() time.Time { return releaseNow }
	cluster.routingPoll = time.Millisecond
	return cluster, fake, handle
}

func version(build string) worker.WorkerDeploymentVersion {
	return worker.WorkerDeploymentVersion{DeploymentName: "orkstack", BuildID: build}
}

func versionPtr(build string) *worker.WorkerDeploymentVersion {
	v := version(build)
	return &v
}

// --- ObserveReleases --------------------------------------------------------

func TestObserveReleases_RolesAndDrainFromTheClustersRouting(t *testing.T) {
	cluster, _, handle := newFakeReleaseCluster(t, "")
	currentSince := releaseNow.Add(-2 * time.Hour)
	rampingSince := releaseNow.Add(-10 * time.Minute)
	handle.desc = client.WorkerDeploymentDescribeResponse{
		ConflictToken: []byte("token-1"),
		Info: client.WorkerDeploymentInfo{
			Name: "orkstack",
			RoutingConfig: client.WorkerDeploymentRoutingConfig{
				CurrentVersion:           versionPtr("v2"),
				RampingVersion:           versionPtr("v3"),
				RampingVersionPercentage: 10,
			},
			VersionSummaries: []client.WorkerDeploymentVersionSummary{
				{Version: version("v0"), CreateTime: releaseNow.Add(-72 * time.Hour), DrainageStatus: client.WorkerDeploymentVersionDrainageStatusDrained},
				{Version: version("v1"), CreateTime: releaseNow.Add(-48 * time.Hour), DrainageStatus: client.WorkerDeploymentVersionDrainageStatusDraining},
				{Version: version("v2"), CreateTime: releaseNow.Add(-24 * time.Hour)},
				{Version: version("v3"), CreateTime: releaseNow.Add(-1 * time.Hour)},
				{Version: version("v4"), CreateTime: releaseNow.Add(-30 * time.Minute)},
				{Version: version("v5"), CreateTime: releaseNow.Add(-90 * time.Hour)},
			},
		},
	}
	checked := releaseNow.Add(-3 * time.Minute)
	handle.versions["v1"] = client.WorkerDeploymentVersionDescription{Info: client.WorkerDeploymentVersionInfo{
		RoutingChangedTime: releaseNow.Add(-2 * time.Hour),
		DrainageInfo:       &client.WorkerDeploymentVersionDrainageInfo{LastCheckedTime: checked},
	}}
	handle.versions["v2"] = client.WorkerDeploymentVersionDescription{Info: client.WorkerDeploymentVersionInfo{
		RoutingChangedTime: currentSince, CurrentSinceTime: currentSince,
	}}
	handle.versions["v3"] = client.WorkerDeploymentVersionDescription{Info: client.WorkerDeploymentVersionInfo{
		RoutingChangedTime: rampingSince, RampingSinceTime: rampingSince,
	}}
	handle.versions["v5"] = client.WorkerDeploymentVersionDescription{Info: client.WorkerDeploymentVersionInfo{
		RoutingChangedTime: releaseNow.Add(-5 * time.Minute),
	}}

	obs, err := cluster.ObserveReleases(context.Background())
	if err != nil {
		t.Fatalf("ObserveReleases: %v", err)
	}
	if want := (Routing{Current: "v2", Ramping: "v3", RampPercent: 10}); obs.Routing != want {
		t.Fatalf("routing = %+v, want %+v", obs.Routing, want)
	}
	if string(obs.conflictToken) != "token-1" {
		t.Fatalf("conflict token = %q", obs.conflictToken)
	}
	if !obs.CurrentSince.Equal(currentSince) || !obs.RampingSince.Equal(rampingSince) {
		t.Fatalf("since = %v / %v", obs.CurrentSince, obs.RampingSince)
	}
	byBuild := map[string]ObservedRelease{}
	for _, r := range obs.Releases {
		byBuild[r.BuildID] = r
	}
	wantDrain := map[string]orkstackv1.ReleaseDrainState{
		"v0": orkstackv1.ReleaseDrainState_RELEASE_DRAIN_STATE_DRAINED,
		"v1": orkstackv1.ReleaseDrainState_RELEASE_DRAIN_STATE_DRAINING,
		"v4": orkstackv1.ReleaseDrainState_RELEASE_DRAIN_STATE_UNSPECIFIED,
	}
	for build, want := range wantDrain {
		if got := byBuild[build].DrainState; got != want {
			t.Errorf("%s drain = %v, want %v", build, got, want)
		}
	}
	// v2 and v3 have changed routing too (they are current/ramping); what this
	// asserts is the PREVIOUS/CANDIDATE split among the others.
	for build, want := range map[string]bool{"v0": true, "v1": true, "v4": false, "v5": true} {
		if got := byBuild[build].HasServed; got != want {
			t.Errorf("%s HasServed = %v, want %v", build, got, want)
		}
	}
	if !byBuild["v1"].DrainCheckedAt.Equal(checked) {
		t.Errorf("v1 drain checked at %v, want %v", byBuild["v1"].DrainCheckedAt, checked)
	}
}

func TestObserveReleases_UnknownDeploymentIsNotVersioned(t *testing.T) {
	cluster, _, handle := newFakeReleaseCluster(t, "")
	handle.descErr = serviceerror.NewNotFound("worker deployment orkstack not found")
	obs, err := cluster.ObserveReleases(context.Background())
	if !errors.Is(err, ErrNotVersioned) {
		t.Fatalf("err = %v, want ErrNotVersioned", err)
	}
	if obs.DeploymentName != "orkstack" {
		t.Fatalf("deployment name = %q", obs.DeploymentName)
	}
}

func TestObserveReleases_ADescribeVersionFailureKeepsTheRelease(t *testing.T) {
	cluster, _, handle := newFakeReleaseCluster(t, "")
	handle.desc = client.WorkerDeploymentDescribeResponse{Info: client.WorkerDeploymentInfo{
		RoutingConfig: client.WorkerDeploymentRoutingConfig{CurrentVersion: versionPtr("v1")},
		VersionSummaries: []client.WorkerDeploymentVersionSummary{
			{Version: version("v1")}, {Version: version("v2")},
		},
	}}
	handle.versions["v1"] = client.WorkerDeploymentVersionDescription{Info: client.WorkerDeploymentVersionInfo{
		TaskQueuesInfos: []client.WorkerDeploymentTaskQueueInfo{{Name: "ork-tasks", Type: client.TaskQueueTypeActivity}},
	}}
	handle.versionErr["v2"] = serviceerror.NewUnavailable("frontend down")
	obs, err := cluster.ObserveReleases(context.Background())
	if err != nil {
		t.Fatalf("ObserveReleases: %v", err)
	}
	if len(obs.Releases) != 2 {
		t.Fatalf("releases = %+v", obs.Releases)
	}
	for _, r := range obs.Releases {
		switch r.BuildID {
		case "v1":
			if r.DescribeErr != nil || !reflect.DeepEqual(r.RegisteredPools, []string{"main"}) {
				t.Errorf("v1 = %+v", r)
			}
		case "v2":
			if r.DescribeErr == nil {
				t.Errorf("v2 has no DescribeErr")
			}
		}
	}
}

// --- engine replicas --------------------------------------------------------

func TestEngineReplicas_CountFreshWorkflowPollersByRelease(t *testing.T) {
	cluster, fake, _ := newFakeReleaseCluster(t, "")
	fresh := releaseNow.Add(-5 * time.Second)
	stale := releaseNow.Add(-5 * time.Minute)
	// The SDK does not export the poller-options type, so the field's own
	// type is instantiated by reflection.
	poller := func(identity string, at time.Time, deployment, build string) client.TaskQueuePollerInfo {
		info := client.TaskQueuePollerInfo{Identity: identity, LastAccessTime: at}
		field := reflect.ValueOf(&info).Elem().FieldByName("WorkerDeploymentPollerOptions")
		opts := reflect.New(field.Type().Elem())
		opts.Elem().FieldByName("DeploymentName").SetString(deployment)
		opts.Elem().FieldByName("BuildID").SetString(build)
		field.Set(opts)
		return info
	}
	fake.enhanced[task.DefaultQueue] = client.TaskQueueDescription{VersionsInfo: map[string]client.TaskQueueVersionInfo{
		"orkstack.v1": {TypesInfo: map[client.TaskQueueType]client.TaskQueueTypeInfo{
			client.TaskQueueTypeWorkflow: {Pollers: []client.TaskQueuePollerInfo{
				poller("1@a", fresh, "orkstack", "v1"),
				poller("2@b", fresh, "orkstack", "v1"),
				poller("9@x", fresh, "other", "v1"),
			}},
		}},
		"orkstack.v2": {TypesInfo: map[client.TaskQueueType]client.TaskQueueTypeInfo{
			client.TaskQueueTypeWorkflow: {Pollers: []client.TaskQueuePollerInfo{
				poller("1@a", fresh, "orkstack", "v1"),
				poller("3@c", fresh, "orkstack", "v2"),
				poller("4@d", stale, "orkstack", "v2"),
			}},
		}},
		"": {TypesInfo: map[client.TaskQueueType]client.TaskQueueTypeInfo{
			client.TaskQueueTypeWorkflow: {Pollers: []client.TaskQueuePollerInfo{
				{Identity: "5@e", LastAccessTime: fresh},
			}},
			client.TaskQueueTypeActivity: {Pollers: []client.TaskQueuePollerInfo{
				{Identity: "orker@f", LastAccessTime: fresh},
			}},
		}},
	}}
	got, err := cluster.engineReplicas(context.Background())
	if err != nil {
		t.Fatalf("engineReplicas: %v", err)
	}
	if want := map[string]int{"v1": 2, "v2": 1, "": 1}; !reflect.DeepEqual(got, want) {
		t.Fatalf("replicas = %v, want %v", got, want)
	}
	req := fake.enhancedReqs[0]
	if req.TaskQueue != task.DefaultQueue || !reflect.DeepEqual(req.TaskQueueTypes, []client.TaskQueueType{client.TaskQueueTypeWorkflow}) ||
		!req.ReportPollers || req.Versions == nil || !req.Versions.AllActive || !req.Versions.Unversioned {
		t.Fatalf("request = %+v", req)
	}
}

// --- registered pools -------------------------------------------------------

func TestRegisteredPools_TranslateThroughTheOnlyMapping(t *testing.T) {
	infos := []client.WorkerDeploymentTaskQueueInfo{
		{Name: "ork-tasks", Type: client.TaskQueueTypeWorkflow},
		{Name: "ork-tasks", Type: client.TaskQueueTypeActivity},
		{Name: "ork-pool-gpu", Type: client.TaskQueueTypeActivity},
		{Name: "ork-pool-build__internal_session_creation", Type: client.TaskQueueTypeActivity},
		{Name: "ork-tasks-agent", Type: client.TaskQueueTypeActivity},
	}
	cluster, _, _ := newFakeReleaseCluster(t, "")
	if got := cluster.registeredPools(infos); !reflect.DeepEqual(got, []string{"gpu", "main"}) {
		t.Fatalf("default route ork-tasks: pools = %v", got)
	}
	gpuDefault, _, _ := newFakeReleaseCluster(t, "ork-pool-gpu")
	got := gpuDefault.registeredPools([]client.WorkerDeploymentTaskQueueInfo{{Name: "ork-pool-gpu", Type: client.TaskQueueTypeActivity}})
	if !reflect.DeepEqual(got, []string{"gpu"}) {
		t.Fatalf("default route ork-pool-gpu: pools = %v", got)
	}
	got = gpuDefault.registeredPools([]client.WorkerDeploymentTaskQueueInfo{{Name: "ork-tasks", Type: client.TaskQueueTypeActivity}})
	if !reflect.DeepEqual(got, []string{"main"}) {
		t.Fatalf("default route ork-pool-gpu, ork-tasks: pools = %v", got)
	}
}

// --- pool use ---------------------------------------------------------------

func TestPoolUse_InUseByPollerBacklogOrRate(t *testing.T) {
	fresh := releaseNow.Add(-5 * time.Second)
	stale := releaseNow.Add(-5 * time.Minute)
	describe := func(poller time.Time, stats *client.TaskQueueStats) client.TaskQueueDescription {
		return client.TaskQueueDescription{VersionsInfo: map[string]client.TaskQueueVersionInfo{
			"": {TypesInfo: map[client.TaskQueueType]client.TaskQueueTypeInfo{
				client.TaskQueueTypeActivity: {
					Pollers: []client.TaskQueuePollerInfo{{Identity: "orker@a", LastAccessTime: poller}},
					Stats:   stats,
				},
			}},
		}}
	}
	cases := []struct {
		name    string
		desc    client.TaskQueueDescription
		err     error
		inUse   bool
		wantErr bool
	}{
		{"fresh poller", describe(fresh, &client.TaskQueueStats{}), nil, true, false},
		{"stale poller, backlog 3", describe(stale, &client.TaskQueueStats{ApproximateBacklogCount: 3}), nil, true, false},
		{"add rate 0.2", describe(stale, &client.TaskQueueStats{TasksAddRate: 0.2}), nil, true, false},
		{"stale and zero", describe(stale, &client.TaskQueueStats{}), nil, false, false},
		{"every stats nil", describe(stale, nil), nil, false, true},
		{"describe error", client.TaskQueueDescription{}, serviceerror.NewUnavailable("down"), false, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cluster, fake, _ := newFakeReleaseCluster(t, "")
			fake.enhanced["ork-pool-build"] = tc.desc
			if tc.err != nil {
				fake.enhancedErr["ork-pool-build"] = tc.err
			}
			use := cluster.describePoolUse(context.Background(), "build")
			if use.InUse() != tc.inUse || (use.Err != nil) != tc.wantErr {
				t.Fatalf("use = %+v, in use %v; want in use %v, err %v", use, use.InUse(), tc.inUse, tc.wantErr)
			}
			if tc.err == nil {
				req := fake.enhancedReqs[0]
				if req.TaskQueue != "ork-pool-build" || !req.ReportStats || !req.ReportPollers ||
					!req.Versions.AllActive || !req.Versions.Unversioned {
					t.Fatalf("request = %+v", req)
				}
			}
		})
	}
}

// --- runs on a release ------------------------------------------------------

type fakeReleaseIndex struct {
	runReadClient
	count   int64
	infos   []*workflowpb.WorkflowExecutionInfo
	counts  []string
	lists   []*workflowservice.ListWorkflowExecutionsRequest
	listErr error
}

func (f *fakeReleaseIndex) CountWorkflow(_ context.Context, in *workflowservice.CountWorkflowExecutionsRequest) (*workflowservice.CountWorkflowExecutionsResponse, error) {
	f.counts = append(f.counts, in.GetQuery())
	return &workflowservice.CountWorkflowExecutionsResponse{Count: f.count}, nil
}

func (f *fakeReleaseIndex) ListWorkflow(_ context.Context, in *workflowservice.ListWorkflowExecutionsRequest) (*workflowservice.ListWorkflowExecutionsResponse, error) {
	f.lists = append(f.lists, in)
	if f.listErr != nil {
		return nil, f.listErr
	}
	n := int(in.GetPageSize())
	if n > len(f.infos) {
		n = len(f.infos)
	}
	return &workflowservice.ListWorkflowExecutionsResponse{Executions: f.infos[:n]}, nil
}

func TestReleaseRunsQuery_IsTheIndexedVersion(t *testing.T) {
	want := `WorkflowType = "RunWorkflow" AND ExecutionStatus = "Running" AND TemporalWorkerDeploymentVersion = "orkstack:v2"`
	if got := releaseRunsQuery("orkstack:v2"); got != want {
		t.Fatalf("query = %s\nwant    %s", got, want)
	}
}

func TestReleaseRuns_CountsAndListsNewestShallowRuns(t *testing.T) {
	dc := converter.GetDefaultDataConverter()
	memo := func(fields map[string]string) *commonpb.Memo {
		out := &commonpb.Memo{Fields: map[string]*commonpb.Payload{}}
		for k, v := range fields {
			p, err := dc.ToPayload(v)
			if err != nil {
				t.Fatal(err)
			}
			out.Fields[k] = p
		}
		return out
	}
	index := &fakeReleaseIndex{count: 14}
	for i := 0; i < 14; i++ {
		index.infos = append(index.infos, &workflowpb.WorkflowExecutionInfo{
			Execution: &commonpb.WorkflowExecution{WorkflowId: fmt.Sprintf("ork-%02d", i)},
			Status:    enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
			StartTime: timestamppb.New(releaseNow.Add(-time.Duration(i) * time.Minute)),
			Memo: memo(map[string]string{
				MemoNamespace: "payments", MemoOrkflowName: "nightly-build", MemoOrkflowVersion: "4",
			}),
		})
	}
	reader := newRunReader(index, dc)
	n, runs, err := reader.ReleaseRuns(context.Background(), "orkstack", "v2", 10)
	if err != nil {
		t.Fatalf("ReleaseRuns: %v", err)
	}
	if n != 14 || len(runs) != 10 {
		t.Fatalf("n = %d, runs = %d; want 14 and 10", n, len(runs))
	}
	if len(index.lists) != 1 || index.lists[0].GetPageSize() != 10 {
		t.Fatalf("lists = %v", index.lists)
	}
	wantQuery := releaseRunsQuery("orkstack:v2")
	if index.counts[0] != wantQuery || index.lists[0].GetQuery() != wantQuery {
		t.Fatalf("queries = %q / %q", index.counts[0], index.lists[0].GetQuery())
	}
	first := runs[0]
	if first.GetId() != "ork-00" || first.GetNamespace() != "payments" || first.GetOrkflowName() != "nightly-build" ||
		first.GetOrkflowVersion() != "4" || first.GetStatus() != orkstackv1.Status_STATUS_RUNNING {
		t.Fatalf("first run = %v", first)
	}
	if !sort.SliceIsSorted(runs, func(i, j int) bool {
		return runs[i].GetCreatedAt().AsTime().After(runs[j].GetCreatedAt().AsTime())
	}) {
		t.Fatalf("runs are not newest first")
	}

	if _, _, err := reader.ReleaseRuns(context.Background(), "orkstack", `v"2`, 10); err == nil {
		t.Fatalf("a build id with a quote was asked of the index")
	}
}

func TestRoutingRevision_EqualExactlyWhenRoutingIsEqual(t *testing.T) {
	routings := []Routing{
		{}, {Current: "v1"}, {Current: "v1", Ramping: "v2", RampPercent: 10},
		{Current: "v1", Ramping: "v2", RampPercent: 10.5}, {Current: "v1", Ramping: "v2", RampPercent: 20},
		{Current: "v2", Ramping: "v1", RampPercent: 10}, {Ramping: "v1", RampPercent: 10},
	}
	for i, a := range routings {
		for j, b := range routings {
			if (a.Revision() == b.Revision()) != (i == j) {
				t.Errorf("%+v vs %+v: revisions %q / %q", a, b, a.Revision(), b.Revision())
			}
		}
	}
	if (Routing{Current: "v1"}).Revision() != (Routing{Current: "v1"}).Revision() {
		t.Fatal("equal routings differ")
	}
}

// --- writes -----------------------------------------------------------------

func TestSetRouting_KeepsTheClustersOwnGuards(t *testing.T) {
	cluster, _, handle := newFakeReleaseCluster(t, "")
	obs := ReleaseObservation{Routing: Routing{Current: "v1"}, conflictToken: []byte("tok")}
	if err := cluster.SetRouting(context.Background(), obs, Routing{Current: "v2"}); err != nil {
		t.Fatalf("SetRouting: %v", err)
	}
	if len(handle.setCurrent) != 1 || len(handle.setRamping) != 0 {
		t.Fatalf("calls: current %d, ramping %d", len(handle.setCurrent), len(handle.setRamping))
	}
	call := handle.setCurrent[0]
	if call.BuildID != "v2" || string(call.ConflictToken) != "tok" || call.IgnoreMissingTaskQueues || call.AllowNoPollers {
		t.Fatalf("call = %+v", call)
	}
}

func TestSetRouting_StoppingARampUnsetsTheRampingRelease(t *testing.T) {
	cluster, _, handle := newFakeReleaseCluster(t, "")
	obs := ReleaseObservation{Routing: Routing{Current: "v1", Ramping: "v2", RampPercent: 10}, conflictToken: []byte("tok")}
	if err := cluster.SetRouting(context.Background(), obs, Routing{Current: "v1"}); err != nil {
		t.Fatalf("SetRouting: %v", err)
	}
	if len(handle.setRamping) != 1 || len(handle.setCurrent) != 0 {
		t.Fatalf("calls: current %d, ramping %d", len(handle.setCurrent), len(handle.setRamping))
	}
	call := handle.setRamping[0]
	if call.BuildID != "" || call.Percentage != 0 || string(call.ConflictToken) != "tok" ||
		call.IgnoreMissingTaskQueues || call.AllowNoPollers {
		t.Fatalf("call = %+v", call)
	}
}

func TestAwaitRouting_ReadsTheEngineChannelInDefaultMode(t *testing.T) {
	routed := func(current string) *workflowservice.DescribeTaskQueueResponse {
		return &workflowservice.DescribeTaskQueueResponse{VersioningInfo: &taskqueuepb.TaskQueueVersioningInfo{
			CurrentDeploymentVersion: &deploymentpb.WorkerDeploymentVersion{DeploymentName: "orkstack", BuildId: current},
		}}
	}
	cluster, fake, _ := newFakeReleaseCluster(t, "")
	fake.routing = []*workflowservice.DescribeTaskQueueResponse{routed("v1"), routed("v1"), routed("v2")}
	ok, err := cluster.AwaitRouting(context.Background(), Routing{Current: "v2"}, 5*time.Second)
	if err != nil || !ok {
		t.Fatalf("AwaitRouting = %v, %v", ok, err)
	}
	if len(fake.routingReqs) != 3 {
		t.Fatalf("polls = %d, want 3", len(fake.routingReqs))
	}
	req := fake.routingReqs[0]
	if req.GetTaskQueue().GetName() != task.DefaultQueue || req.GetTaskQueueType() != enumspb.TASK_QUEUE_TYPE_WORKFLOW ||
		req.GetApiMode() != enumspb.DESCRIBE_TASK_QUEUE_MODE_UNSPECIFIED {
		t.Fatalf("request = %v", req)
	}

	never, fake2, _ := newFakeReleaseCluster(t, "")
	fake2.routing = []*workflowservice.DescribeTaskQueueResponse{routed("v1")}
	start := time.Now()
	ok, err = never.AwaitRouting(context.Background(), Routing{Current: "v2"}, 50*time.Millisecond)
	if ok || err != nil {
		t.Fatalf("never matching: %v, %v", ok, err)
	}
	if time.Since(start) < 50*time.Millisecond {
		t.Fatalf("returned before within elapsed")
	}
}
