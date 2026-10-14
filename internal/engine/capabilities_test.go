package engine

import (
	"context"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	deploymentpb "go.temporal.io/api/deployment/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/orkstack/orkstack/internal/task"
)

// capsPoller is one poller record of a described channel: identity, whether
// its last poll is fresh, and the Worker Deployment Version it registered
// with (an empty build is unversioned).
func capsPoller(identity string, fresh bool, deployment, build string) *taskqueuepb.PollerInfo {
	at := releaseNow.Add(-5 * time.Second)
	if !fresh {
		at = releaseNow.Add(-DefaultPollerFreshness - time.Minute)
	}
	info := &taskqueuepb.PollerInfo{Identity: identity, LastAccessTime: timestamppb.New(at)}
	if build != "" {
		info.DeploymentOptions = &deploymentpb.WorkerDeploymentOptions{DeploymentName: deployment, BuildId: build,
			WorkerVersioningMode: enumspb.WORKER_VERSIONING_MODE_VERSIONED}
	}
	return info
}

// tagged is base advertising this build's capabilities.
func tagged(base string) string { return EngineIdentity(base) }

func routedTo(current, ramping string) *taskqueuepb.TaskQueueVersioningInfo {
	info := &taskqueuepb.TaskQueueVersioningInfo{}
	if current != "" {
		info.CurrentDeploymentVersion = &deploymentpb.WorkerDeploymentVersion{DeploymentName: "orkstack", BuildId: current}
	}
	if ramping != "" {
		info.RampingDeploymentVersion = &deploymentpb.WorkerDeploymentVersion{DeploymentName: "orkstack", BuildId: ramping}
		info.RampingVersionPercentage = 25
	}
	return info
}

func missing(t *testing.T, cluster *TemporalReleaseCluster, scope CapabilityScope) []string {
	t.Helper()
	got, err := cluster.MissingCapability(context.Background(), CapabilityEachItems10k, scope)
	if err != nil {
		t.Fatalf("MissingCapability: %v", err)
	}
	return got
}

func TestEngineIdentity_AdvertisesCapabilities(t *testing.T) {
	identity := EngineIdentity("123@host-a@")
	if !strings.HasPrefix(identity, "123@host-a@ ork-caps=") {
		t.Fatalf("identity = %q, want the SDK identity followed by ` ork-caps=`", identity)
	}
	if got := IdentityCapabilities(identity); !reflect.DeepEqual(got, EngineCapabilities) {
		t.Fatalf("parsed %v back from %q, want %v", got, identity, EngineCapabilities)
	}
	if !identityHas(identity, CapabilityEachItems10k) {
		t.Fatalf("%q does not advertise %s", identity, CapabilityEachItems10k)
	}
	for _, other := range []string{"123@host-a@", "123@host-a@ ork-caps=", "", "456@b@ ork-caps=definition-ref"} {
		if identityHas(other, CapabilityEachItems10k) {
			t.Errorf("%q read as advertising %s", other, CapabilityEachItems10k)
		}
	}
	if got := IdentityCapabilities("1@h@ ork-caps= a , b,,c"); !reflect.DeepEqual(got, []string{"a", "b", "c"}) {
		t.Errorf("parsed %v, want [a b c]", got)
	}
}

func TestMissingCapability_ReportsProcessesWithoutIt(t *testing.T) {
	cluster, fake, _ := newFakeReleaseCluster(t, "")
	fake.channels[task.DefaultQueue] = &workflowservice.DescribeTaskQueueResponse{Pollers: []*taskqueuepb.PollerInfo{
		capsPoller(tagged("1@a@"), true, "", ""),
		capsPoller("2@b@", true, "", ""),
		capsPoller("2@b@", true, "", ""), // one process, two records
		capsPoller("3@c@", false, "", ""),
		capsPoller("", true, "", ""),
		capsPoller("4@d@ ork-caps=", true, "", ""),
	}}
	got := missing(t, cluster, ScopeWorkflow)
	if want := []string{"2@b@", "4@d@ ork-caps=", ""}; !reflect.DeepEqual(got, want) {
		t.Fatalf("missing = %q, want %q (stale 3@c ignored, the anonymous poller lacking)", got, want)
	}

	failing, fake2, _ := newFakeReleaseCluster(t, "")
	fake2.channelErr[task.DefaultQueue] = serviceerror.NewUnavailable("down")
	if _, err := failing.MissingCapability(context.Background(), CapabilityEachItems10k, ScopeWorkflow); err == nil {
		t.Fatal("a failed describe was reported as a fleet with the capability")
	}
}

func TestMissingCapability_VersionedChecksOnlyTheBuildsNewRunsStartOn(t *testing.T) {
	pollers := []*taskqueuepb.PollerInfo{
		capsPoller("1@v1@", true, "orkstack", "v1"), // draining, without the tag
		capsPoller("2@v2@", true, "orkstack", "v2"),
		capsPoller(tagged("3@v2@"), true, "orkstack", "v2"),
		capsPoller("4@v3@", true, "orkstack", "v3"),
		capsPoller("5@unversioned@", true, "", ""),
	}
	cluster, fake, _ := newFakeReleaseCluster(t, "")
	fake.channels[task.DefaultQueue] = &workflowservice.DescribeTaskQueueResponse{
		Pollers: pollers, VersioningInfo: routedTo("v2", "v3")}
	if got, want := missing(t, cluster, ScopeWorkflow), []string{"2@v2@", "4@v3@"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("current v2, ramping v3: missing = %q, want %q", got, want)
	}

	unset, fake2, _ := newFakeReleaseCluster(t, "")
	fake2.channels[task.DefaultQueue] = &workflowservice.DescribeTaskQueueResponse{
		Pollers: pollers, VersioningInfo: routedTo("", "v3")}
	if got, want := missing(t, unset, ScopeWorkflow), []string{"4@v3@", "5@unversioned@"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("current unset, ramping v3: missing = %q, want %q", got, want)
	}
}

func TestMissingCapability_IgnoresAnotherDeploymentsEngine(t *testing.T) {
	cluster, fake, _ := newFakeReleaseCluster(t, "")
	fake.channels[task.DefaultQueue] = &workflowservice.DescribeTaskQueueResponse{Pollers: []*taskqueuepb.PollerInfo{
		capsPoller("9@other@", true, "other", "v1"),
		capsPoller(tagged("1@a@"), true, "orkstack", "v1"),
	}}
	if got := missing(t, cluster, ScopeWorkflow); len(got) != 0 {
		t.Fatalf("missing = %q: another deployment's engine was reported", got)
	}
}

func TestMissingCapability_UnversionedChecksEveryPoller(t *testing.T) {
	cluster, fake, _ := newFakeReleaseCluster(t, "")
	fake.channels[task.DefaultQueue] = &workflowservice.DescribeTaskQueueResponse{Pollers: []*taskqueuepb.PollerInfo{
		capsPoller("1@v1@", true, "orkstack", "v1"),
		capsPoller("2@v2@", true, "orkstack", "v2"),
		capsPoller("3@u@", true, "", ""),
		capsPoller(tagged("4@u@"), true, "", ""),
	}}
	got := missing(t, cluster, ScopeWorkflow)
	sort.Strings(got)
	if want := []string{"1@v1@", "2@v2@", "3@u@"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("missing = %q, want every fresh poller without the tag %q", got, want)
	}
}

func TestMissingCapability_WorkflowScopeNeverReadsTheEventsChannel(t *testing.T) {
	cluster, fake, _ := newFakeReleaseCluster(t, "")
	fake.channels[EventQueue] = &workflowservice.DescribeTaskQueueResponse{Pollers: []*taskqueuepb.PollerInfo{
		capsPoller("7@old@", true, "", "")}}
	if got := missing(t, cluster, ScopeWorkflow); len(got) != 0 {
		t.Fatalf("missing = %q", got)
	}
	if len(fake.channelReqs) != 1 || fake.channelReqs[0].GetTaskQueue().GetName() != task.DefaultQueue {
		t.Fatalf("ScopeWorkflow issued %d describes: %v", len(fake.channelReqs), fake.channelReqs)
	}
}

func TestMissingCapability_EventsScopeDescribesTheActivityChannel(t *testing.T) {
	cluster, fake, _ := newFakeReleaseCluster(t, "")
	fake.typed = map[string]map[enumspb.TaskQueueType]*workflowservice.DescribeTaskQueueResponse{
		task.DefaultQueue: {enumspb.TASK_QUEUE_TYPE_WORKFLOW: {Pollers: []*taskqueuepb.PollerInfo{
			capsPoller(tagged("1@a@"), true, "", "")}}},
		// The projection worker registers only PublishEvent, so its pollers
		// are listed under the activity type alone; a workflow-type describe
		// returns none, as the real cluster does.
		EventQueue: {enumspb.TASK_QUEUE_TYPE_ACTIVITY: {Pollers: []*taskqueuepb.PollerInfo{
			capsPoller(tagged("1@a@"), true, "", ""),
			capsPoller("8@draining@", true, "orkstack", "v0"),
		}}},
	}
	got := missing(t, cluster, ScopeWorkflowAndEvents)
	if want := []string{"8@draining@"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("missing = %q, want the events poller without the tag %q", got, want)
	}
	if len(fake.channelReqs) != 2 {
		t.Fatalf("%d describes, want 2", len(fake.channelReqs))
	}
	for i, want := range []struct {
		queue string
		kind  enumspb.TaskQueueType
	}{{task.DefaultQueue, enumspb.TASK_QUEUE_TYPE_WORKFLOW}, {EventQueue, enumspb.TASK_QUEUE_TYPE_ACTIVITY}} {
		req := fake.channelReqs[i]
		if req.GetTaskQueue().GetName() != want.queue || req.GetTaskQueueType() != want.kind {
			t.Fatalf("describe %d = %s as %v, want %s as %v", i, req.GetTaskQueue().GetName(),
				req.GetTaskQueueType(), want.queue, want.kind)
		}
	}
}

func TestEngineReplicas_CountsDistinctIdentitiesRegardlessOfCapsSuffix(t *testing.T) {
	pollers := func(identity func(string) string) []*taskqueuepb.PollerInfo {
		return []*taskqueuepb.PollerInfo{
			capsPoller(identity("1@a@"), true, "orkstack", "v1"),
			capsPoller(identity("1@a@"), true, "orkstack", "v1"), // one process, two records
			capsPoller(identity("2@b@"), true, "orkstack", "v1"),
			capsPoller(identity("3@c@"), true, "", ""),
		}
	}
	counts := func(identity func(string) string) map[string]int {
		cluster, fake, _ := newFakeReleaseCluster(t, "")
		fake.channels[task.DefaultQueue] = &workflowservice.DescribeTaskQueueResponse{Pollers: pollers(identity)}
		got, err := cluster.engineReplicas(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		return got
	}
	plain := counts(func(s string) string { return s })
	suffixed := counts(EngineIdentity)
	if !reflect.DeepEqual(plain, suffixed) || !reflect.DeepEqual(plain, map[string]int{"v1": 2, "": 1}) {
		t.Fatalf("replicas without the suffix %v, with it %v; want both {v1:2, \"\":1}", plain, suffixed)
	}
}
