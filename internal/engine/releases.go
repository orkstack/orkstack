package engine

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"sync"
	"time"

	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/client"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/task"
)

// Releases (orkctl-release §5): the engine's view of the deployment's
// releases — which one the cluster routes new runs to, which ones it still
// knows, what each still executes — and the one write that changes the
// routing.
//
// A release is one build of OrkStack, `ork` and `orker` deployed together and
// named by ORK_BUILD_ID. The cluster object it is implemented with is
// internal, exactly as a session is internal to a pin: nothing here returns a
// channel name, and every cluster error is logged and wrapped so the API can
// replace it with a fixed sentence (§5.8). Nothing is stored: every answer is
// read from the cluster on the call, so any replica answers identically.

// ErrNotVersioned reports that the cluster has no record of this deployment:
// no process of it has ever polled with ORK_BUILD_ID set.
var ErrNotVersioned = errors.New("no release of this deployment has registered")

// Routing is which releases take new runs. Current "" means the processes
// started without ORK_BUILD_ID do; Ramping "" means nothing is ramping.
type Routing struct {
	Current     string
	Ramping     string
	RampPercent float32
}

// Revision renders Routing as the opaque token ListReleasesResponse.revision
// carries: "r1|<current>|<ramping>|<percent formatted %g>". Nothing parses it;
// two Routings are equal exactly when their revisions are.
func (r Routing) Revision() string {
	return fmt.Sprintf("r1|%s|%s|%g", r.Current, r.Ramping, r.RampPercent)
}

// ReleaseCluster is the engine's view of the deployment's releases: what the
// cluster routes, which releases it knows, and what each still executes.
// Production wires *TemporalReleaseCluster; a test states the facts directly.
type ReleaseCluster interface {
	// ObserveReleases reads the deployment in one pass (§5.2–5.4).
	// ErrNotVersioned when the cluster has no record of it; the observation
	// returned beside that error still carries DeploymentName.
	ObserveReleases(ctx context.Context) (ReleaseObservation, error)
	// ReleaseRuns counts the runs executing on one release and lists up to
	// limit of them, newest first, as shallow projections (§5.5).
	ReleaseRuns(ctx context.Context, buildID string, limit int) (int64, []*orkstackv1.Run, error)
	// RunningRuns counts every run executing on the deployment, on any
	// release or none (§5.5).
	RunningRuns(ctx context.Context) (int64, error)
	// SetRouting makes exactly one cluster call moving from obs.Routing to
	// want (§5.6).
	SetRouting(ctx context.Context, obs ReleaseObservation, want Routing) error
	// AwaitRouting polls the engine's own channel until it routes as want,
	// or within elapses (§5.7).
	AwaitRouting(ctx context.Context, want Routing, within time.Duration) (bool, error)
}

// ReleaseObservation is one read of the deployment's releases.
type ReleaseObservation struct {
	DeploymentName string
	Routing        Routing
	CurrentSince   time.Time
	RampingSince   time.Time
	Releases       []ObservedRelease
	// EngineReplicas maps a build id to its live engine processes; the key ""
	// counts processes started without ORK_BUILD_ID.
	EngineReplicas map[string]int
	// PoolUse is keyed by pool, over the pools the Current and Ramping
	// releases registered (§5.4). Pools only; no channel name crosses this
	// seam.
	PoolUse map[string]PoolUse
	// conflictToken is the cluster's serialization token from the same
	// Describe; SetRouting passes it back. Unexported: it never leaves the
	// engine.
	conflictToken []byte
}

// ObservedRelease is one release the cluster still lists.
type ObservedRelease struct {
	BuildID        string
	FirstSeenAt    time.Time
	DrainState     orkstackv1.ReleaseDrainState
	DrainCheckedAt time.Time
	// HasServed is true once the release has ever been current or ramping:
	// it has a drain state, or its routing has ever changed. It is what
	// tells PREVIOUS from CANDIDATE.
	HasServed bool
	// RegisteredPools are the pools whose channels this release's Orkers have
	// ever polled, as the cluster recorded them (§5.4).
	RegisteredPools []string
	// DescribeErr is set when the release's own description could not be
	// read; RegisteredPools and DrainCheckedAt are then unknown.
	DescribeErr error
}

// PoolUse is the cluster's evidence that a pool's channel is in use.
type PoolUse struct {
	LivePollers int     // distinct identities within DefaultPollerFreshness
	Backlog     int64   // tasks waiting, summed over every version
	AddRate     float32 // tasks added per second, summed
	Err         error   // the channel could not be described, or no stats came back
}

// InUse reports whether the cluster has any evidence of work on the pool.
func (u PoolUse) InUse() bool { return u.LivePollers > 0 || u.Backlog > 0 || u.AddRate > 0 }

// ReleaseClusterOp names which cluster call a ReleaseClusterError came from,
// so the API can choose its fixed sentence without reading the cluster's text.
type ReleaseClusterOp string

const (
	// ReleaseOpRead is any read: Describe, DescribeVersion, a channel
	// description, the run index.
	ReleaseOpRead ReleaseClusterOp = "read"
	// ReleaseOpWrite is SetRouting's one call.
	ReleaseOpWrite ReleaseClusterOp = "write"
)

// ReleaseClusterError wraps a cluster error with the operation that produced
// it. Its text carries the cluster's own text and is for the engine log only;
// the API maps it by Cause's serviceerror type (§5.8).
type ReleaseClusterError struct {
	Op    ReleaseClusterOp
	What  string
	Cause error
}

func (e *ReleaseClusterError) Error() string {
	return fmt.Sprintf("releases: %s: %v", e.What, e.Cause)
}

func (e *ReleaseClusterError) Unwrap() error { return e.Cause }

// releaseReadTimeout bounds every cluster call in this file — the value of
// poolProbeTimeout.
const releaseReadTimeout = poolProbeTimeout

// releaseRoutingPoll is how often AwaitRouting reads the engine's channel.
const releaseRoutingPoll = 250 * time.Millisecond

// releaseFanout bounds the per-release and per-pool fan-outs, as the API's
// inParallel bounds its own.
const releaseFanout = 16

// releaseClient is the slice of client.Client this file uses, narrowed so
// every read and write is testable with a fake — the pattern runReadClient
// and workflowServiceAdmin use.
type releaseClient interface {
	WorkerDeploymentClient() client.WorkerDeploymentClient
	DescribeTaskQueueEnhanced(ctx context.Context, o client.DescribeTaskQueueEnhancedOptions) (client.TaskQueueDescription, error)
	DescribeTaskQueue(ctx context.Context, in *workflowservice.DescribeTaskQueueRequest) (*workflowservice.DescribeTaskQueueResponse, error)
}

// temporalReleaseClient adapts client.Client to releaseClient: the default-mode
// DescribeTaskQueue is the raw service call, bound to the client's namespace —
// the call test/tools/queue-routing makes.
type temporalReleaseClient struct {
	client.Client
	namespace string
}

func (c temporalReleaseClient) DescribeTaskQueue(ctx context.Context, in *workflowservice.DescribeTaskQueueRequest) (*workflowservice.DescribeTaskQueueResponse, error) {
	in.Namespace = c.namespace
	return c.Client.WorkflowService().DescribeTaskQueue(ctx, in)
}

// TemporalReleaseCluster implements ReleaseCluster over one client.Client —
// the one cmd/ork already holds; never a second connection.
type TemporalReleaseCluster struct {
	client       releaseClient
	runs         *TemporalRunReader
	deployment   string
	engineQueue  string
	defaultRoute task.Route

	// Freshness bounds how recently a poller must have been seen to count.
	// Zero uses DefaultPollerFreshness.
	Freshness time.Duration
	// Now is injectable for tests.
	Now func() time.Time
	// Logger receives the cluster's own error text, which never reaches the
	// wire. Nil discards it.
	Logger *slog.Logger

	// routingPoll overrides releaseRoutingPoll in a test.
	routingPoll time.Duration
}

var _ ReleaseCluster = (*TemporalReleaseCluster)(nil)

// NewTemporalReleaseCluster binds the seam to this deployment. temporalNamespace
// is the namespace c is bound to, which the default-mode channel read needs.
// engineQueue is cmd/ork's q.run (task.DefaultQueue); orkerQueue is q.orker,
// from which the default route is resolved with task.DefaultRoute, exactly as
// the pool resolver resolves it.
func NewTemporalReleaseCluster(
	c client.Client,
	runs *TemporalRunReader,
	temporalNamespace, deploymentName, engineQueue, orkerQueue string,
) (*TemporalReleaseCluster, error) {
	return newReleaseCluster(temporalReleaseClient{Client: c, namespace: temporalNamespace},
		runs, deploymentName, engineQueue, orkerQueue)
}

func newReleaseCluster(c releaseClient, runs *TemporalRunReader, deploymentName, engineQueue, orkerQueue string) (*TemporalReleaseCluster, error) {
	if deploymentName == "" {
		return nil, errors.New("releases: the deployment name is empty")
	}
	if engineQueue == "" {
		engineQueue = task.DefaultQueue
	}
	route, err := task.DefaultRoute(orkerQueue)
	if err != nil {
		return nil, fmt.Errorf("releases: resolve the default pool: %w", err)
	}
	return &TemporalReleaseCluster{
		client:       c,
		runs:         runs,
		deployment:   deploymentName,
		engineQueue:  engineQueue,
		defaultRoute: route,
	}, nil
}

func (c *TemporalReleaseCluster) now() time.Time {
	if c.Now != nil {
		return c.Now()
	}
	return time.Now()
}

func (c *TemporalReleaseCluster) freshness() time.Duration {
	if c.Freshness > 0 {
		return c.Freshness
	}
	return DefaultPollerFreshness
}

func (c *TemporalReleaseCluster) logger() *slog.Logger {
	if c.Logger != nil {
		return c.Logger
	}
	return slog.New(slog.DiscardHandler)
}

// fail logs the cluster's own text and wraps it for the API.
func (c *TemporalReleaseCluster) fail(op ReleaseClusterOp, what, buildID string, err error) error {
	c.logger().Warn("release cluster call failed",
		"operation", what, "build_id", buildID, "deployment", c.deployment, "error", err)
	return &ReleaseClusterError{Op: op, What: what, Cause: err}
}

// ObserveReleases reads routing, releases, engine replicas and pool use in
// one pass (§5.2–5.4).
func (c *TemporalReleaseCluster) ObserveReleases(ctx context.Context) (ReleaseObservation, error) {
	obs := ReleaseObservation{DeploymentName: c.deployment}
	handle := c.client.WorkerDeploymentClient().GetHandle(c.deployment)

	describeCtx, cancel := context.WithTimeout(ctx, releaseReadTimeout)
	desc, err := handle.Describe(describeCtx, client.WorkerDeploymentDescribeOptions{})
	cancel()
	if err != nil {
		var notFound *serviceerror.NotFound
		if errors.As(err, &notFound) {
			return obs, ErrNotVersioned
		}
		return obs, c.fail(ReleaseOpRead, "describe the deployment", "", err)
	}

	routing := desc.Info.RoutingConfig
	if routing.CurrentVersion != nil {
		obs.Routing.Current = routing.CurrentVersion.BuildID
	}
	if routing.RampingVersion != nil {
		obs.Routing.Ramping = routing.RampingVersion.BuildID
		obs.Routing.RampPercent = routing.RampingVersionPercentage
	}
	obs.conflictToken = desc.ConflictToken

	obs.Releases = make([]ObservedRelease, len(desc.Info.VersionSummaries))
	for i, summary := range desc.Info.VersionSummaries {
		obs.Releases[i] = ObservedRelease{
			BuildID:     summary.Version.BuildID,
			FirstSeenAt: summary.CreateTime,
			DrainState:  drainStateOf(int(summary.DrainageStatus)),
		}
	}

	var mu sync.Mutex
	boundedParallel(len(obs.Releases), func(i int) {
		release := &obs.Releases[i]
		versionCtx, cancel := context.WithTimeout(ctx, releaseReadTimeout)
		defer cancel()
		described, err := handle.DescribeVersion(versionCtx, client.WorkerDeploymentDescribeVersionOptions{
			BuildID: release.BuildID,
		})
		if err != nil {
			release.DescribeErr = c.fail(ReleaseOpRead, "describe a release", release.BuildID, err)
			release.HasServed = release.DrainState != orkstackv1.ReleaseDrainState_RELEASE_DRAIN_STATE_UNSPECIFIED
			return
		}
		info := described.Info
		if info.DrainageInfo != nil {
			release.DrainCheckedAt = info.DrainageInfo.LastCheckedTime
		}
		release.HasServed = release.DrainState != orkstackv1.ReleaseDrainState_RELEASE_DRAIN_STATE_UNSPECIFIED ||
			!info.RoutingChangedTime.IsZero()
		release.RegisteredPools = c.registeredPools(info.TaskQueuesInfos)
		mu.Lock()
		if release.BuildID == obs.Routing.Current && obs.Routing.Current != "" {
			obs.CurrentSince = info.CurrentSinceTime
		}
		if release.BuildID == obs.Routing.Ramping && obs.Routing.Ramping != "" {
			obs.RampingSince = info.RampingSinceTime
		}
		mu.Unlock()
	})

	replicas, err := c.engineReplicas(ctx)
	if err != nil {
		return obs, err
	}
	obs.EngineReplicas = replicas
	obs.PoolUse = c.poolUse(ctx, obs)
	return obs, nil
}

// drainStateOf maps the SDK's drainage status onto the proto enum.
func drainStateOf(status int) orkstackv1.ReleaseDrainState {
	switch status {
	case client.WorkerDeploymentVersionDrainageStatusDraining:
		return orkstackv1.ReleaseDrainState_RELEASE_DRAIN_STATE_DRAINING
	case client.WorkerDeploymentVersionDrainageStatusDrained:
		return orkstackv1.ReleaseDrainState_RELEASE_DRAIN_STATE_DRAINED
	default:
		return orkstackv1.ReleaseDrainState_RELEASE_DRAIN_STATE_UNSPECIFIED
	}
}

// registeredPools translates a release's recorded channels into pools through
// task.PoolForChannel, the only reverse mapping: activity entries only (a
// workflow entry is the engine), anything the mapping declines dropped rather
// than guessed. Sorted, de-duplicated.
func (c *TemporalReleaseCluster) registeredPools(infos []client.WorkerDeploymentTaskQueueInfo) []string {
	seen := map[string]bool{}
	pools := []string{}
	for _, info := range infos {
		if info.Type != client.TaskQueueTypeActivity {
			continue
		}
		pool, ok := task.PoolForChannel(info.Name, c.defaultRoute)
		if !ok || seen[pool] {
			continue
		}
		seen[pool] = true
		pools = append(pools, pool)
	}
	sort.Strings(pools)
	return pools
}

// engineReplicas counts the live engine processes per release on the engine's
// own channel (§5.3). WORKFLOW pollers only: an Orker is activity-only, and
// the engine's run worker polls workflow tasks only, so this channel's
// workflow pollers are exactly the engine's processes. A failure fails the
// whole read: engine liveness is never reported as zero.
func (c *TemporalReleaseCluster) engineReplicas(ctx context.Context) (map[string]int, error) {
	describeCtx, cancel := context.WithTimeout(ctx, releaseReadTimeout)
	defer cancel()
	description, err := c.client.DescribeTaskQueueEnhanced(describeCtx, client.DescribeTaskQueueEnhancedOptions{
		TaskQueue:      c.engineQueue,
		TaskQueueTypes: []client.TaskQueueType{client.TaskQueueTypeWorkflow},
		ReportPollers:  true,
		Versions:       &client.TaskQueueVersionSelection{AllActive: true, Unversioned: true},
	})
	if err != nil {
		return nil, c.fail(ReleaseOpRead, "describe the engine's channel", "", err)
	}
	now, within := c.now(), c.freshness()
	identities := map[string]map[string]bool{}
	anonymous := map[string]int{}
	for _, version := range description.VersionsInfo {
		info, ok := version.TypesInfo[client.TaskQueueTypeWorkflow]
		if !ok {
			continue
		}
		for _, poller := range info.Pollers {
			if !poller.LastAccessTime.IsZero() && now.Sub(poller.LastAccessTime) > within {
				continue
			}
			key := ""
			if opts := poller.WorkerDeploymentPollerOptions; opts != nil && opts.BuildID != "" {
				if opts.DeploymentName != c.deployment {
					continue // another deployment's engine shares this namespace
				}
				key = opts.BuildID
			}
			if poller.Identity == "" {
				anonymous[key]++
				continue
			}
			if identities[key] == nil {
				identities[key] = map[string]bool{}
			}
			identities[key][poller.Identity] = true
		}
	}
	replicas := map[string]int{}
	for key, set := range identities {
		replicas[key] += len(set)
	}
	for key, n := range anonymous {
		replicas[key] += n
	}
	return replicas, nil
}

// poolUse describes every pool the current and ramping releases registered
// (§5.4). Other releases' registrations are not evidence of current use.
func (c *TemporalReleaseCluster) poolUse(ctx context.Context, obs ReleaseObservation) map[string]PoolUse {
	union := map[string]bool{}
	for _, release := range obs.Releases {
		if release.BuildID == "" {
			continue
		}
		if release.BuildID != obs.Routing.Current && release.BuildID != obs.Routing.Ramping {
			continue
		}
		for _, pool := range release.RegisteredPools {
			union[pool] = true
		}
	}
	pools := make([]string, 0, len(union))
	for pool := range union {
		pools = append(pools, pool)
	}
	sort.Strings(pools)
	uses := make([]PoolUse, len(pools))
	boundedParallel(len(pools), func(i int) {
		uses[i] = c.describePoolUse(ctx, pools[i])
	})
	out := make(map[string]PoolUse, len(pools))
	for i, pool := range pools {
		out[pool] = uses[i]
	}
	return out
}

// describePoolUse is one pool's channel description: live pollers, backlog
// and add rate. No stats at all is an error — whether work is waiting was not
// measured — never "not in use".
func (c *TemporalReleaseCluster) describePoolUse(ctx context.Context, pool string) PoolUse {
	route, err := task.RouteForPool(pool, c.defaultRoute.Queue)
	if err != nil {
		return PoolUse{Err: err}
	}
	describeCtx, cancel := context.WithTimeout(ctx, releaseReadTimeout)
	defer cancel()
	description, err := c.client.DescribeTaskQueueEnhanced(describeCtx, client.DescribeTaskQueueEnhancedOptions{
		TaskQueue:      route.Queue,
		TaskQueueTypes: []client.TaskQueueType{client.TaskQueueTypeActivity},
		ReportPollers:  true,
		ReportStats:    true,
		Versions:       &client.TaskQueueVersionSelection{AllActive: true, Unversioned: true},
	})
	if err != nil {
		return PoolUse{Err: c.fail(ReleaseOpRead, "describe pool "+pool, "", err)}
	}
	use := PoolUse{LivePollers: livePollers(description, c.now(), c.freshness()).Orkers}
	measured := false
	for _, version := range description.VersionsInfo {
		info, ok := version.TypesInfo[client.TaskQueueTypeActivity]
		if !ok || info.Stats == nil {
			continue
		}
		measured = true
		use.Backlog += info.Stats.ApproximateBacklogCount
		use.AddRate += info.Stats.TasksAddRate
	}
	if !measured {
		use.Err = fmt.Errorf("pool %q: the cluster returned no statistics", pool)
	}
	return use
}

// ReleaseRuns delegates to the run reader with this deployment's name.
func (c *TemporalReleaseCluster) ReleaseRuns(ctx context.Context, buildID string, limit int) (int64, []*orkstackv1.Run, error) {
	readCtx, cancel := context.WithTimeout(ctx, releaseReadTimeout)
	defer cancel()
	n, runs, err := c.runs.ReleaseRuns(readCtx, c.deployment, buildID, limit)
	if err != nil {
		return 0, nil, c.fail(ReleaseOpRead, "count the runs on a release", buildID, err)
	}
	return n, runs, nil
}

// RunningRuns delegates to the run reader.
func (c *TemporalReleaseCluster) RunningRuns(ctx context.Context) (int64, error) {
	readCtx, cancel := context.WithTimeout(ctx, releaseReadTimeout)
	defer cancel()
	n, err := c.runs.RunningRuns(readCtx)
	if err != nil {
		return 0, c.fail(ReleaseOpRead, "count the runs in flight", "", err)
	}
	return n, nil
}

// SetRouting makes exactly one cluster call moving obs.Routing to want (§5.6).
// The cluster's own guards stay ON (IgnoreMissingTaskQueues and AllowNoPollers
// false), and the conflict token is the one from the Describe the check
// judged, so a routing change in between is refused rather than overwritten.
func (c *TemporalReleaseCluster) SetRouting(ctx context.Context, obs ReleaseObservation, want Routing) error {
	handle := c.client.WorkerDeploymentClient().GetHandle(c.deployment)
	writeCtx, cancel := context.WithTimeout(ctx, releaseReadTimeout)
	defer cancel()
	if want.Current != obs.Routing.Current {
		_, err := handle.SetCurrentVersion(writeCtx, client.WorkerDeploymentSetCurrentVersionOptions{
			BuildID:       want.Current,
			ConflictToken: obs.conflictToken,
		})
		if err != nil {
			return c.fail(ReleaseOpWrite, "make a release current", want.Current, err)
		}
		return nil
	}
	if want.Ramping == obs.Routing.Ramping && want.RampPercent == obs.Routing.RampPercent {
		return nil
	}
	percent := want.RampPercent
	if want.Ramping == "" {
		percent = 0
	}
	_, err := handle.SetRampingVersion(writeCtx, client.WorkerDeploymentSetRampingVersionOptions{
		BuildID:       want.Ramping,
		Percentage:    percent,
		ConflictToken: obs.conflictToken,
	})
	if err != nil {
		return c.fail(ReleaseOpWrite, "ramp a release", want.Ramping, err)
	}
	return nil
}

// AwaitRouting reads the engine's channel in default mode — the read dispatch
// itself uses — until it routes as want, or within elapses (false, nil).
func (c *TemporalReleaseCluster) AwaitRouting(ctx context.Context, want Routing, within time.Duration) (bool, error) {
	poll := c.routingPoll
	if poll <= 0 {
		poll = releaseRoutingPoll
	}
	deadline := time.NewTimer(within)
	defer deadline.Stop()
	var lastErr error
	for {
		readCtx, cancel := context.WithTimeout(ctx, releaseReadTimeout)
		described, err := c.client.DescribeTaskQueue(readCtx, &workflowservice.DescribeTaskQueueRequest{
			TaskQueue:     &taskqueuepb.TaskQueue{Name: c.engineQueue, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
			TaskQueueType: enumspb.TASK_QUEUE_TYPE_WORKFLOW,
			ApiMode:       enumspb.DESCRIBE_TASK_QUEUE_MODE_UNSPECIFIED,
		})
		cancel()
		lastErr = err
		if err == nil && c.routesAs(described.GetVersioningInfo(), want) {
			return true, nil
		}
		timer := time.NewTimer(poll)
		select {
		case <-ctx.Done():
			timer.Stop()
			return false, ctx.Err()
		case <-deadline.C:
			timer.Stop()
			if lastErr != nil {
				return false, c.fail(ReleaseOpRead, "confirm the routing on the engine's channel", want.Current, lastErr)
			}
			return false, nil
		case <-timer.C:
		}
	}
}

// routesAs compares the channel's own routing with want.
func (c *TemporalReleaseCluster) routesAs(info *taskqueuepb.TaskQueueVersioningInfo, want Routing) bool {
	current := info.GetCurrentDeploymentVersion()
	if want.Current == "" {
		if current != nil && current.GetBuildId() != "" {
			return false
		}
	} else if current.GetBuildId() != want.Current || current.GetDeploymentName() != c.deployment {
		return false
	}
	ramping := info.GetRampingDeploymentVersion()
	if want.Ramping == "" {
		return ramping == nil || ramping.GetBuildId() == ""
	}
	return ramping.GetBuildId() == want.Ramping && ramping.GetDeploymentName() == c.deployment &&
		info.GetRampingVersionPercentage() == want.RampPercent
}

// boundedParallel runs fn(0..n-1) with at most releaseFanout at once.
func boundedParallel(n int, fn func(i int)) {
	if n <= 0 {
		return
	}
	sem := make(chan struct{}, releaseFanout)
	var wg sync.WaitGroup
	wg.Add(n)
	for i := 0; i < n; i++ {
		sem <- struct{}{}
		go func(i int) {
			defer wg.Done()
			defer func() { <-sem }()
			fn(i)
		}(i)
	}
	wg.Wait()
}
