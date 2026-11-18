package engine

import (
	"context"
	"fmt"
	"slices"
	"time"

	enumspb "go.temporal.io/api/enums/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/client"

	"github.com/orkstack/orkstack/internal/task"
)

// The pool metrics' settings (engine.pool_metrics.*, docs/DEPLOY.md §1).
const (
	// DefaultPoolMetricsInterval is how often each replica reads each pool.
	DefaultPoolMetricsInterval = 15 * time.Second
	// MinPoolMetricsInterval and MaxPoolMetricsInterval bound it.
	MinPoolMetricsInterval = 5 * time.Second
	MaxPoolMetricsInterval = 5 * time.Minute
	// MaxPoolMetricsPools bounds engine.pool_metrics.pools.
	MaxPoolMetricsPools = 64
)

// PoolBacklogReading is one read of one pool's channel, for the pool gauges
// (docs/DEPLOY.md, "A pool's backlog, as metrics").
type PoolBacklogReading struct {
	Backlog               int64
	BacklogAge            time.Duration
	AddRate, DispatchRate float64
	// Pollers is the Orkers that polled within DefaultPollerFreshness, one
	// per identity (livePollers).
	Pollers int
}

// poolStatsClient is the one call a pool read makes, bound to the client's
// namespace.
type poolStatsClient interface {
	DescribeTaskQueue(ctx context.Context, in *workflowservice.DescribeTaskQueueRequest) (*workflowservice.DescribeTaskQueueResponse, error)
}

// TemporalPoolBacklog reads what waits on each measured pool: the default
// pool and every pool of engine.pool_metrics.pools. It resolves each pool's
// channel once, here, so the channel never leaves this file: callers name
// pools.
type TemporalPoolBacklog struct {
	client poolStatsClient
	pools  []string
	queues map[string]string // pool → channel
	now    func() time.Time
}

// NewTemporalPoolBacklog reads through c, in temporalNamespace, for the
// deployment whose default channel is defaultQueue (ORK_ORKER_QUEUE). A pool
// that is not a valid name is refused, naming it.
func NewTemporalPoolBacklog(c client.Client, temporalNamespace, defaultQueue string, pools []string) (*TemporalPoolBacklog, error) {
	return newPoolBacklog(temporalReleaseClient{Client: c, namespace: temporalNamespace}, defaultQueue, pools)
}

func newPoolBacklog(c poolStatsClient, defaultQueue string, configured []string) (*TemporalPoolBacklog, error) {
	def, err := task.DefaultRoute(defaultQueue)
	if err != nil {
		return nil, err
	}
	b := &TemporalPoolBacklog{client: c, queues: map[string]string{def.Pool: def.Queue}, now: time.Now}
	var others []string
	for _, pool := range configured {
		route, err := task.RouteForPool(pool, def.Queue)
		if err != nil {
			return nil, fmt.Errorf("pool metrics: pool %q: %w", pool, err)
		}
		if _, ok := b.queues[route.Pool]; !ok {
			b.queues[route.Pool] = route.Queue
			others = append(others, route.Pool)
		}
	}
	slices.Sort(others)
	b.pools = append([]string{def.Pool}, others...)
	return b, nil
}

// Pools is every pool it measures, each once, under the name its route
// answers to: the default pool first, then the others by name.
func (b *TemporalPoolBacklog) Pools() []string { return slices.Clone(b.pools) }

// Read describes pool's channel once: the default-mode activity read with
// statistics, the one that counts every release's waiting tasks and lists
// every release's pollers (TemporalPoolProbe says why not the enhanced one).
// An answer with no statistics did not measure the backlog and is an error,
// never a reading of zero.
func (b *TemporalPoolBacklog) Read(ctx context.Context, pool string) (PoolBacklogReading, error) {
	queue, ok := b.queues[pool]
	if !ok {
		return PoolBacklogReading{}, fmt.Errorf("pool %q is not measured", pool)
	}
	described, err := b.client.DescribeTaskQueue(ctx, &workflowservice.DescribeTaskQueueRequest{
		TaskQueue:     &taskqueuepb.TaskQueue{Name: queue, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
		TaskQueueType: enumspb.TASK_QUEUE_TYPE_ACTIVITY,
		ReportStats:   true,
	})
	if err != nil {
		return PoolBacklogReading{}, fmt.Errorf("describe pool %q: %w", pool, err)
	}
	stats := described.GetStats()
	if stats == nil {
		return PoolBacklogReading{}, fmt.Errorf("pool %q: the cluster returned no statistics", pool)
	}
	// Waiting tasks with no age is an answer that did not measure the age,
	// not an age of zero.
	if stats.GetApproximateBacklogCount() > 0 && stats.GetApproximateBacklogAge() == nil {
		return PoolBacklogReading{}, fmt.Errorf("pool %q: the cluster counted waiting tasks but gave no age", pool)
	}
	return PoolBacklogReading{
		Backlog:      stats.GetApproximateBacklogCount(),
		BacklogAge:   stats.GetApproximateBacklogAge().AsDuration(),
		AddRate:      float64(stats.GetTasksAddRate()),
		DispatchRate: float64(stats.GetTasksDispatchRate()),
		Pollers:      livePollers(activityDescription(described), b.now(), DefaultPollerFreshness).Orkers,
	}, nil
}
