package engine

import (
	"context"
	"fmt"
	"time"

	"go.temporal.io/sdk/client"
)

// TemporalPoolProbe answers "is anything serving this pool" from the cluster
// rather than from a guess.
//
// It is the difference between refusing a submit on evidence and refusing it
// on a hunch. The cluster knows exactly which workers are polling for a
// channel's activities; nothing in this process does, and nothing could — an
// Orker never registers with the engine, it registers with Temporal.
type TemporalPoolProbe struct {
	client client.Client
}

// NewTemporalPoolProbe returns a probe backed by a Temporal client.
func NewTemporalPoolProbe(c client.Client) *TemporalPoolProbe {
	return &TemporalPoolProbe{client: c}
}

var _ PoolProbe = (*TemporalPoolProbe)(nil)

// ObservePool counts the Orkers polling one channel for task activities.
//
// Three details are load-bearing:
//
//   - ACTIVITY pollers only. An Orker is activity-only by construction
//     (cmd/orker sets DisableWorkflowWorker), so a workflow poller on this
//     channel is the engine, not an Orker, and counting it would vouch for a
//     pool nothing serves.
//   - All active versions PLUS unversioned. With ORK_BUILD_ID set the Orkers
//     are versioned, and the default selection reports only the channel's
//     default build id — a rollout would look empty mid-flight.
//   - An unknown channel is not an error. Temporal answers with an empty
//     poller list, which is the same answer as "everyone left", and both mean
//     the same thing to a submit: nothing would run the task.
//
// The question is bounded in time, because a submit is waiting on it: a slow
// cluster must not hold a submission open, and a probe that cannot answer is
// reported as an error rather than as an empty pool.
func (p *TemporalPoolProbe) ObservePool(ctx context.Context, queue string) (PoolLiveness, error) {
	probeCtx, cancel := context.WithTimeout(ctx, poolProbeTimeout)
	defer cancel()
	description, err := p.client.DescribeTaskQueueEnhanced(probeCtx, client.DescribeTaskQueueEnhancedOptions{
		TaskQueue:      queue,
		TaskQueueTypes: []client.TaskQueueType{client.TaskQueueTypeActivity},
		ReportPollers:  true,
		Versions:       &client.TaskQueueVersionSelection{AllActive: true, Unversioned: true},
	})
	if err != nil {
		return PoolLiveness{}, fmt.Errorf("describe pool channel %q: %w", queue, err)
	}

	live := PoolLiveness{}
	for _, version := range description.VersionsInfo {
		info, ok := version.TypesInfo[client.TaskQueueTypeActivity]
		if !ok {
			continue
		}
		for _, poller := range info.Pollers {
			live.Orkers++
			if poller.LastAccessTime.After(live.LastSeen) {
				live.LastSeen = poller.LastAccessTime
			}
		}
	}
	return live, nil
}

// PluginsOnPool asks one pool's Orkers what they can run, by executing the
// Orker's own ListPlugins activity on that pool's channel.
//
// It is asked of a pool with live Orkers only. The activity has nowhere else
// to run, so a pool with none would hold the request open until the timeout
// and answer nothing.
func (s *TemporalStarter) PluginsOnPool(ctx context.Context, queue string) ([]string, error) {
	descriptors, err := s.listPluginsOn(ctx, queue, poolPluginTimeout)
	if err != nil {
		return nil, err
	}
	names := make([]string, 0, len(descriptors))
	for _, descriptor := range descriptors {
		names = append(names, descriptor.Name)
	}
	return names, nil
}

var _ PoolPluginLister = (*TemporalStarter)(nil)

// poolProbeTimeout bounds one liveness question, so a slow cluster cannot hold
// a submit open indefinitely.
const poolProbeTimeout = 5 * time.Second
