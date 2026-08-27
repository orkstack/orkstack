package engine

import (
	"context"
	"fmt"
	"time"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"

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

	// Freshness bounds how recently a poller must have been seen to count.
	// Zero uses DefaultPollerFreshness.
	Freshness time.Duration

	// Now is injectable for tests.
	Now func() time.Time
}

// NewTemporalPoolProbe returns a probe backed by a Temporal client.
func NewTemporalPoolProbe(c client.Client) *TemporalPoolProbe {
	return &TemporalPoolProbe{client: c}
}

var _ PoolProbe = (*TemporalPoolProbe)(nil)

// ObservePool counts the Orkers polling one channel for task activities.
//
// Four details are load-bearing:
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
//   - The count is of Orkers still inside the freshness window, one per
//     identity. See livePollers: the cluster's answer includes workers that
//     have stopped, and reporting those as live is reporting a number no
//     Orker justifies.
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

	return livePollers(description, p.now(), p.freshness()), nil
}

// livePollers reduces one channel's poller records to a count of the Orkers
// still serving it.
//
// Two reductions, and each is the difference between a live count and a count
// of everyone who was ever here:
//
//   - a stale record is dropped INDIVIDUALLY. Temporal remembers a poller for
//     minutes after its worker died, so a pool with one live Orker and two
//     ghosts answers with three records. Counting all three and then judging
//     the list by its NEWEST timestamp reports "3 live" for as long as the
//     one survivor keeps that timestamp fresh — which is exactly the moment
//     an operator scaling the pool down is reading the number.
//   - one identity is one Orker. A worker is `pid@host@channel` to Temporal,
//     and the probe deliberately asks for all active versions plus
//     unversioned (or a rollout would look empty), so one worker can be
//     described by more than one record while a version is promoted.
//
// A record the cluster gave no last-access time for is trusted rather than
// dropped: it has still reported a poller, and the asymmetry runs the same way
// as the window itself — over-reporting degrades to a run that waits, which is
// the old behaviour, while under-reporting invents a refusal of a healthy
// submit.
func livePollers(description client.TaskQueueDescription, now time.Time, within time.Duration) PoolLiveness {
	live := PoolLiveness{}
	orkers := make(map[string]struct{})
	// A record with no identity cannot be matched against any other, so it
	// counts on its own rather than collapsing every anonymous record into one.
	anonymous := 0
	for _, version := range description.VersionsInfo {
		info, ok := version.TypesInfo[client.TaskQueueTypeActivity]
		if !ok {
			continue
		}
		for _, poller := range info.Pollers {
			if !poller.LastAccessTime.IsZero() && now.Sub(poller.LastAccessTime) > within {
				continue
			}
			if poller.Identity == "" {
				anonymous++
			} else {
				orkers[poller.Identity] = struct{}{}
			}
			if poller.LastAccessTime.After(live.LastSeen) {
				live.LastSeen = poller.LastAccessTime
			}
		}
	}
	live.Orkers = len(orkers) + anonymous
	return live
}

func (p *TemporalPoolProbe) freshness() time.Duration {
	if p.Freshness > 0 {
		return p.Freshness
	}
	return DefaultPollerFreshness
}

func (p *TemporalPoolProbe) now() time.Time {
	if p.Now != nil {
		return p.Now()
	}
	return time.Now()
}

// OrkerOnPool asks one pool's Orkers who they are, by executing the Orker's
// own DescribeOrker activity on that pool's channel.
//
// It replaces PluginsOnPool. The question changed — an Orker no longer has a
// plugin list, because which images exist is engine configuration (§4.2) — but
// the ROLE did not: a successful answer is how a pool proves it is alive, and
// that is what the admin inventory renders.
//
// It is asked of a pool with live Orkers only. The activity has nowhere else
// to run, so a pool with none would hold the request open until the timeout
// and answer nothing.
func (s *TemporalStarter) OrkerOnPool(ctx context.Context, queue string) (*orkstackv1.DescribeOrkerResult, error) {
	return s.describeOrkerOn(ctx, queue, poolPluginTimeout)
}

var _ PoolOrkerDescriber = (*TemporalStarter)(nil)

// poolProbeTimeout bounds one liveness question, so a slow cluster cannot hold
// a submit open indefinitely.
const poolProbeTimeout = 5 * time.Second
