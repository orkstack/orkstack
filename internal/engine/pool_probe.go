package engine

import (
	"context"
	"fmt"
	"sort"
	"time"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"

	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/workflowservice/v1"
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
//   - The default-mode read, never the enhanced one. The default-mode answer
//     lists every poller of the channel whatever release it registered with;
//     the enhanced read (all active versions plus unversioned) was measured
//     against the pinned cluster to omit pollers that registered with a
//     release, so a pool served only by versioned Orkers read as empty and
//     every submit to it was refused (TestIntegration_ObservePoolCountsAReleasesOrkers).
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
	// The default-mode read; see the doc comment for why not the enhanced one.
	described, err := p.client.DescribeTaskQueue(probeCtx, queue, enumspb.TASK_QUEUE_TYPE_ACTIVITY)
	if err != nil {
		return PoolLiveness{}, fmt.Errorf("describe pool channel %q: %w", queue, err)
	}

	return livePollers(activityDescription(described), p.now(), p.freshness()), nil
}

// activityDescription reshapes a default-mode answer into the description
// livePollers reduces: one entry holding the channel's activity pollers, every
// record kept as the cluster gave it (identity, last access, rate). Dropping
// stale records and collapsing identities is livePollers' job, not this one's.
func activityDescription(described *workflowservice.DescribeTaskQueueResponse) client.TaskQueueDescription {
	pollers := make([]client.TaskQueuePollerInfo, 0, len(described.GetPollers()))
	for _, poller := range described.GetPollers() {
		info := client.TaskQueuePollerInfo{Identity: poller.GetIdentity(), RatePerSecond: poller.GetRatePerSecond()}
		if at := poller.GetLastAccessTime(); at != nil {
			info.LastAccessTime = at.AsTime()
		}
		pollers = append(pollers, info)
	}
	return client.TaskQueueDescription{VersionsInfo: map[string]client.TaskQueueVersionInfo{
		"": {TypesInfo: map[client.TaskQueueType]client.TaskQueueTypeInfo{
			client.TaskQueueTypeActivity: {Pollers: pollers},
		}},
	}}
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
//     and one worker can be described by more than one record: the
//     default-mode answer may repeat a poller, and the enhanced read
//     ListOrkers uses (describeActivityPollers) reports it once per version
//     entry while a version is promoted.
//
// A record the cluster gave no last-access time for is trusted rather than
// dropped: it has still reported a poller, and the asymmetry runs the same way
// as the window itself — over-reporting degrades to a run that waits, which is
// the old behaviour, while under-reporting invents a refusal of a healthy
// submit.
func livePollers(description client.TaskQueueDescription, now time.Time, within time.Duration) PoolLiveness {
	records, anonymous := pollerRecords(description, now, within)
	live := PoolLiveness{Orkers: len(records) + anonymous.count}
	live.LastSeen = anonymous.lastSeen
	for _, record := range records {
		if record.LastSeen.After(live.LastSeen) {
			live.LastSeen = record.LastSeen
		}
	}
	return live
}

// OrkerPresence is one live poller of a channel, by Temporal worker identity.
type OrkerPresence struct {
	Identity string
	LastSeen time.Time // zero when the cluster reported none
}

// livePollerRecords is livePollers without the reduction: one record per
// identity inside the freshness window, newest LastSeen per identity,
// anonymous records dropped (an identity-less poller cannot be addressed).
// Ordered by identity.
func livePollerRecords(description client.TaskQueueDescription, now time.Time, within time.Duration) []OrkerPresence {
	records, _ := pollerRecords(description, now, within)
	return records
}

type anonymousPollers struct {
	count    int
	lastSeen time.Time
}

// pollerRecords is the one walk over a description both readers share.
func pollerRecords(description client.TaskQueueDescription, now time.Time, within time.Duration) ([]OrkerPresence, anonymousPollers) {
	byIdentity := make(map[string]time.Time)
	seen := make(map[string]bool)
	// A record with no identity cannot be matched against any other, so it
	// counts on its own rather than collapsing every anonymous record into one.
	var anonymous anonymousPollers
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
				anonymous.count++
				if poller.LastAccessTime.After(anonymous.lastSeen) {
					anonymous.lastSeen = poller.LastAccessTime
				}
				continue
			}
			seen[poller.Identity] = true
			if poller.LastAccessTime.After(byIdentity[poller.Identity]) {
				byIdentity[poller.Identity] = poller.LastAccessTime
			}
		}
	}
	records := make([]OrkerPresence, 0, len(seen))
	for identity := range seen {
		records = append(records, OrkerPresence{Identity: identity, LastSeen: byIdentity[identity]})
	}
	sort.Slice(records, func(i, j int) bool { return records[i].Identity < records[j].Identity })
	return records, anonymous
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
// A successful answer is how a pool proves it is alive, and that is what the
// admin inventory renders.
//
// It is asked of a pool with live Orkers only. The activity has nowhere else
// to run, so a pool with none would hold the request open until the timeout
// and answer nothing.
func (s *TemporalStarter) OrkerOnPool(ctx context.Context, queue string) (*orkstackv1.DescribeOrkerResult, error) {
	return s.describeOrkerOn(ctx, queue, poolDescribeTimeout)
}

var _ PoolOrkerDescriber = (*TemporalStarter)(nil)

// poolProbeTimeout bounds one liveness question, so a slow cluster cannot hold
// a submit open indefinitely.
const poolProbeTimeout = 5 * time.Second
