package engine

import (
	"testing"
	"time"

	"go.temporal.io/sdk/client"
)

// The counting half of the liveness probe, tested on the shape Temporal
// actually answers with.
//
// Everything above this function trusts its number: a submit is refused on it
// (PoolResolver.Plan) and an operator reads it out of ListPools as
// Pool.live_orker_count. The integration test cannot pin the number down —
// it can only assert against the Orkers it started, and the cluster's own
// memory of earlier ones is exactly what it cannot arrange. So the arithmetic
// is pinned here, where a poller list can be stated.

func poller(identity string, lastAccess time.Time) client.TaskQueuePollerInfo {
	return client.TaskQueuePollerInfo{Identity: identity, LastAccessTime: lastAccess}
}

// activityPollers is one channel's description with a single version entry, as
// an unversioned deployment produces.
func activityPollers(pollers ...client.TaskQueuePollerInfo) client.TaskQueueDescription {
	return client.TaskQueueDescription{
		VersionsInfo: map[string]client.TaskQueueVersionInfo{
			"": {TypesInfo: map[client.TaskQueueType]client.TaskQueueTypeInfo{
				client.TaskQueueTypeActivity: {Pollers: pollers},
			}},
		},
	}
}

// TestLivePollers_CountsOnlyTheOrkersStillInsideTheWindow is the difference
// between a live count and a count of everyone who was ever here.
//
// Temporal remembers a poller for minutes after its worker died. Counting
// every record it returns and then judging the whole list by its NEWEST
// timestamp reports "3 Orkers" for a pool one Orker serves, for as long as
// that one Orker keeps the newest record fresh — which is precisely the
// moment an operator scaling a pool down is reading the number.
func TestLivePollers_CountsOnlyTheOrkersStillInsideTheWindow(t *testing.T) {
	now := time.Now()
	const within = DefaultPollerFreshness

	t.Run("a poller outside the window is gone, however fresh its neighbour", func(t *testing.T) {
		live := livePollers(activityPollers(
			poller("1@host@ork-pool-alpha", now.Add(-95*time.Second)),
			poller("2@host@ork-pool-alpha", now.Add(-2*time.Second)),
		), now, within)

		if live.Orkers != 1 {
			t.Errorf("counted %d Orkers, want 1: the other stopped polling %v ago, "+
				"which is outside the %v this code calls live",
				live.Orkers, 95*time.Second, within)
		}
		if !live.Fresh(now, within) {
			t.Error("the surviving Orker does not make the observation fresh")
		}
	})

	t.Run("every poller stale means nothing serves the pool", func(t *testing.T) {
		live := livePollers(activityPollers(
			poller("1@host@ork-pool-alpha", now.Add(-75*time.Second)),
			poller("2@host@ork-pool-alpha", now.Add(-6*time.Minute)),
		), now, within)

		if live.Orkers != 0 {
			t.Errorf("counted %d Orkers, want 0: every record is older than %v, so a "+
				"task sent here waits for a worker that is gone", live.Orkers, within)
		}
		if live.Fresh(now, within) {
			t.Error("a pool whose every poller is stale reports as fresh")
		}
	})

	t.Run("last seen is the newest SURVIVING poller", func(t *testing.T) {
		recent := now.Add(-3 * time.Second)
		live := livePollers(activityPollers(
			poller("1@host@ork-pool-alpha", recent),
			poller("2@host@ork-pool-alpha", now.Add(-4*time.Minute)),
		), now, within)

		if !live.LastSeen.Equal(recent) {
			t.Errorf("LastSeen = %v, want the surviving poller's %v", live.LastSeen, recent)
		}
	})
}

// TestLivePollers_CountsOneWorkerOnceHoweverManyRecordsDescribeIt keeps the
// number a count of Orkers rather than of rows.
//
// A worker is `pid@host@channel` to Temporal. The probe asks for all active
// versions plus unversioned — it must, or a rollout would look empty — and one
// worker can appear under more than one of those entries while a version is
// being promoted. Counting records would then double a pool that gained
// nothing.
func TestLivePollers_CountsOneWorkerOnceHoweverManyRecordsDescribeIt(t *testing.T) {
	now := time.Now()
	one := poller("7@host@ork-pool-gpu", now.Add(-1*time.Second))
	description := client.TaskQueueDescription{
		VersionsInfo: map[string]client.TaskQueueVersionInfo{
			"": {TypesInfo: map[client.TaskQueueType]client.TaskQueueTypeInfo{
				client.TaskQueueTypeActivity: {Pollers: []client.TaskQueuePollerInfo{one}},
			}},
			"build-42": {TypesInfo: map[client.TaskQueueType]client.TaskQueueTypeInfo{
				client.TaskQueueTypeActivity: {Pollers: []client.TaskQueuePollerInfo{one}},
			}},
		},
	}

	live := livePollers(description, now, DefaultPollerFreshness)
	if live.Orkers != 1 {
		t.Errorf("counted %d Orkers, want 1: both records are the same worker %q",
			live.Orkers, one.Identity)
	}
}

// TestLivePollers_TrustsARecordTheClusterGaveNoTimestampFor keeps the
// asymmetry the window exists for: over-reporting degrades to today's
// behaviour (a run that waits), under-reporting invents a refusal of a healthy
// submit. A cluster that reports a poller without a last-access time has still
// reported a poller.
func TestLivePollers_TrustsARecordTheClusterGaveNoTimestampFor(t *testing.T) {
	now := time.Now()
	live := livePollers(activityPollers(poller("9@host@ork-pool-gpu", time.Time{})), now, DefaultPollerFreshness)

	if live.Orkers != 1 {
		t.Errorf("counted %d Orkers, want 1: the cluster reported a poller and gave no "+
			"time for it, which is not the same as reporting nobody", live.Orkers)
	}
	if !live.Fresh(now, DefaultPollerFreshness) {
		t.Error("a poller with no timestamp is treated as gone")
	}
}

// TestLivePollers_CountsActivityPollersOnly keeps the engine out of the Orker
// count. An Orker is activity-only by construction (cmd/orker sets
// DisableWorkflowWorker), so a workflow poller on a channel is the engine, and
// counting it would vouch for a pool no Orker serves.
func TestLivePollers_CountsActivityPollersOnly(t *testing.T) {
	now := time.Now()
	description := client.TaskQueueDescription{
		VersionsInfo: map[string]client.TaskQueueVersionInfo{
			"": {TypesInfo: map[client.TaskQueueType]client.TaskQueueTypeInfo{
				client.TaskQueueTypeWorkflow: {Pollers: []client.TaskQueuePollerInfo{
					poller("3@host@ork-tasks", now),
				}},
			}},
		},
	}

	if live := livePollers(description, now, DefaultPollerFreshness); live.Orkers != 0 {
		t.Errorf("counted %d Orkers on a channel only the engine polls, want 0", live.Orkers)
	}
}
