package engine

import (
	"testing"
	"time"

	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/client"
	"google.golang.org/protobuf/types/known/timestamppb"
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

// sdkDescription is the SDK's enhanced answer for a channel: the pollers of
// each build ID's entry (the empty ID is unversioned), by task type. Every use
// of the SDK's VersionsInfo shape in these tests is here, because it is the
// only field the SDK has for a channel's poller records.
func sdkDescription(entries map[string]map[client.TaskQueueType][]client.TaskQueuePollerInfo) client.TaskQueueDescription {
	//lint:ignore SA1019 the SDK's VersioningInfo is the routing state and holds no pollers; VersionsInfo is the only field the SDK has for a channel's poller records
	versions := map[string]client.TaskQueueVersionInfo{}
	for buildID, byType := range entries {
		types := map[client.TaskQueueType]client.TaskQueueTypeInfo{}
		for kind, pollers := range byType {
			types[kind] = client.TaskQueueTypeInfo{Pollers: pollers}
		}
		//lint:ignore SA1019 the SDK's VersioningInfo is the routing state and holds no pollers; VersionsInfo is the only field the SDK has for a channel's poller records
		versions[buildID] = client.TaskQueueVersionInfo{TypesInfo: types}
	}
	//lint:ignore SA1019 the SDK's VersioningInfo is the routing state and holds no pollers; VersionsInfo is the only field the SDK has for a channel's poller records
	return client.TaskQueueDescription{VersionsInfo: versions}
}

// sdkEntries is sdkDescription's inverse: each build ID's pollers by task type.
func sdkEntries(description client.TaskQueueDescription) map[string]map[client.TaskQueueType][]client.TaskQueuePollerInfo {
	entries := map[string]map[client.TaskQueueType][]client.TaskQueuePollerInfo{}
	//lint:ignore SA1019 the SDK's VersioningInfo is the routing state and holds no pollers; VersionsInfo is the only field the SDK has for a channel's poller records
	for buildID, version := range description.VersionsInfo {
		entries[buildID] = map[client.TaskQueueType][]client.TaskQueuePollerInfo{}
		for kind, info := range version.TypesInfo {
			entries[buildID][kind] = info.Pollers
		}
	}
	return entries
}

// activityPollers is one channel's description with a single version entry, as
// an unversioned deployment produces.
func activityPollers(pollers ...client.TaskQueuePollerInfo) client.TaskQueueDescription {
	return sdkDescription(map[string]map[client.TaskQueueType][]client.TaskQueuePollerInfo{
		"": {client.TaskQueueTypeActivity: pollers},
	})
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
			poller("1@host@ork-pool-alpha", now.Add(-2*time.Minute)),
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

// TestLivePollers_AnIdleOrkerBetweenTwoPollsIsLive pins the window's floor: a
// healthy Orker's record is as old as its polling cycle, so the window must
// be wider than that cycle or the probe reports a live Orker as gone.
//
// Temporal stamps a poller when its long poll STARTS. An idle worker's poll
// is held for the cluster's long-poll interval (a minute by default) and can
// never outlast the SDK's own poll deadline (go.temporal.io/sdk
// pollTaskServiceTimeOut, 70 seconds), so just before the next poll arrives a
// live record is up to that old. Measured against the dev cluster, an idle
// Orker's record reached 60.0–63.3 seconds once a minute; with the window at
// 60 seconds, a release promotion read at that moment named the Orker's pool
// "in use and served by no Orker of this release", and a submit could be
// refused as an empty pool.
func TestLivePollers_AnIdleOrkerBetweenTwoPollsIsLive(t *testing.T) {
	now := time.Now()
	const sdkPollDeadline = 70 * time.Second
	for _, age := range []time.Duration{
		63300 * time.Millisecond, // the oldest record measured for a live idle Orker
		sdkPollDeadline + 2*time.Second,
	} {
		description := activityPollers(poller("orker-idle", now.Add(-age)))
		if live := livePollers(description, now, DefaultPollerFreshness); live.Orkers != 1 || !live.Fresh(now, DefaultPollerFreshness) {
			t.Errorf("an idle Orker whose record is %v old (one polling cycle) counted as %d Orkers, fresh=%v; "+
				"want 1 and fresh: the %v window must outlast a live worker's polling cycle",
				age, live.Orkers, live.Fresh(now, DefaultPollerFreshness), DefaultPollerFreshness)
		}
		if records := livePollerRecords(description, now, DefaultPollerFreshness); len(records) != 1 {
			t.Errorf("the Orker list drops an idle Orker whose record is %v old: %v", age, records)
		}
	}
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
	description := sdkDescription(map[string]map[client.TaskQueueType][]client.TaskQueuePollerInfo{
		"":         {client.TaskQueueTypeActivity: {one}},
		"build-42": {client.TaskQueueTypeActivity: {one}},
	})

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
	description := sdkDescription(map[string]map[client.TaskQueueType][]client.TaskQueuePollerInfo{
		"": {client.TaskQueueTypeWorkflow: {poller("3@host@ork-tasks", now)}},
	})

	if live := livePollers(description, now, DefaultPollerFreshness); live.Orkers != 0 {
		t.Errorf("counted %d Orkers on a channel only the engine polls, want 0", live.Orkers)
	}
}

// TestActivityDescription_FeedsTheDefaultModeAnswerToTheSameReduction pins the
// reshaping every SubmitRun's liveness probe takes: ObservePool reads the
// channel in default mode (a flat poller list) and hands the reshaped answer
// to livePollers. The reshaping must keep every record as the cluster gave it
// — identity, last access, a missing timestamp staying missing — so the
// reduction's rules (stale dropped, one identity once, anonymous counted on
// its own) apply to it exactly as they do to the enhanced answer.
func TestActivityDescription_FeedsTheDefaultModeAnswerToTheSameReduction(t *testing.T) {
	now := time.Now()
	const within = DefaultPollerFreshness
	answer := func(pollers ...*taskqueuepb.PollerInfo) *workflowservice.DescribeTaskQueueResponse {
		return &workflowservice.DescribeTaskQueueResponse{Pollers: pollers}
	}
	record := func(identity string, lastAccess time.Time) *taskqueuepb.PollerInfo {
		info := &taskqueuepb.PollerInfo{Identity: identity}
		if !lastAccess.IsZero() {
			info.LastAccessTime = timestamppb.New(lastAccess)
		}
		return info
	}

	cases := []struct {
		name       string
		answer     *workflowservice.DescribeTaskQueueResponse
		wantOrkers int
		wantSeen   time.Time
	}{
		{
			name:   "an empty answer is an empty pool",
			answer: answer(),
		},
		{
			name: "a stale record is dropped",
			answer: answer(
				record("1@host@ork-pool-alpha", now.Add(-5*time.Minute)),
				record("2@host@ork-pool-alpha", now.Add(-2*time.Second)),
			),
			wantOrkers: 1,
			wantSeen:   now.Add(-2 * time.Second),
		},
		{
			name: "one identity is counted once, at its newest record",
			answer: answer(
				record("1@host@ork-pool-alpha", now.Add(-10*time.Second)),
				record("1@host@ork-pool-alpha", now.Add(-1*time.Second)),
			),
			wantOrkers: 1,
			wantSeen:   now.Add(-1 * time.Second),
		},
		{
			name: "an identity-less record is kept and counted on its own",
			answer: answer(
				record("", now.Add(-3*time.Second)),
				record("", now.Add(-4*time.Second)),
				record("1@host@ork-pool-alpha", now.Add(-5*time.Second)),
			),
			wantOrkers: 3,
			wantSeen:   now.Add(-3 * time.Second),
		},
		{
			name:       "a record with no last access is trusted, not dropped",
			answer:     answer(record("1@host@ork-pool-alpha", time.Time{})),
			wantOrkers: 1,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			live := livePollers(activityDescription(tc.answer), now, within)
			if live.Orkers != tc.wantOrkers {
				t.Errorf("counted %d Orkers, want %d", live.Orkers, tc.wantOrkers)
			}
			if !live.LastSeen.Equal(tc.wantSeen) {
				t.Errorf("last seen %v, want %v", live.LastSeen, tc.wantSeen)
			}
		})
	}

	t.Run("the reshaped answer holds activity pollers only, under one entry", func(t *testing.T) {
		got := activityDescription(answer(record("1@host@ork-pool-alpha", now)))
		entries := sdkEntries(got)
		if len(entries) != 1 {
			t.Fatalf("got %d version entries, want 1", len(entries))
		}
		types := entries[""]
		if len(types) != 1 || len(types[client.TaskQueueTypeActivity]) != 1 {
			t.Fatalf("got types %v, want one activity poller", types)
		}
		if records := livePollerRecords(got, now, within); len(records) != 1 || records[0].Identity != "1@host@ork-pool-alpha" {
			t.Fatalf("records %v, want the one identity the cluster named", records)
		}
	})
}
