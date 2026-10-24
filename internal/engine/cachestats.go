package engine

import (
	"sort"
	"time"

	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// The cache read (docs/STATS.md "Cache"): how a namespace's cached tasks
// fared over a window of finished runs, and what memos the store holds now.
// Both are derived on read — nothing is counted or stored.

// AggregateCacheUsage folds every TaskRun — and every fan-out item — whose
// cache.outcome is HIT, MISS or BYPASSED into one row per (orkflow, task),
// ordered by orkflow then task. reused sums source.finished_at -
// source.started_at over hits whose two times are set and ordered. Pure.
func AggregateCacheUsage(runs []*orkstackv1.Run) []*orkstackv1.CacheUsageRow {
	type key struct{ orkflow, task string }
	rows := map[key]*orkstackv1.CacheUsageRow{}
	reused := map[key]time.Duration{}
	for _, run := range runs {
		for _, task := range run.GetTaskRuns() {
			candidates := []*orkstackv1.TaskRun{task}
			if task.GetEach() != nil {
				candidates = task.GetItems()
			}
			for _, row := range candidates {
				c := row.GetCache()
				switch c.GetOutcome() {
				case orkstackv1.CacheOutcome_CACHE_OUTCOME_HIT,
					orkstackv1.CacheOutcome_CACHE_OUTCOME_MISS,
					orkstackv1.CacheOutcome_CACHE_OUTCOME_BYPASSED:
				default:
					continue
				}
				k := key{run.GetOrkflowName(), task.GetTaskId()}
				usage := rows[k]
				if usage == nil {
					usage = &orkstackv1.CacheUsageRow{OrkflowName: k.orkflow, TaskId: k.task}
					rows[k] = usage
				}
				switch c.GetOutcome() {
				case orkstackv1.CacheOutcome_CACHE_OUTCOME_HIT:
					usage.Hits++
					started, finished := c.GetSource().GetStartedAt(), c.GetSource().GetFinishedAt()
					if started != nil && finished != nil && !finished.AsTime().Before(started.AsTime()) {
						reused[k] += finished.AsTime().Sub(started.AsTime())
					}
				case orkstackv1.CacheOutcome_CACHE_OUTCOME_MISS:
					usage.Misses++
				case orkstackv1.CacheOutcome_CACHE_OUTCOME_BYPASSED:
					usage.Bypassed++
				}
			}
		}
	}
	keys := make([]key, 0, len(rows))
	for k := range rows {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool {
		if keys[i].orkflow != keys[j].orkflow {
			return keys[i].orkflow < keys[j].orkflow
		}
		return keys[i].task < keys[j].task
	})
	out := make([]*orkstackv1.CacheUsageRow, 0, len(keys))
	for _, k := range keys {
		usage := rows[k]
		if looked := usage.GetHits() + usage.GetMisses(); looked > 0 {
			usage.HitRateDefined = true
			usage.HitRate = float64(usage.GetHits()) / float64(looked)
		}
		usage.Reused = durationpb.New(reused[k])
		out = append(out, usage)
	}
	return out
}

// GroupMemoInventory folds listed memos into one row per (scope, orkflow,
// task): ORKFLOW before NAMESPACE, then orkflow, then task. Pure.
func GroupMemoInventory(entries []artifacts.MemoEntry) []*orkstackv1.CacheInventoryRow {
	type key struct {
		scope   orkstackv1.CacheScope
		orkflow string
		task    string
	}
	type group struct {
		memos          int64
		oldest, newest time.Time
	}
	groups := map[key]*group{}
	for _, entry := range entries {
		k := key{scope: entry.Ref.Scope(), orkflow: entry.Ref.Orkflow, task: entry.Ref.TaskID}
		g := groups[k]
		if g == nil {
			g = &group{oldest: entry.LastModified, newest: entry.LastModified}
			groups[k] = g
		}
		g.memos++
		if entry.LastModified.Before(g.oldest) {
			g.oldest = entry.LastModified
		}
		if entry.LastModified.After(g.newest) {
			g.newest = entry.LastModified
		}
	}
	keys := make([]key, 0, len(groups))
	for k := range groups {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool {
		if keys[i].scope != keys[j].scope {
			return keys[i].scope < keys[j].scope
		}
		if keys[i].orkflow != keys[j].orkflow {
			return keys[i].orkflow < keys[j].orkflow
		}
		return keys[i].task < keys[j].task
	})
	out := make([]*orkstackv1.CacheInventoryRow, 0, len(keys))
	for _, k := range keys {
		g := groups[k]
		out = append(out, &orkstackv1.CacheInventoryRow{
			Scope:       k.scope,
			OrkflowName: k.orkflow,
			TaskId:      k.task,
			Memos:       g.memos,
			Oldest:      timestamppb.New(g.oldest),
			Newest:      timestamppb.New(g.newest),
		})
	}
	return out
}
