package main

import "sort"

// splitShards deals plan()'s jobs into shards. Each heavy job gets a shard
// of its own — its own Temporal on CI, and a quiet machine locally — and the
// light jobs fill the remaining shards, longest first, each into the least
// loaded, a tie going to the lowest index. The requested n is a floor: with
// more heavy jobs than n, the plan grows so every heavy job stays alone.
// Jobs arrive longest first already; they are sorted again with plan()'s own
// tie breaks so any input order gives the same shards. Each job's Shard is set
// (1-based).
func splitShards(jobs []job, n int, slots int) []planShard {
	sortJobs(jobs)
	if len(jobs) == 0 {
		return []planShard{{Index: 1, Jobs: []string{}}}
	}
	var heavyIdx, lightIdx []int
	for i := range jobs {
		if jobs[i].Heavy {
			heavyIdx = append(heavyIdx, i)
		} else {
			lightIdx = append(lightIdx, i)
		}
	}
	shards := make([]planShard, 0, n)
	for _, i := range heavyIdx {
		idx := len(shards) + 1
		jobs[i].Shard = idx
		shards = append(shards, planShard{
			Index: idx, Jobs: []string{jobs[i].ID}, EstimatedSeconds: jobs[i].Cost,
			EstimatedMakespan: makespan([]job{jobs[i]}, slots),
		})
	}
	lightShards := n - len(heavyIdx)
	if lightShards > len(lightIdx) {
		lightShards = len(lightIdx)
	}
	if lightShards < 1 && len(lightIdx) > 0 {
		lightShards = 1
	}
	firstLight := len(shards)
	for i := 0; i < lightShards; i++ {
		shards = append(shards, planShard{Index: len(shards) + 1, Jobs: []string{}})
	}
	for _, i := range lightIdx {
		k := firstLight
		for s := firstLight; s < len(shards); s++ {
			if shards[s].EstimatedSeconds < shards[k].EstimatedSeconds {
				k = s
			}
		}
		jobs[i].Shard = shards[k].Index
		shards[k].Jobs = append(shards[k].Jobs, jobs[i].ID)
		shards[k].EstimatedSeconds += jobs[i].Cost
	}
	for s := range shards {
		var own []job
		for _, j := range jobs {
			if j.Shard == shards[s].Index {
				own = append(own, j)
			}
		}
		shards[s].EstimatedMakespan = makespan(own, slots)
	}
	return shards
}

// sortJobs is plan()'s order: longest first, then by package, then by first
// test.
func sortJobs(jobs []job) {
	sort.SliceStable(jobs, func(i, j int) bool {
		if jobs[i].Cost != jobs[j].Cost {
			return jobs[i].Cost > jobs[j].Cost
		}
		if jobs[i].Package != jobs[j].Package {
			return jobs[i].Package < jobs[j].Package
		}
		return jobs[i].Tests[0] < jobs[j].Tests[0]
	})
}

// floorSeconds is the longest single job: no number of shards finishes
// sooner.
func floorSeconds(jobs []job) float64 {
	var f float64
	for _, j := range jobs {
		if j.Cost > f {
			f = j.Cost
		}
	}
	return f
}
