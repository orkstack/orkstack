package main

import "sort"

// splitShards deals plan()'s jobs into shards. Heavy jobs fill heavy-only
// shards, up to half the requested count, one heavy test at a time inside a
// shard; light jobs fill the rest, longest first, each into the least loaded,
// a tie going to the lowest index. A heavy shard therefore never measures a
// soak with ordinary work on the same Temporal, and the plan does not grow to
// one shard per heavy test. Jobs arrive longest first already; they are
// sorted again with plan()'s own tie breaks so any input order gives the same
// shards. Each job's Shard is set (1-based).
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
	heavyShards := n / 2
	if heavyShards < 1 && len(heavyIdx) > 0 {
		heavyShards = 1
	}
	if heavyShards > len(heavyIdx) {
		heavyShards = len(heavyIdx)
	}
	shards := make([]planShard, 0, n)
	for i := 0; i < heavyShards; i++ {
		shards = append(shards, planShard{Index: len(shards) + 1, Jobs: []string{}})
	}
	for _, i := range heavyIdx {
		k := 0
		for s := 1; s < heavyShards; s++ {
			if shards[s].EstimatedSeconds < shards[k].EstimatedSeconds {
				k = s
			}
		}
		jobs[i].Shard = shards[k].Index
		shards[k].Jobs = append(shards[k].Jobs, jobs[i].ID)
		shards[k].EstimatedSeconds += jobs[i].Cost
	}
	for s := 0; s < heavyShards; s++ {
		// Heavy tests run one at a time, so the shard's wall is their sum.
		shards[s].EstimatedMakespan = shards[s].EstimatedSeconds
	}
	lightShards := n - heavyShards
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
	for s := firstLight; s < len(shards); s++ {
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
