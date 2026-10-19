package main

import "sort"

// splitShards deals plan()'s jobs into n shards: longest first, each into the
// shard with the least estimated load, a tie going to the lowest index. Jobs
// arrive longest first already; they are sorted again with plan()'s own tie
// breaks so any input order gives the same shards. n is clamped to the number
// of jobs (and to at least 1). Each job's Shard is set (1-based).
func splitShards(jobs []job, n int, slots int) []planShard {
	sortJobs(jobs)
	if n > len(jobs) {
		n = len(jobs)
	}
	if n < 1 {
		n = 1
	}
	shards := make([]planShard, n)
	for i := range shards {
		shards[i] = planShard{Index: i + 1, Jobs: []string{}}
	}
	for i := range jobs {
		k := 0
		for s := range shards {
			if shards[s].EstimatedSeconds < shards[k].EstimatedSeconds {
				k = s
			}
		}
		jobs[i].Shard = k + 1
		shards[k].Jobs = append(shards[k].Jobs, jobs[i].ID)
		shards[k].EstimatedSeconds += jobs[i].Cost
	}
	for s := range shards {
		var own []job
		for _, j := range jobs {
			if j.Shard == s+1 {
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
