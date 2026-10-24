package main

import (
	"math/rand"
	"reflect"
	"testing"
)

func testJobs(costs ...float64) []job {
	jobs := make([]job, len(costs))
	for i, c := range costs {
		jobs[i] = job{ID: string(rune('a' + i)), Package: "p", Tests: []string{"Test" + string(rune('A'+i))}, Cost: c}
	}
	return jobs
}

func TestSplitShards_LongestFirstIntoTheLeastLoaded(t *testing.T) {
	jobs := testJobs(10, 9, 8, 7, 6, 5)
	got := splitShards(jobs, 3, 1)
	// 10 -> 1, 9 -> 2, 8 -> 3, 7 -> 3 (8+7=15? no: loads 10,9,8 -> 7 to
	// shard 3 makes 15; 6 goes to shard 2 (15); 5 to shard 1 (15)).
	want := map[int][]string{1: {"a", "f"}, 2: {"b", "e"}, 3: {"c", "d"}}
	for _, s := range got {
		if !reflect.DeepEqual(s.Jobs, want[s.Index]) {
			t.Errorf("shard %d = %v, want %v (all loads: %+v)", s.Index, s.Jobs, want[s.Index], got)
		}
	}
	for _, s := range got {
		if s.EstimatedSeconds != 15 {
			t.Errorf("shard %d estimate = %v, want 15", s.Index, s.EstimatedSeconds)
		}
	}
}

func TestSplitShards_EveryJobInExactlyOneShard(t *testing.T) {
	rng := rand.New(rand.NewSource(7))
	for trial := 0; trial < 50; trial++ {
		n := 1 + rng.Intn(40)
		costs := make([]float64, n)
		for i := range costs {
			costs[i] = float64(1 + rng.Intn(600))
		}
		jobs := testJobs(costs...)
		got := splitShards(jobs, 1+rng.Intn(20), 1+rng.Intn(4))
		seen := map[string]int{}
		for _, s := range got {
			for _, id := range s.Jobs {
				seen[id]++
			}
		}
		if len(seen) != n {
			t.Fatalf("trial %d: %d of %d jobs placed", trial, len(seen), n)
		}
		for id, c := range seen {
			if c != 1 {
				t.Fatalf("trial %d: job %s is in %d shards", trial, id, c)
			}
		}
	}
}

func TestSplitShards_IsDeterministic(t *testing.T) {
	base := testJobs(40, 40, 30, 30, 20, 20, 10, 10, 5)
	first := splitShards(append([]job{}, base...), 4, 2)
	for i := 0; i < 30; i++ {
		shuffled := append([]job{}, base...)
		rand.New(rand.NewSource(int64(i))).Shuffle(len(shuffled), func(a, b int) {
			shuffled[a], shuffled[b] = shuffled[b], shuffled[a]
		})
		got := splitShards(shuffled, 4, 2)
		if !reflect.DeepEqual(got, first) {
			t.Fatalf("input order %d changed the split:\n%+v\n%+v", i, got, first)
		}
	}
}

func TestSplitShards_ClampsToTheNumberOfJobs(t *testing.T) {
	jobs := testJobs(10, 5)
	got := splitShards(jobs, 8, 1)
	if len(got) != 2 {
		t.Fatalf("%d shards for 2 jobs, want 2", len(got))
	}
	if len(splitShards(nil, 5, 1)) != 1 {
		t.Error("an empty plan did not clamp to one shard")
	}
}

func TestSplitShards_HeavyJobsStayOffTheLightShards(t *testing.T) {
	jobs := testJobs(500, 400, 300, 200, 100, 90, 30, 25, 20, 10)
	for i := 0; i < 6; i++ {
		jobs[i].Heavy = true
	}
	got := splitShards(jobs, 10, 4)
	heavy, light := 0, 0
	for _, s := range got {
		isHeavy := false
		for _, id := range s.Jobs {
			for i := range jobs {
				if jobs[i].ID == id && jobs[i].Heavy {
					isHeavy = true
				}
			}
		}
		if isHeavy {
			heavy++
			for _, id := range s.Jobs {
				for i := range jobs {
					if jobs[i].ID == id && !jobs[i].Heavy {
						t.Fatalf("light job %s shares heavy shard %d: %+v", id, s.Index, got)
					}
				}
			}
		} else {
			light++
		}
	}
	if heavy != 5 || light != 4 {
		t.Errorf("%d heavy and %d light shards, want 5 and 4 (light shards clamp to the light jobs): %+v", heavy, light, got)
	}
	// A one-shard plan keeps one heavy and one light shard when both exist.
	got = splitShards(jobs, 1, 4)
	if len(got) != 2 {
		t.Errorf("%d shards for a one-shard plan with both kinds, want 2", len(got))
	}
}

func TestSplitShards_TheFloorIsTheLongestJob(t *testing.T) {
	jobs := testJobs(548, 300, 200, 100)
	for _, n := range []int{1, 2, 4, 10} {
		shards := splitShards(append([]job{}, jobs...), n, 2)
		longest := 0.0
		for _, s := range shards {
			if s.EstimatedMakespan > longest {
				longest = s.EstimatedMakespan
			}
		}
		if longest < floorSeconds(jobs) {
			t.Errorf("n=%d: the longest shard makespan %.0f is below the floor %.0f", n, longest, floorSeconds(jobs))
		}
	}
	if floorSeconds(jobs) != 548 {
		t.Errorf("floor = %v, want 548", floorSeconds(jobs))
	}
}

func TestPlan_AnUnmeasuredTestIsAConservativeJobOfItsOwn(t *testing.T) {
	jobs := plan(map[string][]string{"p": {"TestNew", "TestKnown"}}, costs{{"p", "TestKnown"}: 1}, map[string]map[string]string{"p": {"TestNew": "integration"}})
	byTest := map[string]job{}
	for _, j := range jobs {
		byTest[j.Tests[0]] = j
	}
	j := byTest["TestNew"]
	if j.Cost != unmeasuredCost || j.Known || len(j.Tests) != 1 || j.Timeout != unknownTimeout {
		t.Errorf("unmeasured integration test = %+v, want cost 300, alone, 30m timeout", j)
	}
}

func TestPlan_AnUnmeasuredUnitFileTestCostsTheUnitDefault(t *testing.T) {
	jobs := plan(map[string][]string{"p": {"TestNewUnit"}}, costs{}, map[string]map[string]string{"p": {"TestNewUnit": "unit"}})
	if len(jobs) != 1 {
		t.Fatalf("jobs = %+v", jobs)
	}
	j := jobs[0]
	if j.Cost != unmeasuredUnitCost || j.Known || len(j.Tests) != 1 || j.Timeout != unknownTimeout {
		t.Errorf("unmeasured unit test = %+v, want cost 30, alone, 30m timeout", j)
	}
}

func TestPlan_OnlyWallClockSensitiveScaleTestsAreHeavy(t *testing.T) {
	jobs := plan(map[string][]string{"p": {
		"TestIntegration_SoakTasks", "TestIntegration_APinTakes300Seconds", "TestQuick",
	}},
		costs{
			{"p", "TestIntegration_SoakTasks"}:           200,
			{"p", "TestIntegration_APinTakes300Seconds"}: 300,
			{"p", "TestQuick"}:                           5,
		},
		map[string]map[string]string{"p": {}})
	byTest := map[string]job{}
	for _, j := range jobs {
		byTest[j.Tests[0]] = j
	}
	if !byTest["TestIntegration_SoakTasks"].Heavy {
		t.Error("the soak, which asserts latency bounds, is not marked heavy")
	}
	if byTest["TestIntegration_APinTakes300Seconds"].Heavy {
		t.Error("a merely slow 300 s test is marked heavy; it must run four-wide")
	}
	if byTest["TestQuick"].Heavy {
		t.Error("a 5 s test is marked heavy")
	}
	// An unmeasured test outside the heavy set is not heavy either.
	unmeasured := plan(map[string][]string{"p": {"TestNew"}}, costs{}, map[string]map[string]string{"p": {"TestNew": "integration"}})
	if unmeasured[0].Heavy {
		t.Error("an unmeasured ordinary test is marked heavy")
	}
	for _, name := range []string{
		"TestIntegration_SoakFanOut",
		"TestIntegration_SoakFanOutMaxParallel",
		"TestIntegration_ThreeThousandTaskDefinitionRuns",
		"TestIntegration_SnapshotOverlayBytesForAThousandTasks",
		"TestIntegration_SoakItemCeilingByReferenceWithOverlays",
		"TestIntegration_ATaskCeilingRunCarriesOnAtAReducedCeiling",
		"TestIntegration_AnItemCeilingRunCarriesOnAtAReducedCeiling",
		"TestIntegration_AFanOutAtAReducedItemCeilingMeetsItsThresholds",
	} {
		j := plan(map[string][]string{"p": {name}}, costs{}, map[string]map[string]string{"p": {name: "integration"}})
		if !j[0].Heavy {
			t.Errorf("%s is not marked heavy", name)
		}
	}
}
