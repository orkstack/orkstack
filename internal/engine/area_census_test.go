package engine_test

import (
	"context"
	"fmt"
	"sort"
	"testing"

	"go.temporal.io/sdk/client"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/internal/testsupport/must"
)

// TestScheduleCensus_DescribesEverySchedulesInTurnWithinItsCap: a pass lists
// every OrkStack schedule of every namespace and describes at most its cap,
// starting where the last pass stopped, so a deployment with more schedules
// than the cap is described in full over a few passes. Each request it made
// is counted.
func TestScheduleCensus_DescribesEverySchedulesInTurnWithinItsCap(t *testing.T) {
	ctx := context.Background()
	cluster := newFakeScheduleCluster()
	s := engine.NewTemporalScheduler(cluster, "")
	var names []string
	for i := range 5 {
		ns := "alpha"
		if i%2 == 1 {
			ns = "beta"
		}
		r := scheduleRecord(ns, fmt.Sprintf("s%d", i), &orkstackv1.ScheduleSpec{Cron: []string{"0 2 * * *"}})
		_, err := s.Create(ctx, r, i == 0)
		must.OK(t, err)
		cluster.sc.entries = append(cluster.sc.entries, &client.ScheduleListEntry{
			ID: engine.TemporalScheduleID(ns, r.GetName()), Paused: i == 0,
			Memo: memoOf(t, map[string]string{
				engine.MemoScheduleID: r.GetScheduleId(), engine.MemoScheduleName: r.GetName(),
				engine.MemoNamespace: ns, engine.MemoOrkflowName: r.GetOrkflowName(),
			}),
		})
		names = append(names, ns+"/"+r.GetName())
	}
	cluster.sc.entries = append(cluster.sc.entries, &client.ScheduleListEntry{ID: "someone-else",
		Memo: memoOf(t, map[string]string{engine.MemoNamespace: "alpha"})})
	before := s.CensusRequests()

	seen := map[string]int{}
	for range 3 {
		census, err := s.Census(ctx, 2)
		must.OK(t, err)
		if len(census.Entries) != 5 {
			t.Fatalf("listed = %d, want 5 of ours", len(census.Entries))
		}
		described := 0
		for _, e := range census.Entries {
			if e.Described {
				described++
				seen[e.Namespace+"/"+e.Name]++
			}
		}
		if described != 2 {
			t.Fatalf("described = %d, want the cap of 2", described)
		}
	}
	var got []string
	for k := range seen {
		got = append(got, k)
	}
	sort.Strings(got)
	sort.Strings(names)
	if fmt.Sprint(got) != fmt.Sprint(names) {
		t.Fatalf("described over three passes = %v, want every schedule %v", got, names)
	}
	if made := s.CensusRequests() - before; made != 3*(1+2) {
		t.Fatalf("requests = %d, want one list and two describes per pass", made)
	}
	census, err := s.Census(ctx, 0)
	must.OK(t, err)
	if !census.Entries[0].Paused || census.Entries[1].Paused {
		t.Fatalf("paused = %v, %v; want the list's answer", census.Entries[0].Paused, census.Entries[1].Paused)
	}
}

func TestScheduleCensus_ACapOfZeroOnlyLists(t *testing.T) {
	cluster := newFakeScheduleCluster()
	s := engine.NewTemporalScheduler(cluster, "")
	census, err := s.Census(context.Background(), 0)
	must.OK(t, err)
	if len(census.Entries) != 0 {
		t.Fatalf("census = %+v", census)
	}
}
