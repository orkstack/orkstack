package engine

import (
	"testing"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
)

// A profile cause is one more cause that holds an Orker's cordon (AGENTS.md
// §7, Cordon): an unavailable profile keeps its pool live, a drift does not,
// and the Orker keeps its pool live only if every cause it holds does.
func TestKeepsPoolLive_ProfileCauses(t *testing.T) {
	unavailable := &orkstackv1.ProfileCordon{Cause: orkstackv1.ProfileCordon_CAUSE_UNAVAILABLE, Profile: "p"}
	drift := &orkstackv1.ProfileCordon{Cause: orkstackv1.ProfileCordon_CAUSE_DRIFT, Profile: "q"}
	unknown := &orkstackv1.ProfileCordon{Profile: "r"}
	outage := &orkstackv1.StoreOutage{Reason: "x"}
	for name, tc := range map[string]struct {
		o            *orkstackv1.DescribeOrkerResult
		live, paused bool
	}{
		"unavailable alone":             {&orkstackv1.DescribeOrkerResult{Cordoned: true, ProfileCordons: []*orkstackv1.ProfileCordon{unavailable}}, true, false},
		"drift alone":                   {&orkstackv1.DescribeOrkerResult{Cordoned: true, ProfileCordons: []*orkstackv1.ProfileCordon{drift}}, false, false},
		"unavailable beside drift":      {&orkstackv1.DescribeOrkerResult{Cordoned: true, ProfileCordons: []*orkstackv1.ProfileCordon{unavailable, drift}}, false, false},
		"cause this engine cannot read": {&orkstackv1.DescribeOrkerResult{Cordoned: true, ProfileCordons: []*orkstackv1.ProfileCordon{unknown}}, false, false},
		"outage beside unavailable":     {&orkstackv1.DescribeOrkerResult{Cordoned: true, StoreOutage: outage, ProfileCordons: []*orkstackv1.ProfileCordon{unavailable}}, true, false},
		"outage beside drift":           {&orkstackv1.DescribeOrkerResult{Cordoned: true, StoreOutage: outage, ProfileCordons: []*orkstackv1.ProfileCordon{drift}}, false, false},
		"outage alone":                  {&orkstackv1.DescribeOrkerResult{Cordoned: true, StoreOutage: outage}, true, true},
		"operator beside unavailable":   {&orkstackv1.DescribeOrkerResult{Cordoned: true, CordonReason: "patch", ProfileCordons: []*orkstackv1.ProfileCordon{unavailable}}, false, false},
	} {
		t.Run(name, func(t *testing.T) {
			if got := keepsPoolLive(tc.o); got != tc.live {
				t.Errorf("keepsPoolLive = %v, want %v", got, tc.live)
			}
			if got := pausedForStoreOutage(tc.o); got != tc.paused {
				t.Errorf("pausedForStoreOutage = %v, want %v", got, tc.paused)
			}
		})
	}
}

// The census counts an Orker held by an unavailable profile as serving its
// pool, never as taking work now; a drifted one does neither.
func TestPoolCensus_ProfileCausesServeButDoNotTake(t *testing.T) {
	orker := func(id string, cause orkstackv1.ProfileCordon_Cause) FleetOrker {
		return FleetOrker{Presence: OrkerPresence{Identity: id}, Described: &orkstackv1.DescribeOrkerResult{
			OrkerId: id, Pool: "p", Cordoned: true,
			ProfileCordons: []*orkstackv1.ProfileCordon{{Cause: cause, Profile: "x"}},
		}}
	}
	census := censusOf([]FleetOrker{
		orker("unavailable", orkstackv1.ProfileCordon_CAUSE_UNAVAILABLE),
		orker("drifted", orkstackv1.ProfileCordon_CAUSE_DRIFT),
	})
	taking, err := census.taking(t.Context(), "p")
	if err != nil || len(taking) != 0 {
		t.Fatalf("taking = %v, %v; want none", taking, err)
	}
	serving, err := census.serving(t.Context(), "p")
	if err != nil || len(serving) != 1 || serving[0].Presence.Identity != "unavailable" {
		t.Fatalf("serving = %v, %v; want only the unavailable Orker", serving, err)
	}
}
