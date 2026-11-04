package engine_test

import (
	"context"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/engine"
)

const capacityGiB = int64(1) << 30

// sizedOrker is one Orker of pool running one task and declaring a capacity:
// capCPU cpu and capMem bytes (0 leaves a dimension unbounded), reserved cpu
// and bytes against it, and a per-task cap of 2 cpu and 4 GiB.
func sizedOrker(id, pool string, capCPU float64, capMem int64, resCPU float64, resMem int64) *orkstackv1.DescribeOrkerResult {
	o := currentOrker(id, pool, 0)
	o.MaxConcurrent, o.RunningTasks = 16, 1
	o.Capacity = &orkstackv1.ResourceQuantity{Cpu: capCPU, MemoryBytes: capMem}
	o.Reserved = &orkstackv1.ResourceQuantity{Cpu: resCPU, MemoryBytes: resMem}
	o.MaxRequest = &orkstackv1.ResourceQuantity{Cpu: 2, MemoryBytes: 4 * capacityGiB}
	return o
}

// capacityWaitsOf reads fleet's inventory once and applies the rule to it,
// as InspectRun does.
func capacityWaitsOf(t *testing.T, fleet engine.FleetReader, pools ...string) (map[string]*orkstackv1.CapacityWait, int) {
	t.Helper()
	orkers, err := engine.ReadFleet(context.Background(), fleet)
	if err != nil {
		t.Fatalf("ReadFleet: %v", err)
	}
	return engine.CapacityWaitsOf(orkers, pools)
}

func editedOrker(o *orkstackv1.DescribeOrkerResult, edit func(*orkstackv1.DescribeOrkerResult)) *orkstackv1.DescribeOrkerResult {
	edit(o)
	return o
}

// TestCapacityWaits_EveryCountedOrkerMustBeFull is the rule: a pool's
// waiting task is waiting for capacity only when every Orker of the pool
// that answered and takes new tasks declares a capacity, runs a task, and
// cannot fit its per-task cap beside what it has reserved — and every live
// Orker answered. Anything else is nil, so the clients keep "waiting for an
// Orker". Every pool asked about is answered from its own Orkers of the one
// inventory: want is pool "build"'s, and also holds the others the row asks
// about, a nil entry being a pool that is not waiting for capacity.
func TestCapacityWaits_EveryCountedOrkerMustBeFull(t *testing.T) {
	cases := []struct {
		name       string
		fleet      *fakeFleet
		want       *orkstackv1.CapacityWait
		also       map[string]*orkstackv1.CapacityWait
		unanswered int
	}{
		{
			name: "every orker is at capacity",
			fleet: fleetOf(
				sizedOrker("b-1", "build", 4, 0, 4, 2*capacityGiB),
				sizedOrker("b-2", "build", 4, 0, 3, 1*capacityGiB)),
			want: &orkstackv1.CapacityWait{Pool: "build", Orkers: 2,
				Reserved: &orkstackv1.ResourceQuantity{Cpu: 7, MemoryBytes: 3 * capacityGiB},
				Capacity: &orkstackv1.ResourceQuantity{Cpu: 8}},
		},
		{
			name: "one orker still fits its cap",
			fleet: fleetOf(
				sizedOrker("b-1", "build", 4, 0, 4, 0),
				sizedOrker("b-2", "build", 4, 0, 2, 0)),
		},
		{
			name: "one orker declares no capacity",
			fleet: fleetOf(
				sizedOrker("b-1", "build", 4, 0, 4, 0),
				editedOrker(sizedOrker("b-2", "build", 0, 0, 0, 0), func(o *orkstackv1.DescribeOrkerResult) {
					o.Capacity, o.Reserved = nil, nil
				})),
		},
		{
			name: "every slot is taken but there is room: waiting for an orker, not for capacity",
			fleet: fleetOf(editedOrker(sizedOrker("b-1", "build", 16, 0, 2, 0), func(o *orkstackv1.DescribeOrkerResult) {
				o.MaxConcurrent = 1
			})),
		},
		{
			name: "an idle orker whose own polls hold its capacity is not full: a poll takes the next task",
			fleet: fleetOf(editedOrker(sizedOrker("b-1", "build", 4, 0, 4, 0), func(o *orkstackv1.DescribeOrkerResult) {
				o.RunningTasks = 0
			})),
		},
		{
			name:  "memory is the dimension that binds",
			fleet: fleetOf(sizedOrker("b-1", "build", 16, 8*capacityGiB, 2, 6*capacityGiB)),
			want: &orkstackv1.CapacityWait{Pool: "build", Orkers: 1,
				Reserved: &orkstackv1.ResourceQuantity{Cpu: 2, MemoryBytes: 6 * capacityGiB},
				Capacity: &orkstackv1.ResourceQuantity{Cpu: 16, MemoryBytes: 8 * capacityGiB}},
		},
		{
			name:  "a pinned member's overshoot is still full",
			fleet: fleetOf(sizedOrker("b-1", "build", 4, 0, 5, 0)),
			want: &orkstackv1.CapacityWait{Pool: "build", Orkers: 1,
				Reserved: &orkstackv1.ResourceQuantity{Cpu: 5},
				Capacity: &orkstackv1.ResourceQuantity{Cpu: 4}},
		},
		{
			name: "a dimension one orker leaves unbounded is reported by neither",
			fleet: fleetOf(
				sizedOrker("b-1", "build", 4, 8*capacityGiB, 4, 1*capacityGiB),
				sizedOrker("b-2", "build", 4, 0, 4, 1*capacityGiB)),
			want: &orkstackv1.CapacityWait{Pool: "build", Orkers: 2,
				Reserved: &orkstackv1.ResourceQuantity{Cpu: 8, MemoryBytes: 2 * capacityGiB},
				Capacity: &orkstackv1.ResourceQuantity{Cpu: 8}},
		},
		{
			name: "a cordoned orker with room takes nothing and is not counted",
			fleet: fleetOf(
				sizedOrker("b-1", "build", 4, 0, 4, 0),
				editedOrker(sizedOrker("b-2", "build", 4, 0, 0, 0), func(o *orkstackv1.DescribeOrkerResult) { o.Cordoned = true })),
			want: &orkstackv1.CapacityWait{Pool: "build", Orkers: 1,
				Reserved: &orkstackv1.ResourceQuantity{Cpu: 4},
				Capacity: &orkstackv1.ResourceQuantity{Cpu: 4}},
		},
		{
			name: "every orker cordoned: nothing to say about capacity",
			fleet: fleetOf(editedOrker(sizedOrker("b-1", "build", 4, 0, 4, 0), func(o *orkstackv1.DescribeOrkerResult) {
				o.Cordoned = true
			})),
		},
		{
			name: "an orker that did not answer could be of the pool, so nothing is claimed",
			fleet: &fakeFleet{
				orkers: []*orkstackv1.DescribeOrkerResult{sizedOrker("b-1", "build", 4, 0, 4, 0)},
				silent: []string{"b-2"},
			},
			unanswered: 1,
		},
		{
			name:       "no orker of the pool answered",
			fleet:      &fakeFleet{silent: []string{"b-1"}},
			unanswered: 1,
		},
		{
			name:  "another pool's full orkers say nothing about this one",
			fleet: fleetOf(sizedOrker("g-1", "gpu", 4, 0, 4, 0)),
		},
		{
			name: "a pool with an orker that has room, and a pool no orker serves, are not waiting beside a full one",
			fleet: fleetOf(
				sizedOrker("b-1", "build", 4, 0, 4, 0),
				sizedOrker("g-1", "gpu", 4, 0, 4, 0),
				sizedOrker("g-2", "gpu", 4, 0, 1, 0)),
			want: &orkstackv1.CapacityWait{Pool: "build", Orkers: 1,
				Reserved: &orkstackv1.ResourceQuantity{Cpu: 4},
				Capacity: &orkstackv1.ResourceQuantity{Cpu: 4}},
			also: map[string]*orkstackv1.CapacityWait{"gpu": nil, "io": nil},
		},
		{
			name: "two full pools are each reported on their own orkers",
			fleet: fleetOf(
				sizedOrker("b-1", "build", 4, 0, 4, 0),
				sizedOrker("g-1", "gpu", 4, 0, 4, 0),
				sizedOrker("g-2", "gpu", 4, 0, 3, 0)),
			want: &orkstackv1.CapacityWait{Pool: "build", Orkers: 1,
				Reserved: &orkstackv1.ResourceQuantity{Cpu: 4},
				Capacity: &orkstackv1.ResourceQuantity{Cpu: 4}},
			also: map[string]*orkstackv1.CapacityWait{"gpu": {Pool: "gpu", Orkers: 2,
				Reserved: &orkstackv1.ResourceQuantity{Cpu: 7},
				Capacity: &orkstackv1.ResourceQuantity{Cpu: 8}}},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			pools := []string{"build"}
			for pool := range tc.also {
				pools = append(pools, pool)
			}
			waits, unanswered := capacityWaitsOf(t, tc.fleet, pools...)
			if got := waits["build"]; !proto.Equal(got, tc.want) {
				t.Fatalf("capacity wait = %v, want %v", got, tc.want)
			}
			for pool, want := range tc.also {
				if got := waits[pool]; !proto.Equal(got, want) {
					t.Fatalf("pool %s capacity wait = %v, want %v", pool, got, want)
				}
			}
			if unanswered != tc.unanswered {
				t.Fatalf("unanswered = %d, want %d", unanswered, tc.unanswered)
			}
		})
	}
}

func TestStoreOutageWaits_OnlyWhenNoOrkerOfThePoolTakesNewTasks(t *testing.T) {
	early := time.Date(2026, 11, 4, 9, 0, 0, 0, time.UTC)
	late := early.Add(time.Minute)
	outage := func(id string, since time.Time) *orkstackv1.DescribeOrkerResult {
		return editedOrker(currentOrker(id, "gpu", 0), func(o *orkstackv1.DescribeOrkerResult) {
			o.Cordoned = true
			o.StoreOutage = &orkstackv1.StoreOutage{Reason: "object storage unreachable", Since: timestamppb.New(since)}
		})
	}
	operator := editedOrker(currentOrker("gpu-op", "gpu", 0), func(o *orkstackv1.DescribeOrkerResult) {
		o.Cordoned = true
		o.CordonReason = "patching"
	})
	cases := []struct {
		name       string
		fleet      *fakeFleet
		wantOrkers int32
		wantSince  time.Time
	}{
		{"one Orker", fleetOf(outage("gpu-1", early)), 1, early},
		{"the earliest since of several", fleetOf(outage("gpu-1", late), outage("gpu-2", early)), 2, early},
		{"beside an operator's cordon", fleetOf(outage("gpu-1", early), operator), 1, early},
		{"a sibling still takes tasks", fleetOf(outage("gpu-1", early), currentOrker("gpu-2", "gpu", 0)), 0, time.Time{}},
		{"an operator's cordon alone", fleetOf(operator), 0, time.Time{}},
		{"no Orker", fleetOf(), 0, time.Time{}},
		{"another pool's outage", fleetOf(editedOrker(outage("cpu-1", early), func(o *orkstackv1.DescribeOrkerResult) { o.Pool = "cpu" })), 0, time.Time{}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			orkers, err := engine.ReadFleet(context.Background(), tc.fleet)
			if err != nil {
				t.Fatal(err)
			}
			wait := engine.StoreOutageWaitsOf(orkers, []string{"gpu"})["gpu"]
			if wait.GetOrkers() != tc.wantOrkers {
				t.Fatalf("wait = %v, want %d Orkers", wait, tc.wantOrkers)
			}
			if tc.wantOrkers > 0 && !wait.GetSince().AsTime().Equal(tc.wantSince) {
				t.Fatalf("since = %s, want %s", wait.GetSince().AsTime(), tc.wantSince)
			}
		})
	}
}
