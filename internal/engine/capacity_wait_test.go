package engine_test

import (
	"context"
	"errors"
	"testing"

	"google.golang.org/protobuf/proto"

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

func editedOrker(o *orkstackv1.DescribeOrkerResult, edit func(*orkstackv1.DescribeOrkerResult)) *orkstackv1.DescribeOrkerResult {
	edit(o)
	return o
}

// TestCapacityWaits_EveryCountedOrkerMustBeFull is the rule: a pool's
// waiting task is waiting for capacity only when every Orker of the pool
// that answered and takes new tasks declares a capacity, runs a task, and
// cannot fit its per-task cap beside what it has reserved — and every live
// Orker answered. Anything else is nil, so the clients keep "waiting for an
// Orker".
func TestCapacityWaits_EveryCountedOrkerMustBeFull(t *testing.T) {
	cases := []struct {
		name       string
		fleet      *fakeFleet
		want       *orkstackv1.CapacityWait
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
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			waits, unanswered, err := engine.CapacityWaits(context.Background(), tc.fleet, []string{"build"})
			if err != nil {
				t.Fatalf("CapacityWaits: %v", err)
			}
			if got := waits["build"]; !proto.Equal(got, tc.want) {
				t.Fatalf("capacity wait = %v, want %v", got, tc.want)
			}
			if unanswered != tc.unanswered {
				t.Fatalf("unanswered = %d, want %d", unanswered, tc.unanswered)
			}
		})
	}
}

// TestCapacityWaits_ReadsTheInventoryOnceForEveryPool bounds the cost: one
// presence read and one describe per live Orker, however many pools ask.
func TestCapacityWaits_ReadsTheInventoryOnceForEveryPool(t *testing.T) {
	fleet := fleetOf(
		sizedOrker("b-1", "build", 4, 0, 4, 0),
		sizedOrker("g-1", "gpu", 4, 0, 4, 0),
		sizedOrker("g-2", "gpu", 4, 0, 1, 0))
	waits, _, err := engine.CapacityWaits(context.Background(), fleet, []string{"build", "gpu", "io"})
	if err != nil {
		t.Fatalf("CapacityWaits: %v", err)
	}
	if fleet.reads != 1 || len(fleet.asked) != 3 {
		t.Fatalf("presence reads = %d and describes = %v, want 1 read and one describe per Orker", fleet.reads, fleet.asked)
	}
	if waits["build"] == nil || waits["gpu"] != nil || waits["io"] != nil {
		t.Fatalf("waits = %v, want only build waiting for capacity", waits)
	}
}

// TestCapacityWaits_NoPoolsReadsNothing: nothing waits, nothing is asked.
func TestCapacityWaits_NoPoolsReadsNothing(t *testing.T) {
	waits, _, err := engine.CapacityWaits(context.Background(), refusingFleet{t}, nil)
	if err != nil || len(waits) != 0 {
		t.Fatalf("CapacityWaits(no pools) = %v, %v; want nothing and no read", waits, err)
	}
}

// TestCapacityWaits_AFailedPresenceReadIsAnError: nothing was measured, so
// the caller is told rather than handed "no pool is full".
func TestCapacityWaits_AFailedPresenceReadIsAnError(t *testing.T) {
	_, _, err := engine.CapacityWaits(context.Background(),
		&fakeFleet{observeErr: errors.New("frontend unavailable")}, []string{"build"})
	if err == nil {
		t.Fatal("CapacityWaits over an unreadable inventory = nil error, want the read's error")
	}
}
