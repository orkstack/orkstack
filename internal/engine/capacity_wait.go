package engine

import (
	"context"
	"math"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
)

// CapacityWaitsOf answers, for each pool named, whether a task that no Orker
// of the pool has started is waiting for capacity rather than for an Orker
// (orkstackv1.CapacityWait, InspectRun's live view), over an inventory
// already read (ReadFleet): a pool is in the map only when its waiting task
// is waiting for capacity. It asks nothing, so one reading answers every
// pool.
//
// It is derived on read and held nowhere (AGENTS.md §7), over the Orkers that
// answered on their own channel and are not cordoned — the Orkers submit
// counts (poolCensus). Unlike a submit, it claims nothing while any live Orker
// did not answer: which pool that Orker serves is what it did not say, so
// "every Orker of the pool is full" could not be known, and the answer stays
// "waiting for an Orker", which is true either way. unanswered says how many
// did not, for the caller to record.
func CapacityWaitsOf(orkers []FleetOrker, pools []string) (waits map[string]*orkstackv1.CapacityWait, unanswered int) {
	waits = map[string]*orkstackv1.CapacityWait{}
	census := censusOf(orkers)
	if unanswered = len(census.unanswered); unanswered > 0 {
		return waits, unanswered
	}
	for _, pool := range pools {
		// The census is already read, so taking asks nothing and fails never.
		taking, _ := census.taking(context.Background(), pool)
		if wait := capacityWait(pool, taking); wait != nil {
			waits[pool] = wait
		}
	}
	return waits, 0
}

// capacityWait is the rule for one pool: every Orker counted declares a
// capacity and is full under it, or the pool is not waiting for capacity.
//
// Full is the capacity half of the Orker's own admission test, negated
// (internal/orker capacity.go, Capacity.grantable, whose arithmetic fullUnder
// repeats on the figures DescribeOrker reports): it reserves its per-task cap
// before it polls for a task, so it has room for another only while that cap
// fits beside what stands reserved in every dimension it bounds. The slots
// half is left out on purpose: an Orker with room whose every slot is taken
// is waiting on max_concurrent, not on capacity, and keeps the pool out of
// the map.
func capacityWait(pool string, orkers []FleetOrker) *orkstackv1.CapacityWait {
	if len(orkers) == 0 {
		return nil
	}
	var reserved, capacity resourceSum
	boundsCPU, boundsMemory := true, true
	for _, orker := range orkers {
		described := orker.Described
		limit := described.GetCapacity()
		if limit == nil || !fullUnder(described) {
			return nil
		}
		reserved.add(described.GetReserved())
		capacity.add(limit)
		boundsCPU = boundsCPU && limit.GetCpu() > 0
		boundsMemory = boundsMemory && limit.GetMemoryBytes() > 0
	}
	// A sum over Orkers one of which leaves a dimension unbounded is not a
	// bound on that dimension, so it is reported as unbounded.
	if !boundsCPU {
		capacity.milli = 0
	}
	if !boundsMemory {
		capacity.bytes = 0
	}
	return &orkstackv1.CapacityWait{
		Pool:     pool,
		Orkers:   int32(len(orkers)),
		Reserved: reserved.proto(),
		Capacity: capacity.proto(),
	}
}

// fullUnder reports whether an Orker declaring a capacity has no room for
// another task: its per-task cap does not fit beside what it has reserved in
// some dimension the capacity bounds (a dimension at 0 is unbounded).
//
// An Orker running no task is never full. What it reserves is then its own
// outstanding polls, each at the cap — an idle Orker whose capacity holds one
// cap reads "reserved 1/1" — and an outstanding poll takes the next task;
// RESERVED cannot tell those polls from tasks, so this is the one reading of
// it that is never a false "full".
func fullUnder(o *orkstackv1.DescribeOrkerResult) bool {
	if o.GetRunningTasks() == 0 {
		return false
	}
	limit, reserved, ceiling := o.GetCapacity(), o.GetReserved(), o.GetMaxRequest()
	if c := millicores(limit.GetCpu()); c > 0 &&
		millicores(reserved.GetCpu())+millicores(ceiling.GetCpu()) > c {
		return true
	}
	if m := limit.GetMemoryBytes(); m > 0 &&
		reserved.GetMemoryBytes()+ceiling.GetMemoryBytes() > m {
		return true
	}
	return false
}

// millicores is cpu in whole millicores, rounded up as the Orker's ledger
// rounds a request (orker capacity.go, amountOf), so the two compare one
// number the same way.
func millicores(cores float64) int64 { return int64(math.Ceil(cores*1000 - 1e-9)) }

// resourceSum adds ResourceQuantity values exactly: cpu in millicores and
// memory in bytes.
type resourceSum struct {
	milli int64
	bytes int64
}

func (s *resourceSum) add(q *orkstackv1.ResourceQuantity) {
	s.milli += millicores(q.GetCpu())
	s.bytes += q.GetMemoryBytes()
}

func (s resourceSum) proto() *orkstackv1.ResourceQuantity {
	return &orkstackv1.ResourceQuantity{Cpu: float64(s.milli) / 1000, MemoryBytes: s.bytes}
}
