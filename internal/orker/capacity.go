package orker

import (
	"context"
	"errors"
	"fmt"
	"math"
	"sync"
	"time"

	"go.temporal.io/sdk/worker"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/quantity"
)

// Capacity admission (docs/DEPLOY.md "Declaring an Orker's capacity").
//
// An Orker that declares a capacity admits no task that would take its
// reservations above it. The mechanism is forced by the Temporal SDK: a
// worker reserves a slot BEFORE it polls, and cannot see a task until it has
// taken it, so the only bound it can hold a poll to is one it knows before
// polling — the per-task cap (orker.resources.max_cpu and max_memory), the
// largest request this Orker runs. So:
//
//   - every poll on the pool's channel reserves the cap, and is granted only
//     while that fits beside everything already reserved;
//   - once the task is in hand, its reservation is trimmed to the task's own
//     resolved request (SecurityPolicy.ReservedResources), and an activity
//     that runs no container (DescribeOrker) to nothing;
//   - what remains is freed when the attempt's container is REMOVED, not when
//     the activity returns: a container whose removal failed still holds its
//     host, and keeps its reservation until a later removal succeeds.
//
// A pinned member is counted but never made to wait. It is taken from its
// pin's own channel, which no capacity can gate without breaking the pin's
// heartbeats or deadlocking two pins (docs/DEPLOY.md), so its request joins
// the ledger on arrival and may push it over capacity; while it stands at or
// over capacity the pool's channel grants no poll, so ordinary work yields
// and the overshoot drains.
//
// The ledger is process-local, exactly as a cordon is: never stored, never
// shared, gone with the process. It is admission, not placement: Temporal
// decides which polling Orker receives a task, and OrkStack never chooses a
// host.

// ReasonResourceAdmissionMismatch fails an attempt whose resolved request is
// larger than the reservation it was polled under, when free capacity cannot
// cover the difference. It cannot happen while every poll reserves the
// per-task cap, which bounds every request this Orker runs; it exists so a
// future change that broke that would fail loudly rather than over-commit.
const ReasonResourceAdmissionMismatch = "RESOURCE_ADMISSION_MISMATCH"

// CapacityCPUVar and CapacityMemoryVar are orker.capacity.cpu and
// orker.capacity.memory: what this Orker admits tasks within. Unset, the
// default, admits by max_concurrent alone.
const (
	CapacityCPUVar    = "ORK_ORKER_CAPACITY_CPU"
	CapacityMemoryVar = "ORK_ORKER_CAPACITY_MEMORY"
)

// capacityReapInterval is how often an Orker with a declared capacity tries
// again to remove the containers it could not remove, releasing each one's
// reservation once it is gone.
const capacityReapInterval = 30 * time.Second

// amount is a reservation: cpu in millicores and memory in bytes, so the
// ledger adds and subtracts exactly. In a limit, a dimension at 0 is
// unbounded.
type amount struct {
	milli int64
	bytes int64
}

func amountOf(cores float64, bytes int64) amount {
	return amount{milli: int64(math.Ceil(cores*1000 - 1e-9)), bytes: bytes}
}

func amountFromProto(r *orkstackv1.ReservedResources) amount {
	if r == nil {
		return amount{}
	}
	return amountOf(r.GetCpu(), r.GetMemoryBytes())
}

func (a amount) add(b amount) amount { return amount{a.milli + b.milli, a.bytes + b.bytes} }
func (a amount) sub(b amount) amount { return amount{a.milli - b.milli, a.bytes - b.bytes} }

// within reports whether a fits under limit, where a limit dimension at 0 is
// unbounded.
func (a amount) within(limit amount) bool {
	return (limit.milli == 0 || a.milli <= limit.milli) && (limit.bytes == 0 || a.bytes <= limit.bytes)
}

// exceeds reports whether a is larger than b in either dimension.
func (a amount) exceeds(b amount) bool { return a.milli > b.milli || a.bytes > b.bytes }

func (a amount) proto() *orkstackv1.ResourceQuantity {
	return &orkstackv1.ResourceQuantity{Cpu: float64(a.milli) / 1000, MemoryBytes: a.bytes}
}

func (a amount) String() string {
	return fmt.Sprintf("%g cpu, %d bytes", float64(a.milli)/1000, a.bytes)
}

// parseCapacity reads orker.capacity: either dimension may be unset, which
// leaves it unbounded; both unset declares no capacity at all (ok false).
func parseCapacity(cpu, memory string) (limit amount, ok bool, err error) {
	if cpu != "" {
		cores, err := quantity.CPU(cpu)
		if err != nil {
			return amount{}, false, fmt.Errorf("%s=%q is not a number of cpus (%v); write it as \"16\" or \"15500m\"",
				CapacityCPUVar, cpu, err)
		}
		limit.milli = amountOf(cores, 0).milli
	}
	if memory != "" {
		bytes, err := quantity.Memory(memory)
		if err != nil {
			return amount{}, false, fmt.Errorf("%s=%q is not a quantity of memory (%v); write it as \"64Gi\" (Gi, not GB)",
				CapacityMemoryVar, memory, err)
		}
		limit.bytes = int64(bytes)
	}
	return limit, cpu != "" || memory != "", nil
}

// Capacity is this Orker's declared capacity and what stands reserved
// against it. Every field below mu is guarded by it.
type Capacity struct {
	limit   amount // what this Orker admits within; a 0 dimension is unbounded
	ceiling amount // the per-task cap: what each poll on the pool channel reserves
	slots   int    // max_concurrent: how many polls and pool-channel tasks at once

	mu       sync.Mutex
	issued   int    // permits outstanding: polling, or holding a pool-channel task
	reserved amount // everything below, summed
	// unclaimed counts permits a task was delivered on whose activity has not
	// yet been admitted, and claimed those admitted whose permit the SDK has
	// not yet released. Every permit carries the same ceiling, so a release
	// need not know WHICH permit it is, only how many of each state remain.
	unclaimed int
	claimed   int
	unremoved map[ContainerID]amount // containers whose removal has not yet succeeded
	changed   chan struct{}
	waiting   int // polls parked in reserve, for a test to know one waits
}

func newCapacity(limit, ceiling amount, slots int) *Capacity {
	return &Capacity{
		limit: limit, ceiling: ceiling, slots: slots,
		unremoved: map[ContainerID]amount{}, changed: make(chan struct{}),
	}
}

// broadcast wakes every waiting poll. Locked.
func (c *Capacity) broadcast() {
	close(c.changed)
	c.changed = make(chan struct{})
}

// grantable reports whether one more poll may be granted now. Locked.
func (c *Capacity) grantable() bool {
	return c.issued < c.slots && c.reserved.add(c.ceiling).within(c.limit)
}

// capacityPermit is a permit's own state: whether the SDK delivered a task on
// it.
type capacityPermit struct{ used bool }

// grant issues a permit and reserves its ceiling. Locked.
func (c *Capacity) grant() *worker.SlotPermit {
	c.issued++
	c.reserved = c.reserved.add(c.ceiling)
	return &worker.SlotPermit{UserData: &capacityPermit{}}
}

// errRefusedWhileWaiting is reserve's answer when a poll became grantable
// while refuse said no: the caller waits on what refused it, then asks again.
var errRefusedWhileWaiting = errors.New("the poll was refused while it waited for capacity")

// reserve blocks until a poll fits, then reserves the ceiling for it. refuse,
// when set, is asked at the moment a poll would be granted: a poll that waited
// for capacity must not be granted past a cordon set while it waited.
func (c *Capacity) reserve(ctx context.Context, refuse func() bool) (*worker.SlotPermit, error) {
	for {
		c.mu.Lock()
		if c.grantable() {
			if refuse != nil && refuse() {
				c.mu.Unlock()
				return nil, errRefusedWhileWaiting
			}
			permit := c.grant()
			c.mu.Unlock()
			return permit, nil
		}
		changed := c.changed
		c.waiting++
		c.mu.Unlock()
		select {
		case <-changed:
		case <-ctx.Done():
		}
		c.mu.Lock()
		c.waiting--
		c.mu.Unlock()
		if err := ctx.Err(); err != nil {
			return nil, err
		}
	}
}

func (c *Capacity) tryReserve() *worker.SlotPermit {
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.grantable() {
		return nil
	}
	return c.grant()
}

func (c *Capacity) markUsed(permit *worker.SlotPermit) {
	p, ok := permit.UserData.(*capacityPermit)
	if !ok {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	p.used = true
	c.unclaimed++
}

// release ends a permit. A poll that came back empty, and a task the SDK
// delivered but never handed to an activity (an unregistered type, a decode
// failure), still hold the ceiling, which is freed now; an admitted task's
// reservation belongs to its Admission and is freed when its container is.
func (c *Capacity) release(permit *worker.SlotPermit) {
	p, ok := permit.UserData.(*capacityPermit)
	if !ok {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.issued--
	switch {
	case !p.used:
		c.reserved = c.reserved.sub(c.ceiling)
	case c.claimed > 0:
		c.claimed--
	default:
		c.unclaimed--
		c.reserved = c.reserved.sub(c.ceiling)
	}
	c.broadcast()
}

// admit trims a delivered task's reservation to what it really holds.
//
// polled says the activity came from the pool's channel, so it holds a
// permit's ceiling to trim; anything else (a pinned member, taken from its
// pin's channel) holds no permit, and its request is added ungated. A trim
// that would grow the reservation past what is free fails
// RESOURCE_ADMISSION_MISMATCH, and the attempt never runs.
func (c *Capacity) admit(polled bool, request amount) (*Admission, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if !polled || c.unclaimed == 0 {
		// Unclaimed is zero only for an activity that holds no permit of
		// this ledger; counting it is the conservative reading.
		c.reserved = c.reserved.add(request)
		c.broadcast()
		return &Admission{c: c, held: request}, nil
	}
	c.unclaimed--
	c.claimed++
	next := c.reserved.sub(c.ceiling).add(request)
	if request.exceeds(c.ceiling) && !next.within(c.limit) {
		c.reserved = c.reserved.sub(c.ceiling)
		c.broadcast()
		return nil, Fail(ReasonResourceAdmissionMismatch,
			"the task's resolved request (%s) is larger than the %s it was polled under, and this Orker "+
				"has %s free beside its other reservations; it is refused rather than run over capacity",
			request, c.ceiling, c.limit.sub(c.reserved))
	}
	c.reserved = next
	c.broadcast()
	return &Admission{c: c, held: request}, nil
}

// seed reserves the ceiling for a container this Orker could not remove at
// boot: it was admitted within the cap by this Orker's previous life, and it
// still holds the host until a later removal succeeds.
func (c *Capacity) seed(id ContainerID) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, seen := c.unremoved[id]; seen {
		return
	}
	c.unremoved[id] = c.ceiling
	c.reserved = c.reserved.add(c.ceiling)
}

// reap tries again to remove every container whose removal has not yet
// succeeded, releasing each one's reservation once it is gone, and returns
// each removal that failed again, so the caller can say which container still
// holds what.
func (c *Capacity) reap(ctx context.Context, remove func(context.Context, ContainerID) error) (failed []error) {
	c.mu.Lock()
	pending := make([]ContainerID, 0, len(c.unremoved))
	for id := range c.unremoved {
		pending = append(pending, id)
	}
	c.mu.Unlock()
	for _, id := range pending {
		if err := remove(ctx, id); err != nil {
			failed = append(failed, fmt.Errorf("container %s: %w", id, err))
			continue
		}
		c.mu.Lock()
		if held, ok := c.unremoved[id]; ok {
			delete(c.unremoved, id)
			c.reserved = c.reserved.sub(held)
			c.broadcast()
		}
		c.mu.Unlock()
	}
	return failed
}

// pendingRemovals is how many containers still hold a reservation without an
// attempt.
func (c *Capacity) pendingRemovals() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.unremoved)
}

// snapshot is what DescribeOrker reports.
func (c *Capacity) snapshot() (limit, reserved amount) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.limit, c.reserved
}

// Admission is one admitted activity's reservation. Its methods are safe on a
// nil Admission, which is what an activity run outside a capacity-declaring
// Orker (orkctl activity test, orkctl task run) has.
type Admission struct {
	c         *Capacity
	held      amount
	container ContainerID
	done      bool // the reservation has left the attempt: freed, or handed to unremoved
}

// ContainerCreated records the container whose removal frees the reservation.
func (a *Admission) ContainerCreated(id ContainerID) {
	if a == nil {
		return
	}
	a.c.mu.Lock()
	defer a.c.mu.Unlock()
	a.container = id
}

// ContainerRemoved frees the reservation once its container is gone; a
// removal that failed keeps it, under the container, until a later reap
// removes it.
func (a *Admission) ContainerRemoved(err error) {
	if a == nil {
		return
	}
	a.c.mu.Lock()
	defer a.c.mu.Unlock()
	if a.done {
		return
	}
	if err != nil && a.container != "" {
		a.keep()
		return
	}
	a.free()
}

// Done ends the activity. An attempt that never created a container (a
// refusal, a cache hit, an adopted commit, DescribeOrker) frees its
// reservation now; one whose container was never reported removed keeps it
// until a reap removes the container.
func (a *Admission) Done() {
	if a == nil {
		return
	}
	a.c.mu.Lock()
	defer a.c.mu.Unlock()
	if a.done {
		return
	}
	if a.container != "" {
		a.keep()
		return
	}
	a.free()
}

// keep hands the reservation to the container's pending removal. Locked.
func (a *Admission) keep() {
	a.done = true
	a.c.unremoved[a.container] = a.held
}

// free returns the reservation. Locked.
func (a *Admission) free() {
	a.done = true
	a.c.reserved = a.c.reserved.sub(a.held)
	a.c.broadcast()
}

type admissionKey struct{}

func withAdmission(ctx context.Context, a *Admission) context.Context {
	return context.WithValue(ctx, admissionKey{}, a)
}

// admissionFrom is the activity's Admission, or nil.
func admissionFrom(ctx context.Context) *Admission {
	a, _ := ctx.Value(admissionKey{}).(*Admission)
	return a
}

// capacitySlotSupplier is the task worker's activity supplier when this Orker
// declares a capacity: max_concurrent slots, each granted only while the
// per-task cap fits beside what is reserved (Capacity). It replaces the fixed
// supplier, inside the cordon's wrapper, so a cordon still refuses first; and
// because a poll can wait here for capacity long after it passed that wrapper,
// the cordon is asked again at the moment a poll would be granted, and a
// cordoned Orker's poll goes back to waiting on the cordon.
type capacitySlotSupplier struct {
	c      *Capacity
	cordon *Cordon
}

var _ worker.SlotSupplier = (*capacitySlotSupplier)(nil)

func (s *capacitySlotSupplier) cordoned() bool {
	if s.cordon == nil {
		return false
	}
	cordoned, _, _ := s.cordon.State()
	return cordoned
}

func (s *capacitySlotSupplier) ReserveSlot(ctx context.Context, _ worker.SlotReservationInfo) (*worker.SlotPermit, error) {
	for {
		if s.cordon != nil {
			if err := s.cordon.Wait(ctx); err != nil {
				return nil, err
			}
		}
		permit, err := s.c.reserve(ctx, s.cordoned)
		if errors.Is(err, errRefusedWhileWaiting) {
			continue
		}
		return permit, err
	}
}

func (s *capacitySlotSupplier) TryReserveSlot(worker.SlotReservationInfo) *worker.SlotPermit {
	if s.cordoned() {
		return nil
	}
	return s.c.tryReserve()
}

func (s *capacitySlotSupplier) MarkSlotUsed(info worker.SlotMarkUsedInfo) {
	s.c.markUsed(info.Permit())
}
func (s *capacitySlotSupplier) ReleaseSlot(info worker.SlotReleaseInfo) { s.c.release(info.Permit()) }
func (s *capacitySlotSupplier) MaxSlots() int                           { return s.c.slots }
