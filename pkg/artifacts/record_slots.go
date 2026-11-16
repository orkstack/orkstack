package artifacts

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"
)

// SlotClass is which of a replica's record requests a slot serves, in the
// order a freed slot is given (docs/DEPLOY.md, "Reading identity records").
type SlotClass int

const (
	// SlotRun: a run's own reads and writes, which a run waits on: a child's
	// end, a collect, a deadline's read, a mint, a commit.
	SlotRun SlotClass = iota + 1
	// SlotDoor: a resolution's door and a telling's reads.
	SlotDoor
	// SlotList: a listing of recorded results, capped below the whole.
	SlotList
)

// The replica's capacity for identity record requests: one semaphore per
// process, in memory, never stored and never a setting. It decides no
// command except through a read's recorded result.
const (
	RecordSlotCapacity = 256
	RecordSlotListCap  = 16
	// RecordSlotDoorReserve is the slots a run's requests never take, so a
	// resolution's door is answered however many run reads wait.
	RecordSlotDoorReserve = 16
)

// replicaRecordSlots is the replica's one semaphore of record requests.
var replicaRecordSlots = newRecordSlotsReserving(RecordSlotCapacity, RecordSlotListCap, RecordSlotDoorReserve)

// recordSlots gives at most capacity slots at a time, at most listCap of
// them to SlotList, and a freed slot to the waiting request of the first
// class, first come within a class.
type recordSlots struct {
	mu      sync.Mutex
	free    int
	listCap int
	listing int
	runCap  int
	running int
	queues  [3][]*slotWaiter
	// queued, when set, is told of each request that starts to wait (tests).
	queued chan struct{}
}

type slotWaiter struct {
	class   SlotClass
	granted chan struct{}
}

func newRecordSlots(capacity, listCap int) *recordSlots {
	return newRecordSlotsReserving(capacity, listCap, 0)
}

// newRecordSlotsReserving keeps doorReserve of capacity from a run's
// requests.
func newRecordSlotsReserving(capacity, listCap, doorReserve int) *recordSlots {
	return &recordSlots{free: capacity, listCap: listCap, runCap: capacity - doorReserve}
}

// acquire waits for a slot of class until ctx ends.
func (s *recordSlots) acquire(ctx context.Context, class SlotClass) (func(), error) {
	return s.acquireWithin(ctx, class, 0)
}

// acquireWithin waits for a slot of class until margin before ctx's
// deadline. No slot by then, or a deadline already passed, is busy
// (*RecordStoreUnanswered); a cancellation is returned as it is. A strict
// priority: a stream of a first class's requests holds the others back
// until their own deadlines, and a listing is guaranteed nothing beyond
// its cap. The release it returns frees the slot once.
func (s *recordSlots) acquireWithin(ctx context.Context, class SlotClass, margin time.Duration) (func(), error) {
	if class < SlotRun || class > SlotList {
		return nil, fmt.Errorf("artifacts: record slot class %d is none", class)
	}
	if err := ctx.Err(); err != nil {
		if errors.Is(err, context.DeadlineExceeded) {
			return nil, &RecordStoreUnanswered{Class: UnansweredBusy, Err: err}
		}
		return nil, err
	}
	s.mu.Lock()
	if s.eligible(class) && s.noneAhead(class) {
		s.take(class)
		s.mu.Unlock()
		return s.releaser(class), nil
	}
	w := &slotWaiter{class: class, granted: make(chan struct{})}
	s.queues[class-1] = append(s.queues[class-1], w)
	s.mu.Unlock()
	if s.queued != nil {
		s.queued <- struct{}{}
	}
	wait := ctx
	if deadline, ok := ctx.Deadline(); ok {
		var cancel context.CancelFunc
		wait, cancel = context.WithDeadline(ctx, deadline.Add(-margin))
		defer cancel()
	}
	select {
	case <-w.granted:
		return s.releaser(class), nil
	case <-wait.Done():
	}
	s.mu.Lock()
	if s.dequeue(w) {
		s.mu.Unlock()
		if err := ctx.Err(); err != nil && !errors.Is(err, context.DeadlineExceeded) {
			return nil, err
		}
		return nil, &RecordStoreUnanswered{Class: UnansweredBusy, Err: errors.New("no record slot within the attempt's time")}
	}
	s.mu.Unlock()
	// Granted as the wait ended: give the slot back.
	<-w.granted
	s.releaser(class)()
	if err := ctx.Err(); err != nil && !errors.Is(err, context.DeadlineExceeded) {
		return nil, err
	}
	return nil, &RecordStoreUnanswered{Class: UnansweredBusy, Err: errors.New("no record slot within the attempt's time")}
}

func (s *recordSlots) eligible(class SlotClass) bool {
	switch {
	case s.free <= 0:
		return false
	case class == SlotList:
		return s.listing < s.listCap
	case class == SlotRun:
		return s.running < s.runCap
	}
	return true
}

// noneAhead says no request of class or a class before it waits.
func (s *recordSlots) noneAhead(class SlotClass) bool {
	for c := SlotRun; c <= class; c++ {
		if len(s.queues[c-1]) > 0 {
			return false
		}
	}
	return true
}

func (s *recordSlots) take(class SlotClass) {
	s.free--
	switch class {
	case SlotList:
		s.listing++
	case SlotRun:
		s.running++
	}
}

func (s *recordSlots) releaser(class SlotClass) func() {
	var once sync.Once
	return func() {
		once.Do(func() {
			s.mu.Lock()
			defer s.mu.Unlock()
			s.free++
			switch class {
			case SlotList:
				s.listing--
			case SlotRun:
				s.running--
			}
			s.dispatch()
		})
	}
}

// dispatch gives free slots to waiters, the first class first.
func (s *recordSlots) dispatch() {
	for c := SlotRun; c <= SlotList; c++ {
		q := &s.queues[c-1]
		for len(*q) > 0 && s.eligible(c) {
			w := (*q)[0]
			*q = (*q)[1:]
			s.take(c)
			close(w.granted)
		}
	}
}

// dequeue removes w if it still waits; false means it was granted.
func (s *recordSlots) dequeue(w *slotWaiter) bool {
	q := &s.queues[w.class-1]
	for i, x := range *q {
		if x == w {
			*q = append((*q)[:i], (*q)[i+1:]...)
			return true
		}
	}
	return false
}
