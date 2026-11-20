package sourcehost

import (
	"time"

	"github.com/orkstack/orkstack/pkg/artifacts"
)

// The wait's numbers (01 §5).
const (
	backoffFirst    = time.Second
	backoffMax      = 30 * time.Second
	cycleDelayFirst = 30 * time.Second
	cycleDelayMax   = 30 * time.Minute
)

// WaitStep is what the instance does next while it waits.
type WaitStep int

const (
	// StepNone: nothing is due yet.
	StepNone WaitStep = iota
	// StepRetryOldest: during the park, try the oldest message's failed
	// call again.
	StepRetryOldest
	// StepReleaseAll: the park ran out; release every message in hand with
	// a 30 s delay, once.
	StepReleaseAll
	// StepProbe: a round of probes without a message.
	StepProbe
	// StepTryOne: a round passed; one lane writes one poll.
	StepTryOne
)

type waitState int

const (
	waitRunning waitState = iota
	waitParked
	waitProbing
	waitTrying
)

// Waiter is one instance's wait (01 §5): park, release, probe without a
// message, try one, and the lanes added back one at a time. It is driven by
// the instance's loop on the instance's clock and is not safe for
// concurrent use.
type Waiter struct {
	park        time.Duration
	concurrency int

	state      waitState
	began      time.Time
	next       time.Time
	backoff    time.Duration
	cycleDelay time.Duration
	lanes      int
}

// NewWaiter is a waiter for an instance of concurrency lanes that parks
// for at most park.
func NewWaiter(park time.Duration, concurrency int) *Waiter {
	return &Waiter{park: park, concurrency: concurrency, cycleDelay: cycleDelayFirst, lanes: concurrency}
}

// Waiting reports whether the instance is waiting.
func (w *Waiter) Waiting() bool { return w.state != waitRunning }

// Lanes is how many lanes may poll now: none while parked or probing, one
// while trying, and then one more per accepted message up to concurrency.
func (w *Waiter) Lanes() int {
	switch w.state {
	case waitParked, waitProbing:
		return 0
	case waitTrying:
		return 1
	}
	return w.lanes
}

// CycleDelay is the delay a recurrence releases its message with.
func (w *Waiter) CycleDelay() time.Duration { return w.cycleDelay }

// Begin starts a wait at now: the instance stops polling on every lane and
// parks.
func (w *Waiter) Begin(now time.Time) {
	if w.state != waitRunning {
		return
	}
	w.state, w.began, w.backoff = waitParked, now, backoffFirst
	w.next = now.Add(w.backoff)
}

// Due says what is due at now.
func (w *Waiter) Due(now time.Time) WaitStep {
	switch w.state {
	case waitParked:
		if now.Sub(w.began) >= w.park {
			return StepReleaseAll
		}
		if !now.Before(w.next) {
			return StepRetryOldest
		}
	case waitProbing:
		if !now.Before(w.next) {
			return StepProbe
		}
	}
	return StepNone
}

// Retried is the outcome of a retry during the park: a success ends the
// wait at once, and the messages in hand are judged again.
func (w *Waiter) Retried(ok bool, now time.Time) {
	if ok {
		w.end()
		return
	}
	w.backoff = min(2*w.backoff, backoffMax)
	w.next = now.Add(w.backoff)
}

// Released is the release of everything in hand: the instance holds
// nothing and probes.
func (w *Waiter) Released(now time.Time) {
	w.state, w.backoff = waitProbing, backoffFirst
	w.next = now
}

// Probed is a probe round: one that passed lets one lane try one poll.
func (w *Waiter) Probed(ok bool, now time.Time) WaitStep {
	if ok {
		w.state = waitTrying
		return StepTryOne
	}
	w.backoff = min(2*w.backoff, backoffMax)
	w.next = now.Add(w.backoff)
	return StepNone
}

// Tried is the one lane's message after a passing probe. A wait again is a
// recurrence: the message is released with the cycle delay it returns, the
// instance probes again, and the cycle delay doubles. Anything else ends
// the wait. A quiet poll changes nothing.
func (w *Waiter) Tried(d *Decision, now time.Time) (releaseDelay time.Duration, recurrence bool) {
	if d == nil {
		return 0, false
	}
	if d.Disposition == Wait {
		if w.state == waitParked {
			// Another message held while parked: the park goes on.
			return 0, false
		}
		delay := w.cycleDelay
		w.cycleDelay = min(2*w.cycleDelay, cycleDelayMax)
		w.state, w.backoff, w.next = waitProbing, backoffFirst, now
		return delay, true
	}
	w.state, w.lanes = waitRunning, 1
	if d.Disposition == Settle && (d.Receipt.Outcome == artifacts.ReceiptAccepted || d.Receipt.Outcome == artifacts.ReceiptDuplicate) {
		w.Accepted()
	}
	return 0, false
}

// Accepted is a message settled ACCEPTED or DUPLICATE: the cycle delay goes
// back to 30 s and, after a wait, one more lane may poll.
func (w *Waiter) Accepted() {
	w.cycleDelay = cycleDelayFirst
	if w.state == waitRunning && w.lanes < w.concurrency {
		w.lanes++
	}
}

func (w *Waiter) end() {
	w.state = waitRunning
}
