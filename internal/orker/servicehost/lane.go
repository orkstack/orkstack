package servicehost

import (
	"sync"
	"time"
)

// maxLateInARow ends an attempt: three exchanges in a row with a late step
// on one lane (14 §4.4).
const maxLateInARow = 3

// Arrival is how a lane takes a container's step.
type Arrival int

const (
	// ArrivalCurrent is the step the lane waits for.
	ArrivalCurrent Arrival = iota
	// ArrivalLeftover is a step of an earlier exchange: ignored (rule 1).
	ArrivalLeftover
	// ArrivalViolation is a step nobody asked for: a seq ahead of the
	// lane's, or an answer while no exchange is open.
	ArrivalViolation
)

// Lane is one strictly sequential conversation (14 §4.1, §4.4): the Orker
// writes the first step of every exchange, seq rises by one per exchange,
// and a container's late step wedges the lane until it arrives. It is
// driven by the caller's clock; one
// worker drives one lane; the instance's watchdog may read it meanwhile,
// so every method holds mu.
type Lane struct {
	mu           sync.Mutex
	seq          int64
	open         bool
	awaiting     bool
	deadline     time.Time
	allowed      time.Duration
	lateCounted  bool
	lateThisOne  bool
	lateInARow   int
	wedgedSince  time.Time
	lastDeadline time.Duration
}

// Seq is the current exchange's seq, 0 before the first.
func (l *Lane) Seq() int64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.seq
}

// Wedged reports whether the lane waits for a step past its deadline, and
// since when.
func (l *Lane) Wedged() (bool, time.Time) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return !l.wedgedSince.IsZero(), l.wedgedSince
}

// wedgedFor is Wedged and the deadline the late step missed.
func (l *Lane) wedgedFor() (bool, time.Time, time.Duration) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return !l.wedgedSince.IsZero(), l.wedgedSince, l.lastDeadline
}

// LateInARow is how many exchanges in a row had a late step.
func (l *Lane) LateInARow() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.lateInARow
}

// CanBegin reports whether the Orker may start an exchange: none is open,
// so none is wedged.
func (l *Lane) CanBegin() bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	return !l.open
}

// Begin opens the next exchange and returns its seq. It is false while an
// exchange is open: the Orker never moves to the next seq while an answer
// to this one could still appear.
func (l *Lane) Begin() (int64, bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.open {
		return 0, false
	}
	l.seq++
	l.open, l.lateThisOne = true, false
	return l.seq, true
}

// Await records that the Orker wrote a step and expects the container's
// within allowed of now.
func (l *Lane) Await(now time.Time, allowed time.Duration) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.awaiting, l.allowed, l.deadline, l.lateCounted = true, allowed, now.Add(allowed), false
}

// Tick counts a miss when the deadline passes (not when the step arrives),
// wedging the lane. It reports whether this lane alone now ends the
// attempt.
func (l *Lane) Tick(now time.Time) (endsAttempt bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.awaiting && !l.lateCounted && now.After(l.deadline) {
		l.lateCounted, l.lateThisOne = true, true
		l.wedgedSince, l.lastDeadline = l.deadline, l.allowed
		l.lateInARow++
	}
	return l.lateInARow >= maxLateInARow
}

// Arrived takes a container's step of seq. A step of the current exchange
// unwedges the lane; whether it was late stays counted.
func (l *Lane) Arrived(seq int64) Arrival {
	l.mu.Lock()
	defer l.mu.Unlock()
	switch {
	case seq < l.seq:
		return ArrivalLeftover
	case seq > l.seq || !l.open || !l.awaiting:
		return ArrivalViolation
	}
	l.awaiting, l.wedgedSince = false, time.Time{}
	return ArrivalCurrent
}

// Close ends the exchange at its closing step (rule 5): later files of this
// seq are ignored. An exchange whose every step was on time breaks a run
// of late ones.
func (l *Lane) Close() {
	l.mu.Lock()
	defer l.mu.Unlock()
	if !l.lateThisOne {
		l.lateInARow = 0
	}
	l.open, l.awaiting, l.wedgedSince = false, false, time.Time{}
}

// Unresponsive is the watchdog's judgement over every lane of a service
// (14 §4.4): more than a quarter of the lanes, and at least one, wedged for
// longer than twice the deadline each missed; or any lane wedged for longer
// than longestHold. One stopped lane of four cannot hide behind three
// healthy ones.
func Unresponsive(lanes []*Lane, now time.Time, longestHold time.Duration) bool {
	stuck := 0
	for _, l := range lanes {
		wedged, since, deadline := l.wedgedFor()
		if !wedged {
			continue
		}
		if now.Sub(since) > longestHold {
			return true
		}
		if now.Sub(since) > 2*deadline {
			stuck++
		}
	}
	return stuck >= 1 && stuck*4 > len(lanes)
}
