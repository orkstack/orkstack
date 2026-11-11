package engine

import (
	"fmt"
	"sync"
	"time"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
)

// EventBus is the in-process pub/sub for run events.
//
// Sequence belongs to the *producer*. RunWorkflow numbers every event it
// emits, so the copy Temporal records when the projection activity is
// scheduled carries the same number the bus stores; that is what makes the
// history-derived stream, this log and the archived stream identical. The bus
// only assigns a number when it is given none, which covers the one stream
// that never reaches Temporal — a run whose start failed — and unit-test
// publishers.
//
// Publishers also supply a dedupe key (stable across Temporal activity retries
// and replays). Subscribers receive the backlog followed by live events, in
// sequence order and without a gap: publishes can land out of order (a
// fan-out's item publishes and a plain task's start events are not waited
// for, so two can be in flight at once), and a subscription holds an event
// until every number before it has arrived (subscription.admitLocked).
type EventBus struct {
	mu        sync.Mutex
	log       map[string][]*orkstackv1.Event
	seen      map[string]map[string]int64
	subs      map[string]map[int64]*subscription
	nextID    int64
	retention terminalRetention
	// orderHold is how long a subscription waits for a missing number
	// before it ends the stream with an error (busOrderHold).
	orderHold time.Duration
}

// busOrderHold bounds how long a live subscription holds events behind a
// number that has not arrived: one projection activity's whole retry window
// (three attempts of eventActivityTimeout and their backoff) fits in it, so a
// publish still in flight arrives first. Past it the stream ends with an
// error naming the missing number, never a silent stall.
const busOrderHold = time.Minute

// NewEventBus returns an empty bus that keeps every run's log.
func NewEventBus() *EventBus {
	return &EventBus{
		log:       map[string][]*orkstackv1.Event{},
		seen:      map[string]map[string]int64{},
		subs:      map[string]map[int64]*subscription{},
		retention: newTerminalRetention(),
		orderHold: busOrderHold,
	}
}

// WithTerminalRetention makes the bus forget a run's log and dedupe keys d
// after its terminal event was first published, on a later Publish
// (terminalRetention). d must outlast the projection activity's whole retry
// window, so a retried terminal publish is still deduplicated. A run with a
// live subscriber is never forgotten. Zero keeps every log. It returns the
// same bus so it can be chained onto NewEventBus.
func (b *EventBus) WithTerminalRetention(d time.Duration) *EventBus {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.retention.ttl = d
	return b
}

// sweepLocked forgets every finished run that is due and has no subscriber.
func (b *EventBus) sweepLocked() {
	for _, runID := range b.retention.due(func(runID string) bool { return len(b.subs[runID]) > 0 }) {
		delete(b.log, runID)
		delete(b.seen, runID)
		delete(b.subs, runID)
	}
}

// Publish appends event to its run's log and fans it out to subscribers. The
// returned event carries the sequence it was stored under: the producer's when
// it set one, otherwise the next for this run. dedupeKey makes publication
// idempotent: republishing a key returns the already-stored event and false.
func (b *EventBus) Publish(event *orkstackv1.Event, dedupeKey string) (*orkstackv1.Event, bool) {
	if event == nil || event.GetRunId() == "" {
		return nil, false
	}
	b.mu.Lock()
	b.sweepLocked()
	runID := event.GetRunId()
	if dedupeKey != "" {
		if seq, dup := b.seen[runID][dedupeKey]; dup {
			for _, existing := range b.log[runID] {
				if existing.GetSequence() == seq {
					out := proto.Clone(existing).(*orkstackv1.Event)
					b.mu.Unlock()
					return out, false
				}
			}
		}
	}

	stored := proto.Clone(event).(*orkstackv1.Event)
	if stored.GetSequence() == 0 {
		stored.Sequence = b.nextSequenceLocked(runID)
	}
	b.insertLocked(runID, stored)
	if dedupeKey != "" {
		if b.seen[runID] == nil {
			b.seen[runID] = map[string]int64{}
		}
		b.seen[runID][dedupeKey] = stored.GetSequence()
	}
	if isTerminalRunEvent(stored) {
		b.retention.mark(runID)
	}
	// enqueue only appends to each subscriber's private queue. Holding b.mu
	// through enqueue preserves the same order for sequence assignment, the
	// stored log, and every live subscriber under concurrent publication.
	for _, sub := range b.subs[runID] {
		sub.enqueue(stored)
	}
	b.mu.Unlock()
	return proto.Clone(stored).(*orkstackv1.Event), true
}

// nextSequenceLocked is one past the highest sequence this run holds, so a bus
// that has already stored producer-numbered events keeps numbering above them
// instead of colliding with one.
func (b *EventBus) nextSequenceLocked(runID string) int64 {
	log := b.log[runID]
	if len(log) == 0 {
		return 1
	}
	return log[len(log)-1].GetSequence() + 1
}

// insertLocked keeps a run's log ordered by sequence.
//
// Publication order is not sequence order once more than one engine replica
// runs the projection worker: the activities of one run are handed to whichever
// replica polls first, so a process sees an arbitrary subset in an arbitrary
// interleaving. The log it keeps — which is what an archive write and a
// backlog replay read — is defined by the producer's numbering, not by arrival.
func (b *EventBus) insertLocked(runID string, stored *orkstackv1.Event) {
	log := b.log[runID]
	at := len(log)
	for at > 0 && log[at-1].GetSequence() > stored.GetSequence() {
		at--
	}
	log = append(log, nil)
	copy(log[at+1:], log[at:])
	log[at] = stored
	b.log[runID] = log
}

// Discard forgets a run's event log and dedupe state.
//
// Same case as RunStore.Delete: the events of a candidate run id Temporal
// refused. That id was never returned to anyone, so no subscriber can hold it
// and there is nothing to close — only a log to forget, so a run that never
// existed cannot be streamed or archived.
func (b *EventBus) Discard(runID string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	delete(b.log, runID)
	delete(b.seen, runID)
	b.retention.unmark(runID)
}

// Events returns the recorded events for a run with sequence greater than
// fromSequence (0 returns the whole log).
func (b *EventBus) Events(runID string, fromSequence int64) []*orkstackv1.Event {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.snapshotLocked(runID, fromSequence)
}

func (b *EventBus) snapshotLocked(runID string, fromSequence int64) []*orkstackv1.Event {
	out := make([]*orkstackv1.Event, 0, len(b.log[runID]))
	for _, event := range b.log[runID] {
		if event.GetSequence() > fromSequence {
			out = append(out, proto.Clone(event).(*orkstackv1.Event))
		}
	}
	return out
}

// Subscribe returns a channel delivering the backlog after fromSequence
// followed by live events, in sequence order and without a gap; a cancel
// function; whether fromSequence has already reached a terminal run event;
// and the stream's error, read once the channel has closed: nil for a stream
// that ended (its terminal event, or the cancel), or the error naming the
// number a subscription waited for longer than the bus's order hold.
// Registration, terminal detection, and backlog capture happen under one lock
// so no event can slip between them.
func (b *EventBus) Subscribe(runID string, fromSequence int64) (<-chan *orkstackv1.Event, func(), bool, func() error) {
	b.mu.Lock()
	backlog := b.snapshotLocked(runID, fromSequence)
	terminalAlreadyReached := false
	for _, event := range b.log[runID] {
		if isTerminalRunEvent(event) && event.GetSequence() <= fromSequence {
			terminalAlreadyReached = true
			break
		}
	}
	sub := newSubscription(runID, backlog, fromSequence, b.orderHold)
	id := b.nextID
	b.nextID++
	if b.subs[runID] == nil {
		b.subs[runID] = map[int64]*subscription{}
	}
	b.subs[runID][id] = sub
	b.mu.Unlock()

	cancel := func() {
		b.mu.Lock()
		delete(b.subs[runID], id)
		b.mu.Unlock()
		sub.close()
	}
	return sub.out, cancel, terminalAlreadyReached, sub.streamErr
}

func isTerminalRunEvent(event *orkstackv1.Event) bool {
	return isTerminalRunEventType(event.GetType())
}

// isTerminalRunEventType is isTerminalRunEvent for an event not built yet.
func isTerminalRunEventType(eventType orkstackv1.EventType) bool {
	switch eventType {
	case orkstackv1.EventType_EVENT_TYPE_RUN_SUCCEEDED,
		orkstackv1.EventType_EVENT_TYPE_RUN_FAILED,
		orkstackv1.EventType_EVENT_TYPE_RUN_CANCELED:
		return true
	default:
		return false
	}
}

// isRunLevelEvent reports whether an event belongs to the run's own
// lifecycle rather than to one task within it. This must be decided from the
// event's TYPE, not from GetTaskId() being empty: RUN_FAILED is a run-level
// event that still carries a task id whenever a task caused the failure —
// runState.fail (workflow.go) passes the failing task's id through to emit
// for diagnostics — so a task-caused run failure is a run-level event too.
func isRunLevelEvent(event *orkstackv1.Event) bool {
	switch event.GetType() {
	case orkstackv1.EventType_EVENT_TYPE_RUN_CREATED,
		orkstackv1.EventType_EVENT_TYPE_RUN_STARTED,
		orkstackv1.EventType_EVENT_TYPE_RUN_SUCCEEDED,
		orkstackv1.EventType_EVENT_TYPE_RUN_FAILED,
		orkstackv1.EventType_EVENT_TYPE_RUN_CANCELED:
		return true
	default:
		return false
	}
}

// subscription buffers events for one subscriber without blocking publishers,
// and hands them over in sequence order without a gap.
//
// next is the number it hands over next, and held every event that arrived
// before a number below it: a run's detached publishes can complete in any
// order. An event is queued once every number before it has arrived. A run
// publishes its terminal event only after every detached publish of it has
// completed (drainItemPublishes), so a number still missing when the terminal
// event arrives was never published — a number the run used without an event
// — and is handed over as the STREAM_GAP event the history reader serves for
// it (StreamGapEvent), then the rest. A gap that stands for hold ends the
// stream with an error naming the missing number (streamErr).
type subscription struct {
	out          chan *orkstackv1.Event
	mu           sync.Mutex
	runID        string
	queue        []*orkstackv1.Event
	held         map[int64]*orkstackv1.Event
	next         int64
	hold         time.Duration
	holdSince    time.Time
	err          error
	wake         chan struct{}
	done         chan struct{}
	closed       bool
	fromSequence int64
}

func newSubscription(runID string, backlog []*orkstackv1.Event, fromSequence int64, hold time.Duration) *subscription {
	sub := &subscription{
		out:          make(chan *orkstackv1.Event),
		runID:        runID,
		held:         map[int64]*orkstackv1.Event{},
		next:         fromSequence + 1,
		hold:         hold,
		wake:         make(chan struct{}, 1),
		done:         make(chan struct{}),
		fromSequence: fromSequence,
	}
	sub.mu.Lock()
	for _, event := range backlog {
		sub.admitLocked(event)
	}
	sub.mu.Unlock()
	go sub.pump()
	if len(backlog) > 0 {
		sub.signal()
	}
	return sub
}

// admitLocked takes one event (the subscription's own copy) into the order:
// queued at once when it is the next number, held otherwise, and every held
// event it makes contiguous queued after it. A terminal event releases every
// gap before it as a STREAM_GAP event.
func (s *subscription) admitLocked(event *orkstackv1.Event) {
	seq := event.GetSequence()
	if seq < s.next {
		return // a duplicate of one already handed over
	}
	s.held[seq] = event
	if isTerminalRunEvent(event) {
		for s.next < seq {
			if _, ok := s.held[s.next]; ok {
				s.release()
				continue
			}
			first := s.next
			for s.next < seq {
				if _, ok := s.held[s.next]; ok {
					break
				}
				s.next++
			}
			s.queue = append(s.queue, StreamGapEvent(s.runID, first, s.next-1, s.gapTimestamp()))
		}
	}
	for {
		if _, ok := s.held[s.next]; !ok {
			break
		}
		s.release()
	}
	if len(s.held) == 0 {
		s.holdSince = time.Time{}
	} else if s.holdSince.IsZero() {
		s.holdSince = time.Now()
	}
}

// release queues the held event numbered next and moves next past it.
func (s *subscription) release() {
	s.queue = append(s.queue, s.held[s.next])
	delete(s.held, s.next)
	s.next++
}

// gapTimestamp is when the event after a gap happened: the lowest held one.
func (s *subscription) gapTimestamp() *timestamppb.Timestamp {
	var at *timestamppb.Timestamp
	lowest := int64(-1)
	for seq, event := range s.held {
		if lowest < 0 || seq < lowest {
			lowest, at = seq, event.GetTimestamp()
		}
	}
	return at
}

func (s *subscription) enqueue(event *orkstackv1.Event) {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return
	}
	if event.GetSequence() <= s.fromSequence {
		terminal := isTerminalRunEvent(event)
		s.mu.Unlock()
		if terminal {
			s.close()
		}
		return
	}
	s.admitLocked(proto.Clone(event).(*orkstackv1.Event))
	s.mu.Unlock()
	s.signal()
}

// streamErr is the error the stream ended with, read once out has closed:
// nil, or the missing number's.
func (s *subscription) streamErr() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.err
}

// expireHold ends the stream when its gap has stood for the hold.
func (s *subscription) expireHold() {
	s.mu.Lock()
	if s.closed || len(s.held) == 0 || s.holdSince.IsZero() || time.Since(s.holdSince) < s.hold {
		s.mu.Unlock()
		return
	}
	s.err = fmt.Errorf("event %d of run %s did not reach this process's event bus within %s, while %d later events did; "+
		"the stream ends here and can be resumed from sequence %d", s.next, s.runID, s.hold, len(s.held), s.next-1)
	s.mu.Unlock()
	s.close()
}

func (s *subscription) signal() {
	select {
	case s.wake <- struct{}{}:
	default:
	}
}

func (s *subscription) pump() {
	defer close(s.out)
	for {
		s.mu.Lock()
		var next *orkstackv1.Event
		if len(s.queue) > 0 {
			next = s.queue[0]
			s.queue = s.queue[1:]
		}
		closed := s.closed
		holdSince := s.holdSince
		s.mu.Unlock()

		if next == nil {
			if closed {
				return
			}
			// A standing gap is waited for at most until its hold ends.
			var expiry <-chan time.Time
			var timer *time.Timer
			if !holdSince.IsZero() {
				timer = time.NewTimer(time.Until(holdSince.Add(s.hold)))
				expiry = timer.C
			}
			select {
			case <-s.wake:
			case <-expiry:
				s.expireHold()
			case <-s.done:
				// Drain whatever is already queued, then stop.
				s.mu.Lock()
				remaining := len(s.queue)
				s.mu.Unlock()
				if remaining == 0 {
					if timer != nil {
						timer.Stop()
					}
					return
				}
			}
			if timer != nil {
				timer.Stop()
			}
			continue
		}
		select {
		case s.out <- next:
		case <-s.done:
			return
		}
	}
}

func (s *subscription) close() {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return
	}
	s.closed = true
	s.mu.Unlock()
	close(s.done)
}
