package engine

import (
	"sync"

	"google.golang.org/protobuf/proto"

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
// and replays). Subscribers receive the backlog followed by live events.
type EventBus struct {
	mu     sync.Mutex
	log    map[string][]*orkstackv1.Event
	seen   map[string]map[string]int64
	subs   map[string]map[int64]*subscription
	nextID int64
}

// NewEventBus returns an empty bus.
func NewEventBus() *EventBus {
	return &EventBus{
		log:  map[string][]*orkstackv1.Event{},
		seen: map[string]map[string]int64{},
		subs: map[string]map[int64]*subscription{},
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
// followed by live events, a cancel function, and whether fromSequence has
// already reached a terminal run event. Registration, terminal detection, and
// backlog capture happen under one lock so no event can slip between them.
func (b *EventBus) Subscribe(runID string, fromSequence int64) (<-chan *orkstackv1.Event, func(), bool) {
	b.mu.Lock()
	backlog := b.snapshotLocked(runID, fromSequence)
	terminalAlreadyReached := false
	for _, event := range b.log[runID] {
		if isTerminalRunEvent(event) && event.GetSequence() <= fromSequence {
			terminalAlreadyReached = true
			break
		}
	}
	sub := newSubscription(backlog, fromSequence)
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
	return sub.out, cancel, terminalAlreadyReached
}

func isTerminalRunEvent(event *orkstackv1.Event) bool {
	switch event.GetType() {
	case orkstackv1.EventType_EVENT_TYPE_RUN_SUCCEEDED,
		orkstackv1.EventType_EVENT_TYPE_RUN_FAILED,
		orkstackv1.EventType_EVENT_TYPE_RUN_CANCELED:
		return true
	default:
		return false
	}
}

// subscription buffers events for one subscriber without blocking publishers.
type subscription struct {
	out          chan *orkstackv1.Event
	mu           sync.Mutex
	queue        []*orkstackv1.Event
	wake         chan struct{}
	done         chan struct{}
	closed       bool
	fromSequence int64
}

func newSubscription(backlog []*orkstackv1.Event, fromSequence int64) *subscription {
	sub := &subscription{
		out:          make(chan *orkstackv1.Event),
		queue:        backlog,
		wake:         make(chan struct{}, 1),
		done:         make(chan struct{}),
		fromSequence: fromSequence,
	}
	go sub.pump()
	if len(backlog) > 0 {
		sub.signal()
	}
	return sub
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
	s.queue = append(s.queue, proto.Clone(event).(*orkstackv1.Event))
	s.mu.Unlock()
	s.signal()
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
		s.mu.Unlock()

		if next == nil {
			if closed {
				return
			}
			select {
			case <-s.wake:
			case <-s.done:
				// Drain whatever is already queued, then stop.
				s.mu.Lock()
				remaining := len(s.queue)
				s.mu.Unlock()
				if remaining == 0 {
					return
				}
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
