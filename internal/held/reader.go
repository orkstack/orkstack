package held

import (
	"container/list"
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"golang.org/x/time/rate"

	"github.com/orkstack/orkstack/internal/authz"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// How old a read may be, and what the reads may cost.
const (
	// describeAge is how long a kept describe may confirm a call of the
	// door's; the mint and an attempt's first confirmation never use one. A hundred busy sources on three
	// replicas cost up to 300 describes a second at one second, an eighth of
	// the 2,400 requests a second a cluster's frontend gives a namespace by
	// default and every engine and Orker shares, and 60, a fortieth, at five.
	// The price is that an instance that lost its slot may be answered as
	// holding it for that long.
	describeAge = 5 * time.Second
	// tombstoneNoneAge is not lengthened with describeAge: a kept "none"
	// admits.
	tombstoneNoneAge  = time.Second
	tombstoneFoundAge = time.Hour
	maxInventoryAge   = 10 * time.Minute

	describesPerSource  = 5
	describesPerOrker   = 5
	describesPerReplica = 50
	// freshPerHolder is past those caps: one that holds a source is not kept
	// from a describe it must have by others who ask about that source.
	freshPerHolder = 1
	rowsPerOrker   = 1
	rowsPerReplica = 10

	// maxKeptBytes bounds what the reader keeps. Evicting costs reads and
	// changes no answer but to unanswered, where a read made again meets its
	// cap.
	maxKeptBytes = 8 << 20
	// maxLimiters bounds the limiters kept for single sources and Orkers; one
	// forgotten starts full again, and the replica's own limit still holds.
	maxLimiters = 4096
	// readTimeout bounds one read, which the callers that share it outlive
	// each other in.
	readTimeout = 10 * time.Second
)

// errCapped is a read that was not made because its cap is spent.
var errCapped = errors.New("held: too many reads were asked for; ask again")

// Options are a reader's settings.
type Options struct {
	// InventoryAge is how long an Orker's row is kept
	// (event_sources.reconcile), at most ten minutes.
	InventoryAge time.Duration
	// Now is the clock; nil is time.Now.
	Now func() time.Time
}

// Reader makes every read of the check, for the door and the mint alike, and
// alone holds what is kept, for how long, and the caps. A replica has one.
//
// golang.org/x/sync/singleflight was weighed for the shared reads and not
// used: it joins a caller to whatever read is in flight, and a read sent
// before a request arrived must not refuse that request. The kept entries
// are bounded in bytes, which hashicorp/golang-lru is not.
type Reader struct {
	clients      Clients
	inventoryAge time.Duration
	now          func() time.Time
	maxBytes     int

	mu      sync.Mutex
	kept    map[string]*list.Element // of *entry, the most recently used in front
	order   *list.List
	bytes   int
	flights map[string]*flight

	sources, orkers, rows    *limiters
	holders                  *limiters
	replica, replicaRowLimit *rate.Limiter
}

// entry is one kept read and the instant it was sent.
type entry struct {
	key   string
	value any
	sent  time.Time
	size  int
	// renewed is when a read was last let through only to renew this one.
	renewed time.Time
}

// flight is one read being made.
type flight struct {
	sent time.Time
	done chan struct{}
	// value and err are set before done is closed.
	value any
	err   error
}

// NewReader is the one reader of a replica, over its raw clients.
func NewReader(clients Clients, o Options) *Reader {
	if o.Now == nil {
		o.Now = time.Now
	}
	if o.InventoryAge <= 0 || o.InventoryAge > maxInventoryAge {
		o.InventoryAge = maxInventoryAge
	}
	return &Reader{clients: clients, inventoryAge: o.InventoryAge, now: o.Now, maxBytes: maxKeptBytes,
		kept: map[string]*list.Element{}, order: list.New(), flights: map[string]*flight{},
		sources: newLimiters(describesPerSource), orkers: newLimiters(describesPerOrker), rows: newLimiters(rowsPerOrker),
		holders:         newLimiters(freshPerHolder),
		replica:         rate.NewLimiter(describesPerReplica, describesPerReplica),
		replicaRowLimit: rate.NewLimiter(rowsPerReplica, rowsPerReplica)}
}

// question is one read as a call asks for it.
type question struct {
	key     string
	arrived time.Time
	// usable says whether a kept value of that age answers the call, though
	// it was sent before the call arrived.
	usable func(value any, age time.Duration) bool
	// transient is a read that is shared while it is made and never kept.
	transient bool
	// allow says whether a read may be made now, with r.mu held. Nil is
	// always.
	allow func(now time.Time) bool
	fetch func(ctx context.Context) (any, error)
	size  func(value any) int
}

// ask answers q from a kept read that is usable, or from a read sent no
// earlier than the call arrived: one in flight that was, or a new one. A read
// in flight that was sent earlier is waited for, and serves only if it is
// usable.
func (r *Reader) ask(ctx context.Context, q question) (any, error) {
	// An instant not given, or later than the reader's own clock, is now: the
	// first would let any kept read refuse, the second no read ever serve.
	if now := r.now(); q.arrived.IsZero() || q.arrived.After(now) {
		q.arrived = now
	}
	for {
		value, f, started, err := r.look(q)
		if f == nil {
			return value, err
		}
		if started {
			// On its own context: a caller that leaves ends no read others
			// share.
			go r.run(context.WithoutCancel(ctx), q, f)
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-f.done:
		}
		if !f.sent.Before(q.arrived) {
			return f.value, f.err
		}
		// Sent before this call arrived: what it read is kept, and serves
		// this call only where it is usable.
	}
}

// look answers q from a kept read, refuses a read past its caps, or names the
// read to wait for, which the caller makes if it is started here.
func (r *Reader) look(q question) (value any, f *flight, started bool, err error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	now := r.now()
	if el, ok := r.kept[q.key]; ok {
		e := el.Value.(*entry)
		// One sent later than now is from before the clock stepped back: its
		// age is not known, and it answers nothing.
		age := now.Sub(e.sent)
		if age >= 0 && (!e.sent.Before(q.arrived) || (q.usable != nil && q.usable(e.value, age))) {
			r.order.MoveToFront(el)
			return e.value, nil, false, nil
		}
	}
	if f = r.flights[q.key]; f != nil {
		return nil, f, false, nil
	}
	if q.allow != nil && !q.allow(now) {
		return nil, nil, false, errCapped
	}
	f = &flight{sent: now, done: make(chan struct{})}
	r.flights[q.key] = f
	return nil, f, true, nil
}

func (r *Reader) run(ctx context.Context, q question, f *flight) {
	ctx, cancel := context.WithTimeout(ctx, readTimeout)
	defer cancel()
	size := 0
	func() {
		// A read that panicked is a read that gave no answer; its waiters
		// must not wait for ever, nor the process end.
		defer func() {
			if p := recover(); p != nil {
				f.value, f.err = nil, fmt.Errorf("held: read %s: %v", q.key, p)
			}
		}()
		if f.value, f.err = q.fetch(ctx); f.err == nil && q.size != nil {
			size = q.size(f.value)
		}
	}()
	r.land(q, f, size)
}

// land keeps what a read found, where it is kept, and wakes its waiters.
func (r *Reader) land(q question, f *flight, size int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	defer close(f.done)
	delete(r.flights, q.key)
	if f.err == nil && !q.transient {
		r.keep(q.key, f, size)
	}
}

// keep keeps a read, with r.mu held, and evicts the least recently used past
// the bound.
func (r *Reader) keep(key string, f *flight, size int) {
	e := &entry{key: key, value: f.value, sent: f.sent, size: len(key) + 128 + size}
	if el, ok := r.kept[key]; ok {
		r.drop(el)
	}
	if e.size > r.maxBytes {
		return
	}
	r.kept[key] = r.order.PushFront(e)
	r.bytes += e.size
	for r.bytes > r.maxBytes {
		r.drop(r.order.Back())
	}
}

func (r *Reader) drop(el *list.Element) {
	e := el.Value.(*entry)
	r.order.Remove(el)
	delete(r.kept, e.key)
	r.bytes -= e.size
}

func (r *Reader) keptBytes() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.bytes
}

// describe reads the source's workflow: a kept one within describeAge, where
// it confirms, or one sent since the call arrived; with fresh, only the
// second. holding says whether one shows the caller holding the attempt,
// whatever its age.
func (r *Reader) describe(ctx context.Context, fresh bool, ref Ref, caller Caller, arrived time.Time, confirms, holding func(Workflow) bool) (Workflow, error) {
	key := "describe/" + ref.SourceID
	v, err := r.ask(ctx, question{
		key: key, arrived: arrived,
		usable: func(v any, age time.Duration) bool {
			return !fresh && age <= describeAge && confirms(v.(Workflow))
		},
		allow: func(now time.Time) bool {
			// A busy source's steady read, for the Orker that holds it, at
			// most one in describeAge whether or not it is answered, and what
			// a holder must read since it asked, one a second: they grow with
			// the sources that Orker was given, not with what it asks.
			if fresh && r.keptLocked(key, holding) && r.holders.allow(ref.SourceID+"/"+caller.OrkerID, now) {
				return true
			}
			if !fresh && r.renewLocked(key, now, describeAge, holding) {
				return true
			}
			return r.sources.allow(ref.SourceID, now) && r.orkers.allow(caller.OrkerID, now) && r.replica.AllowN(now, 1)
		},
		fetch: func(ctx context.Context) (any, error) { return r.clients.Describe(ctx, ref.SourceID) },
		size: func(v any) int {
			wf := v.(Workflow)
			n := 64
			for _, p := range wf.Pending {
				n += len(p.ActivityID) + len(p.LastWorker) + 48
			}
			for _, s := range wf.Memo.Slots {
				n += len(s.Current.Digest) + len(s.Current.AdmissionSave) + 64
				if s.Replacement != nil {
					n += len(s.Replacement.Digest) + len(s.Replacement.AdmissionSave) + 48
				}
			}
			return n
		},
	})
	if err != nil {
		return Workflow{}, err
	}
	return v.(Workflow), nil
}

// keptLocked reports whether the kept read of key, whatever its age, shows
// what holding asks. It is called with r.mu held.
func (r *Reader) keptLocked(key string, holding func(Workflow) bool) bool {
	el, ok := r.kept[key]
	return ok && holding(el.Value.(*entry).value.(Workflow))
}

// renewLocked reports whether the kept read of key is older than age, shows
// what holding asks, and none was let through to renew it within age, and
// notes that one now is. It is called with r.mu held.
func (r *Reader) renewLocked(key string, now time.Time, age time.Duration, holding func(Workflow) bool) bool {
	el, ok := r.kept[key]
	if !ok {
		return false
	}
	e := el.Value.(*entry)
	if now.Sub(e.sent) <= age || now.Sub(e.renewed) <= age || !holding(e.value.(Workflow)) {
		return false
	}
	e.renewed = now
	return true
}

// tombstoned reads the source's tombstone: a found one is kept an hour and
// may refuse, because nothing removes one; a "none" admits, and is kept a
// second.
func (r *Reader) tombstoned(ctx context.Context, sourceID string, arrived time.Time) (bool, error) {
	v, err := r.ask(ctx, question{
		key: "tombstone/" + sourceID, arrived: arrived,
		usable: func(v any, age time.Duration) bool {
			if v.(bool) {
				return age <= tombstoneFoundAge
			}
			return age <= tombstoneNoneAge
		},
		fetch: func(ctx context.Context) (any, error) { return r.clients.Tombstoned(ctx, sourceID) },
	})
	if err != nil {
		return false, err
	}
	return v.(bool), nil
}

// document reads the document under the source's name, for every call: none
// is ever answered from a document not validated since it arrived.
func (r *Reader) document(ctx context.Context, ref Ref, arrived time.Time) (Document, error) {
	v, err := r.ask(ctx, question{
		key: "document/" + ref.Namespace + "/" + ref.Name, arrived: arrived, transient: true,
		fetch: func(ctx context.Context) (any, error) { return r.clients.Document(ctx, ref.Namespace, ref.Name) },
	})
	if err != nil {
		return Document{}, err
	}
	return v.(Document), nil
}

type confirmedRead struct {
	record artifacts.ConfirmRecord
	found  bool
}

// confirmed reads the attempt's confirm record: one that was found never
// changes, and is kept.
func (r *Reader) confirmed(ctx context.Context, ref Ref, arrived time.Time) (artifacts.ConfirmRecord, bool, error) {
	v, err := r.ask(ctx, question{
		key: fmt.Sprintf("confirm/%s/%d/%d/%d", ref.SourceID, ref.Index, ref.Epoch, ref.Attempt), arrived: arrived,
		usable: func(v any, _ time.Duration) bool { return v.(confirmedRead).found },
		fetch: func(ctx context.Context) (any, error) {
			record, found, err := r.clients.Confirmed(ctx, ref.attempt())
			return confirmedRead{record, found}, err
		},
		size: func(v any) int {
			c := v.(confirmedRead).record
			return len(c.OrkerID) + len(c.ServiceDigest) + len(c.AdmissionSave) + 32
		},
	})
	if err != nil {
		return artifacts.ConfirmRecord{}, false, err
	}
	c := v.(confirmedRead)
	return c.record, c.found, nil
}

// orker reads an Orker's row: a kept one within the inventory's interval,
// where it confirms, or one sent since the call arrived.
func (r *Reader) orker(ctx context.Context, orkerID string, arrived time.Time, confirms func(OrkerRow) bool) (OrkerRow, error) {
	v, err := r.ask(ctx, question{
		key: "orker/" + orkerID, arrived: arrived,
		usable: func(v any, age time.Duration) bool { return age <= r.inventoryAge && confirms(v.(OrkerRow)) },
		allow: func(now time.Time) bool {
			return r.rows.allow(orkerID, now) && r.replicaRowLimit.AllowN(now, 1)
		},
		fetch: func(ctx context.Context) (any, error) { return r.clients.Orker(ctx, orkerID) },
		size: func(v any) int {
			n := 16
			for _, p := range v.(OrkerRow).Pools {
				n += len(p) + 16
			}
			return n
		},
	})
	if err != nil {
		return OrkerRow{}, err
	}
	return v.(OrkerRow), nil
}

// permitted decides the permission for this call: a grant may come from the
// document the replica holds while that is fresh; a refusal comes only from
// a read of it sent since the call arrived.
func (r *Reader) permitted(ctx context.Context, caller Caller, perm authz.Permission, pool string, arrived time.Time) (bool, error) {
	if granted, fresh := r.clients.Access.Held(caller.Subject, perm, pool); granted && fresh {
		return true, nil
	}
	v, err := r.ask(ctx, question{
		key: "access", arrived: arrived, transient: true,
		fetch: func(ctx context.Context) (any, error) {
			decide, err := r.clients.Access.Read(ctx)
			if err == nil && decide == nil {
				err = errors.New("held: the access document was read and nothing decides over it")
			}
			return decide, err
		},
	})
	if err != nil {
		return false, err
	}
	return v.(Decider)(caller.Subject, perm, pool), nil
}

// limiters is one limiter a key, bounded.
type limiters struct {
	perSecond int
	mu        sync.Mutex
	byKey     map[string]*rate.Limiter
}

func newLimiters(perSecond int) *limiters {
	return &limiters{perSecond: perSecond, byKey: map[string]*rate.Limiter{}}
}

func (l *limiters) allow(key string, now time.Time) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	lim, ok := l.byKey[key]
	if !ok {
		if len(l.byKey) >= maxLimiters {
			clear(l.byKey)
		}
		lim = rate.NewLimiter(rate.Limit(l.perSecond), l.perSecond)
		l.byKey[key] = lim
	}
	return lim.AllowN(now, 1)
}
