package sourcehost

import (
	"context"
	"slices"
	"sync"
)

// ParseShare bounds the bodies every instance of an Orker reads and judges at
// once. Lanes are served in the order they asked; no instance holds more than
// half of it while another's lane waits, and a body larger than all of it is read
// alone. golang.org/x/sync/semaphore was weighed: it serves in order, and knows
// nothing of whose a share is.
type ParseShare struct {
	total int64

	mu   sync.Mutex
	used int64
	held map[any]int64
	wait []*parseWaiter
}

type parseWaiter struct {
	owner any
	n     int64
	got   chan struct{}
}

// NewParseShare is a share of total bytes.
func NewParseShare(total int64) *ParseShare {
	return &ParseShare{total: max(total, 1), held: map[any]int64{}}
}

// Acquire waits for n bytes of the share for owner, at most until ctx ends, and
// gives what returns them. A nil share bounds nothing.
func (p *ParseShare) Acquire(ctx context.Context, owner any, n int64) (release func(), err error) {
	if p == nil {
		return func() {}, nil
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	w := &parseWaiter{owner: owner, n: min(max(n, 1), p.total), got: make(chan struct{})}
	p.mu.Lock()
	p.wait = append(p.wait, w)
	p.serve()
	p.mu.Unlock()
	select {
	case <-w.got:
	case <-ctx.Done():
		p.mu.Lock()
		defer p.mu.Unlock()
		select {
		case <-w.got:
			p.give(w)
		default:
			p.wait = slices.DeleteFunc(p.wait, func(o *parseWaiter) bool { return o == w })
			// What it stood before may have room now.
			p.serve()
		}
		return nil, ctx.Err()
	}
	return sync.OnceFunc(func() {
		p.mu.Lock()
		defer p.mu.Unlock()
		p.give(w)
	}), nil
}

// give returns a waiter's bytes and serves the next. Locked.
func (p *ParseShare) give(w *parseWaiter) {
	p.used -= w.n
	if p.held[w.owner] -= w.n; p.held[w.owner] == 0 {
		delete(p.held, w.owner)
	}
	p.serve()
}

// serve gives their bytes to the waiters there is room for, in order: one that
// waits for room is not passed, and one over its instance's half is, by another's.
// Locked.
func (p *ParseShare) serve() {
	for i := 0; i < len(p.wait); {
		w := p.wait[i]
		if p.held[w.owner] > 0 && p.held[w.owner]+w.n > p.total/2 && p.othersWait(w.owner) {
			i++
			continue
		}
		if p.used+w.n > p.total {
			return
		}
		p.wait = slices.Delete(p.wait, i, i+1)
		p.used += w.n
		p.held[w.owner] += w.n
		close(w.got)
		// Whose half it counts in has changed for those passed.
		i = 0
	}
}

// othersWait reports that a lane of another instance waits. Locked.
func (p *ParseShare) othersWait(owner any) bool {
	return slices.ContainsFunc(p.wait, func(w *parseWaiter) bool { return w.owner != owner })
}
