package api

import (
	"context"
	"sync"
	"time"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
)

// taskTerminalMemoTTL is how long one run read answers every quiet log
// follower of that run on this replica. It is a disposable cache (AGENTS.md
// §13): a follower is never told "not finished" by an answer older than this,
// and the cost it removes is one whole-history replay per open pane per quiet
// poll.
const taskTerminalMemoTTL = time.Second

// runReadMemo shares one LookupRunIn between the quiet log followers of one
// run: one read per (run, namespace) at a time, shared by every caller asking
// while it runs, and reused for taskTerminalMemoTTL after it lands. A failed
// read answers the callers that waited for it and is never kept.
//
// Its clock is the wall clock, never the Service's: the Service's clock is a
// domain clock a test may freeze, and a frozen clock would turn a one-second
// memo into a permanent answer.
type runReadMemo struct {
	ttl time.Duration
	now func() time.Time

	mu      sync.Mutex
	entries map[runReadKey]*runReadCall
}

type runReadKey struct{ runID, namespace string }

type runReadCall struct {
	done  chan struct{}
	run   *orkstackv1.Run
	err   error
	until time.Time // set once done; the answer is reused before it
}

func newRunReadMemo(ttl time.Duration) *runReadMemo {
	return &runReadMemo{ttl: ttl, now: time.Now, entries: map[runReadKey]*runReadCall{}}
}

// get answers key from a fresh answer, the read in flight, or a new read.
// The returned run is shared between callers and must not be modified.
func (m *runReadMemo) get(
	ctx context.Context,
	key runReadKey,
	read func(ctx context.Context) (*orkstackv1.Run, error),
) (*orkstackv1.Run, error) {
	m.mu.Lock()
	now := m.now()
	call, ok := m.entries[key]
	if ok {
		select {
		case <-call.done:
			if call.err != nil || !now.Before(call.until) {
				ok = false
			}
		default: // in flight: join it
		}
	}
	if !ok {
		// Forget every expired answer before adding one, so the memo holds
		// only the runs being followed right now.
		for k, c := range m.entries {
			select {
			case <-c.done:
				if !now.Before(c.until) || c.err != nil {
					delete(m.entries, k)
				}
			default:
			}
		}
		call = &runReadCall{done: make(chan struct{})}
		m.entries[key] = call
		go m.run(context.WithoutCancel(ctx), key, call, read)
	}
	m.mu.Unlock()

	select {
	case <-call.done:
		return call.run, call.err
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (m *runReadMemo) run(
	detached context.Context,
	key runReadKey,
	call *runReadCall,
	read func(ctx context.Context) (*orkstackv1.Run, error),
) {
	ctx, cancel := context.WithTimeout(detached, 30*time.Second)
	defer cancel()
	run, err := read(ctx)
	m.mu.Lock()
	call.run, call.err = run, err
	call.until = m.now().Add(m.ttl)
	if err != nil && m.entries[key] == call {
		delete(m.entries, key)
	}
	m.mu.Unlock()
	close(call.done)
}
