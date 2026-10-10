package api

import (
	"context"
	"sync"
	"time"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// PoolScopeCacheTTL is how long one replica keeps a namespace's
// pools-named-by-orkflows answer: the pools its stored definitions mention,
// across every stored version. Computing it costs one orkflow List plus one
// Versions per stored orkflow, and GetStats and ListPools both ask for it on
// every computation.
//
// An apply or a delete through THIS replica invalidates the namespace's entry
// at once. One through another replica is seen here when the entry expires:
// a pool named only by an orkflow applied elsewhere appears in this replica's
// pool list within PoolScopeCacheTTL. It is a disposable cache (AGENTS.md
// §13), never an answer of record — the orkflow store is.
const PoolScopeCacheTTL = 30 * time.Second

// poolScopeComputeTimeout bounds one shared computation, which runs detached
// from the caller that started it so a caller that gives up does not fail the
// others waiting on the same answer.
const poolScopeComputeTimeout = 30 * time.Second

// poolScopeCache is the per-Service, per-namespace memo of
// poolsNamedByOrkflowsAcross' answer: one computation per namespace at a
// time, shared by every caller asking while it runs, and kept for
// PoolScopeCacheTTL. An error is handed to the callers that waited for it and
// never kept, so a namespace whose catalog was unreadable is asked again on
// the next call — per-namespace degradation is preserved, never memoized.
type poolScopeCache struct {
	ttl time.Duration
	now func() time.Time

	mu       sync.Mutex
	answers  map[string]poolScopeEntry
	inflight map[string]*poolScopeCall
	// generation is bumped by invalidate. A computation that started under an
	// older generation still answers the callers that joined it, but is not
	// kept: it may predate the write that invalidated the namespace.
	generation map[string]uint64
}

type poolScopeEntry struct {
	pools   []string
	expires time.Time
}

type poolScopeCall struct {
	done       chan struct{}
	generation uint64
	pools      []string
	err        error
}

func newPoolScopeCache(ttl time.Duration, now func() time.Time) *poolScopeCache {
	return &poolScopeCache{
		ttl: ttl, now: now,
		answers:    map[string]poolScopeEntry{},
		inflight:   map[string]*poolScopeCall{},
		generation: map[string]uint64{},
	}
}

// invalidate forgets a namespace's answer, and detaches any computation in
// flight for it so a caller arriving after the write starts a fresh one.
func (c *poolScopeCache) invalidate(namespace string) {
	key := artifacts.ResolveNamespace(namespace)
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.answers, key)
	delete(c.inflight, key)
	c.generation[key]++
}

// get answers ns from the memo, by joining the computation in flight, or by
// starting one with compute.
func (c *poolScopeCache) get(
	ctx context.Context,
	ns *orkstackv1.Namespace,
	compute func(ctx context.Context, ns *orkstackv1.Namespace) ([]string, error),
) ([]string, error) {
	key := artifacts.ResolveNamespace(ns.GetName())
	c.mu.Lock()
	if entry, ok := c.answers[key]; ok && c.now().Before(entry.expires) {
		c.mu.Unlock()
		return append([]string(nil), entry.pools...), nil
	}
	call, joined := c.inflight[key]
	if !joined {
		call = &poolScopeCall{done: make(chan struct{}), generation: c.generation[key]}
		c.inflight[key] = call
		go c.run(context.WithoutCancel(ctx), key, ns, call, compute)
	}
	c.mu.Unlock()

	select {
	case <-call.done:
		if call.err != nil {
			return nil, call.err
		}
		return append([]string(nil), call.pools...), nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (c *poolScopeCache) run(
	detached context.Context,
	key string,
	ns *orkstackv1.Namespace,
	call *poolScopeCall,
	compute func(ctx context.Context, ns *orkstackv1.Namespace) ([]string, error),
) {
	ctx, cancel := context.WithTimeout(detached, poolScopeComputeTimeout)
	defer cancel()
	call.pools, call.err = compute(ctx, ns)

	c.mu.Lock()
	if c.inflight[key] == call {
		delete(c.inflight, key)
	}
	if call.err == nil && c.ttl > 0 && c.generation[key] == call.generation {
		c.answers[key] = poolScopeEntry{pools: append([]string(nil), call.pools...), expires: c.now().Add(c.ttl)}
	}
	c.mu.Unlock()
	close(call.done)
}
