package telemetry

import "sync"

// providerCache memoizes one value per *Providers, built at most once no
// matter how many callers ask for the same sink concurrently. It is the
// shape EngineObserver, OrkerObserver and TaskLogExporter each held by hand
// before this helper existed: one mutex-guarded map from a sink's Providers
// to whatever that type caches for it (instruments, a route, a logger — the
// zero value type parameter T is chosen by each caller).
type providerCache[T any] struct {
	mu    sync.Mutex
	cache map[*Providers]T
}

// getOrBuild returns the cached value for p, calling build to make one the
// first time p is seen. A build error is not cached: a transient failure
// (an instrument name collision is the only one in practice, and that is
// static, but there is no reason to bake in "never retry" here) gets a fresh
// attempt on the next call for the same p.
func (c *providerCache[T]) getOrBuild(p *Providers, build func() (T, error)) (T, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if v, ok := c.cache[p]; ok {
		return v, nil
	}
	v, err := build()
	if err != nil {
		var zero T
		return zero, err
	}
	if c.cache == nil {
		c.cache = map[*Providers]T{}
	}
	c.cache[p] = v
	return v, nil
}

// set seeds the cache for p with v directly, without going through build —
// the seam a constructor that already has p's value in hand (NewOrkerObserver,
// which is handed instruments its caller built) uses instead of getOrBuild.
func (c *providerCache[T]) set(p *Providers, v T) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.cache == nil {
		c.cache = map[*Providers]T{}
	}
	c.cache[p] = v
}
