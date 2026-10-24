package api

import "sync"

// storeFanout bounds how many durable-store or cluster round-trips one admin
// read may have in flight at once.
//
// The deployment-wide reads scan durable state whose size is the deployment's
// whole history: every namespace's orkflow catalog, then every version catalog
// of every orkflow in it, then one cluster probe per pool those definitions
// name. Issued one at a time, that is a round-trip per stored object, so the
// read's latency grows with everything anyone has ever stored — and eventually
// passes the client's deadline. Issued serially against a bucket holding ~90
// namespaces and ~600 orkflows, the catalog scan alone exceeds a 30s client
// deadline, and every pool not yet probed is then reported as "context
// canceled".
//
// Nothing here makes the scan smaller. The same objects are read and the same
// pools are probed; they are simply in flight together, so the cost is
// round-trips DIVIDED by this number. The bound is the other half: without it
// one admin read would open a request per stored object at once, and the
// timeout would come back as store throttling instead.
const storeFanout = 16

// inParallel runs fn for every index below n, at most storeFanout at a time,
// and returns once every one of them has finished.
//
// fn is given an index and nothing else, because the contract is that each
// call writes its result into a slot it alone owns. No lock is needed, and the
// reduction the caller runs afterwards walks those slots in index order — so
// the answer, and the error a caller reports, are identical to the serial scan
// this replaced whatever order the round-trips actually complete in.
//
// Callers must not nest one inParallel inside another's fn: a goroutine
// holding a slot while it waits for a slot is the shape that deadlocks a
// bounded limiter. Flatten the inner work into a second wave instead
// (poolsNamedByOrkflowsAcross is the worked example).
func inParallel(n int, fn func(i int)) {
	if n <= 0 {
		return
	}
	if n == 1 {
		// The overwhelmingly common case for the single-namespace callers,
		// and worth not paying a goroutine and a channel for.
		fn(0)
		return
	}
	limit := storeFanout
	if n < limit {
		limit = n
	}
	sem := make(chan struct{}, limit)
	var wg sync.WaitGroup
	wg.Add(n)
	for i := 0; i < n; i++ {
		sem <- struct{}{}
		go func(i int) {
			defer wg.Done()
			defer func() { <-sem }()
			fn(i)
		}(i)
	}
	wg.Wait()
}
