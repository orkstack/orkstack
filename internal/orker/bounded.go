package orker

import (
	"context"
	"sync"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// transferParallelism bounds how many object-store requests one attempt has
// in flight for one phase: input staging, output upload, and a cache hit's
// existence checks. Eight hides most of a request's latency without letting a
// task with hundreds of refs open hundreds of connections.
const transferParallelism = 8

// eachBounded runs fn for every index in [0, n), at most limit at once, in
// increasing index order of START.
//
// It answers exactly what a serial loop that stops at the first failure would
// have answered: the LOWEST failing index and its error, or (-1, nil). A
// failure at i cancels every running index above i and starts none after it;
// indexes below i keep running, because one of them may fail too and would
// then be the answer. So the reported failure never depends on which request
// happened to finish first, and an error message stays deterministic.
//
// fn receives a context of its own, cancelled when a lower index fails or
// when ctx ends. fn writes its result into a slot it owns (index-addressed),
// so the caller assembles results in index order afterwards.
func eachBounded(ctx context.Context, n, limit int, fn func(ctx context.Context, i int) error) (int, error) {
	if n <= 0 {
		return -1, nil
	}
	if limit < 1 {
		limit = 1
	}
	if limit > n {
		limit = n
	}
	var (
		mu       sync.Mutex
		next     int
		failedAt = n
		failure  error
		cancels  = make([]context.CancelFunc, n)
		wg       sync.WaitGroup
	)
	work := func() {
		defer wg.Done()
		for {
			mu.Lock()
			if next >= n || next > failedAt {
				mu.Unlock()
				return
			}
			i := next
			next++
			itemCtx, cancel := context.WithCancel(ctx)
			cancels[i] = cancel
			mu.Unlock()

			err := fn(itemCtx, i)

			mu.Lock()
			cancels[i] = nil
			cancel()
			if err != nil && i < failedAt {
				failedAt, failure = i, err
				for j := i + 1; j < n; j++ {
					if cancels[j] != nil {
						cancels[j]()
					}
				}
			}
			mu.Unlock()
		}
	}
	wg.Add(limit)
	for w := 0; w < limit; w++ {
		go work()
	}
	wg.Wait()
	if failure != nil {
		return failedAt, failure
	}
	return -1, nil
}

// confirmSourceArtifacts is a cache hit's last check: every artifact the
// source commit names must still exist (docs/STORAGE.md — a deleted source
// artifact is a miss, never a hit). It runs transferParallelism checks at a
// time and reports the lowest-index artifact that could not be confirmed, so
// the miss names the same artifact a serial check would.
func confirmSourceArtifacts(ctx context.Context, store artifacts.Store, refs []*orkstackv1.ArtifactRef) (int, error) {
	return eachBounded(ctx, len(refs), transferParallelism, func(ctx context.Context, i int) error {
		_, err := store.Stat(ctx, refs[i].GetUri())
		return err
	})
}
