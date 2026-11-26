package orker

import (
	"container/list"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
)

// eachIndexCacheDir is the directory, under this identity's part of
// ork-stage, that holds the line-index cache. ork-stage's entries are named by
// a 64-hex digest, so this name can never collide with one; the boot reap of
// the identity's ork-stage directory removes it with the rest.
const eachIndexCacheDir = "index"

// eachIndexCacheMaxBytes bounds the line indexes one Orker keeps: an index is
// at most 64 + 56 x 10,000 bytes (547 KiB), so this holds about a hundred of
// the largest.
const eachIndexCacheMaxBytes = 64 << 20

// errEachIndexDigest reports index bytes that do not hash to the digest the
// engine recorded for them: the store holds something else under the key.
var errEachIndexDigest = errors.New("the line index's bytes do not match its recorded digest")

// errEachIndexFetchEnded is the answer of a fetch that gave none: it panicked,
// or its goroutine exited. Only the loads that waited for it meet it.
var errEachIndexFetchEnded = errors.New("the line index's fetch ended without an answer")

// eachIndexCache keeps the line indexes this Orker has read, for the lifetime
// of the process — the transient download cache AGENTS.md §5 allows. Every
// item of a fan-out over an artifact's lines reads the same index, so without
// it an Orker running 1,000 items reads up to 547 KiB a thousand times.
//
// Unlike the staging cache, an entry outlives the attempt that fetched it: an
// index is immutable and addressed by its SHA-256, and every use re-reads the
// file and checks it against that digest, so a stale or damaged entry is
// fetched again, never trusted. An entry is keyed by the index's uri and its
// digest together, so it answers only a load of that very index: an item
// naming another index never reads this one's copy, whatever digest it carries. The least recently used entries go once the
// cache holds more than maxBytes. Concurrent loads of one digest share one
// fetch. A cache that cannot write its directory still answers, uncached.
//
// golang.org/x/sync/singleflight was weighed for the shared fetch and not
// used: a load must stop waiting when its own context ends, which its Do does
// not, and its DoChan ends the whole process when a fetch panics. What a
// panicked or exited fetch leaves here is what singleflight leaves: nothing in
// flight.
type eachIndexCache struct {
	dir      string
	maxBytes int64

	mu       sync.Mutex
	entries  map[string]*list.Element // of *eachIndexEntry, by eachIndexCacheName
	lru      *list.List               // front is the most recently used
	used     int64
	inflight map[string]*eachIndexFetch
}

type eachIndexEntry struct {
	key  string // eachIndexCacheName of its uri and digest
	size int64
}

// eachIndexFetch is one digest's fetch in flight: done closes once body or
// err is set.
type eachIndexFetch struct {
	done chan struct{}
	body []byte
	err  error
}

func newEachIndexCache(dir string, maxBytes int64) *eachIndexCache {
	return &eachIndexCache{
		dir:      dir,
		maxBytes: maxBytes,
		entries:  map[string]*list.Element{},
		lru:      list.New(),
		inflight: map[string]*eachIndexFetch{},
	}
}

// load returns the bytes of the index whose SHA-256 is sha (lowercase hex):
// the cache's copy when it still hashes to sha, else fetch's. Bytes that do
// not hash to sha are errEachIndexDigest and are never kept. fetch's own
// error is returned wrapped as it is, so the caller can tell a missing index
// from an outage.
func (c *eachIndexCache) load(ctx context.Context, uri, sha string, fetch func(context.Context) ([]byte, error)) ([]byte, error) {
	want, err := hex.DecodeString(sha)
	if err != nil || len(want) != sha256.Size || sha != hex.EncodeToString(want) {
		return nil, fmt.Errorf("line index digest %q is not a SHA-256", sha)
	}
	key := eachIndexCacheName(uri, sha)
	if body, ok := c.cached(key, want); ok {
		return body, nil
	}
	return c.share(ctx, key, want, fetch)
}

// share answers a load that found no kept copy: it waits for the fetch of the
// index in flight, or makes it. A fetch keeps its copy before it leaves the
// flight, so one that is not in flight has kept what it could.
func (c *eachIndexCache) share(ctx context.Context, key string, want []byte, fetch func(context.Context) ([]byte, error)) ([]byte, error) {
	for {
		c.mu.Lock()
		f, ok := c.inflight[key]
		if !ok {
			f = &eachIndexFetch{done: make(chan struct{})}
			c.inflight[key] = f
			c.mu.Unlock()
			return c.lead(ctx, f, key, want, fetch)
		}
		c.mu.Unlock()
		select {
		case <-f.done:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
		if f.err == nil {
			return f.body, nil
		}
		// A leader whose own context ended, or whose fetch answered nobody,
		// did not try to completion: try again while this caller's context
		// allows; any other failure is shared.
		if !errors.Is(f.err, context.Canceled) && !errors.Is(f.err, context.DeadlineExceeded) && !errors.Is(f.err, errEachIndexFetchEnded) {
			return nil, f.err
		}
		if err := ctx.Err(); err != nil {
			return nil, err
		}
	}
}

// lead makes the fetch f for every load that waits for it, and takes it out of
// the flight however it ends: a fetch that panics does so in its own load
// alone.
func (c *eachIndexCache) lead(ctx context.Context, f *eachIndexFetch, key string, want []byte, fetch func(context.Context) ([]byte, error)) ([]byte, error) {
	f.err = errEachIndexFetchEnded // unless the fetch answers
	defer func() {
		c.mu.Lock()
		delete(c.inflight, key)
		c.mu.Unlock()
		close(f.done)
	}()
	// Looked for again: the fetch that was in flight when this load missed may
	// have kept its copy and gone since.
	if body, ok := c.cached(key, want); ok {
		f.body, f.err = body, nil
	} else {
		f.body, f.err = c.fill(ctx, key, want, fetch)
	}
	return f.body, f.err
}

// cached is the cache's copy at key when it holds one that hashes to want; a
// copy that does not, or cannot be read, is dropped.
func (c *eachIndexCache) cached(key string, want []byte) ([]byte, bool) {
	c.mu.Lock()
	el, ok := c.entries[key]
	if ok {
		c.lru.MoveToFront(el)
	}
	c.mu.Unlock()
	if !ok {
		return nil, false
	}
	body, err := os.ReadFile(c.path(key))
	if err == nil {
		if sum := sha256.Sum256(body); string(sum[:]) == string(want) {
			return body, true
		}
	}
	c.drop(key, el)
	return nil, false
}

// fill fetches the index, verifies it against want and keeps a copy at key.
func (c *eachIndexCache) fill(ctx context.Context, key string, want []byte, fetch func(context.Context) ([]byte, error)) ([]byte, error) {
	body, err := fetch(ctx)
	if err != nil {
		return nil, err
	}
	if sum := sha256.Sum256(body); string(sum[:]) != string(want) {
		return nil, errEachIndexDigest
	}
	c.keep(key, body)
	return body, nil
}

// keep writes body as key's entry, read-only, and evicts the least recently
// used entries past maxBytes. A write that fails keeps nothing: the cache is
// advisory, and the caller already holds the verified bytes.
func (c *eachIndexCache) keep(key string, body []byte) {
	if c.dir == "" || int64(len(body)) > c.maxBytes {
		return
	}
	if err := os.MkdirAll(c.dir, 0o700); err != nil {
		return
	}
	tmp, err := os.CreateTemp(c.dir, ".tmp-*")
	if err != nil {
		return
	}
	_, werr := tmp.Write(body)
	cerr := tmp.Close()
	if werr != nil || cerr != nil || os.Chmod(tmp.Name(), 0o444) != nil {
		_ = os.Remove(tmp.Name()) // best effort: the boot reap removes an orphan
		return
	}
	// The file and its entry change together, so a drop never takes the one
	// for the other's.
	c.mu.Lock()
	defer c.mu.Unlock()
	if os.Rename(tmp.Name(), c.path(key)) != nil {
		_ = os.Remove(tmp.Name()) // best effort, as above
		return
	}
	if el, ok := c.entries[key]; ok {
		c.forget(el)
	}
	c.entries[key] = c.lru.PushFront(&eachIndexEntry{key: key, size: int64(len(body))})
	c.used += int64(len(body))
	for c.used > c.maxBytes {
		entry := c.forget(c.lru.Back())
		_ = os.Remove(c.path(entry.key)) // best effort: a file left behind is never read without its digest check
	}
}

// drop forgets el, key's entry as a load read it, and removes its file, unless
// another copy has taken its place since.
func (c *eachIndexCache) drop(key string, el *list.Element) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.entries[key] != el {
		return
	}
	c.forget(el)
	_ = os.Remove(c.path(key)) // best effort, as in keep
}

// forget takes el out of the entries, and answers it. c.mu is held.
func (c *eachIndexCache) forget(el *list.Element) *eachIndexEntry {
	entry := c.lru.Remove(el).(*eachIndexEntry)
	delete(c.entries, entry.key)
	c.used -= entry.size
	return entry
}

func (c *eachIndexCache) path(key string) string { return filepath.Join(c.dir, key) }

// eachIndexCacheName is the key, and the file name, of the entry for the
// index at uri with digest sha: a SHA-256 of both, so a uri's bytes never
// reach a file name, and two indexes that share a digest never share an entry.
func eachIndexCacheName(uri, sha string) string {
	sum := sha256.Sum256([]byte(uri + "\x00" + sha))
	return hex.EncodeToString(sum[:])
}
