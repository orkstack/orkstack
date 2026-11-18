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

// eachIndexCache keeps the line indexes this Orker has read, for the lifetime
// of the process — the transient download cache AGENTS.md §5 allows. Every
// item of a fan-out over an artifact's lines reads the same index, so without
// it an Orker running 1,000 items reads up to 547 KiB a thousand times.
//
// Unlike the staging cache, an entry outlives the attempt that fetched it: an
// index is immutable and addressed by its SHA-256, and every use re-reads the
// file and checks it against that digest, so a stale or damaged entry is
// fetched again, never trusted. The least recently used entries go once the
// cache holds more than maxBytes. Concurrent loads of one digest share one
// fetch. A cache that cannot write its directory still answers, uncached.
type eachIndexCache struct {
	dir      string
	maxBytes int64

	mu       sync.Mutex
	entries  map[string]*list.Element // of *eachIndexEntry, by digest
	lru      *list.List               // front is the most recently used
	used     int64
	inflight map[string]*eachIndexFetch
}

type eachIndexEntry struct {
	sha  string
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
func (c *eachIndexCache) load(ctx context.Context, sha string, fetch func(context.Context) ([]byte, error)) ([]byte, error) {
	want, err := hex.DecodeString(sha)
	if err != nil || len(want) != sha256.Size || sha != hex.EncodeToString(want) {
		return nil, fmt.Errorf("line index digest %q is not a SHA-256", sha)
	}
	if body, ok := c.cached(sha, want); ok {
		return body, nil
	}
	for {
		c.mu.Lock()
		f, ok := c.inflight[sha]
		if !ok {
			f = &eachIndexFetch{done: make(chan struct{})}
			c.inflight[sha] = f
			c.mu.Unlock()
			f.body, f.err = c.fill(ctx, sha, want, fetch)
			c.mu.Lock()
			delete(c.inflight, sha)
			c.mu.Unlock()
			close(f.done)
			return f.body, f.err
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
		// A leader whose own context ended did not try to completion: try
		// again while this caller's context allows; any other failure is
		// shared.
		if !errors.Is(f.err, context.Canceled) && !errors.Is(f.err, context.DeadlineExceeded) {
			return nil, f.err
		}
		if err := ctx.Err(); err != nil {
			return nil, err
		}
	}
}

// cached is the cache's copy of sha when it holds one that hashes to want; a
// copy that does not, or cannot be read, is dropped.
func (c *eachIndexCache) cached(sha string, want []byte) ([]byte, bool) {
	c.mu.Lock()
	el, ok := c.entries[sha]
	if ok {
		c.lru.MoveToFront(el)
	}
	c.mu.Unlock()
	if !ok {
		return nil, false
	}
	body, err := os.ReadFile(c.path(sha))
	if err == nil {
		if sum := sha256.Sum256(body); string(sum[:]) == string(want) {
			return body, true
		}
	}
	c.drop(sha)
	return nil, false
}

// fill fetches sha, verifies it and keeps a copy.
func (c *eachIndexCache) fill(ctx context.Context, sha string, want []byte, fetch func(context.Context) ([]byte, error)) ([]byte, error) {
	body, err := fetch(ctx)
	if err != nil {
		return nil, err
	}
	if sum := sha256.Sum256(body); string(sum[:]) != string(want) {
		return nil, errEachIndexDigest
	}
	c.keep(sha, body)
	return body, nil
}

// keep writes body as sha's entry, read-only, and evicts the least recently
// used entries past maxBytes. A write that fails keeps nothing: the cache is
// advisory, and the caller already holds the verified bytes.
func (c *eachIndexCache) keep(sha string, body []byte) {
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
	if werr != nil || cerr != nil || os.Chmod(tmp.Name(), 0o444) != nil || os.Rename(tmp.Name(), c.path(sha)) != nil {
		_ = os.Remove(tmp.Name()) // best effort: the boot reap removes an orphan
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if el, ok := c.entries[sha]; ok {
		c.lru.MoveToFront(el)
		return
	}
	c.entries[sha] = c.lru.PushFront(&eachIndexEntry{sha: sha, size: int64(len(body))})
	c.used += int64(len(body))
	for c.used > c.maxBytes {
		last := c.lru.Back()
		entry := last.Value.(*eachIndexEntry)
		c.lru.Remove(last)
		delete(c.entries, entry.sha)
		c.used -= entry.size
		_ = os.Remove(c.path(entry.sha)) // best effort: a file left behind is never read without its digest check
	}
}

// drop forgets sha's entry and removes its file.
func (c *eachIndexCache) drop(sha string) {
	c.mu.Lock()
	if el, ok := c.entries[sha]; ok {
		c.lru.Remove(el)
		delete(c.entries, sha)
		c.used -= el.Value.(*eachIndexEntry).size
	}
	c.mu.Unlock()
	_ = os.Remove(c.path(sha)) // best effort, as in keep
}

func (c *eachIndexCache) path(sha string) string { return filepath.Join(c.dir, sha) }
