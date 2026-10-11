package orker

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
)

// StageDirName is the directory under the work root that holds the staging
// cache (F-orker-storage-perf-2). Boot empties it, like ork-spool: an entry
// outlives the attempts using it only when the process died.
const StageDirName = "ork-stage"

// stagingCache shares one verified download of an input artifact between the
// attempts that are staging it AT THE SAME TIME on this Orker — the transient
// download cache AGENTS.md §5 allows.
//
// The case it exists for is a fan-out: every item of a 1000-item fan-out
// carries the same upstream refs, so without it one Orker running 1000 items
// downloads a shared 100 MiB input 1000 times. With it, the first attempt to
// ask for a digest downloads and verifies it once into
// <work root>/ork-stage/<sha256> (mode 0444), every attempt hard-links that
// file into its own read-only input tree, and the entry is deleted when the
// last attempt holding it ends. It is in-flight sharing only: nothing is kept
// for a later attempt, so there is no eviction policy and no stale entry.
//
// Keyed by the reference's sha256, which every entry is verified against
// before any attempt links it, so a link can never hold bytes other than the
// digest's. An attempt whose reference carries no sha256 does not use it.
type stagingCache struct {
	dir string
	// link is os.Link; a test replaces it to exercise the fallback.
	link func(oldname, newname string) error

	mu      sync.Mutex
	entries map[string]*stageEntry
}

// stageEntry is one digest's download: in flight until ready closes, then
// either a verified file at path or a failure.
type stageEntry struct {
	ready chan struct{}
	path  string
	// uri is the reference the leader downloaded, so a failure is shared only
	// with attempts asking for the same object; an attempt asking for the
	// same digest at another URI downloads its own.
	uri     string
	failure *Failure
	// abandoned reports that the leader's own context ended: the download was
	// not tried to completion, so a waiter tries again rather than inheriting
	// someone else's cancellation.
	abandoned bool
	refs      int
}

func newStagingCache(dir string) *stagingCache {
	return &stagingCache{dir: dir, link: os.Link, entries: map[string]*stageEntry{}}
}

// stageLease is one attempt's hold on one entry. release is idempotent.
type stageLease struct {
	once    sync.Once
	release func()
}

func (l *stageLease) Release() {
	if l != nil {
		l.once.Do(l.release)
	}
}

// acquire returns the path of a verified file holding ref's bytes, downloading
// it with download (which writes and verifies dest) when no attempt already
// has. Concurrent callers for one digest share one download. The caller must
// Release the lease once it no longer needs the entry.
func (c *stagingCache) acquire(
	ctx context.Context,
	ref *orkstackv1.ArtifactRef,
	download func(ctx context.Context, ref *orkstackv1.ArtifactRef, dest string) *Failure,
) (string, *stageLease, *Failure) {
	sha := ref.GetSha256()
	for {
		c.mu.Lock()
		entry, ok := c.entries[sha]
		if !ok {
			entry = &stageEntry{ready: make(chan struct{}), uri: ref.GetUri(), refs: 1}
			c.entries[sha] = entry
			c.mu.Unlock()
			c.fill(ctx, sha, entry, ref, download)
		} else {
			entry.refs++
			c.mu.Unlock()
		}
		lease := &stageLease{release: func() { c.release(sha, entry) }}
		select {
		case <-entry.ready:
		case <-ctx.Done():
			lease.Release()
			return "", nil, FailCause(ReasonInputUnavailable, ctx.Err(),
				"fetch input artifact %q from %s: %v", ref.GetName(), ref.GetUri(), ctx.Err())
		}
		if entry.failure == nil {
			return entry.path, lease, nil
		}
		lease.Release()
		if entry.abandoned || entry.uri != ref.GetUri() {
			// Not this caller's failure to inherit: try again, now as the
			// leader if nobody else got there first.
			if err := ctx.Err(); err != nil {
				return "", nil, FailCause(ReasonInputUnavailable, err,
					"fetch input artifact %q from %s: %v", ref.GetName(), ref.GetUri(), err)
			}
			continue
		}
		return "", nil, entry.failure
	}
}

// fill is the leader's download: into a private temporary name, verified,
// sealed 0444, then renamed into place. A failure removes the entry, so no
// later attempt can link a file that was never verified.
func (c *stagingCache) fill(
	ctx context.Context, sha string, entry *stageEntry, ref *orkstackv1.ArtifactRef,
	download func(ctx context.Context, ref *orkstackv1.ArtifactRef, dest string) *Failure,
) {
	defer close(entry.ready)
	fail := func(failure *Failure) {
		entry.failure = failure
		entry.abandoned = ctx.Err() != nil
		c.mu.Lock()
		if c.entries[sha] == entry {
			delete(c.entries, sha)
		}
		c.mu.Unlock()
	}
	if err := os.MkdirAll(c.dir, 0o755); err != nil {
		fail(FailCause(ReasonWorkdirFailed, err, "stage input artifact %q: create the staging cache: %v", ref.GetName(), err))
		return
	}
	var suffix [6]byte
	_, _ = rand.Read(suffix[:])
	tmp := filepath.Join(c.dir, "."+sha+"-"+hex.EncodeToString(suffix[:]))
	if failure := download(ctx, ref, tmp); failure != nil {
		_ = os.Remove(tmp)
		fail(failure)
		return
	}
	final := filepath.Join(c.dir, sha)
	if err := os.Chmod(tmp, ModeInputFile); err != nil {
		_ = os.Remove(tmp)
		fail(FailCause(ReasonWorkdirFailed, err, "stage input artifact %q: seal the cached copy: %v", ref.GetName(), err))
		return
	}
	if err := os.Rename(tmp, final); err != nil {
		_ = os.Remove(tmp)
		fail(FailCause(ReasonWorkdirFailed, err, "stage input artifact %q: %v", ref.GetName(), err))
		return
	}
	entry.path = final
}

// release drops one hold; the last one deletes the entry and its file.
func (c *stagingCache) release(sha string, entry *stageEntry) {
	c.mu.Lock()
	defer c.mu.Unlock()
	entry.refs--
	if entry.refs > 0 {
		return
	}
	if c.entries[sha] == entry {
		delete(c.entries, sha)
	}
	if entry.path != "" {
		_ = os.Remove(entry.path)
	}
}

// size reports the number of live entries, for tests.
func (c *stagingCache) size() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.entries)
}

// linkInto hard-links the cached file at path to dest. The input tree and the
// cache are both under the work root, so this is one filesystem in every
// supported layout; a link that fails anyway (a filesystem without hard
// links, a cross-device root) is reported so the caller downloads directly.
func (c *stagingCache) linkInto(path, dest string) error {
	if err := c.link(path, dest); err != nil {
		return fmt.Errorf("link the staged copy: %w", err)
	}
	return nil
}
