package orker

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
)

func shaHex(b []byte) string {
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

// countingFetch serves body and counts its calls.
func countingFetch(body []byte, calls *atomic.Int32) func(context.Context) ([]byte, error) {
	return func(context.Context) ([]byte, error) {
		calls.Add(1)
		return append([]byte(nil), body...), nil
	}
}

// testIndexURI is the one index uri the cache tests load from.
const testIndexURI = "mem://commits/metadata/commits/default/ork-run/convert/each-lines/jsonl-0.idx"

// cachedPath is the file the cache keeps for the index at uri with digest sha.
func cachedPath(dir, uri, sha string) string { return filepath.Join(dir, eachIndexCacheName(uri, sha)) }

// TestIndexCache_KeysByURIAndDigest: an entry answers only a load of the
// same index uri with the same digest. Another uri carrying a cached index's
// digest is fetched from the store, never served the other index's copy.
func TestIndexCache_KeysByURIAndDigest(t *testing.T) {
	ctx := context.Background()
	c := newEachIndexCache(t.TempDir(), 1<<20)
	body := []byte("an index")
	var calls atomic.Int32
	for _, uri := range []string{testIndexURI, testIndexURI, "mem://commits/metadata/commits/team-b/ork-run/convert/each-lines/jsonl-0.idx"} {
		if _, err := c.load(ctx, uri, shaHex(body), countingFetch(body, &calls)); err != nil {
			t.Fatal(err)
		}
	}
	if calls.Load() != 2 {
		t.Fatalf("fetched %d times; want 2: once per index uri", calls.Load())
	}
}

func TestIndexCache_ReverifiesAndEvicts(t *testing.T) {
	ctx := context.Background()

	t.Run("a second load reads the kept copy, read-only", func(t *testing.T) {
		dir := t.TempDir()
		c := newEachIndexCache(dir, 1<<20)
		body := []byte("index one")
		var calls atomic.Int32
		for range 3 {
			got, err := c.load(ctx, testIndexURI, shaHex(body), countingFetch(body, &calls))
			if err != nil || string(got) != string(body) {
				t.Fatalf("load = %q, %v; want %q", got, err, body)
			}
		}
		if calls.Load() != 1 {
			t.Fatalf("fetched %d times; want 1", calls.Load())
		}
		info, err := os.Stat(cachedPath(dir, testIndexURI, shaHex(body)))
		if err != nil {
			t.Fatal(err)
		}
		if info.Mode().Perm() != 0o444 {
			t.Fatalf("entry mode %v; want 0444", info.Mode().Perm())
		}
	})

	t.Run("a damaged copy is fetched again, never served", func(t *testing.T) {
		dir := t.TempDir()
		c := newEachIndexCache(dir, 1<<20)
		body := []byte("index two")
		var calls atomic.Int32
		if _, err := c.load(ctx, testIndexURI, shaHex(body), countingFetch(body, &calls)); err != nil {
			t.Fatal(err)
		}
		path := cachedPath(dir, testIndexURI, shaHex(body))
		if err := os.Chmod(path, 0o644); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, []byte("tampered"), 0o644); err != nil {
			t.Fatal(err)
		}
		got, err := c.load(ctx, testIndexURI, shaHex(body), countingFetch(body, &calls))
		if err != nil || string(got) != string(body) {
			t.Fatalf("load after tamper = %q, %v; want %q", got, err, body)
		}
		if calls.Load() != 2 {
			t.Fatalf("fetched %d times; want 2 (the damaged copy refetched)", calls.Load())
		}
	})

	t.Run("bytes that do not match the digest are refused and not kept", func(t *testing.T) {
		dir := t.TempDir()
		c := newEachIndexCache(dir, 1<<20)
		want := shaHex([]byte("the recorded index"))
		var calls atomic.Int32
		_, err := c.load(ctx, testIndexURI, want, countingFetch([]byte("something else"), &calls))
		if !errors.Is(err, errEachIndexDigest) {
			t.Fatalf("load = %v; want errEachIndexDigest", err)
		}
		if _, err := os.Stat(cachedPath(dir, testIndexURI, want)); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("a mismatched body was kept: %v", err)
		}
	})

	t.Run("the least recently used entry goes past the bound", func(t *testing.T) {
		dir := t.TempDir()
		c := newEachIndexCache(dir, 20)
		a, b, d := []byte("aaaaaaaaaa"), []byte("bbbbbbbbbb"), []byte("dddddddddd")
		var calls atomic.Int32
		for _, body := range [][]byte{a, b, a, d} { // a is used again, so b is the oldest
			if _, err := c.load(ctx, testIndexURI, shaHex(body), countingFetch(body, &calls)); err != nil {
				t.Fatal(err)
			}
		}
		if _, err := os.Stat(cachedPath(dir, testIndexURI, shaHex(b))); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("b, the least recently used, is still kept: %v", err)
		}
		for _, body := range [][]byte{a, d} {
			if _, err := os.Stat(cachedPath(dir, testIndexURI, shaHex(body))); err != nil {
				t.Fatalf("%s was evicted: %v", body, err)
			}
		}
		if c.used != 20 {
			t.Fatalf("used = %d; want 20", c.used)
		}
	})

	t.Run("concurrent loads of one digest share one fetch", func(t *testing.T) {
		c := newEachIndexCache(t.TempDir(), 1<<20)
		body := []byte("shared index")
		release := make(chan struct{})
		var calls atomic.Int32
		fetch := func(context.Context) ([]byte, error) {
			calls.Add(1)
			<-release
			return body, nil
		}
		var wg sync.WaitGroup
		errs := make(chan error, 8)
		for range 8 {
			wg.Add(1)
			go func() {
				defer wg.Done()
				_, err := c.load(ctx, testIndexURI, shaHex(body), fetch)
				errs <- err
			}()
		}
		for calls.Load() == 0 { // the leader is in fetch
			runtime.Gosched()
		}
		close(release)
		wg.Wait()
		close(errs)
		for err := range errs {
			if err != nil {
				t.Fatal(err)
			}
		}
		if calls.Load() != 1 {
			t.Fatalf("fetched %d times; want 1", calls.Load())
		}
	})

	// A load that looks before the fetch in flight has kept its copy, and
	// asks to share it after it has gone, has missed both.
	t.Run("a load that missed the fetch in flight reads what it kept", func(t *testing.T) {
		c := newEachIndexCache(t.TempDir(), 1<<20)
		body := []byte("shared index")
		sum := sha256.Sum256(body)
		key := eachIndexCacheName(testIndexURI, shaHex(body))
		var calls atomic.Int32
		if _, ok := c.cached(key, sum[:]); ok {
			t.Fatal("an empty cache holds a copy")
		}
		if _, err := c.load(ctx, testIndexURI, shaHex(body), countingFetch(body, &calls)); err != nil {
			t.Fatal(err)
		}
		got, err := c.share(ctx, key, sum[:], countingFetch(body, &calls))
		if err != nil || string(got) != string(body) {
			t.Fatalf("share = %q, %v; want %q", got, err, body)
		}
		if calls.Load() != 1 {
			t.Fatalf("fetched %d times; want 1: the copy is kept", calls.Load())
		}
	})

	// What the flight left behind is checked as any copy is.
	t.Run("a damaged copy is not read in place of the fetch either", func(t *testing.T) {
		dir := t.TempDir()
		c := newEachIndexCache(dir, 1<<20)
		body := []byte("shared index")
		sum := sha256.Sum256(body)
		key := eachIndexCacheName(testIndexURI, shaHex(body))
		var calls atomic.Int32
		if _, err := c.load(ctx, testIndexURI, shaHex(body), countingFetch(body, &calls)); err != nil {
			t.Fatal(err)
		}
		path := cachedPath(dir, testIndexURI, shaHex(body))
		if err := os.Chmod(path, 0o644); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, []byte("tampered"), 0o644); err != nil {
			t.Fatal(err)
		}
		got, err := c.share(ctx, key, sum[:], countingFetch(body, &calls))
		if err != nil || string(got) != string(body) || calls.Load() != 2 {
			t.Fatalf("share = %q, %v after %d fetches; want %q, fetched again", got, err, calls.Load(), body)
		}
	})

	t.Run("a fetch error is returned as it is", func(t *testing.T) {
		c := newEachIndexCache(t.TempDir(), 1<<20)
		outage := errors.New("store down")
		_, err := c.load(ctx, testIndexURI, shaHex([]byte("x")), func(context.Context) ([]byte, error) { return nil, outage })
		if !errors.Is(err, outage) {
			t.Fatalf("load = %v; want the fetch's error", err)
		}
	})

	t.Run("a digest that is not a SHA-256 is refused before any fetch", func(t *testing.T) {
		c := newEachIndexCache(t.TempDir(), 1<<20)
		var calls atomic.Int32
		if _, err := c.load(ctx, testIndexURI, "not-hex", countingFetch(nil, &calls)); err == nil || calls.Load() != 0 {
			t.Fatalf("load = %v after %d fetches; want a refusal and no fetch", err, calls.Load())
		}
	})

	t.Run("a cache with no directory still answers", func(t *testing.T) {
		c := newEachIndexCache("", 1<<20)
		body := []byte("uncached")
		var calls atomic.Int32
		for range 2 {
			if got, err := c.load(ctx, testIndexURI, shaHex(body), countingFetch(body, &calls)); err != nil || string(got) != string(body) {
				t.Fatalf("load = %q, %v", got, err)
			}
		}
		if calls.Load() != 2 {
			t.Fatalf("fetched %d times; want 2 (nothing kept)", calls.Load())
		}
	})
}
